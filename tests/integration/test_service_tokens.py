"""/api/v1/tokens lifecycle (GET/POST/DELETE) for team callers (Plan 17 Task 4)."""

from __future__ import annotations

import hashlib
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import boto3
import httpx
import pytest
from botocore.config import Config
from fastapi import FastAPI
from sqlalchemy import create_engine, delete, insert, select
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import sessionmaker

from loom.admin_secret import AdminSecretVerifier
from loom.data_lifecycle import StagingCapacity, staging_capacity_policy_digest
from loom.data_lifecycle_capacity import CAPACITY_SOURCE
from loom.db.schema import StagingLifecycleCapacity, StagingMutationEpoch, Team, Token
from loom_service.app import create_app
from loom_service.config import LoomServiceSettings
from tests.integration.test_service_auth_sessions import (
    _login,
    auth_setup,  # noqa: F401 - imported so pytest exposes the fixture here.
)

RAW_ADMIN_TOKEN = "loom_admin_" + "S" * 43


@pytest.fixture
async def svc_setup(
    monkeypatch: pytest.MonkeyPatch, postgres_url: str,
) -> AsyncIterator[tuple[FastAPI, str, UUID]]:
    for k, v in {
        "LOOM_SVC_DB_URL": postgres_url,
        "LOOM_SVC_MINIO_ENDPOINT": "http://minio:9000",
        "LOOM_SVC_MINIO_ACCESS_KEY": "x",
        "LOOM_SVC_MINIO_SECRET_KEY": "y",
        "LOOM_SVC_CONTROL_PLANE_URL": "http://cp:8080/",
        "LOOM_SVC_GATEWAY_URL": "http://gw:9100/",
    }.items():
        monkeypatch.setenv(k, v)
    settings = LoomServiceSettings(_env_file=None)
    app = create_app(settings)
    engine = create_async_engine(str(settings.db_url))
    app.state.settings = settings
    app.state.session_factory = async_sessionmaker(
        engine, expire_on_commit=False,
    )
    app.state.admin_secret_verifier = AdminSecretVerifier.from_token(
        RAW_ADMIN_TOKEN,
    )
    app.state.minio_client = boto3.client(
        "s3",
        endpoint_url=settings.minio_endpoint,
        aws_access_key_id=settings.minio_access_key.get_secret_value(),
        aws_secret_access_key=settings.minio_secret_key.get_secret_value(),
        region_name=settings.minio_region,
        config=Config(signature_version="s3v4"),
    )
    app.state.http_client = httpx.AsyncClient(
        base_url=str(settings.control_plane_url), timeout=10.0,
    )

    team_id = uuid4()
    raw = f"loom_team_{uuid4().hex}"
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(insert(Team).values(id=team_id, name=f"t-{team_id}"))
        s.execute(insert(Token).values(
            token_hash=hashlib.sha256(raw.encode()).digest(),
            type="team",
            scopes=["read:own", "submit"],
            team_id=team_id,
            issued_at=datetime.now(UTC),
            expires_at=None,
        ))
        s.commit()
    try:
        yield app, raw, team_id
    finally:
        await app.state.http_client.aclose()
        await engine.dispose()
        with sl() as s:
            s.execute(delete(Token))
            from loom.db.schema import TeamQuota
            s.execute(delete(TeamQuota))
            s.execute(delete(Team))
            s.commit()
        sync_engine.dispose()


async def test_list_own_tokens(
    svc_setup: tuple[FastAPI, str, UUID],
) -> None:
    app, raw, _team_id = svc_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get(
            "/api/v1/tokens", headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 200
    items = r.json()["items"]
    assert len(items) == 1
    assert items[0]["type"] == "team"
    assert "submit" in items[0]["scopes"]
    assert items[0]["revoked_at"] is None


async def test_mint_and_revoke(
    svc_setup: tuple[FastAPI, str, UUID],
) -> None:
    app, raw, team_id = svc_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        post = await ac.post(
            "/api/v1/tokens",
            headers={
                "Authorization": f"Bearer {RAW_ADMIN_TOKEN}",
                "X-Loom-Admin-Actor": "ops-owner",
            },
            json={
                "name": "smoke cli",
                "type": "team",
                "team_id": str(team_id),
                "scopes": ["read:own"],
                "expires_in_days": 30,
            },
        )
        assert post.status_code == 201, post.text
        new_token = post.json()["token"]
        new_prefix = post.json()["token_hash_prefix"]
        assert new_token.startswith("loom_api_")
        assert len(new_prefix) == 8

        listed = await ac.get(
            "/api/v1/tokens", headers={"Authorization": f"Bearer {raw}"},
        )
        assert any(
            it["token_hash_prefix"] == new_prefix
            for it in listed.json()["items"]
        )

        revoke = await ac.delete(
            f"/api/v1/tokens/{new_prefix}",
            headers={
                "Authorization": f"Bearer {RAW_ADMIN_TOKEN}",
                "X-Loom-Admin-Actor": "ops-owner",
            },
        )
        assert revoke.status_code == 204

        after = await ac.get(
            "/api/v1/tokens", headers={"Authorization": f"Bearer {raw}"},
        )
        revoked_items = [
            it for it in after.json()["items"]
            if it["token_hash_prefix"] == new_prefix
        ]
        assert revoked_items
        assert revoked_items[0]["revoked_at"] is not None


async def test_owner_creates_named_api_token_and_whoami_updates_last_used(
    auth_setup: tuple[FastAPI, UUID, UUID, UUID, UUID],  # noqa: F811
) -> None:
    app, _team_a, _team_b, team_c, _team_d = auth_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        body, _cookies = await _login(ac)
        user_id = str(body["user"]["id"])
        csrf = str(body["csrf_token"])
        switched = await ac.post(
            "/api/v1/auth/team",
            json={"team_id": str(team_c)},
            headers={"X-Loom-CSRF": csrf},
        )
        assert switched.status_code == 200, switched.text
        csrf = str(switched.json()["csrf_token"])

        created = await ac.post(
            "/api/v1/tokens",
            json={
                "name": "Laptop CLI",
                "type": "team",
                "scopes": ["read:own", "submit"],
                "expires_in_days": 30,
            },
            headers={"X-Loom-CSRF": csrf},
        )
        assert created.status_code == 201, created.text
        created_body = created.json()
        raw_token = created_body["token"]
        prefix = created_body["token_hash_prefix"]
        assert raw_token.startswith("loom_api_")
        assert created_body["item"]["name"] == "Laptop CLI"
        assert created_body["item"]["token_hash_prefix"] == prefix
        assert created_body["item"]["last_used_at"] is None

        listed = await ac.get("/api/v1/tokens")
        assert listed.status_code == 200, listed.text
        listed_text = listed.text
        assert raw_token not in listed_text
        item = next(
            entry for entry in listed.json()["items"]
            if entry["token_hash_prefix"] == prefix
        )
        assert item["name"] == "Laptop CLI"
        assert item["scopes"] == ["read:own", "submit"]
        assert item["last_used_at"] is None

        whoami = await ac.get(
            "/api/v1/auth/whoami",
            headers={"Authorization": f"Bearer {raw_token}"},
        )
        assert whoami.status_code == 200, whoami.text
        assert whoami.json() == {
            "auth_kind": "bearer",
            "credential_type": "user_owned_api_token",
            "principal_type": "team",
            "user_id": user_id,
            "username": "owner",
            "team_id": str(team_c),
            "team_name": "Gamma-" + str(team_c),
            "role": None,
            "scopes": ["read:own", "submit"],
            "token_prefix": prefix,
            "expires_at": created_body["expires_at"],
        }

        after_use = await ac.get("/api/v1/tokens")
        used_item = next(
            entry for entry in after_use.json()["items"]
            if entry["token_hash_prefix"] == prefix
        )
        assert used_item["last_used_at"] is not None


async def test_legacy_team_token_whoami_reports_compatibility_credential(
    svc_setup: tuple[FastAPI, str, UUID],
) -> None:
    app, raw, team_id = svc_setup
    token_hash_prefix = hashlib.sha256(raw.encode()).digest().hex()[:8]
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        whoami = await ac.get(
            "/api/v1/auth/whoami",
            headers={"Authorization": f"Bearer {raw}"},
        )

    assert whoami.status_code == 200, whoami.text
    body = whoami.json()
    assert body["auth_kind"] == "bearer"
    assert body["credential_type"] == "legacy_team_token"
    assert body["principal_type"] == "team"
    assert body["user_id"] is None
    assert body["username"] is None
    assert body["team_id"] == str(team_id)
    assert body["token_prefix"] == token_hash_prefix


async def test_readonly_probe_is_get_only_and_does_not_update_usage(
    svc_setup: tuple[FastAPI, str, UUID],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, _raw, team_id = svc_setup
    monkeypatch.setenv("LOOM_ENV", "staging")
    monkeypatch.setenv("LOOM_NAMESPACE", "loom-staging")
    raw = f"loom_readonly_{uuid4().hex}"
    token_hash = hashlib.sha256(raw.encode()).digest()
    async with app.state.session_factory() as session:
        await session.execute(insert(Token).values(
            token_hash=token_hash,
            name="staging baseline",
            type="readonly_probe",
            scopes=["read:own"],
            team_id=team_id,
            issued_at=datetime.now(UTC),
            expires_at=datetime.now(UTC) + timedelta(hours=1),
        ))
        await session.execute(insert(StagingMutationEpoch).values(
            environment="staging",
            namespace="loom-staging",
            epoch=9,
            reason="bootstrap",
        ))
        capacity = StagingCapacity(1, 2, 80, 90)
        await session.execute(insert(StagingLifecycleCapacity).values(
            environment="staging",
            namespace="loom-staging",
            object_count=capacity.object_count,
            bytes_used=capacity.bytes_used,
            disk_free_percent=capacity.disk_free_percent,
            inode_free_percent=capacity.inode_free_percent,
            policy_sha256=staging_capacity_policy_digest(),
            evidence_sha256=capacity.evidence_digest,
            source=CAPACITY_SOURCE,
            observed_at=datetime.now(UTC),
        ))
        await session.commit()

    transport = httpx.ASGITransport(app=app)
    listed_buckets: list[tuple[str, int]] = []

    def _list_objects(*, Bucket: str, MaxKeys: int) -> dict[str, object]:  # noqa: N803 - boto3 API
        listed_buckets.append((Bucket, MaxKeys))
        return {"ResponseMetadata": {"HTTPStatusCode": 200}, "KeyCount": 0}

    monkeypatch.setattr(
        app.state.minio_client,
        "list_objects_v2",
        _list_objects,
    )
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        whoami = await ac.get(
            "/api/v1/auth/whoami",
            headers={"Authorization": f"Bearer {raw}"},
        )
        rejected = await ac.post(
            "/api/v1/tokens",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "name": "forbidden",
                "type": "team",
                "scopes": ["read:own"],
                "expires_in_days": 1,
            },
        )
        readiness = await ac.get(
            "/api/v1/health/ready",
            headers={"Authorization": f"Bearer {raw}"},
        )

    assert whoami.status_code == 200, whoami.text
    assert whoami.json()["credential_type"] == "staging_readonly_probe"
    assert whoami.json()["scopes"] == ["read:own"]
    assert whoami.json()["allowed_http_methods"] == ["GET", "HEAD"]
    assert whoami.json()["readonly_authority_version"] == "v1"
    assert rejected.status_code == 401
    assert readiness.status_code == 200, readiness.text
    assert readiness.json()["status"] == "ready"
    assert readiness.json()["blockers"] == []
    assert listed_buckets == [("artifacts", 1), ("trajectories", 1)]
    async with app.state.session_factory() as session:
        usage = (await session.execute(
            select(Token.last_seen_at, Token.last_used_at).where(
                Token.token_hash == token_hash,
            ),
        )).one()
    assert usage == (None, None)


@pytest.mark.parametrize("storage_available", [True, False])
async def test_api_only_readiness_uses_own_dependencies_and_retains_authentication(
    svc_setup: tuple[FastAPI, str, UUID], monkeypatch: pytest.MonkeyPatch,
    storage_available: bool,
) -> None:
    app, token, _team_id = svc_setup
    app.state.settings = app.state.settings.model_copy(update={"service_mode": "api_only"})
    monkeypatch.setenv("LOOM_ENV", "development")
    monkeypatch.setenv("LOOM_NAMESPACE", "loom-dev-personal-readiness")
    calls: list[tuple[str, int]] = []

    def list_objects(*, Bucket: str, MaxKeys: int) -> dict[str, object]:  # noqa: N803 - boto3 API
        calls.append((Bucket, MaxKeys))
        if not storage_available:
            raise RuntimeError("private-provider-secret")
        return {"ResponseMetadata": {"HTTPStatusCode": 200}, "KeyCount": 0}

    monkeypatch.setattr(app.state.minio_client, "list_objects_v2", list_objects)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://svc") as client:
        anonymous = await client.get("/api/v1/health/ready")
        assert anonymous.status_code == 401
        assert calls == []
        response = await client.get("/api/v1/health/ready", headers={"Authorization": f"Bearer {token}"})
    assert response.status_code == (200 if storage_available else 503)
    body = response.json()
    assert body["mode"] == "api_only" and body["postgres"] == "ready"
    assert body["object_store"] == ("ready" if storage_available else "not-ready")
    assert "capacity_ready" not in body and "mutation_epoch" not in body
    assert "private-provider-secret" not in response.text
    assert calls == [("artifacts", 1), ("trajectories", 1)]


async def test_api_only_readiness_handles_database_failure_during_authentication(
    svc_setup: tuple[FastAPI, str, UUID], monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, token, _team_id = svc_setup
    app.state.settings = app.state.settings.model_copy(update={"service_mode": "api_only"})

    async def disconnected(*_args: object, **_kwargs: object) -> None:
        raise OperationalError("SELECT secret-column", {}, RuntimeError("private-db-password"))

    monkeypatch.setattr(AsyncSession, "execute", disconnected)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app, raise_app_exceptions=False), base_url="http://svc",
    ) as client:
        anonymous = await client.get("/api/v1/health/ready")
        assert anonymous.status_code == 401
        response = await client.get("/api/v1/health/ready", headers={"Authorization": f"Bearer {token}"})
    assert response.status_code == 503
    assert response.json() == {"detail": "readiness database unavailable"}
    assert "private-db-password" not in response.text and "secret-column" not in response.text


async def test_owner_rotates_token_revoking_old_secret(
    auth_setup: tuple[FastAPI, UUID, UUID, UUID, UUID],  # noqa: F811
) -> None:
    app, _team_a, _team_b, team_c, _team_d = auth_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        body, _cookies = await _login(ac)
        csrf = str(body["csrf_token"])
        switched = await ac.post(
            "/api/v1/auth/team",
            json={"team_id": str(team_c)},
            headers={"X-Loom-CSRF": csrf},
        )
        assert switched.status_code == 200, switched.text
        csrf = str(switched.json()["csrf_token"])
        created = await ac.post(
            "/api/v1/tokens",
            json={
                "name": "rotating cli",
                "type": "team",
                "scopes": ["read:own"],
                "expires_in_days": 7,
            },
            headers={"X-Loom-CSRF": csrf},
        )
        assert created.status_code == 201, created.text
        old_raw = created.json()["token"]
        old_prefix = created.json()["token_hash_prefix"]

        rotated = await ac.post(
            f"/api/v1/tokens/{old_prefix}/rotate",
            headers={"X-Loom-CSRF": csrf},
        )
        assert rotated.status_code == 200, rotated.text
        new_raw = rotated.json()["token"]
        new_prefix = rotated.json()["token_hash_prefix"]
        assert new_raw.startswith("loom_api_")
        assert new_raw != old_raw
        assert new_prefix != old_prefix
        assert rotated.json()["item"]["name"] == "rotating cli"

        old_whoami = await ac.get(
            "/api/v1/auth/whoami",
            headers={"Authorization": f"Bearer {old_raw}"},
        )
        assert old_whoami.status_code == 401
        new_whoami = await ac.get(
            "/api/v1/auth/whoami",
            headers={"Authorization": f"Bearer {new_raw}"},
        )
        assert new_whoami.status_code == 200
        assert new_whoami.json()["token_prefix"] == new_prefix


async def test_plain_submit_token_cannot_manage_tokens(
    svc_setup: tuple[FastAPI, str, UUID],
) -> None:
    app, raw, _team_id = svc_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        create = await ac.post(
            "/api/v1/tokens",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "name": "illegal escalation",
                "type": "team",
                "scopes": ["read:own"],
                "expires_in_days": 1,
            },
        )
        revoke = await ac.delete(
            "/api/v1/tokens/deadbeef",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert create.status_code == 403
    assert "tokens:manage" in create.json()["detail"]
    assert revoke.status_code == 403
    assert "tokens:manage" in revoke.json()["detail"]


async def test_expired_api_token_fails_whoami(
    svc_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
) -> None:
    app, _raw, team_id = svc_setup
    expired_raw = f"loom_api_{uuid4().hex}"
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(insert(Token).values(
            token_hash=hashlib.sha256(expired_raw.encode()).digest(),
            type="team",
            name="expired",
            scopes=["read:own"],
            team_id=team_id,
            issued_at=datetime.now(UTC) - timedelta(days=8),
            expires_at=datetime.now(UTC) - timedelta(days=1),
        ))
        s.commit()
    try:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://svc",
        ) as ac:
            whoami = await ac.get(
                "/api/v1/auth/whoami",
                headers={"Authorization": f"Bearer {expired_raw}"},
            )
    finally:
        sync_engine.dispose()
    assert whoami.status_code == 401


async def test_admin_audit_redacts_raw_api_token(
    svc_setup: tuple[FastAPI, str, UUID],
) -> None:
    app, _raw, team_id = svc_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        created = await ac.post(
            "/api/v1/tokens",
            headers={
                "Authorization": f"Bearer {RAW_ADMIN_TOKEN}",
                "X-Loom-Admin-Actor": "ops-owner",
            },
            json={
                "name": "ops issued cli",
                "type": "team",
                "team_id": str(team_id),
                "scopes": ["read:own", "submit"],
                "expires_in_days": 7,
            },
        )
        assert created.status_code == 201, created.text
        raw_token = created.json()["token"]
        prefix = created.json()["token_hash_prefix"]
        audit = await ac.get(
            "/api/v1/admin/audit-events",
            headers={"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"},
        )
    assert raw_token not in audit.text
    matching = [
        item for item in audit.json()["items"]
        if item["action"] == "token.create" and item["target_id"] == prefix
    ]
    assert matching
    assert matching[0]["metadata"]["token_hash_prefix"] == prefix
    assert matching[0]["metadata"]["token_name"] == "ops issued cli"


async def test_post_rejects_admin_scope_from_team_caller(
    svc_setup: tuple[FastAPI, str, UUID],
) -> None:
    app, raw, _team_id = svc_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.post(
            "/api/v1/tokens",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "type": "team",
                "name": "bad admin scope",
                "scopes": ["admin:tokens"],
                "expires_in_days": 1,
            },
        )
    assert r.status_code == 400
    assert "database-backed admin scopes" in r.json()["detail"]


async def test_post_rejects_admin_type_from_team_caller(
    svc_setup: tuple[FastAPI, str, UUID],
) -> None:
    app, raw, _team_id = svc_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.post(
            "/api/v1/tokens",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "type": "admin",
                "name": "bad admin type",
                "scopes": ["read:own"],
                "expires_in_days": 1,
            },
        )
    assert r.status_code == 400
    assert "database-backed admin tokens" in r.json()["detail"]


async def test_post_rejects_admin_type_from_singleton_admin(
    svc_setup: tuple[FastAPI, str, UUID],
) -> None:
    app, _raw, _team_id = svc_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.post(
            "/api/v1/tokens",
            headers={
                "Authorization": f"Bearer {RAW_ADMIN_TOKEN}",
                "X-Loom-Admin-Actor": "ops-owner",
            },
            json={
                "type": "admin",
                "name": "bad admin type",
                "scopes": ["admin:tokens"],
                "expires_in_days": 1,
            },
        )
    assert r.status_code == 400
    assert "database-backed admin tokens" in r.json()["detail"]


async def test_post_rejects_admin_scopes_from_singleton_admin(
    svc_setup: tuple[FastAPI, str, UUID],
) -> None:
    app, _raw, team_id = svc_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.post(
            "/api/v1/tokens",
            headers={
                "Authorization": f"Bearer {RAW_ADMIN_TOKEN}",
                "X-Loom-Admin-Actor": "ops-owner",
            },
            json={
                "type": "team",
                "name": "bad admin scope",
                "team_id": str(team_id),
                "scopes": ["admin:tokens"],
                "expires_in_days": 1,
            },
        )
    assert r.status_code == 400
    assert "database-backed admin scopes" in r.json()["detail"]


async def test_db_backed_admin_tokens_are_not_accepted(
    svc_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
) -> None:
    app, _raw, _team_id = svc_setup
    db_admin_raw = f"loom_admin_{uuid4().hex}"
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(insert(Token).values(
            token_hash=hashlib.sha256(db_admin_raw.encode()).digest(),
            type="admin",
            scopes=["admin:tokens", "admin:rate_cards"],
            team_id=None,
            issued_at=datetime.now(UTC),
            expires_at=None,
        ))
        s.commit()
    try:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://svc",
        ) as ac:
            r = await ac.get(
                "/api/v1/tokens",
                headers={"Authorization": f"Bearer {db_admin_raw}"},
            )
    finally:
        sync_engine.dispose()
    assert r.status_code == 401


async def test_unauthenticated_401(
    svc_setup: tuple[FastAPI, str, UUID],
) -> None:
    app, _raw, _t = svc_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get("/api/v1/tokens")
    assert r.status_code == 401


async def test_revoke_invalid_prefix_400(
    svc_setup: tuple[FastAPI, str, UUID],
) -> None:
    app, raw, _t = svc_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.delete(
            "/api/v1/tokens/short",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 400


async def test_revoke_unknown_prefix_404(
    svc_setup: tuple[FastAPI, str, UUID],
) -> None:
    app, _raw, _t = svc_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.delete(
            "/api/v1/tokens/deadbeef",
            headers={
                "Authorization": f"Bearer {RAW_ADMIN_TOKEN}",
                "X-Loom-Admin-Actor": "ops-owner",
            },
        )
    assert r.status_code == 404


async def test_revoke_rejects_non_hex_prefix(
    svc_setup: tuple[FastAPI, str, UUID],
) -> None:
    app, raw, _t = svc_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        # 8 chars but contains non-hex.
        r = await ac.delete(
            "/api/v1/tokens/xyzzyabc",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 400


async def test_post_rejects_unknown_scope(
    svc_setup: tuple[FastAPI, str, UUID],
) -> None:
    """Audit M2: an unrecognized scope is rejected at request time
    rather than silently stored."""
    app, raw, _t = svc_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.post(
            "/api/v1/tokens",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "type": "team",
                "name": "bad unknown scope",
                "scopes": ["bogus:typo"],
                "expires_in_days": 1,
            },
        )
    assert r.status_code == 400
    assert "unrecognized scopes" in r.json()["detail"]


async def test_revoke_other_team_token_returns_404(
    svc_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
) -> None:
    """Audit H1+M1: a team caller cannot revoke (or even probe) a
    token belonging to another team. The lookup is filtered by
    team_id BEFORE the prefix scan, so a colliding prefix gets 404
    (not 403, not silent success)."""
    app, raw, _team_id = svc_setup

    # Seed a token in a different team.
    from datetime import UTC, datetime
    other_team = uuid4()
    other_raw = f"loom_team_{uuid4().hex}"
    other_hash = hashlib.sha256(other_raw.encode()).digest()
    manager_raw = f"loom_team_{uuid4().hex}"
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(insert(Team).values(
            id=other_team, name=f"other-{other_team}",
        ))
        own_team = (s.execute(
            select(Team.id).join(Token, Token.team_id == Team.id)
            .where(Token.token_hash == hashlib.sha256(raw.encode()).digest()),
        )).scalar_one()
        s.execute(insert(Token).values(
            token_hash=hashlib.sha256(manager_raw.encode()).digest(),
            type="team",
            scopes=["read:own", "tokens:manage"],
            team_id=own_team,
            issued_at=datetime.now(UTC),
            expires_at=None,
        ))
        s.execute(insert(Token).values(
            token_hash=other_hash, type="team",
            scopes=["read:own"], team_id=other_team,
            issued_at=datetime.now(UTC), expires_at=None,
        ))
        s.commit()
    try:
        other_prefix = other_hash.hex()[:8]
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://svc",
        ) as ac:
            r = await ac.delete(
                f"/api/v1/tokens/{other_prefix}",
                headers={"Authorization": f"Bearer {manager_raw}"},
            )
        # The other team's token is invisible to this caller; 404
        # rather than 403 (which would have leaked existence).
        assert r.status_code == 404
        # And the other team's token is still unrevoked.
        with sl() as s:
            from sqlalchemy import select as sa_select
            still = s.execute(
                sa_select(Token).where(Token.token_hash == other_hash),
            ).scalar_one()
        assert still.revoked_at is None
    finally:
        with sl() as s:
            s.execute(delete(Token).where(
                Token.token_hash == hashlib.sha256(manager_raw.encode()).digest(),
            ))
            s.execute(delete(Token).where(Token.token_hash == other_hash))
            from loom.db.schema import TeamQuota
            s.execute(delete(TeamQuota).where(TeamQuota.team_id == other_team))
            s.execute(delete(Team).where(Team.id == other_team))
            s.commit()
        sync_engine.dispose()
