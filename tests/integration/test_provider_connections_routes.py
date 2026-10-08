"""Route integration tests for provider_connections CRUD.

Covers the full request-response cycle: auth, SSRF validation (DNS
stubbed), team isolation, soft-delete, pricing validation. Master-key
load is patched so the SecretStore can construct without env vars.
"""

from __future__ import annotations

import base64
import hashlib
import json
import socket
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import UUID, uuid4

import boto3
import httpx
import pytest
from botocore.config import Config
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, delete, insert
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import sessionmaker

from loom.admin_secret import AdminSecretVerifier
from loom.db.schema import (
    AdminAuditEvent,
    ProviderConnection,
    ProviderConnectionShare,
    Secret,
    Team,
    TeamQuota,
    Token,
)
from loom_service.app import create_app
from loom_service.config import LoomServiceSettings
from loom_service.provider_connection_lookup import validate_provider_connection

# Use a fixed test master key so tests are deterministic.
_TEST_MASTER_KEY = base64.b64encode(bytes(range(32))).decode()
_WRONG_TEST_MASTER_KEY = base64.b64encode(bytes(reversed(range(32)))).decode()
RAW_ADMIN_TOKEN = "loom_admin_" + "P" * 43


@pytest.fixture(autouse=True)
def _stub_dns(monkeypatch: pytest.MonkeyPatch) -> None:
    """Stub `socket.getaddrinfo` AT THE MODULE the service imports it
    from (NOT globally — the global socket.getaddrinfo is still needed
    by psycopg, httpx, etc. to talk to the test postgres container).

    Returns mapped IPs for the test hostnames the routes will see;
    falls through to real DNS for anything else (a deliberate
    pass-through so this stub never accidentally affects database/
    HTTP infrastructure)."""
    test_hosts = {
        "api.openai.com": ["104.18.0.1"],
        "api.anthropic.com": ["104.18.0.2"],
        "vllm.lab.local": ["10.0.5.42"],
        "localhost-svc.test": ["127.0.0.1"],
        "metadata.aws.test": ["169.254.169.254"],
    }

    real_getaddrinfo = socket.getaddrinfo

    def _stub(host, port, *args, **kwargs):
        if host in test_hosts:
            return [(socket.AF_INET, socket.SOCK_STREAM, 0, "", (ip, 0)) for ip in test_hosts[host]]
        return real_getaddrinfo(host, port, *args, **kwargs)

    monkeypatch.setattr(
        "loom_service.provider_connections_service.socket.getaddrinfo",
        _stub,
    )


@pytest.fixture
async def app_setup(
    monkeypatch: pytest.MonkeyPatch,
    postgres_url: str,
) -> AsyncIterator[tuple[FastAPI, dict[str, str], dict[str, UUID]]]:
    """Boot a real FastAPI app with two teams + tokens, returning the
    app + the raw token strings (header value) + the team UUIDs."""
    for k, v in {
        "LOOM_SVC_DB_URL": postgres_url,
        "LOOM_SVC_MINIO_ENDPOINT": "http://minio:9000",
        "LOOM_SVC_MINIO_ACCESS_KEY": "x",
        "LOOM_SVC_MINIO_SECRET_KEY": "y",
        "LOOM_SVC_CONTROL_PLANE_URL": "http://cp:8080/",
        "LOOM_SVC_GATEWAY_URL": "http://gw:9100/",
        "LOOM_SECRET_STORE_MASTER_KEY": _TEST_MASTER_KEY,
    }.items():
        monkeypatch.setenv(k, v)
    settings = LoomServiceSettings(_env_file=None)
    app = create_app(settings)
    engine = create_async_engine(str(settings.db_url))
    app.state.settings = settings
    app.state.session_factory = async_sessionmaker(
        engine,
        expire_on_commit=False,
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
    app.state.http_client = httpx.AsyncClient(base_url="http://cp")
    app.state.gateway_client = httpx.AsyncClient(base_url="http://gw")

    # Seed two teams + per-team tokens. Admin access comes from the singleton
    # admin secret verifier wired above, not from DB-backed admin rows.
    team_a = uuid4()
    team_b = uuid4()
    raw_a = f"loom_team_{uuid4().hex}"
    raw_b = f"loom_team_{uuid4().hex}"
    raw_a_limited = f"loom_team_{uuid4().hex}"
    raw_b_limited = f"loom_team_{uuid4().hex}"

    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(insert(Team).values(id=team_a, name=f"team-a-{team_a}"))
        s.execute(insert(Team).values(id=team_b, name=f"team-b-{team_b}"))
        s.execute(insert(TeamQuota).values(team_id=team_a))
        s.execute(insert(TeamQuota).values(team_id=team_b))
        s.execute(
            insert(Token).values(
                token_hash=hashlib.sha256(raw_a.encode()).digest(),
                type="team",
                scopes=["read:own", "write:own", "providers:manage"],
                team_id=team_a,
                issued_at=datetime.now(UTC),
            )
        )
        s.execute(
            insert(Token).values(
                token_hash=hashlib.sha256(raw_b.encode()).digest(),
                type="team",
                scopes=["read:own", "write:own", "providers:manage"],
                team_id=team_b,
                issued_at=datetime.now(UTC),
            )
        )
        s.execute(
            insert(Token).values(
                token_hash=hashlib.sha256(raw_a_limited.encode()).digest(),
                type="team",
                scopes=["read:own", "submit"],
                team_id=team_a,
                issued_at=datetime.now(UTC),
            )
        )
        s.execute(
            insert(Token).values(
                token_hash=hashlib.sha256(raw_b_limited.encode()).digest(),
                type="team",
                scopes=["read:own", "submit"],
                team_id=team_b,
                issued_at=datetime.now(UTC),
            )
        )
        s.commit()

    tokens = {
        "team_a": raw_a,
        "team_a_limited": raw_a_limited,
        "team_b": raw_b,
        "team_b_limited": raw_b_limited,
        "admin": RAW_ADMIN_TOKEN,
    }
    team_ids = {"a": team_a, "b": team_b}
    try:
        yield app, tokens, team_ids
    finally:
        await app.state.gateway_client.aclose()
        await app.state.http_client.aclose()
        await engine.dispose()
        with sl() as s:
            s.execute(delete(AdminAuditEvent))
            s.execute(delete(ProviderConnectionShare))
            s.execute(delete(ProviderConnection))
            s.execute(delete(Secret))
            s.execute(delete(Token))
            s.execute(delete(TeamQuota))
            s.execute(delete(Team))
            s.commit()
        sync_engine.dispose()


def _client(app: FastAPI) -> TestClient:
    return TestClient(app)


def _auth(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def _admin_headers(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


# ──────────────────────────────────────────────────────────────────────
# Create
# ──────────────────────────────────────────────────────────────────────


def test_create_returns_201_with_public_response(app_setup) -> None:
    app, tokens, _ = app_setup
    c = _client(app)
    r = c.post(
        "/api/v1/provider-connections",
        headers=_auth(tokens["team_a"]),
        json={
            "name": "openai-prod",
            "type": "openai-compatible",
            "base_url": "https://api.openai.com/v1",
            "api_key": "sk-test-XXXX",
        },
    )
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["name"] == "openai-prod"
    assert body["type"] == "openai-compatible"
    assert body["base_url"] == "https://api.openai.com/v1"
    assert body["upstream_host"] == "api.openai.com"
    assert body["resolved_egress_ips"] == ["104.18.0.1"]
    assert body["status"] == "pending"
    # openai-compatible defaults to tokens-only.
    assert body["pricing_mode"] == "usage_only"
    assert body["custom_pricing"] is None
    # openai-compatible is ambiguous at the protocol layer, but the
    # default rate-card provider preserves the common OpenAI-hosted path
    # and can be corrected per connection for Together/Fireworks/etc.
    assert body["catalog_id"] is None
    # api_key MUST NOT round-trip; the response only carries opaque
    # public fields.
    assert "api_key" not in body
    assert "encrypted_api_key_ref" not in body


def test_create_protocol_does_not_imply_supplier(app_setup) -> None:
    app, tokens, _ = app_setup
    c = _client(app)
    r = c.post(
        "/api/v1/provider-connections",
        headers=_auth(tokens["team_a"]),
        json={
            "name": "anthropic-prod",
            "type": "anthropic",
            "base_url": "https://api.anthropic.com/",
            "api_key": "sk-ant-XXXX",
        },
    )
    assert r.status_code == 201
    assert r.json()["pricing_mode"] == "usage_only"
    assert r.json()["supplier_id"] is None


def test_create_custom_defaults_to_no_rate_card_provider(app_setup) -> None:
    app, tokens, _ = app_setup
    c = _client(app)
    r = c.post(
        "/api/v1/provider-connections",
        headers=_auth(tokens["team_a"]),
        json={
            "name": "lab-vllm",
            "type": "custom",
            "base_url": "https://api.openai.com/",
            "api_key": "sk-custom-XXXX",
        },
    )
    assert r.status_code == 201
    body = r.json()
    assert body["pricing_mode"] == "usage_only"
    assert body["catalog_id"] is None


def test_create_accepts_explicit_rate_card_provider(app_setup) -> None:
    app, tokens, _ = app_setup
    c = _client(app)
    r = c.post(
        "/api/v1/provider-connections",
        headers=_auth(tokens["team_a"]),
        json={
            "name": "together-prod",
            "type": "openai-compatible",
            "base_url": "https://api.openai.com/",
            "api_key": "sk-together-XXXX",
            "pricing_mode": "catalog", "catalog_id": "supplier:az-gptplus5",
        },
    )
    assert r.status_code == 201
    assert r.json()["catalog_id"] == "supplier:az-gptplus5"


def test_create_operator_supplied_requires_pricing_data(app_setup) -> None:
    app, tokens, _ = app_setup
    c = _client(app)
    r = c.post(
        "/api/v1/provider-connections",
        headers=_auth(tokens["team_a"]),
        json={
            "name": "n",
            "type": "openai-compatible",
            "base_url": "https://api.openai.com/",
            "api_key": "k",
            "pricing_mode": "custom",
        },
    )
    assert r.status_code == 400
    assert "requires a model price table" in r.json()["detail"]


def test_create_operator_supplied_negative_price_rejected(app_setup) -> None:
    app, tokens, _ = app_setup
    c = _client(app)
    r = c.post(
        "/api/v1/provider-connections",
        headers=_auth(tokens["team_a"]),
        json={
            "name": "n",
            "type": "custom",
            "base_url": "https://api.openai.com/",
            "api_key": "k",
            "pricing_mode": "custom",
            "custom_pricing": {"model-a": {
                "input_usd_per_1m": -1.0,
                "output_usd_per_1m": 1.0,
            }},
        },
    )
    assert r.status_code == 422
    assert "greater_than_equal" in r.text


def test_create_rejects_private_ip_by_default(app_setup) -> None:
    """team.allow_private_endpoints defaults to False — RFC1918 host
    rejected with a 400 mentioning the policy flag."""
    app, tokens, _ = app_setup
    c = _client(app)
    r = c.post(
        "/api/v1/provider-connections",
        headers=_auth(tokens["team_a"]),
        json={
            "name": "n",
            "type": "openai-compatible",
            "base_url": "https://vllm.lab.local:8000/v1",
            "api_key": "k",
        },
    )
    assert r.status_code == 400
    assert "private range" in r.json()["detail"]
    assert "allow_private_endpoints" in r.json()["detail"]


def test_create_allows_private_ip_when_team_flag_on(app_setup) -> None:
    app, tokens, team_ids = app_setup
    # Flip the flag via direct DB write (the admin verb lands in a
    # follow-up PR; tests bypass).
    sync_engine = create_engine(str(app.state.settings.db_url))
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(
            TeamQuota.__table__.update()
            .where(TeamQuota.team_id == team_ids["a"])
            .values(allow_private_endpoints=True),
        )
        s.commit()
    sync_engine.dispose()

    c = _client(app)
    r = c.post(
        "/api/v1/provider-connections",
        headers=_auth(tokens["team_a"]),
        json={
            "name": "n",
            "type": "openai-compatible",
            "base_url": "https://vllm.lab.local:8000/v1",
            "api_key": "k",
        },
    )
    assert r.status_code == 201, r.text
    assert r.json()["resolved_egress_ips"] == ["10.0.5.42"]


def test_create_rejects_loopback_even_with_flag(app_setup) -> None:
    """allow_private opens RFC1918 + ULA only — loopback stays
    rejected unconditionally on any team."""
    app, tokens, team_ids = app_setup
    sync_engine = create_engine(str(app.state.settings.db_url))
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(
            TeamQuota.__table__.update()
            .where(TeamQuota.team_id == team_ids["a"])
            .values(allow_private_endpoints=True),
        )
        s.commit()
    sync_engine.dispose()

    c = _client(app)
    r = c.post(
        "/api/v1/provider-connections",
        headers=_auth(tokens["team_a"]),
        json={
            "name": "n",
            "type": "openai-compatible",
            "base_url": "https://localhost-svc.test/v1",
            "api_key": "k",
        },
    )
    assert r.status_code == 400
    assert "127.0.0" in r.json()["detail"]


def test_create_rejects_metadata_endpoint_even_with_flag(app_setup) -> None:
    """169.254.169.254 (AWS/GCP metadata) stays rejected even with
    allow_private. Documented carve-out."""
    app, tokens, team_ids = app_setup
    sync_engine = create_engine(str(app.state.settings.db_url))
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(
            TeamQuota.__table__.update()
            .where(TeamQuota.team_id == team_ids["a"])
            .values(allow_private_endpoints=True),
        )
        s.commit()
    sync_engine.dispose()

    c = _client(app)
    r = c.post(
        "/api/v1/provider-connections",
        headers=_auth(tokens["team_a"]),
        json={
            "name": "n",
            "type": "openai-compatible",
            "base_url": "https://metadata.aws.test/",
            "api_key": "k",
        },
    )
    assert r.status_code == 400
    assert "169.254" in r.json()["detail"]


def test_create_duplicate_active_name_returns_409(app_setup) -> None:
    app, tokens, _ = app_setup
    c = _client(app)
    payload = {
        "name": "dup",
        "type": "openai-compatible",
        "base_url": "https://api.openai.com/",
        "api_key": "k",
    }
    assert (
        c.post(
            "/api/v1/provider-connections", headers=_auth(tokens["team_a"]), json=payload
        ).status_code
        == 201
    )
    r2 = c.post("/api/v1/provider-connections", headers=_auth(tokens["team_a"]), json=payload)
    assert r2.status_code == 409
    assert "already exists" in r2.json()["detail"]


def test_create_requires_team_scoped_token(app_setup) -> None:
    """An admin-only token (team_id is None) can't create — the row
    needs a team_id FK."""
    app, tokens, _ = app_setup
    c = _client(app)
    r = c.post(
        "/api/v1/provider-connections",
        headers=_auth(tokens["admin"]),
        json={
            "name": "n",
            "type": "openai-compatible",
            "base_url": "https://api.openai.com/",
            "api_key": "k",
        },
    )
    assert r.status_code == 400
    assert "team-scoped" in r.json()["detail"]


def test_create_requires_provider_management_scope(app_setup) -> None:
    app, tokens, _ = app_setup
    c = _client(app)
    r = c.post(
        "/api/v1/provider-connections",
        headers=_auth(tokens["team_b_limited"]),
        json={
            "name": "n",
            "type": "openai-compatible",
            "base_url": "https://api.openai.com/",
            "api_key": "k",
        },
    )
    assert r.status_code == 403
    assert "providers:manage" in r.json()["detail"]


# ──────────────────────────────────────────────────────────────────────
# List / Get
# ──────────────────────────────────────────────────────────────────────


def test_list_filters_to_calling_team(app_setup) -> None:
    """team_b creates a connection; team_a should not see it via list."""
    app, tokens, _ = app_setup
    c = _client(app)
    c.post(
        "/api/v1/provider-connections",
        headers=_auth(tokens["team_b"]),
        json={
            "name": "team-b-conn",
            "type": "openai-compatible",
            "base_url": "https://api.openai.com/",
            "api_key": "k",
        },
    )

    r = c.get("/api/v1/provider-connections", headers=_auth(tokens["team_a"]))
    assert r.status_code == 200
    assert r.json()["items"] == []

    r = c.get("/api/v1/provider-connections", headers=_auth(tokens["team_b"]))
    assert r.status_code == 200
    assert len(r.json()["items"]) == 1
    assert r.json()["items"][0]["name"] == "team-b-conn"


def test_list_admin_sees_all_teams(app_setup) -> None:
    app, tokens, _ = app_setup
    c = _client(app)
    c.post(
        "/api/v1/provider-connections",
        headers=_auth(tokens["team_a"]),
        json={
            "name": "a-conn",
            "type": "openai-compatible",
            "base_url": "https://api.openai.com/",
            "api_key": "k",
        },
    )
    c.post(
        "/api/v1/provider-connections",
        headers=_auth(tokens["team_b"]),
        json={
            "name": "b-conn",
            "type": "openai-compatible",
            "base_url": "https://api.openai.com/",
            "api_key": "k",
        },
    )

    r = c.get("/api/v1/provider-connections", headers=_auth(tokens["admin"]))
    assert r.status_code == 200
    names = {item["name"] for item in r.json()["items"]}
    assert names == {"a-conn", "b-conn"}


def test_list_admin_can_filter_by_team_id(app_setup) -> None:
    app, tokens, team_ids = app_setup
    c = _client(app)
    c.post(
        "/api/v1/provider-connections",
        headers=_auth(tokens["team_a"]),
        json={
            "name": "a-conn",
            "type": "openai-compatible",
            "base_url": "https://api.openai.com/",
            "api_key": "k",
        },
    )
    c.post(
        "/api/v1/provider-connections",
        headers=_auth(tokens["team_b"]),
        json={
            "name": "b-conn",
            "type": "openai-compatible",
            "base_url": "https://api.openai.com/",
            "api_key": "k",
        },
    )

    r = c.get(
        "/api/v1/provider-connections",
        headers=_auth(tokens["admin"]),
        params={"team_id": str(team_ids["b"])},
    )

    assert r.status_code == 200
    items = r.json()["items"]
    assert [item["name"] for item in items] == ["b-conn"]
    assert {item["team_id"] for item in items} == {str(team_ids["b"])}


def test_get_cross_team_returns_404_not_403(app_setup) -> None:
    """Existence-leak prevention: team_a probing for team_b's row
    gets the same 404 as a nonexistent ID."""
    app, tokens, _ = app_setup
    c = _client(app)
    create = c.post(
        "/api/v1/provider-connections",
        headers=_auth(tokens["team_b"]),
        json={
            "name": "b-only",
            "type": "openai-compatible",
            "base_url": "https://api.openai.com/",
            "api_key": "k",
        },
    )
    b_id = create.json()["id"]

    r = c.get(f"/api/v1/provider-connections/{b_id}", headers=_auth(tokens["team_a"]))
    assert r.status_code == 404
    # Should NOT be 403 — that would leak existence.

    # Same response for a nonexistent ID.
    r_nope = c.get(
        f"/api/v1/provider-connections/{uuid4()}",
        headers=_auth(tokens["team_a"]),
    )
    assert r_nope.status_code == 404
    assert r.json()["detail"] == r_nope.json()["detail"]


def test_owner_share_makes_provider_visible_and_valid_for_target_team(app_setup) -> None:
    """A provider-owning team can share one provider with another team.

    The target team can list/get/use the shared provider by id, but still
    never receives raw secret material and does not become the owner team.
    """
    app, tokens, team_ids = app_setup
    c = _client(app)
    create = c.post(
        "/api/v1/provider-connections",
        headers=_auth(tokens["team_b"]),
        json={
            "name": "shared-yibuapi",
            "type": "openai-compatible",
            "base_url": "https://api.openai.com/",
            "api_key": "k",
        },
    )
    assert create.status_code == 201, create.text
    conn_id = create.json()["id"]

    before = c.get("/api/v1/provider-connections", headers=_auth(tokens["team_a"]))
    assert before.status_code == 200
    assert before.json()["items"] == []

    share = c.post(
        f"/api/v1/provider-connections/{conn_id}/shares",
        headers=_auth(tokens["team_b"]),
        json={"target_team_id": str(team_ids["a"])},
    )
    assert share.status_code == 201, share.text
    assert share.json() == {
        "provider_connection_id": conn_id,
        "provider_name": "shared-yibuapi",
        "provider_owner_team_id": str(team_ids["b"]),
        "target_team_id": str(team_ids["a"]),
    }

    listed = c.get("/api/v1/provider-connections", headers=_auth(tokens["team_a"]))
    assert listed.status_code == 200
    items = listed.json()["items"]
    assert [item["id"] for item in items] == [conn_id]
    assert items[0]["team_id"] == str(team_ids["b"])
    assert "api_key" not in items[0]
    assert "encrypted_api_key_ref" not in items[0]

    detail = c.get(
        f"/api/v1/provider-connections/{conn_id}",
        headers=_auth(tokens["team_a"]),
    )
    assert detail.status_code == 200
    assert detail.json()["name"] == "shared-yibuapi"
    assert "api_key" not in detail.json()
    assert "encrypted_api_key_ref" not in detail.json()

    async def _validate() -> None:
        async with app.state.session_factory() as session:
            await validate_provider_connection(
                session,
                UUID(conn_id),
                team_id=team_ids["a"],
            )

    import anyio

    anyio.run(_validate)

    sync_engine = create_engine(str(app.state.settings.db_url))
    sl = sessionmaker(sync_engine)
    with sl() as s:
        events = (
            s.execute(
                AdminAuditEvent.__table__.select().where(
                    AdminAuditEvent.target_id == conn_id,
                    AdminAuditEvent.action == "provider_connection.share",
                ),
            )
            .mappings()
            .all()
        )
    sync_engine.dispose()
    assert len(events) == 1
    metadata = events[0]["metadata"]
    assert metadata == {
        "provider_connection_id": conn_id,
        "provider_name": "shared-yibuapi",
        "provider_owner_team_id": str(team_ids["b"]),
        "target_team_id": str(team_ids["a"]),
        "action": "share",
    }
    assert "api_key" not in json.dumps(metadata)


def test_shared_provider_cannot_be_mutated_by_target_team(app_setup) -> None:
    app, tokens, team_ids = app_setup
    c = _client(app)
    create = c.post(
        "/api/v1/provider-connections",
        headers=_auth(tokens["team_b"]),
        json={
            "name": "owner-only-mutate",
            "type": "openai-compatible",
            "base_url": "https://api.openai.com/",
            "api_key": "k",
        },
    )
    conn_id = create.json()["id"]
    c.post(
        f"/api/v1/provider-connections/{conn_id}/shares",
        headers=_auth(tokens["team_b"]),
        json={"target_team_id": str(team_ids["a"])},
    )

    update = c.patch(
        f"/api/v1/provider-connections/{conn_id}",
        headers=_auth(tokens["team_a"]),
        json={"allowed_models": ["glm-5.1-thinking"]},
    )
    assert update.status_code == 404

    delete_resp = c.delete(
        f"/api/v1/provider-connections/{conn_id}",
        headers=_auth(tokens["team_a"]),
    )
    assert delete_resp.status_code == 404


def test_provider_share_requires_owner_manage_scope_or_platform_admin(app_setup) -> None:
    app, tokens, team_ids = app_setup
    c = _client(app)
    create = c.post(
        "/api/v1/provider-connections",
        headers=_auth(tokens["team_b"]),
        json={
            "name": "admin-only-share",
            "type": "openai-compatible",
            "base_url": "https://api.openai.com/",
            "api_key": "k",
        },
    )
    conn_id = create.json()["id"]

    non_owner_attempt = c.post(
        f"/api/v1/provider-connections/{conn_id}/shares",
        headers=_auth(tokens["team_a"]),
        json={"target_team_id": str(team_ids["a"])},
    )
    assert non_owner_attempt.status_code == 404

    owner_limited_attempt = c.post(
        f"/api/v1/provider-connections/{conn_id}/shares",
        headers=_auth(tokens["team_a_limited"]),
        json={"target_team_id": str(team_ids["a"])},
    )
    assert owner_limited_attempt.status_code == 403
    assert "providers:manage" in owner_limited_attempt.json()["detail"]

    admin_missing_actor = c.post(
        f"/api/v1/provider-connections/{conn_id}/shares",
        headers=_auth(tokens["admin"]),
        json={"target_team_id": str(team_ids["a"])},
    )
    assert admin_missing_actor.status_code == 400
    assert "X-Loom-Admin-Actor" in admin_missing_actor.json()["detail"]

    admin_ok = c.post(
        f"/api/v1/provider-connections/{conn_id}/shares",
        headers={
            **_auth(tokens["admin"]),
            "X-Loom-Admin-Actor": "release-operator",
        },
        json={"target_team_id": str(team_ids["a"])},
    )
    assert admin_ok.status_code == 201, admin_ok.text


def test_admin_unshare_revokes_target_team_visibility_and_validation(app_setup) -> None:
    app, tokens, team_ids = app_setup
    c = _client(app)
    create = c.post(
        "/api/v1/provider-connections",
        headers=_auth(tokens["team_b"]),
        json={
            "name": "temporary-share",
            "type": "openai-compatible",
            "base_url": "https://api.openai.com/",
            "api_key": "k",
        },
    )
    conn_id = create.json()["id"]
    assert c.post(
        f"/api/v1/provider-connections/{conn_id}/shares",
        headers=_auth(tokens["team_b"]),
        json={"target_team_id": str(team_ids["a"])},
    ).status_code == 201

    removed = c.delete(
        f"/api/v1/provider-connections/{conn_id}/shares/{team_ids['a']}",
        headers=_auth(tokens["team_b"]),
    )
    assert removed.status_code == 204

    listed = c.get("/api/v1/provider-connections", headers=_auth(tokens["team_a"]))
    assert listed.status_code == 200
    assert listed.json()["items"] == []

    async def _validate_rejected() -> None:
        async with app.state.session_factory() as session:
            with pytest.raises(Exception) as excinfo:
                await validate_provider_connection(
                    session,
                    UUID(conn_id),
                    team_id=team_ids["a"],
                )
            assert excinfo.value.status_code == 404

    import anyio

    anyio.run(_validate_rejected)


# ──────────────────────────────────────────────────────────────────────
# Update
# ──────────────────────────────────────────────────────────────────────


def test_update_base_url_re_resolves_and_re_pendings(app_setup) -> None:
    app, tokens, _ = app_setup
    c = _client(app)
    create = c.post(
        "/api/v1/provider-connections",
        headers=_auth(tokens["team_a"]),
        json={
            "name": "n",
            "type": "openai-compatible",
            "base_url": "https://api.openai.com/",
            "api_key": "k",
        },
    )
    conn_id = create.json()["id"]

    r = c.patch(
        f"/api/v1/provider-connections/{conn_id}",
        headers=_auth(tokens["team_a"]),
        json={"base_url": "https://api.anthropic.com/"},
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["base_url"] == "https://api.anthropic.com/"
    assert body["upstream_host"] == "api.anthropic.com"
    assert body["resolved_egress_ips"] == ["104.18.0.2"]
    # Re-validation triggers status='pending'.
    assert body["status"] == "pending"


def test_update_api_key_rotates_ref(app_setup) -> None:
    """PATCH with api_key encrypts the new value and swaps the
    encrypted_api_key_ref. Retirement is atomic and both refs survive grace."""
    app, tokens, _team_ids = app_setup
    c = _client(app)
    create = c.post(
        "/api/v1/provider-connections",
        headers=_auth(tokens["team_a"]),
        json={
            "name": "n",
            "type": "openai-compatible",
            "base_url": "https://api.openai.com/",
            "api_key": "old-key",
        },
    )
    conn_id = create.json()["id"]

    # Look up the original ref via direct DB query.
    sync_engine = create_engine(str(app.state.settings.db_url))
    sl = sessionmaker(sync_engine)
    with sl() as s:
        orig_ref = (
            s.execute(
                ProviderConnection.__table__.select().where(
                    ProviderConnection.id == UUID(conn_id),
                ),
            )
            .one()
            .encrypted_api_key_ref
        )

    r = c.patch(
        f"/api/v1/provider-connections/{conn_id}",
        headers=_auth(tokens["team_a"]),
        json={"api_key": "new-key"},
    )
    assert r.status_code == 200

    with sl() as s:
        new_ref = (
            s.execute(
                ProviderConnection.__table__.select().where(
                    ProviderConnection.id == UUID(conn_id),
                ),
            )
            .one()
            .encrypted_api_key_ref
        )
        secret_count = s.execute(
            Secret.__table__.select(),
        ).all()
    sync_engine.dispose()

    assert new_ref != orig_ref
    # Both remain during grace; only the superseded key has a retirement clock.
    assert len(secret_count) == 2
    retirement = {row.ref: row.provider_retired_at for row in secret_count}
    assert retirement[orig_ref] is not None
    assert retirement[new_ref] is None


def test_update_rate_card_provider(app_setup) -> None:
    app, tokens, _ = app_setup
    c = _client(app)
    create = c.post(
        "/api/v1/provider-connections",
        headers=_auth(tokens["team_a"]),
        json={
            "name": "n",
            "type": "openai-compatible",
            "base_url": "https://api.openai.com/",
            "api_key": "k",
        },
    )
    conn_id = create.json()["id"]
    assert create.json()["catalog_id"] is None

    r = c.patch(
        f"/api/v1/provider-connections/{conn_id}",
        headers=_auth(tokens["team_a"]),
        json={"pricing_mode": "catalog", "catalog_id": "supplier:az-gptplus5"},
    )
    assert r.status_code == 200, r.text
    assert r.json()["catalog_id"] == "supplier:az-gptplus5"


def test_update_cross_team_returns_404(app_setup) -> None:
    app, tokens, _ = app_setup
    c = _client(app)
    create = c.post(
        "/api/v1/provider-connections",
        headers=_auth(tokens["team_b"]),
        json={
            "name": "n",
            "type": "openai-compatible",
            "base_url": "https://api.openai.com/",
            "api_key": "k",
        },
    )
    conn_id = create.json()["id"]

    r = c.patch(
        f"/api/v1/provider-connections/{conn_id}",
        headers=_auth(tokens["team_a"]),
        json={"base_url": "https://api.anthropic.com/"},
    )
    assert r.status_code == 404


def test_update_to_invalid_pricing_returns_400(app_setup) -> None:
    app, tokens, _ = app_setup
    c = _client(app)
    create = c.post(
        "/api/v1/provider-connections",
        headers=_auth(tokens["team_a"]),
        json={
            "name": "n",
            "type": "openai-compatible",
            "base_url": "https://api.openai.com/",
            "api_key": "k",
        },
    )
    conn_id = create.json()["id"]

    r = c.patch(
        f"/api/v1/provider-connections/{conn_id}",
        headers=_auth(tokens["team_a"]),
        json={"pricing_mode": "custom"},  # no pricing_data
    )
    assert r.status_code == 400


def test_provider_mutations_write_secret_safe_audit_events(
    app_setup,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from loom_service.provider_connections_service import ProbeResult

    async def _fake_probe(*args, **kwargs) -> ProbeResult:  # type: ignore[no-untyped-def]
        return ProbeResult(status="valid", http_status=200, error=None)

    monkeypatch.setattr(
        "loom_service.routes.provider_connections.probe_connection",
        _fake_probe,
    )

    app, tokens, team_ids = app_setup
    c = _client(app)
    create = c.post(
        "/api/v1/provider-connections",
        headers=_auth(tokens["team_a"]),
        json={
            "name": "audited-provider",
            "type": "openai-compatible",
            "base_url": "https://api.openai.com/",
            "api_key": "sk-create-secret",
        },
    )
    conn_id = create.json()["id"]
    rotate = c.patch(
        f"/api/v1/provider-connections/{conn_id}",
        headers=_auth(tokens["team_a"]),
        json={"api_key": "sk-rotated-secret"},
    )
    probe = c.post(
        f"/api/v1/provider-connections/{conn_id}/test",
        headers=_auth(tokens["team_a"]),
    )
    deleted = c.delete(
        f"/api/v1/provider-connections/{conn_id}",
        headers=_auth(tokens["team_a"]),
    )
    audit = c.get(
        "/api/v1/admin/audit-events?limit=20",
        headers=_admin_headers(tokens["admin"]),
    )

    assert create.status_code == 201, create.text
    assert rotate.status_code == 200, rotate.text
    assert probe.status_code == 200, probe.text
    assert deleted.status_code == 204, deleted.text
    assert audit.status_code == 200, audit.text
    events = [
        event
        for event in audit.json()["items"]
        if event["target_type"] == "provider_connection" and event["target_id"] == conn_id
    ]
    assert [event["action"] for event in events] == [
        "provider_connection.delete",
        "provider_connection.test",
        "provider_connection.update",
        "provider_connection.create",
    ]
    for event in events:
        assert event["metadata"]["team_id"] == str(team_ids["a"])
        assert event["metadata"]["provider_type"] == "openai-compatible"

    serialized = json.dumps(events)
    assert "sk-create-secret" not in serialized
    assert "sk-rotated-secret" not in serialized


# ──────────────────────────────────────────────────────────────────────
# Delete
# ──────────────────────────────────────────────────────────────────────


def test_delete_soft_deletes_and_returns_204(app_setup) -> None:
    app, tokens, _ = app_setup
    c = _client(app)
    create = c.post(
        "/api/v1/provider-connections",
        headers=_auth(tokens["team_a"]),
        json={
            "name": "n",
            "type": "openai-compatible",
            "base_url": "https://api.openai.com/",
            "api_key": "k",
        },
    )
    conn_id = create.json()["id"]

    r = c.delete(
        f"/api/v1/provider-connections/{conn_id}",
        headers=_auth(tokens["team_a"]),
    )
    assert r.status_code == 204
    engine = create_engine(str(app.state.settings.db_url))
    try:
        with sessionmaker(engine)() as session:
            connection = session.get(ProviderConnection, UUID(conn_id))
            assert connection is not None
            secret = session.get(Secret, connection.encrypted_api_key_ref)
            assert secret is not None and secret.provider_retired_at is not None
    finally:
        engine.dispose()

    # GET / LIST should now treat it as nonexistent.
    g = c.get(
        f"/api/v1/provider-connections/{conn_id}",
        headers=_auth(tokens["team_a"]),
    )
    assert g.status_code == 404

    listed = c.get(
        "/api/v1/provider-connections",
        headers=_auth(tokens["team_a"]),
    )
    assert listed.json()["items"] == []


def test_delete_then_recreate_with_same_name(app_setup) -> None:
    """Soft-delete frees the display_name for reuse via the partial
    UNIQUE index. Catches a regression to a full UNIQUE."""
    app, tokens, _ = app_setup
    c = _client(app)
    payload = {
        "name": "reusable",
        "type": "openai-compatible",
        "base_url": "https://api.openai.com/",
        "api_key": "k",
    }
    create_1 = c.post("/api/v1/provider-connections", headers=_auth(tokens["team_a"]), json=payload)
    assert create_1.status_code == 201
    c.delete(
        f"/api/v1/provider-connections/{create_1.json()['id']}", headers=_auth(tokens["team_a"])
    )

    create_2 = c.post("/api/v1/provider-connections", headers=_auth(tokens["team_a"]), json=payload)
    assert create_2.status_code == 201, create_2.text
    assert create_2.json()["id"] != create_1.json()["id"]


def test_delete_cross_team_returns_404(app_setup) -> None:
    app, tokens, _ = app_setup
    c = _client(app)
    create = c.post(
        "/api/v1/provider-connections",
        headers=_auth(tokens["team_b"]),
        json={
            "name": "n",
            "type": "openai-compatible",
            "base_url": "https://api.openai.com/",
            "api_key": "k",
        },
    )
    conn_id = create.json()["id"]

    r = c.delete(
        f"/api/v1/provider-connections/{conn_id}",
        headers=_auth(tokens["team_a"]),
    )
    assert r.status_code == 404


# ──────────────────────────────────────────────────────────────────────
# Test (probe)
# ──────────────────────────────────────────────────────────────────────


def test_test_valid_persists_status_and_returns_summary(
    app_setup,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Happy path: probe succeeds → row.status='valid',
    last_validation_error cleared, last_validated_at set."""
    from loom_service.provider_connections_service import ProbeResult

    captured_args: dict[str, object] = {}

    async def _fake_probe(
        provider_type: str,
        base_url: str,
        api_key: str,
        *,
        _client_factory: object = None,
    ) -> ProbeResult:
        captured_args["provider_type"] = provider_type
        captured_args["base_url"] = base_url
        captured_args["api_key"] = api_key
        return ProbeResult(status="valid", http_status=200, error=None)

    monkeypatch.setattr(
        "loom_service.routes.provider_connections.probe_connection",
        _fake_probe,
    )

    app, tokens, _ = app_setup
    c = _client(app)
    create = c.post(
        "/api/v1/provider-connections",
        headers=_auth(tokens["team_a"]),
        json={
            "name": "openai-prod",
            "type": "openai-compatible",
            "base_url": "https://api.openai.com/v1",
            "api_key": "sk-XYZ",
        },
    )
    conn_id = create.json()["id"]
    assert create.json()["status"] == "pending"

    r = c.post(
        f"/api/v1/provider-connections/{conn_id}/test",
        headers=_auth(tokens["team_a"]),
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["connection_id"] == conn_id
    assert body["status"] == "valid"
    assert body["http_status"] == 200
    assert body["last_validation_error"] is None

    # The decrypted secret reached the probe — not the encrypted ref.
    assert captured_args["api_key"] == "sk-XYZ"
    assert captured_args["provider_type"] == "openai-compatible"
    assert captured_args["base_url"] == "https://api.openai.com/v1"

    # Row state now reflects the probe outcome — GET /show should
    # surface 'valid' + last_validated_at.
    show = c.get(
        f"/api/v1/provider-connections/{conn_id}",
        headers=_auth(tokens["team_a"]),
    ).json()
    assert show["status"] == "valid"
    assert show["last_validated_at"] is not None
    assert show["last_validation_error"] is None


def test_test_invalid_persists_error_message(
    app_setup,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from loom_service.provider_connections_service import ProbeResult

    async def _fake_probe(*args, **kwargs) -> ProbeResult:  # type: ignore[no-untyped-def]
        return ProbeResult(
            status="invalid",
            http_status=401,
            error="HTTP 401 from .../models; body excerpt: 'bad key'",
        )

    monkeypatch.setattr(
        "loom_service.routes.provider_connections.probe_connection",
        _fake_probe,
    )

    app, tokens, _ = app_setup
    c = _client(app)
    create = c.post(
        "/api/v1/provider-connections",
        headers=_auth(tokens["team_a"]),
        json={
            "name": "n",
            "type": "openai-compatible",
            "base_url": "https://api.openai.com/",
            "api_key": "wrong",
        },
    )
    conn_id = create.json()["id"]

    r = c.post(
        f"/api/v1/provider-connections/{conn_id}/test",
        headers=_auth(tokens["team_a"]),
    )
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "invalid"
    assert body["http_status"] == 401
    assert "bad key" in body["last_validation_error"]

    show = c.get(
        f"/api/v1/provider-connections/{conn_id}",
        headers=_auth(tokens["team_a"]),
    ).json()
    assert show["status"] == "invalid"
    assert "bad key" in show["last_validation_error"]


def test_test_revalidates_current_dns_before_probe(
    app_setup,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from loom_service.provider_connections_service import ProbeResult

    called = {"count": 0}

    async def _fake_probe(*args, **kwargs) -> ProbeResult:  # type: ignore[no-untyped-def]
        called["count"] += 1
        return ProbeResult(status="valid", http_status=200, error=None)

    monkeypatch.setattr(
        "loom_service.routes.provider_connections.probe_connection",
        _fake_probe,
    )

    app, tokens, _ = app_setup
    c = _client(app)
    conn_id = _create_conn(c, tokens["team_a"])
    _reroute_provider_dns(
        monkeypatch,
        host="api.openai.com",
        ips=["127.0.0.1"],
    )

    r = c.post(
        f"/api/v1/provider-connections/{conn_id}/test",
        headers=_auth(tokens["team_a"]),
    )

    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == "invalid"
    assert body["http_status"] is None
    assert "base_url rejected" in body["last_validation_error"]
    assert "127.0.0.1" in body["last_validation_error"]
    assert called["count"] == 0

    show = c.get(
        f"/api/v1/provider-connections/{conn_id}",
        headers=_auth(tokens["team_a"]),
    ).json()
    assert show["status"] == "invalid"
    assert "127.0.0.1" in show["last_validation_error"]


def test_test_secret_decrypt_failure_returns_actionable_503(
    app_setup,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Provider test should not leak SecretStore decrypt failures as 500s."""
    called = {"count": 0}

    async def _fake_probe(*args, **kwargs):  # type: ignore[no-untyped-def]
        called["count"] += 1
        raise AssertionError("upstream probe must not run without an api key")

    monkeypatch.setattr(
        "loom_service.routes.provider_connections.probe_connection",
        _fake_probe,
    )

    app, tokens, _ = app_setup
    c = TestClient(app, raise_server_exceptions=False)
    create = c.post(
        "/api/v1/provider-connections",
        headers=_auth(tokens["team_a"]),
        json={
            "name": "n",
            "type": "openai-compatible",
            "base_url": "https://api.openai.com/",
            "api_key": "sk-XYZ",
        },
    )
    conn_id = create.json()["id"]

    monkeypatch.setenv("LOOM_SECRET_STORE_MASTER_KEY", _WRONG_TEST_MASTER_KEY)
    r = c.post(
        f"/api/v1/provider-connections/{conn_id}/test",
        headers=_auth(tokens["team_a"]),
    )

    assert r.status_code == 503
    detail = r.json()["detail"]
    assert "stored provider secret" in detail
    assert "restore" in detail.lower()
    assert "rotate" in detail.lower()
    assert "AEAD" not in detail
    assert "loom://" not in detail
    assert "sk-XYZ" not in detail
    assert called["count"] == 0


def test_test_cross_team_returns_404(
    app_setup,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cross-team /test mirrors the GET / PATCH / DELETE behavior: 404,
    not 403 — and crucially BEFORE we decrypt the api_key. The fake
    probe asserts it was never called."""
    from loom_service.provider_connections_service import ProbeResult

    called = {"count": 0}

    async def _fake_probe(*args, **kwargs) -> ProbeResult:  # type: ignore[no-untyped-def]
        called["count"] += 1
        return ProbeResult(status="valid", http_status=200, error=None)

    monkeypatch.setattr(
        "loom_service.routes.provider_connections.probe_connection",
        _fake_probe,
    )

    app, tokens, _ = app_setup
    c = _client(app)
    create = c.post(
        "/api/v1/provider-connections",
        headers=_auth(tokens["team_b"]),
        json={
            "name": "n",
            "type": "openai-compatible",
            "base_url": "https://api.openai.com/",
            "api_key": "k",
        },
    )
    conn_id = create.json()["id"]

    r = c.post(
        f"/api/v1/provider-connections/{conn_id}/test",
        headers=_auth(tokens["team_a"]),
    )
    assert r.status_code == 404
    assert called["count"] == 0


def test_test_returns_404_for_soft_deleted_connection(
    app_setup,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from loom_service.provider_connections_service import ProbeResult

    async def _fake_probe(*args, **kwargs) -> ProbeResult:  # type: ignore[no-untyped-def]
        return ProbeResult(status="valid", http_status=200, error=None)

    monkeypatch.setattr(
        "loom_service.routes.provider_connections.probe_connection",
        _fake_probe,
    )

    app, tokens, _ = app_setup
    c = _client(app)
    create = c.post(
        "/api/v1/provider-connections",
        headers=_auth(tokens["team_a"]),
        json={
            "name": "n",
            "type": "openai-compatible",
            "base_url": "https://api.openai.com/",
            "api_key": "k",
        },
    )
    conn_id = create.json()["id"]
    c.delete(
        f"/api/v1/provider-connections/{conn_id}",
        headers=_auth(tokens["team_a"]),
    )

    r = c.post(
        f"/api/v1/provider-connections/{conn_id}/test",
        headers=_auth(tokens["team_a"]),
    )
    assert r.status_code == 404


# ──────────────────────────────────────────────────────────────────────
# Models cache: list / refresh / hide / unhide
# ──────────────────────────────────────────────────────────────────────


def _stub_fetch_upstream_models(
    monkeypatch: pytest.MonkeyPatch,
    returns: list[str] | None = None,
    *,
    raises: Exception | None = None,
) -> dict[str, object]:
    """Patch fetch_upstream_models on the routes module + record the
    call. Returns a dict the test can inspect for call_count / last_args."""
    state: dict[str, object] = {"call_count": 0, "last_args": None}

    async def _fake(
        provider_type: str,
        base_url: str,
        api_key: str,
        *,
        _client_factory: object = None,
    ) -> list[str]:
        state["call_count"] = int(state["call_count"]) + 1  # type: ignore[arg-type]
        state["last_args"] = (provider_type, base_url, api_key)
        if raises is not None:
            raise raises
        assert returns is not None
        return list(returns)

    monkeypatch.setattr(
        "loom_service.routes.provider_connections.fetch_upstream_models",
        _fake,
    )
    return state


def _reroute_provider_dns(
    monkeypatch: pytest.MonkeyPatch,
    *,
    host: str,
    ips: list[str],
) -> None:
    """Replace provider DNS results after create-time validation.

    This models DNS rebinding / stale DNS: the connection was accepted
    when `host` resolved to a public IP, but the next outbound operation
    sees a policy-forbidden address.
    """

    def _stub(host_arg: str, port, *args, **kwargs):
        if host_arg == host:
            return [(socket.AF_INET, socket.SOCK_STREAM, 0, "", (ip, 0)) for ip in ips]
        raise socket.gaierror(f"no test DNS entry for {host_arg}")

    monkeypatch.setattr(
        "loom_service.provider_connections_service.socket.getaddrinfo",
        _stub,
    )


def _stub_preflight_model(
    monkeypatch: pytest.MonkeyPatch,
    *,
    status: str = "valid",
    http_status: int | None = 200,
    error_code: str | None = None,
    error_message: str | None = None,
) -> dict[str, object]:
    """Patch preflight_model on the routes module + record call args."""
    state: dict[str, object] = {"call_count": 0, "last_args": None}

    async def _fake(
        provider_type: str,
        base_url: str,
        api_key: str,
        model_id: str,
        *,
        _client_factory: object = None,
    ) -> SimpleNamespace:
        state["call_count"] = int(state["call_count"]) + 1  # type: ignore[arg-type]
        state["last_args"] = (provider_type, base_url, api_key, model_id)
        return SimpleNamespace(
            status=status,
            http_status=http_status,
            error_code=error_code,
            error_message=error_message,
        )

    monkeypatch.setattr(
        "loom_service.routes.provider_connections.preflight_model",
        _fake,
        raising=False,
    )
    return state


def _create_conn(c, token: str, name: str = "openai-prod") -> str:  # type: ignore[no-untyped-def]
    r = c.post(
        "/api/v1/provider-connections",
        headers=_auth(token),
        json={
            "name": name,
            "type": "openai-compatible",
            "base_url": "https://api.openai.com/v1",
            "api_key": "sk-XYZ",
        },
    )
    assert r.status_code == 201, r.text
    return r.json()["id"]  # type: ignore[no-any-return]


def test_models_list_initially_empty(app_setup) -> None:
    app, tokens, _ = app_setup
    c = _client(app)
    conn_id = _create_conn(c, tokens["team_a"])
    r = c.get(
        f"/api/v1/provider-connections/{conn_id}/models",
        headers=_auth(tokens["team_a"]),
    )
    assert r.status_code == 200
    assert r.json() == {"items": []}


def test_models_manual_entry_is_tied_to_connection_with_metadata(app_setup) -> None:
    app, tokens, _ = app_setup
    c = _client(app)
    conn_id = _create_conn(c, tokens["team_a"])

    r = c.post(
        f"/api/v1/provider-connections/{conn_id}/models",
        headers=_auth(tokens["team_a"]),
        json={"model_id": "my-lab-checkpoint-20260616"},
    )
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["model_id"] == "my-lab-checkpoint-20260616"
    assert body["source"] == "manual"
    assert body["visible"] is True
    assert body["upstream_present"] is False
    assert body["agent_capable"] is True
    assert body["recommended"] is True
    assert body["visibility"] == "default"
    assert body["hidden_reason"] is None

    listed = c.get(
        f"/api/v1/provider-connections/{conn_id}/models",
        headers=_auth(tokens["team_a"]),
    )
    assert listed.status_code == 200
    assert listed.json()["items"] == [body]


def test_models_manual_entry_survives_refresh_when_absent_upstream(
    app_setup,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, tokens, _ = app_setup
    c = _client(app)
    conn_id = _create_conn(c, tokens["team_a"])
    created = c.post(
        f"/api/v1/provider-connections/{conn_id}/models",
        headers=_auth(tokens["team_a"]),
        json={"model_id": "my-offline-vllm-model"},
    )
    assert created.status_code == 201, created.text

    _stub_fetch_upstream_models(monkeypatch, returns=["gpt-4o"])
    refreshed = c.post(
        f"/api/v1/provider-connections/{conn_id}/models/refresh",
        headers=_auth(tokens["team_a"]),
    )
    assert refreshed.status_code == 200, refreshed.text
    rows = {it["model_id"]: it for it in refreshed.json()["items"]}
    manual = rows["my-offline-vllm-model"]
    assert manual["source"] == "manual"
    assert manual["visible"] is True
    assert manual["upstream_present"] is False
    assert manual["hidden_reason"] is None
    assert manual["recommended"] is True


def test_models_manual_entry_can_be_hidden_and_unhidden_without_upstream_presence(
    app_setup,
) -> None:
    app, tokens, _ = app_setup
    c = _client(app)
    conn_id = _create_conn(c, tokens["team_a"])
    created = c.post(
        f"/api/v1/provider-connections/{conn_id}/models",
        headers=_auth(tokens["team_a"]),
        json={"model_id": "my-offline-vllm-model"},
    )
    assert created.status_code == 201, created.text
    assert created.json()["upstream_present"] is False

    hidden = c.post(
        f"/api/v1/provider-connections/{conn_id}/models/my-offline-vllm-model/hide",
        headers=_auth(tokens["team_a"]),
    )
    assert hidden.status_code == 200, hidden.text
    assert hidden.json()["visible"] is False
    assert hidden.json()["hidden_reason"] == "operator-hidden"

    unhidden = c.post(
        f"/api/v1/provider-connections/{conn_id}/models/my-offline-vllm-model/unhide",
        headers=_auth(tokens["team_a"]),
    )
    assert unhidden.status_code == 200, unhidden.text
    body = unhidden.json()
    assert body["source"] == "manual"
    assert body["visible"] is True
    assert body["hidden_reason"] is None
    assert body["upstream_present"] is False


def test_models_refresh_populates_cache_then_list_returns_it(
    app_setup,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = _stub_fetch_upstream_models(
        monkeypatch,
        returns=["gpt-4o", "gpt-3.5-turbo"],
    )
    app, tokens, _ = app_setup
    c = _client(app)
    conn_id = _create_conn(c, tokens["team_a"])

    r = c.post(
        f"/api/v1/provider-connections/{conn_id}/models/refresh",
        headers=_auth(tokens["team_a"]),
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["added"] == 2
    assert body["refreshed"] == 0
    assert body["missing"] == 0
    ids = [it["model_id"] for it in body["items"]]
    assert sorted(ids) == ["gpt-3.5-turbo", "gpt-4o"]
    # All entries start visible + upstream-present.
    for it in body["items"]:
        assert it["visible"] is True
        assert it["upstream_present"] is True
        assert it["hidden_reason"] is None

    # Decrypted api_key reached the fetcher (not the opaque ref).
    last_args = state["last_args"]
    assert isinstance(last_args, tuple)
    assert last_args[2] == "sk-XYZ"

    # GET /models surfaces the same set.
    listed = c.get(
        f"/api/v1/provider-connections/{conn_id}/models",
        headers=_auth(tokens["team_a"]),
    ).json()
    assert sorted(it["model_id"] for it in listed["items"]) == [
        "gpt-3.5-turbo",
        "gpt-4o",
    ]


def test_models_refresh_revalidates_current_dns_before_fetch(
    app_setup,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, tokens, _ = app_setup
    c = _client(app)
    conn_id = _create_conn(c, tokens["team_a"])
    state = _stub_fetch_upstream_models(monkeypatch, returns=["gpt-4o"])
    _reroute_provider_dns(
        monkeypatch,
        host="api.openai.com",
        ips=["127.0.0.1"],
    )

    r = c.post(
        f"/api/v1/provider-connections/{conn_id}/models/refresh",
        headers=_auth(tokens["team_a"]),
    )

    assert r.status_code == 400, r.text
    detail = r.json()["detail"]
    assert "base_url rejected" in detail
    assert "127.0.0.1" in detail
    assert int(state["call_count"]) == 0


def test_models_preflight_valid_persists_model_status(
    app_setup,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, tokens, _ = app_setup
    c = _client(app)
    conn_id = _create_conn(c, tokens["team_a"])
    c.post(
        f"/api/v1/provider-connections/{conn_id}/models",
        headers=_auth(tokens["team_a"]),
        json={"model_id": "gpt-4o-mini"},
    )
    state = _stub_preflight_model(monkeypatch)

    r = c.post(
        f"/api/v1/provider-connections/{conn_id}/models/gpt-4o-mini/preflight",
        headers=_auth(tokens["team_a"]),
    )

    assert r.status_code == 200, r.text
    body = r.json()
    assert body["model_id"] == "gpt-4o-mini"
    assert body["last_preflight_status"] == "valid"
    assert body["last_preflight_at"] is not None
    assert body["last_preflight_http_status"] == 200
    assert body["last_preflight_error_code"] is None
    assert body["last_preflight_error_message"] is None
    assert state["last_args"] == (
        "openai-compatible",
        "https://api.openai.com/v1",
        "sk-XYZ",
        "gpt-4o-mini",
    )

    listed = c.get(
        f"/api/v1/provider-connections/{conn_id}/models",
        headers=_auth(tokens["team_a"]),
    )
    listed_row = listed.json()["items"][0]
    assert listed_row["last_preflight_status"] == "valid"
    assert listed_row["last_preflight_at"] == body["last_preflight_at"]


def test_models_preflight_access_denied_persists_safe_error(
    app_setup,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, tokens, _ = app_setup
    c = _client(app)
    conn_id = _create_conn(c, tokens["team_a"])
    c.post(
        f"/api/v1/provider-connections/{conn_id}/models",
        headers=_auth(tokens["team_a"]),
        json={"model_id": "gpt-private"},
    )
    _stub_preflight_model(
        monkeypatch,
        status="failed",
        http_status=403,
        error_code="access-denied",
        error_message="HTTP 403 from upstream: [REDACTED]",
    )

    r = c.post(
        f"/api/v1/provider-connections/{conn_id}/models/gpt-private/preflight",
        headers=_auth(tokens["team_a"]),
    )

    assert r.status_code == 200, r.text
    body = r.json()
    assert body["last_preflight_status"] == "failed"
    assert body["last_preflight_http_status"] == 403
    assert body["last_preflight_error_code"] == "access-denied"
    assert body["last_preflight_error_message"] == ("HTTP 403 from upstream: [REDACTED]")
    assert "sk-XYZ" not in json.dumps(body)


def test_models_preflight_revalidates_current_dns_before_upstream_call(
    app_setup,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, tokens, _ = app_setup
    c = _client(app)
    conn_id = _create_conn(c, tokens["team_a"])
    c.post(
        f"/api/v1/provider-connections/{conn_id}/models",
        headers=_auth(tokens["team_a"]),
        json={"model_id": "gpt-private"},
    )
    state = _stub_preflight_model(monkeypatch)
    _reroute_provider_dns(
        monkeypatch,
        host="api.openai.com",
        ips=["127.0.0.1"],
    )

    r = c.post(
        f"/api/v1/provider-connections/{conn_id}/models/gpt-private/preflight",
        headers=_auth(tokens["team_a"]),
    )

    assert r.status_code == 200, r.text
    body = r.json()
    assert body["last_preflight_status"] == "failed"
    assert body["last_preflight_http_status"] is None
    assert body["last_preflight_error_code"] == "egress-policy-rejected"
    assert "base_url rejected" in body["last_preflight_error_message"]
    assert "127.0.0.1" in body["last_preflight_error_message"]
    assert "sk-XYZ" not in json.dumps(body)
    assert int(state["call_count"]) == 0


def test_models_preflight_404_for_uncached_model(app_setup) -> None:
    app, tokens, _ = app_setup
    c = _client(app)
    conn_id = _create_conn(c, tokens["team_a"])

    r = c.post(
        f"/api/v1/provider-connections/{conn_id}/models/gpt-never-cached/preflight",
        headers=_auth(tokens["team_a"]),
    )

    assert r.status_code == 404
    assert "run POST /models/refresh first" in r.json()["detail"]


def test_models_refresh_marks_missing_models_unhidden_to_missing(
    app_setup,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A previously-cached model not in the new upstream response →
    visible=false, hidden_reason='missing-upstream', upstream_present=false."""
    app, tokens, _ = app_setup
    c = _client(app)
    conn_id = _create_conn(c, tokens["team_a"])

    _stub_fetch_upstream_models(
        monkeypatch,
        returns=["gpt-4o", "gpt-3.5-turbo"],
    )
    c.post(
        f"/api/v1/provider-connections/{conn_id}/models/refresh",
        headers=_auth(tokens["team_a"]),
    )

    # Upstream drops gpt-3.5-turbo.
    _stub_fetch_upstream_models(monkeypatch, returns=["gpt-4o"])
    r = c.post(
        f"/api/v1/provider-connections/{conn_id}/models/refresh",
        headers=_auth(tokens["team_a"]),
    )
    body = r.json()
    assert body["added"] == 0
    assert body["refreshed"] == 1
    assert body["missing"] == 1
    rows = {it["model_id"]: it for it in body["items"]}
    assert rows["gpt-4o"]["visible"] is True
    assert rows["gpt-4o"]["upstream_present"] is True
    assert rows["gpt-3.5-turbo"]["visible"] is False
    assert rows["gpt-3.5-turbo"]["upstream_present"] is False
    assert rows["gpt-3.5-turbo"]["hidden_reason"] == "missing-upstream"


def test_models_refresh_preserves_operator_hide_across_refresh(
    app_setup,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A row hidden by the operator stays hidden after refresh even
    when the upstream still returns it. hidden_reason stays
    'operator-hidden', not flipped to NULL or 'missing-upstream'."""
    app, tokens, _ = app_setup
    c = _client(app)
    conn_id = _create_conn(c, tokens["team_a"])

    _stub_fetch_upstream_models(
        monkeypatch,
        returns=["gpt-4o", "gpt-3.5-turbo"],
    )
    c.post(
        f"/api/v1/provider-connections/{conn_id}/models/refresh",
        headers=_auth(tokens["team_a"]),
    )
    c.post(
        f"/api/v1/provider-connections/{conn_id}/models/gpt-3.5-turbo/hide",
        headers=_auth(tokens["team_a"]),
    )

    # Refresh again with same upstream — operator-hidden persists.
    r = c.post(
        f"/api/v1/provider-connections/{conn_id}/models/refresh",
        headers=_auth(tokens["team_a"]),
    )
    rows = {it["model_id"]: it for it in r.json()["items"]}
    assert rows["gpt-3.5-turbo"]["visible"] is False
    assert rows["gpt-3.5-turbo"]["hidden_reason"] == "operator-hidden"
    # upstream_present should still be True — we saw it.
    assert rows["gpt-3.5-turbo"]["upstream_present"] is True


def test_models_refresh_upstream_502_does_not_partially_commit(
    app_setup,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from loom_service.provider_connections_service import (
        UpstreamModelFetchError,
    )

    app, tokens, _ = app_setup
    c = _client(app)
    conn_id = _create_conn(c, tokens["team_a"])

    # Populate with one model first so we can verify cache isn't wiped.
    _stub_fetch_upstream_models(monkeypatch, returns=["gpt-4o"])
    c.post(
        f"/api/v1/provider-connections/{conn_id}/models/refresh",
        headers=_auth(tokens["team_a"]),
    )

    # Now an upstream error.
    _stub_fetch_upstream_models(
        monkeypatch,
        raises=UpstreamModelFetchError("auth failure", http_status=401),
    )
    r = c.post(
        f"/api/v1/provider-connections/{conn_id}/models/refresh",
        headers=_auth(tokens["team_a"]),
    )
    assert r.status_code == 502
    detail = r.json()["detail"]
    assert detail["code"] == "upstream_models_fetch_failed"
    assert detail["message"] == "auth failure"
    assert detail["upstream_http_status"] == 401

    # Cache state still has the prior gpt-4o row, untouched.
    listed = c.get(
        f"/api/v1/provider-connections/{conn_id}/models",
        headers=_auth(tokens["team_a"]),
    ).json()
    assert [it["model_id"] for it in listed["items"]] == ["gpt-4o"]


def test_models_refresh_secret_decrypt_failure_returns_actionable_503(
    app_setup,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Model refresh should fail before contacting upstream if key unwrap fails."""
    state = _stub_fetch_upstream_models(monkeypatch, returns=["gpt-4o"])
    app, tokens, _ = app_setup
    c = TestClient(app, raise_server_exceptions=False)
    conn_id = _create_conn(c, tokens["team_a"])

    monkeypatch.setenv("LOOM_SECRET_STORE_MASTER_KEY", _WRONG_TEST_MASTER_KEY)
    r = c.post(
        f"/api/v1/provider-connections/{conn_id}/models/refresh",
        headers=_auth(tokens["team_a"]),
    )

    assert r.status_code == 503
    detail = r.json()["detail"]
    assert "stored provider secret" in detail
    assert "restore" in detail.lower()
    assert "rotate" in detail.lower()
    assert "AEAD" not in detail
    assert "loom://" not in detail
    assert int(state["call_count"]) == 0


def test_models_hide_404_for_uncached_model(app_setup) -> None:
    app, tokens, _ = app_setup
    c = _client(app)
    conn_id = _create_conn(c, tokens["team_a"])
    r = c.post(
        f"/api/v1/provider-connections/{conn_id}/models/gpt-never-cached/hide",
        headers=_auth(tokens["team_a"]),
    )
    assert r.status_code == 404
    assert "run POST /models/refresh first" in r.json()["detail"]


def test_models_unhide_keeps_missing_upstream_invisible(
    app_setup,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Unhiding a model that's no longer upstream MUST NOT make it
    visible — we can't run a trial against a model the provider
    doesn't serve. hidden_reason flips back to 'missing-upstream'."""
    app, tokens, _ = app_setup
    c = _client(app)
    conn_id = _create_conn(c, tokens["team_a"])

    _stub_fetch_upstream_models(monkeypatch, returns=["gpt-4o"])
    c.post(
        f"/api/v1/provider-connections/{conn_id}/models/refresh",
        headers=_auth(tokens["team_a"]),
    )
    # Drop gpt-4o upstream — it becomes missing-upstream.
    _stub_fetch_upstream_models(monkeypatch, returns=["gpt-3.5"])
    c.post(
        f"/api/v1/provider-connections/{conn_id}/models/refresh",
        headers=_auth(tokens["team_a"]),
    )

    # Operator tries to unhide it.
    r = c.post(
        f"/api/v1/provider-connections/{conn_id}/models/gpt-4o/unhide",
        headers=_auth(tokens["team_a"]),
    )
    assert r.status_code == 200
    body = r.json()
    assert body["visible"] is False
    assert body["hidden_reason"] == "missing-upstream"
    assert body["upstream_present"] is False


def test_models_unhide_makes_operator_hidden_visible(
    app_setup,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, tokens, _ = app_setup
    c = _client(app)
    conn_id = _create_conn(c, tokens["team_a"])

    _stub_fetch_upstream_models(monkeypatch, returns=["gpt-4o"])
    c.post(
        f"/api/v1/provider-connections/{conn_id}/models/refresh",
        headers=_auth(tokens["team_a"]),
    )
    c.post(
        f"/api/v1/provider-connections/{conn_id}/models/gpt-4o/hide",
        headers=_auth(tokens["team_a"]),
    )
    r = c.post(
        f"/api/v1/provider-connections/{conn_id}/models/gpt-4o/unhide",
        headers=_auth(tokens["team_a"]),
    )
    body = r.json()
    assert body["visible"] is True
    assert body["hidden_reason"] is None


def test_test_flips_status_invalid_when_stored_ref_is_malformed(
    app_setup,
    postgres_url: str,
) -> None:
    """#423: legacy staging provider rows can hold an argv-style
    ref like `env:STAGING_SMOKE_OPENAI` in `encrypted_api_key_ref`
    instead of the runtime-supported `loom://<ns>/<uuid>` shape. The
    /test endpoint must surface this as an actionable failure AND flip
    `status='invalid'` so list/show surfaces stop hiding the broken
    row behind status='valid'."""
    app, tokens, _ = app_setup
    c = _client(app)
    create = c.post(
        "/api/v1/provider-connections",
        headers=_auth(tokens["team_a"]),
        json={
            "name": "legacy-ref",
            "type": "openai-compatible",
            "base_url": "https://api.openai.com/v1",
            "api_key": "sk-XYZ",
        },
    )
    assert create.status_code == 201, create.text
    conn_id = create.json()["id"]

    # Corrupt the stored ref to the pre-secret-store legacy shape. The
    # POST path always writes a proper loom://... ref via SecretStore.put
    # so direct DB surgery is the only way to reproduce the pre-#423
    # legacy state.
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    try:
        with sl() as s:
            row = s.execute(
                ProviderConnection.__table__.select().where(
                    ProviderConnection.id == UUID(conn_id),
                ),
            ).first()
            assert row is not None
            s.execute(
                ProviderConnection.__table__.update()
                .where(ProviderConnection.id == UUID(conn_id))
                .values(encrypted_api_key_ref="env:STAGING_SMOKE_OPENAI"),
            )
            s.commit()
    finally:
        sync_engine.dispose()

    r = c.post(
        f"/api/v1/provider-connections/{conn_id}/test",
        headers=_auth(tokens["team_a"]),
    )
    # Controlled service-side error — NOT an unhandled 500. The exact
    # status code is the existing _PROVIDER_SECRET_UNREADABLE_DETAIL
    # contract (503); the new behavior is the row.status flip below.
    assert r.status_code == 503, r.text

    show = c.get(
        f"/api/v1/provider-connections/{conn_id}",
        headers=_auth(tokens["team_a"]),
    )
    assert show.status_code == 200, show.text
    body = show.json()
    assert body["status"] == "invalid", body
    assert body["last_validation_error"] is not None
    assert "malformed_ref" in body["last_validation_error"]
    assert "rotate-key" in body["last_validation_error"]


def test_models_routes_cross_team_return_404(app_setup) -> None:
    """team_a owns the connection; team_b can't list / refresh / hide /
    unhide it. 404 (not 403) so existence isn't leaked."""
    app, tokens, _ = app_setup
    c = _client(app)
    conn_id = _create_conn(c, tokens["team_a"])

    for path in [
        f"/api/v1/provider-connections/{conn_id}/models",
    ]:
        r = c.get(path, headers=_auth(tokens["team_b"]))
        assert r.status_code == 404, path

    for path in [
        f"/api/v1/provider-connections/{conn_id}/models/refresh",
        f"/api/v1/provider-connections/{conn_id}/models/gpt-4o/hide",
        f"/api/v1/provider-connections/{conn_id}/models/gpt-4o/unhide",
        f"/api/v1/provider-connections/{conn_id}/models/gpt-4o/preflight",
    ]:
        r = c.post(path, headers=_auth(tokens["team_b"]))
        assert r.status_code == 404, path


@pytest.mark.asyncio
async def test_concurrent_rotation_retires_each_superseded_ref(app_setup) -> None:
    import asyncio

    from sqlalchemy import select

    app, tokens, _ = app_setup
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        created = await client.post(
            "/api/v1/provider-connections", headers=_auth(tokens["team_a"]),
            json={"name": "concurrent", "type": "openai-compatible",
                  "base_url": "https://api.openai.com/", "api_key": "original"},
        )
        assert created.status_code == 201
        connection_id = created.json()["id"]
        responses = await asyncio.gather(*(
            client.patch(f"/api/v1/provider-connections/{connection_id}",
                         headers=_auth(tokens["team_a"]), json={"api_key": value})
            for value in ("rotation-one", "rotation-two")
        ))
        assert [response.status_code for response in responses] == [200, 200]
    async with app.state.session_factory() as session:
        connection = await session.get(ProviderConnection, UUID(connection_id))
        secrets = (await session.scalars(select(Secret))).all()
        assert len(secrets) == 3
        assert sum(secret.provider_retired_at is not None for secret in secrets) == 2
        assert (await session.get(Secret, connection.encrypted_api_key_ref)).provider_retired_at is None


def test_pricing_roundtrip_mode_transitions_and_explicit_null(app_setup) -> None:
    app, tokens, _ = app_setup
    c = _client(app)
    headers = _auth(tokens["team_a"])
    prices = {"gpt-4o-mini": {"input_usd_per_1m": 0, "output_usd_per_1m": 2}}
    created = c.post("/api/v1/provider-connections", headers=headers, json={
        "name": "pricing-roundtrip", "type": "openai-compatible",
        "base_url": "https://api.openai.com/v1", "api_key": "test-only",
        "pricing_mode": "custom", "custom_pricing": prices,
    })
    assert created.status_code == 201, created.text
    path = f"/api/v1/provider-connections/{created.json()['id']}"
    assert created.json()["custom_pricing"]["gpt-4o-mini"]["input_usd_per_1m"] == 0
    assert "pricing_source" not in created.json() and "pricing_data" not in created.json()
    # Unrelated PATCH omission must retain prices.
    patched = c.patch(path, headers=headers, json={"allowed_models": ["gpt-4o-mini", "new-model"]})
    assert set(patched.json()["custom_pricing"]) == {"gpt-4o-mini"}
    # Explicit null is a clear, not an omission; invalid in active custom mode.
    assert c.patch(path, headers=headers, json={"custom_pricing": None}).status_code == 400
    for mode, extra in [("catalog", {"catalog_id": "supplier:az-gptplus5"}),
                        ("usage_only", {}), ("custom", {"custom_pricing": prices})]:
        result = c.patch(path, headers=headers, json={"pricing_mode": mode, **extra})
        assert result.status_code == 200, result.text
        readback = c.get(path, headers=headers).json()
        assert readback["pricing_mode"] == mode
        if mode != "custom":
            assert readback["custom_pricing"] is None
        if mode != "catalog":
            assert readback["catalog_id"] is None


def test_supplier_defaults_and_explicit_usage_choice(app_setup) -> None:
    app, tokens, _ = app_setup
    c = _client(app)
    for index, override in enumerate([{}, {"pricing_mode": "usage_only"}]):
        result = c.post("/api/v1/provider-connections", headers=_auth(tokens["team_a"]), json={
            "name": f"supplier-{index}", "type": "openai-compatible",
            "base_url": "https://api.openai.com/v1", "api_key": "test-only",
            "supplier_id": "az-gptplus5", **override,
        })
        assert result.status_code == 201, result.text
        assert result.json()["pricing_mode"] == ("catalog" if index == 0 else "usage_only")
        assert result.json()["catalog_id"] == ("supplier:az-gptplus5" if index == 0 else None)


def test_team_catalog_import_is_atomic_and_private(app_setup, postgres_url) -> None:
    from sqlalchemy import update
    app, tokens, _ = app_setup
    c = _client(app)
    headers = _auth(tokens["team_a"])
    payload = {"name": "Negotiated", "prices": {"a": {"input_usd_per_1m": 1, "output_usd_per_1m": 2}}}
    assert c.post("/api/v1/price-catalogs", headers=headers, json=payload).status_code == 403
    engine = create_engine(postgres_url)
    with engine.begin() as conn:
        conn.execute(update(Token).where(Token.token_hash == hashlib.sha256(tokens["team_a"].encode()).digest()).values(scopes=["read:own", "providers:manage", "team:manage"]))
    engine.dispose()
    created = c.post("/api/v1/price-catalogs", headers=headers, json=payload)
    assert created.status_code == 201, created.text
    catalog = created.json()
    path = f"/api/v1/price-catalogs/{catalog['id']}"
    assert c.get(path, headers=_auth(tokens["team_b"])).status_code == 404
    assert catalog['id'] not in [x['id'] for x in c.get('/api/v1/price-catalogs', headers=_auth(tokens['team_b'])).json()['items']]
    body = {"content": "model,input_usd_per_1m,output_usd_per_1m\na,3,4\na,5,6", "format": "csv", "expected_revision": 1, "apply": True}
    assert c.post(path + "/import", headers=headers, json=body).status_code == 400
    assert c.get(path, headers=headers).json()["prices"] == catalog["prices"]
    body["content"] = "model,input_usd_per_1m,output_usd_per_1m\nb,0,4"
    body["apply"] = False
    preview = c.post(path + "/import", headers=headers, json=body)
    assert preview.status_code == 200, preview.text
    assert preview.json()["summary"] == {"added": ["b"], "removed": ["a"], "changed": []}
    assert c.get(path, headers=headers).json()["revision"] == 1
    body["apply"] = True
    assert c.post(path + "/import", headers=headers, json=body).json()["catalog"]["revision"] == 2
    assert c.post(path + "/import", headers=headers, json=body).status_code == 409
    bad_connection = c.post('/api/v1/provider-connections', headers=_auth(tokens['team_b']), json={
        'name': 'private-price-theft', 'type': 'custom', 'base_url': 'https://api.openai.com/v1', 'api_key': 'test-only',
        'pricing_mode': 'catalog', 'catalog_id': catalog['id'],
    })
    assert bad_connection.status_code == 404


@pytest.mark.asyncio
async def test_supplier_failure_retains_last_prices(app_setup, monkeypatch) -> None:
    from loom.db.schema import PriceCatalog
    from loom.provider_pricing import ModelPrice
    from loom_service.price_catalogs import SupplierPriceSnapshot, sync_catalog
    app, _, _ = app_setup
    async def success(_supplier):
        return SupplierPriceSnapshot(prices={"a": ModelPrice(input_usd_per_1m=1, output_usd_per_1m=2)})
    async def failure(_supplier):
        raise ValueError("invalid upstream prices")
    async with app.state.session_factory() as session:
        row = await session.get(PriceCatalog, "supplier:az-gptplus5")
        monkeypatch.setattr("loom_service.price_catalogs.fetch_supplier_prices", success)
        await sync_catalog(session, row)
        prices, revision, updated = row.prices, row.revision, row.updated_at
        monkeypatch.setattr("loom_service.price_catalogs.fetch_supplier_prices", failure)
        await sync_catalog(session, row)
        assert row.prices == prices and row.revision == revision and row.updated_at == updated
        assert row.sync_error and row.checked_at >= updated
        await session.rollback()


@pytest.mark.parametrize("share_first", [False, True])
def test_private_catalog_cannot_leak_through_shared_connection(app_setup, postgres_url, share_first):
    from sqlalchemy import update
    app, tokens, team_ids = app_setup
    c = _client(app)
    headers = _auth(tokens["team_a"])
    engine = create_engine(postgres_url)
    with engine.begin() as conn:
        conn.execute(update(Token).where(Token.token_hash == hashlib.sha256(tokens["team_a"].encode()).digest()).values(scopes=["read:own", "providers:manage", "team:manage"]))
    engine.dispose()
    catalog = c.post("/api/v1/price-catalogs", headers=headers, json={
        "name": "Private", "prices": {"model": {"input_usd_per_1m": 1, "output_usd_per_1m": 2}},
    })
    assert catalog.status_code == 201, catalog.text
    created = c.post("/api/v1/provider-connections", headers=headers, json={
        "name": "privacy-check", "type": "openai-compatible",
        "base_url": "https://api.openai.com/v1", "api_key": "test-only",
    })
    assert created.status_code == 201, created.text
    path = f"/api/v1/provider-connections/{created.json()['id']}"
    def share():
        return c.post(path + "/shares", headers=headers, json={"target_team_id": str(team_ids["b"])})
    def configure():
        return c.patch(path, headers=headers, json={"pricing_mode": "catalog", "catalog_id": catalog.json()["id"]})
    if share_first:
        assert share().status_code == 201
        assert configure().status_code == 400
        assert c.get(path, headers=headers).json()["pricing_mode"] == "usage_only"
    else:
        assert configure().status_code == 200
        assert share().status_code == 400
        assert c.get(path, headers=_auth(tokens["team_b"])).status_code == 404


def test_model_metadata_preserves_discovery_visibility_and_entitlement(app_setup, monkeypatch):
    app, tokens, _ = app_setup
    c = _client(app)
    conn_id = _create_conn(c, tokens["team_a"])
    _stub_fetch_upstream_models(monkeypatch, returns=["gpt-5.4"])
    original = c.post(
        f"/api/v1/provider-connections/{conn_id}/models/refresh",
        headers=_auth(tokens["team_a"]),
    ).json()["items"][0]
    path = f"/api/v1/provider-connections/{conn_id}/models/gpt-5.4/metadata"
    payload = {"context_length": 1050000,
               "specification_url": "https://developers.openai.com/api/docs/models/gpt-5.4"}
    denied = c.patch(path, headers=_auth(tokens["team_b"]), json=payload)
    assert denied.status_code == 404
    updated = c.patch(path, headers=_auth(tokens["team_a"]), json=payload)
    assert updated.status_code == 200, updated.text
    result = updated.json()
    assert result["context_length"] == 1050000
    for key in ("source", "upstream_present", "visible", "hidden_reason", "last_preflight_status", "last_preflight_at"):
        assert result[key] == original[key]
    assert result["capabilities"]["context_specification_url"] == payload["specification_url"]
    absent = c.patch(path.replace("gpt-5.4", "not-cached"), headers=_auth(tokens["team_a"]), json=payload)
    assert absent.status_code == 404
    secret_url = c.patch(path, headers=_auth(tokens["team_a"]), json={**payload, "specification_url": "https://user:secret@example.com/spec"})
    assert secret_url.status_code == 422
