"""Request-pool management exercises the real HTTP/auth/PostgreSQL boundary."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import select

from loom.db.schema import (
    ProviderConnection,
    ProviderConnectionShare,
    ProviderModelCache,
    ProviderRequestPool,
    ProviderRequestPoolMember,
)
from tests.integration.test_provider_connections_routes import app_setup as app_setup

_MODELS = ["student-model", "teacher-model"]


@pytest.fixture
async def pool_setup(app_setup):
    app, tokens, teams = app_setup
    ids = [uuid4() for _ in range(4)]
    async with app.state.session_factory() as session:
        for i, connection_id in enumerate(ids):
            session.add(
                ProviderConnection(
                    id=connection_id,
                    team_id=teams["b"] if i == 3 else teams["a"],
                    provider_type="openai-compatible",
                    display_name=f"pool-{i}",
                    base_url=f"https://provider-{i}.example.test/v1",
                    upstream_host=f"provider-{i}.example.test",
                    resolved_egress_ips=["203.0.113.1"],
                    encrypted_api_key_ref=f"test://pool-secret-{i}",
                    status="valid",
                    created_by="test:pool",
                    allowed_models=_MODELS,
                )
            )
        await session.flush()
        for connection_id in ids:
            for model in _MODELS:
                session.add(
                    ProviderModelCache(
                        provider_connection_id=connection_id,
                        model_id=model,
                        upstream_present=True,
                        visible=True,
                        last_preflight_status="valid",
                    )
                )
        await session.commit()
    return app, tokens, teams, ids


def _config(ids):
    return {
        "required_models": _MODELS,
        "equivalent_models": True,
        "members": [
            {"connection_id": str(i), "quota_group": "shared-yibu" if n else "az"}
            for n, i in enumerate(ids[:3])
        ],
    }


def _headers(token):
    return {"Authorization": f"Bearer {token}"}


async def test_configure_read_reconfigure_disable_preserves_state(pool_setup):
    app, tokens, _, ids = pool_setup
    path = f"/api/v1/provider-connections/{ids[0]}/request-pool"
    headers = _headers(tokens["team_a"])
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        result = await client.put(path, headers=headers, json=_config(ids))
        assert result.status_code == 200, result.text
        body = result.json()
        assert body["enabled"] is True
        assert body["attempt_timeout_sec"] == 20
        assert [m["ordinal"] for m in body["members"]] == [0, 1, 2]
        assert "secret" not in result.text and "api_key" not in result.text
        assert (await client.get(path, headers=headers)).json() == body
        cooldown = datetime.now(UTC) + timedelta(seconds=30)
        async with app.state.session_factory() as session:
            pool = await session.get(ProviderRequestPool, ids[0])
            pool.next_member = 7
            member = await session.get(ProviderRequestPoolMember, (ids[0], ids[1]))
            member.cooldown_until = cooldown
            await session.commit()
        result = await client.put(path, headers=headers, json=_config(ids))
        assert result.status_code == 200, result.text
        async with app.state.session_factory() as session:
            assert (await session.get(ProviderRequestPool, ids[0])).next_member == 7
            assert (
                await session.get(ProviderRequestPoolMember, (ids[0], ids[1]))
            ).cooldown_until == cooldown
        result = await client.delete(path, headers=headers)
        assert result.status_code == 200, result.text
        assert result.json()["enabled"] is False
        assert len(result.json()["members"]) == 3


async def test_shared_root_read_requires_member_access_and_shared_users_cannot_edit(pool_setup):
    app, tokens, teams, ids = pool_setup
    path = f"/api/v1/provider-connections/{ids[0]}/request-pool"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        assert (
            await client.put(path, headers=_headers(tokens["team_a"]), json=_config(ids))
        ).status_code == 200
        assert (await client.get(path, headers=_headers(tokens["team_b"]))).status_code == 404
        async with app.state.session_factory() as session:
            session.add(
                ProviderConnectionShare(
                    provider_connection_id=ids[0],
                    target_team_id=teams["b"],
                    created_by_actor="test:pool",
                )
            )
            await session.commit()
        assert (await client.get(path, headers=_headers(tokens["team_b"]))).status_code == 404
        async with app.state.session_factory() as session:
            for connection_id in ids[1:3]:
                session.add(
                    ProviderConnectionShare(
                        provider_connection_id=connection_id,
                        target_team_id=teams["b"],
                        created_by_actor="test:pool",
                    )
                )
            await session.commit()
        assert (await client.get(path, headers=_headers(tokens["team_b"]))).status_code == 200
        assert (
            await client.put(path, headers=_headers(tokens["team_b"]), json=_config(ids))
        ).status_code == 404
        assert (await client.delete(path, headers=_headers(tokens["team_b"]))).status_code == 404


async def test_rejects_member_not_authorized_for_owner_even_for_admin(pool_setup):
    app, tokens, _, ids = pool_setup
    path = f"/api/v1/provider-connections/{ids[0]}/request-pool"
    config = _config([ids[0], ids[1], ids[3]])
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        for who in ["team_a", "admin"]:
            response = await client.put(
                path,
                headers=_headers(tokens[who]) | {"X-Loom-Admin-Actor": "pool-test"},
                json=config,
            )
            assert response.status_code == 404, response.text
        response = await client.put(
            path, headers=_headers(tokens["team_a_limited"]), json=_config(ids)
        )
        assert response.status_code == 403, response.text


@pytest.mark.parametrize(
    "change",
    ["missing-teacher", "hidden", "not-present", "failed-preflight", "invalid", "wrong-type"],
)
async def test_rejects_incompatible_member_before_pool_creation(pool_setup, change):
    app, tokens, _, ids = pool_setup
    async with app.state.session_factory() as session:
        row = await session.get(ProviderConnection, ids[1])
        model = await session.get(ProviderModelCache, (ids[1], "teacher-model"))
        if change == "missing-teacher":
            await session.delete(model)
        elif change == "hidden":
            model.visible = False
        elif change == "not-present":
            model.upstream_present = False
        elif change == "failed-preflight":
            model.last_preflight_status = "failed"
            model.last_preflight_error_code = "model_not_found"
            model.last_preflight_http_status = 404
        elif change == "invalid":
            row.status = "invalid"
        elif change == "wrong-type":
            row.provider_type = "anthropic"
        await session.commit()
    path = f"/api/v1/provider-connections/{ids[0]}/request-pool"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.put(path, headers=_headers(tokens["team_a"]), json=_config(ids))
        assert response.status_code == 400, response.text
    async with app.state.session_factory() as session:
        assert await session.get(ProviderRequestPool, ids[0]) is None


async def test_rejects_nested_pool_and_missing_equivalence(pool_setup):
    app, tokens, _, ids = pool_setup
    headers = _headers(tokens["team_a"])
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        path = f"/api/v1/provider-connections/{ids[0]}/request-pool"
        config = _config(ids)
        config["equivalent_models"] = False
        assert (await client.put(path, headers=headers, json=config)).status_code == 422
        config = _config(ids)
        config["members"] = config["members"][1:]
        assert (await client.put(path, headers=headers, json=config)).status_code == 400
        assert (await client.put(path, headers=headers, json=_config(ids))).status_code == 200
        other = f"/api/v1/provider-connections/{ids[1]}/request-pool"
        config = _config([ids[1], ids[2]])
        response = await client.put(other, headers=headers, json=config)
        assert response.status_code == 400, response.text
        async with app.state.session_factory() as session:
            assert len((await session.scalars(select(ProviderRequestPool))).all()) == 1


@pytest.mark.parametrize(
    "status,error,http_status", [(None, None, None), ("failed", "upstream_timeout", 504)]
)
async def test_inconclusive_or_missing_preflight_is_not_a_new_paid_gate(
    pool_setup, status, error, http_status
):
    app, tokens, _, ids = pool_setup
    async with app.state.session_factory() as session:
        model = await session.get(ProviderModelCache, (ids[1], "teacher-model"))
        model.last_preflight_status = status
        model.last_preflight_error_code = error
        model.last_preflight_http_status = http_status
        await session.commit()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.put(
            f"/api/v1/provider-connections/{ids[0]}/request-pool",
            headers=_headers(tokens["team_a"]),
            json=_config(ids),
        )
        assert response.status_code == 200, response.text


def test_pool_migration_roundtrip_preserves_existing_connections(migration_template_postgres_url):
    import os
    import subprocess
    import sys
    from contextlib import contextmanager

    from sqlalchemy import create_engine, inspect, text
    from sqlalchemy.engine import make_url

    from tests.integration.provider_connection_fixtures import insert_openai_connection
    from tests.support.postgres_databases import isolated_migration_database

    template = make_url(migration_template_postgres_url).database
    with contextmanager(isolated_migration_database)(
        migration_template_postgres_url,
        template_name=template,
        prepare_template=False,
    ) as url:

        def migrate(direction, revision):
            result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "alembic",
                    "-c",
                    "database/migrations/alembic.ini",
                    direction,
                    revision,
                ],
                env={**os.environ, "LOOM_DB_URL": url},
                capture_output=True,
                text=True,
            )
            assert result.returncode == 0, result.stderr

        migrate("downgrade", "0182")
        engine = create_engine(url)
        from sqlalchemy.orm import Session

        from loom.db.schema import Team

        team = uuid4()
        with Session(engine) as session:
            session.add(Team(id=team, name=f"migration-pool-{team}"))
            session.flush()
            connection = insert_openai_connection(session, team_id=team, model_ids=_MODELS)
            session.commit()
        migrate("upgrade", "0183")
        with engine.begin() as db:
            assert (
                db.scalar(
                    text("SELECT encrypted_api_key_ref FROM provider_connections WHERE id=:id"),
                    {"id": connection},
                )
                == f"test://{connection}"
            )
            db.execute(
                text(
                    "INSERT INTO provider_request_pools(connection_id,required_models) VALUES (:id,ARRAY['student-model'])"
                ),
                {"id": connection},
            )
            assert (
                db.scalar(
                    text(
                        "SELECT attempt_timeout_sec FROM provider_request_pools WHERE connection_id=:id"
                    ),
                    {"id": connection},
                )
                == 20
            )
            assert {"pool_connection_id", "failover_reason"} <= {
                c["name"] for c in inspect(db).get_columns("gateway_dispatch_receipts")
            }
        migrate("downgrade", "0182")
        with engine.connect() as db:
            assert not inspect(db).has_table("provider_request_pools")
            assert (
                db.scalar(
                    text("SELECT encrypted_api_key_ref FROM provider_connections WHERE id=:id"),
                    {"id": connection},
                )
                == f"test://{connection}"
            )
        migrate("upgrade", "0183")
        with engine.connect() as db:
            assert db.scalar(text("SELECT count(*) FROM provider_request_pools")) == 0
        engine.dispose()


@pytest.mark.parametrize("obsolete", ["max_in_flight", "quota_max_in_flight"])
async def test_pool_rejects_obsolete_concurrency_cap_fields(pool_setup, obsolete):
    app, tokens, _, ids = pool_setup
    config = _config(ids)
    config["members"][1][obsolete] = 1
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.put(
            f"/api/v1/provider-connections/{ids[0]}/request-pool",
            headers=_headers(tokens["team_a"]),
            json=config,
        )
        assert response.status_code == 422, response.text
        assert "extra_forbidden" in response.text
