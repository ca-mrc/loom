"""Real management HTTP + PostgreSQL accept only dedicated physical observers."""
from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import func, select, update

from loom.db.nebius_pool_schema import NebiusPoolBinding, NebiusPoolCapture, NebiusPoolObservation
from loom.db.schema import Token
from loom.pipeline.keys import canonical_digest
from loom_execution_capacity_collector.pool_client import PoolObservationClient
from loom_execution_capacity_collector.pool_collector import collect_pool_observation
from loom_execution_capacity_collector.pool_contracts import PoolObservationV1
from loom_service.app import create_app
from loom_service.config import LoomServiceSettings
from tests.integration.test_nebius_pool_auth import credential
from tests.integration.test_nebius_pool_observation_registry import sessions as sessions
from tests.integration.test_nebius_pool_observation_registry import snapshots
from tests.unit.test_nebius_pool_collector import cluster_reader, native_reader, settings
from tests.unit.test_nebius_pool_execution_render import inputs


async def setup(sessions, tmp_path, *, role="observer", **changes):
    participant, _ = inputs()
    participant = participant.model_copy(update={"admission_epoch": 1})
    raw, pool_id, _, _ = await credential(sessions, role=role, participant_config=participant, **changes)
    binding = {"node_selector": {"loom.nebius/role": "execution", "nebius.com/node-group-id": "group-1"}}
    async with sessions.begin() as session:
        await session.execute(update(NebiusPoolBinding).where(NebiusPoolBinding.pool_id == pool_id).values(
            policy_revision=2, binding_json=binding, binding_sha256=canonical_digest(binding).removeprefix("sha256:")))
    app = create_app(LoomServiceSettings(_env_file=None, service_mode="management",
        db_url="postgresql+asyncpg://unused:unused@localhost/unused"))
    app.state.session_factory = sessions
    token = tmp_path / "pool-token"
    token.write_text(raw)
    token.chmod(0o600)
    return app, raw, pool_id, token


async def test_actual_collector_to_management_http_persists_one_physical_snapshot(sessions, tmp_path):
    app, _, pool_id, token = await setup(sessions, tmp_path)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="https://management.example") as http:
        client = PoolObservationClient(origin="https://management.example", bearer_token_file=token,
                                       timeout_seconds=10, client=http)
        config = settings(tmp_path).model_copy(update={"pool_id": pool_id, "nebius_node_group_id": "group-1"})
        result = await collect_pool_observation(config, management=client, provider=native_reader(config),
                                               kubernetes=cluster_reader([], group_id="group-1"))
    async with sessions() as session:
        row = await session.get(NebiusPoolObservation, result.observation_id)
        assert row.observation_sha256 == result.observation_sha256
        assert row.observation_json["kubernetes"]["active_nodes"] == 1
        assert row.observation_json["provider"]["quota_resources"]["nodes"]["used"] == 1
        assert await session.scalar(select(func.count()).select_from(NebiusPoolCapture)) == 1
        assert await session.scalar(select(func.count()).select_from(NebiusPoolObservation)) == 1


async def test_observation_exact_replay_and_changed_body_are_not_a_new_capture(sessions, tmp_path):
    app, raw, pool_id, token = await setup(sessions, tmp_path)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="https://management.example") as http:
        client = PoolObservationClient(origin="https://management.example", bearer_token_file=token,
                                       timeout_seconds=10, client=http)
        capture = await client.issue_capture(pool_id)
        provider, cluster = snapshots(capture)
        observation = PoolObservationV1(capture_id=capture.capture_id, observed_at=datetime.now(UTC),
                                       provider=provider, kubernetes=cluster)
        first = await client.publish(pool_id, observation)
        assert await client.publish(pool_id, observation) == first
        altered = observation.payload()
        altered["provider"]["used_nodes"] += 1
        response = await http.post(f"/internal/pools/v1/{pool_id}/observations", json=altered,
                                   headers={"Authorization": "Bearer " + raw})
        assert response.status_code == 409
        assert response.headers["cache-control"] == "no-store"
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(NebiusPoolObservation)) == 1


@pytest.mark.parametrize("authority", ["missing", "ordinary", "admin", "worker", "participant", "gateway", "revoked", "foreign_pool"])
async def test_observer_http_has_no_generic_bearer_or_role_fallback(sessions, tmp_path, authority):
    changes = {"type": authority} if authority in {"admin", "worker"} else {}
    role = authority if authority in {"participant", "gateway"} else "observer"
    app, raw, pool_id, _ = await setup(sessions, tmp_path, role=role, **changes)
    if authority == "revoked":
        async with sessions.begin() as session:
            await session.execute(update(Token).where(Token.token_hash == hashlib.sha256(raw.encode()).digest()).values(
                revoked_at=datetime.now(UTC)))
    if authority == "foreign_pool":
        pool_id = uuid4()
    headers = {} if authority == "missing" else {"Authorization": "Bearer " + ("ordinary-token" if authority == "ordinary" else raw)}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="https://management.example") as http:
        response = await http.post(f"/internal/pools/v1/{pool_id}/captures", json={}, headers=headers)
    assert response.status_code in {401, 403}
    assert raw not in response.text
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(NebiusPoolCapture)) == 0


@pytest.mark.parametrize("body,status", [(b'{"unexpected":"private-input"}', 422),
    (b'{"broken":', 422), (b" " * (1024 * 1024 + 1), 413)], ids=["unknown-field", "invalid-json", "oversize"])
async def test_capture_body_is_bounded_and_error_does_not_echo_inputs(sessions, tmp_path, body, status):
    app, raw, pool_id, _ = await setup(sessions, tmp_path)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="https://management.example") as http:
        response = await http.post(f"/internal/pools/v1/{pool_id}/captures", content=body,
                                   headers={"Authorization": "Bearer " + raw, "Content-Type": "application/json"})
    assert response.status_code == status
    assert "private-input" not in response.text
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(NebiusPoolCapture)) == 0
