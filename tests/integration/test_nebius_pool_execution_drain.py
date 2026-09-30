"""Execution cancellation signals stop before the local durable output window closes."""
from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import select, update
from sqlalchemy.exc import DBAPIError

from loom.db.nebius_pool_outbox_schema import NebiusPoolExecutionOutbox
from loom.db.nebius_pool_schema import NebiusPoolRequest
from loom.db.schema import ServiceExecutionLease
from loom.pipeline.keys import canonical_digest
from loom_control_plane.service_execution import (
    enqueue_execution_transition,
    mark_execution_output_unavailable,
)
from loom_execution_actuator.pool_execution_driver import PoolExecutionDriver
from tests.integration.test_nebius_pool_execution_activation import selected
from tests.integration.test_nebius_pool_observation_registry import sessions as sessions
from tests.integration.test_nebius_pool_participant_http import client


async def cancel(sessions, active):
    async with sessions.begin() as session:
        lease = await session.get(ServiceExecutionLease, active.lease_id)
        await enqueue_execution_transition(session, lease_id=lease.id, expected_generation=lease.generation,
            desired_state="cancel")


@pytest.mark.parametrize("lost", [None, "stop", "drain"])
async def test_execution_stop_precedes_output_drain_and_lost_replies_replay_exactly(sessions, tmp_path, lost):
    from loom_execution_actuator.pool_client import PoolRequestUnconfirmedError

    outbox, _, proposed, app, token = await selected(sessions, tmp_path)
    sent, dropped = [], False
    inner = httpx.ASGITransport(app=app)

    class Boundary(httpx.AsyncBaseTransport):
        async def handle_async_request(self, incoming):
            nonlocal dropped
            operation = incoming.url.path.rsplit("/", 1)[-1]
            if operation in {"stop", "drain"}:
                sent.append((operation, incoming.content))
                async with sessions() as reader:
                    row = await reader.get(NebiusPoolExecutionOutbox, proposed.request.key.local_work_id)
                    assert getattr(row, operation + "_json") is not None
            response = await inner.handle_async_request(incoming)
            if operation == lost and not dropped:
                assert response.status_code == 200
                dropped = True
                await response.aclose()
                raise httpx.ReadError("lost after commit")
            return response

    async with httpx.AsyncClient(transport=Boundary()) as http:
        driver = PoolExecutionDriver(outbox=outbox, management=client(http, token))
        active = await driver.advance(proposed.request.key)
        assert await outbox.begin_stop(proposed.request.key) is None
        await cancel(sessions, active)
        if lost == "stop":
            with pytest.raises(PoolRequestUnconfirmedError):
                await driver.stop_and_drain(proposed.request.key)
        await driver.stop_and_drain(proposed.request.key)
        assert {operation for operation, _ in sent} == {"stop"}
        async with sessions() as session:
            lease = await session.get(ServiceExecutionLease, active.lease_id)
            assert lease.output_commit_state == "not_started" and lease.deleted_at is None
            remote = await session.get(NebiusPoolRequest, active.reservation_id)
            assert remote.phase == "cleanup_intent" and remote.drain_json is None
            assert remote.cleanup_observation_id is None
        async with sessions.begin() as session:
            lease = await session.get(ServiceExecutionLease, active.lease_id)
            await mark_execution_output_unavailable(session, lease_id=lease.id,
                expected_generation=lease.generation, reason="operator_cancelled", allow_cancel_before_deadline=True)
        if lost == "drain":
            with pytest.raises(PoolRequestUnconfirmedError):
                await driver.stop_and_drain(proposed.request.key)
        await driver.stop_and_drain(proposed.request.key)
        stop, drain = await outbox.begin_stop(proposed.request.key), await outbox.begin_drain(proposed.request.key)
        assert stop.cause == "cancelled" and stop.lease_generation == 1
        assert drain.output_state == "unavailable" and drain.output_generation == 1
        assert drain.stop_sha256 == canonical_digest(stop.model_dump(mode="json")).removeprefix("sha256:")
        assert all(len({body for operation, body in sent if operation == kind}) == 1 for kind in ("stop", "drain"))
        async with sessions() as session:
            remote = await session.get(NebiusPoolRequest, active.reservation_id)
            assert remote.phase == "cleanup_intent" and remote.cleanup_observation_id is None
            assert (await session.get(ServiceExecutionLease, active.lease_id)).deleted_at is None


@pytest.mark.parametrize("wrong_generation", [False, True])
async def test_execution_drain_binds_committed_output_evidence_not_current_command_generation(sessions, tmp_path, wrong_generation):
    outbox, _, proposed, app, token = await selected(sessions, tmp_path)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app)) as http:
        active = await PoolExecutionDriver(outbox=outbox, management=client(http, token)).advance(proposed.request.key)
    await cancel(sessions, active)
    async with sessions.begin() as session:
        lease = await session.get(ServiceExecutionLease, active.lease_id)
        lease.output_commit_state = "committed"
        lease.output_generation = 2 if wrong_generation else 1
        lease.output_upload_session_id = uuid4()
        lease.output_manifest_sha256, lease.output_marker_sha256 = "sha256:" + "a" * 64, "sha256:" + "b" * 64
        lease.output_committed_at = datetime.now(UTC)
    await outbox.begin_stop(proposed.request.key)
    if wrong_generation:
        with pytest.raises(ValueError):
            await outbox.begin_drain(proposed.request.key)
    else:
        drain = await outbox.begin_drain(proposed.request.key)
        assert drain.output_state == "committed" and drain.output_generation == 1
        async with sessions() as session:
            saved = await session.get(NebiusPoolExecutionOutbox, active.lease_id)
            assert saved.output_json["manifest_sha256"] == "sha256:" + "a" * 64
            assert drain.evidence_sha256 == canonical_digest(saved.output_json).removeprefix("sha256:")
            assert (await session.get(ServiceExecutionLease, active.lease_id)).generation == 2


@pytest.mark.parametrize("field", ["stop_json", "drain_json", "output_json"])
async def test_execution_cleanup_attestations_are_database_immutable(sessions, tmp_path, field):
    outbox, _, proposed, app, token = await selected(sessions, tmp_path)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app)) as http:
        active = await PoolExecutionDriver(outbox=outbox, management=client(http, token)).advance(proposed.request.key)
    await cancel(sessions, active)
    await outbox.begin_stop(proposed.request.key)
    async with sessions.begin() as session:
        lease = await session.get(ServiceExecutionLease, active.lease_id)
        await mark_execution_output_unavailable(session, lease_id=lease.id,
            expected_generation=lease.generation, reason="operator_cancelled", allow_cancel_before_deadline=True)
    await outbox.begin_drain(proposed.request.key)
    async with sessions() as session:
        before = await session.scalar(select(getattr(NebiusPoolExecutionOutbox, field)))
    with pytest.raises(DBAPIError):
        async with sessions.begin() as session:
            await session.execute(update(NebiusPoolExecutionOutbox).values({field: {**before, "foreign": "changed"}}))
