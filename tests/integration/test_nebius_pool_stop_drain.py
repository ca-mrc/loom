"""Stop fences creates and signals the Job; final cleanup separately requires drain."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import select, update
from sqlalchemy.exc import DBAPIError

from loom.db.nebius_pool_schema import NebiusPoolRequest
from loom.pipeline.keys import canonical_digest
from tests.integration.test_nebius_pool_control import action
from tests.integration.test_nebius_pool_gateway_journal import setup
from tests.integration.test_nebius_pool_observation_registry import sessions as sessions


async def stop_input(sessions, receipt, **changes):
    from loom.nebius_pool_lifecycle import PoolStopV1
    from loom_service.pool_management.registry import _WORKLOAD

    async with sessions() as session:
        row = await session.get(NebiusPoolRequest, receipt.reservation_id)
        request = _WORKLOAD.validate_python(row.request_json)
    generation = request.build.expected_lease_epoch + 1 if hasattr(request, "build") else request.execution.lease_generation
    return PoolStopV1(action=action(request), reservation_id=receipt.reservation_id,
        plan_sha256=receipt.plan_sha256, lease_generation=generation,
        cause="cancelled", grace_deadline_at=datetime.now(UTC) + timedelta(minutes=5), **changes)


def drain_input(stop, **changes):
    from loom.nebius_pool_lifecycle import PoolDrainV1

    return PoolDrainV1(action=stop.action, reservation_id=stop.reservation_id,
        plan_sha256=stop.plan_sha256, lease_generation=stop.lease_generation,
        stop_sha256=canonical_digest(stop.model_dump(mode="json")).removeprefix("sha256:"),
        output_generation=1, output_state="unavailable", evidence_sha256="d" * 64, **changes)


async def accept_stop(sessions, principal, body):
    from loom_service.pool_management.lifecycle import stop_pool_request

    async with sessions.begin() as session:
        return await stop_pool_request(session, principal, body)


async def accept_drain(sessions, principal, body):
    from loom_service.pool_management.lifecycle import drain_pool_request

    async with sessions.begin() as session:
        return await drain_pool_request(session, principal, body)


async def test_stop_can_signal_job_before_drain_and_retains_bounded_foreground_grace(sessions):
    from loom_service.pool_management.gateway_journal import PoolGatewayError

    journal, gateway, receipt, owner = await setup(sessions)
    created = await journal.prepare_create(gateway, receipt.reservation_id, kind="Job")
    await journal.dispatch_create(gateway, created.effect_id)
    created = await journal.observe_create(gateway, created.effect_id, uid=uuid4(), resource_version="1")
    stop = await stop_input(sessions, receipt)
    result = await accept_stop(sessions, owner, stop)
    assert result.phase == "cleanup_intent" and result.capacity_charged
    deletion = await journal.prepare_delete(gateway, receipt.reservation_id, kind="Job")
    assert deletion.document["propagationPolicy"] == "Foreground"
    assert 0 <= deletion.document["gracePeriodSeconds"] <= 300
    assert deletion.document["preconditions"] == {"uid": str(created.observed_uid)}
    async with sessions() as session:
        row = await session.get(NebiusPoolRequest, receipt.reservation_id)
        assert row.drain_json is None
        assert deletion.document["gracePeriodSeconds"] <= row.plan_json["job"]["spec"]["template"]["spec"]["terminationGracePeriodSeconds"]
    # The same stop also fences every not-yet-dispatched create.
    with pytest.raises(PoolGatewayError):
        await journal.prepare_create(gateway, receipt.reservation_id, kind="ConfigMap")
    assert await accept_stop(sessions, owner, stop) == result
    assert await journal.prepare_delete(gateway, receipt.reservation_id, kind="Job") == deletion


async def test_auxiliary_cleanup_waits_for_exact_drain_and_neither_ack_releases_capacity(sessions):
    from loom_service.pool_management.gateway_journal import PoolGatewayError

    journal, gateway, receipt, owner = await setup(sessions, build=True)
    created = await journal.prepare_create(gateway, receipt.reservation_id, kind="ConfigMap")
    await journal.dispatch_create(gateway, created.effect_id)
    await journal.observe_create(gateway, created.effect_id, uid=uuid4(), resource_version="1")
    stop = await stop_input(sessions, receipt)
    drain = drain_input(stop)
    with pytest.raises(ValueError):
        await accept_drain(sessions, owner, drain)
    await accept_stop(sessions, owner, stop)
    with pytest.raises(PoolGatewayError):
        await journal.prepare_delete(gateway, receipt.reservation_id, kind="ConfigMap")
    result = await accept_drain(sessions, owner, drain)
    assert result.phase == "cleanup_intent" and result.capacity_charged and result.cleanup_observation_id is None
    assert await accept_drain(sessions, owner, drain) == result
    deletion = await journal.prepare_delete(gateway, receipt.reservation_id, kind="ConfigMap")
    assert deletion.document["preconditions"]["uid"]


@pytest.mark.parametrize("damage", ["reservation", "plan", "lease", "digest"])
async def test_foreign_stop_or_drain_cannot_authorize_cleanup(sessions, damage):
    _, _, receipt, owner = await setup(sessions, build=True)
    stop = await stop_input(sessions, receipt)
    change = {"reservation": {"reservation_id": uuid4()}, "plan": {"plan_sha256": "a" * 64},
        "lease": {"lease_generation": stop.lease_generation + 1},
        "digest": {"action": stop.action.model_copy(update={"request_sha256": "b" * 64})}}[damage]
    with pytest.raises(ValueError):
        await accept_stop(sessions, owner, stop.model_copy(update=change))
    async with sessions() as session:
        assert (await session.get(NebiusPoolRequest, receipt.reservation_id)).phase == "create_intent"
    await accept_stop(sessions, owner, stop)
    with pytest.raises(ValueError):
        await accept_drain(sessions, owner, drain_input(stop).model_copy(update=change))
    async with sessions() as session:
        assert (await session.get(NebiusPoolRequest, receipt.reservation_id)).drain_json is None


async def test_stop_and_drain_evidence_are_immutable_in_sql_and_on_replay(sessions):
    _, _, receipt, owner = await setup(sessions)
    stop = await stop_input(sessions, receipt)
    await accept_stop(sessions, owner, stop)
    drain = drain_input(stop)
    await accept_drain(sessions, owner, drain)
    with pytest.raises(ValueError):
        await accept_stop(sessions, owner, stop.model_copy(update={"grace_deadline_at": stop.grace_deadline_at + timedelta(seconds=1)}))
    with pytest.raises(ValueError):
        await accept_drain(sessions, owner, drain.model_copy(update={"evidence_sha256": "f" * 64}))
    async with sessions.begin() as session:
        for values in ({"stop_json": None}, {"stop_json": {}}, {"drain_json": None}, {"drain_json": {}}):
            with pytest.raises(DBAPIError):
                async with session.begin_nested():
                    await session.execute(update(NebiusPoolRequest).values(**values))


async def test_real_participant_transport_retains_stop_then_drain(sessions, tmp_path):
    from tests.integration.test_nebius_pool_participant_http import client
    from tests.integration.test_nebius_pool_participant_http import setup as http_setup

    app, _, token, _, executions, _ = await http_setup(sessions, tmp_path)
    request = executions[0]
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app)) as http:
        management = client(http, token)
        await management.prepare(request)
        receipt = await management.activate(action(request, activation=True))
        stop = await stop_input(sessions, receipt)
        assert (await management.stop(stop)).phase == "cleanup_intent"
        result = await management.drain(drain_input(stop))
        assert result.capacity_charged and result.cleanup_observation_id is None
    async with sessions() as session:
        row = await session.get(NebiusPoolRequest, receipt.reservation_id)
        assert row.stop_json["request"] == stop.model_dump(mode="json")
        assert row.drain_json == drain_input(stop).model_dump(mode="json")
