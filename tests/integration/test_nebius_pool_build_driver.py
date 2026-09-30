"""Drive real local SQL and management HTTP without holding a transaction over I/O."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta

import httpx
import pytest
from sqlalchemy import select, update

from loom.db.nebius_pool_outbox_schema import NebiusPoolBuildOutbox
from loom.db.schema import TaskImageMaterialization, Trial
from tests.integration.test_nebius_pool_build_outbox import counts, local_setup, outbox
from tests.integration.test_nebius_pool_observation_registry import sessions as sessions
from tests.integration.test_nebius_pool_participant_http import client, setup


async def selected(sessions, tmp_path, **changes):
    app, _, token, participants, _, _ = await setup(sessions, tmp_path, **changes)
    participant = participants[0]
    _, original, _ = await local_setup(sessions, environment_id=participant.environment_id)
    request = original.model_copy(update={"pool_id": participant.pool_id,
        "admission_epoch": participant.admission_epoch, "participant_revision": participant.binding_revision,
        "key": original.key.model_copy(update={"participant_id": participant.participant_id})})
    journal = outbox(sessions, participant)
    await journal.remember(request)
    return app, token, participant, request, journal


@pytest.mark.parametrize("lost", [None, "prepare", "activate"])
async def test_restart_recovers_one_grant_attempt_and_intent_after_lost_reply(sessions, tmp_path, lost):
    from loom_execution_actuator.pool_build_driver import PoolBuildDriver
    from loom_execution_actuator.pool_client import PoolRequestUnconfirmedError

    app, token, participant, request, journal = await selected(sessions, tmp_path)
    calls = []

    class Boundary(httpx.AsyncBaseTransport):
        inner = httpx.ASGITransport(app=app)
        dropped = False

        async def handle_async_request(self, incoming):
            operation = incoming.url.path.rsplit("/", 1)[-1]
            calls.append(operation)
            async with sessions() as reader:
                row = (await reader.scalars(select(NebiusPoolBuildOutbox))).one()
                if operation == "activate":
                    assert row.phase == "activation_pending" and row.activation_json is not None
                    assert row.attempt_id is not None  # independently visible: committed before HTTP
            response = await self.inner.handle_async_request(incoming)
            if operation == lost and not self.dropped:
                self.dropped = True
                assert response.status_code == 200
                await response.aclose()
                raise httpx.ReadError("committed reply lost")
            return response

    async with httpx.AsyncClient(transport=Boundary()) as http:
        driver = PoolBuildDriver(outbox=journal, management=client(http, token))
        if lost:
            with pytest.raises(PoolRequestUnconfirmedError):
                await driver.advance(request.key)
        recovered = PoolBuildDriver(outbox=outbox(sessions, participant), management=client(http, token))
        result = await recovered.advance(request.key)
        assert result.phase == "active" and result.activated.phase == "create_intent"
        assert await recovered.advance(request.key) == result
    assert calls.count("activate") == 1
    assert await counts(sessions, request.key.local_work_id) == (1, 1, 1)


async def test_waiting_does_not_consume_attempt_and_cancellation_finishes(sessions, tmp_path):
    from loom_execution_actuator.pool_build_driver import PoolBuildDriver

    app, token, _, request, journal = await selected(sessions, tmp_path, occupied_cpu=3000)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app)) as http:
        driver = PoolBuildDriver(outbox=journal, management=client(http, token))
        assert (await driver.advance(request.key)).phase == "selected"
        assert await counts(sessions, request.key.local_work_id) == (0, 0, 0)
        await journal.request_cancel(request.key)
        assert (await driver.advance(request.key)).phase == "cancelled"
    assert await counts(sessions, request.key.local_work_id) == (0, 0, 0)


async def test_withdrawn_waiting_demand_is_cancelled_without_requiring_a_grant(sessions, tmp_path):
    from loom_execution_actuator.pool_build_driver import PoolBuildDriver

    app, token, _, request, journal = await selected(sessions, tmp_path, occupied_cpu=3000)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app)) as http:
        management = client(http, token)
        driver = PoolBuildDriver(outbox=journal, management=management)
        assert (await driver.advance(request.key)).phase == "selected"
        async with sessions.begin() as session:
            await session.execute(update(Trial).values(cancellation_requested_at=datetime.now(UTC)))
        assert (await driver.advance(request.key)).phase == "cancelled"
        assert (await management.status((await journal.get(request.key)).action)).phase == "cancelled_unstarted"
    assert await counts(sessions, request.key.local_work_id) == (0, 0, 0)


@pytest.mark.parametrize("activated", [False, True])
async def test_stale_local_claim_recovers_global_status_before_cancelling(sessions, tmp_path, activated):
    from loom_execution_actuator.pool_build_driver import PoolBuildDriver

    app, token, _, request, journal = await selected(sessions, tmp_path)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app)) as http:
        management = client(http, token)
        await journal.accept_grant(request.key, await management.prepare(request))
        pending = await journal.begin_activation(request.key)
        if activated:
            await management.activate(pending.activation)
        async with sessions.begin() as session:
            await session.execute(update(TaskImageMaterialization).values(lease_expires_at=datetime.now(UTC) - timedelta(seconds=1)))
        driver = PoolBuildDriver(outbox=journal, management=management)
        result = await driver.advance(request.key)
        assert result.phase == ("stop_pending" if activated else "cancelled")
        assert await counts(sessions, request.key.local_work_id) == (1, 1 if activated else 0, 1)


async def test_cancel_raced_with_committed_activation_retains_stop_intent(sessions, tmp_path):
    from loom_execution_actuator.pool_build_driver import PoolBuildDriver

    app, token, _, request, journal = await selected(sessions, tmp_path)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app)) as http:
        management = client(http, token)
        await journal.accept_grant(request.key, await management.prepare(request))
        pending = await journal.begin_activation(request.key)
        await management.activate(pending.activation)
        await journal.request_cancel(request.key)
        result = await PoolBuildDriver(outbox=journal, management=management).advance(request.key)
        assert result.phase == "stop_pending" and result.activated.capacity_charged
        assert await counts(sessions, request.key.local_work_id) == (1, 1, 1)
