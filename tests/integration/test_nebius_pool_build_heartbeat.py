"""Active native claims must survive slow admission without renewing consent."""
from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta

import httpx
import pytest
from sqlalchemy import update

from loom.db.schema import TaskImageMaterialization, Trial
from loom_execution_actuator.pool_build_driver import PoolBuildDriver
from loom_execution_actuator.pool_build_runtime import PoolNativeBuildController
from tests.integration.test_nebius_pool_build_driver import selected
from tests.integration.test_nebius_pool_build_outbox import grant, local_setup, outbox
from tests.integration.test_nebius_pool_build_runtime import Reader, local_rows
from tests.integration.test_nebius_pool_build_scan import selection
from tests.integration.test_nebius_pool_observation_registry import sessions as sessions
from tests.integration.test_nebius_pool_participant_http import client


async def test_local_heartbeat_progresses_while_older_admission_http_is_stalled(sessions, tmp_path):
    app, token, participant, waiting, journal = await selected(sessions, tmp_path)
    active = await selection(sessions, participant, journal)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app)) as http:
        await PoolBuildDriver(outbox=journal, management=client(http, token)).advance(active.key)
    original = (await journal.get(active.key)).activation
    soon = datetime.now(UTC) + timedelta(seconds=25)
    async with sessions.begin() as session:
        await session.execute(update(TaskImageMaterialization).where(
            TaskImageMaterialization.id == active.key.local_work_id).values(lease_expires_at=soon))
    entered, released = asyncio.Event(), asyncio.Event()

    class SlowAdmission(httpx.AsyncBaseTransport):
        inner = httpx.ASGITransport(app=app)

        async def handle_async_request(self, request):
            if request.url.path.endswith("/prepare") and json.loads(request.content)["key"]["local_work_id"] == str(waiting.key.local_work_id):
                entered.set()
                await released.wait()
            return await self.inner.handle_async_request(request)

    async with httpx.AsyncClient(transport=SlowAdmission()) as http:
        controller = PoolNativeBuildController(driver=PoolBuildDriver(outbox=journal, management=client(http, token)),
            kubernetes=Reader())
        running = asyncio.create_task(controller.run_once())
        try:
            await asyncio.wait_for(entered.wait(), 5)
            await asyncio.wait_for(controller.heartbeat_once(), 5)
            row, _ = await local_rows(sessions, active)
            assert soon < row.lease_expires_at <= active.deadline_at
            assert row.attempt_count == 1 and row.lease_epoch == 1
            assert (await journal.get(active.key)).activation == original
            assert not running.done()
        finally:
            released.set()
            await running


@pytest.mark.parametrize("damage", ["none", "superseded", "expired", "source", "cancelled", "local-stop"])
async def test_local_heartbeat_rechecks_current_attempt_and_demand(sessions, tmp_path, damage):
    app, token, _, request, journal = await selected(sessions, tmp_path)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app)) as http:
        driver = PoolBuildDriver(outbox=journal, management=client(http, token))
        await driver.advance(request.key)
        soon = datetime.now(UTC) + timedelta(seconds=-1 if damage == "expired" else 25)
        values = {"lease_expires_at": soon}
        if damage == "superseded":
            values.update(lease_epoch=2, claimed_by="successor")
        elif damage == "source":
            values["task_source"] = "s3://other/source/"
        async with sessions.begin() as session:
            await session.execute(update(TaskImageMaterialization).values(**values))
            if damage == "cancelled":
                await session.execute(update(Trial).values(cancellation_requested_at=datetime.now(UTC)))
        if damage == "local-stop":
            await journal.request_cancel(request.key)
        controller = PoolNativeBuildController(driver=driver, kubernetes=Reader())
        await controller.heartbeat_once()
    row, _ = await local_rows(sessions, request)
    assert (row.lease_expires_at > soon) == (damage == "none")
    assert row.attempt_count == 1


@pytest.mark.parametrize("phase", ["attached", "activation_pending"])
async def test_pre_activation_heartbeat_is_local_and_cannot_extend_original_cutoff(sessions, phase):
    participant, original, _ = await local_setup(sessions)
    request = original.model_copy(update={"deadline_at": datetime.now(UTC) + timedelta(seconds=60)})
    journal = outbox(sessions, participant)
    await journal.remember(request)
    await journal.accept_grant(request.key, grant(request))
    if phase == "activation_pending":
        await journal.begin_activation(request.key)
    consent = (await journal.get(request.key)).activation

    async def deny_http(request):
        raise AssertionError("local heartbeat attempted external I/O")

    async with httpx.AsyncClient(transport=httpx.MockTransport(deny_http)) as http:
        controller = PoolNativeBuildController(driver=PoolBuildDriver(outbox=journal, management=client(http, "unused")),
            kubernetes=Reader())
        await controller.heartbeat_once()
    row, _ = await local_rows(sessions, request)
    assert row.lease_expires_at == request.deadline_at
    assert row.attempt_count == 1 and (await journal.get(request.key)).phase == phase
    assert (await journal.get(request.key)).activation == consent
