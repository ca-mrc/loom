"""Bounded pages must not let old waiting or broken builds starve later work."""
from __future__ import annotations

import json

import httpx
import pytest

from loom_execution_actuator.pool_build_driver import PoolBuildDriver
from loom_execution_actuator.pool_build_runtime import PoolNativeBuildController
from loom_execution_actuator.pool_client import PoolRequestUnconfirmedError
from tests.integration.test_nebius_pool_build_driver import selected
from tests.integration.test_nebius_pool_build_outbox import grant, local_setup, outbox
from tests.integration.test_nebius_pool_build_runtime import Reader
from tests.integration.test_nebius_pool_observation_registry import sessions as sessions
from tests.integration.test_nebius_pool_participant_http import client


async def selection(sessions, participant, journal):
    _, original, _ = await local_setup(sessions, environment_id=participant.environment_id)
    request = original.model_copy(update={"pool_id": participant.pool_id,
        "admission_epoch": participant.admission_epoch, "participant_revision": participant.binding_revision,
        "key": original.key.model_copy(update={"participant_id": participant.participant_id})})
    await journal.remember(request)
    return request


async def test_keyset_scan_survives_terminal_rows_and_defers_new_selections(sessions):
    participant, first, _ = await local_setup(sessions)
    journal = outbox(sessions, participant)
    await journal.remember(first)
    second = await selection(sessions, participant, journal)
    third = await selection(sessions, participant, journal)
    scan = journal.iter_pending(page_size=1)
    assert (await anext(scan)).request.key == first.key
    for request in (first, second):
        await journal.request_cancel(request.key)
        await journal.confirm_cancel(request.key, grant(request, phase="cancelled_unstarted"))
    later = await selection(sessions, participant, journal)
    assert [item.request.key async for item in scan] == [third.key]
    assert [item.request.key async for item in journal.iter_pending(page_size=1)] == [third.key, later.key]


async def test_controller_reaches_later_pages_even_when_first_hundred_builds_fail(sessions, tmp_path):
    app, token, participant, _, journal = await selected(sessions, tmp_path)
    for _ in range(100):
        last = await selection(sessions, participant, journal)

    class Boundary(httpx.AsyncBaseTransport):
        inner = httpx.ASGITransport(app=app)

        async def handle_async_request(self, request):
            if request.url.path.endswith("/prepare") and json.loads(request.content)["key"]["local_work_id"] != str(last.key.local_work_id):
                raise httpx.ReadTimeout("other request unavailable", request=request)
            return await self.inner.handle_async_request(request)

    async with httpx.AsyncClient(transport=Boundary()) as http:
        driver = PoolBuildDriver(outbox=journal, management=client(http, token))
        with pytest.raises(PoolRequestUnconfirmedError):
            await PoolNativeBuildController(driver=driver, kubernetes=Reader()).run_once()
    assert (await journal.get(last.key)).phase == "active"
