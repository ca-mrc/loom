"""Execution consumers read frozen runtime identity through participant authority."""
from __future__ import annotations

from uuid import uuid4

import httpx
import pytest

from loom.nebius_pool_contract import PoolRequestActionV1
from tests.integration.test_nebius_pool_gateway_journal import setup
from tests.integration.test_nebius_pool_observation_registry import sessions as sessions


async def readback(sessions, principal, receipt):
    from loom_service.pool_management.execution_runtime import execution_runtime

    async with sessions.begin() as session:
        return await execution_runtime(session, principal, PoolRequestActionV1(pool_id=receipt.pool_id,
            request_key=receipt.request_key, admission_epoch=receipt.admission_epoch, request_sha256=receipt.request_sha256))


async def test_execution_runtime_readback_binds_frozen_plan_and_observed_effect(sessions):
    journal, gateway, receipt, owner = await setup(sessions)
    before = await readback(sessions, owner, receipt)
    assert before.receipt == receipt and before.job_effect_id is None
    assert before.resource_generation == receipt.request_key.generation and before.lease_generation == 1
    effect = await journal.prepare_create(gateway, receipt.reservation_id, kind="Job")
    await journal.dispatch_create(gateway, effect.effect_id)
    observed = await journal.observe_create(gateway, effect.effect_id, uid=uuid4(), resource_version="1")
    runtime = await readback(sessions, owner, receipt)
    assert runtime.job_effect_id == observed.effect_id and runtime.receipt.job_uid == observed.observed_uid
    assert runtime.job_name == effect.document["metadata"]["name"]
    assert str(runtime.execution_unit_key) == effect.document["metadata"]["annotations"]["loom.openai.com/execution-unit-key"]
    assert set(runtime.model_dump()) == {"schema_version", "receipt", "target_id", "namespace", "job_name",
        "resource_generation", "lease_generation", "execution_unit_key", "deadline_at", "job_effect_id"}


@pytest.mark.parametrize("build,activate", [(True, True), (False, False)])
async def test_execution_runtime_rejects_build_or_unactivated_request(sessions, build, activate):
    _, _, receipt, owner = await setup(sessions, build=build, activate=activate)
    with pytest.raises(ValueError):
        await readback(sessions, owner, receipt)


async def test_execution_runtime_cannot_be_read_by_another_machine_role(sessions):
    from tests.integration.test_nebius_pool_registry import machine

    _, _, receipt, _ = await setup(sessions)
    gateway = await machine(sessions, receipt.pool_id, role="gateway")
    with pytest.raises(ValueError):
        await readback(sessions, gateway, receipt)


async def test_execution_runtime_http_recovers_without_current_profile(sessions, tmp_path):
    from loom_execution_actuator.pool_execution_driver import PoolExecutionDriver
    from tests.integration.test_nebius_pool_execution_activation import selected
    from tests.integration.test_nebius_pool_participant_http import client

    outbox, _, proposed, app, token = await selected(sessions, tmp_path)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app)) as http:
        management = client(http, token)
        active = await PoolExecutionDriver(outbox=outbox, management=management).advance(proposed.request.key)
        app.state.pool_profiles = None
        runtime = await management.execution_runtime(active.action)
        assert runtime.receipt == active.activated and runtime.execution_unit_key == active.request.execution.execution_unit_key
        assert runtime.deadline_at == active.request.deadline_at
