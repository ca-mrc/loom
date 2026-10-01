"""Native result readers consume retained runtime identity, never current profiles."""
from __future__ import annotations

from uuid import uuid4

import httpx
import pytest
from sqlalchemy import select

from loom.db.nebius_pool_schema import NebiusPoolRequest
from tests.integration.test_nebius_pool_control import action
from tests.integration.test_nebius_pool_gateway_journal import setup
from tests.integration.test_nebius_pool_observation_registry import sessions as sessions


async def readback(sessions, owner, receipt):
    from loom.nebius_pool_contract import PoolRequestActionV1
    from loom_service.pool_management.native_runtime import native_build_runtime

    reference = PoolRequestActionV1(pool_id=receipt.pool_id, request_key=receipt.request_key,
        admission_epoch=receipt.admission_epoch, request_sha256=receipt.request_sha256)
    async with sessions.begin() as session:
        return await native_build_runtime(session, owner, reference)


async def test_native_runtime_identity_is_retained_without_manifest_or_profile_authority(sessions):
    _, _, receipt, owner = await setup(sessions, build=True)
    runtime = await readback(sessions, owner, receipt)
    assert runtime.receipt == receipt
    assert runtime.job_name == f"loom-pool-{receipt.reservation_id.hex}"
    assert runtime.lease_epoch == 3  # Native epoch is NOT selection generation 1.
    assert runtime.registry_repository == "registry.example/tasks"
    assert runtime.job_effect_id is None
    assert set(runtime.model_dump()) == {"schema_version", "receipt", "target_id", "namespace",
        "job_name", "lease_epoch", "deadline_at", "registry_repository", "job_effect_id"}
    async with sessions() as session:
        row = await session.get(NebiusPoolRequest, receipt.reservation_id)
        assert runtime.namespace.uid == row.namespace_uid and runtime.deadline_at == row.deadline_at
        assert runtime.namespace.name == row.plan_json["job"]["metadata"]["namespace"]


async def test_native_readback_binds_observed_job_to_its_retained_effect(sessions):
    journal, gateway, receipt, owner = await setup(sessions, build=True)
    for kind in ("ConfigMap", "Job"):
        effect = await journal.prepare_create(gateway, receipt.reservation_id, kind=kind)
        await journal.dispatch_create(gateway, effect.effect_id)
        observed = await journal.observe_create(gateway, effect.effect_id, uid=uuid4(), resource_version="1")
    runtime = await readback(sessions, owner, receipt)
    assert runtime.job_effect_id == observed.effect_id
    assert runtime.receipt.job_uid == observed.observed_uid and runtime.receipt.phase == "observed"


@pytest.mark.parametrize("build,activate", [(False, True), (True, False)])
async def test_non_native_or_unactivated_request_has_no_native_runtime(sessions, build, activate):
    _, _, receipt, owner = await setup(sessions, build=build, activate=activate)
    with pytest.raises(ValueError):
        await readback(sessions, owner, receipt)


async def test_native_readback_cannot_cross_participant_boundary(sessions):
    from tests.integration.test_nebius_pool_registry import machine

    _, _, receipt, _ = await setup(sessions, build=True)
    other = await machine(sessions, receipt.pool_id, role="gateway")
    with pytest.raises(ValueError):
        await readback(sessions, other, receipt)


async def test_real_native_runtime_http_uses_frozen_plan_even_without_current_profiles(sessions, tmp_path):
    from tests.integration.test_nebius_pool_participant_http import client
    from tests.integration.test_nebius_pool_participant_http import setup as http_setup

    app, _, token, _, _, builds = await http_setup(sessions, tmp_path)
    request = builds[0]
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app)) as http:
        management = client(http, token)
        await management.prepare(request)
        receipt = await management.activate(action(request, activation=True))
        app.state.pool_profiles = None
        runtime = await management.native_runtime(action(request))
        assert runtime.receipt == receipt and runtime.lease_epoch == request.build.expected_lease_epoch + 1
        assert runtime.registry_repository == "registry.example/tasks"
    async with sessions() as session:
        row = (await session.scalars(select(NebiusPoolRequest))).one()
        assert row.phase == "create_intent" and row.stop_json is None
