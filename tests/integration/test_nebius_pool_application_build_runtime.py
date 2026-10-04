"""Application builds use the existing gateway and never release on stop alone."""
from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import insert, update

from loom.db.nebius_application_build_schema import NebiusApplicationBuild
from loom.db.nebius_pool_schema import NebiusPoolMachineCredential, NebiusPoolRequest
from loom.db.schema import Token
from tests.integration.test_nebius_environment_management import (
    environment_registry as environment_registry,
)
from tests.integration.test_nebius_pool_application_build_admission import (
    prepare_application,
    setup_application_pool,
)
from tests.integration.test_nebius_pool_control import action, operate
from tests.integration.test_nebius_pool_native_runtime import readback
from tests.integration.test_nebius_pool_observation_registry import capture_scope
from tests.integration.test_nebius_pool_registry import machine, publish_placement
from tests.unit.test_nebius_application_image_renderer import build_inputs as build_inputs
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


async def test_application_gateway_lost_reply_native_readback_capture_and_drain_hold_one_charge(environment_registry, build_inputs):
    from loom.nebius_pool_lifecycle import PoolDrainV1, PoolStopV1
    from loom.pipeline.keys import canonical_digest
    from loom_execution_capacity_collector.pool import PoolPodClassifier
    from loom_service.pool_management.cleanup import PoolCleanupJournal
    from loom_service.pool_management.gateway_journal import PoolGatewayJournal
    from tests.integration.test_nebius_pool_stop_drain import accept_drain, accept_stop
    from tests.unit.test_nebius_pool_observation import _pod

    factory, principals, requests, profiles, observer, *_ = await setup_application_pool(environment_registry, build_inputs)
    request, owner = requests[0], principals[0]
    await prepare_application(factory, owner, request, profiles)
    receipt = await operate(factory, owner, action(request, activation=True), profiles=profiles)
    runtime = await readback(factory, owner, receipt)
    assert runtime.lease_epoch == 1 and runtime.registry_repository == request.build.registry_repository
    assert runtime.job_effect_id is None
    gateway = await machine(factory, receipt.pool_id, role="gateway")
    journal = PoolGatewayJournal(factory)
    for kind in ("ConfigMap", "Job"):
        effect = await journal.prepare_create(gateway, receipt.reservation_id, kind=kind)
        assert await journal.dispatch_create(gateway, effect.effect_id) is not None
        # A restarted gateway cannot issue another write when the reply was lost.
        assert await PoolGatewayJournal(factory).dispatch_create(gateway, effect.effect_id) is None
        effect = await journal.observe_create(gateway, effect.effect_id, uid=uuid4(), resource_version="1")
    runtime = await readback(factory, owner, receipt)
    assert runtime.job_effect_id == effect.effect_id and runtime.receipt.job_uid == effect.observed_uid
    from copy import deepcopy

    from loom_execution_actuator.pool_native_observation import qualify_native_observation

    observed_job = deepcopy(effect.document)
    observed_job["metadata"].update(uid=str(effect.observed_uid), resourceVersion="1")
    qualify_native_observation(observed_job, runtime)
    observed_job["metadata"]["labels"]["loom.build-attempt"] = "2"
    with pytest.raises(ValueError):
        qualify_native_observation(observed_job, runtime)
    capture = await capture_scope(factory, observer)
    job, = capture.scope.jobs
    assert job.workload_kind == "application_image_build" and job.generation == 1
    assert job.lease_id == "application-image:" + str(request.build.build_id)
    assert job.namespace == runtime.namespace.name and job.reservation_id == receipt.reservation_id
    pod = _pod()
    pod.metadata.namespace = job.namespace
    pod.metadata.labels = effect.document["spec"]["template"]["metadata"]["labels"]
    pod.metadata.annotations = effect.document["spec"]["template"]["metadata"]["annotations"]
    pod.metadata.owner_references[0].name = job.job_name
    pod.metadata.owner_references[0].uid = job.job_uid
    classifier = PoolPodClassifier(capture.scope)
    managed = classifier.managed(pod)
    assert managed is not None and managed.lease_id == "reservation:" + str(receipt.reservation_id)
    pod.metadata.labels["loom.build-attempt"] = "2"
    assert classifier.managed(pod) is None  # Not discounted against an unrelated grant.

    stop = PoolStopV1(action=action(request), reservation_id=receipt.reservation_id,
        plan_sha256=receipt.plan_sha256, lease_generation=1, cause="cancelled", grace_deadline_at=datetime.now(UTC))
    stopped = await accept_stop(factory, owner, stop)
    assert stopped.capacity_charged and stopped.phase == "cleanup_intent"
    cleanup = PoolCleanupJournal(journal)
    with pytest.raises(ValueError):
        await cleanup.prepare(gateway, receipt.reservation_id)
    drain = PoolDrainV1(action=stop.action, reservation_id=receipt.reservation_id, plan_sha256=receipt.plan_sha256,
        lease_generation=1, stop_sha256=canonical_digest(stop.model_dump(mode="json")).removeprefix("sha256:"),
        output_generation=1, output_state="unavailable", evidence_sha256="d" * 64)
    with pytest.raises(ValueError):
        await accept_drain(factory, owner, drain.model_copy(update={"output_generation": 2}))
    drained = await accept_drain(factory, owner, drain)
    assert drained.capacity_charged and drained.cleanup_observation_id is None
    snapshot = await cleanup.prepare(gateway, receipt.reservation_id)
    assert snapshot.has_configmap and snapshot.namespace == runtime.namespace
    async with factory() as session:
        assert (await session.get(NebiusPoolRequest, receipt.reservation_id)).phase == "cleanup_intent"


async def test_real_application_pool_client_routes_keep_frozen_identity_through_activation(environment_registry, build_inputs, tmp_path):
    from loom_service.app import create_app
    from loom_service.config import LoomServiceSettings
    from tests.integration.test_nebius_pool_participant_http import client

    factory, principals, requests, profiles, *_ = await setup_application_pool(environment_registry, build_inputs)
    principal, request = principals[0], requests[0]
    raw = "loom_pool_" + uuid4().hex + uuid4().hex
    hashed = hashlib.sha256(raw.encode()).digest()
    async with factory.begin() as session:
        await session.execute(insert(Token).values(token_hash=hashed, type="pool_machine", scopes=[],
            issued_at=datetime.now(UTC), expires_at=datetime.now(UTC) + timedelta(hours=1)))
        await session.execute(insert(NebiusPoolMachineCredential).values(token_hash=hashed,
            machine_id=principal.machine_id, credential_epoch=principal.credential_epoch))
    token = tmp_path / "participant-token"
    token.write_text(raw)
    token.chmod(0o600)
    app = create_app(LoomServiceSettings(_env_file=None, service_mode="management",
        db_url="postgresql+asyncpg://unused:unused@localhost/unused"))
    app.state.session_factory, app.state.pool_profiles = factory, profiles
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app)) as http:
        management = client(http, token)
        reserved = await management.prepare(request)
        assert reserved.phase == "reserved" and await management.prepare(request) == reserved
        activated = await management.activate(action(request, activation=True))
        assert activated.phase == "create_intent"
        app.state.pool_profiles = None
        runtime = await management.native_runtime(action(request))
        assert runtime.receipt == activated and runtime.lease_epoch == request.build.attempt
        assert await management.status(action(request)) == activated
        await management.close()


async def test_cancelled_personal_wait_does_not_protect_capacity_from_another_owner(environment_registry, build_inputs):
    from loom_execution_capacity_collector.contracts import CapacityPlacement
    from tests.execution_placement_fixtures import placement_fixture

    factory, principals, requests, profiles, observer, *_ = await setup_application_pool(
        environment_registry, build_inputs, occupied_cpu=3000)
    for request in requests:
        assert (await prepare_application(factory, principals[0], request, profiles)).phase == "waiting"
    async with factory.begin() as session:
        await session.execute(update(NebiusApplicationBuild).where(
            NebiusApplicationBuild.build_id == requests[0].build.build_id).values(desired_state="cancelled"))
    await publish_placement(factory, observer, CapacityPlacement.model_validate(placement_fixture(
        target_id="pool-test", node_cpu=3000, node_memory=8192, node_storage=32768,
        requested_cpu=2000, quota_nodes=1)))
    assert (await prepare_application(factory, principals[0], requests[1], profiles)).phase == "reserved"


async def test_cancelled_build_cannot_activate_an_already_reserved_request(environment_registry, build_inputs):
    factory, principals, requests, profiles, *_ = await setup_application_pool(environment_registry, build_inputs)
    receipt = await prepare_application(factory, principals[0], requests[0], profiles)
    async with factory.begin() as session:
        await session.execute(update(NebiusApplicationBuild).where(
            NebiusApplicationBuild.build_id == requests[0].build.build_id).values(desired_state="cancelled"))
    with pytest.raises(ValueError):
        await operate(factory, principals[0], action(requests[0], activation=True), profiles=profiles)
    async with factory() as session:
        retained = await session.get(NebiusPoolRequest, receipt.reservation_id)
        assert retained.phase == "reserved" and retained.plan_json is None
