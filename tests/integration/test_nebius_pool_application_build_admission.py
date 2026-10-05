"""Real owner build history participates in the same global admission ledger."""
from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import timedelta
from uuid import uuid4

import pytest
from sqlalchemy import select, update

from loom.application_source_upload import ApplicationSourceUploadBindingV1
from loom.db.nebius_application_build_schema import (
    NebiusApplicationBuild,
)
from loom.db.nebius_pool_schema import NebiusPoolRequest
from loom.nebius_pool_application_image import PoolApplicationImagePrepareV1
from loom_execution_capacity_collector.contracts import CapacityPlacement
from loom_service.application_management.build_dispatch import ApplicationBuildDispatch
from loom_service.application_management.source_registry import ApplicationSourceRegistry
from tests.execution_placement_fixtures import placement_fixture
from tests.integration.test_nebius_application_build_registry import build_registry, verified
from tests.integration.test_nebius_environment_management import (
    environment_registry as environment_registry,
)
from tests.integration.test_nebius_pool_build_admission import mixed_setup, prepare_build
from tests.integration.test_nebius_pool_control import action, operate
from tests.integration.test_nebius_pool_registry import machine, prepare, publish_placement
from tests.unit.test_nebius_application_image_renderer import build_inputs as build_inputs
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


async def setup_application_pool(environment_registry, build_inputs, **kwargs):
    from loom_service.pool_management.application_images import PoolApplicationImageProfile

    _, factory, owners, _ = environment_registry
    participants, principals, executions, tasks, profiles, observer = await mixed_setup(factory,
        workload_kinds=("trial", "verifier", "task_image_build", "application_image_build"), **kwargs)
    participant = participants[0]
    principals[0] = await machine(factory, participant.pool_id, participant.participant_id,
        workload_scope="application_builder")
    claim = build_inputs[0]
    sources = ApplicationSourceRegistry(factory, binding=ApplicationSourceUploadBindingV1(
        installation_id=participant.installation_id, data_environment_id=participant.environment_id,
        cluster_id=kwargs.get("cluster_id", "cluster-1"), source_bucket=claim.source_bucket))
    registry = build_registry(factory, sources, claim, pool_id=participant.pool_id,
        participant_id=participant.participant_id, profile_id=participant.targets[0].profile_id,
        target_id=participant.targets[0].target_id, admission_epoch=participant.admission_epoch,
        participant_revision=participant.binding_revision)
    task_profile = profiles.task_images[participant.targets[0].profile_id]
    app_profile = PoolApplicationImageProfile(profile_id=registry.binding.profile_id, recipe=claim.recipe,
        target=task_profile.target, settings=task_profile.settings.model_copy(update={
            "service_image": claim.recipe.trusted_image_ref, "source_bucket": claim.source_bucket,
            "storage_endpoint": claim.storage_endpoint, "storage_region": claim.storage_region,
            "registry_repository": claim.registry_repository, "cache_bucket": None, "cache_secret_name": None}))
    profiles = replace(profiles, application_images={app_profile.profile_id: app_profile})
    requests = []
    for owner in owners:
        source = await verified(sources, owner)
        build = await registry.create(principal=owner, upload_id=source.upload_id, idempotency_key="build")
        requests.append(await ApplicationBuildDispatch(registry, request_lifetime_seconds=600).freeze(build.build_id, attempt=1))
    return factory, principals, requests, profiles, observer, executions, tasks, registry


async def prepare_application(factory, principal, request, profiles):
    from loom_service.pool_management.registry import prepare_application_image

    async with factory.begin() as session:
        return await prepare_application_image(session, principal, request, profiles=profiles)


@pytest.mark.parametrize("damage", ["deadline", "unfrozen"])
async def test_admission_requires_the_exact_committed_dispatch_request(environment_registry, build_inputs, damage):
    from loom_service.pool_management.registry import PoolAdmissionError

    factory, principals, apps, profiles, _, _, _, registry = await setup_application_pool(environment_registry, build_inputs)
    request = apps[0]
    if damage == "deadline":
        request = request.model_copy(update={"deadline_at": request.deadline_at + timedelta(seconds=1)})
    else:
        owner = environment_registry[2][0]
        build = await registry.create(principal=owner, upload_id=request.build.upload_id, idempotency_key="not-dispatched")
        request = request.model_copy(update={"key": request.key.model_copy(update={"local_work_id": build.build_id}),
            "origin": request.origin.model_copy(update={"submission_id": build.build_id}),
            "build": request.build.model_copy(update={"build_id": build.build_id})})
    with pytest.raises(PoolAdmissionError):
        await prepare_application(factory, principals[0], request, profiles)


async def test_concurrent_owner_and_task_builds_share_cap_without_reserving_idle_execution_capacity(environment_registry, build_inputs):
    factory, principals, apps, profiles, _, executions, tasks, _ = await setup_application_pool(
        environment_registry, build_inputs, max_nodes=3)
    results = await asyncio.wait_for(asyncio.gather(
        *(prepare_application(factory, principals[0], request, profiles) for request in apps),
        prepare_build(factory, principals[1], tasks[1], profiles),
    ), timeout=15)
    assert sorted(result.phase for result in results) == ["reserved", "reserved", "waiting"]
    assert (await prepare(factory, principals[1], executions[1], profiles)).phase == "reserved"
    async with factory() as session:
        rows = (await session.scalars(select(NebiusPoolRequest))).all()
        assert sorted(row.priority for row in rows if row.workload_kind == "application_image_build") == [3, 3]
        assert sum(row.phase == "reserved" for row in rows if row.workload_kind.endswith("build")) == 2


@pytest.mark.parametrize("higher_class", ["production", "staging", "development"])
async def test_personal_build_yields_to_higher_classes_then_uses_free_capacity(environment_registry, build_inputs, higher_class):
    factory, principals, apps, profiles, observer, executions, _, _ = await setup_application_pool(
        environment_registry, build_inputs, occupied_cpu=3000, environment_classes=("development", higher_class))
    assert (await prepare_application(factory, principals[0], apps[0], profiles)).phase == "waiting"
    assert (await prepare(factory, principals[1], executions[1], profiles)).phase == "waiting"
    await publish_placement(factory, observer, CapacityPlacement.model_validate(placement_fixture(
        target_id="pool-test", node_cpu=3000, node_memory=8192, node_storage=32768,
        requested_cpu=1500, quota_nodes=1)))
    assert (await prepare_application(factory, principals[0], apps[0], profiles)).phase == "waiting"
    granted = await prepare(factory, principals[1], executions[1], profiles)
    assert granted.phase == "reserved"
    await operate(factory, principals[1], action(executions[1]), operation="cancel")
    assert (await prepare_application(factory, principals[0], apps[0], profiles)).phase == "reserved"


@pytest.mark.parametrize("damage", ["owner", "upload", "source", "recipe", "attempt", "absent", "cancelled", "participant", "target"])
async def test_pool_refuses_forged_or_stale_management_build_history(environment_registry, build_inputs, damage):
    from loom_service.pool_management.registry import PoolAdmissionError

    factory, principals, apps, profiles, _, _, _, _ = await setup_application_pool(environment_registry, build_inputs)
    body = apps[0].model_dump(mode="json")
    if damage in {"owner", "upload"}:
        body["build"]["owner_user_id" if damage == "owner" else "upload_id"] = str(uuid4())
    elif damage == "source":
        body["build"]["source"]["archive_sha256"] = "e" * 64
    elif damage == "recipe":
        body["build"]["recipe"]["schema_revision"] = "0174"
    elif damage == "attempt":
        body["build"]["attempt"] = body["key"]["generation"] = 2
    elif damage == "absent":
        identity = str(uuid4())
        body["build"]["build_id"] = body["key"]["local_work_id"] = body["origin"]["submission_id"] = identity
    elif damage == "cancelled":
        async with factory.begin() as session:
            await session.execute(update(NebiusApplicationBuild).where(NebiusApplicationBuild.build_id == apps[0].build.build_id)
                .values(desired_state="cancelled"))
    elif damage == "participant":
        body["key"]["participant_id"] = str(principals[1].participant_id)
    else:
        body["target_id"] = "foreign"
    with pytest.raises(PoolAdmissionError):
        await prepare_application(factory, principals[1] if damage == "participant" else principals[0],
            PoolApplicationImagePrepareV1.model_validate(body), profiles)
    async with factory() as session:
        assert list((await session.scalars(select(NebiusPoolRequest))).all()) == []


async def test_personal_activation_retains_claim_and_replays_without_current_profiles(environment_registry, build_inputs):
    from loom_service.pool_management.registry import PoolProfiles

    factory, principals, apps, profiles, _, _, _, _ = await setup_application_pool(environment_registry, build_inputs)
    first = await prepare_application(factory, principals[0], apps[0], profiles)
    assert await prepare_application(factory, principals[0], apps[0], profiles) == first
    activated = await operate(factory, principals[0], action(apps[0], activation=True), profiles=profiles)
    assert activated.phase == "create_intent" and activated.capacity_charged
    assert await operate(factory, principals[0], action(apps[0], activation=True), profiles=PoolProfiles()) == activated
    async with factory() as session:
        row = await session.get(NebiusPoolRequest, activated.reservation_id)
        assert row.plan_json["job"]["metadata"]["labels"]["loom.application-build-id"] == str(apps[0].build.build_id)
        assert row.plan_json["configmap"] is not None
