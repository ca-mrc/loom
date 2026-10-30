"""Native builds and executions use one management transaction and priority queue."""
from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from sqlalchemy import func, select, update

from loom.db.nebius_pool_schema import NebiusPoolBinding, NebiusPoolObservation, NebiusPoolRequest
from loom.db.schema import TaskImageMaterializationAttempt
from loom.nebius_pool_priority import PoolWorkOriginV1
from loom.nebius_pool_task_image import PoolTaskImagePrepareV1
from loom.pipeline.keys import canonical_digest
from loom_execution_capacity_collector.contracts import CapacityPlacement
from tests.execution_placement_fixtures import placement_fixture
from tests.integration.test_nebius_application_operations import applications as applications
from tests.integration.test_nebius_environment_management import (
    environment_registry as environment_registry,
)
from tests.integration.test_nebius_pool_observation_registry import capture_scope
from tests.integration.test_nebius_pool_registry import (
    BUILD_POOL_CPU_WITH_ONE_BUILD_FREE,
    BUILD_POOL_NODE_CPU,
    BUILD_POOL_NODE_MEMORY,
    BUILD_POOL_NODE_STORAGE,
    prepare,
    publish_placement,
    setup,
)
from tests.integration.test_nebius_pool_registry import sessions as sessions
from tests.support.nebius_platform import platform_inputs as platform_inputs
from tests.unit.test_nebius_pool_task_image_render import build_inputs


async def mixed_setup(sessions, *, build_concurrency_limit=2, **kwargs):
    from loom_service.pool_management.auth import _resolve_hash
    from loom_service.pool_management.capacity import _placement
    from loom_service.pool_management.registry import PoolProfiles

    kwargs.setdefault("node_cpu", BUILD_POOL_NODE_CPU)
    kwargs.setdefault("node_memory", BUILD_POOL_NODE_MEMORY)
    kwargs.setdefault("node_storage", BUILD_POOL_NODE_STORAGE)
    participants, principals, requests, execution_profiles, observer = await setup(sessions, **kwargs)
    if build_concurrency_limit != 2:
        async with sessions.begin() as session:
            pool = await session.get(NebiusPoolBinding, participants[0].pool_id)
            binding = pool.binding_json | {"admission": pool.binding_json["admission"] | {
                "build_concurrency_limit": build_concurrency_limit}}
            pool.binding_json = binding
            pool.binding_sha256 = canonical_digest(binding).removeprefix("sha256:")
            pool.policy_revision += 1
            observation = await session.scalar(select(NebiusPoolObservation).where(
                NebiusPoolObservation.pool_id == pool.pool_id))
            placement, _, _ = _placement(observation.observation_json)
        async with sessions() as session:
            principals = [await _resolve_hash(session, principal.token_hash, locked=False) for principal in principals]
            observer = await _resolve_hash(session, observer.token_hash, locked=False)
        await publish_placement(sessions, observer, placement)
    build_profiles = {}
    builds = []
    for participant, execution in zip(participants, requests, strict=True):
        _, body, profile = build_inputs()
        execution_profile = execution_profiles[participant.targets[0].profile_id]
        target = replace(profile.target, namespace=participant.build_namespace.name,
            node_selector=execution_profile.runtime.node_selector)
        build_profiles[participant.targets[0].profile_id] = replace(profile,
            profile_id=participant.targets[0].profile_id, target=target,
            settings=profile.settings.model_copy(update={"namespace": participant.build_namespace.name}))
        body.update(pool_id=participant.pool_id, key=body["key"] | {"participant_id": participant.participant_id},
                    origin=execution.origin.model_dump(mode="json"))
        builds.append(PoolTaskImagePrepareV1.model_validate(body))
    return participants, principals, requests, builds, PoolProfiles(execution_profiles, build_profiles), observer


async def prepare_build(sessions, principal, request, profiles):
    from loom_service.pool_management.registry import prepare_task_image

    async with sessions.begin() as session:
        return await prepare_task_image(session, principal, request, profiles=profiles)


@pytest.mark.parametrize("build_concurrency_limit", [2, None])
async def test_concurrent_builds_and_execution_cannot_spend_the_same_capacity(sessions, build_concurrency_limit):
    _, principals, executions, builds, profiles, _ = await mixed_setup(
        sessions, build_concurrency_limit=build_concurrency_limit)
    results = await asyncio.wait_for(asyncio.gather(
        prepare(sessions, principals[0], executions[0], profiles),
        prepare_build(sessions, principals[0], builds[0], profiles),
        prepare_build(sessions, principals[1], builds[1], profiles),
    ), timeout=15)
    assert sorted(row.phase for row in results) == ["reserved", "reserved", "waiting"]
    async with sessions() as session:
        rows = (await session.scalars(select(NebiusPoolRequest))).all()
        assert sum(row.cpu_millis for row in rows if row.phase == "reserved") <= BUILD_POOL_NODE_CPU
        assert await session.scalar(select(func.count()).select_from(TaskImageMaterializationAttempt)) == 0


@pytest.mark.parametrize("build_concurrency_limit", [2, None])
async def test_build_concurrency_is_pool_wide_and_does_not_reserve_idle_execution_share(sessions, build_concurrency_limit):
    _, principals, executions, builds, profiles, _ = await mixed_setup(
        sessions, max_nodes=3, build_concurrency_limit=build_concurrency_limit)
    for index in range(2):
        assert (await prepare_build(sessions, principals[index], builds[index], profiles)).phase == "reserved"
    third = builds[0].model_copy(update={"key": builds[0].key.model_copy(update={"local_work_id": uuid4()})})
    result = await prepare_build(sessions, principals[0], third, profiles)
    if build_concurrency_limit is None:
        assert result.phase == "reserved"
    else:
        assert result.phase == "waiting" and result.reason == "pool_build_concurrency_exceeded"
    assert (await prepare(sessions, principals[0], executions[0], profiles)).phase == "reserved"


@pytest.mark.parametrize("first_kind", ["build", "execution"])
async def test_resource_only_mode_charges_each_workload_reservation_against_the_other(sessions, first_kind):
    _, principals, executions, builds, profiles, _ = await mixed_setup(
        sessions, occupied_cpu=BUILD_POOL_CPU_WITH_ONE_BUILD_FREE, max_nodes=2, quota_nodes=1,
        build_concurrency_limit=None)
    # One node has 4 vCPU free: either a build or a 1.5 vCPU execution fits
    # alone, and the two reservations cannot both be granted.
    first, second = ((prepare_build, builds[0]), (prepare, executions[1])) if first_kind == "build" else (
        (prepare, executions[0]), (prepare_build, builds[1]))
    assert (await first[0](sessions, principals[0], first[1], profiles)).phase == "reserved"
    waiting = await second[0](sessions, principals[1], second[1], profiles)
    assert waiting.phase == "waiting" and waiting.reason == "execution_capacity_provider_quota_nodes_exceeded"
    async with sessions() as session:
        reserved = (await session.scalars(select(NebiusPoolRequest).where(NebiusPoolRequest.phase == "reserved"))).all()
        assert len(reserved) == 1
        assert reserved[0].cpu_millis == (4000 if first_kind == "build" else 1500)


@pytest.mark.parametrize("build_concurrency_limit", [2, None])
@pytest.mark.parametrize("classes,winner", [(("development", "development"), "build"),
                                           (("development", "production"), "execution"),
                                           (("development", "staging"), "execution"),
                                           (("staging", "production"), "execution"),
                                           (("production", "staging"), "build")])
async def test_mixed_waiting_work_uses_class_then_age_not_workload_kind(sessions, classes, winner, build_concurrency_limit):
    _, principals, executions, builds, profiles, observer = await mixed_setup(
        sessions, occupied_cpu=BUILD_POOL_NODE_CPU, environment_classes=classes,
        build_concurrency_limit=build_concurrency_limit)
    assert (await prepare_build(sessions, principals[0], builds[0], profiles)).phase == "waiting"
    assert (await prepare(sessions, principals[1], executions[1], profiles)).phase == "waiting"
    await publish_placement(sessions, observer, CapacityPlacement.model_validate(placement_fixture(
        target_id="pool-test", node_cpu=BUILD_POOL_NODE_CPU, node_memory=BUILD_POOL_NODE_MEMORY,
        node_storage=BUILD_POOL_NODE_STORAGE, requested_cpu=BUILD_POOL_CPU_WITH_ONE_BUILD_FREE,
        quota_nodes=1)))
    if winner == "build":
        assert (await prepare(sessions, principals[1], executions[1], profiles)).phase == "waiting"
        assert (await prepare_build(sessions, principals[0], builds[0], profiles)).phase == "reserved"
    else:
        assert (await prepare_build(sessions, principals[0], builds[0], profiles)).phase == "waiting"
        assert (await prepare(sessions, principals[1], executions[1], profiles)).phase == "reserved"


async def test_native_capture_uses_actual_attempt_epoch_not_selection_generation(sessions):
    from loom_service.pool_management.task_images import prepare_pool_task_image

    participants, principals, _, builds, profiles, observer = await mixed_setup(sessions)
    first = await prepare_build(sessions, principals[0], builds[0], profiles)
    replay = await prepare_build(sessions, principals[0], builds[0], profiles)
    assert first == replay
    assert first.request_key.generation == 7
    rendered = prepare_pool_task_image(builds[0], participant=participants[0],
        profile=profiles.task_images[participants[0].targets[0].profile_id],
        reservation_id=first.reservation_id, now=datetime.now(UTC))
    plan = {"job": rendered.job, "configmap": rendered.configmap}
    async with sessions.begin() as session:
        await session.execute(update(NebiusPoolRequest).where(NebiusPoolRequest.request_id == first.reservation_id).values(
            phase="create_intent", plan_json=plan, plan_sha256=canonical_digest(plan).removeprefix("sha256:")))
        await session.execute(update(NebiusPoolRequest).where(NebiusPoolRequest.request_id == first.reservation_id).values(
            phase="observed", job_uid=uuid4()))
    capture = await capture_scope(sessions, observer)
    job, = capture.scope.jobs
    assert job.generation == 3
    assert job.lease_id == "task-image:" + str(builds[0].key.local_work_id)
    assert job.reservation_id == first.reservation_id


@pytest.mark.parametrize("personal_kind", ["build", "execution"])
async def test_personal_and_shared_work_use_distinct_priority_in_the_same_participant(sessions, applications, personal_kind):
    from loom_service.pool_management.registry import PoolAdmissionError

    registry, _, (alice, _), prepare_application, _, _ = applications
    plan = prepare_application()
    await registry.create(principal=alice, idempotency_key="mixed-personal-origin", **plan)
    app = plan["prepared"].registration
    participants, principals, executions, builds, profiles, observer = await mixed_setup(sessions,
        occupied_cpu=BUILD_POOL_NODE_CPU, data_environment_id=app.data_environment_id,
        cluster_id=app.cluster_id)
    origin = PoolWorkOriginV1.model_validate({"kind": "application", "submission_id": uuid4(),
        "data_environment_id": app.data_environment_id, "application": {
            "application_id": app.application_id, "incarnation": app.incarnation,
            "deployment_generation": 1, "release_id": app.release_id, "source_digest": plan["release"].source_digest}})
    personal_body = (builds[0] if personal_kind == "build" else executions[0]).model_copy(update={"origin": origin})
    shared_body = executions[0] if personal_kind == "build" else builds[0]
    personal_prepare = prepare_build if personal_kind == "build" else prepare
    shared_prepare = prepare if personal_kind == "build" else prepare_build
    assert (await personal_prepare(sessions, principals[0], personal_body, profiles)).phase == "waiting"
    assert (await shared_prepare(sessions, principals[0], shared_body, profiles)).phase == "waiting"
    # Changing an old personal selection to a shared origin is a conflict, not
    # legitimate promotion through replay or a common development credential.
    spoofed = personal_body.model_copy(update={"origin": executions[0].origin})
    with pytest.raises(PoolAdmissionError, match="pool_request_conflict"):
        await personal_prepare(sessions, principals[0], spoofed, profiles)
    await publish_placement(sessions, observer, CapacityPlacement.model_validate(placement_fixture(
        target_id="pool-test", node_cpu=BUILD_POOL_NODE_CPU, node_memory=BUILD_POOL_NODE_MEMORY,
        node_storage=BUILD_POOL_NODE_STORAGE, requested_cpu=BUILD_POOL_CPU_WITH_ONE_BUILD_FREE,
        quota_nodes=1)))
    assert (await personal_prepare(sessions, principals[0], personal_body, profiles)).phase == "waiting"
    shared = await shared_prepare(sessions, principals[0], shared_body, profiles)
    assert shared.phase == "reserved"
    assert (await personal_prepare(sessions, principals[0], personal_body, profiles)).phase == "waiting"
    async with sessions.begin() as session:
        rows = (await session.scalars(select(NebiusPoolRequest))).all()
        assert {row.participant_id for row in rows} == {participants[0].participant_id}
        assert sorted(row.priority for row in rows) == [2, 3]
        assert await session.scalar(select(func.count()).select_from(TaskImageMaterializationAttempt)) == 0
        await session.execute(update(NebiusPoolRequest).where(NebiusPoolRequest.request_id == shared.reservation_id).values(
            phase="cancelled_unstarted"))
    # Once the only higher-priority grant is gone, personal work uses all free
    # capacity. No idle shared-dev share remains reserved.
    assert (await personal_prepare(sessions, principals[0], personal_body, profiles)).phase == "reserved"
