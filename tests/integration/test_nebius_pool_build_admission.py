"""Native builds and executions use one management transaction and priority queue."""
from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from sqlalchemy import func, select, update

from loom.db.nebius_pool_schema import NebiusPoolRequest
from loom.db.schema import TaskImageMaterializationAttempt
from loom.nebius_pool_task_image import PoolTaskImagePrepareV1
from loom.pipeline.keys import canonical_digest
from loom_execution_capacity_collector.contracts import CapacityPlacement
from tests.execution_placement_fixtures import placement_fixture
from tests.integration.test_nebius_pool_observation_registry import capture_scope
from tests.integration.test_nebius_pool_registry import prepare, publish_placement, setup
from tests.integration.test_nebius_pool_registry import sessions as sessions
from tests.unit.test_nebius_pool_task_image_render import build_inputs


async def mixed_setup(sessions, **kwargs):
    from loom_service.pool_management.registry import PoolProfiles

    participants, principals, requests, execution_profiles, observer = await setup(sessions, **kwargs)
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


async def test_concurrent_builds_and_execution_cannot_spend_the_same_capacity(sessions):
    _, principals, executions, builds, profiles, _ = await mixed_setup(sessions)
    results = await asyncio.wait_for(asyncio.gather(
        prepare(sessions, principals[0], executions[0], profiles),
        prepare_build(sessions, principals[0], builds[0], profiles),
        prepare_build(sessions, principals[1], builds[1], profiles),
    ), timeout=15)
    assert sorted(row.phase for row in results) == ["reserved", "reserved", "waiting"]
    async with sessions() as session:
        rows = (await session.scalars(select(NebiusPoolRequest))).all()
        assert sum(row.cpu_millis for row in rows if row.phase == "reserved") <= 3000
        assert await session.scalar(select(func.count()).select_from(TaskImageMaterializationAttempt)) == 0


async def test_build_concurrency_is_pool_wide_and_does_not_reserve_idle_execution_share(sessions):
    _, principals, executions, builds, profiles, _ = await mixed_setup(sessions, max_nodes=3)
    for index in range(2):
        assert (await prepare_build(sessions, principals[index], builds[index], profiles)).phase == "reserved"
    third = builds[0].model_copy(update={"key": builds[0].key.model_copy(update={"local_work_id": uuid4()})})
    waiting = await prepare_build(sessions, principals[0], third, profiles)
    assert waiting.phase == "waiting" and waiting.reason == "pool_build_concurrency_exceeded"
    assert (await prepare(sessions, principals[0], executions[0], profiles)).phase == "reserved"


@pytest.mark.parametrize("classes,winner", [(("development", "development"), "build"),
                                           (("development", "production"), "execution")])
async def test_mixed_waiting_work_uses_class_then_age_not_workload_kind(sessions, classes, winner):
    _, principals, executions, builds, profiles, observer = await mixed_setup(
        sessions, occupied_cpu=3000, environment_classes=classes)
    assert (await prepare_build(sessions, principals[0], builds[0], profiles)).phase == "waiting"
    assert (await prepare(sessions, principals[1], executions[1], profiles)).phase == "waiting"
    await publish_placement(sessions, observer, CapacityPlacement.model_validate(placement_fixture(
        target_id="pool-test", node_cpu=3000, node_memory=8192, node_storage=32768,
        requested_cpu=1500, quota_nodes=1)))
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
