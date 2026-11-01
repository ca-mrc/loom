"""Real PostgreSQL mixed-image queue selection, ordering and claim boundaries."""
from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest
from sqlalchemy import delete, select, text, update
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.sql.dml import Update

from loom.db.nebius_pool_outbox_schema import NebiusPoolExecutionOutbox
from loom.db.schema import (
    Batch,
    ExecutionBudgetPolicy,
    ExecutionCapacityPolicy,
    Task,
    TaskImageMaterialization,
    TeamQuota,
    Trial,
    TrialTaskImageMaterialization,
)
from loom.task_image_materialization import (
    get_trial_task_image_execution_grant,
    task_image_materialization_key,
)
from loom_control_plane import service_execution_scheduler as scheduler
from loom_execution_actuator.pool_execution_selection import _CANDIDATES, PoolExecutionSelector
from tests.integration.test_nebius_pool_execution_controller import another_trial
from tests.integration.test_nebius_pool_execution_outbox import grant, setup
from tests.integration.test_nebius_pool_observation_registry import sessions as sessions
from tests.integration.test_service_execution_leases import (
    _configure_scheduler_trial,
    _seed_ready_trial,
)
from tests.support.execution_image_admission import (
    IMAGE_ADMISSION_KEYRING,
    signed_image_admission_bundle,
)
from tests.unit.test_service_execution_materialization import _profile, _provenance, _task, _trial


def workspace_trial():
    return _trial().model_copy(update={"agent_name": "terminus-2"}).model_dump(mode="json")


async def image(session, trial_id, *, state, arch="x86_64"):
    trial = await session.get(Trial, trial_id)
    task = await session.get(Task, trial.task_id)
    materialization = TaskImageMaterialization(
        id=uuid4(), materialization_key=task_image_materialization_key(
            task_id=task.id, task_checksum=task.checksum, cpu_arch=arch),
        task_id=task.id, task_checksum=task.checksum, cpu_arch=arch,
        task_config=task.config, task_source=task.source,
        task_source_provenance=task.source_provenance, state=state,
        registry_images={"task": _profile().task_image_ref}
        if state == "ready" else {},
        ready_at=datetime.now(UTC) if state == "ready" else None,
    )
    session.add(materialization)
    session.add(TrialTaskImageMaterialization(trial_id=trial_id, materialization_id=materialization.id))
    return materialization


async def build_task(sessions, original, suffix):
    async with sessions.begin() as session:
        trial = await session.get(Trial, original)
        task = await session.get(Task, trial.task_id)
        config = _task().model_dump(mode="json")
        config["task"]["id"] = task.id + suffix
        config["environment"].pop("docker_image", None)
        config["environment"]["dockerfile"] = "Dockerfile"
        config["agent"]["name"] = "terminus-2"
        task_id = task.id + suffix
        session.add(Task(id=task_id, checksum=task.checksum, config=config, source_provenance=_provenance()))
        batch = await session.get(Batch, trial.batch_id)
        profile = _profile()
        controller_image = "registry.example/controller@sha256:" + "9" * 64
        batch.service_execution_runtime_profile = profile.model_copy(update={
            "agent_image_ref": controller_image,
            "image_admission": signed_image_admission_bundle((
                profile.task_image_ref, controller_image, profile.runtime_image_ref)),
        }).model_dump(mode="json")
        await session.execute(delete(ExecutionBudgetPolicy))
    return task_id


async def mixed_queue(sessions, *, cold_count=40):
    outbox, original, _ = await setup(sessions)
    now = datetime.now(UTC)
    cold_task = await build_task(sessions, original, "-cold")
    cold = [await another_trial(sessions, original, task_id=cold_task, config=workspace_trial(),
        submitted_at=now-timedelta(hours=1)) for _ in range(cold_count)]
    ready_task = await build_task(sessions, original, "-ready")
    ready = await another_trial(sessions, original, task_id=ready_task, config=workspace_trial(),
        submitted_at=now-timedelta(minutes=30))
    async with sessions.begin() as session:
        # One real cold image prerequisite shared by the older trial cohort.
        pending = await image(session, cold[0], state="queued")
        for trial_id in cold[1:]:
            session.add(TrialTaskImageMaterialization(trial_id=trial_id, materialization_id=pending.id))
        await image(session, ready, state="ready")
        # An unused architecture is an alternative, not an extra prerequisite.
        await image(session, ready, state="queued", arch="arm64")
    return outbox, cold, ready, original


@pytest.mark.parametrize("mode", ["local", "global"])
async def test_cold_front_larger_than_wave_cannot_hide_later_ready_work(sessions, mode):
    outbox, cold, ready, _ = await mixed_queue(sessions)
    now = datetime.now(UTC)
    if mode == "local":
        async with sessions.begin() as session:
            await session.execute(update(ExecutionCapacityPolicy).values(enabled=True))
            lease = await scheduler.reserve_next_service_execution(session,
                environment="staging", pool_id="nebius-cpu", now=now,
                image_admission_keyring=IMAGE_ADMISSION_KEYRING)
            assert lease is not None, (await session.get(Trial, ready)).scheduling_observation
            assert lease.trial_id == ready
    else:
        async with sessions() as session:
            rows = (await session.execute(_CANDIDATES, {"now": now, "pool_id": "nebius-cpu",
                "environment_id": str(outbox.participant.environment_id)})).mappings().all()
            assert rows[0]["id"] == ready
            assert not set(cold).intersection(row["id"] for row in rows)
        selected = await PoolExecutionSelector(outbox=outbox).select_next()
        assert selected is not None
        async with sessions() as session:
            assert (await session.get(NebiusPoolExecutionOutbox, selected.request.key.local_work_id)).trial_id == ready
    async with sessions() as session:
        untouched = (await session.scalars(select(Trial).where(Trial.id.in_(cold)))).all()
        assert len(untouched) == 40
        assert all(t.state == "queued" and t.attempt_count == 0 and t.next_attempt_at is None for t in untouched)


async def test_ready_queue_keeps_priority_time_id_order_and_skip_locked(sessions):
    _, _, ready, prebuilt = await mixed_queue(sessions)
    now = datetime.now(UTC)
    # Ready and no-build-required candidates share the original queue ordering.
    async with sessions.begin() as session:
        first = await session.get(Trial, ready)
        first.submit_priority = 500
    higher = await another_trial(sessions, prebuilt, submit_priority=1000,
        submitted_at=now)
    early = await another_trial(sessions, prebuilt, submitted_at=now-timedelta(minutes=2),
        id=UUID(int=1))
    late = await another_trial(sessions, prebuilt, submitted_at=now-timedelta(minutes=2),
        id=UUID(int=2))
    params = {"now": now, "pool_id": "nebius-cpu"}
    async with sessions() as owner:
        row = (await owner.execute(scheduler._NEXT_SERVICE_TRIAL, params)).mappings().one()
        assert row["id"] == higher
        # A concurrent worker skips the held ready row, not the cold frontier.
        async with sessions() as contender:
            second = (await contender.execute(scheduler._NEXT_SERVICE_TRIAL, params)).mappings().one()
            assert second["id"] == ready
    async with sessions.begin() as session:
        for trial_id in (higher, ready, prebuilt):
            trial = await session.get(Trial, trial_id)
            trial.cancellation_requested_at = now
    async with sessions.begin() as session:
        assert (await session.execute(scheduler._NEXT_SERVICE_TRIAL, params)).mappings().one()["id"] == early
        (await session.get(Trial, early)).cancellation_requested_at = now
        await session.flush()
        assert (await session.execute(scheduler._NEXT_SERVICE_TRIAL, params)).mappings().one()["id"] == late


async def test_executable_filter_preserves_team_fair_share_before_priority(sessions):
    outbox, busy_id, _ = await setup(sessions)
    now = datetime.now(UTC)
    async with sessions.begin() as session:
        busy = await session.get(Trial, busy_id)
        busy.submit_priority = 999
        quota = await session.get(TeamQuota, busy.team_id)
        quota.in_flight_count, quota.fair_share_weight = 2, 4
        other, _ = await _seed_ready_trial(session, now=now, pool_origin={
            **busy.pool_origin, "submission_id": str(uuid4())})
        await _configure_scheduler_trial(session, trial_id=other, now=now)
    async with sessions() as session:
        params = {"now": now, "pool_id": "nebius-cpu"}
        assert (await session.execute(scheduler._NEXT_SERVICE_TRIAL, params)).mappings().one()["id"] == other
        rows = (await session.execute(_CANDIDATES, {**params,
            "environment_id": str(outbox.participant.environment_id)})).mappings().all()
        assert [row["id"] for row in rows] == [other, busy_id]


@pytest.mark.parametrize("mode", ["local", "global"])
async def test_no_build_required_is_not_blocked_by_unused_arm_prerequisite(sessions, mode):
    outbox, original, _ = await setup(sessions)
    async with sessions.begin() as session:
        await image(session, original, state="queued", arch="arm64")
    if mode == "local":
        async with sessions.begin() as session:
            await session.execute(update(ExecutionCapacityPolicy).values(enabled=True))
            lease = await scheduler.reserve_next_service_execution(session,
                environment="staging", pool_id="nebius-cpu", now=datetime.now(UTC),
                image_admission_keyring=IMAGE_ADMISSION_KEYRING)
            assert lease is not None and lease.trial_id == original
    else:
        proposed = await PoolExecutionSelector(outbox=outbox).select_next()
        assert proposed is not None
        attached = await outbox.accept_grant(proposed.request.key, grant(proposed))
        assert attached.phase == "attached" and attached.lease_id is not None
        async with sessions() as session:
            assert (await session.get(NebiusPoolExecutionOutbox, proposed.request.key.local_work_id)).trial_id == original


@pytest.mark.parametrize("case", ["requested-arch-pending", "build-required-arm-only"])
async def test_unused_arch_exception_never_downgrades_a_required_image_grant(sessions, case):
    _, original, _ = await setup(sessions)
    trial_id = original
    if case == "build-required-arm-only":
        task_id = await build_task(sessions, original, "-required")
        trial_id = await another_trial(sessions, original, task_id=task_id, config=workspace_trial())
    async with sessions.begin() as session:
        await image(session, trial_id, state="queued",
            arch="arm64" if case == "build-required-arm-only" else "x86_64")
    async with sessions() as session:
        with pytest.raises(RuntimeError, match="no longer has a ready task-image"):
            await get_trial_task_image_execution_grant(session, trial_id=trial_id, cpu_arches=["x86_64"])


@pytest.mark.parametrize("damage", ["pending", "failed", "cancel"])
async def test_image_readiness_or_cancellation_change_before_global_grant_never_claims(sessions, damage):
    outbox, _, ready, _ = await mixed_queue(sessions, cold_count=1)
    proposed = await PoolExecutionSelector(outbox=outbox).select_next()
    assert proposed is not None
    async with sessions.begin() as session:
        if damage == "cancel":
            (await session.get(Trial, ready)).cancellation_requested_at = datetime.now(UTC)
        else:
            materialization = await session.scalar(select(TaskImageMaterialization)
                .join(TrialTaskImageMaterialization,
                    TrialTaskImageMaterialization.materialization_id == TaskImageMaterialization.id)
                .where(TrialTaskImageMaterialization.trial_id == ready,
                    TaskImageMaterialization.cpu_arch == "x86_64"))
            materialization.state = "queued" if damage == "pending" else "failed"
    attached = await outbox.accept_grant(proposed.request.key, grant(proposed))
    assert attached.phase == "cancel_pending" and attached.lease_id is None
    async with sessions() as session:
        trial = await session.get(Trial, ready)
        assert trial.attempt_count == 0 and trial.claimed_at is None


@pytest.mark.parametrize("mode", ["local", "global"])
async def test_all_failed_still_settles_but_pending_is_not_selected(sessions, mode):
    outbox, trial_id, _ = await setup(sessions)
    failed_task = await build_task(sessions, trial_id, "-failed")
    failed = await another_trial(sessions, trial_id, task_id=failed_task, submit_priority=200,
        config=workspace_trial())
    async with sessions.begin() as session:
        await image(session, failed, state="failed")
    if mode == "local":
        async with sessions.begin() as session:
            await session.execute(update(ExecutionCapacityPolicy).values(enabled=True))
            # Settlement completes the current wave without granting an
            # attempt. The next wave can reserve the ordinary prebuilt trial.
            assert await scheduler.reserve_next_service_execution(session,
                environment="staging", pool_id="nebius-cpu", now=datetime.now(UTC),
                image_admission_keyring=IMAGE_ADMISSION_KEYRING) is None
            lease = await scheduler.reserve_next_service_execution(session,
                environment="staging", pool_id="nebius-cpu", now=datetime.now(UTC),
                image_admission_keyring=IMAGE_ADMISSION_KEYRING)
            assert lease is not None, (await session.get(Trial, trial_id)).scheduling_observation
            assert lease.trial_id == trial_id
    else:
        assert await PoolExecutionSelector(outbox=outbox).select_next() is not None
    async with sessions() as session:
        trial = await session.get(Trial, failed)
        assert trial.state == "failed" and trial.failure_reason == "task_image_build_failed"
        assert trial.attempt_count == 0 and trial.claimed_at is None


@pytest.mark.parametrize("binding", ["missing", "arm-only", "foreign-ready", "old-pending"])
async def test_build_required_without_ready_matching_trial_prerequisite_is_not_executable(sessions, binding):
    outbox, original, _ = await setup(sessions)
    task_id = await build_task(sessions, original, "-missing")
    cold = await another_trial(sessions, original, task_id=task_id, submit_priority=100,
        config=workspace_trial())
    async with sessions.begin() as session:
        if binding in {"arm-only", "old-pending"}:
            if binding == "old-pending":
                (await session.get(Task, task_id)).checksum = "7" * 64
            await image(session, cold, state="queued", arch="arm64" if binding == "arm-only" else "x86_64")
            if binding == "old-pending":
                (await session.get(Task, task_id)).checksum = "8" * 64
        elif binding == "foreign-ready":
            foreign = await image(session, original, state="ready")
            session.add(TrialTaskImageMaterialization(trial_id=cold, materialization_id=foreign.id))
    async with sessions() as session:
        params = {"now": datetime.now(UTC), "pool_id": "nebius-cpu"}
        assert (await session.execute(scheduler._NEXT_SERVICE_TRIAL, params)).mappings().one()["id"] == original
        rows = (await session.execute(_CANDIDATES, {**params,
            "environment_id": str(outbox.participant.environment_id)})).mappings().all()
        assert [row["id"] for row in rows] == [original]


@pytest.mark.parametrize("arch", ["x86_64", "arm64"])
async def test_prebuilt_with_foreign_ready_image_binding_is_not_executable(sessions, arch):
    outbox, original, _ = await setup(sessions)
    following = await another_trial(sessions, original)
    task_id = await build_task(sessions, original, "-foreign")
    foreign_trial = await another_trial(sessions, original, task_id=task_id, config=workspace_trial())
    async with sessions.begin() as session:
        materialization = await image(session, foreign_trial, state="ready", arch=arch)
        session.add(TrialTaskImageMaterialization(trial_id=original, materialization_id=materialization.id))
        (await session.get(Trial, original)).submit_priority = 200
        (await session.get(Trial, foreign_trial)).cancellation_requested_at = datetime.now(UTC)
    async with sessions() as session:
        # The final grant already rejects this inconsistent binding. Selection
        # must also exclude it rather than spend the bounded wave discovering it.
        with pytest.raises(RuntimeError, match="no longer has a ready task-image"):
            await get_trial_task_image_execution_grant(session, trial_id=original, cpu_arches=["x86_64"])
        params = {"now": datetime.now(UTC), "pool_id": "nebius-cpu"}
        assert (await session.execute(scheduler._NEXT_SERVICE_TRIAL, params)).mappings().one()["id"] == following
        rows = (await session.execute(_CANDIDATES, {**params,
            "environment_id": str(outbox.participant.environment_id)})).mappings().all()
        assert [row["id"] for row in rows] == [following]


async def test_task_edit_after_local_selection_cannot_claim_the_stale_prebuilt_plan(sessions):
    _, original, _ = await setup(sessions)
    now = datetime.now(UTC)
    async with sessions.begin() as session:
        await session.execute(update(ExecutionCapacityPolicy).values(enabled=True))
        task_id = (await session.get(Trial, original)).task_id
    selected, resume = asyncio.Event(), asyncio.Event()
    class PauseAfterSelection(AsyncSession):
        async def execute(self, statement, *args, **kwargs):
            result = await super().execute(statement, *args, **kwargs)
            if statement is scheduler._NEXT_SERVICE_TRIAL:
                selected.set()
                await resume.wait()
            return result

    observed = async_sessionmaker(sessions.kw["bind"], class_=PauseAfterSelection, expire_on_commit=False)

    async def reserve():
        async with observed.begin() as session:
            return await scheduler.reserve_next_service_execution(session,
                environment="staging", pool_id="nebius-cpu", now=now,
                image_admission_keyring=IMAGE_ADMISSION_KEYRING)

    reservation = asyncio.create_task(reserve())
    try:
        await asyncio.wait_for(selected.wait(), timeout=10)
        config = _task().model_dump(mode="json")
        config["task"]["id"] = task_id
        config["environment"].update(docker_image=None, dockerfile="Dockerfile")
        # A real second transaction commits the new build-required catalog
        # source while the selected Trial row remains locked by the scheduler.
        async with sessions.begin() as editor:
            await editor.execute(text("SET LOCAL lock_timeout='1s'"))
            await editor.execute(update(Task).where(Task.id == task_id).values(config=config, checksum="8" * 64))
        resume.set()
        admitted = await asyncio.wait_for(reservation, timeout=10)
        assert admitted is None, "Task edit committed, but local admission claimed the stale prebuilt plan"
        async with sessions() as session:
            trial = await session.get(Trial, original)
            assert trial.state == "queued" and trial.attempt_count == 0 and trial.claimed_at is None
    finally:
        resume.set()
        if not reservation.done():
            await asyncio.wait_for(reservation, timeout=10)


async def test_source_readers_share_task_and_batch_locks_but_fence_concurrent_edits(sessions):
    _, original, _ = await setup(sessions)
    second = await another_trial(sessions, original)
    third = await another_trial(sessions, original)
    now = datetime.now(UTC)
    async with sessions.begin() as session:
        (await session.get(Trial, original)).submit_priority = 300
        (await session.get(Trial, second)).submit_priority = 200
        task_id = (await session.get(Trial, original)).task_id
        batch_id = (await session.get(Trial, original)).batch_id
    params = {"now": now, "pool_id": "nebius-cpu"}
    async with sessions.begin() as reader:
        # Exact-source qualification is the same SQL used by local compilation
        # and global proposals. It must allow other readers of a shared source.
        row = (await reader.execute(scheduler._SERVICE_TRIAL_BY_ID,
            {**params, "trial_id": original})).mappings().one()
        async with sessions.begin() as contender:
            await contender.execute(text("SET LOCAL lock_timeout='250ms'"))
            candidate = (await contender.execute(scheduler._NEXT_SERVICE_TRIAL, params)).mappings().one()
            assert candidate["id"] == second
            other = (await contender.execute(scheduler._SERVICE_TRIAL_BY_ID,
                {**params, "trial_id": third})).mappings().one()
            assert other["task_id"] == row["task_id"]
        # Normal non-key UPDATEs, not just FOR UPDATE probes, are fenced until
        # the compilation/claim transaction releases its source read locks.
        for statement in (
            update(Task).where(Task.id == task_id).values(checksum="7" * 64),
            update(Batch).where(Batch.id == batch_id).values(service_execution_runtime_profile={"changed": True}),
        ):
            with pytest.raises(DBAPIError, match="lock timeout"):
                async with sessions.begin() as editor:
                    await editor.execute(text("SET LOCAL lock_timeout='250ms'"))
                    await editor.execute(statement)


async def test_local_configuration_failure_keeps_source_locks_through_terminal_write(sessions):
    _, original, _ = await setup(sessions)
    async with sessions.begin() as session:
        trial = await session.get(Trial, original)
        task = await session.get(Task, trial.task_id)
        task_id, batch_id = task.id, trial.batch_id
    observed = []

    class InspectFailureSession(AsyncSession):
        async def execute(self, statement, *args, **kwargs):
            if isinstance(statement, Update) and statement.compile().params.get("failure_reason") == (
                "service_execution_configuration_invalid"
            ):
                for model, identity in ((Task, task_id), (Batch, batch_id)):
                    with pytest.raises(DBAPIError, match="could not obtain lock"):
                        async with sessions.begin() as editor:
                            await editor.get(model, identity, with_for_update={"nowait": True})
                observed.append(True)
            return await super().execute(statement, *args, **kwargs)

    inspected = async_sessionmaker(sessions.kw["bind"], class_=InspectFailureSession, expire_on_commit=False)
    async with inspected.begin() as session:
        assert await scheduler.reserve_next_service_execution(session,
            environment="staging", pool_id="nebius-cpu", now=datetime.now(UTC),
            image_admission_keyring=IMAGE_ADMISSION_KEYRING, maximum_deadline_seconds=60) is None
    assert observed == [True]
    async with sessions() as session:
        trial = await session.get(Trial, original)
        assert trial.state == "failed" and trial.attempt_count == 0 and trial.claimed_at is None


@pytest.mark.parametrize("environment", ["environment", "verifier"])
@pytest.mark.parametrize("component", ["dockerfile", "sidecar", "compose"])
async def test_missing_build_detection_covers_each_persisted_component(sessions, environment, component):
    outbox, original, _ = await setup(sessions)
    task_id = await build_task(sessions, original, "-component")
    await another_trial(sessions, original, task_id=task_id, submit_priority=100,
        config=workspace_trial())
    async with sessions.begin() as session:
        task = await session.get(Task, task_id)
        config = dict(task.config)
        config["environment"] = dict(config["environment"])
        config["environment"].pop("dockerfile")
        selected = config["environment"]
        if environment == "verifier":
            config["verifier"] = {**config["verifier"], "environment": {"os": "linux"}}
            selected = config["verifier"]["environment"]
        if component == "dockerfile":
            selected["dockerfile"] = "Dockerfile"
        elif component == "sidecar":
            selected["sidecars"] = [{"dockerfile": "sidecar/Dockerfile"}]
        else:
            selected["native_compose"] = {"services": [{"name": "worker", "build": {"dockerfile": "Dockerfile"}}]}
        task.config = config
    async with sessions() as session:
        rows = (await session.execute(_CANDIDATES, {"now": datetime.now(UTC), "pool_id": "nebius-cpu",
            "environment_id": str(outbox.participant.environment_id)})).mappings().all()
        assert [row["id"] for row in rows] == [original]
