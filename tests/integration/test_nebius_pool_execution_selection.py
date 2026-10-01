"""Existing queued-Trial semantics must select globally without local admission."""
from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import func, select

from loom.db.nebius_pool_outbox_schema import NebiusPoolExecutionOutbox
from loom.db.schema import Task, TaskImageMaterialization, Trial, TrialTaskImageMaterialization
from tests.integration.test_nebius_pool_execution_controller import another_trial, connected
from tests.integration.test_nebius_pool_execution_outbox import assert_unclaimed, setup
from tests.integration.test_nebius_pool_observation_registry import sessions as sessions


def selector(outbox):
    from loom_execution_actuator.pool_execution_selection import PoolExecutionSelector

    return PoolExecutionSelector(outbox=outbox)


async def test_concurrent_global_queue_selection_is_one_proposal_without_claims(sessions):
    outbox, trial_id, _ = await setup(sessions)
    selected = [item for item in await asyncio.gather(
        selector(outbox).select_next(), selector(outbox).select_next()) if item is not None]
    assert selected and len({item.request.key for item in selected}) == 1
    assert selected[0].phase == "selected" and selected[0].lease_id is None
    async with sessions() as session:
        saved = (await session.scalars(select(NebiusPoolExecutionOutbox))).one()
        assert saved.trial_id == trial_id
    assert await selector(outbox).select_next() is None
    await assert_unclaimed(sessions, trial_id)


@pytest.mark.parametrize("invalid", ["cancelled", "foreign", "no-origin", "backoff", "exhausted", "already-selected"])
async def test_ineligible_head_cannot_hide_later_global_execution(sessions, invalid):
    outbox, original, target = await setup(sessions)
    following = await another_trial(sessions, original)
    if invalid == "already-selected":
        await outbox.propose(trial_id=original, target_id=target.target_id)
    elif invalid in {"foreign", "no-origin"}:
        async with sessions.begin() as session:
            trial = await session.get(Trial, original)
            trial.cancellation_requested_at = datetime.now(UTC)
            origin = None if invalid == "no-origin" else {**trial.pool_origin, "data_environment_id": str(uuid4())}
        # Origins are immutable; seed the malformed/historical candidate, don't
        # disable the real database guard merely to construct the fixture.
        await another_trial(sessions, original, pool_origin=origin,
            submitted_at=datetime.now(UTC) - timedelta(hours=1))
    else:
        async with sessions.begin() as session:
            trial = await session.get(Trial, original)
            if invalid == "cancelled":
                trial.cancellation_requested_at = datetime.now(UTC)
            elif invalid == "backoff":
                trial.next_attempt_at = datetime.now(UTC) + timedelta(hours=1)
            else:
                trial.attempt_count = 999
    selected = await selector(outbox).select_next()
    async with sessions() as session:
        assert (await session.get(NebiusPoolExecutionOutbox, selected.request.key.local_work_id)).trial_id == following
    await assert_unclaimed(sessions, following)


async def test_shared_origin_precedes_older_personal_work_in_the_same_dev_database(sessions):
    outbox, shared, _ = await setup(sessions)
    outbox.participant = outbox.participant.model_copy(update={"environment_class": "development"})
    async with sessions() as session:
        trial = await session.get(Trial, shared)
        origin = {**trial.pool_origin, "kind": "application", "application": {
            "application_id": str(uuid4()), "incarnation": str(uuid4()), "deployment_generation": 1,
            "release_id": str(uuid4()), "source_digest": "sha256:" + "a" * 64}}
    personal = await another_trial(sessions, shared, pool_origin=origin,
        submitted_at=datetime.now(UTC) - timedelta(hours=1))
    first, second = await selector(outbox).select_next(), await selector(outbox).select_next()
    async with sessions() as session:
        assert (await session.get(NebiusPoolExecutionOutbox, first.request.key.local_work_id)).trial_id == shared
        assert (await session.get(NebiusPoolExecutionOutbox, second.request.key.local_work_id)).trial_id == personal
    await assert_unclaimed(sessions, shared)


async def test_closed_intake_leaves_global_queue_without_claim_or_proposal(sessions):
    from loom.nebius_rollout_guard import acquire

    outbox, trial_id, _ = await setup(sessions)
    async with sessions.begin() as session:
        await acquire(session, owner="execution-selection", candidate="a" * 40)
    assert await selector(outbox).select_next() is None
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(NebiusPoolExecutionOutbox)) == 0
    await assert_unclaimed(sessions, trial_id)


async def test_invalid_deadline_preserves_existing_queued_configuration_failure(sessions):
    outbox, trial_id, _ = await setup(sessions)
    outbox.maximum_deadline_seconds = 60
    assert await selector(outbox).select_next() is None
    async with sessions() as session:
        trial = await session.get(Trial, trial_id)
        assert trial.state == "failed" and trial.failure_reason == "service_execution_configuration_invalid"
        assert trial.attempt_count == 0


@pytest.mark.parametrize("image_state", ["queued", "failed"])
async def test_image_preparation_wait_and_failure_preserve_queue_progress_without_attempts(sessions, image_state):
    from loom.task_image_materialization import task_image_materialization_key

    outbox, trial_id, _ = await setup(sessions)
    following = await another_trial(sessions, trial_id)
    async with sessions.begin() as session:
        trial = await session.get(Trial, trial_id)
        task = await session.get(Task, trial.task_id)
        image = TaskImageMaterialization(id=uuid4(), materialization_key=task_image_materialization_key(
            task_id=task.id, task_checksum=task.checksum, cpu_arch="x86_64"), task_id=task.id,
            task_checksum=task.checksum, cpu_arch="x86_64", task_config=task.config, state=image_state)
        session.add(image)
        session.add(TrialTaskImageMaterialization(trial_id=trial_id, materialization_id=image.id))
    selected = await selector(outbox).select_next()
    async with sessions() as session:
        assert (await session.get(NebiusPoolExecutionOutbox, selected.request.key.local_work_id)).trial_id == following
        trial = await session.get(Trial, trial_id)
        assert trial.attempt_count == 0 and trial.state == ("failed" if image_state == "failed" else "queued")
        assert trial.failure_reason == ("task_image_build_failed" if image_state == "failed" else None)


@pytest.mark.parametrize("occupied_cpu", [0, 3000])
async def test_actual_scheduler_loop_selects_without_legacy_reservation_then_actuator_drives_it(sessions, tmp_path, occupied_cpu):
    from loom_control_plane.service_execution_scheduler import run_service_execution_scheduler_loop
    from tests.support.execution_image_admission import IMAGE_ADMISSION_KEYRING

    async with connected(sessions, tmp_path, occupied_cpu=occupied_cpu) as case:
        following = await another_trial(sessions, case.trial_id)
        actual = selector(case.outbox)
        empty = asyncio.Event()

        class ObservedSelector:
            async def select_next(self):
                result = await actual.select_next()
                if result is None:
                    empty.set()
                return result

        loop = asyncio.create_task(run_service_execution_scheduler_loop(session_factory=sessions,
            environment="staging", pool_id="nebius-cpu", image_admission_keyring=IMAGE_ADMISSION_KEYRING,
            interval_seconds=0.1, maximum_deadline_seconds=7200, global_selector=ObservedSelector()))
        try:
            await asyncio.wait_for(empty.wait(), timeout=10)
            await assert_unclaimed(sessions, following)
        finally:
            loop.cancel()
            await asyncio.gather(loop, return_exceptions=True)
        await case.actuator.reconcile_full_once()
        async with sessions() as session:
            saved = await session.scalar(select(NebiusPoolExecutionOutbox).where(
                NebiusPoolExecutionOutbox.trial_id == following))
            trial = await session.get(Trial, following)
            assert saved.phase == ("selected" if occupied_cpu else "active")
            assert trial.attempt_count == (0 if occupied_cpu else 1)
        assert not case.api.writes  # Only the gateway, never the scheduler/actuator, can create Jobs.
