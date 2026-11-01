"""A denied candidate must release capacity authority before compiling another."""
import asyncio
from contextlib import suppress
from datetime import datetime, timedelta
from uuid import uuid4

from sqlalchemy import select, text

from loom.db.schema import ServiceExecutionLease, Trial
from loom_control_plane import service_execution_scheduler as scheduler
from loom_control_plane.execution_capacity import _CAPACITY_ADMISSION_LOCK
from tests.integration.test_execution_capacity_head_wakeup import (
    _cancel_predecessor,
    _successor_wait,
)
from tests.integration.test_execution_capacity_head_wakeup import (
    native_setup as _native_setup,
)
from tests.integration.test_service_execution_leases import (
    _cleanup_service_execution_test_rows,  # noqa: F401
)
from tests.support.execution_image_admission import IMAGE_ADMISSION_KEYRING

native_setup = _native_setup


def _freeze(monkeypatch, at):
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return at.astimezone(tz) if tz else at.replace(tzinfo=None)
    monkeypatch.setattr(scheduler, "datetime", Clock)


def _dispatch(sessions):
    return asyncio.create_task(scheduler.run_service_execution_scheduler_loop(
        session_factory=sessions, environment="staging", pool_id="nebius-cpu",
        image_admission_keyring=IMAGE_ADMISSION_KEYRING, interval_seconds=60,
        maximum_deadline_seconds=7200,
    ))


async def test_blocked_candidate_releases_capacity_lock_before_next_compile(native_setup, monkeypatch):
    sessions, owned = native_setup
    now, predecessor_id, successor_id = await _successor_wait(sessions, owned)
    at = now + timedelta(seconds=59, milliseconds=500)
    async with sessions() as session, session.begin():
        for trial_id in (predecessor_id, successor_id):
            trial = await session.get(Trial, trial_id)
            trial.next_attempt_at = None
            trial.scheduling_observation = None
        await session.flush()
        row = (await session.execute(scheduler._NEXT_SERVICE_TRIAL,
            {"now": at, "pool_id": "nebius-cpu"})).mappings().one()
        first_id = row["id"]
        assert first_id in {predecessor_id, successor_id}
        second_id = ({predecessor_id, successor_id} - {first_id}).pop()
    _freeze(monkeypatch, at)
    entered, release = asyncio.Event(), asyncio.Event()
    compile_candidate = scheduler._compile_service_candidate

    async def pause_second(*args, **kwargs):
        if kwargs["row"]["id"] == second_id:
            entered.set()
            await release.wait()
        return await compile_candidate(*args, **kwargs)

    monkeypatch.setattr(scheduler, "_compile_service_candidate", pause_second)
    task = _dispatch(sessions)
    try:
        await asyncio.wait_for(entered.wait(), timeout=3)
        async with sessions() as actor, actor.begin():
            available = await actor.scalar(text(str(_CAPACITY_ADMISSION_LOCK).replace(
                "pg_advisory_xact_lock(", "pg_try_advisory_xact_lock(",
            )))
        assert available, "the next candidate is compiling while the denied candidate still holds capacity authority"
        async with sessions() as observer:
            predecessor = await observer.get(Trial, first_id)
            assert predecessor.scheduling_observation["capacity_wait"]
            assert predecessor.next_attempt_at > at and predecessor.attempt_count == 0
            assert not await observer.scalar(select(ServiceExecutionLease.id).where(
                ServiceExecutionLease.trial_id == first_id,
            ))
    finally:
        release.set()
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task


async def test_ready_rate_head_is_rechecked_between_failed_candidates(native_setup, monkeypatch):
    sessions, owned = native_setup
    now, predecessor_id, successor_id = await _successor_wait(sessions, owned)
    await _cancel_predecessor(sessions, predecessor_id, now + timedelta(seconds=60))
    invalid_id, slow_id = uuid4(), uuid4()
    at = now + timedelta(seconds=62, milliseconds=300)
    async with sessions() as session, session.begin():
        successor = await session.get(Trial, successor_id)
        before = dict(successor.scheduling_observation["capacity_wait"])
        for trial_id, age in ((invalid_id, 3), (slow_id, 2)):
            session.add(Trial(
                id=trial_id, team_id=successor.team_id, task_id=successor.task_id,
                batch_id=successor.batch_id, config=dict(successor.config),
                requires_caps=dict(successor.requires_caps), state="queued", attempt_count=0,
                submitted_at=now - timedelta(seconds=age),
            ))
        await session.flush()
        row = (await session.execute(scheduler._NEXT_SERVICE_TRIAL,
            {"now": at, "pool_id": "nebius-cpu"})).mappings().one()
        pair = {invalid_id, slow_id}
        invalid_id = row["id"]
        assert invalid_id in pair
        slow_id = (pair - {invalid_id}).pop()
    _freeze(monkeypatch, at)
    entered, release = asyncio.Event(), asyncio.Event()
    compile_candidate = scheduler._compile_service_candidate

    async def fail_then_pause(*args, **kwargs):
        if kwargs["row"]["id"] == invalid_id:
            raise scheduler.ServiceExecutionConfigurationError("controlled invalid candidate")
        if kwargs["row"]["id"] == slow_id:
            entered.set()
            await release.wait()
        return await compile_candidate(*args, **kwargs)

    monkeypatch.setattr(scheduler, "_compile_service_candidate", fail_then_pause)
    task = _dispatch(sessions)
    try:
        await asyncio.wait_for(entered.wait(), timeout=3)
        async with sessions() as observer:
            successor = await observer.get(Trial, successor_id)
            assert successor.next_attempt_at == at, "a ready head still waits behind the rest of the failed-candidate wave"
            assert {**successor.scheduling_observation["capacity_wait"],
                    "retry_at": before["retry_at"]} == before
            assert successor.attempt_count == 0
            invalid = await observer.get(Trial, invalid_id)
            assert invalid.state == "failed" and invalid.attempt_count == 0
    finally:
        release.set()
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task
