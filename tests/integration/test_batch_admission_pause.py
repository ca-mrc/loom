"""Real SQL: a batch admission pause holds, drains and resumes pre-start work (#2777)."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

import pytest
from sqlalchemy import func, select, text, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from loom.db.schema import (
    Batch,
    ExecutionAdmissionReservation,
    ExecutionCostReservation,
    ServiceExecutionLease,
    TeamQuota,
    Trial,
)
from loom_control_plane.batch_admission import (
    clear_resumed_queued_trials,
    count_pending_release,
    drain_paused_admission,
)
from loom_control_plane.retry_exhausted_sweeper import sweep_retry_exhausted
from loom_control_plane.service_execution import (
    ServiceExecutionConflict,
    request_trial_execution_cancellation,
    set_execution_target_health,
)
from loom_control_plane.service_execution_scheduler import reserve_next_service_execution
from loom_execution_actuator.controller import ExecutionActuator
from loom_execution_actuator.renderer import ExecutionTargetRuntime
from tests.integration import test_service_execution_leases as fixtures
from tests.integration.test_service_execution_leases import (
    _cleanup_service_execution_test_rows,  # noqa: F401 -- disposable owned rows
)
from tests.integration.test_service_execution_regions import _observe
from tests.support.execution_image_admission import IMAGE_ADMISSION_KEYRING


async def _reserve_next(session: AsyncSession, now: datetime) -> ServiceExecutionLease | None:
    return await reserve_next_service_execution(
        session, environment="staging", pool_id="nebius-cpu",
        image_admission_keyring=IMAGE_ADMISSION_KEYRING, now=now,
    )


async def _seed(session: AsyncSession, now: datetime) -> tuple[UUID, UUID, Any]:
    trial_id, target = await fixtures._seed_ready_trial(session, now=now)
    await fixtures._configure_scheduler_trial(session, trial_id=trial_id, now=now)
    trial = await session.get(Trial, trial_id)
    assert trial is not None and trial.batch_id is not None
    return trial_id, trial.batch_id, target


async def _set_paused(session: AsyncSession, batch_id: UUID, paused_at: datetime | None) -> None:
    await session.execute(update(Batch).where(Batch.id == batch_id).values(
        admission_paused_at=paused_at,
        admission_paused_reason="incident" if paused_at is not None else None,
    ))


async def _lease_rows(session: AsyncSession, trial_id: UUID) -> int:
    return int(await session.scalar(
        select(func.count()).select_from(ServiceExecutionLease)
        .where(ServiceExecutionLease.trial_id == trial_id)
    ) or 0)


async def test_paused_batch_reserves_no_attempt_until_resume(postgres_url: str) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    try:
        async with sessions.begin() as session:
            trial_id, batch_id, target = await _seed(session, now)
            await _set_paused(session, batch_id, now)
        for tick in range(3):
            async with sessions.begin() as session:
                assert await _reserve_next(session, now + timedelta(seconds=tick)) is None
        # The authoritative API fences callers that bypass candidate selection.
        async with sessions() as session:
            with pytest.raises(ServiceExecutionConflict, match="batch no longer accepts execution"):
                await fixtures._reserve(session, trial_id=trial_id, target=target, now=now)
            await session.rollback()
        async with sessions() as session:
            trial = await session.get(Trial, trial_id)
            assert trial is not None and trial.state == "queued" and trial.attempt_count == 0
            assert await _lease_rows(session, trial_id) == 0
        async with sessions.begin() as session:
            await _set_paused(session, batch_id, None)
        async with sessions.begin() as session:
            lease = await _reserve_next(session, now + timedelta(seconds=5))
            assert lease is not None and lease.trial_id == trial_id and lease.attempt == 1
    finally:
        await engine.dispose()


async def test_drain_releases_reserved_attempt_cleans_up_and_refunds_budget(
    postgres_url: str,
) -> None:
    """A pause committed with no drain yet converges on a later tick.

    The released attempt does not count against a one-attempt ceiling, so
    the sweeper leaves the trial queued and resume reserves attempt two.
    """
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    kube = fixtures._FakeKubernetesJobApi()
    try:
        async with sessions.begin() as session:
            trial_id, batch_id, target = await _seed(session, now)
            await session.flush()
            trial = await session.get(Trial, trial_id)
            assert trial is not None
            quota = await session.get(TeamQuota, trial.team_id)
            assert quota is not None
            quota.max_attempts_ceiling = 1
        async with sessions.begin() as session:
            first = await _reserve_next(session, now + timedelta(seconds=1))
            assert first is not None and first.attempt == 1
        actuator = ExecutionActuator(
            sessions=sessions,
            kubernetes=kube,
            target=ExecutionTargetRuntime(target_id=target.target_id, namespace=target.namespace_name),
            controller_id="admission-pause",
        )
        # The Job exists but its pod was never scheduled.
        assert await actuator.run_commands_once(now=now + timedelta(seconds=2)) == 1
        assert kube.create_count == 1
        async with sessions.begin() as session:
            await _set_paused(session, batch_id, now + timedelta(seconds=3))
        async with sessions() as session:
            assert await count_pending_release(session, batch_id=batch_id) == 1

        drained = await drain_paused_admission(sessions, now=now + timedelta(seconds=4))
        assert drained.released_lease_count == 1
        replay = await drain_paused_admission(sessions, now=now + timedelta(seconds=5))
        assert replay.released_lease_count == 0
        async with sessions() as session:
            lease = await session.get(ServiceExecutionLease, first.id)
            assert lease is not None and lease.desired_state == "retry"
            assert lease.revoked_at is not None
            assert (lease.error_class, lease.error_code) == ("transient", "admission_paused")
            trial = await session.get(Trial, trial_id)
            assert trial is not None and trial.admission_pause_releases == 1
            assert trial.scheduling_observation["reason"] == "admission_paused"
            assert await count_pending_release(session, batch_id=batch_id) == 0

        after_cleanup = first.deadline_at + timedelta(seconds=302)
        await actuator.run_commands_once(now=now + timedelta(seconds=6))
        await actuator.reconcile_full_once(now=now + timedelta(seconds=7))
        await actuator.run_commands_once(now=after_cleanup)
        await actuator.reconcile_full_once(now=after_cleanup)
        assert kube.delete_count == 1 and not kube.jobs
        async with sessions() as session:
            lease = await session.get(ServiceExecutionLease, first.id)
            assert lease is not None
            assert lease.cleanup_state == "complete" and lease.desired_state == "deleted"
            for model, column in (
                (ExecutionCostReservation, ExecutionCostReservation.lease_id),
                (ExecutionAdmissionReservation, ExecutionAdmissionReservation.owner_id),
            ):
                row = await session.scalar(select(model).where(column == first.id))
                assert row is not None and row.state == "released"
            trial = await session.get(Trial, trial_id)
            assert trial is not None and trial.state == "queued" and trial.attempt_count == 1
        async with sessions.begin() as session:
            assert trial_id not in await sweep_retry_exhausted(session)
        async with sessions.begin() as session:
            await set_execution_target_health(
                session, target_id=target.target_id, desired_state="active",
                observed_state="ready", health_status="healthy", observed_at=after_cleanup,
            )
            await _observe(session, target, after_cleanup, quota=2)
        async with sessions.begin() as session:
            assert await _reserve_next(session, after_cleanup + timedelta(seconds=1)) is None

        async with sessions.begin() as session:
            await _set_paused(session, batch_id, None)
            assert await clear_resumed_queued_trials(session, batch_id=batch_id) == 1
        async with sessions.begin() as session:
            trial = await session.get(Trial, trial_id)
            assert trial is not None and trial.scheduling_observation is None
            resumed = await _reserve_next(session, after_cleanup + timedelta(seconds=2))
            assert resumed is not None and resumed.trial_id == trial_id and resumed.attempt == 2
    finally:
        await engine.dispose()


@pytest.mark.parametrize("boundary", ["scheduled", "cancel_requested", "resumed", "cancelling"])
async def test_drain_leaves_leases_outside_the_pre_start_boundary(
    postgres_url: str, boundary: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    try:
        async with sessions.begin() as session:
            trial_id, batch_id, _ = await _seed(session, now)
        async with sessions.begin() as session:
            first = await _reserve_next(session, now + timedelta(seconds=1))
            assert first is not None
        async with sessions.begin() as session:
            await _set_paused(session, batch_id, now + timedelta(seconds=2))
            if boundary == "scheduled":
                await session.execute(update(ServiceExecutionLease).where(
                    ServiceExecutionLease.id == first.id,
                ).values(pod_scheduled_at=now + timedelta(seconds=2), node_name="node-a"))
            elif boundary == "cancel_requested":
                command = await request_trial_execution_cancellation(
                    session, trial_id=trial_id, now=now + timedelta(seconds=2),
                )
                assert command is not None and command.lease_id == first.id
            elif boundary == "resumed":
                await _set_paused(session, batch_id, None)
            else:
                await session.execute(update(Batch).where(Batch.id == batch_id).values(
                    state="cancelling",
                ))
        drained = await drain_paused_admission(sessions, now=now + timedelta(seconds=3))
        assert drained.released_lease_count == 0
        async with sessions() as session:
            lease = await session.get(ServiceExecutionLease, first.id)
            assert lease is not None and lease.error_code != "admission_paused"
            expected = "cancel" if boundary == "cancel_requested" else "create"
            assert lease.desired_state == expected
            trial = await session.get(Trial, trial_id)
            assert trial is not None and trial.admission_pause_releases == 0
    finally:
        await engine.dispose()


async def test_pause_committed_after_candidate_selection_fences_the_reservation(
    postgres_url: str,
) -> None:
    from loom_control_plane import service_execution_scheduler as scheduler

    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    selected = asyncio.Event()
    paused = asyncio.Event()

    class PauseAfterSelection(AsyncSession):
        async def execute(self, statement: Any, *args: Any, **kwargs: Any) -> Any:
            result = await super().execute(statement, *args, **kwargs)
            if statement is scheduler._NEXT_SERVICE_TRIAL:
                selected.set()
                await paused.wait()
            return result

    observed = async_sessionmaker(engine, class_=PauseAfterSelection, expire_on_commit=False)
    reservation: asyncio.Task[ServiceExecutionLease | None] | None = None
    try:
        async with sessions.begin() as session:
            trial_id, batch_id, _ = await _seed(session, now)
            trial = await session.get(Trial, trial_id)
            assert trial is not None
            trial.submit_priority = 200
            following_id, _, _ = await _seed(session, now)

        async def reserve() -> ServiceExecutionLease | None:
            async with observed.begin() as session:
                return await _reserve_next(session, now)

        reservation = asyncio.create_task(reserve())
        await asyncio.wait_for(selected.wait(), timeout=10)
        # The same parent lock the pause route takes, while selection holds Trial.
        async with sessions.begin() as session:
            await session.execute(text("SET LOCAL lock_timeout = '2s'"))
            await session.execute(select(Batch).where(Batch.id == batch_id).with_for_update())
            await _set_paused(session, batch_id, now)
        paused.set()
        admitted = await asyncio.wait_for(reservation, timeout=10)
        assert admitted is not None and admitted.trial_id == following_id
        async with sessions() as session:
            child = await session.get(Trial, trial_id)
            assert child is not None and child.state == "queued" and child.attempt_count == 0
            assert await _lease_rows(session, trial_id) == 0
    finally:
        paused.set()
        if reservation is not None and not reservation.done():
            await asyncio.wait_for(reservation, timeout=10)
        await engine.dispose()


async def test_pause_waits_for_an_in_flight_reservation_then_drains_it(postgres_url: str) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    pausing_pid: asyncio.Future[int] = asyncio.get_running_loop().create_future()
    pause: asyncio.Task[None] | None = None
    try:
        async with sessions.begin() as session:
            trial_id, batch_id, target = await _seed(session, now)

        async def pause_parent() -> None:
            async with sessions.begin() as session:
                pid = await session.scalar(text("SELECT pg_backend_pid()"))
                assert isinstance(pid, int)
                pausing_pid.set_result(pid)
                await session.execute(select(Batch).where(Batch.id == batch_id).with_for_update())
                await _set_paused(session, batch_id, now)

        async with sessions() as session:
            lease = await fixtures._reserve(session, trial_id=trial_id, target=target, now=now)
            pause = asyncio.create_task(pause_parent())
            pid = await asyncio.wait_for(pausing_pid, timeout=10)
            async with sessions() as observer, asyncio.timeout(5):
                while not await observer.scalar(
                    text("SELECT pg_blocking_pids(:pid)"), {"pid": pid},
                ):
                    assert not pause.done(), "pause committed before the reservation"
                    await asyncio.sleep(0.01)
            await session.commit()
        await asyncio.wait_for(pause, timeout=10)
        drained = await drain_paused_admission(sessions, batch_id=batch_id, now=now)
        assert drained.released_lease_count == 1
        async with sessions() as session:
            current = await session.get(ServiceExecutionLease, lease.id)
            assert current is not None and current.desired_state == "retry"
            assert current.error_code == "admission_paused"
    finally:
        if pause is not None and not pause.done():
            await asyncio.wait_for(pause, timeout=10)
        await engine.dispose()
