"""A rate wait must not outlive the queue state used to calculate its retry."""

import asyncio
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import select, text

from loom.db.schema import (
    ExecutionCapacityPolicy,
    ExecutionProvisioningAuthorization,
    ServiceExecutionLease,
    ServiceExecutionTarget,
    TaskImageCapacityWait,
    TaskImageMaterializationAttempt,
    Trial,
)
from loom_control_plane import service_execution_scheduler as scheduler
from loom_control_plane.execution_capacity_wait import wake_ready_rate_heads
from loom_control_plane.service_execution import set_execution_target_health
from loom_control_plane.task_image_capacity import reserve_native_task_image_capacity
from tests.integration.test_execution_shared_capacity import _family
from tests.integration.test_nebius_task_image_capacity import _native
from tests.integration.test_nebius_task_image_capacity import native_setup as _native_setup
from tests.integration.test_service_execution_leases import (
    _cleanup_service_execution_test_rows,  # noqa: F401
    _configure_scheduler_trial,
    _reserve,
)
from tests.support.execution_image_admission import IMAGE_ADMISSION_KEYRING

native_setup = _native_setup


async def _wake(sessions, at):
    async with sessions() as session, session.begin():
        return await wake_ready_rate_heads(
            session, environment="staging", pool_id="nebius-cpu", now=at,
        )


async def _cancel_predecessor(sessions, predecessor_id, at):
    async with sessions() as session, session.begin():
        predecessor = await session.get(Trial, predecessor_id)
        predecessor.cancellation_requested_at = at


async def _schedule(sessions, at):
    async with sessions() as session, session.begin():
        return await scheduler.reserve_next_service_execution(
            session, environment="staging", pool_id="nebius-cpu",
            image_admission_keyring=IMAGE_ADMISSION_KEYRING, now=at,
        )


async def _successor_wait(sessions, owned):
    """Two real creates and two real queued Trials sharing one physical owner."""
    now = datetime.now(UTC)
    predecessor_id = uuid4()
    async with sessions() as session, session.begin():
        owner, successor, _ = await _family(session, now)
        await _configure_scheduler_trial(session, trial_id=successor[0], now=now)
        trial = await session.get(Trial, successor[0])
        trial.next_attempt_at = now + timedelta(seconds=59, milliseconds=300)
        session.add(Trial(
            id=predecessor_id, team_id=trial.team_id, task_id=trial.task_id,
            batch_id=trial.batch_id, config=dict(trial.config),
            requires_caps=dict(trial.requires_caps), state="queued", attempt_count=0,
            submitted_at=now - timedelta(seconds=1),
        ))
        policy = await session.get(ExecutionCapacityPolicy, owner[1].target_id)
        policy.max_create_per_minute = 2
        await _reserve(session, trial_id=owner[0], target=owner[1],
                       now=now + timedelta(seconds=2))
        attempt_id, _ = await _native(session, owned, owner, now, cpu=1000)
        await reserve_native_task_image_capacity(session, attempt_id=attempt_id,
                                                  now=now + timedelta(seconds=8))
        for target in (owner[1], successor[1]):
            await set_execution_target_health(
                session, target_id=target.target_id, desired_state="active",
                observed_state="ready", health_status="healthy",
                observed_at=now + timedelta(seconds=50),
            )

    assert await _schedule(sessions, now + timedelta(seconds=58, milliseconds=300)) is None
    assert await _schedule(sessions, now + timedelta(seconds=59, milliseconds=300)) is None
    async with sessions() as session:
        predecessor = await session.get(Trial, predecessor_id)
        successor_row = await session.get(Trial, successor[0])
        assert predecessor.scheduling_observation["capacity_wait"]
        assert successor_row.scheduling_observation["capacity_wait"]
        assert predecessor.next_attempt_at == now + timedelta(seconds=62, milliseconds=300)
        assert successor_row.next_attempt_at == now + timedelta(seconds=68, milliseconds=300)
        assert successor_row.attempt_count == 0
    return now, predecessor_id, successor[0]


async def test_dispatcher_rechecks_successor_after_predecessor_cancels(native_setup, monkeypatch):
    sessions, owned = native_setup
    now, predecessor_id, successor_id = await _successor_wait(sessions, owned)
    await _cancel_predecessor(sessions, predecessor_id, now + timedelta(seconds=60))

    at = now + timedelta(seconds=62, milliseconds=300)

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return at.astimezone(tz) if tz else at.replace(tzinfo=None)

    monkeypatch.setattr(scheduler, "datetime", Clock)
    loop = asyncio.create_task(scheduler.run_service_execution_scheduler_loop(
        session_factory=sessions, environment="staging", pool_id="nebius-cpu",
        image_admission_keyring=IMAGE_ADMISSION_KEYRING, interval_seconds=.01,
        maximum_deadline_seconds=7200,
    ))

    async def wait_for_actual_reservation():
        while True:
            async with sessions() as session:
                lease = await session.scalar(select(ServiceExecutionLease).where(
                    ServiceExecutionLease.trial_id == successor_id,
                ))
                if lease is not None:
                    return lease
            await asyncio.sleep(.02)

    try:
        lease = await asyncio.wait_for(wait_for_actual_reservation(), timeout=3)
        assert lease.attempt == 1
    finally:
        loop.cancel()
        with suppress(asyncio.CancelledError):
            await loop


@pytest.mark.parametrize("released", [False, True])
async def test_wakeup_preserves_inclusive_native_history_and_wait_identity(native_setup, released):
    sessions, owned = native_setup
    now, predecessor_id, successor_id = await _successor_wait(sessions, owned)
    await _cancel_predecessor(sessions, predecessor_id, now + timedelta(seconds=60))
    async with sessions() as session, session.begin():
        trial = await session.get(Trial, successor_id)
        before = dict(trial.scheduling_observation["capacity_wait"])
        if released:
            authorization = await session.scalar(select(ExecutionProvisioningAuthorization))
            authorization.state = "released"
            authorization.released_at = now + timedelta(seconds=9)
            attempt = await session.scalar(select(TaskImageMaterializationAttempt).where(
                TaskImageMaterializationAttempt.native_capacity_reserved_at.is_not(None),
            ))
            attempt.native_build = {**attempt.native_build,
                "capacity_released_at": (now + timedelta(seconds=9)).isoformat()}

    # Both kinds of create, including released ones, count at exactly now-60s.
    assert await _wake(sessions, now + timedelta(seconds=62)) == 0
    at = now + timedelta(seconds=62, milliseconds=1)
    assert await _wake(sessions, at) == 1
    async with sessions() as session:
        trial = await session.get(Trial, successor_id)
        after = trial.scheduling_observation["capacity_wait"]
        assert {**after, "retry_at": before["retry_at"]} == before
        assert trial.next_attempt_at == at and after["retry_at"] == at.isoformat()
        assert trial.attempt_count == 0 and trial.state == "queued"
        assert not await session.scalar(select(ServiceExecutionLease.id).where(
            ServiceExecutionLease.trial_id == successor_id,
        ))
    assert (await _schedule(sessions, at)).trial_id == successor_id


async def test_wakeup_does_not_overtake_the_predecessor(native_setup):
    sessions, owned = native_setup
    now, predecessor_id, successor_id = await _successor_wait(sessions, owned)
    at = now + timedelta(seconds=62, milliseconds=1)
    assert await _wake(sessions, at) == 1
    assert (await _schedule(sessions, at)).trial_id == predecessor_id
    assert await _wake(sessions, at) == 0
    assert await _schedule(sessions, at) is None
    async with sessions() as session:
        successor = await session.get(Trial, successor_id)
        assert successor.next_attempt_at > at and successor.attempt_count == 0


async def test_older_validated_native_head_retains_its_rate_slot(native_setup, monkeypatch):
    from loom_control_plane import task_image_capacity_wait

    sessions, owned = native_setup
    now, predecessor_id, successor_id = await _successor_wait(sessions, owned)
    await _cancel_predecessor(sessions, predecessor_id, now + timedelta(seconds=60))
    async with sessions() as session:
        trial = await session.get(Trial, successor_id)
        target_id = trial.scheduling_observation["capacity_wait"]["target_id"]

    async def older_native(*args, **kwargs):
        # This reader's full eligibility is covered by shared-capacity tests;
        # isolate its already-validated prefix at the wakeup boundary here.
        return ([TaskImageCapacityWait(target_id=target_id, materialization_id=uuid4(),
            lease_epoch=1, first_waited_at=now)], None)

    monkeypatch.setattr(task_image_capacity_wait, "read_capacity_waits", older_native)
    assert await _wake(sessions, now + timedelta(seconds=62, milliseconds=300)) == 0


async def test_rollout_pause_prevents_retry_mutation(native_setup):
    sessions, owned = native_setup
    now, predecessor_id, successor_id = await _successor_wait(sessions, owned)
    await _cancel_predecessor(sessions, predecessor_id, now + timedelta(seconds=60))
    owner = "test-rate-wakeup-" + uuid4().hex
    async with sessions() as session, session.begin():
        await session.execute(text("INSERT INTO nebius_rollout_guard(id,owner,candidate_sha) "
                                   "VALUES(1,:owner,:candidate)"), {"owner": owner, "candidate": "c" * 40})
    try:
        assert await _wake(sessions, now + timedelta(seconds=62, milliseconds=300)) == 0
        async with sessions() as session:
            trial = await session.get(Trial, successor_id)
            assert trial.next_attempt_at == now + timedelta(seconds=68, milliseconds=300)
    finally:
        async with sessions() as session, session.begin():
            await session.execute(text("DELETE FROM nebius_rollout_guard WHERE owner=:owner"), {"owner": owner})


@pytest.mark.parametrize("environment,pool", [("other", "nebius-cpu"), ("staging", "other")])
async def test_wakeup_stays_in_its_environment_and_pool(native_setup, environment, pool):
    sessions, owned = native_setup
    now, predecessor_id, successor_id = await _successor_wait(sessions, owned)
    await _cancel_predecessor(sessions, predecessor_id, now + timedelta(seconds=60))
    async with sessions() as session, session.begin():
        assert await wake_ready_rate_heads(session, environment=environment, pool_id=pool,
                                           now=now + timedelta(seconds=62, milliseconds=300)) == 0
    async with sessions() as session:
        trial = await session.get(Trial, successor_id)
        assert trial.next_attempt_at == now + timedelta(seconds=68, milliseconds=300)


async def test_woken_head_still_passes_real_capacity_admission(native_setup):
    sessions, owned = native_setup
    now, predecessor_id, successor_id = await _successor_wait(sessions, owned)
    await _cancel_predecessor(sessions, predecessor_id, now + timedelta(seconds=60))
    async with sessions() as session, session.begin():
        trial = await session.get(Trial, successor_id)
        target_id = trial.scheduling_observation["capacity_wait"]["target_id"]
        policy = await session.get(ExecutionCapacityPolicy, target_id)
        policy.max_pending_jobs = 1
    at = now + timedelta(seconds=62, milliseconds=300)
    assert await _wake(sessions, at) == 1
    assert await _schedule(sessions, at) is None
    async with sessions() as session:
        trial = await session.get(Trial, successor_id)
        assert trial.scheduling_observation["reason"] == "execution_capacity_pending_limit_exceeded"
        assert trial.attempt_count == 0
        assert not await session.scalar(select(ServiceExecutionLease.id).where(
            ServiceExecutionLease.trial_id == successor_id,
        ))
    # The newly observed physical constraint retains its ordinary backoff.
    assert await _wake(sessions, at) == 0


@pytest.mark.parametrize("change", ["cancel", "input", "expired", "target"])
async def test_wakeup_does_not_revive_invalid_waits(native_setup, change):
    sessions, owned = native_setup
    now, predecessor_id, successor_id = await _successor_wait(sessions, owned)
    await _cancel_predecessor(sessions, predecessor_id, now + timedelta(seconds=60))
    at = now + timedelta(seconds=62, milliseconds=300)
    async with sessions() as session, session.begin():
        trial = await session.get(Trial, successor_id)
        retry_at = trial.next_attempt_at
        if change == "cancel":
            trial.cancellation_requested_at = at
        elif change == "input":
            trial.config = {**trial.config, "agent_name": "oracle"}
        elif change == "target":
            target = await session.get(ServiceExecutionTarget,
                trial.scheduling_observation["capacity_wait"]["target_id"])
            target.spec_json = {**target.spec_json,
                "health_stale_after_seconds": target.spec_json["health_stale_after_seconds"] + 1}
        else:
            observation = dict(trial.scheduling_observation)
            observation["capacity_wait"] = {**observation["capacity_wait"], "expires_at": at.isoformat()}
            trial.scheduling_observation = observation
    assert await _wake(sessions, at) == 0
    async with sessions() as session:
        trial = await session.get(Trial, successor_id)
        assert trial.next_attempt_at == retry_at and trial.attempt_count == 0


async def test_locked_head_is_skipped_and_concurrent_wakeups_grant_no_capacity(native_setup):
    sessions, owned = native_setup
    now, predecessor_id, successor_id = await _successor_wait(sessions, owned)
    await _cancel_predecessor(sessions, predecessor_id, now + timedelta(seconds=60))
    at = now + timedelta(seconds=62, milliseconds=300)
    async with sessions() as session, session.begin():
        await session.scalar(select(Trial).where(Trial.id == successor_id).with_for_update())
        assert await asyncio.wait_for(_wake(sessions, at), timeout=3) == 0
    assert sum(await asyncio.gather(_wake(sessions, at), _wake(sessions, at))) == 1
    leases = await asyncio.gather(_schedule(sessions, at), _schedule(sessions, at))
    assert sum(lease is not None for lease in leases) == 1


async def test_global_selector_rechecks_eligibility_without_local_reservation(native_setup, monkeypatch):
    sessions, owned = native_setup
    now, predecessor_id, successor_id = await _successor_wait(sessions, owned)
    await _cancel_predecessor(sessions, predecessor_id, now + timedelta(seconds=60))
    at = now + timedelta(seconds=62, milliseconds=300)
    selected = asyncio.Event()

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return at.astimezone(tz) if tz else at.replace(tzinfo=None)

    class Selector:
        async def select_next(self):
            async with sessions() as session:
                row = (await session.execute(scheduler._NEXT_SERVICE_TRIAL,
                    {"now": at, "pool_id": "nebius-cpu"})).mappings().one_or_none()
                if row is not None:
                    assert row["id"] == successor_id
                    selected.set()
                    return object()
            return None

    monkeypatch.setattr(scheduler, "datetime", Clock)
    loop = asyncio.create_task(scheduler.run_service_execution_scheduler_loop(
        session_factory=sessions, environment="staging", pool_id="nebius-cpu",
        image_admission_keyring=IMAGE_ADMISSION_KEYRING, interval_seconds=.01,
        maximum_deadline_seconds=7200, global_selector=Selector(),
    ))
    try:
        await asyncio.wait_for(selected.wait(), timeout=3)
    finally:
        loop.cancel()
        with suppress(asyncio.CancelledError):
            await loop
    async with sessions() as session:
        trial = await session.get(Trial, successor_id)
        assert trial.attempt_count == 0
        assert not await session.scalar(select(ServiceExecutionLease.id).where(
            ServiceExecutionLease.trial_id == successor_id,
        ))
