"""Rate-limited execution wakes when shared create history actually expires."""

import asyncio
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import select

from loom.db.schema import (
    ExecutionCapacityPolicy,
    ExecutionProvisioningAuthorization,
    ServiceExecutionLease,
    ServiceExecutionTarget,
    TaskImageMaterializationAttempt,
    Trial,
)
from loom_control_plane.execution_capacity import (
    ExecutionProvisioningBlockedError,
    admit_capacity_resources,
)
from loom_control_plane.execution_capacity_wait import ExecutionCapacityWait
from loom_control_plane.service_execution import set_execution_target_health
from loom_control_plane.service_execution_scheduler import reserve_next_service_execution
from loom_control_plane.task_image_capacity import reserve_native_task_image_capacity
from loom_execution_capacity_collector.contracts import ResourceTotals
from tests.integration.test_execution_shared_capacity import (
    _family,
    _guest_reserve,
    _register_guest,
)
from tests.integration.test_nebius_task_image_capacity import _native
from tests.integration.test_nebius_task_image_capacity import native_setup as _native_setup
from tests.integration.test_service_execution_leases import (
    _cleanup_service_execution_test_rows,  # noqa: F401
    _configure_scheduler_trial,
    _reserve,
)
from tests.support.execution_image_admission import IMAGE_ADMISSION_KEYRING

native_setup = _native_setup


@pytest.mark.parametrize("first", ["execution", "native"])
@pytest.mark.parametrize("released", [False, True])
async def test_rate_retry_tracks_shared_history_and_inclusive_boundary(native_setup, first, released):
    sessions, owned = native_setup
    now = datetime.now(UTC)
    async with sessions() as session, session.begin():
        owner, guest, _ = await _family(session, now)
        policy = await session.get(ExecutionCapacityPolicy, owner[1].target_id)
        policy.max_create_per_minute = 1
        if first == "native":
            attempt_id, _ = await _native(session, owned, owner, now, cpu=1000)
            await reserve_native_task_image_capacity(session, attempt_id=attempt_id, now=now + timedelta(seconds=2))
            if released:
                attempt = await session.get(TaskImageMaterializationAttempt, attempt_id)
                attempt.native_build = {**attempt.native_build,
                    "capacity_released_at": (now + timedelta(seconds=3)).isoformat()}
        else:
            lease = await _reserve(session, trial_id=owner[0], target=owner[1], now=now + timedelta(seconds=2))
            if released:
                authorization = await session.scalar(select(ExecutionProvisioningAuthorization).where(
                    ExecutionProvisioningAuthorization.lease_id == lease.id))
                authorization.state = "released"
                authorization.released_at = now + timedelta(seconds=3)

        # Move the simulated scheduler clock across a full rate window while
        # retaining independently fresh target health, as the collector does.
        for target in (owner[1], guest[1]):
            await set_execution_target_health(session, target_id=target.target_id,
                desired_state="active", observed_state="ready", health_status="healthy",
                observed_at=now + timedelta(seconds=50))

    blocked_at = now + timedelta(seconds=58, milliseconds=300)
    with pytest.raises(ExecutionProvisioningBlockedError) as failure:
        async with sessions() as session, session.begin():
            await _guest_reserve(session, guest, blocked_at)
    assert failure.value.reason == "execution_capacity_create_rate_exceeded"
    assert failure.value.retry_after_seconds == 4, "do not sleep thirty seconds after the rate window reopens"

    # History at exactly now-60s still counts, including released/native rows.
    with pytest.raises(ExecutionProvisioningBlockedError) as boundary:
        async with sessions() as session, session.begin():
            await _guest_reserve(session, guest, now + timedelta(seconds=62))
    assert boundary.value.reason == "execution_capacity_create_rate_exceeded"
    assert boundary.value.retry_after_seconds == 1

    # Exercise real shared-family reservation at the returned retry time.
    async with sessions() as session, session.begin():
        lease = await _guest_reserve(session, guest, blocked_at + timedelta(seconds=failure.value.retry_after_seconds))
        assert lease.attempt == 1


@pytest.mark.parametrize("waiting_heads,expected", [(0, 4), (1, 10), (2, 30)])
async def test_mixed_history_waits_for_enough_expirations_without_overtaking(native_setup, monkeypatch, waiting_heads, expected):
    from loom_control_plane import execution_capacity_wait

    sessions, owned = native_setup
    now = datetime.now(UTC)
    async with sessions() as session, session.begin():
        owner, _, _ = await _family(session, now)
        policy = await session.get(ExecutionCapacityPolicy, owner[1].target_id)
        policy.max_create_per_minute = 2
        attempt_id, _ = await _native(session, owned, owner, now, cpu=1000)
        await reserve_native_task_image_capacity(session, attempt_id=attempt_id, now=now + timedelta(seconds=8))
        await _reserve(session, trial_id=owner[0], target=owner[1], now=now + timedelta(seconds=2))

    resources = ResourceTotals(cpu_millis=1000, memory_mib=1024, storage_mib=2048)

    async def older_validated_heads(*_args, **_kwargs):
        # Isolate admission arithmetic after the existing eligibility reader;
        # real-reader fairness is exercised by the shared-capacity suite.
        return ([ExecutionCapacityWait(uuid4(), owner[1].target_id, resources, now)
                 for _ in range(waiting_heads)], None)

    monkeypatch.setattr(execution_capacity_wait, "read_execution_capacity_waits", older_validated_heads)
    with pytest.raises(ExecutionProvisioningBlockedError) as blocked:
        async with sessions() as session, session.begin():
            await admit_capacity_resources(session, target=await session.get(ServiceExecutionTarget, owner[1].target_id),
                demand_id="later-execution", resources=resources,
                current_time=now + timedelta(seconds=58, milliseconds=300))
    assert blocked.value.reason == "execution_capacity_create_rate_exceeded"
    assert blocked.value.retry_after_seconds == expected


async def test_simultaneous_retry_still_grants_only_one_shared_slot(native_setup):
    sessions, owned = native_setup
    now = datetime.now(UTC)
    async with sessions() as session, session.begin():
        owner, guest, _ = await _family(session, now)
        other = await _register_guest(session, owner, now)
        policy = await session.get(ExecutionCapacityPolicy, owner[1].target_id)
        policy.max_create_per_minute = 1
        attempt_id, _ = await _native(session, owned, owner, now, cpu=1000)
        await reserve_native_task_image_capacity(session, attempt_id=attempt_id, now=now + timedelta(seconds=2))
        for target in (owner[1], guest[1]):
            await set_execution_target_health(session, target_id=target.target_id,
                desired_state="active", observed_state="ready", health_status="healthy",
                observed_at=now + timedelta(seconds=50))

    async def retry(pair):
        try:
            async with sessions() as session, session.begin():
                return await _guest_reserve(session, pair, now + timedelta(seconds=62, milliseconds=300))
        except ExecutionProvisioningBlockedError as exc:
            return exc.reason

    outcomes = await asyncio.gather(retry(guest), retry(other))
    assert sum(isinstance(value, ServiceExecutionLease) for value in outcomes) == 1
    assert "execution_capacity_create_rate_exceeded" in outcomes


async def test_normal_scheduler_persists_precise_retry_without_consuming_attempt(native_setup):
    sessions, _ = native_setup
    now = datetime.now(UTC)
    async with sessions() as session, session.begin():
        owner, queued, _ = await _family(session, now)
        await _configure_scheduler_trial(session, trial_id=queued[0], now=now)
        policy = await session.get(ExecutionCapacityPolicy, owner[1].target_id)
        policy.max_create_per_minute = 1
        await _reserve(session, trial_id=owner[0], target=owner[1], now=now + timedelta(seconds=2))
        for target in (owner[1], queued[1]):
            await set_execution_target_health(session, target_id=target.target_id,
                desired_state="active", observed_state="ready", health_status="healthy",
                observed_at=now + timedelta(seconds=50))

    async def schedule(at):
        async with sessions() as session, session.begin():
            return await reserve_next_service_execution(session, environment="staging", pool_id="nebius-cpu",
                image_admission_keyring=IMAGE_ADMISSION_KEYRING, now=at)

    blocked_at = now + timedelta(seconds=58, milliseconds=300)
    assert await schedule(blocked_at) is None
    async with sessions() as session:
        trial = await session.get(Trial, queued[0])
        assert trial.state == "queued" and trial.attempt_count == 0
        assert trial.next_attempt_at == blocked_at + timedelta(seconds=4)
        assert trial.scheduling_observation["reason"] == "execution_capacity_create_rate_exceeded"
        assert trial.scheduling_observation["capacity_wait"]["retry_at"] == trial.next_attempt_at.isoformat()
        assert not await session.scalar(select(ServiceExecutionLease.id).where(ServiceExecutionLease.trial_id == queued[0]))
    assert await schedule(blocked_at + timedelta(seconds=3)) is None
    lease = await schedule(blocked_at + timedelta(seconds=4))
    assert lease is not None and lease.trial_id == queued[0] and lease.attempt == 1
