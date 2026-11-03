from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from uuid import UUID

import pytest
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from loom.db.schema import ServiceExecutionEvent, ServiceExecutionLease
from loom_control_plane.service_execution import (
    ServiceExecutionConflict,
    record_kubernetes_observation,
)
from loom_control_plane.service_execution_locks import lock_execution_trial
from tests.integration.test_service_execution_leases import (
    _cleanup_service_execution_test_rows,  # noqa: F401
    _reserve,
    _seed_ready_trial,
)


async def _wait_for_observation_fence(
    session: AsyncSession, backend_pid: int, blocking_pid: int,
) -> None:
    # Observe the actual blocker instead of timing two coroutines. Observers
    # serialize through the complete Trial→Lease fence, in that order.
    async with asyncio.timeout(5):
        while True:
            waiting = await session.scalar(
                text("SELECT wait_event_type = 'Lock' AND :blocker = ANY(pg_blocking_pids(pid)) "
                     "FROM pg_stat_activity WHERE pid = :pid"),
                {"pid": backend_pid, "blocker": blocking_pid},
            )
            if waiting:
                return
            await asyncio.sleep(0.01)


@pytest.mark.parametrize("changed_payload", [False, True])
async def test_concurrent_observation_uses_existing_ordinal_after_lease_lock(
    postgres_url: str,
    changed_payload: bool,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    delivery: asyncio.Task[tuple[UUID, bool]] | None = None
    try:
        async with sessions() as session, session.begin():
            trial_id, target = await _seed_ready_trial(session, now=now)
            lease = await _reserve(session, trial_id=trial_id, target=target, now=now)
            lease_id = lease.id

        payload = {
            "normalized_state": "pending",
            "job_uid": None,
            "pod_uid": None,
            "resource_version": "same-job-resource-version",
            "pod_resource_version": None,
            "reason": None,
        }

        async with sessions() as first, sessions() as second, sessions() as observer:
            # The API also locks the parent. A transaction that enters it with
            # a lease lock must already own the Trial, as production callers do.
            await lock_execution_trial(first, lease_id)
            await first.get(ServiceExecutionLease, lease_id, with_for_update=True)
            first_pid = await first.scalar(text("SELECT pg_backend_pid()"))
            assert isinstance(first_pid, int)
            second_pid = await second.scalar(text("SELECT pg_backend_pid()"))
            assert isinstance(second_pid, int)

            async def duplicate_delivery() -> tuple[UUID, bool]:
                event, duplicate = await record_kubernetes_observation(
                    second,
                    lease_id=lease_id,
                    generation=1,
                    payload={**payload, "reason": "changed"} if changed_payload else payload,
                    observed_at=now,
                )
                await second.commit()
                return event.id, duplicate

            delivery = asyncio.create_task(duplicate_delivery())
            await _wait_for_observation_fence(observer, second_pid, first_pid)
            # The first observer commits while the other waits for this fence.
            # The second must refresh the key/ordinal after acquiring its lease,
            # preserving exact replay and rejecting changed payloads.
            original, duplicate = await record_kubernetes_observation(
                first,
                lease_id=lease_id,
                generation=1,
                payload=payload,
                observed_at=now,
            )
            assert not duplicate
            await first.commit()
            if changed_payload:
                with pytest.raises(
                    ServiceExecutionConflict, match="execution event replay changed"
                ):
                    await asyncio.wait_for(delivery, timeout=5)
                await second.rollback()
            else:
                event_id, duplicate = await asyncio.wait_for(delivery, timeout=5)
                assert duplicate and event_id == original.id

        async with sessions() as session:
            assert (
                await session.scalar(
                    select(func.count(ServiceExecutionEvent.id)).where(
                        ServiceExecutionEvent.lease_id == lease_id,
                    ),
                )
                == 1
            )
            persisted = await session.get(ServiceExecutionLease, lease_id)
            assert persisted is not None and persisted.last_event_ordinal == original.ordinal == 1
    finally:
        if delivery is not None and not delivery.done():
            delivery.cancel()
            await asyncio.gather(delivery, return_exceptions=True)
        await engine.dispose()
