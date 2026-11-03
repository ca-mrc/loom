"""Cancellation must not deadlock a concurrent lease observation."""
from __future__ import annotations

import asyncio
from datetime import UTC, datetime

from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import Trial
from loom_control_plane.service_execution import (
    ServiceExecutionFenceError,
    enqueue_execution_transition,
    record_kubernetes_observation,
)
from tests.integration.test_service_execution_leases import (
    _cleanup_service_execution_test_rows,  # noqa: F401
    _reserve,
    _seed_ready_trial,
)


async def test_trial_cancellation_can_fence_a_waiting_observer(postgres_url: str) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    worker = None
    try:
        now = datetime.now(UTC)
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            lease = await _reserve(session, trial_id=trial_id, target=target, now=now)
            lease_id = lease.id
            await session.commit()

        pid_ready = asyncio.Future()

        async def observe() -> None:
            async with sessions() as session:
                pid_ready.set_result(await session.scalar(text("SELECT pg_backend_pid()")))
                try:
                    await record_kubernetes_observation(
                        session, lease_id=lease_id, generation=1, observed_at=now,
                        payload={"normalized_state": "running", "job_uid": "lock-test-job",
                                 "resource_version": "1"},
                    )
                    await session.commit()
                except ServiceExecutionFenceError:
                    # Cancellation won while observation waited: fencing is expected.
                    await session.rollback()

        async with sessions() as cancelling:
            await cancelling.get(Trial, trial_id, with_for_update=True)
            worker = asyncio.create_task(observe())
            pid = await pid_ready
            async with sessions() as inspector:
                async with asyncio.timeout(5):
                    while await inspector.scalar(text(
                        "SELECT wait_event_type FROM pg_stat_activity WHERE pid=:pid"
                    ), {"pid": pid}) != "Lock":
                        await asyncio.sleep(0.01)
                        await inspector.rollback()
            # The observer must be waiting BEFORE it owns the lease. Otherwise
            # cancellation's Trial→Lease order forms a cycle with Lease→Trial.
            await cancelling.execute(text("SET LOCAL lock_timeout='250ms'"))
            await enqueue_execution_transition(
                cancelling, lease_id=lease_id, expected_generation=1,
                desired_state="cancel", now=now,
            )
            await cancelling.commit()
        await asyncio.wait_for(worker, 5)
    finally:
        if worker is not None:
            if not worker.done():
                worker.cancel()
            await asyncio.gather(worker, return_exceptions=True)
        await engine.dispose()
