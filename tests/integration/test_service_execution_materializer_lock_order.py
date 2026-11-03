"""Archival recovery must not hold a lease while waiting on its Trial."""
from __future__ import annotations

import asyncio
from datetime import UTC, datetime

from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import ServiceExecutionLease, Trial
from loom.trajectory.storage import FakeObjectStore
from loom_control_plane.service_execution_materializer import ServiceExecutionMaterializer
from tests.integration.test_service_execution_leases import (
    _cleanup_service_execution_test_rows,  # noqa: F401
    _reserve,
    _seed_ready_trial,
)


async def test_archive_recovery_waits_before_locking_lease(postgres_url: str) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    store = FakeObjectStore()
    recovery = None
    try:
        now = datetime.now(UTC)
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            lease = await _reserve(session, trial_id=trial_id, target=target, now=now)
            await session.commit()
        pid_ready = asyncio.Future()

        class ObservedSession(type(session)):
            async def _identify(self):
                if not pid_ready.done():
                    pid_ready.set_result(await super().scalar(text("SELECT pg_backend_pid()")))

            async def get(self, *args, **kwargs):
                await self._identify()
                return await super().get(*args, **kwargs)

            async def execute(self, *args, **kwargs):
                await self._identify()
                return await super().execute(*args, **kwargs)

        observed = async_sessionmaker(engine, class_=ObservedSession, expire_on_commit=False)
        materializer = ServiceExecutionMaterializer(
            session_factory=observed, source_store=store, source_bucket="artifacts",
            canonical_store=store, artifacts_bucket="artifacts", trajectories_bucket="trajectories",
        )
        async with sessions() as cancelling:
            await cancelling.get(Trial, trial_id, with_for_update=True)
            recovery = asyncio.create_task(materializer.retry_usage_roundoff_archive(
                lease_id=lease.id, team_id=lease.team_id,
            ))
            pid = await pid_ready
            async with sessions() as inspector:
                async with asyncio.timeout(5):
                    while await inspector.scalar(text(
                        "SELECT wait_event_type FROM pg_stat_activity WHERE pid=:pid"
                    ), {"pid": pid}) != "Lock":
                        await asyncio.sleep(0.01)
                        await inspector.rollback()
            await cancelling.execute(text("SET LOCAL lock_timeout='250ms'"))
            assert await cancelling.get(ServiceExecutionLease, lease.id, with_for_update=True) is not None
            await cancelling.commit()
        assert await asyncio.wait_for(recovery, 5) is False
    finally:
        if recovery is not None:
            if not recovery.done():
                recovery.cancel()
            await asyncio.gather(recovery, return_exceptions=True)
        await engine.dispose()
