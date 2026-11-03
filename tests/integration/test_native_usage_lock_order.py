"""Native telemetry must not hold a lease while its insert waits on Trial FK."""
from __future__ import annotations

import asyncio
from datetime import UTC, datetime

from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import Trial
from loom.models.resource_usage import ResourceCounters
from loom_control_plane.service_execution import enqueue_execution_transition
from loom_execution_actuator.contracts import (
    ActuatorContractError,
    KubernetesJobObservation,
    NormalizedJobState,
)
from loom_execution_actuator.resource_usage import ResourceSample, persist_native_usage
from tests.integration.test_service_execution_leases import (
    _cleanup_service_execution_test_rows,  # noqa: F401
    _reserve,
    _seed_ready_trial,
)


async def test_cancellation_can_fence_a_usage_insert_waiting_on_trial(postgres_url: str) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    worker = None
    try:
        now = datetime.now(UTC)
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            lease = await _reserve(session, trial_id=trial_id, target=target, now=now)
            lease.job_uid, lease.pod_uid = "usage-lock-job", "usage-lock-pod"
            await session.commit()
        observation = KubernetesJobObservation(
            namespace=lease.namespace_name, job_name=lease.job_name, lease_id=str(lease.id),
            resource_generation=lease.resource_generation, target_id=lease.target_id,
            execution_unit_key=str(lease.execution_unit_key), normalized_state=NormalizedJobState.RUNNING,
            job_uid=lease.job_uid, pod_uid=lease.pod_uid,
        )
        pid_ready = asyncio.Future()
        fenced = asyncio.Event()

        async def sample() -> None:
            async with sessions() as session:
                pid_ready.set_result(await session.scalar(text("SELECT pg_backend_pid()")))
                try:
                    await persist_native_usage(
                        session, lease=lease, observation=observation,
                        samples={"execution": ResourceSample(ResourceCounters(cpu_usage_usec=10))},
                        now=now, terminal=False,
                    )
                    await session.commit()
                except ActuatorContractError:
                    fenced.set()
                    await session.rollback()

        async with sessions() as cancelling:
            await cancelling.get(Trial, trial_id, with_for_update=True)
            worker = asyncio.create_task(sample())
            pid = await pid_ready
            async with sessions() as inspector:
                async with asyncio.timeout(5):
                    while await inspector.scalar(text(
                        "SELECT wait_event_type FROM pg_stat_activity WHERE pid=:pid"
                    ), {"pid": pid}) != "Lock":
                        await asyncio.sleep(0.01)
                        await inspector.rollback()
            await cancelling.execute(text("SET LOCAL lock_timeout='250ms'"))
            await enqueue_execution_transition(
                cancelling, lease_id=lease.id, expected_generation=lease.generation,
                desired_state="cancel", now=now,
            )
            await cancelling.commit()
        await asyncio.wait_for(worker, 5)
        assert fenced.is_set(), "a post-cancel sample must recheck its changed generation"
    finally:
        if worker is not None:
            if not worker.done():
                worker.cancel()
            await asyncio.gather(worker, return_exceptions=True)
        await engine.dispose()
