"""Observer samples preserve their source and original clocks through cleanup."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import TrialResourceUsage
from loom.models.resource_usage import ResourceCounters, aggregate_resource_usage
from loom.resource_usage_store import row_to_report
from loom_control_plane.service_execution import request_trial_execution_cancellation
from loom_execution_actuator.contracts import NormalizedJobState
from loom_execution_actuator.controller import ExecutionActuator
from loom_execution_actuator.renderer import ExecutionTargetRuntime
from loom_execution_actuator.resource_usage import ResourceSample, persist_native_usage
from tests.integration.test_service_execution_leases import (
    _cleanup_service_execution_test_rows,  # noqa: F401
    _FakeKubernetesJobApi,
    _reserve,
    _seed_ready_trial,
)


@pytest.mark.parametrize("prior_source", [False, True])
async def test_observer_usage_source_original_clocks_and_cleanup(postgres_url, prior_source):
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    sampled_at = now - timedelta(seconds=10)
    started_at = now - timedelta(seconds=30)
    pod_uid = str(uuid4())
    kubernetes = _FakeKubernetesJobApi()

    async def resource_summary(*, node_name):
        return {
            "node": {"nodeName": node_name},
            "_native_observation": {"observed_at": sampled_at.isoformat(), "max_age_seconds": 60},
            "pods": [
                {
                    "podRef": {"uid": pod_uid, "namespace": target.namespace_name},
                    "containers": [
                        {
                            "name": "execution",
                            "startTime": started_at.isoformat(),
                            "cpu": {
                                "usageCoreNanoSeconds": 4000000,
                                "time": sampled_at.isoformat(),
                            },
                            "memory": {"usageBytes": 100, "time": sampled_at.isoformat()},
                        }
                    ],
                    "ephemeral-storage": {"usedBytes": 300, "time": sampled_at.isoformat()},
                }
            ],
        }

    kubernetes.resource_summary = resource_summary
    try:
        async with sessions() as session, session.begin():
            trial_id, target = await _seed_ready_trial(session, now=now)
            lease = await _reserve(session, trial_id=trial_id, target=target, now=now)
        actuator = ExecutionActuator(
            sessions=sessions,
            kubernetes=kubernetes,
            target=ExecutionTargetRuntime(
                target_id=target.target_id, namespace=target.namespace_name
            ),
            controller_id="observer-usage-test",
            resource_usage_source="native_node_observer",
        )
        await actuator.run_commands_once(now=now)
        observation = kubernetes.jobs[lease.job_name].model_copy(
            update={
                "normalized_state": NormalizedJobState.RUNNING,
                "node_name": "node-1",
                "pod_uid": pod_uid,
                "resource_version": "2",
            }
        )
        kubernetes.jobs[lease.job_name] = observation
        if prior_source:
            # Establish the same Pod identity without sampling, then persist a
            # previous source explicitly to exercise a bounded source transition.
            async with sessions() as session, session.begin():
                current = await session.get(type(lease), lease.id)
                current.pod_uid = pod_uid
                current.job_uid = observation.job_uid
            fresh = await actuator._lease(lease.id)
            async with sessions() as session, session.begin():
                await persist_native_usage(
                    session,
                    lease=fresh,
                    observation=observation,
                    samples={
                        "execution": ResourceSample(ResourceCounters(cpu_usage_usec=10), started_at)
                    },
                    now=now,
                    terminal=False,
                )
        await actuator.reconcile_full_once(now=now + timedelta(seconds=1))
        fresh = await actuator._lease(lease.id)
        async with sessions() as session, session.begin():
            await persist_native_usage(
                session,
                lease=fresh,
                observation=observation,
                samples={
                    "execution": ResourceSample(
                        ResourceCounters(cpu_usage_usec=2000, memory_current_bytes=999),
                        started_at,
                        sampled_at - timedelta(seconds=1),
                    )
                },
                now=now + timedelta(seconds=2),
                terminal=False,
                source="native_node_observer",
            )
        async with sessions() as session:
            rows = list(
                (
                    await session.scalars(
                        select(TrialResourceUsage).where(
                            TrialResourceUsage.execution_lease_id == lease.id,
                            TrialResourceUsage.role_name == "execution",
                        )
                    )
                ).all()
            )
            main = next(row for row in rows if row.source == "native_node_observer")
            assert main.cpu_usage_usec == 4000
            assert main.memory_current_bytes == 100
            assert main.last_observed_at == sampled_at
            assert main.first_observed_at == sampled_at - timedelta(seconds=1)
            assert main.diagnostic_code == "sampled_maxima_not_kernel_peaks"
            assert len(rows) == (2 if prior_source else 1)
            if prior_source:
                previous = next(row for row in rows if row.source == "kubelet_summary")
                assert previous.finalized_at is not None
                assert previous.terminal_reason == "observation_source_changed"
                assert previous.cpu_usage_usec == 10
                assert aggregate_resource_usage([row_to_report(row) for row in rows])["cpu_usage_usec"] == 4000
        terminal_at = now + timedelta(seconds=3)
        async with sessions() as session, session.begin():
            await request_trial_execution_cancellation(session, trial_id=trial_id, now=terminal_at)
        fresh = await actuator._lease(lease.id)
        await actuator._delete(fresh, observation, now=terminal_at, cancel_immediately=True)
        assert lease.job_name not in kubernetes.jobs
        async with sessions() as session:
            rows = list(
                (
                    await session.scalars(
                        select(TrialResourceUsage).where(
                            TrialResourceUsage.execution_lease_id == lease.id,
                        )
                    )
                ).all()
            )
            assert all(row.finalized_at is not None for row in rows)
            main = next(
                row
                for row in rows
                if row.source == "native_node_observer" and row.role_name == "execution"
            )
            assert main.completeness == "partial"
            assert main.cpu_usage_usec == 4000
            assert main.last_observed_at == sampled_at
    finally:
        await engine.dispose()
