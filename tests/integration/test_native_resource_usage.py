"""Native lease accounting survives cleanup without a fabricated worker."""

from datetime import UTC, datetime, timedelta

import httpx
import pytest
from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import TrialResourceUsage
from loom.models.resource_usage import ResourceCounters, aggregate_resource_usage
from loom.resource_usage_store import row_to_report
from loom_control_plane.service_execution import request_trial_execution_cancellation
from loom_execution_actuator.contracts import ActuatorContractError, NormalizedJobState
from loom_execution_actuator.controller import ExecutionActuator
from loom_execution_actuator.renderer import ExecutionTargetRuntime
from loom_execution_actuator.resource_usage import ResourceSample, persist_native_usage, pod_samples
from tests.integration.test_service_execution_leases import (
    _cleanup_service_execution_test_rows,  # noqa: F401
    _FakeKubernetesJobApi,
    _reserve,
    _seed_ready_trial,
)


def test_kubelet_sample_uid_fence_and_honest_maxima():
    summary = {
        "pods": [
            {"podRef": {"uid": "other", "namespace": "ns"}, "containers": []},
            {
                "podRef": {"uid": "uid", "namespace": "ns"},
                "containers": [
                    {
                        "name": "execution",
                        "cpu": {"usageCoreNanoSeconds": 4000000, "usageNanoCores": 70},
                        "memory": {"usageBytes": 100},
                        "rootfs": {"usedBytes": 30},
                        "logs": {"usedBytes": 10},
                    }
                ],
                "ephemeral-storage": {"usedBytes": 300},
                "volume": [{"name": "workspace", "usedBytes": 260}],
            },
        ]
    }
    assert pod_samples(summary, namespace="wrong", pod_uid="uid") == {}
    assert pod_samples(summary, namespace="ns", pod_uid="absent") == {}
    samples = pod_samples(summary, namespace="ns", pod_uid="uid")
    assert samples["execution"].counters.cpu_usage_usec == 4000
    assert samples["execution"].counters.cpu_sampled_max_nanocores == 70
    assert samples["execution"].counters.memory_sampled_max_bytes == 100
    assert samples["execution"].counters.memory_peak_bytes is None
    assert samples["execution"].counters.cpu_throttled_usec is None
    assert samples["execution"].counters.filesystem_sampled_max_bytes == 40
    assert samples["pod"].counters.ephemeral_storage_sampled_max_bytes == 300
    assert samples["pod"].counters.memory_sampled_max_bytes is None


async def test_native_samples_finalize_before_delete_and_retain(postgres_url):
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    kubernetes = _FakeKubernetesJobApi()
    reads = []

    async def resource_summary(*, node_name):
        reads.append(node_name)
        obs = next(iter(kubernetes.jobs.values()))
        return {
            "pods": [
                {
                    "podRef": {"uid": obs.pod_uid, "namespace": obs.namespace},
                    "containers": [
                        {
                            "name": "execution",
                            "cpu": {"usageCoreNanoSeconds": 9000000},
                            "memory": {"usageBytes": 300},
                        }
                    ],
                    "ephemeral-storage": {"usedBytes": 500},
                }
            ]
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
            controller_id="native-usage-test",
        )
        await actuator.run_commands_once(now=now)
        obs = kubernetes.jobs[lease.job_name].model_copy(
            update={
                "normalized_state": NormalizedJobState.RUNNING,
                "node_name": "node-1",
                "pod_uid": "pod-1",
                "resource_version": "2",
            }
        )
        kubernetes.jobs[lease.job_name] = obs
        await actuator.reconcile_full_once(now=now + timedelta(seconds=1))
        await actuator.reconcile_full_once(now=now + timedelta(seconds=2))
        assert len(reads) == 1  # bounded per-node cache, duplicate watch/repair observations
        async with sessions() as session:
            rows = list(
                (
                    await session.scalars(
                        select(TrialResourceUsage).where(TrialResourceUsage.trial_id == trial_id)
                    )
                ).all()
            )
            main = next(row for row in rows if row.role_name == "execution")
            assert main.worker_id is None
            assert main.execution_lease_id == lease.id
            assert main.cpu_usage_usec == 9000
            assert main.memory_sampled_max_bytes == 300
            assert main.memory_peak_bytes is None
            async with session.begin_nested():
                with pytest.raises(IntegrityError):
                    async with session.begin_nested():
                        await session.execute(
                            update(TrialResourceUsage)
                            .where(TrialResourceUsage.id == main.id)
                            .values(resource_generation=None)
                        )
            assert main.finalized_at is None
            assert (
                aggregate_resource_usage([row_to_report(row) for row in rows])[
                    "pod_ephemeral_storage_sampled_max_sum_bytes"
                ]
                == 500
            )
        fresh = await actuator._lease(lease.id)
        async with sessions() as session:
            with pytest.raises(ActuatorContractError, match="identity changed"):
                await persist_native_usage(
                    session,
                    lease=fresh,
                    observation=obs.model_copy(update={"pod_uid": "foreign"}),
                    samples={"execution": ResourceSample(ResourceCounters(cpu_usage_usec=99999))},
                    now=now + timedelta(seconds=3),
                    terminal=False,
                )
        async with sessions() as session, session.begin():
            await request_trial_execution_cancellation(
                session, trial_id=trial_id, now=now + timedelta(seconds=3)
            )
        fresh = await actuator._lease(lease.id)
        # Same UID deletion captures/finalizes before the Kubernetes object disappears.
        await actuator._delete(fresh, obs, now=now + timedelta(seconds=4), cancel_immediately=True)
        async with sessions() as session:
            rows = list(
                (
                    await session.scalars(
                        select(TrialResourceUsage).where(TrialResourceUsage.trial_id == trial_id)
                    )
                ).all()
            )
            main = next(row for row in rows if row.role_name == "execution")
            assert main.finalized_at == now + timedelta(seconds=4)
            assert main.completeness == "partial"
            assert all(row.finalized_at is not None for row in rows)
            assert (
                next(row for row in rows if row.role_name == "runtime-materializer").completeness
                == "unavailable"
            )
            await persist_native_usage(
                session,
                lease=fresh,
                observation=obs,
                samples={"execution": ResourceSample(ResourceCounters(cpu_usage_usec=99999))},
                now=now + timedelta(seconds=5),
                terminal=True,
            )
            await session.commit()
        async with sessions() as session:
            main = await session.scalar(
                select(TrialResourceUsage).where(
                    TrialResourceUsage.trial_id == trial_id,
                    TrialResourceUsage.role_name == "execution",
                )
            )
            assert main.cpu_usage_usec == 9000
            assert main.finalized_at == now + timedelta(seconds=4)
    finally:
        await engine.dispose()


async def test_telemetry_write_failure_cannot_block_primary_cleanup(
    postgres_url, monkeypatch, caplog
):
    from sqlalchemy import text

    from loom_execution_actuator.metrics import RESOURCE_USAGE_ERRORS_TOTAL

    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    kubernetes = _FakeKubernetesJobApi()

    async def broken_usage(session, **kwargs):
        # Real DB transaction failure, not merely a mocked return value.
        await session.execute(text("SELECT 1 / 0"))

    monkeypatch.setattr("loom_execution_actuator.controller.persist_native_usage", broken_usage)
    before = RESOURCE_USAGE_ERRORS_TOTAL.labels(operation="persist")._value.get()
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
            controller_id="usage-failure-test",
        )
        await actuator.run_commands_once(now=now)
        obs = kubernetes.jobs[lease.job_name].model_copy(
            update={"pod_uid": "pod-1", "normalized_state": NormalizedJobState.RUNNING}
        )
        kubernetes.jobs[lease.job_name] = obs
        await actuator.reconcile_full_once(now=now + timedelta(seconds=1))
        fresh = await actuator._lease(lease.id)
        assert fresh.pod_uid == "pod-1"  # primary observation committed despite usage SQL failure
        async with sessions() as session, session.begin():
            await request_trial_execution_cancellation(
                session, trial_id=trial_id, now=now + timedelta(seconds=2)
            )
        fresh = await actuator._lease(lease.id)
        await actuator._delete(fresh, obs, now=now + timedelta(seconds=3), cancel_immediately=True)
        assert lease.job_name not in kubernetes.jobs
        assert RESOURCE_USAGE_ERRORS_TOTAL.labels(operation="persist")._value.get() > before
        assert "Native resource persistence failed" in caplog.text
        assert "division by zero" not in caplog.text  # no raw exception/payload persistence
    finally:
        await engine.dispose()


async def test_native_container_restart_retains_separate_cumulative_counters(postgres_url):
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    kubernetes = _FakeKubernetesJobApi()
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
            controller_id="usage-restart-test",
        )
        await actuator.run_commands_once(now=now)
        obs = kubernetes.jobs[lease.job_name].model_copy(
            update={"pod_uid": "pod-1", "normalized_state": NormalizedJobState.RUNNING}
        )
        await actuator._persist_observation(lease, obs, now=now)
        fresh = await actuator._lease(lease.id)
        for seconds, usage in [(1, 9000), (2, 4000)]:
            started = now + timedelta(seconds=seconds)
            async with sessions() as session, session.begin():
                await persist_native_usage(
                    session,
                    lease=fresh,
                    observation=obs,
                    samples={
                        "execution": ResourceSample(ResourceCounters(cpu_usage_usec=usage), started)
                    },
                    now=started,
                    terminal=False,
                )
        async with sessions() as session, session.begin():
            await persist_native_usage(
                session,
                lease=fresh,
                observation=obs,
                samples={},
                now=now + timedelta(seconds=3),
                terminal=True,
                diagnostic="pod_stats_unavailable",
            )
        async with sessions() as session:
            rows = list(
                (
                    await session.scalars(
                        select(TrialResourceUsage).where(
                            TrialResourceUsage.trial_id == trial_id,
                            TrialResourceUsage.role_name == "execution",
                        )
                    )
                ).all()
            )
            measured = [row for row in rows if row.cpu_usage_usec is not None]
            assert len(measured) == 2
            placeholder = next(row for row in rows if row.container_started_at is None)
            assert placeholder.completeness == "unavailable"
            assert placeholder.terminal_reason == "first_container_observation"
            assert placeholder.diagnostic_code == "prestart_sample_unavailable"
            restarted = next(
                row for row in measured if row.container_started_at == now + timedelta(seconds=1)
            )
            assert restarted.terminal_reason == "container_restarted"
            assert restarted.diagnostic_code == "container_incarnation_changed"
            assert {row.container_started_at for row in measured} == {
                now + timedelta(seconds=1),
                now + timedelta(seconds=2),
            }
            assert all(row.finalized_at is not None for row in rows)
            assert (
                aggregate_resource_usage([row_to_report(row) for row in measured])["cpu_usage_usec"]
                == 13000
            )
            assert all(row.image_digest is not None for row in measured)
    finally:
        await engine.dispose()


@pytest.mark.parametrize(("recover", "reset", "terminal_failure"), [
    (False, False, False), (True, False, False), (True, True, False),
    (True, False, True), (True, True, True),
])
async def test_sampling_diagnostic_survives_cleanup_and_clears_after_recovery(
    postgres_url, recover, reset, terminal_failure
):
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    kubernetes = _FakeKubernetesJobApi()
    reads = 0

    async def resource_summary(*, node_name):
        nonlocal reads
        reads += 1
        if reads == 1 or (terminal_failure and reads > (4 if reset else 2)):
            raise httpx.ConnectTimeout("private transport details must not be persisted")
        if not recover:
            raise httpx.ReadTimeout("private transport details must not be persisted")
        obs = next(iter(kubernetes.jobs.values()))
        usage = {2: 9000000, 3: 4000000, 4: 10000000}[reads]
        return {"pods": [{
            "podRef": {"uid": obs.pod_uid, "namespace": obs.namespace},
            "containers": [{"name": "execution", "cpu": {"usageCoreNanoSeconds": usage}}],
        }]}

    kubernetes.resource_summary = resource_summary
    try:
        async with sessions() as session, session.begin():
            trial_id, target = await _seed_ready_trial(session, now=now)
            lease = await _reserve(session, trial_id=trial_id, target=target, now=now)
        actuator = ExecutionActuator(
            sessions=sessions, kubernetes=kubernetes,
            target=ExecutionTargetRuntime(target_id=target.target_id, namespace=target.namespace_name),
            controller_id="usage-diagnostic-test",
        )
        await actuator.run_commands_once(now=now)
        obs = kubernetes.jobs[lease.job_name].model_copy(update={
            "normalized_state": NormalizedJobState.RUNNING,
            "node_name": "node-1", "pod_uid": "pod-1", "resource_version": "2",
        })
        kubernetes.jobs[lease.job_name] = obs
        await actuator.reconcile_full_once(now=now + timedelta(seconds=1))
        await actuator.reconcile_full_once(now=now + timedelta(seconds=2))
        assert reads == 1
        async with sessions() as session:
            row = await session.scalar(select(TrialResourceUsage).where(
                TrialResourceUsage.execution_lease_id == lease.id,
                TrialResourceUsage.role_name == "execution",
            ))
            assert row.diagnostic_code == "kubelet_connect_timeout"
            assert row.cpu_usage_usec is None
        if recover:
            await actuator.reconcile_full_once(now=now + timedelta(seconds=17))
            assert reads == 2
        if reset:
            await actuator.reconcile_full_once(now=now + timedelta(seconds=33))
            await actuator.reconcile_full_once(now=now + timedelta(seconds=49))
            assert reads == 4
        if not recover:
            await actuator.reconcile_full_once(now=now + timedelta(seconds=17))
            async with sessions() as session:
                row = await session.scalar(select(TrialResourceUsage).where(
                    TrialResourceUsage.execution_lease_id == lease.id,
                    TrialResourceUsage.role_name == "execution",
                ))
                assert row.diagnostic_code == "kubelet_read_timeout"
        seconds = 66 if reset else 34 if recover else 18
        if not terminal_failure:
            seconds = 50 if reset else 18
        terminal_at = now + timedelta(seconds=seconds)
        async with sessions() as session, session.begin():
            await request_trial_execution_cancellation(session, trial_id=trial_id, now=terminal_at)
        fresh = await actuator._lease(lease.id)
        await actuator._delete(fresh, obs, now=terminal_at, cancel_immediately=True)
        assert lease.job_name not in kubernetes.jobs
        async with sessions() as session:
            rows = list((await session.scalars(select(TrialResourceUsage).where(
                TrialResourceUsage.execution_lease_id == lease.id,
            ))).all())
            main = next(row for row in rows if row.role_name == "execution")
            assert all(row.finalized_at is not None for row in rows)
            assert main.diagnostic_code == (
                "counter_reset_without_new_incarnation" if reset
                else "kubelet_connect_timeout" if terminal_failure
                else "sampled_maxima_not_kernel_peaks" if recover else "kubelet_read_timeout"
            )
            assert main.cpu_usage_usec == (10000 if reset else 9000 if recover else None)
            assert main.completeness == ("partial" if recover else "unavailable")
            assert main.finalized_at == terminal_at
    finally:
        await engine.dispose()
