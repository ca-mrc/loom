"""Native terminal failures converge without inventing runtime results."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import (
    ExecutionAdmissionReservation,
    ExecutionCostReservation,
    ExecutionProvisioningAuthorization,
    ServiceExecutionLease,
    Trial,
)
from loom_execution_actuator.contracts import NormalizedJobState
from loom_execution_actuator.controller import ExecutionActuator
from loom_execution_actuator.renderer import ExecutionTargetRuntime
from tests.integration.test_service_execution_leases import (
    _cleanup_service_execution_test_rows,  # noqa: F401 -- shared database cleanup
    _FakeKubernetesJobApi,
    _reserve,
    _seed_ready_trial,
)


@pytest.mark.parametrize("uploading", [False, True])
async def test_native_start_failure_closes_output_then_releases_trial(postgres_url, uploading):
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    kubernetes = _FakeKubernetesJobApi()
    try:
        async with sessions() as session, session.begin():
            trial_id, target = await _seed_ready_trial(session, now=now)
            lease = await _reserve(session, trial_id=trial_id, target=target, now=now)
            if uploading:
                lease.output_commit_state = "uploading"
                lease.output_upload_session_id = uuid4()
                lease.output_generation = lease.resource_generation
        actuator = ExecutionActuator(
            sessions=sessions,
            kubernetes=kubernetes,
            target=ExecutionTargetRuntime(
                target_id=target.target_id, namespace=target.namespace_name
            ),
            controller_id="native-failure-test",
            command_lease_seconds=5,
        )
        assert await actuator.run_commands_once(now=now) == 1
        failed_at = now + timedelta(seconds=1)
        kubernetes.jobs[lease.job_name] = kubernetes.jobs[lease.job_name].model_copy(
            update={
                "normalized_state": NormalizedJobState.FAILED,
                "reason": "BackoffLimitExceeded",
                "message": "Job has reached the specified backoff limit",
                "resource_version": "2",
            }
        )
        await actuator.reconcile_full_once(now=failed_at)
        # Re-observation, even with a new resource version, must not reset grace.
        kubernetes.jobs[lease.job_name] = kubernetes.jobs[lease.job_name].model_copy(
            update={
                "resource_version": "3",
            }
        )
        await actuator.reconcile_full_once(now=failed_at + timedelta(seconds=299))
        async with sessions() as session:
            current = await session.get(ServiceExecutionLease, lease.id)
            trial = await session.get(Trial, trial_id)
            assert trial.state == "claimed"
            assert current.revoked_at is None
            assert current.output_commit_state == ("uploading" if uploading else "not_started")
        await actuator.reconcile_full_once(now=failed_at + timedelta(minutes=5))
        async with sessions() as session:
            current = await session.get(ServiceExecutionLease, lease.id)
            trial = await session.get(Trial, trial_id)
            assert trial.state == "failed"
            assert trial.result is None
            assert trial.failure_reason == "native_execution_failed"
            assert "BackoffLimitExceeded" in trial.failure_message
            assert trial.started_at is None
            assert trial.finished_at == failed_at + timedelta(minutes=5)
            assert current.output_commit_state == "unavailable"
            assert current.output_manifest_sha256 is None
            assert current.output_marker_sha256 is None
            assert current.desired_state == "delete_pending"
            assert current.error_class == "permanent"
            assert current.error_code == "failed"
            for model in (
                ExecutionAdmissionReservation,
                ExecutionCostReservation,
                ExecutionProvisioningAuthorization,
            ):
                owner = model.owner_id if model is ExecutionAdmissionReservation else model.lease_id
                row = await session.scalar(select(model).where(owner == lease.id))
                assert row.state == "released"
        assert (
            await actuator.run_commands_once(now=failed_at + timedelta(minutes=5, seconds=1)) == 1
        )
        await actuator.reconcile_full_once(now=failed_at + timedelta(minutes=5, seconds=2))
        await actuator.reconcile_full_once(now=failed_at + timedelta(minutes=5, seconds=3))
        async with sessions() as session:
            current = await session.get(ServiceExecutionLease, lease.id)
            trial = await session.get(Trial, trial_id)
            assert (current.desired_state, current.cleanup_state) == ("deleted", "complete")
            assert trial.state == "failed" and trial.result is None
        assert kubernetes.create_count == 1 and kubernetes.delete_count == 1
    finally:
        await engine.dispose()


@pytest.mark.parametrize("window_expired", [False, True])
async def test_output_commit_during_native_failure_grace_keeps_authority(
    postgres_url, monkeypatch, window_expired
):
    """Exercise real multipart prepare/upload/commit with failure before commit."""
    from loom_control_plane.service_execution import (
        finalize_failed_service_execution,
        record_kubernetes_observation,
    )
    from loom_control_plane.service_execution_output import (
        ServiceExecutionBrokerError,
        ServiceExecutionOutputRouteService,
    )
    from tests.integration.test_service_execution_leases import (
        test_observed_pod_broker_commits_semantic_runtime_output,
    )

    class OutputWindowClosedError(Exception):
        pass

    original = ServiceExecutionOutputRouteService.commit
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)

    async def commit_after_native_failure(self, *, lease, session_id, upload_token):
        failed_at = datetime.now(UTC)
        async with sessions() as session, session.begin():
            current = await session.get(ServiceExecutionLease, lease.id)
            if current.output_commit_state == "committed":
                return await original(
                    self, lease=lease, session_id=session_id, upload_token=upload_token
                )
            assert current.output_commit_state == "uploading"
            await record_kubernetes_observation(
                session,
                lease_id=lease.id,
                generation=lease.generation,
                observed_at=failed_at,
                payload={
                    "normalized_state": "failed",
                    "job_uid": current.job_uid,
                    "pod_uid": current.pod_uid,
                    "resource_version": "late-output-failure",
                    "started_at": (failed_at - timedelta(seconds=1)).isoformat(),
                    "reason": "BackoffLimitExceeded",
                    "message": "native Job terminated",
                },
            )
            assert not await finalize_failed_service_execution(
                session,
                lease_id=lease.id,
                generation=lease.generation,
                observed_at=failed_at + timedelta(seconds=299),
            )
        if window_expired:
            async with sessions() as session, session.begin():
                assert await finalize_failed_service_execution(
                    session,
                    lease_id=lease.id,
                    generation=lease.generation,
                    observed_at=failed_at + timedelta(minutes=5),
                )
            with pytest.raises(ServiceExecutionBrokerError, match="execution_generation_fenced"):
                await original(self, lease=lease, session_id=session_id, upload_token=upload_token)
            async with sessions() as session:
                current = await session.get(ServiceExecutionLease, lease.id)
                trial = await session.get(Trial, lease.trial_id)
                assert current.output_commit_state == "unavailable"
                assert trial.state == "failed" and trial.result is None
                cost = await session.scalar(
                    select(ExecutionCostReservation).where(
                        ExecutionCostReservation.lease_id == lease.id,
                    )
                )
                assert cost.state == "awaiting_settlement"
            raise OutputWindowClosedError
        result = await original(self, lease=lease, session_id=session_id, upload_token=upload_token)
        async with sessions() as session:
            current = await session.get(ServiceExecutionLease, lease.id)
            assert current.output_commit_state == "committed"
            assert not await finalize_failed_service_execution(
                session,
                lease_id=lease.id,
                generation=current.generation,
                observed_at=failed_at + timedelta(minutes=5),
            )
        return result

    monkeypatch.setattr(ServiceExecutionOutputRouteService, "commit", commit_after_native_failure)
    try:
        if window_expired:
            with pytest.raises(OutputWindowClosedError):
                await test_observed_pod_broker_commits_semantic_runtime_output(
                    postgres_url,
                    NormalizedJobState.FAILED,
                    extra_artifacts=0,
                )
        else:
            await test_observed_pod_broker_commits_semantic_runtime_output(
                postgres_url,
                NormalizedJobState.FAILED,
                extra_artifacts=0,
            )
    finally:
        await engine.dispose()


@pytest.mark.parametrize("transition", ["cancel", "retry"])
async def test_native_failure_does_not_take_over_revoked_attempt(postgres_url, transition):
    from loom_control_plane.service_execution import (
        ServiceExecutionFenceError,
        enqueue_execution_transition,
        finalize_failed_service_execution,
        record_kubernetes_observation,
    )

    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    try:
        async with sessions() as session, session.begin():
            trial_id, target = await _seed_ready_trial(session, now=now)
            lease = await _reserve(session, trial_id=trial_id, target=target, now=now)
            await record_kubernetes_observation(
                session,
                lease_id=lease.id,
                generation=1,
                observed_at=now,
                payload={
                    "normalized_state": "failed",
                    "job_uid": "job-original",
                    "reason": "BackoffLimitExceeded",
                },
            )
            await enqueue_execution_transition(
                session,
                lease_id=lease.id,
                expected_generation=1,
                desired_state=transition,
                now=now,
            )
        async with sessions() as session:
            with pytest.raises(ServiceExecutionFenceError):
                await finalize_failed_service_execution(
                    session,
                    lease_id=lease.id,
                    generation=1,
                    observed_at=now + timedelta(minutes=6),
                )
            await session.rollback()
            assert not await finalize_failed_service_execution(
                session,
                lease_id=lease.id,
                generation=2,
                observed_at=now + timedelta(minutes=6),
            )
            current = await session.get(ServiceExecutionLease, lease.id)
            trial = await session.get(Trial, trial_id)
            assert current.desired_state == transition
            assert trial.state == ("queued" if transition == "retry" else "claimed")
            assert trial.finished_at is None and trial.result is None
    finally:
        await engine.dispose()


async def test_job_only_backoff_preserves_exact_pod_eviction_reason(postgres_url):
    from loom_control_plane.service_execution import record_kubernetes_observation

    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    try:
        async with sessions() as session, session.begin():
            trial_id, target = await _seed_ready_trial(session, now=now)
            lease = await _reserve(session, trial_id=trial_id, target=target, now=now)
            for index, payload in enumerate(
                (
                    {
                        "normalized_state": "evicted",
                        "pod_uid": "exact-pod",
                        "reason": "DeletionByTaintManager",
                        "message": "taint manager eviction",
                    },
                    {
                        "normalized_state": "failed",
                        "pod_uid": None,
                        "reason": "BackoffLimitExceeded",
                        "message": "backoff exhausted",
                    },
                )
            ):
                await record_kubernetes_observation(
                    session,
                    lease_id=lease.id,
                    generation=1,
                    observed_at=now + timedelta(seconds=index),
                    payload={"job_uid": "exact-job", "resource_version": str(index), **payload},
                )
                assert (lease.error_code, lease.error_class) == ("evicted", "transient")
                assert lease.error_message == "taint manager eviction"
            # An explicit Pod outcome replaces the older explanation.
            await record_kubernetes_observation(
                session,
                lease_id=lease.id,
                generation=1,
                observed_at=now + timedelta(seconds=2),
                payload={
                    "job_uid": "exact-job",
                    "resource_version": "2",
                    "pod_uid": "exact-pod",
                    "normalized_state": "oom_killed",
                    "reason": "OOMKilled",
                    "message": "memory limit",
                },
            )
            assert (lease.error_code, lease.error_class) == ("oom_killed", "permanent")
    finally:
        await engine.dispose()
