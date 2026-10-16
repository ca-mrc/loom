from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import ServiceExecutionCommand, ServiceExecutionLease, Trial
from loom.nebius_rollout_guard import LOCK_KEY
from loom_control_plane import terminal_cleanup_recovery as cleanup_worker
from loom_control_plane.pending_archive_recovery import RecoveryRefusedError
from loom_control_plane.service_execution import (
    acknowledge_execution_command,
    claim_execution_commands,
    defer_execution_command,
    enqueue_execution_transition,
)
from loom_control_plane.terminal_cleanup_recovery import (
    TerminalCleanupRecoveryRequest,
    capture_cleanup_binding,
    reconcile_cleanup,
)
from loom_execution_actuator.contracts import KubernetesJobObservation, NormalizedJobState
from loom_execution_actuator.controller import ExecutionActuator
from loom_execution_actuator.renderer import ExecutionTargetRuntime
from tests.integration.conftest import retain_pool_schema
from tests.integration.test_delete_pending_reconciliation import _retained_projection
from tests.integration.test_service_execution_leases import (
    _cleanup_service_execution_test_rows,  # noqa: F401
    _FakeKubernetesJobApi,
    _reserve,
    _seed_ready_trial,
)


class ReadbackKubernetes(_FakeKubernetesJobApi):
    get_count = 0
    guard_sessions = None
    close_count = 0

    async def close(self):
        self.close_count += 1

    async def get_job(self, *, namespace, job_name):
        self.get_count += 1
        return await super().get_job(namespace=namespace, job_name=job_name)

    async def delete_job(self, **kwargs):
        assert self.guard_sessions is not None
        # The normal shared admission lock must span the provider mutation:
        # another transaction cannot acquire exclusive rollout authority here.
        async with self.guard_sessions.begin() as session:
            assert not await session.scalar(
                text("SELECT pg_try_advisory_xact_lock(:key)"),
                {"key": LOCK_KEY},
            )
        await super().delete_job(**kwargs)


async def _seed_cleanup(sessions, now, *, failed=False):
    async with sessions.begin() as session:
        trial_id, target = await _seed_ready_trial(session, now=now)
        lease = await _reserve(session, trial_id=trial_id, target=target, now=now)
        lease.job_uid, lease.pod_uid = str(uuid4()), str(uuid4())
        lease.finalized_at = now
        lease.observed_state = "finalized"
        lease.output_commit_state = "committed"
        lease.output_upload_session_id = uuid4()
        lease.output_generation = lease.resource_generation
        lease.output_manifest_sha256 = "sha256:" + "a" * 64
        lease.output_marker_sha256 = "sha256:" + "b" * 64
        lease.output_committed_at = now
        lease.materialization_state = "committed"
        lease.materialization_attempts = 1
        lease.materialization_committed_at = now
        lease.canonical_trajectory_sha256 = "sha256:" + "c" * 64
        lease.canonical_atif_sha256 = "sha256:" + "d" * 64
        lease.source_cleanup_state = "retained"
        lease.source_retain_until = now + timedelta(days=1)
        trial = await session.get(Trial, trial_id)
        assert trial is not None
        trial.state, trial.finished_at = "failed" if failed else "succeeded", now
        trial.result = {"reward": {"main": 1.0}, "aggregate_reward": 1.0}
        if failed:
            trial.failure_reason = "task_failed"
            trial.failure_message = "original failure retained"
        (create,) = await claim_execution_commands(
            session,
            consumer_id="fixture",
            target_id=target.target_id,
            limit=1,
            lease_seconds=60,
            now=now,
        )
        await acknowledge_execution_command(
            session,
            command_id=create.id,
            consumer_id="fixture",
            acknowledgement={"job_uid": lease.job_uid},
            now=now,
        )
        command = await enqueue_execution_transition(
            session,
            lease_id=lease.id,
            expected_generation=1,
            desired_state="delete_pending",
            now=now,
        )
        (deletion,) = await claim_execution_commands(
            session,
            consumer_id="fixture",
            target_id=target.target_id,
            limit=1,
            lease_seconds=60,
            now=now,
        )
        await defer_execution_command(
            session,
            command_id=deletion.id,
            consumer_id="fixture",
            error_code="contract_error",
            error_message="execution event replay changed",
            retry_after_seconds=300,
            max_deliveries=1,
            now=now,
        )
        return lease.id, lease.team_id, trial_id, target, command.id


@pytest.mark.parametrize(
    "boundary",
    [
        "qualified",
        "qualified_failed",
        "qualified_entrypoint",
        "guard_locked",
        "target_changed",
        "uid_changed",
        "outcome_changed",
    ],
)
async def test_bound_cleanup_uses_normal_delete_and_exact_absence(
    isolated_migration_postgres_url, boundary, monkeypatch
):
    # This recovery entrypoint deliberately supports installed 0175/0176 only.
    # Use a real isolated 0176 database without downgrading shared current-head tests.
    postgres_url = isolated_migration_postgres_url
    await asyncio.to_thread(retain_pool_schema, postgres_url)
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    guard_owner = "test-cleanup-" + uuid4().hex
    kube = ReadbackKubernetes()
    kube.guard_sessions = sessions
    try:
        lease_id, team_id, trial_id, target, command_id = await _seed_cleanup(
            sessions,
            now,
            failed=boundary == "qualified_failed",
        )
        async with sessions() as session:
            binding = await capture_cleanup_binding(session, team_id=team_id, lease_id=lease_id)
            request = TerminalCleanupRecoveryRequest(
                **binding.model_dump(),
                operation_id=uuid4(),
                cluster_id="mk8scluster-test",
                namespace="loom-nebius-platform",
                installed_candidate="a" * 40,
                candidate_sha="b" * 40,
                schema_head="0176",
                image_ref="cr.region.nebius.cloud/test/loom-control-plane@sha256:" + "b" * 64,
                installed_image_ref="cr.region.nebius.cloud/test/loom-control-plane@sha256:"
                + "a" * 64,
            )
        async with sessions.begin() as session:
            lease = await session.get(ServiceExecutionLease, lease_id)
            trial = await session.get(Trial, trial_id)
            assert lease is not None and trial is not None
            if boundary == "guard_locked":
                await session.execute(
                    text(
                        "INSERT INTO nebius_rollout_guard(id,owner,candidate_sha) VALUES(1,:owner,:candidate)"
                    ),
                    {"owner": guard_owner, "candidate": "a" * 40},
                )
            if boundary == "outcome_changed":
                trial.result = {"reward": {"main": 0}, "aggregate_reward": 0}
            before = _retained_projection(lease, trial)
            kube.jobs[lease.job_name] = KubernetesJobObservation(
                namespace=target.namespace_name,
                job_name=lease.job_name,
                lease_id=str(lease.id),
                resource_generation=lease.resource_generation,
                target_id=target.target_id,
                execution_unit_key=str(lease.execution_unit_key),
                job_uid=str(uuid4()) if boundary == "uid_changed" else lease.job_uid,
                normalized_state=NormalizedJobState.FAILED,
                reason="MissingTerminationSummary",
                resource_version="terminal-pod-gone",
            )
            outbox = (
                await session.scalars(
                    select(ServiceExecutionCommand).where(
                        ServiceExecutionCommand.lease_id == lease_id,
                    )
                )
            ).all()
            original_outbox = {
                row.id: (
                    row.state,
                    row.delivery_count,
                    row.last_error_code,
                    row.last_error_message,
                    row.idempotency_key,
                    row.generation,
                    row.acknowledged_at,
                )
                for row in outbox
            }
        actuator = ExecutionActuator(
            sessions=sessions,
            kubernetes=kube,
            target=ExecutionTargetRuntime(
                target_id="foreign-target" if boundary == "target_changed" else target.target_id,
                namespace=target.namespace_name,
            ),
            controller_id="bound-cleanup",
        )
        if boundary.startswith("qualified"):
            if boundary == "qualified_entrypoint":
                for key, value in {
                    "DB_URL": postgres_url,
                    "CONTROLLER_ID": "existing-actuator",
                    "TARGET_ID": target.target_id,
                    "NAMESPACE": target.namespace_name,
                }.items():
                    monkeypatch.setenv("LOOM_EXECUTION_ACTUATOR_" + key, value)
                for key in (
                    "GLOBAL_POOL",
                    "KUBERNETES_ENDPOINT",
                    "KUBERNETES_CA_FILE",
                    "KUBERNETES_NEBIUS_CREDENTIALS_FILE",
                ):
                    monkeypatch.delenv("LOOM_EXECUTION_ACTUATOR_" + key, raising=False)

                def existing_kube(*, connection):
                    assert connection is None  # Native in-cluster configuration.
                    return kube

                monkeypatch.setattr(cleanup_worker, "InClusterKubernetesJobApi", existing_kube)
                result = await cleanup_worker.recover_cleanup(request)
                assert kube.close_count == 1
            else:
                result = await reconcile_cleanup(request, actuator, sessions)
            assert result["status"] == "cleaned" and kube.delete_count == 1 and not kube.jobs
            assert kube.get_count >= 2
            # Repeating the bound recovery observes completion; no second delete.
            assert (await reconcile_cleanup(request, actuator, sessions))["status"] == "cleaned"
            assert kube.delete_count == 1
        else:
            with pytest.raises(RecoveryRefusedError):
                await reconcile_cleanup(request, actuator, sessions)
            assert kube.delete_count == 0 and len(kube.jobs) == 1
        async with sessions() as session:
            lease = await session.get(ServiceExecutionLease, lease_id)
            trial = await session.get(Trial, trial_id)
            command = await session.get(ServiceExecutionCommand, command_id)
            assert lease is not None and trial is not None and command is not None
            assert _retained_projection(lease, trial) == before
            assert command.state == "dead_letter" and command.delivery_count == 1
            current_outbox = (
                await session.scalars(
                    select(ServiceExecutionCommand).where(
                        ServiceExecutionCommand.lease_id == lease_id,
                    )
                )
            ).all()
            assert {
                row.id: (
                    row.state,
                    row.delivery_count,
                    row.last_error_code,
                    row.last_error_message,
                    row.idempotency_key,
                    row.generation,
                    row.acknowledged_at,
                )
                for row in current_outbox
            } == original_outbox
            assert lease.cleanup_state == (
                "complete" if boundary.startswith("qualified") else "pending"
            )
            assert bool(lease.deleted_at) == boundary.startswith("qualified")
    finally:
        async with sessions.begin() as session:
            await session.execute(
                text("DELETE FROM nebius_rollout_guard WHERE owner=:owner"), {"owner": guard_owner}
            )
        await engine.dispose()


@pytest.mark.parametrize("normal_ack_timing", ["after_delete", "before_worker_ack"])
async def test_cleanup_worker_accepts_absence_already_acknowledged_by_normal_actuator(
    postgres_url,
    normal_ack_timing,
):
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)

    class OtherActuatorFirst(ReadbackKubernetes):
        normal_actuator = None

        async def delete_job(self, **kwargs):
            await super().delete_job(**kwargs)
            assert self.normal_actuator is not None
            # Real normal reconciliation records its standard JobMissing event
            # and closes the lease before the recovery worker checks absence.
            if normal_ack_timing == "after_delete":
                await self.normal_actuator.reconcile_full_once(now=datetime.now(UTC))

    class RecoveryActuator(ExecutionActuator):
        async def _persist_observation(self, lease, observation, *, now):
            if observation.normalized_state == NormalizedJobState.DELETED:
                # A fresh read cannot serialize the later ACK by itself. The
                # ordinary observer may win immediately before persistence.
                if normal_ack_timing == "before_worker_ack":
                    await kube.normal_actuator.reconcile_full_once(now=datetime.now(UTC))
            await super()._persist_observation(lease, observation, now=now)

    kube = OtherActuatorFirst()
    kube.guard_sessions = sessions
    try:
        lease_id, team_id, trial_id, target, command_id = await _seed_cleanup(sessions, now)
        async with sessions() as session:
            binding = await capture_cleanup_binding(session, team_id=team_id, lease_id=lease_id)
            request = TerminalCleanupRecoveryRequest(
                **binding.model_dump(),
                operation_id=uuid4(),
                cluster_id="mk8scluster-test",
                namespace="loom-nebius-platform",
                installed_candidate="a" * 40,
                candidate_sha="b" * 40,
                schema_head="0176",
                image_ref="cr.region.nebius.cloud/test/loom-control-plane@sha256:" + "b" * 64,
                installed_image_ref="cr.region.nebius.cloud/test/loom-control-plane@sha256:"
                + "a" * 64,
            )
            lease = await session.get(ServiceExecutionLease, lease_id)
            trial = await session.get(Trial, trial_id)
            assert lease is not None and trial is not None
            before = _retained_projection(lease, trial)
            kube.jobs[lease.job_name] = KubernetesJobObservation(
                namespace=target.namespace_name,
                job_name=lease.job_name,
                lease_id=str(lease.id),
                resource_generation=lease.resource_generation,
                target_id=target.target_id,
                execution_unit_key=str(lease.execution_unit_key),
                job_uid=lease.job_uid,
                normalized_state=NormalizedJobState.FAILED,
                reason="MissingTerminationSummary",
                resource_version="normal-actuator-ack-race",
            )
        runtime = ExecutionTargetRuntime(
            target_id=target.target_id, namespace=target.namespace_name
        )
        worker = RecoveryActuator(
            sessions=sessions, kubernetes=kube, target=runtime, controller_id="bound-cleanup-worker"
        )
        kube.normal_actuator = ExecutionActuator(
            sessions=sessions,
            kubernetes=kube,
            target=runtime,
            controller_id="normal-active-actuator",
        )
        try:
            report = await reconcile_cleanup(request, worker, sessions)
        finally:
            # Even the unfixed worker error cannot turn real successful cleanup
            # into permission to alter the outcome, reset commands or delete twice.
            async with sessions() as session:
                current = await session.get(ServiceExecutionLease, lease_id)
                trial = await session.get(Trial, trial_id)
                command = await session.get(ServiceExecutionCommand, command_id)
                assert current is not None and trial is not None and command is not None
                assert current.cleanup_state == "complete" and current.deleted_at is not None
                assert _retained_projection(current, trial) == before
                assert command.state == "dead_letter" and command.delivery_count == 1
            assert kube.delete_count == 1 and not kube.jobs
        assert report["status"] == "cleaned"
    finally:
        await engine.dispose()


async def test_complete_cleanup_worker_on_real_installed_0175(
    isolated_migration_postgres_url,
    monkeypatch,
):
    import asyncio

    from alembic import command

    from loom_control_plane.terminal_archive_recovery import archive_sessions
    from tests.integration.test_service_execution_spool_materialization import _config

    # Use the isolated migration database, then apply the actual 0176 downgrade.
    # The installed schema has no baseline_session_id column; no marker spoof.
    await asyncio.to_thread(command.downgrade, _config(isolated_migration_postgres_url), "0175")
    engine = create_async_engine(isolated_migration_postgres_url)
    sessions = archive_sessions(engine, legacy_baseline=True)
    now = datetime.now(UTC)
    kube = ReadbackKubernetes()
    kube.guard_sessions = sessions
    try:
        async with engine.connect() as connection:
            assert (
                await connection.scalar(text("SELECT version_num FROM alembic_version")) == "0175"
            )
            assert not await connection.scalar(
                text(
                    "SELECT EXISTS (SELECT 1 FROM information_schema.columns "
                    "WHERE table_name='llm_calls' AND column_name='baseline_session_id')"
                )
            )
        lease_id, team_id, trial_id, target, command_id = await _seed_cleanup(sessions, now)
        async with sessions() as session:
            binding = await capture_cleanup_binding(session, team_id=team_id, lease_id=lease_id)
            request = TerminalCleanupRecoveryRequest(
                **binding.model_dump(),
                operation_id=uuid4(),
                cluster_id="mk8scluster-test",
                namespace="loom-nebius-platform",
                installed_candidate="a" * 40,
                candidate_sha="b" * 40,
                schema_head="0175",
                image_ref="cr.region.nebius.cloud/test/loom-control-plane@sha256:" + "b" * 64,
                installed_image_ref="cr.region.nebius.cloud/test/loom-control-plane@sha256:"
                + "a" * 64,
            )
            lease = await session.get(ServiceExecutionLease, lease_id)
            trial = await session.get(Trial, trial_id)
            assert lease is not None and trial is not None
            before = _retained_projection(lease, trial)
            kube.jobs[lease.job_name] = KubernetesJobObservation(
                namespace=target.namespace_name,
                job_name=lease.job_name,
                lease_id=str(lease_id),
                resource_generation=lease.resource_generation,
                target_id=target.target_id,
                execution_unit_key=str(lease.execution_unit_key),
                job_uid=lease.job_uid,
                normalized_state=NormalizedJobState.FAILED,
                reason="MissingTerminationSummary",
                resource_version="installed-0175-pod-gone",
            )
        for key, value in {
            "DB_URL": isolated_migration_postgres_url,
            "CONTROLLER_ID": "existing-actuator",
            "TARGET_ID": target.target_id,
            "NAMESPACE": target.namespace_name,
        }.items():
            monkeypatch.setenv("LOOM_EXECUTION_ACTUATOR_" + key, value)
        for key in (
            "GLOBAL_POOL",
            "KUBERNETES_ENDPOINT",
            "KUBERNETES_CA_FILE",
            "KUBERNETES_NEBIUS_CREDENTIALS_FILE",
        ):
            monkeypatch.delenv("LOOM_EXECUTION_ACTUATOR_" + key, raising=False)

        def existing_kube(*, connection):
            assert connection is None
            return kube

        monkeypatch.setattr(cleanup_worker, "InClusterKubernetesJobApi", existing_kube)
        result = await cleanup_worker.recover_cleanup(request)
        assert result["status"] == "cleaned" and kube.delete_count == kube.close_count == 1
        assert kube.get_count >= 2 and not kube.jobs
        async with sessions() as session:
            lease = await session.get(ServiceExecutionLease, lease_id)
            trial = await session.get(Trial, trial_id)
            command = await session.get(ServiceExecutionCommand, command_id)
            assert lease is not None and trial is not None and command is not None
            assert _retained_projection(lease, trial) == before
            assert lease.cleanup_state == "complete" and lease.deleted_at is not None
            assert command.state == "dead_letter" and command.delivery_count == 1
            assert command.last_error_message == "execution event replay changed"
    finally:
        await engine.dispose()
