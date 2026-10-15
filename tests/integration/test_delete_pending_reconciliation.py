from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import (
    ServiceExecutionCommand,
    ServiceExecutionEvent,
    ServiceExecutionLease,
    Trial,
)
from loom_control_plane.service_execution import (
    acknowledge_execution_command,
    claim_execution_commands,
    defer_execution_command,
    enqueue_execution_transition,
)
from loom_execution_actuator.contracts import KubernetesJobObservation, NormalizedJobState
from loom_execution_actuator.controller import ExecutionActuator
from loom_execution_actuator.renderer import ExecutionTargetRuntime
from tests.integration.test_service_execution_leases import (
    _cleanup_service_execution_test_rows,  # noqa: F401
    _FakeKubernetesJobApi,
    _reserve,
    _seed_ready_trial,
)


def _retained_projection(lease: ServiceExecutionLease, trial: Trial) -> tuple[object, ...]:
    return (
        lease.generation, lease.resource_generation, lease.finalized_at, lease.revoked_at,
        lease.output_commit_state, lease.output_generation, lease.output_upload_session_id,
        lease.output_manifest_sha256, lease.output_marker_sha256, lease.output_committed_at,
        lease.materialization_state, lease.materialization_attempts,
        lease.materialization_committed_at, lease.canonical_trajectory_sha256,
        lease.canonical_atif_sha256, lease.source_cleanup_state, lease.source_retain_until,
        trial.state, trial.result, trial.attempt_count, trial.finished_at,
        trial.failure_reason, trial.failure_message,
    )


@pytest.mark.parametrize("boundary", ["terminal", "changed_uid", "output_window_open"])
async def test_reconcile_closes_dead_letter_delete_without_rewriting_terminal_evidence(
    postgres_url: str, boundary: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    kube = _FakeKubernetesJobApi()
    now = datetime.now(UTC)
    try:
        async with sessions() as session, session.begin():
            trial_id, target = await _seed_ready_trial(session, now=now)
            lease = await _reserve(session, trial_id=trial_id, target=target, now=now)
            lease_id = lease.id
            # Seed the durable terminal/output state from the reported incident.
            # Storage materialization has its own real object-store regression;
            # reconciliation must preserve all its retained pointers untouched.
            lease.job_uid, lease.pod_uid = str(uuid4()), str(uuid4())
            trial = await session.get(Trial, trial_id)
            assert trial is not None
            if boundary != "output_window_open":
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
                trial.state = "succeeded"
                trial.finished_at = now
                trial.result = {"reward": {"main": 1.0}, "aggregate_reward": 1.0}

            create, = await claim_execution_commands(
                session, consumer_id="fixture-create", target_id=target.target_id,
                limit=1, lease_seconds=60, now=now,
            )
            await acknowledge_execution_command(
                session, command_id=create.id, consumer_id="fixture-create",
                acknowledgement={"job_uid": lease.job_uid}, now=now,
            )
            command = await enqueue_execution_transition(
                session, lease_id=lease_id, expected_generation=1,
                desired_state="delete_pending", now=now,
            )
            deletion, = await claim_execution_commands(
                session, consumer_id="fixture-delete", target_id=target.target_id,
                limit=1, lease_seconds=60, now=now,
            )
            assert deletion.id == command.id
            await defer_execution_command(
                session, command_id=command.id, consumer_id="fixture-delete",
                error_code="contract_error", error_message="execution event replay changed",
                retry_after_seconds=300, max_deliveries=1, now=now,
            )
            retained = _retained_projection(lease, trial)
            command_id = command.id
            outbox_snapshot = (
                command.state, command.delivery_count, command.last_error_code,
                command.last_error_message, command.idempotency_key, command.generation,
                command.acknowledged_at, command.claimed_by, command.claim_expires_at,
            )
            kube.jobs[lease.job_name] = KubernetesJobObservation(
                namespace=target.namespace_name, job_name=lease.job_name,
                lease_id=str(lease_id), resource_generation=lease.resource_generation,
                target_id=target.target_id, execution_unit_key=str(lease.execution_unit_key),
                job_uid=str(uuid4()) if boundary == "changed_uid" else lease.job_uid,
                # Match the real completed Job whose old Pod is now gone.
                normalized_state=(NormalizedJobState.RUNNING if boundary == "output_window_open"
                                  else NormalizedJobState.FAILED),
                reason=None if boundary == "output_window_open" else "MissingTerminationSummary",
                message=None if boundary == "output_window_open"
                        else "successful runtime Job has no termination summary",
                resource_version="retained-completed-job",
            )

        actuator = ExecutionActuator(
            sessions=sessions, kubernetes=kube,
            target=ExecutionTargetRuntime(target_id=target.target_id, namespace=target.namespace_name),
            controller_id="normal-reconcile-recovery",
        )
        # Ordinary claim does not reset or redeliver a dead-letter row.
        assert await actuator.run_commands_once(now=now + timedelta(seconds=1)) == 0
        await actuator.reconcile_full_once(now=now + timedelta(seconds=1))
        if boundary == "terminal":
            assert kube.delete_count == 1 and not kube.jobs
            # Exact absence is observed by the normal next pass, not fabricated.
            await actuator.reconcile_full_once(now=now + timedelta(seconds=2))
        else:
            assert kube.delete_count == 0 and len(kube.jobs) == 1

        async with sessions() as session:
            persisted = await session.get(ServiceExecutionLease, lease_id)
            trial = await session.get(Trial, trial_id)
            command = await session.get(ServiceExecutionCommand, command_id)
            assert persisted is not None and trial is not None and command is not None
            assert _retained_projection(persisted, trial) == retained
            assert (
                command.state, command.delivery_count, command.last_error_code,
                command.last_error_message, command.idempotency_key, command.generation,
                command.acknowledged_at, command.claimed_by, command.claim_expires_at,
            ) == outbox_snapshot
            if boundary == "terminal":
                assert persisted.cleanup_state == "complete" and persisted.deleted_at is not None
                assert persisted.desired_state == persisted.observed_state == "deleted"
                observation = await session.scalar(
                    select(ServiceExecutionEvent).where(
                        ServiceExecutionEvent.lease_id == lease_id,
                        ServiceExecutionEvent.payload_json["reason"].astext == "MissingTerminationSummary",
                    ),
                )
                assert observation is not None and observation.payload_json["normalized_state"] == "failed"
            else:
                assert persisted.cleanup_state == "pending" and persisted.deleted_at is None
                assert persisted.desired_state == "delete_pending"
    finally:
        await engine.dispose()
