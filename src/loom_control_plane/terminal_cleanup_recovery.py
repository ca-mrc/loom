"""One ordinary UID-fenced deletion for a finalized, archived attempt.

No command is reset and no scheduler, build or migration loop is started.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import signal
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from loom.db.schema import ServiceExecutionCommand, ServiceExecutionLease, Trial
from loom.nebius_rollout_guard import admission_open
from loom.pipeline.keys import canonical_document, digest_bytes
from loom_control_plane.pending_archive_recovery import GitSha, RecoveryRefusedError, Sha256
from loom_control_plane.terminal_archive_recovery import archive_sessions
from loom_execution_actuator.config import ExecutionActuatorSettings
from loom_execution_actuator.contracts import KubernetesJobObservation, NormalizedJobState
from loom_execution_actuator.controller import ExecutionActuator
from loom_execution_actuator.kubernetes_api import InClusterKubernetesJobApi
from loom_execution_actuator.renderer import ExecutionTargetRuntime


class TerminalCleanupBinding(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    team_id: UUID
    trial_id: UUID
    lease_id: UUID
    command_id: UUID
    generation: int = Field(gt=0)
    resource_generation: int = Field(gt=0)
    attempt: int = Field(gt=0)
    job_uid: str = Field(min_length=1, max_length=128)
    target_id: str = Field(min_length=1, max_length=80)
    execution_namespace: str = Field(pattern=r"^[a-z0-9][a-z0-9-]{0,62}$")
    trial_state: str = Field(pattern=r"^(succeeded|failed|cancelled)$")
    result_sha256: Sha256
    failure_reason: str | None
    failure_message_sha256: Sha256
    finished_at: datetime | None
    command_deliveries: int = Field(ge=1)
    canonical_trajectory_sha256: Sha256
    canonical_atif_sha256: Sha256


class TerminalCleanupRecoveryRequest(TerminalCleanupBinding):
    operation_id: UUID
    cluster_id: str = Field(pattern=r"^mk8scluster-[a-z0-9]+$")
    namespace: str = Field(pattern=r"^[a-z0-9][a-z0-9-]{0,62}$")
    installed_candidate: GitSha
    candidate_sha: GitSha
    image_ref: str = Field(
        pattern=r"^cr\.[a-z0-9-]+\.nebius\.cloud/[a-z0-9]+/loom-control-plane@sha256:[0-9a-f]{64}$"
    )
    installed_image_ref: str = Field(
        pattern=r"^cr\.[a-z0-9-]+\.nebius\.cloud/[a-z0-9]+/loom-control-plane@sha256:[0-9a-f]{64}$"
    )
    schema_head: str = Field(pattern=r"^[0-9]{4}$")

    @property
    def digest(self) -> str:
        return digest_bytes(canonical_document(self.model_dump(mode="json")))


async def capture_cleanup_binding(
    session: AsyncSession, *, team_id: UUID, lease_id: UUID, allow_complete: bool = False
) -> TerminalCleanupBinding:
    lease = await session.get(ServiceExecutionLease, lease_id, populate_existing=True)
    if lease is None or lease.team_id != team_id:
        raise RecoveryRefusedError("cleanup_identity_missing")
    trial = await session.get(Trial, lease.trial_id, populate_existing=True)
    complete = lease.desired_state == "deleted" and lease.cleanup_state == "complete"
    if (
        trial is None
        or trial.team_id != team_id
        or trial.attempt_count != lease.attempt
        or trial.state not in {"succeeded", "failed", "cancelled"}
        or lease.execution_role != "attempt"
        or lease.parent_lease_id is not None
        or lease.revoked_at is None
        or lease.finalized_at is None
        or lease.job_uid is None
        or lease.output_commit_state != "committed"
        or lease.materialization_state != "committed"
        or lease.source_cleanup_state != "retained"
        or lease.source_retain_until is None
        or lease.source_retain_until <= datetime.now(UTC)
        or not lease.canonical_trajectory_sha256
        or not lease.canonical_atif_sha256
        or lease.materialization_claim_id is not None
        or (
            not (allow_complete and complete)
            and (lease.desired_state != "delete_pending" or lease.cleanup_state != "pending")
        )
    ):
        raise RecoveryRefusedError("terminal_cleanup_not_eligible")
    command = (
        await session.scalars(
            select(ServiceExecutionCommand).where(
                ServiceExecutionCommand.lease_id == lease.id,
                ServiceExecutionCommand.generation == lease.generation,
                ServiceExecutionCommand.command_type == "delete",
            )
        )
    ).one_or_none()
    if (
        command is None
        or command.state != "dead_letter"
        or command.last_error_code != "contract_error"
    ):
        raise RecoveryRefusedError("cleanup_command_changed")
    return TerminalCleanupBinding(
        team_id=team_id,
        trial_id=trial.id,
        lease_id=lease.id,
        command_id=command.id,
        generation=lease.generation,
        resource_generation=lease.resource_generation,
        attempt=lease.attempt,
        job_uid=lease.job_uid,
        target_id=lease.target_id,
        execution_namespace=lease.namespace_name,
        trial_state=trial.state,
        result_sha256=digest_bytes(canonical_document(trial.result)),
        failure_reason=trial.failure_reason,
        failure_message_sha256=digest_bytes(canonical_document(trial.failure_message)),
        finished_at=trial.finished_at,
        command_deliveries=command.delivery_count,
        canonical_trajectory_sha256=lease.canonical_trajectory_sha256,
        canonical_atif_sha256=lease.canonical_atif_sha256,
    )


async def reconcile_cleanup(
    request: TerminalCleanupRecoveryRequest,
    actuator: ExecutionActuator,
    sessions: async_sessionmaker[AsyncSession],
) -> dict[str, Any]:
    expected = TerminalCleanupBinding.model_validate(
        request.model_dump(include=set(TerminalCleanupBinding.model_fields))
    )
    # Keep the ordinary shared admission lock for the entire bounded deletion.
    # This does not acquire or release the rollout guard or change its predicate.
    async with sessions.begin() as guard:
        if not await admission_open(guard):
            raise RecoveryRefusedError("rollout_admission_closed")
        if (
            await capture_cleanup_binding(
                guard, team_id=request.team_id, lease_id=request.lease_id, allow_complete=True
            )
            != expected
        ):
            raise RecoveryRefusedError("cleanup_projection_changed")
        lease = await actuator._lease(request.lease_id)
        if (
            lease.target_id != actuator._target.target_id
            or lease.namespace_name != actuator._target.namespace
        ):
            raise RecoveryRefusedError("cleanup_target_changed")
        observation = await actuator._get(lease)
        if observation is not None:
            if (
                observation.job_uid != request.job_uid
                or observation.pod_uid is not None
                or observation.normalized_state
                not in {NormalizedJobState.SUCCEEDED, NormalizedJobState.FAILED}
            ):
                raise RecoveryRefusedError("cleanup_job_not_terminal")
            await actuator._delete(lease, observation, now=datetime.now(UTC))
        # Exact GET absence, rather than a delete response, owns cleanup ACK.
        async with asyncio.timeout(60):
            while await actuator._get(lease) is not None:
                await asyncio.sleep(1)
        if lease.desired_state != "deleted":
            missing = KubernetesJobObservation(
                namespace=lease.namespace_name,
                job_name=lease.job_name,
                lease_id=str(lease.id),
                resource_generation=lease.resource_generation,
                target_id=lease.target_id,
                execution_unit_key=str(lease.execution_unit_key),
                normalized_state=NormalizedJobState.DELETED,
                job_uid=lease.job_uid,
                reason="JobMissing",
            )
            await actuator._persist_observation(lease, missing, now=datetime.now(UTC))
        async with sessions() as verify:
            if (
                await capture_cleanup_binding(
                    verify, team_id=request.team_id, lease_id=request.lease_id, allow_complete=True
                )
                != expected
            ):
                raise RecoveryRefusedError("terminal_cleanup_outcome_changed")
            current = await verify.get(ServiceExecutionLease, request.lease_id)
            if (
                current is None
                or current.desired_state != "deleted"
                or current.cleanup_state != "complete"
            ):
                raise RecoveryRefusedError("cleanup_ack_incomplete")
        return {
            "status": "cleaned",
            "lease_id": str(request.lease_id),
            "job_uid": request.job_uid,
            "trial_state": request.trial_state,
            "command_state": "dead_letter",
            "execution_attempts": request.attempt,
        }


async def recover_cleanup(request: TerminalCleanupRecoveryRequest) -> dict[str, Any]:
    settings = ExecutionActuatorSettings()
    if (
        settings.global_pool is not None
        or settings.target_id != request.target_id
        or settings.namespace != request.execution_namespace
    ):
        raise RecoveryRefusedError("cleanup_target_changed")
    engine = create_async_engine(settings.db_url, pool_pre_ping=True)
    kubernetes = InClusterKubernetesJobApi(connection=settings.kubernetes_connection)
    try:
        async with engine.connect() as connection:
            head = (await connection.scalars(text("SELECT version_num FROM alembic_version"))).all()
            if head != [request.schema_head] or request.schema_head not in {"0175", "0176"}:
                raise RecoveryRefusedError("schema_binding_changed")
        sessions = archive_sessions(engine, legacy_baseline=request.schema_head == "0175")
        target = ExecutionTargetRuntime(target_id=settings.target_id, namespace=settings.namespace)
        actuator = ExecutionActuator(
            sessions=sessions,
            kubernetes=kubernetes,
            target=target,
            controller_id="terminal-cleanup-" + str(request.operation_id),
        )
        async with asyncio.timeout(180):
            return await reconcile_cleanup(request, actuator, sessions)
    finally:
        await kubernetes.close()
        await engine.dispose()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--request-json", required=True)
    args = parser.parse_args()
    signal.signal(signal.SIGALRM, lambda *_: os._exit(124))
    signal.alarm(240)
    try:
        if len(args.request_json.encode()) > 16384:
            raise RecoveryRefusedError("request_size")
        request = TerminalCleanupRecoveryRequest.model_validate_json(args.request_json)
        print(json.dumps(asyncio.run(recover_cleanup(request)), sort_keys=True))
        return 0
    except Exception as error:
        reason = (
            str(error) if isinstance(error, RecoveryRefusedError) else "terminal_cleanup_incomplete"
        )
        if not re.fullmatch(r"[a-z][a-z0-9_]{0,119}", reason):
            reason = "terminal_cleanup_incomplete"
        print(
            json.dumps({"status": "blocked", "reason": reason, "error_class": type(error).__name__})
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
