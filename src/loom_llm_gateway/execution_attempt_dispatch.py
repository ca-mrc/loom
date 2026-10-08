"""Live Gateway authorization for Pipeline execution-attempt step JWTs."""

from __future__ import annotations

from datetime import UTC, datetime

from fastapi import HTTPException, Request
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.auth import AuthContext
from loom.db.schema import (
    ExecutionAttempt,
    PipelineRun,
    PipelineStageRun,
    ServiceExecutionLease,
    Trial,
)
from loom.execution_runtime_contract import ExecutionRuntimePlanV1
from loom.pipeline.keys import canonical_digest


def _control_binding_digest(execution_spec: dict[str, object] | None) -> str | None:
    if execution_spec is None:
        raise ValueError("execution spec is unavailable")
    raw_refs = execution_spec.get("control_binding_snapshots")
    if not isinstance(raw_refs, list) or len(raw_refs) > 1:
        raise ValueError("control binding set is not closed")
    if not raw_refs:
        return None
    ref = raw_refs[0]
    if not isinstance(ref, dict):
        raise ValueError("control binding ref is invalid")
    digest = ref.get("snapshot_sha256")
    if not isinstance(digest, str):
        raise ValueError("control binding digest is unavailable")
    return digest


async def authorize_execution_attempt_dispatch(
    session: AsyncSession,
    ctx: AuthContext,
    *,
    lock: bool = False,
) -> None:
    """Fail closed unless an attempt JWT still names the live DB authority.

    Signature verification proves who minted the bearer.  This read proves the
    Attempt has not since been cancelled, finished, lost, re-leased, rotated,
    or rebound before any request is sent to an upstream provider.
    """

    if ctx.execution_attempt_id is None:
        return
    statement = (
        select(ExecutionAttempt, PipelineStageRun, PipelineRun)
        .join(PipelineStageRun, PipelineStageRun.id == ExecutionAttempt.stage_run_id)
        .join(PipelineRun, PipelineRun.id == PipelineStageRun.pipeline_run_id)
        .where(ExecutionAttempt.id == ctx.execution_attempt_id)
    )
    if lock:
        statement = statement.with_for_update(of=(ExecutionAttempt, PipelineStageRun, PipelineRun))
    row = (await session.execute(statement)).one_or_none()
    if row is None:
        raise HTTPException(status_code=403, detail="execution attempt dispatch forbidden")
    attempt, stage, run = row
    try:
        control_binding_digest = _control_binding_digest(stage.resolved_execution_spec_json)
        container_node = (stage.resolved_execution_spec_json or {}).get("container_node")
        network_profile = (
            container_node.get("network_profile") if isinstance(container_node, dict) else None
        )
        authorized = (
            ctx.step_jwt_id is not None
            and ctx.execution_attempt_lease_epoch is not None
            and ctx.execution_spec_digest is not None
            and ctx.execution_authorization_digest is not None
            and ctx.provider_connection_id_bound
            and run.team_id == ctx.team_id
            and run.state == "running"
            and stage.node_key == ctx.step_id
            and stage.state in {"claimed", "running"}
            and stage.execution_spec_digest == ctx.execution_spec_digest
            and stage.provider_connection_ref == ctx.provider_connection_id
            and network_profile == "gateway"
            and attempt.state in {"claimed", "running"}
            and attempt.cancellation_requested_at is None
            and attempt.lease_expires_at is not None
            and attempt.lease_expires_at > datetime.now(UTC)
            and attempt.lease_epoch == ctx.execution_attempt_lease_epoch
            and attempt.step_jwt_id == ctx.step_jwt_id
            and attempt.execution_authorization_digest == ctx.execution_authorization_digest
            and control_binding_digest == ctx.control_binding_snapshot_digest
        )
    except (AttributeError, TypeError, ValueError):
        authorized = False
    if not authorized:
        raise HTTPException(status_code=403, detail="execution attempt dispatch forbidden")


async def authorize_trial_execution_dispatch(
    session: AsyncSession,
    ctx: AuthContext,
    *,
    lock: bool = False,
) -> None:
    """Re-check a service trial JWT against the current live lease generation."""

    if ctx.trial_id is None:
        return
    statement = (
        select(ServiceExecutionLease, Trial)
        .join(Trial, Trial.id == ServiceExecutionLease.trial_id)
        .where(
            ServiceExecutionLease.trial_id == ctx.trial_id,
            ServiceExecutionLease.execution_role == "attempt",
        )
        .order_by(ServiceExecutionLease.attempt.desc())
        .limit(1)
    )
    if lock:
        statement = statement.with_for_update(of=(ServiceExecutionLease, Trial))
    row = (await session.execute(statement)).one_or_none()
    if row is None:
        if ctx.service_execution_lease_id is None and ctx.service_execution_generation is None:
            return
        raise HTTPException(status_code=403, detail="service execution dispatch forbidden")
    lease, trial = row
    try:
        runtime_contract = ExecutionRuntimePlanV1.model_validate(lease.runtime_contract_json)
        runtime_contract_sha256 = canonical_digest(runtime_contract.canonical_payload())
        authorized = (
            ctx.service_execution_lease_id is not None
            and ctx.service_execution_generation is not None
            and ctx.step_jwt_id is not None
            and ctx.service_execution_role == "attempt"
            and ctx.step_id == "agent"
            and ctx.provider_connection_id_bound
            and ctx.service_execution_runtime_contract_sha256 is not None
            and ctx.service_execution_candidate_sha is not None
            and ctx.service_execution_task_revision_sha256 is not None
            and ctx.service_execution_command_identity_sha256 is not None
            and lease.id == ctx.service_execution_lease_id
            and lease.generation == ctx.service_execution_generation
            and lease.team_id == ctx.team_id
            and lease.attempt == trial.attempt_count
            and lease.execution_role == ctx.service_execution_role
            and lease.runtime_contract_sha256 == runtime_contract_sha256
            and runtime_contract_sha256 == ctx.service_execution_runtime_contract_sha256
            and runtime_contract.candidate_sha == ctx.service_execution_candidate_sha
            and runtime_contract.task_revision_sha256 == ctx.service_execution_task_revision_sha256
            and runtime_contract.command_identity_sha256
            == ctx.service_execution_command_identity_sha256
            and trial.provider_connection_id == ctx.provider_connection_id
            and lease.revoked_at is None
            and lease.deleted_at is None
            and lease.desired_state in {"create", "start", "finalize"}
            and lease.observed_state in {"creating", "created", "running", "finalizing"}
            and trial.state in {"claimed", "running"}
        )
    except (TypeError, ValueError):
        authorized = False
    if not authorized:
        raise HTTPException(status_code=403, detail="service execution dispatch forbidden")


async def authorize_trial_model(request: Request, ctx: AuthContext, *, provider: str, model: str) -> None:
    """A native service-execution call may name only its Trial's admitted model.

    Native Trials have exactly one model (multi-model is not admitted), and
    materialization fails a Trial whose ledger holds any other identity. This
    applies the same rule before a provider is contacted, so an off-model call
    is refused instead of incurring cost. `provider` is the provider the
    route's ledger dialect records; `model` is the model name it records.
    """

    if ctx.service_execution_lease_id is None or ctx.trial_id is None:
        return
    async with request.app.state.session_factory() as session:
        config = (
            await session.execute(select(Trial.config).where(Trial.id == ctx.trial_id))
        ).scalar_one_or_none()
    admitted = config.get("agent_model") if isinstance(config, dict) else None
    if not (
        isinstance(admitted, dict)
        and admitted.get("provider") == provider
        and admitted.get("name") == model
    ):
        raise HTTPException(status_code=403, detail="service execution model forbidden")


__all__ = [
    "authorize_execution_attempt_dispatch",
    "authorize_trial_execution_dispatch",
    "authorize_trial_model",
]
