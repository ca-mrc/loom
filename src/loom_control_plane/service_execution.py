"""Durable desired-state and generation fencing for service execution (#1540)."""

from __future__ import annotations

import ipaddress
import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import Text, cast, func, literal, or_, select
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import (
    Artifact,
    ArtifactUploadFile,
    ArtifactUploadSession,
    ExecutionAdmissionReservation,
    ExecutionCostReservation,
    ExecutionProvisioningAuthorization,
    ServiceExecutionClass,
    ServiceExecutionCommand,
    ServiceExecutionEvent,
    ServiceExecutionLease,
    ServiceExecutionTarget,
    TeamQuota,
    Trial,
)
from loom.execution_contract import (
    CapacityEvidenceKind,
    ExecutionAdapterKind,
    ExecutionClassV1,
    ExecutionRouteCandidateV1,
    ExecutionRoutingDecisionV1,
    ExecutionRoutingReason,
    ExecutionTargetV1,
    WorkloadRequirementsV1,
    evaluate_execution_admission,
    workload_requirements_from_task,
)
from loom.execution_image_admission import (
    ImageAdmissionError,
    ImageAdmissionKeyring,
    verify_execution_image_admission,
)
from loom.execution_runtime_contract import (
    ExecutionRuntimePlanV1,
    ExecutionRuntimeResultV1,
    runtime_pod_resources,
    validate_runtime_plan_requirements,
)
from loom.models.task import TaskConfig
from loom.models.trial import TrialConfig
from loom.pipeline.keys import canonical_digest, canonical_uuid5
from loom.task_image_materialization import (
    get_trial_task_image_execution_grant,
    resolve_prepared_task,
)
from loom.terminal_result_semantics import aggregate_reward_scalar
from loom_control_plane.execution_admission import (
    ExecutionAdmissionBlockedError,
    ExecutionAdmissionIdentity,
    reserve_execution_admission,
)
from loom_control_plane.execution_capacity import (
    _CAPACITY_MUTATION_LOCK,
    reserve_execution_provisioning,
)
from loom_control_plane.execution_capacity_targets import (
    resolve_capacity_targets,
    validate_capacity_owner,
)
from loom_control_plane.execution_finance import (
    ExecutionFinanceBlockedError,
    reserve_execution_cost,
)
from loom_control_plane.metrics import (
    SERVICE_EXECUTION_CLEANUP_DEBT,
    SERVICE_EXECUTION_COMMAND_BACKLOG,
    SERVICE_EXECUTION_DUPLICATE_DELIVERIES_TOTAL,
    SERVICE_EXECUTION_STALE_GENERATIONS_TOTAL,
)

_EXECUTION_UNIT_NAMESPACE = UUID("a2f6a80b-b27c-4ae1-b9a0-8c743edb0fa5")
_TERMINAL_EVENT_KINDS = frozenset({"cancelled", "timed_out", "failed", "finalized", "deleted"})
_TERMINAL_TRIAL_STATES = frozenset({"succeeded", "failed", "cancelled"})
_EVENT_TO_OBSERVED = {
    "created": "created",
    "started": "running",
    "cancelled": "cancelled",
    "timed_out": "timed_out",
    "failed": "failed",
    "finalized": "finalized",
    "deleted": "deleted",
}
_DESIRED_TO_COMMAND = {
    "create": "create",
    "start": "start",
    "cancel": "cancel",
    "timeout": "timeout",
    "retry": "retry",
    "finalize": "finalize",
    "delete_pending": "delete",
}
_REVOCATION_STATES = frozenset({"cancel", "timeout", "retry", "delete_pending"})
_RESOURCE_RELEASE_DEADLINE = timedelta(minutes=5)
_NATIVE_TERMINAL_FAILURES = frozenset(
    {"failed", "oom_killed", "evicted", "node_lost", "deadline_exceeded"}
)
# Deferred verifier leases a handoff may spend after the first one fails natively.
MAX_VERIFIER_RETRIES = 2
_ALLOWED_DESIRED_TRANSITIONS = {
    "create": frozenset({"start", "cancel", "timeout", "retry", "delete_pending"}),
    "start": frozenset({"finalize", "cancel", "timeout", "retry", "delete_pending"}),
    "finalize": frozenset({"delete_pending"}),
    "cancel": frozenset({"delete_pending"}),
    "timeout": frozenset({"retry", "delete_pending"}),
    "retry": frozenset({"delete_pending"}),
    "delete_pending": frozenset(),
    "deleted": frozenset(),
}
_EVENT_ALLOWED_DESIRED = {
    "created": frozenset({"create", "start"}),
    "started": frozenset({"start", "finalize"}),
    "heartbeat": frozenset({"start", "finalize"}),
    "gateway_call": frozenset({"start", "finalize"}),
    "artifact_committed": frozenset({"start", "finalize"}),
    "trajectory_committed": frozenset({"start", "finalize"}),
    "usage_reported": frozenset({"start", "finalize"}),
    "result_reported": frozenset({"finalize", "cancel"}),
    "kubernetes_observed": frozenset(
        {"create", "start", "finalize", "cancel", "timeout", "retry", "delete_pending"}
    ),
    "cancelled": frozenset({"cancel"}),
    "timed_out": frozenset({"timeout"}),
    "failed": frozenset({"create", "start", "finalize"}),
    "finalized": frozenset({"finalize"}),
    "deleted": frozenset({"cancel", "timeout", "retry", "delete_pending"}),
}


class ServiceExecutionError(RuntimeError):
    """Base class for fail-closed execution-state errors."""


class ServiceExecutionConflict(ServiceExecutionError):  # noqa: N818 - contract name
    pass


class ServiceExecutionFenceError(ServiceExecutionConflict):
    pass


class CommandState(StrEnum):
    PENDING = "pending"
    LEASED = "leased"
    ACKNOWLEDGED = "acknowledged"
    DEAD_LETTER = "dead_letter"


@dataclass(frozen=True)
class ExecutionFence:
    lease_id: UUID
    generation: int
    trial_id: UUID
    attempt: int
    execution_role: str
    runtime_contract_sha256: str | None
    candidate_sha: str | None
    task_revision_sha256: str | None
    command_identity_sha256: str | None


@dataclass(frozen=True)
class ClaimedExecutionCommand:
    id: UUID
    lease_id: UUID
    generation: int
    sequence: int
    command_type: str
    idempotency_key: str
    payload: dict[str, Any]
    delivery_count: int
    claim_expires_at: datetime


def _bounded_optional_text(value: object, limit: int, name: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value or len(value) > limit:
        raise ServiceExecutionConflict(f"invalid Kubernetes {name}")
    return value


def _validated_pod_ip(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ServiceExecutionConflict("invalid Kubernetes pod_ip")
    try:
        return str(ipaddress.ip_address(value))
    except ValueError as exc:
        raise ServiceExecutionConflict("invalid Kubernetes pod_ip") from exc


def _optional_datetime(value: object, name: str) -> datetime | None:
    if value is None:
        return None
    try:
        parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
    except ValueError as exc:
        raise ServiceExecutionConflict(f"invalid Kubernetes {name}") from exc
    if parsed.tzinfo is None:
        raise ServiceExecutionConflict(f"invalid Kubernetes {name}")
    return parsed.astimezone(UTC)


def _command_key(lease_id: UUID, generation: int, command_type: str) -> str:
    return canonical_digest(
        {
            "schema_version": "loom.execution-command-key.v1",
            "lease_id": str(lease_id),
            "generation": generation,
            "command_type": command_type,
        },
        persisted=False,
    )


def _event_key(lease_id: UUID, generation: int, ordinal: int, event_kind: str) -> str:
    return canonical_digest(
        {
            "schema_version": "loom.execution-event-key.v1",
            "lease_id": str(lease_id),
            "generation": generation,
            "ordinal": ordinal,
            "event_kind": event_kind,
        },
        persisted=False,
    )


def _execution_identity(
    *,
    trial_id: UUID,
    attempt: int,
    generation: int,
    execution_role: str,
    namespace_name: str,
    target_id: str,
    verifier_retry: int = 0,
) -> tuple[str, str, str, UUID]:
    role_suffix = "a" if execution_role == "attempt" else "v"
    # Retry 0 keeps the pre-retry identity of existing verifier leases.
    if verifier_retry:
        role_suffix += f"r{verifier_retry}"
    job_name = f"loom-{trial_id.hex[:12]}-a{attempt}-g{generation}-{role_suffix}"
    unit: dict[str, object] = {
        "schema_version": "loom.execution-unit-key.v1",
        "trial_id": str(trial_id),
        "attempt": attempt,
        "generation": generation,
        "execution_role": execution_role,
    }
    if verifier_retry:
        unit["verifier_retry"] = verifier_retry
    execution_unit_key = canonical_uuid5(_EXECUTION_UNIT_NAMESPACE, unit)
    provider_scope_key = canonical_digest(
        {
            "schema_version": "loom.provider-scope-key.v1",
            "target_id": target_id,
            "namespace": namespace_name,
            "execution_unit_key": str(execution_unit_key),
        },
        persisted=False,
    )
    return provider_scope_key, namespace_name, job_name, execution_unit_key


def _runtime_container_roles(runtime_contract: ExecutionRuntimePlanV1) -> list[str]:
    return [
        "execution",
        runtime_contract.main.role,
        *[sidecar.role_name for sidecar in runtime_contract.sidecars],
        *(["verifier"] if runtime_contract.verifier is not None else []),
    ]


async def persist_execution_catalog(
    session: AsyncSession,
    *,
    execution_class: ExecutionClassV1,
    targets: tuple[ExecutionTargetV1, ...],
) -> None:
    """Persist immutable catalog rows, rejecting same-id semantic drift."""

    class_json = execution_class.model_dump(mode="json")
    existing_class = await session.get(ServiceExecutionClass, execution_class.class_id)
    if existing_class is None:
        session.add(
            ServiceExecutionClass(
                id=execution_class.class_id,
                schema_version=execution_class.schema_version,
                spec_json=class_json,
                spec_sha256=canonical_digest(class_json),
                enabled=True,
            )
        )
        await session.flush()
    elif ExecutionClassV1.model_validate(existing_class.spec_json) != execution_class:
        raise ServiceExecutionConflict("execution class identity already has different content")

    for target in targets:
        if target.execution_class_id != execution_class.class_id:
            raise ServiceExecutionConflict("target binds a different execution class")
        if target.capacity_owner_target_id is not None:
            # Catalog membership changes invalidate older physical snapshots and
            # must serialize with observation and pre-create reservation writers.
            await session.execute(_CAPACITY_MUTATION_LOCK)
            owner = await session.get(ServiceExecutionTarget, target.capacity_owner_target_id)
            try:
                if owner is None:
                    raise ValueError("capacity owner is unavailable")
                validate_capacity_owner(target, ExecutionTargetV1.model_validate(owner.spec_json))
                family = await resolve_capacity_targets(session, owner.id)
                if target.target_id not in family.target_ids and len(family.target_ids) >= 64:
                    raise ValueError("capacity family cannot contain more than 64 targets")
            except ValueError as exc:
                raise ServiceExecutionConflict(str(exc)) from exc
        target_json = target.model_dump(mode="json")
        existing_target = await session.get(ServiceExecutionTarget, target.target_id)
        if existing_target is None:
            session.add(
                ServiceExecutionTarget(
                    id=target.target_id,
                    logical_pool_id=target.logical_pool_id,
                    execution_class_id=target.execution_class_id,
                    schema_version=target.schema_version,
                    spec_json=target_json,
                    spec_sha256=canonical_digest(target_json),
                    environment=target.environment,
                    provider=target.provider,
                    region=target.region,
                    failure_domain=target.failure_domain,
                    data_residency=target.data_residency,
                    desired_state="disabled",
                    observed_state="unknown",
                    health_status="unknown",
                )
            )
        elif ExecutionTargetV1.model_validate(existing_target.spec_json) != target:
            raise ServiceExecutionConflict(
                "execution target identity already has different content"
            )
    await session.flush()


async def set_execution_target_health(
    session: AsyncSession,
    *,
    target_id: str,
    desired_state: str,
    observed_state: str,
    health_status: str,
    observed_at: datetime,
    error_code: str | None = None,
) -> ServiceExecutionTarget:
    target = (
        await session.execute(
            select(ServiceExecutionTarget)
            .where(ServiceExecutionTarget.id == target_id)
            .with_for_update()
        )
    ).scalar_one_or_none()
    if target is None:
        raise ServiceExecutionConflict("execution target not found")
    target.desired_state = desired_state
    target.observed_state = observed_state
    target.health_status = health_status
    target.health_observed_at = observed_at
    target.health_error_code = error_code
    target.updated_at = datetime.now(UTC)
    await session.flush()
    return target


async def refresh_execution_target_health(
    session: AsyncSession,
    *,
    target_id: str,
    observed_at: datetime,
) -> ServiceExecutionTarget:
    """Refresh actuator-owned readiness without changing operator intent.

    The actuator proves that it can list and reconcile the target namespace.
    It must not silently re-enable a target that an operator has disabled or
    placed into draining state, so ``desired_state`` is deliberately preserved.
    """

    target = (
        await session.execute(
            select(ServiceExecutionTarget)
            .where(ServiceExecutionTarget.id == target_id)
            .with_for_update()
        )
    ).scalar_one_or_none()
    if target is None:
        raise ServiceExecutionConflict("execution target not found")
    if target.health_observed_at is not None and observed_at < target.health_observed_at:
        raise ServiceExecutionConflict("execution target health observation regressed")
    target.observed_state = "ready"
    target.health_status = "healthy"
    target.health_observed_at = observed_at
    target.health_error_code = None
    target.updated_at = datetime.now(UTC)
    await session.flush()
    return target


def _bind_kubernetes_execution_route(
    *,
    trial: Trial,
    target: ServiceExecutionTarget,
    execution_class_id: str,
    requirements_digest: str,
    routing_reason: ExecutionRoutingReason,
    current_time: datetime,
    allow_target_reselection: bool = False,
) -> tuple[ExecutionRoutingDecisionV1, str]:
    """Bind or validate the trial's sole physical execution route."""

    if trial.execution_route_json is None:
        if trial.execution_route_pool_name is not None or trial.execution_route_sha256 is not None:
            raise ServiceExecutionConflict("trial execution route is incomplete")
    else:
        try:
            decision = ExecutionRoutingDecisionV1.model_validate(trial.execution_route_json)
        except ValueError as exc:
            raise ServiceExecutionConflict("trial execution route is invalid") from exc
        decision_digest = canonical_digest(decision.model_dump(mode="json"))
        if (
            decision_digest != trial.execution_route_sha256
            or decision.generation != trial.execution_route_generation
            or decision.selected_pool_id != trial.execution_route_pool_name
        ):
            raise ServiceExecutionConflict("trial execution route identity drift")
        if decision.requirements_sha256 != requirements_digest:
            raise ServiceExecutionConflict("trial execution route requirements drift")
        if (
            decision.selected_adapter_kind != ExecutionAdapterKind.KUBERNETES_JOB
            or decision.selected_pool_id != target.logical_pool_id
            or decision.selected_execution_class_id != execution_class_id
        ):
            raise ServiceExecutionConflict("trial is routed to a different execution authority")
        if decision.selected_target_id == target.id:
            return decision, decision_digest
        if not allow_target_reselection:
            raise ServiceExecutionConflict("trial is routed to a different execution authority")

    decision = ExecutionRoutingDecisionV1(
        generation=trial.execution_route_generation + 1,
        requirements_sha256=requirements_digest,
        selected_pool_id=target.logical_pool_id,
        selected_adapter_kind=ExecutionAdapterKind.KUBERNETES_JOB,
        selected_target_id=target.id,
        selected_execution_class_id=execution_class_id,
        reason=routing_reason,
        decided_at=current_time,
        candidates=(
            ExecutionRouteCandidateV1(
                logical_pool_id=target.logical_pool_id,
                adapter_kind=ExecutionAdapterKind.KUBERNETES_JOB,
                target_id=target.id,
                execution_class_id=execution_class_id,
                environment=target.environment,
                region=target.region,
                data_residency=target.data_residency,
                enabled=target.desired_state == "active",
                healthy=(target.observed_state == "ready" and target.health_status == "healthy"),
                draining=False,
                configured_slots=0,
                active_slots=0,
                occupied_slots=0,
                pending_slots=0,
                assigned_queued_slots=0,
                available_slots=0,
                capacity_evidence_kind=CapacityEvidenceKind.PREEXISTING_ASSIGNMENT,
                capacity_observed_at=target.health_observed_at,
            ),
        ),
    )
    decision_json = decision.model_dump(mode="json")
    decision_digest = canonical_digest(decision_json)
    trial.execution_route_generation = decision.generation
    trial.execution_route_pool_name = decision.selected_pool_id
    trial.execution_route_json = decision_json
    trial.execution_route_sha256 = decision_digest
    return decision, decision_digest


async def _can_reselect_unstarted_execution(
    session: AsyncSession,
    *,
    trial: Trial,
    previous: ServiceExecutionLease | None,
    target: ServiceExecutionTarget,
) -> bool:
    """A cleaned infrastructure retry may acquire a new physical route."""
    if (
        previous is None
        or previous.execution_role != "attempt"
        or previous.attempt != trial.attempt_count
        or previous.revoked_at is None
        or previous.generation <= previous.resource_generation
        or previous.desired_state != "deleted"
        or previous.deleted_at is None
        or previous.cleanup_state != "complete"
        or previous.error_code != "unschedulable"
        or previous.pod_started_at is not None
        or previous.pod_scheduled_at is not None
        or previous.node_name is not None
        or previous.output_commit_state == "committed"
        or previous.finalized_at is not None
        or trial.started_at is not None
        or previous.routing_generation != trial.execution_route_generation
        or previous.routing_decision_sha256 != trial.execution_route_sha256
        or previous.selected_pool_id != trial.execution_route_pool_name
    ):
        return False
    old_target = await session.get(ServiceExecutionTarget, previous.target_id)
    if (
        old_target is None
        or old_target.environment != target.environment
        or old_target.data_residency != target.data_residency
    ):
        return False
    # Cleanup changes desired_state from retry to deleted. The existing command
    # retains the retry intent; a cancellation must never become a reroute.
    retry = await session.scalar(
        select(ServiceExecutionCommand.id).where(
            ServiceExecutionCommand.lease_id == previous.id,
            ServiceExecutionCommand.generation == previous.generation,
            ServiceExecutionCommand.command_type == "retry",
        )
    )
    if retry is None:
        return False
    for model, owner in (
        (ExecutionProvisioningAuthorization, ExecutionProvisioningAuthorization.lease_id),
        (ExecutionCostReservation, ExecutionCostReservation.lease_id),
        (ExecutionAdmissionReservation, ExecutionAdmissionReservation.owner_id),
    ):
        state = await session.scalar(select(model.state).where(owner == previous.id))
        if state != "released":
            return False
    return True


async def reserve_trial_execution(
    session: AsyncSession,
    *,
    request_id: UUID,
    trial_id: UUID,
    execution_class_id: str,
    target_id: str,
    requirements: WorkloadRequirementsV1,
    runtime_contract: ExecutionRuntimePlanV1,
    image_admission_keyring: ImageAdmissionKeyring,
    routing_reason: ExecutionRoutingReason = ExecutionRoutingReason.ADMIN_TARGET_BINDING,
    parent_lease_id: UUID | None = None,
    verifier_retry: int = 0,
    deadline_at: datetime,
    now: datetime | None = None,
    pool_handoff_id: UUID | None = None,
) -> ServiceExecutionLease:
    """Atomically reserve a trial and append its durable create command."""

    current_time = now or datetime.now(UTC)
    requirements_json = requirements.model_dump(mode="json")
    requirements_digest = canonical_digest(requirements_json)
    runtime_contract_json = runtime_contract.canonical_payload()
    runtime_contract_digest = canonical_digest(runtime_contract_json)
    execution_role = runtime_contract.execution_role
    existing = (
        await session.execute(
            select(ServiceExecutionLease).where(ServiceExecutionLease.request_id == request_id)
        )
    ).scalar_one_or_none()
    if existing is not None:
        if (
            existing.trial_id != trial_id
            or existing.execution_class_id != execution_class_id
            or existing.target_id != target_id
            or existing.workload_requirements_sha256 != requirements_digest
            or existing.runtime_contract_sha256 != runtime_contract_digest
            or existing.execution_role != execution_role
            or existing.parent_lease_id != parent_lease_id
            or existing.verifier_retry != verifier_retry
            or existing.deadline_at != deadline_at
        ):
            raise ServiceExecutionConflict("reservation request_id changed immutable identity")
        return existing

    from loom.nebius_rollout_guard import admission_open
    from loom_control_plane.execution_capacity import ExecutionProvisioningBlockedError

    if not await admission_open(session):
        raise ExecutionProvisioningBlockedError("platform_deploying", retry_after_seconds=15)

    trial = (
        await session.execute(
            select(Trial)
            .where(Trial.id == trial_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if trial is None:
        raise ServiceExecutionConflict("trial not found")
    parent_lease: ServiceExecutionLease | None = None
    previous_lease: ServiceExecutionLease | None = None
    if execution_role == "attempt":
        if parent_lease_id is not None or verifier_retry:
            raise ServiceExecutionConflict("attempt execution cannot have a parent lease")
        if trial.state != "queued":
            raise ServiceExecutionConflict("trial is not reservable")
        previous_lease = (
            await session.execute(
                select(ServiceExecutionLease)
                .where(
                    ServiceExecutionLease.trial_id == trial_id,
                    ServiceExecutionLease.execution_role == "attempt",
                )
                .order_by(ServiceExecutionLease.attempt.desc())
                .limit(1)
                .with_for_update()
            )
        ).scalar_one_or_none()
        if previous_lease is not None and previous_lease.cleanup_state != "complete":
            raise ServiceExecutionConflict("previous execution cleanup is not complete")
        attempt = trial.attempt_count + 1
    else:
        if parent_lease_id is None:
            raise ServiceExecutionConflict("verifier execution requires a parent lease")
        parent_lease = (
            await session.execute(
                select(ServiceExecutionLease)
                .where(ServiceExecutionLease.id == parent_lease_id)
                .with_for_update()
            )
        ).scalar_one_or_none()
        if parent_lease is None or parent_lease.runtime_contract_json is None:
            raise ServiceExecutionConflict("verifier parent lease is not eligible")
        try:
            parent_contract = ExecutionRuntimePlanV1.model_validate(
                parent_lease.runtime_contract_json
            )
        except ValueError as exc:
            raise ServiceExecutionConflict("verifier parent lease contract is invalid") from exc
        if (
            parent_lease.trial_id != trial.id
            or parent_lease.execution_role != "attempt"
            or parent_contract.execution_role != "attempt"
            or parent_contract.verifier_execution != "separate_execution"
            or parent_contract.candidate_sha != runtime_contract.candidate_sha
            or parent_contract.task_revision_sha256 != runtime_contract.task_revision_sha256
        ):
            raise ServiceExecutionConflict("verifier parent lease is not eligible")
        if (
            parent_lease.output_commit_state != "committed"
            or parent_lease.finalized_at is None
            or parent_lease.observed_state not in {"finalizing", "finalized", "deleted"}
            or _verifier_handoff(trial, parent_lease) is None
        ):
            raise ServiceExecutionConflict("verifier parent result is not ready")
        if parent_lease.cleanup_state != "complete":
            raise ServiceExecutionConflict("verifier parent cleanup is not complete")
        if (
            trial.state not in {"claimed", "running"}
            or trial.attempt_count != parent_lease.attempt
            or trial.cancellation_requested_at is not None
        ):
            raise ServiceExecutionConflict("trial is not awaiting its verifier")
        if runtime_contract.handoff_input is None:
            raise ServiceExecutionConflict("verifier execution requires a workspace handoff")
        if verifier_retry != verifier_retries(trial):
            raise ServiceExecutionConflict("verifier retry does not match the pending handoff")
        attempt = parent_lease.attempt
    if deadline_at <= current_time:
        raise ServiceExecutionConflict("execution deadline must be in the future")

    execution_class = await session.get(ServiceExecutionClass, execution_class_id)
    target = await session.get(ServiceExecutionTarget, target_id)
    if execution_class is None or not execution_class.enabled:
        raise ServiceExecutionConflict("execution class is unavailable")
    if target is None or target.execution_class_id != execution_class_id:
        raise ServiceExecutionConflict("execution target is unavailable")
    if target.desired_state != "active" or target.observed_state != "ready":
        raise ServiceExecutionConflict("execution target is not ready")
    if target.health_status != "healthy" or target.health_observed_at is None:
        raise ServiceExecutionConflict("execution target is not healthy")
    if (
        requirements.data_residency is not None
        and target.data_residency != requirements.data_residency
    ):
        raise ServiceExecutionConflict("execution target violates data residency")
    stale_after = int(target.spec_json["health_stale_after_seconds"])
    if target.health_observed_at + timedelta(seconds=stale_after) <= current_time:
        raise ServiceExecutionConflict("execution target health is stale")
    if runtime_contract.execution_class_id != execution_class_id:
        raise ServiceExecutionConflict("runtime plan binds a different execution class")
    if (runtime_contract.node_resource_allocation is not None
            and runtime_contract.node_resource_allocation.target_id != target_id):
        raise ServiceExecutionConflict("runtime allocation binds a different execution target")
    class_contract = ExecutionClassV1.model_validate(execution_class.spec_json)
    # The Trial association is the authority for user task images. Hold both
    # rows through reservation so retirement cannot race a new execution.
    try:
        grant = await get_trial_task_image_execution_grant(
            session, trial_id=trial_id, cpu_arches=[class_contract.cpu_architecture],
        )
    except (RuntimeError, ValueError) as exc:
        raise ServiceExecutionConflict("task image is not ready for this trial") from exc
    if grant is not None:
        if (
            runtime_contract.task_image_materialization_id != grant.materialization_id
            or runtime_contract.task_image_ref != grant.registry_images.get("task")
            or runtime_contract.task_revision_sha256 != "sha256:" + grant.task_checksum
        ):
            raise ServiceExecutionConflict("runtime plan does not match the trial's prepared image")
        from loom.task_fixtures import validate_prepared_fixtures

        frozen_task = TaskConfig.model_validate(grant.task_config)
        prepared_task = resolve_prepared_task(frozen_task, grant)
        try:
            validate_prepared_fixtures(
                runtime_contract, frozen_task=frozen_task, prepared_task=prepared_task,
            )
        except ValueError as exc:
            raise ServiceExecutionConflict(str(exc)) from exc
        effective_trial = (TrialConfig.model_validate(trial.config)
                           if prepared_task.service_execution is None else None)
        if requirements != workload_requirements_from_task(prepared_task, effective_trial):
            raise ServiceExecutionConflict("requirements do not match the prepared task")
    elif runtime_contract.task_image_materialization_id is not None:
        raise ServiceExecutionConflict("prepared image is not associated with this trial")
    try:
        verify_execution_image_admission(
            runtime_contract.image_admission,
            required_image_refs=runtime_contract.published_image_refs(),
            keyring=image_admission_keyring,
            now=current_time,
        )
    except ImageAdmissionError as exc:
        raise ServiceExecutionConflict(str(exc)) from exc
    pod_resources = runtime_pod_resources(runtime_contract)
    admission_requirements = requirements.model_copy(
        update={
            "cpu_millis": pod_resources.cpu_millis,
            "memory_mib": pod_resources.memory_mib,
            "ephemeral_storage_mib": pod_resources.ephemeral_storage_mib,
        }
    )
    admission = evaluate_execution_admission(admission_requirements, class_contract)
    if not admission.compatible:
        reason_codes = ",".join(reason.code for reason in admission.reasons)
        raise ServiceExecutionConflict(f"workload is not admitted: {reason_codes}")
    try:
        validate_runtime_plan_requirements(runtime_contract, requirements)
    except ValueError as exc:
        raise ServiceExecutionConflict(str(exc)) from exc

    global_identity = None
    if pool_handoff_id is not None:
        from loom_control_plane.pool_execution_handoff import qualify_execution_handoff

        global_identity = await qualify_execution_handoff(session, handoff_id=pool_handoff_id,
            request_id=request_id, trial=trial, target=target, requirements=requirements,
            runtime=runtime_contract, deadline_at=deadline_at, now=current_time)

    allow_target_reselection = False
    if (
        execution_role == "attempt"
        and previous_lease is not None
        and previous_lease.target_id != target.id
    ):
        allow_target_reselection = await _can_reselect_unstarted_execution(
            session, trial=trial, previous=previous_lease, target=target
        )
    routing_decision, routing_decision_digest = _bind_kubernetes_execution_route(
        trial=trial,
        target=target,
        execution_class_id=execution_class_id,
        requirements_digest=requirements_digest,
        routing_reason=routing_reason,
        current_time=current_time,
        allow_target_reselection=allow_target_reselection,
    )
    if parent_lease is not None and (
        parent_lease.routing_generation != routing_decision.generation
        or parent_lease.selected_pool_id != routing_decision.selected_pool_id
        or parent_lease.routing_decision_sha256 != routing_decision_digest
    ):
        raise ServiceExecutionConflict("verifier parent lease binds a different execution route")

    generation = 1
    lease_id = global_identity[0] if global_identity is not None else uuid4()
    provider_scope, namespace_name, job_name, execution_unit_key = _execution_identity(
        trial_id=trial.id,
        attempt=attempt,
        generation=generation,
        execution_role=execution_role,
        namespace_name=str(target.spec_json["namespace_name"]),
        target_id=target.id,
        verifier_retry=verifier_retry,
    )
    if global_identity is not None:
        job_name = global_identity[1]
    lease = ServiceExecutionLease(
        id=lease_id,
        request_id=request_id,
        trial_id=trial.id,
        team_id=trial.team_id,
        lifecycle_authority_id=trial.lifecycle_authority_id,
        attempt=attempt,
        execution_role=execution_role,
        verifier_retry=verifier_retry,
        parent_lease_id=parent_lease.id if parent_lease is not None else None,
        generation=generation,
        resource_generation=generation,
        execution_class_id=execution_class_id,
        target_id=target_id,
        routing_generation=routing_decision.generation,
        selected_pool_id=routing_decision.selected_pool_id,
        routing_reason=routing_decision.reason.value,
        routing_decision_sha256=routing_decision_digest,
        workload_requirements_json=requirements_json,
        workload_requirements_sha256=requirements_digest,
        runtime_contract_json=runtime_contract_json,
        runtime_contract_sha256=runtime_contract_digest,
        desired_state="create",
        observed_state="reserved",
        cleanup_state="not_requested",
        provider_scope_key=provider_scope,
        namespace_name=namespace_name,
        job_name=job_name,
        execution_unit_key=execution_unit_key,
        deadline_at=deadline_at,
    )
    try:
        await reserve_execution_admission(
            session,
            ExecutionAdmissionIdentity(
                trial_id=trial.id,
                attempt=attempt,
                execution_role=execution_role,
                team_id=trial.team_id,
                batch_id=trial.batch_id,
                environment=target.environment,
                region=target.region,
                execution_class_id=execution_class_id,
                pool_id=routing_decision.selected_pool_id,
                owner_kind="service_execution_lease",
                owner_id=lease_id,
            ),
            now=current_time,
        )
    except ExecutionAdmissionBlockedError as exc:
        raise ServiceExecutionConflict(exc.reason) from exc
    # The lease row is still uncommitted and cannot become executable without
    # the deferred outbox constraint. Persisting its identity first lets the
    # paid-execution reservation retain a real FK to the exact generation.
    session.add(lease)
    await session.flush()
    try:
        cost_reservation = await reserve_execution_cost(
            session,
            lease=lease,
            trial=trial,
            target=target,
            runtime_plan=runtime_contract,
            deadline_at=deadline_at,
            now=current_time,
        )
    except ExecutionFinanceBlockedError as exc:
        raise ServiceExecutionConflict(exc.reason) from exc
    # Capacity waits remain queued: reservation, budget and admission writes are
    # rolled back together before any outbox command or execution attempt exists.
    # The actuator reuses this authorization when it eventually creates the Job.
    if global_identity is None:
        await reserve_execution_provisioning(session, lease_id=lease.id, now=current_time)
    command_payload = {
        "schema_version": "loom.execution-command.v1",
        "lease_id": str(lease_id),
        "generation": generation,
        "command_type": "create",
        "trial_id": str(trial.id),
        "attempt": attempt,
        "execution_role": execution_role,
        "parent_lease_id": str(parent_lease.id) if parent_lease is not None else None,
        "execution_class_id": execution_class_id,
        "target_id": target_id,
        "routing_generation": routing_decision.generation,
        "selected_pool_id": routing_decision.selected_pool_id,
        "routing_reason": routing_decision.reason.value,
        "routing_decision_sha256": routing_decision_digest,
        "provider_scope_key": provider_scope,
        "namespace_name": namespace_name,
        "job_name": job_name,
        "execution_unit_key": str(execution_unit_key),
        "workload_requirements_sha256": requirements_digest,
        "runtime_contract_sha256": runtime_contract_digest,
        "candidate_sha": runtime_contract.candidate_sha,
        "task_revision_sha256": runtime_contract.task_revision_sha256,
        "task_image_ref": runtime_contract.task_image_ref,
        "runtime_image_ref": runtime_contract.runtime_image_ref,
        "command_identity_sha256": runtime_contract.command_identity_sha256,
        "container_roles": _runtime_container_roles(runtime_contract),
        "deadline_at": deadline_at.isoformat(),
        "cost_reservation_id": (str(cost_reservation.id) if cost_reservation is not None else None),
        "price_snapshot_id": (
            str(cost_reservation.price_snapshot_id) if cost_reservation is not None else None
        ),
        "estimated_cost_microusd": (
            cost_reservation.estimated_cost_microusd if cost_reservation is not None else None
        ),
        "cost_estimate_sha256": (
            cost_reservation.estimate_sha256 if cost_reservation is not None else None
        ),
    }
    command = ServiceExecutionCommand(
        id=uuid4(),
        lease_id=lease_id,
        generation=generation,
        sequence=1,
        command_type="create",
        idempotency_key=_command_key(lease_id, generation, "create"),
        payload_json=command_payload,
        payload_sha256=canonical_digest(command_payload),
        state=CommandState.PENDING,
        available_at=current_time,
    )
    # Flush the lease identity before its FK-bound outbox row.  Both writes
    # remain in this transaction; the deferred database trigger then proves
    # that the desired state cannot commit without the matching command.
    session.add(command)
    if execution_role == "attempt":
        trial.state = "claimed"
        trial.attempt_count = attempt
        trial.claimed_at = current_time
        trial.pre_start_heartbeat_at = None
        trial.worker_id = None
    await session.flush()
    return lease


async def enqueue_execution_transition(
    session: AsyncSession,
    *,
    lease_id: UUID,
    expected_generation: int,
    desired_state: str,
    payload: dict[str, Any] | None = None,
    now: datetime | None = None,
) -> ServiceExecutionCommand:
    if desired_state not in _DESIRED_TO_COMMAND:
        raise ServiceExecutionConflict("invalid execution desired state")
    lease = (
        await session.execute(
            select(ServiceExecutionLease)
            .where(ServiceExecutionLease.id == lease_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if lease is None:
        raise ServiceExecutionConflict("execution lease not found")
    if lease.generation != expected_generation:
        SERVICE_EXECUTION_STALE_GENERATIONS_TOTAL.labels(surface="command").inc()
        raise ServiceExecutionFenceError("execution generation is stale")
    if desired_state not in _ALLOWED_DESIRED_TRANSITIONS.get(lease.desired_state, frozenset()):
        raise ServiceExecutionConflict(
            f"invalid execution transition {lease.desired_state} -> {desired_state}"
        )
    if (
        lease.desired_state == "finalize"
        and desired_state == "delete_pending"
        and lease.output_commit_state != "committed"
    ):
        raise ServiceExecutionConflict("durable execution output is not committed")

    command_type = _DESIRED_TO_COMMAND[desired_state]
    next_generation = (
        lease.generation + 1 if desired_state in _REVOCATION_STATES else lease.generation
    )
    key = _command_key(lease.id, next_generation, command_type)
    existing = (
        await session.execute(
            select(ServiceExecutionCommand).where(ServiceExecutionCommand.idempotency_key == key)
        )
    ).scalar_one_or_none()
    if existing is not None:
        return existing

    retry_trial = None
    if desired_state == "retry":
        # Validate before changing lease intent: a rejected retry must not
        # autoflush a revocation or resurrect a reference retention has released.
        retry_trial = await session.get(
            Trial, lease.trial_id, with_for_update=True, populate_existing=True
        )
        if retry_trial is None or retry_trial.attempt_count != lease.attempt:
            raise ServiceExecutionFenceError("retry lost the trial attempt fence")
        if retry_trial.state in _TERMINAL_TRIAL_STATES:
            raise ServiceExecutionConflict("retry cannot reopen a terminal trial")

    # Generation and desired state are one atomic intent.  Set desired state
    # before any query can autoflush a generation change.
    lease.desired_state = desired_state
    if desired_state in _REVOCATION_STATES:
        revoked_at = now or datetime.now(UTC)
        lease.generation = next_generation
        lease.revoked_at = revoked_at
        lease.cleanup_state = "pending"
        lease.cleanup_requested_at = revoked_at
        lease.cleanup_deadline_at = revoked_at + _RESOURCE_RELEASE_DEADLINE
        if retry_trial is not None:
            retry_trial.state = "queued"
            retry_trial.worker_id = None
            retry_trial.claimed_at = None
            retry_trial.pre_start_heartbeat_at = None
            retry_trial.started_at = None
    sequence = (
        await session.execute(
            select(func.coalesce(func.max(ServiceExecutionCommand.sequence), 0)).where(
                ServiceExecutionCommand.lease_id == lease.id,
                ServiceExecutionCommand.generation == next_generation,
            )
        )
    ).scalar_one() + 1
    command_payload = {
        "schema_version": "loom.execution-command.v1",
        "lease_id": str(lease.id),
        "generation": next_generation,
        "command_type": command_type,
        "trial_id": str(lease.trial_id),
        "attempt": lease.attempt,
        "execution_role": lease.execution_role,
        "parent_lease_id": str(lease.parent_lease_id) if lease.parent_lease_id else None,
        "target_id": lease.target_id,
        "provider_scope_key": lease.provider_scope_key,
        "namespace_name": lease.namespace_name,
        "job_name": lease.job_name,
        "execution_unit_key": str(lease.execution_unit_key),
        **(payload or {}),
    }
    command = ServiceExecutionCommand(
        id=uuid4(),
        lease_id=lease.id,
        generation=next_generation,
        sequence=sequence,
        command_type=command_type,
        idempotency_key=key,
        payload_json=command_payload,
        payload_sha256=canonical_digest(command_payload),
        state=CommandState.PENDING,
        available_at=now or datetime.now(UTC),
    )
    lease.updated_at = now or datetime.now(UTC)
    session.add(command)
    await session.flush()
    return command


async def _acknowledge_native_cancellation(
    session: AsyncSession, *, trial_id: UUID, attempt: int | None = None,
) -> None:
    trial = await session.get(Trial, trial_id, with_for_update=True)
    if (
        trial is None
        or (attempt is not None and trial.attempt_count != attempt)
        or trial.state not in {"claimed", "running", "cancelled"}
        or trial.cancellation_requested_at is None
        or trial.cancellation_observed_at is not None
    ):
        return
    leases = (await session.scalars(select(ServiceExecutionLease).where(
        ServiceExecutionLease.trial_id == trial.id,
        ServiceExecutionLease.attempt == trial.attempt_count,
    ))).all()
    if not leases or any(
        lease.team_id != trial.team_id or lease.cleanup_state != "complete" or lease.deleted_at is None
        for lease in leases
    ):
        return
    # Keep the request separate from durable cleanup of the whole attempt,
    # including a separate verifier. Replay also repairs historical nulls from
    # already retained deletion evidence without reopening an execution lease.
    trial.cancellation_observed_at = max(
        trial.cancellation_requested_at,
        max(lease.deleted_at for lease in leases if lease.deleted_at is not None),
    )


async def _cancel_awaiting_verifier(session: AsyncSession, *, trial_id: UUID, now: datetime) -> None:
    """Close a handed-off attempt whose verifier lease was never reserved."""
    trial = await session.get(Trial, trial_id, with_for_update=True)
    handoff = (trial.result or {}).get("verifier_execution") if trial is not None else None
    if (
        trial is None
        or trial.state not in {"claimed", "running"}
        or trial.cancellation_requested_at is None
        or not isinstance(handoff, dict)
        or handoff.get("state") != "pending"
    ):
        return
    trial.result = {**(trial.result or {}), "cancelled": True}
    trial.state = "cancelled"
    trial.finished_at = now
    trial.failure_reason = "cancelled"
    trial.failure_message = "service execution cancelled before verification"


async def request_trial_execution_cancellation(
    session: AsyncSession,
    *,
    trial_id: UUID,
    now: datetime | None = None,
) -> ServiceExecutionCommand | None:
    """Revoke the active provider authority when a Trial is cancelled."""

    lease = (
        await session.execute(
            select(ServiceExecutionLease)
            .where(
                ServiceExecutionLease.trial_id == trial_id,
                ServiceExecutionLease.deleted_at.is_(None),
            )
            # A live deferred verifier is the attempt's current execution.
            .order_by(
                ServiceExecutionLease.attempt.desc(),
                (ServiceExecutionLease.execution_role == "verifier").desc(),
            )
            .limit(1)
        )
    ).scalar_one_or_none()
    if lease is None:
        await _cancel_awaiting_verifier(session, trial_id=trial_id, now=now or datetime.now(UTC))
        await _acknowledge_native_cancellation(session, trial_id=trial_id)
        return None
    if lease.desired_state in {"create", "start"}:
        desired_state = "cancel"
    elif lease.desired_state == "finalize":
        # The committed result remains authoritative. The deletion observation
        # must finalize it before the execution record becomes immutable.
        desired_state = "delete_pending"
    else:
        # Revocation/deletion is already durable. Repeated cancellation is
        # idempotent at the provider-resource boundary.
        return None
    return await enqueue_execution_transition(
        session,
        lease_id=lease.id,
        expected_generation=lease.generation,
        desired_state=desired_state,
        now=now,
    )


async def mark_execution_output_unavailable(
    session: AsyncSession,
    *,
    lease_id: UUID,
    expected_generation: int,
    reason: str,
    now: datetime | None = None,
    allow_cancel_before_deadline: bool = False,
) -> ServiceExecutionLease:
    """Close a revoked Pod's bounded output window before resource deletion."""

    if not reason or len(reason) > 120:
        raise ServiceExecutionConflict("invalid output unavailable reason")
    current_time = now or datetime.now(UTC)
    lease = (
        await session.execute(
            select(ServiceExecutionLease)
            .where(ServiceExecutionLease.id == lease_id)
            .with_for_update()
        )
    ).scalar_one_or_none()
    if lease is None:
        raise ServiceExecutionConflict("execution lease not found")
    if lease.generation != expected_generation:
        raise ServiceExecutionFenceError("execution generation is stale")
    if lease.output_commit_state in {"committed", "unavailable"}:
        return lease
    cancellation_may_close_now = (
        allow_cancel_before_deadline and lease.desired_state == "cancel" and lease.pod_uid is None
    )
    if (
        lease.revoked_at is None
        or lease.cleanup_state != "pending"
        or lease.cleanup_deadline_at is None
        or (current_time < lease.cleanup_deadline_at and not cancellation_may_close_now)
    ):
        raise ServiceExecutionConflict("execution output window remains open")
    lease.output_commit_state = "unavailable"
    lease.output_generation = lease.resource_generation
    lease.output_manifest_sha256 = None
    lease.output_marker_sha256 = None
    lease.output_committed_at = None
    lease.output_unavailable_reason = reason
    lease.updated_at = current_time
    await session.flush()
    return lease


async def claim_execution_commands(
    session: AsyncSession,
    *,
    consumer_id: str,
    target_id: str,
    limit: int,
    lease_seconds: int,
    now: datetime | None = None,
) -> tuple[ClaimedExecutionCommand, ...]:
    """Lease commands only for one target, filtering before limit and row locking."""

    current_time = now or datetime.now(UTC)
    if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,79}", target_id):
        raise ServiceExecutionConflict("invalid command target identity")
    if not consumer_id or len(consumer_id) > 120:
        raise ServiceExecutionConflict("invalid command consumer identity")
    if limit < 1 or limit > 100 or lease_seconds < 5 or lease_seconds > 300:
        raise ServiceExecutionConflict("invalid command claim bounds")
    rows = (
        (
            await session.execute(
                select(ServiceExecutionCommand)
                .where(
                    ServiceExecutionCommand.lease_id.in_(
                        select(ServiceExecutionLease.id).where(
                            ServiceExecutionLease.target_id == target_id
                        )
                    ),
                    or_(
                        ServiceExecutionCommand.state == CommandState.PENDING,
                        (
                            (ServiceExecutionCommand.state == CommandState.LEASED)
                            & (ServiceExecutionCommand.claim_expires_at <= current_time)
                        ),
                    ),
                    ServiceExecutionCommand.available_at <= current_time,
                )
                .order_by(
                    ServiceExecutionCommand.available_at,
                    ServiceExecutionCommand.created_at,
                    ServiceExecutionCommand.id,
                )
                .limit(limit)
                .with_for_update(skip_locked=True)
            )
        )
        .scalars()
        .all()
    )
    claimed: list[ClaimedExecutionCommand] = []
    expires_at = current_time + timedelta(seconds=lease_seconds)
    for row in rows:
        if row.state == CommandState.LEASED:
            SERVICE_EXECUTION_DUPLICATE_DELIVERIES_TOTAL.labels(command_type=row.command_type).inc()
        row.state = CommandState.LEASED
        row.claimed_by = consumer_id
        row.claim_expires_at = expires_at
        row.delivery_count += 1
        row.updated_at = current_time
        claimed.append(
            ClaimedExecutionCommand(
                id=row.id,
                lease_id=row.lease_id,
                generation=row.generation,
                sequence=row.sequence,
                command_type=row.command_type,
                idempotency_key=row.idempotency_key,
                payload=row.payload_json,
                delivery_count=row.delivery_count,
                claim_expires_at=expires_at,
            )
        )
    await session.flush()
    await refresh_service_execution_metrics(session, now=current_time)
    return tuple(claimed)


async def acknowledge_execution_command(
    session: AsyncSession,
    *,
    command_id: UUID,
    consumer_id: str,
    acknowledgement: dict[str, Any],
    now: datetime | None = None,
) -> ServiceExecutionCommand:
    row = (
        await session.execute(
            select(ServiceExecutionCommand)
            .where(ServiceExecutionCommand.id == command_id)
            .with_for_update()
        )
    ).scalar_one_or_none()
    if row is None:
        raise ServiceExecutionConflict("execution command not found")
    digest = canonical_digest(acknowledgement)
    if row.state == CommandState.ACKNOWLEDGED:
        if row.acknowledgement_sha256 != digest:
            raise ServiceExecutionConflict("command acknowledgement replay changed")
        return row
    if row.state != CommandState.LEASED or row.claimed_by != consumer_id:
        raise ServiceExecutionFenceError("command delivery lease is not authoritative")
    row.state = CommandState.ACKNOWLEDGED
    row.claimed_by = None
    row.claim_expires_at = None
    row.acknowledged_at = now or datetime.now(UTC)
    row.acknowledgement_sha256 = digest
    row.updated_at = now or datetime.now(UTC)
    await session.flush()
    return row


async def defer_execution_command(
    session: AsyncSession,
    *,
    command_id: UUID,
    consumer_id: str,
    error_code: str,
    error_message: str,
    retry_after_seconds: int,
    max_deliveries: int = 20,
    now: datetime | None = None,
) -> ServiceExecutionCommand:
    """Release a failed delivery with bounded backoff or dead-letter it."""

    if retry_after_seconds < 1 or retry_after_seconds > 300 or max_deliveries < 1:
        raise ServiceExecutionConflict("invalid command retry bounds")
    row = (
        await session.execute(
            select(ServiceExecutionCommand)
            .where(ServiceExecutionCommand.id == command_id)
            .with_for_update()
        )
    ).scalar_one_or_none()
    if row is None:
        raise ServiceExecutionConflict("execution command not found")
    if row.state != CommandState.LEASED or row.claimed_by != consumer_id:
        raise ServiceExecutionFenceError("command delivery lease is not authoritative")
    current_time = now or datetime.now(UTC)
    row.last_error_code = error_code[:120]
    row.last_error_message = error_message[:2000]
    row.claimed_by = None
    row.claim_expires_at = None
    row.updated_at = current_time
    if row.delivery_count >= max_deliveries:
        row.state = CommandState.DEAD_LETTER
    else:
        row.state = CommandState.PENDING
        row.available_at = current_time + timedelta(seconds=retry_after_seconds)
    await session.flush()
    return row


async def verify_trial_execution_fence(
    session: AsyncSession,
    *,
    trial_id: UUID,
    lease_id: UUID | None,
    generation: int | None,
    surface: str,
    allow_terminal_event: bool = False,
    lock: bool = False,
) -> ExecutionFence | None:
    """Allow legacy trials without a lease; fail closed once a lease exists."""

    statement = select(ServiceExecutionLease).where(ServiceExecutionLease.trial_id == trial_id)
    if lease_id is not None:
        statement = statement.where(ServiceExecutionLease.id == lease_id)
    else:
        statement = (
            statement.where(ServiceExecutionLease.execution_role == "attempt")
            .order_by(ServiceExecutionLease.attempt.desc())
            .limit(1)
        )
    if lock:
        statement = statement.with_for_update()
    lease = (await session.execute(statement)).scalar_one_or_none()
    if lease is None:
        return None
    valid = (
        lease_id is not None
        and generation is not None
        and lease.id == lease_id
        and lease.generation == generation
        and (lease.revoked_at is None or allow_terminal_event)
        and lease.deleted_at is None
    )
    if not valid:
        SERVICE_EXECUTION_STALE_GENERATIONS_TOTAL.labels(surface=surface).inc()
        raise ServiceExecutionFenceError("execution lease generation is not authoritative")
    runtime_contract: ExecutionRuntimePlanV1 | None = None
    if lease.runtime_contract_json is not None and lease.runtime_contract_sha256 is not None:
        try:
            candidate = ExecutionRuntimePlanV1.model_validate(lease.runtime_contract_json)
            if canonical_digest(candidate.canonical_payload()) == lease.runtime_contract_sha256:
                runtime_contract = candidate
        except ValueError:
            runtime_contract = None
    return ExecutionFence(
        lease_id=lease.id,
        generation=lease.generation,
        trial_id=lease.trial_id,
        attempt=lease.attempt,
        execution_role=lease.execution_role,
        runtime_contract_sha256=(
            lease.runtime_contract_sha256 if runtime_contract is not None else None
        ),
        candidate_sha=runtime_contract.candidate_sha if runtime_contract is not None else None,
        task_revision_sha256=(
            runtime_contract.task_revision_sha256 if runtime_contract is not None else None
        ),
        command_identity_sha256=(
            runtime_contract.command_identity_sha256 if runtime_contract is not None else None
        ),
    )


async def recover_execution_node_attribution(
    session: AsyncSession,
    *,
    lease_id: UUID,
    apply: bool = False,
) -> dict[str, object]:
    """Recover only missing placement from unambiguous, lease-bound Pod evidence."""
    lease = (
        await session.execute(
            select(ServiceExecutionLease)
            .where(ServiceExecutionLease.id == lease_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if lease is None:
        raise ServiceExecutionConflict("execution lease not found")
    result: dict[str, object] = {
        "lease_id": str(lease.id),
        "target_id": lease.target_id,
        "resource_generation": lease.resource_generation,
        "pod_uid": lease.pod_uid,
        "node_name": lease.node_name,
        "status": "unchanged",
        "evidence_event_ids": [],
    }
    if lease.node_name is not None:
        return result
    if not lease.pod_uid:
        raise ServiceExecutionConflict("node recovery requires a persisted Pod UID")
    events = (
        await session.scalars(
            select(ServiceExecutionEvent)
            .where(
                ServiceExecutionEvent.lease_id == lease.id,
                ServiceExecutionEvent.event_kind == "kubernetes_observed",
                ServiceExecutionEvent.generation.between(
                    lease.resource_generation, lease.generation
                ),
            )
            .order_by(ServiceExecutionEvent.generation, ServiceExecutionEvent.ordinal)
        )
    ).all()
    nodes: set[str] = set()
    evidence: list[str] = []
    for event in events:
        payload = event.payload_json
        node = _bounded_optional_text(payload.get("node_name"), 253, "node_name")
        if node is None or payload.get("pod_uid") is None:
            continue
        if payload["pod_uid"] != lease.pod_uid or (
            payload.get("job_uid") is not None and payload["job_uid"] != lease.job_uid
        ):
            raise ServiceExecutionConflict("node recovery evidence conflicts with Pod/Job identity")
        nodes.add(node)
        evidence.append(str(event.id))
    if len(nodes) != 1:
        raise ServiceExecutionConflict("node recovery requires exactly one evidenced node")
    node = next(iter(nodes))
    if apply:
        lease.node_name = node
        lease.updated_at = datetime.now(UTC)
        await session.flush()
    result.update(
        node_name=node,
        status="recovered" if apply else "recoverable",
        evidence_event_ids=evidence,
    )
    return result


_EVENT_PAYLOAD_REFERENCE = "loom.execution-event-payload-reference.v1"
_EVENT_PAYLOAD_METADATA = "execution_event_payloads"


async def _execution_event_artifact(
    session: AsyncSession, *, lease: ServiceExecutionLease, for_update: bool = False,
) -> Artifact:
    """Resolve the immutable output owner, including after source-spool cleanup."""
    statement = select(Artifact).where(
        Artifact.control_producer_kind == "service_execution",
        Artifact.control_producer_id == lease.id,
    )
    if for_update:
        statement = statement.with_for_update()
    artifact = await session.scalar(statement.execution_options(populate_existing=True))
    if (
        artifact is None
        or lease.output_commit_state != "committed"
        or lease.output_upload_session_id is None
        or lease.output_generation != lease.resource_generation
        or artifact.team_id != lease.team_id
        or artifact.trial_id != lease.trial_id
        or artifact.artifact_upload_session_id != lease.output_upload_session_id
        or artifact.provenance.get("lease_id") != str(lease.id)
        or artifact.provenance.get("generation") != lease.resource_generation
        or artifact.provenance.get("runtime_contract_sha256") != lease.runtime_contract_sha256
    ):
        raise ServiceExecutionConflict("execution event payload artifact identity drift")
    return artifact


def _event_payload_reference(
    *, artifact: Artifact, generation: int, ordinal: int, event_kind: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    reference: dict[str, Any] = {
        "schema_version": _EVENT_PAYLOAD_REFERENCE,
        "artifact_id": str(artifact.id),
        "payload_key": f"{generation}:{ordinal}:{event_kind}",
        "payload_sha256": canonical_digest(payload),
    }
    # Keep the bounded failure reason visible to lifecycle diagnosis readers.
    if event_kind == "finalized" and isinstance(payload.get("failure_reason"), str):
        reference["failure_reason"] = payload["failure_reason"][:120]
    return reference


async def _stored_execution_event_payload(
    session: AsyncSession, *, lease: ServiceExecutionLease, generation: int,
    ordinal: int, event_kind: str, payload: dict[str, Any],
) -> dict[str, Any]:
    if event_kind not in {"result_reported", "finalized"}:
        return payload
    # Use the database's representation: JSONB expands exponent-form numbers,
    # so even a small Python JSON document can exceed the actual constraint.
    size = await session.scalar(select(func.octet_length(cast(literal(payload, JSONB), Text))))
    if size is not None and size <= 65_536:
        return payload
    artifact = await _execution_event_artifact(session, lease=lease, for_update=True)
    reference = _event_payload_reference(
        artifact=artifact, generation=generation, ordinal=ordinal,
        event_kind=event_kind, payload=payload,
    )
    metadata = artifact.artifact_metadata or {}
    payloads = metadata.get(_EVENT_PAYLOAD_METADATA, {})
    if not isinstance(payloads, dict):
        raise ServiceExecutionConflict("execution event payload metadata is invalid")
    key = reference["payload_key"]
    # Preserve numeric types as JSON text. Both JSONB and JCS can turn valid
    # floats into integer tokens outside the canonicalizer's safe integer range.
    # The reference still binds the full payload's canonical digest.
    serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False)
    if key in payloads and payloads[key] != serialized:
        raise ServiceExecutionConflict("execution event payload identity drift")
    artifact.artifact_metadata = {
        **metadata, _EVENT_PAYLOAD_METADATA: {**payloads, key: serialized},
    }
    return reference


async def _resolve_execution_event_payload(
    session: AsyncSession, event: ServiceExecutionEvent,
) -> dict[str, Any]:
    """Verify both the stored event and its full, lease-owned payload on replay."""
    reference = event.payload_json
    if reference.get("schema_version") != _EVENT_PAYLOAD_REFERENCE:
        return reference
    if canonical_digest(reference) != event.payload_sha256:
        raise ServiceExecutionConflict("execution event payload digest drift")
    lease = await session.get(ServiceExecutionLease, event.lease_id)
    if lease is None or event.event_kind not in {"result_reported", "finalized"}:
        raise ServiceExecutionConflict("execution event payload reference is invalid")
    artifact = await _execution_event_artifact(session, lease=lease)
    payloads = (artifact.artifact_metadata or {}).get(_EVENT_PAYLOAD_METADATA, {})
    key = f"{event.generation}:{event.ordinal}:{event.event_kind}"
    serialized = payloads.get(key) if isinstance(payloads, dict) else None
    try:
        payload = json.loads(serialized) if isinstance(serialized, str) else None
        expected = _event_payload_reference(
            artifact=artifact, generation=event.generation, ordinal=event.ordinal,
            event_kind=event.event_kind, payload=payload,
        ) if isinstance(payload, dict) else None
    except ValueError as exc:
        raise ServiceExecutionConflict("execution event payload is invalid") from exc
    if not isinstance(payload, dict) or reference != expected:
        raise ServiceExecutionConflict("execution event payload reference identity drift")
    return payload


async def _execution_event_full_digest(
    session: AsyncSession, event: ServiceExecutionEvent,
) -> str:
    if event.payload_json.get("schema_version") != _EVENT_PAYLOAD_REFERENCE:
        # Preserve legacy inline replay semantics, including JSONB numeric
        # normalization: this digest was computed before the database round trip.
        return event.payload_sha256
    return canonical_digest(await _resolve_execution_event_payload(session, event))


async def record_execution_event(
    session: AsyncSession,
    *,
    lease_id: UUID,
    generation: int,
    ordinal: int,
    event_kind: str,
    payload: dict[str, Any],
    observed_at: datetime,
    idempotency_key: str | None = None,
) -> tuple[ServiceExecutionEvent, bool]:
    if event_kind not in {
        "created",
        "started",
        "heartbeat",
        "gateway_call",
        "artifact_committed",
        "trajectory_committed",
        "usage_reported",
        "result_reported",
        "kubernetes_observed",
        "cancelled",
        "timed_out",
        "failed",
        "finalized",
        "deleted",
    }:
        raise ServiceExecutionConflict("invalid execution event kind")
    if ordinal <= 0 or ordinal > 10_000:
        raise ServiceExecutionConflict("execution event ordinal must be between 1 and 10000")
    key = idempotency_key or _event_key(lease_id, generation, ordinal, event_kind)
    payload_digest = canonical_digest(payload)
    existing = (
        await session.execute(
            select(ServiceExecutionEvent).where(ServiceExecutionEvent.idempotency_key == key)
        )
    ).scalar_one_or_none()
    if existing is not None:
        if (
            existing.lease_id != lease_id
            or existing.generation != generation
            or existing.ordinal != ordinal
            or existing.event_kind != event_kind
            or await _execution_event_full_digest(session, existing) != payload_digest
        ):
            raise ServiceExecutionConflict("execution event replay changed")
        SERVICE_EXECUTION_DUPLICATE_DELIVERIES_TOTAL.labels(command_type="event").inc()
        return existing, True

    lease = (
        await session.execute(
            select(ServiceExecutionLease)
            .where(ServiceExecutionLease.id == lease_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if lease is None:
        raise ServiceExecutionConflict("execution lease not found")
    if event_kind == "result_reported":
        if lease.runtime_contract_json is None or lease.runtime_contract_sha256 is None:
            raise ServiceExecutionConflict("runtime result has no lease-bound contract")
        try:
            runtime_contract = ExecutionRuntimePlanV1.model_validate(lease.runtime_contract_json)
            runtime_result = ExecutionRuntimeResultV1.model_validate(payload)
        except ValueError as exc:
            raise ServiceExecutionConflict("runtime result contract is invalid") from exc
        identity_matches = (
            runtime_result.runtime_contract_sha256 == lease.runtime_contract_sha256
            and runtime_result.candidate_sha == runtime_contract.candidate_sha
            and runtime_result.task_revision_sha256 == runtime_contract.task_revision_sha256
            and runtime_result.command_identity_sha256 == runtime_contract.command_identity_sha256
            and runtime_result.execution_role == lease.execution_role
            and runtime_result.execution_class_id == lease.execution_class_id
            and runtime_result.task_image_ref == runtime_contract.task_image_ref
            and runtime_result.runtime_image_ref == runtime_contract.runtime_image_ref
            and runtime_result.runtime_binary_sha256 == runtime_contract.runtime_binary_sha256
            and list(runtime_result.container_roles) == _runtime_container_roles(runtime_contract)
        )
        expected_phase_roles = [
            *[phase.role for phase in runtime_contract.setup],
            runtime_contract.main.role,
            *([runtime_contract.verifier.role] if runtime_contract.verifier else []),
        ]
        actual_phase_roles = [phase.role for phase in runtime_result.phases]
        phases_match = actual_phase_roles == expected_phase_roles[: len(actual_phase_roles)]
        if runtime_result.status == "succeeded":
            phases_match = phases_match and len(actual_phase_roles) == len(expected_phase_roles)
        if not identity_matches or not phases_match:
            raise ServiceExecutionConflict("runtime result identity does not match lease")
        if any(
            stream.bytes_saved > runtime_contract.max_log_bytes_per_stream
            for phase in runtime_result.phases
            for stream in (phase.stdout, phase.stderr)
        ):
            raise ServiceExecutionConflict("runtime result exceeds lease log bounds")
    await verify_trial_execution_fence(
        session,
        trial_id=lease.trial_id,
        lease_id=lease_id,
        generation=generation,
        surface=event_kind,
        allow_terminal_event=(
            event_kind in _TERMINAL_EVENT_KINDS
            or (event_kind == "result_reported" and lease.desired_state == "cancel"
                and lease.output_commit_state == "committed")
            or (
                event_kind == "kubernetes_observed"
                and lease.desired_state
                in {"cancel", "timeout", "retry", "delete_pending", "deleted"}
            )
        ),
    )
    if lease.desired_state not in _EVENT_ALLOWED_DESIRED[event_kind]:
        raise ServiceExecutionConflict(
            f"execution event {event_kind} is invalid for desired state {lease.desired_state}"
        )
    advances_projection = ordinal > lease.last_event_ordinal
    finalized_projection: tuple[Trial, str] | None = None
    trial_state = payload.get("trial_state")
    if event_kind == "finalized" and advances_projection:
        # Exact replay returned above. A new projection must validate the fresh
        # Trial under its row lock before adding the event or mutating the lease.
        finalized_trial = await session.get(
            Trial, lease.trial_id, with_for_update={"key_share": True}, populate_existing=True
        )
        if (
            finalized_trial is None
            or not isinstance(trial_state, str)
            or trial_state not in {"running", "materializing", "succeeded", "failed", "cancelled"}
        ):
            raise ServiceExecutionConflict("finalized event lacks a valid trial state")
        if finalized_trial.state in _TERMINAL_TRIAL_STATES and trial_state in {"running", "materializing"}:
            raise ServiceExecutionConflict("finalized event cannot reopen a terminal trial")
        if trial_state == "running" and not (
            defers_verification(lease)
            and isinstance(payload.get("result"), dict)
            and _verifier_handoff_result(payload["result"], lease)
        ):
            raise ServiceExecutionConflict("only a verifier handoff keeps its trial running")
        if trial_state in {"running", "materializing", "succeeded"} and not isinstance(
            payload.get("result"), dict
        ):
            raise ServiceExecutionConflict("successful compute finalization requires a result")
        finalized_projection = (finalized_trial, trial_state)
    closes_committed_cleanup = (
        advances_projection
        and lease.desired_state == "delete_pending"
        and lease.finalized_at is None
        and lease.output_commit_state == "committed"
        and (
            event_kind == "deleted"
            or (
                event_kind == "kubernetes_observed" and payload.get("normalized_state") == "deleted"
            )
        )
    )
    committed_cleanup_payload = None
    if closes_committed_cleanup:
        trial = await session.get(Trial, lease.trial_id, with_for_update=True, populate_existing=True)
        if (
            trial is None
            or trial.attempt_count != lease.attempt
            or trial.state not in {"claimed", "running"}
        ):
            raise ServiceExecutionConflict("committed execution no longer owns the trial attempt")
        committed_cleanup_payload = await _committed_result_finalization_payload(session, lease=lease)
        finalized_projection = (trial, committed_cleanup_payload["trial_state"])
    stored_payload = await _stored_execution_event_payload(
        session, lease=lease, generation=generation, ordinal=ordinal,
        event_kind=event_kind, payload=payload,
    )
    event = ServiceExecutionEvent(
        id=uuid4(),
        lease_id=lease_id,
        generation=generation,
        ordinal=ordinal,
        event_kind=event_kind,
        idempotency_key=key,
        payload_json=stored_payload,
        payload_sha256=canonical_digest(stored_payload),
        observed_at=observed_at,
    )
    session.add(event)
    completes_cancellation = (
        advances_projection
        and lease.desired_state == "cancel"
        and lease.finalized_at is None
        and lease.output_commit_state in {"committed", "unavailable"}
        and (
            event_kind == "deleted"
            or (
                event_kind == "kubernetes_observed" and payload.get("normalized_state") == "deleted"
            )
        )
    )
    if completes_cancellation:
        trial = await session.get(
            Trial, lease.trial_id, with_for_update=True, populate_existing=True
        )
        if (
            trial is not None
            and trial.attempt_count == lease.attempt
            and trial.state in {"claimed", "running"}
        ):
            # The fenced deletion event closes this attempt; its raw result and
            # usage remain intact. Existing DB triggers release the team counter.
            if lease.output_commit_state == "committed":
                committed = await _committed_result_finalization_payload(session, lease=lease)
                trial.result = {**committed["result"], "cancelled": True}
            elif trial.result is None:
                trial.result = {
                    "schema_version": "loom.service-execution-trial-result.v1",
                    "cancelled": True,
                    "partial_evidence": True,
                    "reward": None,
                    "aggregate_reward": None,
                    "output_unavailable_reason": lease.output_unavailable_reason,
                }
            trial.state = "cancelled"
            trial.finished_at = observed_at
            trial.failure_reason = "cancelled"
            trial.failure_message = "service execution cancelled"
            lease.finalized_at = observed_at
    if advances_projection:
        lease.last_event_ordinal = ordinal
    if event_kind == "heartbeat" and advances_projection:
        lease.last_heartbeat_at = observed_at
    if (event_kind in _EVENT_TO_OBSERVED and advances_projection
            and not (event_kind == "failed" and lease.finalized_at is not None)):
        lease.observed_state = _EVENT_TO_OBSERVED[event_kind]
    if event_kind == "kubernetes_observed" and advances_projection:
        normalized_state = payload.get("normalized_state")
        observed_states = {
            "absent": "reserved",
            "missing": "failed",
            "pending": "creating",
            "unschedulable": "creating",
            "image_pull_backoff": "creating",
            "running": "running",
            "succeeded": "finalizing",
            "failed": "failed",
            "oom_killed": "failed",
            "evicted": "failed",
            "node_lost": "failed",
            "deadline_exceeded": "failed",
            "terminating": "delete_pending",
            "deleted": "deleted",
        }
        if not isinstance(normalized_state, str) or normalized_state not in observed_states:
            raise ServiceExecutionConflict("invalid normalized Kubernetes state")
        for field in ("job_uid", "pod_uid"):
            incoming = payload.get(field)
            current = getattr(lease, field)
            if incoming is not None and not isinstance(incoming, str):
                raise ServiceExecutionConflict(f"invalid Kubernetes {field}")
            if current is not None and incoming is not None and current != incoming:
                raise ServiceExecutionConflict(f"Kubernetes {field} changed")
            if incoming is not None:
                setattr(lease, field, incoming)
        incoming_pod_ip = _validated_pod_ip(payload.get("pod_ip"))
        current_pod_ip = str(lease.pod_ip) if lease.pod_ip is not None else None
        if (
            current_pod_ip is not None
            and incoming_pod_ip is not None
            and current_pod_ip != incoming_pod_ip
        ):
            raise ServiceExecutionConflict("Kubernetes pod_ip changed")
        if incoming_pod_ip is not None:
            lease.pod_ip = incoming_pod_ip
        lease.kubernetes_resource_version = _bounded_optional_text(
            payload.get("resource_version"), 128, "resource_version"
        )
        incoming_node_name = _bounded_optional_text(payload.get("node_name"), 253, "node_name")
        if incoming_node_name is not None:
            if lease.node_name is not None and lease.node_name != incoming_node_name:
                raise ServiceExecutionConflict("Kubernetes node_name changed")
            lease.node_name = incoming_node_name
        incoming_scheduled_at = _optional_datetime(payload.get("scheduled_at"), "scheduled_at")
        incoming_started_at = _optional_datetime(payload.get("started_at"), "started_at")
        incoming_terminated_at = _optional_datetime(payload.get("terminated_at"), "terminated_at")
        if incoming_scheduled_at is not None:
            lease.pod_scheduled_at = (
                min(lease.pod_scheduled_at, incoming_scheduled_at)
                if lease.pod_scheduled_at is not None
                else incoming_scheduled_at
            )
        if incoming_started_at is not None:
            lease.pod_started_at = (
                min(lease.pod_started_at, incoming_started_at)
                if lease.pod_started_at is not None
                else incoming_started_at
            )
        if incoming_terminated_at is not None:
            lease.pod_terminated_at = (
                max(lease.pod_terminated_at, incoming_terminated_at)
                if lease.pod_terminated_at is not None
                else incoming_terminated_at
            )
        if (
            lease.pod_scheduled_at is not None
            and lease.pod_started_at is not None
            and lease.pod_scheduled_at > lease.pod_started_at
        ):
            lease.pod_scheduled_at = lease.pod_started_at
        if (
            lease.pod_started_at is not None
            and lease.pod_terminated_at is not None
            and lease.pod_started_at > lease.pod_terminated_at
        ):
            lease.pod_terminated_at = lease.pod_started_at
        lease.last_reconciled_at = observed_at
        # Native sidecars may restart repeatedly before the Pod is removed.
        # Keep the first observed loss for this exact Pod as the attempt cause;
        # later status events still retain their own complete diagnostics.
        first_sandbox_loss = None
        if (
            lease.finalized_at is None
            and lease.desired_state != "retry"
            and lease.error_code in _NATIVE_TERMINAL_FAILURES
            and lease.pod_uid is not None
            and lease.job_uid is not None
            and payload.get("job_uid") == lease.job_uid
            and payload.get("pod_uid") in {None, lease.pod_uid}
        ):
            first_sandbox_loss = await session.scalar(
                select(ServiceExecutionEvent)
                .where(
                    ServiceExecutionEvent.lease_id == lease.id,
                    ServiceExecutionEvent.generation == generation,
                    ServiceExecutionEvent.ordinal < ordinal,
                    ServiceExecutionEvent.event_kind == "kubernetes_observed",
                    ServiceExecutionEvent.payload_json["job_uid"].astext == lease.job_uid,
                    ServiceExecutionEvent.payload_json["pod_uid"].astext == lease.pod_uid,
                    ServiceExecutionEvent.payload_json["reason"].astext.in_(
                        {"SandboxRestarted", "SandboxTerminated"}
                    ),
                    ServiceExecutionEvent.payload_json["normalized_state"].astext.in_(
                        _NATIVE_TERMINAL_FAILURES
                    ),
                )
                .order_by(ServiceExecutionEvent.ordinal)
                .limit(1)
            )
        projected_state = observed_states[normalized_state]
        if first_sandbox_loss is not None and normalized_state not in {"terminating", "deleted"}:
            projected_state = "failed"
        if lease.finalized_at is not None:
            if lease.observed_state == "deleted" or normalized_state == "deleted":
                projected_state = "deleted"
            elif lease.observed_state == "delete_pending" or normalized_state == "terminating":
                projected_state = "delete_pending"
            else:
                projected_state = "finalized"
        lease.observed_state = projected_state
        if first_sandbox_loss is not None:
            lease.error_class = "permanent"
            lease.error_code = first_sandbox_loss.payload_json["normalized_state"]
            lease.error_message = _bounded_optional_text(
                first_sandbox_loss.payload_json.get("message"), 2000, "message"
            )
        elif (
            lease.finalized_at is None
            and lease.desired_state != "retry"
            and normalized_state
            in {
                "unschedulable",
                "missing",
                "image_pull_backoff",
                "failed",
                "oom_killed",
                "evicted",
                "node_lost",
                "deadline_exceeded",
            }
        ):
            # Once the exact Pod has explained its failure, its disappearance
            # leaves only a generic Job failure. Do not erase that explanation.
            preserve_pod_failure = (
                normalized_state == "failed"
                and payload.get("reason") == "BackoffLimitExceeded"
                and payload.get("pod_uid") is None
                and lease.pod_uid is not None
                and lease.job_uid is not None
                and payload.get("job_uid") == lease.job_uid
                and lease.error_code in _NATIVE_TERMINAL_FAILURES - {"failed"}
            )
            if not preserve_pod_failure:
                lease.error_class = (
                    "transient"
                    if normalized_state in {"missing", "unschedulable", "evicted", "node_lost"}
                    else "permanent"
                )
                lease.error_code = normalized_state
                lease.error_message = _bounded_optional_text(
                    payload.get("message"), 2000, "message"
                )
        elif lease.finalized_at is None and lease.desired_state != "retry":
            lease.error_class = None
            lease.error_code = None
            lease.error_message = None
        if normalized_state == "deleted" and lease.desired_state in {
            "cancel",
            "timeout",
            "retry",
            "delete_pending",
        }:
            lease.desired_state = "deleted"
            lease.deleted_at = observed_at
            lease.cleanup_state = "complete"
    if finalized_projection is not None:
        lease.finalized_at = observed_at
        trial, trial_state = finalized_projection
        final_payload = committed_cleanup_payload or payload
        trial.state = trial_state
        trial.result = (
            final_payload.get("result") if isinstance(final_payload.get("result"), dict) else None
        )
        trial.failure_reason = (
            final_payload.get("failure_reason")
            if isinstance(final_payload.get("failure_reason"), str)
            else None
        )
        trial.failure_message = (
            final_payload.get("failure_message")
            if isinstance(final_payload.get("failure_message"), str)
            else None
        )
        trial.finished_at = None if trial_state in {"running", "materializing"} else observed_at
    if event_kind == "deleted" and advances_projection:
        lease.desired_state = "deleted"
        lease.deleted_at = observed_at
        lease.cleanup_state = "complete"
    elif event_kind == "failed" and advances_projection:
        lease.error_class = str(payload.get("error_class") or "permanent")
        lease.error_code = str(payload.get("error_code") or "execution_failed")[:120]
        raw_message = payload.get("error_message")
        lease.error_message = str(raw_message)[:2000] if raw_message is not None else None
    if (
        advances_projection
        and lease.execution_role == "attempt"
        and lease.desired_state in {"create", "start", "finalize"}
        and lease.revoked_at is None
        and event_kind in {"started", "kubernetes_observed"}
    ):
        trial = await session.get(Trial, lease.trial_id, with_for_update=True)
        if (
            trial is not None
            and trial.attempt_count == lease.attempt
            and trial.state in {"claimed", "running"}
        ):
            actual_start = lease.pod_started_at if event_kind == "kubernetes_observed" else observed_at
            if actual_start is not None and trial.started_at is None:
                trial.started_at = actual_start
            if event_kind == "started" or (
                payload.get("normalized_state") == "running" and actual_start is not None
            ):
                # Native attempts do not call the legacy worker PATCH /state.
                # Update the durable Trial so list, batch and monitor agree.
                trial.state = "running"
    if event_kind in {"kubernetes_observed", "failed", "finalized"}:
        from loom.execution_diagnosis_store import read_execution_failure

        # Facts can arrive after runtime finalization or out of delivery order.
        # Enrich the existing failed outcome; never reopen it or alter artifacts.
        await session.flush()
        diagnosis = await read_execution_failure(session, lease)
        if diagnosis is not None and diagnosis["reason"] == "oom_killed":
            lease.error_class = "permanent"
            lease.error_code = "oom_killed"
            lease.error_message = diagnosis["message"]
            diagnosed_trial = await session.get(Trial, lease.trial_id, with_for_update=True)
            if (diagnosed_trial is not None and diagnosed_trial.state == "failed"
                    and diagnosed_trial.attempt_count == lease.attempt):
                diagnosed_trial.failure_reason = "oom_killed"
                diagnosed_trial.failure_message = diagnosis["message"]
    if advances_projection and lease.cleanup_state == "complete" and lease.deleted_at is not None:
        await _acknowledge_native_cancellation(session, trial_id=lease.trial_id, attempt=lease.attempt)
    lease.updated_at = datetime.now(UTC)
    await session.flush()
    return event, False


async def record_kubernetes_observation(
    session: AsyncSession,
    *,
    lease_id: UUID,
    generation: int,
    payload: dict[str, Any],
    observed_at: datetime,
) -> tuple[ServiceExecutionEvent, bool]:
    """Persist one resourceVersion/state observation exactly once."""

    idempotency_key = canonical_digest(
        {
            "schema_version": "loom.kubernetes-observation-key.v1",
            "lease_id": str(lease_id),
            "generation": generation,
            "job_uid": payload.get("job_uid"),
            "pod_uid": payload.get("pod_uid"),
            "resource_version": payload.get("resource_version"),
            "pod_resource_version": payload.get("pod_resource_version"),
            "normalized_state": payload.get("normalized_state"),
        },
        persisted=False,
    )
    existing = (
        await session.execute(
            select(ServiceExecutionEvent).where(
                ServiceExecutionEvent.idempotency_key == idempotency_key
            )
        )
    ).scalar_one_or_none()
    if existing is not None:
        ordinal = existing.ordinal
    else:
        lease = (
            await session.execute(
                select(ServiceExecutionLease)
                .where(ServiceExecutionLease.id == lease_id)
                .with_for_update()
            )
        ).scalar_one_or_none()
        if lease is None:
            raise ServiceExecutionConflict("execution lease not found")
        ordinal = lease.last_event_ordinal + 1
    return await record_execution_event(
        session,
        lease_id=lease_id,
        generation=generation,
        ordinal=ordinal,
        event_kind="kubernetes_observed",
        payload=payload,
        observed_at=observed_at,
        idempotency_key=idempotency_key,
    )


async def retry_unscheduled_execution_after_deadline(
    session: AsyncSession,
    *,
    lease_id: UUID,
    generation: int,
    observed_at: datetime,
) -> bool:
    """Recover an unstarted capacity wait through the existing fenced cleanup."""

    lease = await session.get(ServiceExecutionLease, lease_id, with_for_update=True)
    if (
        lease is None
        or lease.generation != generation
        or lease.execution_role != "attempt"
        or lease.desired_state != "create"
        or lease.revoked_at is not None
        or lease.deadline_at > observed_at
        or lease.error_code != "unschedulable"
        or lease.pod_started_at is not None
        or lease.pod_scheduled_at is not None
        or lease.node_name is not None
        or lease.output_commit_state == "committed"
    ):
        return False
    trial = await session.get(Trial, lease.trial_id, with_for_update=True)
    if trial is None or trial.started_at is not None or trial.state != "claimed":
        return False
    attempt_ceiling = await session.scalar(
        select(TeamQuota.max_attempts_ceiling).where(TeamQuota.team_id == trial.team_id)
    )
    await enqueue_execution_transition(
        session,
        lease_id=lease.id,
        expected_generation=generation,
        desired_state="retry",
        now=observed_at,
    )
    trial.next_attempt_at = observed_at + timedelta(seconds=15)
    lease.error_class = "transient"
    lease.error_code = (
        "infra_recovery_exhausted"
        if attempt_ceiling is not None and trial.attempt_count >= attempt_ceiling
        else "unschedulable"
    )
    # Existing attempts remain immutable history. Cleanup releases the same
    # authorization and budget before the scheduler can acquire a new lease.
    await session.flush()
    return True


async def record_committed_runtime_result(
    session: AsyncSession,
    *,
    lease_id: UUID,
    generation: int,
    runtime_result: ExecutionRuntimeResultV1,
    observed_at: datetime,
) -> ServiceExecutionEvent:
    """Advance a committed runtime result to the durable finalize intent."""

    lease = await session.get(ServiceExecutionLease, lease_id, with_for_update=True)
    if lease is None or lease.generation != generation:
        raise ServiceExecutionFenceError("execution generation is stale")
    if lease.output_commit_state != "committed":
        raise ServiceExecutionConflict("runtime result is not durably committed")
    result_payload = runtime_result.model_dump(mode="json")
    existing = (
        await session.execute(
            select(ServiceExecutionEvent)
            .where(
                ServiceExecutionEvent.lease_id == lease.id,
                ServiceExecutionEvent.generation == lease.generation,
                ServiceExecutionEvent.event_kind == "result_reported",
            )
            .order_by(ServiceExecutionEvent.ordinal.desc())
            .limit(1)
        )
    ).scalar_one_or_none()
    if existing is not None:
        if await _execution_event_full_digest(session, existing) != canonical_digest(result_payload):
            raise ServiceExecutionConflict("committed runtime result identity drift")
        return existing
    if lease.desired_state == "create":
        await enqueue_execution_transition(
            session,
            lease_id=lease.id,
            expected_generation=lease.generation,
            desired_state="start",
            now=observed_at,
        )
    if lease.desired_state == "start":
        await enqueue_execution_transition(
            session,
            lease_id=lease.id,
            expected_generation=lease.generation,
            desired_state="finalize",
            now=observed_at,
        )
    if lease.desired_state not in {"finalize", "cancel"}:
        raise ServiceExecutionConflict("runtime result cannot enter finalize state")
    event, _ = await record_execution_event(
        session,
        lease_id=lease.id,
        generation=lease.generation,
        ordinal=lease.last_event_ordinal + 1,
        event_kind="result_reported",
        payload=result_payload,
        observed_at=observed_at,
    )
    return event


async def _committed_result_finalization_payload(
    session: AsyncSession, *, lease: ServiceExecutionLease,
) -> dict[str, Any]:
    """Derive the outcome from the immutable resource generation's committed result."""

    if lease.output_generation != lease.resource_generation:
        raise ServiceExecutionConflict("committed output generation does not match execution")
    result_event = (
        await session.execute(
            select(ServiceExecutionEvent)
            .where(
                ServiceExecutionEvent.lease_id == lease.id,
                ServiceExecutionEvent.generation.in_((lease.resource_generation, lease.generation)),
                ServiceExecutionEvent.event_kind == "result_reported",
            )
            .order_by(ServiceExecutionEvent.ordinal.desc())
            .limit(1)
        )
    ).scalar_one_or_none()
    if result_event is None:
        raise ServiceExecutionConflict("finalizing execution has no committed runtime result")
    runtime_result = ExecutionRuntimeResultV1.model_validate(
        await _resolve_execution_event_payload(session, result_event),
    )
    if lease.execution_role == "verifier":
        return await _verifier_finalization_payload(session, lease=lease, runtime_result=runtime_result)
    result: dict[str, Any] = {
        "schema_version": "loom.service-execution-trial-result.v1",
        "runtime_result": runtime_result.model_dump(mode="json"),
        "reward": runtime_result.verifier_rewards,
        "aggregate_reward": aggregate_reward_scalar(runtime_result.verifier_rewards),
        "output_manifest_sha256": lease.output_manifest_sha256,
        "output_marker_sha256": lease.output_marker_sha256,
    }
    if runtime_result.status == "succeeded" and defers_verification(lease):
        trial = await session.get(Trial, lease.trial_id, with_for_update=True)
        if trial is None:
            raise ServiceExecutionConflict("trial not found")
        if trial.cancellation_requested_at is not None or trial.state in _TERMINAL_TRIAL_STATES:
            return {
                "trial_state": "cancelled",
                "result": {**result, "cancelled": True},
                "failure_reason": "cancelled",
                "failure_message": "service execution cancelled before verification",
            }
        # The agent's evidence is committed; its own verifier lease grades it
        # after this pod releases capacity.
        return {
            "trial_state": "running",
            "result": {
                **result,
                "verifier_execution": {"state": "pending", "parent_lease_id": str(lease.id)},
            },
            "failure_reason": None,
            "failure_message": None,
        }
    if runtime_result.status == "succeeded":
        trial_state = "materializing"
        failure_reason = None
        failure_message = None
    else:
        trial_state = "cancelled" if runtime_result.status == "cancelled" else "failed"
        failure_reason = runtime_result.status
        failure_message = f"service execution runtime reported {runtime_result.status}"
    return {
        "trial_state": trial_state,
        "result": result,
        "failure_reason": failure_reason,
        "failure_message": failure_message,
    }


def defers_verification(lease: ServiceExecutionLease) -> bool:
    """A separate-mode attempt whose grading belongs to a child verifier lease."""
    contract = lease.runtime_contract_json or {}
    return (
        lease.execution_role == "attempt"
        and contract.get("execution_role") == "attempt"
        and contract.get("verifier_execution") == "separate_execution"
    )


class VerifierHandoffUnavailableError(ValueError):
    pass


async def committed_handoff_files(
    session: AsyncSession, *, parent: ServiceExecutionLease,
) -> tuple[list[tuple[str, int, str]], list[str]]:
    """The parent attempt's verified committed outputs and their spool keys."""
    if parent.output_commit_state != "committed" or parent.output_upload_session_id is None:
        raise VerifierHandoffUnavailableError("verifier_handoff_unavailable")
    upload = await session.get(ArtifactUploadSession, parent.output_upload_session_id)
    artifact = await session.scalar(select(Artifact).where(
        Artifact.control_producer_kind == "service_execution",
        Artifact.control_producer_id == parent.id,
    ))
    if (
        upload is None or artifact is None or upload.state != "committed"
        or upload.manifest_sha256 != parent.output_manifest_sha256
    ):
        raise VerifierHandoffUnavailableError("verifier_handoff_unavailable")
    rows = (await session.scalars(
        select(ArtifactUploadFile)
        .where(ArtifactUploadFile.session_id == upload.id)
        .order_by(ArtifactUploadFile.file_index)
    )).all()
    files: list[tuple[str, int, str]] = []
    keys: list[str] = []
    for row in rows:
        if row.state != "verified" or row.actual_size is None or row.computed_sha256 is None:
            raise VerifierHandoffUnavailableError("verifier_handoff_unavailable")
        files.append((row.relative_path, row.actual_size, row.computed_sha256))
        keys.append(f"{upload.prefix}artifacts/{artifact.id}/{row.relative_path}")
    return files, keys


def verifier_retries(trial: Trial) -> int:
    """Verifier leases already spent on the pending handoff (0 before any retry)."""
    handoff = (trial.result or {}).get("verifier_execution")
    retries = handoff.get("retries", 0) if isinstance(handoff, dict) else 0
    return retries if isinstance(retries, int) and retries >= 0 else 0


def _retry_verifier(trial: Trial, lease: ServiceExecutionLease, *, reason: str, now: datetime) -> bool:
    """Return the handoff to pending for another verifier lease, if still allowed.

    Only an infrastructure failure without committed output reaches here; a
    graded verifier result, including reward 0, is never retried.
    """
    handoff = (trial.result or {}).get("verifier_execution")
    if (
        lease.verifier_retry >= MAX_VERIFIER_RETRIES
        or trial.cancellation_requested_at is not None
        or not isinstance(handoff, dict)
        or handoff.get("parent_lease_id") != str(lease.parent_lease_id)
    ):
        return False
    trial.result = {
        **(trial.result or {}),
        "verifier_execution": {
            "state": "pending",
            "parent_lease_id": str(lease.parent_lease_id),
            "retries": lease.verifier_retry + 1,
            "last_retry_reason": reason[:120],
            "pending_since": now.isoformat(),
        },
    }
    return True


def mark_verifier_unavailable(trial: Trial, lease: ServiceExecutionLease | None, *, reason: str) -> None:
    result = dict(trial.result or {})
    handoff = dict(result.get("verifier_execution") or {})
    handoff.update({"state": "unavailable", "reason": reason[:120]})
    if lease is not None:
        handoff["lease_id"] = str(lease.id)
    result["verifier_execution"] = handoff
    trial.result = result
    trial.failure_reason = "verifier_unavailable"


def _verifier_handoff_result(result: dict[str, Any], parent: ServiceExecutionLease) -> bool:
    handoff = result.get("verifier_execution")
    return (
        isinstance(handoff, dict)
        and handoff.get("state") == "pending"
        and handoff.get("parent_lease_id") == str(parent.id)
        and result.get("reward") is None
    )


def _verifier_handoff(trial: Trial, parent: ServiceExecutionLease) -> dict[str, Any] | None:
    handoff = (trial.result or {}).get("verifier_execution")
    if (
        not isinstance(handoff, dict)
        or handoff.get("state") != "pending"
        or handoff.get("parent_lease_id") != str(parent.id)
        or not defers_verification(parent)
    ):
        return None
    return handoff


async def _verifier_finalization_payload(
    session: AsyncSession, *, lease: ServiceExecutionLease, runtime_result: ExecutionRuntimeResultV1,
) -> dict[str, Any]:
    """The verifier owns the reward and terminal state of its parent's attempt."""

    trial = await session.get(Trial, lease.trial_id, with_for_update=True)
    parent = (
        await session.get(ServiceExecutionLease, lease.parent_lease_id)
        if lease.parent_lease_id is not None else None
    )
    if trial is None or parent is None or not isinstance(trial.result, dict):
        raise ServiceExecutionConflict("verifier has no awaiting parent attempt")
    handoff = trial.result.get("verifier_execution")
    if not isinstance(handoff, dict) or handoff.get("parent_lease_id") != str(parent.id):
        raise ServiceExecutionConflict("verifier has no awaiting parent attempt")
    result = {
        **trial.result,
        "reward": runtime_result.verifier_rewards,
        "aggregate_reward": aggregate_reward_scalar(runtime_result.verifier_rewards),
        "verifier_execution": {
            "state": "committed",
            "parent_lease_id": str(parent.id),
            "lease_id": str(lease.id),
            "runtime_result": runtime_result.model_dump(mode="json"),
            "output_manifest_sha256": lease.output_manifest_sha256,
            "output_marker_sha256": lease.output_marker_sha256,
        },
    }
    if runtime_result.status == "succeeded":
        return {"trial_state": "materializing", "result": result,
                "failure_reason": None, "failure_message": None}
    if runtime_result.status == "cancelled":
        return {"trial_state": "cancelled", "result": {**result, "cancelled": True},
                "failure_reason": "cancelled", "failure_message": "service execution cancelled"}
    return {
        "trial_state": "failed",
        "result": result,
        "failure_reason": "verifier_error",
        "failure_message": f"deferred verifier runtime reported {runtime_result.status}",
    }


async def recover_deleted_committed_trial(
    session: AsyncSession, *, lease: ServiceExecutionLease, observed_at: datetime,
) -> None:
    """Recover archival eligibility without mutating a deleted execution record.

    The materializer holds the lease and Trial locks. Only the original attempt's
    committed result may close a Trial left running by an older controller.
    """

    if (
        lease.desired_state != "deleted"
        or lease.observed_state != "deleted"
        or lease.deleted_at is None
        or lease.cleanup_state != "complete"
        or lease.execution_role != "attempt"
        or lease.output_commit_state != "committed"
        or lease.finalized_at is not None
    ):
        raise ServiceExecutionConflict("execution is not eligible for archival recovery")
    trial = await session.get(Trial, lease.trial_id, with_for_update=True, populate_existing=True)
    if trial is None or trial.attempt_count != lease.attempt:
        raise ServiceExecutionConflict("committed execution no longer owns the trial attempt")
    if trial.state not in {"claimed", "running"}:
        return
    payload = await _committed_result_finalization_payload(session, lease=lease)
    trial.state = payload["trial_state"]
    trial.result = payload["result"]
    trial.failure_reason = payload["failure_reason"]
    trial.failure_message = payload["failure_message"]
    trial.finished_at = None if trial.state in {"materializing", "running"} else observed_at
    if trial.state == "running":
        # A handed-off attempt is closed; its verifier lease now owns the Trial.
        lease.finalized_at = observed_at


async def finalize_committed_service_execution(
    session: AsyncSession,
    *,
    lease_id: UUID,
    observed_at: datetime,
) -> bool:
    """Finalize and enqueue cleanup after Kubernetes confirms termination."""

    lease = await session.get(ServiceExecutionLease, lease_id, with_for_update=True)
    if lease is None:
        raise ServiceExecutionConflict("execution lease not found")
    if (
        lease.desired_state != "finalize"
        or lease.observed_state not in {"finalizing", "failed"}
        or lease.output_commit_state != "committed"
    ):
        return False
    payload = await _committed_result_finalization_payload(session, lease=lease)
    await record_execution_event(
        session,
        lease_id=lease.id,
        generation=lease.generation,
        ordinal=lease.last_event_ordinal + 1,
        event_kind="finalized",
        payload=payload,
        observed_at=observed_at,
    )
    await enqueue_execution_transition(
        session,
        lease_id=lease.id,
        expected_generation=lease.generation,
        desired_state="delete_pending",
        now=observed_at,
    )
    return True


async def finalize_failed_service_execution(
    session: AsyncSession,
    *,
    lease_id: UUID,
    generation: int,
    observed_at: datetime,
) -> bool:
    """Close a native terminal failure after the bounded late-output window.

    Leave create/start authoritative during that window so an upload already in
    flight can commit its real result. Reconciliation reuses the first durable
    terminal observation, including after restart; it never refreshes the timer.
    """

    lease = await session.get(ServiceExecutionLease, lease_id, with_for_update=True)
    if lease is None:
        raise ServiceExecutionConflict("execution lease not found")
    if lease.generation != generation:
        raise ServiceExecutionFenceError("execution generation is stale")
    if (
        lease.desired_state not in {"create", "start"}
        or lease.revoked_at is not None
        or lease.finalized_at is not None
        or lease.observed_state != "failed"
        or lease.output_commit_state not in {"not_started", "uploading"}
        or lease.error_code not in _NATIVE_TERMINAL_FAILURES
    ):
        return False
    failure = await session.scalar(
        select(ServiceExecutionEvent)
        .where(
            ServiceExecutionEvent.lease_id == lease.id,
            ServiceExecutionEvent.generation == generation,
            ServiceExecutionEvent.event_kind == "kubernetes_observed",
            ServiceExecutionEvent.payload_json["normalized_state"].astext.in_(
                _NATIVE_TERMINAL_FAILURES
            ),
        )
        .order_by(ServiceExecutionEvent.ordinal)
        .limit(1)
    )
    if failure is None or observed_at < failure.observed_at + _RESOURCE_RELEASE_DEADLINE:
        return False
    trial = await session.get(Trial, lease.trial_id, with_for_update=True)
    if (
        trial is None
        or trial.attempt_count != lease.attempt
        or trial.state not in {"claimed", "running"}
    ):
        return False
    reason = (
        failure.payload_json.get("reason")
        if failure.payload_json.get("normalized_state") == lease.error_code
        else None
    ) or lease.error_code
    message = f"Native execution failed ({reason}) before durable runtime output was committed."
    if lease.error_message:
        message += f" {lease.error_message}"
    await record_execution_event(
        session,
        lease_id=lease.id,
        generation=generation,
        ordinal=lease.last_event_ordinal + 1,
        event_kind="failed",
        payload={
            "error_class": lease.error_class,
            "error_code": lease.error_code,
            "error_message": message,
        },
        observed_at=observed_at,
    )
    # The lease lock serializes this decision with the output commit projection.
    # Keep any upload session for diagnostics; no reward or artifact is invented.
    lease.output_commit_state = "unavailable"
    lease.output_generation = lease.resource_generation
    lease.output_unavailable_reason = "native_execution_failed"
    lease.finalized_at = observed_at
    lease.observed_state = "finalized"
    failure_code = lease.error_code or "native_execution_failed"
    # A retried verifier keeps the Trial running; the scheduler reserves the
    # next verifier lease only after this pod is deleted.
    if not (lease.execution_role == "verifier" and _retry_verifier(
        trial, lease, reason=failure_code, now=observed_at,
    )):
        trial.state = "failed"
        trial.failure_reason = "oom_killed" if lease.error_code == "oom_killed" else "native_execution_failed"
        trial.failure_message = lease.error_message if lease.error_code == "oom_killed" else message
        trial.finished_at = observed_at
        if lease.execution_role == "verifier":
            # The agent's committed attempt is still archived; only grading is lost.
            mark_verifier_unavailable(trial, lease, reason=failure_code)
            trial.failure_message = message
    await enqueue_execution_transition(
        session,
        lease_id=lease.id,
        expected_generation=generation,
        desired_state="delete_pending",
        now=observed_at,
    )
    return True


async def refresh_service_execution_metrics(
    session: AsyncSession,
    *,
    now: datetime | None = None,
) -> None:
    current_time = now or datetime.now(UTC)
    pending, cleanup, oldest = (
        await session.execute(
            select(
                func.count(ServiceExecutionCommand.id).filter(
                    ServiceExecutionCommand.state.in_(["pending", "leased"])
                ),
                func.count(func.distinct(ServiceExecutionLease.id)).filter(
                    ServiceExecutionLease.cleanup_state.in_(["pending", "in_progress", "blocked"])
                ),
                func.min(ServiceExecutionCommand.created_at).filter(
                    ServiceExecutionCommand.state.in_(["pending", "leased"])
                ),
            )
            .select_from(ServiceExecutionLease)
            .outerjoin(
                ServiceExecutionCommand,
                ServiceExecutionCommand.lease_id == ServiceExecutionLease.id,
            )
        )
    ).one()
    SERVICE_EXECUTION_COMMAND_BACKLOG.set(int(pending or 0))
    SERVICE_EXECUTION_CLEANUP_DEBT.set(int(cleanup or 0))
    from loom_control_plane.metrics import SERVICE_EXECUTION_RECONCILE_LAG_SECONDS

    SERVICE_EXECUTION_RECONCILE_LAG_SECONDS.set(
        max(0.0, (current_time - oldest).total_seconds()) if oldest is not None else 0.0
    )


def execution_lease_projection(lease: ServiceExecutionLease) -> dict[str, Any]:
    runtime_identity: dict[str, Any] | None = None
    if lease.runtime_contract_json is not None and lease.runtime_contract_sha256 is not None:
        try:
            plan = ExecutionRuntimePlanV1.model_validate(lease.runtime_contract_json)
            if canonical_digest(plan.canonical_payload()) == lease.runtime_contract_sha256:
                runtime_identity = {
                    "candidate_sha": plan.candidate_sha,
                    "task_revision_sha256": plan.task_revision_sha256,
                    "task_image_ref": plan.task_image_ref,
                    "runtime_image_ref": plan.runtime_image_ref,
                    "execution_role": plan.execution_role,
                    "command_identity_sha256": plan.command_identity_sha256,
                    "container_roles": _runtime_container_roles(plan),
                    "image_admission_sha256": canonical_digest(
                        plan.image_admission.model_dump(mode="json")
                    ),
                    "image_admissions": [
                        {
                            "image_ref": item.statement.image_ref,
                            "signing_key_id": item.signing_key_id,
                            "sbom_sha256": item.statement.sbom_sha256,
                            "provenance_sha256": item.statement.provenance_sha256,
                            "vulnerability_report_sha256": (
                                item.statement.vulnerability_report_sha256
                            ),
                            "policy_sha256": item.statement.policy_sha256,
                            "highest_vulnerability_severity": (
                                item.statement.highest_vulnerability_severity
                            ),
                            "expires_at": item.statement.expires_at.isoformat(),
                        }
                        for item in plan.image_admission.admissions
                    ],
                }
        except ValueError:
            runtime_identity = None
    return {
        "schema_version": "loom.execution-lease-projection.v1",
        "lease_id": str(lease.id),
        "trial_id": str(lease.trial_id),
        "attempt": lease.attempt,
        "execution_role": lease.execution_role,
        "parent_lease_id": str(lease.parent_lease_id) if lease.parent_lease_id else None,
        "generation": lease.generation,
        "resource_generation": lease.resource_generation,
        "execution_class_id": lease.execution_class_id,
        "target_id": lease.target_id,
        "routing_generation": lease.routing_generation,
        "selected_pool_id": lease.selected_pool_id,
        "routing_reason": lease.routing_reason,
        "routing_decision_sha256": lease.routing_decision_sha256,
        "runtime_contract_sha256": lease.runtime_contract_sha256,
        "runtime_identity": runtime_identity,
        "desired_state": lease.desired_state,
        "observed_state": lease.observed_state,
        "cleanup_state": lease.cleanup_state,
        "cleanup_requested_at": (
            lease.cleanup_requested_at.isoformat() if lease.cleanup_requested_at else None
        ),
        "cleanup_deadline_at": (
            lease.cleanup_deadline_at.isoformat() if lease.cleanup_deadline_at else None
        ),
        "provider_scope_key": lease.provider_scope_key,
        "namespace_name": lease.namespace_name,
        "job_name": lease.job_name,
        "execution_unit_key": str(lease.execution_unit_key),
        "job_uid": lease.job_uid,
        "pod_uid": lease.pod_uid,
        "pod_ip": str(lease.pod_ip) if lease.pod_ip is not None else None,
        "kubernetes_resource_version": lease.kubernetes_resource_version,
        "node_name": lease.node_name,
        "deadline_at": lease.deadline_at.isoformat(),
        "pod_scheduled_at": (
            lease.pod_scheduled_at.isoformat() if lease.pod_scheduled_at else None
        ),
        "pod_started_at": lease.pod_started_at.isoformat() if lease.pod_started_at else None,
        "pod_terminated_at": (
            lease.pod_terminated_at.isoformat() if lease.pod_terminated_at else None
        ),
        "last_reconciled_at": (
            lease.last_reconciled_at.isoformat() if lease.last_reconciled_at else None
        ),
        "last_event_ordinal": lease.last_event_ordinal,
        "last_heartbeat_at": (
            lease.last_heartbeat_at.isoformat() if lease.last_heartbeat_at else None
        ),
        "revoked_at": lease.revoked_at.isoformat() if lease.revoked_at else None,
        "finalized_at": lease.finalized_at.isoformat() if lease.finalized_at else None,
        "output_commit_state": lease.output_commit_state,
        "output_upload_session_id": (
            str(lease.output_upload_session_id) if lease.output_upload_session_id else None
        ),
        "output_generation": lease.output_generation,
        "output_manifest_sha256": lease.output_manifest_sha256,
        "output_marker_sha256": lease.output_marker_sha256,
        "output_committed_at": (
            lease.output_committed_at.isoformat() if lease.output_committed_at else None
        ),
        "output_unavailable_reason": lease.output_unavailable_reason,
        "materialization_state": lease.materialization_state,
        "materialization_attempts": lease.materialization_attempts,
        "materialization_next_attempt_at": (
            lease.materialization_next_attempt_at.isoformat()
            if lease.materialization_next_attempt_at
            else None
        ),
        "materialization_started_at": (
            lease.materialization_started_at.isoformat()
            if lease.materialization_started_at
            else None
        ),
        "materialization_committed_at": (
            lease.materialization_committed_at.isoformat()
            if lease.materialization_committed_at
            else None
        ),
        "materialization_error": (
            {
                "code": lease.materialization_error_code,
                "message": lease.materialization_error_message,
            }
            if lease.materialization_error_code is not None
            else None
        ),
        "canonical_trajectory_sha256": lease.canonical_trajectory_sha256,
        "canonical_atif_sha256": lease.canonical_atif_sha256,
        "source_cleanup_state": lease.source_cleanup_state,
        "source_retain_until": (
            lease.source_retain_until.isoformat() if lease.source_retain_until else None
        ),
        "source_cleanup_attempts": lease.source_cleanup_attempts,
        "source_cleanup_error_message": lease.source_cleanup_error_message,
        "deleted_at": lease.deleted_at.isoformat() if lease.deleted_at else None,
        "error": (
            {
                "class": lease.error_class,
                "code": lease.error_code,
                "message": lease.error_message,
            }
            if lease.error_class is not None
            else None
        ),
        "created_at": lease.created_at.isoformat(),
        "updated_at": lease.updated_at.isoformat(),
    }


__all__ = [
    "ClaimedExecutionCommand",
    "ExecutionFence",
    "ServiceExecutionConflict",
    "ServiceExecutionFenceError",
    "acknowledge_execution_command",
    "claim_execution_commands",
    "defer_execution_command",
    "enqueue_execution_transition",
    "execution_lease_projection",
    "finalize_committed_service_execution",
    "mark_execution_output_unavailable",
    "persist_execution_catalog",
    "record_committed_runtime_result",
    "record_execution_event",
    "record_kubernetes_observation",
    "recover_deleted_committed_trial",
    "recover_execution_node_attribution",
    "refresh_service_execution_metrics",
    "request_trial_execution_cancellation",
    "reserve_trial_execution",
    "set_execution_target_health",
    "verify_trial_execution_fence",
]
