"""Trusted execution prepare adapter; no Job write or admission takes place here.

Prepare validates an actual rendered Job and its envelope. Activation must render
once with the remaining absolute deadline and persist that document before any
write; reconciliation must never refresh its runtime allowance.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

from loom.db.schema import ServiceExecutionLease
from loom.execution_contract import ExecutionClassV1, evaluate_execution_admission
from loom.execution_image_admission import ImageAdmissionKeyring, verify_execution_image_admission
from loom.nebius_pool_contract import PoolParticipantV1
from loom.nebius_pool_priority import pool_request_priority
from loom.nebius_pool_workload import PoolExecutionPrepareV1
from loom.pipeline.keys import canonical_digest
from loom_execution_actuator.contracts import ActuatorContractError
from loom_execution_actuator.renderer import ExecutionTargetRuntime, render_execution_job
from loom_execution_capacity_collector.contracts import ResourceTotals
from loom_execution_capacity_collector.kubernetes import rendered_pod_resources


@dataclass(frozen=True)
class PoolExecutionProfile:
    """Protected registration input, not fields accepted from a participant."""

    profile_id: UUID
    runtime: ExecutionTargetRuntime
    candidate_sha: str
    execution_class_id: str
    runtime_image_ref: str
    runtime_binary_sha256: str
    execution_class: ExecutionClassV1
    image_admission_keyring: ImageAdmissionKeyring
    # None is unqualified for a named RuntimeClass, not an assumed zero.
    runtime_class_overhead: ResourceTotals | None = None


@dataclass(frozen=True)
class PreparedPoolExecution:
    request_sha256: str
    resources: ResourceTotals
    pod_slots: int
    namespace_uid: UUID
    job: dict[str, Any]


def prepare_pool_execution(request: PoolExecutionPrepareV1, *, participant: PoolParticipantV1,
                           profile: PoolExecutionProfile, reservation_id: UUID,
                           now: datetime) -> PreparedPoolExecution:
    """Measure one fixed execution Job after protected binding qualification.

    Caller still owns current credential/epoch locks and registered origin lookup.
    This pure adapter does not establish that database authority or grant capacity.
    """
    # Revalidate nested mutable runtime data, even if passed as model instances.
    request = PoolExecutionPrepareV1.model_validate(request.model_dump())
    participant = PoolParticipantV1.model_validate(participant.model_dump())
    if (not reservation_id.int or now.utcoffset() is None or request.deadline_at <= now
            or (request.pool_id, request.key.participant_id, request.admission_epoch, request.participant_revision) != (
                participant.pool_id, participant.participant_id, participant.admission_epoch, participant.binding_revision)):
        raise ValueError("pool_execution_binding_mismatch")
    target = participant.target(request.target_id, request.key.workload_kind)
    runtime = request.execution.runtime
    if ((profile.profile_id, profile.runtime.target_id, profile.runtime.namespace) != (
            target.profile_id, request.target_id, participant.execution_namespace.name)
            or (runtime.candidate_sha, runtime.execution_class_id, runtime.runtime_image_ref, runtime.runtime_binary_sha256) != (
                profile.candidate_sha, profile.execution_class_id, profile.runtime_image_ref, profile.runtime_binary_sha256)
            or profile.execution_class.class_id != profile.execution_class_id
            or (profile.runtime.runtime_class_name is None) != (profile.runtime_class_overhead is None)):
        raise ValueError("pool_execution_profile_mismatch")
    pool_request_priority(participant, request.origin, workload_kind=request.key.workload_kind)
    if not evaluate_execution_admission(request.execution.requirements, profile.execution_class).compatible:
        raise ValueError("pool_execution_requirements_incompatible")
    verify_execution_image_admission(runtime.image_admission, required_image_refs=runtime.published_image_refs(),
                                    keyring=profile.image_admission_keyring, now=now)
    requirements_json = request.execution.requirements.model_dump(mode="json")
    runtime_json = runtime.canonical_payload()
    # Transient adapter only: no environment lease is written to management SQL.
    lease = ServiceExecutionLease(
        id=request.key.local_work_id, generation=request.execution.lease_generation,
        resource_generation=request.key.generation, execution_unit_key=request.execution.execution_unit_key,
        parent_lease_id=request.execution.parent_lease_id, execution_role=runtime.execution_role,
        execution_class_id=profile.execution_class_id, target_id=request.target_id,
        namespace_name=participant.execution_namespace.name, job_name=f"loom-pool-{reservation_id.hex}",
        deadline_at=request.deadline_at, workload_requirements_json=requirements_json,
        workload_requirements_sha256=canonical_digest(requirements_json),
        runtime_contract_json=runtime_json, runtime_contract_sha256=canonical_digest(runtime_json),
    )
    try:
        job = render_execution_job(lease, target=profile.runtime, now=now)
    except ActuatorContractError:
        raise ValueError("pool_execution_render_invalid") from None
    if job["spec"]["parallelism"] != 1 or job["spec"]["completions"] != 1:
        raise ValueError("pool_execution_requires_single_pod")
    pod = job["spec"]["template"]["spec"]
    # A frozen relative Job timeout must not become fresh consent when its
    # CREATE or scheduling was delayed. The trusted PID1 runtime bounds input,
    # proxy and phase contexts by this original deadline; output drain is separate.
    pod["containers"][0]["args"].extend(["--deadline-at", request.deadline_at.isoformat()])
    accounting_pod = dict(pod)
    if profile.runtime_class_overhead is not None:
        overhead = profile.runtime_class_overhead
        accounting_pod["overhead"] = {"cpu": f"{overhead.cpu_millis}m", "memory": f"{overhead.memory_mib}Mi",
                                      "ephemeral-storage": f"{overhead.storage_mib}Mi"}
    return PreparedPoolExecution(
        request_sha256=canonical_digest(request.model_dump(mode="json")).removeprefix("sha256:"),
        resources=rendered_pod_resources(accounting_pod), pod_slots=1,
        namespace_uid=participant.execution_namespace.uid, job=job,
    )
