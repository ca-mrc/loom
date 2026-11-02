"""Actual incoming envelopes, without reserving or moving a queued source."""
from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING
from uuid import UUID, uuid4

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import ServiceExecutionLease, ServiceExecutionTarget
from loom.execution_contract import WorkloadRequirementsV1
from loom.execution_runtime_contract import ExecutionRuntimePlanV1
from loom.pipeline.keys import canonical_digest
from loom_control_plane.execution_capacity import (
    ExecutionProvisioningBlockedError,
    admit_capacity_resources,
)
from loom_control_plane.execution_resource_allocation import allocate_target_resources
from loom_execution_actuator.renderer import ExecutionTargetRuntime, render_execution_job
from loom_execution_capacity_collector.contracts import ResourceTotals
from loom_execution_capacity_collector.kubernetes import rendered_pod_resources

if TYPE_CHECKING:
    from loom_control_plane.service_execution_scheduler import CompiledServiceExecution


async def runtime_projection_available(session: AsyncSession) -> bool:
    """Schema-first compatibility for this optional read projection only."""
    return bool(await session.scalar(text("SELECT EXISTS (SELECT 1 FROM pg_attribute "
        "WHERE attrelid=to_regclass('execution_targets') AND attname='preview_runtime_json' AND NOT attisdropped)")))


def incoming_pod_resources(plan: ExecutionRuntimePlanV1, requirements: WorkloadRequirementsV1, *,
                           target: ServiceExecutionTarget, deadline: datetime, now: datetime) -> tuple[ResourceTotals, dict[str, str], str]:
    evidence = target.preview_runtime_json
    if (not isinstance(evidence, dict) or set(evidence) != {
            "namespace_name", "runtime_class_name", "node_selector", "observed_at"}
            or evidence["namespace_name"] != target.spec_json["namespace_name"]
            or evidence["runtime_class_name"] is not None
            or target.health_observed_at is None
            or not isinstance(evidence["observed_at"], str)
            or datetime.fromisoformat(evidence["observed_at"]) != target.health_observed_at):
        raise ValueError("execution_preview_runtime_unknown")
    selector = evidence["node_selector"]
    if not isinstance(selector, dict) or any(not isinstance(k, str) or not isinstance(v, str) for k, v in selector.items()):
        raise ValueError("execution_preview_selector_unknown")
    runtime = plan.canonical_payload()
    request = requirements.model_dump(mode="json")
    runtime_sha = canonical_digest(runtime)
    lease = ServiceExecutionLease(id=uuid4(), generation=1, resource_generation=1, execution_unit_key=uuid4(),
        execution_role=plan.execution_role, execution_class_id=plan.execution_class_id, target_id=target.id,
        namespace_name=evidence["namespace_name"], job_name="loom-incoming-preview", deadline_at=deadline,
        workload_requirements_json=request, workload_requirements_sha256=canonical_digest(request),
        runtime_contract_json=runtime, runtime_contract_sha256=runtime_sha)
    job = render_execution_job(lease, target=ExecutionTargetRuntime(target_id=target.id,
        namespace=evidence["namespace_name"], node_selector=selector), now=now)
    return rendered_pod_resources(job["spec"]["template"]["spec"]), selector, runtime_sha


async def order_incoming_targets(
    session: AsyncSession, *, targets: list[ServiceExecutionTarget], compiled: CompiledServiceExecution,
    trial_id: UUID, now: datetime,
) -> tuple[list[ServiceExecutionTarget], dict[str, ExecutionRuntimePlanV1], dict[str, dict[str, object]]]:
    if not await runtime_projection_available(session):
        return targets, {}, {}
    plans: dict[str, ExecutionRuntimePlanV1] = {}
    warm: set[str] = set()
    previews: dict[str, dict[str, object]] = {}
    for target in targets:
        if compiled.requirements.data_residency and target.data_residency != compiled.requirements.data_residency:
            continue
        try:
            await session.refresh(target, attribute_names=["preview_runtime_json"])
            plan = (await allocate_target_resources(session, compiled.runtime_plan, target_id=target.id, now=now)
                    if compiled.allocate_resources else compiled.runtime_plan)
            plans[target.id] = plan
            envelope, selector, runtime_sha = incoming_pod_resources(plan, compiled.requirements, target=target,
                                                        deadline=compiled.deadline_at, now=now)
            decision = await admit_capacity_resources(session, target=target,
                demand_id=f"incoming:{trial_id}", resources=envelope,
                current_time=now, preview=True, preview_trial_id=trial_id, preview_node_selector=selector,
                preview_runtime_sha256=runtime_sha)
            previews[target.id] = {key: decision[key] for key in ("target_id", "observation_id", "policy_version",
                "requested_cpu_millis", "requested_memory_mib", "requested_storage_mib", "warm_node_uids",
                "incremental_nodes", "decision_reason")}
            previews[target.id].update(previewed_at=now.isoformat(), source="local-admission-preview")
            if decision["prefer_incoming_warm_fit"]:
                warm.add(target.id)
        except (ExecutionProvisioningBlockedError, ValueError, KeyError, TypeError):
            # Unknown evidence changes no authority. Keep the original cold-
            # capable order for everything without a complete warm proof.
            continue
    return sorted(targets, key=lambda target: target.id not in warm), plans, previews
