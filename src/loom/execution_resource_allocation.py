"""Resolve one node share into the concurrently resident execution containers."""

from __future__ import annotations

from loom.execution_runtime_contract import (
    ContainerResourcesV1,
    ExecutionRuntimePlanV1,
    runtime_pod_resources,
)


def allocate_node_resources(
    plan: ExecutionRuntimePlanV1, *, target_id: str, usable_node: ContainerResourcesV1,
) -> ExecutionRuntimePlanV1:
    """Task declarations are minima; memory reservations equal enforced limits.

    A deferred verifier is not resident in the agent pod. Divide the remaining
    share by two anyway, so the task keeps today's per-sandbox share. The
    verifier pod is a later admission and does not enlarge this one. Large
    declarations still win over the share.
    """
    if plan.node_resource_allocation is not None:
        if plan.node_resource_allocation.target_id != target_id:
            raise ValueError("frozen node allocation belongs to another target")
        return plan
    slots = max(1, usable_node.cpu_millis // 1000)
    fields = tuple(ContainerResourcesV1.model_fields)
    private = [sidecar for sidecar in plan.sidecars if sidecar.private_sandbox]
    overhead = [sidecar.resources for sidecar in plan.sidecars if not sidecar.private_sandbox]
    if private:
        overhead.append(plan.execution_resources)
    # A deferred verifier is not in this pod, and its share is not added to the task.
    deferred = (
        plan.execution_role == "attempt" and plan.verifier_execution == "separate_execution"
    ) or plan.execution_role == "verifier"
    roles = 2 if deferred else (len(private) or 1)
    values = {}
    for field in fields:
        remaining = getattr(usable_node, field) // slots - sum(getattr(r, field) for r in overhead)
        minimum = getattr(plan.task_resources, field)
        explicit = max((getattr(plan.container_request(s.role_name), field) for s in private), default=0)
        values[field] = max(minimum, explicit, remaining // roles)
    task = ContainerResourcesV1(**values)
    payload = plan.model_dump(mode="json")
    payload["node_resource_allocation"] = {
        "policy": "node-share-v1", "target_id": target_id,
        "usable_node": usable_node.model_dump(), "baseline_slots": slots,
        "declared_task": plan.task_resources.model_dump(),
    }
    payload["task_resources"] = task.model_dump()
    payload["workspace_mib"] = max(plan.workspace_mib, task.ephemeral_storage_mib)
    for sidecar in payload["sidecars"]:
        if sidecar["private_sandbox"]:
            sidecar["resources"] = task.model_dump()
    if private:
        # Keep the independently configured controller envelope, including its
        # CPU limit. Scheduling CPU requests need not equal throttling limits.
        controller = plan.execution_resources.model_dump()
        controller["cpu_millis"] = plan.container_request("execution").cpu_millis
        payload["controller_resources"] = plan.execution_resources.model_dump()
        payload["resource_requests"] = {
            "controller": controller,
            **(
                {"verifier_sandbox": task.model_dump()}
                if plan.execution_role == "verifier"
                else {"task_sandbox": task.model_dump()}
            ),
            **(
                {"verifier_sandbox": task.model_dump()}
                if any(sidecar.role_name == "verifier-sandbox" for sidecar in private)
                and plan.execution_role != "verifier"
                else {}
            ),
        }
    resolved = ExecutionRuntimePlanV1.model_validate(payload)
    total = runtime_pod_resources(resolved)
    if any(getattr(total, field) > getattr(usable_node, field) for field in fields):
        raise ValueError("execution_capacity_workload_exceeds_node_allocatable")
    return resolved


def resource_allocation_summary(plan: ExecutionRuntimePlanV1) -> dict[str, object] | None:
    allocation = plan.node_resource_allocation
    if allocation is None:
        return None
    roles = [("execution", plan.execution_resources), *(
        (sidecar.role_name, sidecar.resources) for sidecar in plan.sidecars
    )]
    return {
        "policy": allocation.policy, "baseline_slots": allocation.baseline_slots,
        "declared_task": allocation.declared_task.model_dump(),
        "pod_requests": runtime_pod_resources(plan).model_dump(),
        "containers": [{"role": role, "requests": plan.container_request(role).model_dump(),
                        "limits": limits.model_dump()} for role, limits in roles],
    }
