"""Freeze a target's observed, compatible allocatable capacity before admission."""

from datetime import datetime, timedelta

from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import ExecutionCapacityPolicy
from loom.execution_resource_allocation import allocate_node_resources
from loom.execution_runtime_contract import ContainerResourcesV1, ExecutionRuntimePlanV1
from loom_control_plane.execution_capacity import (
    ExecutionProvisioningBlockedError,
    _latest_observation,
    native_allocatable_sample,
)
from loom_control_plane.execution_capacity_targets import resolve_capacity_targets
from loom_execution_capacity_collector.contracts import CapacityPlacement


async def allocate_target_resources(
    session: AsyncSession, plan: ExecutionRuntimePlanV1, *, target_id: str, now: datetime,
) -> ExecutionRuntimePlanV1:
    group = await resolve_capacity_targets(session, target_id)
    if group.owner.desired_state != "active":
        raise ExecutionProvisioningBlockedError("execution_capacity_owner_not_active")
    policy = await session.get(ExecutionCapacityPolicy, group.owner.id)
    if policy is None or not policy.enabled:
        raise ExecutionProvisioningBlockedError("execution_capacity_policy_unavailable")
    observation = await _latest_observation(session, group.owner.id)
    if observation is None:
        raise ExecutionProvisioningBlockedError("execution_capacity_observation_unavailable")
    if not group.matches_observation_scope(observation.observation_json):
        raise ExecutionProvisioningBlockedError("execution_capacity_target_scope_unavailable")
    if observation.observed_at > now + timedelta(seconds=60):
        raise ExecutionProvisioningBlockedError("execution_capacity_observation_from_future")
    if now > observation.observed_at + timedelta(seconds=policy.observation_max_age_seconds):
        raise ExecutionProvisioningBlockedError("execution_capacity_observation_stale")
    raw = observation.observation_json.get("placement")
    if not raw:
        raise ExecutionProvisioningBlockedError("execution_capacity_placement_unavailable")
    sample = await native_allocatable_sample(session, group.owner.id, CapacityPlacement.model_validate(raw))
    if sample is None:
        raise ExecutionProvisioningBlockedError("execution_capacity_node_allocatable_unknown")
    # Allocatable already excludes kube/system reservations; subtract only the
    # resident DaemonSets once. Never use currently free memory as a default.
    usable = {
        key: min(getattr(sample.allocatable, source), getattr(policy, ceiling))
        - getattr(sample.daemonset_requests, source)
        for key, source, ceiling in (
            ("cpu_millis", "cpu_millis", "node_cpu_millis"),
            ("memory_mib", "memory_mib", "node_memory_mib"),
            ("ephemeral_storage_mib", "storage_mib", "node_storage_mib"),
        )
    }
    if min(usable.values()) <= 0:
        raise ExecutionProvisioningBlockedError("execution_capacity_workload_exceeds_node_allocatable")
    try:
        return allocate_node_resources(plan, target_id=target_id, usable_node=ContainerResourcesV1(**usable))
    except ValueError as exc:
        if str(exc) == "execution_capacity_workload_exceeds_node_allocatable":
            raise ExecutionProvisioningBlockedError(str(exc)) from exc
        raise
