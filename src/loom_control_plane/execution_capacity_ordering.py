"""Read-only physical pressure hints; the reservation transaction owns admission.

No advisory/row lock, reservation, counter or external call is acquired here.
An unknown hint sorts last and still passes through normal fail-closed admission.
"""

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from fractions import Fraction

from sqlalchemy import DateTime, case, cast, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import (
    ExecutionCapacityPolicy,
    ExecutionProvisioningAuthorization,
    ServiceExecutionLease,
    ServiceExecutionTarget,
    TaskImageMaterializationAttempt,
)
from loom_control_plane.execution_capacity import (
    _ACTIVE_AUTHORIZATION_STATES,
    ExecutionProvisioningBlockedError,
    _latest_observation,
    _native_capacity_reservations,
    native_allocatable_sample,
    native_placement_constraint,
)
from loom_control_plane.execution_capacity_targets import resolve_capacity_targets
from loom_control_plane.execution_placement import (
    PlacementConstraint,
    PlacementUnavailableError,
    plan_placement,
    require_provider_quota_headroom,
)
from loom_execution_capacity_collector.contracts import (
    CapacityPlacement,
    NodeTemplateSample,
    ResourceTotals,
)


@dataclass(frozen=True)
class CapacityOrderingHint:
    pressure: Fraction
    last_dispatch: datetime | None = None
    cold_nodes: int = 0


def physical_pressure(
    placement: CapacityPlacement, sample: NodeTemplateSample,
    demands: Iterable[tuple[str, ResourceTotals]],
    *, constraints: dict[str, PlacementConstraint] | None = None,
    max_nodes: int | None = None, max_resources: ResourceTotals | None = None,
) -> tuple[Fraction, int]:
    """Current pressure, not after-incoming pressure (which biases large nodes).

    Observed Pods absorb their durable requests exactly once. Foreign occupancy
    stays charged; resident DaemonSets are removed from both load and capacity.
    A measured cold template gives an idle min-zero owner a comparison budget,
    never permission to create a node. Final placement retains its I/O bindings.
    """
    demands = list(demands)
    durable_by_identity = dict(demands)
    prior = plan_placement(placement, demands, sample=sample, constraints=constraints)
    ready_nodes = [node for node in placement.nodes if node.ready and not node.unschedulable and not node.deleting]
    # An idle min-zero comparison needs one realizable node, not unlimited
    # hypothetical capacity. Reuse the existing raw-node quota arithmetic.
    idle_node = int(not ready_nodes and prior.cold_nodes == 0 and placement.node_group.node_count == 0)
    projected_nodes = placement.node_group.node_count + prior.additional_nodes + idle_node
    if projected_nodes > min(placement.node_group.max_nodes,
            placement.node_group.max_nodes if max_nodes is None else max_nodes):
        raise PlacementUnavailableError("execution_capacity_max_nodes_exceeded")
    if max_resources is not None and any(projected_nodes * getattr(placement.node_group.raw_node, key) > getattr(max_resources, key)
            for key in ("cpu_millis", "memory_mib", "storage_mib")):
        raise PlacementUnavailableError("execution_capacity_node_budget_exceeded")
    require_provider_quota_headroom(placement, additional_nodes=prior.additional_nodes + idle_node, peers=())
    dimensions = ("cpu_millis", "memory_mib", "storage_mib")
    used = [0, 0, 0, 0]
    capacity = [0, 0, 0, 0]
    samples = {row.node_uid: row for row in placement.template_samples}
    observed: set[str] = set()
    for node in placement.nodes:
        resident = samples.get(node.uid, sample)
        for index, key in enumerate(dimensions):
            overhead = getattr(resident.daemonset_requests, key)
            used[index] += max(0, getattr(node.requested, key) - overhead)
            if node.ready and not node.unschedulable and not node.deleting:
                capacity[index] += max(0, getattr(node.allocatable, key) - overhead)
        used[3] += max(0, node.used_pod_slots - resident.daemonset_slots)
        if node.ready and not node.unschedulable and not node.deleting:
            capacity[3] += max(0, node.pod_slots - resident.daemonset_slots)
        observed.update(f"{pod.lease_id}:{pod.generation}" for pod in node.managed_pods)
    for pod in placement.pending_pods:
        identity = f"{pod.lease_id}:{pod.generation}"
        observed.add(identity)
        durable = durable_by_identity.get(identity, pod.requests)
        for index, key in enumerate(dimensions):
            used[index] += max(getattr(pod.requests, key), getattr(durable, key))
        used[3] += 1
    for identity, resources in demands:
        if identity not in observed:
            for index, key in enumerate(dimensions):
                used[index] += getattr(resources, key)
            used[3] += 1
    future = prior.cold_nodes or int(not any(capacity))
    for index, key in enumerate(dimensions):
        capacity[index] += future * max(0, getattr(sample.allocatable, key) - getattr(sample.daemonset_requests, key))
    capacity[3] += future * max(0, sample.pod_slots - sample.daemonset_slots)
    if min(capacity) <= 0:
        raise PlacementUnavailableError("execution_capacity_node_allocatable_unknown")
    return max(Fraction(busy, budget) for busy, budget in zip(used, capacity, strict=True)), prior.cold_nodes


def capacity_order_key(target_id: str, hint: CapacityOrderingHint | None) -> tuple[bool, Fraction, datetime, int, str]:
    return (hint is None, hint.pressure if hint else Fraction(0),
            hint.last_dispatch if hint and hint.last_dispatch else datetime.min.replace(tzinfo=UTC),
            hint.cold_nodes if hint else 0, target_id)


async def order_targets_by_capacity(
    session: AsyncSession, targets: list[ServiceExecutionTarget], *, now: datetime,
) -> list[ServiceExecutionTarget]:
    if len(targets) < 2:
        return targets
    groups = {target.id: await resolve_capacity_targets(session, target.id) for target in targets}
    owner_ids = {group.owner.id for group in groups.values()}
    member_ids = {member for group in groups.values() for member in group.target_ids}
    authorizations = list((await session.scalars(select(ExecutionProvisioningAuthorization).where(
        ExecutionProvisioningAuthorization.target_id.in_(member_ids),
        ExecutionProvisioningAuthorization.state.in_(_ACTIVE_AUTHORIZATION_STATES),
    ))).all())
    generations = {lease_id: generation for lease_id, generation in (await session.execute(
        select(ServiceExecutionLease.id, ServiceExecutionLease.resource_generation)
        .where(ServiceExecutionLease.id.in_([row.lease_id for row in authorizations])))).all()}
    dispatches = {target_id: last for target_id, last in (await session.execute(select(ExecutionProvisioningAuthorization.target_id,
        func.max(ExecutionProvisioningAuthorization.authorized_at)).where(
            ExecutionProvisioningAuthorization.target_id.in_(member_ids)
        ).group_by(ExecutionProvisioningAuthorization.target_id))).all()}
    # Include released historical builds for idle-region rotation. The normal
    # active-reservation reader intentionally trims that history for rate limits.
    native_target = TaskImageMaterializationAttempt.native_capacity_target_id
    reserved = TaskImageMaterializationAttempt.native_capacity_reserved_at
    valid_time = case((func.pg_input_is_valid(reserved, "timestamptz"), cast(reserved, DateTime(timezone=True))))
    for target_id, last in (await session.execute(select(native_target, func.max(valid_time))
            .where(native_target.in_(member_ids)).group_by(native_target))).all():
        if last is not None and (dispatches.get(target_id) is None or last > dispatches[target_id]):
            dispatches[target_id] = last
    native_active = [row for row in await _native_capacity_reservations(session, current_time=now) if not row.released]
    hints: dict[str, CapacityOrderingHint | None] = {}
    for owner_id in sorted(owner_ids):
        group = next(group for group in groups.values() if group.owner.id == owner_id)
        hints[owner_id] = None
        policy = await session.get(ExecutionCapacityPolicy, owner_id)
        observation = await _latest_observation(session, owner_id)
        if (group.owner.desired_state != "active" or policy is None or not policy.enabled or observation is None
                or not group.matches_observation_scope(observation.observation_json)
                or observation.observed_at > now + timedelta(seconds=60)
                or observation.observed_at + timedelta(seconds=policy.observation_max_age_seconds) < now):
            continue
        try:
            placement = CapacityPlacement.model_validate(observation.observation_json.get("placement"))
            sample = await native_allocatable_sample(session, owner_id, placement)
            if sample is None:
                continue
            demands = [(f"{row.lease_id}:{generations[row.lease_id]}", ResourceTotals(
                cpu_millis=row.requested_cpu_millis, memory_mib=row.requested_memory_mib,
                storage_mib=row.requested_storage_mib,
            )) for row in authorizations if row.target_id in group.target_ids]
            demands.extend((row.demand_id, row.resources) for row in native_active if row.target_id in group.target_ids)
            constraints = {row.demand_id: native_placement_constraint(row.native["io_binding"], placement)
                for row in native_active if row.target_id in group.target_ids and row.native.get("io_binding") is not None}
            pressure, cold = physical_pressure(placement, sample, demands, constraints=constraints,
                max_nodes=policy.max_nodes, max_resources=ResourceTotals(cpu_millis=policy.max_vcpu_millis,
                    memory_mib=policy.max_memory_mib, storage_mib=policy.max_storage_mib))
            if (cold or not any(node.ready and not node.unschedulable and not node.deleting for node in placement.nodes)) and (
                    observation.provider_capacity_state != "available" or observation.autoscaler_state in {"stalled", "unknown"}):
                continue
        except (ValueError, KeyError, ExecutionProvisioningBlockedError):
            # This is an ordering hint, never fallback capacity authority.
            continue
        last = max((dispatches[key] for key in group.target_ids if dispatches.get(key) is not None), default=None)
        hints[owner_id] = CapacityOrderingHint(pressure, last, cold)
    return sorted(targets, key=lambda target: capacity_order_key(target.id, hints[groups[target.id].owner.id]))
