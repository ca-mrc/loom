"""Short-lived waiting heads within the existing native capacity transaction."""

from __future__ import annotations

from datetime import datetime, timedelta
from uuid import UUID

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import (
    ExecutionCapacityPolicy,
    ServiceExecutionTarget,
    TaskImageCapacityWait,
    TaskImageMaterialization,
    TaskImageMaterializationAttempt,
)
from loom_control_plane.execution_capacity_targets import resolve_capacity_targets
from loom_control_plane.task_image_materializations import has_nebius_task_image_demand
from loom_execution_capacity_collector.contracts import CapacityPlacement, ResourceTotals

WAIT_LEASE_SECONDS = 120


def wait_resources(wait: TaskImageCapacityWait) -> ResourceTotals:
    return ResourceTotals(cpu_millis=wait.cpu_millis, memory_mib=wait.memory_mib,
                          storage_mib=wait.storage_mib)


async def _realizable(
    session: AsyncSession, *, target_id: str, pool_id: str, resources: ResourceTotals, now: datetime,
) -> bool:
    from loom_control_plane.execution_capacity import _latest_observation, native_allocatable_sample

    target = await session.get(ServiceExecutionTarget, target_id)
    if (target is None or target.provider != "nebius" or target.desired_state != "active"
            or target.logical_pool_id != pool_id
            or target.health_status != "healthy"):
        return False
    group = await resolve_capacity_targets(session, target_id)
    policy = await session.get(ExecutionCapacityPolicy, group.owner.id)
    if group.owner.desired_state != "active" or policy is None or not policy.enabled:
        return False
    if any(value > limit for value, limit in (
        (resources.cpu_millis, policy.node_cpu_millis),
        (resources.memory_mib, policy.node_memory_mib),
        (resources.storage_mib, policy.node_storage_mib),
    )):
        return False
    observed = await _latest_observation(session, group.owner.id)
    if (observed is None or observed.observed_at > now + timedelta(seconds=60)
            or observed.observed_at + timedelta(seconds=policy.observation_max_age_seconds) < now
            or not group.matches_observation_scope(observed.observation_json)
            or not observed.observation_json.get("placement")):
        return False
    placement = CapacityPlacement.model_validate(observed.observation_json["placement"])
    raw = placement.node_group.raw_node
    if min(policy.max_nodes, placement.node_group.max_nodes) < 1 or any(value > limit for value, limit in (
        (raw.cpu_millis, policy.max_vcpu_millis),
        (raw.memory_mib, policy.max_memory_mib),
        (raw.storage_mib, policy.max_storage_mib),
    )):
        return False
    # Occupied quota can drain, but a node larger than the entire allowance is
    # not a realizable pending request. Do not fence another pool on its behalf.
    if any(key in placement.quota_resources and value > placement.quota_resources[key].limit
           for key, value in (("nodes", 1), ("vcpu", raw.cpu_millis),
                              ("memory", raw.memory_mib), ("storage", raw.storage_mib))):
        return False
    sample = await native_allocatable_sample(session, group.owner.id, placement)
    # Existing Ready nodes need no cold-template proof. Only observed managed
    # work is considered drainable; retain foreign/DaemonSet resource and slot
    # occupancy. This is waiting eligibility, never permission to release the
    # actual reservation or extrapolate the node into a cold-node sample.
    dimensions = ("cpu_millis", "memory_mib", "storage_mib")
    for node in placement.nodes:
        if not node.ready or node.unschedulable or node.deleting:
            continue
        count = len(node.managed_pods)
        if (count > node.used_pod_slots
                or len({pod.uid for pod in node.managed_pods}) != count
                or len({(pod.lease_id, pod.generation) for pod in node.managed_pods}) != count):
            continue
        resident = {key: getattr(node.requested, key)
                    - sum(getattr(pod.requests, key) for pod in node.managed_pods)
                    for key in dimensions}
        if (min(resident.values()) >= 0
                and node.pod_slots > node.used_pod_slots - count
                and all(getattr(resources, key) <= getattr(node.allocatable, key) - resident[key]
                        for key in dimensions)):
            return True
    # A policy's raw node shape is not proof of workload allocatable capacity.
    if sample is None or sample.pod_slots <= sample.daemonset_slots:
        return False
    return all(getattr(resources, key) <= getattr(sample.allocatable, key)
               - getattr(sample.daemonset_requests, key)
               for key in ("cpu_millis", "memory_mib", "storage_mib"))


async def _eligible(session: AsyncSession, wait: TaskImageCapacityWait, now: datetime) -> bool:
    if wait.expires_at <= now or wait.renewed_at > now + timedelta(seconds=60):
        return False
    row = await session.get(TaskImageMaterialization, wait.materialization_id,
                            populate_existing=True)
    if (row is None or row.lease_epoch != wait.lease_epoch
            or row.attempt_count >= row.max_attempts
            or row.state not in {"queued", "claimed", "running"}
            or (row.state != "queued" and (row.lease_expires_at is None or row.lease_expires_at > now))
            or (row.next_attempt_at is not None and row.next_attempt_at > now)):
        return False
    if not await has_nebius_task_image_demand(
        session, materialization_id=row.id, pool_id=wait.pool_id,
    ):
        return False
    return await _realizable(session, target_id=wait.target_id,
                             pool_id=wait.pool_id, resources=wait_resources(wait), now=now)


async def read_capacity_waits(
    session: AsyncSession, *, now: datetime,
    claiming_attempt: TaskImageMaterializationAttempt | None = None,
    claiming_target: ServiceExecutionTarget | None = None,
    claiming_resources: ResourceTotals | None = None,
) -> list[TaskImageCapacityWait]:
    """Caller holds capacity lock; older builders precede younger builders.

    Reading never renews a fence. Only the controller that just validated actual
    claim/render eligibility can renew it. A claim's own savepoint has advanced
    its epoch, so use its retained timestamp only for order, not as admission.
    """
    rows = list((await session.scalars(select(TaskImageCapacityWait)
                .where(TaskImageCapacityWait.expires_at > now)
                .order_by(TaskImageCapacityWait.first_waited_at, TaskImageCapacityWait.target_id))).all())
    claiming_materialization_id = claiming_attempt.materialization_id if claiming_attempt else None
    own = next((row for row in rows if row.materialization_id == claiming_materialization_id), None)
    # A new epoch or changed resource/target envelope is a new queue entry.
    # The savepoint has advanced exactly one epoch only for an unchanged wait.
    if own is not None and (
        claiming_attempt is None or claiming_target is None
        or own.lease_epoch + 1 != claiming_attempt.lease_epoch
        or own.target_id != claiming_target.id or own.pool_id != claiming_target.logical_pool_id
        or wait_resources(own) != claiming_resources
        or own.renewed_at > now + timedelta(seconds=60)
    ):
        own = None
    cutoff = (own.first_waited_at, own.target_id) if own else None
    result = []
    for row in rows:
        if row.materialization_id == claiming_materialization_id:
            continue
        if cutoff is not None and (row.first_waited_at, row.target_id) >= cutoff:
            continue
        if await _eligible(session, row, now):
            result.append(row)
    return result


async def remember_capacity_wait(
    session: AsyncSession, *, target_id: str, materialization_id: UUID, lease_epoch: int,
    pool_id: str, resources: ResourceTotals, now: datetime, reason: str | None = None,
) -> None:
    """Persist only after the rejected claim savepoint has rolled back."""
    from loom_control_plane.execution_capacity import _CAPACITY_ADMISSION_LOCK

    await session.execute(_CAPACITY_ADMISSION_LOCK)
    # Expired records cannot preserve queue order or stop another target from
    # claiming the same materialization. The row is disposable waiting state.
    await session.execute(delete(TaskImageCapacityWait).where(TaskImageCapacityWait.expires_at <= now))
    existing = await session.get(TaskImageCapacityWait, target_id, with_for_update=True)
    candidate = TaskImageCapacityWait(
        target_id=target_id, materialization_id=materialization_id, lease_epoch=lease_epoch,
        pool_id=pool_id, cpu_millis=resources.cpu_millis, memory_mib=resources.memory_mib,
        storage_mib=resources.storage_mib, first_waited_at=now, renewed_at=now, reason=reason,
        expires_at=now + timedelta(seconds=WAIT_LEASE_SECONDS),
    )
    if not await _eligible(session, candidate, now):
        return
    if existing is not None:
        if (existing.materialization_id == materialization_id and existing.lease_epoch == lease_epoch
                and existing.pool_id == pool_id and wait_resources(existing) == resources):
            existing.renewed_at, existing.expires_at = candidate.renewed_at, candidate.expires_at
            existing.reason = reason
            await session.flush()
            return
        if existing.materialization_id != materialization_id and await _eligible(session, existing, now):
            return  # One live waiting head per target; do not replace its order.
        await session.delete(existing)
        await session.flush()
    duplicate = await session.scalar(select(TaskImageCapacityWait)
                                      .where(TaskImageCapacityWait.materialization_id == materialization_id))
    if duplicate is not None:
        if await _eligible(session, duplicate, now):
            return
        await session.delete(duplicate)
        await session.flush()
    session.add(candidate)
    await session.flush()
