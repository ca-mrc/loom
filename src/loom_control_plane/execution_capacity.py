"""Durable Nebius quota observations and pre-create provisioning admission."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Literal
from uuid import UUID, uuid4

from sqlalchemy import func, or_, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import (
    ExecutionCapacityObservation,
    ExecutionCapacityPolicy,
    ExecutionCostReservation,
    ExecutionProvisioningAuthorization,
    ServiceExecutionCommand,
    ServiceExecutionLease,
    ServiceExecutionTarget,
    TaskImageMaterializationAttempt,
)
from loom.pipeline.keys import canonical_digest
from loom_control_plane.execution_capacity_targets import resolve_capacity_targets
from loom_control_plane.execution_placement import (
    PlacementUnavailableError,
    cold_sample,
    plan_placement,
    quota_identity,
    require_provider_quota_headroom,
)
from loom_execution_capacity_collector.contracts import (
    CapacityPlacement,
    NodeTemplateSample,
    ResourceTotals,
)

_CAPACITY_MUTATION_LOCK = text(
    "SELECT pg_advisory_xact_lock(hashtextextended('execution-capacity-mutation', 1552))"
)
_CAPACITY_ADMISSION_LOCK = _CAPACITY_MUTATION_LOCK
_ACTIVE_AUTHORIZATION_STATES = (
    "authorized",
    "pending",
    "unschedulable",
    "image_pull_backoff",
    "running",
)
_PENDING_AUTHORIZATION_STATES = (
    "authorized",
    "pending",
    "unschedulable",
    "image_pull_backoff",
)


@dataclass(frozen=True)
class ExecutionProvisioningBlockedError(Exception):
    reason: str
    retry_after_seconds: int = 15


def _utc(value: datetime, *, name: str) -> datetime:
    if value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(UTC)


def _clean_text(value: str, *, name: str, max_length: int) -> str:
    clean = str(value).strip()
    if not clean or len(clean) > max_length:
        raise ValueError(f"{name} must contain 1 to {max_length} characters")
    return clean


def _clean_optional_text(value: str | None, *, name: str) -> str | None:
    if value is None:
        return None
    clean = value.strip()
    if not clean or len(clean) > 500:
        raise ValueError(f"{name} must contain 1 to 500 characters")
    return clean


def _positive_int(value: int, *, name: str) -> int:
    if isinstance(value, bool) or value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _non_negative_int(value: int, *, name: str) -> int:
    if isinstance(value, bool) or value < 0:
        raise ValueError(f"{name} must be a non-negative integer")
    return value


def _normalized_pending_reasons(value: dict[str, int]) -> dict[str, int]:
    normalized: dict[str, int] = {}
    if len(value) > 100:
        raise ValueError("pending_reasons cannot contain more than 100 entries")
    for raw_key, raw_count in value.items():
        key = _clean_text(raw_key, name="pending reason", max_length=120)
        normalized[key] = _non_negative_int(raw_count, name=f"pending reason {key} count")
    return dict(sorted(normalized.items()))


def observed_node_activity(payload: dict[str, Any]) -> dict[str, int | None]:
    """Project aggregate activity without exposing node or lease identities.

    Occupied means at least one nonterminal managed execution/build Pod for
    this target; infrastructure DaemonSets and unassigned Pods do not count.
    Neither the provider target nor a reduced target proves an actual drain.
    """
    placement = payload.get("placement")
    nodes = placement.get("nodes") if isinstance(placement, dict) else None
    if not isinstance(nodes, list):
        return {"occupied_nodes": None, "draining_nodes": None}
    return {
        "occupied_nodes": sum(bool(node["managed_pods"]) for node in nodes),
        "draining_nodes": (
            sum(node["draining"] for node in nodes)
            if all(isinstance(node.get("draining"), bool) for node in nodes)
            else None
        ),
    }


def _normalized_node_states(
    value: dict[str, int] | None,
    *,
    active_nodes: int,
    autoscaler_state: str,
) -> dict[str, int]:
    keys = ("desired", "creating", "ready", "failed", "deleting")
    if value is None:
        ready = active_nodes if autoscaler_state == "ready" else 0
        return {
            "desired": active_nodes,
            "creating": active_nodes - ready,
            "ready": ready,
            "failed": 0,
            "deleting": 0,
        }
    if set(value) != set(keys):
        raise ValueError("node_states must contain desired, creating, ready, failed, deleting")
    return {key: _non_negative_int(value[key], name=f"node_states.{key}") for key in keys}


async def create_execution_capacity_observation(
    session: AsyncSession,
    *,
    target_id: str,
    source: str,
    source_version: str,
    observed_at: datetime,
    provider_capacity_state: Literal["available", "insufficient", "unknown"],
    provider_capacity_reason: str | None,
    autoscaler_state: Literal["ready", "scaling", "stalled", "unknown"],
    autoscaler_reason: str | None,
    provider_quota_nodes: int,
    provider_quota_vcpu_millis: int,
    provider_quota_memory_mib: int,
    provider_quota_storage_mib: int,
    provider_used_nodes: int,
    provider_used_vcpu_millis: int,
    provider_used_memory_mib: int,
    provider_used_storage_mib: int,
    active_nodes: int,
    provisioned_vcpu_millis: int,
    provisioned_memory_mib: int,
    provisioned_storage_mib: int,
    allocatable_cpu_millis: int,
    allocatable_memory_mib: int,
    allocatable_storage_mib: int,
    requested_cpu_millis: int,
    requested_memory_mib: int,
    requested_storage_mib: int,
    pending_jobs: int,
    unschedulable_jobs: int,
    image_pull_backoff_jobs: int,
    pending_reasons: dict[str, int],
    node_states: dict[str, int] | None = None,
    placement: dict[str, Any] | None = None,
) -> tuple[ExecutionCapacityObservation, bool]:
    target_id = _clean_text(target_id, name="target_id", max_length=120)
    source = _clean_text(source, name="source", max_length=120)
    source_version = _clean_text(source_version, name="source_version", max_length=160)
    observed = _utc(observed_at, name="observed_at")
    capacity_reason = _clean_optional_text(
        provider_capacity_reason, name="provider_capacity_reason"
    )
    scale_reason = _clean_optional_text(autoscaler_reason, name="autoscaler_reason")
    positive_values = {
        "provider_quota_nodes": provider_quota_nodes,
        "provider_quota_vcpu_millis": provider_quota_vcpu_millis,
        "provider_quota_memory_mib": provider_quota_memory_mib,
        "provider_quota_storage_mib": provider_quota_storage_mib,
    }
    non_negative_values = {
        "provider_used_nodes": provider_used_nodes,
        "provider_used_vcpu_millis": provider_used_vcpu_millis,
        "provider_used_memory_mib": provider_used_memory_mib,
        "provider_used_storage_mib": provider_used_storage_mib,
        "active_nodes": active_nodes,
        "provisioned_vcpu_millis": provisioned_vcpu_millis,
        "provisioned_memory_mib": provisioned_memory_mib,
        "provisioned_storage_mib": provisioned_storage_mib,
        "allocatable_cpu_millis": allocatable_cpu_millis,
        "allocatable_memory_mib": allocatable_memory_mib,
        "allocatable_storage_mib": allocatable_storage_mib,
        "requested_cpu_millis": requested_cpu_millis,
        "requested_memory_mib": requested_memory_mib,
        "requested_storage_mib": requested_storage_mib,
        "pending_jobs": pending_jobs,
        "unschedulable_jobs": unschedulable_jobs,
        "image_pull_backoff_jobs": image_pull_backoff_jobs,
    }
    for name, value in positive_values.items():
        _non_negative_int(value, name=name)
    for name, value in non_negative_values.items():
        _non_negative_int(value, name=name)
    if provider_capacity_state not in {"available", "insufficient", "unknown"}:
        raise ValueError("provider_capacity_state is invalid")
    if autoscaler_state not in {"ready", "scaling", "stalled", "unknown"}:
        raise ValueError("autoscaler_state is invalid")
    reasons = _normalized_pending_reasons(pending_reasons)
    normalized_node_states = _normalized_node_states(
        node_states,
        active_nodes=active_nodes,
        autoscaler_state=autoscaler_state,
    )
    await session.execute(_CAPACITY_MUTATION_LOCK)
    target = await session.get(ServiceExecutionTarget, target_id)
    if target is None or target.provider != "nebius":
        raise ValueError("capacity observation target must be a Nebius execution target")
    group = await resolve_capacity_targets(session, target_id)
    if group.owner.id != target_id:
        raise ValueError("capacity observations must be published by the capacity owner")
    payload: dict[str, Any] = {
        "schema_version": "loom.execution-capacity-observation.v1",
        "target_id": target_id,
        "provider": target.provider,
        "source": source,
        "source_version": source_version,
        "observed_at": observed.isoformat(),
        "provider_capacity_state": provider_capacity_state,
        "provider_capacity_reason": capacity_reason,
        "autoscaler_state": autoscaler_state,
        "autoscaler_reason": scale_reason,
        **positive_values,
        **non_negative_values,
        "pending_reasons": reasons,
        "node_states": normalized_node_states,
    }
    if placement is not None:
        parsed = CapacityPlacement.model_validate(placement)
        if not {"nodes", "vcpu", "storage"}.issubset(parsed.quota_resources):
            raise ValueError("placement requires native nodes, vcpu and storage quota identities")
        if len({node.uid for node in parsed.nodes}) != len(parsed.nodes) or len(
            {node.provider_id for node in parsed.nodes}
        ) != len(parsed.nodes):
            raise ValueError("placement contains duplicate node identities")
        payload["placement"] = parsed.model_dump(mode="json")
    if not group.matches_observation_scope(payload):
        raise ValueError("capacity observation target scope does not match authoritative membership")
    digest = canonical_digest(payload)
    existing = (
        await session.execute(
            select(ExecutionCapacityObservation).where(
                ExecutionCapacityObservation.target_id == target_id,
                ExecutionCapacityObservation.source == source,
                ExecutionCapacityObservation.source_version == source_version,
            )
        )
    ).scalar_one_or_none()
    if existing is not None:
        if existing.observation_sha256 != digest:
            raise ValueError("capacity observation source version already has different contents")
        return existing, False
    same_time = (
        await session.execute(
            select(ExecutionCapacityObservation.id).where(
                ExecutionCapacityObservation.target_id == target_id,
                ExecutionCapacityObservation.observed_at == observed,
            )
        )
    ).scalar_one_or_none()
    if same_time is not None:
        raise ValueError("capacity target already has evidence for observed_at")
    row = ExecutionCapacityObservation(
        id=uuid4(),
        target_id=target_id,
        provider=target.provider,
        source=source,
        source_version=source_version,
        observed_at=observed,
        provider_capacity_state=provider_capacity_state,
        provider_capacity_reason=capacity_reason,
        autoscaler_state=autoscaler_state,
        autoscaler_reason=scale_reason,
        pending_reasons_json=reasons,
        observation_json=payload,
        observation_sha256=digest,
        **positive_values,
        **non_negative_values,
    )
    session.add(row)
    await session.flush()
    return row, True


async def upsert_execution_capacity_policy(
    session: AsyncSession,
    *,
    target_id: str,
    enabled: bool,
    max_nodes: int,
    max_vcpu_millis: int,
    max_memory_mib: int,
    max_storage_mib: int,
    node_cpu_millis: int,
    node_memory_mib: int,
    node_storage_mib: int,
    max_pending_jobs: int,
    max_unschedulable_jobs: int,
    max_image_pull_backoff_jobs: int,
    max_create_per_minute: int,
    observation_max_age_seconds: int,
    reason: str | None,
    now: datetime | None = None,
) -> ExecutionCapacityPolicy:
    target_id = _clean_text(target_id, name="target_id", max_length=120)
    clean_reason = _clean_optional_text(reason, name="reason")
    positive_values = {
        "max_nodes": max_nodes,
        "max_vcpu_millis": max_vcpu_millis,
        "max_memory_mib": max_memory_mib,
        "max_storage_mib": max_storage_mib,
        "node_cpu_millis": node_cpu_millis,
        "node_memory_mib": node_memory_mib,
        "node_storage_mib": node_storage_mib,
        "max_pending_jobs": max_pending_jobs,
        "max_create_per_minute": max_create_per_minute,
    }
    for name, value in positive_values.items():
        _positive_int(value, name=name)
    _non_negative_int(max_unschedulable_jobs, name="max_unschedulable_jobs")
    _non_negative_int(max_image_pull_backoff_jobs, name="max_image_pull_backoff_jobs")
    if observation_max_age_seconds < 10 or observation_max_age_seconds > 900:
        raise ValueError("observation_max_age_seconds must be between 10 and 900")
    if (
        node_cpu_millis > max_vcpu_millis
        or node_memory_mib > max_memory_mib
        or node_storage_mib > max_storage_mib
    ):
        raise ValueError("node shape cannot exceed target resource maxima")
    current_time = _utc(now or datetime.now(UTC), name="now")
    await session.execute(_CAPACITY_MUTATION_LOCK)
    target = await session.get(ServiceExecutionTarget, target_id)
    if target is None or target.provider != "nebius":
        raise ValueError("capacity policy target must be a Nebius execution target")
    group = await resolve_capacity_targets(session, target_id)
    if group.owner.id != target_id:
        raise ValueError("capacity policy must be configured on the capacity owner")
    row = await session.get(ExecutionCapacityPolicy, target_id, with_for_update=True)
    if row is None:
        row = ExecutionCapacityPolicy(
            target_id=target_id,
            enabled=enabled,
            max_unschedulable_jobs=max_unschedulable_jobs,
            max_image_pull_backoff_jobs=max_image_pull_backoff_jobs,
            observation_max_age_seconds=observation_max_age_seconds,
            reason=clean_reason,
            updated_at=current_time,
            **positive_values,
        )
        session.add(row)
    else:
        for name, value in positive_values.items():
            setattr(row, name, value)
        row.max_unschedulable_jobs = max_unschedulable_jobs
        row.max_image_pull_backoff_jobs = max_image_pull_backoff_jobs
        row.observation_max_age_seconds = observation_max_age_seconds
        row.enabled = enabled
        row.reason = clean_reason
        row.version += 1
        row.updated_at = current_time
    await session.flush()
    return row


async def _latest_observation(
    session: AsyncSession, target_id: str
) -> ExecutionCapacityObservation | None:
    return (
        await session.execute(
            select(ExecutionCapacityObservation)
            .where(ExecutionCapacityObservation.target_id == target_id)
            .order_by(
                ExecutionCapacityObservation.observed_at.desc(),
                ExecutionCapacityObservation.id.desc(),
            )
            .limit(1)
        )
    ).scalar_one_or_none()


async def native_allocatable_sample(
    session: AsyncSession, target_id: str, current: CapacityPlacement,
) -> NodeTemplateSample | None:
    sample = cold_sample(current)
    if sample is not None:
        return sample
    target_id = (await resolve_capacity_targets(session, target_id)).owner.id
    # Reuse the same compatible immutable history for actual admission and
    # waiting eligibility. A zero-node observation need not carry a new sample.
    history = (await session.scalars(select(ExecutionCapacityObservation.observation_json)
        .where(ExecutionCapacityObservation.target_id == target_id,
               ExecutionCapacityObservation.observation_json["placement"]["template_samples"].astext != "[]")
        .order_by(ExecutionCapacityObservation.observed_at.desc()).limit(100))).all()
    return cold_sample(current, (CapacityPlacement.model_validate(row["placement"])
                                for row in history if row.get("placement")))


@dataclass(frozen=True)
class _NativeCapacityReservation:
    attempt_id: UUID
    demand_id: str
    target_id: str
    resources: ResourceTotals
    state: str
    reserved_at: datetime
    released: bool


def native_build_resources(native: dict[str, Any]) -> ResourceTotals:
    raw = native["resources"]
    values = {"cpu_millis": raw["vcpu_millis"], "memory_mib": raw["memory_mib"], "storage_mib": raw["storage_mib"]}
    if any(isinstance(value, bool) or not isinstance(value, int) or value <= 0 for value in values.values()):
        raise ValueError("native build resources must be positive integers")
    return ResourceTotals(**values)


async def _native_capacity_reservations(
    session: AsyncSession, *, current_time: datetime,
) -> list[_NativeCapacityReservation]:
    # Reservation writers use canonical UTC ISO timestamps. Retain all active
    # reservations and only the released history needed by the create-rate limit.
    attempts = (await session.scalars(select(TaskImageMaterializationAttempt).where(
        TaskImageMaterializationAttempt.native_build["capacity_reserved_at"].astext.is_not(None),
        or_(
            TaskImageMaterializationAttempt.native_build["capacity_released_at"].astext.is_(None),
            TaskImageMaterializationAttempt.native_build["capacity_reserved_at"].astext
            >= (current_time - timedelta(minutes=1)).isoformat(),
        ),
    ))).all()
    result = []
    for attempt in attempts:
        native = attempt.native_build or {}
        try:
            result.append(_NativeCapacityReservation(
                attempt_id=attempt.id,
                demand_id=f"task-image:{attempt.materialization_id}:{attempt.lease_epoch}",
                target_id=_clean_text(native["target_id"], name="native target", max_length=160),
                resources=native_build_resources(native), state=str(native.get("state", "reserved")),
                reserved_at=_utc(datetime.fromisoformat(native["capacity_reserved_at"]), name="native reservation time"),
                released=native.get("capacity_released_at") is not None,
            ))
        except (KeyError, TypeError, ValueError) as exc:
            raise ExecutionProvisioningBlockedError("execution_capacity_native_reservation_invalid") from exc
    return result


async def reserve_execution_provisioning(
    session: AsyncSession,
    *,
    lease_id: UUID,
    now: datetime | None = None,
    revalidate_existing: bool = False,
) -> ExecutionProvisioningAuthorization | None:
    """Authorize one Nebius create against fresh provider and cluster evidence."""

    current_time = _utc(now or datetime.now(UTC), name="now")
    lease = await session.get(ServiceExecutionLease, lease_id)
    if lease is None or lease.target_id is None:
        raise ValueError("execution lease or target does not exist")
    target = await session.get(ServiceExecutionTarget, lease.target_id)
    if target is None:
        raise ValueError("execution target does not exist")
    if target.provider != "nebius":
        return None
    await session.execute(_CAPACITY_ADMISSION_LOCK)
    existing = (
        await session.execute(
            select(ExecutionProvisioningAuthorization)
            .where(ExecutionProvisioningAuthorization.lease_id == lease_id)
            .with_for_update()
        )
    ).scalar_one_or_none()
    if existing is not None:
        if existing.state == "released":
            raise ExecutionProvisioningBlockedError("execution_capacity_authorization_released")
        if not revalidate_existing:
            return existing
    cost = (
        await session.execute(
            select(ExecutionCostReservation).where(ExecutionCostReservation.lease_id == lease_id)
        )
    ).scalar_one_or_none()
    if cost is None:
        raise ExecutionProvisioningBlockedError("execution_capacity_resource_envelope_unavailable")
    decision = await admit_capacity_resources(
        session, target=target, demand_id=f"{lease_id}:{lease.resource_generation}",
        resources=ResourceTotals(cpu_millis=cost.requested_cpu_millis, memory_mib=cost.requested_memory_mib,
                                 storage_mib=cost.requested_ephemeral_storage_mib),
        current_time=current_time, already_reserved=existing is not None, exclude_lease_id=lease_id,
    )
    if existing is not None:
        return existing
    payload = {"schema_version": "loom.execution-provisioning-authorization.v1", "lease_id": str(lease_id), **decision}
    row = ExecutionProvisioningAuthorization(
        id=uuid4(),
        lease_id=lease_id,
        target_id=target.id,
        observation_id=UUID(decision["observation_id"]),
        policy_version=decision["policy_version"],
        requested_cpu_millis=cost.requested_cpu_millis,
        requested_memory_mib=cost.requested_memory_mib,
        requested_storage_mib=cost.requested_ephemeral_storage_mib,
        incremental_nodes=decision["incremental_nodes"],
        incremental_vcpu_millis=decision["incremental_vcpu_millis"],
        incremental_memory_mib=decision["incremental_memory_mib"],
        incremental_storage_mib=decision["incremental_storage_mib"],
        decision_reason=decision["decision_reason"],
        authorization_sha256=canonical_digest(payload),
        state="authorized",
        authorized_at=current_time,
        updated_at=current_time,
    )
    session.add(row)
    await session.flush()
    return row


async def admit_capacity_resources(
    session: AsyncSession,
    *,
    target: ServiceExecutionTarget,
    demand_id: str,
    resources: ResourceTotals,
    current_time: datetime,
    already_reserved: bool = False,
    exclude_lease_id: UUID | None = None,
    exclude_native_attempt_id: UUID | None = None,
) -> dict[str, Any]:
    """Shared admission arithmetic under the caller's capacity transaction lock.

    Both reservation writers acquire _CAPACITY_ADMISSION_LOCK before checking
    idempotency and persist the returned decision before releasing that lock.
    """
    if target.desired_state != "active":
        raise ExecutionProvisioningBlockedError("execution_capacity_target_not_active")
    if target.health_status != "healthy":
        raise ExecutionProvisioningBlockedError("execution_capacity_target_unhealthy")
    group = await resolve_capacity_targets(session, target.id)
    if group.owner.desired_state != "active":
        raise ExecutionProvisioningBlockedError("execution_capacity_owner_not_active")
    policy = await session.get(ExecutionCapacityPolicy, group.owner.id, with_for_update=True)
    if policy is None or not policy.enabled:
        raise ExecutionProvisioningBlockedError("execution_capacity_policy_unavailable")
    observation = await _latest_observation(session, group.owner.id)
    if observation is None:
        raise ExecutionProvisioningBlockedError("execution_capacity_observation_unavailable")
    if not group.matches_observation_scope(observation.observation_json):
        raise ExecutionProvisioningBlockedError("execution_capacity_target_scope_unavailable")
    if observation.observed_at > current_time + timedelta(seconds=60):
        raise ExecutionProvisioningBlockedError("execution_capacity_observation_from_future")
    if current_time > observation.observed_at + timedelta(
        seconds=policy.observation_max_age_seconds
    ):
        raise ExecutionProvisioningBlockedError("execution_capacity_observation_stale")
    requested_cpu = resources.cpu_millis
    requested_memory = resources.memory_mib
    requested_storage = resources.storage_mib
    if (
        requested_cpu > policy.node_cpu_millis
        or requested_memory > policy.node_memory_mib
        or requested_storage > policy.node_storage_mib
    ):
        raise ExecutionProvisioningBlockedError("execution_capacity_workload_exceeds_node_shape")
    placement_payload = observation.observation_json.get("placement")
    if not placement_payload:
        raise ExecutionProvisioningBlockedError("execution_capacity_placement_unavailable")
    placement = CapacityPlacement.model_validate(placement_payload)
    authorizations = (
        (
            await session.execute(
                select(ExecutionProvisioningAuthorization).where(
                    ExecutionProvisioningAuthorization.state.in_(_ACTIVE_AUTHORIZATION_STATES),
                    *(() if exclude_lease_id is None else (ExecutionProvisioningAuthorization.lease_id != exclude_lease_id,)),
                )
            )
        )
        .scalars()
        .all()
    )
    native = await _native_capacity_reservations(session, current_time=current_time)
    native_active = [row for row in native if not row.released and row.attempt_id != exclude_native_attempt_id]
    from loom_control_plane.task_image_capacity_wait import read_capacity_waits, wait_resources

    claiming = None
    if exclude_native_attempt_id is not None:
        claiming = await session.get(TaskImageMaterializationAttempt, exclude_native_attempt_id)
    # Previously committed authority precedes later waiting work. Revalidation
    # remains subject to genuine reservations and provider/cleanup evidence.
    waiting = [] if already_reserved else await read_capacity_waits(
        session, now=current_time, claiming_attempt=claiming,
        claiming_target=target, claiming_resources=resources,
    )

    def native_demands(target_ids: frozenset[str], *, include_waits: bool = True) -> list[tuple[str, ResourceTotals]]:
        return [
            *((row.demand_id, row.resources) for row in native_active if row.target_id in target_ids),
            *((f"task-image-wait:{row.materialization_id}:{row.lease_epoch}", wait_resources(row))
              for row in waiting if include_waits and row.target_id in target_ids),
        ]

    same_target = [row for row in authorizations if row.target_id in group.target_ids]
    generations = {
        lease_key: generation
        for lease_key, generation in (
            await session.execute(
                select(ServiceExecutionLease.id, ServiceExecutionLease.resource_generation).where(
                    ServiceExecutionLease.id.in_([row.lease_id for row in authorizations])
                )
            )
        ).all()
    }
    observed_leases = {
        f"{pod.lease_id}:{pod.generation}" for node in placement.nodes for pod in node.managed_pods
    } | {f"{pod.lease_id}:{pod.generation}" for pod in placement.pending_pods}
    recent_authorizations = [
        row
        for row in same_target
        if f"{row.lease_id}:{generations[row.lease_id]}" not in observed_leases
    ]
    recent_native = [row for row in native_active
                     if row.target_id in group.target_ids and row.demand_id not in observed_leases]
    recent_creates = await session.scalar(
        select(func.count(ExecutionProvisioningAuthorization.id)).where(
            ExecutionProvisioningAuthorization.target_id.in_(group.target_ids),
            ExecutionProvisioningAuthorization.authorized_at >= current_time - timedelta(minutes=1),
        )
    )
    recent_creates = int(recent_creates or 0) + sum(
        row.target_id in group.target_ids and row.reserved_at >= current_time - timedelta(minutes=1)
        for row in native
    )
    waiting_here = sum(row.target_id in group.target_ids for row in waiting)
    if not already_reserved and int(recent_creates or 0) + waiting_here >= policy.max_create_per_minute:
        raise ExecutionProvisioningBlockedError("execution_capacity_create_rate_exceeded", 30)
    recent_pending = sum(
        row.state in _PENDING_AUTHORIZATION_STATES for row in recent_authorizations
    )
    recent_pending += sum(row.state != "running" for row in recent_native)
    incoming_pending = int(demand_id not in observed_leases)
    if observation.pending_jobs + recent_pending + incoming_pending + waiting_here > policy.max_pending_jobs:
        raise ExecutionProvisioningBlockedError("execution_capacity_pending_limit_exceeded")
    recent_unschedulable = (sum(row.state == "unschedulable" for row in recent_authorizations)
                            + sum(row.state == "unschedulable" for row in recent_native))
    if (
        observation.unschedulable_jobs + recent_unschedulable >= policy.max_unschedulable_jobs
        and policy.max_unschedulable_jobs >= 0
    ):
        if observation.unschedulable_jobs + recent_unschedulable > 0:
            raise ExecutionProvisioningBlockedError(
                "execution_capacity_unschedulable_limit_exceeded"
            )
    recent_image_pull = (sum(row.state == "image_pull_backoff" for row in recent_authorizations)
                         + sum(row.state == "image_pull_backoff" for row in recent_native))
    if (
        observation.image_pull_backoff_jobs + recent_image_pull
        >= policy.max_image_pull_backoff_jobs
        and observation.image_pull_backoff_jobs + recent_image_pull > 0
    ):
        raise ExecutionProvisioningBlockedError(
            "execution_capacity_image_pull_backoff_limit_exceeded"
        )

    def demands(rows: list[ExecutionProvisioningAuthorization]) -> list[tuple[str, ResourceTotals]]:
        return [
            (
                f"{row.lease_id}:{generations[row.lease_id]}",
                ResourceTotals(
                    cpu_millis=row.requested_cpu_millis,
                    memory_mib=row.requested_memory_mib,
                    storage_mib=row.requested_storage_mib,
                ),
            )
            for row in rows
        ]

    try:
        sample = await native_allocatable_sample(session, group.owner.id, placement)
        actual_demands = [*demands(same_target), *native_demands(group.target_ids, include_waits=False)]
        prior = plan_placement(placement, actual_demands, sample=sample)
        actual_projected = plan_placement(
            placement, [*actual_demands, (demand_id, resources)], sample=sample,
        )
        projected = plan_placement(
            placement,
            [
                *demands(same_target),
                *native_demands(group.target_ids),
                (
                    demand_id,
                    ResourceTotals(
                        cpu_millis=requested_cpu,
                        memory_mib=requested_memory,
                        storage_mib=requested_storage,
                    ),
                ),
            ],
            sample=sample,
        )
    except PlacementUnavailableError as exc:
        raise ExecutionProvisioningBlockedError(str(exc)) from exc
    total_nodes = projected.additional_nodes
    # Waiting protects headroom but is not an acquired create or cost event.
    incremental_nodes = max(0, actual_projected.additional_nodes - prior.additional_nodes)
    raw = placement.node_group.raw_node
    if projected.cold_nodes > 0:
        if observation.provider_capacity_state == "insufficient":
            raise ExecutionProvisioningBlockedError(
                "execution_capacity_physical_capacity_unavailable"
            )
        if observation.provider_capacity_state == "unknown":
            reason = (
                "execution_capacity_provisioning_delay"
                if observation.autoscaler_state == "scaling"
                else "execution_capacity_physical_capacity_unknown"
            )
            raise ExecutionProvisioningBlockedError(reason)
        if observation.autoscaler_state in {"stalled", "unknown"}:
            raise ExecutionProvisioningBlockedError(
                f"execution_capacity_autoscaler_{observation.autoscaler_state}"
            )
    projected_active_nodes = placement.node_group.node_count + total_nodes
    policy_limits = (
        (projected_active_nodes, min(policy.max_nodes, placement.node_group.max_nodes), "nodes"),
        (projected_active_nodes * raw.cpu_millis, policy.max_vcpu_millis, "vcpu"),
        (projected_active_nodes * raw.memory_mib, policy.max_memory_mib, "memory"),
        (projected_active_nodes * raw.storage_mib, policy.max_storage_mib, "storage"),
    )
    for projected_value, limit, label in policy_limits:
        if projected_value > limit:
            raise ExecutionProvisioningBlockedError(f"execution_capacity_max_{label}_exceeded")

    # The same database lock serializes ALL target admissions. Account resources
    # are keyed by their native quota identities (SSD can be shared by CPU/GPU).
    # Recompute unobserved demand per node group; never add different pools' free
    # CPU and RAM together or assume a shared lock alone prevents double spend.
    peer_projections: list[tuple[CapacityPlacement, int]] = []
    other_targets = (
        (
            await session.execute(
                select(ServiceExecutionTarget.id)
                .where(
                    ServiceExecutionTarget.provider == "nebius",
                    ServiceExecutionTarget.id.not_in(group.target_ids),
                )
                .order_by(ServiceExecutionTarget.id)
            )
        )
        .scalars()
        .all()
    )
    seen_owners = {group.owner.id}
    for other_target in other_targets:
        other_group = await resolve_capacity_targets(session, other_target)
        if other_group.owner.id in seen_owners:
            continue
        seen_owners.add(other_group.owner.id)
        other_observation = await _latest_observation(session, other_group.owner.id)
        has_active = (any(row.target_id in other_group.target_ids for row in authorizations)
                      or any(row.target_id in other_group.target_ids for row in native_active)
                      or any(row.target_id in other_group.target_ids for row in waiting))
        if other_observation is None and not has_active:
            continue
        if other_observation is None:
            raise ExecutionProvisioningBlockedError(
                "execution_capacity_shared_observation_unavailable"
            )
        other_payload = other_observation.observation_json.get("placement")
        if not other_payload:
            if not has_active:
                continue
            other_target_row = await session.get(ServiceExecutionTarget, other_target)
            if other_target_row is not None and other_target_row.region != target.region:
                continue
            # An old aggregate-only active pool cannot safely be charged to an
            # unknown quota domain. Wait for its ordinary collector upgrade.
            raise ExecutionProvisioningBlockedError(
                "execution_capacity_shared_placement_unavailable"
            )
        other = CapacityPlacement.model_validate(other_payload)
        shared = {
            key
            for key, quota in placement.quota_resources.items()
            if key in other.quota_resources
            and quota_identity(quota) == quota_identity(other.quota_resources[key])
        }
        if not shared:
            continue
        if not other_group.matches_observation_scope(other_observation.observation_json):
            raise ExecutionProvisioningBlockedError("execution_capacity_shared_target_scope_unavailable")
        other_policy = await session.get(ExecutionCapacityPolicy, other_group.owner.id)
        if (
            other_policy is None
            or other_observation.observed_at > current_time + timedelta(seconds=60)
            or current_time
            > other_observation.observed_at
            + timedelta(seconds=other_policy.observation_max_age_seconds)
        ):
            if not has_active:
                continue
            raise ExecutionProvisioningBlockedError("execution_capacity_shared_observation_stale")
        try:
            other_plan = plan_placement(
                other,
                [*demands([row for row in authorizations if row.target_id in other_group.target_ids]),
                 *native_demands(other_group.target_ids)],
                sample=await native_allocatable_sample(session, other_group.owner.id, other),
            )
        except PlacementUnavailableError as exc:
            raise ExecutionProvisioningBlockedError(
                "execution_capacity_shared_placement_unknown"
            ) from exc
        peer_projections.append((other, other_plan.additional_nodes))
    try:
        require_provider_quota_headroom(placement, additional_nodes=total_nodes, peers=peer_projections)
    except PlacementUnavailableError as exc:
        raise ExecutionProvisioningBlockedError(str(exc)) from exc
    decision_reason = "existing_allocatable" if actual_projected.additional_nodes == 0 else "bounded_scale_headroom"
    payload = {
        "target_id": target.id,
        "observation_id": str(observation.id),
        "policy_version": policy.version,
        "requested_cpu_millis": requested_cpu,
        "requested_memory_mib": requested_memory,
        "requested_storage_mib": requested_storage,
        "incremental_nodes": incremental_nodes,
        "incremental_vcpu_millis": incremental_nodes * raw.cpu_millis,
        "incremental_memory_mib": incremental_nodes * raw.memory_mib,
        "incremental_storage_mib": incremental_nodes * raw.storage_mib,
        "decision_reason": decision_reason,
        "authorized_at": current_time.isoformat(),
    }
    return payload


async def fetch_execution_capacity_status(
    session: AsyncSession,
    *,
    pool_id: str | None = None,
    now: datetime | None = None,
) -> dict[str, object]:
    current_time = _utc(now or datetime.now(UTC), name="now")
    target_stmt = select(ServiceExecutionTarget).where(ServiceExecutionTarget.provider == "nebius")
    if pool_id is not None:
        target_stmt = target_stmt.where(ServiceExecutionTarget.logical_pool_id == pool_id)
    targets = (
        (await session.execute(target_stmt.order_by(ServiceExecutionTarget.id))).scalars().all()
    )
    rows: list[dict[str, object]] = []
    for target in targets:
        group = await resolve_capacity_targets(session, target.id)
        policy = await session.get(ExecutionCapacityPolicy, group.owner.id)
        observation = await _latest_observation(session, group.owner.id)
        authorization_counts: dict[str, int] = {
            str(state): int(count)
            for state, count in (
                await session.execute(
                    select(
                        ExecutionProvisioningAuthorization.state,
                        func.count(ExecutionProvisioningAuthorization.id),
                    )
                    .where(ExecutionProvisioningAuthorization.target_id == target.id)
                    .group_by(ExecutionProvisioningAuthorization.state)
                )
            ).all()
        }
        recent_authorizations = (
            (
                await session.execute(
                    select(ExecutionProvisioningAuthorization)
                    .where(ExecutionProvisioningAuthorization.target_id == target.id)
                    .order_by(
                        ExecutionProvisioningAuthorization.authorized_at.desc(),
                        ExecutionProvisioningAuthorization.id.desc(),
                    )
                    .limit(100)
                )
            )
            .scalars()
            .all()
        )
        command_backlog = await session.scalar(
            select(func.count(ServiceExecutionCommand.id))
            .join(
                ServiceExecutionLease, ServiceExecutionLease.id == ServiceExecutionCommand.lease_id
            )
            .where(
                ServiceExecutionLease.target_id == target.id,
                ServiceExecutionCommand.command_type == "create",
                ServiceExecutionCommand.state.in_(("pending", "leased")),
            )
        )
        fresh_until = (
            observation.observed_at + timedelta(seconds=policy.observation_max_age_seconds)
            if observation is not None and policy is not None
            else None
        )
        observation_is_fresh = (
            fresh_until is not None
            and observation is not None
            and group.matches_observation_scope(observation.observation_json)
            and observation.observed_at <= current_time + timedelta(seconds=60)
            and current_time <= fresh_until
        )
        blockers: list[str] = []
        if policy is None or not policy.enabled:
            blockers.append("execution_capacity_policy_unavailable")
        if target.desired_state != "active":
            blockers.append("execution_capacity_target_not_active")
        if group.owner.id != target.id and group.owner.desired_state != "active":
            blockers.append("execution_capacity_owner_not_active")
        if target.health_status != "healthy":
            blockers.append("execution_capacity_target_unhealthy")
        if observation is None:
            blockers.append("execution_capacity_observation_unavailable")
        elif not observation_is_fresh:
            blockers.append("execution_capacity_observation_stale")
        if observation is not None:
            if not group.matches_observation_scope(observation.observation_json):
                blockers.append("execution_capacity_target_scope_unavailable")
            if not observation.observation_json.get("placement"):
                blockers.append("execution_capacity_placement_unavailable")
            if observation.provider_capacity_state == "insufficient":
                blockers.append("execution_capacity_physical_capacity_unavailable")
            elif observation.provider_capacity_state == "unknown":
                blockers.append("execution_capacity_physical_capacity_unknown")
            if observation.autoscaler_state == "stalled":
                blockers.append("execution_capacity_autoscaler_stalled")
            elif observation.autoscaler_state == "unknown":
                blockers.append("execution_capacity_autoscaler_unknown")
        if observation is not None and policy is not None:
            if observation.pending_jobs >= policy.max_pending_jobs:
                blockers.append("execution_capacity_pending_limit_exceeded")
            if (
                observation.unschedulable_jobs > 0
                and observation.unschedulable_jobs >= policy.max_unschedulable_jobs
            ):
                blockers.append("execution_capacity_unschedulable_limit_exceeded")
            if (
                observation.image_pull_backoff_jobs > 0
                and observation.image_pull_backoff_jobs >= policy.max_image_pull_backoff_jobs
            ):
                blockers.append("execution_capacity_image_pull_backoff_limit_exceeded")
            resource_limits = (
                (observation.active_nodes, policy.max_nodes, "nodes"),
                (
                    observation.provisioned_vcpu_millis,
                    policy.max_vcpu_millis,
                    "vcpu",
                ),
                (observation.provisioned_memory_mib, policy.max_memory_mib, "memory"),
                (observation.provisioned_storage_mib, policy.max_storage_mib, "storage"),
            )
            for used, limit, label in resource_limits:
                if used >= limit:
                    blockers.append(f"execution_capacity_max_{label}_exceeded")
            quota_limits = (
                (observation.provider_used_nodes, observation.provider_quota_nodes, "nodes"),
                (
                    observation.provider_used_vcpu_millis,
                    observation.provider_quota_vcpu_millis,
                    "vcpu",
                ),
                (
                    observation.provider_used_memory_mib,
                    observation.provider_quota_memory_mib,
                    "memory",
                ),
                (
                    observation.provider_used_storage_mib,
                    observation.provider_quota_storage_mib,
                    "storage",
                ),
            )
            for used, limit, label in quota_limits:
                if used >= limit:
                    blockers.append(f"execution_capacity_provider_quota_{label}_exceeded")
        rows.append(
            {
                "target_id": target.id,
                "execution_class_id": target.execution_class_id,
                "capacity_owner_target_id": group.owner.id,
                **({"target_scope": group.scope.model_dump(mode="json")} if group.scope is not None else {}),
                "pool_id": target.logical_pool_id,
                "environment": target.environment,
                "region": target.region,
                "desired_state": target.desired_state,
                "health_status": target.health_status,
                "policy": (
                    {
                        "enabled": policy.enabled,
                        "max_nodes": policy.max_nodes,
                        "max_vcpu_millis": policy.max_vcpu_millis,
                        "max_memory_mib": policy.max_memory_mib,
                        "max_storage_mib": policy.max_storage_mib,
                        "node_cpu_millis": policy.node_cpu_millis,
                        "node_memory_mib": policy.node_memory_mib,
                        "node_storage_mib": policy.node_storage_mib,
                        "max_pending_jobs": policy.max_pending_jobs,
                        "max_unschedulable_jobs": policy.max_unschedulable_jobs,
                        "max_image_pull_backoff_jobs": policy.max_image_pull_backoff_jobs,
                        "max_create_per_minute": policy.max_create_per_minute,
                        "observation_max_age_seconds": policy.observation_max_age_seconds,
                        "reason": policy.reason,
                        "version": policy.version,
                    }
                    if policy is not None
                    else None
                ),
                "observation": (
                    {
                        "id": str(observation.id),
                        "placement_available": bool(observation.observation_json.get("placement")),
                        "native_node_group": (
                            observation.observation_json.get("placement") or {}
                        ).get("node_group"),
                        "source": observation.source,
                        "source_version": observation.source_version,
                        "observed_at": observation.observed_at.isoformat(),
                        "fresh_until": fresh_until.isoformat() if fresh_until else None,
                        "is_fresh": observation_is_fresh,
                        "provider_capacity_state": observation.provider_capacity_state,
                        "provider_capacity_reason": observation.provider_capacity_reason,
                        "autoscaler_state": observation.autoscaler_state,
                        "autoscaler_reason": observation.autoscaler_reason,
                        "provider_quota_nodes": observation.provider_quota_nodes,
                        "provider_used_nodes": observation.provider_used_nodes,
                        "provider_quota_nodes_headroom": max(
                            0,
                            observation.provider_quota_nodes - observation.provider_used_nodes,
                        ),
                        "provider_quota_vcpu_millis": observation.provider_quota_vcpu_millis,
                        "provider_used_vcpu_millis": observation.provider_used_vcpu_millis,
                        "provider_quota_vcpu_millis_headroom": max(
                            0,
                            observation.provider_quota_vcpu_millis
                            - observation.provider_used_vcpu_millis,
                        ),
                        "provider_quota_memory_mib": observation.provider_quota_memory_mib,
                        "provider_used_memory_mib": observation.provider_used_memory_mib,
                        "provider_quota_memory_mib_headroom": max(
                            0,
                            observation.provider_quota_memory_mib
                            - observation.provider_used_memory_mib,
                        ),
                        "provider_quota_storage_mib": observation.provider_quota_storage_mib,
                        "provider_used_storage_mib": observation.provider_used_storage_mib,
                        "provider_quota_storage_mib_headroom": max(
                            0,
                            observation.provider_quota_storage_mib
                            - observation.provider_used_storage_mib,
                        ),
                        "active_nodes": observation.active_nodes,
                        **observed_node_activity(observation.observation_json),
                        "node_states": (
                            observation.observation_json.get("node_states")
                            if isinstance(observation.observation_json, dict)
                            else None
                        ),
                        "policy_nodes_headroom": (
                            max(0, policy.max_nodes - observation.active_nodes)
                            if policy is not None
                            else None
                        ),
                        "provisioned_vcpu_millis": observation.provisioned_vcpu_millis,
                        "policy_vcpu_millis_headroom": (
                            max(
                                0,
                                policy.max_vcpu_millis - observation.provisioned_vcpu_millis,
                            )
                            if policy is not None
                            else None
                        ),
                        "provisioned_memory_mib": observation.provisioned_memory_mib,
                        "policy_memory_mib_headroom": (
                            max(
                                0,
                                policy.max_memory_mib - observation.provisioned_memory_mib,
                            )
                            if policy is not None
                            else None
                        ),
                        "provisioned_storage_mib": observation.provisioned_storage_mib,
                        "policy_storage_mib_headroom": (
                            max(
                                0,
                                policy.max_storage_mib - observation.provisioned_storage_mib,
                            )
                            if policy is not None
                            else None
                        ),
                        "allocatable_cpu_millis": observation.allocatable_cpu_millis,
                        "requested_cpu_millis": observation.requested_cpu_millis,
                        "allocatable_cpu_millis_free": max(
                            0,
                            observation.allocatable_cpu_millis - observation.requested_cpu_millis,
                        ),
                        "allocatable_memory_mib": observation.allocatable_memory_mib,
                        "requested_memory_mib": observation.requested_memory_mib,
                        "allocatable_memory_mib_free": max(
                            0,
                            observation.allocatable_memory_mib - observation.requested_memory_mib,
                        ),
                        "allocatable_storage_mib": observation.allocatable_storage_mib,
                        "requested_storage_mib": observation.requested_storage_mib,
                        "allocatable_storage_mib_free": max(
                            0,
                            observation.allocatable_storage_mib - observation.requested_storage_mib,
                        ),
                        "pending_jobs": observation.pending_jobs,
                        "unschedulable_jobs": observation.unschedulable_jobs,
                        "image_pull_backoff_jobs": observation.image_pull_backoff_jobs,
                        "pending_reasons": observation.pending_reasons_json,
                        "observation_sha256": observation.observation_sha256,
                    }
                    if observation is not None
                    else None
                ),
                "command_backlog": int(command_backlog or 0),
                "authorization_counts": {
                    str(state): int(count) for state, count in authorization_counts.items()
                },
                "recent_authorizations": [
                    {
                        "id": str(authorization.id),
                        "lease_id": str(authorization.lease_id),
                        "observation_id": str(authorization.observation_id),
                        "policy_version": authorization.policy_version,
                        "state": authorization.state,
                        "requested_cpu_millis": authorization.requested_cpu_millis,
                        "requested_memory_mib": authorization.requested_memory_mib,
                        "requested_storage_mib": authorization.requested_storage_mib,
                        "incremental_nodes": authorization.incremental_nodes,
                        "incremental_vcpu_millis": authorization.incremental_vcpu_millis,
                        "incremental_memory_mib": authorization.incremental_memory_mib,
                        "incremental_storage_mib": authorization.incremental_storage_mib,
                        "decision_reason": authorization.decision_reason,
                        "authorization_sha256": authorization.authorization_sha256,
                        "authorized_at": authorization.authorized_at.isoformat(),
                        "released_at": (
                            authorization.released_at.isoformat()
                            if authorization.released_at
                            else None
                        ),
                    }
                    for authorization in recent_authorizations
                ],
                "blockers": blockers,
            }
        )
    return {"targets": rows}


__all__ = [
    "ExecutionProvisioningBlockedError",
    "create_execution_capacity_observation",
    "fetch_execution_capacity_status",
    "reserve_execution_provisioning",
    "upsert_execution_capacity_policy",
]
