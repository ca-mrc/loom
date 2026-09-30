"""Physical snapshots and placement, under the management mutation lock.

No caller-supplied totals or environment snapshots are summed. Each connected
physical pool contributes once; native account usage is a floor, not free quota.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.nebius_pool_schema import (
    NebiusPoolBinding,
    NebiusPoolCapture,
    NebiusPoolObservation,
    NebiusPoolRequest,
)
from loom.nebius_pool_contract import PoolParticipantV1
from loom.pipeline.keys import canonical_digest
from loom_control_plane.execution_placement import (
    PlacementUnavailableError,
    cold_sample,
    plan_placement,
    quota_identity,
    require_provider_quota_headroom,
)
from loom_execution_capacity_collector.contracts import (
    CapacityPlacement,
    KubernetesCapacitySnapshot,
    NodeTemplateSample,
    ProviderCapacitySnapshot,
    ResourceTotals,
)
from loom_execution_capacity_collector.pool import PoolObservationScope, PoolPodClassifier
from loom_service.pool_management.observations import read_pool_registration

CHARGED_PHASES = ("reserved", "create_intent", "observed", "cleanup_intent")


class PoolCapacityPolicyV1(BaseModel):
    """Protected safety bounds; not environment shares or monetary budgets."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)
    observation_max_age_seconds: int = Field(ge=10, le=900)
    max_create_per_minute: int = Field(ge=1)
    max_pending_jobs: int = Field(ge=1)
    max_unschedulable_jobs: int = Field(ge=0)
    max_image_pull_backoff_jobs: int = Field(ge=0)
    build_concurrency_limit: int = Field(ge=1)


@dataclass(frozen=True)
class PoolCapacity:
    pool: NebiusPoolBinding
    participants: tuple[PoolParticipantV1, ...]
    policy: PoolCapacityPolicyV1
    placement: CapacityPlacement
    provider: ProviderCapacitySnapshot
    kubernetes: KubernetesCapacitySnapshot
    sample: NodeTemplateSample | None


def digest(value: Any) -> str:
    return canonical_digest(value).removeprefix("sha256:")


def _quota_ids(pool: NebiusPoolBinding) -> dict[str, tuple[str, ...]]:
    raw = pool.binding_json["quota_identities"]
    # Ordinary Nebius CPU instances have no separate memory quota. Retain
    # physical memory fit without inventing a shared provider allowance.
    if (not isinstance(raw, dict) or not {"nodes", "vcpu", "storage"} <= set(raw)
            or not set(raw) <= {"nodes", "vcpu", "memory", "storage"}):
        raise ValueError("pool_quota_binding_unavailable")
    if any(not isinstance(value, list) or len(value) != 5
           or any(not isinstance(item, str) or not item for item in value) for value in raw.values()):
        raise ValueError("pool_quota_binding_unavailable")
    return {key: tuple(value) for key, value in raw.items()}


def _placement(payload: dict[str, Any]) -> tuple[CapacityPlacement, ProviderCapacitySnapshot, KubernetesCapacitySnapshot]:
    provider = ProviderCapacitySnapshot.model_validate(payload["provider"])
    kubernetes = KubernetesCapacitySnapshot.model_validate(payload["kubernetes"])
    if provider.node_group is None or provider.node_count != provider.node_group.node_count:
        raise ValueError("pool_node_group_unavailable")
    return CapacityPlacement(
        node_group=provider.node_group, quota_resources=provider.quota_resources,
        nodes=kubernetes.nodes, pending_pods=kubernetes.pending_pods,
        daemonsets=kubernetes.daemonsets, template_samples=kubernetes.template_samples,
    ), provider, kubernetes


async def _read_capacity(session: AsyncSession, pool: NebiusPoolBinding, now: datetime) -> PoolCapacity:
    participants, registration = await read_pool_registration(session, pool)
    if pool.binding_json["node_selector"].get("nebius.com/node-group-id") != pool.node_group_id:
        raise ValueError("pool_physical_selector_unavailable")
    policy = PoolCapacityPolicyV1.model_validate(pool.binding_json["admission"])
    result = (await session.execute(select(NebiusPoolObservation, NebiusPoolCapture).join(
        NebiusPoolCapture, NebiusPoolCapture.capture_id == NebiusPoolObservation.capture_id,
    ).where(NebiusPoolObservation.pool_id == pool.pool_id).order_by(
        NebiusPoolObservation.observed_at.desc(), NebiusPoolObservation.observation_id.desc(),
    ).limit(1))).one_or_none()
    if result is None:
        raise ValueError("pool_observation_unavailable")
    observation, capture = result
    payload = observation.observation_json
    scope = PoolObservationScope.model_validate(capture.scope_json)
    fingerprint = PoolPodClassifier(scope).fingerprint
    if (capture.registration_sha256 != registration or capture.admission_epoch != pool.admission_epoch
            or capture.scope_sha256 != fingerprint.removeprefix("sha256:")
            or observation.observation_sha256 != digest(payload)
            or payload["capture_id"] != str(capture.capture_id)
            or datetime.fromisoformat(payload["observed_at"]) != observation.observed_at
            or observation.observed_at > now + timedelta(seconds=60)
            or now > observation.observed_at + timedelta(seconds=policy.observation_max_age_seconds)):
        raise ValueError("pool_observation_unavailable")
    placement, provider, kubernetes = _placement(payload)
    if (placement.node_group.id != pool.node_group_id
            or {key: quota_identity(value) for key, value in placement.quota_resources.items()} != _quota_ids(pool)
            or kubernetes.source_versions.get("pool_scope") != fingerprint):
        raise ValueError("pool_observation_binding_mismatch")
    sample = cold_sample(placement)
    if sample is None:
        # Filter before bounding history: prolonged scale-zero must not push the
        # last measured nonempty template sample out of the search window.
        history = (await session.scalars(select(NebiusPoolObservation).where(
            NebiusPoolObservation.pool_id == pool.pool_id,
            NebiusPoolObservation.observation_json["kubernetes"]["template_samples"].astext != "[]",
            NebiusPoolObservation.observed_at <= observation.observed_at,
        ).order_by(NebiusPoolObservation.observed_at.desc()).limit(100))).all()
        sample = cold_sample(placement, (_placement(row.observation_json)[0] for row in history
                                        if digest(row.observation_json) == row.observation_sha256))
    return PoolCapacity(pool, participants, policy, placement, provider, kubernetes, sample)


async def read_connected_capacity(session: AsyncSession, pool_id: UUID, now: datetime) -> dict[UUID, PoolCapacity]:
    """Qualify the connected quota component; unrelated stale pools cannot block."""
    pools = list((await session.scalars(select(NebiusPoolBinding).order_by(
        NebiusPoolBinding.pool_id).limit(1001).with_for_update().execution_options(populate_existing=True))).all())
    if len(pools) > 1000:
        raise ValueError("pool_registry_bound_exceeded")
    identities = {pool.pool_id: _quota_ids(pool) for pool in pools}
    connected = {pool_id}
    while True:
        before = len(connected)
        for key, quotas in identities.items():
            if any(any(name in identities[peer] and quotas[name] == identities[peer][name]
                       for name in quotas) for peer in tuple(connected)):
                connected.add(key)
        if len(connected) == before:
            break
    return {pool.pool_id: await _read_capacity(session, pool, now) for pool in pools if pool.pool_id in connected}


def resources(row: NebiusPoolRequest) -> ResourceTotals:
    if row.pod_slots != 1:
        raise ValueError("pool_single_pod_required")
    return ResourceTotals(cpu_millis=row.cpu_millis, memory_mib=row.memory_mib, storage_mib=row.ephemeral_storage_mib)


def demand_id(row: NebiusPoolRequest) -> str:
    return f"reservation:{row.request_id}:1"


def require_fit(capacities: dict[UUID, PoolCapacity], *, candidate: NebiusPoolRequest,
                active: list[NebiusPoolRequest], protected_waits: list[NebiusPoolRequest],
                recent_grants: dict[UUID, int]) -> None:
    """Protect fitting earlier waits without acquiring their resources or writes."""
    current = capacities[candidate.pool_id]
    build_kinds = {"task_image_build", "application_image_build"}
    if candidate.workload_kind in build_kinds:
        builds = sum(row.pool_id == candidate.pool_id and row.workload_kind in build_kinds
                     for row in (*active, *protected_waits))
        if builds + 1 > current.policy.build_concurrency_limit:
            raise PlacementUnavailableError("pool_build_concurrency_exceeded")
    rows = [*active, *protected_waits, candidate]
    projected = {key: plan_placement(value.placement,
        ((demand_id(row), resources(row)) for row in rows if row.pool_id == key), sample=value.sample)
        for key, value in capacities.items()}
    plan = projected[candidate.pool_id]
    if current.placement.node_group.node_count + plan.additional_nodes > current.placement.node_group.max_nodes:
        raise PlacementUnavailableError("pool_max_nodes_exceeded")
    if plan.cold_nodes and (current.provider.provider_capacity_state != "available"
                           or current.provider.autoscaler_state not in {"ready", "scaling"}):
        raise PlacementUnavailableError("pool_scale_capacity_unavailable")
    virtual = sum(row.pool_id == candidate.pool_id for row in protected_waits) + 1
    if recent_grants.get(candidate.pool_id, 0) + virtual > current.policy.max_create_per_minute:
        raise PlacementUnavailableError("pool_create_rate_exceeded")
    observed = {f"{pod.lease_id}:{pod.generation}" for node in current.placement.nodes for pod in node.managed_pods}
    observed.update(f"{pod.lease_id}:{pod.generation}" for pod in current.placement.pending_pods)
    unseen = sum(row.pool_id == candidate.pool_id and demand_id(row) not in observed for row in active)
    if current.kubernetes.pending_jobs + unseen + virtual > current.policy.max_pending_jobs:
        raise PlacementUnavailableError("pool_pending_limit_exceeded")
    for amount, limit, reason in (
        (current.kubernetes.unschedulable_jobs, current.policy.max_unschedulable_jobs, "pool_unschedulable_limit_exceeded"),
        (current.kubernetes.image_pull_backoff_jobs, current.policy.max_image_pull_backoff_jobs, "pool_image_pull_backoff_limit_exceeded"),
    ):
        if amount > 0 and amount >= limit:
            raise PlacementUnavailableError(reason)
    require_provider_quota_headroom(current.placement, additional_nodes=plan.additional_nodes,
        peers=((value.placement, projected[key].additional_nodes)
               for key, value in capacities.items() if key != candidate.pool_id))
