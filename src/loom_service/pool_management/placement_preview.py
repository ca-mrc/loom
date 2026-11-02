"""Own-pool READ ONLY preview, sharing the real admission qualifications."""
from __future__ import annotations

from datetime import timedelta
from uuid import uuid4

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.nebius_pool_schema import NebiusPoolParticipant, NebiusPoolRequest
from loom.execution_resource_allocation import allocate_node_resources
from loom.execution_runtime_contract import ContainerResourcesV1
from loom.nebius_pool_allocation import PoolNodeAllocationRequestV1, PoolNodeAllocationV1
from loom.nebius_pool_placement_preview import PoolExecutionPreviewRequestV1, PoolExecutionPreviewV1
from loom.nebius_pool_workload import PoolExecutionPrepareV1
from loom_control_plane.execution_placement import PlacementUnavailableError, incoming_warm_nodes
from loom_control_plane.task_image_io_capacity import NativeIOUnavailable, native_io_candidates
from loom_service.pool_management.admission_metrics import AdmissionWork
from loom_service.pool_management.auth import PoolPrincipal
from loom_service.pool_management.capacity import (
    demand_id,
    observe_connected_capacity,
    resources,
)
from loom_service.pool_management.control import _clock
from loom_service.pool_management.intake import intake_open
from loom_service.pool_management.locks import require_pool_read_snapshot
from loom_service.pool_management.origin import qualify_retained_pool_origin
from loom_service.pool_management.profile_registry import resolve_profiles
from loom_service.pool_management.profile_types import PoolProfiles
from loom_service.pool_management.registry import _render, qualify_waiting_capacity


async def preview_execution(session: AsyncSession, principal: PoolPrincipal,
                            body: PoolExecutionPreviewRequestV1, *, profiles: PoolProfiles | None) -> PoolExecutionPreviewV1:
    await require_pool_read_snapshot(session)
    supported = await session.scalar(text("SELECT EXISTS (SELECT 1 FROM pg_attribute "
        "WHERE attrelid=to_regclass('nebius_pool_bindings') AND attname='prefer_incoming_warm_fit' AND NOT attisdropped)"))
    if not supported:
        raise ValueError("pool_preview_unsupported")
    request = body.request
    if (principal.role != "participant" or principal.workload_scope != "environment"
            or (request.pool_id, request.key.participant_id) != (principal.pool_id, principal.participant_id)
            or request.key.workload_kind != "trial"):
        raise ValueError("pool_preview_scope_unavailable")
    now = await _clock(session)
    capacities = await observe_connected_capacity(session, request.pool_id, now)
    capacity = capacities[request.pool_id]
    pool = capacity.pool
    await session.refresh(pool, attribute_names=["prefer_incoming_warm_fit"])
    participant = next((value for value in capacity.participants if value.participant_id == principal.participant_id), None)
    retained = await session.get(NebiusPoolParticipant, principal.participant_id)
    if (pool.mode != "global" or pool.admission_epoch != request.admission_epoch or participant is None
            or participant.admission_epoch != request.admission_epoch
            or participant.binding_revision != request.participant_revision or retained is None
            or retained.phase != "active" or not await intake_open(session, pool, retained)):
        raise ValueError("pool_preview_scope_unavailable")
    participant.target(request.target_id, "trial")
    priority = await qualify_retained_pool_origin(session, request.origin, participant=participant,
        cluster_id=pool.cluster_id, workload_kind="trial", lock_history=False)
    original, selected = await resolve_profiles(session, pool, participant, request.participant_revision,
                                               retained=False, fallback=profiles)
    allocation = request.execution.node_allocation
    if body.allocate_resources:
        sample = capacity.sample
        if sample is None or allocation is not None or request.execution.runtime.node_resource_allocation is not None:
            raise ValueError("pool_preview_allocation_unavailable")
        allocation = PoolNodeAllocationV1(scope=PoolNodeAllocationRequestV1(pool_id=request.pool_id,
            participant_id=participant.participant_id, admission_epoch=request.admission_epoch,
            participant_revision=request.participant_revision, target_id=request.target_id),
            observation_id=capacity.observation_id, observed_at=capacity.observed_at,
            valid_until=capacity.observed_at + timedelta(seconds=capacity.policy.observation_max_age_seconds),
            usable_node=ContainerResourcesV1(cpu_millis=sample.allocatable.cpu_millis - sample.daemonset_requests.cpu_millis,
                memory_mib=sample.allocatable.memory_mib - sample.daemonset_requests.memory_mib,
                ephemeral_storage_mib=sample.allocatable.storage_mib - sample.daemonset_requests.storage_mib))
        runtime = allocate_node_resources(request.execution.runtime, target_id=request.target_id, usable_node=allocation.usable_node)
        request = PoolExecutionPrepareV1.model_validate(request.model_copy(update={"execution":
            request.execution.model_copy(update={"runtime": runtime, "node_allocation": allocation})}).model_dump())
    prepared = _render(request, original, selected, uuid4(), now)
    if prepared.job["spec"]["template"]["spec"]["nodeSelector"] != pool.binding_json["node_selector"]:
        raise ValueError("pool_preview_selector_unavailable")
    candidate = NebiusPoolRequest(request_id=uuid4(), pool_id=request.pool_id, participant_id=participant.participant_id,
        namespace_uid=prepared.namespace_uid, workload_kind="trial", local_work_id=request.key.local_work_id,
        generation=request.key.generation, admission_epoch=request.admission_epoch, target_id=request.target_id,
        request_sha256=prepared.request_sha256, request_json=request.model_dump(mode="json"),
        deadline_at=request.deadline_at, cpu_millis=prepared.resources.cpu_millis, memory_mib=prepared.resources.memory_mib,
        ephemeral_storage_mib=prepared.resources.storage_mib, pod_slots=1, phase="waiting", priority=priority,
        created_at=now, renewed_at=now)
    uids: frozenset[str] = frozenset()
    eligible, reason = True, "pool_preview_cold_or_unknown"
    try:
        active, waits, _ = await qualify_waiting_capacity(session, capacities=capacities, row=candidate,
            request=request, profiles=profiles, original=original, selected_profiles=selected, now=now,
            work=AdmissionWork(), lock_history=False)
        local = [row for row in [*active, *waits] if row.pool_id == request.pool_id]
        demands = [*((demand_id(row), resources(row)) for row in local), (demand_id(candidate), prepared.resources)]
        uids = incoming_warm_nodes(capacity.placement, demands, incoming_id=demand_id(candidate), sample=capacity.sample)
        # Management has no phase proof for a still-unobserved/heavy build.
        # Never fabricate causal feedback from its reservation timestamp.
        if any(row.workload_kind != "trial" and row.workload_kind != "verifier" for row in local):
            uids = frozenset()
        if uids:
            try:
                choices = native_io_candidates(capacity.placement, [], now=now,
                    max_age_seconds=min(60, capacity.policy.observation_max_age_seconds),
                    max_full_avg10=100, eligible_node_uids=uids)
                uids = uids & frozenset(value["node_uid"] for value in choices if value["mode"] == "node")
            except NativeIOUnavailable:
                uids = frozenset()
        if uids:
            reason = "pool_preview_warm_fit"
    except PlacementUnavailableError as error:
        eligible, reason = False, str(error)
    valid_until = min(request.deadline_at,
        capacity.observed_at + timedelta(seconds=min(60, capacity.policy.observation_max_age_seconds)))
    for node in capacity.placement.nodes:
        pressure = node.io_stall_window if node.io_stall_window is not None else node.io_pressure
        if node.uid in uids and pressure is not None:
            valid_until = min(valid_until, pressure.sampled_at + timedelta(seconds=min(60, capacity.policy.observation_max_age_seconds)))
    return PoolExecutionPreviewV1(pool_id=request.pool_id, participant_id=participant.participant_id,
        target_id=request.target_id, admission_epoch=request.admission_epoch, participant_revision=request.participant_revision,
        execution_namespace_uid=participant.execution_namespace.uid, observation_id=capacity.observation_id,
        observed_at=capacity.observed_at, valid_until=valid_until,
        node_allocation=allocation, effective_requests=prepared.resources, request_sha256=candidate.request_sha256,
        prefer_incoming_warm_fit=pool.prefer_incoming_warm_fit, eligible=eligible,
        warm_node_uids=tuple(sorted(uids)), reason=reason)
