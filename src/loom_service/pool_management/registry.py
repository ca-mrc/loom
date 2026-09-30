"""Atomic execution preparation. Caller owns commit; no external operation runs.

Admission is internal until the production observer, gateway and local outboxes
are connected. A prepare never creates a Job or releases an outstanding grant.
"""
from __future__ import annotations

from collections.abc import Mapping
from datetime import timedelta
from typing import Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict
from sqlalchemy import func, insert, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.nebius_pool_schema import (
    NebiusPoolBinding,
    NebiusPoolCapture,
    NebiusPoolObservation,
    NebiusPoolParticipant,
    NebiusPoolRequest,
)
from loom.nebius_pool_contract import PoolReceiptV1, PoolRequestKeyV1
from loom.nebius_pool_workload import PoolExecutionPrepareV1
from loom_control_plane.execution_placement import PlacementUnavailableError
from loom_service.pool_management.auth import PoolPrincipal, authorize_pool_machine
from loom_service.pool_management.capacity import (
    CHARGED_PHASES,
    digest,
    read_connected_capacity,
    require_fit,
    resources,
)
from loom_service.pool_management.locks import acquire_pool_mutation_lock
from loom_service.pool_management.origin import qualify_pool_origin
from loom_service.pool_management.render import PoolExecutionProfile, prepare_pool_execution


class PoolAdmissionError(ValueError):
    def __init__(self, reason: str = "pool_admission_unavailable") -> None:
        super().__init__(reason)


class PoolWaitingV1(BaseModel):
    """No reservation identity: this demand has acquired no capacity."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    schema_version: Literal["loom.pool-waiting.v1"] = "loom.pool-waiting.v1"
    phase: Literal["waiting"] = "waiting"
    request_key: PoolRequestKeyV1
    pool_id: UUID
    request_sha256: str
    reason: str


def _receipt(row: NebiusPoolRequest) -> PoolReceiptV1:
    return PoolReceiptV1.model_validate({
        "reservation_id": row.request_id, "pool_id": row.pool_id,
        "request_key": {"participant_id": row.participant_id, "workload_kind": row.workload_kind,
                        "local_work_id": row.local_work_id, "generation": row.generation},
        "admission_epoch": row.admission_epoch, "request_sha256": row.request_sha256, "phase": row.phase,
        "plan_sha256": row.plan_sha256, "job_uid": row.job_uid, "cleanup_observation_id": row.cleanup_observation_id,
    })


async def prepare_execution(session: AsyncSession, principal: PoolPrincipal, request: PoolExecutionPrepareV1, *,
                            profiles: Mapping[UUID, PoolExecutionProfile]) -> PoolReceiptV1 | PoolWaitingV1:
    """Serialize quota/pool/identity/request qualification before a first grant."""
    try:
        with session.no_autoflush:
            return await _prepare_execution(session, principal, request, profiles)
    except PoolAdmissionError:
        raise
    except (ValueError, KeyError, TypeError):
        # Do not leak rendering inputs, credential hashes or database documents.
        raise PoolAdmissionError from None


async def _prepare_execution(session: AsyncSession, principal: PoolPrincipal, request: PoolExecutionPrepareV1,
                             profiles: Mapping[UUID, PoolExecutionProfile]) -> PoolReceiptV1 | PoolWaitingV1:
    request = PoolExecutionPrepareV1.model_validate(request.model_dump())
    if any(isinstance(row, (NebiusPoolBinding, NebiusPoolParticipant, NebiusPoolRequest,
                           NebiusPoolCapture, NebiusPoolObservation))
           for row in session.new | session.dirty | session.deleted):
        raise PoolAdmissionError
    if (request.pool_id, request.key.participant_id) != (principal.pool_id, principal.participant_id):
        raise PoolAdmissionError
    await acquire_pool_mutation_lock(session)
    # All writers take the global lock first, then physical rows, then auth and
    # request rows. It also serializes intersecting provider-quota domains.
    pool = (await session.scalars(select(NebiusPoolBinding).where(
        NebiusPoolBinding.pool_id == principal.pool_id,
    ).with_for_update().execution_options(populate_existing=True))).one_or_none()
    await authorize_pool_machine(session, principal, role="participant",
                                 pool_id=request.pool_id, participant_id=request.key.participant_id)
    row = (await session.scalars(select(NebiusPoolRequest).where(
        NebiusPoolRequest.participant_id == request.key.participant_id,
        NebiusPoolRequest.workload_kind == request.key.workload_kind,
        NebiusPoolRequest.local_work_id == request.key.local_work_id,
        NebiusPoolRequest.generation == request.key.generation,
    ).with_for_update().execution_options(populate_existing=True))).one_or_none()
    request_json = request.model_dump(mode="json")
    request_sha = digest(request_json)
    if row is not None:
        if row.request_sha256 != request_sha or row.request_json != request_json:
            raise PoolAdmissionError("pool_request_conflict")
        if row.phase != "waiting":
            # Idempotent readback is not new admission. Never rerender, renew a
            # deadline or make a lost reply depend on still-available capacity.
            return _receipt(row)
    if pool is None or pool.mode != "global":
        raise PoolAdmissionError
    priority = await qualify_pool_origin(session, principal, request.origin,
        target_id=request.target_id, workload_kind=request.key.workload_kind)
    now = (await session.execute(select(func.clock_timestamp()))).scalar_one()
    capacities = await read_connected_capacity(session, pool.pool_id, now)
    participant = next(value for value in capacities[pool.pool_id].participants
                       if value.participant_id == request.key.participant_id)
    target = participant.target(request.target_id, request.key.workload_kind)
    reservation_id = row.request_id if row is not None else uuid4()
    prepared = prepare_pool_execution(request, participant=participant, profile=profiles[target.profile_id],
                                      reservation_id=reservation_id, now=now)
    if prepared.job["spec"]["template"]["spec"]["nodeSelector"] != pool.binding_json["node_selector"]:
        raise PoolAdmissionError
    if row is None:
        row = (await session.scalars(insert(NebiusPoolRequest).values(
            request_id=reservation_id, pool_id=request.pool_id, participant_id=request.key.participant_id,
            namespace_uid=prepared.namespace_uid, workload_kind=request.key.workload_kind,
            local_work_id=request.key.local_work_id, generation=request.key.generation,
            admission_epoch=request.admission_epoch, target_id=request.target_id,
            request_sha256=request_sha, request_json=request_json, deadline_at=request.deadline_at,
            cpu_millis=prepared.resources.cpu_millis, memory_mib=prepared.resources.memory_mib,
            ephemeral_storage_mib=prepared.resources.storage_mib, pod_slots=prepared.pod_slots,
            phase="waiting", priority=priority, created_at=now, renewed_at=now,
        ).returning(NebiusPoolRequest))).one()
    else:
        if (row.priority != priority or resources(row) != prepared.resources
                or row.namespace_uid != prepared.namespace_uid):
            raise PoolAdmissionError
        await session.execute(update(NebiusPoolRequest).where(NebiusPoolRequest.request_id == row.request_id).values(renewed_at=now))
    active = list((await session.scalars(select(NebiusPoolRequest).where(
        NebiusPoolRequest.pool_id.in_(capacities), NebiusPoolRequest.phase.in_(CHARGED_PHASES),
    ).order_by(NebiusPoolRequest.request_id).limit(200_001))).all())
    waiting = list((await session.scalars(select(NebiusPoolRequest).where(
        NebiusPoolRequest.pool_id.in_(capacities), NebiusPoolRequest.phase == "waiting",
        NebiusPoolRequest.renewed_at >= now - timedelta(seconds=120), NebiusPoolRequest.deadline_at > now,
    ).order_by(NebiusPoolRequest.priority, NebiusPoolRequest.created_at, NebiusPoolRequest.request_id).limit(10_001))).all())
    if len(active) > 200_000 or len(waiting) > 10_000:
        raise PoolAdmissionError
    recent_grants = {key: count for key, count in (await session.execute(select(NebiusPoolRequest.pool_id, func.count()).where(
        NebiusPoolRequest.pool_id.in_(capacities), NebiusPoolRequest.granted_at >= now - timedelta(minutes=1),
    ).group_by(NebiusPoolRequest.pool_id))).all()}
    participants = {value.participant_id: value for capacity in capacities.values() for value in capacity.participants}
    active_participants = set((await session.scalars(select(NebiusPoolParticipant.participant_id).where(
        NebiusPoolParticipant.pool_id.in_(capacities), NebiusPoolParticipant.phase == "active",
    ))).all())
    protected_waits: list[NebiusPoolRequest] = []
    for candidate in waiting:
        if candidate.request_id != row.request_id:
            # Waiters are promises only while renewed and currently realizable.
            # A stale binding, unsupported renderer or impossible shape cannot
            # protect phantom capacity. Charged work is NEVER filtered this way.
            try:
                binding = participants[candidate.participant_id]
                if candidate.participant_id not in active_participants or capacities[candidate.pool_id].pool.mode != "global":
                    continue
                prior = PoolExecutionPrepareV1.model_validate(candidate.request_json)
                measured = prepare_pool_execution(prior, participant=binding,
                    profile=profiles[binding.target(prior.target_id, prior.key.workload_kind).profile_id],
                    reservation_id=candidate.request_id, now=now)
                if measured.request_sha256 != candidate.request_sha256 or measured.resources != resources(candidate):
                    continue
                if measured.job["spec"]["template"]["spec"]["nodeSelector"] != capacities[candidate.pool_id].pool.binding_json["node_selector"]:
                    continue
            except (ValueError, KeyError, TypeError):
                continue
        try:
            require_fit(capacities, candidate=candidate, active=active,
                        protected_waits=protected_waits, recent_grants=recent_grants)
        except PlacementUnavailableError as error:
            if candidate.request_id == row.request_id:
                return PoolWaitingV1(request_key=request.key, pool_id=request.pool_id,
                                     request_sha256=request_sha, reason=str(error))
            continue
        if candidate.request_id == row.request_id:
            granted = (await session.scalars(update(NebiusPoolRequest).where(
                NebiusPoolRequest.request_id == row.request_id,
            ).values(phase="reserved", granted_at=now).returning(NebiusPoolRequest).execution_options(populate_existing=True))).one()
            return _receipt(granted)
        protected_waits.append(candidate)
    raise PoolAdmissionError  # A fresh, non-expired request must have been visited.
