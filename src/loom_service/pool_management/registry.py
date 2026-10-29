"""Atomic execution/build preparation. Caller owns commit; no external operation runs.

Admission is internal until the production observer, gateway and local outboxes
are connected. A prepare never creates a Job or releases an outstanding grant.
"""
from __future__ import annotations

import time
from collections import OrderedDict
from datetime import datetime, timedelta
from typing import Annotated
from uuid import UUID, uuid4

from pydantic import Field, TypeAdapter
from sqlalchemy import func, insert, select, tuple_, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import load_only

from loom.db.nebius_pool_schema import (
    NebiusPoolBinding,
    NebiusPoolCancellation,
    NebiusPoolCapture,
    NebiusPoolObservation,
    NebiusPoolParticipant,
    NebiusPoolRequest,
)
from loom.nebius_pool_application_image import PoolApplicationImagePrepareV1
from loom.nebius_pool_contract import PoolParticipantV1, PoolReceiptV1, PoolRequestActionV1
from loom.nebius_pool_contract import PoolWaitingV1 as PoolWaitingV1
from loom.nebius_pool_task_image import PoolTaskImagePrepareV1
from loom.nebius_pool_workload import PoolExecutionPrepareV1
from loom_control_plane.execution_placement import PlacementUnavailableError
from loom_service.pool_management.admission_metrics import AdmissionWork
from loom_service.pool_management.application_images import (
    prepare_pool_application_image,
)
from loom_service.pool_management.auth import PoolPrincipal, authorize_pool_machine
from loom_service.pool_management.capacity import (
    CHARGED_PHASES,
    demand_id,
    digest,
    read_connected_capacity,
    require_fit,
    require_policy_fit,
    resources,
)
from loom_service.pool_management.early_cancellation import read_early_cancellation
from loom_service.pool_management.intake import intake_open
from loom_service.pool_management.locks import acquire_pool_mutation_lock
from loom_service.pool_management.origin import qualify_pool_origin, qualify_retained_pool_origin
from loom_service.pool_management.profile_registry import resolve_profiles
from loom_service.pool_management.profile_types import PoolProfiles as PoolProfiles
from loom_service.pool_management.render import (
    PreparedPoolExecution,
    prepare_pool_execution,
)
from loom_service.pool_management.task_images import (
    PreparedPoolTaskImage,
    prepare_pool_task_image,
)

PoolPrepareWorkload = PoolExecutionPrepareV1 | PoolTaskImagePrepareV1 | PoolApplicationImagePrepareV1
_WORKLOAD: TypeAdapter[PoolPrepareWorkload] = TypeAdapter(Annotated[PoolPrepareWorkload, Field(discriminator="schema_version")])


def _render(request: PoolPrepareWorkload, participant: PoolParticipantV1, profiles: PoolProfiles,
            reservation_id: UUID, now: datetime) -> PreparedPoolExecution | PreparedPoolTaskImage:
    profile_id = participant.target(request.target_id, request.key.workload_kind).profile_id
    if isinstance(request, PoolExecutionPrepareV1):
        return prepare_pool_execution(request, participant=participant, profile=profiles.execution[profile_id],
                                      reservation_id=reservation_id, now=now)
    if isinstance(request, PoolApplicationImagePrepareV1):
        return prepare_pool_application_image(request, participant=participant, profile=profiles.application_images[profile_id],
                                               reservation_id=reservation_id, now=now)
    return prepare_pool_task_image(request, participant=participant, profile=profiles.task_images[profile_id],
                                   reservation_id=reservation_id, now=now)


class PoolAdmissionError(ValueError):
    def __init__(self, reason: str = "pool_admission_unavailable") -> None:
        super().__init__(reason)


def _receipt(row: NebiusPoolRequest) -> PoolReceiptV1:
    return PoolReceiptV1.model_validate({
        "reservation_id": row.request_id, "pool_id": row.pool_id,
        "request_key": {"participant_id": row.participant_id, "workload_kind": row.workload_kind,
                        "local_work_id": row.local_work_id, "generation": row.generation},
        "admission_epoch": row.admission_epoch, "request_sha256": row.request_sha256, "phase": row.phase,
        "plan_sha256": row.plan_sha256, "job_uid": row.job_uid, "cleanup_observation_id": row.cleanup_observation_id,
    })


async def prepare_execution(session: AsyncSession, principal: PoolPrincipal, request: PoolExecutionPrepareV1, *,
                            profiles: PoolProfiles | None = None) -> PoolReceiptV1 | PoolWaitingV1:
    if not isinstance(request, PoolExecutionPrepareV1):
        raise PoolAdmissionError
    return await _prepare(session, principal, request, profiles)


async def prepare_task_image(session: AsyncSession, principal: PoolPrincipal, request: PoolTaskImagePrepareV1, *,
                             profiles: PoolProfiles | None = None) -> PoolReceiptV1 | PoolWaitingV1:
    if not isinstance(request, PoolTaskImagePrepareV1):
        raise PoolAdmissionError
    return await _prepare(session, principal, request, profiles)


async def prepare_application_image(session: AsyncSession, principal: PoolPrincipal, request: PoolApplicationImagePrepareV1, *,
                                    profiles: PoolProfiles | None = None) -> PoolReceiptV1 | PoolWaitingV1:
    if not isinstance(request, PoolApplicationImagePrepareV1):
        raise PoolAdmissionError
    return await _prepare(session, principal, request, profiles)


async def _prepare(session: AsyncSession, principal: PoolPrincipal, request: PoolPrepareWorkload,
                   profiles: PoolProfiles | None) -> PoolReceiptV1 | PoolWaitingV1:
    """Serialize quota/pool/identity/request qualification before a first grant."""
    work = AdmissionWork()
    try:
        with session.no_autoflush:
            return await _prepare_request(session, principal, request, profiles, work=work)
    except PoolAdmissionError:
        raise
    except (ValueError, KeyError, TypeError):
        # Do not leak rendering inputs, credential hashes or database documents.
        raise PoolAdmissionError from None
    finally:
        work.observe()


async def _qualify_verifier_parent(session: AsyncSession, request: PoolExecutionPrepareV1) -> None:
    """A fresh child grant follows the exact parent's committed output and release.

    Frozen sizing evidence is inherited, not renewed. The normal renderer and
    current aggregate admission still measure and charge the child's real Pod.
    """
    parent_id = request.execution.parent_lease_id
    parent = await session.scalar(select(NebiusPoolRequest).where(
        NebiusPoolRequest.pool_id == request.pool_id,
        NebiusPoolRequest.participant_id == request.key.participant_id,
        NebiusPoolRequest.workload_kind == "trial",
        NebiusPoolRequest.local_work_id == parent_id,
        NebiusPoolRequest.generation == 1,
    ))
    if (parent is None or parent.phase != "released" or parent.drain_json is None
            or parent.drain_json.get("output_state") != "committed"):
        raise PoolAdmissionError
    prior = PoolExecutionPrepareV1.model_validate(parent.request_json)
    runtime, original = request.execution.runtime, prior.execution.runtime
    if (prior.target_id != request.target_id or prior.origin != request.origin
            or prior.execution.requirements != request.execution.requirements
            or original.execution_role != "attempt" or original.verifier_execution != "separate_execution"
            or runtime.execution_role != "verifier" or runtime.handoff_input is None
            or (runtime.candidate_sha, runtime.task_revision_sha256, runtime.task_image_ref,
                runtime.runtime_image_ref, runtime.runtime_binary_sha256) != (
                original.candidate_sha, original.task_revision_sha256, original.task_image_ref,
                original.runtime_image_ref, original.runtime_binary_sha256)
            or request.execution.node_allocation != prior.execution.node_allocation
            or runtime.node_resource_allocation != original.node_resource_allocation):
        raise PoolAdmissionError


async def _prepare_request(session: AsyncSession, principal: PoolPrincipal, request: PoolPrepareWorkload,
                           profiles: PoolProfiles | None, *, work: AdmissionWork) -> PoolReceiptV1 | PoolWaitingV1:
    request = _WORKLOAD.validate_json(request.model_dump_json())
    if any(isinstance(row, (NebiusPoolBinding, NebiusPoolParticipant, NebiusPoolRequest, NebiusPoolCancellation,
                           NebiusPoolCapture, NebiusPoolObservation))
           for row in session.new | session.dirty | session.deleted):
        raise PoolAdmissionError
    if (request.pool_id, request.key.participant_id) != (principal.pool_id, principal.participant_id):
        raise PoolAdmissionError
    waiting_for_lock = time.monotonic()
    await acquire_pool_mutation_lock(session)
    work.locked_at = time.monotonic()
    work.lock_wait_seconds = work.locked_at - waiting_for_lock
    # All writers take the global lock first, then physical rows, then auth and
    # request rows. It also serializes intersecting provider-quota domains.
    pool = (await session.scalars(select(NebiusPoolBinding).where(
        NebiusPoolBinding.pool_id == principal.pool_id,
    ).with_for_update().execution_options(populate_existing=True))).one_or_none()
    await authorize_pool_machine(session, principal, role="participant",
                                 pool_id=request.pool_id, participant_id=request.key.participant_id,
                                 workload_kind=request.key.workload_kind)
    request_json = request.model_dump(mode="json")
    request_sha = digest(request_json)
    cancelled = await read_early_cancellation(session, PoolRequestActionV1(pool_id=request.pool_id,
        request_key=request.key, admission_epoch=request.admission_epoch, request_sha256=request_sha))
    if cancelled is not None:
        return cancelled
    row = (await session.scalars(select(NebiusPoolRequest).where(
        NebiusPoolRequest.participant_id == request.key.participant_id,
        NebiusPoolRequest.workload_kind == request.key.workload_kind,
        NebiusPoolRequest.local_work_id == request.key.local_work_id,
        NebiusPoolRequest.generation == request.key.generation,
    ).with_for_update().execution_options(populate_existing=True))).one_or_none()
    if row is not None:
        if row.request_sha256 != request_sha or row.request_json != request_json:
            raise PoolAdmissionError("pool_request_conflict")
        if row.phase != "waiting":
            # Idempotent readback is not new admission. Never rerender, renew a
            # deadline or make a lost reply depend on still-available capacity.
            return _receipt(row)
    if pool is None or pool.mode != "global":
        raise PoolAdmissionError
    if isinstance(request, PoolExecutionPrepareV1) and request.key.workload_kind == "verifier":
        await _qualify_verifier_parent(session, request)
    priority = await qualify_pool_origin(session, principal, request.origin,
        target_id=request.target_id, workload_kind=request.key.workload_kind,
        application_build=request if isinstance(request, PoolApplicationImagePrepareV1) else None,
        retained_revision=request.participant_revision if row is not None else None)
    now = (await session.execute(select(func.clock_timestamp()))).scalar_one()
    capacities = await read_connected_capacity(session, pool.pool_id, now)
    participant = next(value for value in capacities[pool.pool_id].participants
                       if value.participant_id == request.key.participant_id)
    reservation_id = row.request_id if row is not None else uuid4()
    work.operations["profile_resolutions"] += 1
    original, selected_profiles = await resolve_profiles(session, pool, participant, request.participant_revision,
        retained=row is not None, fallback=profiles)
    work.operations["renders"] += 1
    prepared = _render(request, original, selected_profiles, reservation_id, now)
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
    # Charged work is never omitted, even when its caller is fenced or expired.
    # Placement needs resource/identity columns, not every retained workload JSON.
    envelope = (NebiusPoolRequest.request_id, NebiusPoolRequest.pool_id,
        NebiusPoolRequest.participant_id, NebiusPoolRequest.workload_kind,
        NebiusPoolRequest.cpu_millis, NebiusPoolRequest.memory_mib,
        NebiusPoolRequest.ephemeral_storage_mib, NebiusPoolRequest.pod_slots,
        NebiusPoolRequest.priority, NebiusPoolRequest.created_at)
    active = list((await session.scalars(select(NebiusPoolRequest).options(load_only(*envelope)).where(
        NebiusPoolRequest.pool_id.in_(capacities), NebiusPoolRequest.phase.in_(CHARGED_PHASES),
    ).order_by(NebiusPoolRequest.request_id).limit(200_001))).all())
    if len(active) > 200_000:
        # Keep accepted demand and its age; the participant retries this exact
        # durable request. Never hide charged work to make a reservation fit.
        return PoolWaitingV1(request_key=request.key, pool_id=request.pool_id,
            request_sha256=request_sha, reason="pool_admission_charged_backpressure")
    recent_grants = {key: count for key, count in (await session.execute(select(NebiusPoolRequest.pool_id, func.count()).where(
        NebiusPoolRequest.pool_id.in_(capacities), NebiusPoolRequest.granted_at >= now - timedelta(minutes=1),
    ).group_by(NebiusPoolRequest.pool_id))).all()}
    try:
        # Saturated count/health budgets cannot become feasible by protecting
        # more waiters. Avoid scanning the backlog for an already-blocked caller.
        work.operations["policy_checks"] += 1
        require_policy_fit(capacities, candidate=row, active=active,
                           protected_waits=[], recent_grants=recent_grants)
    except PlacementUnavailableError as error:
        return PoolWaitingV1(request_key=request.key, pool_id=request.pool_id,
                             request_sha256=request_sha, reason=str(error))
    participants = {value.participant_id: value for capacity in capacities.values() for value in capacity.participants}
    active_rows = (await session.scalars(select(NebiusPoolParticipant).where(
        NebiusPoolParticipant.pool_id.in_(capacities), NebiusPoolParticipant.phase == "active",
    ))).all()
    active_participants = {participant.participant_id for participant in active_rows
        if await intake_open(session, capacities[participant.pool_id].pool, participant)}
    protected_waits: list[NebiusPoolRequest] = []
    # Only an earlier class/age can protect capacity from this caller. Streaming
    # thin envelopes bounds buffering; impossible prefixes are still traversed,
    # rather than imposing a page-size cap that could permanently hide the tail.
    waiting = await session.stream_scalars(select(NebiusPoolRequest).options(load_only(*envelope)).where(
        NebiusPoolRequest.pool_id.in_(capacities), NebiusPoolRequest.phase == "waiting",
        NebiusPoolRequest.renewed_at >= now - timedelta(seconds=120), NebiusPoolRequest.deadline_at > now,
        tuple_(NebiusPoolRequest.priority, NebiusPoolRequest.created_at, NebiusPoolRequest.request_id)
            <= (row.priority, row.created_at, row.request_id),
    ).order_by(NebiusPoolRequest.priority, NebiusPoolRequest.created_at, NebiusPoolRequest.request_id),
        execution_options={"yield_per": 100})
    # These memos live only under this transaction's mutation lock. Capacities,
    # charged work, bindings and profiles cannot change while a result is reused.
    profile_memo = OrderedDict({(pool.pool_id, participant.participant_id, request.participant_revision):
                               (original, selected_profiles)})
    observed_demands = {
        f"{pod.lease_id}:{pod.generation}"
        for capacity in capacities.values()
        for pod in (*capacity.placement.pending_pods,
                    *(pod for node in capacity.placement.nodes for pod in node.managed_pods))
    }
    fit_memo: OrderedDict[tuple[object, ...], str | None] = OrderedDict()
    try:
        async for candidate in waiting:
            work.operations["waiter_envelopes"] += 1
            current = candidate.request_id == row.request_id
            if not current and (candidate.participant_id not in active_participants
                               or capacities[candidate.pool_id].pool.mode != "global"):
                continue
            # A failed resource fit cannot protect capacity, so qualify payloads
            # only for fitting candidates. Equal envelopes have identical fit
            # under this fixed observation/charged set and protection generation.
            fit_key = (candidate.pool_id, candidate.workload_kind, candidate.cpu_millis,
                candidate.memory_mib, candidate.ephemeral_storage_mib, candidate.pod_slots,
                len(protected_waits),
                demand_id(candidate) if demand_id(candidate) in observed_demands else None)
            if fit_key not in fit_memo:
                try:
                    work.operations["fit_evaluations"] += 1
                    require_fit(capacities, candidate=candidate, active=active,
                                protected_waits=protected_waits, recent_grants=recent_grants)
                except PlacementUnavailableError as error:
                    fit_memo[fit_key] = str(error)
                except (ValueError, KeyError, TypeError):
                    if current:
                        raise
                    # Retained invalid envelopes are not eligible promises.
                    # Preserve the old qualification path's skip behavior.
                    continue
                else:
                    fit_memo[fit_key] = None
                if len(fit_memo) > 512:
                    fit_memo.popitem(last=False)
            reason = fit_memo[fit_key]
            if reason is not None:
                if current:
                    return PoolWaitingV1(request_key=request.key, pool_id=request.pool_id,
                                         request_sha256=request_sha, reason=reason)
                continue
            if not current:
                # These fields are deliberately deferred by the envelope query.
                work.operations["payload_reads"] += 1
                await session.refresh(candidate, attribute_names=["request_json", "request_sha256"])
                try:
                    binding = participants[candidate.participant_id]
                    prior = _WORKLOAD.validate_python(candidate.request_json)
                    profile_key = (candidate.pool_id, candidate.participant_id, prior.participant_revision)
                    if profile_key not in profile_memo:
                        work.operations["profile_resolutions"] += 1
                        profile_memo[profile_key] = await resolve_profiles(session, capacities[candidate.pool_id].pool,
                            binding, prior.participant_revision, retained=True, fallback=profiles)
                        if len(profile_memo) > 128:
                            profile_memo.popitem(last=False)
                    original_waiter, waiter_profiles = profile_memo[profile_key]
                    if isinstance(prior, PoolApplicationImagePrepareV1):
                        await qualify_retained_pool_origin(session, prior.origin, participant=original_waiter,
                            cluster_id=capacities[candidate.pool_id].pool.cluster_id,
                            workload_kind=prior.key.workload_kind, lock_history=True, application_build=prior)
                    work.operations["renders"] += 1
                    measured = _render(prior, original_waiter, waiter_profiles, candidate.request_id, now)
                    if measured.request_sha256 != candidate.request_sha256 or measured.resources != resources(candidate):
                        continue
                    if measured.job["spec"]["template"]["spec"]["nodeSelector"] != capacities[candidate.pool_id].pool.binding_json["node_selector"]:
                        continue
                except (ValueError, KeyError, TypeError):
                    continue
            if current:
                granted = (await session.scalars(update(NebiusPoolRequest).where(
                    NebiusPoolRequest.request_id == row.request_id,
                ).values(phase="reserved", granted_at=now).returning(NebiusPoolRequest).execution_options(populate_existing=True))).one()
                return _receipt(granted)
            protected_waits.append(candidate)
            try:
                work.operations["policy_checks"] += 1
                require_policy_fit(capacities, candidate=row, active=active,
                                   protected_waits=protected_waits, recent_grants=recent_grants)
            except PlacementUnavailableError as error:
                # The already-qualified earlier promises saturate a necessary
                # budget; no later candidate can remove them in this snapshot.
                return PoolWaitingV1(request_key=request.key, pool_id=request.pool_id,
                                     request_sha256=request_sha, reason=str(error))
    finally:
        await waiting.close()
    raise PoolAdmissionError  # A fresh, non-expired request must have been visited.
