"""Freeze activation or cancel unstarted work; caller owns commit, no external I/O.

An activation is a durable intent, not permission for a second uncertain create.
Only the separate fenced gateway may execute its exact retained documents. This
module cannot release started work or assert environment-owned output drain.
"""
from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime
from uuid import uuid4

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.nebius_pool_schema import (
    NebiusPoolBinding,
    NebiusPoolCancellation,
    NebiusPoolParticipant,
    NebiusPoolRequest,
)
from loom.nebius_pool_application_image import PoolApplicationImagePrepareV1
from loom.nebius_pool_contract import (
    PoolActivationV1,
    PoolParticipantV1,
    PoolReceiptV1,
    PoolRequestActionV1,
)
from loom_service.pool_management.auth import PoolPrincipal, authorize_pool_machine
from loom_service.pool_management.capacity import digest, resources
from loom_service.pool_management.early_cancellation import read_early_cancellation
from loom_service.pool_management.locks import acquire_pool_mutation_lock
from loom_service.pool_management.origin import qualify_pool_origin
from loom_service.pool_management.registry import (
    _WORKLOAD,
    PoolProfiles,
    PoolWaitingV1,
    _receipt,
    _render,
)
from loom_service.pool_management.task_images import PreparedPoolTaskImage


class PoolControlError(ValueError):
    def __init__(self, reason: str = "pool_control_unavailable") -> None:
        super().__init__(reason)


async def _clock(session: AsyncSession) -> datetime:
    return (await session.execute(select(func.clock_timestamp()))).scalar_one()  # type: ignore[no-any-return]


@asynccontextmanager
async def _locked_request(session: AsyncSession, principal: PoolPrincipal, action: PoolRequestActionV1, *,
                          allow_absent: bool = False) -> AsyncIterator[
    tuple[NebiusPoolRequest | None, NebiusPoolBinding],
]:
    try:
        with session.no_autoflush:
            action = PoolRequestActionV1.model_validate(action.model_dump())
            if (any(isinstance(row, (NebiusPoolBinding, NebiusPoolParticipant, NebiusPoolRequest, NebiusPoolCancellation))
                    for row in session.new | session.dirty | session.deleted)
                    or (action.pool_id, action.request_key.participant_id) != (principal.pool_id, principal.participant_id)):
                raise PoolControlError
            await acquire_pool_mutation_lock(session)
            pool = (await session.scalars(select(NebiusPoolBinding).where(
                NebiusPoolBinding.pool_id == action.pool_id,
            ).with_for_update().execution_options(populate_existing=True))).one_or_none()
            await authorize_pool_machine(session, principal, role="participant", pool_id=action.pool_id,
                                         participant_id=action.request_key.participant_id,
                                         workload_kind=action.request_key.workload_kind)
            key = action.request_key
            row = (await session.scalars(select(NebiusPoolRequest).where(
                NebiusPoolRequest.participant_id == key.participant_id,
                NebiusPoolRequest.workload_kind == key.workload_kind,
                NebiusPoolRequest.local_work_id == key.local_work_id,
                NebiusPoolRequest.generation == key.generation,
            ).with_for_update().execution_options(populate_existing=True))).one_or_none()
            if (pool is None or (row is None and not allow_absent)
                    or (row is not None and (row.pool_id != pool.pool_id
                        or row.request_sha256 != action.request_sha256 or row.admission_epoch != action.admission_epoch
                        or digest(row.request_json) != row.request_sha256))):
                raise PoolControlError
            yield row, pool
    except PoolControlError:
        raise
    except (ValueError, KeyError, TypeError):
        raise PoolControlError from None


async def pool_request_status(session: AsyncSession, principal: PoolPrincipal,
                              action: PoolRequestActionV1) -> PoolReceiptV1 | PoolWaitingV1:
    async with _locked_request(session, principal, action, allow_absent=True) as (row, _):
        if row is None:
            cancelled = await read_early_cancellation(session, action)
            if cancelled is None:
                raise PoolControlError
            return cancelled
        if row.phase == "waiting":
            return PoolWaitingV1(request_key=action.request_key, pool_id=row.pool_id,
                                 request_sha256=row.request_sha256, reason="pool_request_waiting")
        return _receipt(row)


async def cancel_unstarted_pool_request(session: AsyncSession, principal: PoolPrincipal,
                                        action: PoolRequestActionV1) -> PoolReceiptV1:
    async with _locked_request(session, principal, action, allow_absent=True) as (row, pool):
        if row is None:
            cancelled = await read_early_cancellation(session, action)
            if cancelled is not None:
                return cancelled
            # Closed/fenced intake does not prohibit terminal recovery. Never
            # pre-cancel an as-yet-uninstalled future admission epoch, though.
            if action.admission_epoch > pool.admission_epoch:
                raise PoolControlError
            key = action.request_key
            identity = uuid4()
            session.add(NebiusPoolCancellation(cancellation_id=identity, pool_id=action.pool_id,
                participant_id=key.participant_id, workload_kind=key.workload_kind,
                local_work_id=key.local_work_id, generation=key.generation,
                admission_epoch=action.admission_epoch, request_sha256=action.request_sha256))
            await session.flush()
            return PoolReceiptV1(reservation_id=identity, pool_id=action.pool_id, request_key=key,
                admission_epoch=action.admission_epoch, request_sha256=action.request_sha256, phase="cancelled_unstarted")
        if row.phase == "cancelled_unstarted":
            return _receipt(row)
        if row.phase not in {"waiting", "reserved"}:
            raise PoolControlError("pool_request_already_started")
        cancelled_row = (await session.scalars(update(NebiusPoolRequest).where(
            NebiusPoolRequest.request_id == row.request_id,
        ).values(phase="cancelled_unstarted").returning(NebiusPoolRequest).execution_options(populate_existing=True))).one()
        return _receipt(cancelled_row)


async def activate_pool_request(session: AsyncSession, principal: PoolPrincipal, activation: PoolActivationV1, *,
                                profiles: PoolProfiles) -> PoolReceiptV1:
    activation = PoolActivationV1.model_validate_json(activation.model_dump_json())
    action = activation.action
    async with _locked_request(session, principal, action) as (row, pool):
        assert row is not None  # Activation never accepts a pre-prepare tombstone.
        if row.phase in {"waiting", "cancelled_unstarted"}:
            raise PoolControlError("pool_request_not_reserved")
        if row.phase != "reserved":
            # Retained intent is idempotent readback, never another render or
            # permission to repeat an uncertain external write.
            if (row.plan_json is None or digest(row.plan_json) != row.plan_sha256
                    or row.plan_json.get("activation") != activation.model_dump(mode="json")):
                raise PoolControlError
            return _receipt(row)
        if pool.mode != "global" or row.admission_epoch != pool.admission_epoch or digest(pool.binding_json) != pool.binding_sha256:
            raise PoolControlError
        request = _WORKLOAD.validate_python(row.request_json)
        priority = await qualify_pool_origin(session, principal, request.origin,
            target_id=request.target_id, workload_kind=request.key.workload_kind,
            application_build=request if isinstance(request, PoolApplicationImagePrepareV1) else None)
        registered = await session.get(NebiusPoolParticipant, row.participant_id)
        if registered is None or priority != row.priority:
            raise PoolControlError
        participant = PoolParticipantV1.model_validate(registered.binding_json)
        now = await _clock(session)
        if not now < activation.not_after <= row.deadline_at:
            raise PoolControlError("pool_activation_expired")
        prepared = _render(request, participant, profiles, row.request_id, now)
        if (prepared.request_sha256 != row.request_sha256 or prepared.resources != resources(row)
                or prepared.pod_slots != row.pod_slots or prepared.namespace_uid != row.namespace_uid
                or prepared.job["spec"]["template"]["spec"]["nodeSelector"] != pool.binding_json["node_selector"]):
            raise PoolControlError
        plan = {"schema_version": "loom.pool-workload-plan.v1", "job": prepared.job,
                "configmap": prepared.configmap if isinstance(prepared, PreparedPoolTaskImage) else None,
                "deadline_at": row.deadline_at.isoformat(), "request_sha256": row.request_sha256,
                "activation": activation.model_dump(mode="json")}
        activated = (await session.scalars(update(NebiusPoolRequest).where(
            NebiusPoolRequest.request_id == row.request_id,
            # Rendering/locks may outlive the local consent. Recheck at the
            # actual durable write; no HTTP or Kubernetes I/O occurs here.
            func.clock_timestamp() < activation.not_after,
        ).values(phase="create_intent", plan_json=plan, plan_sha256=digest(plan))
            .returning(NebiusPoolRequest).execution_options(populate_existing=True))).one_or_none()
        if activated is None:
            raise PoolControlError("pool_activation_expired")
        return _receipt(activated)
