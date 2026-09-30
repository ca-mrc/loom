"""Freeze activation or cancel unstarted work; caller owns commit, no external I/O.

An activation is a durable intent, not permission for a second uncertain create.
Only the separate fenced gateway may execute its exact retained documents. This
module cannot release started work or assert environment-owned output drain.
"""
from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.nebius_pool_schema import NebiusPoolBinding, NebiusPoolParticipant, NebiusPoolRequest
from loom.nebius_pool_contract import PoolParticipantV1, PoolReceiptV1, PoolRequestActionV1
from loom_service.pool_management.auth import PoolPrincipal, authorize_pool_machine
from loom_service.pool_management.capacity import digest, resources
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
async def _locked_request(session: AsyncSession, principal: PoolPrincipal, action: PoolRequestActionV1) -> AsyncIterator[
    tuple[NebiusPoolRequest, NebiusPoolBinding],
]:
    try:
        with session.no_autoflush:
            action = PoolRequestActionV1.model_validate(action.model_dump())
            if (any(isinstance(row, (NebiusPoolBinding, NebiusPoolParticipant, NebiusPoolRequest))
                    for row in session.new | session.dirty | session.deleted)
                    or (action.pool_id, action.request_key.participant_id) != (principal.pool_id, principal.participant_id)):
                raise PoolControlError
            await acquire_pool_mutation_lock(session)
            pool = (await session.scalars(select(NebiusPoolBinding).where(
                NebiusPoolBinding.pool_id == action.pool_id,
            ).with_for_update().execution_options(populate_existing=True))).one_or_none()
            await authorize_pool_machine(session, principal, role="participant", pool_id=action.pool_id,
                                         participant_id=action.request_key.participant_id)
            key = action.request_key
            row = (await session.scalars(select(NebiusPoolRequest).where(
                NebiusPoolRequest.participant_id == key.participant_id,
                NebiusPoolRequest.workload_kind == key.workload_kind,
                NebiusPoolRequest.local_work_id == key.local_work_id,
                NebiusPoolRequest.generation == key.generation,
            ).with_for_update().execution_options(populate_existing=True))).one_or_none()
            if (row is None or pool is None or row.pool_id != pool.pool_id
                    or row.request_sha256 != action.request_sha256 or row.admission_epoch != action.admission_epoch
                    or digest(row.request_json) != row.request_sha256):
                raise PoolControlError
            yield row, pool
    except PoolControlError:
        raise
    except (ValueError, KeyError, TypeError):
        raise PoolControlError from None


async def pool_request_status(session: AsyncSession, principal: PoolPrincipal,
                              action: PoolRequestActionV1) -> PoolReceiptV1 | PoolWaitingV1:
    async with _locked_request(session, principal, action) as (row, _):
        if row.phase == "waiting":
            return PoolWaitingV1(request_key=action.request_key, pool_id=row.pool_id,
                                 request_sha256=row.request_sha256, reason="pool_request_waiting")
        return _receipt(row)


async def cancel_unstarted_pool_request(session: AsyncSession, principal: PoolPrincipal,
                                        action: PoolRequestActionV1) -> PoolReceiptV1:
    async with _locked_request(session, principal, action) as (row, _):
        if row.phase == "cancelled_unstarted":
            return _receipt(row)
        if row.phase not in {"waiting", "reserved"}:
            raise PoolControlError("pool_request_already_started")
        cancelled = (await session.scalars(update(NebiusPoolRequest).where(
            NebiusPoolRequest.request_id == row.request_id,
        ).values(phase="cancelled_unstarted").returning(NebiusPoolRequest).execution_options(populate_existing=True))).one()
        return _receipt(cancelled)


async def activate_pool_request(session: AsyncSession, principal: PoolPrincipal, action: PoolRequestActionV1, *,
                                profiles: PoolProfiles) -> PoolReceiptV1:
    async with _locked_request(session, principal, action) as (row, pool):
        if row.phase in {"waiting", "cancelled_unstarted"}:
            raise PoolControlError("pool_request_not_reserved")
        if row.phase != "reserved":
            # Retained intent is idempotent readback, never another render or
            # permission to repeat an uncertain external write.
            if row.plan_json is None or digest(row.plan_json) != row.plan_sha256:
                raise PoolControlError
            return _receipt(row)
        if pool.mode != "global" or row.admission_epoch != pool.admission_epoch or digest(pool.binding_json) != pool.binding_sha256:
            raise PoolControlError
        request = _WORKLOAD.validate_python(row.request_json)
        priority = await qualify_pool_origin(session, principal, request.origin,
            target_id=request.target_id, workload_kind=request.key.workload_kind)
        registered = await session.get(NebiusPoolParticipant, row.participant_id)
        if registered is None or priority != row.priority:
            raise PoolControlError
        participant = PoolParticipantV1.model_validate(registered.binding_json)
        now = await _clock(session)
        prepared = _render(request, participant, profiles, row.request_id, now)
        if (prepared.request_sha256 != row.request_sha256 or prepared.resources != resources(row)
                or prepared.pod_slots != row.pod_slots or prepared.namespace_uid != row.namespace_uid
                or prepared.job["spec"]["template"]["spec"]["nodeSelector"] != pool.binding_json["node_selector"]):
            raise PoolControlError
        plan = {"schema_version": "loom.pool-workload-plan.v1", "job": prepared.job,
                "configmap": prepared.configmap if isinstance(prepared, PreparedPoolTaskImage) else None,
                "deadline_at": row.deadline_at.isoformat(), "request_sha256": row.request_sha256}
        activated = (await session.scalars(update(NebiusPoolRequest).where(
            NebiusPoolRequest.request_id == row.request_id,
        ).values(phase="create_intent", plan_json=plan, plan_sha256=digest(plan))
            .returning(NebiusPoolRequest).execution_options(populate_existing=True))).one()
        return _receipt(activated)
