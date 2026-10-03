"""Retain stop then output drain, with caller-owned transactions and no I/O."""
from __future__ import annotations

from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.nebius_pool_schema import NebiusPoolRequest
from loom.nebius_pool_application_image import PoolApplicationImagePrepareV1
from loom.nebius_pool_contract import PoolReceiptV1
from loom.nebius_pool_lifecycle import PoolDrainV1, PoolStopV1
from loom.nebius_pool_task_image import PoolTaskImagePrepareV1
from loom_service.pool_management.auth import PoolPrincipal
from loom_service.pool_management.capacity import digest
from loom_service.pool_management.control import PoolControlError, _clock, _locked_request
from loom_service.pool_management.registry import _WORKLOAD, _receipt


def _qualified(row: NebiusPoolRequest | None, body: PoolStopV1 | PoolDrainV1) -> NebiusPoolRequest:
    if (row is None or row.phase not in {"create_intent", "observed", "cleanup_intent", "released"}
            or row.request_id != body.reservation_id or row.plan_sha256 != body.plan_sha256
            or row.plan_json is None or digest(row.plan_json) != row.plan_sha256):
        raise PoolControlError
    request = _WORKLOAD.validate_python(row.request_json)
    if isinstance(request, PoolTaskImagePrepareV1):
        generation = request.build.expected_lease_epoch + 1
    elif isinstance(request, PoolApplicationImagePrepareV1):
        generation = request.build.attempt
    else:
        generation = request.execution.lease_generation
    if body.lease_generation != generation:
        raise PoolControlError
    return row


async def stop_pool_request(session: AsyncSession, principal: PoolPrincipal, body: PoolStopV1) -> PoolReceiptV1:
    body = PoolStopV1.model_validate_json(body.model_dump_json())
    async with _locked_request(session, principal, body.action) as (selected, _):
        row = _qualified(selected, body)
        payload = body.model_dump(mode="json")
        if row.stop_json is not None:
            if row.stop_json.get("request") != payload or row.stop_json.get("request_sha256") != digest(payload):
                raise PoolControlError
            return _receipt(row)
        if row.phase == "released":
            raise PoolControlError
        assert row.plan_json is not None
        maximum = row.plan_json["job"]["spec"]["template"]["spec"]["terminationGracePeriodSeconds"]
        if type(maximum) is not int or not 0 <= maximum <= 300:
            raise PoolControlError
        now = await _clock(session)
        grace = max(0, min(maximum, int((body.grace_deadline_at - now).total_seconds())))
        stopped = (await session.scalars(update(NebiusPoolRequest).where(NebiusPoolRequest.request_id == row.request_id)
            .values(phase="cleanup_intent", stop_json={"request": payload, "request_sha256": digest(payload), "grace_seconds": grace})
            .returning(NebiusPoolRequest).execution_options(populate_existing=True))).one()
        return _receipt(stopped)


async def drain_pool_request(session: AsyncSession, principal: PoolPrincipal, body: PoolDrainV1) -> PoolReceiptV1:
    body = PoolDrainV1.model_validate_json(body.model_dump_json())
    async with _locked_request(session, principal, body.action) as (selected, _):
        row = _qualified(selected, body)
        if (row.phase not in {"cleanup_intent", "released"} or row.stop_json is None
                or row.stop_json.get("request_sha256") != body.stop_sha256
                or digest(row.stop_json["request"]) != body.stop_sha256
                or (row.workload_kind in {"task_image_build", "application_image_build"}
                    and body.output_generation != body.lease_generation)):
            raise PoolControlError
        payload = body.model_dump(mode="json")
        if row.drain_json is not None:
            if row.drain_json != payload:
                raise PoolControlError
            return _receipt(row)
        if row.phase == "released":
            raise PoolControlError
        drained = (await session.scalars(update(NebiusPoolRequest).where(NebiusPoolRequest.request_id == row.request_id)
            .values(drain_json=payload).returning(NebiusPoolRequest).execution_options(populate_existing=True))).one()
        return _receipt(drained)
