"""Management-only observer API. No ordinary bearer fallback or dispatch path."""
from __future__ import annotations

from typing import Literal
from uuid import UUID

from fastapi import APIRouter, HTTPException, Request, Response
from pydantic import BaseModel, ConfigDict
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from loom_execution_capacity_collector.pool_contracts import (
    MAX_POOL_OBSERVATION_BYTES,
    PoolCaptureV1,
    PoolObservationReceiptV1,
    PoolObservationV1,
)
from loom_service.pool_management.auth import resolve_pool_machine
from loom_service.pool_management.observations import (
    PoolObservationError,
    issue_pool_capture,
    record_pool_observation,
)

router = APIRouter(prefix="/internal/pools/v1", include_in_schema=False)


class _EmptyCaptureRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")


def _error(status: int, code: str) -> HTTPException:
    return HTTPException(status_code=status, detail=code, headers={"Cache-Control": "no-store"})


async def _observe(request: Request, pool_id: UUID, operation: Literal["capture", "observation"]) -> Response:
    # The management middleware bounds/receives before SQL. Parse only after
    # dedicated authentication, and never echo validation inputs in a response.
    body = await request.body()
    if len(body) > MAX_POOL_OBSERVATION_BYTES:
        raise _error(413, "pool_request_too_large")
    if request.headers.get("content-type", "").split(";", 1)[0].strip().lower() != "application/json":
        raise _error(415, "pool_json_required")
    factory: async_sessionmaker[AsyncSession] = request.app.state.session_factory
    try:
        async with factory.begin() as session:
            principal = await resolve_pool_machine(session, request.headers.get("authorization"))
            if principal is None:
                raise _error(401, "pool_machine_authority_unavailable")
            if principal.pool_id != pool_id or principal.role != "observer" or principal.participant_id is not None:
                raise _error(403, "pool_observer_scope_unavailable")
            result: PoolCaptureV1 | PoolObservationReceiptV1
            if operation == "capture":
                _EmptyCaptureRequest.model_validate_json(body)
                capture = await issue_pool_capture(session, principal)
                result = PoolCaptureV1(capture_id=capture.capture_id, pool_id=capture.pool_id,
                    admission_epoch=capture.admission_epoch, registration_sha256=capture.registration_sha256,
                    scope=capture.scope, created_at=capture.created_at)
            else:
                observation = PoolObservationV1.model_validate_json(body)
                recorded = await record_pool_observation(session, principal, capture_id=observation.capture_id,
                    observed_at=observation.observed_at, provider=observation.provider, kubernetes=observation.kubernetes)
                result = PoolObservationReceiptV1(observation_id=recorded.observation_id,
                    capture_id=recorded.capture_id, observation_sha256=recorded.observation_sha256)
            encoded = result.model_dump_json().encode()
            if len(encoded) > MAX_POOL_OBSERVATION_BYTES:
                # Fail before commit; do not retain an undeliverable new capture.
                raise _error(503, "pool_response_too_large")
        return Response(encoded, media_type="application/json", headers={"Cache-Control": "no-store"})
    except PoolObservationError:
        raise _error(409, "pool_observation_unavailable") from None
    except ValueError:
        raise _error(422, "pool_request_invalid") from None


@router.post("/{pool_id}/captures")
async def issue_capture(request: Request, pool_id: UUID) -> Response:
    return await _observe(request, pool_id, "capture")


@router.post("/{pool_id}/observations")
async def publish_observation(request: Request, pool_id: UUID) -> Response:
    return await _observe(request, pool_id, "observation")
