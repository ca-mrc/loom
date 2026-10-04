"""Management-only machine API. No ordinary bearer or raw-manifest authority."""
from __future__ import annotations

from typing import Literal
from uuid import UUID

from fastapi import APIRouter, HTTPException, Request, Response
from pydantic import BaseModel, ConfigDict
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from loom.nebius_pool_allocation import PoolNodeAllocationRequestV1, PoolNodeAllocationV1
from loom.nebius_pool_application_image import PoolApplicationImagePrepareV1
from loom.nebius_pool_contract import (
    MAX_POOL_REQUEST_BYTES,
    PoolActivationV1,
    PoolReceiptV1,
    PoolRequestActionV1,
    PoolWaitingV1,
)
from loom.nebius_pool_execution_runtime import PoolExecutionRuntimeV1
from loom.nebius_pool_lifecycle import PoolDrainV1, PoolStopV1
from loom.nebius_pool_native_runtime import PoolNativeRuntimeV1
from loom.nebius_pool_workload import PoolExecutionPrepareV1
from loom_execution_capacity_collector.pool_contracts import (
    MAX_POOL_OBSERVATION_BYTES,
    PoolCaptureV1,
    PoolObservationReceiptV1,
    PoolObservationV1,
)
from loom_service.pool_management.allocation import node_allocation
from loom_service.pool_management.auth import PoolAuthenticationError, resolve_pool_machine
from loom_service.pool_management.control import (
    PoolControlError,
    activate_pool_request,
    cancel_unstarted_pool_request,
    pool_request_status,
)
from loom_service.pool_management.execution_runtime import execution_runtime
from loom_service.pool_management.lifecycle import drain_pool_request, stop_pool_request
from loom_service.pool_management.native_runtime import native_build_runtime
from loom_service.pool_management.observations import (
    PoolObservationError,
    issue_pool_capture,
    record_pool_observation,
)
from loom_service.pool_management.registry import (
    _WORKLOAD,
    PoolAdmissionError,
    PoolProfiles,
    prepare_application_image,
    prepare_execution,
    prepare_task_image,
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


async def _participant(request: Request, pool_id: UUID,
                       operation: Literal["prepare", "status", "activate", "cancel-unstarted", "stop", "drain", "native-runtime", "execution-runtime", "node-allocation"]) -> Response:
    body = await request.body()
    if len(body) > MAX_POOL_REQUEST_BYTES:
        raise _error(413, "pool_request_too_large")
    if request.headers.get("content-type", "").split(";", 1)[0].strip().lower() != "application/json":
        raise _error(415, "pool_json_required")
    factory: async_sessionmaker[AsyncSession] = request.app.state.session_factory
    try:
        async with factory.begin() as session:
            principal = await resolve_pool_machine(session, request.headers.get("authorization"))
            if principal is None:
                raise _error(401, "pool_machine_authority_unavailable")
            if principal.pool_id != pool_id or principal.role != "participant" or principal.participant_id is None:
                raise _error(403, "pool_participant_scope_unavailable")
            result: PoolReceiptV1 | PoolWaitingV1 | PoolNativeRuntimeV1 | PoolExecutionRuntimeV1 | PoolNodeAllocationV1
            profiles = getattr(request.app.state, "pool_profiles", None)
            if operation == "node-allocation":
                allocation_request = PoolNodeAllocationRequestV1.model_validate_json(body)
                if allocation_request.pool_id != pool_id or allocation_request.participant_id != principal.participant_id:
                    raise _error(403, "pool_participant_scope_unavailable")
                result = await node_allocation(session, principal, allocation_request)
            elif operation == "prepare":
                workload = _WORKLOAD.validate_json(body)
                if workload.pool_id != pool_id or workload.key.participant_id != principal.participant_id:
                    raise _error(403, "pool_participant_scope_unavailable")
                if not isinstance(profiles, PoolProfiles):
                    raise _error(503, "pool_profiles_unavailable")
                if isinstance(workload, PoolExecutionPrepareV1):
                    result = await prepare_execution(session, principal, workload, profiles=profiles)
                elif isinstance(workload, PoolApplicationImagePrepareV1):
                    result = await prepare_application_image(session, principal, workload, profiles=profiles)
                else:
                    result = await prepare_task_image(session, principal, workload, profiles=profiles)
            elif operation in {"stop", "drain"}:
                lifecycle: PoolStopV1 | PoolDrainV1 = (PoolStopV1.model_validate_json(body) if operation == "stop"
                    else PoolDrainV1.model_validate_json(body))
                if lifecycle.action.pool_id != pool_id or lifecycle.action.request_key.participant_id != principal.participant_id:
                    raise _error(403, "pool_participant_scope_unavailable")
                result = (await stop_pool_request(session, principal, lifecycle) if isinstance(lifecycle, PoolStopV1)
                    else await drain_pool_request(session, principal, lifecycle))
            else:
                activation = PoolActivationV1.model_validate_json(body) if operation == "activate" else None
                action = activation.action if activation is not None else PoolRequestActionV1.model_validate_json(body)
                if action.pool_id != pool_id or action.request_key.participant_id != principal.participant_id:
                    raise _error(403, "pool_participant_scope_unavailable")
                if operation == "status":
                    result = await pool_request_status(session, principal, action)
                elif operation == "native-runtime":
                    result = await native_build_runtime(session, principal, action)
                elif operation == "execution-runtime":
                    result = await execution_runtime(session, principal, action)
                elif operation == "cancel-unstarted":
                    result = await cancel_unstarted_pool_request(session, principal, action)
                else:
                    if not isinstance(profiles, PoolProfiles):
                        raise _error(503, "pool_profiles_unavailable")
                    assert activation is not None
                    result = await activate_pool_request(session, principal, activation, profiles=profiles)
            encoded = result.model_dump_json().encode()
            if len(encoded) > MAX_POOL_REQUEST_BYTES:
                raise _error(503, "pool_response_too_large")
        # Context exit committed before returning a receipt, including on replay.
        return Response(encoded, media_type="application/json", headers={"Cache-Control": "no-store"})
    except PoolAuthenticationError:
        raise _error(401, "pool_machine_authority_unavailable") from None
    except (PoolAdmissionError, PoolControlError):
        raise _error(409, "pool_request_unavailable") from None
    except ValueError:
        raise _error(422, "pool_request_invalid") from None


@router.post("/{pool_id}/prepare")
async def prepare(request: Request, pool_id: UUID) -> Response:
    return await _participant(request, pool_id, "prepare")


@router.post("/{pool_id}/node-allocation")
async def allocation(request: Request, pool_id: UUID) -> Response:
    return await _participant(request, pool_id, "node-allocation")


@router.post("/{pool_id}/status")
async def status(request: Request, pool_id: UUID) -> Response:
    return await _participant(request, pool_id, "status")


@router.post("/{pool_id}/activate")
async def activate(request: Request, pool_id: UUID) -> Response:
    return await _participant(request, pool_id, "activate")


@router.post("/{pool_id}/cancel-unstarted")
async def cancel_unstarted(request: Request, pool_id: UUID) -> Response:
    return await _participant(request, pool_id, "cancel-unstarted")


@router.post("/{pool_id}/stop")
async def stop(request: Request, pool_id: UUID) -> Response:
    return await _participant(request, pool_id, "stop")


@router.post("/{pool_id}/drain")
async def drain(request: Request, pool_id: UUID) -> Response:
    return await _participant(request, pool_id, "drain")


@router.post("/{pool_id}/native-runtime")
async def native_runtime(request: Request, pool_id: UUID) -> Response:
    return await _participant(request, pool_id, "native-runtime")


@router.post("/{pool_id}/execution-runtime")
async def read_execution_runtime(request: Request, pool_id: UUID) -> Response:
    return await _participant(request, pool_id, "execution-runtime")
