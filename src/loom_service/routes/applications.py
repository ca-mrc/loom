"""Authenticated application-only controls, without caller-supplied authority."""
from __future__ import annotations

from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Header, Request, Response

from loom.nebius_application_contract import (
    ApplicationCreateRequestV1,
    ApplicationOperationRequestV1,
    ApplicationOperationV1,
    ApplicationRegistrationV1,
    ApplicationStatusV1,
)
from loom.nebius_application_evidence import ApplicationOperationEvidenceV1
from loom_service.application_management.login import ApplicationLogin
from loom_service.application_management.manager import ApplicationManager
from loom_service.application_management.operation_evidence import read_operation_evidence
from loom_service.environment_management.registry import ManagementError
from loom_service.routes.environments import ManagementPrincipal

router = APIRouter()
IdempotencyKey = Annotated[str, Header(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9._:-]+$")]


def manager(request: Request) -> ApplicationManager:
    value = getattr(request.app.state, "application_manager", None)
    if not isinstance(value, ApplicationManager):
        raise ManagementError("application_management_not_configured", 503)
    return value


@router.post("/applications", status_code=202)
async def create_application(request: Request, payload: ApplicationCreateRequestV1,
                             principal: ManagementPrincipal, idempotency_key: IdempotencyKey) -> ApplicationOperationV1:
    return await manager(request).create(principal, payload, idempotency_key=idempotency_key)


@router.get("/applications")
async def list_applications(request: Request, principal: ManagementPrincipal) -> dict[str, list[ApplicationRegistrationV1]]:
    return {"items": await manager(request).registry.list_applications(principal=principal)}


@router.get("/applications/{application_id}")
async def application_status(request: Request, application_id: UUID, principal: ManagementPrincipal) -> ApplicationStatusV1:
    return await manager(request).registry.status(application_id, principal=principal)


@router.post("/applications/{application_id}/login")
async def application_login(request: Request, response: Response, application_id: UUID,
                            principal: ManagementPrincipal) -> dict[str, Any]:
    login = getattr(request.app.state, "application_login", None)
    if not isinstance(login, ApplicationLogin):
        raise ManagementError("application_login_not_configured", 503)
    proof = await login.issue(principal, application_id)
    response.headers["Cache-Control"] = "no-store"
    return proof


@router.post("/applications/{application_id}/operations", status_code=202)
async def request_operation(request: Request, application_id: UUID, payload: ApplicationOperationRequestV1,
                            principal: ManagementPrincipal, idempotency_key: IdempotencyKey) -> ApplicationOperationV1:
    return await manager(request).transition(principal, application_id, payload, idempotency_key=idempotency_key)


@router.get("/application-operations/{operation_id}")
async def operation_status(request: Request, operation_id: UUID, principal: ManagementPrincipal) -> ApplicationOperationV1:
    return await manager(request).registry.get_operation(operation_id, principal=principal)


@router.post("/application-operations/{operation_id}/retry", status_code=202)
async def retry_operation(request: Request, operation_id: UUID, principal: ManagementPrincipal) -> ApplicationOperationV1:
    return await manager(request).registry.retry(operation_id, principal=principal)


@router.get("/application-operations/{operation_id}/evidence")
async def operation_evidence(request: Request, response: Response, operation_id: UUID,
                             principal: ManagementPrincipal) -> ApplicationOperationEvidenceV1:
    result = await read_operation_evidence(manager(request).registry.session_factory, operation_id, principal=principal)
    response.headers["Cache-Control"] = "no-store"
    return result
