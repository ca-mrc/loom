"""Authenticated application-only controls, without caller-supplied authority."""
from __future__ import annotations

import asyncio
from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Header, Request, Response

from loom.application_image_build import (
    ApplicationImageBuildAttemptRequestV1,
    ApplicationImageBuildRequestV1,
    ApplicationImageBuildStatusV1,
)
from loom.application_source_upload import (
    ApplicationSourceUploadRequestV1,
    ApplicationSourceUploadV1,
)
from loom.nebius_application_capabilities import (
    ApplicationCapabilitiesV1,
    ApplicationWorkerCapability,
)
from loom.nebius_application_contract import (
    ApplicationCreateRequestV1,
    ApplicationOperationRequestV1,
    ApplicationOperationV1,
    ApplicationRegistrationV1,
    ApplicationStatusV1,
)
from loom.nebius_application_evidence import ApplicationOperationEvidenceV1
from loom_service.application_management.build_registry import ApplicationBuildRegistry
from loom_service.application_management.login import ApplicationLogin
from loom_service.application_management.manager import ApplicationManager
from loom_service.application_management.operation_evidence import read_operation_evidence
from loom_service.application_management.service_runtime import ApplicationServiceRuntime
from loom_service.application_management.source_upload import ApplicationSourceUploader
from loom_service.environment_management.registry import ManagementError, owner_identity
from loom_service.routes.environments import ManagementPrincipal

router = APIRouter()
IdempotencyKey = Annotated[str, Header(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9._:-]+$")]


def manager(request: Request) -> ApplicationManager:
    value = getattr(request.app.state, "application_manager", None)
    if not isinstance(value, ApplicationManager):
        raise ManagementError("application_management_not_configured", 503)
    return value


def source_uploader(request: Request) -> ApplicationSourceUploader:
    value = getattr(request.app.state, "application_source_uploader", None)
    if not isinstance(value, ApplicationSourceUploader):
        raise ManagementError("application_source_upload_not_configured", 503)
    return value


def build_registry(request: Request) -> ApplicationBuildRegistry:
    value = getattr(request.app.state, "application_build_registry", None)
    if not isinstance(value, ApplicationBuildRegistry):
        raise ManagementError("application_build_not_configured", 503)
    return value


def _worker_capability(configured: bool, task: asyncio.Task[None] | None,
                       healthy: bool) -> ApplicationWorkerCapability:
    if not configured:
        return "not_configured"
    if task is None:
        return "worker_unavailable"
    return "worker_healthy" if not task.done() and healthy else "worker_unhealthy"


@router.get("/application-capabilities")
async def application_capabilities(request: Request, response: Response,
                                   principal: ManagementPrincipal) -> ApplicationCapabilitiesV1:
    """Observe installed process handles; never probe or change provider/pool state."""
    owner_identity(principal)
    response.headers["Cache-Control"] = "no-store"
    state = request.app.state
    application = getattr(state, "application_runtime", None)
    runtime = application if isinstance(application, ApplicationServiceRuntime) else None
    build = runtime.build_worker if runtime is not None else None
    return ApplicationCapabilitiesV1(
        scope="management_process",
        application_lifecycle=_worker_capability(
            isinstance(getattr(state, "application_manager", None), ApplicationManager),
            runtime.task if runtime is not None else None,
            runtime.worker.healthy if runtime is not None else False,
        ),
        source_upload="configured" if isinstance(
            getattr(state, "application_source_uploader", None), ApplicationSourceUploader,
        ) else "not_configured",
        image_builds=_worker_capability(
            isinstance(getattr(state, "application_build_registry", None), ApplicationBuildRegistry),
            runtime.build_task if runtime is not None and build is not None else None,
            build.healthy if build is not None else False,
        ),
        execution="not_checked",
    )


@router.post("/application-builds", status_code=201)
async def create_build(request: Request, response: Response, payload: ApplicationImageBuildRequestV1,
                       principal: ManagementPrincipal, idempotency_key: IdempotencyKey) -> ApplicationImageBuildStatusV1:
    response.headers["Cache-Control"] = "no-store"
    return await build_registry(request).create(principal=principal, upload_id=payload.upload_id, idempotency_key=idempotency_key)


@router.get("/application-builds/{build_id}")
async def build_status(request: Request, response: Response, build_id: UUID,
                       principal: ManagementPrincipal) -> ApplicationImageBuildStatusV1:
    response.headers["Cache-Control"] = "no-store"
    return await build_registry(request).status(build_id, principal=principal)


@router.post("/application-builds/{build_id}/cancel", status_code=202)
async def cancel_build(request: Request, response: Response, build_id: UUID, payload: ApplicationImageBuildAttemptRequestV1,
                       principal: ManagementPrincipal) -> ApplicationImageBuildStatusV1:
    response.headers["Cache-Control"] = "no-store"
    return await build_registry(request).cancel(build_id, principal=principal, attempt=payload.attempt)


@router.post("/application-builds/{build_id}/retry", status_code=202)
async def retry_build(request: Request, response: Response, build_id: UUID, payload: ApplicationImageBuildAttemptRequestV1,
                      principal: ManagementPrincipal) -> ApplicationImageBuildStatusV1:
    response.headers["Cache-Control"] = "no-store"
    return await build_registry(request).retry(build_id, principal=principal, attempt=payload.attempt)


@router.post("/application-sources", status_code=201)
async def create_source_upload(request: Request, response: Response, payload: ApplicationSourceUploadRequestV1,
                               principal: ManagementPrincipal, idempotency_key: IdempotencyKey) -> ApplicationSourceUploadV1:
    response.headers["Cache-Control"] = "no-store"
    return await source_uploader(request).registry.create(principal=principal, request=payload, idempotency_key=idempotency_key)


@router.get("/application-sources/{upload_id}")
async def source_upload_status(request: Request, response: Response, upload_id: UUID,
                               principal: ManagementPrincipal) -> ApplicationSourceUploadV1:
    response.headers["Cache-Control"] = "no-store"
    return await source_uploader(request).registry.status(upload_id, principal=principal)


@router.put("/application-sources/{upload_id}/content")
async def upload_source_content(request: Request, response: Response, upload_id: UUID,
                                principal: ManagementPrincipal) -> ApplicationSourceUploadV1:
    if request.headers.get("content-type", "").lower() != "application/octet-stream":
        raise ManagementError("application_source_content_type_required", 415)
    response.headers["Cache-Control"] = "no-store"
    return await source_uploader(request).upload(upload_id, principal=principal, body=request.stream())


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
