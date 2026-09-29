"""Typed personal-application API; no fallback to legacy full environments."""

from __future__ import annotations

import time
from types import TracebackType
from typing import Any
from uuid import UUID

from pydantic import TypeAdapter

from loom.nebius_application_contract import (
    ApplicationCreateRequestV1,
    ApplicationOperationRequestV1,
    ApplicationOperationV1,
    ApplicationRegistrationV1,
    ApplicationStatusV1,
)
from loom_cli import server_client
from loom_cli.server_client import assert_2xx


class ApplicationClient:
    def __init__(self) -> None:
        self.http = server_client.authed_client(server_client.require_logged_in())

    def __enter__(self) -> ApplicationClient:
        return self

    def __exit__(self, exc_type: type[BaseException] | None, exc: BaseException | None,
                 traceback: TracebackType | None) -> None:
        self.http.close()

    def create(self, request: ApplicationCreateRequestV1, *, idempotency_key: str) -> ApplicationOperationV1:
        response = self.http.post("/api/v1/applications", json=request.model_dump(mode="json"),
                                  headers={"Idempotency-Key": idempotency_key})
        result = ApplicationOperationV1.model_validate(assert_2xx(response, action="create personal application"))
        if result.action != "create":
            raise ValueError("application action mismatch")
        return result

    def list(self) -> list[ApplicationRegistrationV1]:
        data = assert_2xx(self.http.get("/api/v1/applications"), action="list personal applications")
        return TypeAdapter(list[ApplicationRegistrationV1]).validate_python(data["items"])

    def status(self, application_id: UUID) -> ApplicationStatusV1:
        result = ApplicationStatusV1.model_validate(assert_2xx(
            self.http.get(f"/api/v1/applications/{application_id}"), action="read personal application",
        ))
        if (result.registration.application_id != application_id
                or (result.operation is not None and result.operation.application_id != application_id)):
            raise ValueError("application identity mismatch")
        return result

    def transition(self, application_id: UUID, request: ApplicationOperationRequestV1, *,
                   idempotency_key: str) -> ApplicationOperationV1:
        response = self.http.post(f"/api/v1/applications/{application_id}/operations",
            json=request.model_dump(mode="json"), headers={"Idempotency-Key": idempotency_key})
        result = ApplicationOperationV1.model_validate(assert_2xx(response, action="change personal application"))
        if result.application_id != application_id or result.action != request.action:
            raise ValueError("application operation mismatch")
        return result

    def login(self, application_id: UUID) -> dict[str, Any]:
        result = assert_2xx(self.http.post(f"/api/v1/applications/{application_id}/login"), action="request application login")
        if not isinstance(result, dict):
            raise ValueError("invalid application login proof")
        return result

    def operation(self, operation_id: UUID, *, timeout: float = 30) -> ApplicationOperationV1:
        result = ApplicationOperationV1.model_validate(assert_2xx(
            self.http.get(f"/api/v1/application-operations/{operation_id}", timeout=timeout),
            action="read application operation",
        ))
        if result.operation_id != operation_id:
            raise ValueError("application operation mismatch")
        return result

    def retry(self, operation_id: UUID) -> ApplicationOperationV1:
        result = ApplicationOperationV1.model_validate(assert_2xx(
            self.http.post(f"/api/v1/application-operations/{operation_id}/retry"), action="retry application operation",
        ))
        if result.operation_id != operation_id:
            raise ValueError("application operation mismatch")
        return result

    def wait(self, operation_id: UUID, *, timeout: float) -> tuple[ApplicationOperationV1, bool]:
        if not 0 <= timeout <= 86400:
            raise ValueError("wait timeout must be between zero and one day")
        deadline = time.monotonic() + timeout
        while True:
            result = self.operation(operation_id, timeout=max(0.1, min(30, deadline - time.monotonic())))
            if result.phase in {"completed", "blocked", "superseded"}:
                return result, True
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return result, False
            time.sleep(min(2, remaining))
