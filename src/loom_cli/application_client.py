"""Typed personal-application API; no fallback to legacy full environments."""

from __future__ import annotations

import time
from collections.abc import Iterator
from dataclasses import dataclass
from types import TracebackType
from typing import Any, BinaryIO
from uuid import UUID

import httpx
from pydantic import TypeAdapter

from loom.application_image_build import (
    ApplicationImageBuildAttemptRequestV1,
    ApplicationImageBuildStatusV1,
)
from loom.application_source_upload import (
    ApplicationSourceUploadRequestV1,
    ApplicationSourceUploadV1,
)
from loom.nebius_application_contract import (
    ApplicationCreateRequestV1,
    ApplicationOperationRequestV1,
    ApplicationOperationV1,
    ApplicationRegistrationV1,
    ApplicationStatusV1,
)
from loom.nebius_application_evidence import ApplicationOperationEvidenceV1
from loom_cli import server_client
from loom_cli.application_source import PackagedApplicationSource
from loom_cli.server_client import assert_2xx


def _source_request(source: PackagedApplicationSource) -> ApplicationSourceUploadRequestV1:
    return ApplicationSourceUploadRequestV1(source_digest=source.manifest.digest,
        archive_sha256=source.archive_sha256.removeprefix("sha256:"),
        archive_size_bytes=source.archive_size_bytes, base_commit=source.base_commit)


def _same_source(receipt: ApplicationSourceUploadV1, request: ApplicationSourceUploadRequestV1) -> None:
    if any(getattr(receipt, name) != value for name, value in request.model_dump().items()):
        raise ValueError("application source identity mismatch")


@dataclass(frozen=True)
class _SourceBody:
    archive: BinaryIO

    def __iter__(self) -> Iterator[bytes]:
        # The shared session client can refresh and retry an explicit CSRF
        # rejection. Each such transmission starts at the same frozen byte zero;
        # transport failures/unknown writes are never automatically retried.
        self.archive.seek(0)
        while chunk := self.archive.read(1024 * 1024):
            yield chunk


class ApplicationClient:
    def __init__(self) -> None:
        self.http = server_client.authed_client(server_client.require_logged_in())

    def __enter__(self) -> ApplicationClient:
        return self

    def __exit__(self, exc_type: type[BaseException] | None, exc: BaseException | None,
                 traceback: TracebackType | None) -> None:
        self.http.close()

    def create_source_upload(self, source: PackagedApplicationSource, *, idempotency_key: str) -> ApplicationSourceUploadV1:
        request = _source_request(source)
        response = self.http.post("/api/v1/application-sources", json=request.model_dump(mode="json"),
                                  headers={"Idempotency-Key": idempotency_key})
        receipt = ApplicationSourceUploadV1.model_validate(assert_2xx(response, action="prepare personal source upload"))
        _same_source(receipt, request)
        return receipt

    def source_upload_status(self, upload_id: UUID) -> ApplicationSourceUploadV1:
        receipt = ApplicationSourceUploadV1.model_validate(assert_2xx(
            self.http.get(f"/api/v1/application-sources/{upload_id}"), action="read personal source upload"))
        if receipt.upload_id != upload_id:
            raise ValueError("application source upload mismatch")
        return receipt

    def upload_source(self, receipt: ApplicationSourceUploadV1, source: PackagedApplicationSource) -> ApplicationSourceUploadV1:
        request = _source_request(source)
        _same_source(receipt, request)
        response = self.http.put(f"/api/v1/application-sources/{receipt.upload_id}/content",
            content=_SourceBody(source.archive), timeout=httpx.Timeout(900, connect=30),
            headers={"Content-Type": "application/octet-stream", "Content-Length": str(source.archive_size_bytes)})
        verified = ApplicationSourceUploadV1.model_validate(assert_2xx(response, action="upload personal source"))
        _same_source(verified, request)
        if verified.upload_id != receipt.upload_id or verified.phase != "source_verified":
            raise ValueError("application source upload verification mismatch")
        return verified

    @staticmethod
    def _build_result(response: httpx.Response, *, build_id: UUID | None = None) -> ApplicationImageBuildStatusV1:
        result = ApplicationImageBuildStatusV1.model_validate(assert_2xx(response, action="personal application build"))
        release = result.release
        if ((build_id is not None and result.build_id != build_id)
                or (result.phase == "ready") != (release is not None)
                or (release is not None and (release.release_id != result.build_id or release.source_digest != result.source_digest))):
            raise ValueError("application build response mismatch")
        return result

    def create_build(self, source: ApplicationSourceUploadV1, *, idempotency_key: str) -> ApplicationImageBuildStatusV1:
        if source.phase != "source_verified":
            raise ValueError("application source is not verified")
        result = self._build_result(self.http.post("/api/v1/application-builds", json={"upload_id": str(source.upload_id)},
            headers={"Idempotency-Key": idempotency_key}))
        if result.upload_id != source.upload_id or result.source_digest != source.source_digest:
            raise ValueError("application build source mismatch")
        return result

    def build_status(self, build_id: UUID, *, timeout: float = 30) -> ApplicationImageBuildStatusV1:
        return self._build_result(self.http.get(f"/api/v1/application-builds/{build_id}", timeout=timeout), build_id=build_id)

    def change_build(self, build_id: UUID, *, action: str, attempt: int) -> ApplicationImageBuildStatusV1:
        if action not in {"cancel", "retry"}:
            raise ValueError("invalid application build action")
        request = ApplicationImageBuildAttemptRequestV1(attempt=attempt)
        result = self._build_result(self.http.post(f"/api/v1/application-builds/{build_id}/{action}",
            json=request.model_dump(mode="json")), build_id=build_id)
        if result.attempt != attempt + (action == "retry"):
            raise ValueError("application build attempt mismatch")
        return result

    def wait_build(self, build_id: UUID, *, timeout: float) -> tuple[ApplicationImageBuildStatusV1, bool]:
        if not 0 <= timeout <= 86400:
            raise ValueError("wait timeout must be between zero and one day")
        deadline = time.monotonic() + timeout
        while True:
            result = self.build_status(build_id, timeout=max(0.1, min(30, deadline - time.monotonic())))
            if result.phase in {"ready", "failed", "cancelled"}:
                return result, True
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return result, False
            time.sleep(min(2, remaining))

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

    def evidence(self, operation_id: UUID) -> ApplicationOperationEvidenceV1:
        result = ApplicationOperationEvidenceV1.model_validate(assert_2xx(
            self.http.get(f"/api/v1/application-operations/{operation_id}/evidence"),
            action="read application operation evidence",
        ))
        if result.operation.operation_id != operation_id:
            raise ValueError("application operation evidence mismatch")
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
