"""Stable, bounded image-preparation failures shared by admission and execution."""

from __future__ import annotations

import json
from typing import Any

from botocore.exceptions import (
    ClientError,
    ConnectionClosedError,
    ConnectTimeoutError,
    EndpointConnectionError,
    ReadTimeoutError,
)

ERROR_SCHEMA = "loom.task-image-preparation-error.v1"
PACKAGE_GUARD_PREFIX = "LOOM_IMAGE_PREPARATION_ERROR="
PACKAGE_GUARD_MESSAGE = "Harness preparation requires a change to an authored package; use an explicitly reviewed compatible image/repository snapshot"
# Unknown failures never acquire retry authority from exception prose.
PREPARATION_ERRORS = {
    "image_preparation_base_unsupported": False,
    "image_preparation_dockerfile_invalid": False,
    "image_preparation_input_invalid": False,
    "image_preparation_source_missing": False,
    "image_preparation_source_denied": False,
    "image_preparation_source_unavailable": True,
    "image_preparation_internal_error": False,
    "image_preparation_package_change_required": False,
}


class TaskImagePreparationError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        if code not in PREPARATION_ERRORS:
            raise ValueError("unknown image preparation error code")
        super().__init__(message)
        self.code = code
        self.retryable = PREPARATION_ERRORS[code]

    def receipt(self) -> dict[str, Any]:
        return {"error_schema": ERROR_SCHEMA, "error_code": self.code,
                "retryable": self.retryable, "error": str(self)[:500]}


def preparation_failure_from_receipt(receipt: Any) -> tuple[str, str, bool] | None:
    if not isinstance(receipt, dict) or receipt.get("error_schema") != ERROR_SCHEMA:
        return None
    code = receipt.get("error_code")
    if not isinstance(code, str) or code not in PREPARATION_ERRORS:
        return None
    retryable = PREPARATION_ERRORS[code]
    if type(receipt.get("retryable")) is not bool or receipt["retryable"] != retryable:
        return None
    message = receipt.get("error")
    if not isinstance(message, str) or len(message.encode()) > 2000:
        return None
    return code, message, retryable


def package_guard_failure(log: str) -> tuple[str, str, bool] | None:
    """Read an explicit package guard frame; build logs cannot grant retries.

    Accept raw frames and BuildKit's numbered progress framing. Command echoes,
    ordinary error prose and every retryable code remain outside this contract.
    """
    for line in log[-16384:].splitlines():
        parts = line.split(' ', 2)
        if (len(parts) == 3 and parts[0].startswith('#') and parts[0][1:].isdigit()
                and parts[1].replace('.', '', 1).isdigit()):
            line = parts[2]
        if not line.startswith(PACKAGE_GUARD_PREFIX):
            continue
        try:
            classified = preparation_failure_from_receipt(json.loads(line[len(PACKAGE_GUARD_PREFIX):]))
        except ValueError:
            continue
        if classified is not None and classified[0] == 'image_preparation_package_change_required':
            return classified[0], PACKAGE_GUARD_MESSAGE, False
    return None


def classify_preparation_failure(error: Exception) -> TaskImagePreparationError:
    """Only typed transport/service failures permit the existing bounded retry budget."""
    if isinstance(error, TaskImagePreparationError):
        return error
    if isinstance(error, (EndpointConnectionError, ConnectionClosedError, ConnectTimeoutError, ReadTimeoutError)):
        return TaskImagePreparationError("image_preparation_source_unavailable", "Task image source connection is temporarily unavailable")
    if isinstance(error, ClientError):
        code = error.response.get("Error", {}).get("Code")
        status = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if code in {"NoSuchKey", "NoSuchBucket", "NotFound", "404"} or status == 404:
            return TaskImagePreparationError("image_preparation_source_missing", "Task image source object is missing")
        if code in {"AccessDenied", "InvalidAccessKeyId", "SignatureDoesNotMatch", "403"} or status in {401, 403}:
            return TaskImagePreparationError("image_preparation_source_denied", "Task image source access was denied")
        if code in {"SlowDown", "RequestTimeout", "RequestTimeoutException", "InternalError", "ServiceUnavailable", "Throttling"} or status in {408, 429, 500, 502, 503, 504}:
            return TaskImagePreparationError("image_preparation_source_unavailable", "Task image source service is temporarily unavailable")
    return TaskImagePreparationError("image_preparation_internal_error", "Task image preparation failed without a classified retryable cause")
