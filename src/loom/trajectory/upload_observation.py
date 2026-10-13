"""Bounded, non-authoritative observations of native output publication.

Only explicit native-source/canonical scopes emit. No payloads, object names,
exception strings or arbitrary labels enter this schema. Missing finishes mean
unknown completion; waiter abandonment never closes a worker/transport interval.
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import threading
import time
from collections.abc import Iterator
from contextvars import ContextVar, Token
from datetime import UTC, datetime
from typing import Any, Literal
from uuid import UUID, uuid4

logger = logging.getLogger("loom.upload_observation")
_PROCESS_ID = str(uuid4())
_CONTEXT: ContextVar[dict[str, str | int] | None] = ContextVar("upload_observation", default=None)
_PARENT: ContextVar[UploadSpan | None] = ContextVar("upload_observation_parent", default=None)
_S3_OPERATION: ContextVar[str] = ContextVar("upload_s3_operation", default="other")
Stage = Literal["gateway", "authorization", "processing", "spool", "sdk_worker", "s3_transport", "materialization"]
_OPERATIONS = frozenset({
    "prepare", "part", "complete", "commit", "materialize", "copy",
    "create_multipart_upload", "resume_multipart_upload", "upload_part",
    "upload_part_stream", "complete_multipart_upload", "abort_multipart_upload",
    "put_object", "get_object", "stat_object", "stream_object",
    "CreateMultipartUpload", "UploadPart", "CompleteMultipartUpload", "AbortMultipartUpload",
    "PutObject", "GetObject", "HeadObject", "ListParts", "GetBucketVersioning",
})
_OUTCOMES = frozenset({"success", "error", "cancelled", "timeout", "http_error", "fenced", "retry", "unavailable"})


def configure_upload_observations() -> None:
    """Install one dedicated JSON stderr sink, independent of root log defaults."""
    if not any(getattr(h, "_loom_upload_sink", False) for h in logger.handlers):
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(message)s"))
        handler._loom_upload_sink = True  # type: ignore[attr-defined]
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False


@contextlib.contextmanager
def observe_upload(
    phase: Literal["source", "canonical"], *, lease_id: UUID,
    generation: int | None = None, session_id: UUID | None = None,
    request_id: UUID | None = None, broker_request_id: UUID | None = None,
    file_index: int | None = None, part_number: int | None = None,
) -> Iterator[None]:
    context: dict[str, str | int] = {"phase": phase, "lease_id": str(lease_id)}
    for key, value in (("session_id", session_id), ("request_id", request_id),
                       ("broker_request_id", broker_request_id)):
        if value is not None:
            context[key] = str(value)
    for key, number in (("generation", generation), ("file_index", file_index), ("part_number", part_number)):
        if number is not None and 0 <= number < 2**63:
            context[key] = number
    token = _CONTEXT.set(context)
    parent_token = _PARENT.set(None)
    try:
        yield
    finally:
        _PARENT.reset(parent_token)
        _CONTEXT.reset(token)


def bind_upload_source(*, generation: int, session_id: UUID) -> None:
    """Enrich the owned scope after the materializer loads its fenced lease."""
    context = _CONTEXT.get()
    if context is not None:
        context.update(generation=generation, session_id=str(session_id))


class UploadSpan:
    def __init__(self, stage: Stage, operation: str, *, attempt: int = 1, declared_bytes: int | None = None) -> None:
        self.context = _CONTEXT.get()
        self.stage = stage
        self.operation = operation if operation in _OPERATIONS else "other"
        self.span_id = str(uuid4())
        parent = _PARENT.get()
        self.parent_id = parent.span_id if parent else None
        self.attempt = attempt
        self.declared_bytes = declared_bytes
        self.observed_bytes: int | None = None
        self.status_code: int | None = None
        self.response_body_mode: Literal["streamed", "buffered"] | None = None
        self.outcome = "success"
        self._lock = threading.Lock()
        self._started: int | None = None
        self._finished = False
        self._token: Token[UploadSpan | None] | None = None
        self._transports = 0

    def _emit(self, event: str, **fields: str | int) -> None:
        if self.context is None:
            return
        record: dict[str, Any] = {
            "schema": "loom.upload-observation.v1", "process_id": _PROCESS_ID,
            **self.context, "span_id": self.span_id, "parent_span_id": self.parent_id,
            "stage": self.stage, "operation": self.operation, "attempt": self.attempt,
            "event": event, "wall_time": datetime.now(UTC).isoformat(),
            "monotonic_ns": time.monotonic_ns(), **fields,
        }
        for key, value in (("declared_bytes", self.declared_bytes), ("observed_bytes", self.observed_bytes)):
            if value is not None and 0 <= value < 2**63:
                record[key] = value
        if self.status_code is not None and 100 <= self.status_code <= 599:
            record["status_code"] = self.status_code
        if self.response_body_mode is not None:
            record["response_body_mode"] = self.response_body_mode
        # Telemetry must not alter output publication on a broken logging sink.
        with contextlib.suppress(Exception):
            logger.info(json.dumps(record, separators=(",", ":")))

    def __enter__(self) -> UploadSpan:
        with self._lock:
            self._started = time.monotonic_ns()
            self._emit("started")
        self._token = _PARENT.set(self)
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if isinstance(exc, asyncio.CancelledError):
            self.outcome = "cancelled"
        elif isinstance(exc, TimeoutError):
            self.outcome = "timeout"
        elif exc is not None:
            self.outcome = "error"
        with self._lock:
            self._finished = True
            self._emit("finished", outcome=self.outcome if self.outcome in _OUTCOMES else "error",
                       duration_ns=time.monotonic_ns() - (self._started or time.monotonic_ns()))
        if self._token is not None:
            _PARENT.reset(self._token)

    def abandon_wait(self, outcome: Literal["timeout", "cancelled"]) -> None:
        with self._lock:
            state = "finished" if self._finished else "running" if self._started else "not_started"
            self._emit("wait_abandoned", outcome=outcome, completion_state=state)


def configure_s3_transport(client: Any) -> None:
    """Observe each real botocore HTTP send, including SDK-internal retries.

    Send consumes nonstreaming response bodies (including upload responses),
    but returns at headers for streaming responses. Provider completion/accepted
    bytes remain unknown. The SDK worker also includes parsing/retry backoff.
    This deliberately does not time before-send/after-call events as transport.
    """
    def operation_name(model: Any, **_kwargs: Any) -> None:
        _S3_OPERATION.set(model.name if model.name in _OPERATIONS else "other")

    client.meta.events.register_last("before-call.s3", operation_name)
    endpoint = getattr(client, "_endpoint", None)
    if endpoint is None:  # Protocol-only test doubles have no HTTP transport.
        return
    session = endpoint.http_session
    original_send = session.send

    def send(request: Any) -> Any:
        parent = _PARENT.get()
        if _CONTEXT.get() is None or parent is None:
            return original_send(request)
        parent._transports += 1  # SDK invocation stays on its one worker thread.
        raw_size = request.headers.get("Content-Length")
        try:
            size = int(raw_size) if raw_size is not None else None
        except (ValueError, TypeError):
            size = None
        span = UploadSpan("s3_transport", _S3_OPERATION.get(), attempt=parent._transports,
                          declared_bytes=size)
        span.response_body_mode = "streamed" if request.stream_output else "buffered"
        with span:
            response = original_send(request)
            span.status_code = response.status_code
            if response.status_code >= 400:
                span.outcome = "http_error"
            return response

    session.send = send
