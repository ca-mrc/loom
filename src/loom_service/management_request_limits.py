"""Pre-parse management-only request bounds; never buffer application uploads.

The in-flight budget belongs to one ASGI process/event loop. It is not a
distributed ingress rate limit. Keep configured body and concurrency limits
within the process memory budget, including copies and JSON parsing overhead.
"""

from __future__ import annotations

import asyncio
import re
from collections.abc import Callable

from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from loom.application_source_archive import MAX_APPLICATION_SOURCE_ARCHIVE_BYTES

_SOURCE_CONTENT = re.compile(r"/api/v1/application-sources/[0-9a-f]{8}-(?:[0-9a-f]{4}-){3}[0-9a-f]{12}/content")


class _StreamingBodyError(Exception):
    """Only this outer receive boundary may translate its framing failure."""


class ManagementRequestLimitsMiddleware:
    def __init__(self, app: ASGIApp, *, max_body_bytes: int, max_inflight: int, body_timeout_sec: float,
                 source_upload_enabled: Callable[[], bool] | None = None) -> None:
        self.app = app
        self.max_body_bytes = max_body_bytes
        self.max_inflight = max_inflight
        self.body_timeout_sec = body_timeout_sec
        self.source_upload_enabled = source_upload_enabled
        self._active = 0

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        streaming = (scope["method"] == "PUT" and _SOURCE_CONTENT.fullmatch(scope["path"]) is not None
            and self.source_upload_enabled is not None and self.source_upload_enabled())
        body_limit = MAX_APPLICATION_SOURCE_ARCHIVE_BYTES if streaming else self.max_body_bytes

        async def reject(status: int, detail: str) -> None:
            headers = {"Cache-Control": "no-store"}
            if scope.get("http_version", "1.1").startswith("1."):
                headers["Connection"] = "close"
            if status == 503:
                headers["Retry-After"] = "1"
            await JSONResponse({"detail": detail}, status_code=status, headers=headers)(scope, receive, send)

        headers = scope.get("headers", [])
        lengths = [v for k, v in headers if k.lower() == b"content-length"]
        declared: int | None = None
        if lengths:
            if (len(lengths) != 1 or not lengths[0].isdigit()
                    or any(k.lower() == b"transfer-encoding" for k, _ in headers)):
                await reject(400, "invalid request framing")
                return
            try:
                declared = int(lengths[0])
            except ValueError:
                await reject(400, "invalid request framing")
                return
            if declared > body_limit:
                await reject(413, "management request body too large")
                return
        if any(k.lower() == b"content-encoding" and v.strip().lower() != b"identity" for k, v in headers):
            await reject(415, "encoded management request bodies are not supported")
            return
        # No await between admission test/increment: atomic on the ASGI loop.
        if self._active >= self.max_inflight:
            await reject(503, "management request capacity exhausted")
            return
        self._active += 1
        try:
            if streaming:
                # Do not call receive here: the route must first authenticate
                # owner/membership/CSRF and check the durable upload intent.
                # The configured uploader owns reception/storage deadlines and
                # disk admission; this layer retains framing and the hard cap.
                size, finished, response_started = 0, False, False
                failure: tuple[int, str] | None = None

                def framing_error(status: int, detail: str) -> _StreamingBodyError:
                    nonlocal failure
                    failure = (status, detail)
                    return _StreamingBodyError()

                async def bounded_receive() -> Message:
                    nonlocal size, finished
                    message = await receive()
                    if message["type"] == "http.disconnect":
                        return message
                    if message["type"] != "http.request":
                        raise framing_error(400, "invalid request framing")
                    size += len(message.get("body", b""))
                    if size > body_limit:
                        raise framing_error(413, "management request body too large")
                    if declared is not None and size > declared:
                        raise framing_error(400, "request body length mismatch")
                    if not message.get("more_body", False):
                        if declared is not None and size != declared:
                            raise framing_error(400, "request body length mismatch")
                        finished = True
                    return message

                async def streaming_send(message: Message) -> None:
                    nonlocal response_started
                    if message["type"] == "http.response.start":
                        response_started = True
                        headers = [(key, value) for key, value in message.get("headers", [])
                            if key.lower() != b"cache-control"]
                        headers.append((b"cache-control", b"no-store"))
                        if not finished and scope.get("http_version", "1.1").startswith("1."):
                            headers = [(key, value) for key, value in headers if key.lower() != b"connection"]
                            headers.append((b"connection", b"close"))
                        message = {**message, "headers": headers}
                    await send(message)

                try:
                    await self.app(scope, bounded_receive, streaming_send)
                except Exception as caught:
                    # BaseHTTPMiddleware may wrap receive failures in nested
                    # task groups. Handle only our framing error, never unrelated
                    # handler errors, cancellation or a partially sent response.
                    if isinstance(caught, ExceptionGroup):
                        _, other = caught.split(_StreamingBodyError)
                        if other is not None:
                            raise
                    elif not isinstance(caught, _StreamingBodyError):
                        raise
                    if response_started or failure is None:
                        raise
                    await reject(*failure)
                return
            body_buffer = bytearray()
            size = 0
            error: tuple[int, str] | None = None
            try:
                async with asyncio.timeout(self.body_timeout_sec):
                    while True:
                        message = await receive()
                        if message["type"] == "http.disconnect":
                            return
                        if message["type"] != "http.request":
                            error = (400, "invalid request framing")
                            break
                        chunk = message.get("body", b"")
                        size += len(chunk)
                        if size > self.max_body_bytes:
                            error = (413, "management request body too large")
                            break
                        if declared is not None and size > declared:
                            error = (400, "request body length mismatch")
                            break
                        # One buffer also bounds overhead for empty/tiny frames.
                        body_buffer.extend(chunk)
                        if not message.get("more_body", False):
                            break
            except TimeoutError:
                error = (408, "management request body reception timed out")
            # Only reception is timed. Sending an error must not be cancelled
            # midway and followed by a second response from the timeout handler.
            if error is not None:
                await reject(*error)
                return
            if declared is not None and size != declared:
                await reject(400, "request body length mismatch")
                return
            body = bytes(body_buffer)
            body_buffer.clear()
            replayed = False

            async def replay() -> Message:
                nonlocal replayed
                if not replayed:
                    replayed = True
                    return {"type": "http.request", "body": body, "more_body": False}
                return await receive()

            # Admission lasts through response completion; response chunks are
            # forwarded unchanged and there is no request queue or disk spool.
            await self.app(scope, replay, send)
        finally:
            self._active -= 1
