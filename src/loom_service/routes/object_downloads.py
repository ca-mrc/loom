"""Authenticated object download helpers for loom_service routes."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Iterator
from typing import Any
from urllib.parse import quote

from anyio import CancelScope
from botocore.exceptions import ClientError
from fastapi import HTTPException
from fastapi.responses import StreamingResponse
from starlette.types import Receive, Scope, Send

from loom_service.metrics import ARTIFACT_DOWNLOAD_BYTES

_CHUNK_SIZE = 64 * 1024


def _object_error_code(exc: ClientError) -> str | None:
    code = exc.response.get("Error", {}).get("Code")
    return code if isinstance(code, str) else None


def _iter_body(body: Any) -> Iterator[bytes]:
    iter_chunks = getattr(body, "iter_chunks", None)
    if callable(iter_chunks):
        for chunk in iter_chunks(chunk_size=_CHUNK_SIZE):
            if chunk:
                yield bytes(chunk)
        return

    read = getattr(body, "read", None)
    if not callable(read):
        raise TypeError("object body is not readable")
    while True:
        chunk = read(_CHUNK_SIZE)
        if not chunk:
            break
        yield bytes(chunk)


async def _stream_body(body: Any) -> AsyncIterator[bytes]:
    chunks = _iter_body(body)
    while True:
        worker = asyncio.create_task(asyncio.to_thread(next, chunks, None))
        try:
            chunk = await asyncio.shield(worker)
        except asyncio.CancelledError:
            # Disconnects use AnyIO level cancellation; task.cancel() may also
            # recur. Finish the active read before the response closes its body.
            with CancelScope(shield=True):
                while not worker.done():
                    try:
                        await asyncio.shield(worker)
                    except asyncio.CancelledError:
                        continue
                    except Exception:
                        break
            if not worker.cancelled():
                worker.exception()
            raise
        if chunk is None:
            return
        yield chunk


class _ObjectResponse(StreamingResponse):
    def __init__(self, body: Any, *, headers: dict[str, str], media_type: str) -> None:
        self.object_body = body
        super().__init__(_stream_body(body), headers=headers, media_type=media_type)

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        try:
            await super().__call__(scope, receive, send)
        finally:
            # Own the acquired body even if headers fail before iteration starts.
            close = getattr(self.object_body, "close", None)
            if callable(close):
                close()


def stream_object_response(
    *,
    client: Any,
    bucket: str,
    key: str,
    filename: str,
    artifact_kind: str,
    media_type: str = "application/octet-stream",
) -> StreamingResponse:
    try:
        obj = client.get_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        code = _object_error_code(exc)
        if code in {"NoSuchBucket", "NoSuchKey", "404"}:
            raise HTTPException(
                status_code=404,
                detail="download object not found",
            ) from exc
        raise

    headers = {
        "Content-Disposition": (
            f"attachment; filename*=UTF-8''{quote(filename, safe='')}"
        ),
    }
    content_length = obj.get("ContentLength")
    if isinstance(content_length, int):
        headers["Content-Length"] = str(content_length)
        ARTIFACT_DOWNLOAD_BYTES.labels(
            artifact_kind=artifact_kind,
        ).inc(content_length)

    return _ObjectResponse(
        obj["Body"],
        headers=headers,
        media_type=media_type,
    )
