"""Cancel abandoned model forwarding without competing for request-body reads."""

from __future__ import annotations

import asyncio
from contextlib import suppress

from starlette.types import ASGIApp, Message, Receive, Scope, Send

_MODEL_PATHS = frozenset(
    {
        "/v1/chat/completions",
        "/openai/v1/chat/completions",
        "/v1/messages",
        "/anthropic/v1/messages",
        "/v1/responses",
        "/openai/v1/responses",
        "/harbor-baseline/v1/chat/completions",
    }
)


class ModelClientDisconnectMiddleware:
    """One receive owner after the body; all request tasks are awaited.

    ASGI does not automatically cancel a non-streaming handler when its HTTP
    caller disappears. Close its upstream transport promptly instead of leaving
    inference active until the signed deadline. The request task is awaited so
    its durable dispatch/accounting cleanup completes before we return.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        path = scope.get("path", "")
        if (
            scope["type"] != "http"
            or scope.get("method") != "POST"
            or not (
                path in _MODEL_PATHS
                or path.startswith("/google/v1beta/models/")
                or path.startswith("/v1beta/models/")
            )
        ):
            await self.app(scope, receive, send)
            return
        body_consumed = asyncio.Event()
        disconnected = asyncio.Event()
        response_complete = False

        async def receive_body_or_disconnect() -> Message:
            # Until the application consumes the final body frame, it remains
            # the only reader. Upload/streaming-body backpressure is unchanged.
            if not body_consumed.is_set():
                message = await receive()
                if message["type"] == "http.request" and not message.get("more_body", False):
                    body_consumed.set()
                return message
            await disconnected.wait()
            return {"type": "http.disconnect"}

        async def send_response(message: Message) -> None:
            nonlocal response_complete
            await send(message)
            if message["type"] == "http.response.body" and not message.get("more_body", False):
                # Normal response completion can itself cause ASGI transports
                # to report disconnect. Do not interrupt later accounting.
                response_complete = True

        async def run_request() -> None:
            await self.app(scope, receive_body_or_disconnect, send_response)

        request = asyncio.create_task(run_request())

        async def watch_disconnect() -> None:
            await body_consumed.wait()
            while True:
                message = await receive()
                if message["type"] == "http.disconnect":
                    disconnected.set()
                    if not response_complete:
                        request.cancel()
                    return

        watcher = asyncio.create_task(watch_disconnect())
        try:
            try:
                await request
            except asyncio.CancelledError:
                if not disconnected.is_set():
                    raise
        finally:
            watcher.cancel()
            with suppress(asyncio.CancelledError):
                await watcher
