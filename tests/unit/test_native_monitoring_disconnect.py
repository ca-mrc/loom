from __future__ import annotations

import asyncio
from typing import Any

import pytest

from loom_llm_gateway.client_disconnect import ModelClientDisconnectMiddleware


@pytest.mark.parametrize("method,path", [
    ("POST", "/internal/service-execution/trajectory-events"),
    ("GET", "/internal/service-execution/llm-calls"),
])
async def test_completed_monitoring_response_keeps_post_response_cleanup(
    method: str, path: str,
) -> None:
    response_complete, release, cleaned = asyncio.Event(), asyncio.Event(), asyncio.Event()
    messages: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
    await messages.put({"type": "http.request", "body": b"{}", "more_body": False})

    async def app(scope, receive, send):  # type: ignore[no-untyped-def]
        if method == "POST":
            assert (await receive())["body"] == b"{}"
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"{}", "more_body": False})
        response_complete.set()
        await release.wait()
        cleaned.set()

    async def send(_message: dict[str, Any]) -> None:
        pass

    task = asyncio.create_task(ModelClientDisconnectMiddleware(app)(
        {"type": "http", "method": method, "path": path}, messages.get, send,
    ))
    try:
        await asyncio.wait_for(response_complete.wait(), timeout=1)
        await messages.put({"type": "http.disconnect"})
        for _ in range(3):
            await asyncio.sleep(0)
        assert not task.done(), "normal response completion cancelled cleanup"
        release.set()
        await asyncio.wait_for(task, timeout=1)
        assert cleaned.is_set()
    finally:
        release.set()
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
