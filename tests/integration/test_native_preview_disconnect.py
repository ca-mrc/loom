"""Abandoned native monitoring must not resume DB/auth work after checkout waits."""
from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from loom.db.schema import ServiceExecutionLiveEvent
from tests.integration.test_gateway_model_concurrency import _tcp_gateway
from tests.integration.test_service_execution_leases import (
    _cleanup_service_execution_test_rows,  # noqa: F401
)
from tests.integration.test_service_execution_live import _live_app


@pytest.mark.parametrize("method,path", [
    ("POST", "/internal/service-execution/trajectory-events"),
    ("GET", "/internal/service-execution/llm-calls"),
])
async def test_abandoned_preview_does_not_resume_after_connection_becomes_available(
    postgres_url: str, method: str, path: str,
) -> None:
    checkout_started = asyncio.Event()

    class ObservedSession(AsyncSession):
        async def get(self, *args: Any, **kwargs: Any) -> Any:
            checkout_started.set()
            return await super().get(*args, **kwargs)

    engine = create_async_engine(postgres_url, pool_size=1, max_overflow=0, pool_timeout=10)
    sessions = async_sessionmaker(engine, class_=ObservedSession, expire_on_commit=False)
    request: asyncio.Task[None] | None = None
    try:
        async with _live_app(postgres_url) as setup:
            setup.app.state.session_factory = sessions
            reviews = 0
            original_review = setup.app.state.execution_pod_reviewer.review

            async def review(**kwargs: Any) -> Any:
                nonlocal reviews
                reviews += 1
                return await original_review(**kwargs)

            setup.app.state.execution_pod_reviewer.review = review
            messages: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
            body = json.dumps(setup.payload).encode() if method == "POST" else b""
            await messages.put({"type": "http.request", "body": body, "more_body": False})
            headers = [(b"host", b"loom.example"), (b"content-type", b"application/json"),
                       (b"authorization", setup.write_headers["Authorization"].encode()),
                       (b"x-loom-execution-lease-id", str(setup.lease_id).encode()),
                       (b"x-loom-execution-generation", b"1"), (b"x-loom-execution-role", b"attempt")]
            scope = {"type": "http", "asgi": {"version": "3.0", "spec_version": "2.4"},
                     "http_version": "1.1", "scheme": "https", "method": method,
                     "path": path, "raw_path": path.encode(), "query_string": b"",
                     "headers": headers, "client": ("10.42.0.4", 12345), "server": ("loom.example", 443)}

            async def send(_message: dict[str, Any]) -> None:
                # Uvicorn may discard sends after a socket has gone away. The
                # regression is whether work resumes, not delivery to that peer.
                pass

            async with engine.connect():
                request = asyncio.create_task(setup.app(scope, messages.get, send))
                await asyncio.wait_for(checkout_started.wait(), timeout=2)
                await messages.put({"type": "http.disconnect"})
                # Give the disconnect receiver and cancellation cleanup a turn
                # while the only real DB connection is still unavailable.
                for _ in range(3):
                    await asyncio.sleep(0)
            await asyncio.wait_for(request, timeout=5)
            assert reviews == 0, "the disconnected preview resumed native authorization"
            async with setup.sessions() as db:
                assert await db.scalar(select(func.count()).select_from(ServiceExecutionLiveEvent).where(
                    ServiceExecutionLiveEvent.trial_id == setup.trial_id,
                )) == 0
    finally:
        if request is not None and not request.done():
            request.cancel()
            await asyncio.gather(request, return_exceptions=True)
        await engine.dispose()


@pytest.mark.parametrize("method,path", [
    ("POST", "/internal/service-execution/trajectory-events"),
    ("GET", "/internal/service-execution/llm-calls"),
])
async def test_real_http_preview_disconnect_releases_server_checkout_wait(
    postgres_url: str, method: str, path: str,
) -> None:
    import httpx

    checkout_started, session_closed = asyncio.Event(), asyncio.Event()

    class ObservedSession(AsyncSession):
        async def get(self, *args: Any, **kwargs: Any) -> Any:
            checkout_started.set()
            return await super().get(*args, **kwargs)

        async def __aexit__(self, *args: Any) -> None:
            try:
                await super().__aexit__(*args)
            finally:
                session_closed.set()

    engine = create_async_engine(postgres_url, pool_size=1, max_overflow=0, pool_timeout=10)
    sessions = async_sessionmaker(engine, class_=ObservedSession, expire_on_commit=False)
    request: asyncio.Task[httpx.Response] | None = None
    try:
        async with _live_app(postgres_url) as setup:
            setup.app.state.session_factory = sessions
            async with _tcp_gateway(setup.app) as origin, httpx.AsyncClient(trust_env=False) as client:
                headers = {**setup.write_headers,
                           "X-Loom-Execution-Lease-Id": str(setup.lease_id),
                           "X-Loom-Execution-Generation": "1", "X-Loom-Execution-Role": "attempt"}
                async with engine.connect():
                    request = asyncio.create_task(client.request(
                        method, origin + path, headers=headers,
                        **({"json": setup.payload} if method == "POST" else {}),
                    ))
                    await asyncio.wait_for(checkout_started.wait(), timeout=2)
                    request.cancel()
                    await asyncio.gather(request, return_exceptions=True)
                    # Real socket cancellation must release the server's
                    # queued session while its DB connection is still held.
                    await asyncio.wait_for(session_closed.wait(), timeout=1)
    finally:
        if request is not None and not request.done():
            request.cancel()
            await asyncio.gather(request, return_exceptions=True)
        await engine.dispose()
