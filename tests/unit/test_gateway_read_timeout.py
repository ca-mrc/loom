import asyncio
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from types import SimpleNamespace

import httpx
import pytest
from pydantic import ValidationError
from starlette.requests import Request

from loom_llm_gateway.attempt_deadline import GatewayAttemptDeadline, upstream_timeout
from loom_llm_gateway.config import GatewaySettings
from loom_llm_gateway.retry import RetryBudgetExceededError, send_with_retry


@pytest.mark.parametrize(
    ("remaining", "expected_other", "expected_read"),
    [(300.0, 200.0, 250.0), (50.0, 50.0, 50.0)],
)
def test_read_timeout_is_independent_and_still_fenced_by_attempt(
    remaining: float, expected_other: float, expected_read: float,
) -> None:
    deadline = GatewayAttemptDeadline(remaining, clock=lambda: 0.0)
    timeout = deadline.httpx_timeout(200, read_seconds=250)
    assert timeout.read == expected_read
    assert timeout.connect == timeout.write == timeout.pool == expected_other


def test_read_override_applies_to_legacy_requests_without_an_attempt_claim() -> None:
    app = SimpleNamespace(state=SimpleNamespace(settings=SimpleNamespace(
        upstream_read_timeout_sec=250,
    )))
    request = Request({"type": "http", "method": "POST", "path": "/", "app": app})
    timeout = upstream_timeout(request, 200)
    assert isinstance(timeout, httpx.Timeout)
    assert timeout.read == 250
    assert timeout.connect == timeout.write == timeout.pool == 200


@pytest.mark.parametrize("invalid", [0, -1, float("inf"), float("nan")])
def test_read_override_rejects_unbounded_or_nonpositive_values(invalid: float) -> None:
    with pytest.raises(ValidationError):
        GatewaySettings(db_url="postgresql+psycopg://test:test@localhost/test",
                        upstream_read_timeout_sec=invalid, _env_file=None)


def test_no_read_override_keeps_an_explicit_smaller_transport_timeout() -> None:
    request = Request({"type": "http", "method": "POST", "path": "/"})
    assert upstream_timeout(request, 30) == 30


@asynccontextmanager
async def _slow_provider(delays: list[float]) -> AsyncIterator[tuple[str, list[dict]]]:
    requests: list[dict] = []
    tasks: set[asyncio.Task] = set()
    body = b'{"choices":[{"message":{"content":"ok"}}]}'

    async def serve(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        task = asyncio.current_task()
        assert task is not None
        tasks.add(task)
        try:
            headers = (await reader.readuntil(b"\r\n\r\n")).decode()
            length = next(int(line.split(":", 1)[1]) for line in headers.split("\r\n")
                          if line.lower().startswith("content-length:"))
            requests.append(json.loads(await reader.readexactly(length)))
            writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                         + f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n".encode())
            await writer.drain()
            chunks = [body] if len(delays) == 1 else [body[:10], body[10:]]
            for delay, chunk in zip(delays, chunks, strict=True):
                await asyncio.sleep(delay)
                writer.write(chunk)
                await writer.drain()
        except (ConnectionError, asyncio.IncompleteReadError):
            pass
        finally:
            writer.close()
            await writer.wait_closed()
            tasks.discard(task)

    server = await asyncio.start_server(serve, "127.0.0.1", 0)
    assert server.sockets
    try:
        yield f"http://127.0.0.1:{server.sockets[0].getsockname()[1]}", requests
    finally:
        server.close()
        await server.wait_closed()
        for task in list(tasks):
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


async def _generation_read(
    url: str, *, read_seconds: float | None, total_seconds: float = 1,
) -> httpx.Response:
    settings = SimpleNamespace(
        upstream_read_timeout_sec=read_seconds,
        llm_retry_max_attempts=1, llm_retry_base_backoff_sec=0,
        llm_retry_jitter_sec=0, llm_retry_max_backoff_sec=0,
        llm_retry_budget_sec=total_seconds,
    )
    request = Request({"type": "http", "method": "POST", "path": "/",
                       "app": SimpleNamespace(state=SimpleNamespace(settings=settings))})
    async with httpx.AsyncClient(trust_env=False) as client:
        outcome = await send_with_retry(
            lambda: client.post(url, json={"model": "unchanged", "reasoning_effort": "high",
                                          "temperature": 0.2},
                                timeout=upstream_timeout(request, 0.12)),
            settings=settings, dialect="read-policy-test",
        )
        return outcome.response


async def test_real_http_read_can_finish_without_replaying_long_generation() -> None:
    async with _slow_provider([0.18]) as (url, requests):
        with pytest.raises(httpx.ReadTimeout):
            await _generation_read(url, read_seconds=None)
        assert len(requests) == 1
    async with _slow_provider([0.18]) as (url, requests):
        response = await _generation_read(url, read_seconds=0.24)
        assert response.status_code == 200
        assert requests == [{"model": "unchanged", "reasoning_effort": "high",
                             "temperature": 0.2}]


async def test_real_http_read_limit_is_inactivity_not_whole_response_duration() -> None:
    async with _slow_provider([0.13, 0.13]) as (url, requests):
        response = await _generation_read(url, read_seconds=0.2)
        assert response.status_code == 200
        assert len(requests) == 1


async def test_longer_read_still_stops_at_original_logical_request_budget() -> None:
    async with _slow_provider([0.3]) as (url, requests):
        with pytest.raises(RetryBudgetExceededError):
            await _generation_read(url, read_seconds=0.4, total_seconds=0.15)
        assert len(requests) == 1


def test_read_override_cannot_extend_a_smaller_provider_dispatch_grant() -> None:
    app = SimpleNamespace(state=SimpleNamespace(settings=SimpleNamespace(
        upstream_read_timeout_sec=250,
    )))
    request = Request({"type": "http", "method": "POST", "path": "/", "app": app})
    timeout = upstream_timeout(request, 30, read_ceiling=30)
    assert isinstance(timeout, httpx.Timeout)
    assert timeout.read == timeout.connect == timeout.write == timeout.pool == 30
