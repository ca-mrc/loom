"""Family evolver requests reach a held model endpoint without a shared pool cap."""
from __future__ import annotations

import asyncio
import json

import pytest

from loom_family_orchestrator.gateway_client import OrchestratorGatewayClient


@pytest.mark.parametrize("hold_token_exchange", [False, True])
async def test_family_evolver_allows_101_inflight_and_recovers(
    monkeypatch: pytest.MonkeyPatch, hold_token_exchange: bool,
) -> None:
    monkeypatch.setenv("NO_PROXY", "127.0.0.1,localhost")
    monkeypatch.setenv("no_proxy", "127.0.0.1,localhost")
    expected = 101
    arrived = asyncio.Event()
    release = asyncio.Event()
    bodies: list[dict[str, object]] = []
    token_arrivals = 0
    handlers: set[asyncio.Task[None]] = set()

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        nonlocal token_arrivals
        task = asyncio.current_task()
        assert task is not None
        handlers.add(task)
        try:
            head = await reader.readuntil(b"\r\n\r\n")
            lines = head.decode().split("\r\n")
            headers = dict(line.split(": ", 1) for line in lines[1:] if ": " in line)
            body = json.loads(await reader.readexactly(int(headers["Content-Length"])))
            if lines[0].split()[1] == "/admin/step-tokens":
                assert headers["Authorization"] == "Bearer evolve-fixture"
                token_arrivals += 1
                if hold_token_exchange:
                    if token_arrivals == expected:
                        arrived.set()
                    await release.wait()
                payload = b'{"token":"step-fixture"}'
            else:
                assert headers["Authorization"] == "Bearer step-fixture"
                bodies.append(body)
                if not hold_token_exchange and len(bodies) == expected:
                    arrived.set()
                await release.wait()
                payload = b'{"choices":[]}'
            writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\nContent-Length: " + str(len(payload)).encode() + b"\r\n\r\n" + payload)
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()
            handlers.discard(task)

    server = await asyncio.start_server(handle, "127.0.0.1", 0, backlog=expected * 2)
    port = server.sockets[0].getsockname()[1]
    origin = f"http://127.0.0.1:{port}"
    client = OrchestratorGatewayClient(
        base_url=origin, control_plane_url=origin, worker_token="evolve-fixture",
    )

    async def call() -> dict[str, object]:
        return await client.chat_completion(
            model="openai/fixture", messages=[{"role": "user", "content": "hello"}],
            dialect="family_evolver", max_tokens=1, timeout_sec=10,
            team_id="fixture-team", trial_id="fixture-trial", provider_connection_id="fixture-provider",
        )

    calls = [asyncio.create_task(call()) for _ in range(expected)]
    try:
        try:
            await asyncio.wait_for(arrived.wait(), 3)
        except TimeoutError:
            pytest.fail(f"only {token_arrivals if hold_token_exchange else len(bodies)}/{expected} evolver requests reached held {'token exchange' if hold_token_exchange else 'model upstream'}")
        release.set()
        assert all(result == {"choices": []} for result in await asyncio.gather(*calls))
        assert len(bodies) == token_arrivals == expected
        for body in bodies:
            loom = body["loom"]
            assert isinstance(loom, dict)
            assert loom["provider_connection_id"] == "fixture-provider"
        assert await call() == {"choices": []}
    finally:
        release.set()
        for task in calls:
            task.cancel()
        await asyncio.gather(*calls, return_exceptions=True)
        await client.aclose()
        server.close()
        await server.wait_closed()
        await asyncio.gather(*handlers, return_exceptions=True)
