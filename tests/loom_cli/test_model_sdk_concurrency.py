"""Real local-run SDK requests cross their former active connection thresholds."""
from __future__ import annotations

import asyncio
import json
import resource
from typing import Any

import pytest

from loom_cli import run_cmd


@pytest.mark.parametrize("provider,count", [("openai", 1001), ("anthropic", 1001), ("google", 101)])
async def test_local_sdk_requests_reach_held_upstream_and_close(
    monkeypatch: pytest.MonkeyPatch, provider: str, count: int,
) -> None:
    # This is a test-process OS resource requirement, never a platform/model cap.
    old_soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    required = 8192 if hard == resource.RLIM_INFINITY else min(8192, hard)
    resource.setrlimit(resource.RLIMIT_NOFILE, (max(old_soft, required), hard))
    monkeypatch.setenv("NO_PROXY", "127.0.0.1,localhost")
    monkeypatch.setenv("no_proxy", "127.0.0.1,localhost")
    arrived, release = asyncio.Event(), asyncio.Event()
    received = 0
    handlers: set[asyncio.Task[Any]] = set()
    payloads = {
        "openai": {"id": "fixture", "object": "chat.completion", "created": 0, "model": "fixture", "choices": [{"index": 0, "message": {"role": "assistant", "content": "ok"}, "finish_reason": "stop"}], "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2}},
        "anthropic": {"id": "fixture", "type": "message", "role": "assistant", "model": "fixture", "content": [{"type": "text", "text": "ok"}], "stop_reason": "end_turn", "usage": {"input_tokens": 1, "output_tokens": 1}},
        "google": {"candidates": [{"content": {"role": "model", "parts": [{"text": "ok"}]}, "finishReason": "STOP"}], "usageMetadata": {"promptTokenCount": 1, "candidatesTokenCount": 1}},
    }
    payload = json.dumps(payloads[provider]).encode()

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        nonlocal received
        task = asyncio.current_task()
        assert task is not None
        handlers.add(task)
        try:
            head = await reader.readuntil(b"\r\n\r\n")
            headers = dict(line.split(":", 1) for line in head.decode().lower().split("\r\n")[1:] if ":" in line)
            await reader.readexactly(int(headers.get("content-length", "0")))
            if provider != "google" and head.split(b" ", 2)[1] != b"/redirected":
                # Keep the SDK's existing same-origin 307/308 redirect contract.
                writer.write(b"HTTP/1.1 307 Temporary Redirect\r\nLocation: /redirected\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                await writer.drain()
                return
            received += 1
            if received == count:
                arrived.set()
            await release.wait()
            writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\nContent-Length: " + str(len(payload)).encode() + b"\r\n\r\n" + payload)
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()
            handlers.discard(task)

    server = await asyncio.start_server(handle, "127.0.0.1", 0, backlog=count * 2)
    origin = f"http://127.0.0.1:{server.sockets[0].getsockname()[1]}"
    monkeypatch.setenv("OPENAI_BASE_URL", origin + "/v1")
    monkeypatch.setenv("ANTHROPIC_BASE_URL", origin)
    if provider == "google":
        from google import genai
        from google.genai import _api_client

        # Exercise the supported HTTPX fallback rather than its already-unlimited
        # aiohttp connector. The production constructor and serializers stay real.
        monkeypatch.setattr(_api_client, "has_aiohttp", False)
        real_client = genai.Client

        def construct(**kwargs: Any) -> Any:
            options = kwargs.pop("http_options", {})
            return real_client(**kwargs, http_options={**options, "base_url": origin})

        monkeypatch.setattr(genai, "Client", construct)
    calls: list[asyncio.Task[Any]] = []
    sdk: Any = None
    try:
        async with run_cmd._build_sdk_clients({provider: "fixture-credential"}) as clients:
            sdk = clients[{"anthropic": 0, "openai": 1, "google": 2}[provider]]

            async def call() -> Any:
                if provider == "openai":
                    return await sdk.chat.completions.create(model="fixture", messages=[{"role": "user", "content": "hello"}])
                if provider == "anthropic":
                    return await sdk.messages.create(model="fixture", max_tokens=1, messages=[{"role": "user", "content": "hello"}])
                return await sdk.aio.models.generate_content(model="fixture", contents="hello")

            # Ramp connection establishment around the host's listen backlog;
            # every response stays held, so this still proves count in flight.
            for index in range(count):
                calls.append(asyncio.create_task(call()))
                if index % 16 == 15:
                    await asyncio.sleep(0.01)
            try:
                try:
                    await asyncio.wait_for(arrived.wait(), 15)
                except TimeoutError:
                    pytest.fail(f"only {received}/{count} {provider} SDK requests reached held upstream")
                release.set()
                results = await asyncio.gather(*calls)
                assert len(results) == count
                assert await call() is not None
            finally:
                release.set()
                for task in calls:
                    task.cancel()
                await asyncio.gather(*calls, return_exceptions=True)
        if provider != "google":
            assert sdk.is_closed()
    finally:
        release.set()
        server.close()
        await server.wait_closed()
        await asyncio.gather(*handlers, return_exceptions=True)
        resource.setrlimit(resource.RLIMIT_NOFILE, (old_soft, hard))
