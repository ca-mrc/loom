"""Worker startup's actual shared CP transport does not queue model authority."""
from __future__ import annotations

import asyncio
import json
from uuid import uuid4

import pytest

from loom_worker import main_loop
from loom_worker.config import WorkerSettings


async def test_worker_shared_model_token_transport_crosses_100(
    monkeypatch: pytest.MonkeyPatch, tmp_path,
) -> None:
    monkeypatch.setenv("NO_PROXY", "127.0.0.1,localhost")
    monkeypatch.setenv("no_proxy", "127.0.0.1,localhost")
    count = 101
    arrived, release = asyncio.Event(), asyncio.Event()
    received = 0
    handlers: set[asyncio.Task] = set()

    async def token_endpoint(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        nonlocal received
        task = asyncio.current_task()
        assert task is not None
        handlers.add(task)
        try:
            head = await reader.readuntil(b"\r\n\r\n")
            assert head.startswith(b"POST /admin/step-tokens HTTP/1.1")
            assert b"Authorization: Bearer worker-fixture\r\n" in head
            headers = dict(line.split(":", 1) for line in head.decode().lower().split("\r\n")[1:] if ":" in line)
            body = json.loads(await reader.readexactly(int(headers["content-length"])))
            assert body["step_id"] == "agent"
            received += 1
            if received == count:
                arrived.set()
            await release.wait()
            payload = b'{"token":"step-fixture"}'
            writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\nContent-Length: " + str(len(payload)).encode() + b"\r\n\r\n" + payload)
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()
            handlers.discard(task)

    server = await asyncio.start_server(token_endpoint, "127.0.0.1", 0, backlog=count*2)
    origin = f"http://127.0.0.1:{server.sockets[0].getsockname()[1]}"

    class FinishedMintCheckError(Exception):
        pass

    async def verify_before_registration(**kwargs) -> None:
        client = kwargs["cp_client"]
        calls = [asyncio.create_task(client.mint_step_token(
            team_id=uuid4(), trial_id=uuid4(), step_id="agent", ttl_sec=600,
        )) for _ in range(count)]
        try:
            try:
                await asyncio.wait_for(arrived.wait(), 3)
            except TimeoutError:
                pytest.fail(f"only {received}/{count} model token exchanges reached CP")
            release.set()
            assert await asyncio.gather(*calls) == ["step-fixture"] * count
            assert await client.mint_step_token(team_id=uuid4(), trial_id=uuid4(), step_id="agent", ttl_sec=600) == "step-fixture"
        finally:
            release.set()
            for task in calls:
                task.cancel()
            await asyncio.gather(*calls, return_exceptions=True)
        raise FinishedMintCheckError

    # Stop before registration/claims; the model-authority HTTP client creation,
    # bearer, public mint method and real HTTP transport remain production code.
    monkeypatch.setattr(main_loop, "_register_worker_with_retry", verify_before_registration)
    monkeypatch.setattr(main_loop, "install_signal_handlers", lambda _: None)
    monkeypatch.setattr(main_loop, "_worker_cgroup_parent", lambda _: None)
    monkeypatch.setattr(main_loop, "_configure_blocking_io_executor", lambda _: None)
    monkeypatch.setattr(main_loop, "_log_docker_registry_auth_summary", lambda: None)
    try:
        with pytest.raises(FinishedMintCheckError):
            await main_loop.run_worker(WorkerSettings(
                control_plane_url=origin, gateway_url=origin, token="worker-fixture",
                trajectory_cache_dir=tmp_path / "cache",
                minio_access_key="fixture", minio_secret_key="fixture",
            ))
    finally:
        release.set()
        server.close()
        await server.wait_closed()
        await asyncio.gather(*handlers, return_exceptions=True)
