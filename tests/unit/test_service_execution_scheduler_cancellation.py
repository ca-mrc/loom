"""A readiness completion must not lose a scheduler shutdown request."""
from __future__ import annotations

import asyncio
import socket
from types import SimpleNamespace

import psycopg.waiting as waiting
import pytest

from loom_control_plane import service_execution_scheduler as scheduler


@pytest.mark.parametrize(("cancel_at", "global_mode", "selected"), [
    ("verifier_commit", True, False),
    ("verifier_commit", False, False),
    ("selection", True, False),
    ("selection", True, True),
    ("local_commit", False, False),
    ("local_commit", False, True),
])
async def test_scheduler_stops_when_readiness_consumes_cancellation(
        monkeypatch, cancel_at, global_mode, selected):
    reader, writer = socket.socketpair()
    reader.setblocking(False)
    writer.send(b"ready")
    native_wait_for = waiting.wait_for
    injected = False
    operations = []
    sessions = 0

    async def cancel_on_completion(future, timeout):
        nonlocal injected
        parent = asyncio.current_task()
        child = asyncio.ensure_future(future)
        if not injected:
            injected = True
            child.add_done_callback(lambda _: parent.cancel())
        return await native_wait_for(child, timeout)

    def readiness():
        yield waiting.WAIT_R
        return None

    async def step(name):
        operations.append(name)
        if name == cancel_at and not injected:
            await waiting.wait_async(readiness(), reader.fileno(), interval=0.01)
        await asyncio.sleep(0)

    class Session:
        def __init__(self):
            nonlocal sessions
            sessions += 1
            self.name = "verifier_commit" if sessions % 2 or global_mode else "local_commit"

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def commit(self):
            await step(self.name)

    class Selector:
        async def select_next(self):
            await step("selection")
            return object() if selected else None

    async def reserve_verifiers(*args, **kwargs):
        operations.append("reserve_verifiers")
        return []

    async def reserve_local(*args, **kwargs):
        operations.append("reserve_local")
        if selected:
            return SimpleNamespace(target_id="local-target", selected_pool_id="nebius-cpu")
        return None

    monkeypatch.setattr(waiting, "wait_for", cancel_on_completion)
    monkeypatch.setattr(scheduler, "reserve_next_verifier_executions", reserve_verifiers)
    monkeypatch.setattr(scheduler, "reserve_next_service_execution", reserve_local)
    task = asyncio.create_task(scheduler.run_service_execution_scheduler_loop(
        session_factory=Session, environment="staging", pool_id="nebius-cpu",
        image_admission_keyring=None, interval_seconds=0.01,
        maximum_deadline_seconds=7200, global_selector=Selector() if global_mode else None))
    try:
        done, _ = await asyncio.wait({task}, timeout=0.15)
        assert injected
        assert task in done, f"scheduler kept running after cancellation: {operations[:12]}"
        assert operations.count("reserve_verifiers") == 1
        if cancel_at == "verifier_commit":
            assert "selection" not in operations and "reserve_local" not in operations
        if not task.cancelled():
            assert task.result() is None
    finally:
        task.cancel()
        done, _ = await asyncio.wait({task}, timeout=1)
        assert task in done, "test cleanup must remain bounded"
        if not task.cancelled():
            task.result()
        reader.close()
        writer.close()
