"""Stream iterator boundaries; real PostgreSQL/MinIO coverage lives in integration."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Awaitable, Callable
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from loom_service.routes import trajectory


async def _stream(
    monkeypatch: pytest.MonkeyPatch,
    read: Callable[..., Awaitable[list[dict]]],
    *,
    state: str = "succeeded",
    disconnected: Callable[[], Awaitable[bool]] | None = None,
) -> tuple[AsyncIterator[bytes], SimpleNamespace]:
    trial = SimpleNamespace(id=uuid4(), state=state)
    subscription = SimpleNamespace(closed=False, _push_mode=False)

    class Session:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            pass

        async def execute(self, _query):
            return SimpleNamespace(scalar_one_or_none=lambda: trial)

    class Listen:
        _push_mode = False

        async def __aenter__(self):
            return subscription

        async def __aexit__(self, *_args):
            subscription.closed = True

    listener = Listen()
    subscription.__aexit__ = listener.__aexit__
    monkeypatch.setattr(trajectory, "require_scope", lambda *_args: None)
    monkeypatch.setattr(trajectory, "_load_trial", AsyncMock(return_value=trial))
    monkeypatch.setattr(trajectory, "_ListenSubscription", lambda *_args: listener)
    monkeypatch.setattr(trajectory, "_read_events_with_minio_fallback", read)
    request = SimpleNamespace(
        app=SimpleNamespace(state=SimpleNamespace(
            settings=SimpleNamespace(trajectories_bucket="unused", db_url="postgresql://unused"),
            session_factory=Session, minio_client=None,
        )),
        is_disconnected=disconnected or AsyncMock(return_value=False),
    )
    response = await trajectory.stream_events(request, (None, None), trial.id)
    return response.body_iterator, subscription


def _payload(frame: bytes) -> dict:
    return json.loads(next(line[6:] for line in frame.splitlines() if line.startswith(b"data: ")))


async def test_terminal_backlog_respects_disconnect_between_events(monkeypatch):
    delivered = []

    async def disconnected():
        return len(delivered) >= 7

    async def read(*_args, **_kwargs):
        return [{"seq": seq} for seq in range(200)]

    stream, subscription = await _stream(monkeypatch, read, disconnected=disconnected)
    frames = []
    async for frame in stream:
        frames.append(frame)
        if frame.startswith(b"id:"):
            delivered.append(_payload(frame)["seq"])
    assert delivered == list(range(7))
    assert not any(b"event: complete" in frame for frame in frames)
    assert subscription.closed


async def test_terminal_backlog_respects_deadline_between_events(monkeypatch):
    clock = SimpleNamespace(now=0.0)
    monkeypatch.setattr(trajectory.asyncio, "get_running_loop", lambda: SimpleNamespace(time=lambda: clock.now))
    monkeypatch.setattr(trajectory, "_DEFAULT_SSE_MAX_CONNECTION_SEC", 10.0)

    async def read(*_args, **_kwargs):
        return [{"seq": seq} for seq in range(200)]

    stream, subscription = await _stream(monkeypatch, read)
    frames = []
    async for frame in stream:
        frames.append(frame)
        if frame.startswith(b"id:"):
            clock.now += 1.0
    assert [_payload(frame)["seq"] for frame in frames if frame.startswith(b"id:")] == list(range(10))
    assert frames[-1].startswith(b"event: reconnect")
    assert _payload(frames[-1]) == {"reason": "max_connection_sec", "last_seq": 9}
    assert subscription.closed


async def test_backlog_yields_to_other_tasks_before_reading_next_page(monkeypatch):
    progressed = asyncio.Event()

    async def read(*_args, after_seq, **_kwargs):
        if after_seq < 0:
            asyncio.get_running_loop().call_soon(progressed.set)
            return [{"seq": seq} for seq in range(200)]
        assert progressed.is_set(), "backlog drain starved the event loop"
        return []

    stream, subscription = await _stream(monkeypatch, read)
    frames = [frame async for frame in stream]
    assert _payload(frames[-1]) == {"final_state": "succeeded", "last_seq": 199}
    assert subscription.closed


@pytest.mark.parametrize("state", ["succeeded", "running"])
async def test_empty_read_cannot_wait_or_complete_after_connection_deadline(monkeypatch, state):
    clock = SimpleNamespace(now=0.0)
    monkeypatch.setattr(trajectory.asyncio, "get_running_loop", lambda: SimpleNamespace(time=lambda: clock.now))
    monkeypatch.setattr(trajectory, "_DEFAULT_SSE_MAX_CONNECTION_SEC", 10.0)

    async def read(*_args, **_kwargs):
        clock.now = 10.0
        return []

    async def unexpected_wait(*_args):
        pytest.fail("polling wait started after the connection deadline")

    monkeypatch.setattr(trajectory.asyncio, "sleep", unexpected_wait)
    stream, subscription = await _stream(monkeypatch, read, state=state)
    frames = [frame async for frame in stream]
    assert frames[-1].startswith(b"event: reconnect")
    assert _payload(frames[-1]) == {"reason": "max_connection_sec", "last_seq": -1}
    assert subscription.closed


@pytest.mark.parametrize("second_page", [[{"seq": 0}], [{"seq": 2}, {"seq": 1}]])
async def test_invalid_event_order_closes_without_duplicate_or_false_completion(monkeypatch, second_page):
    async def read(*_args, after_seq, **_kwargs):
        return [{"seq": 0}] if after_seq < 0 else second_page

    stream, subscription = await _stream(monkeypatch, read)
    frames = []
    with pytest.raises(ValueError, match="sequence"):
        async for frame in stream:
            frames.append(frame)
    seqs = [_payload(frame)["seq"] for frame in frames if frame.startswith(b"id:")]
    assert seqs == sorted(set(seqs))
    assert not any(b"event: complete" in frame for frame in frames)
    assert subscription.closed


async def test_cancelled_backlog_read_closes_subscription(monkeypatch):
    reading = asyncio.Event()

    async def read(*_args, **_kwargs):
        reading.set()
        await asyncio.Event().wait()
        return []

    stream, subscription = await _stream(monkeypatch, read)
    assert await anext(stream) == b": stream open\n\n"
    task = asyncio.create_task(anext(stream))
    await reading.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert subscription.closed
