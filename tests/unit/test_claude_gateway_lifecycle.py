"""Cancellation owns both upstream closure and the observed-call ledger."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
from starlette.requests import ClientDisconnect, Request

from loom.auth import AuthContext
from loom.db.schema import ProviderConnection
from loom.models.types import ModelSpec
from loom_llm_gateway.config import GatewaySettings
from loom_llm_gateway.routes import anthropic_dispatch
from loom_llm_gateway.routes.anthropic_dispatch import MessagesDispatch
from tests.unit.test_anthropic_chat_stream import _wire


def _native_wire():
    return (
        b'event: message_start\ndata: {"type":"message_start","message":{"id":"m","type":"message",'
        b'"role":"assistant","model":"alias","content":[],"stop_reason":null,"stop_sequence":null,'
        b'"usage":{"input_tokens":12,"output_tokens":0}}}\n\n'
        b'event: content_block_start\ndata: {"type":"content_block_start","index":0,'
        b'"content_block":{"type":"text","text":""}}\n\n'
        b'event: content_block_delta\ndata: {"type":"content_block_delta","index":0,'
        b'"delta":{"type":"text_delta","text":"ok"}}\n\n'
        b'event: content_block_stop\ndata: {"type":"content_block_stop","index":0}\n\n'
        b'event: message_delta\ndata: {"type":"message_delta","delta":{"stop_reason":"end_turn",'
        b'"stop_sequence":null},"usage":{"output_tokens":4}}\n\n'
        b'event: message_stop\ndata: {"type":"message_stop"}\n\n'
    )


class _TerminalThenTimeout(httpx.AsyncByteStream):
    """A provider can finish its message without closing the HTTP stream."""

    def __init__(self, wire):
        self.wire = wire
        self.closed = False

    async def __aiter__(self):
        yield self.wire
        raise httpx.ReadTimeout("provider left HTTP stream open after its terminal event")

    async def aclose(self):
        self.closed = True


class _SlowNativeStream(httpx.AsyncByteStream):
    def __init__(self):
        self.waiting = asyncio.Event()
        self.closed = False

    async def __aiter__(self):
        yield (b'event: message_start\ndata: {"type":"message_start","message":{"model":"alias",'
               b'"usage":{"input_tokens":12,"output_tokens":1}}}\n\n')
        self.waiting.set()
        await asyncio.Event().wait()

    async def aclose(self):
        self.closed = True


@asynccontextmanager
async def _dispatch(monkeypatch, stream, *, native=True):
    records = []

    async def record(session, **kwargs):
        await asyncio.sleep(0)
        records.append(kwargs)

    @asynccontextmanager
    async def session():
        yield object()

    monkeypatch.setattr(anthropic_dispatch, "record_call", record)
    settings = GatewaySettings(_env_file=None, db_url="postgresql+psycopg://test:test@localhost/test", llm_retry_max_attempts=1)
    app = SimpleNamespace(state=SimpleNamespace(settings=settings, session_factory=session, rate_card_cache=None))
    request = Request({"type": "http", "app": app, "headers": []})
    ctx = AuthContext(token_hash=b"x", type="step", scopes=[], team_id=uuid4(), expires_at=None,
                      trial_id=uuid4(), step_id="agent")
    row = ProviderConnection(id=uuid4(), provider_type="anthropic", pricing_source="operator-supplied",
                             pricing_data={"input_usd_per_1m": 1, "output_usd_per_1m": 2})
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda _: httpx.Response(
        200, stream=stream, headers={"content-type": "text/event-stream"},
    ))) as client:
        owner = MessagesDispatch(request, ctx, row, ModelSpec(provider="vendor", name="m"),
            {"model": "m", "stream": True}, {"model": "m", "stream": True}, native, "secret",
            "https://provider.example/v1/messages", {"x-api-key": "secret"}, client)
        yield owner, records


@pytest.mark.parametrize("native", [True, False])
async def test_terminal_delivery_already_closes_and_accounts_before_client_disconnect(monkeypatch, native):
    stream = _TerminalThenTimeout(_native_wire() if native else _wire())
    async with _dispatch(monkeypatch, stream, native=native) as (owner, records):
        response = await owner.stream()
        iterator = response.body_iterator
        try:
            assert b"event: message_stop" in await anext(iterator)
            # The client may stop reading immediately after message_stop.
            assert stream.closed and len(records) == 1
        finally:
            await iterator.aclose()
            await owner.close_stream()
    assert len(records) == 1
    assert "_loom_call_status" not in records[0]["usage"].provider_extras


async def test_cancelled_stream_closes_upstream_and_records_observed_usage_once(monkeypatch):
    stream = _SlowNativeStream()
    async with _dispatch(monkeypatch, stream) as (owner, records):
        response = await owner.stream()
        iterator = response.body_iterator
        assert b"message_start" in await anext(iterator)
        pending = asyncio.create_task(anext(iterator))
        await asyncio.wait_for(stream.waiting.wait(), 2)
        pending.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pending
        await owner.close_stream()  # Response-level cleanup is idempotent.
    assert stream.closed and len(records) == 1
    usage = records[0]["usage"]
    assert usage.input_tokens == 12 and usage.output_tokens == 1
    assert usage.provider_extras["_loom_call_status"] == "failed"
    assert usage.provider_extras["_loom_usage_status"] == "partial"
    assert usage.provider_extras["_loom_model_identity"] == {"provider": "vendor", "name": "m"}


async def test_disconnect_before_first_body_still_closes_upstream(monkeypatch):
    stream = _SlowNativeStream()
    async with _dispatch(monkeypatch, stream) as (owner, records):
        response = await owner.stream()

        async def send(message):
            raise OSError("client disconnected")

        async def receive():
            return {"type": "http.disconnect"}

        with pytest.raises(ClientDisconnect):
            await response({"type": "http", "asgi": {"spec_version": "2.4"}}, receive, send)
    assert stream.closed and len(records) == 1
    assert records[0]["usage"].provider_extras["_loom_usage_status"] == "missing"
    assert records[0]["usage"].provider_extras["_loom_call_status"] == "failed"


async def test_slow_upstream_close_cannot_discard_observed_accounting(monkeypatch):
    class SlowClose(_SlowNativeStream):
        async def aclose(self):
            self.closed = True
            await asyncio.Event().wait()

    real_timeout = anthropic_dispatch.move_on_after
    monkeypatch.setattr(anthropic_dispatch, "move_on_after", lambda _: real_timeout(0.05))
    stream = SlowClose()
    async with _dispatch(monkeypatch, stream) as (owner, records):
        response = await owner.stream()
        iterator = response.body_iterator
        await anext(iterator)
        await owner.close_stream()
        assert len(records) == 1 and records[0]["usage"].input_tokens == 12
        await iterator.aclose()
    assert stream.closed


def test_native_stream_requires_terminal_usage_before_message_stop():
    from loom.anthropic_chat_compat import AnthropicCompatibilityError
    from loom.anthropic_stream import NativeMessagesStream

    observer = NativeMessagesStream()
    observer.feed(b'data: {"type":"message_start","message":{"usage":{"input_tokens":12,"output_tokens":1}}}\n\n')
    with pytest.raises(AnthropicCompatibilityError, match="terminal"):
        observer.feed(b'data: {"type":"message_stop"}\n\n')
    assert not observer.completed
