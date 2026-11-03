"""Trial-owned local Messages relay; upstream credentials never enter the sandbox."""
from __future__ import annotations

import asyncio
import contextlib
import copy
import hashlib
import hmac
import json
import secrets
import socket
from collections.abc import AsyncIterator, Iterator
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

import httpx
import uvicorn
from anyio import CancelScope, move_on_after
from fastapi import FastAPI, HTTPException, Request
from starlette.responses import JSONResponse, Response, StreamingResponse
from starlette.types import Receive, Scope, Send

from loom.agent.direct_provider import DirectProviderConnection
from loom.anthropic_chat_compat import (
    CHAT_PROFILE,
    AnthropicCompatibilityError,
    anthropic_to_chat,
    chat_to_anthropic,
)
from loom.anthropic_chat_stream import ChatToAnthropicStream
from loom.anthropic_protocol import anthropic_api_base_url
from loom.anthropic_stream import NativeMessagesStream
from loom.attempt_deadline import AttemptDeadline
from loom.llm_http_client import create_model_http_client
from loom.models.types import ModelSpec
from loom.request_params import normalize_request_params
from loom_cli.rate_cards import RateCardTable, compute_cost_usd, load_rate_cards, lookup_entry
from loom_llm_gateway.dialect import DIALECTS

_MAX_BODY = 16 * 1024 * 1024


class _Server(uvicorn.Server):
    @contextlib.contextmanager
    def capture_signals(self) -> Iterator[None]:
        # The CLI owns process signals, not its per-trial HTTP servers.
        yield


class LocalClaudeRelay:
    def __init__(self, *, trial_id: UUID, model: ModelSpec, connection: DirectProviderConnection,
                 transport: httpx.AsyncBaseTransport | None = None, rate_cards: RateCardTable | None = None) -> None:
        if connection.api_dialect not in {"anthropic", "openai_chat"} or not connection.base_url:
            raise ValueError("Claude requires an explicit Anthropic or OpenAI Chat connection")
        self.trial_id, self.model, self.connection = trial_id, model, connection
        self.native = connection.api_dialect == "anthropic"
        # This also rejects URL credentials, ambiguous paths and query params.
        base = anthropic_api_base_url(connection.base_url) if self.native else connection.base_url.rstrip("/")
        anthropic_api_base_url(connection.base_url)
        self.url = base + ("/messages" if self.native else "/chat/completions")
        self.token = secrets.token_urlsafe(32)
        self.base_url = ""
        self._transport = transport
        # Fail before model I/O if the configured rate card is invalid.
        self._rates = rate_cards if rate_cards is not None else load_rate_cards()
        self._client: httpx.AsyncClient | None = None
        self._server: _Server | None = None
        self._serving: asyncio.Task[None] | None = None
        self._socket: socket.socket | None = None
        self._active: tuple[str, AttemptDeadline | None] | None = None
        self._calls: set[_Call] = set()
        self._rows: list[dict[str, Any]] = []
        self.app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
        self.app.add_api_route("/v1/messages", self.messages, methods=["POST"])
        self.app.add_api_route("/health", self.health, methods=["GET"])

    def _authorize(self, request: Request) -> None:
        if not hmac.compare_digest(request.headers.get("authorization", ""), "Bearer " + self.token):
            raise HTTPException(401, "invalid local relay token")

    async def health(self, request: Request) -> dict[str, str]:
        self._authorize(request)
        return {"status": "ready"}

    async def start(self, *, host: str) -> None:
        if self._server is not None:
            raise RuntimeError("local Claude relay already started")
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket = sock
        try:
            sock.bind((host, 0))
            sock.listen(128)
            sock.setblocking(False)
            self.base_url = f"http://{host}:{sock.getsockname()[1]}"
            self._client = create_model_http_client(timeout_sec=600, trust_env=False,
                follow_redirects=False, transport=self._transport)
            self._server = _Server(uvicorn.Config(self.app, log_level="critical", access_log=False,
                lifespan="off", timeout_graceful_shutdown=3, ws="none"))
            self._serving = asyncio.create_task(self._server.serve(sockets=[sock]))
            async with asyncio.timeout(5):
                while not self._server.started:
                    if self._serving.done():
                        await self._serving
                        raise RuntimeError("local Claude relay failed to start")
                    await asyncio.sleep(.01)
        except BaseException:
            await self.aclose()
            raise

    def activate(self, *, step_id: str, deadline: AttemptDeadline | None = None) -> None:
        if self._client is None or self._active is not None or self._calls:
            raise RuntimeError("local Claude relay cannot activate in its current state")
        self._active = (step_id, deadline)

    async def deactivate(self) -> None:
        self._active = None
        calls = tuple(self._calls)
        for call in calls:
            if call.task is not None and call.task is not asyncio.current_task():
                call.task.cancel()
        if calls:
            with CancelScope(shield=True), move_on_after(5):
                await asyncio.gather(*(call.task for call in calls if call.task is not None
                                       and call.task is not asyncio.current_task()), return_exceptions=True)
            for call in calls:
                await call.close()

    async def aclose(self) -> None:
        try:
            await self.deactivate()
        finally:
            if self._server is not None:
                self._server.should_exit = True
            if self._serving is not None:
                try:
                    async with asyncio.timeout(5):
                        await self._serving
                except (TimeoutError, asyncio.CancelledError):
                    self._serving.cancel()
                    await asyncio.gather(self._serving, return_exceptions=True)
                self._serving = None
            if self._socket is not None:
                self._socket.close()
                self._socket = None
            if self._client is not None:
                await self._client.aclose()
                self._client = None

    async def get_trial_llm_calls(self, trial_id: UUID) -> list[dict[str, Any]]:
        if trial_id != self.trial_id:
            raise ValueError("local relay ledger belongs to another trial")
        return copy.deepcopy(self._rows)

    def _record(self, call: _Call) -> None:
        usage = DIALECTS["anthropic" if self.native else "openai_chat"].extract_tokens(call.body)
        extras = dict(usage.provider_extras)
        extras.update(_loom_model_identity=self.model.model_dump(exclude_defaults=True, exclude_none=True),
                      _loom_gateway_route="native_anthropic" if self.native else "anthropic_to_chat")
        if not self.native:
            extras["_loom_compatibility_profile"] = CHAT_PROFILE
        if call.failure:
            extras.update(_loom_call_status="failed", _loom_failure_category=call.failure,
                          _loom_usage_status="partial" if call.body.get("usage") else "missing")
        provider, name = self.model.provider, self.model.name
        if provider == "local":
            server, _, name = name.partition("/")
            provider = "local:" + server
        cost, fingerprint = 0.0, "unpriced"
        try:
            entry = lookup_entry(self._rates, provider=provider, model=name)
        except KeyError:
            extras["_loom_cost_confidence"] = "unavailable"
        else:
            uncached = usage.input_tokens
            if usage.provider_extras.get("_loom_input_includes_cache"):
                uncached = max(0, uncached - usage.cached_input_tokens - usage.cache_write_tokens)
            cost = compute_cost_usd(entry, input_tokens=uncached, output_tokens=usage.output_tokens,
                cached_input_tokens=usage.cached_input_tokens, cache_write_tokens=usage.cache_write_tokens)
            fingerprint = hashlib.sha256(repr(entry).encode()).hexdigest()
            extras["_loom_cost_confidence"] = "configured"
        self._rows.append({"id": str(uuid4()), "trial_id": str(self.trial_id), "step_id": call.step_id,
            "captured_at": datetime.now(UTC).isoformat(), "dialect": "anthropic_facade", "model": self.model.name,
            "requested_model": self.connection.model.name, "response_model": call.body.get("model"),
            "input_tokens": usage.input_tokens, "output_tokens": usage.output_tokens, "cost_usd": cost,
            "rate_card_hash": fingerprint, "provider_extras": extras,
            "request_params": normalize_request_params(call.payload)})

    async def messages(self, request: Request) -> Response:
        self._authorize(request)
        active = self._active
        if active is None:
            raise HTTPException(403, "local model authority is closed")
        raw = bytearray()
        async for chunk in request.stream():
            raw.extend(chunk)
            if len(raw) > _MAX_BODY:
                raise HTTPException(413, "Messages request exceeds size limit")
        try:
            payload = json.loads(raw)
        except ValueError as exc:
            raise HTTPException(400, "invalid Messages JSON") from exc
        if not isinstance(payload, dict) or payload.get("model") != self.connection.model.name:
            raise HTTPException(403, "local relay only serves the selected model")
        try:
            upstream_payload = payload if self.native else anthropic_to_chat(payload)
        except AnthropicCompatibilityError as exc:
            raise HTTPException(400, str(exc)) from exc
        # Closing authority while a slow body was being read must not dispatch.
        if self._active is not active:
            raise HTTPException(403, "local model authority is closed")
        call = _Call(self, active[0], active[1], payload)
        self._calls.add(call)
        return await call.dispatch(upstream_payload, request)


class _Call:
    def __init__(self, relay: LocalClaudeRelay, step_id: str, deadline: AttemptDeadline | None,
                 payload: dict[str, Any]) -> None:
        self.relay, self.step_id, self.deadline, self.payload = relay, step_id, deadline, payload
        self.task = asyncio.current_task()
        self.body: dict[str, Any] = {}
        self.failure: str | None = None
        self.response: httpx.Response | None = None
        self.tracker: NativeMessagesStream | ChatToAnthropicStream | None = None
        self.closed = False
        self._closed_event = asyncio.Event()

    def timeout(self) -> float:
        return self.deadline.remaining() if self.deadline is not None else 600

    async def dispatch(self, payload: dict[str, Any], request: Request) -> Response:
        relay = self.relay
        headers = {"content-type": "application/json"}
        if relay.native:
            headers["anthropic-version"] = request.headers.get("anthropic-version", "2023-06-01")
            if request.headers.get("anthropic-beta"):
                headers["anthropic-beta"] = request.headers["anthropic-beta"]
            if relay.connection.api_key != "EMPTY":
                headers["x-api-key"] = relay.connection.api_key
        elif relay.connection.api_key != "EMPTY":
            headers["authorization"] = "Bearer " + relay.connection.api_key
        streaming = bool(self.payload.get("stream"))
        try:
            assert relay._client is not None
            async with asyncio.timeout(self.timeout()):
                upstream = relay._client.build_request("POST", relay.url, headers=headers, json=payload)
                self.response = await relay._client.send(upstream, stream=True, follow_redirects=False)
                if self.response.status_code != 200:
                    status = self.response.status_code
                    self.failure = "upstream_http_4xx" if 400 <= status < 500 else "upstream_http_5xx" if status >= 500 else "upstream_http_error"
                    raise HTTPException(status if status >= 400 else 502, "upstream Messages request failed")
                if streaming:
                    self.tracker = NativeMessagesStream() if relay.native else ChatToAnthropicStream(model=relay.connection.model.name)
                    return _StreamingCall(self)
                raw = bytearray()
                async for chunk in self.response.aiter_bytes():
                    raw.extend(chunk)
                    if len(raw) > _MAX_BODY:
                        raise AnthropicCompatibilityError("Messages response exceeds size limit")
                body = json.loads(raw)
                if not isinstance(body, dict):
                    raise AnthropicCompatibilityError("invalid Messages response")
                self.body = body
                if relay.native:
                    if DIALECTS["anthropic"].extract_tokens(body).provider_extras.get("_loom_usage_status"):
                        raise AnthropicCompatibilityError("missing Messages usage")
                    output = body
                else:
                    output = chat_to_anthropic(body, model=relay.connection.model.name)
                return JSONResponse(output)
        except HTTPException:
            raise
        except (TimeoutError, httpx.TimeoutException) as exc:
            self.failure = "upstream_timeout"
            raise HTTPException(504, "local Messages request timed out") from exc
        except httpx.HTTPError as exc:
            self.failure = "upstream_transport"
            raise HTTPException(502, "local Messages transport failed") from exc
        except ValueError as exc:
            self.failure = "upstream_protocol"
            raise HTTPException(502, "local Messages protocol failed") from exc
        except BaseException:
            self.failure = "client_cancelled"
            raise
        finally:
            if self.tracker is None:
                await self.close()

    async def iterate(self) -> AsyncIterator[bytes]:
        assert self.response is not None and self.tracker is not None
        try:
            async with asyncio.timeout(self.timeout()):
                async for chunk in self.response.aiter_bytes():
                    converted = self.tracker.feed(chunk)
                    if converted:
                        yield converted
                self.tracker.finish()
        except (ValueError, TimeoutError, httpx.HTTPError) as exc:
            self.failure = ("upstream_timeout" if isinstance(exc, (TimeoutError, httpx.TimeoutException)) else
                            "upstream_transport" if isinstance(exc, httpx.HTTPError) else "upstream_protocol")
            yield b'event: error\ndata: {"type":"error","error":{"type":"api_error","message":"local Messages stream failed"}}\n\n'
        except BaseException:
            self.failure = "client_cancelled"
            raise
        finally:
            await self.close()

    async def close(self) -> None:
        if self.closed:
            with CancelScope(shield=True), move_on_after(4):
                await self._closed_event.wait()
            return
        self.closed = True
        try:
            with CancelScope(shield=True), move_on_after(3):
                if self.response is not None:
                    await self.response.aclose()
        finally:
            if self.tracker is not None:
                self.body = {"usage": self.tracker.usage, "model": self.tracker.response_model}
                if not self.tracker.completed:
                    self.failure = self.failure or "client_cancelled"
            try:
                self.relay._record(self)
            finally:
                self.relay._calls.discard(self)
                self._closed_event.set()


class _StreamingCall(StreamingResponse):
    def __init__(self, call: _Call) -> None:
        super().__init__(call.iterate(), media_type="text/event-stream")
        self.call = call

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        try:
            await super().__call__(scope, receive, send)
        finally:
            await self.call.close()
