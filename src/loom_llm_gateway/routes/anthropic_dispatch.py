"""HTTP ownership and authoritative accounting for connection-bound Messages."""

from __future__ import annotations

import asyncio
import json
import logging
import sys
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from typing import Any

import httpx
from anyio import CancelScope, move_on_after
from fastapi import HTTPException, Request
from starlette.responses import StreamingResponse
from starlette.types import Receive, Scope, Send

from loom.anthropic_chat_compat import CHAT_PROFILE, AnthropicCompatibilityError, chat_to_anthropic
from loom.anthropic_chat_stream import ChatToAnthropicStream
from loom.anthropic_stream import NativeMessagesStream
from loom.auth import AuthContext
from loom.db.schema import ProviderConnection
from loom.models.types import ModelSpec
from loom_llm_gateway.attempt_deadline import (
    AttemptDeadlineReachedError,
    raise_deadline_http_exception,
    request_attempt_deadline,
    upstream_timeout,
)
from loom_llm_gateway.dialect import DIALECTS, TokenUsage
from loom_llm_gateway.dispatch_audit import request_dispatch_audit
from loom_llm_gateway.llm_calls import record_call
from loom_llm_gateway.rate_card import CostEstimate
from loom_llm_gateway.request_params import normalize_request_params
from loom_llm_gateway.retry import send_with_retry
from loom_llm_gateway.routes._facade_common import (
    compute_facade_cost_estimate,
    http_failure_category,
    redact_api_key,
    token_usage_with_cost_metadata,
)

_LOG = logging.getLogger(__name__)


@dataclass
class MessagesDispatch:
    request: Request
    ctx: AuthContext
    row: ProviderConnection
    identity: ModelSpec
    payload: dict[str, Any]
    upstream_payload: dict[str, Any]
    native: bool
    api_key: str = field(repr=False)
    url: str
    headers: dict[str, str] = field(repr=False)
    upstream: httpx.AsyncClient
    _attempt: int = 1
    _recorded: bool = False
    _stream_cm: Any = None
    _tracker: NativeMessagesStream | ChatToAnthropicStream | None = None
    _failure: str | None = None
    _status: int | None = None

    async def _record(self, body: dict[str, Any], *, failure: str | None = None) -> None:
        if self._recorded:
            return
        dialect = "anthropic" if self.native else "openai_chat"
        usage = DIALECTS[dialect].extract_tokens(body)
        extras = dict(usage.provider_extras)
        extras.update({
            "_loom_model_identity": self.identity.model_dump(exclude_defaults=True, exclude_none=True),
            "_loom_gateway_route": "native_anthropic" if self.native else "anthropic_to_chat",
        })
        if not self.native:
            extras["_loom_compatibility_profile"] = CHAT_PROFILE
        if failure:
            extras.update(_loom_call_status="failed", _loom_failure_category=failure)
            if body.get("usage") is not None:
                extras["_loom_usage_status"] = "partial"
            if self._status is not None:
                extras["_loom_failure_status_code"] = self._status
        usage = TokenUsage(usage.input_tokens, usage.output_tokens, extras)
        estimate = (CostEstimate(0.0, "failed-upstream", "unpriced", "unavailable", None,
                                 unpriced_reason="missing_usage")
                    if failure and body.get("usage") is None else
                    await compute_facade_cost_estimate(self.row, self.identity.name, usage,
                        rate_card_cache=self.request.app.state.rate_card_cache))
        usage = token_usage_with_cost_metadata(usage, estimate)
        assert self.ctx.team_id is not None and self.ctx.trial_id is not None and self.ctx.step_id is not None
        async with self.request.app.state.session_factory() as session:
            await record_call(session, team_id=self.ctx.team_id, trial_id=self.ctx.trial_id, step_id=self.ctx.step_id,
                dialect="anthropic_facade", model=self.identity.name, usage=usage, cost_usd=estimate.cost_usd,
                rate_card_hash=estimate.rate_card_hash, provider=self.row.provider_type, attempt=self._attempt,
                request_params=normalize_request_params(self.payload), auth_context=self.ctx,
                response_model=body.get("model") if isinstance(body.get("model"), str) else None)
        self._recorded = True

    async def complete(self) -> dict[str, Any]:
        settings = self.request.app.state.settings
        body: dict[str, Any] = {}
        failure: str | None = None
        try:
            outcome = await send_with_retry(
                lambda: self.upstream.post(self.url, json=self.upstream_payload, headers=self.headers,
                    timeout=upstream_timeout(self.request, settings.upstream_timeout_sec), follow_redirects=False),
                settings=settings, dialect="facade_anthropic", deadline=request_attempt_deadline(self.request),
                dispatch_audit=request_dispatch_audit(self.request, dialect="facade_anthropic", provider_connection_id=self.row.id),
            )
            self._attempt = outcome.attempt
            response = outcome.response
            if response.status_code >= 300:
                self._status = response.status_code
                failure = http_failure_category(response.status_code)
                raise HTTPException(status_code=response.status_code if response.status_code >= 400 else 502,
                    detail=f"upstream returned {response.status_code}: {redact_api_key(response.text, self.api_key)[:4096]}")
            parsed = response.json()
            if not isinstance(parsed, dict):
                raise AnthropicCompatibilityError("upstream Messages response must be an object")
            body = parsed
            if deadline := request_attempt_deadline(self.request):
                deadline.require_remaining()
            if self.native:
                usage = DIALECTS["anthropic"].extract_tokens(body)
                if usage.provider_extras.get("_loom_usage_status"):
                    raise AnthropicCompatibilityError("upstream returned missing/invalid usage block")
                return body
            return chat_to_anthropic(body, model=self.identity.name)
        except AttemptDeadlineReachedError as exc:
            failure = "attempt_deadline_reached"
            raise_deadline_http_exception(exc)
        except httpx.TimeoutException as exc:
            failure = "upstream_timeout"
            raise HTTPException(status_code=504, detail="upstream Messages request timeout") from exc
        except httpx.RequestError as exc:
            failure = "upstream_transport"
            raise HTTPException(status_code=502, detail="upstream Messages transport failed") from exc
        except (ValueError, UnicodeError) as exc:
            failure = "upstream_protocol"
            raise HTTPException(status_code=502, detail=redact_api_key(str(exc), self.api_key)) from exc
        except asyncio.CancelledError:
            failure = "client_cancelled"
            raise
        finally:
            with CancelScope(shield=True), move_on_after(10) as cleanup:
                await self._record(body, failure=failure)
            if cleanup.cancel_called:
                _LOG.error("Messages accounting cleanup timed out trial_id=%s", self.ctx.trial_id)

    async def stream(self) -> StreamingResponse:
        self._tracker = NativeMessagesStream() if self.native else ChatToAnthropicStream(model=self.identity.name)
        settings = self.request.app.state.settings
        deadline = request_attempt_deadline(self.request)
        upstream_cm = self.upstream.stream("POST", self.url, json=self.upstream_payload,
            headers=self.headers, timeout=upstream_timeout(self.request, settings.upstream_timeout_sec), follow_redirects=False)
        audit = request_dispatch_audit(self.request, dialect="facade_anthropic_stream", provider_connection_id=self.row.id)
        cm = (upstream_cm if audit is None else audit.stream(lambda: upstream_cm, deadline=deadline,
              completed=lambda: self._tracker is not None and self._tracker.completed))
        try:
            response = await cm.__aenter__() if deadline is None or audit is not None else await deadline.run(cm.__aenter__)
            self._stream_cm = cm
            if deadline is not None:
                deadline.require_remaining()
            if response.status_code >= 300:
                self._status = response.status_code
                self._failure = http_failure_category(response.status_code)
                raw = await response.aread() if deadline is None else await deadline.run(response.aread)
                raise HTTPException(status_code=response.status_code if response.status_code >= 400 else 502,
                    detail=f"upstream returned {response.status_code}: {redact_api_key(raw.decode(errors='replace'), self.api_key)[:4096]}")
            return _OwnedMessagesResponse(self, self._iterate(response), media_type="text/event-stream")
        except AttemptDeadlineReachedError as exc:
            self._failure = "attempt_deadline_reached"
            await self.close_stream()
            raise_deadline_http_exception(exc)
        except httpx.TimeoutException as exc:
            self._failure = "upstream_timeout"
            await self.close_stream()
            raise HTTPException(status_code=504, detail="upstream Messages request timeout") from exc
        except httpx.RequestError as exc:
            self._failure = "upstream_transport"
            await self.close_stream()
            raise HTTPException(status_code=502, detail="upstream Messages transport failed") from exc
        except BaseException:
            self._failure = self._failure or "client_cancelled"
            await self.close_stream()
            raise

    async def _iterate(self, response: httpx.Response) -> AsyncIterator[bytes]:
        assert self._tracker is not None
        try:
            iterator = response.aiter_bytes()
            while True:
                try:
                    deadline = request_attempt_deadline(self.request)
                    chunk = await iterator.__anext__() if deadline is None else await deadline.anext(iterator)
                except StopAsyncIteration:
                    break
                converted = self._tracker.feed(chunk)
                if converted:
                    yield converted
            self._tracker.finish()
        except (AnthropicCompatibilityError, httpx.HTTPError, AttemptDeadlineReachedError) as exc:
            self._failure = ("attempt_deadline_reached" if isinstance(exc, AttemptDeadlineReachedError)
                             else "upstream_timeout" if isinstance(exc, httpx.TimeoutException)
                             else "upstream_transport" if isinstance(exc, httpx.RequestError)
                             else "upstream_protocol")
            yield b"event: error\ndata: " + json.dumps({"type": "error", "error": {
                "type": "api_error", "message": "Loom Messages stream failed: " + self._failure,
            }}).encode() + b"\n\n"
        except BaseException:
            self._failure = "client_cancelled"
            raise
        finally:
            await self.close_stream()

    async def close_stream(self) -> None:
        """Close and account even if the client disconnects before iteration starts."""
        failure_info = sys.exc_info()
        try:
            with CancelScope(shield=True), move_on_after(5) as close_timeout:
                if self._stream_cm is not None:
                    cm, self._stream_cm = self._stream_cm, None
                    await cm.__aexit__(*failure_info)
            if close_timeout.cancel_called:
                _LOG.error("Messages upstream close timed out trial_id=%s", self.ctx.trial_id)
        finally:
            # Closing a broken upstream must not consume the separate budget
            # needed to persist usage already observed from it.
            with CancelScope(shield=True), move_on_after(5) as accounting_timeout:
                tracker = self._tracker
                failure = self._failure or (None if tracker is not None and tracker.completed else "client_cancelled")
                await self._record({"usage": tracker.usage if tracker else None,
                                    "model": tracker.response_model if tracker else None}, failure=failure)
            if accounting_timeout.cancel_called:
                _LOG.error("Messages accounting cleanup timed out trial_id=%s", self.ctx.trial_id)


class _OwnedMessagesResponse(StreamingResponse):
    def __init__(self, owner: MessagesDispatch, content: AsyncIterator[bytes], *, media_type: str) -> None:
        super().__init__(content, media_type=media_type)
        self.owner = owner

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        try:
            await super().__call__(scope, receive, send)
        finally:
            await self.owner.close_stream()
