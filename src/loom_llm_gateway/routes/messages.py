"""POST /v1/messages — Anthropic native passthrough (Plan 9 Task 7).

Per amendment A9.1, this route does NOT round-trip through LiteLLM
(which would lose cache_control, tool_use blocks, and system-block
separation). Instead, we httpx-passthrough to Anthropic's native
endpoint and forward the response verbatim. The Gateway extracts the
`usage` block for cost attribution + llm_calls insertion.

Streaming responses are forwarded byte-for-byte to the client while
SSE events are tee-parsed to extract usage from `message_start`
(input + cache token counts) and `message_delta` (final cumulative
output token count). Cost is recorded after the upstream stream
completes — or on client disconnect, with whatever usage was
accumulated up to that point. The sibling `/anthropic/v1/messages`
facade ships its own tracker (#561) keyed on per-connection
credentials; both derive the same `(input, output, cache)` totals
from the upstream stream.
"""

from __future__ import annotations

import asyncio
import json
import logging
import sys
from typing import Any

import httpx
from fastapi import APIRouter, Header, HTTPException, Request
from starlette.responses import StreamingResponse

from loom.auth import AuthContext
from loom.models.types import ModelSpec
from loom_llm_gateway.attempt_deadline import (
    AttemptDeadlineReachedError,
    enforce_request_attempt_deadline,
    raise_deadline_http_exception,
    request_attempt_deadline,
    upstream_timeout,
)
from loom_llm_gateway.config import GatewaySettings
from loom_llm_gateway.dialect import DIALECTS, TokenUsage
from loom_llm_gateway.dispatch_audit import request_dispatch_audit
from loom_llm_gateway.execution_attempt_dispatch import authorize_trial_model
from loom_llm_gateway.llm_calls import record_call, record_failed_call
from loom_llm_gateway.rate_card import (
    compute_cost_usd,
    hash_table,
    lookup_entry,
)
from loom_llm_gateway.request_params import normalize_request_params
from loom_llm_gateway.retry import send_with_retry
from loom_llm_gateway.routes._facade_common import (
    http_failure_category,
    redact_api_key,
    verify_facade_auth,
)

router = APIRouter()

ANTHROPIC_BASE_URL = "https://api.anthropic.com"

logger = logging.getLogger(__name__)


@router.post("/v1/messages")
async def messages(
    request: Request,
    payload: dict[str, Any],
    authorization: str | None = Header(default=None),
) -> Any:
    settings = request.app.state.settings
    signing_key = settings.step_jwt_signing_key.get_secret_value()

    async with request.app.state.session_factory() as session:
        ctx = await verify_facade_auth(
            session,
            authorization,
            signing_key,
            allow_execution_attempt=True,
            request=request,
        )
    assert ctx.team_id is not None
    assert ctx.step_id is not None
    request.state.execution_attempt_id = ctx.execution_attempt_id
    if ctx.execution_attempt_id is not None:
        raise HTTPException(
            status_code=403,
            detail="execution-attempt tokens are restricted to the fenced Responses route",
        )

    model_name = payload.get("model")
    if not isinstance(model_name, str) or not model_name:
        raise HTTPException(status_code=400, detail="`model` is required")
    await authorize_trial_model(request, ctx, provider="anthropic", model=model_name)
    if settings.anthropic_api_key is None:
        raise HTTPException(
            status_code=503,
            detail="anthropic_api_key not configured on Gateway",
        )
    api_key = settings.anthropic_api_key.get_secret_value()

    if payload.get("stream"):
        return await _stream_messages(
            request=request,
            payload=payload,
            ctx=ctx,
            settings=settings,
            api_key=api_key,
            model_name=model_name,
        )

    # 1. Forward to Anthropic native endpoint.
    upstream: httpx.AsyncClient = request.app.state.upstream_client
    try:
        outcome = await send_with_retry(
            lambda: upstream.post(
                f"{ANTHROPIC_BASE_URL}/v1/messages",
                json=payload,
                headers={
                    "x-api-key": api_key,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
                timeout=upstream_timeout(request, settings.upstream_timeout_sec),
            ),
            settings=settings,
            dialect="anthropic",
            deadline=request_attempt_deadline(request),
            dispatch_audit=request_dispatch_audit(
                request,
                dialect="anthropic",
            ),
        )
    except AttemptDeadlineReachedError as exc:
        raise_deadline_http_exception(exc)
    except httpx.TimeoutException as exc:
        await _record_failed_message_call(
            request=request,
            ctx=ctx,
            model_name=model_name,
            request_payload=payload,
            failure_category="upstream_timeout",
            failure_error_type=type(exc).__name__,
        )
        raise HTTPException(
            status_code=504,
            detail=f"anthropic upstream timeout: {exc}",
        ) from exc
    except httpx.RequestError as exc:
        await _record_failed_message_call(
            request=request,
            ctx=ctx,
            model_name=model_name,
            request_payload=payload,
            failure_category="upstream_transport",
            failure_error_type=type(exc).__name__,
        )
        raise HTTPException(
            status_code=502,
            detail=f"anthropic upstream request error: {type(exc).__name__}: {exc}",
        ) from exc
    upstream_response = outcome.response
    enforce_request_attempt_deadline(request)
    if upstream_response.status_code >= 400:
        excerpt = redact_api_key(upstream_response.text, api_key)
        await _record_failed_message_call(
            request=request,
            ctx=ctx,
            model_name=model_name,
            request_payload=payload,
            failure_category=http_failure_category(upstream_response.status_code),
            failure_status_code=upstream_response.status_code,
            attempt=outcome.attempt,
        )
        raise HTTPException(
            status_code=upstream_response.status_code,
            detail=f"anthropic upstream returned {upstream_response.status_code}: {excerpt}",
        )
    body: dict[str, Any] = upstream_response.json()

    # 2. Extract usage + record cost. Anthropic 200 responses always
    # include a usage block; if it's missing we treat this as a
    # surprising upstream contract violation and 502 — better than
    # silently inserting a zero-cost row.
    usage = DIALECTS["anthropic"].extract_tokens(body)
    if usage.input_tokens == 0 and usage.output_tokens == 0:
        raise HTTPException(
            status_code=502,
            detail="anthropic 200 response missing usage block; cost cannot be attributed",
        )
    table = await request.app.state.rate_card_cache.get()
    entry = lookup_entry(table, ModelSpec(provider="anthropic", name=model_name))
    cost = compute_cost_usd(
        entry,
        input_tokens=usage.input_tokens,
        output_tokens=usage.output_tokens,
        cached_input_tokens=usage.cached_input_tokens,
        cache_write_tokens=usage.cache_write_tokens,
    )
    async with request.app.state.session_factory() as session:
        await record_call(
            session,
            team_id=ctx.team_id,
            trial_id=ctx.trial_id,
            execution_attempt_id=ctx.execution_attempt_id,
            step_id=ctx.step_id,
            dialect="anthropic",
            model=model_name,
            usage=usage,
            cost_usd=cost,
            rate_card_hash=hash_table(table),
            attempt=outcome.attempt,
            request_params=normalize_request_params(payload),
        )
    return body


def _extract_stream_usage(
    event_name: str,
    data: dict[str, Any],
    accum: dict[str, int],
) -> None:
    """Update `accum` with token counts surfaced by one parsed SSE event.

    Anthropic's stream emits `input_tokens`, `cache_creation_input_tokens`,
    and `cache_read_input_tokens` on `message_start` (inside the
    embedded `message.usage` block) and a final cumulative
    `output_tokens` on `message_delta` (in the top-level `usage` block).
    """
    if event_name == "message_start":
        msg = data.get("message") or {}
        u = msg.get("usage") or {}
        accum["input_tokens"] = int(u.get("input_tokens", 0))
        v = u.get("cache_creation_input_tokens")
        if v is not None:
            accum["cache_creation_input_tokens"] = int(v)
        v = u.get("cache_read_input_tokens")
        if v is not None:
            accum["cache_read_input_tokens"] = int(v)
        # `message_start` carries an initial output_tokens (typically 1
        # for the role token); `message_delta` overwrites with the
        # cumulative final count.
        accum["output_tokens"] = int(u.get("output_tokens", 0))
    elif event_name == "message_delta":
        u = data.get("usage") or {}
        if "output_tokens" in u:
            accum["output_tokens"] = int(u["output_tokens"])


def _parse_sse_blocks(buffer: bytes) -> tuple[list[tuple[str, dict[str, Any]]], bytes]:
    """Split `buffer` on SSE event boundaries (\\n\\n), parse each
    complete block into `(event_name, data_dict)`, and return the
    parsed events plus the remaining tail."""
    events: list[tuple[str, dict[str, Any]]] = []
    while True:
        sep = buffer.find(b"\n\n")
        if sep < 0:
            break
        block, buffer = buffer[:sep], buffer[sep + 2 :]
        event_name: str | None = None
        data_chunks: list[str] = []
        for line in block.split(b"\n"):
            if line.startswith(b"event: "):
                event_name = line[len(b"event: ") :].decode("utf-8", "replace")
            elif line.startswith(b"data: "):
                data_chunks.append(line[len(b"data: ") :].decode("utf-8", "replace"))
        if event_name is None or not data_chunks:
            continue
        try:
            data = json.loads("\n".join(data_chunks))
        except json.JSONDecodeError:
            continue
        if isinstance(data, dict):
            events.append((event_name, data))
    return events, buffer


async def _stream_messages(
    *,
    request: Request,
    payload: dict[str, Any],
    ctx: AuthContext,
    settings: GatewaySettings,
    api_key: str,
    model_name: str,
) -> StreamingResponse:
    """Tee-passthrough Anthropic's SSE stream: forward upstream bytes
    to the client verbatim while parsing events to extract the usage
    counts needed for cost attribution. `record_call` runs after the
    stream completes, even on client disconnect.

    Streaming is not retried — the upstream bytes already in flight
    cannot be rewound. Clients see one upstream attempt and decide
    whether to reissue.
    """
    upstream: httpx.AsyncClient = request.app.state.upstream_client
    provider_completed = asyncio.Event()
    try:
        upstream_stream_ctx = upstream.stream(
            "POST",
            f"{ANTHROPIC_BASE_URL}/v1/messages",
            json=payload,
            headers={
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            timeout=upstream_timeout(request, settings.upstream_timeout_sec),
        )
        deadline = request_attempt_deadline(request)
        audit = request_dispatch_audit(request, dialect="anthropic_stream")
        stream_ctx = (
            upstream_stream_ctx
            if audit is None
            else audit.stream(
                lambda: upstream_stream_ctx,
                deadline=deadline,
                completed=provider_completed.is_set,
            )
        )
        upstream_response = (
            await stream_ctx.__aenter__()
            if deadline is None or audit is not None
            else await deadline.run(stream_ctx.__aenter__)
        )
    except AttemptDeadlineReachedError as exc:
        raise_deadline_http_exception(exc)
    except httpx.TimeoutException as exc:
        await _record_failed_message_call(
            request=request,
            ctx=ctx,
            model_name=model_name,
            request_payload=payload,
            failure_category="upstream_timeout",
            failure_error_type=type(exc).__name__,
        )
        raise HTTPException(
            status_code=504,
            detail=f"anthropic upstream timeout: {exc}",
        ) from exc
    except httpx.RequestError as exc:
        await _record_failed_message_call(
            request=request,
            ctx=ctx,
            model_name=model_name,
            request_payload=payload,
            failure_category="upstream_transport",
            failure_error_type=type(exc).__name__,
        )
        raise HTTPException(
            status_code=502,
            detail=f"anthropic upstream request error: {type(exc).__name__}: {exc}",
        ) from exc

    try:
        enforce_request_attempt_deadline(request)
    except BaseException:
        await stream_ctx.__aexit__(*sys.exc_info())
        raise
    if upstream_response.status_code >= 400:
        try:
            deadline = request_attempt_deadline(request)
            body = (
                await upstream_response.aread()
                if deadline is None
                else await deadline.run(upstream_response.aread)
            )
        except AttemptDeadlineReachedError as exc:
            raise_deadline_http_exception(exc)
        finally:
            await stream_ctx.__aexit__(*sys.exc_info())
        excerpt = redact_api_key(body.decode("utf-8", "replace"), api_key)
        await _record_failed_message_call(
            request=request,
            ctx=ctx,
            model_name=model_name,
            request_payload=payload,
            failure_category=http_failure_category(upstream_response.status_code),
            failure_status_code=upstream_response.status_code,
        )
        raise HTTPException(
            status_code=upstream_response.status_code,
            detail=f"anthropic upstream returned {upstream_response.status_code}: {excerpt}",
        )

    content_type = upstream_response.headers.get(
        "content-type",
        "text/event-stream",
    )

    async def event_iter() -> Any:
        accum: dict[str, int] = {"input_tokens": 0, "output_tokens": 0}
        buffer = b""
        try:
            iterator = upstream_response.aiter_bytes()
            while True:
                try:
                    deadline = request_attempt_deadline(request)
                    chunk = (
                        await iterator.__anext__()
                        if deadline is None
                        else await deadline.anext(iterator)
                    )
                except StopAsyncIteration:
                    provider_completed.set()
                    break
                yield chunk
                buffer += chunk
                events, buffer = _parse_sse_blocks(buffer)
                for name, data in events:
                    _extract_stream_usage(name, data, accum)
        finally:
            await stream_ctx.__aexit__(*sys.exc_info())
            await _record_stream_call(
                request=request,
                ctx=ctx,
                model_name=model_name,
                accum=accum,
                request_payload=payload,
            )

    return StreamingResponse(event_iter(), media_type=content_type)


async def _record_stream_call(
    *,
    request: Request,
    ctx: AuthContext,
    model_name: str,
    accum: dict[str, int],
    request_payload: dict[str, Any],
) -> None:
    """Write the streamed call's usage + cost to `llm_calls`. Skips
    when no usage was accumulated (upstream produced no SSE blocks
    before the stream ended — e.g. immediate client disconnect)."""
    if accum["input_tokens"] == 0 and accum["output_tokens"] == 0:
        logger.warning(
            "anthropic stream produced no usage events; skipping cost record",
            extra={
                "trial_id": str(ctx.trial_id) if ctx.trial_id is not None else None,
                "execution_attempt_id": (
                    str(ctx.execution_attempt_id) if ctx.execution_attempt_id is not None else None
                ),
                "step_id": ctx.step_id,
            },
        )
        return
    # The outer route already 403'd when any of these were None; the
    # asserts narrow types for mypy without changing runtime behavior.
    assert ctx.team_id is not None
    assert ctx.token_subject is not None
    assert ctx.step_id is not None
    extras = {
        k: accum[k]
        for k in ("cache_creation_input_tokens", "cache_read_input_tokens")
        if k in accum
    }
    usage = TokenUsage(
        input_tokens=accum["input_tokens"],
        output_tokens=accum["output_tokens"],
        provider_extras=extras,
    )
    table = await request.app.state.rate_card_cache.get()
    entry = lookup_entry(table, ModelSpec(provider="anthropic", name=model_name))
    cost = compute_cost_usd(
        entry,
        input_tokens=usage.input_tokens,
        output_tokens=usage.output_tokens,
        cached_input_tokens=usage.cached_input_tokens,
        cache_write_tokens=usage.cache_write_tokens,
    )
    async with request.app.state.session_factory() as session:
        await record_call(
            session,
            team_id=ctx.team_id,
            trial_id=ctx.trial_id,
            execution_attempt_id=ctx.execution_attempt_id,
            step_id=ctx.step_id,
            dialect="anthropic",
            model=model_name,
            usage=usage,
            cost_usd=cost,
            rate_card_hash=hash_table(table),
            attempt=1,
            request_params=normalize_request_params(request_payload),
        )


async def _record_failed_message_call(
    *,
    request: Request,
    ctx: AuthContext,
    model_name: str,
    request_payload: dict[str, Any],
    failure_category: str,
    attempt: int = 1,
    failure_status_code: int | None = None,
    failure_error_type: str | None = None,
) -> None:
    assert ctx.team_id is not None
    assert ctx.token_subject is not None
    assert ctx.step_id is not None
    async with request.app.state.session_factory() as session:
        await record_failed_call(
            session,
            auth_context=ctx,
            team_id=ctx.team_id,
            trial_id=ctx.trial_id,
            execution_attempt_id=ctx.execution_attempt_id,
            step_id=ctx.step_id,
            dialect="anthropic",
            model=model_name,
            provider="anthropic",
            attempt=attempt,
            request_params=normalize_request_params(request_payload),
            failure_category=failure_category,
            failure_status_code=failure_status_code,
            failure_error_type=failure_error_type,
        )
