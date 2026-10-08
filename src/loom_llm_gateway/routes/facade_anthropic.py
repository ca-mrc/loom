"""POST /anthropic/v1/messages — provider-connection facade
(cluster-deploy.md §Component map: `loom-llm-gateway`).

Sibling of `/openai/v1/chat/completions` (`facade_openai.py`) for
Anthropic-typed connections. The in-sandbox Anthropic SDK is
configured with `ANTHROPIC_BASE_URL=...` pointing at
`loom-sandbox-gateway.local`, which proxies to this route. Today's
in-process runtimes still hit the legacy `/v1/messages` route which
uses gateway-resident keys; that path is unchanged.

Difference from `/v1/messages`:
- `/v1/messages`: uses gateway-resident `LOOM_GW_ANTHROPIC_API_KEY`,
  rate-card lookup via `(provider, model)` keys, returns the body
  with cost recorded.
- `/anthropic/v1/messages` (this route): resolves a
  `provider_connection_id` to the operator's stored credential,
  decrypts it in-process, forwards verbatim to the connection's
  `base_url + /v1/messages`, and returns the raw upstream Anthropic
  body so the unmodified SDK can parse it. The agent never sees
  the upstream key.

Wire shape (MVP):
- `Authorization: Bearer loom_step_<jwt>` — step-scoped JWT minted
  by control-plane.
- `x-api-key: loom_step_<jwt>` — equivalent auth carrier used by
  stock Anthropic SDK/CLI clients. The facade consumes this JWT and
  replaces it with the stored provider key on the upstream request.
- `x-loom-provider-connection-id: <uuid>` — required header.
  (Eventually folded into the JWT scope; see #72.)
- Body: Anthropic Messages API shape, passed through.

Out of scope for this route:
- Egress proxy routing (Phase 3).
"""

from __future__ import annotations

import asyncio
import json
import logging
import sys
from collections.abc import AsyncIterator
from typing import Any

import httpx
from fastapi import APIRouter, Header, HTTPException, Request
from starlette.responses import StreamingResponse

from loom_llm_gateway.attempt_deadline import (
    AttemptDeadlineReachedError,
    enforce_request_attempt_deadline,
    raise_deadline_http_exception,
    request_attempt_deadline,
    upstream_timeout,
)
from loom_llm_gateway.dialect import DIALECTS
from loom_llm_gateway.dispatch_audit import request_dispatch_audit
from loom_llm_gateway.execution_attempt_dispatch import authorize_trial_model
from loom_llm_gateway.llm_calls import record_call
from loom_llm_gateway.request_params import normalize_request_params
from loom_llm_gateway.retry import send_with_retry
from loom_llm_gateway.routes._facade_common import (
    compute_facade_cost_estimate,
    decrypt_facade_api_key,
    http_failure_category,
    record_facade_failed_call,
    redact_api_key,
    resolve_facade_connection,
    resolve_provider_connection_id,
    token_usage_with_cost_metadata,
    verify_facade_auth,
)

router = APIRouter()
logger = logging.getLogger(__name__)

# Only `anthropic` connections route through this facade. A `custom`
# connection that happens to speak Anthropic is a misconfiguration:
# the operator should re-create as `provider_type='anthropic'` so the
# rest of the system (probe, models cache, pricing defaults) treats
# it correctly.
_ANTHROPIC_TYPES = frozenset({"anthropic"})

# llm_calls.dialect — snake_case for consistency with siblings; the
# projection map in `loom/trial/trial.py` MUST contain a matching
# entry (regression caught by PR #65). `rate_card_hash` carries the
# facade marker for downstream billing/audit.
_FACADE_DIALECT = "anthropic_facade"

# Anthropic API version. Matches the existing `/v1/messages` route's
# header so connections to non-Anthropic upstreams speaking the
# Anthropic shape behave identically.
_ANTHROPIC_VERSION = "2023-06-01"


def _step_authorization_for_anthropic_client(
    authorization: str | None,
    x_api_key: str | None,
) -> str | None:
    if authorization:
        return authorization
    if x_api_key:
        return f"Bearer {x_api_key}"
    return None


@router.post("/anthropic/v1/messages", response_model=None)
async def anthropic_messages_facade(
    request: Request,
    payload: dict[str, Any],
    authorization: str | None = Header(default=None),
    x_api_key: str | None = Header(default=None, alias="x-api-key"),
    x_loom_provider_connection_id: str | None = Header(
        default=None,
        alias="x-loom-provider-connection-id",
    ),
) -> dict[str, Any] | StreamingResponse:
    settings = request.app.state.settings
    signing_key = settings.step_jwt_signing_key.get_secret_value()

    async with request.app.state.session_factory() as session:
        ctx = await verify_facade_auth(
            session,
            _step_authorization_for_anthropic_client(
                authorization,
                x_api_key,
            ),
            signing_key,
            request=request,
        )
    assert ctx.team_id is not None
    assert ctx.trial_id is not None
    assert ctx.step_id is not None
    connection_id = resolve_provider_connection_id(
        ctx,
        x_loom_provider_connection_id,
    )

    if not isinstance(payload.get("model"), str) or not payload["model"]:
        raise HTTPException(status_code=400, detail="`model` is required")
    await authorize_trial_model(request, ctx, provider="anthropic", model=payload["model"])

    async with request.app.state.session_factory() as session:
        row = await resolve_facade_connection(
            session,
            connection_id,
            ctx.team_id,
            supported_types=_ANTHROPIC_TYPES,
            dialect_label="/anthropic/v1/messages",
        )
        api_key = await decrypt_facade_api_key(session, row)

    upstream_url = f"{row.base_url.rstrip('/')}/v1/messages"
    upstream_headers = _anthropic_upstream_headers(request, api_key)
    # #190 PR-C2: when egress mode is on, this resolves to a
    # per-connection_id client whose CONNECT carries the routing
    # header Envoy matches against. When off (default), it returns
    # the shared upstream_client and the call goes direct.
    upstream: httpx.AsyncClient = await request.app.state.egress_client_pool.get(connection_id)

    if payload.get("stream"):
        return await _stream_anthropic_messages(
            request=request,
            upstream=upstream,
            upstream_url=upstream_url,
            payload=payload,
            upstream_headers=upstream_headers,
            api_key=api_key,
            row=row,
            ctx=ctx,
        )

    try:
        outcome = await send_with_retry(
            lambda: upstream.post(
                upstream_url,
                json=payload,
                headers=upstream_headers,
                timeout=upstream_timeout(request, settings.upstream_timeout_sec),
                follow_redirects=False,
            ),
            settings=settings,
            dialect="facade_anthropic",
            deadline=request_attempt_deadline(request),
            dispatch_audit=request_dispatch_audit(
                request,
                dialect="facade_anthropic",
                provider_connection_id=connection_id,
            ),
        )
        upstream_response = outcome.response
    except AttemptDeadlineReachedError as exc:
        raise_deadline_http_exception(exc)
    except httpx.TimeoutException as e:
        await record_facade_failed_call(
            request=request,
            ctx=ctx,
            row=row,
            dialect=_FACADE_DIALECT,
            model=payload["model"],
            request_payload=payload,
            failure_category="upstream_timeout",
            failure_error_type=type(e).__name__,
        )
        raise HTTPException(
            status_code=504,
            detail=f"upstream timeout against {upstream_url}: {e}",
        ) from e
    except httpx.RequestError as e:
        await record_facade_failed_call(
            request=request,
            ctx=ctx,
            row=row,
            dialect=_FACADE_DIALECT,
            model=payload["model"],
            request_payload=payload,
            failure_category="upstream_transport",
            failure_error_type=type(e).__name__,
        )
        raise HTTPException(
            status_code=502,
            detail=(f"upstream request error against {upstream_url}: {type(e).__name__}: {e}"),
        ) from e

    enforce_request_attempt_deadline(request)
    if upstream_response.status_code >= 400:
        excerpt = redact_api_key(upstream_response.text, api_key)
        await record_facade_failed_call(
            request=request,
            ctx=ctx,
            row=row,
            dialect=_FACADE_DIALECT,
            model=payload["model"],
            request_payload=payload,
            failure_category=http_failure_category(upstream_response.status_code),
            failure_status_code=upstream_response.status_code,
            attempt=outcome.attempt,
        )
        raise HTTPException(
            status_code=upstream_response.status_code,
            detail=(f"upstream returned {upstream_response.status_code}: {excerpt}"),
        )

    try:
        body: dict[str, Any] = upstream_response.json()
    except ValueError as e:
        raise HTTPException(
            status_code=502,
            detail=f"upstream returned non-JSON body: {e}",
        ) from e

    # Token usage via the existing Anthropic dialect adapter; carries
    # the cache_creation_input_tokens + cache_read_input_tokens extras
    # forward into llm_calls.provider_extras.
    usage = DIALECTS["anthropic"].extract_tokens(body)
    # Anthropic 200s always include a usage block; missing → upstream
    # contract violation. Same 502 rationale as the legacy /v1/messages
    # route so the worker's cost rollup never gets a silently-zero
    # row from a malformed upstream.
    if usage.input_tokens == 0 and usage.output_tokens == 0:
        raise HTTPException(
            status_code=502,
            detail=(
                "upstream returned 200 with missing/zero usage block; "
                "expected non-zero input_tokens+output_tokens"
            ),
        )

    cost_estimate = await compute_facade_cost_estimate(
        row,
        payload["model"],
        usage,
        rate_card_cache=request.app.state.rate_card_cache,
    )
    usage = token_usage_with_cost_metadata(usage, cost_estimate)

    async with request.app.state.session_factory() as audit_session:
        await record_call(
            audit_session,
            team_id=ctx.team_id,
            trial_id=ctx.trial_id,
            step_id=ctx.step_id,
            dialect=_FACADE_DIALECT,
            model=payload["model"],
            usage=usage,
            cost_usd=cost_estimate.cost_usd,
            rate_card_hash=cost_estimate.rate_card_hash,
            provider=row.provider_type,
            attempt=outcome.attempt,
            request_params=normalize_request_params(payload),
        )

    return body


def _anthropic_upstream_headers(
    request: Request,
    api_key: str,
) -> dict[str, str]:
    # Anthropic uses x-api-key (not Authorization). Matches the existing
    # /v1/messages route's header convention.
    headers = {
        "x-api-key": api_key,
        "anthropic-version": (request.headers.get("anthropic-version") or _ANTHROPIC_VERSION),
        "content-type": "application/json",
    }
    accept = request.headers.get("accept")
    if accept:
        headers["accept"] = accept
    anthropic_beta = request.headers.get("anthropic-beta")
    if anthropic_beta:
        headers["anthropic-beta"] = anthropic_beta
    return headers


async def _stream_anthropic_messages(
    *,
    request: Request,
    upstream: httpx.AsyncClient,
    upstream_url: str,
    payload: dict[str, Any],
    upstream_headers: dict[str, str],
    api_key: str,
    row: Any,
    ctx: Any,
) -> StreamingResponse:
    provider_completed = asyncio.Event()
    try:
        upstream_stream_cm = upstream.stream(
            "POST",
            upstream_url,
            json=payload,
            headers=upstream_headers,
            timeout=upstream_timeout(
                request,
                request.app.state.settings.upstream_timeout_sec,
            ),
            follow_redirects=False,
        )
        deadline = request_attempt_deadline(request)
        audit = request_dispatch_audit(
            request,
            dialect="facade_anthropic_stream",
            provider_connection_id=row.id,
        )
        stream_cm = (
            upstream_stream_cm
            if audit is None
            else audit.stream(
                lambda: upstream_stream_cm,
                deadline=deadline,
                completed=provider_completed.is_set,
            )
        )
        upstream_response = (
            await stream_cm.__aenter__()
            if deadline is None or audit is not None
            else await deadline.run(stream_cm.__aenter__)
        )
    except AttemptDeadlineReachedError as exc:
        raise_deadline_http_exception(exc)
    except httpx.TimeoutException as e:
        await record_facade_failed_call(
            request=request,
            ctx=ctx,
            row=row,
            dialect=_FACADE_DIALECT,
            model=payload["model"],
            request_payload=payload,
            failure_category="upstream_timeout",
            failure_error_type=type(e).__name__,
        )
        raise HTTPException(
            status_code=504,
            detail=f"upstream timeout against {upstream_url}: {e}",
        ) from e
    except httpx.RequestError as e:
        await record_facade_failed_call(
            request=request,
            ctx=ctx,
            row=row,
            dialect=_FACADE_DIALECT,
            model=payload["model"],
            request_payload=payload,
            failure_category="upstream_transport",
            failure_error_type=type(e).__name__,
        )
        raise HTTPException(
            status_code=502,
            detail=(f"upstream request error against {upstream_url}: {type(e).__name__}: {e}"),
        ) from e

    try:
        enforce_request_attempt_deadline(request)
    except BaseException:
        await stream_cm.__aexit__(*sys.exc_info())
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
            await stream_cm.__aexit__(*sys.exc_info())
        excerpt = redact_api_key(body.decode(errors="replace"), api_key)
        await record_facade_failed_call(
            request=request,
            ctx=ctx,
            row=row,
            dialect=_FACADE_DIALECT,
            model=payload["model"],
            request_payload=payload,
            failure_category=http_failure_category(upstream_response.status_code),
            failure_status_code=upstream_response.status_code,
        )
        raise HTTPException(
            status_code=upstream_response.status_code,
            detail=(f"upstream returned {upstream_response.status_code}: {excerpt}"),
        )

    return StreamingResponse(
        _iter_anthropic_sse_and_record_usage(
            request=request,
            response=upstream_response,
            stream_cm=stream_cm,
            row=row,
            ctx=ctx,
            model=payload["model"],
            request_payload=payload,
            provider_completed=provider_completed,
        ),
        media_type=upstream_response.headers.get(
            "content-type",
            "text/event-stream",
        ),
        status_code=upstream_response.status_code,
    )


async def _iter_anthropic_sse_and_record_usage(
    *,
    request: Request,
    response: httpx.Response,
    stream_cm: Any,
    row: Any,
    ctx: Any,
    model: str,
    request_payload: dict[str, Any],
    provider_completed: asyncio.Event | None = None,
) -> AsyncIterator[bytes]:
    tracker = _AnthropicStreamUsageTracker()
    try:
        iterator = response.aiter_bytes()
        while True:
            try:
                deadline = request_attempt_deadline(request)
                chunk = (
                    await iterator.__anext__()
                    if deadline is None
                    else await deadline.anext(iterator)
                )
            except StopAsyncIteration:
                if provider_completed is not None:
                    provider_completed.set()
                break
            tracker.feed(chunk)
            yield chunk
        tracker.finish()
        await _record_anthropic_usage_from_stream(
            request=request,
            row=row,
            ctx=ctx,
            model=model,
            usage_body=tracker.usage_body(),
            request_payload=request_payload,
        )
    finally:
        await stream_cm.__aexit__(*sys.exc_info())


class _AnthropicStreamUsageTracker:
    def __init__(self) -> None:
        self._buffer = ""
        self._usage: dict[str, int] = {}

    def feed(self, chunk: bytes) -> None:
        self._buffer += chunk.decode("utf-8", errors="replace")
        self._buffer = self._buffer.replace("\r\n", "\n")
        while "\n\n" in self._buffer:
            block, self._buffer = self._buffer.split("\n\n", 1)
            self._process_block(block)

    def finish(self) -> None:
        if self._buffer.strip():
            self._process_block(self._buffer)
        self._buffer = ""

    def usage_body(self) -> dict[str, Any]:
        return {"usage": dict(self._usage)}

    def _process_block(self, block: str) -> None:
        data_lines = []
        for line in block.splitlines():
            if not line.startswith("data:"):
                continue
            data = line.removeprefix("data:")
            if data.startswith(" "):
                data = data[1:]
            data_lines.append(data)
        if not data_lines:
            return
        data = "\n".join(data_lines).strip()
        if not data or data == "[DONE]":
            return
        try:
            event = json.loads(data)
        except json.JSONDecodeError:
            return
        if not isinstance(event, dict):
            return
        message = event.get("message")
        if isinstance(message, dict):
            self._merge_usage(message.get("usage"))
        self._merge_usage(event.get("usage"))

    def _merge_usage(self, usage: object) -> None:
        if not isinstance(usage, dict):
            return
        for key in (
            "input_tokens",
            "output_tokens",
            "cache_creation_input_tokens",
            "cache_read_input_tokens",
        ):
            value = usage.get(key)
            if value is None:
                continue
            try:
                self._usage[key] = int(value)
            except (TypeError, ValueError):
                continue


async def _record_anthropic_usage_from_stream(
    *,
    request: Request,
    row: Any,
    ctx: Any,
    model: str,
    usage_body: dict[str, Any],
    request_payload: dict[str, Any],
) -> None:
    usage = DIALECTS["anthropic"].extract_tokens(usage_body)
    if usage.input_tokens == 0 and usage.output_tokens == 0:
        logger.warning(
            "anthropic facade stream completed without usage block trial_id=%s step_id=%s model=%s",
            ctx.trial_id,
            ctx.step_id,
            model,
        )
        return

    cost_estimate = await compute_facade_cost_estimate(
        row,
        model,
        usage,
        rate_card_cache=request.app.state.rate_card_cache,
    )
    usage = token_usage_with_cost_metadata(usage, cost_estimate)

    async with request.app.state.session_factory() as audit_session:
        await record_call(
            audit_session,
            team_id=ctx.team_id,
            trial_id=ctx.trial_id,
            step_id=ctx.step_id,
            dialect=_FACADE_DIALECT,
            model=model,
            usage=usage,
            cost_usd=cost_estimate.cost_usd,
            rate_card_hash=cost_estimate.rate_card_hash,
            provider=row.provider_type,
            attempt=1,
            request_params=normalize_request_params(request_payload),
        )
