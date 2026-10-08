"""POST /openai/v1/chat/completions — provider-connection facade
(cluster-deploy.md §Component map: `loom-llm-gateway`).

This is the route the **in-sandbox agent SDK** targets when it speaks
the OpenAI dialect. The intended consumer is the agent process
running inside a per-trial Docker sandbox (Phase 3, not yet wired):
the sandbox SDK is configured with `OPENAI_BASE_URL=...` pointing at
`loom-sandbox-gateway.local`, which proxies to this route. That code
path is built out alongside `loom-llm-gateway-sandbox` + the per-trial
Docker bridges.

Today's in-process agent runtimes (`HttpLLMGatewayClient`) still hit
the legacy `/v1/chat/completions` — they don't go through this
facade because they live in the worker process, not the sandbox.
Phase 3 closes that loop end-to-end.

Difference from `/v1/chat/completions`:
- `/v1/chat/completions`: routes by `model="provider/name"`, uses
  gateway-resident provider keys, wraps the response with a `loom`
  attribution block.
- `/openai/v1/chat/completions` (this route): resolves a
  `provider_connection_id` to the operator's stored credential,
  decrypts it in-process, forwards verbatim, and returns the raw
  upstream OpenAI body so the unmodified SDK can parse it. The
  agent never sees the upstream key.

Wire shape (MVP):
- `Authorization: Bearer loom_step_<jwt>` — step-scoped JWT minted
  by control-plane (existing `verify_bearer_token` path).
- `x-loom-provider-connection-id: <uuid>` — required header.
  (Eventually folded into the JWT scope alongside trial_id/step_id;
  see cluster-deploy.md §Authentication. For now an explicit header
  keeps this route focused on the facade plumbing.)
- Body: OpenAI-shape chat completion request, passed through.

Out of scope for this route (explicit follow-ups):
- Streaming (`stream=true` returns 501 — see comment below).
- `/anthropic/v1/messages` + `/google/v1beta/...` facade variants
  (each provider's dialect needs its own request/response shape).
- Egress proxy routing (the upstream POST goes direct from the
  gateway pod today; egress-proxy IP allowlisting is Phase 3).
"""

from __future__ import annotations

import json
from typing import Any

import httpx
from fastapi import APIRouter, Header, HTTPException, Request
from starlette.responses import StreamingResponse

from loom_llm_gateway.attempt_deadline import (
    AttemptDeadlineReachedError,
    GatewayAttemptDeadline,
    enforce_request_attempt_deadline,
    raise_deadline_http_exception,
    request_attempt_deadline,
    upstream_timeout,
)
from loom_llm_gateway.dialect import DIALECTS
from loom_llm_gateway.dispatch_audit import dispatch_request_headers, request_dispatch_audit
from loom_llm_gateway.execution_attempt_dispatch import authorize_trial_model
from loom_llm_gateway.llm_calls import record_call
from loom_llm_gateway.model_switch_correlation import (
    extract_and_strip_loom_fields,
    persist_correlated_intent,
)
from loom_llm_gateway.request_params import normalize_request_params
from loom_llm_gateway.retry import send_with_retry
from loom_llm_gateway.routes._facade_common import (
    build_raw_provider_log,
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

# Provider types this facade can serve. Anthropic-native + Google
# Gemini speak different dialects and need their own facade routes;
# routing them through this one would fail at the upstream with a
# 400 once it tries to parse a chat-completion request.
#
# `custom` is treated as openai-compatible because that's the most
# common shape behind operator-supplied base URLs (vLLM, Together,
# Mistral, Fireworks all speak it). Operators who need a non-OpenAI
# `custom` provider use the dialect-specific facade once it lands.
_OPENAI_SHAPED_TYPES = frozenset({"openai-compatible", "custom"})

# String stored in `llm_calls.dialect` for facade-routed traffic.
# Snake-case to match the existing convention (openai_chat,
# openai_responses, anthropic, gemini) so trial.py's projection
# map (_provider_by_dialect) maps facade rows to provider="openai"
# rather than the "unknown" fallback. The `rate_card_hash` field
# carries the facade marker for downstream billing/audit
# (see `record_call(..., rate_card_hash=f"facade:{pricing_source}")`).
_FACADE_DIALECT = "openai_facade"


@router.post("/openai/v1/chat/completions", response_model=None)
async def openai_chat_facade(
    request: Request,
    payload: dict[str, Any],
    authorization: str | None = Header(default=None),
    x_loom_provider_connection_id: str | None = Header(
        default=None,
        alias="x-loom-provider-connection-id",
    ),
) -> dict[str, Any] | StreamingResponse:
    settings = request.app.state.settings
    signing_key = settings.step_jwt_signing_key.get_secret_value()

    # Step-scoped JWT auth — same path as /v1/messages so the trial_id
    # + step_id + team_id are pulled from the JWT (not the body).
    async with request.app.state.session_factory() as session:
        ctx = await verify_facade_auth(
            session,
            authorization,
            signing_key,
            request=request,
        )
    assert ctx.team_id is not None  # narrowed by verify_facade_auth
    assert ctx.trial_id is not None
    assert ctx.step_id is not None
    connection_id = resolve_provider_connection_id(
        ctx,
        x_loom_provider_connection_id,
    )

    if not isinstance(payload.get("model"), str) or not payload["model"]:
        raise HTTPException(status_code=400, detail="`model` is required")
    await authorize_trial_model(request, ctx, provider="openai", model=payload["model"])

    client_requested_stream = bool(payload.get("stream"))
    upstream_payload = dict(payload)
    if client_requested_stream:
        upstream_payload["stream"] = False
        upstream_payload.pop("stream_options", None)
    upstream_payload, loom_extras = extract_and_strip_loom_fields(upstream_payload)

    async with request.app.state.session_factory() as session:
        correlation = await persist_correlated_intent(
            session,
            trial_id=ctx.trial_id,
            step_id=ctx.step_id,
            extras=loom_extras,
            jwt_connection_id=connection_id,
            requested_model=payload["model"],
        )

    # Resolve + decrypt. Both happen inside one session so the
    # SecretStore.get sees the same transaction the lookup did.
    async with request.app.state.session_factory() as session:
        row = await resolve_facade_connection(
            session,
            connection_id,
            ctx.team_id,
            supported_types=_OPENAI_SHAPED_TYPES,
            dialect_label="/openai/v1/chat/completions",
        )
        api_key = await decrypt_facade_api_key(session, row)

    upstream_url = f"{row.base_url.rstrip('/')}/chat/completions"
    upstream_headers = {
        "Authorization": f"Bearer {api_key}",
        "content-type": "application/json",
    }
    # #190 PR-C2: per-connection egress proxy when LOOM_GW_EGRESS_PROXY_URL set.
    upstream: httpx.AsyncClient = await request.app.state.egress_client_pool.get(connection_id)
    upstream_attempted = False

    async def send_upstream() -> httpx.Response:
        nonlocal upstream_attempted
        timeout = upstream_timeout(request, settings.upstream_timeout_sec)
        upstream_attempted = True
        return await upstream.post(
            upstream_url, json=upstream_payload, headers=dispatch_request_headers(upstream_headers),
            timeout=timeout, follow_redirects=False,
        )

    try:
        outcome = await send_with_retry(
            send_upstream,
            settings=settings,
            dialect="facade_openai",
            deadline=request_attempt_deadline(request),
            dispatch_audit=request_dispatch_audit(
                request,
                dialect="facade_openai",
                provider_connection_id=connection_id,
            ),
        )
        upstream_response = outcome.response
    except AttemptDeadlineReachedError as exc:
        if upstream_attempted:
            await record_facade_failed_call(
                request=request, ctx=ctx, row=row, dialect=_FACADE_DIALECT,
                model=payload["model"], request_payload=payload,
                failure_category="attempt_deadline_reached",
                failure_error_type=type(exc).__name__, correlation=correlation,
            )
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
            correlation=correlation,
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
            correlation=correlation,
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
            correlation=correlation,
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

    # Token usage — OpenAI shape is `usage.{prompt,completion,total}_tokens`.
    # A non-2xx already 4xx'd above, so this is the happy path. Missing
    # `usage` (some operator endpoints omit it) → 0/0 with a 0 cost row
    # so attribution still exists for debug.
    usage = DIALECTS["openai_chat"].extract_tokens(body)
    cost_estimate = await compute_facade_cost_estimate(
        row,
        payload["model"],
        usage,
        rate_card_cache=request.app.state.rate_card_cache,
    )
    usage = token_usage_with_cost_metadata(usage, cost_estimate)

    # Audit. We use a fresh session so the upstream call's latency
    # didn't keep the connection-lookup session open longer than
    # needed. `rate_card_hash` carries the pricing_source string —
    # the field is non-null so we store a useful marker.
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
            raw_provider_log=build_raw_provider_log(
                dialect=_FACADE_DIALECT,
                provider=row.provider_type,
                provider_connection_id=connection_id,
                attempt=outcome.attempt,
                request_method="POST",
                request_url=upstream_url,
                request_headers=upstream_headers,
                request_body=upstream_payload,
                response_status_code=upstream_response.status_code,
                response_headers=dict(upstream_response.headers),
                response_body=body,
                api_key=api_key,
                auth_context=ctx,
            ),
            response_model=body.get("model") if isinstance(body.get("model"), str) else None,
            **{
                k: correlation[k]
                for k in (
                    "client_call_id",
                    "agent_execution_id",
                    "agent_run_attempt_id",
                    "episode",
                    "call_ordinal",
                    "requested_model",
                    "role",
                    "correlation_status",
                )
            },
        )

    return openai_chat_facade_result(
        body=body,
        client_requested_stream=client_requested_stream,
        deadline=request_attempt_deadline(request),
    )


def openai_chat_facade_result(
    *,
    body: dict[str, Any],
    client_requested_stream: bool,
    deadline: GatewayAttemptDeadline | None = None,
) -> dict[str, Any] | StreamingResponse:
    if not client_requested_stream:
        return body

    async def _body() -> Any:
        for chunk in _synthetic_openai_chat_sse_chunks(body):
            if deadline is not None:
                deadline.require_remaining()
            yield chunk

    return StreamingResponse(
        _body(),
        media_type="text/event-stream",
    )


def _synthetic_openai_chat_sse_chunks(body: dict[str, Any]) -> list[bytes]:
    base = _chat_completion_chunk_base(body)
    chunks: list[bytes] = []
    choices = body.get("choices")
    if isinstance(choices, list):
        for choice in choices:
            if not isinstance(choice, dict):
                continue
            index = _choice_index(choice)
            delta = _message_to_delta(choice.get("message"))
            if delta:
                chunks.append(
                    _sse_chunk(
                        {
                            **base,
                            "choices": [
                                {
                                    "index": index,
                                    "delta": delta,
                                    "finish_reason": None,
                                }
                            ],
                        }
                    )
                )
            chunks.append(
                _sse_chunk(
                    {
                        **base,
                        "choices": [
                            {
                                "index": index,
                                "delta": {},
                                "finish_reason": choice.get("finish_reason"),
                            }
                        ],
                    }
                )
            )
    usage = body.get("usage")
    if isinstance(usage, dict):
        chunks.append(
            _sse_chunk(
                {
                    **base,
                    "choices": [],
                    "usage": usage,
                }
            )
        )
    chunks.append(b"data: [DONE]\n\n")
    return chunks


def _chat_completion_chunk_base(body: dict[str, Any]) -> dict[str, Any]:
    base: dict[str, Any] = {
        "id": body.get("id", "chatcmpl_loom_facade"),
        "object": "chat.completion.chunk",
        "created": body.get("created", 0),
        "model": body.get("model", ""),
    }
    if "system_fingerprint" in body:
        base["system_fingerprint"] = body["system_fingerprint"]
    return base


def _choice_index(choice: dict[str, Any]) -> int:
    index = choice.get("index")
    return index if isinstance(index, int) else 0


def _message_to_delta(message: Any) -> dict[str, Any]:
    if not isinstance(message, dict):
        return {}
    delta: dict[str, Any] = {}
    role = message.get("role")
    if isinstance(role, str):
        delta["role"] = role
    if message.get("content") is not None:
        delta["content"] = message["content"]
    if message.get("refusal") is not None:
        delta["refusal"] = message["refusal"]
    tool_calls = message.get("tool_calls")
    if isinstance(tool_calls, list):
        delta["tool_calls"] = [
            {"index": idx, **tool_call}
            for idx, tool_call in enumerate(tool_calls)
            if isinstance(tool_call, dict)
        ]
    return delta


def _sse_chunk(event: dict[str, Any]) -> bytes:
    return (
        "data: " + json.dumps(event, separators=(",", ":"), ensure_ascii=False) + "\n\n"
    ).encode("utf-8")
