"""POST /v1beta/models/{model}:generateContent — Gemini native passthrough
(Plan 9 Task 9 + A9.1).

Gemini's native shape uses `contents`/`parts` (not OpenAI `messages`),
`functionCall` parts (not `tool_calls`), and `usageMetadata` (not
`usage`). LiteLLM's round-trip would lose multi-part inlineData
attachments. We passthrough natively.
"""

from __future__ import annotations

from typing import Any

import httpx
from fastapi import APIRouter, Header, HTTPException, Request

from loom.models.types import ModelSpec
from loom_llm_gateway.attempt_deadline import (
    AttemptDeadlineReachedError,
    enforce_request_attempt_deadline,
    raise_deadline_http_exception,
    request_attempt_deadline,
    upstream_timeout,
)
from loom_llm_gateway.dialect import DIALECTS
from loom_llm_gateway.dispatch_audit import request_dispatch_audit
from loom_llm_gateway.execution_attempt_dispatch import (
    authorize_trial_execution_dispatch,
    authorize_trial_model,
)
from loom_llm_gateway.llm_calls import record_call, record_failed_call
from loom_llm_gateway.rate_card import (
    compute_cost_usd,
    hash_table,
    lookup_entry,
)
from loom_llm_gateway.request_params import normalize_request_params
from loom_llm_gateway.retry import send_with_retry
from loom_llm_gateway.routes._auth import require_llm_call_bearer
from loom_llm_gateway.routes._facade_common import http_failure_category

router = APIRouter()

GEMINI_BASE_URL = "https://generativelanguage.googleapis.com"


@router.post("/v1beta/models/{model_path}")
async def gemini_generate_content(
    model_path: str,
    request: Request,
    payload: dict[str, Any],
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    """The colon in the path (`:generateContent` etc.) lives inside the
    `model_path` capture — Gemini supports `:generateContent`,
    `:streamGenerateContent`, `:countTokens`, etc. We forward whatever
    the caller asked for and only cost-attribute the response if it
    contained a `usageMetadata` block."""
    settings = request.app.state.settings
    signing_key = settings.step_jwt_signing_key.get_secret_value()
    async with request.app.state.session_factory() as session:
        ctx = await require_llm_call_bearer(
            session,
            authorization,
            signing_key=signing_key,
            request=request,
        )
        await authorize_trial_execution_dispatch(session, ctx)
    if ctx.trial_id is None or ctx.step_id is None or ctx.team_id is None:
        raise HTTPException(
            status_code=403,
            detail="step-scoped token required",
        )
    # Parse the model name out of "<name>:<action>" — e.g. the path
    # `gemini-2.0-flash:generateContent` yields model_name=gemini-2.0-flash,
    # action=generateContent. We record cost against model_name.
    if ":" not in model_path:
        raise HTTPException(
            status_code=400,
            detail="path must be <model>:<action>, e.g. gemini-2.0-flash:generateContent",
        )
    model_name, action = model_path.split(":", 1)
    await authorize_trial_model(request, ctx, provider="google", model=model_name)
    if settings.google_api_key is None:
        raise HTTPException(
            status_code=503,
            detail="google_api_key not configured on Gateway",
        )
    # Plan 9 audit fix: streaming variant loses the final usage block on
    # mid-stream connection close; refuse in v1.
    if action.startswith("stream"):
        raise HTTPException(
            status_code=400,
            detail=f"action {action!r} not supported on the Gateway in v1 "
            "(cost attribution requires the final usage block)",
        )

    upstream: httpx.AsyncClient = request.app.state.upstream_client
    try:
        outcome = await send_with_retry(
            lambda: upstream.post(
                f"{GEMINI_BASE_URL}/v1beta/models/{model_path}",
                json=payload,
                params={"key": settings.google_api_key.get_secret_value()},
                headers={"content-type": "application/json"},
                timeout=upstream_timeout(request, settings.upstream_timeout_sec),
            ),
            settings=settings,
            dialect="gemini",
            deadline=request_attempt_deadline(request),
            dispatch_audit=request_dispatch_audit(
                request,
                dialect="gemini",
            ),
        )
    except AttemptDeadlineReachedError as exc:
        raise_deadline_http_exception(exc)
    except httpx.TimeoutException as exc:
        await _record_failed_gemini_call(
            request=request,
            ctx=ctx,
            model_name=model_name,
            request_payload=payload,
            failure_category="upstream_timeout",
            failure_error_type=type(exc).__name__,
        )
        raise HTTPException(
            status_code=504,
            detail=f"gemini upstream timeout: {exc}",
        ) from exc
    except httpx.RequestError as exc:
        await _record_failed_gemini_call(
            request=request,
            ctx=ctx,
            model_name=model_name,
            request_payload=payload,
            failure_category="upstream_transport",
            failure_error_type=type(exc).__name__,
        )
        raise HTTPException(
            status_code=502,
            detail=f"gemini upstream request error: {type(exc).__name__}: {exc}",
        ) from exc
    upstream_response = outcome.response
    enforce_request_attempt_deadline(request)
    if upstream_response.status_code >= 400:
        await _record_failed_gemini_call(
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
            detail=f"gemini upstream returned "
            f"{upstream_response.status_code}: "
            f"{upstream_response.text[:500]}",
        )
    body: dict[str, Any] = upstream_response.json()

    usage = DIALECTS["gemini"].extract_tokens(body)
    # `countTokens` legitimately has no usageMetadata block — return
    # the response without inserting a cost row. `generateContent` MUST
    # carry it; a missing block on that action signals an upstream
    # contract violation.
    if usage.input_tokens == 0 and usage.output_tokens == 0:
        if action == "countTokens":
            return body
        raise HTTPException(
            status_code=502,
            detail=f"gemini 200 response missing usageMetadata for action "
            f"{action!r}; cost cannot be attributed",
        )

    table = await request.app.state.rate_card_cache.get()
    entry = lookup_entry(
        table,
        ModelSpec(provider="google", name=model_name),
    )
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
            step_id=ctx.step_id,
            dialect="gemini",
            model=model_name,
            usage=usage,
            cost_usd=cost,
            rate_card_hash=hash_table(table),
            attempt=outcome.attempt,
            request_params=normalize_request_params(payload),
        )
    return body


async def _record_failed_gemini_call(
    *,
    request: Request,
    ctx: Any,
    model_name: str,
    request_payload: dict[str, Any],
    failure_category: str,
    attempt: int = 1,
    failure_status_code: int | None = None,
    failure_error_type: str | None = None,
) -> None:
    assert ctx.team_id is not None
    assert ctx.trial_id is not None
    assert ctx.step_id is not None
    async with request.app.state.session_factory() as session:
        await record_failed_call(
            session,
            auth_context=ctx,
            team_id=ctx.team_id,
            trial_id=ctx.trial_id,
            step_id=ctx.step_id,
            dialect="gemini",
            model=model_name,
            provider="google",
            attempt=attempt,
            request_params=normalize_request_params(request_payload),
            failure_category=failure_category,
            failure_status_code=failure_status_code,
            failure_error_type=failure_error_type,
        )
