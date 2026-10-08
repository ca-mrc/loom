"""POST /google/v1beta/models/{model_path} — provider-connection facade
(cluster-deploy.md §Component map: `loom-llm-gateway`).

Sibling of `/openai/v1/chat/completions` and `/anthropic/v1/messages`
for google-typed connections. The in-sandbox Google GenAI SDK is
configured to target `loom-sandbox-gateway.local` which proxies to
this route. Today's in-process runtimes still hit the legacy
`/v1beta/models/{model_path}` route using the gateway-resident
google_api_key; that path is unchanged.

Difference from `/v1beta/models/{model_path}`:
- Legacy route uses gateway-resident `LOOM_GW_GOOGLE_API_KEY` +
  rate-card lookup keyed on `(provider="google", model)`.
- This facade resolves `provider_connection_id` to the operator's
  stored credential, decrypts it in-process, and forwards verbatim
  to the connection's `base_url + /v1beta/models/{model_path}` with
  the decrypted key in the `?key=` query parameter.

Wire shape (MVP):
- `Authorization: Bearer loom_step_<jwt>` — step-scoped JWT
- `x-loom-provider-connection-id: <uuid>` — required header (#72 will
  fold this into the JWT scope)
- Path: `<model>:<action>` — e.g. `gemini-2.5-flash:generateContent`.
  Streaming actions (`streamGenerateContent`) → 501.
- Body: Gemini Content API shape, passed through.
- `countTokens` is supported (no cost attribution, no llm_calls row).

Out of scope for this route:
- Streaming (`:streamGenerateContent` → 501)
- Egress proxy routing (Phase 3)
"""

from __future__ import annotations

from typing import Any

import httpx
from fastapi import APIRouter, Header, HTTPException, Request

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

_GOOGLE_TYPES = frozenset({"google"})

# llm_calls.dialect — snake_case, matched in
# `loom/trial/trial.py::_provider_by_dialect`.
_FACADE_DIALECT = "gemini_facade"


@router.post("/google/v1beta/models/{model_path:path}")
async def google_generate_content_facade(
    model_path: str,
    request: Request,
    payload: dict[str, Any],
    authorization: str | None = Header(default=None),
    x_loom_provider_connection_id: str | None = Header(
        default=None,
        alias="x-loom-provider-connection-id",
    ),
) -> dict[str, Any]:
    settings = request.app.state.settings
    signing_key = settings.step_jwt_signing_key.get_secret_value()

    async with request.app.state.session_factory() as session:
        ctx = await verify_facade_auth(
            session,
            authorization,
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

    # Parse `<model>:<action>` — e.g. `gemini-2.5-flash:generateContent`.
    if ":" not in model_path:
        raise HTTPException(
            status_code=400,
            detail=("path must be <model>:<action>, e.g. gemini-2.5-flash:generateContent"),
        )
    model_name, action = model_path.split(":", 1)
    if not model_name:
        raise HTTPException(
            status_code=400,
            detail="<model> portion of path is required",
        )
    await authorize_trial_model(request, ctx, provider="google", model=model_name)

    # Streaming variants ask for SSE — same blanket rejection rationale
    # as the openai/anthropic facade (final usageMetadata block needed
    # for cost attribution). Covers both `streamGenerateContent` and
    # the older `:stream` prefix forms.
    if action.startswith("stream"):
        raise HTTPException(
            status_code=501,
            detail=(
                f"action {action!r} not yet supported on the facade "
                "(cost attribution needs the final usageMetadata block)"
            ),
        )

    async with request.app.state.session_factory() as session:
        row = await resolve_facade_connection(
            session,
            connection_id,
            ctx.team_id,
            supported_types=_GOOGLE_TYPES,
            dialect_label="/google/v1beta/models/...",
        )
        api_key = await decrypt_facade_api_key(session, row)

    upstream_url = f"{row.base_url.rstrip('/')}/v1beta/models/{model_path}"
    # #190 PR-C2: per-connection egress proxy when LOOM_GW_EGRESS_PROXY_URL set.
    upstream: httpx.AsyncClient = await request.app.state.egress_client_pool.get(connection_id)
    try:
        outcome = await send_with_retry(
            lambda: upstream.post(
                upstream_url,
                json=payload,
                # Google API key auth is via the `?key=` query parameter,
                # matching the existing `probe_connection` shape for
                # google-typed connections.
                params={"key": api_key},
                headers={"content-type": "application/json"},
                timeout=upstream_timeout(request, settings.upstream_timeout_sec),
                follow_redirects=False,
            ),
            settings=settings,
            dialect="facade_google",
            deadline=request_attempt_deadline(request),
            dispatch_audit=request_dispatch_audit(
                request,
                dialect="facade_google",
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
            model=model_name,
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
            model=model_name,
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
        # Redact: Google's API key lands in both the request URL AND
        # potentially in 4xx debug body bytes. Scrub both before
        # surfacing. The url itself is reconstructed without the
        # query string so the key never appears in the error string.
        excerpt = redact_api_key(upstream_response.text, api_key)
        await record_facade_failed_call(
            request=request,
            ctx=ctx,
            row=row,
            dialect=_FACADE_DIALECT,
            model=model_name,
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

    # `countTokens` legitimately has no usageMetadata — return without
    # an llm_calls row (matches the legacy /v1beta/models route).
    usage = DIALECTS["gemini"].extract_tokens(body)
    if usage.input_tokens == 0 and usage.output_tokens == 0:
        if action == "countTokens":
            return body
        raise HTTPException(
            status_code=502,
            detail=(
                "upstream 200 response missing usageMetadata for "
                f"action {action!r}; cost cannot be attributed"
            ),
        )

    cost_estimate = await compute_facade_cost_estimate(
        row,
        model_name,
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
            model=model_name,
            usage=usage,
            cost_usd=cost_estimate.cost_usd,
            rate_card_hash=cost_estimate.rate_card_hash,
            provider=row.provider_type,
            attempt=outcome.attempt,
            request_params=normalize_request_params(payload),
        )

    return body
