"""POST /v1/chat/completions — OpenAI-compatible endpoint with Loom attribution.

Two modes:

- **Platform-credentialed** (legacy): no ``loom.provider_connection_id``
  in the body. Routes via litellm using the gateway-resident provider
  API keys (LOOM_GW_ANTHROPIC_API_KEY etc). Cost from the static rate
  card; missing entry → 400.
- **BYO connection** (#178 + #179): ``loom.provider_connection_id`` is
  set. Looks up the team's connection, decrypts its api_key from the
  SecretStore, overrides litellm's ``api_key`` + ``api_base`` so the
  request actually hits the BYO upstream. Pricing follows the
  connection's ``pricing_source`` (matches facade routes): rate-card
  goes through the rate-card lookup; operator-supplied / tokens-only
  skip the rate-card entirely. Missing rate-card row degrades to
  cost=0 instead of 400-ing the request.
"""

from __future__ import annotations

import logging
import time
import uuid as uuid_lib
from typing import Any
from uuid import UUID

import httpx
from fastapi import APIRouter, Header, HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from loom.auth import AuthContext
from loom.models.types import ModelSpec
from loom_llm_gateway import litellm_wrapper
from loom_llm_gateway.attempt_deadline import (
    AttemptDeadlineReachedError,
    GatewayAttemptDeadline,
    raise_deadline_http_exception,
    request_attempt_deadline,
)
from loom_llm_gateway.dialect import TokenUsage
from loom_llm_gateway.dispatch_audit import (
    DispatchAudit,
    DispatchAuditUnavailableError,
    request_dispatch_audit,
)
from loom_llm_gateway.errors import RateCardNotFoundError
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
from loom_llm_gateway.routes._facade_common import (
    compute_facade_cost_estimate,
    decrypt_facade_api_key,
    redact_api_key,
    resolve_facade_connection,
    resolve_optional_provider_connection_id,
    token_usage_with_cost_metadata,
)

# Provider types the chat route accepts for BYO routing. All four
# upstream dialects are supported because litellm handles the on-wire
# format from the model string's ``provider`` prefix — we just override
# the destination + auth.
_BYO_SUPPORTED_TYPES = frozenset(
    {
        "openai-compatible",
        "anthropic",
        "google",
        "custom",
    }
)

router = APIRouter()
logger = logging.getLogger(__name__)

# Reserved kwargs we never forward as a splat from `body`. The chat route
# passes these explicitly to acompletion; if a client (accidentally or
# maliciously) includes them in the body, splatting would shadow our named
# args and TypeError out with a 500.
_RESERVED_BODY_KEYS = frozenset(
    {"model", "messages", "loom", "api_key", "timeout"},
)

# Providers we accept on the wire. The Gateway only holds keys for these;
# any other provider in `model="X/Y"` is rejected with 400 upfront rather
# than silently passing api_key=None into LiteLLM.
#
# PR-D additions:
# - `local`: routes `model="local/<server>/<model_id>"` to the operator-
#   configured local OpenAI-compatible server (vLLM / ollama / etc.).
# - `huggingface`: routes `model="huggingface/<id>"` to HF Inference
#   Endpoints via LiteLLM. Requires LOOM_GW_HF_TOKEN (Plan D follow-up
#   adds the explicit setting; LiteLLM falls back to HF_TOKEN env).
# - `local-vllm`: reserved for worker-spawned vLLM (`source=hf` +
#   `hf_execution=local-vllm`). The worker handles this directly without
#   round-tripping the gateway; if a request lands here we surface 501.
_SUPPORTED_PROVIDERS = frozenset(
    {
        "anthropic",
        "openai",
        "together",
        "local",
        "huggingface",
        "local-vllm",
    }
)


class _LoomBlock(BaseModel):
    """Loom attribution block for callers without a step-scoped token.

    ``provider_connection_id`` opts the request into BYO routing
    (#178): the gateway looks up the team's connection, decrypts its
    api_key, and overrides litellm's destination. When omitted, the
    request goes through the platform-credentialed legacy path.
    """

    model_config = ConfigDict(extra="allow")  # allow future-proof extras
    team_id: str = Field(min_length=1)
    trial_id: str = Field(min_length=1)
    step_id: str = Field(min_length=1)
    tier: str | None = None
    region: str | None = None
    provider_connection_id: str | None = None
    # #672 PR-3: caller-declared dialect that overrides the default
    # ``openai_chat`` audit label. The family-run skill_patcher_llm
    # adapter sends ``family_evolver`` so cost + audit dashboards can
    # partition adapter spend from agent spend on the same trial.
    dialect: str | None = None


class ChatRequest(BaseModel):
    """Required-shape envelope for /v1/chat/completions.

    extra="allow" so OpenAI-compatible extras (temperature, max_tokens,
    tools, ...) pass through to LiteLLM via the splat path.
    """

    model_config = ConfigDict(extra="allow")
    model: str = Field(min_length=1)
    messages: list[dict[str, Any]] = Field(min_length=1)
    loom: _LoomBlock | None = None


@router.post("/v1/chat/completions")
async def chat_completions(
    request: Request,
    authorization: str | None = Header(default=None),
    x_loom_provider_connection_id: str | None = Header(
        default=None,
        alias="x-loom-provider-connection-id",
    ),
) -> dict[str, Any]:
    raw_body = await request.json()
    # Bugs 3 + 4 fix: validate shape upfront → 400 with structured detail
    # instead of KeyError → 500, and exclude reserved kwargs from the splat.
    try:
        req = ChatRequest.model_validate(raw_body)
    except ValidationError as exc:
        raise HTTPException(status_code=400, detail=exc.errors()) from exc

    settings = request.app.state.settings
    signing_key = settings.step_jwt_signing_key.get_secret_value()

    # Authentication. The session also covers the BYO provider-
    # connection lookup + api_key decrypt below; keep it open through
    # those reads so we don't pay an extra connection round-trip.
    byo_row = None
    byo_api_key: str | None = None
    async with request.app.state.session_factory() as session:
        ctx = await require_llm_call_bearer(
            session,
            authorization,
            signing_key=signing_key,
            request=request,
        )
        if ctx.execution_attempt_id is not None:
            raise HTTPException(
                status_code=403,
                detail="execution-attempt tokens are not supported by this route",
            )
        await authorize_trial_execution_dispatch(session, ctx)

        # The token/JWT team is authoritative. A caller-supplied body team may
        # provide legacy attribution only for a team-less platform request; it
        # must never grant access to a BYO provider connection.
        if (
            ctx.team_id is not None
            and req.loom is not None
            and req.loom.team_id != str(ctx.team_id)
        ):
            raise HTTPException(
                status_code=403,
                detail="loom.team_id does not match token's team",
            )

        conn_uuid = resolve_optional_provider_connection_id(
            ctx,
            header_value=x_loom_provider_connection_id,
            body_value=(req.loom.provider_connection_id if req.loom is not None else None),
        )

        # BYO routing (#178): resolve + decrypt now while the session is
        # open. The connection row + decrypted key feed dispatch below.
        if conn_uuid is not None:
            if ctx.team_id is None:
                raise HTTPException(
                    status_code=403,
                    detail="team-scoped token required for BYO provider routing",
                )
            byo_row = await resolve_facade_connection(
                session,
                conn_uuid,
                ctx.team_id,
                supported_types=_BYO_SUPPORTED_TYPES,
                dialect_label="chat-completions",
            )
            byo_api_key = await decrypt_facade_api_key(session, byo_row)

    if req.loom is None and (ctx.team_id is None or ctx.trial_id is None or ctx.step_id is None):
        raise HTTPException(
            status_code=400,
            detail="loom attribution is required without a step-scoped token",
        )
    try:
        audit_team_id = (
            ctx.team_id if ctx.team_id is not None else UUID(req.loom.team_id)  # type: ignore[union-attr]
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail=(f"loom.team_id is not a valid UUID: {exc}"),
        ) from exc
    try:
        audit_trial_id = (
            ctx.trial_id if ctx.trial_id is not None else UUID(req.loom.trial_id)  # type: ignore[union-attr]
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail=(f"loom.trial_id is not a valid UUID: {exc}"),
        ) from exc
    audit_step_id = (
        ctx.step_id if ctx.step_id is not None else req.loom.step_id  # type: ignore[union-attr]
    )
    # #672 PR-3: the caller may declare a non-default dialect string
    # so llm_calls audit rows attribute the spend correctly (e.g. the
    # family-run skill_patcher_llm adapter sends ``family_evolver``).
    # None → keep the historical ``openai_chat`` label.
    audit_dialect = (req.loom.dialect if req.loom is not None else None) or "openai_chat"
    requested_tier = req.loom.tier if req.loom is not None else None
    requested_region = req.loom.region if req.loom is not None else None

    # Provider extraction: "provider/name" or bare "name" (defaults openai).
    raw_model = req.model
    if "/" in raw_model:
        provider, model_name = raw_model.split("/", 1)
    else:
        provider, model_name = "openai", raw_model
    await authorize_trial_model(request, ctx, provider=provider, model=model_name)

    # Bug 6 fix: reject unsupported providers upfront with a clear message
    # rather than passing api_key=None into LiteLLM and getting a cryptic
    # upstream error.
    if provider not in _SUPPORTED_PROVIDERS:
        raise HTTPException(
            status_code=400,
            detail=(f"unsupported provider {provider!r}; allowed: {sorted(_SUPPORTED_PROVIDERS)}"),
        )

    # PR-D: worker-spawned vLLM bypasses the gateway entirely. If a
    # request lands here something is misconfigured upstream.
    if provider == "local-vllm":
        raise HTTPException(
            status_code=501,
            detail=(
                "local-vllm execution is handled by the worker, not "
                "the gateway. The agent should call the worker's local "
                "vLLM directly. If you see this error, the worker "
                "dispatcher is forwarding HF+local-vllm requests to "
                "the gateway by mistake."
            ),
        )

    # PR-D: per-provider dispatch. Three paths converge into a single
    # acompletion call with different (model_string, api_key, api_base,
    # rate_card) tuples. Local servers don't yet have rate cards →
    # cost=0; HF Inference models often don't either → cost=0.
    api_base: str | None = None
    entry: object | None
    if provider == "local":
        # `local/<server>/<id>` — split off the server name.
        try:
            server_name, local_model_id = model_name.split("/", 1)
        except ValueError as exc:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"local model string must be 'local/<server>/<model_id>', got {raw_model!r}"
                ),
            ) from exc
        cfg = settings.local_providers.get(server_name)
        if cfg is None:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"local server {server_name!r} is not configured. "
                    "Set LOOM_GW_LOCAL_<NAME>_BASE_URL."
                ),
            )
        # LiteLLM's openai/ dialect + api_base override hits any
        # OpenAI-compatible endpoint (ollama, vLLM, lm-studio, etc.).
        litellm_model = f"openai/{local_model_id}"
        api_key = cfg.api_key or "no-key"
        api_base = cfg.base_url
        entry = None
    elif provider == "huggingface":
        # LiteLLM resolves `huggingface/<id>` against HF Inference
        # Endpoints. Requires HF_TOKEN in the gateway env (LiteLLM
        # auto-reads it).
        litellm_model = raw_model
        api_key = (
            settings.huggingface_api_key.get_secret_value()
            if (settings.huggingface_api_key is not None)
            else None
        )
        # HF models may not have a rate card; allow None → cost=0.
        table = await request.app.state.rate_card_cache.get()
        spec = ModelSpec(
            provider=provider,
            name=model_name,
            tier=requested_tier,
            region=requested_region,
        )
        try:
            entry = lookup_entry(table, spec)
        except RateCardNotFoundError:
            entry = None
    elif byo_row is not None:
        # BYO connection path (#178 + #179): override destination +
        # auth so the call hits the team's upstream, not the platform-
        # default endpoint. Cost honors the connection's
        # ``pricing_source`` — tokens-only / operator-supplied skip the
        # rate-card lookup that would otherwise 400 the request (#179).
        # The cost branch below routes through ``compute_facade_cost_usd``
        # rather than the legacy entry-based path; ``entry`` stays None
        # to make the type checker happy.
        #
        # litellm dispatches by the model string's provider prefix:
        # `anthropic/X` → POST {api_base}/v1/messages, `openai/X` →
        # POST {api_base}/chat/completions, etc. For openai-compatible
        # BYO endpoints (yibuapi, openrouter, deepinfra, …) the proxy
        # serves every model via the OpenAI dialect — so force the
        # model string to `openai/<name>` regardless of what the agent
        # claimed the upstream provider was. Otherwise base_urls like
        # `https://yibuapi.com/v1` get doubled to `/v1/v1/messages`
        # because litellm's anthropic transport appends `/v1/messages`.
        if byo_row.provider_type in ("openai-compatible", "custom"):
            litellm_model = f"openai/{model_name}"
        else:
            # `anthropic` / `google` BYO endpoints: pass through —
            # litellm's native transport for that provider will
            # construct the upstream URL itself.
            litellm_model = raw_model
        api_key = byo_api_key
        api_base = byo_row.base_url
        entry = None
    else:
        # Existing api-providers path (anthropic / openai / together).
        litellm_model = raw_model
        api_key = _pick_api_key(provider, settings)
        table = await request.app.state.rate_card_cache.get()
        spec = ModelSpec(
            provider=provider,
            name=model_name,
            tier=requested_tier,
            region=requested_region,
        )
        try:
            entry = lookup_entry(table, spec)
        except RateCardNotFoundError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    extra_kwargs = {k: v for k, v in raw_body.items() if k not in _RESERVED_BODY_KEYS}
    chat_messages = _omit_none_chat_message_fields(req.messages)
    started = time.monotonic()
    attempt = 1
    if byo_row is not None and byo_row.provider_type in ("openai-compatible", "custom"):
        raw, attempt = await _forward_openai_compatible_byo_chat(
            egress_client_pool=request.app.state.egress_client_pool,
            session_factory=request.app.state.session_factory,
            connection_id=byo_row.id,
            base_url=byo_row.base_url,
            api_key=api_key or "",
            model_name=model_name,
            messages=chat_messages,
            extra_kwargs=extra_kwargs,
            timeout=settings.upstream_timeout_sec,
            settings=settings,
            audit_team_id=audit_team_id,
            audit_trial_id=audit_trial_id,
            audit_step_id=audit_step_id,
            audit_dialect=audit_dialect,
            provider_label=byo_row.provider_type,
            request_params=normalize_request_params(raw_body),
            attempt_deadline=request_attempt_deadline(request),
            dispatch_audit=request_dispatch_audit(
                request,
                dialect="chat_byo",
                provider_connection_id=byo_row.id,
            ),
            auth_context=ctx,
        )
    else:
        acompletion_kwargs = dict(
            model=litellm_model,
            messages=chat_messages,
            api_key=api_key,
            timeout=settings.upstream_timeout_sec,
            **extra_kwargs,
        )
        if api_base is not None:
            acompletion_kwargs["api_base"] = api_base
        try:
            deadline = request_attempt_deadline(request)
            if deadline is not None:
                acompletion_kwargs["timeout"] = deadline.cap_seconds(settings.upstream_timeout_sec)
            audit = request_dispatch_audit(
                request,
                dialect="chat_litellm",
                provider_connection_id=byo_row.id if byo_row is not None else None,
                purpose="adapter_call",
            )
            if audit is not None:
                raw = await audit.send(
                    lambda: litellm_wrapper.acompletion(**acompletion_kwargs),
                    deadline=deadline,
                )
            elif deadline is not None:
                raw = await deadline.run(lambda: litellm_wrapper.acompletion(**acompletion_kwargs))
            else:
                raw = await litellm_wrapper.acompletion(**acompletion_kwargs)
        except AttemptDeadlineReachedError as exc:
            raise_deadline_http_exception(exc)
        except (HTTPException, DispatchAuditUnavailableError):
            raise
        except Exception as exc:
            detail = _redact_provider_exception(exc, api_key)
            async with request.app.state.session_factory() as audit_session:
                await record_failed_call(
                    audit_session,
                    auth_context=ctx,
                    team_id=audit_team_id,
                    trial_id=audit_trial_id,
                    step_id=audit_step_id,
                    dialect=audit_dialect,
                    model=model_name,
                    provider=byo_row.provider_type if byo_row is not None else provider,
                    request_params=normalize_request_params(raw_body),
                    failure_category="upstream_transport",
                    failure_error_type=type(exc).__name__,
                )
            logger.warning(
                "upstream LiteLLM completion failed provider=%s model=%s error=%s",
                provider,
                model_name,
                detail,
            )
            raise HTTPException(
                status_code=502,
                detail=f"upstream provider call failed: {detail}",
            ) from None
    duration_sec = time.monotonic() - started

    # Parse + cost. PR-D: rate cards are optional for `local` and
    # `huggingface` paths; when entry is None we report cost=0 rather
    # than 400-ing the call. Future work: per-server pricing tables.
    # #178/#179: BYO connections delegate cost to the facade helper,
    # which honors the connection's ``pricing_source``.
    parsed = litellm_wrapper.parse_litellm_response(raw, provider=provider)
    rate_card_hash: str
    usage_for_audit = TokenUsage(
        input_tokens=parsed.input_tokens,
        output_tokens=parsed.output_tokens,
        provider_extras=dict(parsed.provider_extras),
    )
    if byo_row is not None:
        # TokenUsage's cached_input_tokens / cache_write_tokens are
        # computed properties summing dialect-specific extras keys;
        # feed them via provider_extras using Anthropic's canonical
        # names (matches the worker's parse_litellm_response output).
        usage_for_pricing = TokenUsage(
            input_tokens=parsed.input_tokens,
            output_tokens=parsed.output_tokens,
            provider_extras={
                **dict(parsed.provider_extras),
                "cache_read_input_tokens": parsed.cached_input_tokens,
                "cache_creation_input_tokens": parsed.cache_write_tokens,
            },
        )
        if getattr(byo_row, "pricing_config", None) is not None:
            from loom_llm_gateway.dialect import DIALECTS
            usage_for_pricing = DIALECTS["openai_chat"].extract_tokens(parsed.raw_response)
        cost_estimate = await compute_facade_cost_estimate(
            byo_row,
            model_name,
            usage_for_pricing,
            rate_card_cache=request.app.state.rate_card_cache,
        )
        cost = cost_estimate.cost_usd
        rate_card_hash = cost_estimate.rate_card_hash
        usage_for_audit = token_usage_with_cost_metadata(
            usage_for_pricing,
            cost_estimate,
        )
    elif entry is None:
        cost = 0.0
        # Build response. PR-D: `local` provider skips the rate-card
        # lookup → no table was loaded; report a sentinel hash so the
        # field still has a deterministic shape downstream.
        rate_card_hash = "local-server-no-card" if provider == "local" else hash_table(table)
    else:
        cost = compute_cost_usd(
            entry,
            input_tokens=parsed.input_tokens,
            output_tokens=parsed.output_tokens,
            cached_input_tokens=parsed.cached_input_tokens,
            cache_write_tokens=parsed.cache_write_tokens,
        )
        rate_card_hash = hash_table(table)
    loom_block = {
        "input_tokens": parsed.input_tokens,
        "cached_input_tokens": parsed.cached_input_tokens,
        "cache_write_tokens": parsed.cache_write_tokens,
        "output_tokens": parsed.output_tokens,
        "thinking_tokens": parsed.thinking_tokens,
        "provider_extras": parsed.provider_extras,
        "cost_usd": cost,
        "rate_card_hash": rate_card_hash,
        "finish_reason": parsed.finish_reason,
        "duration_sec": duration_sec,
        "streamed": False,
        "time_to_first_token_sec": None,
        "gateway_request_id": str(uuid_lib.uuid4()),
        "attempt": attempt,
    }
    async with request.app.state.session_factory() as audit_session:
        await record_call(
            audit_session,
            team_id=audit_team_id,
            trial_id=audit_trial_id,
            step_id=audit_step_id,
            dialect=audit_dialect,
            model=model_name,
            usage=usage_for_audit,
            cost_usd=cost,
            rate_card_hash=rate_card_hash,
            provider=byo_row.provider_type if byo_row is not None else provider,
            attempt=attempt,
            request_params=normalize_request_params(raw_body),
        )
    response = dict(parsed.raw_response)
    response["loom"] = loom_block
    return response


def _pick_api_key(provider: str, settings: Any) -> str | None:
    if provider == "anthropic" and settings.anthropic_api_key is not None:
        return str(settings.anthropic_api_key.get_secret_value())
    if provider == "openai" and settings.openai_api_key is not None:
        return str(settings.openai_api_key.get_secret_value())
    if provider == "together" and settings.together_api_key is not None:
        return str(settings.together_api_key.get_secret_value())
    return None


def _redact_provider_exception(exc: Exception, api_key: str | None) -> str:
    detail = redact_api_key(str(exc), api_key or "")
    return detail or exc.__class__.__name__


def _omit_none_chat_message_fields(
    messages: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    return [
        {key: value for key, value in message.items() if value is not None} for message in messages
    ]


async def _forward_openai_compatible_byo_chat(
    *,
    egress_client_pool: Any,
    session_factory: Any,
    connection_id: UUID,
    base_url: str,
    api_key: str,
    model_name: str,
    messages: list[dict[str, Any]],
    extra_kwargs: dict[str, Any],
    timeout: float,
    settings: Any,
    audit_team_id: UUID,
    audit_trial_id: UUID,
    audit_step_id: str,
    audit_dialect: str,
    provider_label: str,
    request_params: dict[str, Any],
    attempt_deadline: GatewayAttemptDeadline | None = None,
    dispatch_audit: DispatchAudit | None = None,
    auth_context: AuthContext | None = None,
) -> tuple[dict[str, Any], int]:
    """Forward BYO OpenAI-compatible chat through the egress client pool.

    LiteLLM cannot reliably inject the CONNECT-level
    ``x-loom-connection-id`` proxy header Envoy matches on. For
    OpenAI-compatible BYO endpoints we own the wire shape, so the chat
    route sends the request directly through ``EgressClientPool``.

    Returns `(body, attempt)` — attempt is the 1-indexed gateway-
    internal try that produced the body (#298 Slice B). Caller threads
    it into `record_call(attempt=...)` for the llm_calls row.
    """
    upstream_url = f"{base_url.rstrip('/')}/chat/completions"
    payload = {
        "model": model_name,
        "messages": messages,
        **extra_kwargs,
    }
    headers = {
        "Authorization": f"Bearer {api_key}",
        "content-type": "application/json",
    }
    upstream: httpx.AsyncClient = await egress_client_pool.get(connection_id)
    try:
        outcome = await send_with_retry(
            lambda: upstream.post(
                upstream_url,
                json=payload,
                headers=headers,
                timeout=(
                    timeout if attempt_deadline is None else attempt_deadline.httpx_timeout(timeout)
                ),
                follow_redirects=False,
            ),
            settings=settings,
            dialect="chat_byo",
            deadline=attempt_deadline,
            dispatch_audit=dispatch_audit,
        )
    except AttemptDeadlineReachedError as exc:
        raise_deadline_http_exception(exc)
    except httpx.TimeoutException as exc:
        async with session_factory() as audit_session:
            await record_failed_call(
                audit_session,
                auth_context=auth_context,
                team_id=audit_team_id,
                trial_id=audit_trial_id,
                step_id=audit_step_id,
                dialect=audit_dialect,
                model=model_name,
                provider=provider_label,
                request_params=request_params,
                failure_category="upstream_timeout",
                failure_error_type=type(exc).__name__,
            )
        raise HTTPException(
            status_code=504,
            detail=f"upstream timeout against {upstream_url}: {exc}",
        ) from exc
    except httpx.RequestError as exc:
        detail = redact_api_key(str(exc), api_key)
        async with session_factory() as audit_session:
            await record_failed_call(
                audit_session,
                auth_context=auth_context,
                team_id=audit_team_id,
                trial_id=audit_trial_id,
                step_id=audit_step_id,
                dialect=audit_dialect,
                model=model_name,
                provider=provider_label,
                request_params=request_params,
                failure_category="upstream_transport",
                failure_error_type=type(exc).__name__,
            )
        raise HTTPException(
            status_code=502,
            detail=(
                f"upstream request error against {upstream_url}: {type(exc).__name__}: {detail}"
            ),
        ) from exc

    upstream_response = outcome.response
    if upstream_response.status_code >= 400:
        excerpt = redact_api_key(upstream_response.text, api_key)
        async with session_factory() as audit_session:
            await record_failed_call(
                audit_session,
                auth_context=auth_context,
                team_id=audit_team_id,
                trial_id=audit_trial_id,
                step_id=audit_step_id,
                dialect=audit_dialect,
                model=model_name,
                provider=provider_label,
                attempt=outcome.attempt,
                request_params=request_params,
                failure_category=_http_failure_category(upstream_response.status_code),
                failure_status_code=upstream_response.status_code,
            )
        raise HTTPException(
            status_code=upstream_response.status_code,
            detail=(f"upstream returned {upstream_response.status_code}: {excerpt}"),
        )

    try:
        body = upstream_response.json()
    except ValueError as exc:
        raise HTTPException(
            status_code=502,
            detail=f"upstream returned non-JSON body: {exc}",
        ) from exc
    if not isinstance(body, dict):
        raise HTTPException(
            status_code=502,
            detail="upstream returned a non-object JSON body",
        )
    return body, outcome.attempt


def _http_failure_category(status_code: int) -> str:
    if 400 <= status_code < 500:
        return "upstream_http_4xx"
    if 500 <= status_code < 600:
        return "upstream_http_5xx"
    return "upstream_http_error"
