"""Shared facade helpers used by `/openai/v1/...`, `/anthropic/v1/...`,
and `/google/v1beta/...` routes.

Each dialect-specific route is a thin module that:
1. Calls `verify_facade_auth` to validate the step-JWT.
2. Calls `parse_connection_id_header` to extract + UUID-parse the
   `x-loom-provider-connection-id` header.
3. Calls `resolve_facade_connection` to look up the connection and
   restrict by team ownership/share + supported type.
4. Decrypts the api_key via SecretStore.
5. Builds the dialect-specific upstream request (URL + headers).
6. Forwards and audits via `record_call`.

The helpers below cover steps 1-3 + the common error paths so each
dialect route stays focused on the upstream contract.
"""

from __future__ import annotations

import logging
import re
from typing import Any
from uuid import UUID

from fastapi import HTTPException, Request
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError

from loom.auth import AuthContext
from loom.db.schema import ProviderConnection, ProviderConnectionShare
from loom.models.types import ModelSpec
from loom.security.redaction import redact_text
from loom.security.secret_store import (
    DecryptError,
    InvalidRefError,
    LocalEncryptedSecretStore,
    SecretNotFoundError,
)
from loom_llm_gateway.dialect import TokenUsage
from loom_llm_gateway.errors import RateCardNotFoundError
from loom_llm_gateway.execution_attempt_dispatch import (
    authorize_execution_attempt_dispatch,
    authorize_trial_execution_dispatch,
)
from loom_llm_gateway.llm_calls import record_failed_call
from loom_llm_gateway.rate_card import (
    CostEstimate,
    RateCardCache,
    compute_cost_usd,
    hash_table,
    lookup_entry,
)
from loom_llm_gateway.request_params import normalize_request_params
from loom_llm_gateway.routes._auth import require_llm_call_bearer
from loom_llm_gateway.yibuapi_pricing import (
    YIBUAPI_RATE_CARD_PROVIDER,
    normalize_yibuapi_model_name,
)

_TYPE_TO_DEFAULT_RATE_CARD_PROVIDER = {
    "anthropic": "anthropic",
    "google": "google",
    "openai-compatible": "openai",
}

_LOG = logging.getLogger(__name__)

_BEARER_VALUE_RE = re.compile(
    r"(?i)(\bBearer\s+)([A-Za-z0-9._~+/=-]{4,})",
)
_SECRET_FIELD_RE = re.compile(
    r"(?i)(api[_-]?key|authorization|bearer|token|secret|password|cookie)",
)


async def verify_facade_auth(
    session: Any,
    authorization: str | None,
    signing_key: str,
    *,
    allow_execution_attempt: bool = False,
    request: Request | None = None,
) -> AuthContext:
    """Step-scoped JWT auth path shared by all facade routes.

    Returns the validated AuthContext with non-None trial_id, step_id,
    and team_id. Raises 401 / 403 directly on auth failure.
    """
    ctx = await require_llm_call_bearer(
        session,
        authorization,
        signing_key=signing_key,
        request=request,
    )
    if ctx.token_subject is None or ctx.step_id is None or ctx.team_id is None:
        raise HTTPException(
            status_code=403,
            detail="step-scoped token required (loom_step_<jwt>)",
        )
    if ctx.execution_attempt_id is not None and not allow_execution_attempt:
        raise HTTPException(
            status_code=403,
            detail="execution-attempt tokens are not supported by this route",
        )
    await authorize_execution_attempt_dispatch(session, ctx)
    await authorize_trial_execution_dispatch(session, ctx)
    return ctx


def parse_connection_id_header(raw: str | None) -> UUID:
    """Validate the `x-loom-provider-connection-id` header. Raises 400
    on missing or non-UUID values.

    Kept for callers that still want pure-header resolution. New code
    should prefer `resolve_provider_connection_id` which honors the
    JWT-scoped value when present (issue #72).
    """
    if not raw:
        raise HTTPException(
            status_code=400,
            detail="x-loom-provider-connection-id header is required",
        )
    try:
        return UUID(raw)
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail=(f"x-loom-provider-connection-id is not a valid UUID: {exc}"),
        ) from exc


def resolve_provider_connection_id(
    ctx: AuthContext,
    header_value: str | None,
) -> UUID:
    """Resolve the connection_id from EITHER the step-JWT scope
    (``ctx.provider_connection_id``) OR the
    ``x-loom-provider-connection-id`` header.

    Precedence rules (issue #72):
    - **Both set + match** ⇒ use the value (canonical case during
      transition; sandbox SDK can send both safely).
    - **Both set + mismatch** ⇒ 400. The JWT scope is authoritative;
      the response message says so explicitly so operators know to
      drop the header or align it.
    - **JWT only** ⇒ use the JWT value (forward-compatible: post-
      transition path).
    - **Header only** ⇒ use the header value (legacy callers; will
      keep working until the header is sunset in a follow-up).
    - **Neither** ⇒ 400. The facade has nothing to route against.

    The JWT scope binds at mint time (CP looks it up from the trial
    row); the header is operator-supplied. When they agree, the
    request is unambiguous regardless of trust level.
    """
    resolved = resolve_optional_provider_connection_id(
        ctx,
        header_value=header_value,
        body_value=None,
    )
    if resolved is not None:
        return resolved
    raise HTTPException(
        status_code=400,
        detail=(
            "provider_connection_id is required: pass via the "
            "x-loom-provider-connection-id header or mint a step-JWT "
            "with provider_connection_id in scope (issue #72)."
        ),
    )


def resolve_optional_provider_connection_id(
    ctx: AuthContext,
    *,
    header_value: str | None,
    body_value: str | None,
) -> UUID | None:
    """Resolve an optional provider connection across JWT/header/body.

    The step-JWT claim is authoritative when present. Every non-empty
    caller-controlled source must agree with it. Without a JWT claim, body and
    header are equivalent legacy transports: either one may supply the value,
    but sending both with different UUIDs is rejected. Returning ``None`` is
    intentional for routes such as ``/v1/chat/completions`` whose no-connection
    shape selects the platform-credentialed path.
    """

    header_uuid = _parse_optional_connection_id(
        header_value,
        source="x-loom-provider-connection-id",
    )
    body_uuid = _parse_optional_connection_id(
        body_value,
        source="loom.provider_connection_id",
    )
    jwt_uuid = ctx.provider_connection_id
    jwt_bound = ctx.provider_connection_id_bound or jwt_uuid is not None

    if jwt_bound:
        mismatches = [
            (source, value)
            for source, value in (
                ("header", header_uuid),
                ("body", body_uuid),
            )
            if value is not None and value != jwt_uuid
        ]
        if mismatches:
            source, value = mismatches[0]
            raise HTTPException(
                status_code=400,
                detail=(
                    "provider_connection_id mismatch: JWT scope says "
                    f"{jwt_uuid or 'platform'}, {source} says {value}. The JWT scope is "
                    "authoritative — drop the conflicting value or align it "
                    "to the JWT value."
                ),
            )
        return jwt_uuid

    if header_uuid is not None and body_uuid is not None:
        if header_uuid != body_uuid:
            raise HTTPException(
                status_code=400,
                detail=(
                    "provider_connection_id mismatch: body says "
                    f"{body_uuid}, header says {header_uuid}. Align the two "
                    "values or send only one."
                ),
            )
        return body_uuid
    return body_uuid or header_uuid


def _parse_optional_connection_id(
    raw: str | None,
    *,
    source: str,
) -> UUID | None:
    if not raw:
        return None
    try:
        return UUID(raw)
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail=f"{source} is not a valid UUID: {exc}",
        ) from exc


async def resolve_facade_connection(
    session: Any,
    connection_id: UUID,
    team_id: UUID,
    *,
    supported_types: frozenset[str],
    dialect_label: str,
    allow_request_pool: bool = False,
) -> ProviderConnection:
    """Lookup, ownership/share-scope, soft-delete, and dialect-type checks all in
    one helper. 404 (not 403) on cross-team to match loom_service's
    existence-hiding convention; the 400 on type mismatch surfaces a
    diagnostic hint pointing the operator at the matched facade.

    Cross-team 404 fires BEFORE the dialect check so cross-team
    callers can't probe which provider_type a connection has.
    """
    row: ProviderConnection | None = (
        await session.execute(
            select(ProviderConnection).where(
                ProviderConnection.id == connection_id,
                ProviderConnection.deleted_at.is_(None),
            ),
        )
    ).scalar_one_or_none()
    if row is None:
        raise HTTPException(
            status_code=404,
            detail="provider_connection not found",
        )
    if row.team_id != team_id:
        shared = (
            await session.execute(
                select(ProviderConnectionShare.provider_connection_id).where(
                    ProviderConnectionShare.provider_connection_id == connection_id,
                    ProviderConnectionShare.target_team_id == team_id,
                ),
            )
        ).scalar_one_or_none()
        if shared is None:
            raise HTTPException(
                status_code=404,
                detail="provider_connection not found",
            )
    if row.provider_type not in supported_types:
        raise HTTPException(
            status_code=400,
            detail=(
                f"provider_connection.type={row.provider_type!r} is "
                f"not served by {dialect_label}; use the "
                f"dialect-matched facade for this provider type."
            ),
        )
    if not allow_request_pool:
        from loom.db.schema import ProviderRequestPool

        pool_enabled = await session.scalar(
            select(ProviderRequestPool.enabled).where(
                ProviderRequestPool.connection_id == connection_id,
            )
        )
        if pool_enabled:
            raise HTTPException(
                status_code=400,
                detail="request pool requires hosted Terminus2 non-streaming complete-messages Chat",
            )
    return row


def token_usage_with_cost_metadata(
    usage: TokenUsage,
    estimate: CostEstimate,
) -> TokenUsage:
    extras = dict(usage.provider_extras)
    extras.update(estimate.provider_extras())
    return TokenUsage(
        input_tokens=usage.input_tokens,
        output_tokens=usage.output_tokens,
        provider_extras=extras,
    )


async def compute_facade_cost_estimate(
    row: ProviderConnection,
    model_name: str,
    usage: TokenUsage,
    *,
    rate_card_cache: RateCardCache | None,
) -> CostEstimate:
    """Compute facade call cost and the audit hash/marker.

    `operator-supplied` and `tokens-only` keep the existing marker
    contract. `rate-card` looks up the connection's explicit
    `rate_card_provider`, falling back to safe provider-type defaults
    where possible. Missing provider/model rows degrade to cost=0 with
    an explainable marker for downstream billing audits.
    """
    if getattr(row, "pricing_config", None) is not None:
        from loom_llm_gateway.provider_pricing import configured_cost

        if rate_card_cache is not None:
            try:
                async with rate_card_cache.session_factory() as session:
                    return await configured_cost(session, row, model_name, usage)
            except SQLAlchemyError as exc:
                # The provider has already responded. Close the failed pricing
                # transaction before the caller records usage in a fresh one.
                # Never log SQL/parameters or invent a resolved zero price.
                _LOG.error("catalog_lookup_failed: %s", type(exc).__name__)
                return CostEstimate(
                    0, "facade:price-unknown", "unpriced", "unavailable", None,
                    pricing_source="rate-card", unpriced_reason="catalog_lookup_failed",
                )
        return await configured_cost(None, row, model_name, usage)

    if row.pricing_source == "operator-supplied":
        data = row.pricing_data or {}
        try:
            in_per_1m = float(data.get("input_usd_per_1m", 0) or 0)
            out_per_1m = float(data.get("output_usd_per_1m", 0) or 0)
        except (TypeError, ValueError):
            return CostEstimate(
                cost_usd=0.0,
                rate_card_hash="facade:operator-supplied:invalid",
                source="unpriced",
                confidence="unavailable",
                currency=None,
                pricing_source="operator-supplied",
                unpriced_reason="invalid_operator_pricing",
            )
        cost = (usage.input_tokens / 1_000_000.0) * in_per_1m
        cost += (usage.output_tokens / 1_000_000.0) * out_per_1m
        return CostEstimate(
            cost_usd=cost,
            rate_card_hash="facade:operator-supplied",
            source="operator-supplied",
            confidence="configured",
            currency="USD",
            pricing_source="operator-supplied",
        )

    if row.pricing_source != "rate-card":
        return CostEstimate(
            cost_usd=0.0,
            rate_card_hash=f"facade:{row.pricing_source}",
            source="tokens-only",
            confidence="not_applicable",
            currency=None,
            pricing_source=row.pricing_source,
        )

    lookup_provider = row.rate_card_provider or _TYPE_TO_DEFAULT_RATE_CARD_PROVIDER.get(
        row.provider_type
    )
    if lookup_provider is None or rate_card_cache is None:
        return CostEstimate(
            cost_usd=0.0,
            rate_card_hash="facade:rate-card:missing",
            source="unpriced",
            confidence="unavailable",
            currency=None,
            pricing_source="rate-card",
            rate_card_provider=lookup_provider,
            unpriced_reason="rate_card_unavailable",
        )

    try:
        table = await rate_card_cache.get()
        lookup_model = (
            normalize_yibuapi_model_name(model_name)
            if lookup_provider == YIBUAPI_RATE_CARD_PROVIDER
            else model_name
        )
        entry = lookup_entry(
            table,
            ModelSpec(provider=lookup_provider, name=lookup_model),
        )
    except RateCardNotFoundError:
        return CostEstimate(
            cost_usd=0.0,
            rate_card_hash="facade:rate-card:missing",
            source="unpriced",
            confidence="unavailable",
            currency=None,
            pricing_source="rate-card",
            rate_card_provider=lookup_provider,
            unpriced_reason="missing_rate_card_entry",
        )

    cost = compute_cost_usd(
        entry,
        input_tokens=usage.input_tokens,
        output_tokens=usage.output_tokens,
        # Keep legacy catalog behavior until an explicit pricing-mode migration.
        # OpenAI cache counters were not extracted in the legacy path; Gemini's
        # existing counters remain unchanged. New configurations normalize their
        # inclusive input totals in configured_cost instead.
        cached_input_tokens=(0 if usage.provider_extras.get("_loom_input_includes_cache")
                             and "cachedContentTokenCount" not in usage.provider_extras
                             else usage.cached_input_tokens),
        cache_write_tokens=usage.cache_write_tokens,
    )
    return CostEstimate(
        cost_usd=cost,
        rate_card_hash=hash_table(table),
        source="rate-card",
        confidence="configured",
        currency=entry.currency or table.currency or "USD",
        pricing_source="rate-card",
        rate_card_provider=lookup_provider,
    )


async def compute_facade_cost_usd(
    row: ProviderConnection,
    model_name: str,
    usage: TokenUsage,
    *,
    rate_card_cache: RateCardCache | None,
) -> tuple[float, str]:
    estimate = await compute_facade_cost_estimate(
        row,
        model_name,
        usage,
        rate_card_cache=rate_card_cache,
    )
    return estimate.cost_usd, estimate.rate_card_hash


async def decrypt_facade_api_key(
    session: Any,
    row: ProviderConnection,
) -> str:
    """Decrypt the provider api_key, translating SecretStore failures
    into controlled 502 responses so the facade never bubbles an
    unhandled traceback on a row with a malformed/missing/undecryptable
    stored ref. #423

    Three failure shapes are distinguished so the operator can pick the
    right repair path (see operator-runbook.md):

    - `malformed_ref` — `encrypted_api_key_ref` doesn't parse as the
      runtime-supported `loom://<namespace>/<uuid>` shape. Legacy rows
      created before secret-store enforcement (e.g. argv-style
      `env:STAGING_SMOKE_OPENAI` strings) hit this. Fix:
      `loom providers rotate-key <name> --api-key env:<NEW>`.
    - `missing_secret` — ref is well-formed but the `secrets` row was
      pruned. Same operator fix.
    - `decrypt_failed` — ciphertext won't validate (wrong master key,
      mid-rotation with no fallback configured). Restore the master
      key, deploy with `LOOM_SECRET_STORE_MASTER_KEYS` carrying both
      old and new, or run `loom admin secret-store rewrap`.

    The status flip lives in the service `/test` endpoint, not here —
    a transient gateway hot-path write to `provider_connections` would
    risk flipping every row to invalid during a master-key blip.
    """
    store = LocalEncryptedSecretStore(session)
    try:
        return await store.get(row.encrypted_api_key_ref)
    except InvalidRefError:
        raise HTTPException(
            status_code=502,
            detail=(
                f"provider_connection {row.id} stored api_key reference is "
                "malformed (kind=malformed_ref). The administrator "
                f"must repair this connection via "
                f"`loom providers rotate-key`."
            ),
        ) from None
    except SecretNotFoundError:
        raise HTTPException(
            status_code=502,
            detail=(
                f"provider_connection {row.id} stored api_key secret is "
                "missing (kind=missing_secret). The administrator "
                f"must re-register the api_key via "
                f"`loom providers rotate-key`."
            ),
        ) from None
    except DecryptError:
        raise HTTPException(
            status_code=502,
            detail=(
                f"provider_connection {row.id} stored api_key cannot be "
                "decrypted (kind=decrypt_failed). Restore the "
                f"SecretStore master key (or its rotation fallback) and "
                f"retry."
            ),
        ) from None


def redact_api_key(text: str, api_key: str, *, limit: int = 500) -> str:
    """Scrub the decrypted api_key from an excerpt before it lands in
    a user-visible error string. Same 4-char minimum as
    `provider_connections_service._redact_secret` to avoid
    over-redaction on degenerate inputs (1-3 char secrets would
    obliterate common substrings). The central redactor also removes
    signed URLs, secret refs, and internal-only endpoints.
    """
    excerpt = (text or "")[:limit]
    if api_key and len(api_key) >= 4:
        excerpt = excerpt.replace(api_key, "[REDACTED]")
    excerpt = _BEARER_VALUE_RE.sub(r"\1[REDACTED]", excerpt)
    return redact_text(excerpt)


def _redact_raw_value(value: Any, api_key: str) -> Any:
    if isinstance(value, dict):
        redacted: dict[str, Any] = {}
        for key, item in value.items():
            key_text = str(key)
            if _SECRET_FIELD_RE.search(key_text):
                redacted[key_text] = "[REDACTED]"
            else:
                redacted[key_text] = _redact_raw_value(item, api_key)
        return redacted
    if isinstance(value, list):
        return [_redact_raw_value(item, api_key) for item in value]
    if isinstance(value, str):
        out = value
        if api_key and len(api_key) >= 4:
            out = out.replace(api_key, "[REDACTED]")
        out = _BEARER_VALUE_RE.sub(r"\1[REDACTED]", out)
        return redact_text(out)
    return value


def _redact_raw_headers(headers: dict[str, str], api_key: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for key, value in headers.items():
        key_l = key.lower()
        if key_l == "authorization":
            out[key_l] = str(_redact_raw_value(value, api_key))
        elif _SECRET_FIELD_RE.search(key_l):
            out[key_l] = "[REDACTED]"
        else:
            out[key_l] = str(_redact_raw_value(value, api_key))
    return out


def build_raw_provider_log(
    *,
    dialect: str,
    provider: str,
    provider_connection_id: UUID,
    attempt: int,
    request_method: str,
    request_url: str,
    request_headers: dict[str, str],
    request_body: dict[str, Any],
    response_status_code: int,
    response_headers: dict[str, str],
    response_body: dict[str, Any],
    api_key: str,
    auth_context: AuthContext | None = None,
) -> dict[str, Any]:
    """Build the redacted raw request/response record for export.

    The provider prompt/assistant body is intentionally retained for
    downstream training/audit handoff; only secret-bearing fields, auth
    headers, bearer values, and known secret text are scrubbed.
    """

    result = {
        "schema_version": "1",
        "dialect": dialect,
        "provider": provider,
        "provider_connection_id": str(provider_connection_id),
        "attempt": max(int(attempt or 1), 1),
        "request": {
            "method": request_method.upper(),
            "url": str(_redact_raw_value(request_url, api_key)),
            "headers": _redact_raw_headers(request_headers, api_key),
            "body": _redact_raw_value(request_body, api_key),
        },
        "response": {
            "status_code": int(response_status_code),
            "headers": _redact_raw_headers(response_headers, api_key),
            "body": _redact_raw_value(response_body, api_key),
        },
    }
    # This attribution comes from the authenticated, currently fenced step JWT,
    # never from client request fields. It distinguishes repeated Trial attempts
    # whose upstream completions may be persisted at overlapping wall times.
    if auth_context is not None and auth_context.service_execution_lease_id is not None:
        if auth_context.service_execution_generation is None:
            raise ValueError("service execution call lacks its generation")
        result["service_execution"] = {
            "lease_id": str(auth_context.service_execution_lease_id),
            "generation": auth_context.service_execution_generation,
        }
    return result


def http_failure_category(status_code: int) -> str:
    if 400 <= status_code < 500:
        return "upstream_http_4xx"
    if 500 <= status_code < 600:
        return "upstream_http_5xx"
    return "upstream_http_error"


async def record_facade_failed_call(
    *,
    request: Any,
    ctx: AuthContext,
    row: ProviderConnection,
    dialect: str,
    model: str,
    request_payload: dict[str, Any],
    failure_category: str,
    attempt: int = 1,
    failure_status_code: int | None = None,
    failure_error_type: str | None = None,
    correlation: dict[str, Any] | None = None,
    request_pool_attribution: dict[str, Any] | None = None,
) -> None:
    assert ctx.team_id is not None
    assert ctx.trial_id is not None
    assert ctx.step_id is not None
    corr = correlation or {}
    async with request.app.state.session_factory() as audit_session:
        await record_failed_call(
            audit_session,
            auth_context=ctx,
            team_id=ctx.team_id,
            trial_id=ctx.trial_id,
            step_id=ctx.step_id,
            dialect=dialect,
            model=model,
            provider=row.provider_type,
            attempt=attempt,
            request_params=normalize_request_params(request_payload),
            failure_category=failure_category,
            request_pool_attribution=request_pool_attribution,
            failure_status_code=failure_status_code,
            failure_error_type=failure_error_type,
            client_call_id=corr.get("client_call_id"),
            agent_execution_id=corr.get("agent_execution_id"),
            agent_run_attempt_id=corr.get("agent_run_attempt_id"),
            episode=corr.get("episode"),
            call_ordinal=corr.get("call_ordinal"),
            requested_model=corr.get("requested_model") or model,
            role=corr.get("role"),
            correlation_status=str(corr.get("correlation_status") or "legacy_uncorrelated"),
        )
