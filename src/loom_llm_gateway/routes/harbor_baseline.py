"""Restricted OpenAI text-chat endpoint for independently run native Harbor."""

from __future__ import annotations

import asyncio
import json
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

from fastapi import APIRouter, Header, HTTPException, Request
from sqlalchemy import select

from loom.db.schema import HarborBaselineDispatch, HarborBaselineSession
from loom.harbor_baseline import (
    authenticated_baseline,
    bounded_chat_payload,
    dollars,
    model_context_bound,
    reserve_budget,
)
from loom.provider_pricing import ModelPrice, calculate_price
from loom.request_params import normalize_request_params
from loom_llm_gateway.dialect import DIALECTS, TokenUsage
from loom_llm_gateway.llm_calls import record_call, record_failed_call
from loom_llm_gateway.provider_pricing import configured_cost
from loom_llm_gateway.rate_card import CostEstimate
from loom_llm_gateway.routes._facade_common import (
    decrypt_facade_api_key,
    resolve_facade_connection,
    token_usage_with_cost_metadata,
)

router = APIRouter()
_MAX_REQUEST_BYTES = 4 * 1024 * 1024
_DIALECT = "harbor_baseline_openai_chat"


def price_upper_bound(price: ModelPrice, context: int, output: int) -> Decimal:
    # OpenAI input totals include cache reads. This route rejects modalities,
    # multiple candidates and tools; it never has an extra cache-write bill.
    input_rate = max(price.input_usd_per_1m, price.cache_read_usd_per_1m or 0)
    return dollars(
        Decimal(str(input_rate)) * context / 1_000_000
        + Decimal(str(price.output_usd_per_1m)) * output / 1_000_000
    )


def validated_usage(body: dict[str, Any], context: int, output: int) -> TokenUsage:
    raw = body.get("usage")
    if not isinstance(raw, dict) or any(
        type(raw.get(key)) is not int or raw[key] < 0
        for key in ("prompt_tokens", "completion_tokens")
    ):
        raise ValueError("baseline_usage_unavailable")
    if raw["prompt_tokens"] > context or raw["completion_tokens"] > output:
        raise ValueError("baseline_provider_token_contract_exceeded")
    usage = DIALECTS["openai_chat"].extract_tokens(body)
    if usage.provider_extras.get("_loom_unsupported_billing"):
        raise ValueError("baseline_unsupported_billing")
    if usage.cached_input_tokens > usage.input_tokens:
        raise ValueError("baseline_provider_token_contract_exceeded")
    return usage


async def fail_dispatch(
    request: Request,
    grant_id: UUID,
    dispatch_id: UUID,
    payload: dict[str, Any],
    reason: str,
) -> None:
    async with request.app.state.session_factory() as session:
        grant = await session.scalar(
            select(HarborBaselineSession)
            .where(HarborBaselineSession.id == grant_id)
            .with_for_update()
        )
        dispatch = await session.get(HarborBaselineDispatch, dispatch_id)
        assert grant is not None and dispatch is not None
        if dispatch.outcome != "reserved":
            return
        # A timeout, malformed/missing usage or lost response can have incurred
        # spend. Keep the entire reservation and require a new explicit grant.
        dispatch.outcome = reason
        grant.blocked_reason = reason
        await record_failed_call(
            session,
            team_id=grant.team_id,
            baseline_session_id=grant.id,
            step_id=str(dispatch_id),
            dialect=_DIALECT,
            model=grant.model,
            failure_category=reason,
            request_params=normalize_request_params(payload),
            commit=False,
        )
        await session.commit()


@router.post("/harbor-baseline/v1/chat/completions")
async def baseline_chat(
    request: Request,
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    async with request.app.state.session_factory() as session:
        grant = await authenticated_baseline(session, authorization)
        grant_id = grant.id
    raw = bytearray()
    async for chunk in request.stream():
        raw.extend(chunk)
        if len(raw) > _MAX_REQUEST_BYTES:
            raise HTTPException(413, "baseline_request_too_large")
    try:
        payload = json.loads(raw)
    except ValueError:
        raise HTTPException(400, "baseline_invalid_json") from None
    if not isinstance(payload, dict):
        raise HTTPException(400, "baseline_invalid_json")

    async with request.app.state.session_factory() as session:
        grant = await authenticated_baseline(session, authorization, locked=True)
        payload = bounded_chat_payload(payload, grant)
        provider = await resolve_facade_connection(
            session,
            grant.provider_connection_id,
            grant.team_id,
            supported_types=frozenset({"openai-compatible", "custom"}),
            dialect_label="Harbor baseline",
        )
        context = await model_context_bound(session, provider, grant.model)
        if context != grant.max_input_tokens:
            raise HTTPException(409, "baseline_model_context_changed")
        # One in-flight call per bearer. A process crash leaves a durable
        # reservation and cannot silently reopen the same spending authority.
        pending = await session.scalar(
            select(HarborBaselineDispatch.id)
            .where(
                HarborBaselineDispatch.baseline_session_id == grant.id,
                HarborBaselineDispatch.outcome == "reserved",
            )
            .limit(1)
        )
        if pending is not None:
            raise HTTPException(409, "baseline_dispatch_pending")
        output = payload["max_completion_tokens"]
        estimate = CostEstimate(0, "facade:tokens-only", "tokens-only", "not_applicable", None)
        price = None
        if provider.pricing_config is not None:
            estimate = await configured_cost(
                session,
                provider,
                grant.model,
                TokenUsage(context, output, {"_loom_cache_usage_known": True}),
            )
            if (
                estimate.currency == "USD"
                and estimate.price_basis
                and "prices" in estimate.price_basis
            ):
                price = ModelPrice.model_validate(estimate.price_basis["prices"])
        if grant.budget_usd is not None and price is None:
            raise HTTPException(409, "baseline_usd_price_unavailable")
        reserve_cost = price_upper_bound(price, context, output) if price else Decimal(0)
        reserve_budget(grant, tokens=context + output, cost=reserve_cost)
        dispatch_id = uuid4()
        session.add(
            HarborBaselineDispatch(
                id=dispatch_id,
                baseline_session_id=grant.id,
                reserved_tokens=context + output,
                reserved_cost_usd=reserve_cost,
                outcome="reserved",
            )
        )
        # Decryption and destination selection are existing Gateway-owned
        # boundaries. No key, endpoint or provider header reaches the client.
        api_key = await decrypt_facade_api_key(session, provider)
        expires_at = grant.expires_at
        await session.commit()

    try:
        remaining = (expires_at - datetime.now(UTC)).total_seconds()
        if remaining <= 0:
            raise ValueError("baseline_expired_before_dispatch")
        client = await request.app.state.egress_client_pool.get(provider.id)
        response = await client.post(
            provider.base_url.rstrip("/") + "/chat/completions",
            json=payload,
            headers={"Authorization": "Bearer " + api_key, "content-type": "application/json"},
            timeout=min(remaining, request.app.state.settings.upstream_timeout_sec),
            follow_redirects=False,
        )
        if response.status_code != 200:
            raise ValueError("baseline_upstream_http_error")
        body = response.json()
        if not isinstance(body, dict):
            raise ValueError("baseline_upstream_malformed_response")
        usage = validated_usage(body, context, output)
        actual_cost = Decimal(0)
        if price is not None:
            value = (
                calculate_price(
                    price,
                    input_tokens=usage.input_tokens,
                    output_tokens=usage.output_tokens,
                    cache_read_tokens=usage.cached_input_tokens,
                    input_includes_cache=True,
                )
                if usage.provider_extras.get("_loom_cache_usage_known")
                else None
            )
            if value is None:
                # Missing cache dimensions never become a fabricated free call.
                actual_cost = price_upper_bound(price, usage.input_tokens, usage.output_tokens)
                estimate = replace(estimate, confidence="upper_bound")
            else:
                actual_cost = dollars(value)
            if actual_cost > reserve_cost:
                raise ValueError("baseline_provider_price_contract_exceeded")
        estimate = replace(estimate, cost_usd=float(actual_cost))
        usage = token_usage_with_cost_metadata(usage, estimate)
        async with request.app.state.session_factory() as session:
            settled = await session.scalar(
                select(HarborBaselineSession)
                .where(HarborBaselineSession.id == grant_id)
                .with_for_update()
            )
            dispatch = await session.get(HarborBaselineDispatch, dispatch_id)
            assert settled is not None and dispatch is not None
            settled.tokens_reserved -= (
                dispatch.reserved_tokens - usage.input_tokens - usage.output_tokens
            )
            settled.cost_reserved_usd -= dispatch.reserved_cost_usd - actual_cost
            dispatch.outcome = "completed"
            await record_call(
                session,
                team_id=settled.team_id,
                baseline_session_id=settled.id,
                step_id=str(dispatch_id),
                dialect=_DIALECT,
                model=settled.model,
                usage=usage,
                cost_usd=float(actual_cost),
                rate_card_hash=estimate.rate_card_hash,
                provider=provider.provider_type,
                request_params=normalize_request_params(payload),
                response_model=body.get("model") if isinstance(body.get("model"), str) else None,
                correlation_status="legacy_uncorrelated",
                commit=False,
            )
            await session.commit()
        return body
    except (Exception, asyncio.CancelledError) as exc:
        reason = "baseline_upstream_transport"
        if isinstance(exc, ValueError):
            candidate = str(exc)
            if candidate.startswith("baseline_") and len(candidate) < 80:
                reason = candidate
        await asyncio.shield(fail_dispatch(request, grant_id, dispatch_id, payload, reason))
        if isinstance(exc, asyncio.CancelledError):
            raise
        # No provider response excerpt, endpoint, bearer or decrypt error leaks.
        raise HTTPException(502, reason) from None
