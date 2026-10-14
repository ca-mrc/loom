"""Narrow external Harbor authorization, separate from execution credentials."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from decimal import ROUND_CEILING, Decimal
from typing import Any

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import HarborBaselineSession, ProviderConnection, ProviderModelCache, Team

PREFIX = "loom_baseline_"


def token_hash(token: str) -> bytes:
    return hashlib.sha256(token.encode()).digest()


def dollars(value: float | Decimal) -> Decimal:
    return Decimal(str(value)).quantize(Decimal("0.000001"), rounding=ROUND_CEILING)


async def model_context_bound(
    session: AsyncSession,
    provider: ProviderConnection,
    model: str,
) -> int:
    """Use the provider's entire advertised context, never a prompt estimate.

    This first external baseline contract is limited to OpenAI GPT chat models:
    max_completion_tokens bounds both visible output and reasoning. Arbitrary
    compatible models need a separately qualified output-token contract.
    """
    if provider.status != "valid":
        raise HTTPException(409, "baseline_provider_not_valid")
    if not model.startswith("gpt-"):
        raise HTTPException(409, "baseline_output_token_contract_unqualified")
    cached = await session.get(ProviderModelCache, (provider.id, model))
    if (
        cached is None
        or not cached.upstream_present
        or not cached.visible
        or cached.context_length is None
        or not 0 < cached.context_length <= 2_000_000
        or (provider.allowed_models is not None and model not in provider.allowed_models)
    ):
        raise HTTPException(409, "baseline_model_context_unavailable")
    return cached.context_length


async def authenticated_baseline(
    session: AsyncSession,
    authorization: str | None,
    *,
    locked: bool = False,
) -> HarborBaselineSession:
    if not authorization or not authorization.startswith("Bearer " + PREFIX):
        raise HTTPException(401, "invalid_or_expired_baseline_bearer")
    token = authorization.removeprefix("Bearer ")
    if len(token) > 256:
        raise HTTPException(401, "invalid_or_expired_baseline_bearer")
    statement = select(HarborBaselineSession).where(
        HarborBaselineSession.token_hash == token_hash(token)
    )
    if locked:
        statement = statement.with_for_update()
    grant = await session.scalar(statement)
    if grant is None or grant.revoked_at is not None or grant.expires_at <= datetime.now(UTC):
        raise HTTPException(401, "invalid_or_expired_baseline_bearer")
    disabled = await session.scalar(select(Team.disabled_at).where(Team.id == grant.team_id))
    if disabled is not None:
        raise HTTPException(403, "baseline_team_disabled")
    if grant.blocked_reason is not None:
        raise HTTPException(409, "baseline_accounting_blocked")
    return grant


def bounded_chat_payload(payload: dict[str, Any], grant: HarborBaselineSession) -> dict[str, Any]:
    allowed = {
        "model",
        "messages",
        "temperature",
        "top_p",
        "frequency_penalty",
        "presence_penalty",
        "seed",
        "reasoning_effort",
        "response_format",
        "stop",
        "max_completion_tokens",
        "max_tokens",
        "n",
        "stream",
    }
    if set(payload) - allowed or payload.get("stream") or payload.get("n", 1) != 1:
        raise HTTPException(400, "baseline_requires_single_nonstreaming_text_chat")
    if payload.get("model") != grant.model:
        raise HTTPException(403, "baseline_model_mismatch")
    messages = payload.get("messages")
    if not isinstance(messages, list) or not messages or len(messages) > 1000:
        raise HTTPException(400, "baseline_text_messages_required")
    for message in messages:
        if (
            not isinstance(message, dict)
            or set(message) - {"role", "content", "name"}
            or message.get("role") not in {"system", "developer", "user", "assistant"}
            or not isinstance(message.get("content"), str)
            or ("name" in message and not isinstance(message["name"], str))
        ):
            raise HTTPException(400, "baseline_text_messages_required")
    if "max_tokens" in payload and "max_completion_tokens" in payload:
        raise HTTPException(400, "baseline_ambiguous_output_limit")
    maximum = payload.get(
        "max_completion_tokens", payload.get("max_tokens", grant.max_output_tokens)
    )
    if type(maximum) is not int or not 1 <= maximum <= grant.max_output_tokens:
        raise HTTPException(400, "baseline_output_limit_exceeded")
    # Normalize the legacy SDK field at the OpenAI GPT boundary. The modern
    # field also bounds hidden reasoning tokens for reasoning models.
    return {
        **{k: v for k, v in payload.items() if k != "max_tokens"},
        "max_completion_tokens": maximum,
        "n": 1,
        "stream": False,
    }


def reserve_budget(grant: HarborBaselineSession, *, tokens: int, cost: Decimal) -> None:
    """Caller holds the row lock and commits before any upstream dispatch."""
    if (
        grant.calls_reserved + 1 > grant.max_calls
        or grant.tokens_reserved + tokens > grant.max_total_tokens
        or (grant.budget_usd is not None and grant.cost_reserved_usd + cost > grant.budget_usd)
    ):
        raise HTTPException(429, "baseline_budget_exhausted")
    grant.calls_reserved += 1
    grant.tokens_reserved += tokens
    grant.cost_reserved_usd += cost
