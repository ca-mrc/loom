"""Shared LLM usage and cost-status projection helpers."""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from sqlalchemy import Text, case, column, func, select, true, values

from loom.db.schema import LlmCall, ProviderConnection, RateCard, Trial
from loom.models.types import ModelSpec
from loom_llm_gateway.dialect import USAGE_STATUS_KEY
from loom_llm_gateway.errors import RateCardNotFoundError
from loom_llm_gateway.rate_card import (
    COST_META_CONFIDENCE_KEY,
    COST_META_SOURCE_KEY,
    RateCardTable,
    compute_cost_usd,
    hash_table,
    lookup_entry,
)
from loom_llm_gateway.yibuapi_pricing import (
    YIBUAPI_RATE_CARD_PROVIDER,
    normalize_yibuapi_model_name,
)

_TYPE_TO_DEFAULT_RATE_CARD_PROVIDER = {
    "anthropic": "anthropic",
    "google": "google",
    "openai-compatible": "openai",
}

BUDGET_HARD_LIMIT_REASON = "budget_hard_limit_exceeded"


@dataclass(frozen=True)
class PreRunBudgetEstimate:
    budget_usd: float | None
    budget_policy: str
    pre_run_estimated_cost_usd: float | None
    cost_estimate_source: str
    cost_estimate_confidence: str
    pre_run_estimated_llm_calls_count: int
    pre_run_estimated_prompt_tokens: int
    pre_run_estimated_completion_tokens: int
    unpriced_reason: str | None = None

    def as_api_dict(self) -> dict[str, Any]:
        return {
            "budget_usd": self.budget_usd,
            "budget_policy": self.budget_policy,
            "pre_run_estimated_cost_usd": self.pre_run_estimated_cost_usd,
            "cost_estimate_source": self.cost_estimate_source,
            "cost_estimate_confidence": self.cost_estimate_confidence,
            "pre_run_estimated_llm_calls_count": (
                self.pre_run_estimated_llm_calls_count
            ),
            "pre_run_estimated_prompt_tokens": (
                self.pre_run_estimated_prompt_tokens
            ),
            "pre_run_estimated_completion_tokens": (
                self.pre_run_estimated_completion_tokens
            ),
            "unpriced_reason": self.unpriced_reason,
        }


def _float_cost(value: Decimal | int | float | str | None) -> float:
    if value is None:
        return 0.0
    return float(Decimal(str(value)))


def _dominant_count_key(
    counts: Mapping[str, int] | None,
    *,
    fallback: str,
) -> str:
    if not counts:
        return fallback
    nonzero = {
        str(key): int(value or 0)
        for key, value in counts.items()
        if int(value or 0) > 0
    }
    if not nonzero:
        return fallback
    if len(nonzero) == 1:
        return next(iter(nonzero))
    return "mixed"


def summarize_usage_counts(
    *,
    llm_calls_count: int,
    total_prompt_tokens: int,
    total_completion_tokens: int,
    total_cost_usd: Decimal | int | float | str | None,
    priced_llm_calls_count: int,
    token_only_llm_calls_count: int,
    price_unknown_llm_calls_count: int,
    failed_upstream_llm_calls_count: int = 0,
    partial_usage_llm_calls_count: int = 0,
    missing_usage_llm_calls_count: int = 0,
    cost_source_counts: Mapping[str, int] | None = None,
    cost_confidence_counts: Mapping[str, int] | None = None,
) -> dict[str, Any]:
    """Return the API projection for an aggregate of LLM call rows.

    `total_cost_usd` is kept for backwards compatibility with existing
    clients. New clients should prefer `estimated_cost_usd` plus
    `cost_status`: token-only self-deployed/private API calls have no monetary
    amount even though their legacy `total_cost_usd` is necessarily zero.
    """

    llm_calls = max(int(llm_calls_count or 0), 0)
    prompt = max(int(total_prompt_tokens or 0), 0)
    completion = max(int(total_completion_tokens or 0), 0)
    priced = max(int(priced_llm_calls_count or 0), 0)
    token_only = max(int(token_only_llm_calls_count or 0), 0)
    price_unknown = max(int(price_unknown_llm_calls_count or 0), 0)
    failed_upstream = max(int(failed_upstream_llm_calls_count or 0), 0)
    partial_usage = max(int(partial_usage_llm_calls_count or 0), 0)
    missing_usage = max(int(missing_usage_llm_calls_count or 0), 0)
    cost = _float_cost(total_cost_usd)

    modes: list[str] = []
    if priced:
        modes.append("priced")
    if token_only:
        modes.append("tokens-only")
    if price_unknown:
        modes.append("price-unknown")
    if failed_upstream:
        modes.append("failed-upstream")

    if llm_calls == 0:
        cost_status = "no_usage"
        estimated_cost_usd: float | None = None
        cost_estimate_source = "none"
        cost_estimate_confidence = "none"
    elif len(modes) > 1:
        cost_status = "mixed"
        estimated_cost_usd = cost if priced else None
        cost_estimate_source = _dominant_count_key(
            cost_source_counts,
            fallback="mixed",
        )
        cost_estimate_confidence = _dominant_count_key(
            cost_confidence_counts,
            fallback="mixed",
        )
    elif price_unknown:
        cost_status = "price_unknown"
        estimated_cost_usd = None
        cost_estimate_source = _dominant_count_key(
            cost_source_counts,
            fallback="unpriced",
        )
        cost_estimate_confidence = _dominant_count_key(
            cost_confidence_counts,
            fallback="unavailable",
        )
    elif token_only:
        cost_status = "not_applicable"
        estimated_cost_usd = None
        cost_estimate_source = _dominant_count_key(
            cost_source_counts,
            fallback="tokens-only",
        )
        cost_estimate_confidence = _dominant_count_key(
            cost_confidence_counts,
            fallback="not_applicable",
        )
    elif failed_upstream:
        cost_status = "failed_upstream"
        estimated_cost_usd = None
        cost_estimate_source = _dominant_count_key(
            cost_source_counts,
            fallback="failed-upstream",
        )
        cost_estimate_confidence = _dominant_count_key(
            cost_confidence_counts,
            fallback="unavailable",
        )
    else:
        cost_status = "estimated"
        estimated_cost_usd = cost
        if not modes:
            modes.append("priced")
        cost_estimate_source = _dominant_count_key(
            cost_source_counts,
            fallback="rate-card",
        )
        cost_estimate_confidence = _dominant_count_key(
            cost_confidence_counts,
            fallback="configured",
        )

    if llm_calls == 0:
        usage_reporting_status = "no_usage"
        usage_estimate_confidence = "none"
    elif missing_usage >= llm_calls:
        usage_reporting_status = "missing"
        usage_estimate_confidence = "missing"
    elif missing_usage or partial_usage:
        usage_reporting_status = "partial"
        usage_estimate_confidence = "partial"
    else:
        usage_reporting_status = "complete"
        usage_estimate_confidence = "high"

    return {
        "total_prompt_tokens": prompt,
        "total_completion_tokens": completion,
        "total_tokens": prompt + completion,
        "llm_calls_count": llm_calls,
        "total_cost_usd": cost,
        "estimated_cost_usd": estimated_cost_usd,
        "cost_currency": "USD" if estimated_cost_usd is not None else None,
        "cost_status": cost_status,
        "cost_estimate_source": cost_estimate_source,
        "cost_estimate_confidence": cost_estimate_confidence,
        "pricing_modes": modes,
        "priced_llm_calls_count": priced,
        "token_only_llm_calls_count": token_only,
        "price_unknown_llm_calls_count": price_unknown,
        "failed_upstream_llm_calls_count": failed_upstream,
        "partial_usage_llm_calls_count": partial_usage,
        "missing_usage_llm_calls_count": missing_usage,
        "usage_reporting_status": usage_reporting_status,
        "usage_estimate_confidence": usage_estimate_confidence,
    }


def empty_usage_projection() -> dict[str, Any]:
    return summarize_usage_counts(
        llm_calls_count=0,
        total_prompt_tokens=0,
        total_completion_tokens=0,
        total_cost_usd=Decimal("0"),
        priced_llm_calls_count=0,
        token_only_llm_calls_count=0,
        price_unknown_llm_calls_count=0,
    )


def _nullable_float(value: Decimal | int | float | str | None) -> float | None:
    if value is None:
        return None
    return float(Decimal(str(value)))


def _settings_int(settings: Any, name: str, default: int) -> int:
    try:
        value = int(getattr(settings, name))
    except (AttributeError, TypeError, ValueError):
        return default
    return max(value, 0)


def _operator_cost_usd(
    pricing_data: Mapping[str, Any] | None,
    *,
    input_tokens: int,
    output_tokens: int,
) -> float | None:
    if not pricing_data:
        return None
    try:
        input_per_1m = float(pricing_data.get("input_usd_per_1m", 0) or 0)
        output_per_1m = float(pricing_data.get("output_usd_per_1m", 0) or 0)
    except (TypeError, ValueError):
        return None
    return (
        (max(input_tokens, 0) / 1_000_000.0) * input_per_1m
        + (max(output_tokens, 0) / 1_000_000.0) * output_per_1m
    )


async def _latest_rate_card_table(session: Any) -> RateCardTable | None:
    row = (
        await session.execute(
            select(RateCard).order_by(RateCard.captured_at.desc()).limit(1),
        )
    ).scalar_one_or_none()
    if row is None:
        return None
    payload = dict(row.table or {})
    payload["id"] = row.id
    payload["captured_at"] = row.captured_at
    return RateCardTable(**payload)


async def estimate_pre_run_batch_budget(
    session: Any,
    *,
    provider_connection: ProviderConnection | None,
    provider_model_id: str | None,
    expected_trial_count: int,
    settings: Any,
    budget_usd: Decimal | int | float | str | None,
    budget_policy: str,
    secondary_model_id: str | None = None,
    teacher_episodes: int | None = None,
    episode_ceiling: int | None = None,
) -> PreRunBudgetEstimate:
    calls_per_trial = _settings_int(
        settings,
        "batch_budget_estimate_llm_calls_per_trial",
        1,
    )
    input_per_call = _settings_int(
        settings,
        "batch_budget_estimate_input_tokens_per_call",
        1_000_000,
    )
    output_per_call = _settings_int(
        settings,
        "batch_budget_estimate_output_tokens_per_call",
        0,
    )
    estimated_calls = max(int(expected_trial_count or 0), 0) * calls_per_trial
    estimated_input_tokens = estimated_calls * input_per_call
    estimated_output_tokens = estimated_calls * output_per_call
    budget_value = _nullable_float(budget_usd)
    policy = str(budget_policy or "none")

    if provider_connection is None or not provider_model_id:
        return PreRunBudgetEstimate(
            budget_usd=budget_value,
            budget_policy=policy,
            pre_run_estimated_cost_usd=None,
            cost_estimate_source="none",
            cost_estimate_confidence="unavailable",
            pre_run_estimated_llm_calls_count=estimated_calls,
            pre_run_estimated_prompt_tokens=estimated_input_tokens,
            pre_run_estimated_completion_tokens=estimated_output_tokens,
            unpriced_reason="provider_model_not_configured",
        )

    if getattr(provider_connection, "pricing_config", None) is not None:
        from loom_llm_gateway.dialect import TokenUsage
        from loom_llm_gateway.provider_pricing import configured_cost

        usage = TokenUsage(estimated_input_tokens, estimated_output_tokens)
        estimate = await configured_cost(session, provider_connection, provider_model_id, usage)
        if secondary_model_id and estimate.confidence == "configured":
            teacher = await configured_cost(session, provider_connection, secondary_model_id, usage)
            if teacher.confidence != "configured":
                estimate = teacher
            else:
                from dataclasses import replace
                share = min(max(int(teacher_episodes or 2), 1) / max(int(episode_ceiling or 50), 2), 0.95)
                estimate = replace(estimate, cost_usd=(1 - share) * estimate.cost_usd + share * teacher.cost_usd)
        return PreRunBudgetEstimate(
            budget_usd=budget_value, budget_policy=policy,
            pre_run_estimated_cost_usd=estimate.cost_usd if estimate.confidence == "configured" else None,
            cost_estimate_source=estimate.source, cost_estimate_confidence=estimate.confidence,
            pre_run_estimated_llm_calls_count=estimated_calls,
            pre_run_estimated_prompt_tokens=estimated_input_tokens,
            pre_run_estimated_completion_tokens=estimated_output_tokens,
            unpriced_reason=estimate.unpriced_reason,
        )

    pricing_source = str(provider_connection.pricing_source or "tokens-only")
    if pricing_source == "operator-supplied":
        cost = _operator_cost_usd(
            provider_connection.pricing_data,
            input_tokens=estimated_input_tokens,
            output_tokens=estimated_output_tokens,
        )
        if cost is None:
            return PreRunBudgetEstimate(
                budget_usd=budget_value,
                budget_policy=policy,
                pre_run_estimated_cost_usd=None,
                cost_estimate_source="unpriced",
                cost_estimate_confidence="unavailable",
                pre_run_estimated_llm_calls_count=estimated_calls,
                pre_run_estimated_prompt_tokens=estimated_input_tokens,
                pre_run_estimated_completion_tokens=estimated_output_tokens,
                unpriced_reason="invalid_operator_pricing",
            )
        return PreRunBudgetEstimate(
            budget_usd=budget_value,
            budget_policy=policy,
            pre_run_estimated_cost_usd=cost,
            cost_estimate_source="operator-supplied",
            cost_estimate_confidence="configured",
            pre_run_estimated_llm_calls_count=estimated_calls,
            pre_run_estimated_prompt_tokens=estimated_input_tokens,
            pre_run_estimated_completion_tokens=estimated_output_tokens,
        )

    if pricing_source != "rate-card":
        return PreRunBudgetEstimate(
            budget_usd=budget_value,
            budget_policy=policy,
            pre_run_estimated_cost_usd=None,
            cost_estimate_source="tokens-only",
            cost_estimate_confidence="not_applicable",
            pre_run_estimated_llm_calls_count=estimated_calls,
            pre_run_estimated_prompt_tokens=estimated_input_tokens,
            pre_run_estimated_completion_tokens=estimated_output_tokens,
        )

    lookup_provider = (
        provider_connection.rate_card_provider
        or _TYPE_TO_DEFAULT_RATE_CARD_PROVIDER.get(provider_connection.provider_type)
    )
    if lookup_provider is None:
        return PreRunBudgetEstimate(
            budget_usd=budget_value,
            budget_policy=policy,
            pre_run_estimated_cost_usd=None,
            cost_estimate_source="unpriced",
            cost_estimate_confidence="unavailable",
            pre_run_estimated_llm_calls_count=estimated_calls,
            pre_run_estimated_prompt_tokens=estimated_input_tokens,
            pre_run_estimated_completion_tokens=estimated_output_tokens,
            unpriced_reason="rate_card_provider_not_configured",
        )

    table = await _latest_rate_card_table(session)
    if table is None:
        return PreRunBudgetEstimate(
            budget_usd=budget_value,
            budget_policy=policy,
            pre_run_estimated_cost_usd=None,
            cost_estimate_source="unpriced",
            cost_estimate_confidence="unavailable",
            pre_run_estimated_llm_calls_count=estimated_calls,
            pre_run_estimated_prompt_tokens=estimated_input_tokens,
            pre_run_estimated_completion_tokens=estimated_output_tokens,
            unpriced_reason="rate_card_unavailable",
        )

    lookup_model = (
        normalize_yibuapi_model_name(provider_model_id)
        if lookup_provider == YIBUAPI_RATE_CARD_PROVIDER
        else provider_model_id
    )
    try:
        entry = lookup_entry(
            table,
            ModelSpec(provider=lookup_provider, name=lookup_model),
        )
    except RateCardNotFoundError:
        return PreRunBudgetEstimate(
            budget_usd=budget_value,
            budget_policy=policy,
            pre_run_estimated_cost_usd=None,
            cost_estimate_source="unpriced",
            cost_estimate_confidence="unavailable",
            pre_run_estimated_llm_calls_count=estimated_calls,
            pre_run_estimated_prompt_tokens=estimated_input_tokens,
            pre_run_estimated_completion_tokens=estimated_output_tokens,
            unpriced_reason="missing_rate_card_entry",
        )

    cost = compute_cost_usd(
        entry,
        input_tokens=estimated_input_tokens,
        output_tokens=estimated_output_tokens,
        cached_input_tokens=0,
        cache_write_tokens=0,
    )
    if secondary_model_id:
        teacher_lookup = (
            normalize_yibuapi_model_name(secondary_model_id)
            if lookup_provider == YIBUAPI_RATE_CARD_PROVIDER
            else secondary_model_id
        )
        try:
            teacher_entry = lookup_entry(
                table,
                ModelSpec(provider=lookup_provider, name=teacher_lookup),
            )
        except RateCardNotFoundError:
            return PreRunBudgetEstimate(
                budget_usd=budget_value,
                budget_policy=policy,
                pre_run_estimated_cost_usd=None,
                cost_estimate_source="unpriced",
                cost_estimate_confidence="unavailable",
                pre_run_estimated_llm_calls_count=estimated_calls,
                pre_run_estimated_prompt_tokens=estimated_input_tokens,
                pre_run_estimated_completion_tokens=estimated_output_tokens,
                unpriced_reason="missing_rate_card_entry",
            )
        teacher_cost = compute_cost_usd(
            teacher_entry,
            input_tokens=estimated_input_tokens,
            output_tokens=estimated_output_tokens,
            cached_input_tokens=0,
            cache_write_tokens=0,
        )
        ceiling = max(int(episode_ceiling or 50), 2)
        teacher_n = max(int(teacher_episodes or 2), 1)
        teacher_share = min(teacher_n / ceiling, 0.95)
        student_share = 1.0 - teacher_share
        cost = (student_share * float(cost)) + (teacher_share * float(teacher_cost))
    return PreRunBudgetEstimate(
        budget_usd=budget_value,
        budget_policy=policy,
        pre_run_estimated_cost_usd=cost,
        cost_estimate_source="rate-card",
        cost_estimate_confidence="configured",
        pre_run_estimated_llm_calls_count=estimated_calls,
        pre_run_estimated_prompt_tokens=estimated_input_tokens,
        pre_run_estimated_completion_tokens=estimated_output_tokens,
    )


def project_batch_budget(
    batch: Any,
    usage: Mapping[str, Any],
) -> dict[str, Any]:
    budget_usd = _nullable_float(getattr(batch, "budget_usd", None))
    policy = str(getattr(batch, "budget_policy", "none") or "none")
    pre_run_cost = _nullable_float(
        getattr(batch, "pre_run_estimated_cost_usd", None),
    )
    consumed = usage.get("estimated_cost_usd")
    consumed_float = float(consumed) if isinstance(consumed, (int, float)) else None
    llm_calls = int(usage.get("llm_calls_count") or 0)
    if budget_usd is None or policy == "none":
        remaining: float | None = None
        status = "none"
    elif consumed_float is not None:
        remaining = budget_usd - consumed_float
        if consumed_float > budget_usd:
            status = f"{policy}_budget_exceeded"
        else:
            status = "within_budget"
    elif llm_calls == 0:
        remaining = budget_usd
        if pre_run_cost is not None and pre_run_cost > budget_usd:
            status = f"{policy}_over_pre_run_estimate"
        else:
            status = "within_budget"
    else:
        remaining = None
        status = "unknown_live_cost"

    return {
        "budget_usd": budget_usd,
        "budget_policy": policy,
        "budget_remaining_usd": remaining,
        "budget_consumed_usd": consumed_float,
        "budget_status": status,
        "pre_run_estimated_cost_usd": pre_run_cost,
        "pre_run_cost_estimate_source": (
            getattr(batch, "pre_run_cost_estimate_source", None)
        ),
        "pre_run_cost_estimate_confidence": (
            getattr(batch, "pre_run_cost_estimate_confidence", None)
        ),
        "budget_diagnostics": getattr(batch, "budget_diagnostics", None) or [],
    }


def hard_budget_exceeded_diagnostic(
    *,
    batch_id: UUID,
    budget_usd: float,
    estimated_cost_usd: float,
) -> dict[str, Any]:
    return {
        "reason": BUDGET_HARD_LIMIT_REASON,
        "batch_id": str(batch_id),
        "budget_usd": budget_usd,
        "estimated_cost_usd": estimated_cost_usd,
        "seen_at": datetime.now(UTC).isoformat(),
    }


_TERMINAL_TRIAL_STATES: frozenset[str] = frozenset(
    {"succeeded", "failed", "cancelled"},
)
_CODEX_RUNTIME_EXIT_RE = re.compile(r"\bcodex exited rc=\d+\b", re.IGNORECASE)
_CODEX_HIGH_DEMAND_RE = re.compile(
    r"currently experiencing high demand|high demand, which may cause temporary errors",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class NoCallEvidenceReason:
    reason: str
    message: str
    retryable: bool


_TERMINAL_MODEL_BACKED_NO_CALL = NoCallEvidenceReason(
    reason="terminal_model_backed_no_call",
    message=(
        "Terminal model-backed trial did not record any LLM calls; exclude this "
        "trial from clean parity/request-parameter evidence unless a retry "
        "succeeds."
    ),
    retryable=False,
)
_AGENT_STEP_NO_CALL = NoCallEvidenceReason(
    reason="agent_step_no_call",
    message=(
        "Agent step failed before any gateway request; exclude this trial from "
        "clean parity/request-parameter evidence unless a retry succeeds."
    ),
    retryable=False,
)
_CODEX_RUNTIME_NO_CALL = NoCallEvidenceReason(
    reason="codex_runtime_no_call",
    message=(
        "Codex subprocess exited before any gateway request; exclude this trial "
        "from clean parity/request-parameter evidence unless a retry succeeds."
    ),
    retryable=True,
)
_CODEX_HIGH_DEMAND_NO_CALL = NoCallEvidenceReason(
    reason="codex_high_demand_no_call",
    message=(
        "Codex exited before any gateway request because the Codex service "
        "reported high demand; exclude this trial from clean parity/"
        "request-parameter evidence unless a retry succeeds."
    ),
    retryable=True,
)


def _model_from_config(config: Any) -> Mapping[str, Any] | None:
    if not isinstance(config, Mapping):
        return None
    model = config.get("agent_model")
    if isinstance(model, Mapping) and model:
        return model
    agent = config.get("agent")
    if isinstance(agent, Mapping):
        legacy_model = agent.get("model")
        if isinstance(legacy_model, Mapping) and legacy_model:
            return legacy_model
    return None


def trial_requires_llm(trial: Any) -> bool:
    """Return True when a trial is configured to call a real model provider."""

    if getattr(trial, "provider_connection_id", None) is not None:
        return True
    provider_model_id = getattr(trial, "provider_model_id", None)
    if isinstance(provider_model_id, str) and provider_model_id:
        return True
    return _model_from_config(getattr(trial, "config", None)) is not None


def _step_error_messages(result: Any) -> list[str]:
    if not isinstance(result, Mapping):
        return []
    raw_steps = result.get("steps")
    if not isinstance(raw_steps, Sequence) or isinstance(raw_steps, (str, bytes)):
        return []
    messages: list[str] = []
    for raw_step in raw_steps:
        if not isinstance(raw_step, Mapping):
            continue
        raw_error = raw_step.get("error")
        if not isinstance(raw_error, Mapping):
            continue
        if raw_error.get("phase") != "agent":
            continue
        message = raw_error.get("message")
        if isinstance(message, str) and message:
            messages.append(message)
    return messages


_EXPECTED_NO_CALL_MESSAGES = {
    "task_image_build_failed": "Task image preparation failed before model execution.",
    "task_image_build_timeout": "Task image preparation timed out before model execution.",
    "cancelled": "Trial was cancelled before execution started; no model calls were expected.",
}


def no_call_evidence_reason(trial: Any) -> NoCallEvidenceReason:
    """Classify why a terminal model-backed trial has no LLM-call evidence."""

    state = getattr(trial, "state", None)
    reason = getattr(trial, "failure_reason", None)
    if state == "failed" and reason in {
        "task_image_build_failed",
        "task_image_build_timeout",
    }:
        return NoCallEvidenceReason(reason, _EXPECTED_NO_CALL_MESSAGES[reason], False)
    if (
        state == "cancelled"
        and getattr(trial, "started_at", None) is None
        and getattr(trial, "result", None) is None
    ):
        return NoCallEvidenceReason("cancelled", _EXPECTED_NO_CALL_MESSAGES["cancelled"], False)

    candidates = [
        item
        for item in (
            getattr(trial, "failure_message", None),
            *_step_error_messages(getattr(trial, "result", None)),
        )
        if isinstance(item, str) and item
    ]
    if any(
        _CODEX_RUNTIME_EXIT_RE.search(item) and _CODEX_HIGH_DEMAND_RE.search(item)
        for item in candidates
    ):
        return _CODEX_HIGH_DEMAND_NO_CALL
    if any(_CODEX_RUNTIME_EXIT_RE.search(item) for item in candidates):
        return _CODEX_RUNTIME_NO_CALL
    if candidates:
        return _AGENT_STEP_NO_CALL
    return _TERMINAL_MODEL_BACKED_NO_CALL


def project_trial_llm_evidence(
    trial: Any,
    *,
    llm_calls_count: int,
) -> dict[str, Any]:
    """Project whether persisted LLM-call evidence is usable for a trial."""

    requires_llm = trial_requires_llm(trial)
    state = str(getattr(trial, "state", ""))
    calls = max(int(llm_calls_count or 0), 0)
    if not requires_llm:
        return {
            "llm_evidence_status": "not_applicable",
            "no_call": False,
            "no_call_reason": None,
            "no_call_message": None,
            "no_call_retryable": False,
        }
    if state not in _TERMINAL_TRIAL_STATES:
        return {
            "llm_evidence_status": "pending",
            "no_call": False,
            "no_call_reason": None,
            "no_call_message": None,
            "no_call_retryable": False,
        }
    if calls == 0:
        reason = no_call_evidence_reason(trial)
        return {
            "llm_evidence_status": (
                "not_applicable"
                if reason.reason in _EXPECTED_NO_CALL_MESSAGES
                else "no_calls_invalid"
            ),
            "no_call": True,
            "no_call_reason": reason.reason,
            "no_call_message": reason.message,
            "no_call_retryable": reason.retryable,
        }
    return {
        "llm_evidence_status": "calls_observed",
        "no_call": False,
        "no_call_reason": None,
        "no_call_message": None,
        "no_call_retryable": False,
    }


def classify_no_call_evidence_failure(
    trial: Any,
    *,
    llm_calls_count: int,
) -> dict[str, Any] | None:
    evidence = project_trial_llm_evidence(trial, llm_calls_count=llm_calls_count)
    if evidence["llm_evidence_status"] != "no_calls_invalid":
        return None
    reason = str(evidence["no_call_reason"] or "terminal_model_backed_no_call")
    retryable = bool(evidence["no_call_retryable"])
    if reason.startswith("codex_"):
        category = "agent"
        attribution = "provider" if reason == "codex_high_demand_no_call" else "platform"
    elif reason == "agent_step_no_call":
        category = "agent"
        attribution = "model"
    else:
        category = "gateway"
        attribution = "platform"
    return {
        "reason_code": f"trial.{reason}",
        "reason": reason,
        "category": category,
        "attribution": attribution,
        "message": evidence["no_call_message"],
        "failure_class": "platform_failure",
        "root_cause": reason,
        "platform_outcome": "failed",
        "score_outcome": "unscored",
        "rerun_recommendation": "auto_safe" if retryable else "operator_approval",
        "rerunnable": True,
        "requires_operator_approval": not retryable,
        "requires_task_change": False,
    }


def llm_call_counts_by_trial_id(
    llm_calls: Sequence[LlmCall],
) -> dict[UUID, int]:
    counts: dict[UUID, int] = {}
    for call in llm_calls:
        if call.trial_id is None:
            continue
        counts[call.trial_id] = counts.get(call.trial_id, 0) + 1
    return counts


def summarize_llm_evidence_for_trials(
    trials: Sequence[Any],
    *,
    llm_call_counts: Mapping[UUID, int],
) -> dict[str, Any]:
    terminal_model_backed = 0
    no_call = 0
    unexpected_no_call = 0
    no_call_reason_counts: dict[str, int] = {}
    for trial in trials:
        if str(getattr(trial, "state", "")) not in _TERMINAL_TRIAL_STATES:
            continue
        if not trial_requires_llm(trial):
            continue
        terminal_model_backed += 1
        calls = int(llm_call_counts.get(trial.id, 0) or 0)
        if calls == 0:
            no_call += 1
            reason = no_call_evidence_reason(trial).reason
            if reason not in _EXPECTED_NO_CALL_MESSAGES:
                unexpected_no_call += 1
            no_call_reason_counts[reason] = no_call_reason_counts.get(reason, 0) + 1

    total_calls = sum(max(int(value or 0), 0) for value in llm_call_counts.values())
    if terminal_model_backed == 0:
        status = "not_applicable"
    elif no_call == 0:
        status = "calls_observed" if total_calls > 0 else "pending"
    elif unexpected_no_call == 0:
        status = "calls_observed" if total_calls > 0 else "not_applicable"
    elif total_calls == 0:
        status = "no_calls_invalid"
    else:
        status = "partial_no_calls"

    return {
        "llm_evidence_status": status,
        "no_call_trial_count": no_call,
        "no_call_reason_counts": dict(sorted(no_call_reason_counts.items())),
        "model_backed_terminal_trial_count": terminal_model_backed,
    }


def usage_status_filter(status: str) -> Any:
    return LlmCall.provider_extras.op("->>")(USAGE_STATUS_KEY) == status


def cost_meta_filter(key: str, value: str) -> Any:
    """Interpret legacy local calls without inventing a configured price."""
    local_value = {
        COST_META_SOURCE_KEY: "unpriced",
        COST_META_CONFIDENCE_KEY: "unavailable",
    }[key]
    effective = case(
        (LlmCall.rate_card_hash == "local-server-no-card", local_value),
        else_=func.coalesce(LlmCall.provider_extras.op("->>")(key), ""),
    )
    return effective == value


def _is_facade_rate_card_hash(value: str) -> bool:
    return value.startswith("facade:")


def _rate_card_snapshot(
    *,
    rate_card_hash: str,
    table: RateCardTable | None,
) -> dict[str, Any]:
    return {
        "rate_card_hash": rate_card_hash,
        "rate_card_id": table.id if table is not None else None,
        "resolved": table is not None,
        "provider": table.provider if table is not None else None,
        "source_url": table.source_url if table is not None else None,
        "pricing_version": table.pricing_version if table is not None else None,
        "last_checked_at": (
            table.last_checked_at.isoformat()
            if table is not None and table.last_checked_at is not None
            else None
        ),
        "currency": table.currency if table is not None else None,
        "group": table.group if table is not None else None,
        "group_ratio": table.group_ratio if table is not None else None,
    }


_PRICE_BASIS_KEY = "_loom_price_basis"
# Rate-card and provider-price hashes are SHA-256 digests; anything else
# (for example `failed-upstream`) can never carry a frozen price basis.
_PRICE_DIGEST = re.compile(r"[0-9a-f]{64}")


def _wanted_price_hashes(hashes: Iterable[object]) -> list[str]:
    return sorted(
        {
            value
            for value in hashes
            if isinstance(value, str)
            and value
            and value != "local-server-no-card"
            and not _is_facade_rate_card_hash(value)
        }
    )


async def _rate_card_tables_by_hash(session: Any, wanted: set[str]) -> dict[str, RateCardTable]:
    tables_by_hash: dict[str, RateCardTable] = {}
    rows = (await session.execute(select(RateCard))).scalars().all()
    for row in rows:
        payload = dict(row.table or {})
        payload["id"] = row.id
        payload["captured_at"] = row.captured_at
        try:
            table = RateCardTable.model_validate(payload)
        except ValueError:
            continue
        table_hash = hash_table(table)
        if table_hash in wanted:
            tables_by_hash[table_hash] = table
    return tables_by_hash


async def _frozen_price_bases(
    session: Any,
    hashes: Sequence[str],
    trial_ids: Sequence[UUID],
) -> dict[str, dict[str, Any]]:
    """Read one frozen price basis per hash from the given Trials' calls.

    A provider-price hash is the digest of its basis, so any one call with
    the hash carries the same basis. Only the basis key is extracted, never
    the full raw-log JSON, and the lookup stays inside the Trials' calls (#2687).
    """
    if not hashes or not trial_ids:
        return {}
    wanted = values(column("rate_card_hash", Text), name="wanted_hashes").data(
        [(digest,) for digest in hashes],
    )
    call = (
        select(LlmCall.provider_extras[_PRICE_BASIS_KEY].label("basis"))
        .where(
            LlmCall.trial_id.in_(list(trial_ids)),
            LlmCall.rate_card_hash == wanted.c.rate_card_hash,
            LlmCall.provider_extras.has_key(_PRICE_BASIS_KEY),
        )
        .limit(1)
        .lateral("priced_call")
    )
    rows = (
        await session.execute(
            select(wanted.c.rate_card_hash, call.c.basis).select_from(
                wanted.join(call, true()),
            ),
        )
    ).all()
    return {digest: basis for digest, basis in rows if isinstance(basis, dict)}


def _frozen_snapshot(digest: str, basis: dict[str, Any]) -> dict[str, Any]:
    return {
        **_rate_card_snapshot(rate_card_hash=digest, table=None), **basis,
        "rate_card_id": basis.get("catalog_id"), "resolved": True,
        "provider": basis.get("supplier_id"),
        "pricing_version": str(basis["revision"]) if "revision" in basis else None,
        "last_checked_at": basis.get("updated_at"),
    }


async def price_snapshots_for_trial_sets(
    session: Any,
    *trial_id_sets: Iterable[UUID],
) -> list[list[dict[str, Any]]]:
    """Resolve price snapshots for several Trial sets with one shared lookup.

    Returns one snapshot list per input set, in input order. Facade
    pseudo-hashes such as `facade:tokens-only` are cost-status signals, not
    immutable hosted-pricing snapshots, so they are intentionally omitted.
    """

    id_sets = [frozenset(ids) for ids in trial_id_sets]
    hashes_by_set: dict[frozenset[UUID], list[str]] = {}
    for ids in id_sets:
        if ids and ids not in hashes_by_set:
            hashes_by_set[ids] = _wanted_price_hashes(
                (
                    await session.execute(
                        select(LlmCall.rate_card_hash)
                        .where(LlmCall.trial_id.in_(list(ids)))
                        .distinct(),
                    )
                ).scalars(),
            )
    wanted = set().union(*hashes_by_set.values()) if hashes_by_set else set()
    if not wanted:
        return [[] for _ in id_sets]

    tables_by_hash = await _rate_card_tables_by_hash(session, wanted)
    frozen = await _frozen_price_bases(
        session,
        sorted(
            digest for digest in wanted
            if digest not in tables_by_hash and _PRICE_DIGEST.fullmatch(digest)
        ),
        sorted(set().union(*hashes_by_set)),
    )
    snapshots = {
        digest: (
            _frozen_snapshot(digest, frozen[digest]) if digest in frozen
            else _rate_card_snapshot(rate_card_hash=digest, table=tables_by_hash.get(digest))
        )
        for digest in wanted
    }
    return [
        [snapshots[digest] for digest in hashes_by_set.get(ids, [])]
        for ids in id_sets
    ]


async def price_snapshots_for_trials(
    session: Any,
    trial_ids: Iterable[UUID],
) -> list[dict[str, Any]]:
    (snapshots,) = await price_snapshots_for_trial_sets(session, trial_ids)
    return snapshots


def _priced_call_filter() -> Any:
    return (
        ~LlmCall.rate_card_hash.like("facade:tokens-only%")
        & ~_price_unknown_call_filter()
        & (LlmCall.rate_card_hash != "failed-upstream")
    )


def _price_unknown_call_filter() -> Any:
    return LlmCall.rate_card_hash.like("facade:rate-card:missing%") | cost_meta_filter(
        COST_META_SOURCE_KEY, "unpriced"
    )


def _cost_source_counts(row: Any) -> dict[str, int]:
    return {
        "operator-supplied": int(row.cost_source_operator_supplied_count or 0),
        "rate-card": int(row.cost_source_rate_card_count or 0),
        "tokens-only": int(row.cost_source_tokens_only_count or 0),
        "unpriced": int(row.cost_source_unpriced_count or 0),
    }


def _cost_confidence_counts(row: Any) -> dict[str, int]:
    return {
        "configured": int(row.cost_confidence_configured_count or 0),
        "not_applicable": int(row.cost_confidence_not_applicable_count or 0),
        "unavailable": int(row.cost_confidence_unavailable_count or 0),
    }


async def usage_by_batch_ids(
    session: Any,
    batch_ids: Sequence[UUID],
) -> dict[UUID, dict[str, Any]]:
    if not batch_ids:
        return {}
    rows = (
        await session.execute(
            select(
                Trial.batch_id.label("batch_id"),
                func.coalesce(
                    func.sum(LlmCall.input_tokens),
                    0,
                ).label("total_prompt_tokens"),
                func.coalesce(
                    func.sum(LlmCall.output_tokens),
                    0,
                ).label("total_completion_tokens"),
                func.count(LlmCall.id).label("llm_calls_count"),
                func.coalesce(
                    func.sum(LlmCall.cost_usd),
                    0,
                ).label("total_cost_usd"),
                func.count(LlmCall.id)
                .filter(_priced_call_filter())
                .label("priced_llm_calls_count"),
                func.count(LlmCall.id)
                .filter(LlmCall.rate_card_hash.like("facade:tokens-only%"))
                .label("token_only_llm_calls_count"),
                func.count(LlmCall.id)
                .filter(_price_unknown_call_filter())
                .label("price_unknown_llm_calls_count"),
                func.count(LlmCall.id)
                .filter(LlmCall.rate_card_hash == "failed-upstream")
                .label("failed_upstream_llm_calls_count"),
                func.count(LlmCall.id)
                .filter(cost_meta_filter(COST_META_SOURCE_KEY, "operator-supplied"))
                .label("cost_source_operator_supplied_count"),
                func.count(LlmCall.id)
                .filter(cost_meta_filter(COST_META_SOURCE_KEY, "rate-card"))
                .label("cost_source_rate_card_count"),
                func.count(LlmCall.id)
                .filter(cost_meta_filter(COST_META_SOURCE_KEY, "tokens-only"))
                .label("cost_source_tokens_only_count"),
                func.count(LlmCall.id)
                .filter(cost_meta_filter(COST_META_SOURCE_KEY, "unpriced"))
                .label("cost_source_unpriced_count"),
                func.count(LlmCall.id)
                .filter(cost_meta_filter(COST_META_CONFIDENCE_KEY, "configured"))
                .label("cost_confidence_configured_count"),
                func.count(LlmCall.id)
                .filter(
                    cost_meta_filter(COST_META_CONFIDENCE_KEY, "not_applicable"),
                )
                .label("cost_confidence_not_applicable_count"),
                func.count(LlmCall.id)
                .filter(cost_meta_filter(COST_META_CONFIDENCE_KEY, "unavailable"))
                .label("cost_confidence_unavailable_count"),
                func.count(LlmCall.id)
                .filter(usage_status_filter("partial"))
                .label("partial_usage_llm_calls_count"),
                func.count(LlmCall.id)
                .filter(usage_status_filter("missing"))
                .label("missing_usage_llm_calls_count"),
            )
            .join(LlmCall, LlmCall.trial_id == Trial.id)
            .where(Trial.batch_id.in_(batch_ids))
            .group_by(Trial.batch_id),
        )
    ).all()
    return {
        row.batch_id: summarize_usage_counts(
            llm_calls_count=int(row.llm_calls_count or 0),
            total_prompt_tokens=int(row.total_prompt_tokens or 0),
            total_completion_tokens=int(row.total_completion_tokens or 0),
            total_cost_usd=row.total_cost_usd,
            priced_llm_calls_count=int(row.priced_llm_calls_count or 0),
            token_only_llm_calls_count=int(
                row.token_only_llm_calls_count or 0,
            ),
            price_unknown_llm_calls_count=int(
                row.price_unknown_llm_calls_count or 0,
            ),
            failed_upstream_llm_calls_count=int(
                row.failed_upstream_llm_calls_count or 0,
            ),
            partial_usage_llm_calls_count=int(
                row.partial_usage_llm_calls_count or 0,
            ),
            missing_usage_llm_calls_count=int(
                row.missing_usage_llm_calls_count or 0,
            ),
            cost_source_counts=_cost_source_counts(row),
            cost_confidence_counts=_cost_confidence_counts(row),
        )
        for row in rows
    }
