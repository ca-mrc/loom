"""Shared projection from persisted gateway usage rows to trajectory events."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from loom.models.trajectory import ChatMessage, LLMCallEvent
from loom.models.types import ModelSpec
from loom.request_params import coerce_request_params

_PROVIDER_BY_DIALECT = {
    "openai_chat": "openai",
    "openai_responses": "openai",
    "openai_facade": "openai",
    "anthropic_facade": "anthropic",
    "anthropic": "anthropic",
    "gemini_facade": "google",
    "gemini": "google",
}


def llm_call_row_to_event(
    row: Mapping[str, Any],
    *,
    trial_id: UUID,
    seq: int,
) -> LLMCallEvent:
    """Project one `llm_calls` row into the synthetic event used for exports.

    The gateway persists accounting facts, not full prompt/response bodies.
    Message-level semantics remain the adapter trajectory's responsibility; this
    projection preserves model, token, retry, request-param, and cost facts.
    """

    extras = row.get("provider_extras") or {}
    if not isinstance(extras, Mapping):
        extras = {}
    identity = extras.get("_loom_model_identity")
    model = (ModelSpec.model_validate(identity) if identity is not None else ModelSpec(
        provider=_PROVIDER_BY_DIALECT.get(str(row.get("dialect") or ""), "unknown"),
        name=str(row.get("model") or "unknown"),
    ))
    if identity is not None and (not model.provider or model.name != row.get("model")):
        raise ValueError("invalid Gateway model identity")
    return LLMCallEvent(
        emitted_at=_captured_at(row.get("captured_at")),
        trial_id=trial_id,
        step_id=str(row.get("step_id") or "__trial__"),
        seq=seq,
        model=model,
        rate_card_hash=str(row.get("rate_card_hash") or ""),
        system_prompt=None,
        messages=[],
        response=ChatMessage(role="assistant", content=""),
        finish_reason="synthetic",
        call_status=("failed" if row.get("call_status") == "failed"
                     or extras.get("_loom_call_status") == "failed" else "completed"),
        usage_status=(extras.get("_loom_usage_status")
                      if extras.get("_loom_usage_status") in ("missing", "partial") else None),
        failure_category=(extras.get("_loom_failure_category")
                          if extras.get("_loom_failure_category") in (
                              "upstream_timeout", "upstream_transport", "upstream_http_4xx",
                              "upstream_http_5xx", "upstream_http_error", "attempt_deadline_reached",
                              "upstream_protocol", "client_cancelled", "gateway_retry_budget_exhausted",
                          ) else None),
        input_tokens=int(row.get("input_tokens") or 0),
        cached_input_tokens=int(
            _numeric_counter(extras.get("cache_read_input_tokens"))
            + _numeric_counter(extras.get("cachedContentTokenCount")),
        ),
        cache_write_tokens=int(
            _numeric_counter(extras.get("cache_creation_input_tokens")),
        ),
        output_tokens=int(row.get("output_tokens") or 0),
        thinking_tokens=int(
            _numeric_counter(extras.get("reasoning_tokens"))
            + _numeric_counter(extras.get("thoughtsTokenCount")),
        ),
        provider_extras={
            str(k): int(v)
            for k, v in extras.items()
            if isinstance(v, int | float) and not isinstance(v, bool)
        },
        request_params=coerce_request_params(row.get("request_params")),
        cost_usd_snapshot=float(row.get("cost_usd") or 0.0),
        cost_status=("price_unknown" if extras.get("_loom_cost_confidence") in {"unavailable", "not_applicable"}
                     else "priced" if extras.get("_loom_cost_confidence") in {"configured", "estimated"} else None),
        duration_sec=0.0,
        streamed=False,
        time_to_first_token_sec=None,
        gateway_request_id=str(row.get("id") or ""),
        attempt=int(row.get("attempt") or 1),
        requested_model=(
            str(row["requested_model"]) if row.get("requested_model") else None
        ),
        response_model=(
            str(row["response_model"]) if row.get("response_model") else None
        ),
        role=row.get("role") if row.get("role") in {"student", "teacher"} else None,
        gateway_route=(extras.get("_loom_gateway_route") if extras.get("_loom_gateway_route")
                       in {"native_anthropic", "anthropic_to_chat"} else None),
        compatibility_profile=("claude-code-chat-v1" if extras.get("_loom_compatibility_profile")
                               == "claude-code-chat-v1" else None),
    )


def _captured_at(raw: Any) -> datetime:
    if isinstance(raw, datetime):
        return raw if raw.tzinfo is not None else raw.replace(tzinfo=UTC)
    if raw:
        return datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    return datetime.now(UTC)


def _numeric_counter(raw: Any) -> int | float:
    return raw if isinstance(raw, int | float) and not isinstance(raw, bool) else 0


def llm_call_diagnostic_counts(calls: Iterable[LLMCallEvent]) -> dict[str, int]:
    """Expose incomplete accounting separately from reported numeric totals.

    Omit all-zero diagnostics so historical healthy source usage documents
    retain their exact shape at the materializer's validation boundary.
    """
    counts = {"failed_call_count": 0, "missing_usage_call_count": 0, "partial_usage_call_count": 0}
    for call in calls:
        counts["failed_call_count"] += int(call.call_status == "failed")
        counts["missing_usage_call_count"] += int(call.usage_status == "missing")
        counts["partial_usage_call_count"] += int(call.usage_status == "partial")
    return counts if any(counts.values()) else {}
