"""Shared public serialization for the authoritative Gateway call ledger."""

from __future__ import annotations

from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import LlmCall, ServiceExecutionLease
from loom.request_params import coerce_request_params


def serialize_llm_call(r: LlmCall, *, include_provider_log: bool = True) -> dict[str, Any]:
    """Keep worker and lease-scoped ledger field semantics identical."""
    payload = {
        "id": str(r.id),
        "trial_id": str(r.trial_id),
        "step_id": r.step_id,
        "dialect": r.dialect,
        "model": r.model,
        "input_tokens": r.input_tokens,
        "output_tokens": r.output_tokens,
        "provider_extras": r.provider_extras,
        "request_params": coerce_request_params(r.request_params),
        "cost_usd": float(r.cost_usd),
        "rate_card_hash": r.rate_card_hash,
        "captured_at": r.captured_at.isoformat(),
        # #298 Slice B: gateway-internal retry attempt that
        # produced this row. Defaults to 1 for pre-#298 rows.
        "attempt": r.attempt,
        "client_call_id": str(r.client_call_id) if r.client_call_id else None,
        "episode": r.episode,
        "call_ordinal": r.call_ordinal,
        "requested_model": r.requested_model,
        "response_model": r.response_model,
        "role": r.role,
        "correlation_status": r.correlation_status,
    }
    if not include_provider_log:
        payload["provider_extras"] = {
            key: value
            for key, value in (r.provider_extras or {}).items()
            if key != "_loom_raw_provider_log"
        }
    return payload


async def read_service_execution_llm_calls(
    session: AsyncSession, lease: ServiceExecutionLease, *, generation: int | None = None,
) -> list[dict[str, Any]]:
    """Read the authoritative agent ledger for exactly one tenant/lease/generation.

    Export only accounting fields, never provider request/response logs or headers.
    The same scope serves the active Pod and terminal canonical materialization.
    """
    binding = LlmCall.provider_extras["_loom_raw_provider_log"]["service_execution"]
    rows = (await session.scalars(select(LlmCall).where(
        LlmCall.team_id == lease.team_id,
        LlmCall.trial_id == lease.trial_id,
        LlmCall.step_id == "agent",
        binding["lease_id"].astext == str(lease.id),
        binding["generation"].astext == str(generation if generation is not None else lease.generation),
    ).order_by(LlmCall.captured_at, LlmCall.id))).all()
    return [serialize_execution_accounting_call(row) for row in rows]


def serialize_execution_accounting_call(row: LlmCall) -> dict[str, Any]:
    payload = serialize_llm_call(row, include_provider_log=False)
    extras = row.provider_extras or {}
    # Arbitrary provider strings and nested objects are not exportable evidence.
    payload["provider_extras"] = {
        key: value for key, value in extras.items()
        if isinstance(value, int | float) and not isinstance(value, bool)
    }
    payload["call_status"] = "failed" if extras.get("_loom_call_status") == "failed" else "completed"
    if payload["call_status"] == "failed":
        payload["provider_extras"]["_loom_call_status"] = "failed"
    # Zero numeric placeholders on failed or partial calls are not measured
    # zero usage. Retain only the Gateway's bounded diagnostic vocabulary;
    # arbitrary upstream strings and request/response bodies stay private.
    for key, allowed in {
        "_loom_usage_status": {"missing", "partial"},
        "_loom_failure_category": {
            "upstream_timeout", "upstream_transport", "upstream_http_4xx",
            "upstream_http_5xx", "upstream_http_error", "attempt_deadline_reached",
        },
        "_loom_gateway_route": {"native_responses", "chat_translation"},
    }.items():
        value = extras.get(key)
        if isinstance(value, str) and value in allowed:
            payload["provider_extras"][key] = value
    raw = extras.get("_loom_raw_provider_log")
    response = raw.get("response") if isinstance(raw, dict) else None
    body = response.get("body") if isinstance(response, dict) else None
    choices = body.get("choices") if isinstance(body, dict) else None
    reason = (
        choices[0].get("finish_reason")
        if isinstance(choices, list) and choices and isinstance(choices[0], dict) else None
    )
    payload["finish_reason"] = reason if isinstance(reason, str) and reason in {
        "stop", "length", "tool_calls", "function_call", "content_filter",
    } else None
    return payload
