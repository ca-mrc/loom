"""llm_calls insert helper — one writer for all dialect endpoints
(Plan 9 Task 6)."""

from __future__ import annotations

import json
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.auth import AuthContext
from loom.data_lifecycle_registry import ensure_trial_event_lifecycle_authority
from loom.db.schema import (
    ExecutionAttempt,
    HarborBaselineSession,
    LlmCall,
    PipelineRun,
    PipelineStageRun,
)
from loom.request_params import coerce_request_params, normalize_request_params
from loom_llm_gateway.dialect import USAGE_STATUS_KEY, TokenUsage
from loom_llm_gateway.metrics import COST_USD_TOTAL, LLM_CALLS_TOTAL
from loom_llm_gateway.rate_card import COST_META_CONFIDENCE_KEY, COST_META_SOURCE_KEY


def _projected_text(value: Any) -> str:
    # Mirrors `provider_extras ->> key`, with '' for an absent or null key.
    if value is None:
        return ""
    return value if isinstance(value, str) else json.dumps(value)


def usage_projection_columns(provider_extras: dict[str, Any]) -> dict[str, str]:
    """Insert-time copies of the cost/usage keys that aggregates filter on (#2687)."""
    return {
        "cost_source": _projected_text(provider_extras.get(COST_META_SOURCE_KEY)),
        "cost_confidence": _projected_text(provider_extras.get(COST_META_CONFIDENCE_KEY)),
        "usage_status": _projected_text(provider_extras.get(USAGE_STATUS_KEY)),
    }


def _service_execution_binding(
    auth_context: AuthContext | None,
    *,
    team_id: UUID,
    trial_id: UUID | None,
) -> dict[str, Any] | None:
    """The lease/generation a service-execution call is attributed to.

    Dispatch already fenced this context before the upstream request. Retain
    that identity even if the request finishes after revocation; never infer
    a newer generation from current lease state or client-supplied fields.
    """
    if auth_context is None or auth_context.service_execution_lease_id is None:
        return None
    if auth_context.service_execution_generation is None:
        raise ValueError("service execution call lacks its generation")
    if auth_context.trial_id != trial_id or auth_context.team_id != team_id:
        raise ValueError("service execution call subject differs from authenticated context")
    return {
        "lease_id": str(auth_context.service_execution_lease_id),
        "generation": auth_context.service_execution_generation,
    }


async def record_call(
    session: AsyncSession,
    *,
    team_id: UUID,
    trial_id: UUID | None = None,
    execution_attempt_id: UUID | None = None,
    baseline_session_id: UUID | None = None,
    step_id: str,
    dialect: str,
    model: str,
    usage: TokenUsage,
    cost_usd: float,
    rate_card_hash: str,
    provider: str | None = None,
    attempt: int = 1,
    request_params: dict[str, Any] | None = None,
    raw_provider_log: dict[str, Any] | None = None,
    client_call_id: UUID | None = None,
    agent_execution_id: UUID | None = None,
    agent_run_attempt_id: UUID | None = None,
    episode: int | None = None,
    call_ordinal: int | None = None,
    requested_model: str | None = None,
    response_model: str | None = None,
    role: str | None = None,
    correlation_status: str = "legacy_uncorrelated",
    commit: bool = True,
    auth_context: AuthContext | None = None,
) -> None:
    """Insert one row into `llm_calls`. Called by every dialect endpoint
    (chat / messages / responses / gemini) AFTER the upstream provider
    returns successfully. The trial's worker reads these rows at finalize
    (via the CP endpoint) and projects each into an LLMCallEvent.

    Side effect (#81 slice B-2): emits prometheus counters for
    `loom_gateway_llm_calls_total{result="ok"}` and
    `loom_gateway_cost_usd_total`. `provider` is the connection's
    `provider_type` (openai, anthropic, google, openai-compatible,
    custom) — defaults to `dialect` if not supplied for backwards
    compatibility with callers that haven't been updated.

    `attempt` is the gateway-internal attempt number that produced
    this successful row (#298 Slice B). Defaults to 1 so callers that
    don't go through the retry helper keep the historical semantics.
    """
    if (
        sum(
            subject is not None for subject in (trial_id, execution_attempt_id, baseline_session_id)
        )
        != 1
    ):
        raise ValueError("exactly one LLM call subject is required")
    if baseline_session_id is not None:
        baseline_team = await session.scalar(
            select(HarborBaselineSession.team_id).where(
                HarborBaselineSession.id == baseline_session_id
            )
        )
        if baseline_team != team_id:
            raise ValueError("Harbor baseline attribution does not belong to team")
    if execution_attempt_id is not None:
        attempt_team_id = (
            await session.execute(
                select(PipelineRun.team_id)
                .join(PipelineStageRun, PipelineStageRun.pipeline_run_id == PipelineRun.id)
                .join(ExecutionAttempt, ExecutionAttempt.stage_run_id == PipelineStageRun.id)
                .where(ExecutionAttempt.id == execution_attempt_id)
            )
        ).scalar_one_or_none()
        if attempt_team_id != team_id:
            raise ValueError("ExecutionAttempt attribution does not belong to team")

    audit_request_params = (
        normalize_request_params({})
        if request_params is None
        else coerce_request_params(request_params)
    )
    llm_call_id = uuid4()
    provider_extras = dict(usage.provider_extras)
    if raw_provider_log is not None:
        provider_extras["_loom_raw_provider_log"] = {
            **raw_provider_log,
            "llm_call_id": str(llm_call_id),
            "trial_id": str(trial_id) if trial_id is not None else None,
            "execution_attempt_id": (
                str(execution_attempt_id) if execution_attempt_id is not None else None
            ),
            "baseline_session_id": str(baseline_session_id) if baseline_session_id else None,
            "step_id": step_id,
            "ref": f"llm_calls/{llm_call_id}/provider_extras/_loom_raw_provider_log",
        }
    # Every dialect, not only routes that keep a raw provider log: the lease
    # ledger reads only bound rows (#2054).
    if (
        binding := _service_execution_binding(auth_context, team_id=team_id, trial_id=trial_id)
    ) is not None:
        provider_extras["_loom_raw_provider_log"] = {
            **provider_extras.get("_loom_raw_provider_log", {}),
            "service_execution": binding,
        }

    lifecycle_authority_id = None
    if trial_id is not None:
        lifecycle_authority_id = await ensure_trial_event_lifecycle_authority(
            session,
            trial_id=trial_id,
            expected_team_id=team_id,
        )

    await session.execute(
        insert(LlmCall).values(
            id=llm_call_id,
            team_id=team_id,
            trial_id=trial_id,
            execution_attempt_id=execution_attempt_id,
            baseline_session_id=baseline_session_id,
            step_id=step_id,
            dialect=dialect,
            model=model,
            input_tokens=usage.input_tokens,
            output_tokens=usage.output_tokens,
            provider_extras=provider_extras,
            **usage_projection_columns(provider_extras),
            request_params=audit_request_params,
            cost_usd=cost_usd,
            rate_card_hash=rate_card_hash,
            attempt=attempt,
            lifecycle_authority_id=lifecycle_authority_id,
            client_call_id=client_call_id,
            agent_execution_id=agent_execution_id,
            agent_run_attempt_id=agent_run_attempt_id,
            episode=episode,
            call_ordinal=call_ordinal,
            requested_model=requested_model or model,
            response_model=response_model,
            role=role,
            correlation_status=correlation_status,
        )
    )
    if commit:
        await session.commit()
    provider_label = provider if provider is not None else dialect
    LLM_CALLS_TOTAL.labels(
        provider=provider_label,
        dialect=dialect,
        result="ok",
    ).inc()
    if cost_usd > 0:
        COST_USD_TOTAL.labels(
            team_id=str(team_id),
            provider=provider_label,
        ).inc(cost_usd)


async def record_failed_call(
    session: AsyncSession,
    *,
    team_id: UUID,
    trial_id: UUID | None = None,
    execution_attempt_id: UUID | None = None,
    baseline_session_id: UUID | None = None,
    step_id: str,
    dialect: str,
    model: str,
    provider: str | None = None,
    attempt: int = 1,
    request_params: dict[str, Any] | None = None,
    failure_category: str,
    failure_status_code: int | None = None,
    failure_error_type: str | None = None,
    client_call_id: UUID | None = None,
    agent_execution_id: UUID | None = None,
    agent_run_attempt_id: UUID | None = None,
    episode: int | None = None,
    call_ordinal: int | None = None,
    requested_model: str | None = None,
    response_model: str | None = None,
    role: str | None = None,
    correlation_status: str = "legacy_uncorrelated",
    auth_context: AuthContext | None = None,
    commit: bool = True,
    request_pool_attribution: dict[str, Any] | None = None,
) -> None:
    """Insert one zero-token audit row for an attempted upstream call.

    This is intentionally separate from `record_call`: successful calls carry
    usage/cost, while failed upstream attempts preserve attribution and safe
    request controls so debug surfaces can distinguish "no request attempted"
    from "request attempted and failed upstream".
    """
    if (
        sum(
            subject is not None for subject in (trial_id, execution_attempt_id, baseline_session_id)
        )
        != 1
    ):
        raise ValueError("exactly one LLM call subject is required")
    if baseline_session_id is not None:
        baseline_team = await session.scalar(
            select(HarborBaselineSession.team_id).where(
                HarborBaselineSession.id == baseline_session_id
            )
        )
        if baseline_team != team_id:
            raise ValueError("Harbor baseline attribution does not belong to team")
    if execution_attempt_id is not None:
        attempt_team_id = (
            await session.execute(
                select(PipelineRun.team_id)
                .join(PipelineStageRun, PipelineStageRun.pipeline_run_id == PipelineRun.id)
                .join(ExecutionAttempt, ExecutionAttempt.stage_run_id == PipelineStageRun.id)
                .where(ExecutionAttempt.id == execution_attempt_id)
            )
        ).scalar_one_or_none()
        if attempt_team_id != team_id:
            raise ValueError("ExecutionAttempt attribution does not belong to team")

    audit_request_params = (
        normalize_request_params({})
        if request_params is None
        else coerce_request_params(request_params)
    )
    provider_extras: dict[str, Any] = {
        "_loom_call_status": "failed",
        "_loom_failure_category": failure_category,
        "_loom_usage_status": "missing",
    }
    if request_pool_attribution is not None:
        provider_extras["_loom_request_pool"] = request_pool_attribution
    if failure_status_code is not None:
        provider_extras["_loom_failure_status_code"] = int(failure_status_code)
    if failure_error_type:
        provider_extras["_loom_failure_error_type"] = failure_error_type

    if (
        binding := _service_execution_binding(auth_context, team_id=team_id, trial_id=trial_id)
    ) is not None:
        provider_extras["_loom_raw_provider_log"] = {"service_execution": binding}

    lifecycle_authority_id = None
    if trial_id is not None:
        lifecycle_authority_id = await ensure_trial_event_lifecycle_authority(
            session,
            trial_id=trial_id,
            expected_team_id=team_id,
        )

    await session.execute(
        insert(LlmCall).values(
            team_id=team_id,
            trial_id=trial_id,
            execution_attempt_id=execution_attempt_id,
            baseline_session_id=baseline_session_id,
            step_id=step_id,
            dialect=dialect,
            model=model,
            input_tokens=0,
            output_tokens=0,
            provider_extras=provider_extras,
            **usage_projection_columns(provider_extras),
            request_params=audit_request_params,
            cost_usd=0,
            rate_card_hash="failed-upstream",
            attempt=max(int(attempt or 1), 1),
            lifecycle_authority_id=lifecycle_authority_id,
            client_call_id=client_call_id,
            agent_execution_id=agent_execution_id,
            agent_run_attempt_id=agent_run_attempt_id,
            episode=episode,
            call_ordinal=call_ordinal,
            requested_model=requested_model or model,
            response_model=response_model,
            role=role,
            correlation_status=correlation_status,
        )
    )
    if commit:
        await session.commit()
    provider_label = provider if provider is not None else dialect
    result = "timeout" if failure_category == "upstream_timeout" else "upstream_error"
    LLM_CALLS_TOTAL.labels(
        provider=provider_label,
        dialect=dialect,
        result=result,
    ).inc()
