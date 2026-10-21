"""Exactly-once durable accounting boundary for Pipeline provider dispatches."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import datetime
from decimal import ROUND_CEILING, Decimal
from typing import Any, Literal
from uuid import UUID, uuid4

from sqlalchemy import and_, func, or_, select, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession

from loom.auth import AuthContext
from loom.db.schema import (
    ExecutionAttempt,
    ExecutionAttemptProviderBudget,
    LlmCall,
    PipelineBudgetLedger,
    PipelineBudgetReservation,
    PipelineProviderDispatch,
    PipelineRun,
    PipelineRunControlBinding,
    PipelineStageRun,
    ProviderConnection,
)
from loom.request_params import coerce_request_params, normalize_request_params
from loom_llm_gateway.dialect import TokenUsage
from loom_llm_gateway.execution_attempt_dispatch import authorize_execution_attempt_dispatch

logger = logging.getLogger(__name__)
_ACCOUNTING_LOCK_ATTEMPTS = 3

_RECOVERABLE_ACCOUNTING_ERRORS = frozenset({
    "provider_dispatch_accounting_unavailable",
    "provider_dispatch_accounting_drift",
    "provider_dispatch_accounting_violation_conflict",
})

DispatchOutcome = Literal["not_dispatched", "succeeded", "failed", "uncertain"]
_DIGEST_RE = re.compile(r"sha256:[0-9a-f]{64}")
_SAFE_USAGE_EXTRA_KEYS = frozenset(
    {
        "cache_creation_input_tokens",
        "cache_read_input_tokens",
        "reasoning_tokens",
        "thoughtsTokenCount",
        "_loom_cost_source",
        "_loom_cost_confidence",
        "_loom_cost_currency",
        "_loom_pricing_source",
        "_loom_rate_card_provider",
        "_loom_unpriced_reason",
    }
)


class ProviderDispatchError(RuntimeError):
    """A stable secret-free provider dispatch rejection."""

    def __init__(self, reason: str, *, status_code: int = 409) -> None:
        super().__init__(reason)
        self.reason = reason
        self.status_code = status_code


@dataclass(frozen=True, slots=True)
class ProviderDispatchGrant:
    dispatch_id: UUID
    provider_request_id: UUID
    reservation_id: UUID
    reserved_cost_microusd: int
    timeout_seconds: int


@dataclass(frozen=True, slots=True)
class ProviderDispatchSettlement:
    dispatch_id: UUID
    outcome: DispatchOutcome
    actual_cost_microusd: int
    llm_call_id: UUID | None


def cost_microusd(cost_usd: float | Decimal) -> int:
    value = Decimal(str(cost_usd))
    if not value.is_finite() or value < 0:
        raise ProviderDispatchError("provider_dispatch_cost_invalid", status_code=502)
    return int((value * Decimal(1_000_000)).to_integral_value(rounding=ROUND_CEILING))


async def reserve_provider_dispatch(
    session: AsyncSession,
    *,
    ctx: AuthContext,
    provider_request_id: UUID,
    request_digest: str,
    provider_connection_id: UUID,
    provider: str,
    model: str,
    wire_api: Literal["responses", "messages"],
) -> ProviderDispatchGrant:
    """Reserve the whole remaining Attempt cost bound before any upstream send."""

    _require_attempt_context(ctx)
    if (
        _DIGEST_RE.fullmatch(request_digest) is None
        or not provider
        or not model
        or wire_api not in {"responses", "messages"}
    ):
        raise ProviderDispatchError("provider_dispatch_request_invalid", status_code=400)
    async with session.begin():
        await authorize_execution_attempt_dispatch(session, ctx, lock=True)
        budget = await session.get(
            ExecutionAttemptProviderBudget,
            ctx.execution_attempt_id,
            with_for_update=True,
        )
        if budget is None:
            raise ProviderDispatchError("provider_dispatch_budget_unavailable")
        timeout_seconds = budget.per_call_timeout_seconds
        binding = (
            await session.execute(
                select(PipelineRunControlBinding)
                .join(
                    PipelineStageRun,
                    (PipelineStageRun.pipeline_run_id == PipelineRunControlBinding.pipeline_run_id)
                    & (PipelineStageRun.node_key == PipelineRunControlBinding.node_key),
                )
                .join(ExecutionAttempt, ExecutionAttempt.stage_run_id == PipelineStageRun.id)
                .where(
                    ExecutionAttempt.id == ctx.execution_attempt_id,
                    PipelineRunControlBinding.node_key == ctx.step_id,
                    PipelineRunControlBinding.snapshot_sha256 == budget.binding_snapshot_sha256,
                )
            )
        ).scalar_one_or_none()
        connection = await session.get(ProviderConnection, provider_connection_id)
        _validate_binding(
            ctx=ctx,
            binding=binding,
            connection=connection,
            provider_connection_id=provider_connection_id,
            provider=provider,
            model=model,
            wire_api=wire_api,
        )
        existing = (
            await session.execute(
                select(PipelineProviderDispatch)
                .where(
                    PipelineProviderDispatch.execution_attempt_id == ctx.execution_attempt_id,
                    PipelineProviderDispatch.provider_request_id == provider_request_id,
                )
                .with_for_update()
            )
        ).scalar_one_or_none()
        if existing is not None:
            _validate_replay(
                existing,
                request_digest=request_digest,
                binding_snapshot_sha256=budget.binding_snapshot_sha256,
                provider_connection_id=provider_connection_id,
                provider=provider,
                model=model,
                wire_api=wire_api,
            )
            if existing.state != "reserved":
                raise ProviderDispatchError(
                    f"provider_dispatch_replay_{existing.state}_{existing.outcome or 'pending'}"
                )
            return ProviderDispatchGrant(
                dispatch_id=existing.id,
                provider_request_id=existing.provider_request_id,
                reservation_id=existing.reservation_id,
                reserved_cost_microusd=existing.reserved_cost_microusd,
                timeout_seconds=timeout_seconds,
            )

        if budget.requests_reserved + budget.requests_settled >= budget.request_limit:
            raise ProviderDispatchError(
                "provider_dispatch_request_budget_exhausted", status_code=429
            )
        worst_case_cost = (
            budget.cost_limit_microusd
            - budget.cost_reserved_microusd
            - budget.cost_settled_microusd
        )
        if worst_case_cost <= 0:
            raise ProviderDispatchError("provider_dispatch_cost_budget_exhausted", status_code=429)
        binding_run_id = binding.pipeline_run_id if binding is not None else None
        ledger = (
            await session.execute(
                select(PipelineBudgetLedger)
                .where(PipelineBudgetLedger.pipeline_run_id == binding_run_id)
                .with_for_update()
            )
        ).scalar_one_or_none()
        if ledger is None or ledger.terminal_cause is not None:
            raise ProviderDispatchError("provider_dispatch_run_not_dispatchable")
        run_remaining = (
            ledger.provider_limit_microusd
            - ledger.provider_reserved_microusd
            - ledger.provider_settled_microusd
        )
        if worst_case_cost > run_remaining:
            raise ProviderDispatchError("provider_dispatch_run_budget_exhausted", status_code=429)

        reservation_id = uuid4()
        dispatch_id = uuid4()
        reservation_key = f"provider:{ctx.execution_attempt_id}:{provider_request_id}"
        session.add(
            PipelineBudgetReservation(
                id=reservation_id,
                pipeline_run_id=ledger.pipeline_run_id,
                execution_attempt_id=ctx.execution_attempt_id,
                kind="provider",
                reservation_key=reservation_key,
                request_digest=request_digest,
                reserved_amount=worst_case_cost,
                state="active",
                metadata_json={
                    "schema_version": "loom.pipeline-provider-dispatch-reservation.v1",
                    "provider_request_id": str(provider_request_id),
                },
            )
        )
        budget.requests_reserved += 1
        budget.cost_reserved_microusd += worst_case_cost
        budget.version += 1
        ledger.provider_reserved_microusd += worst_case_cost
        ledger.version += 1
        ledger.updated_at = _database_now()
        session.add(
            PipelineProviderDispatch(
                id=dispatch_id,
                execution_attempt_id=ctx.execution_attempt_id,
                provider_request_id=provider_request_id,
                reservation_id=reservation_id,
                binding_snapshot_sha256=budget.binding_snapshot_sha256,
                request_digest=request_digest,
                provider_connection_id=provider_connection_id,
                provider=provider,
                model=model,
                wire_api=wire_api,
                state="reserved",
                reserved_cost_microusd=worst_case_cost,
            )
        )
    return ProviderDispatchGrant(
        dispatch_id=dispatch_id,
        provider_request_id=provider_request_id,
        reservation_id=reservation_id,
        reserved_cost_microusd=worst_case_cost,
        timeout_seconds=timeout_seconds,
    )


async def mark_provider_dispatch_sent(
    session: AsyncSession,
    *,
    ctx: AuthContext,
    dispatch_id: UUID,
) -> None:
    """Persist the reached-upstream boundary before opening the network call."""

    _require_attempt_context(ctx)
    async with session.begin():
        await authorize_execution_attempt_dispatch(session, ctx, lock=True)
        dispatch = await session.get(
            PipelineProviderDispatch, dispatch_id, with_for_update=True, populate_existing=True
        )
        if dispatch is None or dispatch.execution_attempt_id != ctx.execution_attempt_id:
            raise ProviderDispatchError("provider_dispatch_not_found", status_code=404)
        if dispatch.state != "reserved":
            raise ProviderDispatchError(
                f"provider_dispatch_replay_{dispatch.state}_{dispatch.outcome or 'pending'}"
            )
        dispatch.state = "dispatched"
        dispatch.upstream_attempt_count = 1
        dispatch.dispatched_at = _database_now()
        dispatch.version += 1


async def release_provider_dispatch_unsent(
    session: AsyncSession,
    *,
    ctx: AuthContext,
    dispatch_id: UUID,
) -> ProviderDispatchSettlement:
    """Release only a reservation whose durable state proves no upstream send."""

    _require_attempt_context(ctx)
    async with session.begin():
        dispatch = await session.get(
            PipelineProviderDispatch, dispatch_id, with_for_update=True, populate_existing=True
        )
        if dispatch is None or dispatch.execution_attempt_id != ctx.execution_attempt_id:
            raise ProviderDispatchError("provider_dispatch_not_found", status_code=404)
        if dispatch.state == "settled":
            if dispatch.outcome != "not_dispatched":
                raise ProviderDispatchError("provider_dispatch_release_after_send")
            return _settlement(dispatch)
        if dispatch.state != "reserved":
            raise ProviderDispatchError("provider_dispatch_release_after_send")
        settlement = await _release_locked_unsent_dispatch(session, dispatch)
    return settlement


async def _release_locked_unsent_dispatch(
    session: AsyncSession,
    dispatch: PipelineProviderDispatch,
    *,
    ledger_nowait: bool = False,
) -> ProviderDispatchSettlement:
    reservation, budget, ledger = await _lock_accounting(
        session, dispatch, ledger_nowait=ledger_nowait
    )
    budget.requests_reserved -= 1
    budget.cost_reserved_microusd -= dispatch.reserved_cost_microusd
    budget.version += 1
    ledger.provider_reserved_microusd -= dispatch.reserved_cost_microusd
    ledger.version += 1
    ledger.updated_at = _database_now()
    reservation.state = "released"
    reservation.settled_at = _database_now()
    dispatch.state = "settled"
    dispatch.outcome = "not_dispatched"
    dispatch.actual_cost_microusd = 0
    dispatch.outcome_at = _database_now()
    dispatch.settled_at = dispatch.outcome_at
    dispatch.version += 1
    return _settlement(dispatch)


async def settle_provider_dispatch(
    session: AsyncSession,
    *,
    ctx: AuthContext,
    dispatch_id: UUID,
    outcome: Literal["succeeded", "failed", "uncertain"],
    usage: TokenUsage,
    actual_cost_microusd: int,
    rate_card_hash: str,
    request_params: dict[str, Any],
    response_digest: str | None,
    failure_category: str | None = None,
) -> ProviderDispatchSettlement:
    """Atomically correlate LLMCall, provider outcome, and both budget ledgers."""

    _require_attempt_context(ctx)
    if actual_cost_microusd < 0:
        raise ProviderDispatchError("provider_dispatch_cost_invalid", status_code=502)
    if (outcome == "succeeded") != (response_digest is not None):
        raise ProviderDispatchError("provider_dispatch_response_digest_invalid", status_code=502)
    for lock_attempt in range(_ACCOUNTING_LOCK_ATTEMPTS):
        try:
            async with session.begin():
                # Accounting remains valid after cancellation; lock without re-authorizing send.
                await session.get(ExecutionAttempt, ctx.execution_attempt_id, with_for_update=True)
                dispatch = await session.get(
                    PipelineProviderDispatch, dispatch_id, with_for_update=True, populate_existing=True
                )
                if dispatch is None or dispatch.execution_attempt_id != ctx.execution_attempt_id:
                    raise ProviderDispatchError("provider_dispatch_not_found", status_code=404)
                if dispatch.state == "settled":
                    if (
                        dispatch.outcome != outcome
                        or dispatch.actual_cost_microusd != actual_cost_microusd
                        or dispatch.response_digest != response_digest
                    ):
                        raise ProviderDispatchError("provider_dispatch_settlement_replay_drift")
                    return _settlement(dispatch)
                if dispatch.state != "dispatched":
                    raise ProviderDispatchError("provider_dispatch_not_sent")
                assert ctx.team_id is not None
                assert ctx.execution_attempt_id is not None
                assert ctx.step_id is not None
                settlement = await _settle_locked_dispatch(
                    session,
                    dispatch=dispatch,
                    team_id=ctx.team_id,
                    step_id=ctx.step_id,
                    outcome=outcome,
                    usage=usage,
                    actual_cost_microusd=actual_cost_microusd,
                    rate_card_hash=rate_card_hash,
                    request_params=request_params,
                    response_digest=response_digest,
                    failure_category=failure_category,
                    ledger_nowait=True,
                )
            return settlement

        except DBAPIError as exc:
            if getattr(exc.orig, "sqlstate", None) != "55P03":
                raise
            if lock_attempt == _ACCOUNTING_LOCK_ATTEMPTS - 1:
                raise ProviderDispatchError("provider_dispatch_accounting_busy", status_code=503) from exc
            # The failed transaction has released its Attempt/dispatch/accounting
            # locks. Wait on the ledger alone so cancellation can finish its
            # ledger-before-Attempt order, then retry the complete settlement.
            try:
                await _wait_for_accounting_ledger(session, ctx.execution_attempt_id)
            except DBAPIError as wait_exc:
                if getattr(wait_exc.orig, "sqlstate", None) != "55P03":
                    raise
                raise ProviderDispatchError(
                    "provider_dispatch_accounting_busy", status_code=503
                ) from wait_exc
    raise AssertionError("unreachable accounting retry exhaustion")


async def _wait_for_accounting_ledger(session: AsyncSession, attempt_id: UUID | None) -> None:
    async with session.begin():
        # At most two one-second waits across the three settlement attempts.
        await session.execute(text("SET LOCAL lock_timeout = '1s'"))
        await session.execute(
            select(PipelineBudgetLedger.pipeline_run_id)
            .join(
                PipelineStageRun,
                PipelineStageRun.pipeline_run_id == PipelineBudgetLedger.pipeline_run_id,
            )
            .join(ExecutionAttempt, ExecutionAttempt.stage_run_id == PipelineStageRun.id)
            .where(ExecutionAttempt.id == attempt_id)
            .with_for_update(of=PipelineBudgetLedger, read=True)
        )


async def settle_stale_provider_dispatches(
    session: AsyncSession,
    *,
    stale_before: datetime,
    limit: int = 100,
) -> int:
    """Release abandoned unsent reserves and conservatively settle old sends."""

    if not 1 <= limit <= 1000:
        raise ValueError("limit must be between 1 and 1000")
    recovered = 0
    async with session.begin():
        # Recovery must never become a Gateway shutdown or readiness blocker
        # behind a live request's accounting locks. A timed-out sweep rolls
        # back without mutating the dispatch and retries on the next interval.
        await session.execute(text("SET LOCAL lock_timeout = '1s'"))
        await session.execute(text("SET LOCAL statement_timeout = '5s'"))
        rows = (
            await session.execute(
                select(PipelineProviderDispatch, PipelineStageRun, PipelineRun)
                .join(
                    ExecutionAttempt,
                    ExecutionAttempt.id == PipelineProviderDispatch.execution_attempt_id,
                )
                .join(PipelineStageRun, PipelineStageRun.id == ExecutionAttempt.stage_run_id)
                .join(PipelineRun, PipelineRun.id == PipelineStageRun.pipeline_run_id)
                .where(
                    or_(
                        and_(
                            PipelineProviderDispatch.state == "reserved",
                            PipelineProviderDispatch.created_at <= stale_before,
                        ),
                        and_(
                            PipelineProviderDispatch.state == "dispatched",
                            PipelineProviderDispatch.dispatched_at <= stale_before,
                        ),
                    ),
                )
                .order_by(
                    func.coalesce(
                        PipelineProviderDispatch.dispatched_at,
                        PipelineProviderDispatch.created_at,
                    ),
                    PipelineProviderDispatch.id,
                )
                .limit(limit)
                .with_for_update(
                    # Cleanup and senders own the Attempt before accounting.
                    # Include it here so a later LlmCall FK insert cannot cycle
                    # with cleanup waiting for our run-ledger lock.
                    of=(ExecutionAttempt, PipelineProviderDispatch),
                    skip_locked=True,
                )
                .execution_options(populate_existing=True)
            )
        ).all()
        for dispatch, stage, run in rows:
            dispatch_id = dispatch.id
            try:
                async with session.begin_nested():
                    if dispatch.state == "reserved":
                        await _release_locked_unsent_dispatch(
                            session, dispatch, ledger_nowait=True
                        )
                    else:
                        await _settle_locked_dispatch(
                            session,
                            dispatch=dispatch,
                            team_id=run.team_id,
                            step_id=stage.node_key,
                            outcome="uncertain",
                            usage=TokenUsage(input_tokens=0, output_tokens=0),
                            actual_cost_microusd=dispatch.reserved_cost_microusd,
                            rate_card_hash="pipeline:stale-dispatch-recovery",
                            request_params={},
                            response_digest=None,
                            failure_category="gateway_crash_after_send",
                            ledger_nowait=True,
                        )
            except ProviderDispatchError as exc:
                if exc.reason not in _RECOVERABLE_ACCOUNTING_ERRORS:
                    raise
                logger.warning(
                    "provider_dispatch_recovery_skipped dispatch_id=%s reason=%s",
                    dispatch_id, exc.reason,
                )
            except DBAPIError as exc:
                if getattr(exc.orig, "sqlstate", None) != "55P03":
                    raise
                # Cancellation may acquire the ledger after our candidate lock.
                # Roll back this savepoint rather than wait while holding its Attempt.
                logger.warning(
                    "provider_dispatch_recovery_skipped dispatch_id=%s reason=%s",
                    dispatch_id, "provider_dispatch_accounting_busy",
                )
            else:
                recovered += 1
    return recovered


async def _settle_locked_dispatch(
    session: AsyncSession,
    *,
    dispatch: PipelineProviderDispatch,
    team_id: UUID,
    step_id: str,
    outcome: Literal["succeeded", "failed", "uncertain"],
    usage: TokenUsage,
    actual_cost_microusd: int,
    rate_card_hash: str,
    request_params: dict[str, Any],
    response_digest: str | None,
    failure_category: str | None,
    ledger_nowait: bool = False,
) -> ProviderDispatchSettlement:
    reservation, budget, ledger = await _lock_accounting(
        session, dispatch, ledger_nowait=ledger_nowait
    )
    overage = (
        actual_cost_microusd > dispatch.reserved_cost_microusd
        or budget.cost_settled_microusd + actual_cost_microusd > budget.cost_limit_microusd
        or ledger.provider_settled_microusd + actual_cost_microusd > ledger.provider_limit_microusd
    )
    if overage:
        if ledger.terminal_cause not in {None, "accounting_violation"}:
            raise ProviderDispatchError("provider_dispatch_accounting_violation_conflict")
        ledger.terminal_cause = "accounting_violation"
        ledger.terminal_cause_at = _database_now()
    llm_call_id = uuid4()
    provider_extras: dict[str, Any] = {
        "_loom_provider_dispatch_id": str(dispatch.id),
        "_loom_provider_request_id": str(dispatch.provider_request_id),
        "_loom_dispatch_outcome": outcome,
    }
    if outcome != "succeeded":
        provider_extras.update(
            _loom_call_status="failed",
            _loom_failure_category=failure_category or outcome,
            _loom_usage_status="conservative_reserved_cost",
        )
    else:
        provider_extras.update(_safe_usage_extras(usage.provider_extras))
    session.add(
        LlmCall(
            id=llm_call_id,
            team_id=team_id,
            execution_attempt_id=dispatch.execution_attempt_id,
            trial_id=None,
            step_id=step_id,
            dialect=f"pipeline_{dispatch.wire_api}",
            model=dispatch.model,
            input_tokens=usage.input_tokens if outcome == "succeeded" else 0,
            output_tokens=usage.output_tokens if outcome == "succeeded" else 0,
            provider_extras=provider_extras,
            request_params=(
                normalize_request_params({})
                if not request_params
                else coerce_request_params(request_params)
            ),
            cost_usd=Decimal(actual_cost_microusd) / Decimal(1_000_000),
            rate_card_hash=rate_card_hash,
            attempt=1,
            lifecycle_authority_id=None,
        )
    )
    budget.requests_reserved -= 1
    budget.requests_settled += 1
    budget.cost_reserved_microusd -= dispatch.reserved_cost_microusd
    budget.cost_settled_microusd += actual_cost_microusd
    budget.version += 1
    ledger.provider_reserved_microusd -= dispatch.reserved_cost_microusd
    ledger.provider_settled_microusd += actual_cost_microusd
    ledger.version += 1
    ledger.updated_at = _database_now()
    reservation.state = "settled"
    reservation.settled_amount = actual_cost_microusd
    reservation.settled_at = _database_now()
    dispatch.state = "settled"
    dispatch.outcome = outcome
    dispatch.actual_cost_microusd = actual_cost_microusd
    dispatch.response_digest = response_digest
    dispatch.llm_call_id = llm_call_id
    dispatch.outcome_at = _database_now()
    dispatch.settled_at = dispatch.outcome_at
    dispatch.version += 1
    return _settlement(dispatch)


def _validate_binding(
    *,
    ctx: AuthContext,
    binding: PipelineRunControlBinding | None,
    connection: ProviderConnection | None,
    provider_connection_id: UUID,
    provider: str,
    model: str,
    wire_api: str,
) -> None:
    snapshot = binding.snapshot_json if binding is not None else None
    if (
        binding is None
        or connection is None
        or not isinstance(snapshot, dict)
        or ctx.control_binding_snapshot_digest != binding.snapshot_sha256
        or ctx.provider_connection_id != provider_connection_id
        or binding.provider_connection_id != provider_connection_id
        or connection.id != provider_connection_id
        or connection.status != "valid"
        or connection.deleted_at is not None
        or (connection.allowed_models is not None and model not in connection.allowed_models)
        or snapshot.get("provider_connection_id") != str(provider_connection_id)
        or snapshot.get("provider") != provider
        or snapshot.get("model") != model
        or snapshot.get("wire_api") != wire_api
    ):
        raise ProviderDispatchError("provider_dispatch_binding_drift", status_code=403)


def _require_attempt_context(ctx: AuthContext) -> None:
    if ctx.execution_attempt_id is None or ctx.step_id is None or ctx.team_id is None:
        raise ProviderDispatchError("provider_dispatch_attempt_required", status_code=403)


def _validate_replay(
    dispatch: PipelineProviderDispatch,
    *,
    request_digest: str,
    binding_snapshot_sha256: str,
    provider_connection_id: UUID,
    provider: str,
    model: str,
    wire_api: str,
) -> None:
    if (
        dispatch.request_digest != request_digest
        or dispatch.binding_snapshot_sha256 != binding_snapshot_sha256
        or dispatch.provider_connection_id != provider_connection_id
        or dispatch.provider != provider
        or dispatch.model != model
        or dispatch.wire_api != wire_api
    ):
        raise ProviderDispatchError("provider_dispatch_request_id_conflict")


async def _lock_accounting(
    session: AsyncSession,
    dispatch: PipelineProviderDispatch,
    *,
    ledger_nowait: bool = False,
) -> tuple[PipelineBudgetReservation, ExecutionAttemptProviderBudget, PipelineBudgetLedger]:
    reservation = await session.get(
        PipelineBudgetReservation,
        dispatch.reservation_id,
        with_for_update=True,
        populate_existing=True,
    )
    budget = await session.get(
        ExecutionAttemptProviderBudget,
        dispatch.execution_attempt_id,
        with_for_update=True,
        populate_existing=True,
    )
    if reservation is None or budget is None:
        raise ProviderDispatchError("provider_dispatch_accounting_unavailable")
    ledger = await session.get(
        PipelineBudgetLedger,
        reservation.pipeline_run_id,
        with_for_update={"nowait": ledger_nowait},
        populate_existing=True,
    )
    if (
        ledger is None
        or reservation.execution_attempt_id != dispatch.execution_attempt_id
        or reservation.kind != "provider"
        or reservation.state != "active"
        or reservation.reserved_amount != dispatch.reserved_cost_microusd
        or reservation.request_digest != dispatch.request_digest
        or budget.requests_reserved < 1
        or budget.cost_reserved_microusd < dispatch.reserved_cost_microusd
        or ledger.provider_reserved_microusd < dispatch.reserved_cost_microusd
    ):
        raise ProviderDispatchError("provider_dispatch_accounting_drift")
    return reservation, budget, ledger


def _settlement(dispatch: PipelineProviderDispatch) -> ProviderDispatchSettlement:
    assert dispatch.outcome is not None
    assert dispatch.actual_cost_microusd is not None
    return ProviderDispatchSettlement(
        dispatch_id=dispatch.id,
        outcome=dispatch.outcome,  # type: ignore[arg-type]
        actual_cost_microusd=dispatch.actual_cost_microusd,
        llm_call_id=dispatch.llm_call_id,
    )


def _safe_usage_extras(values: dict[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key in _SAFE_USAGE_EXTRA_KEYS:
        value = values.get(key)
        if isinstance(value, bool | int | float) or (
            isinstance(value, str) and len(value.encode("utf-8")) <= 128
        ):
            result[key] = value
    basis = values.get("_loom_price_basis")
    if isinstance(basis, dict):
        from loom.provider_pricing import ModelPrice
        try:
            prices = ModelPrice.model_validate(basis.get("prices"))
        except ValueError:
            pass
        else:
            result["_loom_price_basis"] = {
                key: value for key, value in basis.items()
                if key in {"mode", "model", "currency", "unit", "catalog_id", "supplier_id", "revision",
                           "source_model", "source_url", "updated_at", "input_includes_cache"}
                and (value is None or isinstance(value, bool | int | float | str))
            }
            result["_loom_price_basis"]["prices"] = prices.model_dump(mode="json")
            supplier_metadata = basis.get("supplier_metadata")
            if isinstance(supplier_metadata, dict):
                result["_loom_price_basis"]["supplier_metadata"] = {
                    key: value for key, value in supplier_metadata.items()
                    if key in {"supplier", "group", "group_ratio", "pricing_version"}
                    and isinstance(value, str | int | float)
                }
    return result


def _database_now() -> Any:
    from sqlalchemy import func

    return func.clock_timestamp()


__all__ = [
    "ProviderDispatchError",
    "ProviderDispatchGrant",
    "ProviderDispatchSettlement",
    "cost_microusd",
    "mark_provider_dispatch_sent",
    "release_provider_dispatch_unsent",
    "reserve_provider_dispatch",
    "settle_provider_dispatch",
    "settle_stale_provider_dispatches",
]
