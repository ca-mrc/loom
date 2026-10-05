"""Durable paid-execution pricing, budget admission, and bill attribution (#1552)."""

from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from fractions import Fraction
from typing import Literal
from uuid import UUID, uuid4

from sqlalchemy import func, or_, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import (
    ExecutionBudgetPolicy,
    ExecutionCostReservation,
    ExecutionCostReservationDebit,
    ExecutionNodeCostAllocation,
    ExecutionNodeCostRecord,
    ExecutionPriceSnapshot,
    ExecutionTargetPriceBinding,
    ServiceExecutionLease,
    ServiceExecutionTarget,
    Trial,
)
from loom.execution_runtime_contract import ExecutionRuntimePlanV1, runtime_pod_resources
from loom.pipeline.keys import canonical_digest

_FINANCE_POLICY_LOCK = text(
    "SELECT pg_advisory_xact_lock(hashtextextended('execution-finance-policy-mutation', 1552))"
)
_FINANCE_POLICY_SHARED_LOCK = text(
    "SELECT pg_advisory_xact_lock_shared("
    "hashtextextended('execution-finance-policy-mutation', 1552))"
)
_BILLING_MUTATION_LOCK = text(
    "SELECT pg_advisory_xact_lock(hashtextextended('execution-finance-billing-mutation', 1552))"
)


@dataclass(frozen=True)
class ExecutionCostEstimate:
    duration_seconds: int
    requested_cpu_millis: int
    requested_memory_mib: int
    requested_ephemeral_storage_mib: int
    estimated_cost_microusd: int
    daily_costs: tuple[tuple[date, int], ...]
    estimate_sha256: str


@dataclass(frozen=True)
class ExecutionFinanceBlockedError(Exception):
    reason: str


def _clean_text(value: str, *, name: str, max_length: int) -> str:
    clean = str(value).strip()
    if not clean or len(clean) > max_length:
        raise ValueError(f"{name} must contain 1 to {max_length} characters")
    return clean


def _utc(value: datetime, *, name: str) -> datetime:
    if value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(UTC)


def _periods(now: datetime) -> tuple[date, date]:
    day = _utc(now, name="period timestamp").date()
    return day, day.replace(day=1)


def _ceil_fraction(value: Fraction) -> int:
    return (value.numerator + value.denominator - 1) // value.denominator


def _ceil_timedelta_seconds(value: timedelta) -> int:
    whole_seconds = value.days * 86_400 + value.seconds
    return whole_seconds + int(value.microseconds > 0)


def estimate_execution_cost(
    runtime_plan: ExecutionRuntimePlanV1,
    price: ExecutionPriceSnapshot,
    *,
    acquired_at: datetime,
    deadline_at: datetime,
) -> ExecutionCostEstimate:
    """Price the complete Pod request for the full admitted deadline.

    This is a conservative preflight reservation, not a provider bill. The
    fixed runtime-materializer request and all native sidecars are included.
    """

    start = _utc(acquired_at, name="acquired_at")
    deadline = _utc(deadline_at, name="deadline_at")
    if deadline <= start:
        raise ValueError("deadline_at must be after acquired_at")
    duration_seconds = max(1, _ceil_timedelta_seconds(deadline - start))
    pod_resources = runtime_pod_resources(runtime_plan)
    cpu_millis = pod_resources.cpu_millis
    memory_mib = pod_resources.memory_mib
    # Disk-backed emptyDir usage is included in the Pod's ephemeral-storage
    # envelope, not reserved in addition to container requests. Keep finance,
    # pre-Pod admission and native placement on the same effective request.
    storage_mib = pod_resources.ephemeral_storage_mib
    hourly_cost = (
        Fraction(price.base_microusd_per_hour)
        + Fraction(price.vcpu_microusd_per_hour * cpu_millis, 1000)
        + Fraction(price.memory_gib_microusd_per_hour * memory_mib, 1024)
        + Fraction(price.ephemeral_storage_gib_microusd_per_hour * storage_mib, 1024)
    )
    daily_costs: list[tuple[date, int]] = []
    cursor = start
    allocated_seconds = 0
    while cursor < deadline:
        next_day = datetime(cursor.year, cursor.month, cursor.day, tzinfo=UTC) + timedelta(days=1)
        segment_end = min(deadline, next_day)
        # Round cumulative elapsed time so each UTC boundary cannot add a
        # separate second to the complete reservation.
        cumulative_seconds = _ceil_timedelta_seconds(segment_end - start)
        segment_seconds = cumulative_seconds - allocated_seconds
        daily_costs.append(
            (cursor.date(), max(1, _ceil_fraction(hourly_cost * segment_seconds / 3600)))
        )
        allocated_seconds += segment_seconds
        cursor = segment_end
    estimated_microusd = sum(amount for _, amount in daily_costs)
    payload = {
        "schema_version": "loom.execution-cost-estimate.v1",
        "price_snapshot_id": str(price.id),
        "rate_card_sha256": price.rate_card_sha256,
        "duration_seconds": duration_seconds,
        "requested_cpu_millis": cpu_millis,
        "requested_memory_mib": memory_mib,
        "requested_ephemeral_storage_mib": storage_mib,
        "estimated_cost_microusd": estimated_microusd,
        "daily_costs": [
            {"budget_day": budget_day.isoformat(), "estimated_cost_microusd": amount}
            for budget_day, amount in daily_costs
        ],
    }
    return ExecutionCostEstimate(
        duration_seconds=duration_seconds,
        requested_cpu_millis=cpu_millis,
        requested_memory_mib=memory_mib,
        requested_ephemeral_storage_mib=storage_mib,
        estimated_cost_microusd=estimated_microusd,
        daily_costs=tuple(daily_costs),
        estimate_sha256=canonical_digest(payload),
    )


async def create_execution_price_snapshot(
    session: AsyncSession,
    *,
    provider: str,
    region: str,
    sku: str,
    source: str,
    source_version: str,
    source_uri: str,
    effective_at: datetime,
    observed_at: datetime,
    base_microusd_per_hour: int,
    vcpu_microusd_per_hour: int,
    memory_gib_microusd_per_hour: int,
    ephemeral_storage_gib_microusd_per_hour: int,
) -> tuple[ExecutionPriceSnapshot, bool]:
    provider = _clean_text(provider, name="provider", max_length=80)
    region = _clean_text(region, name="region", max_length=120)
    sku = _clean_text(sku, name="sku", max_length=120)
    source = _clean_text(source, name="source", max_length=120)
    source_version = _clean_text(source_version, name="source_version", max_length=160)
    source_uri = _clean_text(source_uri, name="source_uri", max_length=2048)
    if not source_uri.startswith("https://"):
        raise ValueError("source_uri must use https")
    effective_at = _utc(effective_at, name="effective_at")
    observed_at = _utc(observed_at, name="observed_at")
    rates = (
        base_microusd_per_hour,
        vcpu_microusd_per_hour,
        memory_gib_microusd_per_hour,
        ephemeral_storage_gib_microusd_per_hour,
    )
    if any(isinstance(value, bool) or value < 0 for value in rates) or sum(rates) <= 0:
        raise ValueError(
            "price rates must be non-negative integers with at least one positive rate"
        )
    payload = {
        "schema_version": "loom.execution-price-snapshot.v1",
        "provider": provider,
        "region": region,
        "sku": sku,
        "currency": "USD",
        "source": source,
        "source_version": source_version,
        "source_uri": source_uri,
        "effective_at": effective_at.isoformat(),
        "observed_at": observed_at.isoformat(),
        "base_microusd_per_hour": base_microusd_per_hour,
        "vcpu_microusd_per_hour": vcpu_microusd_per_hour,
        "memory_gib_microusd_per_hour": memory_gib_microusd_per_hour,
        "ephemeral_storage_gib_microusd_per_hour": (ephemeral_storage_gib_microusd_per_hour),
    }
    digest = canonical_digest(payload)
    await session.execute(_FINANCE_POLICY_LOCK)
    existing = (
        await session.execute(
            select(ExecutionPriceSnapshot).where(
                ExecutionPriceSnapshot.provider == provider,
                ExecutionPriceSnapshot.region == region,
                ExecutionPriceSnapshot.sku == sku,
                ExecutionPriceSnapshot.source == source,
                ExecutionPriceSnapshot.source_version == source_version,
            )
        )
    ).scalar_one_or_none()
    if existing is not None:
        if existing.rate_card_sha256 != digest:
            raise ValueError("price snapshot source version already has different contents")
        return existing, False
    row = ExecutionPriceSnapshot(
        provider=provider,
        region=region,
        sku=sku,
        currency="USD",
        source=source,
        source_version=source_version,
        source_uri=source_uri,
        effective_at=effective_at,
        observed_at=observed_at,
        base_microusd_per_hour=base_microusd_per_hour,
        vcpu_microusd_per_hour=vcpu_microusd_per_hour,
        memory_gib_microusd_per_hour=memory_gib_microusd_per_hour,
        ephemeral_storage_gib_microusd_per_hour=ephemeral_storage_gib_microusd_per_hour,
        rate_card_json=payload,
        rate_card_sha256=digest,
    )
    session.add(row)
    await session.flush()
    return row, True


async def upsert_target_price_binding(
    session: AsyncSession,
    *,
    target_id: str,
    price_snapshot_id: UUID,
    enabled: bool,
    reason: str | None,
    now: datetime | None = None,
) -> ExecutionTargetPriceBinding:
    target_id = _clean_text(target_id, name="target_id", max_length=120)
    clean_reason = reason.strip() if reason is not None else None
    if clean_reason == "":
        clean_reason = None
    if clean_reason is not None and len(clean_reason) > 500:
        raise ValueError("reason must contain at most 500 characters")
    await session.execute(_FINANCE_POLICY_LOCK)
    target = await session.get(ServiceExecutionTarget, target_id)
    price = await session.get(ExecutionPriceSnapshot, price_snapshot_id)
    if target is None:
        raise ValueError("execution target does not exist")
    if price is None:
        raise ValueError("execution price snapshot does not exist")
    if price.provider != target.provider or price.region != target.region:
        raise ValueError("price snapshot provider/region does not match target")
    row = await session.get(ExecutionTargetPriceBinding, target_id, with_for_update=True)
    current_time = _utc(now or datetime.now(UTC), name="now")
    if row is None:
        row = ExecutionTargetPriceBinding(
            target_id=target_id,
            price_snapshot_id=price_snapshot_id,
            enabled=enabled,
            reason=clean_reason,
            updated_at=current_time,
        )
        session.add(row)
    else:
        row.price_snapshot_id = price_snapshot_id
        row.enabled = enabled
        row.reason = clean_reason
        row.version += 1
        row.updated_at = current_time
    await session.flush()
    return row


async def _reconciled_budget_counters(
    session: AsyncSession,
    *,
    policy_id: UUID,
    scope_kind: str,
    scope_key: str,
    day: date,
    month: date,
) -> tuple[int, int, int, int]:
    daily_reserved, monthly_reserved = (
        await session.execute(
            select(
                func.coalesce(
                    func.sum(ExecutionCostReservationDebit.reserved_microusd).filter(
                        ExecutionCostReservationDebit.state == "active",
                        ExecutionCostReservationDebit.budget_day == day,
                    ),
                    0,
                ),
                func.coalesce(
                    func.sum(ExecutionCostReservationDebit.reserved_microusd).filter(
                        ExecutionCostReservationDebit.state == "active",
                        ExecutionCostReservationDebit.budget_month == month,
                    ),
                    0,
                ),
            ).where(ExecutionCostReservationDebit.policy_id == policy_id)
        )
    ).one()
    node_scope = (
        ExecutionNodeCostRecord.target_id == scope_key
        if scope_kind == "target"
        else ExecutionNodeCostRecord.target_id.in_(
            select(ServiceExecutionTarget.id).where(
                ServiceExecutionTarget.logical_pool_id == scope_key
            )
        )
    )
    daily_settled, monthly_settled = (
        await session.execute(
            select(
                func.coalesce(
                    func.sum(ExecutionNodeCostRecord.provider_billed_microusd).filter(
                        func.date(func.timezone("UTC", ExecutionNodeCostRecord.interval_started_at))
                        == day
                    ),
                    0,
                ),
                func.coalesce(
                    func.sum(ExecutionNodeCostRecord.provider_billed_microusd).filter(
                        func.date_trunc(
                            "month",
                            func.timezone("UTC", ExecutionNodeCostRecord.interval_started_at),
                        )
                        == datetime(month.year, month.month, 1)
                    ),
                    0,
                ),
            ).where(node_scope)
        )
    ).one()
    return tuple(
        int(value) for value in (daily_reserved, daily_settled, monthly_reserved, monthly_settled)
    )  # type: ignore[return-value]


async def upsert_execution_budget_policy(
    session: AsyncSession,
    *,
    scope_kind: Literal["pool", "target"],
    scope_key: str,
    daily_limit_microusd: int,
    monthly_limit_microusd: int,
    per_attempt_limit_microusd: int,
    max_estimate_duration_seconds: int,
    emergency_stop: bool,
    enabled: bool,
    reason: str | None,
    now: datetime | None = None,
) -> ExecutionBudgetPolicy:
    if scope_kind not in {"pool", "target"}:
        raise ValueError("scope_kind must be pool or target")
    scope_key = _clean_text(scope_key, name="scope_key", max_length=120)
    if any(
        isinstance(value, bool) or value <= 0
        for value in (
            daily_limit_microusd,
            monthly_limit_microusd,
            per_attempt_limit_microusd,
            max_estimate_duration_seconds,
        )
    ):
        raise ValueError("budget limits and maximum duration must be positive integers")
    if monthly_limit_microusd < daily_limit_microusd:
        raise ValueError("monthly budget limit must be at least the daily limit")
    if per_attempt_limit_microusd > daily_limit_microusd:
        raise ValueError("per-attempt limit cannot exceed the daily limit")
    if max_estimate_duration_seconds > 604_800:
        raise ValueError("maximum estimate duration cannot exceed 604800 seconds")
    clean_reason = reason.strip() if reason is not None else None
    if clean_reason == "":
        clean_reason = None
    if clean_reason is not None and len(clean_reason) > 500:
        raise ValueError("reason must contain at most 500 characters")
    await session.execute(_FINANCE_POLICY_LOCK)
    if scope_kind == "target":
        if await session.get(ServiceExecutionTarget, scope_key) is None:
            raise ValueError("execution target does not exist")
    else:
        pool_exists = (
            await session.execute(
                select(ServiceExecutionTarget.id)
                .where(ServiceExecutionTarget.logical_pool_id == scope_key)
                .limit(1)
            )
        ).scalar_one_or_none()
        if pool_exists is None:
            raise ValueError("execution pool does not have a registered target")
    row = (
        await session.execute(
            select(ExecutionBudgetPolicy)
            .where(
                ExecutionBudgetPolicy.scope_kind == scope_kind,
                ExecutionBudgetPolicy.scope_key == scope_key,
            )
            .with_for_update()
        )
    ).scalar_one_or_none()
    is_new = row is None
    current_time = _utc(now or datetime.now(UTC), name="now")
    day, month = _periods(current_time)
    if row is None:
        row = ExecutionBudgetPolicy(
            scope_kind=scope_kind,
            scope_key=scope_key,
            daily_limit_microusd=daily_limit_microusd,
            monthly_limit_microusd=monthly_limit_microusd,
            per_attempt_limit_microusd=per_attempt_limit_microusd,
            max_estimate_duration_seconds=max_estimate_duration_seconds,
            emergency_stop=emergency_stop,
            enabled=enabled,
            reason=clean_reason,
            current_day=day,
            current_month=month,
            updated_at=current_time,
        )
        session.add(row)
        await session.flush()
    counters = await _reconciled_budget_counters(
        session,
        policy_id=row.id,
        scope_kind=scope_kind,
        scope_key=scope_key,
        day=day,
        month=month,
    )
    daily_reserved, daily_settled, monthly_reserved, monthly_settled = counters
    if enabled and daily_reserved + daily_settled > daily_limit_microusd:
        raise ValueError("daily limit is below current reserved and provider-billed spend")
    if enabled and monthly_reserved + monthly_settled > monthly_limit_microusd:
        raise ValueError("monthly limit is below current reserved and provider-billed spend")
    row.daily_limit_microusd = daily_limit_microusd
    row.monthly_limit_microusd = monthly_limit_microusd
    row.per_attempt_limit_microusd = per_attempt_limit_microusd
    row.max_estimate_duration_seconds = max_estimate_duration_seconds
    row.emergency_stop = emergency_stop
    row.enabled = enabled
    row.reason = clean_reason
    row.current_day = day
    row.current_month = month
    row.daily_reserved_microusd = daily_reserved
    row.daily_settled_microusd = daily_settled
    row.monthly_reserved_microusd = monthly_reserved
    row.monthly_settled_microusd = monthly_settled
    if not is_new:
        row.version += 1
    row.updated_at = current_time
    await session.flush()
    return row


def _roll_budget_period(policy: ExecutionBudgetPolicy, *, day: date, month: date) -> None:
    if policy.current_day != day:
        policy.current_day = day
        policy.daily_reserved_microusd = 0
        policy.daily_settled_microusd = 0
    if policy.current_month != month:
        policy.current_month = month
        policy.monthly_reserved_microusd = 0
        policy.monthly_settled_microusd = 0


async def reserve_execution_cost(
    session: AsyncSession,
    *,
    lease: ServiceExecutionLease,
    trial: Trial,
    target: ServiceExecutionTarget,
    runtime_plan: ExecutionRuntimePlanV1,
    deadline_at: datetime,
    now: datetime | None = None,
) -> ExecutionCostReservation | None:
    """Reserve worst-case spend before a paid Nebius command is admitted."""

    if target.provider != "nebius":
        return None
    current_time = _utc(now or datetime.now(UTC), name="now")
    await session.execute(_FINANCE_POLICY_SHARED_LOCK)
    binding = await session.get(ExecutionTargetPriceBinding, target.id)
    if binding is None or not binding.enabled:
        raise ExecutionFinanceBlockedError("execution_cost_price_binding_unavailable")
    price = await session.get(ExecutionPriceSnapshot, binding.price_snapshot_id)
    if price is None or price.provider != target.provider or price.region != target.region:
        raise ExecutionFinanceBlockedError("execution_cost_price_snapshot_mismatch")
    if price.effective_at > current_time:
        raise ExecutionFinanceBlockedError("execution_cost_price_snapshot_not_effective")
    estimate = estimate_execution_cost(
        runtime_plan,
        price,
        acquired_at=current_time,
        deadline_at=deadline_at,
    )
    policies = (
        (
            await session.execute(
                select(ExecutionBudgetPolicy)
                .where(
                    ExecutionBudgetPolicy.enabled.is_(True),
                    or_(
                        (
                            (ExecutionBudgetPolicy.scope_kind == "pool")
                            & (ExecutionBudgetPolicy.scope_key == target.logical_pool_id)
                        ),
                        (
                            (ExecutionBudgetPolicy.scope_kind == "target")
                            & (ExecutionBudgetPolicy.scope_key == target.id)
                        ),
                    ),
                )
                .order_by(ExecutionBudgetPolicy.scope_kind, ExecutionBudgetPolicy.scope_key)
                .with_for_update()
            )
        )
        .scalars()
        .all()
    )
    # Budget enforcement is opt-in. Price attribution remains mandatory for
    # Nebius, but an absent pool/target policy is not an implicit spend cap.
    # Every enabled policy that does exist is still enforced below.
    day, month = _periods(current_time)
    monthly_estimates: dict[date, int] = {}
    for budget_day, amount in estimate.daily_costs:
        budget_month = budget_day.replace(day=1)
        monthly_estimates[budget_month] = monthly_estimates.get(budget_month, 0) + amount
    for policy in policies:
        current_counters = await _reconciled_budget_counters(
            session,
            policy_id=policy.id,
            scope_kind=policy.scope_kind,
            scope_key=policy.scope_key,
            day=day,
            month=month,
        )
        _roll_budget_period(policy, day=day, month=month)
        (
            policy.daily_reserved_microusd,
            policy.daily_settled_microusd,
            policy.monthly_reserved_microusd,
            policy.monthly_settled_microusd,
        ) = current_counters
        if policy.emergency_stop:
            raise ExecutionFinanceBlockedError(
                f"execution_budget_{policy.scope_kind}_emergency_stop"
            )
        if estimate.duration_seconds > policy.max_estimate_duration_seconds:
            raise ExecutionFinanceBlockedError(
                f"execution_budget_{policy.scope_kind}_duration_exceeded"
            )
        if estimate.estimated_cost_microusd > policy.per_attempt_limit_microusd:
            raise ExecutionFinanceBlockedError(
                f"execution_budget_{policy.scope_kind}_attempt_limit_exceeded"
            )
        for budget_day, amount in estimate.daily_costs:
            budget_month = budget_day.replace(day=1)
            daily_reserved, daily_settled, _, _ = await _reconciled_budget_counters(
                session,
                policy_id=policy.id,
                scope_kind=policy.scope_kind,
                scope_key=policy.scope_key,
                day=budget_day,
                month=budget_month,
            )
            if daily_reserved + daily_settled + amount > policy.daily_limit_microusd:
                raise ExecutionFinanceBlockedError(
                    f"execution_budget_{policy.scope_kind}_daily_limit_exceeded"
                )
        for budget_month, amount in monthly_estimates.items():
            _, _, monthly_reserved, monthly_settled = await _reconciled_budget_counters(
                session,
                policy_id=policy.id,
                scope_kind=policy.scope_kind,
                scope_key=policy.scope_key,
                day=budget_month,
                month=budget_month,
            )
            if monthly_reserved + monthly_settled + amount > policy.monthly_limit_microusd:
                raise ExecutionFinanceBlockedError(
                    f"execution_budget_{policy.scope_kind}_monthly_limit_exceeded"
                )
    reservation = ExecutionCostReservation(
        id=uuid4(),
        lease_id=lease.id,
        trial_id=trial.id,
        team_id=trial.team_id,
        batch_id=trial.batch_id,
        attempt=lease.attempt,
        execution_role=lease.execution_role,
        verifier_retry=lease.verifier_retry,
        pool_id=target.logical_pool_id,
        target_id=target.id,
        price_snapshot_id=price.id,
        estimate_duration_seconds=estimate.duration_seconds,
        requested_cpu_millis=estimate.requested_cpu_millis,
        requested_memory_mib=estimate.requested_memory_mib,
        requested_ephemeral_storage_mib=estimate.requested_ephemeral_storage_mib,
        estimated_cost_microusd=estimate.estimated_cost_microusd,
        estimate_sha256=estimate.estimate_sha256,
        state="reserved",
        acquired_at=current_time,
    )
    session.add(reservation)
    await session.flush()
    for policy in policies:
        for budget_day, amount in estimate.daily_costs:
            budget_month = budget_day.replace(day=1)
            session.add(
                ExecutionCostReservationDebit(
                    reservation_id=reservation.id,
                    policy_id=policy.id,
                    budget_day=budget_day,
                    budget_month=budget_month,
                    reserved_microusd=amount,
                    state="active",
                )
            )
            if budget_day == day:
                policy.daily_reserved_microusd += amount
            if budget_month == month:
                policy.monthly_reserved_microusd += amount
        policy.updated_at = current_time
    await session.flush()
    return reservation


def _node_digest(*values: str) -> str:
    scoped_identity = "\0".join(values)
    return "sha256:" + hashlib.sha256(scoped_identity.encode("utf-8")).hexdigest()


def _normalized_allocations(raw: list[int], total_bill: int) -> list[int]:
    raw_total = sum(raw)
    if raw_total <= total_bill:
        return raw
    if raw_total == 0:
        return [0 for _ in raw]
    values = [total_bill * value // raw_total for value in raw]
    remainder = total_bill - sum(values)
    for index in sorted(
        range(len(raw)),
        key=lambda item: ((total_bill * raw[item]) % raw_total, -item),
        reverse=True,
    )[:remainder]:
        values[index] += 1
    return values


async def record_execution_node_cost(
    session: AsyncSession,
    *,
    target_id: str,
    price_snapshot_id: UUID,
    provider_record_id: str,
    node_name: str,
    interval_started_at: datetime,
    interval_stopped_at: datetime,
    node_cpu_millis: int,
    node_memory_mib: int,
    node_ephemeral_storage_mib: int,
    provider_billed_microusd: int,
    billing_source: str,
    billing_source_version: str,
    observed_at: datetime,
) -> tuple[ExecutionNodeCostRecord, bool]:
    """Persist one provider bill and deterministically allocate its node cost."""

    target_id = _clean_text(target_id, name="target_id", max_length=120)
    provider_record_id = _clean_text(provider_record_id, name="provider_record_id", max_length=240)
    node_name = _clean_text(node_name, name="node_name", max_length=253)
    billing_source = _clean_text(billing_source, name="billing_source", max_length=120)
    billing_source_version = _clean_text(
        billing_source_version, name="billing_source_version", max_length=160
    )
    start = _utc(interval_started_at, name="interval_started_at")
    stop = _utc(interval_stopped_at, name="interval_stopped_at")
    observed = _utc(observed_at, name="observed_at")
    if stop <= start or observed < stop:
        raise ValueError("node cost interval/observation timestamps are invalid")
    if start.date() != (stop - timedelta(microseconds=1)).date():
        raise ValueError("node cost intervals must be split at UTC day boundaries")
    if any(
        isinstance(value, bool) or value <= 0
        for value in (node_cpu_millis, node_memory_mib, node_ephemeral_storage_mib)
    ):
        raise ValueError("node resource capacities must be positive integers")
    if isinstance(provider_billed_microusd, bool) or provider_billed_microusd < 0:
        raise ValueError("provider_billed_microusd must be a non-negative integer")
    await session.execute(_FINANCE_POLICY_SHARED_LOCK)
    await session.execute(_BILLING_MUTATION_LOCK)
    target = await session.get(ServiceExecutionTarget, target_id)
    price = await session.get(ExecutionPriceSnapshot, price_snapshot_id)
    if target is None or price is None:
        raise ValueError("execution target or price snapshot does not exist")
    if target.provider != price.provider or target.region != price.region:
        raise ValueError("node cost target and price snapshot do not match")
    if price.effective_at > start:
        raise ValueError("node cost price snapshot was not effective for the billed interval")
    node_hash = _node_digest(target.provider, target.region, target_id, node_name)
    evidence = {
        "schema_version": "loom.execution-node-cost.v1",
        "target_id": target_id,
        "price_snapshot_id": str(price_snapshot_id),
        "provider": target.provider,
        "provider_record_id": provider_record_id,
        "node_identity_sha256": node_hash,
        "interval_started_at": start.isoformat(),
        "interval_stopped_at": stop.isoformat(),
        "node_cpu_millis": node_cpu_millis,
        "node_memory_mib": node_memory_mib,
        "node_ephemeral_storage_mib": node_ephemeral_storage_mib,
        "provider_billed_microusd": provider_billed_microusd,
        "currency": "USD",
        "billing_source": billing_source,
        "billing_source_version": billing_source_version,
        "allocation_method": "dominant_requested_resource_time_v1",
        "observed_at": observed.isoformat(),
    }
    evidence_sha256 = canonical_digest(evidence)
    existing = (
        await session.execute(
            select(ExecutionNodeCostRecord).where(
                ExecutionNodeCostRecord.provider == target.provider,
                ExecutionNodeCostRecord.provider_record_id == provider_record_id,
            )
        )
    ).scalar_one_or_none()
    if existing is not None:
        if existing.evidence_sha256 != evidence_sha256:
            raise ValueError("provider cost record id already has different evidence")
        return existing, False
    overlap = (
        await session.execute(
            select(ExecutionNodeCostRecord.id).where(
                ExecutionNodeCostRecord.provider == target.provider,
                ExecutionNodeCostRecord.node_identity_sha256 == node_hash,
                ExecutionNodeCostRecord.interval_started_at < stop,
                ExecutionNodeCostRecord.interval_stopped_at > start,
            )
        )
    ).scalar_one_or_none()
    if overlap is not None:
        raise ValueError("node cost interval overlaps existing provider evidence")
    incomplete_lease = (
        await session.execute(
            select(ServiceExecutionLease.id)
            .where(
                ServiceExecutionLease.target_id == target_id,
                ServiceExecutionLease.node_name == node_name,
                ServiceExecutionLease.pod_started_at.is_not(None),
                ServiceExecutionLease.pod_started_at < stop,
                ServiceExecutionLease.pod_terminated_at.is_(None),
            )
            .limit(1)
        )
    ).scalar_one_or_none()
    if incomplete_lease is not None:
        raise ValueError(
            "node cost interval overlaps an execution lease without a persisted "
            "termination timestamp"
        )
    candidates = (
        await session.execute(
            select(ExecutionCostReservation, ServiceExecutionLease)
            .join(
                ServiceExecutionLease, ServiceExecutionLease.id == ExecutionCostReservation.lease_id
            )
            .where(
                ExecutionCostReservation.target_id == target_id,
                ExecutionCostReservation.state.in_(("reserved", "awaiting_settlement")),
                ServiceExecutionLease.node_name == node_name,
                ServiceExecutionLease.pod_started_at.is_not(None),
                ServiceExecutionLease.pod_terminated_at.is_not(None),
                ServiceExecutionLease.pod_started_at < stop,
                ServiceExecutionLease.pod_terminated_at > start,
            )
            .order_by(ExecutionCostReservation.id)
        )
    ).all()
    interval_seconds = max(1, math.ceil((stop - start).total_seconds()))
    allocation_inputs: list[tuple[ExecutionCostReservation, ServiceExecutionLease, int, int]] = []
    raw_allocations: list[int] = []
    for reservation, lease in candidates:
        assert lease.pod_started_at is not None
        assert lease.pod_terminated_at is not None
        overlap_start = max(start, lease.pod_started_at)
        overlap_stop = min(stop, lease.pod_terminated_at)
        overlap_seconds = max(0, math.ceil((overlap_stop - overlap_start).total_seconds()))
        if overlap_seconds <= 0:
            continue
        fraction_ppb = min(
            1_000_000_000,
            max(
                math.ceil(reservation.requested_cpu_millis * 1_000_000_000 / node_cpu_millis),
                math.ceil(reservation.requested_memory_mib * 1_000_000_000 / node_memory_mib),
                math.ceil(
                    reservation.requested_ephemeral_storage_mib
                    * 1_000_000_000
                    / node_ephemeral_storage_mib
                ),
            ),
        )
        raw_cost = (provider_billed_microusd * overlap_seconds * fraction_ppb) // (
            interval_seconds * 1_000_000_000
        )
        allocation_inputs.append((reservation, lease, overlap_seconds, fraction_ppb))
        raw_allocations.append(raw_cost)
    allocations = _normalized_allocations(raw_allocations, provider_billed_microusd)
    allocated_total = sum(allocations)
    row = ExecutionNodeCostRecord(
        id=uuid4(),
        target_id=target_id,
        price_snapshot_id=price_snapshot_id,
        provider=target.provider,
        provider_record_id=provider_record_id,
        node_identity_sha256=node_hash,
        interval_started_at=start,
        interval_stopped_at=stop,
        node_cpu_millis=node_cpu_millis,
        node_memory_mib=node_memory_mib,
        node_ephemeral_storage_mib=node_ephemeral_storage_mib,
        provider_billed_microusd=provider_billed_microusd,
        allocated_microusd=allocated_total,
        idle_system_fragmentation_microusd=(provider_billed_microusd - allocated_total),
        currency="USD",
        billing_source=billing_source,
        billing_source_version=billing_source_version,
        allocation_method="dominant_requested_resource_time_v1",
        evidence_sha256=evidence_sha256,
        observed_at=observed,
    )
    session.add(row)
    await session.flush()
    for (reservation, lease, overlap_seconds, fraction_ppb), amount in zip(
        allocation_inputs, allocations, strict=True
    ):
        session.add(
            ExecutionNodeCostAllocation(
                node_cost_record_id=row.id,
                cost_reservation_id=reservation.id,
                lease_id=lease.id,
                overlap_seconds=overlap_seconds,
                dominant_resource_fraction_ppb=fraction_ppb,
                allocated_microusd=amount,
            )
        )
    policies = (
        (
            await session.execute(
                select(ExecutionBudgetPolicy)
                .where(
                    or_(
                        (
                            (ExecutionBudgetPolicy.scope_kind == "pool")
                            & (ExecutionBudgetPolicy.scope_key == target.logical_pool_id)
                        ),
                        (
                            (ExecutionBudgetPolicy.scope_kind == "target")
                            & (ExecutionBudgetPolicy.scope_key == target.id)
                        ),
                    )
                )
                .order_by(ExecutionBudgetPolicy.scope_kind, ExecutionBudgetPolicy.scope_key)
                .with_for_update()
            )
        )
        .scalars()
        .all()
    )
    current_day, current_month = _periods(observed)
    for policy in policies:
        counters = await _reconciled_budget_counters(
            session,
            policy_id=policy.id,
            scope_kind=policy.scope_kind,
            scope_key=policy.scope_key,
            day=current_day,
            month=current_month,
        )
        _roll_budget_period(policy, day=current_day, month=current_month)
        (
            policy.daily_reserved_microusd,
            policy.daily_settled_microusd,
            policy.monthly_reserved_microusd,
            policy.monthly_settled_microusd,
        ) = counters
        policy.updated_at = observed
    await session.flush()
    return row, True


async def settle_execution_cost_reservation(
    session: AsyncSession,
    *,
    reservation_id: UUID,
    billing_complete_through: datetime,
    now: datetime | None = None,
) -> ExecutionCostReservation:
    """Replace a terminal attempt's estimate with provider-bill allocations."""

    complete_through = _utc(billing_complete_through, name="billing_complete_through")
    current_time = _utc(now or datetime.now(UTC), name="now")
    await session.execute(_FINANCE_POLICY_SHARED_LOCK)
    await session.execute(_BILLING_MUTATION_LOCK)
    reservation = await session.get(ExecutionCostReservation, reservation_id, with_for_update=True)
    if reservation is None:
        raise ValueError("execution cost reservation does not exist")
    if reservation.state == "settled":
        return reservation
    if reservation.state != "awaiting_settlement":
        raise ValueError("execution cost reservation is not awaiting settlement")
    lease = await session.get(ServiceExecutionLease, reservation.lease_id)
    if lease is None or lease.pod_started_at is None or lease.pod_terminated_at is None:
        raise ValueError("execution lease lacks a complete provider runtime interval")
    if complete_through < lease.pod_terminated_at:
        raise ValueError("billing evidence does not cover the complete provider runtime")
    allocation_rows = (
        await session.execute(
            select(ExecutionNodeCostAllocation, ExecutionNodeCostRecord)
            .join(
                ExecutionNodeCostRecord,
                ExecutionNodeCostRecord.id == ExecutionNodeCostAllocation.node_cost_record_id,
            )
            .where(ExecutionNodeCostAllocation.cost_reservation_id == reservation.id)
            .order_by(ExecutionNodeCostRecord.interval_started_at)
        )
    ).all()
    cursor = lease.pod_started_at
    actual_allocated = 0
    actual_by_day: dict[date, int] = {}
    latest_stop: datetime | None = None
    for allocation, node_record in allocation_rows:
        clipped_start = max(lease.pod_started_at, node_record.interval_started_at)
        clipped_stop = min(lease.pod_terminated_at, node_record.interval_stopped_at)
        if clipped_stop <= clipped_start:
            continue
        if clipped_start > cursor:
            raise ValueError("provider billing evidence has a gap in the lease runtime")
        cursor = max(cursor, clipped_stop)
        latest_stop = max(latest_stop or clipped_stop, clipped_stop)
        actual_allocated += allocation.allocated_microusd
        bill_day = node_record.interval_started_at.astimezone(UTC).date()
        actual_by_day[bill_day] = actual_by_day.get(bill_day, 0) + allocation.allocated_microusd
    if cursor < lease.pod_terminated_at or latest_stop is None:
        raise ValueError("provider billing evidence does not cover the lease runtime")
    if complete_through > latest_stop:
        raise ValueError("billing_complete_through exceeds persisted provider evidence")
    debits = (
        (
            await session.execute(
                select(ExecutionCostReservationDebit)
                .where(ExecutionCostReservationDebit.reservation_id == reservation.id)
                .order_by(ExecutionCostReservationDebit.policy_id)
                .with_for_update()
            )
        )
        .scalars()
        .all()
    )
    policies = {
        policy.id: policy
        for policy in (
            (
                await session.execute(
                    select(ExecutionBudgetPolicy)
                    .where(ExecutionBudgetPolicy.id.in_([debit.policy_id for debit in debits]))
                    .order_by(ExecutionBudgetPolicy.scope_kind, ExecutionBudgetPolicy.scope_key)
                    .with_for_update()
                )
            )
            .scalars()
            .all()
        )
    }
    for debit in debits:
        if debit.state != "active":
            raise ValueError("execution cost reservation debit is not active")
        policy = policies[debit.policy_id]
        if policy.current_day == debit.budget_day:
            policy.daily_reserved_microusd = max(
                0, policy.daily_reserved_microusd - debit.reserved_microusd
            )
        if policy.current_month == debit.budget_month:
            policy.monthly_reserved_microusd = max(
                0, policy.monthly_reserved_microusd - debit.reserved_microusd
            )
        policy.updated_at = current_time
        debit.state = "settled"
        debit.actual_microusd = actual_by_day.get(debit.budget_day, 0)
        debit.updated_at = current_time
    reservation.state = "settled"
    reservation.settled_at = current_time
    reservation.billing_complete_through = complete_through
    reservation.actual_allocated_microusd = actual_allocated
    await session.flush()
    return reservation


async def fetch_execution_finance_status(
    session: AsyncSession,
    *,
    pool_id: str | None = None,
) -> dict[str, object]:
    price_rows = (
        (
            await session.execute(
                select(ExecutionPriceSnapshot).order_by(
                    ExecutionPriceSnapshot.provider,
                    ExecutionPriceSnapshot.region,
                    ExecutionPriceSnapshot.sku,
                    ExecutionPriceSnapshot.effective_at,
                )
            )
        )
        .scalars()
        .all()
    )
    binding_stmt = select(ExecutionTargetPriceBinding, ServiceExecutionTarget).join(
        ServiceExecutionTarget,
        ServiceExecutionTarget.id == ExecutionTargetPriceBinding.target_id,
    )
    policy_stmt = select(ExecutionBudgetPolicy)
    reservation_stmt = select(ExecutionCostReservation)
    node_stmt = select(ExecutionNodeCostRecord)
    if pool_id is not None:
        binding_stmt = binding_stmt.where(ServiceExecutionTarget.logical_pool_id == pool_id)
        policy_stmt = policy_stmt.where(
            or_(
                (
                    (ExecutionBudgetPolicy.scope_kind == "pool")
                    & (ExecutionBudgetPolicy.scope_key == pool_id)
                ),
                (
                    (ExecutionBudgetPolicy.scope_kind == "target")
                    & (
                        ExecutionBudgetPolicy.scope_key.in_(
                            select(ServiceExecutionTarget.id).where(
                                ServiceExecutionTarget.logical_pool_id == pool_id
                            )
                        )
                    )
                ),
            )
        )
        reservation_stmt = reservation_stmt.where(ExecutionCostReservation.pool_id == pool_id)
        node_stmt = node_stmt.where(
            ExecutionNodeCostRecord.target_id.in_(
                select(ServiceExecutionTarget.id).where(
                    ServiceExecutionTarget.logical_pool_id == pool_id
                )
            )
        )
    binding_rows = (
        await session.execute(binding_stmt.order_by(ExecutionTargetPriceBinding.target_id))
    ).all()
    policies = (
        (
            await session.execute(
                policy_stmt.order_by(
                    ExecutionBudgetPolicy.scope_kind, ExecutionBudgetPolicy.scope_key
                )
            )
        )
        .scalars()
        .all()
    )
    reservations = (
        (
            await session.execute(
                reservation_stmt.order_by(ExecutionCostReservation.acquired_at.desc()).limit(100)
            )
        )
        .scalars()
        .all()
    )
    node_records = (
        (
            await session.execute(
                node_stmt.order_by(ExecutionNodeCostRecord.interval_started_at.desc()).limit(100)
            )
        )
        .scalars()
        .all()
    )
    current_time = datetime.now(UTC)
    current_day, current_month = _periods(current_time)
    policy_payloads: list[dict[str, object]] = []
    for row in policies:
        counters = await _reconciled_budget_counters(
            session,
            policy_id=row.id,
            scope_kind=row.scope_kind,
            scope_key=row.scope_key,
            day=current_day,
            month=current_month,
        )
        daily_reserved, daily_settled, monthly_reserved, monthly_settled = counters
        counter_in_sync = (
            row.current_day == current_day
            and row.current_month == current_month
            and row.daily_reserved_microusd == daily_reserved
            and row.daily_settled_microusd == daily_settled
            and row.monthly_reserved_microusd == monthly_reserved
            and row.monthly_settled_microusd == monthly_settled
        )
        policy_payloads.append(
            {
                "id": str(row.id),
                "scope_kind": row.scope_kind,
                "scope_key": row.scope_key,
                "enabled": row.enabled,
                "emergency_stop": row.emergency_stop,
                "daily_limit_microusd": row.daily_limit_microusd,
                "daily_reserved_microusd": daily_reserved,
                "daily_settled_microusd": daily_settled,
                "monthly_limit_microusd": row.monthly_limit_microusd,
                "monthly_reserved_microusd": monthly_reserved,
                "monthly_settled_microusd": monthly_settled,
                "per_attempt_limit_microusd": row.per_attempt_limit_microusd,
                "max_estimate_duration_seconds": row.max_estimate_duration_seconds,
                "current_day": current_day.isoformat(),
                "current_month": current_month.isoformat(),
                "persisted_current_day": (row.current_day.isoformat() if row.current_day else None),
                "persisted_current_month": (
                    row.current_month.isoformat() if row.current_month else None
                ),
                "persisted_daily_reserved_microusd": row.daily_reserved_microusd,
                "persisted_daily_settled_microusd": row.daily_settled_microusd,
                "persisted_monthly_reserved_microusd": row.monthly_reserved_microusd,
                "persisted_monthly_settled_microusd": row.monthly_settled_microusd,
                "counter_in_sync": counter_in_sync,
                "reason": row.reason,
                "version": row.version,
            }
        )
    return {
        "price_snapshots": [
            {
                "id": str(row.id),
                "provider": row.provider,
                "region": row.region,
                "sku": row.sku,
                "currency": row.currency,
                "source": row.source,
                "source_version": row.source_version,
                "source_uri": row.source_uri,
                "effective_at": row.effective_at.isoformat(),
                "observed_at": row.observed_at.isoformat(),
                "rate_card_sha256": row.rate_card_sha256,
            }
            for row in price_rows
        ],
        "target_bindings": [
            {
                "target_id": binding.target_id,
                "pool_id": target.logical_pool_id,
                "price_snapshot_id": str(binding.price_snapshot_id),
                "enabled": binding.enabled,
                "reason": binding.reason,
                "version": binding.version,
            }
            for binding, target in binding_rows
        ],
        "budget_policies": policy_payloads,
        "cost_reservations": [
            {
                "id": str(row.id),
                "lease_id": str(row.lease_id),
                "trial_id": str(row.trial_id),
                "team_id": str(row.team_id),
                "pool_id": row.pool_id,
                "target_id": row.target_id,
                "price_snapshot_id": str(row.price_snapshot_id),
                "state": row.state,
                "estimated_cost_microusd": row.estimated_cost_microusd,
                "actual_allocated_microusd": row.actual_allocated_microusd,
                "estimate_sha256": row.estimate_sha256,
                "acquired_at": row.acquired_at.isoformat(),
                "terminal_at": row.terminal_at.isoformat() if row.terminal_at else None,
                "settled_at": row.settled_at.isoformat() if row.settled_at else None,
            }
            for row in reservations
        ],
        "node_cost_records": [
            {
                "id": str(row.id),
                "target_id": row.target_id,
                "provider": row.provider,
                "provider_record_id": row.provider_record_id,
                "node_identity_sha256": row.node_identity_sha256,
                "price_snapshot_id": str(row.price_snapshot_id),
                "provider_billed_microusd": row.provider_billed_microusd,
                "allocated_microusd": row.allocated_microusd,
                "idle_system_fragmentation_microusd": (row.idle_system_fragmentation_microusd),
                "billing_source": row.billing_source,
                "billing_source_version": row.billing_source_version,
                "interval_started_at": row.interval_started_at.isoformat(),
                "interval_stopped_at": row.interval_stopped_at.isoformat(),
                "evidence_sha256": row.evidence_sha256,
            }
            for row in node_records
        ],
    }


__all__ = [
    "ExecutionCostEstimate",
    "ExecutionFinanceBlockedError",
    "create_execution_price_snapshot",
    "estimate_execution_cost",
    "fetch_execution_finance_status",
    "record_execution_node_cost",
    "reserve_execution_cost",
    "settle_execution_cost_reservation",
    "upsert_execution_budget_policy",
    "upsert_target_price_binding",
]
