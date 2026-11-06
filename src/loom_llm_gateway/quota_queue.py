"""Durable FIFO admission for one opted-in provider request pool.

The caller already holds the pool row lock. This adds one advisory lock, in
that same order, so two gateway replicas share one line. The lock ends when
the admission transaction ends. Waiting happens in a later transaction.
"""

from __future__ import annotations

from datetime import timedelta
from uuid import UUID, uuid4

from fastapi import HTTPException
from sqlalchemy import func, literal, select, text, union_all, update
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import (
    GatewayDispatchQueue,
    ProviderRequestPoolGroupCap,
    ProviderRequestPoolMember,
)
from loom.quota_queue import QUEUE_WAITER_DEAD_AFTER, AdmissionChoice, GroupView, choose_admission
from loom_llm_gateway.dispatch_audit import PoolAdmissionBusyError
from loom_llm_gateway.dispatch_capacity import active_dispatch_count


async def decide_group_queue(
    session: AsyncSession,
    *,
    pool_id: UUID,
    request_id: UUID,
    bound_group: str,
    attempt: int,
    upstream_timeout_sec: float,
    members: list[ProviderRequestPoolMember],
) -> AdmissionChoice:
    """Choose admit, overflow, or bounded wait. Does not insert a receipt."""

    acquired = await session.scalar(
        text("SELECT pg_try_advisory_xact_lock(hashtext('loom-pool-queue'), hashtext(:pool_id))"),
        {"pool_id": str(pool_id)},
    )
    if not acquired:
        raise PoolAdmissionBusyError
    ordered = [member for member in members if member.enabled]
    groups: list[str] = []
    for member in ordered:
        if member.quota_group not in groups:
            groups.append(member.quota_group)
    caps = {
        row.quota_group: int(row.max_in_flight)
        for row in (
            await session.scalars(
                select(ProviderRequestPoolGroupCap).where(
                    ProviderRequestPoolGroupCap.pool_connection_id == pool_id,
                )
            )
        ).all()
    }
    if any(group not in caps for group in groups):
        raise HTTPException(status_code=503, detail="provider_queue_unconfigured")
    now = await session.scalar(select(func.clock_timestamp()))
    if now is None:
        raise HTTPException(status_code=503, detail="provider_queue_clock_unavailable")
    stale_after = now - timedelta(seconds=upstream_timeout_sec + 5.0)
    # Disabled members still occupy their group's quota while an admitted
    # generation is live. Each physical member uses two indexed lifetime ranges.
    member_counts = union_all(*(
        select(
            literal(member.quota_group).label("quota_group"),
            active_dispatch_count(member.connection_id, now=now, stale_after=stale_after)
            .label("in_flight"),
        )
        for member in members
    )).subquery()
    counts = {
        group: int(count or 0)
        for group, count in (
            await session.execute(
                select(member_counts.c.quota_group, func.sum(member_counts.c.in_flight))
                .group_by(member_counts.c.quota_group)
            )
        ).all()
    }
    waiters = list(
        (
            await session.scalars(
                select(GatewayDispatchQueue)
                .where(
                    GatewayDispatchQueue.pool_connection_id == pool_id,
                    GatewayDispatchQueue.closed_at.is_(None),
                    GatewayDispatchQueue.renewed_at >= now - QUEUE_WAITER_DEAD_AFTER,
                )
                .order_by(GatewayDispatchQueue.seq)
            )
        ).all()
    )
    oldest: dict[str, UUID] = {}
    for waiter in waiters:
        oldest.setdefault(waiter.bound_group, waiter.request_id)
    views = [
        GroupView(group, caps[group], counts.get(group, 0), oldest.get(group)) for group in groups
    ]
    # These counts already exclude settled and expired occupancy. A full group
    # is waiting on valid dispatches, even if no new call was admitted recently.
    # Last-admission age races legitimate long reads and is not proof of a stall.
    # Waiting is bounded by the request budget, signed deadline and cancellation.
    return choose_admission(
        bound_group=bound_group, groups=views, request_id=request_id,
        front=attempt > 1,
    )


async def touch_queue_row(
    session: AsyncSession,
    *,
    pool_id: UUID,
    request_id: UUID,
    trial_id: UUID | None,
    bound_group: str,
) -> None:
    """Renew one live waiter and expire replicas that stopped renewing."""

    await session.execute(
        update(GatewayDispatchQueue)
        .where(
            GatewayDispatchQueue.pool_connection_id == pool_id,
            GatewayDispatchQueue.closed_at.is_(None),
            GatewayDispatchQueue.renewed_at < func.clock_timestamp() - QUEUE_WAITER_DEAD_AFTER,
        )
        .values(closed_at=func.clock_timestamp(), outcome="expired")
    )
    existing = await session.scalar(
        select(GatewayDispatchQueue.id).where(
            GatewayDispatchQueue.pool_connection_id == pool_id,
            GatewayDispatchQueue.request_id == request_id,
            GatewayDispatchQueue.closed_at.is_(None),
        )
    )
    if existing is None:
        session.add(
            GatewayDispatchQueue(
                id=uuid4(),
                pool_connection_id=pool_id,
                bound_group=bound_group,
                request_id=request_id,
                trial_id=trial_id,
            )
        )
        return
    await session.execute(
        update(GatewayDispatchQueue)
        .where(GatewayDispatchQueue.id == existing)
        .values(renewed_at=func.clock_timestamp())
    )


async def close_queue_row(
    session: AsyncSession,
    *,
    request_id: UUID,
    outcome: str,
    group: str | None = None,
    connection_id: UUID | None = None,
) -> None:
    await session.execute(
        update(GatewayDispatchQueue)
        .where(
            GatewayDispatchQueue.request_id == request_id,
            GatewayDispatchQueue.closed_at.is_(None),
        )
        .values(
            closed_at=func.clock_timestamp(),
            outcome=outcome,
            admitted_group=group,
            admitted_connection_id=connection_id,
        )
    )
