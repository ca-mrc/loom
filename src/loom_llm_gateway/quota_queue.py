"""Durable FIFO admission for one opted-in provider request pool.

The caller already holds the pool row lock. This adds one advisory lock, in
that same order, so two gateway replicas share one line. The lock ends when
the admission transaction ends. Waiting happens in a later transaction.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import timedelta
from uuid import UUID, uuid4

from fastapi import HTTPException
from sqlalchemy import and_, func, or_, select, text, union_all, update
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import (
    GatewayDispatchQueue,
    GatewayDispatchReceipt,
    ProviderRequestPoolGroupCap,
    ProviderRequestPoolMember,
)
from loom.quota_queue import QUEUE_WAITER_DEAD_AFTER, AdmissionChoice, GroupView, choose_admission


async def decide_group_queue(
    session: AsyncSession,
    *,
    pool_id: UUID,
    request_id: UUID,
    bound_group: str,
    bound_connection_id: UUID,
    attempt: int,
    stall_sec: float,
    upstream_timeout_sec: float,
    members: list[ProviderRequestPoolMember],
) -> AdmissionChoice:
    """Choose admit, overflow, wait, or stall. Does not insert a receipt."""

    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtext('loom-pool-queue'), hashtext(:pool_id))"),
        {"pool_id": str(pool_id)},
    )
    ordered = [member for member in members if member.enabled]
    groups: list[str] = []
    members_by_group: dict[str, list[UUID]] = defaultdict(list)
    for member in ordered:
        members_by_group[member.quota_group].append(member.connection_id)
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
    counts = {
        group: int(count or 0)
        for group, count in (
            await session.execute(
                select(ProviderRequestPoolMember.quota_group, func.count())
                .join(
                    GatewayDispatchReceipt,
                    GatewayDispatchReceipt.provider_connection_id
                    == ProviderRequestPoolMember.connection_id,
                )
                .where(
                    ProviderRequestPoolMember.pool_connection_id == pool_id,
                    GatewayDispatchReceipt.provider_connection_id.in_(
                        [member.connection_id for member in members]
                    ),
                    GatewayDispatchReceipt.provider_outcome == "admitted",
                    or_(
                        GatewayDispatchReceipt.attempt_deadline_wall_clock > now,
                        and_(
                            GatewayDispatchReceipt.attempt_deadline_wall_clock.is_(None),
                            GatewayDispatchReceipt.admitted_at > stale_after,
                        ),
                    ),
                )
                .group_by(ProviderRequestPoolMember.quota_group)
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
    choice = choose_admission(
        bound_group=bound_group, groups=views, request_id=request_id,
        front=attempt > 1, idle_sec=0, stall_sec=stall_sec,
    )
    # Stall age matters only for a full group with no overflow. Free slots and
    # FIFO ordering never need historical receipts on their admission path.
    if choice.kind != "wait" or counts.get(bound_group, 0) < caps[bound_group]:
        return choice
    connection_ids = members_by_group.get(bound_group, [bound_connection_id])
    # One indexed extremum per physical connection. MAX with an IN predicate
    # can scan every completed receipt for a multi-key quota group, even when
    # (provider_connection_id, admitted_at) is indexed. The partial in-flight
    # index cannot answer this query: completed calls also reset stall age.
    latest_by_member = union_all(*(
        select(func.max(GatewayDispatchReceipt.admitted_at).label("latest_admission")).where(
            GatewayDispatchReceipt.provider_connection_id == connection_id,
        )
        for connection_id in connection_ids
    )).subquery()
    last_admitted = await session.scalar(
        select(func.max(latest_by_member.c.latest_admission))
    )
    if last_admitted is not None:
        idle_sec = max(0.0, (now - last_admitted).total_seconds())
    else:
        mine = next((row for row in waiters if row.request_id == request_id), None)
        idle_sec = max(0.0, (now - mine.enqueued_at).total_seconds()) if mine is not None else 0.0
    return choose_admission(
        bound_group=bound_group,
        groups=views,
        request_id=request_id,
        front=attempt > 1,
        idle_sec=idle_sec,
        stall_sec=stall_sec,
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
