"""Quota-group admission decisions and committed queue-time credit.

Credit is computed in Postgres from committed queue rows. Callers do not
subtract timestamps on their own clock. A failed read adds nothing.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Literal
from uuid import UUID

from sqlalchemy import Float, bindparam, text
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)
# A waiter that has not renewed within this interval is dead. Credit for an
# open row stops at the same instant, using the database clock in both places.
QUEUE_WAITER_DEAD_AFTER = timedelta(seconds=15)

AdmissionKind = Literal["admit", "overflow", "wait", "stall"]


@dataclass(frozen=True, slots=True)
class GroupView:
    """One quota group in pool member order."""

    name: str
    cap: int
    in_flight: int
    oldest_waiter: UUID | None


@dataclass(frozen=True, slots=True)
class AdmissionChoice:
    kind: AdmissionKind
    group: str | None = None


def choose_admission(
    *,
    bound_group: str,
    groups: Sequence[GroupView],
    request_id: UUID,
    front: bool,
    idle_sec: float,
    stall_sec: float,
) -> AdmissionChoice:
    """Pick a free slot without letting a new call pass a live waiter.

    Overflow names a later group that has room and nobody waiting. The trial
    binding is not an input and is not changed here. A full group that has
    admitted nobody for ``stall_sec`` stalls instead of waiting forever.
    """

    ordered = list(groups)
    bound = next((group for group in ordered if group.name == bound_group), None)
    if bound is None:
        raise KeyError(bound_group)
    if _can_take(bound, request_id, front=front):
        return AdmissionChoice("admit", bound.name)
    if bound.in_flight < bound.cap:
        return AdmissionChoice("wait", bound.name)
    seen_bound = False
    for group in ordered:
        if group.name == bound_group:
            seen_bound = True
            continue
        if not seen_bound or group.oldest_waiter is not None:
            continue
        if group.in_flight < group.cap:
            return AdmissionChoice("overflow", group.name)
    if idle_sec >= stall_sec:
        return AdmissionChoice("stall", bound.name)
    return AdmissionChoice("wait", bound.name)


def _can_take(group: GroupView, request_id: UUID, *, front: bool) -> bool:
    if group.in_flight >= group.cap:
        return False
    if group.oldest_waiter is None or front:
        return True
    return group.oldest_waiter == request_id


def pause_credit_seconds(
    spans: Sequence[tuple[datetime, datetime | None, datetime]],
    *,
    now: datetime,
    max_pause_sec: float,
) -> float:
    """Same formula as ``gateway_dispatch_queue_credit``, for tests.

    ``now`` stands in for ``clock_timestamp()``. Production credit does not
    call this with a process clock.
    """

    if max_pause_sec <= 0:
        return 0.0
    total = 0.0
    for enqueued_at, closed_at, renewed_at in spans:
        end = min(closed_at or now, renewed_at + QUEUE_WAITER_DEAD_AFTER, now)
        total += max(0.0, (end - enqueued_at).total_seconds())
    return min(max(0.0, total), max_pause_sec)


_CREDIT_FOR_TRIALS = text(
    """
    SELECT ids.trial_id,
           gateway_dispatch_queue_credit(ids.trial_id, ids.max_pause) AS credit,
           EXISTS (
               SELECT 1
                 FROM gateway_dispatch_queue AS open_row
                WHERE open_row.trial_id = ids.trial_id
                  AND open_row.closed_at IS NULL
                  AND open_row.renewed_at >= clock_timestamp() - interval '15 seconds'
           ) AS waiting
      FROM unnest(:trial_ids, :max_pauses) AS ids(trial_id, max_pause)
    """
).bindparams(
    bindparam("trial_ids", type_=ARRAY(PG_UUID(as_uuid=True))),
    bindparam("max_pauses", type_=ARRAY(Float())),
)


async def committed_pause_credits(
    session: AsyncSession,
    limits: Mapping[UUID, float],
) -> dict[UUID, tuple[float, bool]]:
    """Read pause credit and live-waiter state inside one database statement.

    The statement uses ``clock_timestamp()`` for every comparison. A permission
    or database error rolls back only this read and returns no credit, so a
    failed poll cannot extend a deadline and cannot abort the caller's
    transaction.
    """

    trial_ids = list(limits)
    if not trial_ids:
        return {}
    max_pauses = [float(limits[trial_id]) for trial_id in trial_ids]
    try:
        async with session.begin_nested():
            rows = (
                await session.execute(
                    _CREDIT_FOR_TRIALS,
                    {"trial_ids": trial_ids, "max_pauses": max_pauses},
                )
            ).all()
    except SQLAlchemyError:
        logger.warning("gateway_queue_credit_unavailable")
        return {}
    found: dict[UUID, tuple[float, bool]] = {}
    for trial_id, credit, waiting in rows:
        cap = float(limits[trial_id])
        amount = min(max(0.0, float(credit or 0.0)), cap)
        found[trial_id] = (amount, bool(waiting))
    return found


async def committed_pause_credit(
    session: AsyncSession,
    trial_id: UUID | None,
    max_pause_sec: float,
) -> float:
    if trial_id is None:
        return 0.0
    credits = await committed_pause_credits(session, {trial_id: max_pause_sec})
    return credits.get(trial_id, (0.0, False))[0]
