"""Indexed, disjoint lifetime ranges for unknown provider dispatches."""

from datetime import datetime
from uuid import UUID

from sqlalchemy import func, literal_column, select
from sqlalchemy.sql.elements import ColumnElement

from loom.db.schema import GatewayDispatchReceipt


def active_dispatch_count(
    connection_id: UUID, *, now: datetime, stale_after: datetime,
) -> ColumnElement[int]:
    """Count one connection without scanning its expired unknown outcomes.

    Keep the deadline and fallback-age ranges separate: an OR can make the
    planner scan every admitted receipt before filtering expired ones. The
    fixed outcome literal also lets generic prepared plans use partial indexes.
    A signed future deadline counts even when admission was a long time ago.
    """
    base = select(func.count()).select_from(GatewayDispatchReceipt).where(
        GatewayDispatchReceipt.provider_connection_id == connection_id,
        GatewayDispatchReceipt.provider_outcome == literal_column("'admitted'"),
    )
    dated = base.where(GatewayDispatchReceipt.attempt_deadline_wall_clock > now)
    undated = base.where(
        GatewayDispatchReceipt.attempt_deadline_wall_clock.is_(None),
        GatewayDispatchReceipt.admitted_at > stale_after,
    )
    return dated.scalar_subquery() + undated.scalar_subquery()
