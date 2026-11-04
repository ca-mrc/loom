"""Shared batch-drain cancellation rules.

The service route, the batch runner, and the Control Plane all use this module
so a trial retained while it is verifying or archiving cannot be cancelled by a
later sweep, and a cancelling batch cannot admit a new attempt.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import and_, exists, func, or_, select, update
from sqlalchemy.orm import aliased
from sqlalchemy.sql.elements import ColumnElement

from loom.db.schema import Batch, ServiceExecutionLease, Trial

CANCELLING = "cancelling"
CANCELLED = "cancelled"
MODE_FULL = "full"
MODE_DRAIN = "drain"
ACTIVE_TRIAL_STATES = ("queued", "protected-pending", "claimed", "running")
DRAIN_BLOCKING_TRIAL_STATES = (*ACTIVE_TRIAL_STATES, "materializing")


def reservation_allowed(
    parent_state: str | None,
    execution_role: str,
    *,
    retained: bool,
    cancellation_requested: bool,
    handoff_pending: bool,
) -> bool:
    """Whether a batch may receive a new execution lease.

    A cancelling batch still admits the verifier of a trial that was retained
    and whose handoff is pending. Attempt leases stay fenced.
    """

    if parent_state in {"submitted", "running"}:
        return True
    return (
        parent_state == CANCELLING
        and execution_role == "verifier"
        and retained
        and not cancellation_requested
        and handoff_pending
    )


def revocation_desired_state(current_desired: str, output_commit_state: str) -> str | None:
    """Lease transition for a cancellation that has already been recorded.

    A finalize lease whose output is still uploading keeps its current desired
    state. The commit path asks again once the output is durable, instead of
    raising and rolling the cancellation request back.
    """

    if current_desired in {"create", "start"}:
        return "cancel"
    if current_desired == "finalize":
        if output_commit_state != "committed":
            return None
        return "delete_pending"
    return None


def trial_is_retained() -> ColumnElement[bool]:
    """SQL predicate correlated to ``Trial``.

    A trial is retained while its latest attempt is still uploading, finalizing,
    or archiving output, while a deferred verifier handoff is pending, or while
    a verifier lease is still live.
    """

    attempt = aliased(ServiceExecutionLease)
    verifier = aliased(ServiceExecutionLease)
    latest_attempt_id = (
        select(ServiceExecutionLease.id)
        .where(
            ServiceExecutionLease.trial_id == Trial.id,
            ServiceExecutionLease.execution_role == "attempt",
        )
        .order_by(
            ServiceExecutionLease.attempt.desc(),
            ServiceExecutionLease.created_at.desc(),
            ServiceExecutionLease.id.desc(),
        )
        .limit(1)
        .correlate(Trial)
        .scalar_subquery()
    )
    archive_open = or_(
        attempt.output_commit_state == "uploading",
        and_(
            attempt.observed_state.in_(("finalizing", "finalized")),
            attempt.materialization_state.is_distinct_from("committed"),
            attempt.materialization_state.is_distinct_from("unavailable"),
        ),
        attempt.materialization_state.in_(("pending", "running")),
    )
    attempt_open = exists(
        select(attempt.id).where(attempt.id == latest_attempt_id, archive_open)
    )
    handoff_pending = and_(
        Trial.state.in_(("claimed", "running")),
        Trial.result["verifier_execution"]["state"].astext == "pending",
    )
    verifier_live = exists(
        select(verifier.id).where(
            verifier.trial_id == Trial.id,
            verifier.execution_role == "verifier",
            verifier.deleted_at.is_(None),
        )
    )
    return or_(attempt_open, handoff_pending, verifier_live)


def _latest_attempt_filter(attempt: Any, newer: Any) -> ColumnElement[bool]:
    return ~exists(
        select(newer.id).where(
            newer.trial_id == attempt.trial_id,
            newer.execution_role == "attempt",
            or_(
                newer.attempt > attempt.attempt,
                and_(
                    newer.attempt == attempt.attempt,
                    newer.created_at > attempt.created_at,
                ),
                and_(
                    newer.attempt == attempt.attempt,
                    newer.created_at == attempt.created_at,
                    newer.id > attempt.id,
                ),
            ),
        )
    )


def _drain_still_open(batch_id: Any) -> ColumnElement[bool]:
    """Committed work that must finish before a cancelling batch can close.

    This is a predicate on the batch update, not a trial lock. Reservation
    locks the trial before the parent batch, so the drain must not lock trials
    while it already holds the batch.
    """

    attempt = aliased(ServiceExecutionLease)
    newer = aliased(ServiceExecutionLease)
    verifier = aliased(ServiceExecutionLease)
    return or_(
        exists().where(
            Trial.batch_id == batch_id,
            Trial.state.in_(DRAIN_BLOCKING_TRIAL_STATES),
        ),
        exists(
            select(attempt.id)
            .join(Trial, Trial.id == attempt.trial_id)
            .where(
                Trial.batch_id == batch_id,
                attempt.execution_role == "attempt",
                _latest_attempt_filter(attempt, newer),
                attempt.materialization_state.in_(("pending", "running")),
            )
        ),
        exists(
            select(verifier.id)
            .join(Trial, Trial.id == verifier.trial_id)
            .where(
                Trial.batch_id == batch_id,
                Trial.cancellation_retained_at.is_not(None),
                verifier.execution_role == "verifier",
                verifier.deleted_at.is_(None),
            )
        ),
    )


async def finish_batch_drain(session: Any, batch: Batch) -> bool:
    """Move a locked cancelling batch to cancelled once its drain is idle."""

    if batch.state != CANCELLING:
        return False
    result = await session.execute(
        update(Batch)
        .where(
            Batch.id == batch.id,
            Batch.state == CANCELLING,
            ~_drain_still_open(batch.id),
        )
        .values(
            state=CANCELLED,
            result_status=CANCELLED,
            finished_at=func.coalesce(Batch.finished_at, func.now()),
        )
    )
    if result.rowcount != 1:
        return False
    batch.state = CANCELLED
    batch.result_status = CANCELLED
    return True


def handoff_is_pending(trial: Any) -> bool:
    handoff = (getattr(trial, "result", None) or {}).get("verifier_execution")
    return isinstance(handoff, dict) and handoff.get("state") == "pending"


def parent_accepts_reservation(
    parent_state: str | None,
    trial: Any,
    execution_role: str,
) -> bool:
    return reservation_allowed(
        parent_state,
        execution_role,
        retained=getattr(trial, "cancellation_retained_at", None) is not None,
        cancellation_requested=getattr(trial, "cancellation_requested_at", None) is not None,
        handoff_pending=handoff_is_pending(trial),
    )
