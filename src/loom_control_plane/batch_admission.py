"""Platform-admin admission pause for one batch's pre-start queue (#2777).

The batch flag is the durable authority: every attempt reservation re-reads it
under the batch key-share fence. This module converges the leases that won a
reservation before the pause committed. Each one is released through the
ordinary fenced ``retry`` transition, so cleanup returns admission, cost and
provisioning reservations, and the attempt number is refunded to the budget.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from sqlalchemy import exists, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.nebius_pool_outbox_schema import NebiusPoolExecutionOutbox
from loom.db.schema import Batch, ServiceExecutionLease, Trial
from loom_control_plane.service_execution import enqueue_execution_transition
from loom_control_plane.service_execution_locks import lock_execution_trial

_LOG = logging.getLogger(__name__)

ADMISSION_PAUSED_REASON = "admission_paused"
_ACTIVE_BATCH_STATES = ("submitted", "running")
_HINT_BATCH = 500


@dataclass(frozen=True)
class AdmissionDrainResult:
    released_lease_count: int
    hinted_trial_count: int


def _unstarted_attempt_filters() -> tuple[Any, ...]:
    """A reserved attempt whose pod was never scheduled and produced no output.

    This is the same fence as the unschedulable-deadline retry: a Job or a
    pending Pod may exist, and the retry's generation bump fences it before
    cleanup deletes it.
    """
    return (
        ServiceExecutionLease.execution_role == "attempt",
        ServiceExecutionLease.desired_state == "create",
        ServiceExecutionLease.revoked_at.is_(None),
        ServiceExecutionLease.pod_scheduled_at.is_(None),
        ServiceExecutionLease.pod_started_at.is_(None),
        ServiceExecutionLease.node_name.is_(None),
        ServiceExecutionLease.output_commit_state == "not_started",
        Trial.state == "claimed",
        Trial.started_at.is_(None),
        Trial.cancellation_requested_at.is_(None),
        Trial.attempt_count == ServiceExecutionLease.attempt,
        # Pool-routed leases are owned by the pool handoff, which revokes
        # them through the manager's cancelled_unstarted receipt instead.
        ~exists(select(NebiusPoolExecutionOutbox.lease_id).where(
            NebiusPoolExecutionOutbox.lease_id == ServiceExecutionLease.id,
        )),
    )


def paused_unstarted_attempts(*, batch_id: UUID | None = None, limit: int = 100) -> Any:
    query = (
        select(ServiceExecutionLease.id, ServiceExecutionLease.generation)
        .join(Trial, Trial.id == ServiceExecutionLease.trial_id)
        .join(Batch, Batch.id == Trial.batch_id)
        .where(
            Batch.admission_paused_at.is_not(None),
            Batch.state.in_(_ACTIVE_BATCH_STATES),
            *_unstarted_attempt_filters(),
        )
        .order_by(ServiceExecutionLease.created_at, ServiceExecutionLease.id)
        .limit(limit)
    )
    if batch_id is not None:
        query = query.where(Batch.id == batch_id)
    return query


async def release_unstarted_paused_attempt(
    session: AsyncSession, *, lease_id: UUID, generation: int, now: datetime,
) -> bool:
    """Requeue one reserved, never-started attempt of a paused batch.

    Lock order is Trial, then lease, then a key-share read of the batch, the
    same order reservation uses, so this never deadlocks with a reservation.
    Every fence is rechecked under those locks; a lease the actuator advanced
    in the meantime is left to its normal lifecycle.
    """
    await lock_execution_trial(session, lease_id)
    lease = await session.get(
        ServiceExecutionLease, lease_id, with_for_update=True, populate_existing=True,
    )
    if lease is None or lease.generation != generation:
        return False
    row = (await session.execute(
        select(Trial, Batch.admission_paused_at, Batch.state)
        .select_from(ServiceExecutionLease)
        .join(Trial, Trial.id == ServiceExecutionLease.trial_id)
        .join(Batch, Batch.id == Trial.batch_id)
        .where(ServiceExecutionLease.id == lease.id, *_unstarted_attempt_filters())
        .with_for_update(of=Batch, key_share=True)
        .execution_options(populate_existing=True)
    )).one_or_none()
    if row is None:
        return False
    trial, paused_at, batch_state = row
    if paused_at is None or batch_state not in _ACTIVE_BATCH_STATES:
        return False
    await enqueue_execution_transition(
        session, lease_id=lease.id, expected_generation=generation,
        desired_state="retry", now=now,
    )
    lease.error_class = "transient"
    lease.error_code = ADMISSION_PAUSED_REASON
    lease.error_message = "released by a batch admission pause before the pod was scheduled"
    # Lease attempt numbers are unique, so the next reservation takes the next
    # number. Refund this one so a pause never spends the retry budget.
    trial.admission_pause_releases += 1
    trial.scheduling_observation = {
        "reason": ADMISSION_PAUSED_REASON, "observed_at": now.isoformat(),
    }
    await session.flush()
    return True


async def mark_paused_queued_trials(
    session: AsyncSession, *, batch_id: UUID | None = None, now: datetime,
) -> int:
    """Replace capacity-wait hints of held trials with the pause reason.

    Rows another transaction holds are skipped and picked up next tick, so
    this bulk write never waits on, or deadlocks with, a per-trial writer.
    """
    paused = select(Batch.id).where(
        Batch.admission_paused_at.is_not(None), Batch.state.in_(_ACTIVE_BATCH_STATES),
    )
    if batch_id is not None:
        paused = paused.where(Batch.id == batch_id)
    targets = (
        select(Trial.id)
        .where(
            Trial.batch_id.in_(paused),
            Trial.state == "queued",
            Trial.cancellation_requested_at.is_(None),
            func.coalesce(Trial.scheduling_observation["reason"].astext, "")
            != ADMISSION_PAUSED_REASON,
        )
        .limit(_HINT_BATCH)
        .with_for_update(skip_locked=True)
        .scalar_subquery()
    )
    updated = await session.scalars(
        update(Trial)
        .where(Trial.id.in_(targets))
        .values(scheduling_observation={
            "reason": ADMISSION_PAUSED_REASON, "observed_at": now.isoformat(),
        })
        .returning(Trial.id)
        .execution_options(synchronize_session=False)
    )
    return len(updated.all())


async def clear_resumed_queued_trials(session: AsyncSession, *, batch_id: UUID) -> int:
    """Drop the pause reason so resumed trials compete as ordinary queued work."""
    updated = await session.scalars(
        update(Trial)
        .where(
            Trial.batch_id == batch_id,
            Trial.state == "queued",
            Trial.scheduling_observation["reason"].astext == ADMISSION_PAUSED_REASON,
        )
        .values(scheduling_observation=None)
        .returning(Trial.id)
        .execution_options(synchronize_session=False)
    )
    return len(updated.all())


async def count_admission_held(session: AsyncSession, *, batch_id: UUID) -> int:
    """Trials of a paused batch that are waiting to start."""
    return int(await session.scalar(
        select(func.count()).select_from(Trial).where(
            Trial.batch_id == batch_id,
            Trial.state == "queued",
            Trial.cancellation_requested_at.is_(None),
        )
    ) or 0)


async def count_pending_release(session: AsyncSession, *, batch_id: UUID) -> int:
    """Reserved, never-started attempts the next drain pass will release."""
    query = paused_unstarted_attempts(batch_id=batch_id, limit=1_000_000).order_by(None)
    return int(await session.scalar(
        select(func.count()).select_from(query.subquery())
    ) or 0)


async def drain_paused_admission(
    session_factory: Any,
    *,
    batch_id: UUID | None = None,
    limit: int = 100,
    now: datetime | None = None,
) -> AdmissionDrainResult:
    """Converge paused batches: release unstarted leases, then mark held trials.

    Each lease is released in its own transaction. The scan runs outside any
    batch lock, so a crash between the pause commit and this drain converges
    on the next scheduler tick.
    """
    async with session_factory() as session:
        candidates = (await session.execute(
            paused_unstarted_attempts(batch_id=batch_id, limit=limit)
        )).all()
        await session.commit()
    released = 0
    for lease_id, generation in candidates:
        try:
            async with session_factory() as session:
                if await release_unstarted_paused_attempt(
                    session, lease_id=lease_id, generation=generation,
                    now=now or datetime.now(UTC),
                ):
                    released += 1
                await session.commit()
        except Exception as exc:
            # The actuator may advance or revoke this lease concurrently; its
            # fences reject this release and the next tick re-reads it.
            _LOG.warning("batch_admission_release_skipped", extra={
                "lease_id": str(lease_id), "error": type(exc).__name__,
            })
    async with session_factory() as session:
        hinted = await mark_paused_queued_trials(
            session, batch_id=batch_id, now=now or datetime.now(UTC),
        )
        await session.commit()
    if released or hinted:
        _LOG.info("batch_admission_drained", extra={
            "released_leases": released, "hinted_trials": hinted,
            "batch_id": str(batch_id) if batch_id is not None else None,
        })
    return AdmissionDrainResult(released_lease_count=released, hinted_trial_count=hinted)


__all__ = [
    "ADMISSION_PAUSED_REASON",
    "AdmissionDrainResult",
    "clear_resumed_queued_trials",
    "count_admission_held",
    "count_pending_release",
    "drain_paused_admission",
    "mark_paused_queued_trials",
    "paused_unstarted_attempts",
    "release_unstarted_paused_attempt",
]
