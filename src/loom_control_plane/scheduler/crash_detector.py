"""Background sweep that reclaims trials from workers whose heartbeat has
lapsed (spec §2.8 + §3.10).

Runs every `interval_sec`. Claimed/running trials and historical started-but-
queued rows owned by an expired local worker lose that owner and receive an
actionable diagnostic with a 30-second retry backoff. Current native execution
attempt leases are excluded; they retain their own recovery authority.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import exists, func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import (
    ExecutionAdmissionReservation,
    LlmCall,
    Task,
    Trial,
    TrialEvent,
    Worker,
)
from loom.trial.stale_running import StaleRunningDecision, evaluate_stale_running_trial
from loom_control_plane.metrics import WORKER_RECLAIM_TOTAL

logger = logging.getLogger(__name__)

_RECLAIM_SQL = text("""
UPDATE trials
   SET state = 'queued',
       worker_id = NULL,
       failure_reason = CASE
           WHEN failure_reason = 'prod_capacity_pressure'
           THEN failure_reason
           ELSE 'worker_lost_claim'
       END,
       failure_message = CASE
           WHEN failure_reason = 'prod_capacity_pressure'
           THEN failure_message
           WHEN state = 'claimed' AND started_at IS NULL
           THEN CONCAT(
               'claimed_without_started_reclaimed trial_id=', id::text,
               ' worker_id=', worker_id::text,
               ' claimed_at=', claimed_at::text,
               ' pre_start_heartbeat_at=', COALESCE(pre_start_heartbeat_at::text, 'NULL'),
               ' expiry_sec=', (:expiry_sec)::int::text,
               ' started_at=NULL'
           )
           ELSE CONCAT(
               'worker_heartbeat_expired_reclaimed trial_id=', id::text,
               ' worker_id=', worker_id::text,
               ' previous_state=', state,
               ' started_at=', COALESCE(started_at::text, 'NULL'),
               ' expiry_sec=', (:expiry_sec)::int::text
           )
       END,
       next_attempt_at = NOW() + INTERVAL '1 second' * 30
 WHERE (state IN ('claimed', 'running') OR (state = 'queued' AND started_at IS NOT NULL))
   AND worker_id IN (
       SELECT id FROM workers
        WHERE last_seen_at < NOW() - INTERVAL '1 second' * (:expiry_sec)::int
   )
   AND NOT EXISTS (
       SELECT 1 FROM execution_leases lease
        WHERE lease.trial_id = trials.id
          AND lease.execution_role = 'attempt'
          AND lease.attempt = trials.attempt_count
   )
 RETURNING id;
""")

_STALE_CLAIM_SQL = text("""
UPDATE trials
   SET state = 'queued',
       worker_id = NULL,
       failure_reason = 'worker_lost_claim',
       failure_message = CONCAT(
           'claimed_without_started_reclaimed trial_id=', id::text,
           ' worker_id=', worker_id::text,
           ' claimed_at=', claimed_at::text,
           ' pre_start_heartbeat_at=', COALESCE(pre_start_heartbeat_at::text, 'NULL'),
           ' expiry_sec=', (:expiry_sec)::int::text,
           ' started_at=NULL'
       ),
       next_attempt_at = NOW() + INTERVAL '1 second' * 30
 WHERE state = 'claimed'
   AND started_at IS NULL
   AND claimed_at IS NOT NULL
   AND COALESCE(pre_start_heartbeat_at, claimed_at) < NOW() - INTERVAL '1 second' * (:expiry_sec)::int
   AND NOT EXISTS (
       SELECT 1 FROM execution_leases lease
        WHERE lease.trial_id = trials.id
          AND lease.execution_role = 'attempt'
          AND lease.attempt = trials.attempt_count
   )
 RETURNING id;
""")


async def reclaim_expired_workers(
    session: AsyncSession,
    *,
    expiry_sec: int,
    claimed_without_start_expiry_sec: int | None = None,
    running_stale_timeout_multiplier: float | None = None,
    running_stale_grace_sec: float = 900.0,
    running_stale_silence_sec: float = 900.0,
    now: datetime | None = None,
) -> int:
    """Run a single sweep. Returns the number of trials reclaimed."""
    expired_rows = (
        await session.execute(
            _RECLAIM_SQL,
            {
                "expiry_sec": expiry_sec,
            },
        )
    ).all()
    stale_count = 0
    if claimed_without_start_expiry_sec is not None:
        stale_count = len(
            (
                await session.execute(
                    _STALE_CLAIM_SQL,
                    {
                        "expiry_sec": claimed_without_start_expiry_sec,
                    },
                )
            ).all()
        )
    running_stale_count = 0
    if running_stale_timeout_multiplier is not None:
        running_stale_count = await reclaim_stale_running_trials(
            session,
            now=now,
            worker_heartbeat_expiry_sec=expiry_sec,
            timeout_multiplier=running_stale_timeout_multiplier,
            grace_sec=running_stale_grace_sec,
            silence_sec=running_stale_silence_sec,
        )
    return len(expired_rows) + stale_count + running_stale_count


async def reclaim_stale_running_trials(
    session: AsyncSession,
    *,
    now: datetime | None = None,
    worker_heartbeat_expiry_sec: float,
    timeout_multiplier: float,
    grace_sec: float,
    silence_sec: float,
) -> int:
    """Fail running trials that are past timeout, silent, and still owned by
    a fresh-heartbeating worker.

    Dead-worker reclaim remains the queued retry path above. This path targets
    the #378 shape: the worker is alive, but its in-flight opencode/subprocess
    trial stopped emitting events and LLM calls long past the configured agent
    timeout.
    """
    if timeout_multiplier <= 0:
        return 0
    observed_at = now or datetime.now(UTC)
    rows = (
        await session.execute(
            select(Trial, Task, Worker)
            .join(Task, Task.id == Trial.task_id)
            .outerjoin(Worker, Worker.id == Trial.worker_id)
            .where(Trial.state == "running")
            .where(Trial.started_at.is_not(None))
            .where(
                ~exists()
                .where(ExecutionAdmissionReservation.trial_id == Trial.id)
                .where(ExecutionAdmissionReservation.execution_role == "attempt")
                .where(
                    ExecutionAdmissionReservation.owner_kind
                    == "protected_worker_claim"
                )
                .where(ExecutionAdmissionReservation.state == "active")
            )
        )
    ).all()
    if not rows:
        return 0

    trial_ids = [trial.id for trial, _task, _worker in rows]
    last_events = {
        trial_id: last_at
        for trial_id, last_at in (
            await session.execute(
                select(TrialEvent.trial_id, func.max(TrialEvent.created_at))
                .where(TrialEvent.trial_id.in_(trial_ids))
                .group_by(TrialEvent.trial_id)
            )
        ).all()
    }
    last_llm_calls = {
        trial_id: last_at
        for trial_id, last_at in (
            await session.execute(
                select(LlmCall.trial_id, func.max(LlmCall.captured_at))
                .where(LlmCall.trial_id.in_(trial_ids))
                .group_by(LlmCall.trial_id)
            )
        ).all()
    }

    reclaimed = 0
    for trial, task, worker in rows:
        decision = evaluate_stale_running_trial(
            state=str(trial.state),
            started_at=trial.started_at,
            finished_at=trial.finished_at,
            trial_config=trial.config,
            task_config=task.config,
            last_event_at=last_events.get(trial.id),
            last_llm_call_at=last_llm_calls.get(trial.id),
            worker_last_seen_at=(worker.last_seen_at if worker is not None else None),
            now=observed_at,
            worker_heartbeat_expiry_sec=worker_heartbeat_expiry_sec,
            timeout_multiplier=timeout_multiplier,
            grace_sec=grace_sec,
            silence_sec=silence_sec,
        )
        if not decision.reclaimable:
            continue
        trial.state = "failed"
        trial.failure_reason = "agent_timeout"
        trial.failure_message = _stale_running_failure_message(trial, decision)
        trial.finished_at = observed_at
        trial.next_attempt_at = None
        reclaimed += 1
    return reclaimed


def _stale_running_failure_message(
    trial: Trial,
    decision: StaleRunningDecision,
) -> str:
    return " ".join(
        (
            "stale_running_reclaimed",
            f"trial_id={trial.id}",
            f"worker_id={trial.worker_id}",
            f"runtime_sec={_fmt_num(decision.runtime_sec)}",
            f"agent_timeout_sec={_fmt_num(decision.agent_timeout_sec)}",
            f"hard_deadline_sec={_fmt_num(decision.hard_deadline_sec)}",
            f"silence_sec={_fmt_num(decision.silence_sec)}",
            f"last_event_at={_fmt_dt(decision.last_event_at)}",
            f"last_llm_call_at={_fmt_dt(decision.last_llm_call_at)}",
            f"last_activity_at={_fmt_dt(decision.last_activity_at)}",
            f"worker_last_seen_at={_fmt_dt(decision.worker_last_seen_at)}",
            f"worker_heartbeat_status={decision.worker_heartbeat_status}",
            "reason=fresh_worker_timeout_and_silent",
        )
    )


def _fmt_num(value: float | None) -> str:
    return "unknown" if value is None else f"{value:g}"


def _fmt_dt(value: datetime | None) -> str:
    return "none" if value is None else value.isoformat()


async def run_crash_detector_loop(
    *,
    session_factory: Any,
    expiry_sec: int,
    interval_sec: int,
    claimed_without_start_expiry_sec: int | None = None,
    running_stale_timeout_multiplier: float | None = None,
    running_stale_grace_sec: float = 900.0,
    running_stale_silence_sec: float = 900.0,
) -> None:
    """Long-running task — sweeps every `interval_sec` seconds. Cancellation
    is the only way to stop it; lifespan teardown awaits the cancellation."""
    while True:
        try:
            async with session_factory() as session:
                count = await reclaim_expired_workers(
                    session,
                    expiry_sec=expiry_sec,
                    claimed_without_start_expiry_sec=(claimed_without_start_expiry_sec),
                    running_stale_timeout_multiplier=running_stale_timeout_multiplier,
                    running_stale_grace_sec=running_stale_grace_sec,
                    running_stale_silence_sec=running_stale_silence_sec,
                )
                await session.commit()
            if count:
                logger.info("crash_detector_reclaimed", extra={"count": count})
                WORKER_RECLAIM_TOTAL.inc(count)
        except asyncio.CancelledError:
            return
        except Exception as exc:
            logger.warning("crash_detector_error", extra={"err": str(exc)})
        await asyncio.sleep(interval_sec)
