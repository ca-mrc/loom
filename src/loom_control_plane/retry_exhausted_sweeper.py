"""Background sweep that transitions queued trials to failed when they have
exhausted their retry budget (spec §2.8).

A trial's retry budget is exhausted when
`attempt_count - admission_pause_releases >= team_quotas.max_attempts_ceiling`
— the admin's per-team ceiling, excluding attempts an admission pause released
before their pod was scheduled. The claim SQL already filters these out (workers will never receive them), but
nothing was transitioning them to a terminal state — they sat in `queued`
forever, keeping their parent batch in `running` forever.

This sweeper closes that gap: on each tick it atomically sets
`state='failed', failure_reason='retry_exhausted', finished_at=NOW()` for
all such trials, then increments the `loom_retry_exhausted_total` counter so
dashboards can see the rate.

A LIMIT cap of 100 per tick prevents table-lock pile-ups on pathological
deployments; the next tick picks up any remainder.

Model after `scheduler/crash_detector.py` — same loop shape, same error
handling, same cancellation contract.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from loom_control_plane.metrics import RETRY_EXHAUSTED_TOTAL

logger = logging.getLogger(__name__)

_SWEEP_SQL = text("""
UPDATE trials
   SET state          = 'failed',
       failure_reason = 'retry_exhausted',
       finished_at    = NOW()
 WHERE state = 'queued'
   AND cancellation_requested_at IS NULL
   AND id IN (
       SELECT t.id
         FROM trials t
         JOIN team_quotas q ON q.team_id = t.team_id
        WHERE t.state        = 'queued'
          AND t.cancellation_requested_at IS NULL
          AND t.attempt_count - t.admission_pause_releases >= q.max_attempts_ceiling
        LIMIT 100
   )
 RETURNING id;
""")


async def sweep_retry_exhausted(session: AsyncSession) -> list[Any]:
    """Run a single sweep tick.

    Returns the list of trial IDs that were transitioned so callers (tests,
    loop) can inspect or count them.
    """
    rows = (await session.execute(_SWEEP_SQL)).all()
    return [r[0] for r in rows]


async def run_retry_exhausted_sweeper_loop(
    *,
    session_factory: Any,
    interval_sec: int = 30,
) -> None:
    """Long-running task — sweeps every `interval_sec` seconds.

    Cancellation is the only stop signal; lifespan teardown cancels + awaits
    this task (same contract as crash_detector and metrics_refresher).
    """
    while True:
        try:
            async with session_factory() as session:
                trial_ids = await sweep_retry_exhausted(session)
                await session.commit()
            if trial_ids:
                count = len(trial_ids)
                RETRY_EXHAUSTED_TOTAL.inc(count)
                sample = [str(t) for t in trial_ids[:5]]
                logger.info(
                    "retry_exhausted_sweep",
                    extra={
                        "count": count,
                        "sample_ids": sample,
                    },
                )
        except asyncio.CancelledError:
            return
        except Exception as exc:
            logger.warning(
                "retry_exhausted_sweeper_error", extra={"err": str(exc)},
            )
        await asyncio.sleep(interval_sec)
