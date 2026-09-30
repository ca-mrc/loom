"""Persist global proposals from the existing queued-Trial eligibility contract.

This selects work, never claims attempts, grants capacity or creates Jobs. The
outbox rechecks the advisory scan under its existing exact-selection locks.
"""
from __future__ import annotations

import logging
from datetime import datetime
from uuid import UUID

from sqlalchemy import text, update

from loom.db.schema import Trial
from loom.nebius_rollout_guard import admission_open
from loom_control_plane.execution_capacity import ExecutionProvisioningBlockedError
from loom_control_plane.service_execution_scheduler import (
    _SERVICE_TRIAL_SQL,
    ServiceExecutionConfigurationError,
)
from loom_execution_actuator.pool_execution_outbox import (
    PoolExecutionHandoff,
    PoolExecutionOutbox,
    _clock,
)

_LOG = logging.getLogger(__name__)
_CANDIDATES = text(_SERVICE_TRIAL_SQL + """
 AND NOT EXISTS (
     SELECT 1 FROM nebius_pool_execution_outbox proposal
      WHERE proposal.trial_id = t.id AND proposal.phase NOT IN ('cancelled', 'released')
 )
 ORDER BY CASE WHEN t.pool_origin->>'kind' = 'environment'
                    AND t.pool_origin->>'data_environment_id' = :environment_id
               THEN 0 ELSE 1 END,
          (q.in_flight_count::double precision / q.fair_share_weight) ASC,
          t.submit_priority DESC, t.submitted_at ASC, t.id ASC
""")


class PoolExecutionSelector:
    def __init__(self, *, outbox: PoolExecutionOutbox) -> None:
        self.outbox = outbox
        self.targets = tuple(item.target_id for item in outbox.participant.targets if "trial" in item.workload_kinds)
        if not self.targets:
            raise ValueError("global execution selection requires a registered trial target")

    async def _configuration_failure(self, trial_id: UUID, error: ServiceExecutionConfigurationError,
                                     now: datetime) -> None:
        # Match normal queued configuration failure: no attempt, lease or spend.
        async with self.outbox.sessions.begin() as session:
            await session.execute(update(Trial).where(Trial.id == trial_id, Trial.state == "queued",
                Trial.cancellation_requested_at.is_(None)).values(state="failed",
                failure_reason="service_execution_configuration_invalid", failure_message=str(error),
                finished_at=now, next_attempt_at=None))

    async def select_next(self) -> PoolExecutionHandoff | None:
        async with self.outbox.sessions() as session:
            if not await admission_open(session):
                return None
            now = await _clock(session)
            rows = await session.stream(_CANDIDATES, {"now": now, "pool_id": self.outbox.logical_pool_id,
                "environment_id": str(self.outbox.participant.environment_id)},
                execution_options={"yield_per": 100})
            try:
                async for candidate in rows.mappings():
                    for target_id in self.targets:
                        try:
                            selected = await self.outbox.propose(trial_id=candidate["id"], target_id=target_id)
                            if selected is not None:
                                return selected
                            break  # No runtime/image ready on any target for this candidate.
                        except ServiceExecutionConfigurationError as error:
                            await self._configuration_failure(candidate["id"], error, now)
                            break
                        except (ValueError, ExecutionProvisioningBlockedError) as error:
                            # A stale/incompatible candidate cannot hide later work.
                            # No HTTP or external writer is invoked by selection.
                            _LOG.debug("Global execution selection deferred trial=%s error=%s",
                                candidate["id"], type(error).__name__)
            finally:
                await rows.close()
        return None
