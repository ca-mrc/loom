"""Persist global proposals from the existing queued-Trial eligibility contract.

This selects work, never claims attempts, grants capacity or creates Jobs. The
outbox rechecks the advisory scan under its existing exact-selection locks.
"""
from __future__ import annotations

import logging
from typing import Protocol

from sqlalchemy import text

from loom.nebius_pool_allocation import PoolNodeAllocationRequestV1, PoolNodeAllocationV1
from loom.nebius_rollout_guard import admission_open
from loom_control_plane.execution_capacity import ExecutionProvisioningBlockedError
from loom_control_plane.service_execution_scheduler import _SERVICE_TRIAL_SQL
from loom_execution_actuator.pool_client import PoolRequestUnconfirmedError
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


class PoolNodeAllocationReader(Protocol):
    async def node_allocation(self, scope: PoolNodeAllocationRequestV1) -> PoolNodeAllocationV1: ...


class PoolExecutionSelector:
    def __init__(self, *, outbox: PoolExecutionOutbox, allocation_reader: PoolNodeAllocationReader | None = None) -> None:
        self.outbox = outbox
        self.allocation_reader = allocation_reader
        self.targets = tuple(item.target_id for item in outbox.participant.targets if "trial" in item.workload_kinds)
        if not self.targets:
            raise ValueError("global execution selection requires a registered trial target")

    async def select_next(self) -> PoolExecutionHandoff | None:
        allocations = {}
        if self.allocation_reader is not None:
            participant = self.outbox.participant
            for target_id in self.targets:
                scope = PoolNodeAllocationRequestV1(pool_id=participant.pool_id, participant_id=participant.participant_id,
                    admission_epoch=participant.admission_epoch, participant_revision=participant.binding_revision,
                    target_id=target_id)
                try:
                    allocations[target_id] = await self.allocation_reader.node_allocation(scope)
                except PoolRequestUnconfirmedError:
                    # Explicit resource plans can still queue; node-share plans
                    # require qualified evidence and never use local snapshots.
                    pass
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
                            selected = await self.outbox.propose(trial_id=candidate["id"], target_id=target_id,
                                node_allocation=allocations.get(target_id))
                            if selected is not None:
                                return selected
                            break  # Candidate failed configuration/image checks or has no ready runtime.
                        except (ValueError, ExecutionProvisioningBlockedError) as error:
                            # A stale/incompatible candidate cannot hide later work.
                            # No HTTP or external writer is invoked by selection.
                            _LOG.debug("Global execution selection deferred trial=%s error=%s",
                                candidate["id"], type(error).__name__)
            finally:
                await rows.close()
        return None
