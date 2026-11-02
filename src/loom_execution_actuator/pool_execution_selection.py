"""Persist global proposals from the existing queued-Trial eligibility contract.

This selects work, never claims attempts, grants capacity or creates Jobs. The
outbox rechecks the advisory scan under its existing exact-selection locks.
"""
from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Protocol
from uuid import UUID

from sqlalchemy import text

from loom.nebius_pool_allocation import PoolNodeAllocationRequestV1, PoolNodeAllocationV1
from loom.nebius_pool_contract import PoolParticipantV1
from loom.nebius_pool_placement_preview import PoolExecutionPreviewRequestV1, PoolExecutionPreviewV1
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


class PoolExecutionPreviewReader(Protocol):
    async def execution_preview(self, body: PoolExecutionPreviewRequestV1) -> PoolExecutionPreviewV1: ...


class PoolExecutionSelector:
    def __init__(self, *, outbox: PoolExecutionOutbox, allocation_reader: PoolNodeAllocationReader | None = None,
                 preview_reader: PoolExecutionPreviewReader | None = None, prefer_incoming_warm_fit: bool = False,
                 placement_peers: tuple[PoolParticipantV1, ...] = (), gather_seconds: float = 2) -> None:
        self.outbox = outbox
        self.allocation_reader = allocation_reader
        self.preview_reader = preview_reader
        self.prefer_incoming_warm_fit = prefer_incoming_warm_fit
        self.placement_peers = placement_peers
        self.gather_seconds = min(60, max(0, gather_seconds))
        self.configured_alternative_owner_count = len(placement_peers)
        self.alternative_owner_count = 0
        self.targets = tuple(item.target_id for item in outbox.participant.targets if "trial" in item.workload_kinds)
        if not self.targets:
            raise ValueError("global execution selection requires a registered trial target")

    async def select_next(self) -> PoolExecutionHandoff | None:
        verifier = await self.outbox.propose_next_verifier()
        if verifier is not None:
            return verifier
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
        if self.prefer_incoming_warm_fit and self.preview_reader is not None:
            return await self._select_previewed(allocations)
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


    async def _select_previewed(self, allocations: dict[str, PoolNodeAllocationV1]) -> PoolExecutionHandoff | None:
        from loom_execution_actuator.pool_placement_advice import (
            choose_advice,
            forget_advice,
            publish_advice,
        )

        assert self.preview_reader is not None
        self.alternative_owner_count = 0
        gather_until = time.monotonic() + self.gather_seconds
        offset = 0
        while True:
            # Close each bounded SQL page BEFORE manager HTTP. The final
            # proposal still locks/rechecks the source and the unique outbox.
            async with self.outbox.sessions() as session:
                if not await admission_open(session):
                    return None
                now = await _clock(session)
                rows = (await session.execute(text(_CANDIDATES.text + " LIMIT 100 OFFSET :offset"),
                    {"now": now, "pool_id": self.outbox.logical_pool_id,
                     "environment_id": str(self.outbox.participant.environment_id), "offset": offset})).mappings().all()
            if not rows:
                return None
            for candidate in rows:
                body = None
                ordinary = False
                for target_id in self.targets:
                    try:
                        body = await self.outbox.compile_prospective(trial_id=candidate["id"], target_id=target_id)
                        if body is None:
                            await forget_advice(self.outbox, candidate["id"], target_id)
                            break
                        preview = await self.preview_reader.execution_preview(body)
                        await publish_advice(self.outbox, candidate["id"], body, preview,
                            ttl_seconds=2 * self.gather_seconds)
                        if not preview.prefer_incoming_warm_fit:
                            ordinary = True
                            break
                    except (PoolRequestUnconfirmedError, ValueError, ExecutionProvisioningBlockedError):
                        await forget_advice(self.outbox, candidate["id"], target_id)
                        ordinary = True
                        break
                if body is None and not ordinary:
                    continue
                if not ordinary and body is not None:
                    mode, advice, self.alternative_owner_count = await choose_advice(self.outbox, trial_id=candidate["id"], body=body,
                        peers=self.placement_peers, gather_seconds=self.gather_seconds)
                    if mode == "wait":
                        # Prefetch other fair heads only within this existing
                        # poll budget, rather than holding a dispatcher in an
                        # unbounded HTTP scan when a peer is absent.
                        if time.monotonic() >= gather_until:
                            return None
                        continue
                    if mode == "warm" and advice is not None:
                        preview = PoolExecutionPreviewV1.model_validate(advice["preview"])
                        if preview.participant_id != self.outbox.participant.participant_id:
                            continue  # The selected owner commits with its own credential/driver.
                        try:
                            return await self.outbox.propose(trial_id=candidate["id"], target_id=preview.target_id,
                                node_allocation=preview.node_allocation,
                                prospective_lease_id=UUID(advice["lease_id"]),
                                prospective_deadline=datetime.fromisoformat(advice["deadline_at"]),
                                expected_request_sha256=preview.request_sha256,
                                expected_routing_generation=advice["routing_generation"],
                                preview_valid_until=min(preview.valid_until, datetime.fromisoformat(advice["advice_valid_until"])),
                                preview_evidence=preview,
                                alternative_owner_count=self.alternative_owner_count)
                        except (ValueError, ExecutionProvisioningBlockedError):
                            await forget_advice(self.outbox, candidate["id"], preview.target_id)
                            continue
                # Unsupported/mixed/unknown is the original cold-capable path.
                for target_id in self.targets:
                    try:
                        proposal = await self.outbox.propose(trial_id=candidate["id"], target_id=target_id,
                            node_allocation=allocations.get(target_id))
                        if proposal is not None:
                            return proposal
                        break
                    except (ValueError, ExecutionProvisioningBlockedError):
                        continue
            offset += len(rows)
