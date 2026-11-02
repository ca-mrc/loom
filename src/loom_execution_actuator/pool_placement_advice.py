"""Cooperative advice in the existing CP database; never a distributed lease.

Each process calls only its own manager using its existing credential. Peers
are non-secret, already-registered scope metadata, not enrollment or authority.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any
from uuid import UUID

from sqlalchemy import delete, select, text
from sqlalchemy.dialects.postgresql import insert

from loom.db.nebius_pool_outbox_schema import NebiusPoolPlacementAdvice
from loom.nebius_pool_contract import PoolParticipantV1
from loom.nebius_pool_placement_preview import PoolExecutionPreviewRequestV1, PoolExecutionPreviewV1
from loom_execution_actuator.pool_execution_outbox import PoolExecutionOutbox, _clock


async def publish_advice(outbox: PoolExecutionOutbox, trial_id: UUID,
                         body: PoolExecutionPreviewRequestV1, preview: PoolExecutionPreviewV1,
                         *, ttl_seconds: float = 10) -> None:
    participant = outbox.participant
    if (preview.execution_namespace_uid != participant.execution_namespace.uid
            or preview.pool_id != participant.pool_id or preview.participant_id != participant.participant_id):
        raise ValueError("pool_preview_namespace_unavailable")
    async with outbox.sessions.begin() as session:
        now = await _clock(session)
        if not preview.observed_at <= now < preview.valid_until:
            raise ValueError("pool_preview_expired")
        expiry = min(preview.valid_until, now + timedelta(seconds=min(60, max(2, ttl_seconds))))
        payload = dict(lease_id=str(body.request.key.local_work_id), deadline_at=body.request.deadline_at.isoformat(),
            advice_valid_until=expiry.isoformat(),
            routing_generation=body.routing_generation,
            task_revision_sha256=body.request.execution.runtime.task_revision_sha256,
            preview=preview.model_dump(mode="json"))
        # Repeated refresh must not reset the bounded advice-gather time.
        statement = insert(NebiusPoolPlacementAdvice).values(trial_id=trial_id,
            participant_id=participant.participant_id, target_id=preview.target_id,
            requested_at=now, observed_at=now, valid_until=expiry, advice_json=payload)
        await session.execute(statement.on_conflict_do_update(
            index_elements=["trial_id", "participant_id", "target_id"],
            set_={"observed_at": now, "valid_until": expiry, "advice_json": payload}))
        # Bounded cleanup of this owner's expired advisory records. Deleting
        # them cannot release a resource or affect manager queue age.
        expired = select(NebiusPoolPlacementAdvice.trial_id).where(
            NebiusPoolPlacementAdvice.participant_id == participant.participant_id,
            NebiusPoolPlacementAdvice.valid_until <= now).limit(100)
        await session.execute(delete(NebiusPoolPlacementAdvice).where(
            NebiusPoolPlacementAdvice.participant_id == participant.participant_id,
            NebiusPoolPlacementAdvice.trial_id.in_(expired)))


async def forget_advice(outbox: PoolExecutionOutbox, trial_id: UUID, target_id: str) -> None:
    """A failed owner cannot leave an old warm hint hiding ordinary cold work."""
    async with outbox.sessions.begin() as session:
        if await session.scalar(text("SELECT to_regclass('nebius_pool_placement_advice') IS NOT NULL")):
            await session.execute(delete(NebiusPoolPlacementAdvice).where(
                NebiusPoolPlacementAdvice.trial_id == trial_id,
                NebiusPoolPlacementAdvice.participant_id == outbox.participant.participant_id,
                NebiusPoolPlacementAdvice.target_id == target_id))


async def choose_advice(outbox: PoolExecutionOutbox, *, trial_id: UUID, body: PoolExecutionPreviewRequestV1,
                        peers: tuple[PoolParticipantV1, ...], gather_seconds: float) -> tuple[str, dict[str, Any] | None, int]:
    """Return warm, wait, or ordinary; no missing owner is treated as zero load."""
    owners = {row.participant_id: row for row in (outbox.participant, *peers)}
    async with outbox.sessions() as session:
        now = await _clock(session)
        rows = (await session.scalars(select(NebiusPoolPlacementAdvice).where(
            NebiusPoolPlacementAdvice.trial_id == trial_id,
            NebiusPoolPlacementAdvice.participant_id.in_(owners),
            NebiusPoolPlacementAdvice.valid_until > now))).all()
    valid = []
    first: datetime | None = None
    for row in rows:
        try:
            payload = row.advice_json
            preview = PoolExecutionPreviewV1.model_validate(payload["preview"])
            owner = owners[row.participant_id]
            lease_id = UUID(payload["lease_id"])
            deadline = datetime.fromisoformat(payload["deadline_at"])
            advice_expiry = datetime.fromisoformat(payload["advice_valid_until"])
            if (not lease_id.int or deadline.utcoffset() is None or advice_expiry.utcoffset() is None
                    or deadline <= now or not now < advice_expiry <= row.valid_until
                    or advice_expiry > preview.valid_until
                    or type(payload["routing_generation"]) is not int):
                continue
            owner.target(row.target_id, "trial")
            if (preview.pool_id, preview.participant_id, preview.admission_epoch, preview.participant_revision,
                    preview.execution_namespace_uid, preview.target_id) != (
                    owner.pool_id, owner.participant_id, owner.admission_epoch, owner.binding_revision,
                    owner.execution_namespace.uid, row.target_id):
                continue
            if (not preview.observed_at <= now < preview.valid_until
                    or payload["routing_generation"] != body.routing_generation
                    or payload["task_revision_sha256"] != body.request.execution.runtime.task_revision_sha256):
                continue
            first = min(first, row.requested_at) if first else row.requested_at
            valid.append((preview, payload))
        except (ValueError, KeyError, TypeError):
            continue
    alternatives = len({preview.participant_id for preview, _ in valid
                        if preview.eligible and preview.participant_id != outbox.participant.participant_id})
    if any(not preview.prefer_incoming_warm_fit for preview, _ in valid):
        return "ordinary", None, alternatives
    expected = {(owner.participant_id, target.target_id) for owner in owners.values()
                for target in owner.targets if "trial" in target.workload_kinds}
    present = {(preview.participant_id, preview.target_id) for preview, _ in valid}
    if not expected <= present:
        if first and (now - first).total_seconds() < gather_seconds:
            return "wait", None, alternatives
        return "ordinary", None, alternatives
    warm = [(preview, payload) for preview, payload in valid if preview.eligible and preview.warm_node_uids]
    if not warm:
        return "ordinary", None, alternatives
    # Work fairness is the existing fair candidate scan, not this target tie.
    # All processes see the same deterministic target winner before committing.
    _, selected = min(warm, key=lambda item: (item[0].target_id, str(item[0].participant_id)))
    return "warm", selected, alternatives
