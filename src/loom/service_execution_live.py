"""Bounded native monitoring previews; canonical accounting and archives own final data."""
from __future__ import annotations

import json
from itertools import pairwise
from typing import Any

from pydantic import TypeAdapter, ValidationError
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.data_lifecycle_registry import ensure_trial_event_lifecycle_authority
from loom.db.schema import ServiceExecutionLease, ServiceExecutionLiveEvent, Trial, TrialEvent
from loom.models.trajectory import TrajectoryEvent

_EVENT_ADAPTER: TypeAdapter[TrajectoryEvent] = TypeAdapter(TrajectoryEvent)
MAX_BATCH_EVENTS = 500
MAX_EVENT_BYTES = 256 * 1024
MAX_BATCH_BYTES = 4 * 1024 * 1024
_TERMINAL = frozenset({'succeeded', 'failed', 'cancelled'})


class LiveTrajectoryError(ValueError):
    def __init__(self, code: str, status_code: int = 409) -> None:
        super().__init__(code)
        self.code, self.status_code = code, status_code


async def append_live_events(
    session: AsyncSession, *, lease: ServiceExecutionLease, trial: Trial,
    events: list[dict[str, Any]],
) -> dict[str, Any]:
    """Append one ordered batch under the caller's current lease and Trial locks."""
    if lease.trial_id != trial.id or lease.team_id != trial.team_id or lease.attempt != trial.attempt_count or lease.execution_role != 'attempt':
        raise LiveTrajectoryError('live_trajectory_execution_identity_invalid')
    if not 1 <= len(events) <= MAX_BATCH_EVENTS:
        raise LiveTrajectoryError('live_trajectory_batch_size_invalid', 413)
    payloads: list[dict[str, Any]] = []
    total_bytes = 0
    for raw in events:
        if type(raw.get('seq')) is not int:
            raise LiveTrajectoryError('live_trajectory_sequence_invalid', 422)
        try:
            encoded = json.dumps(raw, separators=(',', ':'), ensure_ascii=False).encode()
            event = _EVENT_ADAPTER.validate_python(raw)
        except (ValidationError, ValueError, TypeError) as exc:
            raise LiveTrajectoryError('live_trajectory_event_invalid', 422) from exc
        total_bytes += len(encoded)
        if len(encoded) > MAX_EVENT_BYTES or total_bytes > MAX_BATCH_BYTES:
            raise LiveTrajectoryError('live_trajectory_payload_too_large', 413)
        if event.trial_id != trial.id:
            raise LiveTrajectoryError('live_trajectory_trial_identity_invalid')
        payloads.append(event.model_dump(mode='json'))
    seqs = [payload['seq'] for payload in payloads]
    if any(b != a + 1 for a, b in pairwise(seqs)):
        raise LiveTrajectoryError('live_trajectory_sequence_invalid')
    source = (ServiceExecutionLiveEvent.lease_id == lease.id,
              ServiceExecutionLiveEvent.resource_generation == lease.resource_generation)
    last = await session.scalar(select(func.max(ServiceExecutionLiveEvent.seq)).where(*source))
    expected = 0 if last is None else last + 1
    existing = {row.seq: row for row in (await session.scalars(
        select(ServiceExecutionLiveEvent).where(*source, ServiceExecutionLiveEvent.seq.in_(seqs))
    )).all()}
    fresh = []
    for payload in payloads:
        seq = payload['seq']
        previous = existing.get(seq)
        if previous is not None:
            if previous.trial_id != trial.id or previous.payload != payload:
                raise LiveTrajectoryError('live_trajectory_event_identity_drift')
            continue
        if seq != expected:
            raise LiveTrajectoryError('live_trajectory_sequence_gap')
        expected += 1
        fresh.append(payload)
    if fresh:
        authority = await ensure_trial_event_lifecycle_authority(
            session, trial_id=trial.id, expected_team_id=trial.team_id,
        )
        session.add_all(ServiceExecutionLiveEvent(
            trial_id=trial.id, lease_id=lease.id, resource_generation=lease.resource_generation,
            seq=payload['seq'], payload=payload, lifecycle_authority_id=authority,
        ) for payload in fresh)
        await session.flush()
    return {'inserted': len(fresh), 'next_after_seq': seqs[-1]}


async def read_live_preview(
    session: AsyncSession, *, trial: Trial, source: str | None = None,
    after_seq: int = -1, limit: int = 200,
) -> dict[str, Any]:
    """Read only the current attempt; canonical publication supersedes previews."""
    empty: dict[str, Any] = {'source': None, 'events': [], 'next_after_seq': None}
    if await session.scalar(select(TrialEvent.id).where(TrialEvent.trial_id == trial.id).limit(1)):
        return empty
    statement = select(ServiceExecutionLease).where(
        ServiceExecutionLease.trial_id == trial.id,
        ServiceExecutionLease.team_id == trial.team_id,
        ServiceExecutionLease.attempt == trial.attempt_count,
        ServiceExecutionLease.execution_role == 'attempt',
    )
    if trial.state not in _TERMINAL and trial.state != 'materializing':
        statement = statement.where(ServiceExecutionLease.revoked_at.is_(None))
    lease = await session.scalar(statement.order_by(ServiceExecutionLease.created_at.desc()).limit(1))
    if lease is None:
        return empty
    current_source = f'{lease.id}:{lease.resource_generation}'
    cursor = after_seq if source == current_source else -1
    events = list((await session.scalars(select(ServiceExecutionLiveEvent.payload).where(
        ServiceExecutionLiveEvent.trial_id == trial.id,
        ServiceExecutionLiveEvent.lease_id == lease.id,
        ServiceExecutionLiveEvent.resource_generation == lease.resource_generation,
        ServiceExecutionLiveEvent.seq > cursor,
    ).order_by(ServiceExecutionLiveEvent.seq).limit(limit))).all())
    return {'source': current_source, 'events': events,
            'next_after_seq': events[-1]['seq'] if events else None}
