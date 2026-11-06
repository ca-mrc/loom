"""Shared Control Plane authority for trial cancellation."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any
from uuid import UUID

from fastapi import Request
from sqlalchemy import func, select, text, update

from loom.auth import AuthContext
from loom.batch_cancellation import trial_is_retained
from loom.db.schema import Trial
from loom.security.redaction import redact_text
from loom_control_plane.service_execution import request_trial_execution_cancellation
from loom_service.admin_audit import actor_from_context, write_admin_audit_event

_REQUEST_CANCEL_SQL = text("""
UPDATE trials
   SET state = CASE WHEN state IN ('queued', 'protected-pending')
                     OR (state = 'materializing' AND jsonb_typeof(result->'native_retry') = 'object')
                   THEN 'cancelled'
                    ELSE state END,
       cancellation_requested_at = COALESCE(cancellation_requested_at, NOW()),
       cancellation_observed_at = CASE
           WHEN state IN ('queued', 'protected-pending')
             AND jsonb_typeof(result->'native_retry') IS DISTINCT FROM 'object'
           THEN NOW()
           ELSE cancellation_observed_at END,
       finished_at = CASE
           WHEN state IN ('queued', 'protected-pending')
             OR (state = 'materializing' AND jsonb_typeof(result->'native_retry') = 'object')
           THEN COALESCE(finished_at, NOW())
           ELSE finished_at END,
       result = CASE
           WHEN state IN ('queued', 'materializing') AND jsonb_typeof(result->'native_retry') = 'object'
           THEN result || jsonb_build_object('cancelled', true) ELSE result END,
       failure_reason = CASE
           WHEN state IN ('queued', 'materializing') AND jsonb_typeof(result->'native_retry') = 'object'
           THEN 'cancelled' ELSE failure_reason END,
       failure_message = CASE
           WHEN state IN ('queued', 'materializing') AND jsonb_typeof(result->'native_retry') = 'object'
           THEN 'service execution cancelled before model retry' ELSE failure_message END,
       next_attempt_at = CASE
           WHEN state IN ('queued', 'materializing') AND jsonb_typeof(result->'native_retry') = 'object'
           THEN NULL ELSE next_attempt_at END,
       cancellation_retained_at = NULL
 WHERE id = (:trial_id)::uuid
   AND ((:team_id)::uuid IS NULL OR team_id = (:team_id)::uuid)
   AND (state IN ('queued', 'protected-pending', 'claimed', 'running')
        OR (state = 'materializing' AND jsonb_typeof(result->'native_retry') = 'object'))
   AND (cancellation_requested_at IS NULL OR state IN ('queued', 'protected-pending'))
 RETURNING id, state;
""")

_READ_CANCEL_REPLAY_SQL = text("""
SELECT id, state
  FROM trials
 WHERE id = (:trial_id)::uuid
   AND ((:team_id)::uuid IS NULL OR team_id = (:team_id)::uuid)
   AND cancellation_requested_at IS NOT NULL
   AND state IN ('claimed', 'running', 'cancelled');
""")


async def _retain_trial(
    session: Any,
    *,
    trial_id: UUID,
    team_id: UUID | None,
) -> Mapping[str, Any] | None:
    filters = [
        Trial.id == trial_id,
        Trial.state.notin_(("succeeded", "failed", "cancelled")),
        Trial.cancellation_requested_at.is_(None),
        trial_is_retained(),
    ]
    if team_id is not None:
        filters.append(Trial.team_id == team_id)
    retained = (
        await session.execute(
            select(Trial.id, Trial.state).where(*filters).with_for_update()
        )
    ).first()
    if retained is None:
        return None
    await session.execute(
        update(Trial)
        .where(Trial.id == trial_id)
        .values(cancellation_retained_at=func.coalesce(Trial.cancellation_retained_at, func.now()))
    )
    return {"id": retained.id, "state": retained.state, "outcome": "retained"}


async def cancel_trial_under_authority(
    *,
    session_factory: Any,
    trial_id: UUID,
    team_id: UUID | None,
    admin_context: AuthContext | None = None,
    reason: str | None = None,
    request: Request | None = None,
    preserve_retained: bool = False,
) -> Mapping[str, Any] | None:
    """Request cancellation and fence native execution in one transaction."""
    async with session_factory() as session:
        actor = None
        if admin_context is not None:
            actor = await actor_from_context(
                session, admin_context,
                request.headers.get("X-Loom-Admin-Actor") if request else None,
            )
        row: Mapping[str, Any] | None = None
        if preserve_retained:
            row = await _retain_trial(session, trial_id=trial_id, team_id=team_id)
        if row is None:
            row = (
                (await session.execute(
                    _REQUEST_CANCEL_SQL, {"trial_id": trial_id, "team_id": team_id},
                )).mappings().one_or_none()
            )
        if row is None:
            row = (
                (await session.execute(
                    _READ_CANCEL_REPLAY_SQL, {"trial_id": trial_id, "team_id": team_id},
                )).mappings().one_or_none()
            )
        if row is not None and row.get("outcome") != "retained":
            await request_trial_execution_cancellation(session, trial_id=trial_id)
            if actor is not None:
                await write_admin_audit_event(
                    session, actor=actor, action="trial.cancel",
                    target_type="trial", target_id=str(trial_id), request=request,
                    metadata={"reason": redact_text(reason) if reason else None,
                              "state": row["state"]},
                )
        elif row is not None and actor is not None:
            await write_admin_audit_event(
                session, actor=actor, action="trial.cancel.retain",
                target_type="trial", target_id=str(trial_id), request=request,
                metadata={"reason": redact_text(reason) if reason else None,
                          "state": row["state"]},
            )
        await session.commit()
    return row
