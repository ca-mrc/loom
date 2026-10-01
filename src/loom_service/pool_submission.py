"""Stamp new work from protected service configuration, never HTTP metadata."""
from __future__ import annotations

from typing import Any
from uuid import UUID, uuid4

from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import NebiusPoolSubmission
from loom.nebius_submission import submission_payload_digest
from loom_service.config import LoomServiceSettings


def submission_origin(settings: LoomServiceSettings, submission_id: UUID) -> dict[str, Any] | None:
    source = settings.pool_submission_source
    # Old work/configuration has unknown provenance. Global admission must not
    # interpret this NULL as an environment-class origin.
    return None if source is None else source.origin(submission_id).model_dump(mode="json")


async def prepare_trial_submission(session: AsyncSession, settings: LoomServiceSettings, *,
                                   team_id: UUID | None, user_id: UUID | None,
                                   payload: dict[str, Any]) -> tuple[dict[str, Any], UUID | None]:
    """Caller commits this provenance and releases auth locks before forwarding."""
    identity = uuid4()
    origin = submission_origin(settings, identity)
    if origin is None:
        return payload, None
    if team_id is None or user_id is None:
        raise ValueError("submission requires a team and submitting user")
    forwarded = dict(payload)
    if forwarded.get("idempotency_key") is None:
        forwarded["idempotency_key"] = "pool-submission:" + str(identity)
    session.add(NebiusPoolSubmission(id=identity, team_id=team_id, user_id=user_id,
        request_sha256=submission_payload_digest(forwarded), pool_origin=origin))
    await session.flush()
    return forwarded, identity
