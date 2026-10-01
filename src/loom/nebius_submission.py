"""Qualify a service-written origin against the actual authenticated CP request."""
from __future__ import annotations

import hashlib
import json
from typing import Any
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import NebiusPoolSubmission
from loom.nebius_pool_priority import PoolWorkOriginV1


def submission_payload_digest(payload: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"),
                                    allow_nan=False).encode()).hexdigest()


async def qualify_trial_submission(session: AsyncSession, submission_id: str, *,
                                   team_id: UUID | None, user_id: UUID | None,
                                   payload: dict[str, Any]) -> PoolWorkOriginV1:
    """Never infer provenance from caller JSON, a parent batch, or the header alone.

    Personal backends are already trusted writers of the shared development DB.
    This binds that same boundary for direct submissions without dispatch tokens.
    An immutable row needs no held lock or transaction across the network hop.
    """
    if len(submission_id) != 36 or team_id is None or user_id is None:
        raise ValueError("submission handoff unavailable")
    identity = UUID(submission_id)
    row = await session.get(NebiusPoolSubmission, identity)
    if (row is None or row.team_id != team_id or row.user_id != user_id
            or not isinstance(payload.get("idempotency_key"), str)
            or row.request_sha256 != submission_payload_digest(payload)):
        raise ValueError("submission handoff unavailable")
    origin = PoolWorkOriginV1.model_validate(row.pool_origin)
    if origin.submission_id != identity or origin.kind == "personal_build":
        raise ValueError("submission handoff unavailable")
    return origin
