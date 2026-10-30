"""Validate a `provider_connection_id` on a submitted Trial / Batch
payload.

The Trial / Batch routes accept an optional `provider_connection_id`.
The user supplies a UUID; the route must check that:

1. The UUID parses (handled by Pydantic before this is called).
2. The row exists.
3. The row belongs to the caller's team or is explicitly shared with
   that team (unshared cross-team → 404, matching provider route +
   gateway existence-hiding behavior).
4. The row is not soft-deleted (deleted_at IS NULL).
5. For agent submissions, the row speaks the OpenAI wire protocol.
   Native Anthropic/Google connections are not admitted for new hosted
   agent submissions in this phase (#2054).

Centralized here so the Trial and Batch routes use the same shape +
error messages.
"""

from __future__ import annotations

from typing import Any
from uuid import UUID

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import ProviderConnection, ProviderConnectionShare

# Provider types the gateway routes with the OpenAI wire protocol (see
# loom_llm_gateway.routes.responses._OPENAI_SHAPED_TYPES).
OPENAI_SHAPED_PROVIDER_TYPES = frozenset({"openai-compatible", "custom"})


async def validate_provider_connection(
    session: AsyncSession,
    provider_connection_id: UUID,
    *,
    team_id: UUID,
    agent_submission: bool = False,
    submission_configs: list[dict[str, Any]] | None = None,
) -> None:
    """Raise HTTPException if the connection is missing, soft-deleted,
    or owned by a different team. With `agent_submission`, also reject
    connections that don't speak the OpenAI wire protocol. Returns None
    on success."""
    row = (await session.execute(
        select(
            ProviderConnection.team_id,
            ProviderConnection.deleted_at,
            ProviderConnection.provider_type,
        ).where(ProviderConnection.id == provider_connection_id),
    )).one_or_none()
    if row is None:
        raise HTTPException(
            status_code=400,
            detail=(
                f"provider_connection {provider_connection_id} not found. "
                f"Run `loom providers list` to see what's available."
            ),
        )
    found_team_id, deleted_at, provider_type = row
    if deleted_at is not None:
        raise HTTPException(
            status_code=400,
            detail=(
                f"provider_connection {provider_connection_id} has been "
                f"deleted. Re-create with `loom providers create` or "
                f"pick a different one."
            ),
        )
    if found_team_id != team_id:
        shared = (await session.execute(
            select(ProviderConnectionShare.provider_connection_id).where(
                ProviderConnectionShare.provider_connection_id == provider_connection_id,
                ProviderConnectionShare.target_team_id == team_id,
            ),
        )).scalar_one_or_none()
        if shared is None:
            raise HTTPException(
                status_code=404, detail="provider_connection not found",
            )
    if agent_submission and provider_type not in OPENAI_SHAPED_PROVIDER_TYPES:
        raise HTTPException(
            status_code=400,
            detail=(
                f"provider_connection {provider_connection_id} is a native "
                f"{provider_type!r} connection; hosted agent submissions "
                "accept only OpenAI-compatible Provider Connections"
            ),
        )

    # Pool selection keeps the existing logical connection id, while every
    # physical member must satisfy the new submission's agent/model contract.
    from loom_service.provider_pool_validation import validate_request_pool_submission

    await validate_request_pool_submission(
        session, provider_connection_id, team_id=team_id,
        submission_configs=submission_configs,
    )
