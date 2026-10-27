"""Provider-model checks shared by Batch and single-Trial submission."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

from sqlalchemy import select

from loom.db.schema import ProviderConnection, ProviderModelCache
from loom_service.agent_catalog import get_agent
from loom_service.execution_admission import reject_submission
from loom_service.provider_connections_service import preflight_failure_kind


async def reject_if_known_failed_provider_model(
    session: Any,
    *,
    provider_connection_id: UUID | None,
    provider_model_id: str | None,
    context: str | None = None,
) -> None:
    if provider_connection_id is None or not provider_model_id:
        return
    row = (
        await session.execute(
            select(ProviderModelCache).where(
                ProviderModelCache.provider_connection_id == provider_connection_id,
                ProviderModelCache.model_id == provider_model_id,
            ),
        )
    ).scalar_one_or_none()
    if row is None:
        prefix = f"{context}: " if context else ""
        reject_submission(
            reason="provider_model_cache",
            status_code=400,
            detail=(
                f"{prefix}provider model {provider_model_id!r} is not in the model cache "
                "for this provider connection; run `loom providers models "
                "NAME --refresh` or choose a cached model"
            ),
        )
    # Only a definitive rejection blocks (#948). A timed-out or otherwise
    # inconclusive probe is not evidence the model is unusable; the run
    # itself surfaces a real failure if there is one.
    if preflight_failure_kind(
        row.last_preflight_status,
        row.last_preflight_error_code,
        row.last_preflight_http_status,
    ) != "rejected":
        return
    prefix = f"{context}: " if context else ""
    detail = f"{prefix}provider model {provider_model_id!r} last preflight failed for this provider connection"
    if row.last_preflight_error_code:
        detail += f" ({row.last_preflight_error_code})"
    if row.last_preflight_at is not None:
        detail += f" at {row.last_preflight_at.isoformat()}"
    detail += "; run provider model preflight again or choose another model"
    reject_submission(
        reason="provider_model_preflight",
        status_code=400,
        detail=detail,
    )


# Codex retries an unroutable model inside the run before it fails (#2054).
# Its selections need preflight evidence no older than this, so a model the
# upstream cannot serve is rejected before any workload is dispatched.
_CODEX_PREFLIGHT_TTL = timedelta(hours=24)


def is_codex(agent_name: object) -> bool:
    entry = get_agent(agent_name) if isinstance(agent_name, str) else None
    return entry is not None and entry.name == "codex"


async def refresh_codex_model_preflight(
    session_factory: Any,
    *,
    provider_connection_id: UUID | None,
    provider_model_id: str | None,
) -> None:
    """Preflight a Codex model without fresh evidence, before the gate reads it.

    The result commits in its own transaction so it outlives the rejection it
    may cause.
    """
    from loom_service.routes.provider_connections import run_cached_model_preflight

    if provider_connection_id is None or not provider_model_id:
        return
    async with session_factory() as session:
        cache_row = (
            await session.execute(
                select(ProviderModelCache).where(
                    ProviderModelCache.provider_connection_id == provider_connection_id,
                    ProviderModelCache.model_id == provider_model_id,
                ),
            )
        ).scalar_one_or_none()
        if cache_row is None:
            return  # The gate rejects an uncached model with its own message.
        checked_at = cache_row.last_preflight_at
        if checked_at is not None and datetime.now(UTC) - checked_at < _CODEX_PREFLIGHT_TTL:
            return
        connection = await session.get(ProviderConnection, provider_connection_id)
        if connection is None:
            return
        await run_cached_model_preflight(session, connection, cache_row)
        await session.commit()
