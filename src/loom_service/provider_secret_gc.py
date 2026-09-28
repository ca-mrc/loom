"""Bounded, transactional reclamation of retired provider API keys.

Only explicit retirement or a retained soft-deleted owner establishes ownership.
A team namespace alone is never evidence that an old orphan is disposable.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import timedelta
from uuid import UUID

from sqlalchemy import func, select, text, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from loom.db.schema import Secret
from loom.security.secret_store import InvalidRefError, parse_ref

logger = logging.getLogger(__name__)
PROVIDER_SECRET_GRACE = timedelta(hours=24)
_BATCH_SIZE = 100
_POLL_SECONDS = 60

# Keep aligned with reference-attachment guards and native Secret foreign keys. Retain
# generic references regardless of historical consumer state, including terminal
# stage runs. Soft-deleted provider records retain attribution, not credentials.
_REFERENCED_SQL = """
    SELECT EXISTS (
        SELECT 1 FROM provider_connections
        WHERE encrypted_api_key_ref = :ref AND deleted_at IS NULL
        UNION ALL SELECT 1 FROM dev_instances WHERE secret_ref = :ref
        UNION ALL SELECT 1 FROM task_image_build_projections
        WHERE bootstrap_secret_ref = :ref OR session_secret_ref = :ref
        UNION ALL SELECT 1 FROM task_image_build_session_generations
        WHERE session_secret_ref = :ref
        UNION ALL SELECT 1 FROM pipeline_stage_runs
        WHERE :ref = ANY(secret_refs)
        UNION ALL SELECT 1 FROM nebius_application_material WHERE secret_ref = :ref
    )
"""


async def retire_provider_secret(session: AsyncSession, *, ref: str, team_id: UUID) -> None:
    """Record retirement in the caller's transaction; never delete inline."""
    try:
        parsed = parse_ref(ref)
    except InvalidRefError:
        return
    if parsed.namespace != f"team:{team_id}" or parsed.as_string() != ref:
        return
    # Reset the grace when a shared ref loses another owner. The timestamp is
    # independent of secret creation time and survives process restarts.
    await session.execute(
        update(Secret).where(Secret.ref == ref).values(provider_retired_at=func.now()),
    )


async def collect_provider_secrets(session: AsyncSession, *, batch_size: int = _BATCH_SIZE) -> int:
    """Run one bounded pass; caller owns commit/rollback and cancellation.

    Attachment triggers take KEY SHARE on the exact secret; FOR UPDATE here
    conflicts with those locks. The subsequent READ COMMITTED reference check
    sees writers that committed before the lock. A later attachment waits and
    rejects a missing secret. No reference table or unrelated secret is locked.
    """
    if not 1 <= batch_size <= 1000:
        raise ValueError("batch_size must be between 1 and 1000")
    if await session.scalar(text("SHOW transaction_isolation")) != "read committed":
        raise RuntimeError("provider secret collection requires READ COMMITTED")
    await session.execute(text("SET LOCAL lock_timeout = '1s'"))
    await session.execute(text("SET LOCAL statement_timeout = '10s'"))
    # Legacy soft-deleted connections supply positive ownership evidence. Mark
    # discovery NOW, never backdate to created_at/deleted_at. Old rotation
    # orphans lack such evidence and are deliberately outside this collector.
    await session.execute(text("""
        WITH discovered AS (
            SELECT s.ref FROM secrets s
            WHERE s.provider_retired_at IS NULL AND EXISTS (
                SELECT 1 FROM provider_connections p
                WHERE p.deleted_at IS NOT NULL
                  AND p.encrypted_api_key_ref = s.ref
                  AND s.ref ~ ('^loom://team:' || p.team_id::text ||
                      '/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')
            )
            ORDER BY s.ref LIMIT :batch_size FOR UPDATE OF s SKIP LOCKED
        )
        UPDATE secrets SET provider_retired_at = now()
        FROM discovered WHERE secrets.ref = discovered.ref
    """), {"batch_size": batch_size})
    candidates = (await session.scalars(
        select(Secret).where(
            Secret.provider_retired_at <= func.now() - PROVIDER_SECRET_GRACE,
        ).order_by(Secret.provider_retired_at, Secret.ref)
        .limit(batch_size).with_for_update(skip_locked=True)
        .execution_options(populate_existing=True),
    )).all()
    deleted = 0
    for secret in candidates:
        referenced = await session.scalar(text(_REFERENCED_SQL), {"ref": secret.ref})
        if referenced:
            # Preserve ownership evidence, refresh grace, and avoid starving
            # later candidates behind an indefinitely retained historical ref.
            secret.provider_retired_at = await session.scalar(select(func.now()))
        else:
            await session.delete(secret)
            deleted += 1
    await session.flush()
    return deleted


async def run_loop(*, session_factory: async_sessionmaker[AsyncSession]) -> None:
    """Each worker may run this loop; row claims coordinate overlapping passes."""
    while True:
        try:
            # Bound total pass duration too, rather than 100 statement timeouts.
            async with asyncio.timeout(15):
                async with session_factory.begin() as session:
                    deleted = await collect_provider_secrets(session)
            if deleted:
                logger.info("provider_secret_gc deleted=%d", deleted)
        except Exception as exc:
            # Context-manager rollback also applies to cancellation (which is
            # not caught here). Never log ciphertext, values, or secret refs.
            logger.warning("provider_secret_gc pass failed error_type=%s", type(exc).__name__)
        await asyncio.sleep(_POLL_SECONDS)
