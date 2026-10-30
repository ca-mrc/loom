"""Fill the 0182 usage projection on `llm_calls` rows written before it (#2687).

Run in the Service environment. The default only counts remaining rows;
`--apply` updates them in short, separately committed primary-key chunks. Safe
to stop and rerun: projected rows are skipped without reading their raw-log JSON,
and readers resolve an unprojected row to the same values.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import time
from collections.abc import Callable
from typing import cast
from uuid import UUID

from sqlalchemy import CursorResult, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from loom_llm_gateway.dialect import USAGE_STATUS_KEY
from loom_llm_gateway.rate_card import COST_META_CONFIDENCE_KEY, COST_META_SOURCE_KEY
from loom_service.config import LoomServiceSettings

_UNPROJECTED = "(cost_source IS NULL OR cost_confidence IS NULL OR usage_status IS NULL)"
_COUNT = text(f"SELECT count(*) FROM llm_calls WHERE {_UNPROJECTED}")
_CHUNK_END = text(
    "SELECT id FROM llm_calls WHERE (CAST(:after AS uuid) IS NULL OR id > :after) "
    "ORDER BY id LIMIT 1 OFFSET :offset",
)
# Mirrors `usage_projection_columns`: `->>` text, with '' for an absent or null key.
_PROJECT = text(
    f"""
    UPDATE llm_calls SET
        cost_source = COALESCE(cost_source, provider_extras->>:source_key, ''),
        cost_confidence = COALESCE(cost_confidence, provider_extras->>:confidence_key, ''),
        usage_status = COALESCE(usage_status, provider_extras->>:status_key, '')
    WHERE (CAST(:after AS uuid) IS NULL OR id > :after)
      AND (CAST(:until AS uuid) IS NULL OR id <= :until)
      AND {_UNPROJECTED}
    """,
)
_KEYS = {
    "source_key": COST_META_SOURCE_KEY,
    "confidence_key": COST_META_CONFIDENCE_KEY,
    "status_key": USAGE_STATUS_KEY,
}


async def count_unprojected(session: AsyncSession) -> int:
    return int((await session.execute(_COUNT)).scalar_one())


async def backfill(
    factory: async_sessionmaker[AsyncSession],
    *,
    chunk_size: int = 500,
    min_pause_seconds: float = 0.2,
    report: Callable[[dict[str, object]], None] = lambda _progress: None,
) -> int:
    """Project every remaining row and return how many were updated.

    Each chunk pauses for at least as long as it ran, so the backfill takes at
    most about half of a small Postgres during live batches. Lock and statement
    timeouts make it give way to other work; rerun to continue after one fires.
    """
    updated = 0
    after: UUID | None = None
    while True:
        started = time.monotonic()
        async with factory() as session, session.begin():
            await session.execute(text("SET LOCAL lock_timeout = '2s'"))
            await session.execute(text("SET LOCAL statement_timeout = '60s'"))
            until = (
                await session.execute(_CHUNK_END, {"after": after, "offset": chunk_size - 1})
            ).scalar_one_or_none()
            result = await session.execute(_PROJECT, {"after": after, "until": until, **_KEYS})
        updated += cast("CursorResult[object]", result).rowcount
        report({"updated": updated, "through_id": str(until) if until else "end"})
        if until is None:
            return updated
        after = until
        await asyncio.sleep(max(min_pause_seconds, time.monotonic() - started))


async def _main(args: argparse.Namespace) -> None:
    settings = LoomServiceSettings()
    engine = create_async_engine(settings.db_engine_url, connect_args=settings.db_engine_connect_args)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        if args.apply:
            await backfill(
                factory,
                chunk_size=args.chunk_size,
                min_pause_seconds=args.min_pause_seconds,
                report=lambda progress: print(json.dumps(progress), flush=True),
            )
        async with factory() as session:
            print(json.dumps({"remaining": await count_unprojected(session)}), flush=True)
    finally:
        await engine.dispose()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="update rows; default only counts them")
    parser.add_argument("--chunk-size", type=int, default=500)
    parser.add_argument("--min-pause-seconds", type=float, default=0.2)
    asyncio.run(_main(parser.parse_args()))
