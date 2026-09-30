"""One database-wide idle rollout guard for the independent Nebius platform.

Admission holds a shared transaction lock until its reservation commits. Rollout
tries the exclusive lock once, checks durable work and persists its owner. No
waiting, expiry or background retry: a crashed deployment stays paused for recovery.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

# Separate from capacity/source-journal locks; acquisition never waits.
LOCK_KEY = 731946021


async def admission_open(session: AsyncSession) -> bool:
    locked = await session.scalar(text(
        "SELECT pg_try_advisory_xact_lock_shared(:key)"
    ), {"key": LOCK_KEY})
    if not locked:
        return False
    return not bool(await session.scalar(text("SELECT EXISTS (SELECT 1 FROM nebius_rollout_guard)")))


async def acquire(session: AsyncSession, *, owner: str, candidate: str) -> dict[str, Any]:
    if not await session.scalar(text("SELECT pg_try_advisory_xact_lock(:key)"), {"key": LOCK_KEY}):
        return {"status": "skipped_busy", "reason": "admission_in_progress"}
    existing = (await session.execute(text("SELECT owner FROM nebius_rollout_guard"))).first()
    if existing:
        return {"status": "skipped_locked", "reason": "deployment_or_recovery_in_progress"}
    # Count reservations before Pods exist, result processing after Pods stop,
    # and native build cleanup even if the materialization is already ready.
    counts = dict((await session.execute(text("""
        SELECT
          (SELECT count(*) FROM trials WHERE state IN ('claimed','running')) AS trials,
          (SELECT count(*) FROM execution_leases
            WHERE revoked_at IS NULL OR cleanup_state <> 'complete'
               OR materialization_state IN ('pending','running')) AS executions,
          (SELECT count(*) FROM task_image_materializations
            WHERE state IN ('claimed','running')) AS builds,
          (SELECT count(*) FROM task_image_materialization_attempts
            WHERE native_build IS NOT NULL
              AND native_build->>'capacity_released_at' IS NULL) AS build_cleanup
    """))).mappings().one())
    if any(counts.values()):
        return {"status": "skipped_busy", "active": counts}
    await session.execute(text("""
        INSERT INTO nebius_rollout_guard (id, owner, candidate_sha)
        VALUES (1, :owner, :candidate)
    """), {"owner": owner, "candidate": candidate})
    return {"status": "acquired", "active": counts}


async def release(session: AsyncSession, *, owner: str, candidate: str) -> dict[str, Any]:
    # Recovery belongs to the exact paused rollout, including its candidate.
    result = await session.execute(text(
        "DELETE FROM nebius_rollout_guard "
        "WHERE id = 1 AND owner = :owner AND candidate_sha = :candidate RETURNING id"
    ), {"owner": owner, "candidate": candidate})
    if result.scalar_one_or_none() is None:
        raise ValueError("rollout guard owner or candidate does not match")
    return {"status": "released"}


async def observe(session: AsyncSession, *, owner: str, candidate: str) -> dict[str, Any]:
    """Read recovery ownership without acquiring, stealing or clearing a pause."""
    existing = (await session.execute(text(
        "SELECT owner, candidate_sha FROM nebius_rollout_guard WHERE id = 1"
    ))).first()
    if existing is None:
        return {"status": "open"}
    return {"status": "held" if existing.owner == owner and existing.candidate_sha == candidate else "skipped_locked"}


async def _run(args: argparse.Namespace) -> dict[str, Any]:
    from loom_control_plane.config import ControlPlaneSettings

    settings = ControlPlaneSettings()
    engine = create_async_engine(settings.db_engine_url, connect_args=settings.db_engine_connect_args)
    try:
        async with AsyncSession(engine) as session, session.begin():
            if args.action == "acquire":
                return await acquire(session, owner=args.owner, candidate=args.candidate)
            if args.action == "observe":
                return await observe(session, owner=args.owner, candidate=args.candidate)
            return await release(session, owner=args.owner, candidate=args.candidate)
    finally:
        await engine.dispose()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("acquire", "release", "observe"))
    parser.add_argument("--owner", required=True)
    parser.add_argument("--candidate", required=True)
    args = parser.parse_args()
    if not args.candidate:
        parser.error("--candidate must not be empty")
    try:
        print(json.dumps(asyncio.run(_run(args))))
    except Exception:
        # DB/driver errors may contain credentials. Never serialize them.
        print("Rollout guard unavailable; no automatic deployment or resume", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
