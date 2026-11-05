"""Idle checks retain every cleanup obligation without reading released manifests."""

import json
import random
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import null, text, update

from loom.db.schema import TaskImageMaterialization, TaskImageMaterializationAttempt
from loom.nebius_rollout_guard import ACTIVITY_SQL, acquire, release
from tests.integration.test_nebius_task_image_capacity import _native
from tests.integration.test_nebius_task_image_capacity import native_setup as native_setup
from tests.integration.test_service_execution_leases import (
    _cleanup_service_execution_test_rows,  # noqa: F401 -- disposable fixture cleanup
    _seed_ready_trial,
)

REFERENCE_CLEANUP_SQL = """SELECT count(*) FROM task_image_materialization_attempts
    WHERE native_build IS NOT NULL AND native_build->>'capacity_released_at' IS NULL"""


async def _finished_build(session, owned, pair, now):
    attempt_id, materialization_id = await _native(
        session, owned, pair, now, checksum=uuid4().hex * 2,
    )
    materialization = await session.get(TaskImageMaterialization, materialization_id)
    materialization.state = "failed"
    return await session.get(TaskImageMaterializationAttempt, attempt_id)


async def test_idle_guard_skips_toasted_released_build_history(native_setup):
    sessions, owned = native_setup
    now = datetime.now(UTC)
    past = now - timedelta(hours=2)
    opaque = random.Random(0).randbytes(40_000).hex()
    async with sessions() as session, session.begin():
        pair = await _seed_ready_trial(session, now=now)
        for _ in range(120):
            attempt = await _finished_build(session, owned, pair, past)
            attempt.native_build = {**attempt.native_build,
                "capacity_released_at": past.isoformat(), "job": {"opaque_manifest": opaque}}
    async with sessions() as session, session.begin():
        await session.execute(text("ANALYZE task_image_materialization_attempts"))
        actual = (await session.execute(text(
            "EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) " + ACTIVITY_SQL,
        ))).scalar_one()
        reference = (await session.execute(text(
            "EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) " + REFERENCE_CLEANUP_SQL,
        ))).scalar_one()

        def blocks(plan):
            parsed = json.loads(plan) if isinstance(plan, str) else plan
            root = parsed[0]["Plan"]
            return root.get("Shared Hit Blocks", 0) + root.get("Shared Read Blocks", 0)

        active = dict((await session.execute(text(ACTIVITY_SQL))).mappings().one())
        assert active == {"trials": 0, "executions": 0, "builds": 0, "build_cleanup": 0}
        assert blocks(actual) * 4 < blocks(reference), (
            "the complete idle check must not detoast released historical Job manifests",
            blocks(actual), blocks(reference),
        )


@pytest.mark.parametrize("payload,expected", [
    ("sql_null", 0),
    (None, 1),  # JSON null is non-SQL-null: retain the existing conservative count.
    ({}, 1),
    ({"capacity_released_at": None}, 1),
    ({"job": {"name": "never-reserved-but-needs-cleanup"}}, 1),
    ({"capacity_reserved_at": "2026-01-01T00:00:00+00:00"}, 1),
    ({"capacity_released_at": "2026-01-01T00:00:00+00:00"}, 0),
    ({"capacity_released_at": ""}, 0),
])
async def test_projection_preserves_cleanup_counts_and_atomic_guard(native_setup, payload, expected):
    sessions, owned = native_setup
    now = datetime.now(UTC)
    async with sessions() as session, session.begin():
        pair = await _seed_ready_trial(session, now=now)
        attempt = await _finished_build(session, owned, pair, now)
        if payload == "sql_null":
            await session.execute(update(TaskImageMaterializationAttempt).where(
                TaskImageMaterializationAttempt.id == attempt.id,
            ).values(native_build=null()))
        else:
            attempt.native_build = payload
        await session.flush()
        reference = await session.scalar(text(REFERENCE_CLEANUP_SQL))
        active = dict((await session.execute(text(ACTIVITY_SQL))).mappings().one())
        assert active["build_cleanup"] == reference == expected
        decision = await acquire(session, owner="projection-test", candidate="a" * 40)
        if expected:
            assert decision == {"status": "skipped_busy", "active": active}
        else:
            assert decision["status"] == "acquired"
            await release(session, owner="projection-test", candidate="a" * 40)
