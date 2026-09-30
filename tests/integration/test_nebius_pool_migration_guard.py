"""The guarded Pod command reaches the actual durable PostgreSQL idle barrier."""
from __future__ import annotations

import asyncio
import os
import subprocess
import sys

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from loom.nebius_rollout_guard import admission_open, release
from tests.ops.test_nebius_pool_migration_guard import guard_runtime as guard_runtime


@pytest.mark.parametrize("lose_reply", [False, True])
def test_fixed_transport_invokes_real_idle_guard_and_retains_lost_commit(guard_runtime, isolated_migration_postgres_url, lose_reply):
    from scripts.ops.nebius_pool_migration import PoolMigrationError

    api, state = guard_runtime
    owner = str(state.request.registration.spec.operation_id)
    env = {**os.environ, "LOOM_CP_DB_URL": isolated_migration_postgres_url,
        "LOOM_CP_MINIO_ACCESS_KEY": "unused", "LOOM_CP_MINIO_SECRET_KEY": "unused",
        "LOOM_CP_STEP_JWT_SIGNING_KEY": "unused-migration-test"}
    acquires = []

    def execute(args):
        command = [sys.executable, *args[args.index("--") + 2:]]
        result = state.subprocess_run(command, capture_output=True, timeout=30, check=False, env=env)
        if "acquire" in command:
            acquires.append(command)
            if lose_reply:
                assert result.returncode == 0
                raise subprocess.TimeoutExpired("fixed guard", 30)
        return result

    state.exec_hook = execute

    async def database(*, cleanup=False):
        engine = create_async_engine(isolated_migration_postgres_url)
        try:
            async with AsyncSession(engine) as session, session.begin():
                opened = await admission_open(session)
                if cleanup and not opened:
                    await release(session, owner=owner, candidate=state.request.registration.candidate["candidate_sha"])
                return opened
        finally:
            await engine.dispose()

    try:
        assert api.guard(state.target, "observe") == {"status": "open"}
        if lose_reply:
            with pytest.raises(PoolMigrationError):
                api.guard(state.target, "acquire")
        else:
            assert api.guard(state.target, "acquire") == {"status": "acquired"}
        assert asyncio.run(database()) is False
        assert api.guard(state.target, "observe") == {"status": "held"}
        assert len(acquires) == 1
        with pytest.raises(PoolMigrationError):
            api.guard(state.target, "release")
        assert asyncio.run(database()) is False
    finally:
        asyncio.run(database(cleanup=True))
