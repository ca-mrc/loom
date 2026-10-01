"""The guarded Pod command reaches the actual durable PostgreSQL idle barrier."""
from __future__ import annotations

import asyncio
import os
import subprocess
import sys

import psycopg
import pytest
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from loom.nebius_rollout_guard import acquire, admission_open, release
from tests.ops.test_nebius_pool_database_guard import database_guard as database_guard
from tests.ops.test_nebius_pool_migration_guard import guard_runtime as guard_runtime
from tests.ops.test_nebius_pool_runtime import runtime_inputs as runtime_inputs
from tests.unit.test_nebius_management_render import management_inputs as management_inputs
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


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


@pytest.mark.parametrize('foreign', [None, 'owner', 'candidate'])
def test_retired_controller_observer_reads_actual_guard_without_mutating(database_guard, isolated_migration_postgres_url, foreign):
    api, state = database_guard
    owner = str(state.request.registration.spec.operation_id)
    candidate = state.request.registration.candidate['candidate_sha']
    held_owner = 'other-owner' if foreign == 'owner' else owner
    held_candidate = 'f' * 40 if foreign == 'candidate' else candidate
    url = make_url(isolated_migration_postgres_url).set(drivername='postgresql').render_as_string(hide_password=False)

    async def hold(*, cleanup=False):
        engine = create_async_engine(isolated_migration_postgres_url)
        try:
            async with AsyncSession(engine) as session, session.begin():
                operation = release if cleanup else acquire
                return await operation(session, owner=held_owner, candidate=held_candidate)
        finally:
            await engine.dispose()

    with psycopg.connect(url, autocommit=True) as connection:
        def execute(query):
            with connection.cursor() as cursor:
                cursor.execute(query, prepare=False)
                values = []
                while True:
                    if cursor.description:
                        values.extend(cursor.fetchall())
                    if not cursor.nextset():
                        break
                assert len(values) == 1
                return values[0][0]

        state.exec_hook = execute
        assert api.guard(state.target, 'observe') == {'status': 'open'}
        try:
            assert asyncio.run(hold())['status'] == 'acquired'
            before = connection.execute('SELECT * FROM public.nebius_rollout_guard').fetchall()
            assert api.guard(state.target, 'observe') == {'status': 'held' if foreign is None else 'skipped_locked'}
            assert connection.execute('SELECT * FROM public.nebius_rollout_guard').fetchall() == before
        finally:
            asyncio.run(hold(cleanup=True))
        assert api.guard(state.target, 'observe') == {'status': 'open'}
