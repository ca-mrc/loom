"""The guarded Pod command reaches the actual durable PostgreSQL idle barrier."""
from __future__ import annotations

import asyncio
import os
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from secrets import token_urlsafe
from uuid import uuid4

import psycopg
import pytest
from sqlalchemy import create_engine, insert, select
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from loom.nebius_rollout_guard import acquire, admission_open, release
from tests.integration.test_nebius_application_schema import migration_access as migration_access
from tests.ops.test_nebius_pool_database_guard import database_guard as database_guard
from tests.ops.test_nebius_pool_migration_guard import guard_runtime as guard_runtime
from tests.ops.test_nebius_pool_runtime import runtime_inputs as runtime_inputs
from tests.unit.test_nebius_management_render import management_inputs as management_inputs
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


@pytest.fixture
def cutover_database(database_guard, isolated_migration_postgres_url):
    api, state = database_guard
    url = make_url(isolated_migration_postgres_url).set(drivername='postgresql').render_as_string(hide_password=False)
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
        engine = create_engine(isolated_migration_postgres_url)
        try:
            yield api, state, connection, engine
        finally:
            engine.dispose()


def seed_cutover_backlog(fixture_state, connection, *, known=True, direct=False, **trial_values):
    from loom.db.schema import Batch, Task, Team, Trial

    participant = next(row for row in fixture_state.request.registration.spec.participants
        if row.participant_id == fixture_state.target.participant_id)
    pool_id = next(row['value'] for row in fixture_state.target.controller['spec']['template']['spec']['containers'][0]['env']
        if row['name'] == 'LOOM_CP_SERVICE_EXECUTION_SCHEDULER_POOL_ID')
    team_id, batch_id, trial_id = uuid4(), uuid4(), uuid4()
    task_id = 'cutover-backlog/' + uuid4().hex
    origin = {'schema_version': 'loom.pool-work-origin.v1', 'data_environment_id': str(participant.environment_id),
        'submission_id': str(trial_id if direct else batch_id), 'kind': 'environment', 'application': None} if known else None
    connection.execute(insert(Team).values(id=team_id, name=str(team_id)))
    connection.execute(insert(Task).values(id=task_id, checksum='a' * 64, config={}))
    if not direct:
        connection.execute(insert(Batch).values(id=batch_id, team_id=team_id, name='cutover-backlog', backend='nebius',
            task_filter={}, trial_config={}, created_by_token_prefix='test', pool_origin=origin))
    connection.execute(insert(Trial).values(**({'id': trial_id, 'team_id': team_id, 'task_id': task_id,
        'batch_id': None if direct else batch_id, 'state': 'queued', 'config': {},
        'requires_caps': {'worker_pool': pool_id, 'backend': 'nebius'}, 'pool_origin': origin} | trial_values)))
    return batch_id, trial_id, origin


def test_cutover_reads_actual_schema_and_empty_backlog_without_database_changes(cutover_database):
    api, state, connection, _ = cutover_database
    before = connection.execute('SELECT * FROM public.nebius_rollout_guard').fetchall()
    assert api.cutover_readiness_page(state.target, after=None) == {
        'status': 'observed', 'schema_revision': '0173', 'rows': []}
    assert connection.execute('SELECT * FROM public.nebius_rollout_guard').fetchall() == before


@pytest.mark.parametrize('direct', [False, True])
def test_cutover_preserves_qualified_future_work_even_during_retry_backoff(cutover_database, direct):
    from loom.db.schema import Trial

    api, state, _, engine = cutover_database
    with engine.begin() as connection:
        batch, trial, origin = seed_cutover_backlog(state, connection, direct=direct,
            next_attempt_at=datetime.now(UTC) + timedelta(days=1))
    report = api.cutover_readiness_page(state.target, after=None)
    assert report['status'] == 'observed'
    assert [(row['key'], row['origin']) for row in report['rows']] == (
        [('trial:' + str(trial), origin)] if direct else
        [('batch:' + str(batch), origin), ('trial:' + str(trial), origin)])
    with engine.connect() as connection:
        assert connection.execute(select(Trial.state, Trial.pool_origin).where(Trial.id == trial)).one() == ('queued', origin)


@pytest.mark.parametrize('direct', [False, True])
def test_cutover_refuses_unknown_future_origin_without_backfill_or_cancellation(cutover_database, direct):
    from scripts.ops.nebius_pool_migration import PoolMigrationError

    from loom.db.schema import Trial

    api, state, _, engine = cutover_database
    with engine.begin() as connection:
        _, trial, _ = seed_cutover_backlog(state, connection, known=False, direct=direct,
            next_attempt_at=datetime.now(UTC) + timedelta(days=1))
    with pytest.raises(PoolMigrationError):
        api.cutover_readiness_page(state.target, after=None)
    with engine.connect() as connection:
        assert connection.execute(select(Trial.state, Trial.pool_origin).where(Trial.id == trial)).one() == ('queued', None)


def test_cutover_detects_unfanned_batch_before_any_trial_exists(cutover_database):
    from scripts.ops.nebius_pool_migration import PoolMigrationError

    from loom.db.schema import Batch, Team

    api, state, _, engine = cutover_database
    identity, team = uuid4(), uuid4()
    with engine.begin() as connection:
        connection.execute(insert(Team).values(id=team, name=str(team)))
        connection.execute(insert(Batch).values(id=identity, team_id=team, name='not-fanned-out', backend='nebius',
            task_filter={}, trial_config={}, created_by_token_prefix='test'))
    with pytest.raises(PoolMigrationError):
        api.cutover_readiness_page(state.target, after=None)
    with engine.connect() as connection:
        assert connection.execute(select(Batch.state, Batch.pool_origin).where(Batch.id == identity)).one() == ('submitted', None)


@pytest.mark.parametrize('values', [
    {'state': 'succeeded', 'result': {}}, {'state': 'failed'}, {'state': 'cancelled'},
    {'cancellation_requested_at': datetime(2026, 10, 1, tzinfo=UTC)},
    {'requires_caps': {'backend': 'nebius', 'worker_pool': 'foreign-pool'}},
    {'requires_caps': {'backend': 'docker', 'worker_pool': 'foreign-pool'}},
])
def test_cutover_does_not_adopt_or_change_terminal_cancelled_or_foreign_direct_work(cutover_database, values):
    from loom.db.schema import Trial

    api, state, _, engine = cutover_database
    with engine.begin() as connection:
        _, trial, _ = seed_cutover_backlog(state, connection, known=False, direct=True, **values)
    assert api.cutover_readiness_page(state.target, after=None)['rows'] == []
    with engine.connect() as connection:
        assert connection.execute(select(Trial.pool_origin).where(Trial.id == trial)).scalar_one() is None


@pytest.mark.parametrize('damage', ['old_schema', 'foreign_guard', 'active_application_access'])
def test_cutover_qualifies_schema_guard_and_disconnected_application_credentials(cutover_database, migration_access, damage):
    from scripts.ops.nebius_pool_migration import PoolMigrationError

    api, state, connection, _ = cutover_database
    if damage == 'old_schema':
        connection.execute("UPDATE public.alembic_version SET version_num='0171'")
    elif damage == 'foreign_guard':
        connection.execute("INSERT INTO nebius_rollout_guard(id,owner,candidate_sha) VALUES (1,'foreign',%s)", ('f' * 40,))
    else:
        # No personal application session exists. A still-valid key can create
        # one later, so stopping application Pods is not credential retirement.
        migration_access[2].grant(uuid4(), uuid4(), 1, token_urlsafe(48), schema_revision='0173')
    with pytest.raises(PoolMigrationError):
        api.cutover_readiness_page(state.target, after=None)


def test_cutover_accepts_retired_and_drained_application_access_without_erasing_history(cutover_database, migration_access):
    api, state, connection, _ = cutover_database
    access = migration_access[2]
    application, incarnation = uuid4(), uuid4()
    access.grant(application, incarnation, 1, token_urlsafe(48), schema_revision='0173')
    access.revoke(application, incarnation, 1)
    assert access.drain(application, incarnation, 1)
    history = connection.execute('SELECT * FROM loom_application_access.generations').fetchall()
    assert api.cutover_readiness_page(state.target, after=None)['rows'] == []
    assert connection.execute('SELECT * FROM loom_application_access.generations').fetchall() == history


def test_cutover_reads_every_pending_batch_across_fixed_readonly_pages(cutover_database):
    from loom.db.schema import Batch, Team

    api, state, _, engine = cutover_database
    participant = next(row for row in state.request.registration.spec.participants
        if row.participant_id == state.target.participant_id)
    team, identities = uuid4(), sorted(str(uuid4()) for _ in range(129))
    with engine.begin() as connection:
        connection.execute(insert(Team).values(id=team, name=str(team)))
        for identity in identities:
            connection.execute(insert(Batch).values(id=identity, team_id=team, name='paged-pending', backend='nebius',
                task_filter={}, trial_config={}, created_by_token_prefix='test', pool_origin={
                    'schema_version': 'loom.pool-work-origin.v1', 'data_environment_id': str(participant.environment_id),
                    'submission_id': identity, 'kind': 'environment', 'application': None}))
    first = api.cutover_readiness_page(state.target, after=None)
    assert len(first['rows']) == 128
    second = api.cutover_readiness_page(state.target, after=first['rows'][-1]['key'])
    assert [row['key'] for row in first['rows'] + second['rows']] == ['batch:' + value for value in identities]
    assert len(second['rows']) == 1


def test_cutover_does_not_trust_a_replaced_application_readiness_routine(cutover_database, migration_access):
    from scripts.ops.nebius_pool_migration import PoolMigrationError

    api, state, connection, _ = cutover_database
    migration_access[2].grant(uuid4(), uuid4(), 1, token_urlsafe(48), schema_revision='0173')
    connection.execute("""CREATE OR REPLACE FUNCTION loom_application_access.migration_ready() RETURNS boolean
        LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS 'BEGIN RETURN TRUE; END'""")
    assert connection.execute('SELECT loom_application_access.migration_ready()').fetchone() == (True,)
    with pytest.raises(PoolMigrationError):
        api.cutover_readiness_page(state.target, after=None)


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
