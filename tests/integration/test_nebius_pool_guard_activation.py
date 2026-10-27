"""A delayed original release cannot clear the operation's recovery guard."""
from __future__ import annotations

import asyncio
from uuid import uuid4

import psycopg
import pytest
from sqlalchemy import text

from loom.db.schema_startup import service_schema_head
from loom.nebius_rollout_guard import LOCK_KEY, acquire, admission_open, observe, release
from tests.integration.conftest import retained_pool_sessions
from tests.integration.test_nebius_pool_activation_fence import read_sql

sessions = retained_pool_sessions


@pytest.mark.parametrize('action', ['observe', 'fence', 'release'])
async def test_participant_guard_rejects_unaudited_schema_before_mutation(
        isolated_migration_postgres_url, action):
    from scripts.ops.nebius_pool_guard_activation import pool_guard_activation_sql
    from sqlalchemy.engine import make_url

    url = make_url(isolated_migration_postgres_url).set(drivername='postgresql').render_as_string(hide_password=False)
    owner, participant = uuid4(), uuid4()
    with psycopg.connect(url, autocommit=True) as db:
        assert db.execute('SELECT version_num FROM alembic_version').fetchall() == [(service_schema_head(),)]
        db.execute("UPDATE alembic_version SET version_num='0199'")
        with pytest.raises(psycopg.errors.RaiseException, match='pool activation schema unqualified'):
            db.execute(pool_guard_activation_sql(owner, participant, 'a' * 40, action=action), prepare=False)
        db.rollback()
        assert db.execute('SELECT count(*) FROM nebius_rollout_guard').fetchone() == (0,)


@pytest.mark.parametrize('schema_revision', ['0175', '0176', '0177', '0178', '0179', '0180'])
def test_participant_schema_supports_complete_handoff_and_recovery_without_upgrading_manager(
        isolated_migration_postgres_url, schema_revision):
    from scripts.ops.nebius_pool_activation_database import pool_activation_state_sql
    from scripts.ops.nebius_pool_guard_activation import pool_guard_activation_sql
    from scripts.ops.nebius_pool_migration_guard import (
        pool_cutover_readiness_sql,
        qualify_cutover_readiness_page,
    )
    from scripts.ops.nebius_pool_recovery_database import (
        participant_recovery_drain_sql,
        qualify_participant_recovery_drain,
    )
    from scripts.ops.nebius_pool_recovery_release import pool_guard_recovery_release_sql
    from sqlalchemy.engine import make_url

    from loom_service.pool_management.participant_refresh import PoolParticipantRefresh
    from tests.integration.conftest import retain_pool_schema
    from tests.integration.test_nebius_pool_participant_refresh import selection

    retain_pool_schema(isolated_migration_postgres_url, schema_revision)
    value, _ = selection()
    spec = PoolParticipantRefresh.model_validate(value).before
    participant = spec.participants[0]
    owner, identity, candidate = uuid4(), participant.participant_id, 'a' * 40
    url = make_url(isolated_migration_postgres_url).set(drivername='postgresql').render_as_string(hide_password=False)
    with psycopg.connect(url, autocommit=True) as db:
        assert db.execute('SELECT version_num FROM alembic_version').fetchall() == [(schema_revision,)]
        db.execute('INSERT INTO nebius_rollout_guard(id,owner,candidate_sha) VALUES(1,%s,%s)', (str(owner), candidate))
        with db.cursor() as cursor:
            cursor.execute(pool_cutover_readiness_sql(owner=str(owner), candidate=candidate,
                logical_pool_id='nebius-cpu', after=None), prepare=False)
            rows = []
            while True:
                if cursor.description:
                    rows.extend(cursor.fetchall())
                if not cursor.nextset():
                    break
        (page,), = rows
    assert page == {'status': 'observed', 'schema_revision': schema_revision, 'rows': []}
    assert qualify_cutover_readiness_page(page, participant=participant, after=None) == ()
    with pytest.raises(ValueError):
        qualify_cutover_readiness_page({**page, 'schema_revision': '0199'}, participant=participant, after=None)
    assert read_sql(url, pool_guard_activation_sql(owner, identity, candidate, action='observe'))['status'] == 'held'
    assert read_sql(url, pool_guard_activation_sql(owner, identity, candidate, action='release'))['status'] == 'open'
    assert read_sql(url, pool_guard_activation_sql(owner, identity, candidate, action='fence'))['status'] == 'fenced'
    report = read_sql(url, participant_recovery_drain_sql(owner, identity, candidate))
    assert qualify_participant_recovery_drain(owner, identity, candidate, report)
    assert read_sql(url, pool_guard_recovery_release_sql(owner, identity, candidate))['status'] == 'open'
    with psycopg.connect(url, autocommit=True) as db:
        assert db.execute('SELECT count(*) FROM nebius_rollout_guard').fetchone() == (0,)
        assert db.execute('SELECT version_num FROM alembic_version').fetchall() == [(schema_revision,)]
        if schema_revision == '0175':
            assert db.execute("SELECT to_regclass('harbor_baseline_sessions')").fetchone() == (None,)
        if schema_revision in {'0177', '0178', '0179', '0180'}:
            assert db.execute('SELECT count(*) FROM task_source_publications').fetchone() == (0,)
        if schema_revision != '0176':
            with pytest.raises(psycopg.errors.RaiseException, match='pool activation schema unqualified'):
                db.execute(pool_activation_state_sql(spec), prepare=False)
            db.rollback()


@pytest.mark.parametrize('released', [False, True])
async def test_recovery_owner_fences_original_release_and_keeps_admission_closed(sessions, released):
    from scripts.ops.nebius_pool_guard_activation import pool_guard_activation_sql

    owner, participant, candidate = uuid4(), uuid4(), 'a' * 40
    url = sessions.kw['bind'].url.render_as_string(hide_password=False)
    async with sessions.begin() as session:
        assert (await acquire(session, owner=str(owner), candidate=candidate))['status'] == 'acquired'
    assert read_sql(url, pool_guard_activation_sql(owner, participant, candidate, action='observe'))['status'] == 'held'
    if released:
        assert read_sql(url, pool_guard_activation_sql(owner, participant, candidate, action='release'))['status'] == 'open'
    fenced = read_sql(url, pool_guard_activation_sql(owner, participant, candidate, action='fence'))
    assert fenced == {'schema': 'loom.pool-local-guard.v1', 'operation_id': str(owner),
        'participant_id': str(participant), 'status': 'fenced'}
    assert read_sql(url, pool_guard_activation_sql(owner, participant, candidate, action='fence')) == fenced
    async with sessions.begin() as session:
        assert not await admission_open(session)
        assert (await observe(session, owner='pool-recovery:' + str(owner), candidate=candidate))['status'] == 'held'
    with pytest.raises(ValueError):
        async with sessions.begin() as session:
            await release(session, owner=str(owner), candidate=candidate)
    with pytest.raises(psycopg.Error):
        read_sql(url, pool_guard_activation_sql(owner, participant, candidate, action='release'))
    assert read_sql(url, pool_guard_activation_sql(owner, participant, candidate, action='observe')) == fenced


@pytest.mark.parametrize('damage', ['owner', 'candidate'])
async def test_guard_recovery_does_not_adopt_another_operation(sessions, damage):
    from scripts.ops.nebius_pool_guard_activation import pool_guard_activation_sql

    owner, participant, candidate = uuid4(), uuid4(), 'a' * 40
    actual_owner, actual_candidate = (str(uuid4()), candidate) if damage == 'owner' else (str(owner), 'b' * 40)
    async with sessions.begin() as session:
        await acquire(session, owner=actual_owner, candidate=actual_candidate)
    url = sessions.kw['bind'].url.render_as_string(hide_password=False)
    for action in ('release', 'fence'):
        with pytest.raises(psycopg.Error):
            read_sql(url, pool_guard_activation_sql(owner, participant, candidate, action=action))
    assert read_sql(url, pool_guard_activation_sql(owner, participant, candidate, action='observe'))['status'] == 'foreign'
    async with sessions() as session:
        assert (await observe(session, owner=actual_owner, candidate=actual_candidate))['status'] == 'held'


@pytest.mark.parametrize('first', ['release', 'fence'])
async def test_guard_release_and_fence_serialize_in_both_lock_orders(sessions, first):
    from scripts.ops.nebius_pool_guard_activation import pool_guard_activation_sql

    owner, participant, candidate = uuid4(), uuid4(), 'a' * 40
    url = sessions.kw['bind'].url.render_as_string(hide_password=False)
    async with sessions.begin() as session:
        await acquire(session, owner=str(owner), candidate=candidate)
    tasks = {}
    try:
        async with sessions.begin() as holder:
            await holder.execute(text('SELECT pg_advisory_xact_lock(:key)'), {'key': LOCK_KEY})
            for action in (first, 'fence' if first == 'release' else 'release'):
                name = action + '-' + uuid4().hex
                tasks[action] = asyncio.create_task(asyncio.to_thread(read_sql, url,
                    pool_guard_activation_sql(owner, participant, candidate, action=action), application_name=name))
                async with asyncio.timeout(5):
                    while True:
                        async with sessions() as inspect:
                            pid = await inspect.scalar(text("""SELECT a.pid FROM pg_stat_activity a
                                JOIN pg_locks l ON a.pid=l.pid WHERE a.application_name=:name
                                AND a.datname=current_database() AND l.locktype='advisory' AND NOT l.granted"""), {'name': name})
                        if pid is not None:
                            break
                        await asyncio.sleep(0.01)
        assert (await tasks['fence'])['status'] == 'fenced'
        if first == 'release':
            assert (await tasks['release'])['status'] == 'open'
        else:
            with pytest.raises(psycopg.Error):
                await tasks['release']
        assert read_sql(url, pool_guard_activation_sql(owner, participant, candidate, action='observe'))['status'] == 'fenced'
    finally:
        await asyncio.gather(*tasks.values(), return_exceptions=True)


async def test_recovery_fence_does_not_need_idle_work_or_release_existing_claims(sessions):
    from scripts.ops.nebius_pool_guard_activation import pool_guard_activation_sql

    from tests.integration import test_service_execution_leases as execution

    owner, participant, candidate = uuid4(), uuid4(), 'a' * 40
    # A pending execution is real durable work; ordinary idle acquire refuses it.
    from datetime import UTC, datetime
    async with sessions.begin() as session:
        await execution._seed_ready_trial(session, now=datetime.now(UTC))
        await session.execute(text("UPDATE trials SET state='claimed'"))
    async with sessions.begin() as session:
        assert (await acquire(session, owner=str(owner), candidate=candidate))['status'] == 'skipped_busy'
    url = sessions.kw['bind'].url.render_as_string(hide_password=False)
    assert read_sql(url, pool_guard_activation_sql(owner, participant, candidate, action='fence'))['status'] == 'fenced'
    async with sessions.begin() as session:
        assert not await admission_open(session)
        assert await session.scalar(text("SELECT count(*) FROM trials WHERE state='claimed'")) == 1


async def test_original_release_already_waiting_on_row_cannot_delete_recovery_owner(sessions):
    from scripts.ops.nebius_pool_guard_activation import pool_guard_activation_sql

    owner, participant, candidate = uuid4(), uuid4(), 'a' * 40
    url = sessions.kw['bind'].url.render_as_string(hide_password=False)
    fence_name, pause_key = 'fence-' + uuid4().hex, 191500731
    async with sessions.begin() as session:
        await acquire(session, owner=str(owner), candidate=candidate)
        await session.execute(text(f"""CREATE FUNCTION pause_recovery_guard() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN PERFORM pg_advisory_xact_lock({pause_key}); RETURN NEW; END $$"""))
        await session.execute(text('''CREATE TRIGGER pause_recovery_guard BEFORE UPDATE ON nebius_rollout_guard
            FOR EACH ROW EXECUTE FUNCTION pause_recovery_guard()'''))
    release_pid = asyncio.get_running_loop().create_future()

    async def old_release():
        async with sessions.begin() as session:
            release_pid.set_result(await session.scalar(text('SELECT pg_backend_pid()')))
            return await release(session, owner=str(owner), candidate=candidate)

    async def wait_for(statement, parameters):
        async with asyncio.timeout(5):
            while True:
                async with sessions() as inspect:
                    value = await inspect.scalar(text(statement), parameters)
                if value:
                    return value
                await asyncio.sleep(0.01)

    tasks = []
    try:
        async with sessions.begin() as holder:
            await holder.execute(text('SELECT pg_advisory_xact_lock(:key)'), {'key': pause_key})
            fence = asyncio.create_task(asyncio.to_thread(read_sql, url,
                pool_guard_activation_sql(owner, participant, candidate, action='fence'), application_name=fence_name))
            tasks.append(fence)
            # The real fence now holds the row and is paused in its UPDATE.
            fence_pid = await wait_for('''SELECT a.pid FROM pg_stat_activity a JOIN pg_locks l ON a.pid=l.pid
                WHERE a.application_name=:name AND a.datname=current_database()
                AND l.locktype='advisory' AND NOT l.granted''', {'name': fence_name})
            pending = asyncio.create_task(old_release())
            tasks.append(pending)
            pid = await release_pid
            await wait_for('SELECT :fence = ANY(pg_blocking_pids(:release))', {'fence': fence_pid, 'release': pid})
        assert (await fence)['status'] == 'fenced'
        with pytest.raises(ValueError, match='owner'):
            await pending
        assert read_sql(url, pool_guard_activation_sql(owner, participant, candidate, action='observe'))['status'] == 'fenced'
    finally:
        await asyncio.gather(*tasks, return_exceptions=True)
