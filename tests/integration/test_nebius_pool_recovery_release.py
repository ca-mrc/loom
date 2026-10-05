"""Only the exact idle recovery owner can reopen local admission atomically."""
from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from uuid import uuid4

import psycopg
import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url

from loom.nebius_rollout_guard import LOCK_KEY, admission_open
from tests.integration import test_service_execution_leases as execution
from tests.integration.test_nebius_pool_activation_fence import read_sql
from tests.integration.test_nebius_pool_registry import sessions as sessions


def database_url(sessions):
    return sessions.kw['bind'].url.render_as_string(hide_password=False)


def fence(sessions, operation, participant, candidate):
    from scripts.ops.nebius_pool_guard_activation import pool_guard_activation_sql

    return read_sql(database_url(sessions), pool_guard_activation_sql(operation, participant, candidate, action='fence'))


async def test_exact_recovery_release_preserves_queued_work_and_has_one_safe_report(sessions):
    from scripts.ops.nebius_pool_recovery_release import pool_guard_recovery_release_sql

    operation, participant, candidate = uuid4(), uuid4(), 'a' * 40
    async with sessions.begin() as session:
        await execution._seed_ready_trial(session, now=datetime.now(UTC))
        before = (await session.execute(text('SELECT id,state FROM trials'))).all()
    fence(sessions, operation, participant, candidate)
    query = pool_guard_recovery_release_sql(operation, participant, candidate)
    with psycopg.connect(make_url(database_url(sessions)).set(drivername='postgresql').render_as_string(hide_password=False),
            autocommit=True) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, prepare=False)
            rows = []
            while True:
                if cursor.description:
                    rows.extend(cursor.fetchall())
                if not cursor.nextset():
                    break
    assert rows == [({'schema': 'loom.pool-recovery-release.v1', 'operation_id': str(operation),
        'participant_id': str(participant), 'candidate_sha': candidate, 'status': 'open'},)]
    async with sessions.begin() as session:
        assert await admission_open(session)
        assert (await session.execute(text('SELECT id,state FROM trials'))).all() == before
    # A successful release is not reusable authority and cannot delete a later
    # guard. Lost-result recovery reads status rather than dispatching twice.
    with pytest.raises(psycopg.Error):
        read_sql(database_url(sessions), query)


@pytest.mark.parametrize('damage', ['original_owner', 'other_owner', 'candidate', 'schema', 'active_trial'])
async def test_recovery_release_refuses_foreign_scope_or_busy_state_without_changing_guard(sessions, damage):
    from scripts.ops.nebius_pool_recovery_release import pool_guard_recovery_release_sql

    operation, participant, candidate = uuid4(), uuid4(), 'a' * 40
    fence(sessions, operation, participant, candidate)
    async with sessions.begin() as session:
        if damage in {'original_owner', 'other_owner'}:
            await session.execute(text('UPDATE nebius_rollout_guard SET owner=:owner'),
                {'owner': str(operation) if damage == 'original_owner' else 'pool-recovery:' + str(uuid4())})
        elif damage == 'candidate':
            await session.execute(text("UPDATE nebius_rollout_guard SET candidate_sha=:candidate"), {'candidate': 'b' * 40})
        elif damage == 'schema':
            await session.execute(text("UPDATE alembic_version SET version_num='0171'"))
        else:
            await execution._seed_ready_trial(session, now=datetime.now(UTC))
            await session.execute(text("UPDATE trials SET state='claimed'"))
        before = (await session.execute(text('SELECT id,owner,candidate_sha FROM nebius_rollout_guard'))).all()
    with pytest.raises(psycopg.Error):
        read_sql(database_url(sessions), pool_guard_recovery_release_sql(operation, participant, candidate))
    async with sessions.begin() as session:
        assert not await admission_open(session)
        assert (await session.execute(text('SELECT id,owner,candidate_sha FROM nebius_rollout_guard'))).all() == before
        if damage == 'active_trial':
            assert await session.scalar(text("SELECT count(*) FROM trials WHERE state='claimed'")) == 1


@pytest.mark.parametrize('kind', ['execution', 'build'])
async def test_unclaimed_handoff_blocks_reopening_before_any_pod_exists(sessions, tmp_path, kind):
    from scripts.ops.nebius_pool_recovery_release import pool_guard_recovery_release_sql

    if kind == 'execution':
        from tests.integration.test_nebius_pool_execution_activation import selected

        journal, _, _, _, _ = await selected(sessions, tmp_path)
        participant = journal.participant.participant_id
    else:
        from tests.integration.test_nebius_pool_build_driver import selected

        _, _, identity, _, _ = await selected(sessions, tmp_path)
        participant = identity.participant_id
    operation, candidate = uuid4(), 'a' * 40
    fence(sessions, operation, participant, candidate)
    with pytest.raises(psycopg.Error):
        read_sql(database_url(sessions), pool_guard_recovery_release_sql(operation, participant, candidate))
    async with sessions.begin() as session:
        assert not await admission_open(session)
        assert await session.scalar(text(f"SELECT count(*) FROM nebius_pool_{kind}_outbox WHERE phase='selected'")) == 1


async def test_recovery_release_checks_idle_after_waiting_for_the_real_admission_lock(sessions):
    from scripts.ops.nebius_pool_recovery_release import pool_guard_recovery_release_sql

    operation, participant, candidate = uuid4(), uuid4(), 'a' * 40
    fence(sessions, operation, participant, candidate)
    name = 'recovery-release-' + uuid4().hex
    pending = None
    try:
        async with sessions.begin() as holder:
            await holder.execute(text('SELECT pg_advisory_xact_lock_shared(:key)'), {'key': LOCK_KEY})
            pending = asyncio.create_task(asyncio.to_thread(read_sql, database_url(sessions),
                pool_guard_recovery_release_sql(operation, participant, candidate), application_name=name))
            async with asyncio.timeout(5):
                while True:
                    async with sessions() as inspect:
                        blocked = await inspect.scalar(text('''SELECT EXISTS (SELECT 1 FROM pg_stat_activity a
                            JOIN pg_locks l ON a.pid=l.pid WHERE a.application_name=:name
                            AND a.datname=current_database() AND l.locktype='advisory' AND NOT l.granted)'''), {'name': name})
                    if blocked:
                        break
                    await asyncio.sleep(0.01)
            assert not pending.done()
            await execution._seed_ready_trial(holder, now=datetime.now(UTC))
            await holder.execute(text("UPDATE trials SET state='claimed'"))
        with pytest.raises(psycopg.Error):
            await pending
        async with sessions.begin() as session:
            assert not await admission_open(session)
            assert await session.scalar(text("SELECT count(*) FROM trials WHERE state='claimed'")) == 1
    finally:
        if pending is not None:
            await asyncio.gather(pending, return_exceptions=True)


async def test_recovery_release_failure_after_delete_rolls_back_the_guard(sessions):
    from scripts.ops.nebius_pool_recovery_release import pool_guard_recovery_release_sql

    operation, participant, candidate = uuid4(), uuid4(), 'a' * 40
    fence(sessions, operation, participant, candidate)
    async with sessions.begin() as session:
        await session.execute(text('''CREATE FUNCTION reject_recovery_release() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN RAISE EXCEPTION 'test-only failure after delete'; END $$'''))
        await session.execute(text('''CREATE TRIGGER reject_recovery_release AFTER DELETE ON nebius_rollout_guard
            FOR EACH ROW EXECUTE FUNCTION reject_recovery_release()'''))
    with pytest.raises(psycopg.Error):
        read_sql(database_url(sessions), pool_guard_recovery_release_sql(operation, participant, candidate))
    async with sessions.begin() as session:
        assert not await admission_open(session)
        assert (await session.execute(text('SELECT owner,candidate_sha FROM nebius_rollout_guard'))).one() == (
            'pool-recovery:' + str(operation), candidate)
