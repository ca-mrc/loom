"""A delayed original release cannot clear the operation's recovery guard."""
from __future__ import annotations

import asyncio
from uuid import uuid4

import psycopg
import pytest
from sqlalchemy import text

from loom.nebius_rollout_guard import LOCK_KEY, acquire, admission_open, observe, release
from tests.integration.test_nebius_pool_activation_fence import read_sql
from tests.integration.test_nebius_pool_registry import sessions as sessions


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
