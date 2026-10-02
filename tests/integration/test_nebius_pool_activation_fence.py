"""Protected pool opening and cancellation serialize on the real admission lock."""
from __future__ import annotations

import asyncio
import subprocess
import sys
from dataclasses import replace
from uuid import uuid4

import psycopg
import pytest
from sqlalchemy import select, text, update
from sqlalchemy.engine import make_url

from loom.db.nebius_pool_schema import NebiusPoolBinding
from tests.integration.test_nebius_pool_registry import sessions as sessions
from tests.integration.test_nebius_pool_startup_capacity import prepare_startup_capacity


def read_sql(url, query, *, application_name='pool-fence-test'):
    reports = []
    with psycopg.connect(make_url(url).set(drivername='postgresql').render_as_string(hide_password=False),
            autocommit=True, application_name=application_name) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, prepare=False)
            while True:
                if cursor.description and cursor.description[0].name == 'report':
                    reports.extend(row[0] for row in cursor.fetchall())
                if not cursor.nextset():
                    break
    assert len(reports) == 1
    return reports[0]


async def open_pool(prepared, tmp_path):
    from scripts.ops.nebius_pool_startup_capacity import BOUND_POOL_ACTIVATION_COMMAND

    _, _, environment, _, nonce, signature, _ = prepared
    return await asyncio.to_thread(subprocess.run, [sys.executable, '-c', BOUND_POOL_ACTIVATION_COMMAND, nonce, signature],
        env=environment, cwd=tmp_path, capture_output=True, timeout=35, check=False)


@pytest.mark.parametrize('opened', [False, True])
async def test_activation_readback_and_fence_never_reopen_or_renew_original_authority(sessions, tmp_path, opened):
    from scripts.ops.nebius_pool_activation_database import (
        fence_pool_activation_sql,
        pool_activation_state_sql,
        qualify_pool_activation_report,
    )

    prepared = await prepare_startup_capacity(sessions, tmp_path)
    spec, _, environment, _, _, _, _ = prepared
    url = environment['LOOM_POOL_GATEWAY_DB_URL']
    assert qualify_pool_activation_report(spec, read_sql(url, pool_activation_state_sql(spec))) == 'closed'
    if opened:
        result = await open_pool(prepared, tmp_path)
        assert result.returncode == 0 and result.stdout == b'{"status": "global"}\n'
        assert qualify_pool_activation_report(spec, read_sql(url, pool_activation_state_sql(spec))) == 'global'
    # No gateway process/token is needed to close recovery authority.
    prepared[3].unlink()
    first = read_sql(url, fence_pool_activation_sql(spec))
    assert qualify_pool_activation_report(spec, first) == 'fenced'
    assert read_sql(url, fence_pool_activation_sql(spec)) == first
    for _ in range(2):
        assert read_sql(url, pool_activation_state_sql(spec)) == first
    async with sessions() as session:
        row = await session.get(NebiusPoolBinding, spec.pool_id)
        assert (row.mode, row.policy_revision, row.admission_epoch) == ('closed', spec.policy_revision + 1, spec.admission_epoch)


@pytest.mark.parametrize('first', ['opening', 'fence'])
async def test_opening_and_fence_serialize_in_both_real_lock_orders(sessions, tmp_path, first):
    from scripts.ops.nebius_pool_activation_database import (
        fence_pool_activation_sql,
        pool_activation_state_sql,
        qualify_pool_activation_report,
    )
    from scripts.ops.nebius_pool_startup_capacity import BOUND_POOL_ACTIVATION_COMMAND

    from loom_service.pool_management.locks import acquire_pool_mutation_lock

    prepared = await prepare_startup_capacity(sessions, tmp_path)
    spec, _, environment, _, nonce, signature, _ = prepared
    url = environment['LOOM_POOL_GATEWAY_DB_URL']
    opening_name, fence_name = 'open-' + uuid4().hex, 'fence-' + uuid4().hex
    environment['LOOM_POOL_GATEWAY_DB_URL'] = make_url(url).update_query_dict(
        {'application_name': opening_name}).render_as_string(hide_password=False)
    # Warm imports before either two-second production lock timeout starts.
    # The actual fixed command and lock implementation run unchanged afterward.
    warmup = ('import sys, sqlalchemy.ext.asyncio\n'
        'import loom_service.pool_management.__main__, loom_service.pool_management.auth\n'
        'import loom_service.pool_management.capacity, loom_service.pool_management.observations\n'
        'from loom_execution_capacity_collector.control_plane import read_owner_only_secret\n'
        'print("ready", flush=True)\nsys.stdin.readline()\n')
    process = await asyncio.create_subprocess_exec(sys.executable, '-u', '-c', warmup + BOUND_POOL_ACTIVATION_COMMAND,
        nonce, signature, env=environment, cwd=tmp_path, stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)

    async def waiting(name):
        async with asyncio.timeout(5):
            while True:
                async with sessions() as inspect:
                    pid = await inspect.scalar(text("""SELECT a.pid FROM pg_stat_activity a JOIN pg_locks l ON l.pid=a.pid
                        WHERE a.application_name=:name AND a.datname=current_database()
                        AND l.locktype='advisory' AND NOT l.granted"""), {'name': name})
                if pid is not None:
                    return pid
                await asyncio.sleep(0.01)

    fence = None
    try:
        async with asyncio.timeout(15):
            assert await process.stdout.readline() == b'ready\n'
        async with sessions.begin() as holder:
            await acquire_pool_mutation_lock(holder)
            for operation in (first, 'fence' if first == 'opening' else 'opening'):
                if operation == 'opening':
                    process.stdin.write(b'go\n')
                    await process.stdin.drain()
                    await waiting(opening_name)
                else:
                    fence = asyncio.create_task(asyncio.to_thread(read_sql, url,
                        fence_pool_activation_sql(spec), application_name=fence_name))
                    await waiting(fence_name)
        async with asyncio.timeout(30):
            stdout, stderr = await process.communicate()
        if first == 'opening':
            assert (process.returncode, stdout, stderr) == (0, b'{"status": "global"}\n', b'')
        else:
            assert (process.returncode, stdout, stderr) == (1, b'', b'Pool activation unconfirmed; preserve recovery evidence\n')
        assert qualify_pool_activation_report(spec, await fence) == 'fenced'
    finally:
        if process.returncode is None:
            process.kill()
            await process.communicate()
        if fence is not None:
            await asyncio.gather(fence, return_exceptions=True)
    assert qualify_pool_activation_report(spec, read_sql(url, pool_activation_state_sql(spec))) == 'fenced'


async def test_fence_transaction_failure_leaves_original_authority_unchanged(sessions, tmp_path):
    from scripts.ops.nebius_pool_activation_database import (
        fence_pool_activation_sql,
        pool_activation_state_sql,
        qualify_pool_activation_report,
    )

    spec, _, environment, _, _, _, _ = await prepare_startup_capacity(sessions, tmp_path)
    url = environment['LOOM_POOL_GATEWAY_DB_URL']
    with pytest.raises(psycopg.errors.DivisionByZero):
        read_sql(url, fence_pool_activation_sql(spec).replace('COMMIT;', 'SELECT 1 / 0; COMMIT;'))
    assert qualify_pool_activation_report(spec, read_sql(url, pool_activation_state_sql(spec))) == 'closed'


async def test_fence_after_committed_open_is_terminal_for_old_opening_challenge(sessions, tmp_path):
    from scripts.ops.nebius_pool_activation_database import (
        fence_pool_activation_sql,
        pool_activation_state_sql,
        qualify_pool_activation_report,
    )

    prepared = await prepare_startup_capacity(sessions, tmp_path)
    spec, _, environment, _, _, _, _ = prepared
    url = environment['LOOM_POOL_GATEWAY_DB_URL']
    # Treat the first result as a lost response: only a fresh DB readback may
    # determine whether it committed; executing the write again is not recovery.
    await open_pool(prepared, tmp_path)
    assert qualify_pool_activation_report(spec, read_sql(url, pool_activation_state_sql(spec))) == 'global'
    assert qualify_pool_activation_report(spec, read_sql(url, fence_pool_activation_sql(spec))) == 'fenced'
    result = await open_pool(prepared, tmp_path)
    assert result.returncode == 1
    assert qualify_pool_activation_report(spec, read_sql(url, pool_activation_state_sql(spec))) == 'fenced'


@pytest.mark.parametrize('damage', ['revision', 'binding', 'epoch', 'legacy'])
async def test_old_operation_cannot_fence_reconfigured_or_unrelated_pool(sessions, tmp_path, damage):
    from scripts.ops.nebius_pool_activation_database import (
        fence_pool_activation_sql,
        pool_activation_state_sql,
        qualify_pool_activation_report,
    )

    prepared = await prepare_startup_capacity(sessions, tmp_path)
    spec, _, environment, _, _, _, _ = prepared
    values = {'revision': {'policy_revision': spec.policy_revision + 2},
        'binding': {'policy_revision': spec.policy_revision + 1, 'binding_json': {'foreign': True}, 'binding_sha256': '0' * 64},
        'epoch': {'admission_epoch': spec.admission_epoch + 1}, 'legacy': {'mode': 'legacy'}}[damage]
    async with sessions.begin() as session:
        await session.execute(update(NebiusPoolBinding).where(NebiusPoolBinding.pool_id == spec.pool_id).values(**values))
    async def state():
        async with sessions() as session:
            row = (await session.scalars(select(NebiusPoolBinding).where(NebiusPoolBinding.pool_id == spec.pool_id))).one()
            return row.mode, row.policy_revision, row.admission_epoch, row.binding_json, row.binding_sha256
    before = await state()
    url = environment['LOOM_POOL_GATEWAY_DB_URL']
    with pytest.raises(ValueError):
        qualify_pool_activation_report(spec, read_sql(url, pool_activation_state_sql(spec)))
    with pytest.raises(psycopg.Error):
        read_sql(url, fence_pool_activation_sql(spec))
    assert await state() == before


async def test_revision_fence_preserves_existing_closed_mode_cleanup(sessions):
    from tests.integration.test_nebius_pool_cleanup_journal import observed

    journal, principal, receipt, _ = await observed(sessions)
    deletion = await journal.prepare_delete(principal, receipt.reservation_id, kind='Job')
    async with sessions.begin() as session:
        await session.execute(update(NebiusPoolBinding).where(NebiusPoolBinding.pool_id == principal.pool_id).values(
            mode='closed', policy_revision=principal.policy_revision + 1))
    principal = replace(principal, pool_mode='closed', policy_revision=principal.policy_revision + 1)
    assert (await journal.dispatch_delete(principal, deletion.effect_id)).phase == 'dispatched'
    assert (await journal.observe_delete(principal, deletion.effect_id)).phase == 'observed'
