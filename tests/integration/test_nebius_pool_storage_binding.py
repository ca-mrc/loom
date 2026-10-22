"""Physical quota correction is atomic and refuses any used or unqualified pool."""
from __future__ import annotations

import asyncio
import hmac
import importlib
import json
import os
import subprocess
import sys
from datetime import UTC, datetime
from uuid import uuid4

import psycopg
import pytest
from sqlalchemy import insert, update

from loom.db.nebius_pool_schema import (
    NebiusPoolBinding,
    NebiusPoolCancellation,
    NebiusPoolParticipant,
    NebiusPoolRequest,
)
from loom.db.schema import Token
from loom_service.pool_management.installation import register_installation
from loom_service.pool_management.locks import acquire_pool_mutation_lock
from tests.integration.conftest import retained_pool_sessions
from tests.integration.test_nebius_pool_activation_fence import read_sql
from tests.integration.test_nebius_pool_participant_refresh import reserved, snapshot
from tests.ops.test_nebius_pool_storage_binding import module, selection

sessions = retained_pool_sessions


async def setup(sessions):
    request = module().ClosedPoolStorageRefresh.model_validate(selection())
    async with sessions.begin() as session:
        await register_installation(session, request.before)
    return request, sessions.kw['bind'].url.render_as_string(hide_password=False)


async def test_closed_storage_update_is_atomic_and_uncertain_reply_uses_exact_readback(sessions):
    storage = module()
    request, url = await setup(sessions)
    before = await snapshot(sessions)
    query = storage.pool_storage_state_sql(request)
    # Observation is read-only and does not wait for another mutation transaction.
    async with sessions.begin() as session:
        await acquire_pool_mutation_lock(session)
        observed = await asyncio.to_thread(read_sql, url, query)
    assert storage.qualify_storage_report(request, observed, read_only=True) == 'ready'
    assert await snapshot(sessions) == before
    applied = await asyncio.to_thread(read_sql, url, storage.refresh_closed_storage_sql(request))
    assert storage.qualify_storage_report(request, applied, read_only=False) == 'complete'
    assert applied['operation_id'] == str(request.operation_id)
    after = await snapshot(sessions)
    pool, = after['nebius_pool_bindings']
    assert (pool['mode'], pool['admission_epoch'], pool['policy_revision']) == (
        'closed', request.before.admission_epoch, request.before.policy_revision + 1)
    assert pool['binding_json']['quota_identities']['storage'] == [
        'parent', 'eu-north1', 'compute', 'compute.disk.size.network-ssd-non-replicated', 'MiB']
    assert pool['binding_json']['profile_catalog_sha256'] == before['nebius_pool_bindings'][0]['binding_json']['profile_catalog_sha256']
    for table in before.keys() - {'nebius_pool_bindings'}:
        assert after[table] == before[table]
    replay = await asyncio.to_thread(read_sql, url, query)
    assert storage.qualify_storage_report(request, replay, read_only=True) == 'complete'
    with pytest.raises(psycopg.Error):
        await asyncio.to_thread(read_sql, url, storage.refresh_closed_storage_sql(request))
    assert await snapshot(sessions) == after
    for key in ('operation_id', 'before_sha256', 'after_sha256', 'read_only', 'extra'):
        with pytest.raises(ValueError):
            storage.qualify_storage_report(request, {**replay, key: 'foreign'}, read_only=True)


@pytest.mark.parametrize('damage', ['global', 'participant-fenced', 'terminal-work', 'cancellation', 'credential'])
async def test_closed_storage_update_rejects_use_or_authority_drift_without_mutation(sessions, damage):
    storage = module()
    request, url = await setup(sessions)
    if damage == 'terminal-work':
        identity = await reserved(sessions, request, request.before.participants[1], phase='waiting')
        async with sessions.begin() as session:
            await session.execute(update(NebiusPoolRequest).where(NebiusPoolRequest.request_id == identity)
                .values(phase='cancelled_unstarted'))
    else:
        async with sessions.begin() as session:
            if damage == 'global':
                await session.execute(update(NebiusPoolBinding).where(NebiusPoolBinding.pool_id == request.before.pool_id)
                    .values(mode='global'))
            elif damage == 'participant-fenced':
                await session.execute(update(NebiusPoolParticipant).where(
                    NebiusPoolParticipant.participant_id == request.before.participants[1].participant_id).values(phase='fenced'))
            elif damage == 'credential':
                await session.execute(update(Token).where(Token.token_hash == bytes.fromhex(request.before.machines[0].token_sha256))
                    .values(revoked_at=datetime.now(UTC)))
            else:
                await session.execute(insert(NebiusPoolCancellation).values(cancellation_id=uuid4(),
                    pool_id=request.before.pool_id, participant_id=request.before.participants[1].participant_id,
                    workload_kind='trial', local_work_id=uuid4(), generation=1,
                    admission_epoch=request.before.admission_epoch, request_sha256='a' * 64))
    before = await snapshot(sessions)
    observed = await asyncio.to_thread(read_sql, url, storage.pool_storage_state_sql(request))
    assert observed['state'] is None
    with pytest.raises(ValueError):
        storage.qualify_storage_report(request, observed, read_only=True)
    with pytest.raises(psycopg.Error):
        await asyncio.to_thread(read_sql, url, storage.refresh_closed_storage_sql(request))
    assert await snapshot(sessions) == before


async def test_closed_storage_update_uses_global_admission_lock_without_partial_changes(sessions):
    storage = module()
    request, url = await setup(sessions)
    before = await snapshot(sessions)
    async with sessions.begin() as session:
        await acquire_pool_mutation_lock(session)
        with pytest.raises(psycopg.errors.LockNotAvailable):
            await asyncio.to_thread(read_sql, url, storage.refresh_closed_storage_sql(request))
    assert await snapshot(sessions) == before


@pytest.mark.parametrize('failure', [None, 'lost-before', 'lost-after'])
async def test_storage_registry_journal_never_redispatches_unknown_commit(sessions, tmp_path, failure):
    name = 'scripts.ops.nebius_pool_storage_registry'
    assert importlib.util.find_spec(name) is not None, 'storage registry recovery journal is missing'
    journal = importlib.import_module(name)
    storage = module()
    request, url = await setup(sessions)
    dispatches = []

    class API:
        def observe(self, selected):
            assert selected == request
            return read_sql(url, storage.pool_storage_state_sql(selected))

        def commit(self, selected):
            assert selected == request
            dispatches.append(selected.operation_id)
            if failure == 'lost-before':
                raise TimeoutError('transport outcome unknown')
            result = read_sql(url, storage.refresh_closed_storage_sql(selected))
            if failure == 'lost-after':
                raise TimeoutError('committed response lost')
            return result

    def run():
        return journal.advance_storage_registry(request=request, api=API(),
            state_dir=tmp_path / 'registry', qualify=lambda: None)

    before = await snapshot(sessions)
    if failure == 'lost-before':
        for _ in range(2):
            with pytest.raises(ValueError):
                await asyncio.to_thread(run)
        assert await snapshot(sessions) == before
    else:
        assert await asyncio.to_thread(run)
        after = await snapshot(sessions)
        assert after != before
        assert await asyncio.to_thread(run)
        assert await snapshot(sessions) == after
        journal.validate_storage_registry(request=request, state_dir=tmp_path / 'registry', complete=True)
    assert dispatches == [request.operation_id]


async def test_storage_publication_requires_new_exact_capture_and_never_opens_pool(sessions, tmp_path):
    name = 'scripts.ops.nebius_pool_storage_capacity'
    assert importlib.util.find_spec(name) is not None, 'closed storage aggregate proof is missing'
    command = importlib.import_module(name)
    from loom_execution_capacity_collector.contracts import QuotaResource
    from loom_service.pool_management.auth import resolve_pool_machine
    from tests.integration.test_nebius_pool_installation import installation
    from tests.integration.test_nebius_pool_observation_registry import (
        capture_scope,
        publish,
        snapshots,
    )

    before, tokens = installation(('staging', 'development'))
    before['quota_identities']['storage'][3] = 'compute.disk.size.network-ssd'
    after = json.loads(json.dumps(before))
    after['quota_identities']['storage'][3] = 'compute.disk.size.network-ssd-non-replicated'
    after['policy_revision'] += 1
    refresh = module().ClosedPoolStorageRefresh(schema_version='loom.closed-pool-storage-refresh.v1',
        operation_id=uuid4(), target_disk_type='NETWORK_SSD_NON_REPLICATED', before=before, after=after)
    async with sessions.begin() as session:
        await register_installation(session, refresh.before)
    observer, = (row for row in refresh.before.machines if row.role == 'observer')
    async def report(spec, *, wrong_quota=False):
        # Each real collector request authenticates against the current policy;
        # a principal resolved before the correction is deliberately stale.
        async with sessions() as session:
            principal = await resolve_pool_machine(session, 'Bearer ' + tokens[observer.machine_id])
        capture = await capture_scope(sessions, principal)
        provider, kubernetes = snapshots(capture)
        quotas = {key: QuotaResource(parent_id=value[0], region=value[1], service=value[2], name=value[3],
            unit=value[4], limit={'nodes': 2, 'vcpu': 8000, 'storage': 65536}[key], used=0)
            for key, value in spec.quota_identities.items()}
        if wrong_quota:
            quotas['storage'] = quotas['storage'].model_copy(update={'name': 'foreign-storage'})
        await publish(sessions, principal, capture, provider=provider.model_copy(update={'quota_resources': quotas}),
            kubernetes=kubernetes)

    await report(refresh.before)
    url = sessions.kw['bind'].url.render_as_string(hide_password=False)
    environment = {key: value for key, value in os.environ.items() if not key.startswith(('LOOM_', 'DATABASE_'))}
    environment.update(LOOM_SVC_DB_URL=url, LOOM_SVC_SERVICE_MODE='management')
    nonce = 'ab' * 32
    expected = command.expected_storage_capacity(refresh.after)
    signature = hmac.new(bytes.fromhex(nonce), json.dumps(expected, sort_keys=True,
        separators=(',', ':')).encode(), 'sha256').hexdigest()

    async def run():
        frozen = await snapshot(sessions)
        result = await asyncio.to_thread(subprocess.run, [sys.executable, '-c', command.BOUND_CLOSED_STORAGE_CAPACITY_COMMAND,
            str(refresh.after.pool_id), nonce, signature], env=environment, cwd=tmp_path,
            capture_output=True, timeout=35, check=False)
        assert await snapshot(sessions) == frozen
        assert all(token.encode() not in result.stdout + result.stderr for token in tokens.values())
        return result

    assert (await run()).returncode == 1  # A matching physical report cannot replace registry authority.
    await asyncio.to_thread(read_sql, url, module().refresh_closed_storage_sql(refresh))
    pending = await run()
    assert (pending.returncode, pending.stdout, pending.stderr) == (0, b'{"status": "pending"}\n', b'')
    await report(refresh.after)
    ready = await run()
    assert (ready.returncode, ready.stdout, ready.stderr) == (0, b'{"status": "qualified"}\n', b'')
    await report(refresh.after, wrong_quota=True)
    assert (await run()).returncode == 1
    async with sessions.begin() as session:
        await session.execute(update(NebiusPoolBinding).where(NebiusPoolBinding.pool_id == refresh.after.pool_id).values(mode='global'))
    assert (await run()).returncode == 1
