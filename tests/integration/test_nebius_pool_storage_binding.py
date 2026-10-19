"""Physical quota correction is atomic and refuses any used or unqualified pool."""
from __future__ import annotations

import asyncio
import importlib
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
