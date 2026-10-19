"""Protected inventory is a real read-only observation, never mutation consent."""
from __future__ import annotations

import copy
import importlib
from dataclasses import replace
from uuid import uuid4

import pytest
from sqlalchemy import func, select, text

from loom.db.nebius_pool_lifecycle_schema import NebiusPoolLifecycleOperation
from loom_service.pool_management.capacity import digest
from tests.integration.test_nebius_pool_administration import administration, authorization
from tests.integration.test_nebius_pool_administration_job import inputs, jobs
from tests.integration.test_nebius_pool_registry import sessions as sessions


def inspection():
    name = 'loom_service.pool_management.inspection'
    assert importlib.util.find_spec(name), 'protected read-only pool inspection is missing'
    return importlib.import_module(name)


async def prepared(sessions):
    module = inspection()
    original, selected, change = await inputs(sessions)
    request = module.PoolInspectionV2(schema_version='loom.pool-inspection.v2',
        operation_id=uuid4(), installation_id=selected.installation_id, pool_id=selected.pool_id,
        participant_id=selected.participant_id, environment_id=selected.environment_id,
        incarnation=selected.incarnation, environment_class=selected.environment_class)
    grant = authorization(administration(), selected, request)
    job = replace(original, request=request, authorization=grant, action='status', observation_id=uuid4())
    return module, job, selected, change


async def observe(sessions, job):
    async with sessions.begin() as session:
        await session.execute(text('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY'))
        return await administration().administer(session, job.request.model_dump_json().encode(),
            job.authorization, action='status')


async def test_inspection_reports_exact_current_identity_without_audit_or_lifecycle_writes(sessions):
    _module, job, selected, change = await prepared(sessions)
    async with sessions() as session:
        before = await session.scalar(select(func.count()).select_from(NebiusPoolLifecycleOperation))
    first = await observe(sessions, job)
    jobs().validate_administration_report(job, first)
    snapshot = first['snapshot']
    assert snapshot['participant']['binding'] == selected.model_dump(mode='json')
    assert snapshot['participant']['head']['state'] == 'active'
    assert snapshot['pool']['pool_id'] == str(selected.pool_id)
    assert snapshot['pool']['cluster_id'] == 'cluster-1'
    assert set(snapshot['capabilities']) == {'physical-v1', 'registry-v1', 'scoped-lifecycle-v1'}
    assert {row['role'] for row in snapshot['machines']} == {'gateway', 'observer'}
    assert all(set(row) == {'machine_id', 'role', 'credential_epoch'} for row in snapshot['machines'])
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(NebiusPoolLifecycleOperation)) == before
    await change(selected, 'suspend')
    second = await observe(sessions, job)
    assert second['snapshot']['participant']['head']['state'] == 'suspended'
    assert second['snapshot']['pool_authority_sha256'] == snapshot['pool_authority_sha256']


async def test_unenrolled_selection_is_explicitly_absent_not_a_same_class_peer(sessions):
    _module, job, _selected, _change = await prepared(sessions)
    request = job.request.model_copy(update={'participant_id': uuid4(), 'environment_id': uuid4(), 'incarnation': uuid4()})
    grant = job.authorization.model_copy(update={'participant_id': request.participant_id,
        'environment_id': request.environment_id, 'incarnation': request.incarnation,
        'request_sha256': digest(request.model_dump(mode='json'))})
    result = await observe(sessions, replace(job, request=request, authorization=grant))
    assert result['snapshot']['participant'] is None


@pytest.mark.parametrize('damage', ['apply', 'class', 'installation', 'incarnation', 'request'])
async def test_inspection_scope_cannot_mutate_or_cross_authority(sessions, damage):
    _module, job, _selected, _change = await prepared(sessions)
    grant = job.authorization
    if damage == 'class':
        grant = grant.model_copy(update={'authority': 'production'})
    elif damage in {'installation', 'incarnation'}:
        grant = grant.model_copy(update={damage + '_id' if damage == 'installation' else damage: uuid4()})
    elif damage == 'request':
        grant = grant.model_copy(update={'request_sha256': 'f' * 64})
    with pytest.raises(ValueError):
        async with sessions.begin() as session:
            await administration().administer(session, job.request.model_dump_json().encode(), grant,
                action='apply' if damage == 'apply' else 'status')


@pytest.mark.parametrize('damage', ['request', 'binding', 'participant', 'catalog', 'capability'])
async def test_job_readback_rejects_rebound_or_incoherent_inventory(sessions, damage):
    _module, job, _selected, _change = await prepared(sessions)
    report = copy.deepcopy(await observe(sessions, job))
    if damage == 'request':
        report['request_sha256'] = 'f' * 64
    elif damage == 'binding':
        report['snapshot']['pool']['node_group_id'] = 'foreign'
    elif damage == 'participant':
        report['snapshot']['participant']['binding']['environment_id'] = str(uuid4())
    elif damage == 'catalog':
        report['snapshot']['participant']['catalog_sha256'] = 'f' * 64
    else:
        report['snapshot']['capabilities'] = []
    with pytest.raises(ValueError):
        jobs().validate_administration_report(job, report)


async def test_inspection_job_cannot_be_rendered_as_apply(sessions):
    _module, job, _selected, _change = await prepared(sessions)
    documents = jobs().administration_documents(job)
    container = next(row for row in documents.values() if row['kind'] == 'Job')['spec']['template']['spec']['containers'][0]
    assert {'name': 'LOOM_POOL_ADMINISTRATION_ACTION', 'value': 'status'} in container['env']
    with pytest.raises(ValueError):
        jobs().administration_documents(replace(job, action='apply', observation_id=None))
