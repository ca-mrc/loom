"""Protected inventory is a real read-only observation, never mutation consent."""
from __future__ import annotations

import copy
import importlib
from dataclasses import replace
from datetime import UTC, datetime
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


async def test_capacity_inspection_is_read_only_bounded_and_not_an_available_slot_claim(sessions):
    module, job, selected, _change = await prepared(sessions)
    assert 'require_capacity' in module.PoolInspectionV2.model_fields, 'activation capacity inspection missing'
    # Ordinary retained inventory request/report bytes remain unchanged.
    assert 'require_capacity' not in job.request.model_dump(mode='json')
    ordinary = await observe(sessions, job)
    assert 'capacity' not in ordinary['snapshot']
    request = module.PoolInspectionV2.model_validate({**job.request.model_dump(mode='json'), 'require_capacity': True})
    grant = job.authorization.model_copy(update={'request_sha256': digest(request.model_dump(mode='json'))})
    before = None
    async with sessions() as session:
        before = await session.scalar(select(func.count()).select_from(NebiusPoolLifecycleOperation))
    result = await observe(sessions, replace(job, request=request, authorization=grant))
    jobs().validate_administration_report(replace(job, request=request, authorization=grant), result)
    capacity, = result['snapshot']['capacity']
    assert capacity['pool_id'] == str(selected.pool_id)
    assert capacity['pool_authority_sha256'] == ordinary['snapshot']['pool_authority_sha256']
    assert datetime.fromisoformat(result['snapshot']['valid_until']) <= datetime.fromisoformat(capacity['valid_until'])
    assert set(capacity) == {'pool_id', 'pool_authority_sha256', 'observation_id', 'observed_at', 'valid_until', 'placement_sha256'}
    # Removing a required observation is not a successful inventory-only report.
    damaged = copy.deepcopy(result)
    damaged['snapshot'].pop('capacity')
    with pytest.raises(ValueError):
        jobs().validate_administration_report(replace(job, request=request, authorization=grant), damaged)
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(NebiusPoolLifecycleOperation)) == before


async def test_capacity_inspection_requires_authentic_observation_but_enrollment_does_not(sessions):
    from tests.integration.test_nebius_pool_participant_lifecycle import platform

    _, spec, _, _, _, _ = await platform(sessions)
    module = inspection()
    assert 'require_capacity' in module.PoolInspectionV2.model_fields, 'activation capacity inspection missing'
    selected = spec.participants[2]
    body = dict(schema_version='loom.pool-inspection.v2', operation_id=uuid4(), installation_id=spec.installation_id,
        pool_id=spec.pool_id, participant_id=selected.participant_id, environment_id=selected.environment_id,
        incarnation=selected.incarnation, environment_class=selected.environment_class)
    async def run(required):
        request = module.PoolInspectionV2(**body, require_capacity=required)
        grant = authorization(administration(), selected, request)
        async with sessions.begin() as session:
            await session.execute(text('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY'))
            return await administration().administer(session, request.model_dump_json().encode(), grant, action='status')
    assert (await run(False))['snapshot']['participant'] is None
    with pytest.raises(ValueError, match='pool_observation_unavailable'):
        await run(True)


@pytest.mark.parametrize('transaction', ['READ WRITE', 'READ ONLY'])
async def test_unlocked_capacity_cannot_replace_serialized_admission_or_use_mixed_snapshots(sessions, transaction):
    from loom_service.pool_management.capacity import observe_connected_capacity

    _module, _job, selected, _change = await prepared(sessions)
    async with sessions.begin() as session:
        await session.execute(text('SET TRANSACTION ISOLATION LEVEL READ COMMITTED, ' + transaction))
        with pytest.raises(ValueError, match='pool_inspection_requires_read_only_snapshot'):
            await observe_connected_capacity(session, selected.pool_id, datetime.now(UTC))


@pytest.mark.parametrize('damage', [None, 'different_grant', 'different_head'])
async def test_platform_authority_inspection_binds_committed_protected_open_not_just_global_mode(sessions, damage):
    from datetime import timedelta

    from loom_service.pool_management.administration import PoolAdministrationAuthorizationV2
    from loom_service.pool_management.lifecycle_contracts import PoolPlatformOperationV2
    from tests.integration.test_nebius_pool_platform_lifecycle import apply, prepare

    module = inspection()
    assert 'platform_authorization_id' in module.PoolInspectionV2.model_fields, 'audited platform authority readback missing'
    _module, spec, installed, authority, capture, opening = await prepare(sessions)
    await capture()
    operation = opening(installed.operation_id, authority)
    committed = await apply(sessions, operation)
    await capture()  # Opening advanced physical authority; never reuse pre-open capacity.
    authorization_id = committed['authorization_id']
    if damage == 'different_grant':
        authorization_id = str(uuid4())
    elif damage == 'different_head':
        close = PoolPlatformOperationV2(schema_version='loom.pool-platform-operation.v2', operation_id=uuid4(),
            installation_id=spec.installation_id, pool_id=spec.pool_id, expected_head=operation.operation_id,
            pool_authority_sha256=committed['receipt']['pool_authority_sha256'], action='close')
        closed = await apply(sessions, close)
        await capture()
        await apply(sessions, opening(close.operation_id, closed['receipt']['pool_authority_sha256']))
        await capture()
    request = module.PoolInspectionV2(schema_version='loom.pool-inspection.v2', operation_id=uuid4(),
        installation_id=spec.installation_id, pool_id=spec.pool_id,
        require_capacity=True, platform_authorization_id=authorization_id)
    now = datetime.now(UTC)
    grant = PoolAdministrationAuthorizationV2(schema_version='loom.pool-administration-authorization.v2',
        authorization_id=uuid4(), installation_id=spec.installation_id, pool_id=spec.pool_id, authority='platform',
        request_sha256=digest(request.model_dump(mode='json')), issued_at=now, expires_at=now + timedelta(minutes=5))

    async def read():
        async with sessions.begin() as session:
            await session.execute(text('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY'))
            result = await administration().administer(session, request.model_dump_json().encode(), grant, action='status')
            return module.qualify_inspection(result, request, grant.authorization_id)

    if damage:
        with pytest.raises(ValueError, match='platform_authority'):
            await read()
        return
    observed = await read()
    assert observed.platform_authority.operation_id == operation.operation_id
    assert str(observed.platform_authority.authorization_id) == authorization_id
    assert observed.platform_authority.request_sha256 == digest(operation.model_dump(mode='json'))
    assert observed.platform_authority.receipt_sha256 == digest(committed['receipt'])
    # The snapshot reports only a committed authority reference, not legacy
    # participant material, token hashes or private prior readiness documents.
    assert set(observed.platform_authority.model_dump()) == {
        'operation_id', 'authorization_id', 'authorization_sha256', 'request_sha256', 'receipt_sha256'}
