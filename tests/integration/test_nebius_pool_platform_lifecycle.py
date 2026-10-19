"""A fresh generic platform opens through protected authority, without staging."""
from __future__ import annotations

import importlib
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import func, select

from loom.db.nebius_pool_schema import NebiusPoolBinding, NebiusPoolParticipant
from loom_execution_capacity_collector.contracts import CapacityPlacement
from loom_service.pool_management.administration import PoolAdministrationAuthorizationV2, administer
from loom_service.pool_management.auth import resolve_pool_machine
from loom_service.pool_management.capacity import digest
from loom_service.pool_management.observation_binding import physical_binding_digest
from tests.execution_placement_fixtures import placement_fixture
from tests.integration.test_nebius_pool_participant_lifecycle import platform
from tests.integration.test_nebius_pool_registry import publish_placement
from tests.integration.test_nebius_pool_registry import sessions as sessions


def lifecycle():
    name = 'loom_service.pool_management.platform_lifecycle'
    assert importlib.util.find_spec(name), 'protected platform opening is missing'
    return importlib.import_module(name)


async def prepare(sessions):
    module = lifecycle()
    _, spec, installed, tokens, _, authority = await platform(sessions)

    async def observe():
        observer, = (row for row in spec.machines if row.role == 'observer')
        async with sessions() as session:
            principal = await resolve_pool_machine(session, 'Bearer ' + tokens[observer.machine_id])
        value = placement_fixture(target_id='group-1', parent_id='parent', quota_nodes=1,
            node_cpu=32000, node_memory=65536, node_storage=262144)
        del value['quota_resources']['memory']
        await publish_placement(sessions, principal, CapacityPlacement.model_validate(value))

    def opening(head, checksum):
        return module.PoolPlatformOperationV2(
            schema_version='loom.pool-platform-operation.v2', operation_id=uuid4(),
            installation_id=spec.installation_id, pool_id=spec.pool_id, expected_head=head,
            pool_authority_sha256=checksum, action='open',
            readiness=dict(observed_at=datetime.now(UTC), valid_until=datetime.now(UTC) + timedelta(minutes=1),
                pool_authority_sha256=checksum, exclusive_writer_sha256='a' * 64,
                gateway_machine_id=next(row.machine_id for row in spec.machines if row.role == 'gateway'),
                observer_machine_id=next(row.machine_id for row in spec.machines if row.role == 'observer'),
                gateway_credential_epoch=1, observer_credential_epoch=1,
                capabilities=('physical-v1', 'registry-v1', 'scoped-lifecycle-v1')))

    return module, spec, installed, authority, observe, opening


async def apply(sessions, request, *, authority='platform'):
    grant = PoolAdministrationAuthorizationV2(schema_version='loom.pool-administration-authorization.v2',
        authorization_id=uuid4(), installation_id=request.installation_id, pool_id=request.pool_id,
        authority=authority, request_sha256=digest(request.model_dump(mode='json')),
        issued_at=datetime.now(UTC), expires_at=datetime.now(UTC) + timedelta(minutes=5),
        **({} if authority == 'platform' else dict(participant_id=uuid4(), environment_id=uuid4(), incarnation=uuid4())))
    async with sessions.begin() as session:
        return await administer(session, request.model_dump_json().encode(), grant, action='apply')


async def test_empty_platform_opens_without_any_environment_and_exact_old_replay_never_reopens(sessions):
    module, spec, installed, authority, observe, opening = await prepare(sessions)
    await observe()
    request = opening(installed.operation_id, authority)
    result = await apply(sessions, request)
    assert result['receipt']['mode'] == 'global'
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(NebiusPoolParticipant)) == 0
        row = await session.get(NebiusPoolBinding, spec.pool_id)
        assert row.mode == 'global' and row.admission_epoch == installed.admission_epoch
        assert physical_binding_digest(row) == result['receipt']['pool_authority_sha256'] != authority
    close = module.PoolPlatformOperationV2(schema_version='loom.pool-platform-operation.v2', operation_id=uuid4(),
        installation_id=spec.installation_id, pool_id=spec.pool_id, expected_head=request.operation_id,
        pool_authority_sha256=result['receipt']['pool_authority_sha256'], action='close')
    closed = await apply(sessions, close)
    assert closed['receipt']['mode'] == 'closed'
    replay = await apply(sessions, request)
    assert replay['receipt'] == result['receipt']
    async with sessions() as session:
        assert (await session.get(NebiusPoolBinding, spec.pool_id)).mode == 'closed'
    stale = opening(request.operation_id, result['receipt']['pool_authority_sha256'])
    with pytest.raises(ValueError):
        await apply(sessions, stale)
    reopen = opening(close.operation_id, closed['receipt']['pool_authority_sha256'])
    with pytest.raises(ValueError):
        await apply(sessions, reopen)  # Opening needs fresh post-transition physical evidence.
    await observe()
    assert (await apply(sessions, reopen))['receipt']['mode'] == 'global'


@pytest.mark.parametrize('damage', ['expired', 'observer', 'gateway_epoch', 'capability', 'authority', 'predecessor', 'dev_grant', 'no_capture'])
async def test_unqualified_platform_open_cannot_change_mode(sessions, damage):
    _, spec, installed, authority, observe, opening = await prepare(sessions)
    if damage != 'no_capture':
        await observe()
    request = opening(installed.operation_id, authority)
    updates = {}
    if damage == 'expired':
        updates = dict(observed_at=datetime.now(UTC) - timedelta(minutes=2), valid_until=datetime.now(UTC) - timedelta(minutes=1))
    elif damage == 'observer':
        updates = dict(observer_machine_id=uuid4())
    elif damage == 'gateway_epoch':
        updates = dict(gateway_credential_epoch=2)
    elif damage == 'capability':
        updates = dict(capabilities=('physical-v1',))
    elif damage == 'authority':
        request = request.model_copy(update={'pool_authority_sha256': 'e' * 64})
    elif damage == 'predecessor':
        request = request.model_copy(update={'expected_head': uuid4()})
    if updates:
        request = request.model_copy(update={'readiness': request.readiness.model_copy(update=updates)})
    with pytest.raises(ValueError):
        await apply(sessions, request, authority='development' if damage == 'dev_grant' else 'platform')
    async with sessions() as session:
        assert (await session.get(NebiusPoolBinding, spec.pool_id)).mode == 'closed'
