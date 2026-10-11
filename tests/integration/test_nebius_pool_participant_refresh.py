"""Protected participant refresh preserves every other participant and its work."""
from __future__ import annotations

import copy
import importlib
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import func, insert, select, update

from loom.db.nebius_pool_schema import NebiusPoolBinding, NebiusPoolParticipant, NebiusPoolRequest
from loom_service.pool_management.auth import authorize_pool_machine, resolve_pool_machine
from loom_service.pool_management.installation import PoolInstallation, register_installation
from tests.integration.test_nebius_pool_installation import installation
from tests.integration.test_nebius_pool_registry import sessions as sessions


def module():
    name = 'loom_service.pool_management.participant_refresh'
    assert importlib.util.find_spec(name) is not None, 'protected participant refresh transaction is missing'
    return importlib.import_module(name)


def selection():
    before, tokens = installation(('staging', 'development'))
    after = copy.deepcopy(before)
    after['operation_id'] = str(uuid4())
    after['policy_revision'] += 1
    selected = after['participants'][0]
    selected['binding_revision'] += 1
    old = selected['targets'][0]['profile_id']
    selected['targets'][0]['profile_id'] = str(uuid4())
    for row in after['profiles']['execution']:
        if row['profile_id'] == old:
            row.update(profile_id=selected['targets'][0]['profile_id'], candidate_sha='2' * 40,
                runtime_image_ref='registry.example/runtime@sha256:' + 'd' * 64,
                runtime_binary_sha256='sha256:' + 'e' * 64)
    for row in after['profiles']['task_images']:
        if row['profile_id'] == old:
            row['profile_id'] = selected['targets'][0]['profile_id']
            row['settings']['service_image'] = 'registry.example/service@sha256:' + 'f' * 64
    return {'schema_version': 'loom.pool-participant-refresh.v1', 'participant_id': selected['participant_id'],
        'before': before, 'after': after}, tokens


@pytest.mark.parametrize('damage', ['pool-policy', 'other-profile', 'other-revision', 'selected-identity',
    'selected-priority', 'selected-build-config', 'selected-runtime', 'candidate-reuse', 'credential',
    'epoch', 'revision', 'operation', 'profile-reuse', 'keyring'])
def test_refresh_rejects_widened_scope_even_when_both_installations_are_valid(damage):
    value, _ = selection()
    after = value['after']
    if damage == 'pool-policy':
        after['admission']['build_concurrency_limit'] += 1
    elif damage == 'other-profile':
        after['profiles']['execution'][1]['runtime_binary_sha256'] = 'sha256:' + 'a' * 64
    elif damage == 'other-revision':
        after['participants'][1]['binding_revision'] += 1
    elif damage == 'selected-identity':
        after['participants'][0]['incarnation'] = str(uuid4())
    elif damage == 'selected-priority':
        after['participants'][0]['environment_class'] = 'production'
    elif damage == 'selected-build-config':
        after['profiles']['task_images'][0]['settings']['cpu_millis'] = 9000
    elif damage == 'selected-runtime':
        after['profiles']['execution'][0]['runtime']['credential_broker_url'] = 'https://foreign.invalid'
    elif damage == 'candidate-reuse':
        after['profiles']['execution'][0]['runtime_binary_sha256'] = value['before']['profiles']['execution'][0]['runtime_binary_sha256']
        after['profiles']['execution'][0]['candidate_sha'] = value['before']['profiles']['execution'][0]['candidate_sha']
    elif damage == 'credential':
        after['machines'][0]['credential_epoch'] += 1
    elif damage == 'epoch':
        after['admission_epoch'] += 1
        for participant in after['participants']:
            participant['admission_epoch'] = after['admission_epoch']
    elif damage == 'revision':
        after['participants'][0]['binding_revision'] += 1
    elif damage == 'operation':
        after['operation_id'] = value['before']['operation_id']
    elif damage == 'profile-reuse':
        old = value['before']['participants'][0]['targets'][0]['profile_id']
        after['participants'][0]['targets'][0]['profile_id'] = old
        after['profiles']['execution'][0]['profile_id'] = old
        after['profiles']['task_images'][0]['profile_id'] = old
    else:
        after['profiles']['image_admission_keyring']['keys'][0]['signing_key_id'] += '-changed'
    # A schema-invalid installation would not test this new delta boundary.
    PoolInstallation.model_validate(value['before'])
    PoolInstallation.model_validate(after)
    with pytest.raises(ValueError):
        module().PoolParticipantRefresh.model_validate(value)


async def setup(sessions):
    value, tokens = selection()
    request = module().PoolParticipantRefresh.model_validate(value)
    async with sessions.begin() as session:
        await register_installation(session, request.before)
        await session.execute(update(NebiusPoolBinding).where(NebiusPoolBinding.pool_id == request.before.pool_id)
            .values(mode='global'))
    return request, tokens


async def reserved(sessions, request, participant, *, phase='reserved'):
    identity = uuid4()
    async with sessions.begin() as session:
        await session.execute(insert(NebiusPoolRequest).values(request_id=identity, pool_id=request.before.pool_id,
            participant_id=participant.participant_id, namespace_uid=participant.execution_namespace.uid,
            workload_kind='trial', local_work_id=uuid4(), generation=1, admission_epoch=participant.admission_epoch,
            target_id=participant.targets[0].target_id, request_sha256='a' * 64, request_json={'retained': True},
            deadline_at=datetime.now(UTC) + timedelta(minutes=10), phase=phase, cpu_millis=1000, memory_mib=1024,
            ephemeral_storage_mib=1024, pod_slots=1, granted_at=func.clock_timestamp() if phase == 'reserved' else None,
            priority=2))
    return identity


async def snapshot(sessions):
    async with sessions() as session:
        return {table.__tablename__: [{column.name: getattr(row, column.name) for column in table.__table__.columns}
            for row in await session.scalars(select(table).order_by(*table.__table__.primary_key))]
            for table in (NebiusPoolBinding, NebiusPoolParticipant, NebiusPoolRequest)}


async def test_refresh_fences_only_selected_participant_and_preserves_charged_neighbor(sessions):
    refresh = module()
    request, tokens = await setup(sessions)
    neighbor = request.before.participants[1]
    busy = await reserved(sessions, request, neighbor)
    original = await snapshot(sessions)

    async def advance(action):
        async with sessions.begin() as session:
            return await refresh.refresh_participant(session, request, action=action)

    for action, expected in (('observe', 'ready'), ('fence', 'fenced'), ('fence', 'fenced'),
            ('stage', 'staged'), ('stage', 'staged'), ('observe', 'staged')):
        result = await advance(action)
        assert result['status'] == expected
        assert result['operation_id'] == str(request.after.operation_id)
        assert result['participant_id'] == str(request.participant_id)
    staged = await snapshot(sessions)
    assert staged['nebius_pool_requests'] == original['nebius_pool_requests']
    assert staged['nebius_pool_requests'][0]['request_id'] == busy
    other_before, = (row for row in original['nebius_pool_participants'] if row['participant_id'] == neighbor.participant_id)
    other_after, = (row for row in staged['nebius_pool_participants'] if row['participant_id'] == neighbor.participant_id)
    assert other_after == other_before
    assert staged['nebius_pool_bindings'][0]['mode'] == 'global'
    assert staged['nebius_pool_bindings'][0]['admission_epoch'] == request.before.admission_epoch
    assert staged['nebius_pool_bindings'][0]['policy_revision'] == request.before.policy_revision + 1
    selected, = (row for row in staged['nebius_pool_participants'] if row['participant_id'] == request.participant_id)
    assert selected['phase'] == 'fenced' and selected['binding_json'] == request.after.participants[0].model_dump(mode='json')
    async with sessions.begin() as session:
        for machine in request.before.machines:
            principal = await resolve_pool_machine(session, 'Bearer ' + tokens[machine.machine_id])
            assert principal is not None
            await authorize_pool_machine(session, principal, role=machine.role, pool_id=request.before.pool_id,
                participant_id=machine.participant_id)
            if machine.participant_id == neighbor.participant_id:
                assert principal.participant_revision == neighbor.binding_revision and principal.participant_phase == 'active'
    assert (await advance('open'))['status'] == 'complete'
    # Completed readback may not re-fence a participant now serving new work.
    await reserved(sessions, request, request.after.participants[0])
    completed = await snapshot(sessions)
    assert (await advance('open'))['status'] == 'complete'
    assert (await advance('observe'))['status'] == 'complete'
    for action in ('stage', 'fence'):
        with pytest.raises(ValueError):
            await advance(action)
    assert await snapshot(sessions) == completed


@pytest.mark.parametrize('phase', ['waiting', 'reserved'])
async def test_refresh_never_fences_selected_outstanding_work(sessions, phase):
    request, _ = await setup(sessions)
    await reserved(sessions, request, request.before.participants[0], phase=phase)
    original = await snapshot(sessions)
    async with sessions.begin() as session:
        result = await module().refresh_participant(session, request, action='fence')
        assert result['status'] == 'pending_work'
    assert await snapshot(sessions) == original


async def test_refresh_requires_its_predecessor_and_order_and_caller_owned_commit(sessions):
    request, _ = await setup(sessions)
    refresh = module()
    original = await snapshot(sessions)
    for action in ('stage', 'open'):
        with pytest.raises(ValueError):
            async with sessions.begin() as session:
                await refresh.refresh_participant(session, request, action=action)
    assert await snapshot(sessions) == original
    async with sessions() as session:
        assert (await refresh.refresh_participant(session, request, action='fence'))['status'] == 'fenced'
        await session.rollback()
    assert await snapshot(sessions) == original
    async with sessions.begin() as session:
        await session.execute(update(NebiusPoolParticipant).where(
            NebiusPoolParticipant.participant_id == request.before.participants[1].participant_id).values(phase='fenced'))
    changed = await snapshot(sessions)
    with pytest.raises(ValueError):
        async with sessions.begin() as session:
            await refresh.refresh_participant(session, request, action='fence')
    assert await snapshot(sessions) == changed
