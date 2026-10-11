"""Protected participant refresh preserves every other participant and its work."""
from __future__ import annotations

import copy
import importlib
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import func, insert, select, update

from loom.db.nebius_pool_schema import (
    NebiusPoolBinding,
    NebiusPoolMachine,
    NebiusPoolMachineCredential,
    NebiusPoolParticipant,
    NebiusPoolRequest,
)
from loom.db.schema import Token
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
    before['node_selector'] = copy.deepcopy(before['profiles']['execution'][0]['runtime']['node_selector'])
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
            for table in (NebiusPoolBinding, NebiusPoolParticipant, NebiusPoolRequest,
                NebiusPoolMachine, NebiusPoolMachineCredential, Token)}


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
    for table in ('nebius_pool_machines', 'nebius_pool_machine_credentials', 'tokens'):
        assert staged[table] == original[table]
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


@pytest.mark.parametrize('damage', ['closed', 'revision', 'payload', 'machine', 'revoked-token', 'extra-credential'])
async def test_refresh_rejects_live_authority_drift_without_partial_writes(sessions, damage):
    request, _ = await setup(sessions)
    machine = request.before.machines[0]
    token_hash = bytes.fromhex(machine.token_sha256)
    async with sessions.begin() as session:
        if damage in {'closed', 'revision', 'payload'}:
            changes = {'mode': 'closed'} if damage == 'closed' else {'policy_revision': request.before.policy_revision + 1}
            if damage == 'payload':
                changes['binding_sha256'] = 'f' * 64
            await session.execute(update(NebiusPoolBinding).where(NebiusPoolBinding.pool_id == request.before.pool_id).values(**changes))
        elif damage == 'machine':
            await session.execute(update(NebiusPoolMachine).where(NebiusPoolMachine.machine_id == machine.machine_id)
                .values(phase='revoked'))
        elif damage == 'revoked-token':
            await session.execute(update(Token).where(Token.token_hash == token_hash).values(revoked_at=func.clock_timestamp()))
        else:
            await session.execute(insert(Token).values(token_hash=b'x' * 32, type='pool_machine', scopes=[],
                issued_at=machine.issued_at, expires_at=machine.expires_at))
            await session.execute(insert(NebiusPoolMachineCredential).values(token_hash=b'x' * 32,
                machine_id=machine.machine_id, credential_epoch=machine.credential_epoch))
    original = await snapshot(sessions)
    with pytest.raises(ValueError):
        async with sessions.begin() as session:
            await module().refresh_participant(session, request, action='fence')
    assert await snapshot(sessions) == original


async def test_refresh_revalidates_nested_mutation_and_preserves_dirty_session(sessions):
    request, _ = await setup(sessions)
    original = await snapshot(sessions)
    request.after.node_selector['foreign'] = 'scope'
    with pytest.raises(ValueError):
        async with sessions.begin() as session:
            await module().refresh_participant(session, request, action='fence')
    del request.after.node_selector['foreign']
    async with sessions() as session:
        pool = await session.get(NebiusPoolBinding, request.before.pool_id)
        pool.mode = 'closed'
        with pytest.raises(ValueError):
            await module().refresh_participant(session, request, action='fence')
        assert pool.mode == 'closed' and pool in session.dirty
        await session.rollback()
    assert await snapshot(sessions) == original


def test_release_can_keep_identical_runtime_binary_and_image_when_the_source_changes():
    value, _ = selection()
    for field in ('runtime_binary_sha256', 'runtime_image_ref'):
        value['after']['profiles']['execution'][0][field] = value['before']['profiles']['execution'][0][field]
    module().PoolParticipantRefresh.model_validate(value)


@pytest.mark.parametrize('condition', ['fresh', 'before-open', 'expired', 'connected-stale', 'unrelated-stale', 'full', 'zero'])
async def test_capacity_read_requires_fresh_post_open_component_without_demanding_headroom(sessions, condition):
    from loom.db.nebius_pool_schema import NebiusPoolObservation
    from loom_execution_capacity_collector.contracts import CapacityPlacement
    from loom_service.pool_management.capacity import digest
    from tests.execution_placement_fixtures import placement_fixture
    from tests.integration.test_nebius_pool_registry import publish_placement
    from tests.integration.test_nebius_pool_registry import setup as setup_neighbor

    request, tokens = await setup(sessions)
    machine, = (row for row in request.before.machines if row.role == 'observer')
    payload = placement_fixture(target_id='group-1', parent_id='parent', quota_nodes=1,
        node_cpu=32000, node_memory=65536, node_storage=262144,
        requested_cpu=32000 if condition == 'full' else 0)
    del payload['quota_resources']['memory']
    if condition == 'zero':
        payload['nodes'] = []
        payload['node_group']['node_count'] = 0
        payload['template_samples'] = []
    placement = CapacityPlacement.model_validate(payload)

    async def publish_current():
        # Principals bind current policy revision; resolve afresh after staging.
        async with sessions() as session:
            observer = await resolve_pool_machine(session, 'Bearer ' + tokens[machine.machine_id])
        await publish_placement(sessions, observer, placement)

    for action in ('fence', 'stage'):
        async with sessions.begin() as session:
            await module().refresh_participant(session, request, action=action)
    if condition == 'before-open':
        await publish_current()
        async with sessions.begin() as session:
            with pytest.raises(ValueError, match='capacity_requires_complete'):
                await module().refresh_participant(session, request, action='capacity')
    async with sessions.begin() as session:
        await module().refresh_participant(session, request, action='open')
    if condition != 'before-open':
        await publish_current()
    stale_pool = None
    if condition == 'expired':
        stale_pool = request.after.pool_id
    elif condition in {'connected-stale', 'unrelated-stale'}:
        neighbors, *_ = await setup_neighbor(sessions, group_id='group-2', memory_quota=False,
            parent_id='parent' if condition == 'connected-stale' else 'foreign-parent')
        stale_pool = neighbors[0].pool_id
    if stale_pool is not None:
        async with sessions.begin() as session:
            observation = await session.scalar(select(NebiusPoolObservation).where(NebiusPoolObservation.pool_id == stale_pool))
            expired = datetime.now(UTC) - timedelta(minutes=30)
            data = {**observation.observation_json, 'observed_at': expired.isoformat()}
            await session.execute(update(NebiusPoolObservation).where(NebiusPoolObservation.observation_id == observation.observation_id)
                .values(observed_at=expired, observation_json=data, observation_sha256=digest(data)))
    before = await snapshot(sessions)
    async with sessions.begin() as session:
        if condition in {'before-open', 'expired', 'connected-stale'}:
            with pytest.raises(ValueError, match='pool_observation_unavailable'):
                await module().refresh_participant(session, request, action='capacity')
        else:
            result = await module().refresh_participant(session, request, action='capacity')
            assert result['status'] == 'complete'
            observed = await session.scalar(select(NebiusPoolObservation).where(NebiusPoolObservation.pool_id == request.after.pool_id))
            proof = result['capacity']
            assert proof['selected_observation_id'] == str(observed.observation_id)
            assert datetime.fromisoformat(proof['selected_observed_at']) == observed.observed_at
            assert proof['connected_pools'] == 1
            checked, until = (datetime.fromisoformat(proof[key]) for key in ('checked_at', 'valid_until'))
            assert observed.observed_at <= checked < until
            assert until == observed.observed_at + timedelta(seconds=request.after.admission.observation_max_age_seconds)
            assert len(proof['observations_sha256']) == 64
    assert await snapshot(sessions) == before


@pytest.mark.parametrize('kind', ['execution', 'build'])
async def test_refresh_preserves_real_neighbor_activation_and_requires_fresh_capacity_after_each_transition(sessions, kind):
    from loom.nebius_pool_task_image import PoolTaskImagePrepareV1
    from loom.nebius_pool_workload import PoolExecutionPrepareV1
    from loom_execution_capacity_collector.contracts import CapacityPlacement
    from loom_service.pool_management.observations import PoolObservationError
    from loom_service.pool_management.registry import PoolAdmissionError
    from tests.execution_placement_fixtures import placement_fixture
    from tests.integration.test_nebius_pool_build_admission import prepare_build
    from tests.integration.test_nebius_pool_control import action, operate
    from tests.integration.test_nebius_pool_observation_registry import capture_scope, publish
    from tests.integration.test_nebius_pool_registry import prepare, publish_placement
    from tests.support.execution_image_admission import signed_image_admission_bundle
    from tests.unit.test_nebius_pool_execution_render import inputs
    from tests.unit.test_nebius_pool_task_image_render import build_inputs

    request, tokens = await setup(sessions)
    neighbor = request.before.participants[1]
    placement_json = placement_fixture(target_id='group-1', parent_id='parent', quota_nodes=1,
        node_cpu=32000, node_memory=65536, node_storage=262144)
    del placement_json['quota_resources']['memory']
    placement = CapacityPlacement.model_validate(placement_json)

    async def principal(owner=None):
        machine, = (row for row in request.before.machines if row.role == ('observer' if owner is None else 'participant')
            and row.participant_id == owner)
        async with sessions() as session:
            return await resolve_pool_machine(session, 'Bearer ' + tokens[machine.machine_id])

    def workload(participant, *, successor=False):
        if kind == 'execution':
            _, body = inputs()
            model = PoolExecutionPrepareV1
            if successor:
                runtime = body['execution']['runtime']
                runtime.update(candidate_sha='2' * 40, runtime_image_ref='registry.example/runtime@sha256:' + 'd' * 64,
                    runtime_binary_sha256='sha256:' + 'e' * 64)
                runtime['image_admission'] = signed_image_admission_bundle(
                    (runtime['task_image_ref'], runtime['runtime_image_ref']), now=datetime.now(UTC)).model_dump(mode='json')
        else:
            _, body, _ = build_inputs()
            model = PoolTaskImagePrepareV1
        body.update(pool_id=participant.pool_id, admission_epoch=participant.admission_epoch,
            participant_revision=participant.binding_revision)
        body['key']['participant_id'] = participant.participant_id
        body['origin']['data_environment_id'] = participant.environment_id
        return model.model_validate(body)

    preparing = prepare if kind == 'execution' else prepare_build
    before_profiles = request.before.profiles.profiles()
    after_profiles = request.after.profiles.profiles()
    await publish_placement(sessions, await principal(), placement)
    retained_body = workload(neighbor)
    retained = await preparing(sessions, await principal(neighbor.participant_id), retained_body, before_profiles)
    assert retained.phase == 'reserved'
    selected_body = workload(request.before.participants[0])
    before_capture = await capture_scope(sessions, await principal())

    async with sessions.begin() as session:
        assert (await module().refresh_participant(session, request, action='fence'))['status'] == 'fenced'
    # Even phase-only transitions invalidate a physical observation. Never release
    # a rollout guard on the strength of a sample taken before reopening.
    with pytest.raises(PoolObservationError):
        await publish(sessions, await principal(), before_capture)
    await publish_placement(sessions, await principal(), placement)
    with pytest.raises(PoolAdmissionError):
        await preparing(sessions, await principal(request.participant_id), selected_body, before_profiles)
    while_fenced_body = workload(neighbor)
    neighbor_while_fenced = await preparing(sessions, await principal(neighbor.participant_id), while_fenced_body, before_profiles)
    assert neighbor_while_fenced.phase == 'reserved'
    # Keep the real two-builder limit: release this test's unstarted second grant
    # through its normal owner, while retaining the original across refresh.
    assert (await operate(sessions, await principal(neighbor.participant_id), action(while_fenced_body),
        operation='cancel')).phase == 'cancelled_unstarted'

    async with sessions.begin() as session:
        assert (await module().refresh_participant(session, request, action='stage'))['status'] == 'staged'
    activated = await operate(sessions, await principal(neighbor.participant_id), action(retained_body, activation=True),
        profiles=after_profiles)
    assert activated.reservation_id == retained.reservation_id and activated.phase == 'create_intent'
    assert activated.capacity_charged
    retained_rows = (await snapshot(sessions))['nebius_pool_requests']
    # A replay uses the retained plan, not a new render or another create intent.
    assert await operate(sessions, await principal(neighbor.participant_id), action(retained_body, activation=True),
        profiles=after_profiles) == activated
    assert (await snapshot(sessions))['nebius_pool_requests'] == retained_rows
    new_neighbor_body = workload(neighbor)
    with pytest.raises(PoolAdmissionError):
        await preparing(sessions, await principal(neighbor.participant_id), new_neighbor_body, after_profiles)
    await publish_placement(sessions, await principal(), placement)
    assert (await preparing(sessions, await principal(neighbor.participant_id), new_neighbor_body, after_profiles)).phase == 'reserved'
    assert (await operate(sessions, await principal(neighbor.participant_id), action(new_neighbor_body),
        operation='cancel')).phase == 'cancelled_unstarted'

    async with sessions.begin() as session:
        assert (await module().refresh_participant(session, request, action='open'))['status'] == 'complete'
    successor = workload(request.after.participants[0], successor=True)
    with pytest.raises(PoolAdmissionError):
        await preparing(sessions, await principal(request.participant_id), successor, after_profiles)
    await publish_placement(sessions, await principal(), placement)
    # Old participant revision/catalog is no longer new-admission authority.
    with pytest.raises(PoolAdmissionError):
        await preparing(sessions, await principal(request.participant_id), selected_body, before_profiles)
    final = await preparing(sessions, await principal(request.participant_id), successor, after_profiles)
    assert final.phase == 'reserved'
    await operate(sessions, await principal(request.participant_id), action(successor, activation=True), profiles=after_profiles)
    async with sessions() as session:
        row = await session.get(NebiusPoolRequest, final.reservation_id)
        pod = row.plan_json['job']['spec']['template']['spec']
        containers = [*pod.get('initContainers', []), *pod['containers']]
        expected_image = ('registry.example/runtime@sha256:' + 'd' * 64 if kind == 'execution'
            else 'registry.example/service@sha256:' + 'f' * 64)
        assert expected_image in {container['image'] for container in containers}
