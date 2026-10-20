"""Independent lifecycle uses real SQL authority and real runtime HTTP consumers."""
from __future__ import annotations

import copy
import importlib
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import func, select, update

from loom.db.nebius_pool_schema import (
    NebiusPoolBinding,
    NebiusPoolMachine,
    NebiusPoolParticipant,
    NebiusPoolRequest,
)
from loom.nebius_pool_contract import PoolActivationV1
from loom_execution_capacity_collector.contracts import CapacityPlacement
from loom_service.app import create_app
from loom_service.config import LoomServiceSettings
from loom_service.pool_management.auth import resolve_pool_machine
from loom_service.pool_management.capacity import digest
from loom_service.pool_management.installation import PoolInstallation
from loom_service.pool_management.observation_binding import physical_binding_digest
from tests.execution_placement_fixtures import placement_fixture
from tests.integration.test_nebius_pool_installation import add_application_builder, installation
from tests.integration.test_nebius_pool_observation_continuity import workload
from tests.integration.test_nebius_pool_participant_http import action
from tests.integration.test_nebius_pool_profile_registry import catalog
from tests.integration.test_nebius_pool_registry import publish_placement
from tests.integration.test_nebius_pool_registry import sessions as sessions


def lifecycle():
    name = 'loom_service.pool_management.participant_lifecycle'
    assert importlib.util.find_spec(name), 'protected independent participant lifecycle is missing'
    return importlib.import_module(name)


async def platform(sessions, *, application_recipe=None):
    module = lifecycle()
    config, tokens = installation(('production', 'staging', 'development', 'staging'))
    if application_recipe is not None:
        config, machine_id, token = add_application_builder(config, application_recipe)
        tokens[machine_id] = token
        config['profiles']['application_images'][0]['settings'].update(
            registry_repository='cr.eu-north1.nebius.cloud/registryid/personal-apps',
            storage_endpoint='https://storage.eu-north1.nebius.cloud',
            registry_auth_kind='nebius', cache_bucket=None, cache_secret_name=None)
    config['node_selector'] = copy.deepcopy(config['profiles']['execution'][0]['runtime']['node_selector'])
    spec = PoolInstallation.model_validate(config)
    body = spec.model_dump(mode='json', exclude={'participants', 'profiles'})
    body['schema_version'] = 'loom.pool-platform-installation.v2'
    body['machines'] = [row for row in body['machines'] if row['role'] != 'participant']
    platform_spec = module.PoolPlatformInstallationV2.model_validate(body)
    async with sessions.begin() as session:
        receipt = await module.register_platform(session, platform_spec)
        pool = await session.get(NebiusPoolBinding, spec.pool_id)
        authority = physical_binding_digest(pool)
    return module, spec, platform_spec, tokens, receipt, authority


async def setup(sessions, *, active=True, application_recipe=None):
    module, spec, _, tokens, _, authority = await platform(sessions, application_recipe=application_recipe)
    heads = {}
    for participant in spec.participants:
        request = module.PoolParticipantEnrollmentV2(
            schema_version='loom.pool-participant-enrollment.v2', operation_id=uuid4(),
            pool_authority_sha256=authority, participant=participant,
            foundation_sha256='1' * 64, profiles=catalog(spec, participant),
            machines=tuple(row for row in spec.machines if row.participant_id == participant.participant_id))
        async with sessions.begin() as session:
            heads[participant.participant_id] = await module.enroll_participant(session, request)
    # Sole-writer qualification is an external protected operation, not enrollment.
    # This fixture opens only its disposable DB after explicit installation.
    async with sessions.begin() as session:
        await session.execute(update(NebiusPoolBinding).where(NebiusPoolBinding.pool_id == spec.pool_id).values(mode='global'))
    app = create_app(LoomServiceSettings(_env_file=None, service_mode='management',
        db_url='postgresql+asyncpg://unused:unused@localhost/unused'))
    app.state.session_factory = sessions

    def headers(participant, scope='environment'):
        machine, = (row for row in spec.machines if row.participant_id == participant.participant_id and row.workload_scope == scope)
        return {'Authorization': 'Bearer ' + tokens[machine.machine_id]}

    async def observe(*, full=False):
        observer, = (row for row in spec.machines if row.role == 'observer')
        async with sessions() as session:
            principal = await resolve_pool_machine(session, 'Bearer ' + tokens[observer.machine_id])
        value = placement_fixture(target_id='group-1', parent_id='parent', quota_nodes=1,
            node_cpu=32000, node_memory=65536, node_storage=262144, requested_cpu=32000 if full else 0)
        del value['quota_resources']['memory']
        await publish_placement(sessions, principal, CapacityPlacement.model_validate(value))

    async def change(participant, verb, *, apply=True, **changes):
        async with sessions() as session:
            row = await session.get(NebiusPoolParticipant, participant.participant_id)
            binding_sha = row.binding_sha256
        document = dict(schema_version='loom.pool-participant-operation.v2', operation_id=uuid4(),
            participant_id=participant.participant_id, pool_id=spec.pool_id, incarnation=participant.incarnation,
            expected_head=heads[participant.participant_id]['operation_id'], expected_binding_sha256=binding_sha,
            pool_authority_sha256=authority, action=verb)
        if verb in {'ready', 'activate', 'resume'}:
            document['readiness'] = dict(observed_at=datetime.now(UTC), valid_until=datetime.now(UTC) + timedelta(seconds=60),
                participant_binding_sha256=binding_sha, pool_authority_sha256=authority,
                catalog_sha256=digest({'participant': participant.model_dump(mode='json'),
                    'profiles': catalog(spec, participant).model_dump(mode='json')}),
                gateway_machine_id=next(row.machine_id for row in spec.machines if row.role == 'gateway'),
                observer_machine_id=next(row.machine_id for row in spec.machines if row.role == 'observer'),
                gateway_credential_epoch=1, observer_credential_epoch=1,
                capabilities=('physical-v1', 'registry-v1', 'scoped-lifecycle-v1'))
        document.update(changes)
        request = module.PoolParticipantOperationV2.model_validate(document)
        if not apply:
            return request
        async with sessions.begin() as session:
            result = await module.transition_participant(session, request)
        heads[participant.participant_id] = result
        return result

    await observe()
    if active:
        for participant in spec.participants:
            await change(participant, 'ready')
            await change(participant, 'activate')
    return module, spec, app, headers, observe, change


async def test_empty_platform_installs_without_dummy_environments_and_replays(sessions):
    module, spec, platform_spec, _, receipt, _ = await platform(sessions)
    async with sessions.begin() as session:
        assert await module.register_platform(session, platform_spec) == receipt
        assert await session.scalar(select(func.count()).select_from(NebiusPoolParticipant)) == 0
        assert await session.scalar(select(func.count()).select_from(NebiusPoolMachine)) == 2
        assert (await session.get(NebiusPoolBinding, spec.pool_id)).mode == 'closed'


async def test_explicit_identity_enrollment_supports_multiple_same_class_peers(sessions):
    _, spec, app, headers, _, _ = await setup(sessions)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='https://manager.example') as http:
        for participant in spec.participants:
            response = await http.post(f'/internal/pools/v1/{spec.pool_id}/prepare',
                json=workload(participant).model_dump(mode='json'), headers=headers(participant))
            assert response.status_code == 200, response.text
            assert response.json()['phase'] == 'reserved'
        response = await http.post(f'/internal/pools/v1/{spec.pool_id}/prepare',
            json=workload(spec.participants[0]).model_dump(mode='json'), headers=headers(spec.participants[2]))
        assert response.status_code == 403


async def test_suspension_blocks_new_intake_but_reserved_work_can_activate_and_peers_continue(sessions):
    _, spec, app, headers, _, change = await setup(sessions)
    selected = spec.participants[2]
    request = workload(selected)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='https://manager.example') as http:
        path = f'/internal/pools/v1/{spec.pool_id}'
        response = await http.post(path + '/prepare', json=request.model_dump(mode='json'), headers=headers(selected))
        assert response.status_code == 200 and response.json()['phase'] == 'reserved'
        assert (await change(selected, 'suspend'))['state'] == 'suspended'
        response = await http.post(path + '/prepare', json=workload(selected).model_dump(mode='json'), headers=headers(selected))
        assert response.status_code == 409
        response = await http.post(path + '/activate', json=PoolActivationV1(action=action(request),
            not_after=request.deadline_at).model_dump(mode='json'), headers=headers(selected))
        assert response.status_code == 200, response.text
        assert response.json()['phase'] == 'create_intent'
        reservation_id = response.json()['reservation_id']
        response = await http.post(path + '/prepare', json=workload(spec.participants[1]).model_dump(mode='json'),
            headers=headers(spec.participants[1]))
        assert response.status_code == 200 and response.json()['phase'] == 'reserved'
    async with sessions() as session:
        assert (await session.scalar(select(NebiusPoolRequest).where(
            NebiusPoolRequest.participant_id == selected.participant_id))).phase == 'create_intent'
    from uuid import UUID

    from loom_service.pool_management.gateway_journal import PoolGatewayJournal
    from tests.integration.test_nebius_pool_registry import machine

    gateway = await machine(sessions, spec.pool_id, role='gateway')
    journal = PoolGatewayJournal(sessions)
    effect = await journal.prepare_create(gateway, UUID(reservation_id), kind='Job')
    assert (await journal.dispatch_create(gateway, effect.effect_id)).phase == 'dispatched'


async def test_enrolled_participant_cannot_admit_before_ready_and_activation(sessions):
    _, spec, app, headers, _, _ = await setup(sessions, active=False)
    selected = spec.participants[2]
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='https://manager.example') as http:
        response = await http.post(f'/internal/pools/v1/{spec.pool_id}/prepare',
            json=workload(selected).model_dump(mode='json'), headers=headers(selected))
        assert response.status_code == 409


async def test_transition_replay_cannot_resuspend_after_resume_and_stale_cas_is_refused(sessions):
    module, spec, _, _, _, change = await setup(sessions)
    selected = spec.participants[2]
    suspend = await change(selected, 'suspend', apply=False)
    async with sessions.begin() as session:
        retained = await module.transition_participant(session, suspend)
    resumed = await change(selected, 'resume', expected_head=suspend.operation_id)
    assert resumed['state'] == 'active'
    async with sessions.begin() as session:
        assert await module.transition_participant(session, suspend) == retained
    with pytest.raises(ValueError, match='predecessor'):
        await change(selected, 'suspend', expected_head=suspend.operation_id)


async def test_retirement_requires_exact_waiter_disposition_and_preserves_tombstone(sessions):
    _, spec, app, headers, observe, change = await setup(sessions)
    selected = spec.participants[2]
    await observe(full=True)
    request = workload(selected)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='https://manager.example') as http:
        response = await http.post(f'/internal/pools/v1/{spec.pool_id}/prepare',
            json=request.model_dump(mode='json'), headers=headers(selected))
        assert response.status_code == 200 and response.json()['phase'] == 'waiting'
    await change(selected, 'suspend')
    with pytest.raises(ValueError, match='waiting'):
        await change(selected, 'retire')
    async with sessions() as session:
        row = await session.scalar(select(NebiusPoolRequest).where(NebiusPoolRequest.participant_id == selected.participant_id))
        cancellation = {'request_id': row.request_id, 'request_sha256': row.request_sha256}
    assert (await change(selected, 'retire', cancel_waiting=[cancellation]))['state'] == 'retiring'
    assert (await change(selected, 'complete_retirement'))['state'] == 'retired'
    async with sessions() as session:
        assert (await session.get(NebiusPoolRequest, cancellation['request_id'])).phase == 'cancelled_unstarted'
        assert (await session.get(NebiusPoolParticipant, selected.participant_id)).phase == 'fenced'
        assert (await session.scalar(select(NebiusPoolMachine).where(
            NebiusPoolMachine.participant_id == selected.participant_id))).phase == 'revoked'
    with pytest.raises(ValueError):
        await change(selected, 'resume')


async def test_retirement_does_not_release_charged_or_unknown_outcome_work(sessions):
    _, spec, app, headers, _, change = await setup(sessions)
    selected = spec.participants[2]
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='https://manager.example') as http:
        response = await http.post(f'/internal/pools/v1/{spec.pool_id}/prepare',
            json=workload(selected).model_dump(mode='json'), headers=headers(selected))
        assert response.status_code == 200 and response.json()['phase'] == 'reserved'
    await change(selected, 'suspend')
    await change(selected, 'retire')
    with pytest.raises(ValueError, match='outstanding'):
        await change(selected, 'complete_retirement')
    async with sessions() as session:
        assert (await session.scalar(select(NebiusPoolRequest).where(
            NebiusPoolRequest.participant_id == selected.participant_id))).phase == 'reserved'


@pytest.mark.parametrize('damage', ['expired', 'binding', 'gateway', 'capability'])
async def test_activation_rejects_unqualified_readiness_before_opening(sessions, damage):
    _, spec, _, _, _, change = await setup(sessions, active=False)
    selected = spec.participants[2]
    await change(selected, 'ready')
    request = await change(selected, 'activate', apply=False)
    evidence = request.readiness.model_dump()
    if damage == 'expired':
        evidence.update(observed_at=datetime.now(UTC) - timedelta(minutes=2), valid_until=datetime.now(UTC) - timedelta(seconds=1))
    elif damage == 'binding':
        evidence['participant_binding_sha256'] = 'f' * 64
    elif damage == 'gateway':
        evidence['gateway_machine_id'] = uuid4()
    else:
        evidence['capabilities'] = ('physical-v1',)
    with pytest.raises(ValueError):
        await change(selected, 'activate', readiness=evidence)
    async with sessions() as session:
        assert (await session.get(NebiusPoolParticipant, selected.participant_id)).phase == 'fenced'


async def test_pending_operation_refuses_changed_policy_without_mutating_selected_participant(sessions):
    module, spec, _, _, _, change = await setup(sessions)
    selected = spec.participants[2]
    request = await change(selected, 'suspend', apply=False)
    async with sessions.begin() as session:
        await session.execute(update(NebiusPoolBinding).where(NebiusPoolBinding.pool_id == spec.pool_id)
            .values(policy_revision=2))
    with pytest.raises(ValueError, match='authority'):
        async with sessions.begin() as session:
            await module.transition_participant(session, request)


async def test_retiring_never_activated_participant_does_not_grant_execution_authority(sessions):
    _, spec, _, _, _, change = await setup(sessions, active=False)
    selected = spec.participants[2]
    await change(selected, 'retire')
    async with sessions() as session:
        assert (await session.get(NebiusPoolParticipant, selected.participant_id)).phase == 'fenced'


async def test_suspended_high_priority_waiter_does_not_hold_peer_capacity_and_can_resume(sessions):
    _, spec, app, headers, observe, change = await setup(sessions)
    selected, peer = spec.participants[0], spec.participants[2]
    request = workload(selected)
    await observe(full=True)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='https://manager.example') as http:
        path = f'/internal/pools/v1/{spec.pool_id}/prepare'
        first = await http.post(path, json=request.model_dump(mode='json'), headers=headers(selected))
        assert first.status_code == 200 and first.json()['phase'] == 'waiting'
        await change(selected, 'suspend')
        await observe()
        held = await http.post(path, json=request.model_dump(mode='json'), headers=headers(selected))
        assert held.status_code == 409
        lower = await http.post(path, json=workload(peer).model_dump(mode='json'), headers=headers(peer))
        assert lower.status_code == 200 and lower.json()['phase'] == 'reserved'
        await change(selected, 'resume')
        resumed = await http.post(path, json=request.model_dump(mode='json'), headers=headers(selected))
        assert resumed.status_code == 200 and resumed.json()['phase'] == 'reserved'


async def test_pending_intake_state_cannot_be_erased_or_flushed_by_runtime_admission(sessions):
    from loom.db.nebius_pool_lifecycle_schema import NebiusPoolParticipantLifecycle
    from loom_service.pool_management.registry import prepare_execution

    _, spec, _, headers, _, _ = await setup(sessions)
    selected = spec.participants[2]
    async with sessions() as session:
        principal = await resolve_pool_machine(session, headers(selected)['Authorization'])
        head = await session.get(NebiusPoolParticipantLifecycle, selected.participant_id)
        head.state = 'suspended'
        with pytest.raises(ValueError):
            await prepare_execution(session, principal, workload(selected))
        assert head in session.dirty and head.state == 'suspended'


async def test_same_participant_operations_use_cas_but_peer_operations_do_not_conflict(sessions):
    import asyncio

    module, spec, _, _, _, change = await setup(sessions)
    first = await change(spec.participants[2], 'suspend', apply=False)
    concurrent = first.model_copy(update={'operation_id': uuid4()})
    peer = await change(spec.participants[1], 'suspend', apply=False)

    async def run(request):
        async with sessions.begin() as session:
            return await module.transition_participant(session, request)

    results = await asyncio.wait_for(asyncio.gather(run(first), run(concurrent), run(peer), return_exceptions=True), 10)
    assert sum(isinstance(row, dict) for row in results) == 2
    assert isinstance(results[2], dict) and results[2]['state'] == 'suspended'
    assert sum(isinstance(row, ValueError) and 'predecessor' in str(row) for row in results) == 1


async def test_scoped_suspension_and_connected_pool_admission_share_one_quota_lock(sessions):
    import asyncio

    from loom_service.pool_management.control import cancel_unstarted_pool_request
    from tests.integration.test_nebius_pool_registry import machine, prepare
    from tests.integration.test_nebius_pool_registry import setup as connected_setup

    _, spec, _, headers, _, change = await setup(sessions)
    selected, peer = spec.participants[2], spec.participants[1]
    other = await connected_setup(sessions, occupied_cpu=3000, max_nodes=2, quota_nodes=3,
        parent_id='parent', group_id='connected-pool', memory_quota=False)
    value = placement_fixture(target_id='group-1', parent_id='parent', quota_nodes=3,
        node_cpu=3000, node_memory=8192, node_storage=32768, requested_cpu=3000)
    del value['quota_resources']['memory']
    await publish_placement(sessions, await machine(sessions, spec.pool_id), CapacityPlacement.model_validate(value))
    async with sessions() as session:
        principal = await resolve_pool_machine(session, headers(peer)['Authorization'])
        before = (await session.get(NebiusPoolBinding, spec.pool_id)).binding_sha256
    requests = (workload(peer), other[2][0])
    principals = (principal, other[1][0])
    profiles = ({}, other[3])
    changed, *receipts = await asyncio.wait_for(asyncio.gather(
        change(selected, 'suspend'),
        *(prepare(sessions, owner, request, catalog) for owner, request, catalog
          in zip(principals, requests, profiles, strict=True)),
    ), timeout=15)
    assert changed['state'] == 'suspended'
    # Two occupied physical pools share room for exactly one additional node.
    # A scoped transition cannot invalidate peer evidence or double-spend it.
    assert sorted(receipt.phase for receipt in receipts) == ['reserved', 'waiting']
    async with sessions() as session:
        assert (await session.get(NebiusPoolBinding, spec.pool_id)).binding_sha256 == before
        charged = (await session.scalars(select(NebiusPoolRequest).where(NebiusPoolRequest.phase == 'reserved'))).all()
        assert len(charged) == 1 and charged[0].cpu_millis == 1500 and charged[0].plan_json is None
    winner = next(index for index, receipt in enumerate(receipts) if receipt.phase == 'reserved')
    async with sessions.begin() as session:
        assert (await cancel_unstarted_pool_request(session, principals[winner], action(requests[winner]))).phase == 'cancelled_unstarted'
    waiting = 1 - winner
    assert (await prepare(sessions, principals[waiting], requests[waiting], profiles[waiting])).phase == 'reserved'


async def test_retired_environment_can_reenroll_with_new_incarnation_without_resurrecting_credentials(sessions):
    module, spec, _, headers, observe, change = await setup(sessions)
    selected = spec.participants[2]
    await change(selected, 'suspend')
    await change(selected, 'retire')
    await change(selected, 'complete_retirement')
    replacement = selected.model_copy(update={'participant_id': uuid4(), 'incarnation': uuid4(),
        'execution_namespace': selected.execution_namespace.model_copy(update={'uid': uuid4()}),
        'build_namespace': selected.build_namespace.model_copy(update={'uid': uuid4()})})
    # Profiles preserve the unchanged names, but authority binds new namespace UIDs.
    machine, = (row for row in spec.machines if row.participant_id == selected.participant_id)
    new_machine = machine.model_copy(update={'participant_id': replacement.participant_id,
        'machine_id': uuid4(), 'token_sha256': uuid4().hex + uuid4().hex})
    async with sessions() as session:
        pool = await session.get(NebiusPoolBinding, spec.pool_id)
        authority = physical_binding_digest(pool)
    enrollment = module.PoolParticipantEnrollmentV2(schema_version='loom.pool-participant-enrollment.v2',
        operation_id=uuid4(), pool_authority_sha256=authority, foundation_sha256='2' * 64,
        participant=replacement, machines=(new_machine,), profiles=catalog(spec, selected))
    async with sessions.begin() as session:
        assert (await module.enroll_participant(session, enrollment))['state'] == 'enrolled'
    await observe()  # Retired names/UIDs cannot poison the new collector scope.
    async with sessions() as session:
        assert await resolve_pool_machine(session, headers(selected)['Authorization']) is None
        assert await session.get(NebiusPoolParticipant, selected.participant_id) is not None
        assert (await session.get(NebiusPoolParticipant, replacement.participant_id)).phase == 'fenced'


@pytest.mark.parametrize('statement', [
    "UPDATE nebius_pool_lifecycle_operations SET receipt_json='{}'::jsonb",
    'DELETE FROM nebius_pool_lifecycle_operations',
    'TRUNCATE nebius_pool_lifecycle_operations CASCADE',
    "UPDATE nebius_pool_participant_lifecycle SET state='retired'",
    'DELETE FROM nebius_pool_participant_lifecycle',
    'TRUNCATE nebius_pool_participant_lifecycle',
])
async def test_operation_and_head_history_cannot_be_rewritten_or_erased(sessions, statement):
    from sqlalchemy import text
    from sqlalchemy.exc import DBAPIError

    await setup(sessions)
    with pytest.raises(DBAPIError):
        async with sessions.begin() as session:
            await session.execute(text(statement))
