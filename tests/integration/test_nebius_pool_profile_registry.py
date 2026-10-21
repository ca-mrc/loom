"""The running manager resolves immutable participant profiles transactionally."""
from __future__ import annotations

import copy
import importlib
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import func, select, update
from sqlalchemy.exc import DBAPIError

from loom.db.nebius_pool_schema import NebiusPoolBinding, NebiusPoolParticipant, NebiusPoolRequest
from loom.nebius_pool_contract import PoolActivationV1
from loom_execution_capacity_collector.contracts import CapacityPlacement
from loom_service.app import create_app
from loom_service.config import LoomServiceSettings
from loom_service.pool_management.auth import resolve_pool_machine
from loom_service.pool_management.capacity import digest
from loom_service.pool_management.installation import PoolInstallation, register_installation
from loom_service.pool_management.profiles import PoolProfileCatalog
from tests.execution_placement_fixtures import placement_fixture
from tests.integration.test_nebius_pool_installation import installation
from tests.integration.test_nebius_pool_observation_continuity import workload
from tests.integration.test_nebius_pool_participant_http import action
from tests.integration.test_nebius_pool_registry import publish_placement
from tests.integration.test_nebius_pool_registry import sessions as sessions


def registry():
    name = 'loom_service.pool_management.profile_registry'
    assert importlib.util.find_spec(name) is not None, 'independent participant profile registry is missing'
    return importlib.import_module(name)


def catalog(spec, participant):
    wanted = {target.profile_id for target in participant.targets}
    return spec.profiles.model_copy(update={
        'execution': tuple(row for row in spec.profiles.execution if row.profile_id in wanted),
        'task_images': tuple(row for row in spec.profiles.task_images if row.profile_id in wanted),
        'application_images': tuple(row for row in spec.profiles.application_images if row.profile_id in wanted),
    })


async def setup(sessions, *, occupied=False):
    module = registry()
    config, tokens = installation()
    config['node_selector'] = copy.deepcopy(config['profiles']['execution'][0]['runtime']['node_selector'])
    spec = PoolInstallation.model_validate(config)
    async with sessions.begin() as session:
        await register_installation(session, spec)
        pool = await session.get(NebiusPoolBinding, spec.pool_id)
        binding = dict(pool.binding_json, observation_binding='physical-v1', profile_source='registry-v1')
        await session.execute(update(NebiusPoolBinding).where(NebiusPoolBinding.pool_id == spec.pool_id)
            .values(mode='global', policy_revision=pool.policy_revision + 1,
                binding_json=binding, binding_sha256=digest(binding)))
        for participant in spec.participants:
            await module.store_participant_catalog(session, participant, catalog(spec, participant))
    app = create_app(LoomServiceSettings(_env_file=None, service_mode='management',
        db_url='postgresql+asyncpg://unused:unused@localhost/unused'))
    app.state.session_factory = sessions

    def headers(participant):
        machine, = (row for row in spec.machines if row.participant_id == participant.participant_id)
        return {'Authorization': 'Bearer ' + tokens[machine.machine_id]}

    async def observe(*, full=False):
        machine, = (row for row in spec.machines if row.role == 'observer')
        async with sessions() as session:
            observer = await resolve_pool_machine(session, 'Bearer ' + tokens[machine.machine_id])
        placement = placement_fixture(target_id='group-1', parent_id='parent', quota_nodes=1,
            node_cpu=32000, node_memory=65536, node_storage=262144, requested_cpu=32000 if full else 0)
        del placement['quota_resources']['memory']
        await publish_placement(sessions, observer, CapacityPlacement.model_validate(placement))

    await observe(full=occupied)
    return spec, app, headers, observe


@pytest.mark.parametrize('index', [0, 1, 2])
async def test_real_http_admission_uses_participant_registry_without_startup_catalog(sessions, index):
    spec, app, headers, _ = await setup(sessions)
    participant = spec.participants[index]
    request = workload(participant)
    assert not hasattr(app.state, 'pool_profiles')
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='https://management.example') as http:
        response = await http.post(f'/internal/pools/v1/{spec.pool_id}/prepare',
            json=request.model_dump(mode='json'), headers=headers(participant))
        assert response.status_code == 200, response.text
        assert response.json()['phase'] == 'reserved'
        activation = PoolActivationV1(action=action(request), not_after=request.deadline_at)
        response = await http.post(f'/internal/pools/v1/{spec.pool_id}/activate',
            json=activation.model_dump(mode='json'), headers=headers(participant))
        assert response.status_code == 200, response.text
        assert response.json()['phase'] == 'create_intent'


async def replace_release(sessions, spec, selected):
    from loom_service.pool_management.locks import acquire_pool_mutation_lock

    old = catalog(spec, selected)
    replacements = {target.profile_id: uuid4() for target in selected.targets}
    following = selected.model_copy(update={'binding_revision': selected.binding_revision + 1,
        'targets': tuple(target.model_copy(update={'profile_id': replacements[target.profile_id]})
            for target in selected.targets)})
    profiles = old.model_copy(update={
        'execution': tuple(row.model_copy(update={'profile_id': replacements[row.profile_id], 'candidate_sha': '2' * 40})
            for row in old.execution),
        'task_images': tuple(row.model_copy(update={'profile_id': replacements[row.profile_id]}) for row in old.task_images),
    })
    async with sessions.begin() as session:
        await acquire_pool_mutation_lock(session)
        payload = following.model_dump(mode='json')
        await session.execute(update(NebiusPoolParticipant).where(
            NebiusPoolParticipant.participant_id == selected.participant_id).values(
                binding_revision=following.binding_revision, binding_json=payload, binding_sha256=digest(payload)))
        await registry().store_participant_catalog(session, following, profiles)
    return following


async def test_new_release_is_visible_to_running_manager_without_peer_catalog_replacement(sessions):
    spec, app, headers, _ = await setup(sessions)
    selected = spec.participants[2]
    following = await replace_release(sessions, spec, selected)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='https://management.example') as http:
        for participant in (spec.participants[0], spec.participants[1], following):
            body = workload(participant).model_dump(mode='json')
            if participant == following:
                body['execution']['runtime']['candidate_sha'] = '2' * 40
            response = await http.post(f'/internal/pools/v1/{spec.pool_id}/prepare', json=body,
                headers=headers(participant))
            assert response.status_code == 200, response.text
            assert response.json()['phase'] == 'reserved'


async def test_retained_waiter_resumes_original_profile_but_new_stale_request_is_refused(sessions):
    spec, app, headers, observe = await setup(sessions, occupied=True)
    selected = spec.participants[2]
    request = workload(selected)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='https://management.example') as http:
        async def prepare(body):
            return await http.post(f'/internal/pools/v1/{spec.pool_id}/prepare',
                json=body.model_dump(mode='json'), headers=headers(selected))

        response = await prepare(request)
        assert response.status_code == 200 and response.json()['phase'] == 'waiting'
        await replace_release(sessions, spec, selected)
        await observe()
        response = await prepare(request)
        assert response.status_code == 200, response.text
        assert response.json()['phase'] == 'reserved'
        stale = await prepare(workload(selected))
        assert stale.status_code == 409
        response = await http.post(f'/internal/pools/v1/{spec.pool_id}/activate',
            json=PoolActivationV1(action=action(request), not_after=request.deadline_at).model_dump(mode='json'),
            headers=headers(selected))
        assert response.status_code == 200, response.text
        assert response.json()['phase'] == 'create_intent'
        reservation_id = response.json()['reservation_id']
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(NebiusPoolRequest)) == 1
    from uuid import UUID

    from loom_service.pool_management.gateway_journal import PoolGatewayJournal
    from tests.integration.test_nebius_pool_registry import machine

    gateway = await machine(sessions, spec.pool_id, role='gateway')
    journal = PoolGatewayJournal(sessions)
    effect = await journal.prepare_create(gateway, UUID(reservation_id), kind='Job')
    assert (await journal.dispatch_create(gateway, effect.effect_id)).phase == 'dispatched'


async def test_catalog_replay_is_exact_and_sql_evidence_cannot_be_rewritten(sessions):
    spec, _, _, _ = await setup(sessions)
    selected = spec.participants[0]
    async with sessions.begin() as session:
        first = await registry().store_participant_catalog(session, selected, catalog(spec, selected))
        assert await registry().store_participant_catalog(session, selected, catalog(spec, selected)) == first
    from loom.db.nebius_pool_catalog_schema import NebiusPoolParticipantCatalog

    async with sessions.begin() as session:
        with pytest.raises(DBAPIError):
            async with session.begin_nested():
                await session.execute(update(NebiusPoolParticipantCatalog).values(document_json={}))


@pytest.mark.parametrize('damage', ['foreign-namespace', 'foreign-selector', 'unbound-profile', 'wrong-participant'])
async def test_catalog_cannot_widen_the_registered_participant(sessions, damage):
    spec, _, _, _ = await setup(sessions)
    selected = spec.participants[0]
    document = catalog(spec, selected).model_dump(mode='json')
    if damage == 'foreign-namespace':
        document['execution'][0]['runtime']['namespace'] = 'foreign'
    elif damage == 'foreign-selector':
        document['execution'][0]['runtime']['node_selector']['nebius.com/node-group-id'] = 'foreign'
    elif damage == 'unbound-profile':
        document['execution'][0]['profile_id'] = str(uuid4())
    else:
        selected = spec.participants[1]
    async with sessions.begin() as session:
        with pytest.raises(ValueError):
            await registry().store_participant_catalog(session, selected, PoolProfileCatalog.model_validate(document))


async def test_profile_identity_cannot_be_reused_for_different_bytes(sessions):
    spec, _, _, _ = await setup(sessions)
    selected = spec.participants[0]
    changed = catalog(spec, selected).model_dump(mode='json')
    changed['execution'][0]['candidate_sha'] = '2' * 40
    async with sessions.begin() as session:
        with pytest.raises(ValueError, match='pool_profile_identity_conflict'):
            await registry().store_participant_catalog(session, selected, PoolProfileCatalog.model_validate(changed))


async def test_resolver_does_not_flush_or_use_pending_catalog_edits(sessions):
    from loom.db.nebius_pool_catalog_schema import NebiusPoolParticipantCatalog

    spec, _, _, _ = await setup(sessions)
    selected = spec.participants[0]
    async with sessions() as session:
        pool = await session.get(NebiusPoolBinding, spec.pool_id)
        row = await session.get(NebiusPoolParticipantCatalog, (selected.participant_id, selected.binding_revision))
        row.document_json = {'pending': True}
        with pytest.raises(ValueError, match='pool_profile_registry_requires_clean_transaction'):
            await registry().resolve_profiles(session, pool, selected, selected.binding_revision,
                retained=False, fallback=None)
        assert row in session.dirty and row.document_json == {'pending': True}
