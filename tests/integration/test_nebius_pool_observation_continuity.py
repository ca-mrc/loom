"""Independent participants do not invalidate unchanged physical pool evidence."""
from __future__ import annotations

import copy
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from sqlalchemy import update

from loom.db.nebius_pool_schema import NebiusPoolBinding, NebiusPoolParticipant, NebiusPoolRequest
from loom.nebius_pool_workload import PoolExecutionPrepareV1
from loom_execution_capacity_collector.contracts import CapacityPlacement
from loom_service.pool_management.auth import resolve_pool_machine
from loom_service.pool_management.capacity import digest, read_connected_capacity
from loom_service.pool_management.installation import PoolInstallation, register_installation
from tests.execution_placement_fixtures import placement_fixture
from tests.integration.test_nebius_pool_installation import installation
from tests.integration.test_nebius_pool_observation_registry import capture_scope, publish
from tests.integration.test_nebius_pool_registry import prepare, publish_placement
from tests.integration.test_nebius_pool_registry import sessions as sessions
from tests.unit.test_nebius_pool_execution_render import inputs


async def setup(sessions, *, physical=True):
    config, tokens = installation()
    config['node_selector'] = copy.deepcopy(config['profiles']['execution'][0]['runtime']['node_selector'])
    spec = PoolInstallation.model_validate(config)
    async with sessions.begin() as session:
        await register_installation(session, spec)
        pool = await session.get(NebiusPoolBinding, spec.pool_id)
        binding = dict(pool.binding_json)
        if physical:
            binding['observation_binding'] = 'physical-v1'
        await session.execute(update(NebiusPoolBinding).where(NebiusPoolBinding.pool_id == spec.pool_id)
            .values(mode='global', policy_revision=pool.policy_revision + int(physical),
                binding_json=binding, binding_sha256=digest(binding)))

    async def principal(participant=None):
        machine, = (row for row in spec.machines if row.role == ('observer' if participant is None else 'participant')
            and row.participant_id == participant)
        async with sessions() as session:
            result = await resolve_pool_machine(session, 'Bearer ' + tokens[machine.machine_id])
        assert result is not None
        return result

    placement = placement_fixture(target_id='group-1', parent_id='parent', quota_nodes=1,
        node_cpu=32000, node_memory=65536, node_storage=262144)
    del placement['quota_resources']['memory']
    await publish_placement(sessions, await principal(), CapacityPlacement.model_validate(placement))
    return spec, principal


async def change_participant(sessions, selected, mutation):
    async with sessions.begin() as session:
        if mutation == 'suspend':
            await session.execute(update(NebiusPoolParticipant).where(
                NebiusPoolParticipant.participant_id == selected.participant_id).values(phase='fenced'))
        else:
            following = selected.model_copy(update={'binding_revision': selected.binding_revision + 1,
                'targets': tuple(target.model_copy(update={'profile_id': uuid4()}) for target in selected.targets)})
            payload = following.model_dump(mode='json')
            await session.execute(update(NebiusPoolParticipant).where(
                NebiusPoolParticipant.participant_id == selected.participant_id).values(
                    binding_revision=following.binding_revision, binding_json=payload, binding_sha256=digest(payload)))


def workload(participant):
    _, body = inputs()
    body.update(pool_id=participant.pool_id, admission_epoch=participant.admission_epoch,
        participant_revision=participant.binding_revision)
    body['key']['participant_id'] = participant.participant_id
    body['origin']['data_environment_id'] = participant.environment_id
    return PoolExecutionPrepareV1.model_validate(body)


@pytest.mark.parametrize('mutation', ['suspend', 'release'])
async def test_peer_admission_survives_dev_change_without_new_physical_sample(sessions, mutation):
    spec, principal = await setup(sessions)
    production, staging, development = spec.participants
    profiles = spec.profiles.profiles()
    retained = await prepare(sessions, await principal(production.participant_id), workload(production), profiles)
    assert retained.phase == 'reserved'
    async with sessions.begin() as session:
        before = (await read_connected_capacity(session, spec.pool_id, datetime.now(UTC)))[spec.pool_id]
    await change_participant(sessions, development, mutation)
    admitted = await prepare(sessions, await principal(staging.participant_id), workload(staging), profiles)
    assert admitted.phase == 'reserved'
    async with sessions.begin() as session:
        after = (await read_connected_capacity(session, spec.pool_id, datetime.now(UTC)))[spec.pool_id]
        assert after.observation_id == before.observation_id
        assert (await session.get(NebiusPoolRequest, retained.reservation_id)).phase == 'reserved'
        assert (await session.get(NebiusPoolBinding, spec.pool_id)).mode == 'global'


@pytest.mark.parametrize('mutation', ['suspend', 'release'])
async def test_capture_in_flight_remains_recordable_across_dev_change(sessions, mutation):
    spec, principal = await setup(sessions)
    capture = await capture_scope(sessions, await principal())
    await change_participant(sessions, spec.participants[2], mutation)
    result = await publish(sessions, await principal(), capture)
    assert result.capture_id == capture.capture_id


@pytest.mark.parametrize('mutation', ['suspend', 'release'])
async def test_legacy_registration_freshness_still_required(sessions, mutation):
    from loom_service.pool_management.observations import PoolObservationError

    spec, principal = await setup(sessions, physical=False)
    capture = await capture_scope(sessions, await principal())
    await change_participant(sessions, spec.participants[2], mutation)
    with pytest.raises(PoolObservationError):
        await publish(sessions, await principal(), capture)
    async with sessions.begin() as session:
        with pytest.raises(ValueError, match='pool_observation_unavailable'):
            await read_connected_capacity(session, spec.pool_id, datetime.now(UTC))


@pytest.mark.parametrize('damage', ['selector', 'quota', 'policy', 'epoch', 'unknown-binding', 'downgrade',
    'policy-revision', 'whole-catalog'])
async def test_physical_changes_require_new_evidence(sessions, damage):
    from loom_service.pool_management.observations import PoolObservationError

    spec, principal = await setup(sessions)
    capture = await capture_scope(sessions, await principal())
    async with sessions.begin() as session:
        pool = await session.get(NebiusPoolBinding, spec.pool_id)
        binding = copy.deepcopy(pool.binding_json)
        values = {'policy_revision': pool.policy_revision + 1}
        if damage == 'selector':
            binding['node_selector']['loom.nebius/node-arch'] = 'arm64'
        elif damage == 'quota':
            binding['quota_identities']['nodes'][0] = 'different-parent'
        elif damage == 'policy':
            binding['admission']['build_concurrency_limit'] += 1
        elif damage == 'epoch':
            values['admission_epoch'] = pool.admission_epoch + 1
        elif damage == 'unknown-binding':
            binding['future_authority'] = 'changed'
        elif damage == 'whole-catalog':
            binding['profile_catalog_sha256'] = 'b' * 64
        elif damage == 'downgrade':
            del binding['observation_binding']
        await session.execute(update(NebiusPoolBinding).where(NebiusPoolBinding.pool_id == spec.pool_id)
            .values(**values, binding_json=binding, binding_sha256=digest(binding)))
    with pytest.raises(PoolObservationError):
        await publish(sessions, await principal(), capture)
    async with sessions.begin() as session:
        with pytest.raises(ValueError, match='pool_observation_unavailable'):
            await read_connected_capacity(session, spec.pool_id, datetime.now(UTC))


async def test_physical_capture_records_namespace_uids_without_changing_legacy_scope(sessions):
    spec, principal = await setup(sessions)
    capture = await capture_scope(sessions, await principal())
    scope = capture.scope.model_dump(mode='json')
    bindings = {row['environment_id']: row for row in scope['environments']}
    assert scope.get('authority') is not None, 'physical capture must retain its own qualified authority'
    for participant in spec.participants:
        binding = bindings[str(participant.environment_id)]
        assert binding['execution_namespace_uid'] == str(participant.execution_namespace.uid)
        assert binding['build_namespace_uid'] == str(participant.build_namespace.uid)
