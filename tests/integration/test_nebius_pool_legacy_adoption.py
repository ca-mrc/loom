"""Explicit import of v1 authority does not replay or invent a writer transfer."""
from __future__ import annotations

import copy
import importlib
from uuid import uuid4

import pytest
from sqlalchemy import select, update

from loom.db.nebius_pool_lifecycle_schema import NebiusPoolParticipantLifecycle
from loom.db.nebius_pool_schema import NebiusPoolBinding, NebiusPoolParticipant
from loom_service.pool_management.auth import resolve_pool_machine
from loom_service.pool_management.installation import PoolInstallation, register_installation
from loom_service.pool_management.observation_binding import physical_binding_digest
from tests.integration.test_nebius_pool_installation import installation
from tests.integration.test_nebius_pool_registry import sessions as sessions


def adapter():
    name = 'loom_service.pool_management.legacy_adoption'
    assert importlib.util.find_spec(name), 'explicit legacy adoption adapter is missing'
    return importlib.import_module(name)


async def setup(sessions, *, mode='global'):
    module = adapter()
    config, tokens = installation(('staging', 'development'))
    config['node_selector'] = copy.deepcopy(config['profiles']['execution'][0]['runtime']['node_selector'])
    spec = PoolInstallation.model_validate(config)
    async with sessions.begin() as session:
        await register_installation(session, spec)
        await session.execute(update(NebiusPoolBinding).where(NebiusPoolBinding.pool_id == spec.pool_id).values(mode=mode))
        pool = await session.get(NebiusPoolBinding, spec.pool_id)
        request = module.PoolLegacyAdoptionV2(schema_version='loom.pool-legacy-adoption.v2', operation_id=uuid4(),
            expected_pool_authority_sha256=physical_binding_digest(pool), expected_mode=mode, before=spec)
    return module, spec, tokens, request


@pytest.mark.parametrize('mode,expected', [('closed', 'enrolled'), ('global', 'active')])
async def test_adoption_preserves_pool_mode_and_exact_credentials_without_reinstall(sessions, mode, expected):
    module, spec, tokens, request = await setup(sessions, mode=mode)
    async with sessions.begin() as session:
        first = await module.adopt_legacy_platform(session, request)
    async with sessions.begin() as session:
        assert await module.adopt_legacy_platform(session, request) == first
        pool = await session.get(NebiusPoolBinding, spec.pool_id)
        assert pool.mode == mode and pool.admission_epoch == spec.admission_epoch
        assert pool.policy_revision == spec.policy_revision + 1
        for selected in spec.participants:
            row = await session.get(NebiusPoolParticipant, selected.participant_id)
            assert row.binding_json == selected.model_dump(mode='json') and row.binding_revision == selected.binding_revision
            assert (await session.get(NebiusPoolParticipantLifecycle, selected.participant_id)).state == expected
        for token in tokens.values():
            principal = await resolve_pool_machine(session, 'Bearer ' + token)
            assert principal is not None and principal.pool_mode == mode


async def test_adoption_preserves_legacy_selected_fence_and_peer_active_state(sessions):
    module, spec, _, request = await setup(sessions)
    async with sessions.begin() as session:
        await session.execute(update(NebiusPoolParticipant).where(
            NebiusPoolParticipant.participant_id == spec.participants[1].participant_id).values(phase='fenced'))
        await module.adopt_legacy_platform(session, request)
    async with sessions() as session:
        selected = await session.get(NebiusPoolParticipant, spec.participants[1].participant_id)
        assert selected.phase == 'fenced'
        heads = {row.participant_id: row.state for row in await session.scalars(select(NebiusPoolParticipantLifecycle))}
        assert heads == {spec.participants[0].participant_id: 'active', spec.participants[1].participant_id: 'suspended'}


@pytest.mark.parametrize('damage', ['policy', 'participant', 'standalone', 'credential'])
async def test_adoption_refuses_unqualified_current_authority_without_partial_import(sessions, damage):
    from loom.db.nebius_pool_schema import NebiusPoolMachine

    module, spec, _, request = await setup(sessions)
    async with sessions.begin() as session:
        if damage == 'policy':
            await session.execute(update(NebiusPoolBinding).where(NebiusPoolBinding.pool_id == spec.pool_id).values(policy_revision=9))
        elif damage == 'standalone':
            await session.execute(update(NebiusPoolBinding).where(NebiusPoolBinding.pool_id == spec.pool_id).values(mode='legacy'))
        elif damage == 'credential':
            await session.execute(update(NebiusPoolMachine).where(NebiusPoolMachine.machine_id == spec.machines[0].machine_id).values(phase='revoked'))
        else:
            await session.execute(update(NebiusPoolParticipant).where(
                NebiusPoolParticipant.participant_id == spec.participants[1].participant_id).values(binding_revision=2))
    with pytest.raises(ValueError):
        async with sessions.begin() as session:
            await module.adopt_legacy_platform(session, request)
    async with sessions() as session:
        assert list(await session.scalars(select(NebiusPoolParticipantLifecycle))) == []
        assert 'lifecycle_source' not in (await session.get(NebiusPoolBinding, spec.pool_id)).binding_json
