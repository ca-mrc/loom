"""Startup observes exact current SQL authority; it never replays registration."""
from __future__ import annotations

import asyncio
import copy
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import psycopg
import pytest
from sqlalchemy import insert, select, update
from sqlalchemy.engine import make_url
from sqlalchemy.exc import ProgrammingError

from loom.db.nebius_pool_schema import (
    NebiusPoolBinding,
    NebiusPoolEffect,
    NebiusPoolMachine,
    NebiusPoolParticipant,
    NebiusPoolRequest,
)
from loom.db.schema import Token
from loom_service.pool_management.capacity import digest
from loom_service.pool_management.installation import PoolInstallation, register_installation
from tests.integration.conftest import retained_pool_sessions
from tests.integration.test_nebius_pool_installation import add_application_builder, installation
from tests.unit.test_nebius_application_image_renderer import build_inputs as build_inputs

sessions = retained_pool_sessions


async def test_participant_registry_reader_follows_real_refresh_without_writes_or_mutation_lock(sessions):
    from scripts.ops import nebius_pool_startup_database as reader

    from loom_service.pool_management.locks import acquire_pool_mutation_lock
    from loom_service.pool_management.participant_refresh import refresh_participant
    from tests.integration.test_nebius_pool_activation_fence import read_sql
    from tests.integration.test_nebius_pool_participant_refresh import reserved, setup, snapshot

    request, _ = await setup(sessions)
    await reserved(sessions, request, request.before.participants[1])
    url = sessions.kw['bind'].url.render_as_string(hide_password=False)
    query = reader.pool_participant_authority_sql(request)
    for action, state in ((None, 'ready'), ('fence', 'fenced'), ('stage', 'staged'), ('open', 'complete')):
        if action is not None:
            async with sessions.begin() as session:
                assert (await refresh_participant(session, request, action=action))['status'] == state
        before = await snapshot(sessions)
        # Ordinary admission holds these locks. Observation must remain a
        # READ ONLY snapshot, not call the refresh transaction's observe action.
        async with sessions.begin() as session:
            await acquire_pool_mutation_lock(session)
            await session.get(NebiusPoolBinding, request.before.pool_id, with_for_update=True)
            report = await asyncio.to_thread(read_sql, url, query)
        assert report == {'schema': 'loom.pool-participant-authority.v1',
            'operation_id': str(request.after.operation_id), 'pool_id': str(request.before.pool_id),
            'before_sha256': digest(request.before.model_dump(mode='json')),
            'after_sha256': digest(request.after.model_dump(mode='json')), 'read_only': True, 'state': state}
        assert reader.qualify_participant_authority_report(request, report, states=frozenset({state})) == state
        for damage in ('operation_id', 'before_sha256', 'after_sha256', 'pool_id', 'read_only', 'extra'):
            changed = copy.deepcopy(report)
            changed[damage] = 1 if damage == 'read_only' else 'foreign'
            with pytest.raises(ValueError, match='participant_authority'):
                reader.qualify_participant_authority_report(request, changed, states=frozenset({state}))
        with pytest.raises(ValueError, match='participant_authority'):
            reader.qualify_participant_authority_report(request, report,
                states=frozenset({'fenced' if state != 'fenced' else 'ready'}))
        assert await snapshot(sessions) == before


@pytest.mark.parametrize('damage', ['neighbor-fenced', 'selected-binding', 'pool-closed', 'extra-machine',
    'revoked-token', 'expired-token'])
async def test_participant_registry_reader_rejects_drift_during_staged_release(sessions, damage):
    from scripts.ops import nebius_pool_startup_database as reader

    from loom_service.pool_management.participant_refresh import refresh_participant
    from tests.integration.test_nebius_pool_activation_fence import read_sql
    from tests.integration.test_nebius_pool_participant_refresh import setup, snapshot

    request, _ = await setup(sessions)
    for action in ('fence', 'stage'):
        async with sessions.begin() as session:
            await refresh_participant(session, request, action=action)
    async with sessions.begin() as session:
        if damage == 'neighbor-fenced':
            await session.execute(update(NebiusPoolParticipant).where(
                NebiusPoolParticipant.participant_id == request.before.participants[1].participant_id).values(phase='fenced'))
        elif damage == 'selected-binding':
            await session.execute(update(NebiusPoolParticipant).where(
                NebiusPoolParticipant.participant_id == request.participant_id).values(binding_sha256='0' * 64,
                    binding_revision=request.after.participants[0].binding_revision + 1))
        elif damage == 'pool-closed':
            await session.execute(update(NebiusPoolBinding).where(
                NebiusPoolBinding.pool_id == request.before.pool_id).values(mode='closed'))
        elif damage == 'extra-machine':
            await session.execute(insert(NebiusPoolMachine).values(machine_id=uuid4(), pool_id=request.before.pool_id,
                participant_id=None, role='gateway', credential_epoch=1, phase='active'))
        else:
            values = ({'revoked_at': datetime.now(UTC)} if damage == 'revoked-token' else
                {'expires_at': datetime.now(UTC) - timedelta(seconds=1)})
            await session.execute(update(Token).where(Token.token_hash == bytes.fromhex(request.before.machines[0].token_sha256)).values(**values))
    before = await snapshot(sessions)
    url = sessions.kw['bind'].url.render_as_string(hide_password=False)
    report = await asyncio.to_thread(read_sql, url, reader.pool_participant_authority_sql(request))
    assert report['state'] is None
    with pytest.raises(ValueError, match='participant_authority'):
        reader.qualify_participant_authority_report(request, report, states=frozenset({'ready', 'fenced', 'staged', 'complete'}))
    assert await snapshot(sessions) == before


def observe(url, spec, *, mode='closed'):
    from scripts.ops.nebius_pool_startup_database import (
        pool_startup_closed_sql,
        qualify_startup_closed_report,
    )

    query, qualify = pool_startup_closed_sql, qualify_startup_closed_report
    if mode == 'global':
        from scripts.ops.nebius_pool_startup_database import (
            pool_active_authority_sql,
            qualify_active_authority_report,
        )

        query, qualify = pool_active_authority_sql, qualify_active_authority_report

    with psycopg.connect(make_url(url).set(drivername='postgresql').render_as_string(hide_password=False), autocommit=True) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query(spec), prepare=False)
            rows = []
            while True:
                if cursor.description:
                    rows.extend(cursor.fetchall())
                if not cursor.nextset():
                    break
    assert len(rows) == 1
    qualify(spec, rows[0][0])
    return rows[0][0]


async def test_dedicated_builder_scope_survives_startup_and_qualified_retirement(sessions, build_inputs):
    from scripts.ops.nebius_pool_activation_database import fence_pool_activation_sql
    from scripts.ops.nebius_pool_machine_database import (
        pool_machine_retirement_sql,
        qualify_machine_retirement_report,
    )

    from tests.integration.test_nebius_pool_activation_fence import read_sql

    config, _ = installation()
    config, builder, _ = add_application_builder(config, build_inputs[0].recipe)
    spec = PoolInstallation.model_validate(config)
    async with sessions.begin() as session:
        await register_installation(session, spec)
    url = sessions.kw['bind'].url.render_as_string(hide_password=False)
    assert observe(url, spec)['qualified'] is True

    async def scope(value):
        async with sessions.begin() as session:
            await session.execute(update(NebiusPoolMachine).where(NebiusPoolMachine.machine_id == builder).values(workload_scope=value))

    # The actual schema rejects scope replacement before a reader can see it.
    # Preserve that trigger rather than manufacturing a weaker database fixture.
    with pytest.raises(ProgrammingError, match='global pool machine identity is immutable'):
        await scope('environment')
    assert observe(url, spec)['qualified'] is True
    read_sql(url, fence_pool_activation_sql(spec))
    assert qualify_machine_retirement_report(spec, read_sql(url, pool_machine_retirement_sql(spec, action='observe'))) == 'active'
    with pytest.raises(ProgrammingError, match='global pool machine identity is immutable'):
        await scope('environment')
    async with sessions() as session:
        assert set(await session.scalars(select(NebiusPoolMachine.phase))) == {'active'}
    assert qualify_machine_retirement_report(spec, read_sql(url, pool_machine_retirement_sql(spec, action='revoke'))) == 'revoked'
    async with sessions() as session:
        rows = list(await session.scalars(select(NebiusPoolMachine)))
        assert all(row.phase == 'revoked' for row in rows)
        assert {row.machine_id: row.workload_scope for row in rows} == {
            row.machine_id: 'application_builder' if row.machine_id == builder else 'environment' for row in spec.machines}


@pytest.mark.parametrize('mode', ['closed', 'global'])
@pytest.mark.parametrize('damage', [None, 'missing', 'mode', 'epoch', 'binding', 'participant',
    'participant_binding', 'machine_epoch', 'machine_revoked', 'foreign_machine', 'token_revoked', 'token_expired', 'token_scope'])
async def test_registration_reader_requires_its_exact_pool_mode_and_machine_authority(sessions, damage, mode):
    config, _ = installation()
    spec = PoolInstallation.model_validate(config)
    if damage != 'missing':
        async with sessions.begin() as session:
            await register_installation(session, spec)
    async with sessions.begin() as session:
        if mode == 'global':
            await session.execute(update(NebiusPoolBinding).where(NebiusPoolBinding.pool_id == spec.pool_id).values(mode='global'))
        if damage in {'mode', 'epoch', 'binding'}:
            values = ({'mode': 'global' if mode == 'closed' else 'closed'} if damage == 'mode' else {'admission_epoch': spec.admission_epoch + 1} if damage == 'epoch'
                else {'binding_sha256': '0' * 64, 'policy_revision': spec.policy_revision + 1})
            await session.execute(update(NebiusPoolBinding).where(NebiusPoolBinding.pool_id == spec.pool_id).values(**values))
        elif damage in {'participant', 'participant_binding'}:
            values = {'phase': 'fenced'} if damage == 'participant' else {'binding_sha256': '0' * 64, 'binding_revision': spec.participants[0].binding_revision + 1}
            await session.execute(update(NebiusPoolParticipant).where(NebiusPoolParticipant.participant_id == spec.participants[0].participant_id).values(**values))
        elif damage in {'machine_epoch', 'machine_revoked'}:
            values = {'credential_epoch': 2} if damage == 'machine_epoch' else {'phase': 'revoked'}
            await session.execute(update(NebiusPoolMachine).where(NebiusPoolMachine.machine_id == spec.machines[0].machine_id).values(**values))
        elif damage == 'foreign_machine':
            await session.execute(insert(NebiusPoolMachine).values(machine_id=uuid4(), pool_id=spec.pool_id,
                participant_id=None, role='gateway', credential_epoch=1, phase='active'))
        elif damage in {'token_revoked', 'token_expired', 'token_scope'}:
            values = ({'revoked_at': datetime.now(UTC)} if damage == 'token_revoked' else
                {'expires_at': datetime.now(UTC) - timedelta(seconds=1)} if damage == 'token_expired' else {'scopes': ['admin']})
            await session.execute(update(Token).where(Token.token_hash == bytes.fromhex(spec.machines[0].token_sha256)).values(**values))
    url = sessions.kw['bind'].url.render_as_string(hide_password=False)
    if damage:
        with pytest.raises(ValueError, match='pool_startup_closed_registration_unqualified' if mode == 'closed' else 'pool_active_authority_unqualified'):
            observe(url, spec, mode=mode)
    else:
        first = observe(url, spec, mode=mode)
        assert observe(url, spec, mode=mode) == first
        assert first == {'schema': 'loom.pool-startup-closed.v1' if mode == 'closed' else 'loom.pool-active-authority.v1', 'operation_id': str(spec.operation_id),
            'installation_sha256': digest(spec.model_dump(mode='json')),
            'read_only': True, 'qualified': True}
        # Observing startup leaves the original registration exactly replayable.
        if mode == 'closed':
            async with sessions.begin() as session:
                assert (await register_installation(session, spec))['mode'] == 'closed'


@pytest.mark.parametrize('phase', ['waiting', 'reserved', 'create_intent', 'observed'])
async def test_active_authority_allows_unfinished_work_without_changing_it(sessions, tmp_path, phase):
    from loom_service.pool_management.gateway_journal import PoolGatewayJournal
    from tests.integration.test_nebius_pool_control import action, operate
    from tests.integration.test_nebius_pool_recovery_drain import request_setup

    spec, url, principal, request = await request_setup(sessions, tmp_path, waiting=phase == 'waiting')
    if phase in {'create_intent', 'observed'}:
        receipt = await operate(sessions, await principal('participant'), action(request, activation=True),
            profiles=spec.profiles.profiles())
        assert receipt.phase == 'create_intent'
        if phase == 'observed':
            journal, gateway = PoolGatewayJournal(sessions), await principal('gateway')
            effect = await journal.prepare_create(gateway, receipt.reservation_id, kind='Job')
            await journal.dispatch_create(gateway, effect.effect_id)
            await journal.observe_create(gateway, effect.effect_id, uid=uuid4(), resource_version='1')

    async def snapshot():
        async with sessions() as session:
            return [list((await session.execute(select(model.__table__))).mappings())
                for model in (NebiusPoolRequest, NebiusPoolBinding, NebiusPoolParticipant, NebiusPoolMachine, Token, NebiusPoolEffect)]

    before = await snapshot()
    assert len(before[0]) == 1 and before[0][0]['phase'] == phase
    assert (before[0][0]['job_uid'] is not None) == (phase == 'observed')
    assert observe(url, spec, mode='global')['qualified'] is True
    assert await snapshot() == before
    # This proof does not relax startup's closed-mode requirement.
    with pytest.raises(ValueError, match='pool_startup_closed_registration_unqualified'):
        observe(url, spec)
