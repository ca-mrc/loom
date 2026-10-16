"""Recovery revokes only exact drained machine authority in real PostgreSQL."""
from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import psycopg
import pytest
from sqlalchemy import insert, select, text, update
from sqlalchemy.engine import make_url

from loom.db.nebius_pool_schema import (
    NebiusPoolBinding,
    NebiusPoolMachine,
    NebiusPoolMachineCredential,
    NebiusPoolParticipant,
)
from loom.db.schema import Token
from loom_service.pool_management.auth import (
    PoolAuthenticationError,
    authorize_pool_machine,
    resolve_pool_machine,
)
from loom_service.pool_management.installation import PoolInstallation, register_installation
from tests.integration.conftest import retained_pool_sessions
from tests.integration.test_nebius_pool_activation_fence import read_sql
from tests.integration.test_nebius_pool_installation import installation

sessions = retained_pool_sessions


async def registered(sessions):
    config, tokens = installation()
    spec = PoolInstallation.model_validate(config)
    async with sessions.begin() as session:
        await register_installation(session, spec)
    return spec, tokens, sessions.kw['bind'].url.render_as_string(hide_password=False)


async def test_retirement_denies_new_and_retained_principals_without_changing_other_tokens_or_history(sessions):
    from scripts.ops.nebius_pool_activation_database import fence_pool_activation_sql
    from scripts.ops.nebius_pool_machine_database import (
        pool_machine_retirement_sql,
        qualify_machine_retirement_report,
    )

    spec, tokens, url = await registered(sessions)
    foreign = b'x' * 32
    async with sessions.begin() as session:
        await session.execute(insert(Token).values(token_hash=foreign, type='worker', scopes=[], issued_at=datetime.now(UTC)))
    read_sql(url, fence_pool_activation_sql(spec))
    async with sessions() as session:
        principals = [await resolve_pool_machine(session, 'Bearer ' + secret) for secret in tokens.values()]
        before = (await session.execute(text('SELECT to_jsonb(t) FROM tokens t WHERE token_hash=:key'), {'key': foreign})).scalar_one()
        counts = (await session.execute(text('SELECT (SELECT count(*) FROM nebius_pool_requests), '
            '(SELECT count(*) FROM nebius_pool_effects), (SELECT count(*) FROM nebius_pool_machine_credentials)'))).one()
    assert all(principal is not None for principal in principals)
    assert qualify_machine_retirement_report(spec, read_sql(url, pool_machine_retirement_sql(spec, action='observe'))) == 'active'
    first = read_sql(url, pool_machine_retirement_sql(spec, action='revoke'))
    assert qualify_machine_retirement_report(spec, first) == 'revoked'
    async with sessions() as session:
        revoked = list(await session.scalars(select(Token.revoked_at).where(Token.type == 'pool_machine')))
        assert all(value is not None for value in revoked)
        for secret in tokens.values():
            assert await resolve_pool_machine(session, 'Bearer ' + secret) is None
        for principal in principals:
            with pytest.raises(PoolAuthenticationError):
                await authorize_pool_machine(session, principal, role=principal.role, pool_id=principal.pool_id,
                    participant_id=principal.participant_id)
        assert (await session.execute(text('SELECT to_jsonb(t) FROM tokens t WHERE token_hash=:key'), {'key': foreign})).scalar_one() == before
        assert (await session.execute(text('SELECT (SELECT count(*) FROM nebius_pool_requests), '
            '(SELECT count(*) FROM nebius_pool_effects), (SELECT count(*) FROM nebius_pool_machine_credentials)'))).one() == counts
    # SQL is monotonic even if invoked independently; the stage still dispatches
    # it only once and uses observation to recover lost replies.
    assert read_sql(url, pool_machine_retirement_sql(spec, action='observe')) == first
    assert read_sql(url, pool_machine_retirement_sql(spec, action='revoke')) == first
    async with sessions() as session:
        assert list(await session.scalars(select(Token.revoked_at).where(Token.type == 'pool_machine'))) == revoked
        assert set(await session.scalars(select(NebiusPoolMachine.phase))) == {'revoked'}


async def test_retirement_transaction_failure_preserves_all_authority(sessions):
    from scripts.ops.nebius_pool_activation_database import fence_pool_activation_sql
    from scripts.ops.nebius_pool_machine_database import (
        pool_machine_retirement_sql,
        qualify_machine_retirement_report,
    )

    spec, _, url = await registered(sessions)
    read_sql(url, fence_pool_activation_sql(spec))
    with pytest.raises(psycopg.errors.DivisionByZero):
        read_sql(url, pool_machine_retirement_sql(spec, action='revoke').replace('COMMIT;', 'SELECT 1/0; COMMIT;'))
    assert qualify_machine_retirement_report(spec, read_sql(url, pool_machine_retirement_sql(spec, action='observe'))) == 'active'


async def test_locked_authorizations_serialize_with_machine_retirement(sessions):
    from scripts.ops.nebius_pool_activation_database import fence_pool_activation_sql
    from scripts.ops.nebius_pool_machine_database import (
        pool_machine_retirement_sql,
        qualify_machine_retirement_report,
    )

    spec, tokens, url = await registered(sessions)
    read_sql(url, fence_pool_activation_sql(spec))
    async with sessions() as session:
        principal = await resolve_pool_machine(session, 'Bearer ' + next(iter(tokens.values())))
    name = 'retire-' + uuid4().hex
    pending = None
    try:
        async with sessions.begin() as holder:
            await authorize_pool_machine(holder, principal, role=principal.role, pool_id=principal.pool_id,
                participant_id=principal.participant_id)
            pending = asyncio.create_task(asyncio.to_thread(read_sql, url,
                pool_machine_retirement_sql(spec, action='revoke'), application_name=name))
            async with asyncio.timeout(5):
                while True:
                    async with sessions() as inspect:
                        waiting = await inspect.scalar(text("SELECT EXISTS (SELECT 1 FROM pg_stat_activity "
                            "WHERE application_name=:name AND datname=current_database() AND wait_event_type='Lock')"), {'name': name})
                    if waiting:
                        break
                    await asyncio.sleep(0.01)
            assert not pending.done()
        assert qualify_machine_retirement_report(spec, await pending) == 'revoked'
    finally:
        if pending is not None:
            await asyncio.gather(pending, return_exceptions=True)
    async with sessions.begin() as session:
        with pytest.raises(PoolAuthenticationError):
            await authorize_pool_machine(session, principal, role=principal.role, pool_id=principal.pool_id,
                participant_id=principal.participant_id)


async def test_machine_retirement_transport_emits_only_one_safe_json_report(sessions):
    from scripts.ops.nebius_pool_activation_database import fence_pool_activation_sql
    from scripts.ops.nebius_pool_machine_database import pool_machine_retirement_sql

    spec, _, url = await registered(sessions)
    read_sql(url, fence_pool_activation_sql(spec))
    columns = []
    with psycopg.connect(make_url(url).set(drivername='postgresql').render_as_string(hide_password=False), autocommit=True) as connection:
        with connection.cursor() as cursor:
            cursor.execute(pool_machine_retirement_sql(spec, action='revoke'), prepare=False)
            while True:
                if cursor.description:
                    columns.append([column.name for column in cursor.description])
                if not cursor.nextset():
                    break
    assert columns == [['report']]


@pytest.mark.parametrize('damage', ['no_fence', 'participant', 'epoch', 'token_scope', 'partial', 'extra_credential'])
async def test_retirement_rejects_changed_or_partial_authority_without_repair(sessions, damage):
    from scripts.ops.nebius_pool_activation_database import fence_pool_activation_sql
    from scripts.ops.nebius_pool_machine_database import pool_machine_retirement_sql

    spec, _, url = await registered(sessions)
    if damage != 'no_fence':
        read_sql(url, fence_pool_activation_sql(spec))
    machine = spec.machines[0]
    async with sessions.begin() as session:
        if damage == 'participant':
            await session.execute(update(NebiusPoolParticipant).where(
                NebiusPoolParticipant.participant_id == spec.participants[0].participant_id).values(phase='fenced'))
        elif damage == 'epoch':
            await session.execute(update(NebiusPoolMachine).where(NebiusPoolMachine.machine_id == machine.machine_id).values(credential_epoch=2))
        elif damage == 'token_scope':
            await session.execute(update(Token).where(Token.token_hash == bytes.fromhex(machine.token_sha256)).values(scopes=['admin']))
        elif damage == 'partial':
            await session.execute(update(Token).where(Token.token_hash == bytes.fromhex(machine.token_sha256)).values(revoked_at=datetime.now(UTC)))
        elif damage == 'extra_credential':
            await session.execute(insert(Token).values(token_hash=b'z' * 32, type='pool_machine', scopes=[], issued_at=datetime.now(UTC)))
            await session.execute(insert(NebiusPoolMachineCredential).values(token_hash=b'z' * 32,
                machine_id=machine.machine_id, credential_epoch=machine.credential_epoch))
    for action in ('observe', 'revoke'):
        with pytest.raises(psycopg.Error):
            read_sql(url, pool_machine_retirement_sql(spec, action=action))
    async with sessions() as session:
        assert set(await session.scalars(select(NebiusPoolMachine.phase))) == {'active'}


async def test_retirement_does_not_disable_cleanup_for_a_reserved_request(sessions, tmp_path):
    from scripts.ops.nebius_pool_activation_database import fence_pool_activation_sql
    from scripts.ops.nebius_pool_machine_database import pool_machine_retirement_sql

    from tests.integration.test_nebius_pool_recovery_drain import request_setup

    spec, url, principal, _ = await request_setup(sessions, tmp_path)
    read_sql(url, fence_pool_activation_sql(spec))
    with pytest.raises(psycopg.Error):
        read_sql(url, pool_machine_retirement_sql(spec, action='revoke'))
    assert await principal('gateway') is not None


async def test_recovery_authority_can_be_observed_while_real_requests_still_need_cleanup(sessions, tmp_path):
    from scripts.ops import nebius_pool_machine_database as module
    from scripts.ops.nebius_pool_activation_database import fence_pool_activation_sql
    from scripts.ops.nebius_pool_recovery_database import (
        pool_recovery_drain_sql,
        qualify_pool_recovery_drain,
    )

    from tests.integration.test_nebius_pool_recovery_drain import request_setup

    assert hasattr(module, 'pool_recovery_machine_state_sql'), 'read-only recovery authority without a drain precondition is missing'
    spec, url, principal, _ = await request_setup(sessions, tmp_path)
    read_sql(url, fence_pool_activation_sql(spec))
    before = read_sql(url, pool_recovery_drain_sql(spec))
    assert not qualify_pool_recovery_drain(spec, before)
    report = read_sql(url, module.pool_recovery_machine_state_sql(spec))
    assert module.qualify_recovery_machine_state_report(spec, report) == 'active'
    assert report['read_only'] is True
    assert read_sql(url, pool_recovery_drain_sql(spec)) == before
    # Observing authority must not retire credentials, cancel work, or relax the
    # existing retirement barrier. The real cleanup principal remains usable.
    for action in ('observe', 'revoke'):
        with pytest.raises(psycopg.Error):
            read_sql(url, module.pool_machine_retirement_sql(spec, action=action))
    assert await principal('gateway') is not None


@pytest.mark.parametrize('damage', [None, 'binding', 'partial', 'token_scope', 'extra_credential'])
async def test_recovery_authority_observation_requires_exact_machine_state(sessions, damage):
    from scripts.ops import nebius_pool_machine_database as module
    from scripts.ops.nebius_pool_activation_database import fence_pool_activation_sql

    assert hasattr(module, 'pool_recovery_machine_state_sql'), 'read-only recovery authority without a drain precondition is missing'
    spec, _, url = await registered(sessions)
    read_sql(url, fence_pool_activation_sql(spec))
    machine = spec.machines[0]
    async with sessions.begin() as session:
        if damage == 'binding':
            await session.execute(update(NebiusPoolBinding).where(NebiusPoolBinding.pool_id == spec.pool_id).values(policy_revision=spec.policy_revision + 2))
        elif damage == 'partial':
            await session.execute(update(Token).where(Token.token_hash == bytes.fromhex(machine.token_sha256)).values(revoked_at=datetime.now(UTC)))
        elif damage == 'token_scope':
            await session.execute(update(Token).where(Token.token_hash == bytes.fromhex(machine.token_sha256)).values(scopes=['admin']))
        elif damage == 'extra_credential':
            await session.execute(insert(Token).values(token_hash=b'z' * 32, type='pool_machine', scopes=[], issued_at=datetime.now(UTC)))
            await session.execute(insert(NebiusPoolMachineCredential).values(token_hash=b'z' * 32,
                machine_id=machine.machine_id, credential_epoch=machine.credential_epoch))

    def observe():
        return module.qualify_recovery_machine_state_report(spec, read_sql(url, module.pool_recovery_machine_state_sql(spec)))

    if damage is not None:
        with pytest.raises(ValueError):
            observe()
    else:
        assert observe() == 'active'
        read_sql(url, module.pool_machine_retirement_sql(spec, action='revoke'))
        assert observe() == 'revoked'


@pytest.mark.parametrize('mode', ['closed', 'global'])
async def test_expired_original_credentials_can_be_observed_and_retired_without_renewal(sessions, monkeypatch, mode):
    from scripts.ops.nebius_pool_activation_database import fence_pool_activation_sql
    from scripts.ops.nebius_pool_machine_database import (
        pool_machine_retirement_sql,
        pool_recovery_machine_state_sql,
        qualify_machine_retirement_report,
        qualify_recovery_machine_state_report,
    )

    import loom_service.pool_management.installation as module

    config, _ = installation()
    old = datetime.now(UTC) - timedelta(days=3)
    for row in config['machines']:
        row.update(issued_at=(old - timedelta(hours=1)).isoformat(), expires_at=(old + timedelta(hours=1)).isoformat())
    async def past_clock(session):
        return old
    monkeypatch.setattr(module, '_clock', past_clock)
    spec = PoolInstallation.model_validate(config)
    async with sessions.begin() as session:
        await register_installation(session, spec)
        if mode == 'global':
            await session.execute(update(NebiusPoolBinding).where(NebiusPoolBinding.pool_id == spec.pool_id).values(mode='global'))
    url = sessions.kw['bind'].url.render_as_string(hide_password=False)
    assert qualify_recovery_machine_state_report(spec, read_sql(url, pool_recovery_machine_state_sql(spec))) == 'active'
    read_sql(url, fence_pool_activation_sql(spec))
    assert qualify_recovery_machine_state_report(spec, read_sql(url, pool_recovery_machine_state_sql(spec))) == 'active'
    assert qualify_machine_retirement_report(spec, read_sql(url, pool_machine_retirement_sql(spec, action='revoke'))) == 'revoked'
    assert qualify_recovery_machine_state_report(spec, read_sql(url, pool_recovery_machine_state_sql(spec))) == 'revoked'
    async with sessions() as session:
        assert set(await session.scalars(select(Token.expires_at))) == {old + timedelta(hours=1)}
