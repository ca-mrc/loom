"""Startup observes exact current SQL authority; it never replays registration."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import psycopg
import pytest
from sqlalchemy import insert, update
from sqlalchemy.engine import make_url

from loom.db.nebius_pool_schema import NebiusPoolBinding, NebiusPoolMachine, NebiusPoolParticipant
from loom.db.schema import Token
from loom_service.pool_management.capacity import digest
from loom_service.pool_management.installation import PoolInstallation, register_installation
from tests.integration.test_nebius_pool_installation import installation
from tests.integration.test_nebius_pool_registry import sessions as sessions


def observe(url, spec):
    from scripts.ops.nebius_pool_startup_database import (
        pool_startup_closed_sql,
        qualify_startup_closed_report,
    )

    with psycopg.connect(make_url(url).set(drivername='postgresql').render_as_string(hide_password=False), autocommit=True) as connection:
        with connection.cursor() as cursor:
            cursor.execute(pool_startup_closed_sql(spec), prepare=False)
            rows = []
            while True:
                if cursor.description:
                    rows.extend(cursor.fetchall())
                if not cursor.nextset():
                    break
    assert len(rows) == 1
    qualify_startup_closed_report(spec, rows[0][0])
    return rows[0][0]


@pytest.mark.parametrize('damage', [None, 'missing', 'mode', 'epoch', 'binding', 'participant',
    'participant_binding', 'machine_epoch', 'machine_revoked', 'foreign_machine', 'token_revoked', 'token_expired', 'token_scope'])
async def test_startup_reads_exact_closed_pool_and_live_machine_authority(sessions, damage):
    config, _ = installation()
    spec = PoolInstallation.model_validate(config)
    if damage != 'missing':
        async with sessions.begin() as session:
            await register_installation(session, spec)
    async with sessions.begin() as session:
        if damage in {'mode', 'epoch', 'binding'}:
            values = ({'mode': 'global'} if damage == 'mode' else {'admission_epoch': spec.admission_epoch + 1} if damage == 'epoch'
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
        with pytest.raises(ValueError, match='pool_startup_closed_registration_unqualified'):
            observe(url, spec)
    else:
        first = observe(url, spec)
        assert observe(url, spec) == first
        assert first == {'schema': 'loom.pool-startup-closed.v1', 'operation_id': str(spec.operation_id),
            'installation_sha256': digest(spec.model_dump(mode='json')),
            'read_only': True, 'qualified': True}
        # Observing startup leaves the original registration exactly replayable.
        async with sessions.begin() as session:
            assert (await register_installation(session, spec))['mode'] == 'closed'
