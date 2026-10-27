"""Bound manager observations traverse real retained Alembic schema revisions."""
from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import psycopg
import pytest
from alembic import command
from alembic.config import Config
from scripts.ops.nebius_pool_activation_database import (
    fence_pool_activation_sql,
    pool_activation_state_sql,
    qualify_pool_activation_report,
)
from scripts.ops.nebius_pool_machine_database import (
    pool_machine_retirement_sql,
    qualify_machine_retirement_report,
)
from scripts.ops.nebius_pool_recovery_database import (
    pool_recovery_drain_sql,
    qualify_pool_recovery_drain,
)
from scripts.ops.nebius_pool_startup_database import (
    pool_active_authority_sql,
    qualify_active_authority_report,
)
from sqlalchemy import select, update
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.nebius_pool_schema import NebiusPoolBinding, NebiusPoolMachine
from loom.db.schema import Token
from loom_service.pool_management.installation import PoolInstallation, register_installation
from tests.integration.test_nebius_pool_installation import installation


def _read(url: str, query: str) -> dict:
    """Also check the actual transaction mode, not merely a reported constant."""
    query = query.replace('ROLLBACK;', "SELECT current_setting('transaction_read_only') AS read_only; ROLLBACK;")
    reports, modes = [], []
    with psycopg.connect(make_url(url).set(drivername='postgresql').render_as_string(hide_password=False),
            autocommit=True) as connection, connection.cursor() as cursor:
        cursor.execute(query, prepare=False)
        while True:
            if cursor.description:
                rows = cursor.fetchall()
                if cursor.description[0].name == 'read_only':
                    modes.extend(row[0] for row in rows)
                else:
                    reports.extend(row[0] for row in rows)
            if not cursor.nextset():
                break
    assert modes == ['on']
    assert len(reports) == 1
    return reports[0]


@pytest.fixture
async def manager_database(isolated_migration_postgres_url: str, request):
    """Downgrade only this disposable clone, never relabel a newer schema."""
    revision = request.param
    root = Path(__file__).resolve().parents[2]
    cfg = Config(str(root / 'database/migrations/alembic.ini'))
    cfg.set_main_option('script_location', str(root / 'database/migrations'))
    url = make_url(isolated_migration_postgres_url).set(drivername='postgresql+psycopg').render_as_string(hide_password=False)
    cfg.set_main_option('sqlalchemy.url', url.replace('%', '%%'))
    await asyncio.to_thread(command.downgrade, cfg, revision)
    engine = create_async_engine(isolated_migration_postgres_url)
    try:
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        value, _ = installation()
        spec = PoolInstallation.model_validate(value)
        async with sessions.begin() as session:
            await register_installation(session, spec)
        yield spec, sessions, isolated_migration_postgres_url, revision
    finally:
        await engine.dispose()


@pytest.mark.parametrize('manager_database', ['0174', '0176', '0177', '0178', '0179', '0180', '0181', '0182', '0183', '0184'], indirect=True)
@pytest.mark.parametrize('reader', ['activation', 'active', 'drain', 'retirement'])
async def test_bound_manager_observation_uses_exact_revision_without_relaxing_scope(manager_database, reader):
    spec, sessions, url, revision = manager_database
    if reader != 'activation':
        async with sessions.begin() as session:
            await session.execute(update(NebiusPoolBinding).where(NebiusPoolBinding.pool_id == spec.pool_id).values(
                mode='global' if reader == 'active' else 'closed',
                policy_revision=spec.policy_revision if reader == 'active' else spec.policy_revision + 1))
    generate = {
        'activation': pool_activation_state_sql,
        'active': pool_active_authority_sql,
        'drain': pool_recovery_drain_sql,
        'retirement': lambda selected, **kw: pool_machine_retirement_sql(selected, action='observe', **kw),
    }[reader]
    qualify = {
        'activation': qualify_pool_activation_report,
        'active': qualify_active_authority_report,
        'drain': qualify_pool_recovery_drain,
        'retirement': qualify_machine_retirement_report,
    }[reader]
    if revision != '0176':
        with pytest.raises(psycopg.Error, match='schema unqualified'):
            _read(url, generate(spec))
    report = _read(url, generate(spec, manager_revision=revision))
    assert qualify(spec, report) == {'activation': 'closed', 'active': None, 'drain': True, 'retirement': 'active'}[reader]
    if reader == 'retirement':
        async with sessions.begin() as session:
            await session.execute(update(NebiusPoolMachine).where(NebiusPoolMachine.pool_id == spec.pool_id).values(phase='revoked'))
            await session.execute(update(Token).where(Token.type == 'pool_machine').values(revoked_at=datetime.now(UTC)))
        assert qualify(spec, _read(url, generate(spec, manager_revision=revision))) == 'revoked'
    wrong_revision = '0177' if revision != '0177' else '0174'
    with pytest.raises(psycopg.Error, match='schema unqualified'):
        _read(url, generate(spec, manager_revision=wrong_revision))
    foreign = spec.model_copy(update={'policy_revision': spec.policy_revision + 2})
    if reader in {'drain', 'retirement'}:
        with pytest.raises(psycopg.Error, match='unqualified'):
            _read(url, generate(foreign, manager_revision=revision))
    else:
        damaged = _read(url, generate(foreign, manager_revision=revision))
        with pytest.raises(ValueError):
            qualify(foreign, damaged)
    if revision != '0176':
        with pytest.raises(psycopg.Error, match='schema unqualified'):
            _read(url, fence_pool_activation_sql(spec))


@pytest.mark.parametrize('manager_database', ['0174', '0177', '0178', '0179', '0180', '0181', '0182', '0183', '0184'], indirect=True)
async def test_bound_manager_revision_preserves_busy_journals_and_retirement_refusal(manager_database):
    from tests.integration.test_nebius_pool_participant_refresh import reserved, snapshot

    spec, sessions, url, revision = manager_database
    async with sessions.begin() as session:
        await session.execute(update(NebiusPoolBinding).where(NebiusPoolBinding.pool_id == spec.pool_id).values(mode='global'))
    await reserved(sessions, SimpleNamespace(before=spec), spec.participants[0], phase='waiting')
    async with sessions.begin() as session:
        await session.execute(update(NebiusPoolBinding).where(NebiusPoolBinding.pool_id == spec.pool_id).values(
            mode='closed', policy_revision=spec.policy_revision + 1))
    before = await snapshot(sessions)
    report = _read(url, pool_recovery_drain_sql(spec, manager_revision=revision))
    assert report['counts']['unstarted_requests'] == 1
    assert qualify_pool_recovery_drain(spec, report) is False
    with pytest.raises(psycopg.Error, match='journal not drained'):
        _read(url, pool_machine_retirement_sql(spec, action='observe', manager_revision=revision))
    assert await snapshot(sessions) == before


@pytest.mark.parametrize('manager_database', ['0177', '0178', '0179', '0180', '0181', '0182', '0183', '0184'], indirect=True)
async def test_successor_revocation_requires_exact_installed_revision_and_scope(manager_database):
    from tests.integration.test_nebius_pool_activation_fence import read_sql

    spec, sessions, url, revision = manager_database
    read_sql(url, fence_pool_activation_sql(spec, manager_revision=revision))
    for selected in ('0176', '0177' if revision == '0178' else '0178'):
        with pytest.raises(psycopg.Error, match='schema unqualified'):
            read_sql(url, pool_machine_retirement_sql(spec, action='revoke', manager_revision=selected))
    foreign = spec.model_copy(update={'policy_revision': spec.policy_revision + 2})
    with pytest.raises(psycopg.Error, match='scope unqualified'):
        read_sql(url, pool_machine_retirement_sql(foreign, action='revoke', manager_revision=revision))
    assert qualify_machine_retirement_report(spec, _read(url,
        pool_machine_retirement_sql(spec, action='observe', manager_revision=revision))) == 'active'
    report = read_sql(url, pool_machine_retirement_sql(spec, action='revoke', manager_revision=revision))
    assert qualify_machine_retirement_report(spec, report) == 'revoked'
    async with sessions() as session:
        assert set(await session.scalars(select(NebiusPoolMachine.phase))) == {'revoked'}
        assert all(value is not None for value in await session.scalars(
            select(Token.revoked_at).where(Token.type == 'pool_machine')))


@pytest.mark.parametrize('revision', ['0174', '0175', '0182', '0199', "0176'; COMMIT; --", None])
def test_manager_revision_rejects_unqualified_observations_and_writes(revision):
    value, _ = installation()
    spec = PoolInstallation.model_validate(value)
    with pytest.raises(ValueError):
        pool_machine_retirement_sql(spec, action='revoke', manager_revision=revision)
    if revision not in {'0174', '0177', '0178', '0179', '0180', '0181', '0182', '0183', '0184'}:
        for generate in (pool_activation_state_sql, pool_active_authority_sql, pool_recovery_drain_sql):
            with pytest.raises(ValueError):
                generate(spec, manager_revision=revision)
        with pytest.raises(ValueError):
            pool_machine_retirement_sql(spec, action='observe', manager_revision=revision)
