"""Native preview rollback keeps retained evidence and never waits on parent readers."""
from __future__ import annotations

from datetime import UTC, datetime

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import Trial
from loom.db.schema_startup import service_schema_head
from loom.service_execution_live import append_live_events
from tests.integration.test_service_execution_leases import _reserve, _seed_ready_trial


def _config(url: str) -> Config:
    config = Config('database/migrations/alembic.ini')
    config.set_main_option('sqlalchemy.url', url.replace('%', '%%'))
    return config


@pytest.mark.parametrize('table', ['service_execution_live_events', 'trials',
                                 'execution_leases', 'data_lifecycle_authorities'])
def test_preview_downgrade_refuses_journal_and_parent_read_locks_without_waiting(
    isolated_migration_postgres_url: str, table: str,
) -> None:
    url = make_url(isolated_migration_postgres_url).update_query_dict(
        {'options': '-c statement_timeout=2000'},
    )
    config = _config(url.render_as_string(hide_password=False))
    engine = create_engine(isolated_migration_postgres_url)
    try:
        with engine.begin() as connection:
            connection.exec_driver_sql(f'LOCK TABLE "{table}" IN ACCESS SHARE MODE')
            with pytest.raises(DBAPIError, match='could not obtain lock'):
                command.downgrade(config, '0179')
        with engine.connect() as connection:
            assert connection.scalar(text('SELECT version_num FROM alembic_version')) == service_schema_head()
    finally:
        engine.dispose()


def test_empty_preview_migration_round_trip(isolated_migration_postgres_url: str) -> None:
    config = _config(isolated_migration_postgres_url)
    engine = create_engine(isolated_migration_postgres_url)
    try:
        assert inspect(engine).has_table('service_execution_live_events')
        command.downgrade(config, '0179')
        assert not inspect(engine).has_table('service_execution_live_events')
        command.upgrade(config, 'head')
        assert inspect(engine).has_table('service_execution_live_events')
        with engine.connect() as connection:
            assert connection.scalar(text('SELECT version_num FROM alembic_version')) == service_schema_head()
            assert connection.scalar(text("SELECT EXISTS(SELECT 1 FROM pg_trigger WHERE tgname='service_execution_live_events_notify')"))
    finally:
        engine.dispose()


async def test_preview_downgrade_retains_nonempty_evidence(isolated_migration_postgres_url: str) -> None:
    now = datetime.now(UTC)
    engine = create_async_engine(isolated_migration_postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            lease = await _reserve(session, trial_id=trial_id, target=target, now=now)
            trial = await session.get(Trial, trial_id)
            await append_live_events(session, lease=lease, trial=trial, events=[{
                'kind': 'step_start', 'trial_id': str(trial_id), 'step_id': 'agent',
                'seq': 0, 'emitted_at': now.isoformat(), 'instruction_excerpt': 'retained native preview',
            }])
            await session.commit()
    finally:
        await engine.dispose()
    config = _config(isolated_migration_postgres_url)
    with pytest.raises(DBAPIError, match='cannot discard native live trajectory evidence'):
        command.downgrade(config, '0179')
    sync_engine = create_engine(isolated_migration_postgres_url)
    try:
        with sync_engine.connect() as connection:
            assert connection.scalar(text('SELECT version_num FROM alembic_version')) == service_schema_head()
            assert connection.scalar(text('SELECT count(*) FROM service_execution_live_events')) == 1
            assert connection.scalar(text('SELECT count(*) FROM trial_events')) == 0
    finally:
        sync_engine.dispose()
