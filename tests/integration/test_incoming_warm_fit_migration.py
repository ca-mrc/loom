"""Real schema upgrade/downgrade keeps physical registration fences intact."""
import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError

from tests.integration.test_nebius_pool_schema import registered


def _allow_current_cancellation_columns(url: str) -> None:
    """Let the current models insert on a database pinned at 0186.

    These tests stay at 0186 so the warm-fit column is absent. The drain
    columns and the queue pause deadline are nullable and unused on that path.
    """

    engine = create_engine(url)
    try:
        with engine.begin() as connection:
            connection.execute(text(
                "ALTER TABLE trials ADD COLUMN IF NOT EXISTS cancellation_retained_at TIMESTAMPTZ"
            ))
            connection.execute(text(
                "ALTER TABLE batches ADD COLUMN IF NOT EXISTS cancellation_requested_at TIMESTAMPTZ"
            ))
            connection.execute(text(
                "ALTER TABLE batches ADD COLUMN IF NOT EXISTS cancellation_mode TEXT"
            ))
            connection.execute(text(
                "ALTER TABLE batches ADD COLUMN IF NOT EXISTS admission_paused_at TIMESTAMPTZ"
            ))
            connection.execute(text(
                "ALTER TABLE batches ADD COLUMN IF NOT EXISTS admission_paused_reason TEXT"
            ))
            connection.execute(text(
                "ALTER TABLE trials ADD COLUMN IF NOT EXISTS "
                "admission_pause_releases INTEGER NOT NULL DEFAULT 0"
            ))
            connection.execute(text(
                "ALTER TABLE execution_leases ADD COLUMN IF NOT EXISTS "
                "working_deadline_at TIMESTAMPTZ"
            ))
            connection.execute(text(
                "ALTER TABLE provider_request_pools "
                "ADD COLUMN IF NOT EXISTS queue_enabled BOOLEAN NOT NULL DEFAULT false"
            ))
            connection.execute(text(
                "ALTER TABLE provider_request_pools "
                "ADD COLUMN IF NOT EXISTS stall_sec DOUBLE PRECISION NOT NULL DEFAULT 200"
            ))
            connection.execute(text(
                "ALTER TABLE provider_request_pools "
                "ADD COLUMN IF NOT EXISTS max_pause_sec DOUBLE PRECISION NOT NULL DEFAULT 3600"
            ))
            connection.execute(text(
                "ALTER TABLE provider_request_pools "
                "ADD COLUMN IF NOT EXISTS config_version BIGINT NOT NULL DEFAULT 1"
            ))
    finally:
        engine.dispose()


@pytest.mark.parametrize(('direction', 'table', 'lock_mode'), [
    ('downgrade', 'trials', 'ACCESS SHARE'),
    ('downgrade', 'nebius_application_build_attempts', 'ACCESS SHARE'),
    ('downgrade', 'execution_targets', 'ACCESS SHARE'),
    ('downgrade', 'nebius_pool_bindings', 'ACCESS SHARE'),
    ('downgrade', 'nebius_cache_runtime_policies', 'ACCESS SHARE'),
    ('downgrade', 'nebius_pool_placement_advice', 'ACCESS SHARE'),
    ('upgrade', 'trials', 'ROW EXCLUSIVE'),
    ('upgrade', 'nebius_application_build_attempts', 'ACCESS SHARE'),
    ('upgrade', 'execution_targets', 'ACCESS SHARE'),
    ('upgrade', 'nebius_pool_bindings', 'ACCESS SHARE'),
])
def test_0187_refuses_busy_relations_before_any_schema_change(
    isolated_migration_postgres_url, direction, table, lock_mode,
):
    config = Config('database/migrations/alembic.ini')
    url = make_url(isolated_migration_postgres_url).update_query_dict(
        {'options': '-c statement_timeout=2000'})
    config.set_main_option('sqlalchemy.url', url.render_as_string(hide_password=False).replace('%', '%%'))
    engine = create_engine(isolated_migration_postgres_url)
    try:
        # Alembic runs a multi-step downgrade in one transaction, so start
        # directly at 0187 to observe its own refusal.
        command.downgrade(config, '0186' if direction == 'upgrade' else '0187')
        with engine.begin() as connection:
            connection.exec_driver_sql(f'LOCK TABLE "{table}" IN {lock_mode} MODE')
            with pytest.raises(DBAPIError, match='could not obtain lock'):
                if direction == 'upgrade':
                    command.upgrade(config, '0187')
                else:
                    command.downgrade(config, '0186')
        with engine.connect() as connection:
            expected = '0186' if direction == 'upgrade' else '0187'
            assert connection.scalar(text('SELECT version_num FROM alembic_version')) == expected
            assert inspect(connection).has_table('nebius_cache_runtime_policies') == (expected == '0187')
            assert inspect(connection).has_table('nebius_pool_placement_advice') == (expected == '0187')
            assert ('cache_policy_json' in {column['name'] for column in
                inspect(connection).get_columns('nebius_application_build_attempts')}) == (expected == '0187')
    finally:
        engine.dispose()


def test_0187_independent_preference_and_cache_scope_preserve_physical_identity(isolated_migration_postgres_url):
    engine = create_engine(isolated_migration_postgres_url)
    config = Config("database/migrations/alembic.ini")
    config.set_main_option("sqlalchemy.url", isolated_migration_postgres_url.replace("%", "%%"))
    try:
        with engine.begin() as connection:
            pool, _ = registered(connection)
            before = connection.execute(text("SELECT cluster_id,node_group_id,policy_revision,admission_epoch,binding_sha256 "
                "FROM nebius_pool_bindings WHERE pool_id=:pool"), {"pool": pool}).one()
            connection.execute(text("UPDATE nebius_pool_bindings SET prefer_incoming_warm_fit=true WHERE pool_id=:pool"), {"pool": pool})
            assert connection.execute(text("SELECT cluster_id,node_group_id,policy_revision,admission_epoch,binding_sha256 "
                "FROM nebius_pool_bindings WHERE pool_id=:pool"), {"pool": pool}).one() == before
            with pytest.raises(DBAPIError, match="identity is immutable"), connection.begin_nested():
                connection.execute(text("UPDATE nebius_pool_bindings SET cluster_id='foreign' WHERE pool_id=:pool"), {"pool": pool})
        command.downgrade(config, "0186")
        assert "prefer_incoming_warm_fit" not in {col["name"] for col in inspect(engine).get_columns("nebius_pool_bindings")}
        command.upgrade(config, "head")
        with engine.begin() as connection:
            assert connection.scalar(text("SELECT prefer_incoming_warm_fit FROM nebius_pool_bindings WHERE pool_id=:pool"), {"pool": pool}) is False
            connection.execute(text("UPDATE nebius_pool_bindings SET prefer_incoming_warm_fit=true WHERE pool_id=:pool"), {"pool": pool})
    finally:
        engine.dispose()


@pytest.mark.parametrize("enabled", [False, True])
def test_current_cold_reader_and_namespace_refresh_work_on_actual_0186_schema(isolated_migration_postgres_url, enabled):
    import asyncio
    from datetime import UTC, datetime, timedelta

    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from loom.db.schema import Trial
    from loom_control_plane.incoming_capacity_preview import runtime_projection_available
    from loom_control_plane.service_execution import refresh_execution_target_health
    from loom_control_plane.service_execution_scheduler import reserve_next_service_execution
    from tests.execution_placement_fixtures import placement_fixture
    from tests.integration.test_execution_capacity_placement import _record
    from tests.integration.test_service_execution_leases import (
        _configure_scheduler_trial,
        _seed_ready_trial,
    )
    from tests.support.execution_image_admission import IMAGE_ADMISSION_KEYRING

    config = Config("database/migrations/alembic.ini")
    config.set_main_option("sqlalchemy.url", isolated_migration_postgres_url.replace("%", "%%"))
    command.downgrade(config, "0186")
    _allow_current_cancellation_columns(isolated_migration_postgres_url)

    async def legacy_path():
        engine = create_async_engine(isolated_migration_postgres_url)
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        now = datetime.now(UTC)
        try:
            async with sessions.begin() as session:
                assert not await runtime_projection_available(session)
                trial, cold = await _seed_ready_trial(session, now=now)
                unused, warm = await _seed_ready_trial(session, now=now)
                (await session.get(Trial, unused)).cancellation_requested_at = now
                await _configure_scheduler_trial(session, trial_id=trial, now=now)
                for target, count in ((cold, 0), (warm, 1)):
                    await _record(session, target.target_id, now + timedelta(seconds=1),
                        placement_fixture(target_id=target.target_id, nodes=count, used_nodes=count, quota_nodes=2))
                    # A new actor on a retained old namespace skips only its
                    # unavailable optional projection; normal health stays live.
                    await refresh_execution_target_health(session, target_id=target.target_id,
                        observed_at=now + timedelta(seconds=1), preview_runtime={
                            "namespace_name": target.namespace_name, "runtime_class_name": None, "node_selector": {}})
            async with sessions.begin() as session:
                lease = await reserve_next_service_execution(session, environment=cold.environment,
                    pool_id=cold.logical_pool_id, image_admission_keyring=IMAGE_ADMISSION_KEYRING,
                    placement_policy="environment-local-capacity-balanced", prefer_incoming_warm_fit=enabled,
                    now=now + timedelta(seconds=2))
                assert lease is not None
        finally:
            await engine.dispose()

    asyncio.run(legacy_path())


def test_current_global_default_path_uses_actual_0186_manager_without_new_state(isolated_migration_postgres_url, tmp_path):
    import asyncio

    import httpx
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from loom_execution_actuator.pool_execution_driver import PoolExecutionDriver
    from loom_execution_actuator.pool_execution_selection import PoolExecutionSelector
    from tests.integration.test_nebius_pool_execution_activation import selected
    from tests.integration.test_nebius_pool_execution_controller import another_trial
    from tests.integration.test_nebius_pool_participant_http import client

    config = Config("database/migrations/alembic.ini")
    config.set_main_option("sqlalchemy.url", isolated_migration_postgres_url.replace("%", "%%"))
    command.downgrade(config, "0186")
    _allow_current_cancellation_columns(isolated_migration_postgres_url)

    async def legacy_global_path():
        engine = create_async_engine(isolated_migration_postgres_url)
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        try:
            journal, original, _, app, token = await selected(sessions, tmp_path)
            await another_trial(sessions, original)
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app)) as http:
                management = client(http, token)
                work = await PoolExecutionSelector(outbox=journal, allocation_reader=management).select_next()
                assert work is not None
                assert (await PoolExecutionDriver(outbox=journal, management=management).advance(work.request.key)).phase == "active"
        finally:
            await engine.dispose()

    asyncio.run(legacy_global_path())
