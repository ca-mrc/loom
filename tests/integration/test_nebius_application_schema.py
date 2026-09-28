"""Shared schema fencing using real PostgreSQL and the actual Alembic entrypoint."""
from __future__ import annotations

from pathlib import Path
from secrets import token_urlsafe
from uuid import uuid4

import psycopg
import pytest
from alembic import command
from alembic.config import Config
from psycopg import sql
from sqlalchemy import create_engine, pool
from sqlalchemy.engine import make_url

from loom.nebius_application_database import (
    ApplicationDatabaseAccess,
    ApplicationDatabaseAccessError,
    install_application_database_access,
)
from tests.integration.test_nebius_application_database import access_postgres as access_postgres
from tests.integration.test_nebius_application_database import database_access as database_access
from tests.integration.test_nebius_application_database import login


@pytest.fixture
def migration_access(isolated_migration_postgres_url):
    url = make_url(isolated_migration_postgres_url).set(drivername="postgresql")
    manager, password, data_id = "mgr_" + uuid4().hex, token_urlsafe(48), uuid4()
    manager_url = url.set(username=manager, password=password).render_as_string(hide_password=False)
    root = Path(__file__).resolve().parents[2]
    config = Config(str(root / "database/migrations/alembic.ini"))
    config.set_main_option("script_location", str(root / "database/migrations"))
    config.set_main_option("sqlalchemy.url", isolated_migration_postgres_url.replace("%", "%%"))
    with psycopg.connect(url.render_as_string(hide_password=False), autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE ROLE {} LOGIN NOINHERIT PASSWORD {}").format(
            sql.Identifier(manager), sql.Literal(password)))
        install_application_database_access(admin, data_environment_id=data_id, manager_role=manager)
        with psycopg.connect(manager_url, autocommit=True) as manager_connection:
            yield admin, manager_url, ApplicationDatabaseAccess(manager_connection, data_id), config


@pytest.mark.parametrize("change", ["downgrade", "stamp", "purge"])
def test_alembic_refuses_changes_until_personal_access_is_drained(migration_access, change):
    admin, url, access, config = migration_access
    app, incarnation, password = uuid4(), uuid4(), token_urlsafe(48)
    role = access.grant(app, incarnation, 1, password, schema_revision="0166")

    def migrate():
        if change == "downgrade":
            command.downgrade(config, "0165")
        elif change == "stamp":
            command.stamp(config, "0165")
        else:
            command.stamp(config, "head", purge=True)

    # No connected personal sessions is not enough: outstanding credentials can
    # establish one later. Also keep legitimate no-op/diagnostic commands working.
    command.upgrade(config, "head")
    command.current(config)
    with pytest.raises(RuntimeError, match="application_database_access_active"):
        migrate()
    assert admin.execute("SELECT version_num FROM public.alembic_version").fetchone() == ("0166",)
    with login(url, role, password) as client:
        access.revoke(app, incarnation, 1)
        # NOLOGIN does not terminate an existing session.
        assert client.execute("SELECT 1").fetchone() == (1,)
        with pytest.raises(RuntimeError, match="application_database_access_active"):
            migrate()
        assert access.drain(app, incarnation, 1)
        migrate()
    expected = "0166" if change == "purge" else "0165"
    assert admin.execute("SELECT version_num FROM public.alembic_version").fetchone() == (expected,)


def engine_for(admin):
    # psycopg's DSN is key/value, not a SQLAlchemy URL; keep its exact connection.
    return create_engine("postgresql+psycopg://", creator=lambda: psycopg.connect(admin.info.dsn), poolclass=pool.NullPool)


def test_schema_lock_survives_commit_and_blocks_grants_until_physical_close(database_access):
    from loom.nebius_application_schema import guard_shared_application_schema

    admin, _, access, _ = database_access
    engine = engine_for(admin)
    try:
        with engine.connect() as migration:
            guard_shared_application_schema(migration, target_revisions=("test_revision",))
            migration.exec_driver_sql("UPDATE public.alembic_version SET version_num='next_revision'")
            migration.commit()
            access.connection.execute("SET statement_timeout='200ms'")
            with pytest.raises(ApplicationDatabaseAccessError, match="operation_failed"):
                access.grant(uuid4(), uuid4(), 1, token_urlsafe(48), schema_revision="next_revision")
            assert admin.execute("SELECT count(*) FROM loom_application_access.generations").fetchone() == (0,)
        with pytest.raises(ApplicationDatabaseAccessError, match="schema_mismatch"):
            access.grant(uuid4(), uuid4(), 1, token_urlsafe(48), schema_revision="test_revision")
        assert access.grant(uuid4(), uuid4(), 1, token_urlsafe(48), schema_revision="next_revision")
    finally:
        engine.dispose()


def test_inflight_grant_blocks_migration_then_committed_grant_blocks_change(database_access):
    from loom.nebius_application_schema import guard_shared_application_schema

    admin, _, access, data_id = database_access
    engine = engine_for(admin)
    try:
        with access.connection.transaction():
            access.connection.execute("SELECT loom_application_access.grant_access_at_schema(%s,%s,%s,1,%s,%s)",
                                      (data_id, uuid4(), uuid4(), token_urlsafe(48), "test_revision"))
            with engine.connect() as migration:
                with pytest.raises(RuntimeError, match="application_database_schema_busy"):
                    guard_shared_application_schema(migration, target_revisions=("next_revision",))
        with engine.connect() as migration:
            with pytest.raises(RuntimeError, match="application_database_access_active"):
                guard_shared_application_schema(migration, target_revisions=("next_revision",))
    finally:
        engine.dispose()


def test_first_installation_cannot_race_migration_before_private_schema_exists(database_access):
    from loom.nebius_application_schema import guard_shared_application_schema

    admin, url, _, data_id = database_access
    admin.execute("DROP SCHEMA loom_application_access CASCADE")
    engine = engine_for(admin)
    try:
        with engine.connect() as migration:
            guard_shared_application_schema(migration, target_revisions=("next_revision",))
            with pytest.raises(ApplicationDatabaseAccessError, match="application_database_schema_busy"):
                install_application_database_access(admin, data_environment_id=data_id, manager_role=make_url(url).username)
    finally:
        engine.dispose()
