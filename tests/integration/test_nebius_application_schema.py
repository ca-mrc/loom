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

from loom.db.schema_startup import service_schema_head
from loom.nebius_application_database import (
    ApplicationDatabaseAccess,
    ApplicationDatabaseAccessError,
    install_application_database_access,
)
from tests.integration.test_nebius_application_database import (
    access_postgres as access_postgres,
)
from tests.integration.test_nebius_application_database import database_access as database_access
from tests.integration.test_nebius_application_database import login

_HEAD = service_schema_head()


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


@pytest.mark.parametrize("change", ["downgrade", "stamp", "purge", "multi_stamp"])
def test_alembic_refuses_changes_until_personal_access_is_drained(migration_access, change):
    admin, url, access, config = migration_access
    app, incarnation, password = uuid4(), uuid4(), token_urlsafe(48)
    role = access.grant(app, incarnation, 1, password, schema_revision=_HEAD)

    def migrate():
        if change == "downgrade":
            command.downgrade(config, "0165")
        elif change == "stamp":
            command.stamp(config, "0165")
        elif change == "multi_stamp":
            command.stamp(config, "0165")
        else:
            command.stamp(config, "head", purge=True)

    # No connected personal sessions is not enough: outstanding credentials can
    # establish one later. Also keep legitimate no-op/diagnostic commands working.
    command.upgrade(config, "head")
    command.current(config)
    with pytest.raises(RuntimeError, match="application_database_access_active"):
        migrate()
    assert admin.execute("SELECT version_num FROM public.alembic_version").fetchone() == (_HEAD,)
    with login(url, role, password) as client:
        access.revoke(app, incarnation, 1)
        # NOLOGIN does not terminate an existing session.
        assert client.execute("SELECT 1").fetchone() == (1,)
        with pytest.raises(RuntimeError, match="application_database_access_active"):
            migrate()
        assert access.drain(app, incarnation, 1)
        migrate()
    expected = _HEAD if change == "purge" else "0165"
    assert admin.execute("SELECT version_num FROM public.alembic_version").fetchone() == (expected,)


@pytest.mark.parametrize("noop", ["stamp", "relative"])
def test_actual_noop_migration_plan_preserves_active_access(migration_access, noop):
    admin, url, access, config = migration_access
    password = token_urlsafe(48)
    role = access.grant(uuid4(), uuid4(), 1, password, schema_revision=_HEAD)
    if noop == "stamp":
        command.stamp(config, "head")
    else:
        command.upgrade(config, _HEAD + "+0")
    assert admin.execute("SELECT version_num FROM public.alembic_version").fetchone() == (_HEAD,)
    with login(url, role, password) as client:
        assert client.execute("SELECT version_num FROM public.alembic_version").fetchone() == (_HEAD,)


def engine_for(admin):
    # psycopg's DSN deliberately omits the password. Reuse both parts of the
    # disposable fixture connection without logging its credential.
    return create_engine("postgresql+psycopg://", creator=lambda: psycopg.connect(
        admin.info.dsn, password=admin.info.password), poolclass=pool.NullPool)


def test_schema_lock_survives_commit_and_blocks_grants_until_physical_close(database_access):
    from loom.nebius_application_schema import guard_shared_application_schema

    admin, _, access, _ = database_access
    engine = engine_for(admin)
    try:
        with engine.connect() as migration:
            guard_shared_application_schema(migration, changing_schema=True)
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
                    guard_shared_application_schema(migration, changing_schema=True)
        with engine.connect() as migration:
            with pytest.raises(RuntimeError, match="application_database_access_active"):
                guard_shared_application_schema(migration, changing_schema=True)
    finally:
        engine.dispose()


def test_first_installation_cannot_race_migration_before_private_schema_exists(database_access):
    from loom.nebius_application_schema import guard_shared_application_schema

    admin, url, _, data_id = database_access
    admin.execute("DROP SCHEMA loom_application_access CASCADE")
    engine = engine_for(admin)
    try:
        with engine.connect() as migration:
            guard_shared_application_schema(migration, changing_schema=True)
            with pytest.raises(ApplicationDatabaseAccessError, match="application_database_schema_busy"):
                install_application_database_access(admin, data_environment_id=data_id, manager_role=make_url(url).username)
    finally:
        engine.dispose()


def test_database_owner_can_check_quiescence_without_private_table_access(database_access):
    from loom.nebius_application_schema import guard_shared_application_schema

    admin, url, access, data_id = database_access
    owner = "migration_owner_" + uuid4().hex
    admin.execute(sql.SQL("CREATE ROLE {} NOLOGIN NOINHERIT").format(sql.Identifier(owner)))
    admin.execute(sql.SQL("ALTER DATABASE {} OWNER TO {}").format(sql.Identifier(admin.info.dbname), sql.Identifier(owner)))
    admin.execute(sql.SQL("GRANT SELECT ON public.alembic_version TO {}").format(sql.Identifier(owner)))
    install_application_database_access(admin, data_environment_id=data_id, manager_role=make_url(url).username)
    app, incarnation = uuid4(), uuid4()
    access.grant(app, incarnation, 1, token_urlsafe(48), schema_revision="test_revision")
    with pytest.raises(psycopg.errors.InsufficientPrivilege):
        access.connection.execute("SELECT loom_application_access.migration_ready()")
    with psycopg.connect(admin.info.dsn, password=admin.info.password, autocommit=True) as restricted:
        restricted.execute(sql.SQL("SET ROLE {}").format(sql.Identifier(owner)))
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            restricted.execute("SELECT * FROM loom_application_access.generations")
        assert restricted.execute("SELECT loom_application_access.migration_ready()").fetchone() == (False,)
    engine = engine_for(admin)
    try:
        with engine.connect() as migration:
            migration.exec_driver_sql('SET ROLE "' + owner + '"')
            with pytest.raises(RuntimeError, match="application_database_access_active"):
                guard_shared_application_schema(migration, changing_schema=True)
        access.revoke(app, incarnation, 1)
        assert access.drain(app, incarnation, 1)
        with engine.connect() as migration:
            migration.exec_driver_sql('SET ROLE "' + owner + '"')
            guard_shared_application_schema(migration, changing_schema=True)
    finally:
        engine.dispose()


def test_failed_migration_releases_lock_without_changing_schema(database_access):
    from loom.nebius_application_schema import guard_shared_application_schema

    admin, _, access, _ = database_access
    engine = engine_for(admin)
    try:
        with pytest.raises(RuntimeError, match="injected migration failure"):
            with engine.connect() as migration, migration.begin():
                guard_shared_application_schema(migration, changing_schema=True)
                migration.exec_driver_sql("UPDATE public.alembic_version SET version_num='next_revision'")
                raise RuntimeError("injected migration failure")
        assert admin.execute("SELECT version_num FROM public.alembic_version").fetchone() == ("test_revision",)
        assert access.grant(uuid4(), uuid4(), 1, token_urlsafe(48), schema_revision="test_revision")
    finally:
        engine.dispose()


def test_migration_refuses_stale_transaction_snapshot(database_access):
    from loom.nebius_application_schema import guard_shared_application_schema

    admin, _, _, _ = database_access
    engine = engine_for(admin)
    try:
        with engine.connect().execution_options(isolation_level="REPEATABLE READ") as migration:
            with pytest.raises(RuntimeError, match="application_database_isolation"):
                guard_shared_application_schema(migration, changing_schema=True)
    finally:
        engine.dispose()


def test_version_table_recreation_is_a_schema_change(database_access):
    admin, url, access, _ = database_access
    access.grant(uuid4(), uuid4(), 1, token_urlsafe(48), schema_revision="test_revision")
    admin.execute("DROP TABLE public.alembic_version")
    root = Path(__file__).resolve().parents[2]
    config = Config(str(root / "database/migrations/alembic.ini"))
    config.set_main_option("script_location", str(root / "database/migrations"))
    admin_url = make_url(url).set(drivername="postgresql+psycopg", username=admin.info.user, password=admin.info.password)
    config.set_main_option("sqlalchemy.url", admin_url.render_as_string(hide_password=False).replace("%", "%%"))
    with pytest.raises(RuntimeError, match="application_database_access_active"):
        command.ensure_version(config)
    assert admin.execute("SELECT to_regclass('public.alembic_version')").fetchone() == (None,)
