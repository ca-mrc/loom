"""Alembic environment — reads DB URL from LOOM_DB_URL env var."""

from __future__ import annotations

import os
import re
from logging.config import fileConfig
from typing import Any
from uuid import uuid4

from alembic import context
from alembic.runtime.migration import MigrationContext
from sqlalchemy import Connection, engine_from_config, pool, text

from loom.db import schema  # noqa: F401  (registers models with Base.metadata)
from loom.db.base import Base
from loom.nebius_application_schema import guard_shared_application_schema


def _assume_application_owner(connection: Connection, owner_role: str) -> None:
    """Use an explicit sealed owner, never the application's runtime login.

    This verifies migration authority, not completed legacy-writer retirement.
    The protected provisioner still owns exact object transfer, runtime grants,
    credential delivery, session reconciliation, and post-migration sealing.
    """
    if re.fullmatch(r"[a-z][a-z0-9_]{0,62}", owner_role) is None:
        raise RuntimeError("application migration owner must be an explicit canonical SQL role")
    valid = connection.execute(
        text(
            "SELECT current_user = session_user AND session_user <> :owner "
            "AND EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = session_user "
            "AND rolcanlogin AND NOT rolinherit AND NOT rolsuper AND NOT rolcreatedb "
            "AND NOT rolcreaterole AND NOT rolreplication AND NOT rolbypassrls "
            "AND rolvaliduntil > CURRENT_TIMESTAMP "
            "AND rolvaliduntil <= CURRENT_TIMESTAMP + interval '1 hour') "
            "AND EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = :owner "
            "AND NOT rolcanlogin AND NOT rolinherit AND NOT rolsuper AND NOT rolcreatedb "
            "AND NOT rolcreaterole AND NOT rolreplication AND NOT rolbypassrls) "
            "AND (SELECT pg_catalog.pg_get_userbyid(datdba) FROM pg_catalog.pg_database "
            "WHERE datname = current_database()) = :owner"
        ),
        {"owner": owner_role},
    ).scalar_one()
    if valid is not True:
        raise RuntimeError(
            "application migration requires a sealed owner and bounded transient login"
        )
    memberships = connection.execute(
        text(
            "SELECT granted.rolname, member.rolname = session_user, "
            "m.admin_option, m.inherit_option, m.set_option "
            "FROM pg_catalog.pg_auth_members AS m "
            "JOIN pg_catalog.pg_roles AS granted ON granted.oid = m.roleid "
            "JOIN pg_catalog.pg_roles AS member ON member.oid = m.member "
            "WHERE member.rolname IN (session_user, :owner) "
            "OR granted.rolname IN (session_user, :owner)"
        ),
        {"owner": owner_role},
    ).all()
    if [tuple(row) for row in memberships] != [(owner_role, True, False, False, True)]:
        raise RuntimeError("application migration owner membership must be exclusive and non-admin")
    quoted_owner = connection.dialect.identifier_preparer.quote(owner_role)
    connection.exec_driver_sql(f"SET LOCAL ROLE {quoted_owner}")
    if connection.execute(text("SELECT current_user")).scalar_one() != owner_role:
        raise RuntimeError("application migration owner assumption failed")


def _assert_direct_postgres_connection(connectable: Any) -> None:
    """Alembic MUST run direct-to-Postgres (#609).

    Under pgbouncer transaction mode, session-scoped operations
    (SET LOCAL, autocommit-DDL, session-scoped advisory locks) silently
    break because backends are recycled between transactions. Today's
    Loom migrations work on either path, but the invariant is worth
    enforcing at every migration run so future migration authors can't
    accidentally depend on session semantics that only work on one path.

    Probe uses its OWN short-lived connection so Alembic's connection
    is untouched. SET a synthetic application_name, commit, read it
    back. Under session-preserving semantics the value persists; under
    pgbouncer transaction mode the next statement lands on a different
    backend with the default application_name.
    """
    marker = f"alembic-probe-{uuid4()}"
    with connectable.connect() as probe_conn:
        probe_conn.exec_driver_sql(f"SET application_name = '{marker}'")
        probe_conn.commit()
        actual = probe_conn.exec_driver_sql("SHOW application_name").scalar()
    if actual != marker:
        raise RuntimeError(
            f"Alembic connection is not direct-to-Postgres. "
            f"application_name did not persist across commit "
            f"(saw {actual!r}, expected {marker!r}). This means the "
            f"connection routes through pgbouncer transaction mode, "
            f"which silently breaks session-scoped operations that "
            f"Alembic migrations may depend on. "
            f"Fix: point LOOM_DB_URL at loom-postgres:5432 direct, "
            f"not loom-pgbouncer:6432."
        )


def _assert_compatible_migration_lineage(connection: Any) -> None:
    """Fail before DDL when a historical Nebius revision aliases dev history."""
    if connection.exec_driver_sql("SELECT to_regclass('public.alembic_version')").scalar() is None:
        return
    revisions = set(connection.exec_driver_sql("SELECT version_num FROM public.alembic_version").scalars())
    if revisions.intersection({"0133", "0134", "0135", "0136"}) and connection.exec_driver_sql(
        "SELECT to_regclass('public.gateway_dispatch_receipts')"
    ).scalar() is None:
        raise RuntimeError(
            "isolated Nebius migration lineage cannot upgrade through dev history; "
            "prepare and verify the documented lineage conversion before deployment"
        )


target_metadata = Base.metadata

# The block below only executes when Alembic drives this file directly.
# Importing the module in unit tests (e.g. to access _assert_direct_postgres_connection)
# does NOT trigger it, because context.config is unavailable outside an Alembic run.
if hasattr(context, "config"):
    config = context.config
    if config.config_file_name is not None:
        # disable_existing_loggers=False prevents fileConfig from disabling
        # loggers configured before alembic runs — notably pytest's caplog
        # attachment to project loggers like `loom_worker.trial_runner`.
        fileConfig(config.config_file_name, disable_existing_loggers=False)

    configured_url = config.get_main_option("sqlalchemy.url")
    db_url: str | None = configured_url or os.environ.get("LOOM_DB_URL")
    if not db_url:
        raise RuntimeError(
            "sqlalchemy.url or LOOM_DB_URL must be set to run migrations",
        )
    # ConfigParser consumes percent escapes; escape only at this INI boundary
    # so the engine receives the original URL (including credentials and TLS).
    config.set_main_option("sqlalchemy.url", db_url.replace("%", "%%"))
    owner_role = os.environ.get("LOOM_DB_OWNER_ROLE")

    def run_migrations_offline() -> None:
        if owner_role is not None:
            raise RuntimeError("application migration owner verification requires online execution")
        context.configure(
            url=db_url,
            target_metadata=target_metadata,
            literal_binds=True,
            dialect_opts={"paramstyle": "named"},
        )
        with context.begin_transaction():
            context.run_migrations()

    def run_migrations_online() -> None:
        connectable = engine_from_config(
            config.get_section(config.config_ini_section, {}),
            prefix="sqlalchemy.",
            poolclass=pool.NullPool,
        )
        try:
            _assert_direct_postgres_connection(connectable)
            with connectable.connect() as connection, connection.begin():
                if owner_role is not None:
                    _assume_application_owner(connection, owner_role)
                context.configure(connection=connection, target_metadata=target_metadata)
                options = context.get_context().opts
                plan_migrations = options["fn"]

                def guarded_steps(revisions: tuple[str, ...], migration_context: MigrationContext) -> list[Any]:
                    # Inspect every actual step, not the destination argument:
                    # Alembic's revision-argument helper truncates stamp tuples,
                    # and relative/no-op targets cannot be compared as strings.
                    steps = list(plan_migrations(revisions, migration_context))
                    if steps:
                        guard_shared_application_schema(connection, changing_schema=True)
                    return steps

                context.configure(connection=connection, target_metadata=target_metadata, fn=guarded_steps)
                with context.begin_transaction():
                    # Alembic purges the version table before invoking fn, so
                    # that path requires quiescence before run_migrations too.
                    guard_shared_application_schema(connection, changing_schema=bool(options.get("purge")))
                    _assert_compatible_migration_lineage(connection)
                    context.run_migrations()
        finally:
            connectable.dispose()

    if context.is_offline_mode():
        run_migrations_offline()
    else:
        run_migrations_online()
