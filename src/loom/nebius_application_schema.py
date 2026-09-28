"""Database-local coordination between personal access and shared migrations."""

from __future__ import annotations

from sqlalchemy import Connection, text

# PostgreSQL advisory locks are local to a database. Keep this key stable across
# releases and before installation of the private access-management schema.
APPLICATION_SCHEMA_LOCK = 0x4C4F4F4D415050  # LOOMAPP


def guard_shared_application_schema(
    connection: Connection, *, changing_schema: bool,
) -> None:
    """Fence admission for the lifetime of this physical migration connection.

    The caller must use a disposable (NullPool) direct PostgreSQL connection,
    including on error. A session lock, unlike a transaction lock, survives
    migration commits. Call before planning and again with changing_schema=True
    before any nonempty migration plan (or version-table purge) is executed.
    Reentrant session locks are all released by closing the physical connection.
    """
    if connection.exec_driver_sql("SHOW transaction_isolation").scalar_one() != "read committed":
        raise RuntimeError("application_database_isolation")
    if connection.execute(text("SELECT pg_catalog.pg_try_advisory_lock(:key)"),
                          {"key": APPLICATION_SCHEMA_LOCK}).scalar_one() is not True:
        raise RuntimeError("application_database_schema_busy")
    if connection.exec_driver_sql("SELECT pg_catalog.to_regnamespace('loom_application_access')").scalar_one() is None:
        return
    # Alembic can create its missing version table before calling its plan
    # function (including ensure_version, which returns no migration steps).
    missing_version = connection.exec_driver_sql("SELECT pg_catalog.to_regclass('public.alembic_version')").scalar_one() is None
    if not changing_schema and not missing_version:
        return
    if connection.exec_driver_sql("SELECT pg_catalog.to_regprocedure('loom_application_access.migration_ready()')").scalar_one() is None:
        raise RuntimeError("application_database_schema_guard_not_installed")
    if connection.exec_driver_sql("SELECT loom_application_access.migration_ready()").scalar_one() is not True:
        raise RuntimeError("application_database_access_active")
