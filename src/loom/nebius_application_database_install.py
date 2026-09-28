"""Fixed shared SQL setup for the protected installer, never personal API startup."""
from __future__ import annotations

from uuid import UUID

import psycopg
from psycopg import sql
from sqlalchemy.engine import make_url

from loom.nebius_application_database import (
    _PASSWORD,
    _REVISION,
    install_application_database_access,
)
from loom.nebius_application_schema import APPLICATION_SCHEMA_LOCK


class ApplicationDatabaseInstallError(RuntimeError):
    """Setup failure without credentials, connection routes or SQL diagnostics."""


def install_shared_manager(admin_url: str, *, data_environment_id: UUID,
                           schema_revision: str, manager_password: str) -> str:
    """Create only a missing login; retain its identity and verifier on retries.

    The caller retains the password before starting this operation and supplies
    the qualified shared-development administrator route. Schema coordination is
    shared with normal migrations; this operation never runs those migrations.
    """
    try:
        if (not isinstance(data_environment_id, UUID) or not data_environment_id.int
                or _REVISION.fullmatch(schema_revision) is None or _PASSWORD.fullmatch(manager_password) is None):
            raise ValueError
        role = 'loom_app_manager_' + data_environment_id.hex
        manager_url = make_url(admin_url).set(username=role, password=manager_password).render_as_string(hide_password=False)
        options = '-c statement_timeout=30000 -c lock_timeout=10000'
        with psycopg.connect(admin_url, autocommit=True, connect_timeout=10, options=options) as admin:
            if admin.execute('SELECT current_user=session_user AND rolsuper FROM pg_catalog.pg_roles WHERE rolname=current_user').fetchone() != (True,):
                raise ValueError
            # The session lock spans login creation, authentication and routine
            # installation. Its nested transaction locks are reentrant; closing
            # this private connection always releases it, including on failure.
            if admin.execute('SELECT pg_catalog.pg_try_advisory_lock(%s)', (APPLICATION_SCHEMA_LOCK,)).fetchone() != (True,):
                raise ValueError
            if admin.execute('SELECT version_num FROM public.alembic_version').fetchall() != [(schema_revision,)]:
                raise ValueError
            existing = admin.execute('SELECT oid FROM pg_catalog.pg_roles WHERE rolname=%s', (role,)).fetchone()
            if existing is None:
                if admin.execute("SELECT pg_catalog.to_regnamespace('loom_application_access')").fetchone() != (None,):
                    raise ValueError  # Never substitute a new OID for a lost bound login.
                admin.execute(sql.SQL('CREATE ROLE {} LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD {}').format(
                    sql.Identifier(role), sql.Literal(manager_password)))
            # A pre-existing role is usable only with the retained credential.
            # Do not rotate/adopt it by ALTER ROLE after an uncertain setup.
            with psycopg.connect(manager_url, autocommit=True, connect_timeout=10, options=options) as manager:
                if manager.execute('SELECT current_user,session_user,current_database()').fetchone() != (role, role, admin.info.dbname):
                    raise ValueError
            install_application_database_access(admin, data_environment_id=data_environment_id, manager_role=role)
        return role
    except Exception:
        raise ApplicationDatabaseInstallError('application_database_setup_failed') from None
