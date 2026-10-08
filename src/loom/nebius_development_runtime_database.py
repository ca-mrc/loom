"""Fixed fresh-dev runtime SQL setup, never a migration or admission opener.

The protected parent qualifies the independent dev database and retains material
before invoking this command. It must still deliver/qualify stopped workloads;
this receipt alone is not authority to activate a worker or the physical pool.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
from pathlib import Path
from typing import Any
from uuid import UUID

import psycopg
from psycopg import sql
from sqlalchemy.engine import make_url

from loom.nebius_application_database import _PASSWORD
from loom.nebius_application_schema import APPLICATION_SCHEMA_LOCK
from loom.nebius_platform_bootstrap import (
    ACTUATOR_POLICY_UPDATES,
    ACTUATOR_TABLES,
    ACTUATOR_TASK_IMAGE_WRITES,
    database_url,
)

_ROLE = 'loom_actuator'
_REVISION = '0174'
_OPTIONS = '-c statement_timeout=30000 -c lock_timeout=10000 -c search_path=pg_catalog,public,pg_temp'
_TABLE_PRIVILEGES = ('SELECT', 'INSERT', 'UPDATE', 'DELETE', 'TRUNCATE', 'REFERENCES', 'TRIGGER')
_COLUMN_PRIVILEGES = ('SELECT', 'INSERT', 'UPDATE', 'REFERENCES')
_MUTABLE = (
    'trials', 'execution_targets', 'execution_leases', 'execution_commands',
    'execution_events', 'execution_lease_history', 'execution_provisioning_authorizations',
    'execution_admission_reservations', 'execution_cost_reservations', 'execution_cost_reservation_debits',
)


class DevelopmentRuntimeDatabaseError(RuntimeError):
    """No protected URL, password, token or arbitrary SQL diagnostic is exposed."""


def _authority() -> tuple[dict[str, set[str]], dict[str, dict[str, set[str]]]]:
    tables = {name: {'SELECT'} for name in ACTUATOR_TABLES}
    for name in _MUTABLE:
        tables[name].update(('INSERT', 'UPDATE', 'DELETE'))
    for name, privileges in ACTUATOR_TASK_IMAGE_WRITES.items():
        tables[name].update(privileges)
    tables['trial_resource_usage'].update(('INSERT', 'UPDATE'))
    for name in ('nebius_pool_execution_outbox', 'nebius_pool_build_outbox'):
        tables[name] = {'SELECT', 'INSERT', 'UPDATE'}
    for name in ('tasks', 'task_bundle_sources', 'task_bundle_source_incarnations', 'task_bundle_source_references'):
        tables[name] = {'SELECT'}
    tables['task_bundle_source_references'].add('INSERT')
    columns = {name: {'UPDATE': set(values)} for name, values in ACTUATOR_POLICY_UPDATES.items()}
    columns.update({
        'artifacts': {'UPDATE': {'metadata'}},
        'tasks': {'UPDATE': {'registered_at'}},
        'batches': {'UPDATE': {'pool_origin'}},
        'task_bundle_sources': {'UPDATE': {'created_at'}},
        'team_quotas': {'SELECT': {'team_id', 'in_flight_count', 'max_attempts_ceiling'}, 'UPDATE': {'in_flight_count'}},
    })
    return tables, columns


def _sequences(db: psycopg.Connection[Any]) -> set[str]:
    # Match the published actuator's sequence inventory, not arbitrary future
    # tables. Global outboxes/source references have no serial-column grant.
    return {row[0] for row in db.execute(
        "SELECT pg_get_serial_sequence(format('%%I.%%I',table_schema,table_name),column_name) "
        "FROM information_schema.columns WHERE table_schema='public' AND table_name=ANY(%s)",
        (list(ACTUATOR_TABLES),)).fetchall() if row[0] is not None}


def _grant(db: psycopg.Connection[Any]) -> None:
    tables, columns = _authority()
    db.execute('GRANT CONNECT ON DATABASE loom TO loom_actuator')
    db.execute('GRANT USAGE ON SCHEMA public TO loom_actuator')
    for table, privileges in tables.items():
        db.execute(sql.SQL('GRANT {} ON public.{} TO loom_actuator').format(
            sql.SQL(',').join(map(sql.SQL, sorted(privileges))), sql.Identifier(table)))
    for table, column_grants in columns.items():
        for privilege, names in column_grants.items():
            db.execute(sql.SQL('GRANT {} ({}) ON public.{} TO loom_actuator').format(
                sql.SQL(privilege), sql.SQL(',').join(map(sql.Identifier, sorted(names))), sql.Identifier(table)))
    for sequence in _sequences(db):
        # Names originate in pg_get_serial_sequence for the fixed public inventory.
        db.execute(sql.SQL('GRANT USAGE,SELECT ON SEQUENCE {} TO loom_actuator').format(
            sql.Identifier(*sequence.split('.'))))


def _qualify(db: psycopg.Connection[Any], marker: str, token_hash: bytes) -> int:
    role = db.execute("SELECT oid,rolcanlogin,rolsuper,rolcreatedb,rolcreaterole,rolreplication,rolbypassrls,"
        "rolinherit,rolconnlimit,rolvaliduntil,rolconfig,shobj_description(oid,'pg_authid') "
        "FROM pg_roles WHERE rolname='loom_actuator'").fetchone()
    if role is None or role[1:] != (True, False, False, False, False, False, False, -1, None, None, marker):
        raise ValueError
    oid = int(role[0])
    if (db.execute('SELECT 1 FROM pg_auth_members WHERE member=%s OR roleid=%s', (oid, oid)).fetchone()
            or db.execute('SELECT 1 FROM pg_db_role_setting WHERE setrole=%s', (oid,)).fetchone()
            or db.execute("SELECT 1 FROM pg_shdepend WHERE refclassid='pg_authid'::regclass "
                "AND refobjid=%s AND deptype='o'", (oid,)).fetchone()
            or db.execute("SELECT has_database_privilege(%s,current_database(),'CREATE') "
                "OR has_schema_privilege(%s,'public','CREATE')", (oid, oid)).fetchone() != (False,)):
        raise ValueError
    if db.execute("SELECT has_database_privilege(%s,current_database(),'CONNECT'),"
            "has_schema_privilege(%s,'public','USAGE')", (oid, oid)).fetchone() != (True, True):
        raise ValueError
    if (db.execute("SELECT 1 FROM pg_default_acl d CROSS JOIN LATERAL aclexplode(d.defaclacl) a "
            "WHERE a.grantee=%s LIMIT 1", (oid,)).fetchone()
            or db.execute("SELECT 1 FROM ("
                "SELECT relacl AS acl FROM pg_class WHERE relnamespace='public'::regnamespace UNION ALL "
                "SELECT a.attacl FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid "
                "WHERE c.relnamespace='public'::regnamespace UNION ALL "
                "SELECT nspacl FROM pg_namespace WHERE nspname='public' UNION ALL "
                "SELECT datacl FROM pg_database WHERE datname=current_database()"
                ") x CROSS JOIN LATERAL aclexplode(x.acl) a "
                "WHERE a.grantee IN (0,%s) AND a.is_grantable LIMIT 1", (oid,)).fetchone()):
        raise ValueError
    tables, columns = _authority()
    actual_tables = {(table, privilege) for table, privilege in db.execute(
        "SELECT c.relname,p FROM pg_class c CROSS JOIN unnest(%s::text[]) p "
        "WHERE c.relnamespace='public'::regnamespace AND c.relkind IN ('r','p','v','m','f') "
        "AND has_table_privilege(%s,c.oid,p)", (list(_TABLE_PRIVILEGES), oid)).fetchall()}
    if actual_tables != {(table, privilege) for table, values in tables.items() for privilege in values}:
        raise ValueError
    for table, column, privilege, allowed in db.execute(
            "SELECT c.relname,a.attname,p,has_column_privilege(%s,c.oid,a.attnum,p) "
            "FROM pg_class c JOIN pg_attribute a ON a.attrelid=c.oid CROSS JOIN unnest(%s::text[]) p "
            "WHERE c.relnamespace='public'::regnamespace AND c.relkind IN ('r','p','v','m','f') "
            "AND a.attnum>0 AND NOT a.attisdropped", (oid, list(_COLUMN_PRIVILEGES))).fetchall():
        expected = privilege in tables.get(table, set()) or column in columns.get(table, {}).get(privilege, set())
        if allowed != expected:
            raise ValueError
    sequences = _sequences(db)
    for name, privilege, allowed in db.execute(
            "SELECT format('%%I.%%I',n.nspname,c.relname),p,has_sequence_privilege(%s,c.oid,p) "
            "FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
            "CROSS JOIN unnest(ARRAY['USAGE','SELECT','UPDATE']) p "
            "WHERE n.nspname='public' AND c.relkind='S'", (oid,)).fetchall():
        if allowed != (name in sequences and privilege in {'USAGE', 'SELECT'}):
            raise ValueError
    if db.execute("SELECT type,scopes,team_id,expires_at,revoked_at FROM tokens WHERE token_hash=%s",
            (token_hash,)).fetchone() != ('worker', ['submit:batch'], None, None, None):
        raise ValueError
    return oid


def install_runtime_database(admin_url: str, *, operation_id: UUID, schema_revision: str,
        actuator_password: str, batch_runner_token: str) -> dict[str, str | int]:
    """Install once; replay authenticates and observes, never rotates or repairs."""
    try:
        if (not isinstance(operation_id, UUID) or not operation_id.int or schema_revision != _REVISION
                or _PASSWORD.fullmatch(actuator_password) is None
                or re.fullmatch(r'loom_br_[A-Za-z0-9_-]{32,128}', batch_runner_token) is None):
            raise ValueError
        token_hash = hashlib.sha256(batch_runner_token.encode()).digest()
        with psycopg.connect(admin_url, autocommit=True, connect_timeout=10, options=_OPTIONS) as db:
            if db.execute("SELECT current_user=session_user AND rolsuper AND current_database()='loom' "
                    "FROM pg_roles WHERE rolname=current_user").fetchone() != (True,):
                raise ValueError
            if db.execute('SELECT pg_try_advisory_lock(%s)', (APPLICATION_SCHEMA_LOCK,)).fetchone() != (True,):
                raise ValueError
            if db.execute('SELECT version_num FROM public.alembic_version').fetchall() != [(schema_revision,)]:
                raise ValueError
            identity = db.execute('SELECT oid,(SELECT system_identifier::text FROM pg_control_system()) '
                'FROM pg_database WHERE datname=current_database()').fetchone()
            if identity is None:
                raise ValueError
            marker = json.dumps({'kind': 'loom.development-runtime-database.v1', 'operation_id': str(operation_id),
                'database_oid': identity[0], 'system_identifier': identity[1], 'schema_revision': schema_revision,
                'token_sha256': token_hash.hex()}, sort_keys=True, separators=(',', ':'))
            with db.transaction():
                if db.execute("SELECT 1 FROM pg_roles WHERE rolname='loom_actuator'").fetchone() is None:
                    if db.execute("SELECT EXISTS(SELECT 1 FROM execution_targets) OR "
                            "EXISTS(SELECT 1 FROM nebius_pool_execution_outbox) OR "
                            "EXISTS(SELECT 1 FROM nebius_pool_build_outbox) OR "
                            "EXISTS(SELECT 1 FROM tokens WHERE type='worker')").fetchone() != (False,):
                        raise ValueError
                    db.execute(sql.SQL('CREATE ROLE loom_actuator LOGIN NOINHERIT NOSUPERUSER NOCREATEDB '
                        'NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD {}').format(sql.Literal(actuator_password)))
                    _grant(db)
                    db.execute("INSERT INTO tokens (token_hash,type,scopes,team_id,issued_at,expires_at) "
                        "VALUES (%s,'worker',ARRAY['submit:batch'],NULL,now(),NULL)", (token_hash,))
                    db.execute(sql.SQL('COMMENT ON ROLE loom_actuator IS {}').format(sql.Literal(marker)))
                oid = _qualify(db, marker, token_hash)
            actuator_url = make_url(admin_url).set(username=_ROLE, password=actuator_password).render_as_string(hide_password=False)
            with psycopg.connect(actuator_url, autocommit=True, connect_timeout=10, options=_OPTIONS) as actuator:
                if actuator.execute('SELECT current_user,session_user,current_database()').fetchone() != (_ROLE, _ROLE, 'loom'):
                    raise ValueError
            # Hold the schema coordination lock across authentication and final
            # readback. A committed-but-unreported first call is safely replayed.
            if _qualify(db, marker, token_hash) != oid:
                raise ValueError
        return {'operation_id': str(operation_id), 'role': _ROLE, 'role_oid': oid, 'token_sha256': token_hash.hex()}
    except Exception:
        raise DevelopmentRuntimeDatabaseError('development_runtime_database_unqualified') from None


def main() -> int:
    try:
        with Path(os.environ['LOOM_DEVELOPMENT_RUNTIME_CONFIG']).open('rb') as stream:
            raw = stream.read(16385)
        if not 0 < len(raw) <= 16384:
            raise ValueError
        config = json.loads(raw)
        if (not isinstance(config, dict) or set(config) != {'namespace', 'operation_id', 'schema_revision'}
                or config['namespace'] != 'loom-dev'):
            raise ValueError
        receipt = install_runtime_database(database_url(os.environ['LOOM_DB_URL'], 'loom-dev'),
            operation_id=UUID(config['operation_id']), schema_revision=config['schema_revision'],
            actuator_password=os.environ['LOOM_DB_ACTUATOR_PASSWORD'],
            batch_runner_token=os.environ['LOOM_BATCH_RUNNER_TOKEN'])
    except Exception:
        print(json.dumps({'status': 'development_runtime_database_unqualified'}))
        return 1
    print(json.dumps({'status': 'development_runtime_database_installed', **receipt}, sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
