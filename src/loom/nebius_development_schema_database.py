"""Fixed, credential-preserving database half of a protected dev schema upgrade.

The connected parent owns writer fencing and checkpoint qualification. This
runner accepts only a retained predecessor, an exact candidate and a checkpoint
digest. An interrupted dispatch requires explicit recovery; it is never retried
because observing the old or new Alembic revision cannot prove grant completion.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
from typing import Annotated, Any, Literal, Self
from uuid import UUID

import psycopg
from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from psycopg import sql
from psycopg.types.json import Jsonb
from pydantic import BaseModel, ConfigDict, Field, model_validator
from sqlalchemy import create_engine, pool
from sqlalchemy.engine import make_url

from loom.db.schema_startup import _default_alembic_ini, service_schema_head
from loom.nebius_application_database import install_application_database_access
from loom.nebius_application_schema import APPLICATION_SCHEMA_LOCK
from loom.nebius_development_runtime_database import _authority as actuator_authority
from loom.nebius_development_runtime_database import _qualify as qualify_actuator
from loom.nebius_development_runtime_database import _sequences as actuator_sequences
from loom.nebius_platform_bootstrap import database_url

_Digest = Annotated[str, Field(pattern=r'^[0-9a-f]{64}$')]
_OPTIONS = '-c statement_timeout=120000 -c lock_timeout=10000 -c timezone=UTC -c search_path=pg_catalog,public,pg_temp'
_SCHEMA = 'loom_development_upgrade'
_IDLE_PREDICATES = {
    'nebius_applications': "desired_state='active'",
    'nebius_application_operations': "phase NOT IN ('completed','superseded')",
    'nebius_application_build_attempts': "phase NOT IN ('ready','failed','cancelled')",
    'nebius_application_source_uploads': "phase='awaiting_source' AND expires_at>now()",
    'nebius_pool_requests': 'true',
    'nebius_pool_cancellations': 'true',
    'nebius_pool_execution_outbox': "phase NOT IN ('cancelled','released')",
    'nebius_pool_build_outbox': "phase NOT IN ('cancelled','released')",
    'execution_leases': "desired_state<>'deleted' OR cleanup_state<>'complete' OR materialization_state IN ('pending','running')",
    'task_image_materializations': "state NOT IN ('ready','failed','retired')",
    'trials': "state NOT IN ('succeeded','failed','cancelled')",
}


class DevelopmentSchemaDatabaseError(RuntimeError):
    """A closed failure category; no SQL text, URL, token or password."""


class DevelopmentSchemaSelection(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True, hide_input_in_errors=True)
    operation_id: UUID
    mode: Literal['shared', 'manager']
    namespace: Literal['loom-dev', 'loom-nebius-management-dev']
    predecessor_revision: str = Field(pattern=r'^[0-9]{4}$')
    candidate_revision: str = Field(pattern=r'^[0-9]{4}$')
    candidate_sha: str = Field(pattern=r'^[0-9a-f]{40}$')
    data_environment_id: UUID | None = None
    pool_id: UUID | None = None

    @model_validator(mode='after')
    def identity(self) -> Self:
        shared = self.mode == 'shared'
        if (not self.operation_id.int or self.namespace != ('loom-dev' if shared else 'loom-nebius-management-dev')
                or self.predecessor_revision != ('0175' if shared else '0176')
                or (self.data_environment_id is not None) != shared or (self.pool_id is not None) == shared
                or any(value is not None and not value.int for value in (self.data_environment_id, self.pool_id))):
            raise ValueError('development_schema_selection_unqualified')
        return self


class DevelopmentSchemaProbe(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True, hide_input_in_errors=True)
    schema_version: Literal['loom.development-schema-probe.v1'] = 'loom.development-schema-probe.v1'
    selection_sha256: _Digest
    revision: str = Field(pattern=r'^[0-9]{4}$')
    database_oid: int = Field(gt=0, strict=True)
    system_identifier: str = Field(pattern=r'^[0-9]{1,20}$')
    authority_sha256: _Digest
    grants_sha256: _Digest


class DevelopmentSchemaUpgrade(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True, hide_input_in_errors=True)
    selection: DevelopmentSchemaSelection
    predecessor: DevelopmentSchemaProbe
    checkpoint_sha256: _Digest

    @model_validator(mode='after')
    def predecessor_matches(self) -> Self:
        if (self.predecessor.selection_sha256 != _digest(self.selection.model_dump(mode='json'))
                or self.predecessor.revision != self.selection.predecessor_revision):
            raise ValueError('development_schema_predecessor_unqualified')
        return self


def _digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'), default=str).encode()).hexdigest()


def _candidate(selection: DevelopmentSchemaSelection) -> Config:
    ini = _default_alembic_ini()
    config = Config(str(ini))
    config.set_main_option('script_location', str(ini.parent))
    if selection.candidate_revision != service_schema_head(ini):
        raise ValueError
    migrations = list(ScriptDirectory.from_config(config).iterate_revisions(
        selection.candidate_revision, selection.predecessor_revision))
    if not migrations or migrations[-1].down_revision != selection.predecessor_revision:
        raise ValueError
    return config


def _roles(selection: DevelopmentSchemaSelection) -> list[str]:
    if selection.mode == 'manager':
        return ['loom_service']
    assert selection.data_environment_id is not None
    return ['loom_service', 'loom_control_plane', 'loom_gateway', 'loom_actuator',
        'loom_app_manager_' + selection.data_environment_id.hex, 'loom_app_runtime_' + selection.data_environment_id.hex]


def _lock(db: psycopg.Connection[Any]) -> None:
    if (db.execute('SELECT current_user=session_user AND rolsuper FROM pg_roles WHERE rolname=current_user').fetchone() != (True,)
            or db.execute('SHOW transaction_isolation').fetchone() != ('read committed',)
            or db.execute('SELECT pg_try_advisory_lock(%s)', (APPLICATION_SCHEMA_LOCK,)).fetchone() != (True,)):
        raise ValueError


def schema_idle_sql(selection: DevelopmentSchemaSelection) -> str:
    """Bounded GET-only preflight before the parent stops any runtime writer.

    Runtime sessions may exist here. This establishes empty intake and retired
    personal credentials, not the stronger migration-time absence of sessions.
    All SQL structure is source-owned; only validated literal identities vary.
    """
    selection = DevelopmentSchemaSelection.model_validate(selection.model_dump())
    _candidate(selection)
    conditions: list[sql.Composable] = [sql.SQL('(SELECT count(*)=1 AND bool_and(version_num={}) FROM public.alembic_version)').format(
        sql.Literal(selection.predecessor_revision))]
    if selection.mode == 'shared':
        conditions.extend([sql.SQL('loom_application_access.migration_ready()'),
            sql.SQL('(SELECT count(*)=1 AND bool_and(data_environment_id={}) FROM loom_application_access.binding)').format(
                sql.Literal(selection.data_environment_id))])
    else:
        conditions.append(sql.SQL("(SELECT count(*)=1 AND bool_and(pool_id={} AND mode='closed') FROM public.nebius_pool_bindings)").format(
            sql.Literal(selection.pool_id)))
    conditions.extend(sql.SQL('NOT EXISTS(SELECT 1 FROM public.{} WHERE {} LIMIT 1)').format(sql.Identifier(table), sql.SQL(predicate))
        for table, predicate in _IDLE_PREDICATES.items())
    return sql.SQL("SELECT jsonb_build_object('schema_version','loom.development-schema-idle.v1',"
        "'selection_sha256',{},'revision',(SELECT min(version_num) FROM public.alembic_version),"
        "'status',CASE WHEN {} THEN 'qualified' ELSE 'unqualified' END)").format(
            sql.Literal(_digest(selection.model_dump(mode='json'))), sql.SQL(' AND ').join(conditions)).as_string()


def _quiescent(db: psycopg.Connection[Any], selection: DevelopmentSchemaSelection) -> None:
    # A stopped Deployment is insufficient when an old client or a terminating
    # Pod still holds a connection. Include idle and unknown client identities.
    db.execute('SELECT pg_stat_clear_snapshot()')
    if db.execute("SELECT 1 FROM pg_stat_activity WHERE datid=(SELECT oid FROM pg_database "
            "WHERE datname=current_database()) AND pid<>pg_backend_pid() AND backend_type='client backend' LIMIT 1").fetchone():
        raise ValueError
    if selection.mode == 'shared':
        if db.execute('SELECT loom_application_access.migration_ready()').fetchone() != (True,):
            raise ValueError
    elif db.execute('SELECT pool_id,mode FROM nebius_pool_bindings ORDER BY pool_id').fetchall() != [(selection.pool_id, 'closed')]:
        raise ValueError
    for table, predicate in _IDLE_PREDICATES.items():
        if db.execute(sql.SQL('SELECT 1 FROM public.{} WHERE {} LIMIT 1').format(sql.Identifier(table), sql.SQL(predicate))).fetchone():
            raise ValueError


def _snapshot_sql(selection: DevelopmentSchemaSelection) -> str:
    """One source-owned snapshot shared by the runner and HTTPS readback."""
    roles = _roles(selection)
    role_array = sql.Literal(roles).as_string()
    admitted = ''
    if selection.mode == 'shared':
        admitted = (' AND NOT (roleid=' + sql.Literal(roles[-1]).as_string() + '::regrole::oid '
            'AND NOT admin_option AND inherit_option AND NOT set_option AND EXISTS('
            'SELECT 1 FROM loom_application_access.generations g WHERE g.role_oid=member AND NOT g.retired))')
    queries = {
        'revision': 'SELECT jsonb_build_array(version_num) AS row FROM public.alembic_version',
        'physical': 'SELECT jsonb_build_array(oid::bigint,(SELECT system_identifier::text FROM pg_control_system())) AS row '
            'FROM pg_database WHERE datname=current_database()',
        'identities': "SELECT jsonb_build_array(oid::bigint,rolname,rolcanlogin,rolsuper,rolcreatedb,rolcreaterole,rolreplication,rolbypassrls,"
        "rolinherit,rolconnlimit,rolvaliduntil,(SELECT rolconfig FROM pg_roles WHERE oid=pg_authid.oid),"
        "rolpassword,shobj_description(oid,'pg_authid')) AS row "
        'FROM pg_authid WHERE rolname=ANY(' + role_array + ') ORDER BY rolname',
        'forbidden': "SELECT jsonb_build_array(rolname) AS row FROM pg_roles r WHERE rolname=ANY(" + role_array + ') AND ('
            "has_database_privilege(r.oid,current_database(),'CREATE') OR has_schema_privilege(r.oid,'public','CREATE') "
            "OR EXISTS(SELECT 1 FROM pg_class WHERE relnamespace='public'::regnamespace AND relowner=r.oid) "
            "OR EXISTS(SELECT 1 FROM pg_proc WHERE pronamespace='public'::regnamespace AND proowner=r.oid) "
            "OR EXISTS(SELECT 1 FROM pg_type WHERE typnamespace='public'::regnamespace AND typowner=r.oid)) LIMIT 1",
        'binding': ('SELECT jsonb_build_array(data_environment_id,manager_role,runtime_role,manager_oid::bigint,runtime_oid::bigint,database_oid::bigint,system_identifier) AS row '
            'FROM loom_application_access.binding') if selection.mode == 'shared' else 'SELECT NULL::jsonb AS row WHERE false',
        'tokens': "SELECT jsonb_build_array(encode(token_hash,'hex'),type,scopes,team_id,expires_at,revoked_at) AS row FROM public.tokens "
            "WHERE type='worker' ORDER BY token_hash",
        'memberships': 'SELECT jsonb_build_array(roleid::bigint,member::bigint,grantor::bigint,admin_option,inherit_option,set_option) AS row '
            'FROM pg_auth_members WHERE (roleid IN (SELECT oid FROM pg_roles WHERE rolname=ANY(' + role_array + ')) '
            'OR member IN (SELECT oid FROM pg_roles WHERE rolname=ANY(' + role_array + ')))' + admitted + ' ORDER BY roleid,member',
        'grants': "SELECT jsonb_build_array(c.relname,c.relacl::text) AS row FROM pg_class c WHERE c.relnamespace='public'::regnamespace "
            "AND c.relkind IN ('r','p','v','m','S') ORDER BY c.relname",
        'columns': "SELECT jsonb_build_array(c.relname,a.attname,a.attacl::text) AS row FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid "
            "WHERE c.relnamespace='public'::regnamespace AND a.attnum>0 AND NOT a.attisdropped ORDER BY c.relname,a.attnum",
        'defaults': 'SELECT jsonb_build_array(defaclrole::bigint,defaclnamespace::bigint,defaclobjtype,defaclacl::text) AS row FROM pg_default_acl '
            'ORDER BY defaclrole,defaclnamespace,defaclobjtype',
    }
    fields = [sql.SQL("{},(SELECT coalesce(jsonb_agg(row),'[]'::jsonb) FROM ({}) AS observation)").format(
        sql.Literal(name), sql.SQL(query)) for name, query in queries.items()]
    return sql.SQL('SELECT jsonb_build_object({})').format(sql.SQL(',').join(fields)).as_string()


def _snapshot_report(selection: DevelopmentSchemaSelection, revision: str, report: dict[str, Any]) -> DevelopmentSchemaProbe:
    if (set(report) != {'revision', 'physical', 'identities', 'forbidden', 'binding', 'tokens', 'memberships', 'grants', 'columns', 'defaults'}
            or report['revision'] != [[revision]] or len(report['physical']) != 1 or report['forbidden']):
        raise ValueError
    physical, roles = report['physical'][0], _roles(selection)
    identities = report['identities']
    if ({row[1] for row in identities} != set(roles) or any(any(row[3:8]) for row in identities)
            or any(row[2] != (not row[1].startswith('loom_app_runtime_')) for row in identities)):
        raise ValueError
    binding: Any = None
    if selection.mode == 'shared':
        binding = report['binding']
        if (len(binding) != 1 or binding[0][:3] != [str(selection.data_environment_id), roles[-2], roles[-1]]
                or binding[0][3:5] != [next(row[0] for row in identities if row[1] == role) for role in roles[-2:]]
                or binding[0][5:] != physical):
            raise ValueError
        marker = next(row[-1] for row in identities if row[1] == 'loom_actuator')
        parsed = json.loads(marker)
        if (parsed['kind'] != 'loom.development-runtime-database.v1'
                or [parsed['database_oid'], parsed['system_identifier']] != physical):
            raise ValueError
    return DevelopmentSchemaProbe(selection_sha256=_digest(selection.model_dump(mode='json')), revision=revision,
        database_oid=physical[0], system_identifier=physical[1],
        authority_sha256=_digest([identities, report['memberships'], binding, report['tokens']]),
        grants_sha256=_digest([report['grants'], report['columns'], report['defaults']]))


def _snapshot(db: psycopg.Connection[Any], selection: DevelopmentSchemaSelection, revision: str) -> DevelopmentSchemaProbe:
    row = db.execute(_snapshot_sql(selection)).fetchone()
    assert row is not None
    return _snapshot_report(selection, revision, row[0])


def probe_database(admin_url: str, selection: DevelopmentSchemaSelection) -> dict[str, Any]:
    """Read the stopped predecessor; caller retains this before its checkpoint."""
    try:
        selection = DevelopmentSchemaSelection.model_validate(selection.model_dump())
        _candidate(selection)
        with psycopg.connect(admin_url, autocommit=True, connect_timeout=10, options=_OPTIONS) as db:
            _lock(db)
            _quiescent(db, selection)
            return _snapshot(db, selection, selection.predecessor_revision).model_dump(mode='json')
    except Exception:
        raise DevelopmentSchemaDatabaseError('development_schema_predecessor_unqualified') from None


def _credential(db: psycopg.Connection[Any], admin_url: str, selection: DevelopmentSchemaSelection, password: str | None) -> None:
    if selection.mode != 'shared':
        if password is not None:
            raise ValueError
        return
    if password is None:
        raise ValueError
    role = _roles(selection)[-2]
    url = make_url(admin_url).set(drivername='postgresql', username=role, password=password).render_as_string(hide_password=False)
    with psycopg.connect(url, autocommit=True, connect_timeout=10, options=_OPTIONS) as manager:
        if manager.execute('SELECT current_user,session_user,current_database()').fetchone() != (role, role, db.info.dbname):
            raise ValueError


def _grant(db: psycopg.Connection[Any], selection: DevelopmentSchemaSelection) -> None:
    roles = ['loom_service'] + (['loom_control_plane'] if selection.mode == 'shared' else [])
    with db.transaction():
        tables = db.execute("SELECT relname FROM pg_class WHERE relnamespace='public'::regnamespace "
            "AND relkind IN ('r','p','v') AND relname<>'alembic_version' ORDER BY relname").fetchall()
        for role in roles:
            for (table,) in tables:
                db.execute(sql.SQL('GRANT SELECT,INSERT,UPDATE,DELETE ON public.{} TO {}').format(sql.Identifier(table), sql.Identifier(role)))
            db.execute(sql.SQL('GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA public TO {}').format(sql.Identifier(role)))
        if selection.mode == 'shared':
            tables_authority, columns_authority = actuator_authority()
            for table, privileges in tables_authority.items():
                db.execute(sql.SQL('GRANT {} ON public.{} TO loom_actuator').format(
                    sql.SQL(',').join(map(sql.SQL, sorted(privileges))), sql.Identifier(table)))
            for table, grants in columns_authority.items():
                for privilege, names in grants.items():
                    db.execute(sql.SQL('GRANT {} ({}) ON public.{} TO loom_actuator').format(sql.SQL(privilege),
                        sql.SQL(',').join(map(sql.Identifier, sorted(names))), sql.Identifier(table)))
            for sequence in actuator_sequences(db):
                db.execute(sql.SQL('GRANT USAGE,SELECT ON SEQUENCE {} TO loom_actuator').format(sql.Identifier(*sequence.split('.'))))
            # The Gateway owns the Pod-bound append/replay preview endpoint.
            db.execute('GRANT SELECT,INSERT ON public.service_execution_live_events TO loom_gateway')
            # 0183 adds the per-request rotation cursor and failed-member cooldown.
            db.execute('GRANT SELECT,UPDATE ON public.provider_request_pools, '
                'public.provider_request_pool_members TO loom_gateway')
            # 0176 introduced these Gateway-owned bearer budget/dispatch rows.
            db.execute('GRANT SELECT,UPDATE ON public.harbor_baseline_sessions TO loom_gateway; '
                'GRANT SELECT,INSERT,UPDATE ON public.harbor_baseline_dispatches TO loom_gateway')
        for role in _roles(selection):
            db.execute(sql.SQL('REVOKE ALL ON public.alembic_version FROM {}; GRANT SELECT ON public.alembic_version TO {}').format(
                sql.Identifier(role), sql.Identifier(role)))
            db.execute(sql.SQL('REVOKE INSERT(version_num),UPDATE(version_num),REFERENCES(version_num) '
                'ON public.alembic_version FROM {}').format(sql.Identifier(role)))
        # The access manager must remain unable to read public application data.
        if selection.mode == 'shared':
            db.execute(sql.SQL('REVOKE ALL ON public.alembic_version FROM {}').format(sql.Identifier(_roles(selection)[-2])))
    if selection.mode == 'shared':
        assert selection.data_environment_id is not None
        install_application_database_access(db, data_environment_id=selection.data_environment_id, manager_role=_roles(selection)[-2])
        marker = db.execute("SELECT shobj_description(oid,'pg_authid') FROM pg_roles WHERE rolname='loom_actuator'").fetchone()
        assert marker is not None
        qualify_actuator(db, marker[0], bytes.fromhex(json.loads(marker[0])['token_sha256']))
    for role in _roles(selection):
        if db.execute("SELECT has_table_privilege(%s,'public.alembic_version','INSERT,UPDATE,DELETE,TRUNCATE,TRIGGER,REFERENCES') "
                "OR has_any_column_privilege(%s,'public.alembic_version','INSERT,UPDATE,REFERENCES')", (role, role)).fetchone() != (False,):
            raise ValueError


def _existing(db: psycopg.Connection[Any], request: DevelopmentSchemaUpgrade) -> dict[str, Any] | None:
    if db.execute('SELECT to_regnamespace(%s)', (_SCHEMA,)).fetchone() == (None,):
        return None
    row = db.execute(schema_successor_sql(request)).fetchone()
    assert row is not None
    return qualify_schema_successor_report(request, row[0])


def schema_successor_sql(request: DevelopmentSchemaUpgrade) -> str:
    """Fresh read-only receipt and fingerprint observation, never an old Job log.

    The caller must select UTC and acquire the schema lock before this statement.
    It contains private authority material; qualify locally and return only the
    bounded receipt. No caller SQL, repair, grant or migration is accepted.
    """
    request = DevelopmentSchemaUpgrade.model_validate(request.model_dump())
    _candidate(request.selection)
    return sql.SQL("SELECT jsonb_build_object('schema_version','loom.development-schema-observation.v1',"
        "'authority',(SELECT current_user=session_user AND rolsuper FROM pg_roles WHERE rolname=current_user),"
        "'schema_acl',(SELECT nspowner=(SELECT oid FROM pg_roles WHERE rolname=current_user) "
            "AND NOT EXISTS(SELECT 1 FROM aclexplode(nspacl) WHERE grantee<>nspowner) "
            "FROM pg_namespace WHERE nspname='loom_development_upgrade'),"
        "'table_acl',(SELECT relowner=(SELECT oid FROM pg_roles WHERE rolname=current_user) AND relkind='r' "
            "AND NOT EXISTS(SELECT 1 FROM aclexplode(relacl) WHERE grantee<>relowner) "
            "FROM pg_class WHERE oid='loom_development_upgrade.operation'::regclass),"
        "'operation',(SELECT jsonb_build_array(request_json,request_sha256,status,receipt_json) "
            'FROM loom_development_upgrade.operation WHERE operation_id={}),'
        "'snapshot',({}))").format(sql.Literal(request.selection.operation_id), sql.SQL(_snapshot_sql(request.selection))).as_string()


def schema_successor_readback_sql(request: DevelopmentSchemaUpgrade) -> str:
    """Fixed psql readback, with one fresh snapshot after the schema lock."""
    return ('BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED READ ONLY; SET LOCAL statement_timeout=10000; '
        "SET LOCAL lock_timeout=2000; SET LOCAL timezone='UTC'; "
        'DO $loom$ BEGIN IF NOT pg_try_advisory_xact_lock(' + str(APPLICATION_SCHEMA_LOCK) + ') '
        "THEN RAISE EXCEPTION 'development_schema_lock_unavailable'; END IF; END; $loom$; "
        + schema_successor_sql(request) + '; ROLLBACK;')


def qualify_schema_successor_report(request: DevelopmentSchemaUpgrade, report: dict[str, Any]) -> dict[str, Any]:
    """Use the runner's exact current role, credential and grant contract."""
    try:
        request = DevelopmentSchemaUpgrade.model_validate(request.model_dump())
        if (set(report) != {'schema_version', 'authority', 'schema_acl', 'table_acl', 'operation', 'snapshot'}
                or report['schema_version'] != 'loom.development-schema-observation.v1'
                or any(report[name] is not True for name in ('authority', 'schema_acl', 'table_acl'))):
            raise ValueError
        row = report['operation']
        if not isinstance(row, list) or len(row) != 4 or row[0] != request.model_dump(mode='json') or row[1] != _digest(row[0]):
            raise ValueError
        if row[2] != 'complete':
            raise DevelopmentSchemaDatabaseError('development_schema_recovery_required')
        actual = _snapshot_report(request.selection, request.selection.candidate_revision, report['snapshot'])
        receipt = row[3]
        if (not isinstance(receipt, dict) or receipt != _receipt(request, actual)
                or actual.authority_sha256 != request.predecessor.authority_sha256
                or (actual.database_oid, actual.system_identifier) != (request.predecessor.database_oid, request.predecessor.system_identifier)):
            raise ValueError
        return dict(receipt)
    except DevelopmentSchemaDatabaseError:
        raise
    except Exception:
        raise DevelopmentSchemaDatabaseError('development_schema_successor_unqualified') from None


def _receipt(request: DevelopmentSchemaUpgrade, result: DevelopmentSchemaProbe) -> dict[str, Any]:
    return {'schema_version': 'loom.development-schema-upgrade.v1', 'status': 'complete',
        'operation_id': str(request.selection.operation_id), 'mode': request.selection.mode,
        'request_sha256': _digest(request.model_dump(mode='json')), 'revision': result.revision,
        'checkpoint_sha256': request.checkpoint_sha256, 'successor': result.model_dump(mode='json')}


def read_database_upgrade(admin_url: str, request: DevelopmentSchemaUpgrade) -> dict[str, Any]:
    """Qualify only a durable completed successor; never repair a partial one."""
    try:
        request = DevelopmentSchemaUpgrade.model_validate(request.model_dump())
        _candidate(request.selection)
        with psycopg.connect(admin_url, autocommit=True, connect_timeout=10, options=_OPTIONS) as db:
            _lock(db)
            result = _existing(db, request)
            if result is None:
                raise ValueError
            return result
    except DevelopmentSchemaDatabaseError:
        raise
    except Exception:
        raise DevelopmentSchemaDatabaseError('development_schema_successor_unqualified') from None


def upgrade_database(admin_url: str, request: DevelopmentSchemaUpgrade, *, application_manager_password: str | None = None) -> dict[str, Any]:
    """Run the normal candidate chain once, preserving the physical connection lock.

    The committed intent precedes Alembic and its possible autocommit DDL. Any
    loss between that intent and the committed grant receipt is recovery-only.
    A completed replay merely reads the exact successor, without regranting.
    """
    engine = None
    try:
        request = DevelopmentSchemaUpgrade.model_validate(request.model_dump())
        selection = request.selection
        config = _candidate(selection)
        engine = create_engine(make_url(admin_url).set(drivername='postgresql+psycopg'), poolclass=pool.NullPool,
            connect_args={'connect_timeout': 10, 'options': _OPTIONS})
        with engine.connect() as connection:
            db = connection.connection.driver_connection
            if not isinstance(db, psycopg.Connection):
                raise ValueError
            db.autocommit = True
            _lock(db)
            completed = _existing(db, request)
            if completed is not None:
                return completed
            _credential(db, admin_url, selection, application_manager_password)
            _quiescent(db, selection)
            if _snapshot(db, selection, selection.predecessor_revision) != request.predecessor:
                raise ValueError
            with db.transaction():
                db.execute('CREATE SCHEMA loom_development_upgrade; REVOKE ALL ON SCHEMA loom_development_upgrade FROM PUBLIC; '
                    'CREATE TABLE loom_development_upgrade.operation(operation_id uuid PRIMARY KEY, request_json jsonb NOT NULL, '
                    "request_sha256 text NOT NULL,status text NOT NULL CHECK(status IN ('intent','complete')),receipt_json jsonb)")
                db.execute("INSERT INTO loom_development_upgrade.operation VALUES(%s,%s,%s,'intent',NULL)",
                    (selection.operation_id, Jsonb(request.model_dump(mode='json')), _digest(request.model_dump(mode='json'))))
            db.autocommit = False
            # Historical migrations create unqualified public objects. Keeping
            # pg_catalog first would direct CREATE TABLE into the system schema.
            db.execute('SET search_path=public,pg_catalog')
            db.commit()
            config.attributes['connection'] = connection
            config.set_main_option('sqlalchemy.url', make_url(admin_url).set(drivername='postgresql+psycopg').render_as_string(hide_password=False).replace('%', '%%'))
            command.upgrade(config, selection.candidate_revision)
            db.autocommit = True
            db.execute('SET search_path=pg_catalog,public,pg_temp')
            _quiescent(db, selection)
            if _snapshot(db, selection, selection.candidate_revision).authority_sha256 != request.predecessor.authority_sha256:
                raise ValueError
            _grant(db, selection)
            result = _snapshot(db, selection, selection.candidate_revision)
            if result.authority_sha256 != request.predecessor.authority_sha256:
                raise ValueError
            receipt = _receipt(request, result)
            db.execute("UPDATE loom_development_upgrade.operation SET status='complete',receipt_json=%s "
                "WHERE operation_id=%s AND status='intent'", (Jsonb(receipt), selection.operation_id))
            return receipt
    except DevelopmentSchemaDatabaseError:
        raise
    except Exception:
        raise DevelopmentSchemaDatabaseError('development_schema_upgrade_unqualified') from None
    finally:
        if engine is not None:
            engine.dispose()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('phase', choices=('probe', 'upgrade', 'inspect'))
    args = parser.parse_args()
    try:
        with Path(os.environ['LOOM_DEVELOPMENT_SCHEMA_CONFIG']).open('rb') as stream:
            raw = stream.read(32769)
        if not 0 < len(raw) <= 32768:
            raise ValueError
        if args.phase == 'probe':
            selection = DevelopmentSchemaSelection.model_validate_json(raw)
            result = probe_database(database_url(os.environ['LOOM_DB_URL'], selection.namespace), selection)
        else:
            request = DevelopmentSchemaUpgrade.model_validate_json(raw)
            url = database_url(os.environ['LOOM_DB_URL'], request.selection.namespace)
            result = (upgrade_database(url, request, application_manager_password=os.environ.get('LOOM_APPLICATION_MANAGER_PASSWORD'))
                if args.phase == 'upgrade' else read_database_upgrade(url, request))
        print(json.dumps(result, sort_keys=True))
        return 0
    except Exception as error:
        code = str(error) if isinstance(error, DevelopmentSchemaDatabaseError) else 'development_schema_input_unqualified'
        print(json.dumps({'status': 'unqualified', 'error_code': code}))
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
