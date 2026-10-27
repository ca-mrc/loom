"""Candidate Alembic upgrades preserve installed identity and fail closed on interruption."""
from __future__ import annotations

import hashlib
import importlib
import json
from secrets import token_urlsafe
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from sqlalchemy.engine import make_url

from loom.db.schema_startup import service_schema_head
from loom.nebius_application_database import ApplicationDatabaseAccess
from loom.nebius_application_database_install import install_shared_manager
from tests.integration.conftest import retain_pool_schema


def implementation():
    name = 'loom.nebius_development_schema_database'
    if importlib.util.find_spec(name) is None:
        pytest.fail('qualified credential-preserving development schema upgrade is missing')
    return importlib.import_module(name)


@pytest.fixture(params=['shared', 'manager'])
def installed(request, isolated_migration_postgres_url):
    mode = request.param
    url = make_url(isolated_migration_postgres_url).set(drivername='postgresql').render_as_string(hide_password=False)
    revision = '0175' if mode == 'shared' else '0176'
    retain_pool_schema(url, revision)
    data, pool = uuid4(), uuid4()
    password = token_urlsafe(48)
    roles = ['loom_service'] if mode == 'manager' else ['loom_service', 'loom_control_plane', 'loom_gateway', 'loom_actuator']
    with psycopg.connect(url, autocommit=True) as db:
        for role in roles:
            db.execute(sql.SQL('CREATE ROLE {} LOGIN NOINHERIT PASSWORD {}').format(sql.Identifier(role), sql.Literal(password)))
            db.execute(sql.SQL('GRANT CONNECT ON DATABASE {} TO {}; GRANT USAGE ON SCHEMA public TO {}').format(
                sql.Identifier(db.info.dbname), sql.Identifier(role), sql.Identifier(role)))
        for role in ('loom_service', *(['loom_control_plane'] if mode == 'shared' else [])):
            db.execute(sql.SQL('GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA public TO {}').format(sql.Identifier(role)))
        db.execute('INSERT INTO teams(id,name) VALUES(%s,%s)', (data, 'retained-schema-test'))
        if mode == 'shared':
            # Exact historical role marker is immutable across a schema upgrade.
            marker = json.dumps({'kind': 'loom.development-runtime-database.v1', 'schema_revision': revision,
                'operation_id': str(uuid4()), 'database_oid': db.execute('SELECT oid FROM pg_database WHERE datname=current_database()').fetchone()[0],
                'system_identifier': db.execute('SELECT system_identifier::text FROM pg_control_system()').fetchone()[0],
                'token_sha256': hashlib.sha256(b'retained-batch-token').hexdigest()}, sort_keys=True, separators=(',', ':'))
            db.execute(sql.SQL('COMMENT ON ROLE loom_actuator IS {}').format(sql.Literal(marker)))
            # The established actuator grant implementation assumes DB name loom;
            # a cloned migration fixture intentionally has another database name.
            db.execute(sql.SQL('GRANT CONNECT ON DATABASE {} TO loom_actuator').format(sql.Identifier(db.info.dbname)))
            # Qualify its pre-existing grants independently of the successor.
            from loom.nebius_development_runtime_database import _authority
            tables, columns = _authority()
            for table, privileges in tables.items():
                db.execute(sql.SQL('GRANT {} ON public.{} TO loom_actuator').format(
                    sql.SQL(',').join(map(sql.SQL, sorted(privileges))), sql.Identifier(table)))
            for table, grants in columns.items():
                for privilege, names in grants.items():
                    db.execute(sql.SQL('GRANT {} ({}) ON public.{} TO loom_actuator').format(sql.SQL(privilege),
                        sql.SQL(',').join(map(sql.Identifier, sorted(names))), sql.Identifier(table)))
            db.execute("INSERT INTO tokens(token_hash,type,scopes,issued_at) VALUES(%s,'worker',ARRAY['submit:batch'],now())",
                (hashlib.sha256(b'retained-batch-token').digest(),))
            roles.extend([install_shared_manager(url, data_environment_id=data, schema_revision=revision,
                manager_password=password), 'loom_app_runtime_' + data.hex])
        else:
            db.execute("INSERT INTO nebius_pool_bindings(pool_id,installation_id,cluster_id,node_group_id,policy_revision,admission_epoch,mode,binding_json,binding_sha256) "
                "VALUES(%s,%s,'cluster','nodes',1,1,'closed','{}',%s)", (pool, uuid4(), 'a' * 64))
    yield {'url': url, 'mode': mode, 'revision': revision, 'data': data, 'pool': pool, 'password': password, 'roles': roles}
    with psycopg.connect(url, autocommit=True) as db:
        # All these cluster roles belong solely to this disposable fixture.
        for role in reversed(roles):
            db.execute(sql.SQL('DROP OWNED BY {}').format(sql.Identifier(role)))
            db.execute(sql.SQL('DROP ROLE {}').format(sql.Identifier(role)))


def selection(module, installed):
    return module.DevelopmentSchemaSelection(operation_id=uuid4(), mode=installed['mode'],
        namespace='loom-dev' if installed['mode'] == 'shared' else 'loom-nebius-management-dev',
        predecessor_revision=installed['revision'], candidate_revision=service_schema_head(), candidate_sha='b' * 40,
        data_environment_id=installed['data'] if installed['mode'] == 'shared' else None,
        pool_id=installed['pool'] if installed['mode'] == 'manager' else None)


def upgrade_request(module, installed):
    selected = selection(module, installed)
    try:
        proof = module.probe_database(installed['url'], selected)
    except module.DevelopmentSchemaDatabaseError as error:
        raise AssertionError('disposable predecessor failed qualification') from error.__context__
    return module.DevelopmentSchemaUpgrade(selection=selected, predecessor=proof, checkpoint_sha256='c' * 64)


def authority(db):
    return db.execute("SELECT oid,rolname,rolpassword,shobj_description(oid,'pg_authid') FROM pg_authid "
        "WHERE rolname LIKE 'loom_%' ORDER BY rolname").fetchall()


def test_real_retained_schema_upgrades_preserving_identity_and_granting_new_tables(installed):
    module = implementation()
    request = upgrade_request(module, installed)
    with psycopg.connect(installed['url'], autocommit=True) as db:
        before = authority(db)
    try:
        receipt = module.upgrade_database(installed['url'], request,
            application_manager_password=installed['password'] if installed['mode'] == 'shared' else None)
    except module.DevelopmentSchemaDatabaseError as error:
        raise AssertionError('disposable upgrade failed qualification') from error.__context__
    assert receipt['status'] == 'complete'
    assert receipt['revision'] == '0180'
    assert module.read_database_upgrade(installed['url'], request) == receipt
    assert module.upgrade_database(installed['url'], request,
        application_manager_password=installed['password'] if installed['mode'] == 'shared' else None) == receipt
    with psycopg.connect(installed['url'], autocommit=True) as db:
        assert authority(db) == before
        assert db.execute('SELECT version_num FROM alembic_version').fetchall() == [('0180',)]
        assert db.execute('SELECT name FROM teams WHERE id=%s', (installed['data'],)).fetchone() == ('retained-schema-test',)
        roles = ['loom_service'] + (['loom_control_plane', 'loom_app_runtime_' + installed['data'].hex] if installed['mode'] == 'shared' else [])
        for role in roles:
            assert db.execute("SELECT has_table_privilege(%s,'task_source_publications','SELECT,INSERT,UPDATE,DELETE'), "
                "has_table_privilege(%s,'nebius_pool_participant_catalogs','SELECT,INSERT,UPDATE,DELETE'), "
                "has_table_privilege(%s,'alembic_version','INSERT,UPDATE,DELETE')", (role, role, role)).fetchone() == (True, True, False)
            for privilege in ('SELECT', 'INSERT', 'UPDATE', 'DELETE'):
                assert db.execute("SELECT has_table_privilege(%s,'service_execution_live_events',%s)",
                    (role, privilege)).fetchone() == (True,)
        if installed['mode'] == 'shared':
            for role in ('loom_actuator', 'loom_gateway', 'loom_app_manager_' + installed['data'].hex):
                assert db.execute("SELECT has_table_privilege(%s,'service_execution_live_events',"
                    "'SELECT,INSERT,UPDATE,DELETE')", (role,)).fetchone() == (False,)
    login = make_url(installed['url']).set(username='loom_service', password=installed['password']).render_as_string(hide_password=False)
    with psycopg.connect(login) as db:
        assert db.execute('SELECT count(*) FROM nebius_pool_lifecycle_operations').fetchone() == (0,)
        assert module.read_database_upgrade(installed['url'], request) == receipt
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            db.execute('UPDATE loom_development_upgrade.operation SET status=\'complete\'')
    if installed['mode'] == 'shared':
        manager_url = make_url(installed['url']).set(username='loom_app_manager_' + installed['data'].hex,
            password=installed['password']).render_as_string(hide_password=False)
        with psycopg.connect(manager_url, autocommit=True) as db:
            access = ApplicationDatabaseAccess(db, installed['data'])
            app, incarnation = uuid4(), uuid4()
            role = access.grant(app, incarnation, 1, token_urlsafe(48), schema_revision='0180')
            installed['roles'].append(role)
            # Normal successor application admission does not invalidate the
            # retained migration proof for fixed platform credential identities.
            assert module.read_database_upgrade(installed['url'], request) == receipt
            access.revoke(app, incarnation, 1)
            assert access.drain(app, incarnation, 1)


def test_foreign_session_and_changed_authority_reject_without_upgrade_receipt(installed):
    module = implementation()
    request = upgrade_request(module, installed)
    with psycopg.connect(installed['url'], autocommit=True) as concurrent:
        concurrent.execute('SELECT 1')
        with pytest.raises(module.DevelopmentSchemaDatabaseError):
            module.upgrade_database(installed['url'], request)
        assert concurrent.execute("SELECT to_regnamespace('loom_development_upgrade')").fetchone() == (None,)
    with psycopg.connect(installed['url'], autocommit=True) as db:
        db.execute("COMMENT ON ROLE loom_service IS 'concurrent-owner-change'")
    with pytest.raises(module.DevelopmentSchemaDatabaseError):
        module.upgrade_database(installed['url'], request)
    with psycopg.connect(installed['url'], autocommit=True) as db:
        assert db.execute('SELECT version_num FROM alembic_version').fetchone() == (installed['revision'],)
        assert db.execute("SELECT to_regnamespace('loom_development_upgrade')").fetchone() == (None,)


@pytest.mark.parametrize('position', ['before', 'after'])
def test_interrupted_migration_is_never_automatically_reissued(installed, monkeypatch, position):
    module = implementation()
    request = upgrade_request(module, installed)
    real = module.command.upgrade

    def interrupted(*args, **kwargs):
        if position == 'after':
            real(*args, **kwargs)
        raise RuntimeError('simulated lost migration result')

    monkeypatch.setattr(module.command, 'upgrade', interrupted)
    with pytest.raises(module.DevelopmentSchemaDatabaseError):
        module.upgrade_database(installed['url'], request,
            application_manager_password=installed['password'] if installed['mode'] == 'shared' else None)
    monkeypatch.setattr(module.command, 'upgrade', real)
    with pytest.raises(module.DevelopmentSchemaDatabaseError, match='recovery_required'):
        module.upgrade_database(installed['url'], request,
            application_manager_password=installed['password'] if installed['mode'] == 'shared' else None)
    with psycopg.connect(installed['url'], autocommit=True) as db:
        assert db.execute('SELECT version_num FROM alembic_version').fetchone() == (
            '0180' if position == 'after' else installed['revision'],)


@pytest.mark.parametrize('installed', ['shared'], indirect=True)
def test_old_personal_generation_and_retired_session_block_before_any_mutation(installed):
    module = implementation()
    selected = selection(module, installed)
    manager_url = make_url(installed['url']).set(username='loom_app_manager_' + installed['data'].hex,
        password=installed['password']).render_as_string(hide_password=False)
    app, incarnation, password = uuid4(), uuid4(), token_urlsafe(48)
    with psycopg.connect(manager_url, autocommit=True) as manager:
        role = ApplicationDatabaseAccess(manager, installed['data']).grant(app, incarnation, 1, password,
            schema_revision='0175')
    installed['roles'].append(role)
    with psycopg.connect(installed['url'], autocommit=True) as db:
        assert db.execute(module.schema_idle_sql(selected)).fetchone()[0]['status'] == 'unqualified'
    with pytest.raises(module.DevelopmentSchemaDatabaseError):
        module.probe_database(installed['url'], selected)
    application_url = make_url(installed['url']).set(username=role, password=password).render_as_string(hide_password=False)
    with psycopg.connect(application_url, autocommit=True) as application:
        with psycopg.connect(manager_url, autocommit=True) as manager:
            ApplicationDatabaseAccess(manager, installed['data']).revoke(app, incarnation, 1)
        assert application.execute('SELECT 1').fetchone() == (1,)
        with pytest.raises(module.DevelopmentSchemaDatabaseError):
            module.probe_database(installed['url'], selected)
        with psycopg.connect(manager_url, autocommit=True) as manager:
            assert ApplicationDatabaseAccess(manager, installed['data']).drain(app, incarnation, 1)
    assert module.probe_database(installed['url'], selected)['revision'] == '0175'
    with psycopg.connect(installed['url'], autocommit=True) as db:
        assert db.execute("SELECT to_regnamespace('loom_development_upgrade')").fetchone() == (None,)


@pytest.mark.parametrize('installed', ['manager'], indirect=True)
@pytest.mark.parametrize('blocker', ['active_application', 'queued_build', 'awaiting_source', 'open_pool'])
def test_intake_work_or_open_pool_rejects_before_migration_intent(installed, blocker):
    module = implementation()
    request = upgrade_request(module, installed)
    with psycopg.connect(installed['url'], autocommit=True) as db:
        owner, upload, build, installation, data = uuid4(), uuid4(), uuid4(), uuid4(), uuid4()
        db.execute("INSERT INTO users(id,username,username_normalized,status) VALUES(%s,'owner','owner','active')", (owner,))
        if blocker == 'open_pool':
            db.execute("UPDATE nebius_pool_bindings SET mode='global'")
        elif blocker == 'active_application':
            db.execute("INSERT INTO nebius_applications(application_id,incarnation,owner_user_id,owner_team_id,data_environment_id,"
                "cluster_id,slug,application_namespace,public_host,release_id,deployment_generation,access_generation,desired_state) "
                "VALUES(%s,%s,%s,%s,%s,'cluster','owner','loom-dev-owner','owner.example.com',%s,1,1,'active')",
                (uuid4(), uuid4(), owner, installed['data'], uuid4(), uuid4()))
        else:
            pending = blocker == 'awaiting_source'
            db.execute("INSERT INTO nebius_application_source_uploads(upload_id,owner_user_id,owner_team_id,installation_id,data_environment_id,"
                "cluster_id,source_bucket,object_key,idempotency_key,request_sha256,source_digest,archive_sha256,archive_size_bytes,phase,expires_at,verified_at) "
                "VALUES(%s,%s,%s,%s,%s,'cluster','sources',%s,'source',%s,%s,%s,10240,'awaiting_source',now()+interval '10 minutes',NULL)",
                (upload, owner, installed['data'], installation, data,
                    'application-sources/v1/sha256/' + 'd' * 64 + '.tar', 'd' * 64, 'sha256:' + 'd' * 64, 'd' * 64,
                    ))
            if not pending:
                db.execute("UPDATE nebius_application_source_uploads SET phase='source_verified',verified_at=now() WHERE upload_id=%s", (upload,))
                db.execute("INSERT INTO nebius_application_builds(build_id,upload_id,owner_user_id,owner_team_id,installation_id,"
                    "data_environment_id,cluster_id,idempotency_key,request_sha256,binding_json,current_attempt,desired_state) "
                    "VALUES(%s,%s,%s,%s,%s,%s,'cluster','build',%s,'{\"recipe\":{}}',1,'running')",
                    (build, upload, owner, installed['data'], installation, data, 'd' * 64))
                claim = {'build_id': str(build), 'attempt': 1, 'owner_user_id': str(owner),
                    'owner_team_id': str(installed['data']), 'installation_id': str(installation),
                    'data_environment_id': str(data), 'cluster_id': 'cluster', 'upload_id': str(upload),
                    'source': {'source_digest': 'sha256:' + 'd' * 64, 'archive_sha256': 'd' * 64}, 'recipe': {}}
                db.execute("INSERT INTO nebius_application_build_attempts(build_id,attempt,claim_json,claim_sha256,phase) "
                    "VALUES(%s,1,%s::jsonb,%s,'queued')", (build, json.dumps(claim), 'd' * 64))
        assert db.execute(module.schema_idle_sql(request.selection)).fetchone()[0]['status'] == 'unqualified'
    with pytest.raises(module.DevelopmentSchemaDatabaseError):
        module.upgrade_database(installed['url'], request)
    with psycopg.connect(installed['url'], autocommit=True) as db:
        assert db.execute('SELECT version_num FROM alembic_version').fetchone() == ('0176',)
        assert db.execute("SELECT to_regnamespace('loom_development_upgrade')").fetchone() == (None,)


@pytest.mark.parametrize('installed', ['manager'], indirect=True)
def test_completed_receipt_cannot_mask_missing_grants_or_be_repaired_on_replay(installed):
    module = implementation()
    request = upgrade_request(module, installed)
    receipt = module.upgrade_database(installed['url'], request)
    with psycopg.connect(installed['url'], autocommit=True) as db:
        db.execute('SET default_transaction_read_only=on')
        cursor = db.execute(module.schema_successor_readback_sql(request))
        reports = []
        while True:
            if cursor.description:
                reports.append(cursor.fetchone()[0])
            if not cursor.nextset():
                break
        report, = reports
        assert module.qualify_schema_successor_report(request, report) == receipt
        db.execute('SET default_transaction_read_only=off')
        db.execute('REVOKE INSERT ON nebius_pool_participant_catalogs FROM loom_service')
        db.execute('SET default_transaction_read_only=on')
        changed = db.execute(module.schema_successor_sql(request)).fetchone()[0]
        with pytest.raises(module.DevelopmentSchemaDatabaseError):
            module.qualify_schema_successor_report(request, changed)
    with pytest.raises(module.DevelopmentSchemaDatabaseError):
        module.read_database_upgrade(installed['url'], request)
    with pytest.raises(module.DevelopmentSchemaDatabaseError):
        module.upgrade_database(installed['url'], request)
    with psycopg.connect(installed['url'], autocommit=True) as db:
        assert db.execute("SELECT has_table_privilege('loom_service','nebius_pool_participant_catalogs','INSERT')").fetchone() == (False,)


@pytest.mark.parametrize('installed', ['shared'], indirect=True)
def test_retained_manager_password_is_authenticated_before_migration(installed):
    module = implementation()
    request = upgrade_request(module, installed)
    with pytest.raises(module.DevelopmentSchemaDatabaseError):
        module.upgrade_database(installed['url'], request, application_manager_password=token_urlsafe(48))
    with psycopg.connect(installed['url'], autocommit=True) as db:
        assert db.execute('SELECT version_num FROM alembic_version').fetchone() == ('0175',)
        assert db.execute("SELECT to_regnamespace('loom_development_upgrade')").fetchone() == (None,)


def test_idle_preflight_is_read_only_and_allows_existing_runtime_sessions(installed):
    module = implementation()
    selected = selection(module, installed)
    with psycopg.connect(installed['url'], autocommit=True) as db:
        db.execute('SET default_transaction_read_only=on')
        report = db.execute(module.schema_idle_sql(selected)).fetchone()[0]
        assert report['status'] == 'qualified'
        assert report['revision'] == installed['revision']
        # The same still-connected runtime must block the stopped-phase probe.
        with pytest.raises(module.DevelopmentSchemaDatabaseError):
            module.probe_database(installed['url'], selected)
        assert db.execute("SELECT to_regnamespace('loom_development_upgrade')").fetchone() == (None,)


@pytest.mark.parametrize('installed', ['shared'], indirect=True)
def test_schema_lock_remains_held_between_alembic_and_regrant(installed, monkeypatch):
    from loom.nebius_application_database import ApplicationDatabaseAccessError

    module = implementation()
    request = upgrade_request(module, installed)
    original = module._grant

    def concurrent_grant(db, selected):
        manager_url = make_url(installed['url']).set(username='loom_app_manager_' + installed['data'].hex,
            password=installed['password']).render_as_string(hide_password=False)
        with psycopg.connect(manager_url, autocommit=True, options='-c statement_timeout=100') as manager:
            with pytest.raises(ApplicationDatabaseAccessError, match='operation_failed'):
                ApplicationDatabaseAccess(manager, installed['data']).grant(uuid4(), uuid4(), 1,
                    token_urlsafe(48), schema_revision='0179')
        assert db.execute('SELECT count(*) FROM loom_application_access.generations').fetchone() == (0,)
        original(db, selected)

    monkeypatch.setattr(module, '_grant', concurrent_grant)
    assert module.upgrade_database(installed['url'], request,
        application_manager_password=installed['password'])['status'] == 'complete'
