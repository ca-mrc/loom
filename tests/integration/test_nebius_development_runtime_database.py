"""Fresh dev runtime setup preserves the independently installed data services."""
from __future__ import annotations

import hashlib
import importlib
import json
from uuid import UUID

import psycopg
import pytest
from sqlalchemy.engine import make_url

from loom import nebius_platform_bootstrap as bootstrap
from tests.integration.test_nebius_platform_bootstrap import platform_database as platform_database

OPERATION = UUID('6cba7ad5-ff30-49ef-b4bb-6d49b2eae492')
PASSWORD = 'test-actuator-' + 'a' * 40
TOKEN = 'loom_br_' + 'b' * 64


def runtime():
    name = 'loom.nebius_development_runtime_database'
    if importlib.util.find_spec(name) is None:
        pytest.fail('non-rotating fresh development runtime database setup is missing')
    return importlib.import_module(name)


@pytest.fixture(scope='module')
def private_database(platform_database):
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(bootstrap, 'database_url', lambda _value, _namespace: platform_database)
        patch.setenv('LOOM_DB_URL', platform_database)
        for name in ('SERVICE', 'CONTROL_PLANE', 'GATEWAY'):
            patch.setenv('LOOM_DB_' + name + '_PASSWORD', 'private-test-' + name + 'x' * 30)
        bootstrap.bootstrap_development_database({'schema_version': 'loom.nebius-development-bootstrap.v1',
            'namespace': 'loom-dev', 'environment': 'development'})
    return platform_database


@pytest.fixture
def database(private_database):
    yield private_database
    # This fixture owns its disposable PostgreSQL container, never a live URL.
    with psycopg.connect(private_database, autocommit=True) as db:
        if db.execute("SELECT 1 FROM pg_roles WHERE rolname='loom_actuator'").fetchone():
            db.execute('DROP OWNED BY loom_actuator')
            db.execute('DROP ROLE loom_actuator')
        db.execute("DELETE FROM tokens WHERE type='worker'")


def install(module, database, **changes):
    arguments = dict(operation_id=OPERATION, schema_revision='0175',
        actuator_password=PASSWORD, batch_runner_token=TOKEN)
    arguments.update(changes)
    return module.install_runtime_database(database, **arguments)


def identities(db):
    return db.execute("SELECT oid,rolname,rolpassword FROM pg_authid WHERE rolname IN "
        "('loom_service','loom_control_plane','loom_gateway') ORDER BY rolname").fetchall()


def test_fresh_runtime_preserves_services_and_replay_identity(database):
    module = runtime()
    with psycopg.connect(database) as db:
        before = identities(db)
        grants = db.execute("SELECT grantee,table_name,privilege_type FROM information_schema.role_table_grants "
            "WHERE grantee IN ('loom_service','loom_control_plane','loom_gateway') ORDER BY 1,2,3").fetchall()
    receipt = install(module, database)
    assert receipt['operation_id'] == str(OPERATION)
    assert receipt['role'] == 'loom_actuator'
    assert receipt['token_sha256'] == hashlib.sha256(TOKEN.encode()).hexdigest()
    assert PASSWORD not in repr(receipt) and TOKEN not in repr(receipt)
    with psycopg.connect(database) as db:
        assert identities(db) == before
        assert db.execute("SELECT grantee,table_name,privilege_type FROM information_schema.role_table_grants "
            "WHERE grantee IN ('loom_service','loom_control_plane','loom_gateway') ORDER BY 1,2,3").fetchall() == grants
        role = db.execute("SELECT oid,rolpassword FROM pg_authid WHERE rolname='loom_actuator'").fetchone()
        assert receipt['role_oid'] == role[0]
        assert db.execute("SELECT type,scopes,team_id,expires_at,revoked_at FROM tokens WHERE type='worker'").fetchall() == [
            ('worker', ['submit:batch'], None, None, None)]
        assert db.execute('SELECT count(*) FROM execution_targets').fetchone() == (0,)
        assert db.execute('SELECT count(*) FROM nebius_pool_bindings').fetchone() == (0,)
    assert install(module, database) == receipt
    with psycopg.connect(database) as db:
        assert db.execute("SELECT oid,rolpassword FROM pg_authid WHERE rolname='loom_actuator'").fetchone() == role
    actuator_url = make_url(database).set(username='loom_actuator', password=PASSWORD).render_as_string(hide_password=False)
    with psycopg.connect(actuator_url, autocommit=True) as db:
        assert db.execute('SELECT current_user').fetchone() == ('loom_actuator',)
        for table in ('nebius_pool_execution_outbox', 'nebius_pool_build_outbox'):
            for privilege in ('INSERT', 'UPDATE'):
                assert db.execute('SELECT has_table_privilege(current_user,%s,%s)', (table, privilege)).fetchone() == (True,)
        db.execute('UPDATE tasks SET registered_at=registered_at WHERE false')
        db.execute('UPDATE batches SET pool_origin=pool_origin WHERE false')
        db.execute('UPDATE task_bundle_sources SET created_at=created_at WHERE false')
        for query in ('SELECT * FROM tokens', 'SELECT * FROM nebius_pool_machine_credentials',
                'DELETE FROM nebius_pool_execution_outbox', 'UPDATE tasks SET config=config WHERE false',
                'CREATE ROLE unexpected SUPERUSER'):
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                db.execute(query)


@pytest.mark.parametrize('changes', [
    {'actuator_password': 'wrong-password-' + 'w' * 40},
    {'operation_id': UUID('8c882972-b457-4d02-b11a-e0ba93506a72')},
    {'batch_runner_token': 'loom_br_' + 'c' * 64},
    {'schema_revision': '0173'},
    {'schema_revision': '0174'},
])
def test_replay_rejects_changed_authority_without_rotating(database, changes):
    module = runtime()
    receipt = install(module, database)
    with psycopg.connect(database) as db:
        before = db.execute("SELECT oid,rolpassword FROM pg_authid WHERE rolname='loom_actuator'").fetchone()
    with pytest.raises(module.DevelopmentRuntimeDatabaseError, match='development_runtime_database_unqualified') as error:
        install(module, database, **changes)
    assert PASSWORD not in str(error.value) and TOKEN not in str(error.value)
    assert install(module, database) == receipt
    with psycopg.connect(database) as db:
        assert db.execute("SELECT oid,rolpassword FROM pg_authid WHERE rolname='loom_actuator'").fetchone() == before
        assert db.execute("SELECT count(*) FROM tokens WHERE type='worker'").fetchone() == (1,)


@pytest.mark.parametrize('occupied', ['foreign-role', 'worker-token'])
def test_initial_install_rejects_existing_execution_authority(database, occupied):
    module = runtime()
    with psycopg.connect(database) as db:
        if occupied == 'foreign-role':
            db.execute("CREATE ROLE loom_actuator LOGIN PASSWORD 'foreign-password-not-to-be-changed'")
        else:
            db.execute("INSERT INTO tokens (token_hash,type,scopes,team_id,issued_at) "
                "VALUES (%s,'worker',ARRAY['submit:batch'],NULL,now())", (b'foreign-token-hash',))
    with pytest.raises(module.DevelopmentRuntimeDatabaseError):
        install(module, database)
    with psycopg.connect(database) as db:
        assert db.execute('SELECT count(*) FROM tokens WHERE token_hash=%s', (hashlib.sha256(TOKEN.encode()).digest(),)).fetchone() == (0,)
        if occupied == 'worker-token':
            assert db.execute("SELECT oid FROM pg_roles WHERE rolname='loom_actuator'").fetchone() is None


@pytest.mark.parametrize('drift', ['excess-grant', 'missing-grant', 'revoked-token'])
def test_replay_fails_closed_on_drift_without_repair(database, drift):
    module = runtime()
    install(module, database)
    with psycopg.connect(database) as db:
        if drift == 'excess-grant':
            db.execute('GRANT SELECT ON tokens TO loom_actuator')
        elif drift == 'missing-grant':
            db.execute('REVOKE INSERT ON nebius_pool_execution_outbox FROM loom_actuator')
        else:
            db.execute("UPDATE tokens SET revoked_at=now() WHERE type='worker'")
    with pytest.raises(module.DevelopmentRuntimeDatabaseError):
        install(module, database)
    with psycopg.connect(database) as db:
        if drift == 'excess-grant':
            assert db.execute("SELECT has_table_privilege('loom_actuator','tokens','SELECT')").fetchone() == (True,)
        elif drift == 'missing-grant':
            assert db.execute("SELECT has_table_privilege('loom_actuator','nebius_pool_execution_outbox','INSERT')").fetchone() == (False,)
        else:
            assert db.execute("SELECT revoked_at IS NOT NULL FROM tokens WHERE type='worker'").fetchone() == (True,)


@pytest.mark.parametrize('grant', [
    'GRANT SELECT ON tasks TO loom_actuator WITH GRANT OPTION',
    'GRANT UPDATE (registered_at) ON tasks TO loom_actuator WITH GRANT OPTION',
    'ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO loom_actuator',
])
def test_replay_rejects_delegation_and_future_table_authority(database, grant):
    module = runtime()
    install(module, database)
    with psycopg.connect(database) as db:
        db.execute(grant)
    with pytest.raises(module.DevelopmentRuntimeDatabaseError):
        install(module, database)


def test_fixed_job_command_emits_only_receipt_and_refuses_staging(database, monkeypatch, tmp_path, capsys):
    module = runtime()
    config = {'namespace': 'loom-dev', 'operation_id': str(OPERATION), 'schema_revision': '0175'}
    path = tmp_path / 'runtime.json'
    path.write_text(json.dumps(config))
    monkeypatch.setenv('LOOM_DEVELOPMENT_RUNTIME_CONFIG', str(path))
    monkeypatch.setenv('LOOM_DB_URL', database)
    monkeypatch.setenv('LOOM_DB_ACTUATOR_PASSWORD', PASSWORD)
    monkeypatch.setenv('LOOM_BATCH_RUNNER_TOKEN', TOKEN)
    monkeypatch.setattr(module, 'database_url', lambda value, namespace: value if namespace == 'loom-dev' else None)
    assert module.main() == 0
    receipt = json.loads(capsys.readouterr().out)
    assert receipt['status'] == 'development_runtime_database_installed'
    assert receipt['operation_id'] == str(OPERATION)
    assert PASSWORD not in repr(receipt) and TOKEN not in repr(receipt)
    config['namespace'] = 'loom-nebius-platform'
    path.write_text(json.dumps(config))
    assert module.main() == 1
    assert json.loads(capsys.readouterr().out) == {'status': 'development_runtime_database_unqualified'}
