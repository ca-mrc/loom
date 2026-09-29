"""Protected SQL setup reuses credentials, preserves data and installs narrow grants."""
from __future__ import annotations

import json
import os
import subprocess
import sys
from secrets import token_urlsafe
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from sqlalchemy.engine import make_url

from loom.nebius_application_database import ApplicationDatabaseAccess
from loom.nebius_application_schema import APPLICATION_SCHEMA_LOCK
from tests.integration.test_nebius_application_database import access_postgres as access_postgres


@pytest.fixture
def shared_database(access_postgres):
    name = 'setup_' + uuid4().hex
    with psycopg.connect(access_postgres.render_as_string(hide_password=False), autocommit=True) as root:
        root.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(name)))
    url = access_postgres.set(database=name).render_as_string(hide_password=False)
    with psycopg.connect(url, autocommit=True) as admin:
        admin.execute('CREATE TABLE public.alembic_version(version_num text PRIMARY KEY)')
        admin.execute("INSERT INTO public.alembic_version VALUES ('test_revision')")
        admin.execute('CREATE TABLE public.shared_records(id bigserial PRIMARY KEY, value text NOT NULL)')
        admin.execute("INSERT INTO public.shared_records(value) VALUES ('retained')")
        yield url, admin, uuid4(), token_urlsafe(48)


def test_setup_replays_without_rotating_login_or_copying_shared_data(shared_database):
    from loom.nebius_application_database_install import install_shared_manager

    url, admin, data, password = shared_database
    role = install_shared_manager(url, data_environment_id=data, schema_revision='test_revision', manager_password=password)
    before = admin.execute('SELECT oid,rolpassword FROM pg_authid WHERE rolname=%s', (role,)).fetchone()
    assert role == 'loom_app_manager_' + data.hex
    assert install_shared_manager(url, data_environment_id=data, schema_revision='test_revision', manager_password=password) == role
    assert admin.execute('SELECT oid,rolpassword FROM pg_authid WHERE rolname=%s', (role,)).fetchone() == before
    manager_url = make_url(url).set(username=role, password=password).render_as_string(hide_password=False)
    with psycopg.connect(manager_url, autocommit=True) as manager:
        for statement in ('SELECT * FROM public.shared_records', 'CREATE TABLE public.forbidden(id int)',
                          'CREATE ROLE forbidden LOGIN'):
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                manager.execute(statement)
        app_password = token_urlsafe(48)
        app_role = ApplicationDatabaseAccess(manager, data).grant(uuid4(), uuid4(), 1, app_password,
            schema_revision='test_revision')
    app_url = make_url(url).set(username=app_role, password=app_password).render_as_string(hide_password=False)
    with psycopg.connect(app_url, autocommit=True) as application:
        assert application.execute('SELECT value FROM public.shared_records').fetchall() == [('retained',)]
        application.execute("INSERT INTO public.shared_records(value) VALUES ('from-application')")
    assert admin.execute('SELECT value FROM public.shared_records ORDER BY id').fetchall() == [
        ('retained',), ('from-application',)]


def test_wrong_schema_creates_no_manager_role(shared_database):
    from loom.nebius_application_database_install import (
        ApplicationDatabaseInstallError,
        install_shared_manager,
    )

    url, admin, data, password = shared_database
    with pytest.raises(ApplicationDatabaseInstallError):
        install_shared_manager(url, data_environment_id=data, schema_revision='wrong_revision', manager_password=password)
    assert admin.execute('SELECT oid FROM pg_roles WHERE rolname=%s', ('loom_app_manager_' + data.hex,)).fetchone() is None
    assert admin.execute("SELECT to_regnamespace('loom_application_access')").fetchone() == (None,)


@pytest.mark.parametrize('damage', ['password', 'privileges'])
def test_existing_manager_mismatch_is_rejected_without_rotation_or_adoption(shared_database, damage):
    from loom.nebius_application_database_install import (
        ApplicationDatabaseInstallError,
        install_shared_manager,
    )

    url, admin, data, password = shared_database
    role = 'loom_app_manager_' + data.hex
    retained = token_urlsafe(48) if damage == 'password' else password
    admin.execute(sql.SQL('CREATE ROLE {} LOGIN NOINHERIT PASSWORD {}').format(sql.Identifier(role), sql.Literal(retained)))
    if damage == 'privileges':
        admin.execute(sql.SQL('GRANT SELECT ON public.shared_records TO {}').format(sql.Identifier(role)))
    before = admin.execute('SELECT oid,rolpassword FROM pg_authid WHERE rolname=%s', (role,)).fetchone()
    with pytest.raises(ApplicationDatabaseInstallError) as error:
        install_shared_manager(url, data_environment_id=data, schema_revision='test_revision', manager_password=password)
    assert password not in str(error.value) and url not in str(error.value)
    assert admin.execute('SELECT oid,rolpassword FROM pg_authid WHERE rolname=%s', (role,)).fetchone() == before
    assert admin.execute("SELECT to_regnamespace('loom_application_access')").fetchone() == (None,)


def test_interrupted_setup_reuses_the_retained_manager_credential(shared_database, monkeypatch):
    from loom import nebius_application_database_install as setup

    url, admin, data, password = shared_database
    original = setup.install_application_database_access

    def interrupted(*args, **kwargs):
        raise RuntimeError('private-installation-value')

    monkeypatch.setattr(setup, 'install_application_database_access', interrupted)
    with pytest.raises(setup.ApplicationDatabaseInstallError, match='application_database_setup_failed'):
        setup.install_shared_manager(url, data_environment_id=data, schema_revision='test_revision', manager_password=password)
    role = 'loom_app_manager_' + data.hex
    before = admin.execute('SELECT oid,rolpassword FROM pg_authid WHERE rolname=%s', (role,)).fetchone()
    assert before is not None
    monkeypatch.setattr(setup, 'install_application_database_access', original)
    assert setup.install_shared_manager(url, data_environment_id=data, schema_revision='test_revision', manager_password=password) == role
    assert admin.execute('SELECT oid,rolpassword FROM pg_authid WHERE rolname=%s', (role,)).fetchone() == before


def test_setup_does_not_race_a_shared_schema_migration(shared_database):
    from loom.nebius_application_database_install import (
        ApplicationDatabaseInstallError,
        install_shared_manager,
    )

    url, admin, data, password = shared_database
    with admin.transaction():
        admin.execute('SELECT pg_advisory_xact_lock(%s)', (APPLICATION_SCHEMA_LOCK,))
        with pytest.raises(ApplicationDatabaseInstallError):
            install_shared_manager(url, data_environment_id=data, schema_revision='test_revision', manager_password=password)
        assert admin.execute('SELECT oid FROM pg_roles WHERE rolname=%s', ('loom_app_manager_' + data.hex,)).fetchone() is None


def test_setup_never_recreates_a_lost_bound_manager_identity(shared_database):
    from loom.nebius_application_database_install import (
        ApplicationDatabaseInstallError,
        install_shared_manager,
    )

    url, admin, data, password = shared_database
    role = install_shared_manager(url, data_environment_id=data, schema_revision='test_revision', manager_password=password)
    admin.execute(sql.SQL('DROP OWNED BY {}').format(sql.Identifier(role)))
    admin.execute(sql.SQL('DROP ROLE {}').format(sql.Identifier(role)))
    with pytest.raises(ApplicationDatabaseInstallError):
        install_shared_manager(url, data_environment_id=data, schema_revision='test_revision', manager_password=password)
    assert admin.execute('SELECT oid FROM pg_roles WHERE rolname=%s', (role,)).fetchone() is None


def test_fixed_setup_entrypoint_installs_real_manager_from_protected_inputs(shared_database, monkeypatch, tmp_path, capsys):
    from loom import nebius_application_database_install as setup

    url, admin, data, password = shared_database
    config = tmp_path / 'setup.json'
    config.write_text(json.dumps({'namespace': 'loom-nebius-platform', 'data_environment_id': str(data),
        'schema_revision': 'test_revision'}))
    monkeypatch.setenv('LOOM_APPLICATION_SETUP_CONFIG', str(config))
    monkeypatch.setenv('LOOM_DB_URL', url)
    monkeypatch.setenv('LOOM_APPLICATION_MANAGER_PASSWORD', password)
    # This disposable database is host-exposed; the protected Job renderer must
    # supply the existing namespace-local verify-full route in installed use.
    monkeypatch.setattr(setup, 'database_url', lambda value, namespace: url, raising=False)
    assert setup.main() == 0
    assert json.loads(capsys.readouterr().out) == {'status': 'application_database_installed'}
    assert admin.execute('SELECT manager_role FROM loom_application_access.binding').fetchone() == (
        'loom_app_manager_' + data.hex,)


@pytest.mark.parametrize('damage', ['missing', 'malformed', 'extra', 'oversized', 'foreign-route'])
def test_setup_module_rejects_unqualified_inputs_without_leaking_secrets(tmp_path, damage):
    path = tmp_path / 'setup.json'
    value = {'namespace': 'loom-nebius-platform', 'data_environment_id': str(uuid4()), 'schema_revision': 'test_revision'}
    if damage == 'extra':
        value['sql'] = 'secret-must-not-appear'
    if damage != 'missing':
        path.write_text('secret-must-not-appear' if damage == 'malformed' else 'x' * 16385 if damage == 'oversized'
                        else json.dumps(value))
    result = subprocess.run([sys.executable, '-m', 'loom.nebius_application_database_install'],
        env=os.environ | {'LOOM_APPLICATION_SETUP_CONFIG': str(path),
            'LOOM_DB_URL': 'postgresql://secret-must-not-appear@foreign.invalid/loom',
            'LOOM_APPLICATION_MANAGER_PASSWORD': 'secret-must-not-appear'},
        capture_output=True, text=True, timeout=10)
    assert result.returncode == 1
    assert json.loads(result.stdout) == {'status': 'application_database_setup_failed'}
    assert result.stderr == '' and 'secret-must-not-appear' not in result.stdout
