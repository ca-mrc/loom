"""Installed refresh probe binds a fixed service URL and never emits secrets."""
from __future__ import annotations

import json

import pytest
from sqlalchemy.engine import URL

from loom import nebius_management_refresh_probe as probe
from tests.unit.test_nebius_application_render import inputs
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


def config(platform_inputs, mode='manager'):
    shared = inputs(platform_inputs)[2]
    return probe.RefreshProbeSettings(mode=mode, namespace='loom-nebius-management',
        expected_revision='0168' if mode == 'manager' else shared.schema_revision, shared=shared)


def db_url(settings):
    namespace = settings.namespace if settings.mode == 'manager' else settings.shared.platform_namespace
    return URL.create('postgresql', username='loom_service', password='not-a-real-password',
        host=f'loom-postgres.{namespace}.svc', port=5432, database='loom',
        query={'sslmode': 'verify-full', 'sslrootcert': '/var/run/loom-db/ca.crt'})


@pytest.mark.parametrize('mode', ['manager', 'shared'])
def test_bound_service_url_requires_exact_database_role_route_and_tls(platform_inputs, mode):
    settings = config(platform_inputs, mode)
    url = db_url(settings)
    assert probe.refresh_database_url(url.render_as_string(hide_password=False), settings) == url.set(drivername='postgresql+psycopg')
    for key, value in [('host', 'external.example'), ('username', 'loom_admin'), ('database', 'postgres'),
                       ('port', 5433), ('drivername', 'postgresql+psycopg'), ('password', ''),
                       ('query', {'sslmode': 'require'}), ('query', dict(url.query) | {'options': '-c search_path=other'})]:
        with pytest.raises(ValueError, match='refresh_probe_unqualified'):
            probe.refresh_database_url(url.set(**{key: value}).render_as_string(hide_password=False), settings)


def test_fixed_entry_emits_only_closed_summary(platform_inputs, tmp_path, monkeypatch, capsys):
    settings = config(platform_inputs)
    path = tmp_path / 'probe.json'
    path.write_text(settings.model_dump_json())
    monkeypatch.setattr(probe, 'SETTINGS_PATH', path)
    monkeypatch.setenv('LOOM_REFRESH_DB_URL', db_url(settings).render_as_string(hide_password=False))
    calls = []

    async def snapshot(url, received):
        calls.append((url, received))
        return {'schema': probe.SCHEMA, 'status': 'qualified', 'mode': 'manager',
            'revision': '0168', 'operations_checked': 2}

    monkeypatch.setattr(probe, 'database_snapshot', snapshot)
    assert probe.main() == 0
    assert json.loads(capsys.readouterr().out) == {'schema': probe.SCHEMA, 'status': 'qualified',
        'mode': 'manager', 'revision': '0168', 'operations_checked': 2}
    assert calls == [(db_url(settings).set(drivername='postgresql+psycopg'), settings)]


@pytest.mark.parametrize('fault', ['oversize', 'extra_setting', 'url', 'database'])
def test_fixed_entry_sanitizes_configuration_and_database_errors(platform_inputs, tmp_path, monkeypatch, capsys, fault):
    settings = config(platform_inputs)
    path = tmp_path / 'probe.json'
    raw = settings.model_dump_json()
    if fault == 'oversize':
        raw = ' ' * 262145 + raw
    elif fault == 'extra_setting':
        raw = json.dumps(settings.model_dump(mode='json') | {'sql': 'unapproved'})
    path.write_text(raw)
    monkeypatch.setattr(probe, 'SETTINGS_PATH', path)
    monkeypatch.setenv('LOOM_REFRESH_DB_URL', 'private-credential-value' if fault == 'url'
        else db_url(settings).render_as_string(hide_password=False))
    calls = []

    async def snapshot(url, received):
        calls.append(True)
        raise OSError('private-credential-value')

    monkeypatch.setattr(probe, 'database_snapshot', snapshot)
    assert probe.main() == 1
    assert json.loads(capsys.readouterr().out) == {'schema': probe.SCHEMA, 'status': 'unqualified'}
    assert calls == ([True] if fault == 'database' else [])
