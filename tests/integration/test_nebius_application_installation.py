"""Protected configuration starts the concrete personal worker in management."""
from __future__ import annotations

import asyncio
import base64
import json
from uuid import uuid4

import httpx
import pytest
from psycopg.conninfo import make_conninfo

from loom.nebius_application_authority import ApplicationNamespaceAuthorityV1
from loom_service.app import create_app
from loom_service.config import LoomServiceSettings
from loom_service.environment_management.installation import ManagementInstallation
from tests.integration.test_nebius_management_installation import (
    installation_file as installation_file,
)
from tests.unit.test_nebius_application_render import inputs
from tests.unit.test_nebius_kubernetes import FakeSDK
from tests.unit.test_nebius_kubernetes import connection as connection
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


@pytest.fixture
def application_installation(installation_file, connection, platform_inputs, tmp_path):
    _, release, shared, foundation = inputs(platform_inputs)
    token = tmp_path / 'application-token'
    token.write_text('explicit-application-token')
    token.chmod(0o440)
    material = tmp_path / 'shared-material.json'
    material.write_text(json.dumps({'ca_pem': connection.ca_file.read_text(), 'database_name': 'loom',
        'secret_store_master_keys': base64.b64encode(b's' * 32).decode()}))
    material.chmod(0o640)
    dsn = tmp_path / 'shared-manager-dsn'
    dsn.write_text(make_conninfo(host=f'loom-postgres.{shared.platform_namespace}.svc', port='5432',
        dbname='loom', user='loom_application_manager', password='fixture-private-value',
        sslmode='verify-full', sslrootcert=str(connection.ca_file)))
    dsn.chmod(0o640)
    data = json.loads(installation_file.read_text())
    data['foundation'] = foundation.model_dump(mode='json') | {'provisioning_project_id': 'project-managed-storage'}
    data['applications'] = {
        'shared': shared.model_dump(mode='json'), 'releases': [release.model_dump(mode='json')],
        'authority': ApplicationNamespaceAuthorityV1(installation_id=uuid4(), namespace='loom-nebius-management',
            cluster_id=shared.cluster_id, data_environment_id=shared.data_environment_id,
            shared_namespace=shared.platform_namespace).model_dump(mode='json'),
        'storage': {'data_environment_id': str(shared.data_environment_id), 'project_id': 'project-managed-storage',
            'data_group_id': 'group-shared-data', 'source_group_id': 'group-shared-source'},
        'runtime': {'kubernetes': {'kind': 'projected_service_account', 'endpoint': connection.endpoint,
            'ca_file': str(connection.ca_file), 'token_file': str(token)},
            'cloud_credentials_file': str(connection.credentials_file), 'database_connection_file': str(dsn),
            'shared_credentials_file': str(material), 'poll_seconds': 1},
    }
    installation_file.write_text(json.dumps(data))
    return installation_file, data


def test_application_installation_preserves_exact_protected_bindings(application_installation):
    path, data = application_installation
    installation = ManagementInstallation.load(path)
    assert installation.applications.shared.data_environment_id == installation.applications.storage.data_environment_id
    assert installation.applications.releases[0].model_dump(mode='json') == data['applications']['releases'][0]
    assert installation.provider_runtime is None


@pytest.mark.parametrize('damage', ['data', 'namespace', 'project', 'duplicate-release', 'ambient-kube', 'dual-runtime'])
def test_application_installation_rejects_mismatched_or_combined_authority(application_installation, damage, connection):
    path, data = application_installation
    config = data['applications']
    if damage == 'data':
        config['storage']['data_environment_id'] = str(uuid4())
    elif damage == 'namespace':
        config['authority']['shared_namespace'] = 'foreign-development'
    elif damage == 'project':
        config['storage']['project_id'] = 'foreign-project'
    elif damage == 'duplicate-release':
        config['releases'] *= 2
    elif damage == 'ambient-kube':
        config['runtime']['kubernetes'] = connection.model_dump(mode='json')
    else:
        data['provider_runtime'] = {'kubernetes': connection.model_dump(mode='json'),
            'cloud_credentials_file': str(connection.credentials_file)}
    path.write_text(json.dumps(data))
    with pytest.raises(ValueError, match='invalid_environment_management_installation'):
        ManagementInstallation.load(path)


@pytest.mark.parametrize('damage', ['insecure-db', 'wrong-db-host', 'missing', 'public', 'oversized', 'bad-keyring', 'bad-ca'])
def test_runtime_material_rejects_unsafe_inputs_without_echoing_secrets(application_installation, damage):
    from loom_service.application_management.installation import ApplicationInstallation

    _, data = application_installation
    installation = ApplicationInstallation.model_validate(data['applications'])
    runtime = installation.runtime
    if damage in {'insecure-db', 'wrong-db-host'}:
        value = runtime.database_connection_file.read_text()
        value = value.replace('sslmode=verify-full', 'sslmode=disable') if damage == 'insecure-db' else value.replace(
            'loom-postgres.', 'foreign-postgres.')
        runtime.database_connection_file.write_text(value)
    elif damage == 'missing':
        runtime.database_connection_file.unlink()
    elif damage == 'public':
        runtime.database_connection_file.chmod(0o644)
    elif damage == 'oversized':
        runtime.shared_credentials_file.write_text('fixture-private-value' * 100000)
    else:
        value = json.loads(runtime.shared_credentials_file.read_text())
        value['secret_store_master_keys' if damage == 'bad-keyring' else 'ca_pem'] = 'fixture-private-value'
        runtime.shared_credentials_file.write_text(json.dumps(value))
    with pytest.raises(ValueError, match='invalid_application_runtime_material') as caught:
        installation.load_credentials()
    assert 'fixture-private-value' not in str(caught.value)


async def test_configured_management_starts_one_application_worker_and_supervises_it(
        application_installation, isolated_migration_postgres_url, monkeypatch):
    import nebius.sdk

    from loom.db.schema import Team, TeamMembership, User
    from loom_service.application_management.worker import ApplicationWorker
    from loom_service.password_auth import hash_password

    path, data = application_installation
    created = []

    def sdk_factory(**kwargs):
        assert kwargs['credentials_file_name'] == data['applications']['runtime']['cloud_credentials_file']
        instance = FakeSDK()
        created.append(instance)
        return instance

    monkeypatch.setattr(nebius.sdk, 'SDK', sdk_factory)
    run = ApplicationWorker.run

    async def supervised_run(worker, **kwargs):
        try:
            await run(worker, **kwargs)
        finally:
            assert not worker.coordinator.runtime.kubernetes.http.is_closed
            assert not worker.coordinator.object_verifier.http.is_closed
            assert not created[0].closed

    monkeypatch.setattr(ApplicationWorker, 'run', supervised_run)
    app = create_app(LoomServiceSettings(_env_file=None, service_mode='management',
        db_url=isolated_migration_postgres_url, environment_management_config_file=path,
        environment_management_github_token='fixture-token', auth_local_http=False,
        public_base_url='https://management.example.com'))
    async with app.router.lifespan_context(app):
        runtime = app.state.application_runtime
        assert app.state.application_login.registry is app.state.application_manager.registry
        assert not hasattr(app.state, 'environment_runtime')
        assert runtime.worker.registry is app.state.application_manager.registry
        async with asyncio.timeout(5):
            while not runtime.ready:
                await asyncio.sleep(0.01)
        team, owner = uuid4(), uuid4()
        async with app.state.session_factory.begin() as session:
            session.add(Team(id=team, name='application-owners'))
            session.add(User(id=owner, username='alice', username_normalized='alice', status='active',
                password_hash=hash_password('fixture-owner-passphrase')))
            await session.flush()
            session.add(TeamMembership(team_id=team, user_id=owner, role='owner'))
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='https://management.example.com') as client:
            login = await client.post('/api/v1/auth/login', json={'username': 'alice', 'password': 'fixture-owner-passphrase'})
            assert login.status_code == 200
            assert (await client.get('/api/v1/applications')).json() == {'items': []}
            ready = await client.get('/api/v1/health/ready')
            assert ready.status_code == 200 and ready.json()['application_provisioner'] == 'ready'
            capabilities = await client.get('/api/v1/application-capabilities')
            assert capabilities.status_code == 200, capabilities.text
            assert capabilities.json()['application_lifecycle'] == 'worker_healthy'
            assert capabilities.json()['source_upload'] == 'not_configured'
            assert capabilities.json()['image_builds'] == 'not_configured'
            assert capabilities.json()['execution'] == 'not_checked'
            runtime.task.cancel()
            await asyncio.gather(runtime.task, return_exceptions=True)
            stopped = await client.get('/api/v1/health/ready')
            assert stopped.status_code == 503 and stopped.json()['application_provisioner'] == 'not-ready'
            capabilities = await client.get('/api/v1/application-capabilities')
            assert capabilities.json()['application_lifecycle'] == 'worker_unhealthy'
            assert capabilities.json()['execution'] == 'not_checked'
            del app.state.application_runtime
            capabilities = await client.get('/api/v1/application-capabilities')
            assert capabilities.json()['application_lifecycle'] == 'worker_unavailable'
            assert (await client.get('/api/v1/health')).status_code == 200
    assert len(created) == 1 and created[0].closed
    assert runtime.kubernetes.http.is_closed and runtime.object_verifier.http.is_closed
    assert runtime.task.cancelled() and not runtime.ready
    for attribute in ('application_runtime', 'application_manager', 'application_login', 'session_factory'):
        assert not hasattr(app.state, attribute)


async def test_failed_runtime_startup_never_exposes_owner_admission_and_closes_sdk(
        application_installation, isolated_migration_postgres_url, monkeypatch):
    import nebius.sdk

    path, _ = application_installation
    sdk = FakeSDK()
    sdk.failure = True
    monkeypatch.setattr(nebius.sdk, 'SDK', lambda **kwargs: sdk)
    app = create_app(LoomServiceSettings(_env_file=None, service_mode='management',
        db_url=isolated_migration_postgres_url, environment_management_config_file=path,
        environment_management_github_token='fixture-token'))
    with pytest.raises(ValueError, match='invalid_application_provider_credentials'):
        async with app.router.lifespan_context(app):
            pytest.fail('started with unusable provider credentials')
    assert sdk.closed
    for attribute in ('application_runtime', 'application_manager', 'application_login', 'session_factory'):
        assert not hasattr(app.state, attribute)


async def test_omitted_application_config_does_not_start_runtime(installation_file, isolated_migration_postgres_url):
    app = create_app(LoomServiceSettings(_env_file=None, service_mode='management',
        db_url=isolated_migration_postgres_url, environment_management_config_file=installation_file,
        environment_management_github_token='fixture-token'))
    async with app.router.lifespan_context(app):
        assert not hasattr(app.state, 'application_manager') and not hasattr(app.state, 'application_runtime')
