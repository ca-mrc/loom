"""Foundation readiness reaches the real authenticated API and PostgreSQL."""
from __future__ import annotations

import json
import tomllib

import httpx
import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.admin_secret import AdminSecretVerifier
from loom_service.app import create_app
from loom_service.config import LoomServiceSettings
from loom_service.environment_management.kubernetes_provider import KubernetesEnvironmentProvider
from tests.ops.test_nebius_environment_foundation import Remote, prepare, run
from tests.ops.test_nebius_foundation_readiness import database_controller
from tests.support.nebius_platform import platform_inputs as platform_inputs


async def test_real_api_dependency_proof_requires_own_credentials_and_configured_storage(
        tmp_path, platform_inputs, isolated_migration_postgres_url, monkeypatch):
    from scripts.ops.nebius_foundation_readiness import observe_foundation

    state = tmp_path / 'foundation'
    reference = prepare(state, platform_inputs)
    remote = Remote(state)
    await run(state, reference, remote)
    database_controller(remote)
    writes = list(remote.writes)
    material = json.loads((state / 'material.json').read_text())
    token = tomllib.loads(material['loom-admin-secret']['secrets.toml'])['admin']['token']
    monkeypatch.setenv('LOOM_ENV', 'development')
    monkeypatch.setenv('LOOM_NAMESPACE', 'app-red')
    settings = LoomServiceSettings(_env_file=None, db_url=isolated_migration_postgres_url,
        minio_access_key='test-only', minio_secret_key='test-only',
        artifacts_bucket='red-canonical', trajectories_bucket='red-canonical')
    app = create_app(settings)
    engine = create_async_engine(isolated_migration_postgres_url)
    app.state.settings = settings
    app.state.session_factory = async_sessionmaker(engine, expire_on_commit=False)
    app.state.admin_secret_verifier = AdminSecretVerifier.from_token(token)

    class Store:
        unavailable = False

        def list_objects_v2(self, **request):
            assert request == {'Bucket': 'red-canonical', 'MaxKeys': 1}
            if self.unavailable:
                raise OSError('sensitive-provider-message')
            return {'ResponseMetadata': {'HTTPStatusCode': 200}}

    store = Store()
    app.state.minio_client = store
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app)) as public, \
                httpx.AsyncClient(base_url='https://api.cluster.test', transport=httpx.MockTransport(remote.http)) as kube:
            async def observe():
                return await observe_foundation(state_dir=state, reference=reference,
                    kubernetes=KubernetesEnvironmentProvider(kube), cloud=remote, public_http=public)
            assert (await observe()).foundation.identity == reference.identity
            app.state.admin_secret_verifier = AdminSecretVerifier.from_token('loom_admin_' + 'other' * 16)
            with pytest.raises(ValueError, match='foundation_readiness_unqualified'):
                await observe()
            app.state.admin_secret_verifier = AdminSecretVerifier.from_token(token)
            store.unavailable = True
            with pytest.raises(ValueError, match='foundation_readiness_unqualified') as error:
                await observe()
            assert 'sensitive' not in str(error.value)
    finally:
        await engine.dispose()
    assert remote.writes == writes
