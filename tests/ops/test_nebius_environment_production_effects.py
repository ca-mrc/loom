"""Production approval expires at real installed HTTP/SDK write boundaries."""
from __future__ import annotations

import asyncio
import hashlib
import json
import ssl
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
from tests.ops.test_nebius_production_promotion import promotion_fixture as promotion_fixture
from tests.unit.test_nebius_candidate_catalog import publication as publication


@pytest.fixture
def effects(tmp_path, promotion_fixture, monkeypatch):
    from nebius import sdk
    from scripts.ops import nebius_environment_legacy_foundation as foundation
    from scripts.ops import nebius_environment_lifecycle_inputs as inputs
    from scripts.ops import nebius_pool_administration_dispatch as dispatch
    from scripts.ops import nebius_production_promotion as production
    from scripts.ops.nebius_platform_namespace import PlatformNamespaceBindingV2
    from scripts.ops.nebius_pool_administration_job import PoolAdministrationRuntimeV2

    from loom.nebius_kubernetes import NebiusKubernetesConnection

    fixture = promotion_fixture
    promotion = fixture['freeze']()
    clock, writes = [100.0], []
    monkeypatch.setattr(production, 'time', SimpleNamespace(monotonic=lambda: clock[0]))
    paths = {}
    for name in ('github', 'cloud', 'ca', 'kube'):
        path = tmp_path / name
        path.write_bytes(b'test-github-secret' if name == 'github' else b'private-test-material')
        path.chmod(0o600)
        paths[name] = path
    connection = NebiusKubernetesConnection(endpoint='https://api.cluster.test', ca_file=paths['ca'], credentials_file=paths['kube'])
    runtime = PoolAdministrationRuntimeV2(binding=PlatformNamespaceBindingV2(
        installation_id=str(promotion.target.scope.installation_id), namespace='platform-services',
        namespace_uid=str(uuid4()), kube_system_uid=str(uuid4())),
        service_image='registry.example/loom-service@sha256:' + 'e' * 64,
        database_secret='ledger', database_url_key='admin-url', database_ca_key='ca.crt',
        node_selector={'platform.example/role': 'control'}, tolerations=(), region='eu-north1')
    host = dispatch.PoolAdministrationHostV2(runtime=runtime, scope=promotion.target.scope,
        connection=connection, state_root=tmp_path / 'operations', anchor_root=tmp_path / 'anchors')

    class Credentials:
        def __init__(self, *args, **kwargs):
            self.ssl_context = ssl.create_default_context()

        async def get_token(self, **kwargs):
            return 'test-token'

        def get_token_sync(self):
            return 'test-token'

        async def close(self):
            pass

        @staticmethod
        def _usable(token):
            return token

    monkeypatch.setattr(inputs, 'NebiusKubernetesCredentials', Credentials)
    monkeypatch.setattr(dispatch, 'NebiusKubernetesCredentials', Credentials)
    monkeypatch.setattr(sdk, 'SDK', Credentials)

    class CloudRemote:
        async def create(self, request, **kwargs):
            writes.append('cloud')
            return self

        async def wait(self, **kwargs):
            pass

        def successful(self):
            return True

        resource_id = 'service-account-id'

        async def get(self, *args, **kwargs):
            return self

        def to_json(self, **kwargs):
            return '{"metadata":{"id":"service-account-id"},"spec":{}}'

    cloud_type = foundation.NebiusFoundationCloud
    monkeypatch.setattr(foundation, 'NebiusFoundationCloud', lambda sdk, **kwargs:
        cloud_type(sdk, clients={'service_account': CloudRemote()}, **kwargs))
    original_client = httpx.AsyncClient

    def kube(request):
        if request.method != 'GET':
            writes.append(request.method)
        return httpx.Response(200 if request.method == 'GET' else 201, json={'ok': True})

    async def remote(request):
        if request.url.host == 'api.cluster.test':
            return kube(request)
        return await fixture['transport'].handle_async_request(request)

    monkeypatch.setattr(httpx, 'AsyncClient', lambda **kwargs:
        original_client(**dict(kwargs, transport=httpx.MockTransport(remote))))
    monkeypatch.setattr(httpx, 'HTTPTransport', lambda **kwargs: httpx.MockTransport(kube))
    def private(name):
        return inputs.PrivateFileV2(path=paths[name], sha256=hashlib.sha256(paths[name].read_bytes()).hexdigest())
    selected = inputs.LifecycleInputsV2.model_construct(github_token=private('github'), cloud_credentials=private('cloud'),
        registry_prefix=fixture['candidate']['registry_prefix'])
    policy = SimpleNamespace(host=host, application_publication=promotion.publication, manager_publication=promotion.publication,
        profiles=SimpleNamespace(image_admission_keyring=json.loads(fixture['keyring'])))

    @asynccontextmanager
    async def connected():
        async with inputs.lifecycle_clients(selected, policy, lambda: None) as clients:
            yield clients

    return SimpleNamespace(production=production, promotion=promotion, clock=clock, writes=writes,
        connected=connected, policy=policy, credentials=Credentials())


@pytest.mark.parametrize('boundary', ['kubernetes', 'cloud'])
async def test_installed_foundation_writes_require_fresh_production_authority(effects, boundary):
    row = effects
    async with row.connected() as clients:
        async def write():
            if boundary == 'kubernetes':
                return await clients.kubernetes._request('POST', '/api/v1/namespaces', body={})
            return await clients.cloud.create('service_account', {'metadata': {'parent_id': 'project', 'name': 'new'},
                'spec': {}}, idempotency_key='retained-key')

        # Read-only qualification is usable before the production contexts exist.
        assert await clients.kubernetes._request('GET', '/api/v1/namespaces') == {'ok': True}
        with pytest.raises(ValueError, match='production_promotion_required'):
            await write()
        async with row.production.production_authority(promotion=row.promotion, scope=row.policy.host.scope,
                publication=row.promotion.publication, catalog=clients.catalog):
            await write()
            assert len(row.writes) == 1
            # A later target approval must not hide an expired source approval.
            row.clock[0] += 200
            later = row.promotion.model_copy(update={'publication': row.promotion.publication.model_copy(
                update={'candidate_id': uuid4()})})
            clients.catalog.publications[later.publication.candidate_id] = later.publication
            async with row.production.production_authority(promotion=later, scope=row.policy.host.scope,
                    publication=later.publication, catalog=clients.catalog):
                row.clock[0] += 101
                assert await clients.kubernetes._request('GET', '/api/v1/namespaces') == {'ok': True}
                with pytest.raises(ValueError, match='production_promotion_required'):
                    await write()
                assert len(row.writes) == 1


@pytest.mark.parametrize('boundary', ['runtime', 'administration'])
async def test_threaded_runtime_and_administration_stop_before_expired_write(effects, boundary):
    from scripts.ops.nebius_environment_runtime_install import _Resources
    from scripts.ops.nebius_pool_administration_job import PoolAdministrationJobV2

    from loom_service.pool_management.administration import PoolAdministrationAuthorizationV2
    from loom_service.pool_management.capacity import digest
    from loom_service.pool_management.lifecycle_contracts import PoolParticipantOperationV2

    row = effects
    scope = row.policy.host.scope
    request = PoolParticipantOperationV2(schema_version='loom.pool-participant-operation.v2', operation_id=uuid4(),
        pool_id=scope.pool_id, participant_id=scope.participant_id, incarnation=scope.incarnation,
        expected_head=uuid4(), expected_binding_sha256='a' * 64, pool_authority_sha256='b' * 64, action='suspend')
    grant = PoolAdministrationAuthorizationV2(schema_version='loom.pool-administration-authorization.v2',
        authorization_id=uuid4(), **scope.model_dump(), request_sha256=digest(request.model_dump(mode='json')),
        issued_at=datetime.now(UTC), expires_at=datetime.now(UTC) + timedelta(minutes=5))
    job = PoolAdministrationJobV2(runtime=row.policy.host.runtime, request=request, authorization=grant, action='apply')

    def write():
        connection = (_Resources(policy=row.policy, documents={}, namespaces={}, qualify=lambda: None, credentials=row.credentials)
            if boundary == 'runtime' else clients.connect(job))
        with connection as api:
            # Runtime patchers use the same authenticated client directly.
            assert api.client.get('/api/v1/namespaces').status_code == 200
            api.client.patch('/apis/apps/v1/namespaces/app/deployments/loom-service', json=[])

    async with row.connected() as clients, row.production.production_authority(promotion=row.promotion, scope=scope,
            publication=row.promotion.publication, catalog=clients.catalog):
        await asyncio.to_thread(write)
        assert row.writes == ['PATCH']
        row.clock[0] += 301
        with pytest.raises(ValueError, match='production_promotion_required'):
            await asyncio.to_thread(write)
        assert row.writes == ['PATCH']
