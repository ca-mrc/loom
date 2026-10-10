"""Fixed dev catalog API setup; response loss never repeats the POST."""
from __future__ import annotations

import copy
import importlib
import json
from pathlib import Path
from uuid import UUID

import httpx
import pytest

from loom.execution_contract import ExecutionClassV1, ExecutionTargetV1
from loom.nebius_platform_render import build_platform
from loom.pipeline.keys import canonical_digest
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


@pytest.fixture
def catalog_request(platform_inputs):
    config, candidate, profile = platform_inputs
    files = build_platform(config, candidate, profile, {}, repo_root=Path(__file__).parents[2])
    catalog = json.loads(files['10-config-network.yaml'][0]['data']['catalog.json'])
    target, = catalog['topology']['targets']
    target.update(environment='development', namespace_name='loom-nebius-dev-execution')
    return {'schema_version': 'loom.development-runtime-catalog.v1',
        'operation_id': 'aecc7407-b7b8-4c38-8d1f-bca5dca9840f', **catalog}


def module():
    name = 'loom.nebius_development_catalog'
    if importlib.util.find_spec(name) is None:
        pytest.fail('fixed development catalog command is missing')
    return importlib.import_module(name)


class CatalogServer:
    def __init__(self, request, *, loss=None):
        self.request, self.loss, self.saved = request, loss, None
        self.calls = []

    def observed(self):
        execution_class = ExecutionClassV1.model_validate(self.request['execution_class']).model_dump(mode='json')
        target = ExecutionTargetV1.model_validate(self.request['topology']['targets'][0]).model_dump(mode='json')
        return {'execution_class': execution_class, 'execution_class_sha256': canonical_digest(execution_class),
            'class_enabled': True, 'class_retired_at': None, 'target': target, 'target_sha256': canonical_digest(target),
            'desired_state': 'disabled', 'observed_state': 'unknown', 'health_status': 'unknown',
            'health_observed_at': None, 'health_error_code': None}

    def handle(self, message):
        self.calls.append((message.method, message.url.path))
        assert str(message.url).startswith('http://loom-control-plane.loom-dev.svc:8080/')
        assert message.headers['Authorization'] == 'Bearer loom_admin_' + 'e' * 64
        if message.method == 'GET':
            assert message.url.path == '/admin/service-execution/catalog/' + self.request['topology']['targets'][0]['target_id']
            return httpx.Response(404) if self.saved is None else httpx.Response(200, json=self.saved)
        assert message.method == 'POST' and message.url.path == '/admin/service-execution/catalog'
        assert json.loads(message.content) == {key: self.request[key] for key in ('execution_class', 'topology')}
        if self.loss == 'before':
            raise httpx.ReadTimeout('secret-token-must-not-be-exposed')
        self.saved = self.observed()
        if self.loss == 'after':
            raise httpx.ReadTimeout('secret-token-must-not-be-exposed')
        return httpx.Response(200, json={'execution_class_id': self.request['execution_class']['class_id'],
            'logical_pool_id': 'nebius-cpu', 'target_ids': [self.request['topology']['targets'][0]['target_id']]})


def install(request, server):
    with httpx.Client(transport=httpx.MockTransport(server.handle), follow_redirects=False) as client:
        return module().install_catalog(request, token='loom_admin_' + 'e' * 64, client=client)


@pytest.mark.parametrize('loss', [None, 'after'])
def test_catalog_create_and_replay_have_one_post(catalog_request, loss):
    server = CatalogServer(catalog_request, loss=loss)
    receipt = install(catalog_request, server)
    assert receipt['operation_id'] == str(UUID(catalog_request['operation_id']))
    assert receipt['target_id'] == catalog_request['topology']['targets'][0]['target_id']
    assert receipt['catalog_sha256'] == canonical_digest(catalog_request)
    assert install(catalog_request, server) == receipt
    assert [method for method, _ in server.calls].count('POST') == 1


def test_catalog_unresolved_post_is_not_repeated(catalog_request):
    server = CatalogServer(catalog_request, loss='before')
    with pytest.raises(ValueError, match='development catalog unqualified') as caught:
        install(catalog_request, server)
    assert 'secret-token' not in str(caught.value)
    assert [method for method, _ in server.calls] == ['GET', 'POST', 'GET']


@pytest.mark.parametrize('damage', ['class', 'target', 'digest', 'enabled', 'active', 'health'])
def test_catalog_drift_is_not_repaired(catalog_request, damage):
    server = CatalogServer(catalog_request)
    server.saved = server.observed()
    if damage == 'class':
        server.saved['execution_class']['class_id'] = 'foreign'
    elif damage == 'target':
        server.saved['target']['namespace_name'] = 'loom-staging'
    elif damage == 'digest':
        server.saved['target_sha256'] = 'sha256:' + 'f' * 64
    elif damage == 'enabled':
        server.saved['class_enabled'] = False
    elif damage == 'active':
        server.saved['desired_state'] = 'active'
    else:
        server.saved['health_status'] = 'healthy'
    with pytest.raises(ValueError, match='development catalog unqualified'):
        install(catalog_request, server)
    assert [method for method, _ in server.calls] == ['GET']


@pytest.mark.parametrize('damage', ['environment', 'namespace', 'operation'])
def test_catalog_rejects_other_environment_before_http(catalog_request, damage):
    request = copy.deepcopy(catalog_request)
    if damage == 'operation':
        request['operation_id'] = str(UUID(int=0))
    else:
        request['topology']['targets'][0]['environment' if damage == 'environment' else 'namespace_name'] = 'staging' if damage == 'environment' else 'loom-staging'
    server = CatalogServer(request)
    with pytest.raises(ValueError, match='development catalog unqualified'):
        install(request, server)
    assert not server.calls


@pytest.mark.parametrize('damage', [None, 'oversize', 'foreign'])
def test_catalog_command_uses_fixed_transport_and_scrubs_failure(catalog_request, tmp_path, monkeypatch, capsys, damage):
    setup = module()
    if not hasattr(setup, 'main'):
        pytest.fail('fixed development catalog CLI is missing')
    config, admin = tmp_path / 'catalog.json', tmp_path / 'admin.toml'
    body = copy.deepcopy(catalog_request)
    if damage == 'foreign':
        body['topology']['targets'][0]['environment'] = 'staging'
    config.write_text('x' * (1024**2 + 1) if damage == 'oversize' else json.dumps(body))
    admin.write_text('[admin]\ntoken = "loom_admin_' + 'e' * 64 + '"\n')
    monkeypatch.setenv('LOOM_DEVELOPMENT_RUNTIME_CATALOG_CONFIG', str(config))
    monkeypatch.setattr(setup, '_ADMIN_SECRET', admin)
    server = CatalogServer(catalog_request)
    original_client = httpx.Client
    def client(**kwargs):
        assert kwargs['trust_env'] is False and kwargs['follow_redirects'] is False
        return original_client(transport=httpx.MockTransport(server.handle), **kwargs)
    monkeypatch.setattr(setup.httpx, 'Client', client)
    assert setup.main() == (0 if damage is None else 1)
    output = capsys.readouterr()
    assert 'loom_admin_' not in output.out + output.err
    result = json.loads(output.out)
    if damage is None:
        assert result['operation_id'] == catalog_request['operation_id']
        assert result['catalog_sha256'] == canonical_digest(catalog_request)
    else:
        assert result == {'status': 'development_catalog_unqualified'}
        assert not server.calls
