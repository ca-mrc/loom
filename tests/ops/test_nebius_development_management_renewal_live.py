"""Exercise actual HTTPS renewal/Secret requests with external API responses."""
from __future__ import annotations

import copy
import importlib
import json
import ssl
from dataclasses import replace
from uuid import uuid4

import httpx
import pytest
from tests.ops.test_nebius_development_management_renewal import (
    application_management_inputs as application_management_inputs,
)
from tests.ops.test_nebius_development_management_renewal import (
    application_material as application_material,
)
from tests.ops.test_nebius_development_management_renewal import capacity_checks as capacity_checks
from tests.ops.test_nebius_development_management_renewal import cloud as cloud
from tests.ops.test_nebius_development_management_renewal import installation as installation
from tests.ops.test_nebius_development_management_renewal import inventory as inventory
from tests.ops.test_nebius_development_management_renewal import (
    management_inputs as management_inputs,
)
from tests.ops.test_nebius_development_management_renewal import manager_entry as manager_entry
from tests.ops.test_nebius_development_management_renewal import material as material
from tests.ops.test_nebius_development_management_renewal import platform_inputs as platform_inputs
from tests.ops.test_nebius_development_management_renewal import provider_checks as provider_checks
from tests.ops.test_nebius_development_management_renewal import renewal as renewal
from tests.ops.test_nebius_development_management_renewal import retained as retained
from tests.ops.test_nebius_development_management_renewal import route as route
from tests.ops.test_nebius_development_management_renewal import tls_material as tls_material


class Server:
    def __init__(self, request, ingress):
        self.binding = request.retained.binding
        self.ingress = ingress
        self.secret = None
        self.calls = []
        self.failure = None
        self.identity = True
        self.on_preview = lambda: None
        self.on_secret_preview = lambda: None

    def handle(self, message):
        self.calls.append(message)
        namespace = self.binding.namespace
        ingress_path = '/apis/networking.k8s.io/v1/namespaces/' + namespace + '/ingresses/loom-management'
        secret_path = '/api/v1/namespaces/' + namespace + '/secrets'
        path = message.url.path
        if path in {'/api/v1/namespaces/kube-system', '/api/v1/namespaces/' + namespace}:
            assert message.method == 'GET'
            name = path.rsplit('/', 1)[-1]
            uid = self.binding.kube_system_uid if name == 'kube-system' else self.binding.namespace_uid
            return httpx.Response(200, json={'apiVersion': 'v1', 'kind': 'Namespace', 'metadata': {
                'name': name, 'uid': uid if self.identity else str(uuid4()),
                'labels': {'loom.nebius/management-installation': self.binding.installation_id,
                           'pod-security.kubernetes.io/enforce': 'restricted'}}})
        if path.startswith(secret_path + '/'):
            assert message.method == 'GET'
            return httpx.Response(200, json=self.secret) if self.secret else httpx.Response(404)
        if path == secret_path:
            assert message.method == 'POST'
            value = json.loads(message.content)
            assert value['immutable'] is True and value['type'] == 'kubernetes.io/tls'
            if message.url.query == b'dryRun=All':
                self.on_secret_preview()
                return httpx.Response(201, json=value)
            assert not message.url.query
            if self.failure == 'secret-before':
                raise httpx.ReadTimeout('external response unavailable')
            self.secret = value
            value['metadata'].update(uid=str(uuid4()), resourceVersion='15')
            if self.failure == 'secret-after':
                raise httpx.ReadTimeout('external response unavailable')
            return httpx.Response(201, json=value)
        assert path == ingress_path
        if message.method == 'GET':
            return httpx.Response(200, json=self.ingress)
        assert message.method == 'PATCH'
        assert message.headers['Content-Type'] == 'application/json-patch+json'
        patches = json.loads(message.content)
        assert patches[:3] == [
            {'op': 'test', 'path': '/metadata/uid', 'value': self.ingress['metadata']['uid']},
            {'op': 'test', 'path': '/metadata/resourceVersion', 'value': self.ingress['metadata']['resourceVersion']},
            {'op': 'test', 'path': '/spec', 'value': self.ingress['spec']}]
        assert len(patches) == 4 and patches[3]['op'] == 'replace'
        assert patches[3]['path'] == '/spec/tls/0/secretName'
        value = copy.deepcopy(self.ingress)
        value['spec']['tls'][0]['secretName'] = patches[3]['value']
        if message.url.query == b'dryRun=All':
            self.on_preview()
            return httpx.Response(200, json=value)
        assert not message.url.query
        if self.failure in {403, 409, 422}:
            return httpx.Response(self.failure, json={'apiVersion': 'v1', 'kind': 'Status',
                'status': 'Failure', 'code': self.failure,
                'reason': {403: 'Forbidden', 409: 'Conflict', 422: 'Invalid'}[self.failure]})
        if self.failure == 'malformed-conflict':
            return httpx.Response(409, json={'kind': 'Status', 'reason': 'Conflict'})
        if self.failure == 'redirect':
            return httpx.Response(307, headers={'Location': 'https://foreign.example.com'})
        if self.failure == 'before':
            raise httpx.ReadTimeout('external response unavailable')
        self.ingress.clear()
        self.ingress.update(value)
        self.ingress['metadata']['resourceVersion'] = str(int(value['metadata']['resourceVersion']) + 1)
        if self.failure == 'after':
            raise httpx.ReadTimeout('external response unavailable')
        return httpx.Response(200, json=value)

    def writes(self, method):
        return [call for call in self.calls if call.method == method and not call.url.query]


@pytest.fixture
def live(renewal, route, tmp_path):
    from loom.nebius_platform_render import digest

    module = importlib.import_module('scripts.ops.nebius_development_management_renewal_live')
    request = renewal[0]
    router, _, resources, _, _, _, ingresses = route
    foundation = request.retained.inputs.deployment.installation.foundation
    resources['controller']['spec']['template']['metadata']['labels'][
        'app.kubernetes.io/name'] = foundation.ingress_controller_label
    router.settings = router.settings.model_copy(update={
        'controller_spec_digest': digest(resources['controller']['spec'])})
    ingress = copy.deepcopy(request.retained.ingress)
    ingress['metadata']['resourceVersion'] = '10'
    ingresses.append(ingress)
    server = Server(request, ingress)
    private = tmp_path / 'captured-private-input'
    private.write_bytes(b'original')
    private.chmod(0o600)
    api = module.HTTPSDevelopmentManagementRenewalAPI(request=request, route=router,
        api_server=router.api_server, ssl_context=ssl.create_default_context(), token='operator',
        private_files={private: b'original'})
    api.client.close()
    api.client = httpx.Client(base_url=api.api_server, transport=httpx.MockTransport(server.handle))
    with api:
        yield request, api, server, private


def run(live, *, execute=True):
    from scripts.ops.nebius_development_management_renewal import renew_management_tls

    return renew_management_tls(request=live[0], api=live[1], execute=execute)


def test_connected_https_renewal_preserves_route_and_replays_without_writes(live, route):
    before = copy.deepcopy(live[2].ingress)
    result = run(live)
    assert result['status'] == 'development_management_tls_renewed'
    assert run(live) == result
    server = live[2]
    assert len(server.writes('PATCH')) == len(server.writes('POST')) == 1
    current = copy.deepcopy(server.ingress)
    current['spec']['tls'] = before['spec']['tls']
    current['metadata']['resourceVersion'] = before['metadata']['resourceVersion']
    assert current == before
    assert route[5][-1] == (live[0].material.public_host, '8.8.8.8', result['fingerprint_sha256'])
    assert live[0].material.key not in json.dumps(result)


def test_connected_preflight_has_only_get_requests(live):
    assert run(live, execute=False)['status'] == 'development_management_tls_preflight_qualified'
    assert all(call.method == 'GET' for call in live[2].calls)


@pytest.mark.parametrize('failure', ['before', 'after', 'malformed-conflict', 'redirect', 403, 409, 422])
def test_http_uncertainty_is_not_retried_and_only_typed_rejection_is_terminal(live, failure):
    from scripts.ops.nebius_development_management_renewal import RenewalError

    live[2].failure = failure
    for _ in range(2):
        if failure in {'before', 'malformed-conflict', 'redirect'}:
            with pytest.raises(RenewalError, match='unresolved'):
                run(live)
        else:
            result = run(live)
            assert result['status'] == ('development_management_tls_renewed' if failure == 'after' else 'rejected')
    assert len(live[2].writes('PATCH')) == 1


@pytest.mark.parametrize('failure', ['secret-before', 'secret-after'])
def test_connected_secret_lost_reply_is_resolved_by_readback_only(live, failure):
    from scripts.ops.nebius_development_management_renewal import RenewalError

    live[2].failure = failure
    for _ in range(2):
        if failure == 'secret-before':
            with pytest.raises(RenewalError):
                run(live)
        else:
            assert run(live)['status'] == 'development_management_tls_renewed'
    assert len(live[2].writes('POST')) == 1
    assert len(live[2].writes('PATCH')) == (1 if failure == 'secret-after' else 0)


@pytest.mark.parametrize('damage', ['identity', 'private-input', 'retained-input'])
def test_changed_input_after_secret_preview_blocks_actual_secret_create(live, damage):
    from scripts.ops.nebius_development_management_renewal import RenewalError

    def change():
        if damage == 'identity':
            live[2].identity = False
        else:
            path = live[3] if damage == 'private-input' else next(iter(live[0].retained.files))
            path.write_bytes(b'changed')
    live[2].on_secret_preview = change
    with pytest.raises(RenewalError):
        run(live)
    assert not live[2].writes('POST') and not live[2].writes('PATCH')


def test_fresh_resource_version_is_used_after_dry_run(live):
    def advance():
        live[2].ingress['metadata']['resourceVersion'] = '77'
    live[2].on_preview = advance
    assert run(live)['status'] == 'development_management_tls_renewed'
    patch, = live[2].writes('PATCH')
    assert json.loads(patch.content)[1]['value'] == '77'


@pytest.mark.parametrize('damage', ['host', 'namespace', 'secret', 'metadata'])
def test_transport_rejects_any_patch_beyond_its_fixed_certificate_reference(live, damage):
    from scripts.ops.nebius_development_management_renewal import RenewalError, _target

    request, api, server, _ = live
    import hashlib
    before = copy.deepcopy(server.ingress)
    target = _target(before, request.retained.binding.installation_id,
        hashlib.sha256(request.material.chain.encode()).hexdigest())
    if damage == 'host':
        target['spec']['rules'][0]['host'] = 'foreign.example.com'
    elif damage == 'namespace':
        target['metadata']['namespace'] = 'loom-nebius-platform'
    elif damage == 'secret':
        target['spec']['tls'][0]['secretName'] = 'foreign'
    else:
        target['metadata']['labels']['foreign'] = 'true'
    with pytest.raises(RenewalError):
        api.patch(before, target)
    assert not server.writes('PATCH')


def test_public_tls_propagation_is_pending_but_late_route_drift_is_failure(live, monkeypatch):
    from scripts.ops import nebius_development_management_route as route_module
    from scripts.ops.nebius_development_management_renewal import RenewalError
    from scripts.ops.nebius_ingress_probe import ProbeError

    def pending(*args, **kwargs):
        raise ProbeError('certificate not propagated')
    monkeypatch.setattr(route_module, 'qualify_tls_address', pending)
    assert run(live)['status'] == 'pending'
    def drift(*args, **kwargs):
        live[2].ingress['metadata']['uid'] = str(uuid4())
    monkeypatch.setattr(route_module, 'qualify_tls_address', drift)
    with pytest.raises(RenewalError):
        run(live)
    assert len(live[2].writes('PATCH')) == 1


def test_same_generation_refuses_a_new_operation_without_more_writes(live):
    from scripts.ops.nebius_development_management_renewal import RenewalError

    run(live)
    changed = (replace(live[0], operation_id=uuid4()), *live[1:])
    with pytest.raises(RenewalError, match='already active'):
        run(changed)
    assert len(live[2].writes('POST')) == len(live[2].writes('PATCH')) == 1


def test_new_operation_after_definite_conflict_reuses_delivered_secret(live):
    live[2].failure = 409
    assert run(live)['status'] == 'rejected'
    live[2].failure = None
    changed = (replace(live[0], operation_id=uuid4()), *live[1:])
    live[1].request = changed[0]
    assert run(changed)['status'] == 'development_management_tls_renewed'
    assert len(live[2].writes('POST')) == 1 and len(live[2].writes('PATCH')) == 2
