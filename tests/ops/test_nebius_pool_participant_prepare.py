"""New preparation captures real objects; recovery never uses this capture path."""
from __future__ import annotations

import copy
import importlib
import ssl
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
from scripts.ops.nebius_ingress_stage import _key
from scripts.ops.nebius_management_transport import ManagementKubernetesTransport
from scripts.ops.nebius_pool_participant_operation import retain_participant_rollout
from scripts.ops.nebius_pool_participant_resource import _resource_path
from tests.ops.test_nebius_pool_participant_operation import catalog_inputs as catalog_inputs
from tests.ops.test_nebius_pool_participant_operation import management_inputs as management_inputs
from tests.ops.test_nebius_pool_participant_operation import operation_inputs as operation_inputs
from tests.ops.test_nebius_pool_participant_operation import platform_inputs as platform_inputs
from tests.ops.test_nebius_pool_participant_operation import release_inputs as release_inputs
from tests.ops.test_nebius_pool_participant_operation import (
    staging_attachment_inputs as staging_attachment_inputs,
)

from loom.nebius_platform_render import build_platform
from loom.service_execution_materialization import ServiceExecutionRuntimeProfileV1


def module():
    name = 'scripts.ops.nebius_pool_participant_prepare'
    assert importlib.util.find_spec(name) is not None, 'new release cannot capture a complete source-qualified baseline'
    return importlib.import_module(name)


@pytest.fixture
def preparation(operation_inputs, monkeypatch):
    from scripts.ops import nebius_shared_pool_forward_census as census

    inputs = operation_inputs
    predecessor = inputs['predecessor']
    current = {_key(row): copy.deepcopy(row) for row in inputs['current'] if row['kind'] != 'Job'}
    config = inputs['config']
    profile = ServiceExecutionRuntimeProfileV1.model_validate_json(
        current[f"ConfigMap:{config['namespace']}:loom-platform-config"]['data']['profile.json'])
    old_render = build_platform(config, predecessor.migration.registration.candidate,
        profile.model_dump(mode='json'), predecessor.migration.registration.spec.profiles.image_admission_keyring,
        repo_root=inputs['repo_root'])
    # This test begins at the completed semantic predecessor boundary. Initial
    # receipt selection is supplied explicitly, not manufactured by production.
    narrow = {'Namespace', 'Deployment', 'StatefulSet', 'CronJob', 'Service', 'ConfigMap',
        'Role', 'RoleBinding', 'ClusterRole', 'ClusterRoleBinding'}
    originals = tuple(copy.deepcopy(row) for rows in old_render.values() for row in rows if row['kind'] in narrow)
    for row in originals:
        row['metadata'].update(uid=current[_key(row)]['metadata']['uid'], resourceVersion='16')
    resources = {**current, _key(inputs['manager']): copy.deepcopy(inputs['manager']),
        _key(inputs['catalog']): copy.deepcopy(inputs['catalog'])}
    binding = predecessor.migration.registration.binding
    runtime = SimpleNamespace(resources={_key(inputs['manager']): copy.deepcopy(inputs['manager']),
        _key(inputs['catalog']): copy.deepcopy(inputs['catalog'])},
        request=SimpleNamespace(database=SimpleNamespace(manager=SimpleNamespace(deployment=SimpleNamespace(
            public_host='manage.example.test')))))
    bindings = SimpleNamespace(plan=SimpleNamespace(development=SimpleNamespace(runtime=runtime),
        staging=SimpleNamespace(attachment=SimpleNamespace(config=config, runtime_profile=profile, resources=originals))))
    selected = {key: (copy.deepcopy(row),) for key, row in current.items() if row['kind'] in {
        'Deployment', 'StatefulSet', 'CronJob', 'Role', 'RoleBinding', 'ClusterRole', 'ClusterRoleBinding'}}
    choices = ({'test_boundary': 'completed handoff receipt choices'}, selected, {}, {})

    def forward(bound, state):
        assert bound is bindings and state == predecessor.state_dir
        return copy.deepcopy(choices)

    monkeypatch.setattr(census, '_forward_options', forward)
    reads = []
    paths = {_resource_path(row): key for key, row in resources.items()}
    control = SimpleNamespace(resources=resources, reads=reads, after_read=None, choices=choices)

    def respond(request):
        assert request.method == 'GET' and not request.url.query, 'preparation must have no effects'
        path = request.url.path
        reads.append(path)
        if path == '/api/v1/namespaces/kube-system':
            value = {'apiVersion': 'v1', 'kind': 'Namespace', 'metadata': {'name': 'kube-system',
                'uid': binding.kube_system_uid, 'resourceVersion': '1'}}
        else:
            value = resources.get(paths[path])
        response = httpx.Response(404) if value is None else httpx.Response(200, json=copy.deepcopy(value))
        if control.after_read is not None:
            control.after_read(path)
        return response

    with ManagementKubernetesTransport(api_server=config['kubernetes_api_server'], ssl_context=ssl.create_default_context(),
            token='operator-test-token') as transport:
        transport.client.close()
        transport.client = httpx.Client(base_url=config['kubernetes_api_server'], transport=httpx.MockTransport(respond))
        yield dict(predecessor=predecessor, bindings=bindings, shared_state=predecessor.state_dir,
            repo_root=inputs['repo_root'], transport=transport), control


def test_new_release_captures_complete_live_baseline_then_retains_it(operation_inputs, preparation):
    options, live = preparation
    captured = module().capture_participant_release(**options)
    current = {_key(row): row for row in captured.current}
    expected = {_key(row): row for row in operation_inputs['current'] if row['kind'] != 'Job'}
    assert current == expected
    assert {'ServiceAccount', 'NetworkPolicy', 'ValidatingAdmissionPolicy', 'ValidatingAdmissionPolicyBinding'} <= {
        row['kind'] for row in captured.current}
    assert captured.config == operation_inputs['config']
    assert captured.management_origin == 'https://manage.example.test'
    assert captured.manager == operation_inputs['manager'] and captured.catalog == operation_inputs['catalog']
    retained = retain_participant_rollout(**{**operation_inputs, 'current': captured.current,
        'config': captured.config, 'manager': captured.manager, 'catalog': captured.catalog,
        'management_origin': captured.management_origin})
    assert retained.inputs.current == captured.current
    assert retained.release.refresh.before == options['predecessor'].migration.registration.spec
    assert all(live.reads.count(_resource_path(row)) == 2 for row in captured.current)
    assert all(row['kind'] not in {'Secret', 'Job'} for row in captured.current)


@pytest.mark.parametrize('damage', ['missing-account', 'account-token', 'network-policy', 'policy',
    'controller-identity', 'controller-template', 'collector-open', 'manager-catalog',
    'resource-replaced-during-read', 'history-changed', 'receipt-changed', 'endpoint'])
def test_new_release_rejects_incomplete_changed_or_foreign_baseline(operation_inputs, preparation, damage):
    options, live = preparation
    namespace = operation_inputs['config']['namespace']
    execution = operation_inputs['config']['execution_namespace']
    if damage == 'missing-account':
        del live.resources[f'ServiceAccount:{namespace}:loom-platform']
    elif damage == 'account-token':
        live.resources[f'ServiceAccount:{namespace}:loom-platform']['automountServiceAccountToken'] = True
    elif damage == 'network-policy':
        row = next(row for row in live.resources.values() if row['kind'] == 'NetworkPolicy')
        row['spec']['podSelector'] = {'matchLabels': {'foreign': 'true'}}
    elif damage == 'policy':
        row = next(row for row in live.resources.values() if row['kind'] == 'ValidatingAdmissionPolicy')
        row['spec']['validations'] = [{'expression': 'true'}]
    elif damage.startswith('controller-'):
        row = live.resources[f'Deployment:{namespace}:loom-control-plane']
        if damage == 'controller-identity':
            row['metadata']['uid'] = str(uuid4())
        else:
            row['spec']['template']['spec']['containers'][0]['image'] = 'foreign@sha256:' + 'f' * 64
    elif damage == 'collector-open':
        live.resources[f'CronJob:{execution}:loom-execution-capacity-collector']['spec']['suspend'] = False
    elif damage == 'manager-catalog':
        live.resources[_key(operation_inputs['catalog'])]['data']['profiles.json'] = '{}'
    elif damage == 'endpoint':
        options['transport'].api_server = 'https://foreign.invalid'
    else:
        def changed(path):
            if damage == 'history-changed':
                history, = options['predecessor'].history
                history.write_text('changed retained history')
            elif damage == 'receipt-changed':
                live.choices[0]['changed'] = True
            elif path.endswith('/serviceaccounts/loom-platform'):
                live.resources[f'ServiceAccount:{namespace}:loom-platform']['metadata']['uid'] = str(uuid4())
        live.after_read = changed
    with pytest.raises(ValueError, match='participant preparation'):
        module().capture_participant_release(**options)
    assert not operation_inputs['state_root'].exists() and not operation_inputs['anchor_dir'].exists()
