"""Catalog refresh preserves the manager executable and proves real Pod readiness."""
from __future__ import annotations

import copy
import importlib
import json
import ssl
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
from tests.ops.test_nebius_pool_participant_release import catalog_inputs as catalog_inputs
from tests.ops.test_nebius_pool_participant_release import management_inputs as management_inputs
from tests.ops.test_nebius_pool_participant_release import platform_inputs as platform_inputs
from tests.ops.test_nebius_pool_participant_release import release_inputs as release_inputs
from tests.ops.test_nebius_pool_participant_release import (
    staging_attachment_inputs as staging_attachment_inputs,
)


def module():
    name = 'scripts.ops.nebius_pool_participant_catalog'
    assert importlib.util.find_spec(name) is not None, 'concrete participant catalog transition is missing'
    return importlib.import_module(name)


def live_children(manager):
    namespace, name = manager['metadata']['namespace'], manager['metadata']['name']
    template = copy.deepcopy(manager['spec']['template'])
    labels = {**template['metadata']['labels'], 'pod-template-hash': 'abcdef1234'}
    replica = {'apiVersion': 'apps/v1', 'kind': 'ReplicaSet', 'metadata': {'namespace': namespace,
        'name': name + '-abcdef1234', 'uid': str(uuid4()), 'resourceVersion': '24', 'generation': 1, 'labels': labels,
        'ownerReferences': [{'apiVersion': 'apps/v1', 'kind': 'Deployment', 'name': name,
            'uid': manager['metadata']['uid'], 'controller': True}]},
        'spec': {'replicas': 1, 'selector': {'matchLabels': {**manager['spec']['selector']['matchLabels'],
            'pod-template-hash': 'abcdef1234'}}, 'template': template},
        'status': {'observedGeneration': 1, 'replicas': 1, 'readyReplicas': 1, 'availableReplicas': 1}}
    replica['spec']['template']['metadata']['labels'] = labels
    pod = {'apiVersion': 'v1', 'kind': 'Pod', 'metadata': {'namespace': namespace, 'name': name + '-abcdef1234-live',
        'uid': str(uuid4()), 'resourceVersion': '25', 'labels': labels,
        'annotations': copy.deepcopy(template['metadata'].get('annotations', {})),
        'ownerReferences': [{'apiVersion': 'apps/v1', 'kind': 'ReplicaSet', 'name': replica['metadata']['name'],
            'uid': replica['metadata']['uid'], 'controller': True}]},
        'spec': copy.deepcopy(template['spec']), 'status': {'phase': 'Running',
            'conditions': [{'type': 'Ready', 'status': 'True'}], 'containerStatuses': [
                {'name': row['name'], 'ready': True, 'state': {'running': {}}} for row in template['spec']['containers']],
            'initContainerStatuses': [{'name': row['name'], 'state': {'terminated': {'exitCode': 0}}}
                for row in template['spec'].get('initContainers', [])]}}
    return ({'apiVersion': 'apps/v1', 'kind': 'ReplicaSetList', 'metadata': {'resourceVersion': '26'}, 'items': [replica]},
        {'apiVersion': 'v1', 'kind': 'PodList', 'metadata': {'resourceVersion': '27'}, 'items': [pod]})


@pytest.fixture(params=['participant', 'catalog-only'])
def connection(catalog_inputs, tmp_path, request):
    inputs = copy.deepcopy(catalog_inputs)
    binding = inputs['release'].migration.registration.binding
    if request.param == 'catalog-only':
        from scripts.ops import nebius_pool_participant_release as release

        inputs['release'] = release.PoolCatalogRefresh(inputs['release'].refresh, binding)
    manager = inputs['manager']
    manager['metadata']['generation'] = 1
    manager['status'] = {'observedGeneration': 1, 'replicas': 1, 'updatedReplicas': 1,
        'readyReplicas': 1, 'availableReplicas': 1}
    request = module().ParticipantCatalogRequest(**inputs)
    state = SimpleNamespace(request=request, directory=tmp_path, manager=copy.deepcopy(manager), config=None,
        writes=[], failure=None, ready=True, init_ready=True, old_pod=False, replaced_catalog=False, collections=None)

    def respond(message):
        path = message.url.path
        if path in ('/api/v1/namespaces/kube-system', '/api/v1/namespaces/' + binding.namespace):
            assert message.method == 'GET'
            name = path.rsplit('/', 1)[-1]
            return httpx.Response(200, json={'apiVersion': 'v1', 'kind': 'Namespace', 'metadata': {'name': name,
                'uid': binding.kube_system_uid if name == 'kube-system' else binding.namespace_uid,
                'labels': {'loom.nebius/management-installation': binding.installation_id,
                    'pod-security.kubernetes.io/enforce': 'restricted'}}})
        if '/configmaps' in path:
            if message.method == 'GET':
                if state.config is None:
                    return httpx.Response(404)
                value = copy.deepcopy(state.config)
                if state.replaced_catalog:
                    value['metadata']['uid'] = str(uuid4())
                return httpx.Response(200, json=value)
            assert message.method == 'POST'
            value = json.loads(message.content)
            if message.url.params:
                assert dict(message.url.params) == {'dryRun': 'All'}
            else:
                state.writes.append(('catalog', value))
                value['metadata'].update(uid=str(uuid4()), resourceVersion='20')
                state.config = copy.deepcopy(value)
            return httpx.Response(201, json=value)
        if path.endswith('/replicasets') or path.endswith('/pods'):
            assert message.method == 'GET' and dict(message.url.params) == {'limit': '1000'}
            replicas, pods = state.collections
            value = copy.deepcopy(pods if path.endswith('/pods') else replicas)
            if path.endswith('/pods'):
                if not state.ready:
                    value['items'][0]['status']['containerStatuses'][0]['ready'] = False
                if not state.init_ready:
                    value['items'][0]['status']['initContainerStatuses'][0]['state'] = {'running': {}}
                if state.old_pod:
                    old = copy.deepcopy(value['items'][0])
                    old['metadata'].update(uid=str(uuid4()), name='old-manager')
                    value['items'].append(old)
            return httpx.Response(200, json=value)
        assert path == '/apis/apps/v1/namespaces/' + binding.namespace + '/deployments/loom-service'
        if message.method == 'GET':
            return httpx.Response(200, json=state.manager)
        assert message.method == 'PATCH'
        patch = json.loads(message.content)
        assert patch[:3] == [
            {'op': 'test', 'path': '/metadata/uid', 'value': state.manager['metadata']['uid']},
            {'op': 'test', 'path': '/metadata/resourceVersion', 'value': state.manager['metadata']['resourceVersion']},
            {'op': 'test', 'path': '/spec', 'value': state.manager['spec']}]
        value = copy.deepcopy(state.manager)
        for item in patch[3:]:
            pieces = item['path'].strip('/').split('/')
            owner = value
            for key in pieces[:-1]:
                owner = owner[key]
            if item['op'] == 'remove':
                del owner[pieces[-1]]
            else:
                assert item['op'] in {'add', 'replace'}
                owner[pieces[-1]] = item['value']
        if message.url.params:
            assert dict(message.url.params) == {'dryRun': 'All'}
            if state.failure == 'catalog-after-preview':
                state.replaced_catalog = True
            return httpx.Response(200, json=value)
        state.writes.append(('manager', patch))
        if state.failure == 'conflict':
            return httpx.Response(409, json={'apiVersion': 'v1', 'kind': 'Status', 'status': 'Failure', 'code': 409, 'reason': 'Conflict'})
        if state.failure == 'lost-before':
            raise httpx.ReadError('private-marker')
        value['metadata'].update(resourceVersion='21', generation=2)
        value['status']['observedGeneration'] = 2
        state.manager = value
        state.collections = live_children(value)
        if state.failure == 'lost-after':
            raise httpx.ReadError('private-marker')
        return httpx.Response(200, json=value)

    api = module().HTTPSParticipantCatalogAPI(request=request, state_dir=tmp_path, api_server='https://cluster.example',
        ssl_context=ssl.create_default_context())
    api.client.close()
    api.client = httpx.Client(base_url='https://cluster.example', transport=httpx.MockTransport(respond))
    return api, state


def stage(api, state):
    return module().stage_participant_catalog(request=state.request, api=api, state_dir=state.directory)


def advance(api, state):
    return module().refresh_participant_manager(request=state.request, api=api, state_dir=state.directory)


def test_catalog_receipt_loader_retains_successor_identity_and_original_history(connection):
    from scripts.ops.nebius_ingress_stage import _snapshot

    api, state = connection
    original = copy.deepcopy(state.request)
    with api:
        stage(api, state)
        assert advance(api, state) is True
    observed = module().read_applied_catalog(request=state.request, state_dir=state.directory)
    assert observed.manager['metadata']['uid'] == original.manager['metadata']['uid']
    assert _snapshot(observed.manager) == _snapshot(state.manager)
    assert observed.catalog['metadata']['uid'] == state.config['metadata']['uid']
    assert _snapshot(observed.catalog) == _snapshot(state.config)
    assert set(observed.files) == {state.directory / 'catalog/stage.json', state.directory / 'manager/transition.json'}
    assert all(path.read_bytes() == raw for path, raw in observed.files.items())
    assert state.request == original


@pytest.mark.parametrize('damage', ['missing-catalog', 'missing-manager', 'pending', 'image', 'catalog-body', 'catalog-uid'])
def test_catalog_receipt_loader_rejects_incomplete_or_changed_children(connection, damage):
    from scripts.ops import nebius_certificates as private_state

    api, state = connection
    with api:
        stage(api, state)
        assert advance(api, state) is True
    catalog_path = state.directory / 'catalog/stage.json'
    manager_path = state.directory / 'manager/transition.json'
    if damage.startswith('missing-'):
        (catalog_path if damage == 'missing-catalog' else manager_path).unlink()
    else:
        path = catalog_path if damage.startswith('catalog-') else manager_path
        record = json.loads(path.read_text())
        item, = record['resources'].values()
        if damage == 'pending':
            item['status'] = 'intent'
        elif damage == 'image':
            item['expected']['spec']['template']['spec']['containers'][0]['image'] = 'foreign@sha256:' + 'a' * 64
        elif damage == 'catalog-body':
            item['observed']['data']['profiles.json'] = '{}'
        else:
            item['uid'] = 'invalid-uid'
        private_state._atomic_json(path, record)
    with pytest.raises(ValueError):
        module().read_applied_catalog(request=state.request, state_dir=state.directory)


@pytest.mark.parametrize('failure', [None, 'lost-after', 'unready', 'init-unready', 'old-pod', 'conflict'])
def test_catalog_update_changes_only_mount_and_replays_without_duplicate_patch(connection, failure):
    api, state = connection
    original = copy.deepcopy(state.manager)
    state.failure = failure
    state.ready, state.old_pod = failure != 'unready', failure == 'old-pod'
    state.init_ready = failure != 'init-unready'
    with api:
        stage(api, state)
        assert advance(api, state) is (failure not in {'unready', 'init-unready', 'old-pod', 'conflict'})
        state.failure, state.ready, state.old_pod = None, True, False
        state.init_ready = True
        stage(api, state)
        assert advance(api, state) is True
    assert [kind for kind, _ in state.writes] == ['catalog', 'manager'] + (['manager'] if failure == 'conflict' else [])
    wanted = copy.deepcopy(original['spec'])
    volume, = (row for row in wanted['template']['spec']['volumes'] if row['name'] == 'pool-profiles')
    volume['configMap']['name'] = 'loom-pool-profiles-' + state.request.release.refresh.after.operation_id.hex
    assert state.manager['spec'] == wanted
    assert state.config['immutable'] is True


@pytest.mark.parametrize('damage', ['lost-before', 'catalog-after-preview', 'catalog-replaced', 'foreign-image', 'missing-history'])
def test_catalog_transition_refuses_uncertain_or_unbound_replacements(connection, damage):
    api, state = connection
    with api:
        stage(api, state)
        if damage == 'catalog-replaced':
            state.replaced_catalog = True
        elif damage == 'foreign-image':
            state.manager['spec']['template']['spec']['containers'][0]['image'] = 'foreign:latest'
        elif damage == 'missing-history':
            (state.directory / 'catalog/stage.json').unlink()
        else:
            state.failure = damage
        for _ in range(2):
            with pytest.raises(ValueError):
                advance(api, state)
    assert [kind for kind, _ in state.writes] == ['catalog'] + (['manager'] if damage == 'lost-before' else [])
