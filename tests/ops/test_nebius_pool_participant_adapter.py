"""Ordinary rollout connects fixed pool Jobs, catalog CAS and fresh capacity."""
from __future__ import annotations

import copy
import importlib
import json
import ssl
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
from scripts.ops.nebius_ingress_stage import _key
from scripts.ops.nebius_pool_participant_operation import retain_participant_rollout
from tests.ops.test_nebius_management_stage import PhaseAPI
from tests.ops.test_nebius_pool_participant_catalog import live_children
from tests.ops.test_nebius_pool_participant_creation import terminal_pod
from tests.ops.test_nebius_pool_participant_operation import catalog_inputs as catalog_inputs
from tests.ops.test_nebius_pool_participant_operation import management_inputs as management_inputs
from tests.ops.test_nebius_pool_participant_operation import operation_inputs as operation_inputs
from tests.ops.test_nebius_pool_participant_operation import platform_inputs as platform_inputs
from tests.ops.test_nebius_pool_participant_operation import release_inputs as release_inputs
from tests.ops.test_nebius_pool_participant_operation import staging_attachment_inputs as staging_attachment_inputs

from loom_service.pool_management.capacity import digest


def module():
    name = 'scripts.ops.nebius_pool_participant_adapter'
    assert importlib.util.find_spec(name) is not None, 'ordinary rollout has no retained pool effect adapter'
    return importlib.import_module(name)


@pytest.fixture
def connection(operation_inputs, monkeypatch):
    retained = retain_participant_rollout(**operation_inputs)
    binding = retained.release.migration.registration.binding
    api = PhaseAPI(binding)
    api.key = _key
    manager = copy.deepcopy(retained.inputs.manager)
    manager['metadata']['generation'] = 1
    manager['status'] = {'observedGeneration': 1, 'replicas': 1, 'updatedReplicas': 1,
        'readyReplicas': 1, 'availableReplicas': 1}
    api.resources[_key(manager)] = manager
    api.resources[_key(retained.inputs.catalog)] = copy.deepcopy(retained.inputs.catalog)
    state = SimpleNamespace(retained=retained, api=api, effects=[], allow=True, ready=True, busy=False,
        failure=None, pods={}, reports={}, gates=[], manager=manager)
    kube = object()

    def qualify(observed_kube, operation, phase):
        assert observed_kube is kube and operation == retained and phase in {'preflight', 'guarded', 'ready'}
        state.gates.append(phase)
        if not state.allow or (phase == 'ready' and not state.ready):
            raise ValueError('qualification unavailable')

    def respond(request):
        path = request.url.path
        if path in ('/api/v1/namespaces/kube-system', '/api/v1/namespaces/' + binding.namespace):
            assert request.method == 'GET'
            name = path.rsplit('/', 1)[-1]
            return httpx.Response(200, json={'apiVersion': 'v1', 'kind': 'Namespace', 'metadata': {'name': name,
                'uid': binding.kube_system_uid if name == 'kube-system' else binding.namespace_uid,
                'labels': {'loom.nebius/management-installation': binding.installation_id,
                    'pod-security.kubernetes.io/enforce': 'restricted'}}})
        if request.method == 'GET' and path.endswith(('/pods', '/replicasets')):
            if 'labelSelector' in request.url.params:
                uid = request.url.params['labelSelector'].split('=', 1)[1]
                pods = [pod for pod in state.pods.values() if pod['metadata']['ownerReferences'][0]['uid'] == uid]
                return httpx.Response(200, json={'apiVersion': 'v1', 'kind': 'PodList', 'metadata': {}, 'items': pods})
            replicas, pods = live_children(state.manager)
            return httpx.Response(200, json=pods if path.endswith('/pods') else replicas)
        if request.method == 'GET' and '/pods/' in path:
            name = path.split('/pods/')[1].split('/')[0]
            if path.endswith('/log'):
                return httpx.Response(200, content=json.dumps(state.reports[name]).encode())
            return httpx.Response(200, json=state.pods[name])
        if request.method == 'GET':
            collection, name = path.rsplit('/', 2)[-2:]
            kind = {'configmaps': 'ConfigMap', 'jobs': 'Job', 'deployments': 'Deployment'}[collection]
            row = api.resources.get(kind + ':' + binding.namespace + ':' + name)
            return httpx.Response(404) if row is None else httpx.Response(200, json=row)
        assert state.allow, 'a rejected protected qualification must precede all effects'
        if request.method == 'PATCH':
            assert path.endswith('/deployments/loom-service')
            value = copy.deepcopy(state.manager)
            patch = json.loads(request.content)
            for row in patch:
                owner = value
                pieces = row['path'].strip('/').split('/')
                for key in pieces[:-1]:
                    owner = owner[key]
                if row['op'] == 'test':
                    assert owner[pieces[-1]] == row['value']
                elif row['op'] == 'remove':
                    del owner[pieces[-1]]
                else:
                    owner[pieces[-1]] = row['value']
            if not request.url.params:
                state.effects.append('manager')
                value['metadata'].update(resourceVersion='22', generation=2)
                value['status']['observedGeneration'] = 2
                state.manager = value
                api.resources[_key(value)] = value
            return httpx.Response(200, json=value)
        assert request.method == 'POST'
        document = json.loads(request.content)
        if request.url.params:
            assert dict(request.url.params) == {'dryRun': 'All'}
            return httpx.Response(201, json=api.default_resource(document))
        action = document['spec']['template']['spec']['containers'][0]['command'][-1] if document['kind'] == 'Job' else 'config'
        state.effects.append(action)
        if action == 'fence' and state.failure == 'before':
            raise httpx.ReadError('lost before create')
        api.create_resource(document)
        actual = api.resources[_key(document)]
        if document['kind'] == 'Job':
            actual['status'] = {'conditions': [{'type': 'Complete', 'status': 'True'}], 'succeeded': 1, 'active': 0, 'failed': 0}
            pod = terminal_pod(actual, succeeded=True)
            name = pod['metadata']['name']
            state.pods[name] = pod
            refresh = retained.release.refresh
            report = {'schema_version': 'loom.pool-participant-refresh-receipt.v1',
                'operation_id': str(refresh.after.operation_id), 'participant_id': str(refresh.participant_id),
                'pool_id': str(refresh.after.pool_id), 'before_sha256': digest(refresh.before.model_dump(mode='json')),
                'after_sha256': digest(refresh.after.model_dump(mode='json')),
                'status': {'fence': 'pending_work' if state.busy else 'fenced', 'stage': 'staged',
                    'open': 'complete', 'capacity': 'complete'}[action]}
            if action == 'capacity':
                now = datetime.now(UTC)
                report['capacity'] = {'checked_at': now.isoformat(), 'valid_until': (now + timedelta(seconds=60)).isoformat(),
                    'selected_observation_id': str(uuid4()), 'selected_observed_at': now.isoformat(),
                    'connected_pools': 1, 'observations_sha256': 'e' * 64}
            state.reports[name] = report
        if action == 'fence' and state.failure == 'after':
            state.failure = None
            raise httpx.ReadError('lost accepted response')
        return httpx.Response(201, json=actual)

    real_client = httpx.Client
    monkeypatch.setattr(httpx, 'Client', lambda *args, **kwargs: real_client(
        base_url=retained.inputs.config['kubernetes_api_server'], transport=httpx.MockTransport(respond)))

    def connect():
        return module().RetainedParticipantPoolAdapter(retained=retained,
            api_server=retained.inputs.config['kubernetes_api_server'], ssl_context=ssl.create_default_context(),
            qualify=qualify)

    return state, kube, connect


def test_pool_adapter_orders_fixed_effects_and_replays_without_duplicate_writes(connection):
    state, kube, connect = connection
    pool, release = connect(), state.retained.release
    pool.preflight(kube, release)
    pool.under_guard(kube, release)
    assert state.effects == []
    assert pool.activate(kube, release) == 'ready'
    assert state.effects == ['config', 'fence', 'stage', 'config', 'manager']
    assert connect().activate(kube, release) == 'ready'
    pool.finish(kube, release)
    connect().finish(kube, release)
    first_id = uuid4()
    first = pool.read_capacity(kube, release, observation_id=first_id)
    second = connect().read_capacity(kube, release, observation_id=uuid4())
    assert first != second and state.effects == ['config', 'fence', 'stage', 'config', 'manager', 'open', 'capacity', 'capacity']
    before = list(state.effects)
    with pytest.raises(ValueError, match='preserve'):
        connect().read_capacity(kube, release, observation_id=first_id)
    assert state.effects == before


def test_busy_fence_cannot_stage_open_or_modify_manager(connection):
    state, kube, connect = connection
    state.busy = True
    pool, release = connect(), state.retained.release
    assert pool.activate(kube, release) == 'busy'
    assert connect().activate(kube, release) == 'busy'
    with pytest.raises(ValueError, match='preserve'):
        pool.finish(kube, release)
    assert state.effects == ['config', 'fence']


@pytest.mark.parametrize('failure', ['before', 'after'])
def test_lost_pool_create_reconciles_only_original_job(connection, failure):
    state, kube, connect = connection
    state.failure = failure
    for _ in range(2):
        if failure == 'before':
            with pytest.raises(ValueError, match='preserve'):
                connect().activate(kube, state.retained.release)
        else:
            assert connect().activate(kube, state.retained.release) == 'ready'
    assert state.effects.count('fence') == 1


def test_pool_adapter_requires_live_qualification_and_final_readiness(connection):
    state, kube, connect = connection
    pool, release = connect(), state.retained.release
    state.allow = False
    with pytest.raises(ValueError, match='preserve'):
        pool.activate(kube, release)
    assert state.effects == []
    state.allow = True
    pool.activate(kube, release)
    state.ready = False
    with pytest.raises(ValueError, match='preserve'):
        pool.finish(kube, release)
    assert 'open' not in state.effects
    state.ready = True
    pool.finish(kube, release)
    assert state.effects[-1] == 'open'
