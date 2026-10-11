"""Candidate updates use retained originals and atomic UID/version preconditions."""
from __future__ import annotations

import copy
import importlib
import json
import ssl
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
from scripts.ops import nebius_pool_participant_operation as operation
from scripts.ops.nebius_ingress_stage import _key
from tests.ops.test_nebius_pool_participant_operation import catalog_inputs as catalog_inputs
from tests.ops.test_nebius_pool_participant_operation import management_inputs as management_inputs
from tests.ops.test_nebius_pool_participant_operation import operation_inputs as operation_inputs
from tests.ops.test_nebius_pool_participant_operation import platform_inputs as platform_inputs
from tests.ops.test_nebius_pool_participant_operation import release_inputs as release_inputs
from tests.ops.test_nebius_pool_participant_operation import staging_attachment_inputs as staging_attachment_inputs


def module():
    name = 'scripts.ops.nebius_pool_participant_resource'
    assert importlib.util.find_spec(name) is not None, 'retained candidate resource CAS is missing'
    return importlib.import_module(name)


@pytest.fixture
def connection(operation_inputs):
    for row in operation_inputs['current']:
        if row['kind'] == 'Service':
            row['spec'].update(clusterIP='10.0.0.7', clusterIPs=['10.0.0.7'],
                ipFamilies=['IPv4'], ipFamilyPolicy='SingleStack')
            if row['spec'].get('type') == 'LoadBalancer':
                row['spec']['ports'][0]['nodePort'] = 30443
        if row['kind'] == 'Namespace':
            row['spec'] = {'finalizers': ['kubernetes']}
    retained = operation.retain_participant_rollout(**operation_inputs)
    state = SimpleNamespace(retained=retained, rows={_key(row): copy.deepcopy(row) for row in retained.inputs.current},
        writes=[], requests=[], failure=None, qualifications=[], foreign=False)

    def qualify(current, key):
        assert current == retained
        state.qualifications.append(key)
        if state.foreign:
            raise ValueError('guard ownership lost')

    def connect(key):
        api = module().HTTPSParticipantResourceAPI(retained=retained, key=key,
            api_server=retained.inputs.config['kubernetes_api_server'], ssl_context=ssl.create_default_context(),
            qualify_resource=qualify)

        def respond(message):
            state.requests.append(message)
            expected_paths = {
                'Namespace': '/api/v1/namespaces/',
                'ConfigMap': '/api/v1/namespaces/' + retained.inputs.config['namespace'] + '/configmaps/',
                'Service': '/api/v1/namespaces/' + retained.inputs.config['namespace'] + '/services/',
                'Deployment': '/apis/apps/v1/namespaces/' + retained.inputs.config['namespace'] + '/deployments/',
                'StatefulSet': '/apis/apps/v1/namespaces/' + retained.inputs.config['namespace'] + '/statefulsets/',
            }
            row = state.rows[key]
            assert message.url.path == expected_paths[row['kind']] + row['metadata']['name']
            if message.method == 'GET':
                return httpx.Response(200, json=row)
            assert message.method == 'PATCH'
            assert message.headers['content-type'] == 'application/json-patch+json'
            patches = json.loads(message.content)
            assert patches[:2] == [
                {'op': 'test', 'path': '/metadata/uid', 'value': row['metadata']['uid']},
                {'op': 'test', 'path': '/metadata/resourceVersion', 'value': row['metadata']['resourceVersion']},
            ]
            dry_run = bool(message.url.params)
            if dry_run:
                assert dict(message.url.params) == {'dryRun': 'All'}
            else:
                state.writes.append(key)
                if state.failure == 'conflict':
                    return httpx.Response(409, json={'apiVersion': 'v1', 'kind': 'Status', 'status': 'Failure',
                        'code': 409, 'reason': 'Conflict'})
                if state.failure == 'lost-before':
                    raise httpx.ReadError('transport unavailable')
            value = copy.deepcopy(row)
            for item in patches[2:]:
                parts = item['path'].strip('/').split('/')
                owner = value
                for part in parts[:-1]:
                    owner = owner[part]
                if item['op'] == 'test':
                    assert owner[parts[-1]] == item['value']
                elif item['op'] == 'remove':
                    del owner[parts[-1]]
                else:
                    assert item['op'] in {'add', 'replace'}
                    owner[parts[-1]] = item['value']
            if state.failure == 'unsafe-default':
                value['spec']['template']['spec']['hostNetwork'] = True
            if not dry_run:
                value['metadata']['resourceVersion'] = str(int(row['metadata']['resourceVersion']) + 1)
                state.rows[key] = value
                if state.failure == 'lost-after':
                    raise httpx.ReadError('reply lost after commit')
            return httpx.Response(200, json=value)

        api.client.close()
        api.client = httpx.Client(base_url=retained.inputs.config['kubernetes_api_server'], transport=httpx.MockTransport(respond))
        return api

    return state, connect


def advance(state, api, key):
    return module().update_participant_resource(retained=state.retained, key=key, api=api)


def test_candidate_updates_cover_non_workload_resources_without_changing_allocations(connection):
    state, connect = connection
    namespace = state.retained.inputs.config['namespace']
    keys = ['Namespace:-:' + namespace, 'ConfigMap:' + namespace + ':loom-platform-config',
        'Service:' + namespace + ':loom-postgres', 'StatefulSet:' + namespace + ':loom-postgres',
        'Deployment:' + namespace + ':loom-service', 'Service:' + namespace + ':loom-web']
    originals = copy.deepcopy(state.rows)
    for key in keys:
        with connect(key) as api:
            assert advance(state, api, key) is True
            assert advance(state, api, key) is True
    assert state.writes == keys
    assert all(state.rows[key]['metadata']['uid'] == originals[key]['metadata']['uid'] for key in keys)
    assert state.rows[keys[0]]['spec']['finalizers'] == ['kubernetes']
    for key in (keys[2], keys[-1]):
        assert state.rows[key]['spec']['clusterIPs'] == ['10.0.0.7']
        assert state.rows[key]['spec']['ipFamilyPolicy'] == 'SingleStack'
    assert state.rows[keys[-1]]['spec']['type'] == 'LoadBalancer'
    assert state.rows[keys[-1]]['spec']['ports'][0]['nodePort'] == 30443
    profile = json.loads(state.rows[keys[1]]['data']['profile.json'])
    assert profile['candidate_sha'] == 'd' * 40
    assert all(row['image'].endswith('@sha256:' + 'e' * 64)
        for row in state.rows[keys[4]]['spec']['template']['spec']['containers'])
    assert set(state.rows) == set(originals)
    assert all(state.rows[key] == originals[key] for key in originals.keys() - set(keys))


@pytest.mark.parametrize('failure', ['lost-before', 'lost-after', 'conflict'])
def test_candidate_cas_only_retries_an_explicit_rejection(connection, failure):
    state, connect = connection
    key = 'Deployment:' + state.retained.inputs.config['namespace'] + ':loom-service'
    state.failure = failure
    with connect(key) as api:
        if failure == 'lost-before':
            for _ in range(2):
                with pytest.raises(ValueError, match='preserve'):
                    advance(state, api, key)
            assert state.writes == [key]
        elif failure == 'lost-after':
            assert advance(state, api, key) is True
            assert advance(state, api, key) is True
            assert state.writes == [key]
        else:
            assert advance(state, api, key) is False
            state.failure = None
            assert advance(state, api, key) is True
            assert state.writes == [key, key]


@pytest.mark.parametrize('damage', ['uid', 'original', 'anchor', 'guard', 'unsafe-default'])
def test_candidate_update_rejects_lost_authority_or_drift_before_write(connection, damage):
    state, connect = connection
    key = 'Deployment:' + state.retained.inputs.config['namespace'] + ':loom-service'
    with connect(key) as api:
        if damage == 'uid':
            state.rows[key]['metadata']['uid'] = str(uuid4())
        elif damage == 'original':
            state.rows[key]['spec']['replicas'] = 2
        elif damage == 'anchor':
            anchor, = state.retained.anchor_dir.glob('*.json')
            anchor.unlink()
        elif damage == 'guard':
            state.foreign = True
        else:
            state.failure = damage
        with pytest.raises(ValueError, match='preserve'):
            advance(state, api, key)
    assert not state.writes


@pytest.mark.parametrize('outside', ['secret', 'retired', 'manager', 'job', 'endpoint'])
def test_candidate_resource_transport_has_no_arbitrary_write_scope(connection, outside):
    state, _ = connection
    ns = state.retained.inputs.config['namespace']
    keys = {'secret': 'Secret:' + ns + ':loom-platform-db',
        'retired': 'CronJob:' + state.retained.inputs.config['execution_namespace'] + ':loom-execution-capacity-collector',
        'manager': _key(state.retained.inputs.manager),
        'job': _key(state.retained.release.files['30-migrate.yaml'][0]),
        'endpoint': 'Deployment:' + ns + ':loom-service'}
    with pytest.raises(ValueError, match='participant resource'):
        module().HTTPSParticipantResourceAPI(retained=state.retained, key=keys[outside],
            api_server='https://foreign.invalid' if outside == 'endpoint' else state.retained.inputs.config['kubernetes_api_server'],
            ssl_context=ssl.create_default_context(), qualify_resource=lambda *_: None)
