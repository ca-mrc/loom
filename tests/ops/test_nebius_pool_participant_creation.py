"""Fixed candidate/backup creation retains exact UID lineage and uncertain intent."""
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
from tests.ops.test_nebius_management_stage import PhaseAPI
from tests.ops.test_nebius_pool_participant_operation import catalog_inputs as catalog_inputs
from tests.ops.test_nebius_pool_participant_operation import management_inputs as management_inputs
from tests.ops.test_nebius_pool_participant_operation import operation_inputs as operation_inputs
from tests.ops.test_nebius_pool_participant_operation import platform_inputs as platform_inputs
from tests.ops.test_nebius_pool_participant_operation import release_inputs as release_inputs
from tests.ops.test_nebius_pool_participant_operation import (
    staging_attachment_inputs as staging_attachment_inputs,
)


def module():
    name = 'scripts.ops.nebius_pool_participant_creation'
    assert importlib.util.find_spec(name) is not None, 'retained candidate and backup creation is missing'
    return importlib.import_module(name)


@pytest.fixture
def connection(operation_inputs):
    ns = operation_inputs['config']['namespace']
    operation_inputs['rendered_files']['10-config-network.yaml'].append({
        'apiVersion': 'v1', 'kind': 'ConfigMap', 'metadata': {'name': 'loom-feature-config', 'namespace': ns},
        'data': {'mode': 'candidate-feature'}})
    retained = operation.retain_participant_rollout(**operation_inputs)
    external = PhaseAPI(retained.release.migration.registration.binding)
    external.key = _key
    state = SimpleNamespace(retained=retained, external=external, requests=[], allow=True, before_dispatch=[],
        pods=[], continuation=False, job_reads=0, replace_during_observation=False,
        keys=[_key(retained.release.files[phase][0]) for phase in ('30-migrate.yaml', '50-configure.yaml')]
            + ['pre-upgrade-backup', 'ConfigMap:' + ns + ':loom-feature-config'])

    def qualify(current, key):
        assert current == retained and key in state.keys
        if not state.allow:
            raise ValueError('guard ownership unavailable')

    def connect(key):
        api = module().HTTPSParticipantCreationAPI(retained=retained, key=key,
            api_server=retained.inputs.config['kubernetes_api_server'], ssl_context=ssl.create_default_context(),
            qualify_creation=qualify)

        def respond(message):
            state.requests.append(message)
            if message.url.path == '/api/v1/namespaces/' + ns + '/pods':
                assert message.method == 'GET' and dict(message.url.params) == {'limit': '1000'}
                return httpx.Response(200, json={'apiVersion': 'v1', 'kind': 'PodList',
                    'metadata': {'resourceVersion': '27', 'continue': 'more' if state.continuation else ''}, 'items': state.pods})
            prefix = '/api/v1/namespaces/' + ns + '/configmaps' if key.startswith('ConfigMap:') else (
                '/apis/batch/v1/namespaces/' + ns + '/jobs')
            if message.method == 'GET':
                assert message.url.path.startswith(prefix + '/')
                name = message.url.path.removeprefix(prefix + '/')
                kind = 'ConfigMap' if key.startswith('ConfigMap:') else 'Job'
                actual = external.resources.get(kind + ':' + ns + ':' + name)
                if kind == 'Job':
                    state.job_reads += 1
                    if state.replace_during_observation and state.job_reads == 2:
                        actual = copy.deepcopy(actual)
                        actual['metadata']['uid'] = str(uuid4())
                return httpx.Response(404) if actual is None else httpx.Response(200, json=actual)
            assert message.method == 'POST' and message.url.path == prefix
            doc = json.loads(message.content)
            if message.url.params:
                assert dict(message.url.params) == {'dryRun': 'All'}
                return httpx.Response(201, json=external.default_resource(doc))
            journals = [json.loads(path.read_text()) for path in (retained.state_dir / 'candidate-creations').rglob('stage.json')]
            pending, = (item for journal in journals for item in journal['resources'].values()
                if item['desired'] == doc)
            state.before_dispatch.append(copy.deepcopy(pending))
            external.create_resource(doc)
            return httpx.Response(201, json=external.get_resource(doc))

        api.client.close()
        api.client = httpx.Client(base_url=retained.inputs.config['kubernetes_api_server'], transport=httpx.MockTransport(respond))
        return api

    return state, connect


def terminal_pod(job, *, succeeded):
    template = copy.deepcopy(job['spec']['template'])
    return {'apiVersion': 'v1', 'kind': 'Pod', 'metadata': {
        'namespace': job['metadata']['namespace'], 'name': job['metadata']['name'] + '-owned',
        'uid': str(uuid4()), 'resourceVersion': '25', 'labels': template['metadata']['labels'],
        'annotations': template['metadata'].get('annotations', {}),
        'ownerReferences': [{'apiVersion': 'batch/v1', 'kind': 'Job', 'name': job['metadata']['name'],
            'uid': job['metadata']['uid'], 'controller': True, 'blockOwnerDeletion': True}]},
        'spec': {**template['spec'], 'nodeName': 'system-0'}, 'status': {
            'phase': 'Succeeded' if succeeded else 'Failed',
            'containerStatuses': [{'name': row['name'], 'state': {'terminated': {'exitCode': 0 if succeeded else 1}}}
                for row in template['spec']['containers']],
            'initContainerStatuses': [{'name': row['name'], 'state': {'terminated': {'exitCode': 0}}}
                for row in template['spec'].get('initContainers', [])]}}


@pytest.mark.parametrize('selected', [0, 2])
@pytest.mark.parametrize('outcome', ['pending', 'complete', 'failed', 'draining'])
def test_job_observation_requires_terminal_owned_processes_without_writes(connection, selected, outcome):
    state, connect = connection
    with connect(state.keys[selected]) as api:
        stage(state, api, state.keys[selected])
        job, = state.external.resources.values()
        if outcome != 'pending':
            complete = outcome == 'complete'
            job['status'] = {'conditions': [{'type': 'Complete' if complete else 'Failed', 'status': 'True'}],
                'active': 0, 'succeeded': int(complete), 'failed': int(not complete)}
            state.pods = [terminal_pod(job, succeeded=complete)]
            if outcome == 'draining':
                state.pods[0]['status']['phase'] = 'Running'
                state.pods[0]['status']['containerStatuses'][0]['state'] = {'running': {}}
        state.requests.clear()
        result = api.job_outcome()
        if outcome in {'pending', 'draining'}:
            assert result is None
        else:
            assert result == {'status': outcome, 'job_uid': job['metadata']['uid'], 'job_name': job['metadata']['name'],
                'pod_uids': [state.pods[0]['metadata']['uid']]}
        assert all(row.method == 'GET' for row in state.requests)
        assert len(state.external.creates) == 1


@pytest.mark.parametrize('damage', ['pod-template', 'pod-owner', 'partial-list', 'job-replaced', 'no-success', 'duplicate-uid'])
def test_job_success_rejects_unqualified_execution_evidence(connection, damage):
    state, connect = connection
    with connect(state.keys[0]) as api:
        stage(state, api, state.keys[0])
        job, = state.external.resources.values()
        job['status'] = {'conditions': [{'type': 'Complete', 'status': 'True'}], 'active': 0, 'succeeded': 1}
        state.pods = [terminal_pod(job, succeeded=True)]
        if damage == 'pod-template':
            state.pods[0]['spec']['containers'][0]['image'] = 'foreign/image'
        elif damage == 'pod-owner':
            state.pods[0]['metadata']['ownerReferences'][0]['controller'] = False
        elif damage == 'partial-list':
            state.continuation = True
        elif damage == 'job-replaced':
            state.job_reads = 0
            state.replace_during_observation = True
        elif damage == 'no-success':
            state.pods[0]['status']['containerStatuses'][0]['state']['terminated']['exitCode'] = 1
        else:
            state.pods.append(copy.deepcopy(state.pods[0]))
        state.requests.clear()
        with pytest.raises(ValueError, match='preserve'):
            api.job_outcome()
        assert all(row.method == 'GET' for row in state.requests)


def stage(state, api, key):
    return module().stage_participant_creation(retained=state.retained, key=key, api=api)


def test_new_candidate_and_original_backup_are_created_once_with_durable_uids(connection):
    state, connect = connection
    observed = []
    for key in state.keys:
        with connect(key) as api:
            result = stage(state, api, key)
            assert stage(state, api, key) == result
            observed.extend(result['resource_uids'].values())
    assert len(set(observed)) == 4
    assert len(state.external.creates) == 4
    assert all(item['status'] == 'create_intent' and item['uid'] is None for item in state.before_dispatch)
    backup, = (row for row in state.external.resources.values() if row['metadata']['name'].startswith('loom-predeploy-'))
    old, = (row for row in state.retained.inputs.current if row['kind'] == 'CronJob'
        and row['metadata']['name'] == 'loom-platform-backup')
    wanted = old['spec']['jobTemplate']['spec']
    assert backup['spec']['template']['spec'] == wanted['template']['spec']
    assert backup['spec']['activeDeadlineSeconds'] == 1800
    assert backup['metadata']['name'] == 'loom-predeploy-' + state.retained.reference.operation_id.hex
    assert backup['metadata'].get('ownerReferences', []) == []
    migration_key = state.keys[0]
    migration = state.external.resources[migration_key]
    assert migration['spec']['template']['spec']['containers'][0]['command'][-1] == 'database'
    assert migration['spec']['template']['spec']['containers'][0]['image'].endswith('@sha256:' + 'e' * 64)
    assert not any('status' in row for row in state.external.resources.values())  # Creation is not execution proof.


@pytest.mark.parametrize('failure', ['before', 'after'])
@pytest.mark.parametrize('selected', [0, 2])
def test_unknown_candidate_or_backup_create_is_only_read_back(connection, failure, selected):
    state, connect = connection
    key = state.keys[selected]
    state.external.failure = failure
    with connect(key) as api:
        for _ in range(2):
            if failure == 'before':
                with pytest.raises(ValueError, match='preserve'):
                    stage(state, api, key)
            else:
                assert len(stage(state, api, key)['resource_uids']) == 1
    assert len(state.external.creates) == 1


@pytest.mark.parametrize('damage', ['replaced', 'changed', 'missing', 'guard', 'anchor'])
def test_candidate_create_recovery_refuses_drift_without_recreating(connection, damage):
    state, connect = connection
    key = state.keys[0]
    with connect(key) as api:
        stage(state, api, key)
        row = state.external.resources[key]
        if damage == 'replaced':
            row['metadata']['uid'] = str(uuid4())
        elif damage == 'changed':
            row['spec']['template']['spec']['containers'][0]['image'] = 'foreign/image'
        elif damage == 'missing':
            del state.external.resources[key]
        elif damage == 'guard':
            state.allow = False
        else:
            anchor, = state.retained.anchor_dir.glob('*.json')
            anchor.unlink()
        with pytest.raises(ValueError, match='preserve'):
            stage(state, api, key)
    assert state.external.creates == [key]


@pytest.mark.parametrize('outside', ['existing', 'secret', 'unknown', 'endpoint', 'payload'])
def test_create_connection_cannot_overwrite_or_accept_unretained_resources(connection, outside):
    state, connect = connection
    ns = state.retained.inputs.config['namespace']
    if outside == 'payload':
        with connect(state.keys[0]) as api:
            foreign = copy.deepcopy(state.retained.release.files['30-migrate.yaml'][0])
            foreign['spec']['template']['spec']['containers'][0]['image'] = 'foreign/image'
            with pytest.raises(ValueError, match='participant creation'):
                api.create_resource(foreign)
        assert not state.requests
        return
    key = {'existing': 'ConfigMap:' + ns + ':loom-platform-config', 'secret': 'Secret:' + ns + ':loom-platform-db',
        'unknown': 'Job:' + ns + ':foreign', 'endpoint': state.keys[0]}[outside]
    with pytest.raises(ValueError, match='participant creation'):
        module().HTTPSParticipantCreationAPI(retained=state.retained, key=key,
            api_server='https://foreign.invalid' if outside == 'endpoint' else state.retained.inputs.config['kubernetes_api_server'],
            ssl_context=ssl.create_default_context(), qualify_creation=lambda *_: None)
