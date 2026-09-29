"""Connected refresh uses only bound HTTPS reads and exact Deployment CAS."""
from __future__ import annotations

import copy
import json
import ssl
from uuid import uuid4

import httpx
import pytest
from tests.ops.test_nebius_management_refresh import refresh_request as refresh_request
from tests.ops.test_nebius_management_refresh_switch import (
    drained_observation as drained_observation,
)
from tests.ops.test_nebius_management_refresh_switch import (
    refresh as refresh,
)
from tests.unit.test_nebius_management_render import (
    application_management_inputs as application_management_inputs,
)
from tests.unit.test_nebius_management_render import (
    management_inputs as management_inputs,
)
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


@pytest.fixture
def connected(refresh, drained_observation):
    from scripts.ops.nebius_management_material import ManagementBinding
    from scripts.ops.nebius_management_refresh_live import HTTPSManagementRefreshSwitchAPI

    request, fake = refresh
    binding = ManagementBinding(str(request.render.after.installation_id), request.render.after.namespace,
        str(uuid4()), str(uuid4()))
    application = request.render.after.installation.applications
    shared_uid = str(uuid4())
    identities = {binding.namespace: binding.namespace_uid, 'kube-system': binding.kube_system_uid,
        application.shared.platform_namespace: shared_uid}
    state = {'calls': [], 'qualified': False, 'response': None, 'failure': None,
        'namespace_drift': False, 'final_drift': False, 'deployment_reads': 0,
        'sets': drained_observation[1], 'pods': drained_observation[2]}

    def response(message):
        state['calls'].append(message)
        path = message.url.path
        if message.method == 'PATCH':
            if state['response'] is not None:
                return state['response']
            patch = json.loads(message.content)
            assert patch[:3] == [
                {'op': 'test', 'path': '/metadata/uid', 'value': fake.document['metadata']['uid']},
                {'op': 'test', 'path': '/metadata/resourceVersion', 'value': fake.document['metadata']['resourceVersion']},
                {'op': 'test', 'path': '/spec', 'value': fake.document['spec']}]
            assert message.headers['content-type'] == 'application/json-patch+json'
            assert path == '/apis/apps/v1/namespaces/' + binding.namespace + '/deployments/loom-service'
            action = 'activate' if patch[4]['value'] == 1 else 'retire'
            paths = ['/metadata/annotations', '/spec/replicas'] + (['/spec/template'] if action == 'activate' else [])
            assert [row['path'] for row in patch[3:]] == paths
            desired = fake.desired(action)
            assert patch[3]['value'] == desired['metadata']['annotations']
            if action == 'activate':
                assert patch[-1]['value'] == desired['spec']['template']
            if message.url.params.get('dryRun') == 'All':
                return httpx.Response(200, json=desired)
            assert not message.url.query
            if state['failure'] == 'before':
                raise httpx.ReadTimeout('private-transport-data')
            fake.document = desired
            if state['failure'] == 'after':
                raise httpx.ReadTimeout('private-transport-data')
            return httpx.Response(200, json=desired)
        assert message.method == 'GET'
        name = path.rsplit('/', 1)[-1]
        if name == 'loom-service':
            state['deployment_reads'] += 1
            if state['final_drift'] and state['deployment_reads'] > 1:
                fake.document['spec']['replicas'] = 1
            return httpx.Response(200, json=fake.document)
        if name in {'pods', 'replicasets'}:
            assert dict(message.url.params) == {'limit': '100', 'labelSelector': 'app=loom-service'}
            return httpx.Response(200, json=state['pods' if name == 'pods' else 'sets'])
        assert path == '/api/v1/namespaces/' + name
        return httpx.Response(200, json={'apiVersion': 'v1', 'kind': 'Namespace', 'metadata': {
            'name': name, 'uid': str(uuid4()) if state['namespace_drift'] else identities[name],
            'labels': {'loom.nebius/management-installation': binding.installation_id,
                'pod-security.kubernetes.io/enforce': 'restricted'}}})

    def activation(actual):
        assert actual == request
        return state['qualified']

    with HTTPSManagementRefreshSwitchAPI(request=request, binding=binding, shared_namespace_uid=shared_uid,
            api_server=application.runtime.kubernetes.endpoint, ssl_context=ssl.create_default_context(),
            activation_check=activation) as api:
        api.client.close()
        api.client = httpx.Client(base_url=api.api_server, transport=httpx.MockTransport(response))
        yield api, state, fake


def test_connected_cutover_requires_exact_drain_and_activation_evidence(refresh, connected, tmp_path):
    from scripts.ops.nebius_management_refresh_switch import switch_refresh

    request, _ = refresh
    api, state, fake = connected
    args = dict(request=request, api=api, state_dir=tmp_path)
    assert switch_refresh(**args, activate=False) is False
    fake.document['status'] = {'observedGeneration': fake.document['metadata']['generation']}
    assert switch_refresh(**args, activate=False) is True
    assert switch_refresh(**args, activate=True) is False
    state['qualified'] = True
    assert switch_refresh(**args, activate=True) is True
    assert switch_refresh(**args, activate=True) is True
    writes = [row for row in state['calls'] if row.method == 'PATCH' and not row.url.query]
    assert len(writes) == 2
    assert not any(row.method not in {'GET', 'PATCH'} for row in state['calls'])


@pytest.fixture
def running(refresh, connected):
    request, _ = refresh
    api, state, fake = connected
    fake.document = fake.desired('activate')
    fake.document['status'] = {'observedGeneration': fake.document['metadata']['generation'],
        'replicas': 1, 'updatedReplicas': 1, 'readyReplicas': 1, 'availableReplicas': 1}
    replica = copy.deepcopy(state['sets']['items'][0])
    replica['metadata']['uid'] = str(uuid4())
    replica['metadata']['name'] = 'loom-service-1234567890'
    replica['metadata']['labels']['pod-template-hash'] = '1234567890'
    replica['spec'] = {'replicas': 1, 'template': copy.deepcopy(fake.document['spec']['template']),
        'selector': {'matchLabels': {'app': 'loom-service', 'pod-template-hash': '1234567890'}}}
    replica['spec']['template']['metadata']['labels']['pod-template-hash'] = '1234567890'
    replica['status'] = {'observedGeneration': replica['metadata']['generation'], 'replicas': 1,
        'readyReplicas': 1, 'availableReplicas': 1, 'fullyLabeledReplicas': 1}
    template = replica['spec']['template']
    pod = {'apiVersion': 'v1', 'kind': 'Pod', 'metadata': {**copy.deepcopy(template['metadata']),
        'namespace': request.render.after.namespace, 'name': replica['metadata']['name'] + '-abc', 'uid': str(uuid4()),
        'ownerReferences': [{'apiVersion': 'apps/v1', 'kind': 'ReplicaSet', 'name': replica['metadata']['name'],
            'uid': replica['metadata']['uid'], 'controller': True, 'blockOwnerDeletion': True}]},
        'spec': copy.deepcopy(template['spec']), 'status': {'phase': 'Running',
            'conditions': [{'type': 'Ready', 'status': 'True'}],
            'containerStatuses': [{'name': 'loom-service', 'ready': True, 'restartCount': 0,
                'state': {'running': {'startedAt': '2026-09-29T22:00:00Z'}}}],
            'initContainerStatuses': [{'name': container['name'], 'ready': True, 'restartCount': 0,
                'state': {'terminated': {'exitCode': 0}}} for container in template['spec'].get('initContainers', [])]}}
    state['sets']['items'] = [replica]
    state['pods']['items'] = [pod]
    return api, state, fake, replica, pod


def test_ready_refresh_requires_actual_current_owned_pod_and_final_deployment_readback(running):
    api, state, _, _, _ = running
    assert api.workload_ready() is True
    assert state['deployment_reads'] == 2
    assert all(message.method == 'GET' for message in state['calls'])


@pytest.mark.parametrize('damage', ['stale_deployment', 'stale_replicaset', 'pending_pod', 'extra_pod',
    'terminating_pod', 'foreign_owner', 'foreign_image', 'privileged', 'failed_init', 'partial_list', 'final_drift'])
def test_public_refresh_readiness_rejects_stale_or_foreign_runtime(running, damage):
    from scripts.ops.nebius_management_stage import ManagementStageError

    api, state, fake, replica, pod = running
    pending = damage in {'stale_deployment', 'stale_replicaset', 'pending_pod', 'extra_pod', 'terminating_pod'}
    if damage == 'stale_deployment':
        fake.document['status']['observedGeneration'] -= 1
    elif damage == 'stale_replicaset':
        replica['status']['observedGeneration'] -= 1
    elif damage == 'pending_pod':
        pod['status']['containerStatuses'][0]['ready'] = False
    elif damage == 'extra_pod':
        state['pods']['items'].append(copy.deepcopy(pod))
    elif damage == 'terminating_pod':
        pod['metadata']['deletionTimestamp'] = '2026-09-29T22:00:01Z'
    elif damage == 'foreign_owner':
        pod['metadata']['ownerReferences'][0]['uid'] = str(uuid4())
    elif damage == 'foreign_image':
        pod['spec']['containers'][0]['image'] = 'foreign:latest'
    elif damage == 'privileged':
        pod['spec']['containers'][0]['securityContext']['privileged'] = True
    elif damage == 'failed_init':
        pod['status']['initContainerStatuses'][0]['state']['terminated']['exitCode'] = 1
    elif damage == 'partial_list':
        state['pods']['metadata']['continue'] = 'next'
    else:
        # Existing response hook changes replicas to1; changing desired replicas
        # cannot expose drift here, so drift the returned live generation instead.
        original_read = api.read
        def changed():
            value = original_read()
            if state['deployment_reads'] > 1:
                value['metadata']['generation'] += 1
            return value
        api.read = changed
    if pending:
        assert api.workload_ready() is False
    else:
        with pytest.raises(ManagementStageError):
            api.workload_ready()
    assert all(message.method == 'GET' for message in state['calls'])


@pytest.mark.parametrize('status,reason', [(409, 'Conflict'), (422, 'Invalid')])
def test_only_definite_kubernetes_rejection_clears_intent(refresh, connected, tmp_path, status, reason):
    from scripts.ops.nebius_management_refresh_switch import switch_refresh

    api, state, _ = connected
    state['response'] = httpx.Response(status, json={'apiVersion': 'v1', 'kind': 'Status',
        'status': 'Failure', 'code': status, 'reason': reason})
    assert switch_refresh(request=refresh[0], api=api, state_dir=tmp_path, activate=False) is False
    assert json.loads((tmp_path / 'cutover.json').read_text())['phase'] == 'prepared'


@pytest.mark.parametrize('status', [409, 422, 500])
def test_proxy_or_truncated_error_is_uncertain_and_not_retried(refresh, connected, tmp_path, status):
    from scripts.ops.nebius_management_refresh_switch import switch_refresh

    api, state, _ = connected
    state['response'] = httpx.Response(status, content=b'private-non-kubernetes-proxy-error')
    for _ in range(2):
        with pytest.raises(ValueError, match='unresolved') as error:
            switch_refresh(request=refresh[0], api=api, state_dir=tmp_path, activate=False)
        assert 'private' not in str(error.value)
    assert len([row for row in state['calls'] if row.method == 'PATCH']) == 1
    assert json.loads((tmp_path / 'cutover.json').read_text())['phase'] == 'retire_intent'


@pytest.mark.parametrize('failure', ['before', 'after'])
def test_lost_patch_outcome_uses_only_readback(refresh, connected, tmp_path, failure):
    from scripts.ops.nebius_management_refresh_switch import switch_refresh

    api, state, _ = connected
    state['failure'] = failure
    for _ in range(2):
        if failure == 'before':
            with pytest.raises(ValueError, match='unresolved'):
                switch_refresh(request=refresh[0], api=api, state_dir=tmp_path, activate=False)
        else:
            assert switch_refresh(request=refresh[0], api=api, state_dir=tmp_path, activate=False) is False
    assert len([row for row in state['calls'] if row.method == 'PATCH']) == 1


@pytest.mark.parametrize('damage', ['namespace', 'snapshot', 'operation', 'action'])
def test_foreign_scope_cannot_reach_a_patch(refresh, connected, damage):
    api, state, fake = connected
    original = copy.deepcopy(fake.document)
    operation, action = str(refresh[0].operation_id), 'retire'
    if damage == 'namespace':
        state['namespace_drift'] = True
    elif damage == 'snapshot':
        original['spec']['template']['spec']['containers'][0]['image'] = 'unapproved:latest'
    elif damage == 'operation':
        operation = str(uuid4())
    else:
        action = 'delete'
    with pytest.raises(RuntimeError):
        api.patch(original, action, operation)
    assert not any(row.method != 'GET' for row in state['calls'])


def test_drain_rechecks_stopped_deployment_after_inventory(connected):
    api, state, fake = connected
    fake.document = fake.desired('retire')
    fake.document['status'] = {'observedGeneration': fake.document['metadata']['generation']}
    state['final_drift'] = True
    with pytest.raises(RuntimeError, match='drain'):
        api.retired()
    assert state['deployment_reads'] == 2
