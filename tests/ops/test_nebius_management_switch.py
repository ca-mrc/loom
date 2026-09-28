"""Fixed upgrade retires old processes and never repeats an uncertain update."""
from __future__ import annotations

import copy
import json
import ssl
from dataclasses import replace
from uuid import uuid4

import httpx
import pytest
from tests.ops.test_nebius_application_setup import setup_request as setup_request
from tests.unit.test_nebius_management_render import (
    application_management_inputs as application_management_inputs,
)
from tests.unit.test_nebius_management_render import management_inputs as management_inputs
from tests.unit.test_nebius_management_render import render
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


class SwitchAPI:
    def __init__(self, request, target):
        self.document = copy.deepcopy(request.original)
        self.target = target
        self.calls = []
        self.failure = None
        self.processes = True

    def read(self):
        return copy.deepcopy(self.document)

    def desired(self, before, action, operation_id):
        from scripts.ops.nebius_management_switch import MARKER

        result = copy.deepcopy(before)
        result['metadata'].setdefault('annotations', {})[MARKER] = operation_id
        result['spec']['replicas'] = 0 if action == 'retire' else 1
        if action == 'activate':
            result['spec']['template'] = copy.deepcopy(self.target['spec']['template'])
        result['metadata']['generation'] += 1
        result['metadata']['resourceVersion'] = str(int(before['metadata']['resourceVersion']) + 1)
        return result

    def preview(self, before, operation_id):
        return self.desired(before, 'activate', operation_id)

    def patch(self, before, action, operation_id):
        self.calls.append(action)
        assert before['metadata']['resourceVersion'] == self.document['metadata']['resourceVersion']
        if self.failure == 'conflict':
            return False
        if self.failure == 'before':
            raise OSError('uncertain fixture write')
        self.document = self.desired(before, action, operation_id)
        if self.failure == 'after':
            raise OSError('uncertain fixture reply')
        return True

    def retired(self):
        return not self.processes


@pytest.fixture
def switch_inputs(setup_request, management_inputs):
    from scripts.ops.nebius_management_switch import ManagementSwitchRequest

    from loom_service.environment_management.deployment import render_management

    setup, _ = setup_request
    legacy = copy.deepcopy(management_inputs)
    runtime = legacy[0]['installation'].pop('applications')['runtime']
    legacy[0]['installation']['provider_runtime'] = {'kubernetes': runtime['kubernetes'],
        'cloud_credentials_file': '/var/run/loom-management-cloud/credentials.json'}
    original = copy.deepcopy(next(doc for doc in render(legacy).files['40-services.yaml']
        if doc['kind'] == 'Deployment'))
    original['metadata'].update(uid=str(uuid4()), resourceVersion='11', generation=1)
    original['status'] = {'observedGeneration': 1, 'replicas': 1, 'readyReplicas': 1}
    target = next(doc for doc in render_management(setup.deployment, candidate=setup.candidate,
        profile=setup.profile, repo_root=setup.repo_root).files['40-services.yaml'] if doc['kind'] == 'Deployment')
    request = ManagementSwitchRequest(setup=setup, original=original)
    return request, SwitchAPI(request, target)


def retire(inputs, state):
    from scripts.ops.nebius_management_switch import retire_management

    return retire_management(request=inputs[0], api=inputs[1], state_dir=state)


def activate(inputs, state):
    from scripts.ops.nebius_management_switch import activate_management

    return activate_management(request=inputs[0], api=inputs[1], state_dir=state)


def test_switch_preserves_original_and_waits_for_old_processes(switch_inputs, tmp_path):
    state = tmp_path / 'switch'
    request, api = switch_inputs
    original = copy.deepcopy(request.original)
    assert retire(switch_inputs, state) is False
    assert activate(switch_inputs, state) is False
    assert api.calls == ['retire']
    assert api.document['spec']['template'] == original['spec']['template']
    api.processes = False
    assert retire(switch_inputs, state) is True
    assert activate(switch_inputs, state) is True
    current = copy.deepcopy(api.document)
    assert activate(switch_inputs, state) is True
    assert api.document == current and api.calls == ['retire', 'activate']
    assert api.document['metadata']['uid'] == original['metadata']['uid']
    assert api.document['spec']['template']['spec']['serviceAccountName'] == 'loom-application-provisioner'
    assert api.document['spec']['selector'] == original['spec']['selector']
    assert json.loads((state / 'switch.json').read_text())['original'] == original
    assert (state / 'switch.json').stat().st_mode & 0o777 == 0o600


@pytest.mark.parametrize('action', ['retire', 'activate'])
@pytest.mark.parametrize('failure', ['before', 'after'])
def test_uncertain_patch_reconciles_without_resending(switch_inputs, tmp_path, action, failure):
    from scripts.ops.nebius_management_stage import ManagementStageError

    state = tmp_path / 'switch'
    _, api = switch_inputs
    if action == 'activate':
        retire(switch_inputs, state)
        api.processes = False
    api.failure = failure
    run = retire if action == 'retire' else activate
    if failure == 'before':
        for _ in range(2):
            with pytest.raises(ManagementStageError, match='unresolved'):
                run(switch_inputs, state)
    else:
        run(switch_inputs, state)
        run(switch_inputs, state)
    assert api.calls.count(action) == 1


@pytest.mark.parametrize('damage', ['uid', 'template', 'shared-uid'])
def test_drift_or_changed_input_cannot_be_adopted(switch_inputs, tmp_path, damage):
    from scripts.ops.nebius_management_stage import ManagementStageError

    request, api = switch_inputs
    state = tmp_path / 'switch'
    retire(switch_inputs, state)
    if damage == 'uid':
        api.document['metadata']['uid'] = str(uuid4())
    elif damage == 'template':
        api.document['spec']['template']['spec']['containers'][0]['image'] = 'foreign/image:latest'
    else:
        switch_inputs = replace(request, setup=replace(request.setup, shared_namespace_uid=str(uuid4()))), api
    with pytest.raises(ManagementStageError):
        retire(switch_inputs, state)
    assert api.calls == ['retire']


def test_definite_conflict_can_be_reobserved_but_not_retried_in_call(switch_inputs, tmp_path):
    state = tmp_path / 'switch'
    _, api = switch_inputs
    api.failure = 'conflict'
    assert retire(switch_inputs, state) is False
    assert api.calls == ['retire']
    api.failure = None
    assert retire(switch_inputs, state) is False
    assert api.calls == ['retire', 'retire']


def test_activation_needs_retained_retirement_and_cannot_hide_defaulted_privilege(switch_inputs, tmp_path):
    from scripts.ops.nebius_management_stage import ManagementStageError

    state = tmp_path / 'switch'
    _, api = switch_inputs
    with pytest.raises(ManagementStageError):
        activate(switch_inputs, state)
    assert not api.calls
    retire(switch_inputs, state)
    api.processes = False
    original = api.preview

    def unsafe(before, operation_id):
        result = original(before, operation_id)
        result['spec']['template']['spec']['hostNetwork'] = True
        return result

    api.preview = unsafe
    with pytest.raises(ManagementStageError, match='defaulting'):
        activate(switch_inputs, state)
    assert api.calls == ['retire']


def namespace_response(request, name):
    setup = request.setup
    identities = {setup.binding.namespace: setup.binding.namespace_uid, 'kube-system': setup.binding.kube_system_uid,
        setup.deployment.installation.applications.shared.platform_namespace: setup.shared_namespace_uid}
    return {'apiVersion': 'v1', 'kind': 'Namespace', 'metadata': {'name': name, 'uid': identities[name],
        'labels': {'loom.nebius/management-installation': setup.binding.installation_id,
            'pod-security.kubernetes.io/enforce': 'restricted'}}}


@pytest.mark.parametrize('status', [200, 409])
def test_https_switch_uses_exact_uid_version_and_only_fixed_fields(switch_inputs, tmp_path, status):
    from scripts.ops.nebius_management_switch import MARKER, HTTPSManagementSwitchAPI

    request, fake = switch_inputs
    patches = []

    def response(http):
        if http.method == 'PATCH':
            patch = json.loads(http.content)
            patches.append(patch)
            assert http.headers['Content-Type'] == 'application/json-patch+json'
            assert http.url.path.endswith('/deployments/loom-service') and not http.url.query
            if status == 200:
                owner = patch[3]['value'][MARKER]
                fake.document = fake.desired(fake.document, 'retire', owner)
            return httpx.Response(status, json=fake.document if status == 200 else {'kind': 'Status', 'reason': 'Conflict'})
        name = http.url.path.rsplit('/', 1)[1]
        value = fake.document if name == 'loom-service' else namespace_response(request, name)
        return httpx.Response(200, json=value)

    with HTTPSManagementSwitchAPI(request=request,
            api_server=request.setup.deployment.installation.applications.runtime.kubernetes.endpoint,
            ssl_context=ssl.create_default_context()) as api:
        api.client.close()
        api.client = httpx.Client(transport=httpx.MockTransport(response), base_url=api.api_server)
        assert retire((request, api), tmp_path / 'switch') is False
    assert len(patches) == 1
    assert patches[0][:3] == [
        {'op': 'test', 'path': '/metadata/uid', 'value': request.original['metadata']['uid']},
        {'op': 'test', 'path': '/metadata/resourceVersion', 'value': '11'},
        {'op': 'test', 'path': '/spec', 'value': request.original['spec']},
    ]
    assert [row['path'] for row in patches[0][3:]] == ['/metadata/annotations', '/spec/replicas']
    assert patches[0][-1]['value'] == 0


def test_https_switch_rejects_foreign_snapshot_before_network(switch_inputs):
    from scripts.ops.nebius_management_stage import ManagementStageError
    from scripts.ops.nebius_management_switch import HTTPSManagementSwitchAPI

    request, _ = switch_inputs
    calls = []
    with HTTPSManagementSwitchAPI(request=request,
            api_server=request.setup.deployment.installation.applications.runtime.kubernetes.endpoint,
            ssl_context=ssl.create_default_context()) as api:
        api.client.close()
        api.client = httpx.Client(transport=httpx.MockTransport(lambda http: calls.append(http)))
        before = copy.deepcopy(request.original)
        before['spec']['template']['spec']['containers'][0]['image'] = 'foreign:latest'
        with pytest.raises(ManagementStageError):
            api.patch(before, 'retire', str(uuid4()))
    assert not calls


def test_switch_transport_does_not_expose_inherited_setup_writes(switch_inputs):
    from scripts.ops.nebius_management_stage import ManagementStageError
    from scripts.ops.nebius_management_switch import HTTPSManagementSwitchAPI

    request, _ = switch_inputs
    calls = []
    with HTTPSManagementSwitchAPI(request=request,
            api_server=request.setup.deployment.installation.applications.runtime.kubernetes.endpoint,
            ssl_context=ssl.create_default_context()) as api:
        api.client.close()
        api.client = httpx.Client(transport=httpx.MockTransport(lambda http: calls.append(http)))
        document = copy.deepcopy(next(iter(api.documents.values())))
        document['metadata'].setdefault('annotations', {})['loom.nebius/management-stage-operation'] = str(uuid4())
        with pytest.raises(ManagementStageError):
            api.create_resource(document)
    assert not calls


@pytest.mark.parametrize('remaining', ['none', 'generation', 'deployment', 'pod', 'replicaset', 'foreign', 'page', 'fence', 'foreign-fence'])
def test_retirement_observes_controllers_and_terminating_pods(switch_inputs, remaining):
    from scripts.ops.nebius_management_stage import ManagementStageError
    from scripts.ops.nebius_management_switch import HTTPSManagementSwitchAPI

    request, fake = switch_inputs
    current = fake.desired(request.original, 'retire', str(uuid4()))
    current['status'] = {'observedGeneration': 2, 'replicas': 0, 'readyReplicas': 0}
    if remaining == 'generation':
        current['status']['observedGeneration'] = 1
    if remaining == 'deployment':
        current['status']['replicas'] = 1
    rs = {'metadata': {'name': 'loom-service-rs', 'namespace': request.setup.binding.namespace,
        'ownerReferences': [{'apiVersion': 'apps/v1', 'kind': 'Deployment', 'name': 'loom-service',
            'uid': request.original['metadata']['uid'], 'controller': True}]},
        'spec': {'replicas': 1 if remaining == 'replicaset' else 0}, 'status': {'replicas': 0}}
    if remaining == 'foreign':
        rs['metadata']['ownerReferences'][0]['uid'] = str(uuid4())

    def response(http):
        if http.method == 'POST':
            assert http.url.params['dryRun'] == 'All'
            if remaining == 'fence':
                return httpx.Response(201, json=json.loads(http.content))
            name = request.setup.deployment.installation.applications.authority.name + '-legacy-pods'
            return httpx.Response(403, json={'kind': 'Status', 'code': 403, 'reason': 'Forbidden',
                'message': 'unrelated denial' if remaining == 'foreign-fence' else name + ': legacy management process is retired'})
        name = http.url.path.rsplit('/', 1)[1]
        if name == 'loom-service':
            value = current
        elif name in {'pods', 'replicasets'}:
            assert http.url.params['labelSelector'] == 'app=loom-service'
            items = ([{'metadata': {'namespace': request.setup.binding.namespace, 'deletionTimestamp': '2026-09-28T00:00:00Z'}}]
                if name == 'pods' and remaining == 'pod' else [] if name == 'pods' else [rs])
            value = {'apiVersion': 'v1' if name == 'pods' else 'apps/v1',
                'kind': 'PodList' if name == 'pods' else 'ReplicaSetList',
                'metadata': {'resourceVersion': '3', 'continue': 'next' if remaining == 'page' else ''}, 'items': items}
        else:
            value = namespace_response(request, name)
        return httpx.Response(200, json=value)

    with HTTPSManagementSwitchAPI(request=request,
            api_server=request.setup.deployment.installation.applications.runtime.kubernetes.endpoint,
            ssl_context=ssl.create_default_context()) as api:
        api.client.close()
        api.client = httpx.Client(transport=httpx.MockTransport(response), base_url=api.api_server)
        if remaining in {'foreign', 'page', 'foreign-fence'}:
            with pytest.raises(ManagementStageError):
                api.retired()
        else:
            assert api.retired() is (remaining == 'none')
