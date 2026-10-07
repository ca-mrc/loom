"""A refresh probe needs its exact Job, Pod, settings and closed runtime report."""
from __future__ import annotations

import copy
import json
import ssl
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
from tests.ops.test_nebius_management_refresh import refresh_request as refresh_request
from tests.ops.test_nebius_management_refresh_resources import (
    resources_request as resources_request,
)
from tests.ops.test_nebius_management_stage import PhaseAPI
from tests.unit.test_nebius_management_render import (
    application_management_inputs as application_management_inputs,
)
from tests.unit.test_nebius_management_render import management_inputs as management_inputs
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


@pytest.fixture
def probe_live(resources_request, tmp_path):
    from scripts.ops.nebius_management_refresh_evidence import HTTPSManagementRefreshEvidenceAPI
    from scripts.ops.nebius_management_refresh_resources import stage_refresh_resources

    def build(phase='manager-probe'):
        request = resources_request
        fake = PhaseAPI(request.binding)
        state_dir = tmp_path / phase
        stage_refresh_resources(request=request, phase=phase, api=fake, state_dir=state_dir)
        config = next(value for value in fake.resources.values() if value['kind'] == 'ConfigMap')
        job = next(value for value in fake.resources.values() if value['kind'] == 'Job')
        namespace, name, uid = (job['metadata'][key] for key in ('namespace', 'name', 'uid'))
        job['status'] = {'conditions': [{'type': 'Complete', 'status': 'True'}], 'succeeded': 1}
        pod = {'apiVersion': 'v1', 'kind': 'Pod', 'metadata': {
            **copy.deepcopy(job['spec']['template']['metadata']), 'namespace': namespace, 'name': name + '-abc',
            'uid': str(uuid4()), 'ownerReferences': [{'apiVersion': 'batch/v1', 'kind': 'Job',
                'name': name, 'uid': uid, 'controller': True, 'blockOwnerDeletion': True}]},
            'spec': copy.deepcopy(job['spec']['template']['spec']),
            'status': {'phase': 'Succeeded', 'containerStatuses': [{'name': job['spec']['template']['spec']['containers'][0]['name'],
                'restartCount': 0, 'state': {'terminated': {'exitCode': 0}}}]}}
        pod['metadata']['labels']['batch.kubernetes.io/controller-uid'] = uid
        pod['metadata']['labels']['topology.kubernetes.io/region'] = request.switch.render.after.installation.foundation.platform_config['region']
        settings = json.loads(config['data']['probe.json'])
        report = {'schema': 'loom.nebius-management-refresh-probe.v1', 'status': 'qualified',
            'mode': settings['mode'], 'revision': settings['expected_revision'], 'operations_checked': 0}
        state = SimpleNamespace(request=request, phase=phase, state_dir=state_dir, fake=fake, job=job, pod=pod,
            report=report, log=None, calls=[], extra_pod=False, continuation=False, final_drift=False, namespace_drift=False,
            pod_reads=0, on_pod_read=None)
        binding = request.binding
        identities = {binding.namespace: binding.namespace_uid, 'kube-system': binding.kube_system_uid,
            request.switch.render.after.installation.applications.shared.platform_namespace: request.shared_namespace_uid}

        def respond(message):
            state.calls.append(message)
            assert message.method == 'GET'
            path = message.url.path
            if path in {'/api/v1/namespaces/' + value for value in identities}:
                ns = path.rsplit('/', 1)[-1]
                return httpx.Response(200, json={'apiVersion': 'v1', 'kind': 'Namespace', 'metadata': {
                    'name': ns, 'uid': str(uuid4()) if state.namespace_drift else identities[ns],
                    'labels': {'loom.nebius/management-installation': binding.installation_id,
                        'pod-security.kubernetes.io/enforce': 'restricted'}}})
            if '/configmaps/' in path:
                return httpx.Response(200, json=config)
            if '/jobs/' in path:
                return httpx.Response(200, json=state.job)
            base = '/api/v1/namespaces/' + namespace + '/pods'
            if path == base:
                assert dict(message.url.params) == {'labelSelector': 'batch.kubernetes.io/controller-uid=' + uid, 'limit': '2'}
                item = {key: value for key, value in state.pod.items() if key not in {'apiVersion', 'kind'}}
                return httpx.Response(200, json={'apiVersion': 'v1', 'kind': 'PodList',
                    'metadata': {'continue': 'next' if state.continuation else ''},
                    'items': [item] * (2 if state.extra_pod else 1)})
            if path.endswith('/log'):
                assert message.url.params['limitBytes'] == '16384'
                return httpx.Response(200, content=state.log if state.log is not None else json.dumps(state.report).encode())
            assert path == base + '/' + state.pod['metadata']['name']
            state.pod_reads += 1
            if state.on_pod_read is not None:
                state.on_pod_read(state, state.pod_reads)
            value = copy.deepcopy(state.pod)
            if state.final_drift:
                value['metadata']['uid'] = str(uuid4())
            return httpx.Response(200, json=value)

        endpoint = request.switch.render.after.installation.applications.runtime.kubernetes.endpoint
        api = HTTPSManagementRefreshEvidenceAPI(request=request, phase=phase, api_server=endpoint,
            ssl_context=ssl.create_default_context())
        api.client.close()
        api.client = httpx.Client(base_url=endpoint, transport=httpx.MockTransport(respond))
        return api, state

    return build


@pytest.mark.parametrize('phase', ['manager-probe', 'shared-probe', 'post-migration-probe'])
def test_exact_runtime_report_is_bound_to_recorded_config_job_and_pod(probe_live, phase):
    api, state = probe_live(phase)
    with api:
        result = api.probe_report(state.state_dir)
        assert result == {'job_uid': state.job['metadata']['uid'], 'pod_uid': state.pod['metadata']['uid'],
            'probe': state.report}
        assert api.probe_report(state.state_dir) == result
    assert all(message.method == 'GET' for message in state.calls)


@pytest.mark.parametrize('damage', ['pending', 'failed', 'recreated_job', 'extra_pod', 'continuation', 'owner',
    'namespace', 'image', 'write_role', 'privileged', 'restart', 'nonzero', 'report', 'wrong_revision',
    'count_bool', 'log_secret', 'log_oversize', 'trailer', 'final_drift', 'lost_state'])
def test_incomplete_or_foreign_evidence_never_qualifies(probe_live, damage):
    from scripts.ops.nebius_management_stage import ManagementStageError

    api, state = probe_live()
    if damage == 'pending':
        state.job['status'] = {}
    elif damage == 'failed':
        state.job['status']['conditions'] = [{'type': 'Failed', 'status': 'True'}]
    elif damage == 'recreated_job':
        state.job['metadata']['uid'] = str(uuid4())
    elif damage in {'extra_pod', 'continuation', 'final_drift'}:
        setattr(state, damage, True)
    elif damage == 'namespace':
        state.namespace_drift = True
    elif damage == 'owner':
        state.pod['metadata']['ownerReferences'][0]['uid'] = str(uuid4())
    elif damage in {'image', 'write_role', 'privileged'}:
        container = state.pod['spec']['containers'][0]
        if damage == 'image':
            container['image'] = 'foreign:latest'
        elif damage == 'write_role':
            container['env'][0]['valueFrom']['secretKeyRef']['key'] = 'admin-url'
        else:
            container['securityContext']['privileged'] = True
    elif damage in {'restart', 'nonzero'}:
        status = state.pod['status']['containerStatuses'][0]
        if damage == 'restart':
            status['restartCount'] = 1
        else:
            status['state']['terminated']['exitCode'] = 1
    elif damage == 'report':
        state.report['private_plan'] = 'private-marker'
    elif damage == 'wrong_revision':
        state.report['revision'] = '0000'
    elif damage == 'count_bool':
        state.report['operations_checked'] = True
    elif damage == 'log_secret':
        state.log = b'private-marker'
    elif damage == 'log_oversize':
        state.log = b' ' * 16385
    elif damage == 'trailer':
        state.log = json.dumps(state.report).encode() + b'\nprivate-marker'
    elif damage == 'lost_state':
        (state.state_dir / 'stage.json').unlink()
    with api:
        if damage == 'pending':
            assert api.probe_report(state.state_dir) is None
        else:
            with pytest.raises(ManagementStageError) as error:
                api.probe_report(state.state_dir)
            assert 'private-marker' not in str(error.value)


def test_completed_shared_probe_cannot_claim_management_operation_inspection(probe_live):
    from scripts.ops.nebius_management_stage import ManagementStageError

    api, state = probe_live('shared-probe')
    state.report['operations_checked'] = 1
    with api, pytest.raises(ManagementStageError):
        api.probe_report(state.state_dir)


@pytest.mark.parametrize('field', ['resource_version', 'managed_fields', 'terminal_resources', 'job_bookkeeping'])
def test_completed_probe_requires_equal_full_readbacks_after_bookkeeping_settles(probe_live, field):
    api, state = probe_live()

    def converge(state, read):
        if read != 1:
            return
        if field == 'resource_version':
            state.pod['metadata']['resourceVersion'] = '2'
        elif field == 'managed_fields':
            state.pod['metadata']['managedFields'] = [{'manager': 'kubelet', 'fieldsV1': {'f:status': {}}}]
        elif field == 'terminal_resources':
            state.pod['status']['resources'] = {'requests': {'cpu': '0'}, 'limits': {}}
        else:
            state.job['metadata']['resourceVersion'] = '2'
            state.job['metadata']['managedFields'] = [{'manager': 'kube-controller-manager'}]

    state.on_pod_read = converge
    with api:
        result = api.probe_report(state.state_dir)
    assert result == {'job_uid': state.job['metadata']['uid'], 'pod_uid': state.pod['metadata']['uid'],
        'probe': state.report}
    assert state.pod_reads == 2  # A single changed readback must never qualify.
    assert all(message.method == 'GET' for message in state.calls)


def test_continuously_changing_bookkeeping_exhausts_bounded_readbacks(probe_live):
    from scripts.ops.nebius_management_stage import ManagementStageError

    api, state = probe_live()
    state.on_pod_read = lambda state, read: state.pod['metadata'].update(resourceVersion=str(read))
    with api, pytest.raises(ManagementStageError):
        api.probe_report(state.state_dir)
    assert state.pod_reads == 3
    assert all(message.method == 'GET' for message in state.calls)


@pytest.mark.parametrize('damage', ['report_bool', 'restart_bool', 'job_count_bool', 'unknown_accounting'])
def test_bookkeeping_convergence_cannot_accept_malformed_equal_python_values(probe_live, damage):
    from scripts.ops.nebius_management_stage import ManagementStageError

    api, state = probe_live()

    def changed(state, read):
        if read != 1:
            return
        state.pod['metadata']['resourceVersion'] = '2'
        if damage == 'report_bool':
            state.report['operations_checked'] = False
        elif damage == 'restart_bool':
            state.pod['status']['containerStatuses'][0]['restartCount'] = False
        elif damage == 'job_count_bool':
            state.job['status']['succeeded'] = True
        else:
            state.pod['status']['resources'] = {'foreign': 'private-marker'}

    state.on_pod_read = changed
    with api, pytest.raises(ManagementStageError):
        api.probe_report(state.state_dir)
    assert state.pod_reads == 1


@pytest.mark.parametrize('damage', ['uid', 'deletion', 'labels', 'owner', 'image', 'command', 'secret',
    'privileged', 'resources', 'restart', 'exit', 'phase', 'conditions', 'unknown_status', 'annotations',
    'job_status', 'namespace', 'report', 'log'])
@pytest.mark.parametrize('when', [1, 2])
def test_convergence_never_hides_other_changes_or_retries_rejected_evidence(probe_live, damage, when):
    from scripts.ops.nebius_management_stage import ManagementStageError

    api, state = probe_live()

    def changed(state, read):
        state.pod['metadata']['resourceVersion'] = str(read)
        if read != when:
            return
        pod = state.pod
        if damage == 'uid':
            pod['metadata']['uid'] = str(uuid4())
        elif damage == 'deletion':
            pod['metadata']['deletionTimestamp'] = '2026-10-07T00:00:00Z'
        elif damage == 'labels':
            pod['metadata']['labels']['foreign'] = 'private-marker'
        elif damage == 'owner':
            pod['metadata']['ownerReferences'][0]['uid'] = str(uuid4())
        elif damage in {'image', 'command', 'secret', 'privileged', 'resources'}:
            container = pod['spec']['containers'][0]
            if damage == 'image':
                container['image'] = 'foreign:latest'
            elif damage == 'command':
                container['command'] = ['foreign']
            elif damage == 'secret':
                container['env'][0]['valueFrom']['secretKeyRef']['key'] = 'admin-url'
            elif damage == 'privileged':
                container['securityContext']['privileged'] = True
            else:
                container['resources'] = {'requests': {'cpu': '99'}}
        elif damage in {'restart', 'exit'}:
            status = pod['status']['containerStatuses'][0]
            if damage == 'restart':
                status['restartCount'] = 1
            else:
                status['state']['terminated']['exitCode'] = 1
        elif damage == 'phase':
            pod['status']['phase'] = 'Failed'
        elif damage == 'conditions':
            pod['status']['conditions'] = [{'type': 'Ready', 'status': 'True'}]
        elif damage == 'unknown_status':
            pod['status']['foreign'] = 'private-marker'
        elif damage == 'annotations':
            pod['metadata']['annotations'] = {'foreign': 'private-marker'}
        elif damage == 'job_status':
            state.job['status']['failed'] = 1
        elif damage == 'namespace':
            state.namespace_drift = True
        elif damage == 'report':
            state.report['operations_checked'] = 1
        else:
            state.log = b'private-marker'

    state.on_pod_read = changed
    with api, pytest.raises(ManagementStageError) as error:
        api.probe_report(state.state_dir)
    assert state.pod_reads == when  # Reject immediately, including after one benign change.
    assert 'private-marker' not in str(error.value)
    assert all(message.method == 'GET' for message in state.calls)
