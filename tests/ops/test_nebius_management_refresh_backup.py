"""Management migration needs exact refresh Job execution and off-node bytes."""
from __future__ import annotations

import copy
import hashlib
import io
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
def backup(resources_request, tmp_path):
    from scripts.ops.nebius_management_refresh_backup import HTTPSManagementRefreshBackupAPI
    from scripts.ops.nebius_management_refresh_resources import stage_refresh_resources

    request = resources_request
    fake = PhaseAPI(request.binding)
    stage_refresh_resources(request=request, phase='backup', api=fake, state_dir=tmp_path)
    job, = fake.resources.values()
    job['status'] = {'conditions': [{'type': 'Complete', 'status': 'True'}], 'succeeded': 1}
    namespace, name, uid = (job['metadata'][key] for key in ('namespace', 'name', 'uid'))
    spec = copy.deepcopy(job['spec']['template']['spec'])
    pod = {'apiVersion': 'v1', 'kind': 'Pod', 'metadata': {'namespace': namespace, 'name': name + '-abc',
        'uid': str(uuid4()), 'labels': {'batch.kubernetes.io/controller-uid': uid}, 'ownerReferences': [
            {'apiVersion': 'batch/v1', 'kind': 'Job', 'name': name, 'uid': uid, 'controller': True}]},
        'spec': spec, 'status': {'phase': 'Succeeded', **{status: [
            {'name': container['name'], 'restartCount': 0, 'state': {'terminated': {'exitCode': 0}}}
            for container in spec.get(field, [])] for field, status in (
                ('containers', 'containerStatuses'), ('initContainers', 'initContainerStatuses'))}}}
    data = b'PGDMP-fixture-database'
    checksum = hashlib.sha256(data).hexdigest()
    report = {'backup_key': namespace + '/2026/09/29/220000-' + checksum[:12] + '.dump',
        'sha256': checksum, 'bytes': len(data)}
    state = SimpleNamespace(request=request, path=tmp_path, job=job, pod=pod, data=data, report=report,
        calls=[], storage_calls=[], shared_drift=False, final_drift=False, reads=0)
    binding = request.binding
    shared = request.switch.render.after.installation.applications.shared.platform_namespace
    namespaces = {'kube-system': binding.kube_system_uid, namespace: binding.namespace_uid, shared: request.shared_namespace_uid}

    def handler(message):
        state.calls.append(message)
        assert message.method == 'GET'
        path = message.url.path
        if path in {'/api/v1/namespaces/' + ns for ns in namespaces}:
            ns = path.rsplit('/', 1)[-1]
            return httpx.Response(200, json={'apiVersion': 'v1', 'kind': 'Namespace', 'metadata': {
                'name': ns, 'uid': str(uuid4()) if ns == shared and state.shared_drift else namespaces[ns],
                'labels': {'loom.nebius/management-installation': binding.installation_id,
                    'pod-security.kubernetes.io/enforce': 'restricted'}}})
        if path.endswith('/jobs/' + name):
            state.reads += 1
            value = copy.deepcopy(state.job)
            if state.final_drift and state.storage_calls:
                value['metadata']['uid'] = str(uuid4())
            return httpx.Response(200, json=value)
        if path.endswith('/pods'):
            assert message.url.params['labelSelector'] == 'batch.kubernetes.io/controller-uid=' + uid
            item = {key: value for key, value in state.pod.items() if key not in {'apiVersion', 'kind'}}
            return httpx.Response(200, json={'apiVersion': 'v1', 'kind': 'PodList', 'metadata': {}, 'items': [item]})
        if path.endswith('/log'):
            return httpx.Response(200, content=(json.dumps(state.report) + '\nNebius platform backup complete\n').encode())
        assert path.endswith('/pods/' + pod['metadata']['name'])
        return httpx.Response(200, json=state.pod)

    class Storage:
        def head_object(self, **args):
            state.storage_calls.append(('HEAD', args))
            assert args == {'Bucket': request.switch.render.after.backup_bucket, 'Key': report['backup_key']}
            return {'ContentLength': len(data), 'Metadata': {'sha256': checksum}, 'ETag': 'fixture-etag'}

        def get_object(self, **args):
            state.storage_calls.append(('GET', args))
            assert args == {'Bucket': request.switch.render.after.backup_bucket, 'Key': report['backup_key'], 'IfMatch': 'fixture-etag'}
            return {'ContentLength': len(data), 'ETag': 'fixture-etag', 'Body': io.BytesIO(state.data)}

    endpoint = request.switch.render.after.installation.applications.runtime.kubernetes.endpoint
    with HTTPSManagementRefreshBackupAPI(request=request, api_server=endpoint, ssl_context=ssl.create_default_context()) as api:
        api.client.close()
        api.client = httpx.Client(base_url=endpoint, transport=httpx.MockTransport(handler))
        yield api, state, Storage()


def test_refresh_backup_receipt_requires_exact_job_and_retained_bucket_bytes(backup):
    api, state, storage = backup
    result = api.backup_receipt(state_dir=state.path, client=storage)
    assert result == {'job_uid': state.job['metadata']['uid'], 'key': state.report['backup_key'],
        'sha256': hashlib.sha256(b'PGDMP-fixture-database').hexdigest(), 'bytes': 22}
    assert [verb for verb, _ in state.storage_calls] == ['HEAD', 'GET', 'HEAD']
    assert state.reads >= 4
    assert all(call.method == 'GET' for call in state.calls)


@pytest.mark.parametrize('damage', ['lost_journal', 'job_uid', 'job_image', 'pending', 'failed', 'pod_image',
    'restart', 'shared_namespace', 'object_bytes', 'final_drift'])
def test_unbound_or_unreadable_refresh_backup_never_qualifies(backup, damage):
    api, state, storage = backup
    if damage == 'lost_journal':
        (state.path / 'stage.json').unlink()
    elif damage == 'job_uid':
        state.job['metadata']['uid'] = str(uuid4())
    elif damage == 'job_image':
        state.job['spec']['template']['spec']['containers'][0]['image'] = 'foreign:latest'
    elif damage == 'pending':
        state.job['status'] = {}
    elif damage == 'failed':
        state.job['status']['conditions'].append({'type': 'Failed', 'status': 'True'})
    elif damage == 'pod_image':
        state.pod['spec']['containers'][0]['image'] = 'foreign:latest'
    elif damage == 'restart':
        state.pod['status']['containerStatuses'][0]['restartCount'] = 1
    elif damage == 'shared_namespace':
        state.shared_drift = True
    elif damage == 'object_bytes':
        state.data = b'PGDMP-corrupted'
    else:
        state.final_drift = True
    with pytest.raises(RuntimeError) as error:
        api.backup_receipt(state_dir=state.path, client=storage)
    assert 'fixture' not in str(error.value) and 'foreign' not in str(error.value)
    if damage not in {'object_bytes', 'final_drift'}:
        assert not state.storage_calls


def test_refresh_evidence_adapter_has_no_token_or_resource_write_route(backup):
    api, state, _ = backup
    with pytest.raises(RuntimeError):
        api.runtime_token(service_account_uid=str(uuid4()))
    with pytest.raises(RuntimeError):
        api.create_resource(state.job)
    assert not state.calls
