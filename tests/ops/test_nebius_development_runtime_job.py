"""Fixed runtime Job evidence, including the catalog's credential initializer."""
from __future__ import annotations

import copy
import importlib
import json
from types import SimpleNamespace

import httpx
import pytest


@pytest.fixture
def execution():
    uid = 'de273330-3309-4b65-8b34-d4bcc591316a'
    labels = {'app': 'loom-dev-catalog-test', 'batch.kubernetes.io/controller-uid': uid}
    container = {'name': 'setup', 'image': 'example@sha256:' + 'a' * 64,
        'command': ['python', '-m', 'loom.nebius_development_catalog'],
        'securityContext': {'allowPrivilegeEscalation': False}}
    spec = {'containers': [container], 'initContainers': [{**copy.deepcopy(container), 'name': 'prepare-admin-secret'}],
        'automountServiceAccountToken': False, 'restartPolicy': 'Never',
        'securityContext': {'runAsNonRoot': True, 'runAsUser': 1000}, 'volumes': []}
    job = {'apiVersion': 'batch/v1', 'kind': 'Job',
        'metadata': {'namespace': 'loom-dev', 'name': 'loom-dev-catalog-test', 'uid': uid},
        'spec': {'template': {'metadata': {'labels': labels}, 'spec': spec}},
        'status': {'conditions': [{'type': 'Complete', 'status': 'True'}], 'succeeded': 1}}
    pod = {'apiVersion': 'v1', 'kind': 'Pod', 'metadata': {'namespace': 'loom-dev',
        'name': 'loom-dev-catalog-test-abc', 'uid': '05aeb9b7-39d1-4e3d-8c10-789b0cda0194', 'labels': labels.copy(),
        'ownerReferences': [{'apiVersion': 'batch/v1', 'kind': 'Job', 'name': 'loom-dev-catalog-test',
            'uid': uid, 'controller': True}]}, 'spec': copy.deepcopy(spec),
        'status': {'phase': 'Succeeded', **{field: [{'name': name, 'restartCount': 0,
            'state': {'terminated': {'exitCode': 0}}}] for field, name in (
                ('containerStatuses', 'setup'), ('initContainerStatuses', 'prepare-admin-secret'))}}}
    report = {'operation_id': '699555d7-8ac9-4ea6-bb46-0b5e7d7f2180', 'target_id': 'dev', 'catalog_sha256': 'sha256:' + 'b' * 64}
    return SimpleNamespace(resources={'job': job}, pod=pod, report=report, calls=[], proofs=[],
        duplicate=False, continued=False, compressed=False, late_drift=False, reads=0)


def observe(state):
    name = 'scripts.ops.nebius_development_runtime_job'
    if importlib.util.find_spec(name) is None:
        pytest.fail('shared runtime Job execution evidence is missing')

    def recorded():
        state.reads += 1
        return copy.deepcopy(state.resources)

    def handle(message):
        assert message.method == 'GET'
        state.calls.append(message)
        path = message.url.path
        if path.endswith('/pods'):
            assert message.url.params['limit'] == '2'
            assert message.url.params['labelSelector'] == 'batch.kubernetes.io/controller-uid=de273330-3309-4b65-8b34-d4bcc591316a'
            return httpx.Response(200, json={'apiVersion': 'v1', 'kind': 'PodList',
                'metadata': {'continue': 'next'} if state.continued else {},
                'items': [state.pod, state.pod] if state.duplicate else [state.pod]})
        assert path.startswith('/api/v1/namespaces/loom-dev/pods/loom-dev-catalog-test-abc')
        if path.endswith('/log'):
            assert dict(message.url.params) == {'container': 'setup', 'limitBytes': '16384', 'timestamps': 'false'}
            if state.late_drift:
                state.resources['job']['metadata']['uid'] = '3168dcdc-b5db-475d-a762-a3f43fa4b78c'
            return httpx.Response(200, content=json.dumps(state.report).encode(),
                headers={'content-encoding': 'private-unsupported-codec'} if state.compressed else {})
        return httpx.Response(200, json=state.pod)

    with httpx.Client(base_url='https://api.example.test', transport=httpx.MockTransport(handle)) as client:
        def read(path):
            return client.get(path).json()

        def validate(proof):
            assert proof['catalog'] == {'operation_id': '699555d7-8ac9-4ea6-bb46-0b5e7d7f2180',
                'target_id': 'dev', 'catalog_sha256': 'sha256:' + 'b' * 64}
            state.proofs.append(proof)

        return importlib.import_module(name).read_runtime_job(client=client, read=read,
            recorded=recorded, private_inputs=lambda: None, region='eu-north1', report_field='catalog', validate=validate)


def test_catalog_job_receipt_requires_complete_sole_pod_and_initializer(execution):
    execution.resources['job']['status'] = {}
    assert observe(execution) is None
    assert not execution.calls and not execution.proofs
    execution.resources['job']['status'] = {'conditions': [{'type': 'Complete', 'status': 'True'}], 'succeeded': 1}
    assert observe(execution) == {'job_uid': 'de273330-3309-4b65-8b34-d4bcc591316a',
        'pod_uid': '05aeb9b7-39d1-4e3d-8c10-789b0cda0194', 'catalog': execution.report}
    assert execution.reads >= 3


@pytest.mark.parametrize('damage', ['duplicate', 'continued', 'compressed', 'late_drift', 'owner',
    'namespace', 'restart', 'init-failed', 'init-missing', 'extra-container', 'privileged', 'host', 'receipt', 'oversized'])
def test_runtime_job_rejects_ambiguous_execution_or_changed_authority(execution, damage):
    if damage in {'duplicate', 'continued', 'compressed', 'late_drift'}:
        setattr(execution, damage, True)
    elif damage == 'owner':
        execution.pod['metadata']['ownerReferences'][0]['uid'] = execution.pod['metadata']['uid']
    elif damage == 'namespace':
        execution.pod['metadata']['namespace'] = 'loom-staging'
    elif damage == 'restart':
        execution.pod['status']['containerStatuses'][0]['restartCount'] = 1
    elif damage == 'init-failed':
        execution.pod['status']['initContainerStatuses'][0]['state']['terminated']['exitCode'] = 1
    elif damage == 'init-missing':
        execution.pod['status'].pop('initContainerStatuses')
    elif damage == 'extra-container':
        execution.pod['spec']['containers'].append(copy.deepcopy(execution.pod['spec']['containers'][0]))
    elif damage == 'privileged':
        execution.pod['spec']['containers'][0]['securityContext']['privileged'] = True
    elif damage == 'host':
        execution.pod['spec']['hostNetwork'] = True
    elif damage == 'receipt':
        execution.report['target_id'] = 'staging'
    else:
        execution.report['extra'] = 'sensitive' * 3000
    with pytest.raises(ValueError, match='development runtime Job evidence unqualified'):
        observe(execution)
