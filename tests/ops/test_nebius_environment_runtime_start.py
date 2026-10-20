"""The real startup transport must not release Pods for dormant foreign Jobs."""
from __future__ import annotations

import copy
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
from scripts.ops.nebius_environment_runtime_start import _Workloads
from scripts.ops.nebius_ingress_stage import _key, _snapshot


@pytest.mark.parametrize('foreign', [True, False])
def test_reader_quota_cannot_start_preexisting_jobs(foreign):
    quota = {'apiVersion': 'v1', 'kind': 'ResourceQuota', 'metadata': {'namespace': 'worker-red',
        'name': 'loom-execution-disabled', 'uid': str(uuid4()), 'resourceVersion': '3'}, 'spec': {'hard': {'pods': '0'}}}
    desired = _snapshot(quota)
    desired['spec']['hard'] = {'pods': '1', 'count/jobs.batch': '0'}
    patches = []
    def request(method, path):
        assert method == 'GET' and '/namespaces/worker-red/' in path
        resource = path.split('/')[-1].split('?')[0]
        kind = {'jobs': 'Job', 'pods': 'Pod', 'cronjobs': 'CronJob', 'deployments': 'Deployment',
            'replicasets': 'ReplicaSet', 'statefulsets': 'StatefulSet', 'daemonsets': 'DaemonSet'}[resource]
        return {'apiVersion': 'v1' if kind == 'Pod' else ('batch/v1' if kind in {'Job', 'CronJob'} else 'apps/v1'),
            'kind': kind + 'List', 'metadata': {'resourceVersion': '5'}, 'items': ([{
                'metadata': {'namespace': 'worker-red', 'name': 'untracked-job', 'uid': str(uuid4())},
                'spec': {'parallelism': 1}}] if foreign and kind == 'Job' else [])}
    def patch(request):
        assert request.method == 'PATCH' and request.url.path == '/api/v1/namespaces/worker-red/resourcequotas/loom-execution-disabled'
        assert request.headers['Content-Type'] == 'application/json-patch+json'
        import json
        patches.extend(json.loads(request.content))
        return httpx.Response(200, json={**desired, 'metadata': {**desired['metadata'], 'uid': quota['metadata']['uid']}})
    with httpx.Client(transport=httpx.MockTransport(patch), base_url='https://cluster.test') as client:
        api = SimpleNamespace(client=client, policy=SimpleNamespace(host=SimpleNamespace(runtime=SimpleNamespace(binding='test'))),
            verify_identity=lambda binding: None, _request=request)
        runtime = _Workloads(api, {_key(quota): quota}, {_key(quota): desired}, 'eu-north1', {})
        if foreign:
            with pytest.raises(ValueError, match='environment_runtime'):
                runtime.preview_workload(_key(quota), quota, desired)
            assert not patches
        else:
            assert runtime.preview_workload(_key(quota), quota, desired) is not None
            assert patches[:3] == [{'op': 'test', 'path': '/metadata/uid', 'value': quota['metadata']['uid']},
                {'op': 'test', 'path': '/metadata/resourceVersion', 'value': '3'},
                {'op': 'test', 'path': '/spec', 'value': copy.deepcopy(quota['spec'])}]
