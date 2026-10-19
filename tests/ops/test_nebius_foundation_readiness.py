"""Resource completion cannot stand in for live database/public dependency proof."""
from __future__ import annotations

import copy
import importlib
import json
import tomllib
from uuid import uuid4

import httpx
import pytest

from loom_service.environment_management.kubernetes_provider import KubernetesEnvironmentProvider
from tests.ops.test_nebius_environment_foundation import Remote, prepare, run
from tests.support.nebius_platform import platform_inputs as platform_inputs


def module():
    name = 'scripts.ops.nebius_foundation_readiness'
    assert importlib.util.find_spec(name), 'foundation readiness observer is missing'
    return importlib.import_module(name)


def database_controller(remote):
    """Remote API double: Kubernetes controller effects, not provisioner writes."""
    ns = 'app-red'
    database = remote.kubernetes[f'/apis/apps/v1/namespaces/{ns}/statefulsets/loom-postgres']
    service = remote.kubernetes[f'/api/v1/namespaces/{ns}/services/loom-postgres']
    database['status'].update(currentReplicas=1, currentRevision='postgres-v1', updateRevision='postgres-v1')
    pod = copy.deepcopy(database['spec']['template'])
    pod.update(apiVersion='v1', kind='Pod')
    pod['metadata'].update(name='loom-postgres-0', namespace=ns, uid=str(uuid4()),
        ownerReferences=[{'apiVersion': 'apps/v1', 'kind': 'StatefulSet', 'name': 'loom-postgres',
            'uid': database['metadata']['uid'], 'controller': True}])
    pod['metadata']['labels']['controller-revision-hash'] = 'postgres-v1'
    pod['spec'].setdefault('volumes', []).append({'name': 'data', 'persistentVolumeClaim': {'claimName': 'data-loom-postgres-0'}})
    pod['status'] = {'phase': 'Running', 'podIP': '10.0.1.10', 'podIPs': [{'ip': '10.0.1.10'}],
        'containerStatuses': [{'name': 'loom-postgres', 'ready': True}]}
    remote.kubernetes[f'/api/v1/namespaces/{ns}/pods'] = {'apiVersion': 'v1', 'kind': 'PodList',
        'metadata': {'resourceVersion': '2'}, 'items': [pod]}
    pvc = remote.kubernetes[f'/api/v1/namespaces/{ns}/persistentvolumeclaims/data-loom-postgres-0']
    pvc['spec']['volumeName'] = 'pv-owned'
    pvc['status'] = {'phase': 'Bound'}
    remote.kubernetes[f'/api/v1/namespaces/{ns}/persistentvolumeclaims/data-loom-postgres-0'] = pvc
    port = {'port': 5432, 'protocol': 'TCP'}
    if 'name' in service['spec']['ports'][0]:
        port['name'] = service['spec']['ports'][0]['name']
    endpoints = {'apiVersion': 'discovery.k8s.io/v1', 'kind': 'EndpointSliceList',
        'metadata': {'resourceVersion': '2'}, 'items': [{'metadata': {'name': 'postgres-endpoint', 'namespace': ns,
            'uid': str(uuid4()), 'labels': {'kubernetes.io/service-name': 'loom-postgres'},
            'ownerReferences': [{'apiVersion': 'v1', 'kind': 'Service', 'name': 'loom-postgres',
                'uid': service['metadata']['uid'], 'controller': True}]}, 'addressType': 'IPv4', 'ports': [port],
            'endpoints': [{'addresses': ['10.0.1.10'], 'conditions': {'ready': True},
                'targetRef': {'kind': 'Pod', 'namespace': ns, 'name': 'loom-postgres-0', 'uid': pod['metadata']['uid']}}]}]}
    remote.kubernetes[f'/apis/discovery.k8s.io/v1/namespaces/{ns}/endpointslices'] = endpoints
    return pod, pvc, endpoints


@pytest.mark.parametrize('damage', [None, 'pod_owner', 'pvc', 'backend', 'namespace', 'redirect', 'unready', 'deleted'])
async def test_readiness_requires_exact_database_and_authenticated_public_route(tmp_path, platform_inputs, damage):
    subject = module()
    state = tmp_path / 'foundation'
    reference = prepare(state, platform_inputs)
    remote = Remote(state)
    await run(state, reference, remote)
    writes = copy.deepcopy(remote.writes)
    pod, pvc, endpoints = database_controller(remote)
    if damage == 'pod_owner':
        pod['metadata']['ownerReferences'][0]['uid'] = str(uuid4())
    elif damage == 'pvc':
        pvc['status']['phase'] = 'Lost'
    elif damage == 'backend':
        endpoints['items'][0]['endpoints'][0]['targetRef']['uid'] = str(uuid4())
    elif damage == 'deleted':
        del remote.kubernetes['/api/v1/namespaces/app-red/secrets/loom-platform-db']
    token = tomllib.loads(json.loads((state / 'material.json').read_text())['loom-admin-secret']['secrets.toml'])['admin']['token']

    def public(request):
        assert request.method == 'GET' and str(request.url) == 'https://red.env.example.com/api/v1/health/ready'
        assert request.headers['Authorization'] == 'Bearer ' + token
        assert not request.headers.get('Cookie')
        if damage == 'redirect':
            return httpx.Response(302, headers={'Location': 'https://foreign.test/'})
        return httpx.Response(503 if damage == 'unready' else 200, json={
            'status': 'ready', 'postgres': 'ready', 'object_store': 'ready', 'blockers': [],
            'environment': 'development', 'namespace': 'foreign' if damage == 'namespace' else 'app-red'})

    async with httpx.AsyncClient(base_url='https://api.cluster.test', transport=httpx.MockTransport(remote.http)) as kube, \
            httpx.AsyncClient(transport=httpx.MockTransport(public), headers={'Authorization': 'foreign-default'},
                cookies={'session': 'foreign-cookie'}) as public_http:
        async def observe():
            return await subject.observe_foundation(state_dir=state, reference=reference,
                kubernetes=KubernetesEnvironmentProvider(kube), cloud=remote, public_http=public_http)
        if damage is None:
            proof = await observe()
            assert proof.foundation.identity == reference.identity
            assert proof.database_pod_uid == pod['metadata']['uid']
            assert proof.database_pvc_uid == pvc['metadata']['uid']
            assert proof.origin == 'https://red.env.example.com'
            assert proof.foundation.execution_enabled is False
            # A prior resource receipt or public health result cannot mask a
            # later replacement of the database's persistent identity.
            pvc['metadata']['uid'] = str(uuid4())
            with pytest.raises(ValueError, match='foundation'):
                await observe()
        else:
            with pytest.raises(ValueError, match='foundation'):
                await observe()
    assert remote.writes == writes


async def test_readiness_cannot_install_missing_foundation(tmp_path, platform_inputs):
    subject = module()
    state = tmp_path / 'foundation'
    reference = prepare(state, platform_inputs)
    remote = Remote(state)
    async with httpx.AsyncClient(base_url='https://api.cluster.test', transport=httpx.MockTransport(remote.http)) as kube, \
            httpx.AsyncClient(transport=httpx.MockTransport(lambda request: pytest.fail('public before installed'))) as public:
        with pytest.raises(ValueError, match='foundation'):
            await subject.observe_foundation(state_dir=state, reference=reference,
                kubernetes=KubernetesEnvironmentProvider(kube), cloud=remote, public_http=public)
    assert remote.writes == []
