"""Admission-boundary intent selects only the enrolled owner's exact resources."""
from __future__ import annotations

import copy
import importlib
import json
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
from scripts.ops.nebius_ingress_stage import _key, _snapshot
from tests.ops.test_nebius_development_runtime_readiness import running as running
from tests.ops.test_nebius_environment_runtime import runtime_request as runtime_request
from tests.ops.test_nebius_pool_runtime import runtime_inputs as runtime_inputs
from tests.support.nebius_platform import platform_inputs as platform_inputs
from tests.unit.test_nebius_management_render import management_inputs as management_inputs


@pytest.mark.parametrize('damage', [None, 'namespace_uid', 'peer_namespace', 'open_quota', 'unprotected_builds'])
def test_boundary_intent_changes_only_owned_quotas_and_explicit_build_psa(runtime_request, damage):
    name = 'scripts.ops.nebius_environment_runtime_boundaries'
    assert importlib.util.find_spec(name), 'selected environment boundary implementation missing'
    module = importlib.import_module(name)
    participant = runtime_request['participant']
    build = participant.build_namespace
    namespace = {'apiVersion': 'v1', 'kind': 'Namespace', 'metadata': {'name': build.name, 'uid': str(build.uid),
        'labels': {'pod-security.kubernetes.io/enforce': 'restricted', 'pod-security.kubernetes.io/audit': 'restricted',
            'example.org/owned': 'preserve'}}, 'spec': {'finalizers': ['kubernetes']}}
    originals = {'Namespace:-:' + build.name: namespace}
    for space, hard in ((participant.execution_namespace.name, {'pods': '2', 'count/jobs.batch': '0'}), (build.name, {'pods': '0'})):
        originals['ResourceQuota:' + space + ':loom-execution-disabled'] = {'apiVersion': 'v1', 'kind': 'ResourceQuota',
            'metadata': {'name': 'loom-execution-disabled', 'namespace': space, 'uid': str(uuid4())}, 'spec': {'hard': hard}}
    if damage == 'namespace_uid':
        namespace['metadata']['uid'] = str(uuid4())
    elif damage == 'peer_namespace':
        namespace['metadata']['name'] = 'peer-builds'
    elif damage == 'open_quota':
        originals['ResourceQuota:' + participant.execution_namespace.name + ':loom-execution-disabled']['spec'] = {}
    elif damage == 'unprotected_builds':
        namespace['metadata']['labels']['pod-security.kubernetes.io/enforce'] = 'privileged'
    before = copy.deepcopy(originals)
    def render():
        return module.boundary_targets(participant=participant, reader_count=2, originals=originals)
    if damage:
        with pytest.raises(ValueError, match='environment_boundary'):
            render()
    else:
        targets = render()
        assert originals == before and targets.keys() == originals.keys()
        assert targets['Namespace:-:' + build.name]['metadata']['labels'] == {
            'pod-security.kubernetes.io/enforce': 'privileged', 'pod-security.kubernetes.io/audit': 'restricted',
            'example.org/owned': 'preserve'}
        assert targets['Namespace:-:' + build.name]['spec'] == {'finalizers': ['kubernetes']}
        assert all(doc['spec'] == {} for doc in targets.values() if doc['kind'] == 'ResourceQuota')
        assert all('uid' not in doc['metadata'] for doc in targets.values())


@pytest.mark.parametrize('foreign', [None, 'Job', 'Pod', 'ReplicaSet', 'Deployment', 'truncated'])
def test_boundary_quota_release_accepts_only_exact_running_readers(running, foreign):
    module = importlib.import_module('scripts.ops.nebius_environment_runtime_boundaries')
    assert hasattr(module, 'EnvironmentBoundaryWorkloads'), 'selected boundary transport missing'
    reader, collections, replica, pod = running
    namespace = reader['metadata']['namespace']
    quota = {'apiVersion': 'v1', 'kind': 'ResourceQuota', 'metadata': {'namespace': namespace,
        'name': 'loom-execution-disabled', 'uid': str(uuid4()), 'resourceVersion': '3'},
        'spec': {'hard': {'pods': '1', 'count/jobs.batch': '0'}}}
    desired = _snapshot(quota)
    desired['spec'] = {}
    writes = []
    def request(method, path):
        assert method == 'GET'
        resource = path.split('/')[-1].split('?')[0]
        kind = {'pods': 'Pod', 'jobs': 'Job', 'cronjobs': 'CronJob', 'deployments': 'Deployment',
            'replicasets': 'ReplicaSet', 'statefulsets': 'StatefulSet', 'daemonsets': 'DaemonSet'}[resource]
        rows = copy.deepcopy({'Pod': [pod], 'ReplicaSet': [replica], 'Deployment': [reader]}.get(kind, []))
        if foreign == kind:
            row = copy.deepcopy(rows[0]) if rows else {'metadata': {'namespace': namespace, 'name': 'dormant'}, 'spec': {'parallelism': 1}}
            row['metadata'].update(uid=str(uuid4()), ownerReferences=[], labels={'app': 'foreign'})
            rows.append(row)
        return {'apiVersion': 'v1' if kind == 'Pod' else ('batch/v1' if kind in {'Job', 'CronJob'} else 'apps/v1'),
            'kind': kind + 'List', 'metadata': {'resourceVersion': '10', **({'continue': 'next'} if foreign == 'truncated' else {})}, 'items': rows}
    def patch(request):
        assert request.method == 'PATCH' and request.url.params['dryRun'] == 'All'
        writes.append(json.loads(request.content))
        return httpx.Response(200, json={**desired, 'metadata': {**desired['metadata'], 'uid': quota['metadata']['uid']}})
    with httpx.Client(transport=httpx.MockTransport(patch), base_url='https://cluster.test') as client:
        api = SimpleNamespace(client=client, policy=SimpleNamespace(host=SimpleNamespace(runtime=SimpleNamespace(binding='test'))),
            verify_identity=lambda binding: None, _request=request)
        runtime = module.EnvironmentBoundaryWorkloads(api, {_key(quota): quota}, {_key(quota): desired}, 'eu-north1',
            {_key(reader): reader}, build_policy={}, build_profile=None)
        if foreign:
            with pytest.raises(ValueError, match='environment_boundary'):
                runtime.preview_workload(_key(quota), quota, desired)
            assert not writes
        else:
            assert runtime.preview_workload(_key(quota), quota, desired) is not None
            assert writes[0][:3] == [{'op': 'test', 'path': '/metadata/uid', 'value': quota['metadata']['uid']},
                {'op': 'test', 'path': '/metadata/resourceVersion', 'value': '3'},
                {'op': 'test', 'path': '/spec', 'value': quota['spec']}]


@pytest.mark.parametrize('denial', ['policy', 'generic', 'accepted', 'stale', 'drift'])
def test_build_namespace_cas_requires_effective_exact_native_policy(runtime_request, denial):
    module = importlib.import_module('scripts.ops.nebius_environment_runtime_boundaries')
    assert hasattr(module, 'EnvironmentBoundaryWorkloads'), 'selected boundary transport missing'
    from scripts.ops.nebius_environment_runtime import render_environment_runtime

    namespace = {'apiVersion': 'v1', 'kind': 'Namespace', 'metadata': {'name': 'build-red',
        'uid': str(runtime_request['participant'].build_namespace.uid), 'resourceVersion': '8',
        'labels': {'pod-security.kubernetes.io/enforce': 'restricted'}}, 'spec': {'finalizers': ['kubernetes']}}
    desired = _snapshot(namespace)
    desired['metadata']['labels']['pod-security.kubernetes.io/enforce'] = 'privileged'
    policies = {_key(doc): copy.deepcopy(doc) for doc in render_environment_runtime(**runtime_request).isolation
        if doc['kind'] in {'ValidatingAdmissionPolicy', 'ValidatingAdmissionPolicyBinding'}}
    for doc in policies.values():
        doc['metadata'].update(uid=str(uuid4()), generation=1)
    live = copy.deepcopy(policies)
    policy, = (doc for doc in live.values() if doc['kind'] == 'ValidatingAdmissionPolicy')
    policy['status'] = {'observedGeneration': 0 if denial == 'stale' else 1, 'typeChecking': {}}
    patches, probes = [], []
    def read(method, path):
        assert method == 'GET'
        if path.endswith('/build-red'):
            return namespace
        return next(doc for doc in live.values() if path.endswith('/' + doc['metadata']['name']))
    def serve(request):
        assert request.url.params['dryRun'] == 'All'
        if request.method == 'PATCH':
            assert request.url.path == '/api/v1/namespaces/build-red'
            patches.append(json.loads(request.content))
            return httpx.Response(200, json={**desired, 'metadata': {**desired['metadata'], 'uid': namespace['metadata']['uid']}})
        assert request.method == 'POST' and request.url.path == '/api/v1/namespaces/build-red/pods'
        probes.append(json.loads(request.content))
        if denial == 'drift':
            policy['metadata']['uid'] = str(uuid4())
        if denial == 'accepted':
            return httpx.Response(201, json=probes[-1])
        message = ('denied by ' + policy['metadata']['name'] + ': Only sequential prepare, build and publish phases are allowed.'
            if denial != 'generic' else 'denied by ResourceQuota')
        return httpx.Response(403, json={'apiVersion': 'v1', 'kind': 'Status', 'status': 'Failure',
            'reason': 'Forbidden', 'code': 403, 'message': message})
    with httpx.Client(transport=httpx.MockTransport(serve), base_url='https://cluster.test') as client:
        api = SimpleNamespace(client=client, policy=SimpleNamespace(host=SimpleNamespace(runtime=SimpleNamespace(binding='test'))),
            verify_identity=lambda binding: None, _request=read)
        runtime = module.EnvironmentBoundaryWorkloads(api, {_key(namespace): namespace}, {_key(namespace): desired},
            'eu-north1', {}, build_policy=policies, build_profile=runtime_request['profiles'].task_images[0])
        if denial in {'generic', 'drift'}:
            with pytest.raises(ValueError):
                runtime.preview_workload(_key(namespace), namespace, desired)
        else:
            result = runtime.preview_workload(_key(namespace), namespace, desired)
            assert (result is not None) == (denial == 'policy')
        assert bool(patches) == (denial == 'policy')
        assert bool(probes) == (denial != 'stale')
        if patches:
            assert patches[0][:2] == [{'op': 'test', 'path': '/metadata/uid', 'value': namespace['metadata']['uid']},
                {'op': 'test', 'path': '/metadata/resourceVersion', 'value': '8'}]
