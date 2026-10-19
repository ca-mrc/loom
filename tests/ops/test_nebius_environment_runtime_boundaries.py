"""Admission-boundary intent selects only the enrolled owner's exact resources."""
from __future__ import annotations

import copy
import importlib
from uuid import uuid4

import pytest
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
