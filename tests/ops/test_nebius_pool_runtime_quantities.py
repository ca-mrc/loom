"""Runtime init-container equality accepts quantity spelling, not template drift."""
from __future__ import annotations

import copy

import pytest
from tests.ops.test_nebius_pool_migration_guard import guard_runtime as guard_runtime


def runtime_with_initializer(guard_runtime):
    api, state = guard_runtime
    initializer = {'name': 'prepare-source', 'image': 'registry.example/loom@sha256:' + 'a' * 64,
        'command': ['python', '-c', 'print("ready")'],
        'env': [{'name': 'MODE', 'value': 'legacy'}],
        'securityContext': {'runAsNonRoot': True},
        'resources': {'requests': {'memory': '33554432', 'cpu': '10m'},
                      'limits': {'memory': '67108864', 'cpu': '100m'}}}
    original = copy.deepcopy(state.controller)
    original['spec']['template']['spec']['initContainers'] = [initializer]
    state.controller = copy.deepcopy(original)
    state.replica['spec']['template'] = copy.deepcopy(original['spec']['template'])
    state.pod['spec'] = copy.deepcopy(original['spec']['template']['spec'])
    actual = state.pod['spec']['initContainers'][0]
    actual['resources']['requests']['memory'] = '32Mi'
    actual['resources']['limits']['memory'] = '64Mi'
    return api, state, original, actual


def test_runtime_accepts_api_equivalent_init_container_quantities(guard_runtime):
    api, state, original, _ = runtime_with_initializer(guard_runtime)
    retained, observed = copy.deepcopy(original), copy.deepcopy(state.pod)
    assert api._runtime(state.target, original=original, expected=original) == state.pod
    assert original == retained and state.pod == observed
    assert not state.executed


@pytest.mark.parametrize('damage', ['memory', 'cpu', 'image', 'command', 'env',
    'security', 'unknown_field', 'extra_initializer', 'not_ready', 'pod_owner'])
def test_runtime_quantity_normalization_preserves_exact_identity_and_init_template(guard_runtime, damage):
    api, state, original, actual = runtime_with_initializer(guard_runtime)
    if damage == 'memory':
        actual['resources']['limits']['memory'] = '67108865'
    elif damage == 'cpu':
        actual['resources']['requests']['cpu'] = '0.0101'
    elif damage == 'image':
        actual['image'] = 'registry.example/foreign:latest'
    elif damage == 'command':
        actual['command'].append('foreign')
    elif damage == 'env':
        actual['env'][0]['value'] = 'foreign'
    elif damage == 'security':
        actual['securityContext']['runAsNonRoot'] = False
    elif damage == 'unknown_field':
        actual['foreign'] = True
    elif damage == 'extra_initializer':
        state.pod['spec']['initContainers'].append(copy.deepcopy(actual))
    elif damage == 'not_ready':
        state.pod['status']['containerStatuses'][0]['ready'] = False
    else:
        state.pod['metadata']['ownerReferences'][0]['uid'] = '00000000-0000-4000-8000-000000000001'
    with pytest.raises(ValueError):
        api._runtime(state.target, original=original, expected=original)
    assert not state.executed
