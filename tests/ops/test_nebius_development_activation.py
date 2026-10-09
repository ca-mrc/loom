"""Fresh activation consumes genuine completed runtime history, never migration."""
from __future__ import annotations

import copy
import hashlib
import importlib
import json
from pathlib import Path

import pytest
from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_management_stage import _canonical_quantities
from tests.ops.test_nebius_development_pool_retained import (
    application_management_inputs as application_management_inputs,
)
from tests.ops.test_nebius_development_pool_retained import (
    application_material as application_material,
)
from tests.ops.test_nebius_development_pool_retained import (
    build_inputs as build_inputs,
)
from tests.ops.test_nebius_development_pool_retained import (
    capacity_checks as capacity_checks,
)
from tests.ops.test_nebius_development_pool_retained import (
    cloud as cloud,
)
from tests.ops.test_nebius_development_pool_retained import (
    collector_cloud as collector_cloud,
)
from tests.ops.test_nebius_development_pool_retained import (
    completed_pool as completed_pool,
)
from tests.ops.test_nebius_development_pool_retained import (
    connected as connected,
)
from tests.ops.test_nebius_development_pool_retained import (
    development_inputs as development_inputs,
)
from tests.ops.test_nebius_development_pool_retained import (
    entry as entry,
)
from tests.ops.test_nebius_development_pool_retained import (
    handoff as handoff,
)
from tests.ops.test_nebius_development_pool_retained import (
    installation as installation,
)
from tests.ops.test_nebius_development_pool_retained import (
    inventory as inventory,
)
from tests.ops.test_nebius_development_pool_retained import (
    live as live,
)
from tests.ops.test_nebius_development_pool_retained import (
    management_inputs as management_inputs,
)
from tests.ops.test_nebius_development_pool_retained import (
    manager_entry as manager_entry,
)
from tests.ops.test_nebius_development_pool_retained import (
    material as material,
)
from tests.ops.test_nebius_development_pool_retained import (
    original_cloud as original_cloud,
)
from tests.ops.test_nebius_development_pool_retained import (
    original_development_inputs as original_development_inputs,
)
from tests.ops.test_nebius_development_pool_retained import (
    original_manager_entry as original_manager_entry,
)
from tests.ops.test_nebius_development_pool_retained import (
    original_platform_inputs as original_platform_inputs,
)
from tests.ops.test_nebius_development_pool_retained import (
    original_pool_inputs as original_pool_inputs,
)
from tests.ops.test_nebius_development_pool_retained import (
    parent_install_fixture,
)
from tests.ops.test_nebius_development_pool_retained import (
    platform_inputs as platform_inputs,
)
from tests.ops.test_nebius_development_pool_retained import (
    pool_entry as pool_entry,
)
from tests.ops.test_nebius_development_pool_retained import (
    pool_inputs as pool_inputs,
)
from tests.ops.test_nebius_development_pool_retained import (
    preflight as preflight,
)
from tests.ops.test_nebius_development_pool_retained import (
    provider_checks as provider_checks,
)
from tests.ops.test_nebius_development_pool_retained import (
    publication as publication,
)
from tests.ops.test_nebius_development_pool_retained import (
    published_source as published_source,
)
from tests.ops.test_nebius_development_pool_retained import (
    publisher_cloud as publisher_cloud,
)
from tests.ops.test_nebius_development_pool_retained import (
    retained as retained,
)
from tests.ops.test_nebius_development_pool_retained import (
    route as route,
)
from tests.ops.test_nebius_development_pool_retained import (
    source_checkout as source_checkout,
)
from tests.ops.test_nebius_development_pool_retained import (
    tls_material as tls_material,
)

pytestmark = [
    pytest.mark.parametrize('manager_entry', ['foundation-runtime-build-material'], indirect=True),
    pytest.mark.parametrize('retained', [False], indirect=True),
]


def loader():
    name = 'scripts.ops.nebius_development_runtime_retained'
    if importlib.util.find_spec(name) is None:
        pytest.fail('completed fresh runtime predecessor is missing')
    return importlib.import_module(name)


@pytest.fixture
def completed_runtime(completed_pool, publisher_cloud):
    runtime, request, api, state, anchor = parent_install_fixture(completed_pool, publisher_cloud)
    api.database_complete = api.catalog_complete = True
    assert runtime.install_development_runtime(request=request, api=api, execute=True)['status'] == 'development_runtime_installed_closed'
    return runtime, request, api, state, anchor


def test_completed_runtime_load_is_readonly_and_keeps_real_successors(completed_runtime, monkeypatch):
    runtime, request, api, state, anchor = completed_runtime
    module = loader()

    def no_write(*args, **kwargs):
        pytest.fail('historical activation predecessor attempted a write')

    monkeypatch.setattr(private_state, '_atomic_json', no_write)
    monkeypatch.setattr(runtime, 'install_development_runtime', no_write)
    result = module.load_completed_runtime(request)
    assert result.request == request and result.request is not request
    assert result.plan == api.plan
    assert result.state_dir == state and result.anchor_dir == anchor.parent
    assert {state / 'installation.json', anchor} <= result.files.keys()
    assert all(path.read_bytes() == raw for path, raw in result.files.items())
    for key, actual in api.store.resources.items():
        expected = result.resources[key]
        assert expected['metadata']['uid'] == actual['metadata']['uid']
        if actual['kind'] in {'Deployment', 'CronJob'}:
            assert _canonical_quantities(expected)['spec'] == _canonical_quantities(actual)['spec']
    gateway = result.resources['Deployment:loom-nebius-management-dev:loom-pool-gateway']
    assert gateway['spec']['replicas'] == 0
    assert result.resources['Deployment:loom-dev:loom-service']['spec']['replicas'] == 1
    assert result.resources['Deployment:loom-dev:loom-control-plane']['spec']['replicas'] == 1
    assert result.history_sha256.startswith('sha256:')
    # Returned mutable projections must not alias the caller's request or disk.
    result.resources['Deployment:loom-dev:loom-service']['spec']['replicas'] = 99
    assert module.load_completed_runtime(request).resources['Deployment:loom-dev:loom-service']['spec']['replicas'] == 1


@pytest.mark.parametrize('damage', ['incomplete', 'lost-anchor', 'lost-parent', 'lost-child', 'changed-child', 'forged-proof', 'wrong-uid', 'wrong-observed'])
def test_completed_runtime_rejects_incomplete_or_rewritten_history(completed_runtime, damage):
    _, request, _, state, anchor = completed_runtime
    module = loader()
    parent = json.loads((state / 'installation.json').read_bytes())
    child = state / 'material/stage.json'
    if damage == 'incomplete':
        parent['phases']['start'].update(status='started', sha256=None)
    elif damage.startswith('lost-'):
        {'lost-anchor': anchor, 'lost-parent': state / 'installation.json', 'lost-child': child}[damage].unlink()
    elif damage == 'changed-child':
        value = json.loads(child.read_bytes())
        value['phase'] = 'foreign-phase'
        private_state._atomic_json(child, value)
    elif damage == 'forged-proof':
        parent['phases']['database']['proof']['database']['role_oid'] = 0
    elif damage in {'wrong-uid', 'wrong-observed'}:
        value = json.loads(child.read_bytes())
        item = next(iter(value['resources'].values()))
        if damage == 'wrong-uid':
            item['uid'] = 'not-a-uid'
        else:
            item['observed']['metadata']['name'] = 'foreign-resource'
        private_state._atomic_json(child, value)
        parent['phases']['material']['sha256'] = hashlib.sha256(child.read_bytes()).hexdigest()
    if damage not in {'lost-parent', 'changed-child'}:
        private_state._atomic_json(state / 'installation.json', parent)
    with pytest.raises(ValueError, match='completed development runtime unqualified'):
        module.load_completed_runtime(request)


def test_completed_runtime_rejects_changed_qualified_request(completed_runtime):
    _, request, _, _, _ = completed_runtime
    module = loader()
    changed = copy.deepcopy(request)
    changed.database.database[0]['data']['setup.json'] = '{}'
    with pytest.raises(ValueError, match='completed development runtime unqualified'):
        module.load_completed_runtime(changed)


def test_completed_runtime_rejects_history_changing_during_load(completed_runtime, monkeypatch):
    _, request, _, state, _ = completed_runtime
    module = loader()
    read = private_state._private_read
    target = state / 'start/transition.json'
    original = target.read_bytes()
    reads = 0

    def changed(path, *args, **kwargs):
        nonlocal reads
        raw = read(path, *args, **kwargs)
        if Path(path) == target:
            reads += 1
            if reads > 1:
                return original + b'\n'
        return raw

    monkeypatch.setattr(private_state, '_private_read', changed)
    with pytest.raises(ValueError, match='completed development runtime unqualified'):
        module.load_completed_runtime(request)


def test_activation_preparation_limits_writes_to_retained_gateway_and_build_namespace(completed_runtime, monkeypatch):
    _, request, _, _, _ = completed_runtime
    name = 'scripts.ops.nebius_development_activation'
    if importlib.util.find_spec(name) is None:
        pytest.fail('fresh development activation preparation is missing')
    module = importlib.import_module(name)

    def no_write(*args, **kwargs):
        pytest.fail('activation preparation attempted a write')

    monkeypatch.setattr(private_state, '_atomic_json', no_write)
    plan = module.prepare_development_activation(request)
    spec = request.database.manager.retained.request.registration.spec
    participant, = spec.participants
    assert plan.runtime.request == request
    assert plan.gateway_original['spec']['replicas'] == 0
    assert plan.gateway_target['spec']['replicas'] == 1
    assert plan.gateway_target['metadata']['namespace'] == 'loom-nebius-management-dev'
    pod = plan.gateway_target['spec']['template']['spec']
    assert pod['serviceAccountName'] == 'loom-pool-gateway'
    assert pod['containers'][0]['image'] == request.database.manager.publication.bundle.candidate['images']['service']['image_ref']
    assert {row['metadata']['namespace'] for row in plan.authority.values() if row['kind'] in {'Role', 'RoleBinding'}} == {
        participant.execution_namespace.name, participant.build_namespace.name}
    for row in plan.authority.values():
        if row['kind'] == 'ClusterRole':
            assert row['rules'] == [{'apiGroups': [''], 'resources': ['namespaces'], 'verbs': ['get'],
                'resourceNames': sorted([participant.execution_namespace.name, participant.build_namespace.name])}]
        for subject in row.get('subjects', []):
            assert subject == {'kind': 'ServiceAccount', 'name': 'loom-pool-gateway', 'namespace': 'loom-nebius-management-dev'}
    before, after = plan.build_namespace_original, plan.build_namespace_target
    assert before['metadata']['name'] == after['metadata']['name'] == participant.build_namespace.name
    assert before['metadata']['uid'] == str(participant.build_namespace.uid)
    assert before['metadata']['labels']['pod-security.kubernetes.io/enforce'] == 'restricted'
    assert after['metadata']['labels']['pod-security.kubernetes.io/enforce'] == 'privileged'
    assert {row['kind'] for row in plan.build_policy.values()} == {
        'ValidatingAdmissionPolicy', 'ValidatingAdmissionPolicyBinding'}
    assert plan.input_digest.startswith('sha256:')
    assert not any(key in plan.authority for key in plan.runtime.resources)
