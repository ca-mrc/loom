"""Fresh activation consumes genuine completed runtime history, never migration."""
from __future__ import annotations

import copy
import hashlib
import importlib
import json
from pathlib import Path

import pytest
from scripts.ops import nebius_certificates as private_state
from tests.ops.test_nebius_development_pool_retained import (
    application_management_inputs as application_management_inputs,
    application_material as application_material,
    build_inputs as build_inputs,
    capacity_checks as capacity_checks,
    cloud as cloud,
    collector_cloud as collector_cloud,
    completed_pool as completed_pool,
    connected as connected,
    development_inputs as development_inputs,
    entry as entry,
    handoff as handoff,
    installation as installation,
    inventory as inventory,
    live as live,
    management_inputs as management_inputs,
    manager_entry as manager_entry,
    material as material,
    original_cloud as original_cloud,
    original_development_inputs as original_development_inputs,
    original_manager_entry as original_manager_entry,
    original_platform_inputs as original_platform_inputs,
    original_pool_inputs as original_pool_inputs,
    platform_inputs as platform_inputs,
    pool_entry as pool_entry,
    pool_inputs as pool_inputs,
    preflight as preflight,
    provider_checks as provider_checks,
    publication as publication,
    published_source as published_source,
    publisher_cloud as publisher_cloud,
    retained as retained,
    route as route,
    source_checkout as source_checkout,
    tls_material as tls_material,
    parent_install_fixture,
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
            assert expected['spec'] == actual['spec']
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
