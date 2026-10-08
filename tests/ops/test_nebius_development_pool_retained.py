"""Runtime successors consume closed pool evidence without replaying installation."""
from __future__ import annotations

import hashlib
import importlib
import json
from pathlib import Path

import pytest
from tests.ops.test_nebius_development_pool_install import (
    application_management_inputs as application_management_inputs,
)
from tests.ops.test_nebius_development_pool_install import (
    application_material as application_material,
)
from tests.ops.test_nebius_development_pool_install import (
    build_inputs as build_inputs,
)
from tests.ops.test_nebius_development_pool_install import (
    capacity_checks as capacity_checks,
)
from tests.ops.test_nebius_development_pool_install import (
    cloud as cloud,
)
from tests.ops.test_nebius_development_pool_install import (
    connected as connected,
)
from tests.ops.test_nebius_development_pool_install import (
    installation as installation,
)
from tests.ops.test_nebius_development_pool_install import (
    inventory as inventory,
)
from tests.ops.test_nebius_development_pool_install import (
    management_inputs as management_inputs,
)
from tests.ops.test_nebius_development_pool_install import (
    manager_entry as manager_entry,
)
from tests.ops.test_nebius_development_pool_install import (
    material as material,
)
from tests.ops.test_nebius_development_pool_install import (
    original_platform_inputs as original_platform_inputs,
)
from tests.ops.test_nebius_development_pool_install import (
    platform_inputs as platform_inputs,
)
from tests.ops.test_nebius_development_pool_install import (
    pool_entry as pool_entry,
)
from tests.ops.test_nebius_development_pool_install import (
    pool_inputs as pool_inputs,
)
from tests.ops.test_nebius_development_pool_install import (
    provider_checks as provider_checks,
)
from tests.ops.test_nebius_development_pool_install import (
    retained as retained,
)
from tests.ops.test_nebius_development_pool_install import (
    route as route,
)
from tests.ops.test_nebius_development_pool_install import (
    tls_material as tls_material,
)


def module():
    name = 'scripts.ops.nebius_development_pool_retained'
    if importlib.util.find_spec(name) is None:
        pytest.fail('read-only completed development pool loader is missing')
    return importlib.import_module(name)


@pytest.fixture
def completed_pool(pool_entry, capsys):
    entry, operation, inputs, path, connected = pool_entry
    connected.server.complete = True
    assert entry.main(str(path), 'install') == 0
    assert json.loads(capsys.readouterr().out)['status'] == 'development_pool_installed_closed'
    return operation, inputs, path, connected


def reference(loader, completed_pool):
    path = completed_pool[2]
    return loader.RetainedDevelopmentPoolReference(operation_path=path,
        operation_sha256=hashlib.sha256(path.read_bytes()).hexdigest())


def test_completed_pool_load_is_readonly_and_keeps_actual_identities(completed_pool, monkeypatch):
    from scripts.ops import nebius_development_pool_install, nebius_development_pool_intent

    loader = module()
    operation, _, _, connected = completed_pool
    before = len(connected.server.calls)
    monkeypatch.setattr(nebius_development_pool_install, 'install_development_pool',
        lambda **_: pytest.fail('installer replayed'))
    for name in ('namespace_documents', 'delivery_documents'):
        monkeypatch.setattr(nebius_development_pool_intent, name,
            lambda *_: pytest.fail('historical runtime rerendered'))
    value = loader.load_retained_pool(reference(loader, completed_pool))
    assert value.operation == operation
    participant, = value.request.registration.spec.participants
    for namespace in (participant.execution_namespace, participant.build_namespace):
        assert str(namespace.uid) == connected.server.namespaces[namespace.name]['metadata']['uid']
    deployment, = [row for row in value.resources.values() if row['kind'] == 'Deployment']
    assert deployment['spec']['replicas'] == 0
    assert len(connected.server.calls) == before
    assert all(path.read_bytes() == raw for path, raw in value.files.items())
    assert not any(token in repr(value) for token in connected.intent.tokens.values())


def test_retired_operator_files_do_not_invalidate_completed_pool(completed_pool):
    loader = module()
    _, inputs, _, connected = completed_pool
    for name in ('ca_file', 'credentials_file'):
        Path(inputs['operator_connection'][name]).unlink()
    value = loader.load_retained_pool(reference(loader, completed_pool))
    assert value.request.registration.spec.pool_id == connected.server.request.registration.spec.pool_id


@pytest.mark.parametrize('damage', ['anchor', 'parent', 'phase', 'incomplete', 'inputs', 'receipt'])
def test_missing_or_changed_completion_cannot_become_successor_authority(completed_pool, damage):
    loader = module()
    _, _, _, connected = completed_pool
    selector = reference(loader, completed_pool)
    original = Path(connected.server.request.retained.operation['state_dir']).parent
    state = original / 'pool-installation'
    parent_path = state / 'installation.json'
    parent = json.loads(parent_path.read_bytes())
    if damage == 'anchor':
        (Path(connected.server.request.retained.operation['anchor_dir']) / 'pool-installation.json').unlink()
    elif damage == 'parent':
        parent_path.unlink()
    elif damage == 'phase':
        (state / 'material/stage.json').unlink()
    elif damage == 'incomplete':
        parent['phases']['workload'] = {'status': 'started', 'sha256': None}
        parent_path.write_text(json.dumps(parent))
    elif damage == 'inputs':
        inputs_path = Path(completed_pool[0]['inputs_path'])
        inputs_path.write_bytes(inputs_path.read_bytes() + b' ')
    else:
        registration = original / 'pool-registration/registration.json'
        value = json.loads(registration.read_bytes())
        value['proof']['registration']['mode'] = 'global'
        registration.write_text(json.dumps(value))
        parent['phases']['registration']['sha256'] = hashlib.sha256(registration.read_bytes()).hexdigest()
        parent_path.write_text(json.dumps(parent))
    before = len(connected.server.calls)
    with pytest.raises(ValueError, match='retained development pool') as caught:
        loader.load_retained_pool(selector)
    assert len(connected.server.calls) == before
    assert not any(token in str(caught.value) for token in connected.intent.tokens.values())
