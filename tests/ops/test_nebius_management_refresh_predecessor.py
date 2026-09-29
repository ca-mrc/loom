"""Refresh history comes from a completed upgrade, never a replay/reset of it."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from uuid import uuid4

import pytest
from tests.ops.test_nebius_application_setup import application_material as application_material
from tests.ops.test_nebius_management_cloud_scope import cloud as cloud
from tests.ops.test_nebius_management_entry import entry_inputs as entry_inputs
from tests.ops.test_nebius_management_install import installation as installation
from tests.ops.test_nebius_management_prerequisites import checks as checks
from tests.ops.test_nebius_management_supplied import material as material
from tests.ops.test_nebius_management_upgrade import UpgradeAPI
from tests.ops.test_nebius_management_upgrade_entry import private_upgrade as private_upgrade
from tests.unit.test_nebius_management_render import (
    application_management_inputs as application_management_inputs,
)
from tests.unit.test_nebius_management_render import management_inputs as management_inputs
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


def checksum(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.fixture
def completed_upgrade(private_upgrade):
    from scripts.ops.nebius_management_entry import load_upgrade_inputs
    from scripts.ops.nebius_management_upgrade import upgrade_management

    metadata, _, _, installed = private_upgrade
    _, request, _, _ = load_upgrade_inputs(metadata)
    api = UpgradeAPI(installed, request.setup)
    api.authority = api.public_ready = True
    api.switch.processes = False
    state = Path(metadata['state_dir'])
    for _ in range(8):
        result = upgrade_management(request=request, api=api, state_dir=state, anchor_dir=Path(metadata['anchor_dir']))
        if result['status'] == 'management_upgraded':
            break
        api.complete('loom')
    assert result['status'] == 'management_upgraded'
    selector = {'kind': 'upgrade', 'operation': metadata, 'state_sha256': checksum(state / 'upgrade.json'),
        'switch_sha256': checksum(state / 'switch/switch.json')}
    return selector, request, api


def load(selector):
    from scripts.ops.nebius_management_refresh_predecessor import (
        UpgradePredecessorV1,
        load_completed_upgrade,
    )

    return load_completed_upgrade(UpgradePredecessorV1.model_validate(selector))


def test_completed_upgrade_yields_bound_retained_runtime_without_writes(completed_upgrade):
    selector, request, _ = completed_upgrade
    root = Path(selector['operation']['inputs_path']).parent.parent
    before = {path: path.read_bytes() for path in root.rglob('*.json')}
    result = load(selector)
    assert result.upgrade == request
    assert result.deployment == request.setup.deployment
    assert result.active['spec']['template']['spec']['serviceAccountName'] == 'loom-application-provisioner'
    assert result.active['metadata']['uid']
    assert result.history[Path(selector['operation']['state_dir']) / 'upgrade.json'] == selector['state_sha256']
    assert result.history[request.original_state / 'installation.json'] == checksum(request.original_state / 'installation.json')
    assert result.history[Path(selector['operation']['inputs_path'])] == selector['operation']['inputs_sha256']
    assert len(result.history) < 64
    assert {path: path.read_bytes() for path in root.rglob('*.json')} == before
    assert {value['kind'] for value in result.retained.values()} >= {'ConfigMap', 'Secret', 'Job', 'RoleBinding', 'ValidatingAdmissionPolicy'}


@pytest.mark.parametrize('damage', ['state_hash', 'switch_hash', 'lost_phase', 'changed_phase', 'lost_anchor',
    'pending_switch', 'lost_original', 'input_hash', 'shared_uid', 'active_uid', 'extra_field', 'phase_not_created'])
def test_unqualified_predecessor_is_rejected_without_rewriting_history(completed_upgrade, damage):
    selector, request, _ = completed_upgrade
    state = Path(selector['operation']['state_dir'])
    if damage == 'state_hash':
        selector['state_sha256'] = '0' * 64
    elif damage == 'switch_hash':
        selector['switch_sha256'] = '0' * 64
    elif damage == 'lost_phase':
        (state / 'network/stage.json').unlink()
    elif damage == 'changed_phase':
        (state / 'network/stage.json').write_text('{}')
    elif damage == 'lost_anchor':
        (Path(selector['operation']['anchor_dir']) / (request.setup.binding.installation_id + '.json')).unlink()
    elif damage == 'lost_original':
        (request.original_state / 'installation.json').unlink()
    elif damage == 'input_hash':
        selector['operation']['inputs_sha256'] = '0' * 64
    elif damage == 'phase_not_created':
        path = state / 'config/stage.json'
        value = json.loads(path.read_text())
        item = next(iter(value['resources'].values()))
        item.update(status='create_intent', uid=None, observed=None)
        path.write_text(json.dumps(value))
        parent = json.loads((state / 'upgrade.json').read_text())
        parent['phases']['config']['sha256'] = checksum(path)
        (state / 'upgrade.json').write_text(json.dumps(parent))
        selector['state_sha256'] = checksum(state / 'upgrade.json')
    else:
        path = state / 'switch/switch.json'
        value = json.loads(path.read_text())
        if damage == 'pending_switch':
            value['phase'] = 'activate_intent'
        elif damage == 'shared_uid':
            value['shared_namespace_uid'] = str(uuid4())
        elif damage == 'active_uid':
            value['original_uid'] = str(uuid4())
        else:
            value['unknown_authority'] = True
        path.write_text(json.dumps(value))
        selector['switch_sha256'] = checksum(path)
    root = Path(selector['operation']['inputs_path']).parent.parent
    before = {path: path.read_bytes() for path in root.rglob('*.json')}
    with pytest.raises(ValueError, match='refresh_predecessor_unqualified'):
        load(selector)
    assert {path: path.read_bytes() for path in root.rglob('*.json')} == before
