"""Refresh history comes from a completed upgrade, never a replay/reset of it."""
from __future__ import annotations

import copy
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


def history_credential(root):
    from scripts.ops.nebius_management_material import _documents

    material = json.loads((root.upgrade.original_state / 'bootstrap/material/material.json').read_text())
    credential = _documents(material['material'], root.upgrade.setup.binding, material['operation_id'])['loom-platform-db']
    credential['metadata'].update(uid=material['resources']['loom-platform-db']['uid'], resourceVersion='100')
    return credential


def test_pool_history_target_comes_from_completed_original_database_and_current_manager(completed_upgrade):
    from scripts.ops.nebius_pool_origin_history import derive_management_history_target

    root = load(completed_upgrade[0])
    before = {path: path.read_bytes() for path in root.history}
    credential = history_credential(root)
    target = derive_management_history_target(original=root, predecessor=root, credential=credential)
    record = json.loads((root.upgrade.original_state / 'database/stage.json').read_text())
    assert target.controller == root.active
    assert target.namespace == root.upgrade.setup.binding.namespace
    assert str(target.namespace_uid) == root.upgrade.setup.binding.namespace_uid
    assert target.database.statefulset['metadata']['uid'] == record['resources']['StatefulSet:' + target.namespace + ':loom-postgres']['uid']
    assert str(target.database.credential_uid) == credential['metadata']['uid']
    assert target.database.credential_resource_version == '100'
    assert {path: path.read_bytes() for path in root.history} == before


def test_pool_history_target_preserves_a_completed_refresh_and_original_database(completed_upgrade):
    from scripts.ops.nebius_pool_origin_history import derive_management_history_target

    root = load(completed_upgrade[0])
    selector, _ = complete_refresh(root)
    predecessor = load_refresh(selector, root)
    before = {path: path.read_bytes() for path in predecessor.history}
    target = derive_management_history_target(original=root, predecessor=predecessor, credential=history_credential(root))
    assert target.controller == predecessor.active
    assert target.controller != root.active
    assert {path: path.read_bytes() for path in predecessor.history} == before


@pytest.mark.parametrize('damage', ['uid', 'value', 'missing_version', 'history', 'predecessor'])
def test_pool_history_target_denies_foreign_credentials_or_lost_completed_history(completed_upgrade, damage):
    from dataclasses import replace

    from scripts.ops.nebius_pool_origin_history import derive_management_history_target

    root = load(completed_upgrade[0])
    credential = history_credential(root)
    predecessor = root
    if damage == 'uid':
        credential['metadata']['uid'] = str(uuid4())
    elif damage == 'value':
        credential['data']['service-url'] = 'Zm9yZWlnbi1wcml2YXRlLW1hcmtlcg=='
    elif damage == 'missing_version':
        credential['metadata'].pop('resourceVersion')
    elif damage == 'history':
        path = root.upgrade.original_state / 'database/stage.json'
        path.write_bytes(path.read_bytes() + b'\n')
    else:
        active = copy.deepcopy(root.active)
        active['metadata']['uid'] = str(uuid4())
        predecessor = replace(root, active=active)
    with pytest.raises(ValueError) as error:
        derive_management_history_target(original=root, predecessor=predecessor, credential=credential)
    assert 'private-marker' not in str(error.value)


def refresh_case(root, predecessor=None, *, pool_baseline=None):
    """Prepare the real parent request, without staging or cutover."""
    from scripts.ops.nebius_management_refresh import ManagementRefreshRenderRequest
    from scripts.ops.nebius_management_refresh_resources import ManagementRefreshResourcesRequest
    from scripts.ops.nebius_management_refresh_switch import ManagementRefreshSwitchRequest
    from tests.ops.test_nebius_management_refresh_install import install_case

    prior = predecessor or root
    setup = root.upgrade.setup
    operation_id = uuid4()
    directory = Path(root.selector.operation['inputs_path']).parent.parent / 'refresh' / str(operation_id)
    directory.mkdir(parents=True, mode=0o700)
    candidate, profile = copy.deepcopy(setup.candidate), copy.deepcopy(setup.profile)
    candidate['images']['service']['image_ref'] = candidate['images']['service']['image_ref'].split('@')[0] + '@sha256:' + operation_id.hex * 2
    profile['task_image_ref'] = candidate['images']['service']['image_ref']
    inputs = directory / 'inputs.json'
    inputs.write_text(json.dumps({'fixture': 'hash-bound protected inputs', 'operation_id': str(operation_id)}))
    inputs.chmod(0o600)
    render = ManagementRefreshRenderRequest(prior.deployment, prior.deployment, prior.active,
        candidate, profile, setup.repo_root)
    resources = ManagementRefreshResourcesRequest(ManagementRefreshSwitchRequest(render, operation_id),
        setup.binding, setup.shared_namespace_uid, '0168', '0168')
    case = install_case(resources, directory, history={**prior.history, inputs: checksum(inputs)},
        installation_anchor=root.upgrade.original_anchor, pool_baseline=pool_baseline)
    case[1].switch.document['metadata'].update(resourceVersion='30', generation=5)
    return case


def complete_refresh(root, predecessor=None, *, pool_baseline=None):
    """Run real journals with external cluster/proof boundaries doubled."""
    from tests.ops.test_nebius_management_refresh_install import run

    case = refresh_case(root, predecessor, pool_baseline=pool_baseline)
    operation_id = case[0].resources.switch.operation_id
    directory = case[2].parent
    inputs = directory / 'inputs.json'
    assert run(case)['status'] == 'management_refreshed'
    selector = {'kind': 'refresh', 'operation_id': str(operation_id), 'inputs_path': str(inputs),
        'inputs_sha256': checksum(inputs), 'state_dir': str(directory / 'state'), 'anchor_dir': str(directory / 'anchor'),
        'completion_sha256': checksum(directory / 'state/completion.json')}
    return selector, case


def load_refresh(selector, root):
    from scripts.ops.nebius_management_refresh_predecessor import (
        RefreshPredecessorV1,
        load_completed_refresh,
    )

    return load_completed_refresh(RefreshPredecessorV1.model_validate(selector), original=root)


def test_successive_completed_refreshes_have_bounded_read_only_history(completed_upgrade):
    from scripts.ops.nebius_management_refresh_switch import MARKER

    root = load(completed_upgrade[0])
    predecessor = None
    sizes = []
    for _ in range(3):
        selector, case = complete_refresh(root, predecessor)
        before = {path: path.read_bytes() for path in case[2].parent.rglob('*.json')}
        predecessor = load_refresh(selector, root)
        assert predecessor.deployment == root.deployment
        assert predecessor.active['metadata']['uid'] == root.active['metadata']['uid']
        assert predecessor.active['metadata']['annotations'][MARKER] == selector['operation_id']
        assert predecessor.active['spec']['template']['spec']['containers'][0]['image'] == case[0].resources.switch.render.candidate['images']['service']['image_ref']
        assert all(predecessor.history[path] == checksum for path, checksum in root.history.items())
        assert {path: path.read_bytes() for path in before} == before
        sizes.append(len(predecessor.history))
    assert len(set(sizes)) == 1 and max(sizes) < 100


@pytest.mark.parametrize('resource,native,canonical', [
    ('cpu', '100m', '0.1'), ('memory', '256Mi', '268435456'),
])
def test_completed_native_quantity_receipt_is_reusable_without_rewriting_history(
        completed_upgrade, monkeypatch, resource, native, canonical):
    from decimal import Decimal

    from tests.ops.test_nebius_management_refresh_switch import API

    root = load(completed_upgrade[0])
    desired = API.desired

    def native_desired(self, action):
        document = desired(self, action)
        requests = document['spec']['template']['spec']['containers'][0]['resources']['requests']
        assert Decimal(requests[resource]) == Decimal(canonical)
        requests[resource] = native
        return document

    monkeypatch.setattr(API, 'desired', native_desired)
    selector, case = complete_refresh(root)
    receipt_path = case[2] / 'completion.json'
    receipt = json.loads(receipt_path.read_text())
    assert receipt['active']['spec']['template']['spec']['containers'][0]['resources']['requests'][resource] == native
    before = {path: path.read_bytes() for path in case[2].parent.rglob('*.json')}
    predecessor = load_refresh(selector, root)
    assert predecessor.active['metadata']['uid'] == root.active['metadata']['uid']
    assert {path: path.read_bytes() for path in before} == before


@pytest.mark.parametrize('resource,changed', [('cpu', '101m'), ('memory', '257Mi')])
def test_quantity_comparison_still_rejects_actual_resource_change(completed_upgrade, resource, changed):
    root = load(completed_upgrade[0])
    selector, case = complete_refresh(root)
    path = case[2] / 'completion.json'
    receipt = json.loads(path.read_text())
    receipt['active']['spec']['template']['spec']['containers'][0]['resources']['requests'][resource] = changed
    path.write_text(json.dumps(receipt))
    selector['completion_sha256'] = checksum(path)
    parent_path = case[2] / 'refresh.json'
    parent = json.loads(parent_path.read_text())
    parent['completion_sha256'] = selector['completion_sha256']
    parent_path.write_text(json.dumps(parent))
    with pytest.raises(ValueError, match='refresh_predecessor_unqualified'):
        load_refresh(selector, root)


@pytest.mark.parametrize('damage', ['completion_hash', 'lost_anchor', 'lost_phase', 'lost_input',
    'changed_input', 'contract', 'pending', 'active_uid', 'active_material', 'probe_proof', 'phase_hash', 'switch_hash', 'layout'])
def test_incomplete_or_rebound_refresh_cannot_be_a_predecessor(completed_upgrade, damage):
    root = load(completed_upgrade[0])
    selector, case = complete_refresh(root)
    _, _, state, anchor = case
    receipt_path = state / 'completion.json'
    receipt = json.loads(receipt_path.read_text())
    assert 'contract' in receipt, 'completion must retain its immutable input contract'
    if damage == 'completion_hash':
        selector['completion_sha256'] = '0' * 64
    elif damage == 'lost_anchor':
        (anchor / (selector['operation_id'] + '.json')).unlink()
    elif damage == 'lost_phase':
        (state / 'migration/stage.json').unlink()
    elif damage == 'lost_input':
        Path(selector['inputs_path']).unlink()
    elif damage == 'changed_input':
        Path(selector['inputs_path']).write_text('{}')
    elif damage == 'layout':
        selector['state_dir'] = str(state.parent / 'other-state')
    else:
        if damage == 'contract':
            receipt['contract']['target_manager_revision'] = '0000'
        elif damage == 'pending':
            receipt['status'] = 'pending'
        elif damage == 'active_uid':
            receipt['active_uid'] = str(uuid4())
        elif damage == 'active_material':
            volume = next(row for row in receipt['active']['spec']['template']['spec']['volumes'] if row['name'] == 'management-cloud')
            volume['secret']['secretName'] = 'loom-applications-cloud-' + '0' * 12
        elif damage == 'probe_proof':
            receipt['phases']['manager-probe']['proof']['probe']['revision'] = '0000'
        elif damage == 'phase_hash':
            receipt['phases']['migration']['sha256'] = '0' * 64
        else:
            receipt['switch_sha256'] = '0' * 64
        receipt_path.write_text(json.dumps(receipt))
        selector['completion_sha256'] = checksum(receipt_path)
        # Change the parent hash too: hash checks alone are not qualification.
        parent_path = state / 'refresh.json'
        parent = json.loads(parent_path.read_text())
        parent['completion_sha256'] = selector['completion_sha256']
        parent_path.write_text(json.dumps(parent))
    with pytest.raises(ValueError, match='refresh_predecessor_unqualified'):
        load_refresh(selector, root)


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
