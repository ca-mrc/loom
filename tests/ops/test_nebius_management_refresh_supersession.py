"""A successor can inherit only a frozen, definitely pre-migration failure."""
from __future__ import annotations

import copy
import json
from pathlib import Path
from uuid import uuid4

import pytest
from tests.ops.test_nebius_application_setup import application_material as application_material
from tests.ops.test_nebius_management_cloud_scope import cloud as cloud
from tests.ops.test_nebius_management_entry import entry_inputs as entry_inputs
from tests.ops.test_nebius_management_install import installation as installation
from tests.ops.test_nebius_management_prerequisites import checks as checks
from tests.ops.test_nebius_management_refresh_entry import context, private_refresh
from tests.ops.test_nebius_management_refresh_install import install_case, run
from tests.ops.test_nebius_management_refresh_predecessor import checksum, load
from tests.ops.test_nebius_management_refresh_predecessor import (
    completed_upgrade as completed_upgrade,
)
from tests.ops.test_nebius_management_supplied import material as material
from tests.ops.test_nebius_management_upgrade_entry import private_upgrade as private_upgrade
from tests.unit.test_nebius_management_render import (
    application_management_inputs as application_management_inputs,
)
from tests.unit.test_nebius_management_render import management_inputs as management_inputs
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


def failed_case(completed_upgrade, phase='shared-probe'):
    from scripts.ops.nebius_management_refresh_install import ManagementRefreshInstallError

    root = load(completed_upgrade[0])
    operation, _, _ = private_refresh(root)
    bound = context(operation)
    case = install_case(bound.request.resources, Path(operation['state_dir']).parent,
        history=bound.request.history, installation_anchor=bound.request.installation_anchor)
    case[1].switch.document['metadata'].update(resourceVersion='30', generation=5)
    case[1].failed = phase
    with pytest.raises(ManagementRefreshInstallError) as error:
        run(case)
    assert error.value.stage == phase
    selector = {'operation': operation, 'refresh_sha256': checksum(case[2] / 'refresh.json'),
        'switch_sha256': checksum(case[2] / 'switch/cutover.json')}
    return root, case, selector


def read(case, selector):
    from scripts.ops.nebius_management_refresh_supersession import (
        SupersededRefreshV1,
        load_failed_refresh,
    )

    return load_failed_refresh(case[0], SupersededRefreshV1.model_validate(selector))


@pytest.mark.parametrize('phase', ['manager-probe', 'shared-probe'])
def test_failed_history_is_qualified_read_only_with_exact_stopped_runtime(completed_upgrade, phase):
    _, case, selector = failed_case(completed_upgrade, phase)
    root = case[2].parent.parent.parent
    before = {path: path.read_bytes() for path in root.rglob('*.json')}
    proof = read(case, selector)
    assert proof.failed_phase == phase
    assert proof.stopped['spec']['replicas'] == 0
    assert proof.stopped['spec']['template'] == case[0].resources.switch.render.active['spec']['template']
    assert proof.stopped['metadata']['uid'] == case[1].switch.document['metadata']['uid']
    assert proof.stopped['metadata']['annotations']['loom.nebius/management-refresh-id'] == selector['operation']['operation_id']
    assert proof.history[case[2] / 'refresh.json'] == selector['refresh_sha256']
    assert proof.history[case[2] / 'switch/cutover.json'] == selector['switch_sha256']
    assert proof.failed_job_uid == next(item['metadata']['uid'] for item in proof.documents[phase] if item['kind'] == 'Job')
    assert {path: path.read_bytes() for path in root.rglob('*.json')} == before


@pytest.mark.parametrize('damage', ['input_hash', 'parent_hash', 'switch_hash', 'lost_anchor', 'changed_anchor',
    'changed_phase', 'lost_phase', 'uncertain_child', 'failed_proof', 'early_proof', 'later_phase', 'backup_path',
    'migration_path', 'post_probe_path', 'completion_path', 'activation', 'completion', 'switch_intent',
    'switch_active', 'switch_original', 'namespace', 'operation'])
def test_incomplete_changed_or_late_history_never_authorizes_supersession(completed_upgrade, damage):
    _, case, selector = failed_case(completed_upgrade)
    _, _, state, anchor = case
    parent_path, switch_path = state / 'refresh.json', state / 'switch/cutover.json'
    parent = json.loads(parent_path.read_text())
    rewrite_parent = False
    if damage == 'input_hash':
        selector['operation']['inputs_sha256'] = '0' * 64
    elif damage == 'parent_hash':
        selector['refresh_sha256'] = '0' * 64
    elif damage == 'switch_hash':
        selector['switch_sha256'] = '0' * 64
    elif damage in ('lost_anchor', 'changed_anchor'):
        path = anchor / (selector['operation']['operation_id'] + '.json')
        if damage == 'lost_anchor':
            path.unlink()
        else:
            path.write_text('{}')
    elif damage in ('changed_phase', 'lost_phase', 'uncertain_child'):
        path = state / 'shared-probe/stage.json'
        if damage == 'lost_phase':
            path.unlink()
        elif damage == 'changed_phase':
            path.write_text('{}')
        else:
            record = json.loads(path.read_text())
            next(iter(record['resources'].values()))['status'] = 'create_intent'
            path.write_text(json.dumps(record))
            parent['phases']['shared-probe']['sha256'] = checksum(path)
            rewrite_parent = True
    elif damage in ('backup_path', 'migration_path', 'post_probe_path', 'completion_path'):
        path = state / {'backup_path': 'backup', 'migration_path': 'migration',
            'post_probe_path': 'post-migration-probe', 'completion_path': 'completion.json'}[damage]
        path.write_text('{}')
    elif damage in ('failed_proof', 'early_proof', 'later_phase', 'activation', 'completion'):
        if damage == 'failed_proof':
            parent['phases']['shared-probe']['proof'] = parent['phases']['manager-probe']['proof']
        elif damage == 'early_proof':
            parent['phases']['manager-probe']['proof']['probe']['revision'] = 'unknown'
        elif damage == 'later_phase':
            parent['phases']['backup'] = {'sha256': None, 'proof': None}
        elif damage == 'activation':
            parent['activation_started'] = True
        else:
            parent['completion_sha256'] = 'a' * 64
        rewrite_parent = True
    elif damage.startswith('switch_'):
        switch = json.loads(switch_path.read_text())
        if damage == 'switch_intent':
            switch['phase'] = 'retire_intent'
        elif damage == 'switch_active':
            switch['active'] = case[1].switch.document
        else:
            switch['original']['spec']['replicas'] = 0
        switch_path.write_text(json.dumps(switch))
        selector['switch_sha256'] = checksum(switch_path)
    elif damage == 'namespace':
        selector['operation']['namespace'] += '-foreign'
    else:
        selector['operation']['operation_id'] = str(uuid4())
    if rewrite_parent:
        parent_path.write_text(json.dumps(parent))
        selector['refresh_sha256'] = checksum(parent_path)
    before = {path: path.read_bytes() for path in state.rglob('*.json')}
    with pytest.raises(ValueError, match='refresh_supersession_unqualified'):
        read(case, selector)
    assert {path: path.read_bytes() for path in state.rglob('*.json')} == before


def successor(root, selector):
    operation, payload, _ = private_refresh(root)
    payload['supersedes'] = selector
    rewrite_inputs(operation, payload)
    return operation, payload


def rewrite_inputs(operation, payload):
    path = Path(operation['inputs_path'])
    path.write_text(json.dumps(payload))
    operation['inputs_sha256'] = checksum(path)


def test_successor_entry_keeps_failed_evidence_and_completes_all_new_barriers(completed_upgrade):
    from tests.ops.test_nebius_management_refresh_predecessor import load_refresh

    root, old, selector = failed_case(completed_upgrade)
    operation, _ = successor(root, selector)
    before = {path: path.read_bytes() for path in old[2].parent.rglob('*.json')}
    loaded = context(operation)
    assert loaded.superseded.selector.operation == selector['operation']
    assert loaded.request.resources.switch.initial_stopped == loaded.superseded.stopped
    assert all(loaded.request.history[path] == checksum for path, checksum in loaded.superseded.history.items())
    case = install_case(loaded.request.resources, Path(operation['state_dir']).parent,
        history=loaded.request.history, installation_anchor=loaded.request.installation_anchor)
    case[1].switch.document = copy.deepcopy(old[1].switch.document)
    assert run(case)['status'] == 'management_refreshed'
    assert list(case[1].stages) == ['config', 'manager-probe', 'shared-probe', 'backup', 'migration', 'post-migration-probe']
    assert case[1].events.index('proof:backup') < case[1].events.index('migration') < case[1].events.index('public')
    assert case[1].switch.calls == ['retire', 'activate']
    completed = {'kind': 'refresh', **{key: operation[key] for key in (
        'operation_id', 'inputs_path', 'inputs_sha256', 'state_dir', 'anchor_dir')},
        'completion_sha256': checksum(case[2] / 'completion.json')}
    assert load_refresh(completed, root).active['spec']['replicas'] == 1
    assert run(case)['status'] == 'management_refreshed'
    assert {path: path.read_bytes() for path in before} == before


@pytest.mark.parametrize('damage', ['same_operation', 'same_config', 'root', 'predecessor', 'manager_revision', 'old_inputs', 'old_phase'])
def test_successor_entry_rejects_unbound_history_before_creating_state(completed_upgrade, damage):
    from scripts.ops.nebius_management_entry import EntryError
    from tests.ops.test_nebius_management_refresh_predecessor import complete_refresh

    root, old, selector = failed_case(completed_upgrade)
    operation, payload = successor(root, selector)
    if damage == 'same_operation':
        payload['supersedes']['operation'] = dict(operation)
    elif damage == 'same_config':
        previous = json.loads(Path(selector['operation']['inputs_path']).read_text())
        for key in ('deployment', 'candidate', 'profile'):
            payload[key] = previous[key]
    elif damage == 'root':
        payload['original_upgrade']['operation']['installation_id'] = str(uuid4())
    elif damage == 'predecessor':
        payload['predecessor'] = complete_refresh(root)[0]
    elif damage == 'manager_revision':
        payload['manager_revision'] = '0167'
    elif damage == 'old_inputs':
        Path(selector['operation']['inputs_path']).write_text('{}')
    else:
        (old[2] / 'shared-probe/stage.json').write_text('{}')
    rewrite_inputs(operation, payload)
    with pytest.raises(EntryError):
        context(operation)
    assert not Path(operation['state_dir']).exists()


def test_successor_chain_has_an_explicit_ancestry_bound(completed_upgrade, monkeypatch):
    from scripts.ops import nebius_management_refresh_entry as entry
    from scripts.ops.nebius_management_entry import EntryError
    from scripts.ops.nebius_management_refresh_install import ManagementRefreshInstallError

    root, old, selector = failed_case(completed_upgrade, 'manager-probe')
    first_inputs = json.loads(Path(selector['operation']['inputs_path']).read_text())
    # Root is already qualified. Cache only that unchanged read to keep the
    # real recursive input, hash, journal and switch checks inexpensive here.
    monkeypatch.setattr(entry, 'load_completed_upgrade', lambda selected: root if selected == root.selector else None)
    for depth in range(1, 10):
        operation, _ = successor(root, selector)
        if depth == 2:
            collision, payload = successor(root, selector)
            for key in ('deployment', 'candidate', 'profile'):
                payload[key] = first_inputs[key]
            rewrite_inputs(collision, payload)
            with pytest.raises(EntryError):
                context(collision)
            assert not Path(collision['state_dir']).exists()
        if depth == 9:
            with pytest.raises(EntryError):
                context(operation)
            assert not Path(operation['state_dir']).exists()
            break
        loaded = context(operation)
        case = install_case(loaded.request.resources, Path(operation['state_dir']).parent,
            history=loaded.request.history, installation_anchor=loaded.request.installation_anchor)
        case[1].switch.document = copy.deepcopy(old[1].switch.document)
        case[1].failed = 'manager-probe'
        with pytest.raises(ManagementRefreshInstallError):
            run(case)
        selector = {'operation': operation, 'refresh_sha256': checksum(case[2] / 'refresh.json'),
            'switch_sha256': checksum(case[2] / 'switch/cutover.json')}
        old = case
