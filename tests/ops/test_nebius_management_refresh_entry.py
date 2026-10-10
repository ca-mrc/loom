"""Protected refresh inputs bind a completed predecessor, never reset its state."""
from __future__ import annotations

import hashlib
import json
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest
from tests.ops.test_nebius_application_setup import application_material as application_material
from tests.ops.test_nebius_management_cloud_scope import cloud as cloud
from tests.ops.test_nebius_management_entry import entry_inputs as entry_inputs
from tests.ops.test_nebius_management_install import installation as installation
from tests.ops.test_nebius_management_prerequisites import checks as checks
from tests.ops.test_nebius_management_refresh_install import install_case
from tests.ops.test_nebius_management_refresh_predecessor import (
    complete_refresh,
    load,
    load_refresh,
    refresh_case,
)
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


def private_refresh(root, predecessor=None):
    request, _, state, anchor = refresh_case(root, predecessor)
    render = request.resources.switch.render
    payload = {'schema_version': 'loom.nebius-management-refresh-private-inputs.v1',
        'original_upgrade': root.selector.model_dump(mode='json'),
        'predecessor': (predecessor or root).selector.model_dump(mode='json'),
        'deployment': render.after.model_dump(mode='json'), 'candidate': render.candidate, 'profile': render.profile,
        'manager_revision': '0168', 'target_manager_revision': '0168',
        'prerequisites': root.inputs.prerequisites.model_dump(mode='json'), 'foundation_candidate': root.inputs.foundation_candidate}
    path = state.parent / 'inputs.json'
    path.write_text(json.dumps(payload))
    metadata = {'schema': 'loom.nebius-management-refresh-operation.v1',
        'source_sha': render.candidate['candidate_sha'], 'candidate': render.candidate['candidate_sha'],
        'installation_id': request.resources.binding.installation_id, 'namespace': request.resources.binding.namespace,
        'state_dir': str(state), 'anchor_dir': str(anchor), 'inputs_path': str(path),
        'inputs_sha256': hashlib.sha256(path.read_bytes()).hexdigest(), 'operation_id': str(request.resources.switch.operation_id)}
    operation_path = state.parent / 'operation.json'
    operation_path.write_text(json.dumps(metadata))
    operation_path.chmod(0o600)
    return metadata, payload, operation_path


def context(metadata):
    from scripts.ops.nebius_management_refresh_entry import load_refresh_inputs

    return load_refresh_inputs(metadata)


def test_refresh_inputs_preserve_root_material_and_bind_current_private_bytes(completed_upgrade):
    root = load(completed_upgrade[0])
    metadata, _, _ = private_refresh(root)
    before = {path: path.read_bytes() for path in root.history}
    loaded = context(metadata)
    assert loaded.original == loaded.predecessor == root
    assert loaded.request.resources.switch.render.before == root.deployment
    assert loaded.request.resources.switch.render.active == root.active
    assert str(loaded.request.resources.switch.operation_id) == metadata['operation_id']
    assert loaded.request.history[Path(metadata['inputs_path'])] == metadata['inputs_sha256']
    assert loaded.request.installation_anchor == root.upgrade.original_anchor
    assert {path: path.read_bytes() for path in before} == before
    assert not Path(metadata['state_dir']).exists()


def test_refresh_inputs_accept_a_completed_refresh_without_recursive_ancestry(completed_upgrade):
    root = load(completed_upgrade[0])
    selector, _ = complete_refresh(root)
    predecessor = load_refresh(selector, root)
    metadata, _, _ = private_refresh(root, predecessor)
    loaded = context(metadata)
    assert loaded.predecessor == predecessor
    assert loaded.request.resources.switch.render.before == predecessor.deployment
    assert loaded.request.resources.switch.render.active == predecessor.active
    assert len(loaded.request.history) == len(predecessor.history) + 1


@pytest.mark.parametrize('damage', ['inputs_hash', 'namespace', 'candidate', 'predecessor', 'root', 'permission', 'revision', 'nil_uuid'])
def test_refresh_private_drift_or_wider_scope_is_rejected_without_writes(completed_upgrade, damage):
    from scripts.ops.nebius_management_entry import EntryError

    root = load(completed_upgrade[0])
    metadata, payload, _ = private_refresh(root)
    if damage == 'inputs_hash':
        metadata['inputs_sha256'] = '0' * 64
    elif damage == 'namespace':
        metadata['namespace'] += '-other'
    elif damage == 'nil_uuid':
        metadata['operation_id'] = str(uuid4())
    else:
        if damage == 'candidate':
            payload['candidate']['candidate_sha'] = 'e' * 40
        elif damage == 'predecessor':
            payload['predecessor']['state_sha256'] = '0' * 64
        elif damage == 'root':
            payload['original_upgrade']['state_sha256'] = '0' * 64
        elif damage == 'permission':
            payload['deployment']['installation']['platform_budget']['cpu_millis'] += 1000
        else:
            payload['target_manager_revision'] = 'head; arbitrary'
        path = Path(metadata['inputs_path'])
        path.write_text(json.dumps(payload))
        metadata['inputs_sha256'] = hashlib.sha256(path.read_bytes()).hexdigest()
    with pytest.raises(EntryError) as error:
        context(metadata)
    assert 'private' in str(error.value) and 'arbitrary' not in str(error.value)
    assert not Path(metadata['state_dir']).exists()


def test_entry_dispatch_and_bound_public_reports_preserve_original_history(completed_upgrade, monkeypatch, capsys):
    from scripts.ops import nebius_management_entry as entry
    from scripts.ops import nebius_management_refresh_entry as refresh

    root = load(completed_upgrade[0])
    metadata, _, operation_path = private_refresh(root)
    bound = context(metadata)
    case = install_case(bound.request.resources, Path(metadata['state_dir']).parent,
        history=bound.request.history, installation_anchor=bound.request.installation_anchor)
    case[1].switch.document['metadata'].update(resourceVersion='30', generation=5)
    original = {path: path.read_bytes() for path in root.history}

    @contextmanager
    def connected(selected, operation):
        assert selected == bound and operation == metadata
        yield case[1]

    monkeypatch.setattr(refresh, 'connected_refresh_api', connected)
    for action, status in [('preflight', 'preflight_qualified'), ('install', 'management_refreshed'), ('install', 'management_refreshed')]:
        assert entry.main(str(operation_path), action) == 0
        report = json.loads(capsys.readouterr().out)
        assert report['status'] == status and report['operation_id'] == metadata['operation_id']
        assert set(report) <= {'status', 'operation_id', 'installation_id', 'namespace', 'namespace_uid', 'revision', 'source_sha', 'candidate'}
    assert case[1].switch.calls == ['retire', 'activate']
    assert {path: path.read_bytes() for path in original} == original


@pytest.mark.parametrize('stage', ['activation', 'resource_inventory', 'publication', 'persistent_storage'])
def test_blocked_refresh_entry_reports_only_bound_phase_not_provider_payload(completed_upgrade, monkeypatch, capsys, stage):
    from scripts.ops import nebius_management_entry as entry
    from scripts.ops import nebius_management_refresh_entry as refresh
    from scripts.ops.nebius_management_refresh_install import ManagementRefreshInstallError

    root = load(completed_upgrade[0])
    metadata, _, path = private_refresh(root)

    @contextmanager
    def unavailable(_context, _operation):
        raise ManagementRefreshInstallError(stage) from RuntimeError('private-provider-payload')
        yield

    monkeypatch.setattr(refresh, 'connected_refresh_api', unavailable)
    assert entry.main(str(path), 'install') == 0
    report = json.loads(capsys.readouterr().out)
    assert report == {'status': 'blocked', 'stage': 'refresh_' + stage, **{key: metadata[key]
        for key in ('source_sha', 'candidate', 'installation_id', 'namespace', 'operation_id')}}
    assert 'private-provider-payload' not in json.dumps(report)


@pytest.mark.parametrize('message,expected,typed', [
    ('pool cutover publication unqualified', 'refresh_pool_publication', True),
    ('pool cutover operator readers unqualified', 'refresh_pool_operator_readers', True),
    ('pool cutover runtime databases unqualified', 'refresh_pool_runtime_databases', True),
    ('pool cutover runtime telemetry unqualified', 'refresh_pool_runtime_telemetry', True),
    ('pool cutover runtime telemetry tls_api_verify_20 unqualified', 'refresh_pool_runtime_telemetry_tls_api_verify_20', True),
    ('pool cutover management database unqualified', 'refresh_pool_management_database', True),
    ('pool cutover provider unqualified', 'refresh_pool_provider', True),
    ('pool cutover connected scope unqualified', 'refresh_pool_connected_scope', True),
    ('pool cutover context changed before connection', 'refresh_pool_private_inputs', True),
    ('pool cutover context changed during publication', 'refresh_pool_private_inputs', True),
    ('private-provider-payload', 'refresh_connection', True),
    ('pool cutover publication unqualified: private-provider-payload', 'refresh_connection', True),
    ('pool cutover publication unqualified', 'refresh_connection', False),
])
def test_blocked_refresh_preserves_only_typed_allowlisted_pool_connection_errors(
        completed_upgrade, monkeypatch, capsys, message, expected, typed):
    from scripts.ops import nebius_management_entry as entry
    from scripts.ops import nebius_management_refresh_entry as refresh
    from scripts.ops.nebius_management_gateway import safe_report

    metadata, _, path = private_refresh(load(completed_upgrade[0]))

    @contextmanager
    def unavailable(_context, _operation):
        raise (entry.EntryError if typed else RuntimeError)(message) from RuntimeError('private-provider-payload')
        yield

    monkeypatch.setattr(refresh, 'connected_refresh_api', unavailable)
    assert entry.main(str(path), 'preflight') == 0
    report = json.loads(capsys.readouterr().out)
    assert report == {'status': 'blocked', 'stage': expected, **{key: metadata[key]
        for key in ('source_sha', 'candidate', 'installation_id', 'namespace', 'operation_id')}}
    assert 'private-provider-payload' not in json.dumps(report)
    assert safe_report(json.dumps(report).encode(), metadata) == report


@pytest.mark.parametrize('damage', [False, True])
def test_refresh_entry_carries_only_valid_capacity_failure_details(completed_upgrade, monkeypatch, capsys, damage):
    from scripts.ops import nebius_management_entry as entry
    from scripts.ops import nebius_management_refresh_entry as refresh
    from scripts.ops.nebius_management_refresh_install import ManagementRefreshInstallError
    from tests.ops.test_nebius_management_gateway import capacity_report

    root = load(completed_upgrade[0])
    metadata, _, path = private_refresh(root)
    detail = capacity_report()
    if damage:
        detail['provider_message'] = 'private-secret'
    def preflight(_request):
        raise ManagementRefreshInstallError('platform_capacity')
    @contextmanager
    def connected(_context, _operation):
        yield SimpleNamespace(preflight=preflight, checks=SimpleNamespace(capacity_diagnostic=detail))
    monkeypatch.setattr(refresh, 'connected_refresh_api', connected)
    assert entry.main(str(path), 'preflight') == 0
    report = json.loads(capsys.readouterr().out)
    assert report['stage'] == 'refresh_platform_capacity'
    if damage:
        assert 'capacity' not in report
    else:
        assert report['capacity'] == detail
    assert 'private-secret' not in json.dumps(report)
    assert not Path(metadata['state_dir']).exists()
