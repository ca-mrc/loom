"""Protected management transport accepts one exact source/input authority only."""
from __future__ import annotations

import hashlib
import importlib
import io
import json
from types import SimpleNamespace
from uuid import uuid4

import pytest
from tests.ops.test_nebius_ingress_bootstrap import archive


def module():
    return importlib.import_module("scripts.ops.nebius_management_gateway")


def operation(tmp_path):
    root = tmp_path / "nebius-management"
    return {"schema": "loom.nebius-management-operation.v1", "source_sha": "a" * 40,
        "candidate": "b" * 40, "installation_id": "18718d96-d389-40b3-a79b-11489924d0d4",
        "namespace": "loom-nebius-management", "state_dir": str(root / "state"),
        "anchor_dir": str(root / "anchor"), "inputs_path": str(root / "inputs.json"),
        "inputs_sha256": "c" * 64}


def upgrade_operation(tmp_path):
    metadata = operation(tmp_path)
    root = tmp_path / "nebius-management/upgrade"
    return {**metadata, "schema": "loom.nebius-management-upgrade-operation.v1",
        "state_dir": str(root / "state"), "anchor_dir": str(root / "anchor"),
        "inputs_path": str(root / "inputs.json")}


def retirement_operation(tmp_path):
    metadata = operation(tmp_path)
    root = tmp_path / "nebius-management/retirement"
    return {**metadata, "schema": "loom.nebius-management-retirement-operation.v1",
        "state_dir": str(root / "state"), "anchor_dir": str(root / "anchor"), "inputs_path": str(root / "inputs.json")}


def diagnostic_operation(tmp_path):
    metadata = operation(tmp_path)
    root = tmp_path / "nebius-management/retirement-diagnostic"
    return {**metadata, "schema": "loom.nebius-management-retirement-diagnostic-operation.v1",
        "state_dir": str(root / "state"), "anchor_dir": str(root / "anchor"), "inputs_path": str(root / "inputs.json")}


def recovery_operation(tmp_path):
    metadata = operation(tmp_path)
    root = tmp_path / "nebius-management/retirement-recovery"
    return {**metadata, "schema": "loom.nebius-management-retirement-recovery-operation.v1",
        "state_dir": str(root / "state"), "anchor_dir": str(root / "anchor"), "inputs_path": str(root / "inputs.json")}


def refresh_operation(tmp_path):
    metadata = operation(tmp_path)
    operation_id = str(uuid4())
    root = tmp_path / 'nebius-management/refresh' / operation_id
    return {**metadata, 'schema': 'loom.nebius-management-refresh-operation.v1', 'source_sha': metadata['candidate'],
        'operation_id': operation_id, 'state_dir': str(root / 'state'), 'anchor_dir': str(root / 'anchor'),
        'inputs_path': str(root / 'inputs.json')}


def pool_operation(tmp_path):
    metadata = refresh_operation(tmp_path)
    root = tmp_path / 'nebius-management/pool-cutover' / metadata['operation_id']
    return {**metadata, 'schema': 'loom.nebius-pool-cutover-operation.v1',
        'state_dir': str(root / 'state'), 'anchor_dir': str(root / 'anchor'), 'inputs_path': str(root / 'inputs.json')}


@pytest.mark.parametrize('detail', [
    'writer_bindings', 'writer_workloads', 'connected_prerequisites', 'capacity',
    'scope', 'database_report', 'pending_source', 'pending_page', 'origin_history', 'database_readiness',
])
def test_pool_preflight_failure_category_survives_both_protected_filters(tmp_path, detail):
    gateway = module()
    metadata = pool_operation(tmp_path)
    report = {key: metadata[key] for key in ('source_sha', 'candidate', 'installation_id', 'namespace', 'operation_id')}
    report.update(status='blocked', stage='pool_preflight_' + detail)
    first = gateway.safe_report(json.dumps(report | {'private': 'never-export'}).encode(), metadata)
    assert first == report
    assert gateway.safe_report(json.dumps(first).encode(), metadata) == report
    with pytest.raises(gateway.GatewayError):
        gateway.safe_report(json.dumps(report | {'stage': report['stage'] + '_private-value'}).encode(), metadata)


@pytest.mark.parametrize('outcome', ['global', 'legacy'])
def test_pool_authority_reports_terminal_history_but_never_installed_acceptance(tmp_path, outcome):
    gateway = module()
    metadata = pool_operation(tmp_path)
    gateway.validate_operation(metadata)
    common = {key: metadata[key] for key in ('source_sha', 'candidate', 'installation_id', 'namespace', 'operation_id')}
    complete = {**common, 'status': 'pool_cutover_completed', 'outcome': outcome,
        'completion_sha256': 'f' * 64, 'acceptance_verified': False}
    assert gateway.safe_report(json.dumps(complete | {'private': 'never-export'}).encode(), metadata) == complete
    for damage in ({'operation_id': str(uuid4())}, {'outcome': 'private-value'}, {'acceptance_verified': True},
            {'acceptance_verified': 0}, {'completion_sha256': 'private-value'}, {'status': 'management_installed'}):
        with pytest.raises(gateway.GatewayError):
            gateway.safe_report(json.dumps(complete | damage).encode(), metadata)
    for phase in ('cutover', 'startup', 'activation', 'startup-fence', 'shutdown', 'machine-retirement',
            'gateway-retirement', 'template-restoration', 'role-restoration', 'legacy-restart', 'legacy-reopening'):
        pending = {**common, 'status': 'pending', 'phase': phase}
        assert gateway.safe_report(json.dumps(pending).encode(), metadata) == pending
        blocked = {**common, 'status': 'blocked', 'stage': 'pool_' + phase.replace('-', '_')}
        assert gateway.safe_report(json.dumps(blocked).encode(), metadata) == blocked
    with pytest.raises(gateway.GatewayError):
        gateway.safe_report(json.dumps({**common, 'status': 'pending', 'phase': 'private-value'}).encode(), metadata)


@pytest.mark.parametrize('reason', [
    'pending_pool_cleanup', 'pending_shutdown_update', 'pending_shutdown_outcome', 'pending_successor_drain',
])
def test_shutdown_pending_reason_survives_both_filters_only_in_its_fixed_phase(tmp_path, reason):
    gateway = module()
    metadata = pool_operation(tmp_path)
    common = {key: metadata[key] for key in ('source_sha', 'candidate', 'installation_id', 'namespace', 'operation_id')}
    report = {**common, 'status': 'pending', 'phase': 'shutdown', 'pending_reason': reason}
    first = gateway.safe_report(json.dumps(report | {'private': 'never-export'}).encode(), metadata)
    assert first == report
    assert gateway.safe_report(json.dumps(first).encode(), metadata) == report
    for damage in ({'pending_reason': reason + '-private-value'}, {'phase': 'startup'}):
        with pytest.raises(gateway.GatewayError):
            gateway.safe_report(json.dumps(report | damage).encode(), metadata)


@pytest.mark.parametrize('result', [
    {'status': 'preflight_qualified'}, {'status': 'pending', 'phase': 'startup'},
    {'status': 'pending', 'phase': 'activation'}, {'status': 'pending', 'phase': 'legacy-reopening'},
    {'status': 'pool_cutover_completed', 'outcome': 'global', 'completion_sha256': 'f' * 64, 'acceptance_verified': False},
    {'status': 'pool_cutover_completed', 'outcome': 'legacy', 'completion_sha256': 'f' * 64, 'acceptance_verified': False},
])
@pytest.mark.parametrize('telemetry', [
    {'status': 'available', 'checks': 2, 'unavailable': 0, 'reasons': []},
    {'status': 'unavailable', 'checks': 2, 'unavailable': 1, 'reasons': ['tls_kubelet_verify_19']},
    {'status': 'not_observed', 'checks': 0, 'unavailable': 0, 'reasons': []},
])
def test_pool_telemetry_availability_survives_both_protected_filters(tmp_path, result, telemetry):
    gateway = module()
    metadata = pool_operation(tmp_path)
    report = {key: metadata[key] for key in ('source_sha', 'candidate', 'installation_id', 'namespace', 'operation_id')}
    report.update(result, telemetry=telemetry)
    first = gateway.safe_report(json.dumps(report | {'private': 'never-export'}).encode(), metadata)
    assert first == report
    assert gateway.safe_report(json.dumps(first).encode(), metadata) == report


@pytest.mark.parametrize('damage', [
    None, [], {}, {'status': 'healthy'}, {'status': 'available'}, {'status': 'not_observed'},
    {'checks': 0}, {'checks': -1}, {'checks': True}, {'checks': 2**31}, {'checks': '2'},
    {'unavailable': 0}, {'unavailable': 3}, {'unavailable': True}, {'unavailable': -1},
    {'reasons': []}, {'reasons': 'counters'}, {'reasons': ['counters', 'counters']},
    {'reasons': ['tls_api_verify_19']}, {'reasons': ['tls_unknown_verify_19']},
    {'reasons': ['tls_kubelet_verify_256']}, {'reasons': ['authorization']},
    {'reasons': ['network']}, {'reasons': ['payload']}, {'reasons': ['authority']},
    {'reasons': ['close']}, {'reasons': ['private-token']}, {'private': 'never-export'},
])
def test_pool_telemetry_rejects_false_health_and_unqualified_details(tmp_path, damage):
    gateway = module()
    metadata = pool_operation(tmp_path)
    telemetry = {'status': 'unavailable', 'checks': 2, 'unavailable': 1, 'reasons': ['tls_kubelet_verify_19']}
    report = {key: metadata[key] for key in ('source_sha', 'candidate', 'installation_id', 'namespace', 'operation_id')}
    report.update(status='preflight_qualified', telemetry=telemetry | damage if damage else damage)
    with pytest.raises(gateway.GatewayError):
        gateway.safe_report(json.dumps(report).encode(), metadata)


@pytest.mark.parametrize('damage', ['source', 'uuid', 'borrow_refresh', 'namespace', 'extra'])
def test_pool_metadata_cannot_borrow_other_authority_or_choose_a_child_phase(tmp_path, damage):
    metadata = pool_operation(tmp_path)
    if damage == 'source':
        metadata['source_sha'] = 'd' * 40
    elif damage == 'uuid':
        metadata['operation_id'] = str(uuid4())
    elif damage == 'borrow_refresh':
        other = refresh_operation(tmp_path)
        metadata.update({key: other[key] for key in ('state_dir', 'anchor_dir', 'inputs_path')})
    elif damage == 'namespace':
        metadata['namespace'] = 'kube-system'
    else:
        metadata['phase'] = 'activation'
    with pytest.raises(module().GatewayError):
        module().validate_operation(metadata)


def test_refresh_authority_binds_operation_uuid_layout_and_closed_results(tmp_path):
    gateway = module()
    metadata = refresh_operation(tmp_path)
    gateway.validate_operation(metadata)
    common = {key: metadata[key] for key in ('source_sha', 'candidate', 'installation_id', 'namespace', 'operation_id')}
    for status in ('preflight_qualified', 'management_refreshed', 'pending', 'blocked'):
        report = {**common, 'status': status, 'private': 'must-not-leave-gateway'}
        if status in {'management_refreshed', 'pending'}:
            report.update(namespace_uid=str(uuid4()), revision='sha256:' + 'f' * 64)
        if status == 'pending':
            report['phase'] = 'post-migration-probe'
        elif status == 'blocked':
            report['stage'] = 'refresh_activation'
        result = gateway.safe_report(json.dumps(report).encode(), metadata)
        assert result['operation_id'] == metadata['operation_id'] and 'private' not in result
        report['operation_id'] = str(uuid4())
        with pytest.raises(gateway.GatewayError):
            gateway.safe_report(json.dumps(report).encode(), metadata)


@pytest.mark.parametrize('stage', [
    'refresh_recovery', 'refresh_cluster_identity', 'refresh_resource_inventory',
    'refresh_persistent_storage', 'refresh_prerequisites', 'refresh_foundation',
    'refresh_shared_material', 'refresh_platform_capacity', 'refresh_publication',
    'refresh_cloud_identity', 'refresh_public_route', 'refresh_supersession', 'refresh_pool_authority',
])
def test_refresh_preserves_closed_retained_preflight_diagnostics(tmp_path, stage):
    gateway = module()
    metadata = refresh_operation(tmp_path)
    report = {key: metadata[key] for key in ('source_sha', 'candidate', 'installation_id', 'namespace', 'operation_id')}
    report.update(status='blocked', stage=stage, private='private-provider-payload')
    safe = gateway.safe_report(json.dumps(report).encode(), metadata)
    assert safe == {key: value for key, value in report.items() if key != 'private'}
    with pytest.raises(gateway.GatewayError):
        gateway.safe_report(json.dumps(report | {'stage': 'refresh_private-provider-payload'}).encode(), metadata)


def capacity_report():
    return {'schema': 'loom.nebius-platform-capacity-diagnostic.v1', 'stage': 'capacity',
        'kind': None, 'error_type': 'ManagementCapacityError', 'nodes': [{
            'node_uid': '18718d96-d389-40b3-a79b-11489924d0d7', 'placement_matches': True,
            'allocatable': {'cpu_millis': 1000, 'memory_mib': 2048, 'ephemeral_storage_mib': 4096, 'pods': 8},
            'required': {'cpu_millis': 1200, 'memory_mib': 1024, 'ephemeral_storage_mib': 2048, 'pods': 4}}]}


def test_refresh_capacity_details_survive_both_report_boundaries(tmp_path):
    gateway = module()
    metadata = refresh_operation(tmp_path)
    report = {key: metadata[key] for key in ('source_sha', 'candidate', 'installation_id', 'namespace', 'operation_id')}
    report.update(status='blocked', stage='refresh_platform_capacity', capacity=capacity_report())
    first = gateway.safe_report(json.dumps(report).encode(), metadata)
    assert first == report
    assert gateway.safe_report(json.dumps(first).encode(), metadata) == report


@pytest.mark.parametrize('damage', ['private', 'stage', 'kind', 'error', 'bool', 'negative', 'huge',
    'extra_resource', 'uid', 'many_nodes', 'wrong_outer_stage'])
def test_capacity_report_cannot_export_unbounded_or_private_details(tmp_path, damage):
    gateway = module()
    metadata = refresh_operation(tmp_path)
    report = {key: metadata[key] for key in ('source_sha', 'candidate', 'installation_id', 'namespace', 'operation_id')}
    report.update(status='blocked', stage='refresh_platform_capacity', capacity=capacity_report())
    detail = report['capacity']
    if damage == 'private':
        detail['message'] = 'secret-provider-response'
    elif damage in {'stage', 'kind'}:
        detail[damage] = 'private-workload-name'
    elif damage == 'error':
        detail['error_type'] = 'secret-exception-name'
    elif damage in {'bool', 'negative', 'huge'}:
        detail['nodes'][0]['required']['pods'] = {'bool': True, 'negative': -1, 'huge': 2**63}[damage]
    elif damage == 'extra_resource':
        detail['nodes'][0]['required']['credentials'] = 'secret'
    elif damage == 'uid':
        detail['nodes'][0]['node_uid'] = 'private-host'
    elif damage == 'many_nodes':
        detail['nodes'] *= 65
    else:
        report['stage'] = 'refresh_publication'
    with pytest.raises(gateway.GatewayError):
        gateway.safe_report(json.dumps(report).encode(), metadata)


@pytest.mark.parametrize('damage', ['nil_uuid', 'other_uuid_path', 'old_state', 'source_mismatch', 'extra_field'])
def test_refresh_metadata_cannot_borrow_or_reset_another_operation(tmp_path, damage):
    gateway = module()
    metadata = refresh_operation(tmp_path)
    if damage == 'nil_uuid':
        metadata['operation_id'] = '00000000-0000-0000-0000-000000000000'
    elif damage == 'other_uuid_path':
        metadata['operation_id'] = str(uuid4())
    elif damage == 'old_state':
        metadata['state_dir'] = upgrade_operation(tmp_path)['state_dir']
    elif damage == 'source_mismatch':
        metadata['source_sha'] = 'e' * 40
    else:
        metadata['command'] = 'unqualified'
    with pytest.raises(gateway.GatewayError):
        gateway.validate_operation(metadata)


def startup_report():
    return {"schema": "loom.nebius-retirement-startup-probe.v1", "status": "observed", "stage": "complete",
        "checks": ["database_binding", "kubernetes_ca", "kubernetes_token", "database", "kubernetes"],
        "operations": [{"operation_id": "18718d96-d389-40b3-a79b-11489924d0d6", "phase": "pending",
            "runner_epoch": 0, "lease_present": False, "error_present": False, "resource_count": 3, "effects_started": False}]}


def recovery_report():
    return {"schema": "loom.nebius-retirement-recovery-report.v1", "status": "completed", "stage": "complete",
        "retirement_started": True, "error_type": None, "startup": startup_report(), "operations": [{
            "operation_id": "18718d96-d389-40b3-a79b-11489924d0d6", "phase": "completed",
            "non_storage_released": True, "storage_preserved": True}]}


def test_recovery_authority_reports_only_qualified_completion_or_closed_failure(tmp_path):
    gateway = module()
    metadata = recovery_operation(tmp_path)
    gateway.validate_operation(metadata)
    report = {**metadata, "status": "retirement_recovered", "namespace_uid": "18718d96-d389-40b3-a79b-11489924d0d5",
        "revision": "sha256:" + "f" * 64, "recovery": recovery_report()}
    assert gateway.safe_report(json.dumps(report).encode(), metadata)["recovery"] == recovery_report()
    blocked = recovery_report() | {"status": "blocked", "stage": "retirement", "error_type": "ManagementError", "operations": []}
    report.update(status="blocked", stage="recovery_runtime", recovery=blocked)
    assert gateway.safe_report(json.dumps(report).encode(), metadata)["recovery"] == blocked
    report["status"] = "retirement_recovered"
    with pytest.raises(gateway.GatewayError):
        gateway.safe_report(json.dumps(report).encode(), metadata)
    for field in ("inputs_path", "state_dir", "anchor_dir"):
        wrong = metadata | {field: diagnostic_operation(tmp_path)[field]}
        with pytest.raises(gateway.GatewayError):
            gateway.validate_operation(wrong)


@pytest.mark.parametrize("change", ["raw_message", "missing_storage", "not_released", "foreign_operation",
    "duplicate", "startup_unavailable", "prior_attempt", "not_started", "boolean_proof", "error"])
def test_recovery_report_cannot_invent_release_or_leak_raw_failures(change):
    gateway = module()
    report = recovery_report()
    if change == "raw_message":
        report["message"] = "private-provider-error"
    elif change == "missing_storage":
        del report["operations"][0]["storage_preserved"]
    elif change == "not_released":
        report["operations"][0]["non_storage_released"] = False
    elif change == "foreign_operation":
        report["operations"][0]["operation_id"] = "18718d96-d389-40b3-a79b-11489924d0d7"
    elif change == "duplicate":
        report["operations"].append(dict(report["operations"][0]))
    elif change == "startup_unavailable":
        report["startup"] = None
    elif change == "prior_attempt":
        report["startup"]["operations"][0]["runner_epoch"] = 1
    elif change == "not_started":
        report["retirement_started"] = False
    elif change == "boolean_proof":
        report["operations"][0]["storage_preserved"] = 1
    else:
        report["error_type"] = "private-exception"
    with pytest.raises(gateway.GatewayError):
        gateway.validate_recovery_report(report)


def test_diagnostic_metadata_and_bounded_result_are_separate_from_retirement(tmp_path):
    gateway = module()
    metadata = diagnostic_operation(tmp_path)
    gateway.validate_operation(metadata)
    report = {**metadata, "status": "retirement_diagnostic_observed", "namespace_uid": "18718d96-d389-40b3-a79b-11489924d0d5",
        "revision": "sha256:" + "f" * 64, "probe": startup_report(), "private": "never-return"}
    result = gateway.safe_report(json.dumps(report).encode(), metadata)
    assert result["probe"] == startup_report() and "private" not in result
    for wrong in ("management_retired", "management_installed", "management_upgraded"):
        with pytest.raises(gateway.GatewayError):
            gateway.safe_report(json.dumps(report | {"status": wrong}).encode(), metadata)
    report.update(status="pending", phase="retirement-diagnostic")
    assert gateway.safe_report(json.dumps(report).encode(), metadata)["phase"] == "retirement-diagnostic"


@pytest.mark.parametrize("change", ["raw_message", "checks", "operation_field", "boolean_epoch", "phase", "duplicate", "unknown_stage"])
def test_startup_report_contract_cannot_leak_or_invent_observations(change):
    gateway = module()
    report = startup_report()
    if change == "raw_message":
        report["message"] = "private-provider-error"
    elif change == "checks":
        report["checks"] = []
    elif change == "operation_field":
        report["operations"][0]["lease_token"] = "private-token"
    elif change == "boolean_epoch":
        report["operations"][0]["runner_epoch"] = True
    elif change == "phase":
        report["operations"][0]["phase"] = "private-phase"
    elif change == "duplicate":
        report["operations"].append(dict(report["operations"][0]))
    else:
        report["stage"] = "private-stage"
    with pytest.raises(gateway.GatewayError):
        gateway.validate_startup_report(report)


def test_unavailable_startup_report_is_observation_not_cleanup_success(tmp_path):
    gateway = module()
    metadata = diagnostic_operation(tmp_path)
    probe = {"schema": "loom.nebius-retirement-startup-probe.v1", "status": "unavailable", "stage": "kubernetes_get",
        "checks": ["database_binding", "kubernetes_ca", "kubernetes_token", "database"],
        "operations": startup_report()["operations"], "error_type": "HTTPStatusError", "http_status": 403}
    report = {**metadata, "status": "retirement_diagnostic_observed", "namespace_uid": "18718d96-d389-40b3-a79b-11489924d0d5",
        "revision": "sha256:" + "f" * 64, "probe": probe}
    assert gateway.safe_report(json.dumps(report).encode(), metadata)["probe"] == probe
    with pytest.raises(gateway.GatewayError):
        gateway.validate_startup_report(probe | {"error_type": "private-error"})


def test_retirement_authority_uses_separate_recovery_and_reports_no_bootstrap_success(tmp_path):
    gateway = module()
    metadata = retirement_operation(tmp_path)
    gateway.validate_operation(metadata)
    for status, phase in (("pending", "retirement"), ("management_retired", None)):
        report = {**metadata, "status": status, "namespace_uid": "18718d96-d389-40b3-a79b-11489924d0d5",
            "revision": "sha256:" + "f" * 64, "private": "do-not-report"}
        if phase:
            report["phase"] = phase
        assert "private" not in gateway.safe_report(json.dumps(report).encode(), metadata)
    report["status"] = "management_installed"
    with pytest.raises(gateway.GatewayError):
        gateway.safe_report(json.dumps(report).encode(), metadata)
    metadata["inputs_path"] = str(tmp_path / "nebius-management/upgrade/inputs.json")
    with pytest.raises(gateway.GatewayError):
        gateway.validate_operation(metadata)


def bundle(tmp_path):
    files = {name: b"fixture source" for name in module().SOURCES}
    files.update({"uv": b"fixture binary", "requirements.txt": b"fixture hashed dependencies",
        "operation.json": json.dumps(operation(tmp_path)).encode(),
        "wheels/loom-0.0.0-py3-none-any.whl": b"loom wheel",
        "wheels/loom_bundle_checksum-0.1.0-py3-none-any.whl": b"checksum wheel"})
    return files


@pytest.mark.parametrize("change", ["extra", "missing_template", "hash", "missing_wheel", "private_input"])
def test_unqualified_bundle_cannot_create_tooling(tmp_path, change):
    files = bundle(tmp_path)
    if change == "extra":
        files["../outside"] = b"bad"
    elif change == "missing_template":
        files.pop("deploy/k8s/nebius-execution-actuator.yaml")
    elif change == "missing_wheel":
        files.pop("wheels/loom_bundle_checksum-0.1.0-py3-none-any.whl")
    elif change == "private_input":
        files["inputs.json"] = b"must-never-transfer"
    with pytest.raises(module().GatewayError):
        module().prepare_release(archive(files, bad_hash=change == "hash"))
    assert not (tmp_path / "nebius-management").exists()


@pytest.mark.parametrize("field,value", [("state_dir", "/tmp/foreign/state"), ("anchor_dir", "/tmp/elsewhere"),
    ("source_sha", "dev"), ("candidate", "HEAD"), ("inputs_sha256", ""), ("private_key", "never-transfer")])
def test_operation_metadata_cannot_broaden_authority(tmp_path, field, value):
    metadata = operation(tmp_path)
    metadata[field] = value
    with pytest.raises(module().GatewayError):
        module().validate_operation(metadata)


def test_tooling_is_private_pinned_and_replay_does_not_reinstall(tmp_path, monkeypatch):
    gateway = module()
    calls = []
    monkeypatch.setattr(gateway, "run_private", lambda args, **kwargs: calls.append(args) or b"")
    content = archive(bundle(tmp_path))
    release = gateway.prepare_release(content)
    assert (release / "operation.json").read_bytes() == bundle(tmp_path)["operation.json"]
    assert (release / "deploy/k8s/nebius-capacity-collector.yaml").is_file()
    assert "--require-hashes" in calls[1] and "--only-binary" in calls[1]
    assert "--offline" in calls[2] and "--no-deps" in calls[2]
    assert calls[3][-1] == "qualify" and calls[3][1:4] == ["-I", "-B", "-c"]
    assert gateway.prepare_release(content) == release and len(calls) == 4
    assert not (release / "inputs.json").exists()
    assert not (release.parent.parent / "state").exists()
    assert all(path.stat().st_mode & 0o077 == 0 for path in release.rglob("*"))


def test_incomplete_tooling_never_retries_and_modified_tooling_never_replays(tmp_path, monkeypatch):
    gateway = module()
    calls = []
    def fail(args, **kwargs):
        calls.append(args)
        raise RuntimeError("private failure")
    monkeypatch.setattr(gateway, "run_private", fail)
    content = archive(bundle(tmp_path))
    for _ in range(2):
        with pytest.raises(gateway.GatewayError):
            gateway.prepare_release(content)
    assert len(calls) == 1


@pytest.mark.parametrize('failed_step,stage', [
    (1, 'tooling_venv'), (2, 'tooling_dependency_sync'),
    (3, 'tooling_wheel_install'), (4, 'tooling_import_qualification'),
])
def test_tooling_failure_identifies_boundary_without_completing_or_exposing_child_output(
        tmp_path, monkeypatch, failed_step, stage):
    from pathlib import Path

    gateway = module()
    calls = []

    def run(args, **kwargs):
        calls.append(args)
        if len(calls) == failed_step:
            raise RuntimeError('private child output with credentials and paths')
        return b''

    monkeypatch.setattr(gateway, 'run_private', run)
    content = archive(bundle(tmp_path))
    with pytest.raises(gateway.ToolingPreparationError) as failure:
        gateway.prepare_release(content)
    assert failure.value.stage == stage
    assert 'private child' not in str(failure.value)
    root = Path(operation(tmp_path)['state_dir']).parent
    assert not (root / 'releases' / hashlib.sha256(content).hexdigest() / 'complete').exists()
    assert not (root / 'state').exists() and len(calls) == failed_step


@pytest.mark.parametrize("command", ["", "loom-nebius-management-install-v1 extra", "kubectl apply", "loom-nebius-ingress-v1"])
def test_forced_command_rejects_arbitrary_or_other_authority_without_reading_input(command, monkeypatch):
    import sys
    monkeypatch.setenv("SSH_ORIGINAL_COMMAND", command)
    monkeypatch.setattr(sys, "stdin", object())
    assert module().authorized_main("a" * 64) == 126


def test_forced_command_requires_exact_bundle_digest(tmp_path, monkeypatch):
    import sys
    content = archive(bundle(tmp_path))
    monkeypatch.setenv("SSH_ORIGINAL_COMMAND", "loom-nebius-management-install-v1")
    monkeypatch.setattr(sys, "stdin", SimpleNamespace(buffer=io.BytesIO(content)))
    assert module().authorized_main("a" * 64) == 126
    assert not (tmp_path / "nebius-management").exists()


@pytest.mark.parametrize("status", ["preflight_qualified", "pending", "management_installed", "blocked"])
def test_public_report_strips_all_private_material_and_binds_operation(tmp_path, status):
    metadata = operation(tmp_path)
    report = {key: metadata[key] for key in ("source_sha", "candidate", "installation_id", "namespace")}
    report.update(status=status, stage="storage_class", phase="database", namespace_uid="52f5b18c-7dd3-4095-bd7e-49f6a6330391",
                  revision="sha256:" + "d" * 64, material="must-never-transfer")
    report["backup"] = {"job_uid": "52f5b18c-7dd3-4095-bd7e-49f6a6330391", "sha256": "f" * 64,
        "bytes": 1234, "key": "loom-nebius-management/2026/09/24/120000-" + "f" * 12 + ".dump", "private": "must-never-transfer"}
    safe = module().safe_report(json.dumps(report).encode(), metadata)
    assert safe["status"] == status and "must-never-transfer" not in json.dumps(safe)
    if status == "blocked":
        assert safe["stage"] == "storage_class" and "backup" not in safe and "namespace_uid" not in safe
    if status == "management_installed":
        assert safe["backup"] == {key: value for key, value in report["backup"].items() if key != "private"}
    report["candidate"] = "e" * 40
    with pytest.raises(module().GatewayError):
        module().safe_report(json.dumps(report).encode(), metadata)


@pytest.mark.parametrize("field,value", [("key", "outside/private-input.json"), ("bytes", -1),
    ("job_uid", "private diagnostic"), ("sha256", "unbounded-output")])
def test_backup_receipt_rejects_malformed_or_private_fields(tmp_path, field, value):
    metadata = operation(tmp_path)
    report = {key: metadata[key] for key in ("source_sha", "candidate", "installation_id", "namespace")}
    report.update(status="management_installed", namespace_uid="52f5b18c-7dd3-4095-bd7e-49f6a6330391", revision="sha256:" + "d" * 64,
        backup={"job_uid": "52f5b18c-7dd3-4095-bd7e-49f6a6330391", "sha256": "f" * 64, "bytes": 1234,
                "key": "loom-nebius-management/2026/09/24/120000-" + "f" * 12 + ".dump"})
    report["backup"][field] = value
    with pytest.raises(module().GatewayError):
        module().safe_report(json.dumps(report).encode(), metadata)


@pytest.mark.parametrize("status", ["preflight_qualified", "blocked"])
def test_forced_command_runs_only_selected_action_and_exports_sanitized_report(tmp_path, monkeypatch, capsys, status):
    import sys
    gateway = module()
    content = archive(bundle(tmp_path))
    report = {key: operation(tmp_path)[key] for key in ("source_sha", "candidate", "installation_id", "namespace")}
    report.update(status=status, stage="foundation", private="must-never-transfer")
    calls = []
    monkeypatch.setenv("SSH_ORIGINAL_COMMAND", "loom-nebius-management-preflight-v1")
    monkeypatch.setattr(sys, "stdin", SimpleNamespace(buffer=io.BytesIO(content)))
    monkeypatch.setattr(gateway, "prepare_release", lambda content: tmp_path / "release")
    monkeypatch.setattr(gateway, "run_private", lambda args, **kwargs: calls.append(args) or json.dumps(report).encode())
    assert gateway.authorized_main(hashlib.sha256(content).hexdigest()) == 0
    assert calls[0][-1] == "preflight"
    assert "must-never-transfer" not in capsys.readouterr().out


@pytest.mark.parametrize("stage", ["private-token", "", None, ["foundation"], {"secret": "value"}])
def test_blocked_report_rejects_unqualified_diagnostic(tmp_path, stage):
    metadata = operation(tmp_path)
    report = {key: metadata[key] for key in ("source_sha", "candidate", "installation_id", "namespace")}
    report.update(status="blocked", stage=stage)
    with pytest.raises(module().GatewayError):
        module().safe_report(json.dumps(report).encode(), metadata)


@pytest.mark.parametrize(('detail', 'valid'), [
    ('tls_api', True), ('tls_kubelet', True), ('tls_api_verify_20', True),
    ('tls_kubelet_verify_64', True), ('tls_unknown_verify_10', True),
    ('tls_unknown_verify_0', True), ('tls_api_verify_255', True),
    ('tls_kubelet_verify_256', False), ('tls_api_verify_-1', False),
    ('tls_unknown_verify_True', False), ('tls_api_verify_20_private', False),
])
def test_pool_tls_report_preserves_only_bounded_transport_and_verification_code(tmp_path, detail, valid):
    gateway = module()
    metadata = pool_operation(tmp_path)
    report = {key: metadata[key] for key in ('source_sha', 'candidate', 'installation_id', 'namespace', 'operation_id')}
    report.update(status='blocked', stage='pool_runtime_telemetry_' + detail)
    if valid:
        assert gateway.safe_report(json.dumps(report | {'error': 'private-certificate-and-token'}).encode(), metadata) == report
    else:
        with pytest.raises(gateway.GatewayError):
            gateway.safe_report(json.dumps(report).encode(), metadata)


def test_upgrade_bundle_uses_separate_private_recovery_and_fixed_existing_actions(tmp_path, monkeypatch):
    metadata = upgrade_operation(tmp_path)
    (tmp_path / 'nebius-management').mkdir(mode=0o700)
    files = bundle(tmp_path)
    files['operation.json'] = json.dumps(metadata).encode()
    calls = []
    monkeypatch.setattr(module(), 'run_private', lambda args, **kwargs: calls.append(args) or b'')
    release = module().prepare_release(archive(files))
    assert release.parent.parent == tmp_path / 'nebius-management/upgrade'
    assert json.loads((release / 'operation.json').read_bytes()) == metadata
    assert not (tmp_path / 'nebius-management/state').exists()
    assert module().command(release, 'install')[-1] == 'install'
    assert len(calls) == 4


@pytest.mark.parametrize('change', ['old_state', 'old_anchor', 'old_inputs', 'legacy_schema', 'unknown_schema'])
def test_upgrade_cannot_reuse_bootstrap_paths_or_implicit_schema(tmp_path, change):
    metadata = upgrade_operation(tmp_path)
    if change.startswith('old_'):
        field = {'old_state': 'state_dir', 'old_anchor': 'anchor_dir', 'old_inputs': 'inputs_path'}[change]
        metadata[field] = operation(tmp_path)[field]
    else:
        metadata['schema'] = ('loom.nebius-management-operation.v1' if change == 'legacy_schema' else 'unknown')
    with pytest.raises(module().GatewayError):
        module().validate_operation(metadata)


@pytest.mark.parametrize('status,phase', [('management_upgraded', None),
    *[('pending', phase) for phase in ('admission', 'authority', 'database', 'retirement', 'retire', 'migration', 'activate', 'service')]])
def test_upgrade_report_preserves_fixed_progress_without_new_backup_or_private_data(tmp_path, status, phase):
    metadata = upgrade_operation(tmp_path)
    report = {key: metadata[key] for key in ('source_sha', 'candidate', 'installation_id', 'namespace')}
    report.update(status=status, phase=phase, namespace_uid='52f5b18c-7dd3-4095-bd7e-49f6a6330391',
        revision='sha256:' + 'd' * 64, material='private-value')
    safe = module().safe_report(json.dumps(report).encode(), metadata)
    assert safe['status'] == status and 'private-value' not in json.dumps(safe) and 'backup' not in safe
    if phase is not None:
        assert safe['phase'] == phase
    legacy = operation(tmp_path)
    if status == 'management_upgraded' or phase not in {'database', 'migration', 'service'}:
        with pytest.raises(module().GatewayError):
            module().safe_report(json.dumps(report).encode(), legacy)


def test_upgrade_cannot_report_bootstrap_success_or_unqualified_phase(tmp_path):
    metadata = upgrade_operation(tmp_path)
    report = {key: metadata[key] for key in ('source_sha', 'candidate', 'installation_id', 'namespace')}
    report.update(namespace_uid='52f5b18c-7dd3-4095-bd7e-49f6a6330391', revision='sha256:' + 'd' * 64)
    for extra in ({'status': 'management_installed'}, {'status': 'pending', 'phase': 'backup'},
                  {'status': 'pending', 'phase': 'private-value'}):
        with pytest.raises(module().GatewayError):
            module().safe_report(json.dumps(report | extra).encode(), metadata)


@pytest.mark.parametrize('stage', ['shared_material', 'upgrade_material', 'upgrade_retire', 'upgrade_activate'])
def test_upgrade_failure_keeps_only_fixed_stage_for_recovery(tmp_path, stage):
    metadata = upgrade_operation(tmp_path)
    report = {key: metadata[key] for key in ('source_sha', 'candidate', 'installation_id', 'namespace')}
    report.update(status='blocked', stage=stage, private='never-export')
    safe = module().safe_report(json.dumps(report).encode(), metadata)
    assert safe['stage'] == stage and 'never-export' not in json.dumps(safe)
