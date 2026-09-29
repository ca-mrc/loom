"""Protected management transport accepts one exact source/input authority only."""
from __future__ import annotations

import hashlib
import importlib
import io
import json
from types import SimpleNamespace

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
    assert calls[3][-1] == "qualify" and calls[3][1:3] == ["-I", "-c"]
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
