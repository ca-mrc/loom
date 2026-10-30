"""Actions builds only integrated tooling and sends no private installation inputs."""
from __future__ import annotations

import importlib
import io
import json
import subprocess
import zipfile
from pathlib import Path

import pytest
import yaml
from tests.ops.test_nebius_ingress_bootstrap import archive
from tests.ops.test_nebius_management_gateway import (
    bundle,
    diagnostic_operation,
    operation,
    pool_operation,
    recovery_operation,
    recovery_report,
    refresh_operation,
    startup_report,
    upgrade_operation,
)
from tests.ops.test_nebius_pool_repair_authority import repair_operation

from loom.db.schema_startup import service_schema_head


def module():
    return importlib.import_module("scripts.ops.nebius_management_rollout")


def image_repair_operation(tmp_path):
    return repair_operation(tmp_path, 'v2')


def runtime_image_repair_operation(tmp_path):
    return repair_operation(tmp_path, 'v3')


def image_tooling_operation(tmp_path):
    return repair_operation(tmp_path, 'v4')


@pytest.mark.parametrize('action,metadata_factory', [
    ('preflight', operation), ('install', operation), ('rollback', image_tooling_operation),
])
def test_preparation_failure_survives_gateway_transport_without_retry_or_private_output(
        tmp_path, monkeypatch, action, metadata_factory):
    import hashlib
    import sys
    from contextlib import redirect_stderr, redirect_stdout
    from types import SimpleNamespace

    from scripts.ops import nebius_management_gateway as gateway

    metadata = metadata_factory(tmp_path)
    files = bundle(tmp_path)
    files['operation.json'] = json.dumps(metadata).encode()
    if metadata_factory is image_tooling_operation:
        files[gateway.MANAGER_SCHEMA_PROOF] = json.dumps({
            'schema': 'loom.nebius-manager-schema.v1', 'source_sha': metadata['source_sha'],
            'revision': '0174'}, sort_keys=True).encode()
    content = archive(files)
    Path(metadata['inputs_path']).parent.mkdir(mode=0o700, parents=True)
    calls = []

    def private(args, **kwargs):
        calls.append(args)
        if args[1:3] == ['pip', 'sync']:
            raise RuntimeError('private dependency output with credential and path')
        assert args[1] == 'venv', 'failed preparation must not dispatch the operation'
        return b''

    def ssh(args, **kwargs):
        monkeypatch.setenv('SSH_ORIGINAL_COMMAND', args[-1])
        monkeypatch.setattr(sys, 'stdin', SimpleNamespace(buffer=io.BytesIO(kwargs['input'])))
        output, error = io.StringIO(), io.StringIO()
        with redirect_stdout(output), redirect_stderr(error):
            code = gateway.authorized_main(hashlib.sha256(content).hexdigest())
        return subprocess.CompletedProcess(args, code, output.getvalue().encode(), error.getvalue().encode())

    monkeypatch.setattr(gateway, 'run_private', private)
    monkeypatch.setattr(subprocess, 'run', ssh)
    args = {'action': action, 'target': 'codex@host', 'key': Path('/private/key'),
            'known_hosts': Path('/private/hosts')}
    result = module().transfer(content, **args)
    expected = {key: metadata[key] for key in ('source_sha', 'candidate', 'installation_id', 'namespace')}
    if action == 'rollback':
        expected.update(operation_id=metadata['operation_id'], original_operation_id=metadata['original_operation_id'])
    assert result == {**expected, 'status': 'blocked', 'stage': 'tooling_dependency_sync'}
    assert len(calls) == 2
    release = Path(metadata['state_dir']).parent / 'releases' / hashlib.sha256(content).hexdigest()
    before = {path.relative_to(release): path.read_bytes() for path in release.rglob('*') if path.is_file()}
    assert not (release / 'complete').exists() and not Path(metadata['state_dir']).exists()
    assert module().transfer(content, **args) == {**expected, 'status': 'blocked', 'stage': 'tooling_retained_incomplete'}
    assert len(calls) == 2
    assert {path.relative_to(release): path.read_bytes() for path in release.rglob('*') if path.is_file()} == before


@pytest.mark.parametrize('version', ['v2', 'v3', 'v4'])
def test_image_repair_bundle_binds_schema_head_from_source_and_rejects_wrong_source(tmp_path, version):
    import hashlib

    from scripts.ops.nebius_management_gateway import GatewayError, unpack_bundle

    uv, requirements, wheels = tmp_path / 'uv', tmp_path / 'requirements', tmp_path / 'wheels'
    uv.write_bytes(b'fixture uv')
    requirements.write_bytes(b'fixture requirements')
    wheels.mkdir()
    for name in ('loom-0.0.0-py3-none-any.whl', 'loom_bundle_checksum-0.1.0-py3-none-any.whl'):
        (wheels / name).write_bytes(b'fixture wheel')
    operation = repair_operation(tmp_path, version)
    content = module().build_bundle(operation, uv=uv, requirements=requirements, wheels=wheels)
    files, selected = unpack_bundle(content)
    assert selected == operation
    assert json.loads(files['manager-schema.json']) == {
        'schema': 'loom.nebius-manager-schema.v1', 'source_sha': operation['source_sha'], 'revision': service_schema_head()}
    proof = json.loads(files['manager-schema.json'])
    proof['source_sha'] = 'f' * 40
    files['manager-schema.json'] = json.dumps(proof, sort_keys=True).encode()
    files['manifest.json'] = json.dumps({name: hashlib.sha256(value).hexdigest()
        for name, value in files.items() if name != 'manifest.json'}, sort_keys=True).encode()
    with pytest.raises(GatewayError):
        unpack_bundle(archive(files))


def test_bundle_is_reproducible_complete_and_excludes_private_inputs(tmp_path):
    uv, requirements, wheels = tmp_path / "uv", tmp_path / "requirements.txt", tmp_path / "wheels"
    uv.write_bytes(b"approved uv")
    requirements.write_bytes(b"approved dependencies")
    wheels.mkdir()
    for name in ("loom-0.0.0-py3-none-any.whl", "loom_bundle_checksum-0.1.0-py3-none-any.whl"):
        (wheels / name).write_bytes(b"wheel fixture")
    metadata = operation(tmp_path)
    content = module().build_bundle(metadata, uv=uv, requirements=requirements, wheels=wheels)
    assert content == module().build_bundle(metadata, uv=uv, requirements=requirements, wheels=wheels)
    with zipfile.ZipFile(io.BytesIO(content)) as result:
        assert "inputs.json" not in result.namelist()
        assert json.loads(result.read("operation.json")) == metadata
        assert result.read("scripts/ops/nebius_management_entry.py") == (
            Path(__file__).resolve().parents[2] / "scripts/ops/nebius_management_entry.py").read_bytes()
        assert len([name for name in result.namelist() if name.endswith(".whl")]) == 2


@pytest.mark.parametrize("metadata_factory", [upgrade_operation, diagnostic_operation, recovery_operation, refresh_operation, pool_operation])
def test_bundled_upgrade_entry_imports_without_workspace_scripts_or_private_inputs(tmp_path, metadata_factory):
    import os
    import sys

    uv, requirements, wheels = tmp_path / 'uv', tmp_path / 'requirements', tmp_path / 'wheels'
    uv.write_bytes(b'fixture uv')
    requirements.write_bytes(b'fixture requirements')
    wheels.mkdir()
    for name in ('loom-0.0.0-py3-none-any.whl', 'loom_bundle_checksum-0.1.0-py3-none-any.whl'):
        (wheels / name).write_bytes(b'fixture wheel')
    metadata = metadata_factory(tmp_path)
    content = module().build_bundle(metadata, uv=uv, requirements=requirements, wheels=wheels)
    release = tmp_path / 'isolated'
    with zipfile.ZipFile(io.BytesIO(content)) as archive:
        for name in archive.namelist():
            path = release / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(archive.read(name))
            path.chmod(0o600)
    script = 'from scripts.ops.nebius_management_entry import main; raise SystemExit(main("' + str(release / 'operation.json') + '", "qualify"))'
    if metadata_factory is diagnostic_operation:
        script = 'from scripts.ops.nebius_management_retirement_diagnostic_live import HTTPSRetirementDiagnosticAPI; ' + script
    elif metadata_factory is recovery_operation:
        script = ('from scripts.ops.nebius_management_retirement_recovery_live import HTTPSRetirementRecoveryAPI; '
                  'from scripts.ops.nebius_retirement_recovery_runner import run_recovery; ') + script
    elif metadata_factory is refresh_operation:
        script = ('from scripts.ops.nebius_management_refresh_entry import load_refresh_inputs; '
                  'from scripts.ops.nebius_management_refresh_connected import HTTPSManagementRefreshInstaller; ') + script
    elif metadata_factory is pool_operation:
        script = 'from scripts.ops.nebius_pool_cutover_entry import execute_pool_cutover; ' + script
    result = subprocess.run([sys.executable, '-c', script], cwd=release, capture_output=True, text=True,
        timeout=30, env={**os.environ, 'PYTHONPATH': str(Path(__file__).resolve().parents[2] / 'src')})
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {'status': 'tooling_qualified'}
    assert not Path(metadata['inputs_path']).exists()


@pytest.mark.parametrize('metadata_factory,missing', [
    (refresh_operation, missing) for missing in (None, 'nebius_management_refresh_connected', 'nebius_pool_cutover_entry')
] + [
    (repair_operation, missing) for missing in (None, 'nebius_pool_repair_entry', 'nebius_pool_startup_repair_live')
] + [
    (image_repair_operation, missing) for missing in (None, 'nebius_pool_image_entry', 'nebius_pool_manager_image_live')
]+[
    (runtime_image_repair_operation, missing) for missing in (None, 'nebius_pool_image_entry', 'nebius_pool_runtime_image')
]+[
    (image_tooling_operation, missing) for missing in (None, 'nebius_pool_image_entry', 'nebius_pool_runtime_image')
])
def test_actual_tooling_qualification_loads_refresh_dependencies_without_private_inputs(tmp_path, metadata_factory, missing):
    """A qualified bundle must include the entry's deferred pool dependencies."""
    import os
    import sys

    from scripts.ops.nebius_management_gateway import command

    uv, requirements, wheels = tmp_path / 'uv', tmp_path / 'requirements', tmp_path / 'wheels'
    uv.write_bytes(b'fixture uv')
    requirements.write_bytes(b'fixture requirements')
    wheels.mkdir()
    for name in ('loom-0.0.0-py3-none-any.whl', 'loom_bundle_checksum-0.1.0-py3-none-any.whl'):
        (wheels / name).write_bytes(b'fixture wheel')
    metadata = metadata_factory(tmp_path)
    content = module().build_bundle(metadata, uv=uv, requirements=requirements, wheels=wheels)
    release = tmp_path / 'isolated'
    with zipfile.ZipFile(io.BytesIO(content)) as packed:
        for name in packed.namelist():
            path = release / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(packed.read(name))
            path.chmod(0o600)
    if missing is not None:
        (release / 'scripts/ops' / (missing + '.py')).unlink(missing_ok=True)
    args = command(release, 'qualify')
    # Use the real fixed isolated, bytecode-free invocation. Only the interpreter
    # is supplied by this test; installed-wheel qualification has its own cluster-lane test.
    args[0] = sys.executable
    result = subprocess.run(args, cwd=release, capture_output=True, text=True,
        timeout=30, env={**os.environ, 'PYTHONPATH': '/must-not-use-ambient-imports'})
    if missing is None:
        assert result.returncode == 0, result.stderr
        assert json.loads(result.stdout) == {'status': 'tooling_qualified'}
    else:
        assert result.returncode != 0, 'incomplete protected tooling was qualified'
        assert 'tooling_qualified' not in result.stdout
    assert not list(release.rglob('*.pyc')), 'immutable tooling must not accumulate bytecode caches'
    assert not Path(metadata['inputs_path']).exists()


@pytest.mark.parametrize("action,status", [("preflight", "preflight_qualified"), ("install", "pending"),
    ("install", "management_installed"), ("preflight", "blocked"), ("install", "blocked")])
def test_exact_operation_transports_only_bundle_and_strips_private_reports(tmp_path, monkeypatch, action, status):
    content, metadata = archive(bundle(tmp_path)), operation(tmp_path)
    report = {key: metadata[key] for key in ("source_sha", "candidate", "installation_id", "namespace")}
    report.update(status=status, stage="cluster_identity", private="never-transfer", phase="database", revision="sha256:" + "d" * 64,
                  namespace_uid="52f5b18c-7dd3-4095-bd7e-49f6a6330391")
    if status == "management_installed":
        report["backup"] = {"job_uid": "52f5b18c-7dd3-4095-bd7e-49f6a6330391", "sha256": "f" * 64, "bytes": 1234,
            "key": "loom-nebius-management/2026/09/24/120000-" + "f" * 12 + ".dump"}
    calls = []
    def run(args, **kwargs):
        calls.append(args)
        assert args[-1] == "loom-nebius-management-" + action + "-v1"
        assert args[-2] == "codex@192.0.2.1"
        assert "StrictHostKeyChecking=yes" in args and "IdentitiesOnly=yes" in args
        assert kwargs["input"] == content
        return subprocess.CompletedProcess(args, 0, json.dumps(report).encode(), b"private logs")
    monkeypatch.setattr(subprocess, "run", run)
    result = module().transfer(content, action=action, target="codex@192.0.2.1", key=Path("/private/key"),
                               known_hosts=Path("/private/hosts"))
    assert result["status"] == status and "never-transfer" not in json.dumps(result) and len(calls) == 1


@pytest.mark.parametrize('status', ['blocked', 'management_upgraded', 'retirement_recovered', 'management_refreshed'])
def test_rollout_records_bound_result_and_distinguishes_failure(tmp_path, monkeypatch, capsys, status):
    import sys
    target = module()
    metadata = (refresh_operation(tmp_path) if status == 'management_refreshed' else recovery_operation(tmp_path) if status == 'retirement_recovered'
        else upgrade_operation(tmp_path) if status == 'management_upgraded' else operation(tmp_path))
    report = {"status": status, "stage": "storage_class",
              **{key: metadata[key] for key in ("source_sha", "candidate", "installation_id", "namespace")}}
    if status in {'management_upgraded', 'retirement_recovered', 'management_refreshed'}:
        report.pop('stage')
        report.update(namespace_uid='52f5b18c-7dd3-4095-bd7e-49f6a6330391', revision='sha256:' + 'd' * 64)
    if status == 'retirement_recovered':
        report['recovery'] = recovery_report()
    if status == 'management_refreshed':
        report['operation_id'] = metadata['operation_id']
    monkeypatch.setenv("NEBIUS_MANAGEMENT_OPERATION_JSON", json.dumps(metadata))
    monkeypatch.setenv("LOOM_DEPLOY_SSH_TARGET", "codex@host")
    monkeypatch.setenv("LOOM_DEPLOY_SSH_KEY_FILE", "/private/key")
    monkeypatch.setenv("LOOM_DEPLOY_SSH_KNOWN_HOSTS_FILE", "/private/hosts")
    monkeypatch.setattr(target, "verify_source", lambda config: None)
    monkeypatch.setattr(target.shutil, "which", lambda name: "/usr/bin/uv")
    monkeypatch.setattr(target.subprocess, "run", lambda *args, **kwargs:
        subprocess.CompletedProcess(args, 0, b"uv 0.11.26 (x86_64-unknown-linux-gnu)\n", b""))
    monkeypatch.setattr(target, "build_wheels", lambda *args, **kwargs: tmp_path)
    monkeypatch.setattr(target, "build_bundle", lambda *args, **kwargs: b"qualified bundle")
    monkeypatch.setattr(target, "transfer", lambda *args, **kwargs: report)
    evidence = tmp_path / "evidence"
    monkeypatch.setattr(sys, "argv", ["rollout", "--operation", "install", "--requirements", str(tmp_path / "requirements"),
                                    "--evidence-dir", str(evidence)])
    assert target.main() == (1 if status == 'blocked' else 0)
    assert json.loads(capsys.readouterr().out) == report
    assert json.loads((evidence / "management-result.json").read_bytes()) == report


@pytest.mark.parametrize("outcome", ["observed", "unavailable", "blocked", "invalid"])
def test_diagnostic_rollout_exit_reports_delivery_not_retirement(tmp_path, monkeypatch, capsys, outcome):
    """A delivered observation must not become a false failed workflow or cleanup success."""
    import sys

    target = module()
    metadata = diagnostic_operation(tmp_path)
    probe = startup_report()
    if outcome == "unavailable":
        probe.update(status="unavailable", stage="kubernetes_get", error_type="HTTPStatusError", http_status=403)
        probe["checks"].remove("kubernetes")
    report = {key: metadata[key] for key in ("source_sha", "candidate", "installation_id", "namespace")}
    if outcome == "blocked":
        report.update(status="blocked", stage="diagnostic_readback")
    else:
        report.update(status="management_retired" if outcome == "invalid" else "retirement_diagnostic_observed",
            namespace_uid="52f5b18c-7dd3-4095-bd7e-49f6a6330391", revision="sha256:" + "d" * 64, probe=probe)
    uv, requirements, wheels = tmp_path / "uv", tmp_path / "requirements", tmp_path / "wheels"
    uv.write_bytes(b"fixture uv")
    requirements.write_bytes(b"fixture requirements")
    wheels.mkdir()
    for name in ("loom-0.0.0-py3-none-any.whl", "loom_bundle_checksum-0.1.0-py3-none-any.whl"):
        (wheels / name).write_bytes(b"fixture wheel")
    monkeypatch.setenv("NEBIUS_MANAGEMENT_OPERATION_JSON", json.dumps(metadata))
    monkeypatch.setenv("LOOM_DEPLOY_SSH_TARGET", "codex@host")
    monkeypatch.setenv("LOOM_DEPLOY_SSH_KEY_FILE", "/private/key")
    monkeypatch.setenv("LOOM_DEPLOY_SSH_KNOWN_HOSTS_FILE", "/private/hosts")
    monkeypatch.setattr(target, "verify_source", lambda config: None)
    monkeypatch.setattr(target.shutil, "which", lambda name: str(uv))
    monkeypatch.setattr(target, "build_wheels", lambda *args, **kwargs: wheels)
    calls = []

    def run(args, **kwargs):
        if args == [str(uv), "--version"]:
            return subprocess.CompletedProcess(args, 0, b"uv 0.11.26 (x86_64-unknown-linux-gnu)\n", b"")
        assert args[0] == "ssh" and args[-1] == "loom-nebius-management-install-v1"
        _, sent = target.unpack_bundle(kwargs["input"])
        assert sent == metadata
        calls.append(args)
        return subprocess.CompletedProcess(args, 0, json.dumps(report).encode(), b"private stderr")

    monkeypatch.setattr(target.subprocess, "run", run)
    evidence = tmp_path / "evidence"
    monkeypatch.setattr(sys, "argv", ["rollout", "--operation", "install", "--requirements", str(requirements),
                                    "--evidence-dir", str(evidence)])
    assert target.main() == (1 if outcome in {"blocked", "invalid"} else 0)
    expected = {"status": "blocked", "phase": "gateway_operation"} if outcome == "invalid" else report
    assert json.loads(capsys.readouterr().out) == expected
    assert json.loads((evidence / "management-result.json").read_bytes()) == expected
    assert len(calls) == 1


@pytest.mark.parametrize("case", ["failure", "timeout", "wrong_action", "other_candidate"])
def test_unknown_or_misbound_result_never_retries_or_leaks(tmp_path, monkeypatch, case):
    content, metadata = archive(bundle(tmp_path)), operation(tmp_path)
    calls = []
    def run(args, **kwargs):
        calls.append(args)
        if case == "timeout":
            raise subprocess.TimeoutExpired(args, 10, output=b"private data")
        report = {key: metadata[key] for key in ("source_sha", "candidate", "installation_id", "namespace")}
        report["status"] = "preflight_qualified"
        if case == "other_candidate":
            report["candidate"] = "0" * 40
        return subprocess.CompletedProcess(args, 1 if case == "failure" else 0, json.dumps(report).encode(), b"private data")
    monkeypatch.setattr(subprocess, "run", run)
    with pytest.raises(module().RolloutError) as error:
        module().transfer(content, action="install", target="codex@host", key=Path("/private/key"), known_hosts=Path("/private/hosts"))
    assert len(calls) == 1 and "private data" not in str(error.value)


@pytest.mark.parametrize('case', ['legacy', 'global', 'foreign_schema'])
def test_explicit_pool_rollback_cannot_dispatch_other_schema_or_claim_global_success(tmp_path, monkeypatch, case):
    metadata = operation(tmp_path) if case == 'foreign_schema' else pool_operation(tmp_path)
    files = bundle(tmp_path) | {'operation.json': json.dumps(metadata).encode()}
    content = archive(files)
    calls = []

    def run(args, **kwargs):
        calls.append(args)
        assert args[-1] == 'loom-nebius-pool-rollback-v1'
        report = {key: metadata[key] for key in ('source_sha', 'candidate', 'installation_id', 'namespace', 'operation_id')}
        report.update(status='pool_cutover_completed', outcome=case, completion_sha256='a' * 64, acceptance_verified=False)
        return subprocess.CompletedProcess(args, 0, json.dumps(report).encode(), b'private-marker')

    monkeypatch.setattr(subprocess, 'run', run)
    args = {'action': 'rollback', 'target': 'codex@host', 'key': Path('/private/key'), 'known_hosts': Path('/private/hosts')}
    if case == 'legacy':
        assert module().transfer(content, **args)['outcome'] == 'legacy'
    else:
        with pytest.raises(module().RolloutError):
            module().transfer(content, **args)
    assert len(calls) == (0 if case == 'foreign_schema' else 1)


@pytest.mark.parametrize("case", ["clean", "dirty", "different_head", "not_integrated"])
def test_source_must_be_exact_clean_and_integrated(tmp_path, monkeypatch, case):
    metadata = operation(tmp_path)
    def run(args, **kwargs):
        if args[1] == "rev-parse":
            result = b"0" * 40 if case == "different_head" else b"a" * 40
        elif args[1] == "status":
            result = b" M source.py" if case == "dirty" else b""
        else:
            assert args == ["git", "merge-base", "--is-ancestor", "a" * 40, "refs/remotes/origin/dev"]
            return subprocess.CompletedProcess(args, int(case == "not_integrated"), b"", b"")
        return subprocess.CompletedProcess(args, 0, result, b"")
    monkeypatch.setattr(subprocess, "run", run)
    if case == "clean":
        module().verify_source(metadata)
    else:
        with pytest.raises(module().RolloutError):
            module().verify_source(metadata)


def test_management_workflow_uses_protected_environment_and_separate_fixed_authority():
    path = Path(__file__).resolve().parents[2] / ".github/workflows/nebius-rollout.yml"
    workflow = yaml.safe_load(path.read_text())
    dispatch = workflow.get("on", workflow.get(True))["workflow_dispatch"]["inputs"]["operation"]["options"]
    assert {"management-preflight", "management-install"} <= set(dispatch)
    job = workflow["jobs"]["management"]
    assert job["environment"]["name"] == "nebius-integration" and job["permissions"] == {"contents": "read"}
    assert "workflow_dispatch" in job["if"] and "refs/heads/dev" in job["if"]
    assert workflow["concurrency"]["cancel-in-progress"] is False
    run = next(step for step in job["steps"] if step.get("name") == "Run fixed management operation")
    assert run["env"]["DEPLOY_SSH_KEY"] == "${{ secrets.NEBIUS_MANAGEMENT_SSH_KEY }}"
    assert not any("SERVICE_ACCOUNT" in name for name in run["env"])


@pytest.mark.parametrize('authority,action,version', [
    (authority, action, 'v1') for authority in ('initial', 'diagnostic', 'recovery', 'refresh', 'pool', 'pool-repair')
    for action in ('preflight', 'install')
] + [('pool', 'rollback', 'v1'), ('pool-repair', 'rollback', 'v1')] + [
    ('pool-repair', action, version) for version in ('v2', 'v3', 'v4')
    for action in ('preflight', 'install', 'rollback')])
def test_workflow_selects_exact_metadata_and_key_without_cross_fallback(tmp_path, authority, action, version):
    import os

    root = Path(__file__).resolve().parents[2]
    workflow = yaml.safe_load((root / ".github/workflows/nebius-rollout.yml").read_text())
    operation_name = "management-" + (authority + "-" if authority != "initial" else "") + action
    choices = workflow.get("on", workflow.get(True))["workflow_dispatch"]["inputs"]["operation"]["options"]
    assert operation_name in choices
    job = workflow["jobs"]["management"]
    assert "management-diagnostic-preflight" in job["if"] and "management-diagnostic-install" in job["if"]
    steps = job["steps"]
    selector = next(s for s in steps if s.get("id") == "tooling")
    runner = next(s for s in steps if s.get("name") == "Run fixed management operation")
    assert selector["env"]["NEBIUS_MANAGEMENT_DIAGNOSTIC_OPERATION_JSON"] == "${{ vars.NEBIUS_MANAGEMENT_DIAGNOSTIC_OPERATION_JSON }}"
    assert runner["env"]["DIAGNOSTIC_SSH_KEY"] == "${{ secrets.NEBIUS_MANAGEMENT_DIAGNOSTIC_SSH_KEY }}"
    if authority == "recovery":
        assert selector["env"]["NEBIUS_MANAGEMENT_RECOVERY_OPERATION_JSON"] == "${{ vars.NEBIUS_MANAGEMENT_RECOVERY_OPERATION_JSON }}"
        assert runner["env"]["RECOVERY_SSH_KEY"] == "${{ secrets.NEBIUS_MANAGEMENT_RECOVERY_SSH_KEY }}"
    if authority == 'refresh':
        assert selector['env']['NEBIUS_MANAGEMENT_REFRESH_OPERATION_JSON'] == '${{ vars.NEBIUS_MANAGEMENT_REFRESH_OPERATION_JSON }}'
        assert runner['env']['REFRESH_SSH_KEY'] == '${{ secrets.NEBIUS_MANAGEMENT_REFRESH_SSH_KEY }}'
    if authority == 'pool':
        assert selector['env']['NEBIUS_MANAGEMENT_POOL_OPERATION_JSON'] == '${{ vars.NEBIUS_MANAGEMENT_POOL_OPERATION_JSON }}'
        assert runner['env']['POOL_SSH_KEY'] == '${{ secrets.NEBIUS_MANAGEMENT_POOL_SSH_KEY }}'
    if authority == 'pool-repair':
        assert selector['env']['NEBIUS_MANAGEMENT_POOL_REPAIR_OPERATION_JSON'] == '${{ vars.NEBIUS_MANAGEMENT_POOL_REPAIR_OPERATION_JSON }}'
        assert runner['env']['POOL_REPAIR_SSH_KEY'] == '${{ secrets.NEBIUS_MANAGEMENT_POOL_REPAIR_SSH_KEY }}'
    original, probe = operation(tmp_path), diagnostic_operation(tmp_path)
    probe["source_sha"] = "d" * 40
    recovery = recovery_operation(tmp_path) | {"source_sha": "e" * 40}
    refresh = refresh_operation(tmp_path)
    pool = pool_operation(tmp_path)
    repair = repair_operation(tmp_path, version)
    selected = {"initial": original, "diagnostic": probe, "recovery": recovery, 'refresh': refresh, 'pool': pool, 'pool-repair': repair}[authority]
    bindir = tmp_path / "bin"
    bindir.mkdir()
    # Fake only external executables; run the actual checked-in shell/Python
    # selection and credential-file lifetime. Nothing contacts SSH or Kubernetes.
    git = bindir / "git"
    git.write_text('#!/bin/sh\ntest "$1 $2 $3" = "merge-base --is-ancestor $EXPECTED_SHA"\n')
    git.chmod(0o700)
    uv = bindir / "uv"
    uv.write_text('''#!/usr/bin/env python3
import json, os, pathlib, sys
assert sys.argv[sys.argv.index('--operation') + 1] == os.environ['EXPECTED_ACTION']
assert json.loads(os.environ['NEBIUS_MANAGEMENT_OPERATION_JSON']) == json.loads(os.environ['EXPECTED_METADATA'])
assert pathlib.Path(os.environ['LOOM_DEPLOY_SSH_KEY_FILE']).read_text().strip() == os.environ['EXPECTED_KEY']
pathlib.Path(os.environ['RUNNER_TEMP'], 'transport-invoked').write_text('selected')
''')
    uv.chmod(0o700)
    env = os.environ | {"PATH": str(bindir) + ":" + os.environ["PATH"], "MANAGEMENT_OPERATION": operation_name,
        "NEBIUS_MANAGEMENT_OPERATION_JSON": json.dumps(original), "NEBIUS_MANAGEMENT_DIAGNOSTIC_OPERATION_JSON": json.dumps(probe),
        "NEBIUS_MANAGEMENT_RECOVERY_OPERATION_JSON": json.dumps(recovery), "RECOVERY_SSH_KEY": "private-recovery-key",
        'NEBIUS_MANAGEMENT_REFRESH_OPERATION_JSON': json.dumps(refresh), 'REFRESH_SSH_KEY': 'private-refresh-key',
        'NEBIUS_MANAGEMENT_POOL_OPERATION_JSON': json.dumps(pool), 'POOL_SSH_KEY': 'private-pool-key',
        'NEBIUS_MANAGEMENT_POOL_REPAIR_OPERATION_JSON': json.dumps(repair), 'POOL_REPAIR_SSH_KEY': 'private-pool-repair-key',
        "DEPLOY_SSH_KEY": "private-original-key", "DIAGNOSTIC_SSH_KEY": "private-diagnostic-key",
        "DEPLOY_KNOWN_HOSTS": "fixture-host", "LOOM_DEPLOY_SSH_TARGET": "fixture-target", "RUNNER_TEMP": str(tmp_path),
        "GITHUB_OUTPUT": str(tmp_path / "outputs"), "EXPECTED_SHA": selected["source_sha"], "EXPECTED_ACTION": action,
        "EXPECTED_METADATA": json.dumps(selected), "EXPECTED_KEY": "private-" + ("original" if authority == "initial" else authority) + "-key"}
    for step in (selector, runner):
        result = subprocess.run(["bash", "-e", "-c", step["run"]], env=env, cwd=root, capture_output=True, text=True, timeout=20)
        assert result.returncode == 0, result.stderr
        assert "private-" not in result.stdout + result.stderr
    assert (tmp_path / "outputs").read_text() == "sha=" + selected["source_sha"] + "\n"
    assert (tmp_path / "transport-invoked").read_text() == "selected"
    assert not (tmp_path / "nebius-management-key").exists()
    if authority != "initial":
        (tmp_path / "transport-invoked").unlink()
        # Missing diagnostic key must fail, never use the original install key.
        result = subprocess.run(["bash", "-e", "-c", runner["run"]], env=env | {authority.upper().replace('-', '_') + "_SSH_KEY": ""},
            cwd=root, capture_output=True, text=True, timeout=20)
        assert result.returncode != 0 and not (tmp_path / "transport-invoked").exists()
        # A diagnostic action cannot select an original retirement/install schema.
        result = subprocess.run(["bash", "-e", "-c", selector["run"]],
            env=env | {"NEBIUS_MANAGEMENT_" + authority.upper().replace('-', '_') + "_OPERATION_JSON": json.dumps(original)},
            cwd=root, capture_output=True, text=True, timeout=20)
        assert result.returncode != 0
        result = subprocess.run(["bash", "-e", "-c", selector["run"]],
            env=env | {"MANAGEMENT_OPERATION": "management-" + action,
                       "NEBIUS_MANAGEMENT_OPERATION_JSON": json.dumps(selected)},
            cwd=root, capture_output=True, text=True, timeout=20)
        assert result.returncode != 0
