"""Dev-manager certificate delivery cannot inherit legacy wildcard authority."""
from __future__ import annotations

import hashlib
import io
import json
import os
import subprocess
import sys
import zipfile
from pathlib import Path
from types import SimpleNamespace

import pytest
from scripts.ops import install_nebius_certificate_entrypoint as installer
from scripts.ops import nebius_certificate_gateway as legacy_gateway
from scripts.ops import nebius_certificate_rollout as runner
from scripts.ops import nebius_development_management_certificate_gateway as gateway
from tests.ops.test_install_nebius_certificate_entrypoint import inputs as inputs
from tests.ops.test_nebius_rollout_skip_workflow import selected_jobs
from tests.ops.test_nebius_rollout_skip_workflow import workflow as workflow

COMMAND = "loom-nebius-development-management-certificate-v1"
HOST = "development-management.example.test"
IDENTITY = "f09f269d-3048-4cf5-af78-e30320960e4a"


@pytest.fixture
def selection(tmp_path):
    parent = tmp_path / ".loom"
    parent.mkdir(mode=0o700)
    config = {
        "schema": "loom.nebius-management-certificate-installation.v1",
        "installation_id": IDENTITY,
        "zone": "example.test",
        "management_host": HOST,
        "credential_file": str(tmp_path / "private-dns.json"),
        "state_dir": str(parent / "nebius-development-management-certificates" / "state"),
        "email": None,
    }
    uv = tmp_path / "uv"
    uv.write_bytes(b"qualified uv")
    requirements = tmp_path / "requirements.txt"
    requirements.write_text("certbot==5.8.0 --hash=sha256:fixture\n")
    return config, uv, requirements


def bundle(selection):
    config, uv, requirements = selection
    return runner.build_bundle(config, uv=uv, requirements=requirements, management_only=True)


def report():
    return {"status": "qualified", "installation_id": IDENTITY,
        "fingerprint_sha256": "a" * 64, "generation": "b" * 64,
        "expires_at": "2026-12-02T12:00:00+00:00", "sans": [HOST]}


def test_legacy_dispatch_keeps_preexisting_exact_bundle_authority(selection, monkeypatch, capsys):
    """New manager delivery must not require reinstalling the legacy SSH grant."""
    config = {"schema": "loom.nebius-certificate-installation.v1", "installation_id": IDENTITY,
        "zone": "example.test", "child_domain": "dev.example.test", "management_host": "management.example.test",
        "credential_file": "/home/codex/private-dns.json",
        "state_dir": "/home/codex/.loom/nebius-certificates/state", "email": None}
    # Captured using the actual pre-change runner at 4ddff8fc8 with these fixed
    # fixture inputs. A change to any bundled legacy code invalidates its already
    # installed exact-digest grant, even if its CLI/default behavior is unchanged.
    approved = "b2ec0aedde958e777bdd2165393b639d48418be5c6d48037854cef8df31cd183"
    content = runner.build_bundle(config, uv=selection[1], requirements=selection[2])
    issued = []

    def issue(value):
        issued.append(value)
        return {"status": "qualified"}

    monkeypatch.setattr(legacy_gateway, "qualify_bundle", issue)
    monkeypatch.setenv("SSH_ORIGINAL_COMMAND", "loom-nebius-certificate-v1")
    monkeypatch.setattr(sys, "stdin", SimpleNamespace(buffer=io.BytesIO(content)))
    assert legacy_gateway.authorized_main(approved) == 0
    assert json.loads(capsys.readouterr().out) == {"status": "qualified"}
    assert issued == [content]
    monkeypatch.setattr(sys, "stdin", SimpleNamespace(buffer=io.BytesIO(content + b"changed")))
    assert legacy_gateway.authorized_main(approved) == 126
    assert issued == [content]


def test_manager_report_accepts_only_one_exact_host_and_redacts_private_fields():
    value = report() | {"private": "secret-marker"}
    assert gateway.safe_report(json.dumps(value).encode()) == report()
    for names in (["*.example.test"], [HOST, "*.dev.example.test"], [], [7]):
        with pytest.raises(gateway.GatewayError):
            gateway.safe_report(json.dumps(value | {"sans": names}).encode())
    with pytest.raises(legacy_gateway.GatewayError):
        legacy_gateway.safe_report(json.dumps(value).encode())


def test_manager_bundle_uses_separate_immutable_root_and_existing_hash_locked_bootstrap(selection, monkeypatch):
    content = bundle(selection)
    calls = []
    monkeypatch.setattr(gateway, "run_private", lambda args, **kwargs: calls.append(args) or b"")
    release, path = gateway.prepare_release(content)
    root = Path(selection[0]["state_dir"]).parent
    assert release == root / "releases" / hashlib.sha256(content).hexdigest()
    assert json.loads(path.read_bytes()) == selection[0]
    assert len(calls) == 2 and "--require-hashes" in calls[1]
    assert gateway.prepare_release(content) == (release, path)
    assert len(calls) == 2
    assert not (root.parent / "nebius-certificates").exists()
    (release / "uv").write_bytes(b"changed")
    with pytest.raises(gateway.GatewayError):
        gateway.prepare_release(content)
    assert len(calls) == 2


def test_extracted_manager_bundle_executes_actual_issuer_client_without_checkout(selection, tmp_path):
    """Missing transitive launcher code must fail before a live issuance attempt."""
    extracted = tmp_path / "extracted"
    with zipfile.ZipFile(io.BytesIO(bundle(selection))) as archive:
        archive.extractall(extracted)
    program = """
import pathlib, subprocess, sys
root = pathlib.Path(sys.argv[1])
sys.path.insert(0, str(root))
from scripts.ops import nebius_certificates as issuer
assert pathlib.Path(issuer.__file__).resolve().is_relative_to(root)
marker = root / 'client-executed'
command = [sys.executable, '-c',
    'from pathlib import Path; import sys; Path(sys.argv[1]).write_text("executed"); print("private-client-output")',
    str(marker)]
result = issuer._run_client(command, timeout=10, start_new_session=True,
    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
assert result.returncode == 0
assert marker.read_text() == 'executed'
print('client executed')
"""
    result = subprocess.run([sys.executable, "-I", "-c", program, str(extracted)],
        cwd=tmp_path, env={"PATH": os.defpath, "LANG": "C.UTF-8"}, capture_output=True, timeout=20)
    assert result.returncode == 0, result.stderr.decode()
    assert result.stdout == b"client executed\n"
    assert b"private-client-output" not in result.stderr
    assert not Path(selection[0]["state_dir"]).exists()


@pytest.mark.parametrize("damage", ["legacy_mode", "legacy_schema", "legacy_root"])
def test_cross_scope_bundle_is_rejected_before_tooling_writes(selection, damage, monkeypatch):
    config = selection[0]
    if damage == "legacy_schema":
        config["schema"] = "loom.nebius-certificate-installation.v1"
        config["child_domain"] = "dev.example.test"
    elif damage == "legacy_root":
        config["state_dir"] = str(Path(config["state_dir"]).parents[1] / "nebius-certificates" / "state")
    monkeypatch.setattr(gateway, "run_private", lambda *a, **kw: pytest.fail("cross-scope tooling executed"))
    selected = legacy_gateway if damage == "legacy_mode" else gateway
    with pytest.raises(selected.GatewayError):
        selected.prepare_release(bundle(selection))
    assert not Path(config["state_dir"]).parent.exists()


@pytest.mark.parametrize("changed", [None, "host", "installation"])
def test_qualification_dispatches_exact_host_issuer_and_binds_returned_identity(selection, changed, monkeypatch):
    value = report()
    if changed == "host":
        value["sans"] = ["foreign.example.test"]
    elif changed == "installation":
        value["installation_id"] = "024cfbfb-a7e8-4d85-9c60-c1d838730f9a"
    issuer_calls = []

    def external(args, **kwargs):
        if args[1].endswith("nebius_certificates.py"):
            issuer_calls.append(args)
            assert args[2] == "issue-management" and kwargs["timeout"] == 1900
            return json.dumps(value).encode()
        return b""

    monkeypatch.setattr(gateway, "run_private", external)
    if changed:
        with pytest.raises(gateway.GatewayError):
            gateway.qualify_bundle(bundle(selection))
    else:
        assert gateway.qualify_bundle(bundle(selection)) == report()
    assert len(issuer_calls) == 1


@pytest.mark.parametrize("command", ["loom-nebius-certificate-v1", COMMAND + " extra", ""])
def test_manager_forced_command_rejects_legacy_or_unrecognized_command_before_stdin(command, monkeypatch):
    monkeypatch.setenv("SSH_ORIGINAL_COMMAND", command)
    monkeypatch.setattr(sys, "stdin", object())
    assert gateway.authorized_main("a" * 64) == 126


def test_manager_forced_command_rejects_unapproved_bundle(selection, monkeypatch):
    monkeypatch.setenv("SSH_ORIGINAL_COMMAND", COMMAND)
    monkeypatch.setattr(sys, "stdin", SimpleNamespace(buffer=io.BytesIO(bundle(selection))))
    assert gateway.authorized_main("a" * 64) == 126
    assert not Path(selection[0]["state_dir"]).parent.exists()


def test_installed_entrypoint_freezes_mode_and_preserves_foreign_keys(inputs):
    old_root, keys, key, _, _ = inputs
    root = old_root.with_name("nebius-development-management-certificates")
    source = b'def authorized_main(expected):\n    return 0\n'
    config = {"schema": "loom.nebius-management-certificate-installation.v1",
        "installation_id": IDENTITY, "state_dir": str(root / "state")}
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as archive:
        archive.writestr("scripts/ops/nebius_development_management_certificate_gateway.py", source)
        archive.writestr("installation.json", json.dumps(config))
    content = output.getvalue()
    digest = hashlib.sha256(content).hexdigest()
    previous = keys.read_bytes()
    installer.install(content, expected_sha256=digest, public_key=key, management_only=True)
    assert keys.read_bytes() == previous and not root.exists()
    result = installer.install(content, expected_sha256=digest, public_key=key, management_only=True, apply=True)
    after = keys.read_bytes()
    assert after.startswith(previous) and not old_root.exists()
    entry = root / "authority" / digest / "entrypoint.py"
    for command, code in ((COMMAND, 0), ("loom-nebius-certificate-v1", 126), (COMMAND + " extra", 126)):
        execution = subprocess.run([sys.executable, "-I", str(entry)], input=content, capture_output=True,
            env={"SSH_ORIGINAL_COMMAND": command}, timeout=10)
        assert execution.returncode == code
    assert installer.install(content, expected_sha256=digest, public_key=key, management_only=True, apply=True) == result
    assert keys.read_bytes() == after
    (entry.parent / "gateway.py").write_bytes(b'print("substituted")')
    denied = subprocess.run([sys.executable, "-I", str(entry)], input=content, capture_output=True,
        env={"SSH_ORIGINAL_COMMAND": COMMAND}, timeout=10)
    assert denied.returncode == 126 and b"substituted" not in denied.stdout


def test_installed_manager_entrypoint_runs_actual_isolated_gateway_payload(inputs, tmp_path):
    """Execute the bundled gateway; only uv/package install and issuance are substitutes."""
    old_root, keys, key, _, _ = inputs
    tools = tmp_path / "tooling"
    tools.mkdir()
    config, uv, requirements = selection.__wrapped__(tools)
    root = old_root.with_name("nebius-development-management-certificates")
    config["state_dir"] = str(root / "state")
    issuer = (f"#!{sys.executable}\nimport json,sys\nfrom pathlib import Path\n"
        "assert sys.argv[1].endswith('/scripts/ops/nebius_certificates.py')\n"
        "assert sys.argv[2:4] == ['issue-management', '--config']\n"
        "config=json.loads(Path(sys.argv[4]).read_text())\n"
        f"assert config['management_host'] == {HOST!r}\n"
        f"print(json.dumps({report()!r}))\n")
    uv.write_text(f"#!{sys.executable}\nimport sys\nfrom pathlib import Path\n"
        "if sys.argv[1]=='venv':\n"
        " target=Path(sys.argv[-1])/'bin'/'python'\n"
        " target.parent.mkdir(parents=True, mode=0o700)\n"
        f" target.write_text({issuer!r})\n"
        " target.chmod(0o700)\n"
        "else:\n"
        " assert sys.argv[1:3]==['pip','sync'] and '--require-hashes' in sys.argv\n")
    content = bundle((config, uv, requirements))
    checksum = hashlib.sha256(content).hexdigest()
    previous = keys.read_bytes()
    installer.install(content, expected_sha256=checksum, public_key=key, management_only=True, apply=True)
    entry = root / "authority" / checksum / "entrypoint.py"
    result = subprocess.run([sys.executable, "-I", str(entry)], input=content, capture_output=True,
        env={"SSH_ORIGINAL_COMMAND": COMMAND}, timeout=15)
    assert result.returncode == 0, result.stderr.decode()
    assert json.loads(result.stdout) == report()
    assert keys.read_bytes().startswith(previous)
    assert not old_root.exists()
    assert (root / "releases" / checksum / "complete").read_bytes() == b"complete"


@pytest.mark.parametrize("change", [None, "host", "installation", "failure"])
def test_runner_uses_dedicated_command_and_rejects_foreign_success_or_retries(selection, change, monkeypatch):
    value = report()
    if change == "host":
        value["sans"] = ["foreign.example.test"]
    elif change == "installation":
        value["installation_id"] = "024cfbfb-a7e8-4d85-9c60-c1d838730f9a"
    calls = []

    def ssh(args, **kwargs):
        calls.append(args)
        assert args[-1] == COMMAND and "StrictHostKeyChecking=yes" in args
        assert kwargs["input"] == bundle(selection)
        return subprocess.CompletedProcess(args, int(change == "failure"), json.dumps(value).encode(), b"private-marker")

    monkeypatch.setattr(subprocess, "run", ssh)
    kwargs = dict(target="codex@192.0.2.1", key=Path("/private/manager-key"),
        known_hosts=Path("/private/known-hosts"), management_only=True)
    if change:
        with pytest.raises((runner.RolloutError, gateway.GatewayError)) as error:
            runner.transfer(bundle(selection), **kwargs)
        assert "private-marker" not in str(error.value)
    else:
        assert runner.transfer(bundle(selection), **kwargs) == report()
    assert len(calls) == 1


@pytest.mark.parametrize("configured", [False, True])
def test_management_preparation_never_falls_back_to_legacy_configuration(selection, configured, tmp_path, monkeypatch, capsys):
    config, uv, requirements = selection
    output = tmp_path / "prepared.zip"
    evidence = tmp_path / "evidence"
    monkeypatch.setenv("NEBIUS_CERTIFICATE_INSTALLATION_JSON", json.dumps(config))
    monkeypatch.delenv("NEBIUS_DEVELOPMENT_MANAGEMENT_CERTIFICATE_INSTALLATION_JSON", raising=False)
    if configured:
        monkeypatch.setenv("NEBIUS_DEVELOPMENT_MANAGEMENT_CERTIFICATE_INSTALLATION_JSON", json.dumps(config))
    monkeypatch.setattr(sys, "argv", ["prepare", "--management-only", "--requirements", str(requirements),
        "--evidence-dir", str(evidence), "--prepare-bundle", str(output)])
    monkeypatch.setattr(runner.shutil, "which", lambda name: str(uv))

    def version(args, **kwargs):
        assert args == [str(uv), "--version"]
        return subprocess.CompletedProcess(args, 0, b"uv 0.11.26 (x86_64-unknown-linux-gnu)\n", b"")

    monkeypatch.setattr(subprocess, "run", version)
    assert runner.main() == (0 if configured else 1)
    result = json.loads(capsys.readouterr().out)
    assert result["status"] == ("prepared" if configured else "blocked")
    assert output.exists() == configured
    if configured:
        with zipfile.ZipFile(output) as archive:
            assert json.loads(archive.read("installation.json")) == config


@pytest.mark.parametrize("enabled", ["true", "false"])
def test_workflow_dispatch_selects_only_the_separately_protected_issuer(workflow, enabled):
    assert selected_jobs(workflow, event="workflow_dispatch", operation="development-management-certificate",
        enabled=enabled) == {"development-management-certificate"}
    job = workflow["jobs"]["development-management-certificate"]
    assert job["environment"]["name"] == "nebius-integration"
    assert job["permissions"] == {"contents": "read"}
    step = next(item for item in job["steps"] if item.get("name") == "Qualify dev-manager certificate on the private gateway")
    assert step["env"]["DEPLOY_SSH_KEY"] == "${{ secrets.NEBIUS_DEVELOPMENT_MANAGEMENT_CERTIFICATE_SSH_KEY }}"
    assert step["env"]["NEBIUS_DEVELOPMENT_MANAGEMENT_CERTIFICATE_INSTALLATION_JSON"] == (
        "${{ vars.NEBIUS_DEVELOPMENT_MANAGEMENT_CERTIFICATE_INSTALLATION_JSON }}")
    assert "NEBIUS_CERTIFICATE_INSTALLATION_JSON" not in step["env"]


@pytest.mark.parametrize("context", [{"repository_id": "999"}, {"ref": "refs/heads/feature"},
    {"event": "pull_request"}, {"operation": "certificate"}])
def test_dev_certificate_job_rejects_foreign_dispatch_without_changing_legacy(workflow, context):
    args = {"event": "workflow_dispatch", "operation": "development-management-certificate"} | context
    assert "development-management-certificate" not in selected_jobs(workflow, **args)
    assert selected_jobs(workflow, event="workflow_dispatch", operation="certificate") == {"certificate"}


@pytest.mark.parametrize("outcome", ["success", "failure", "missing_key"])
def test_dev_certificate_workflow_executes_fixed_runner_and_cleans_keys(workflow, tmp_path, outcome):
    job = workflow["jobs"]["development-management-certificate"]
    step = next(item for item in job["steps"] if item.get("name") == "Qualify dev-manager certificate on the private gateway")
    executable = tmp_path / "uv"
    record = tmp_path / "invocation.json"
    executable.write_text("#!/usr/bin/env python3\nimport json,os,sys\nfrom pathlib import Path\n"
        "key=Path(os.environ['LOOM_DEPLOY_SSH_KEY_FILE'])\n"
        "hosts=Path(os.environ['LOOM_DEPLOY_SSH_KNOWN_HOSTS_FILE'])\n"
        "assert key.read_text().strip()=='dedicated-test-key'\n"
        "assert key.stat().st_mode & 0o077 == 0 and hosts.stat().st_mode & 0o077 == 0\n"
        "assert 'DEPLOY_SSH_KEY' not in os.environ\n"
        "Path(os.environ['PROBE_RECORD']).write_text(json.dumps({'args':sys.argv[1:], 'key':str(key), 'hosts':str(hosts)}))\n"
        "raise SystemExit(int(os.environ['PROBE_EXIT']))\n")
    executable.chmod(0o700)
    result = subprocess.run(["bash", "-c", step["run"]], capture_output=True, timeout=15,
        env={"PATH": str(tmp_path) + os.pathsep + os.environ["PATH"], "RUNNER_TEMP": str(tmp_path),
            "DEPLOY_SSH_KEY": "" if outcome == "missing_key" else "dedicated-test-key",
            "NEBIUS_CERTIFICATE_SSH_KEY": "legacy-key-must-not-be-used",
            "DEPLOY_KNOWN_HOSTS": "qualified-test-host", "LOOM_DEPLOY_SSH_TARGET": "codex@192.0.2.1",
            "NEBIUS_DEVELOPMENT_MANAGEMENT_CERTIFICATE_INSTALLATION_JSON": "{}",
            "PROBE_RECORD": str(record), "PROBE_EXIT": "23" if outcome == "failure" else "0"})
    assert result.returncode == {"success": 0, "failure": 23, "missing_key": 1}[outcome]
    assert b"dedicated-test-key" not in result.stdout + result.stderr
    if outcome == "missing_key":
        assert not record.exists()
    else:
        value = json.loads(record.read_text())
        assert value["args"][:4] == ["run", "--no-sync", "python", "scripts/ops/nebius_certificate_rollout.py"]
        assert "--management-only" in value["args"]
        assert not Path(value["key"]).exists() and not Path(value["hosts"]).exists()
