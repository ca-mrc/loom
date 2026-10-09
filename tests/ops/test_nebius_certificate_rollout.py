"""Certificate workflow transports only approved tooling and cannot select app rollout."""
from __future__ import annotations

import importlib
import io
import json
import os
import subprocess
import zipfile
from pathlib import Path

import pytest
import yaml


def module():
    return importlib.import_module("scripts.ops.nebius_certificate_rollout")


def test_bundle_is_repeatable_and_does_not_include_dns_credentials(tmp_path):
    uv = tmp_path / "uv"
    uv.write_bytes(b"qualified uv")
    requirements = tmp_path / "requirements.txt"
    requirements.write_text("certbot==5.8.0 --hash=sha256:fixture\n")
    config = {"credential_file": "/gateway/private/godaddy.json", "state_dir": "/gateway/nebius-certificates/state"}
    first = module().build_bundle(config, uv=uv, requirements=requirements)
    assert first == module().build_bundle(config, uv=uv, requirements=requirements)
    with zipfile.ZipFile(io.BytesIO(first)) as archive:
        assert set(archive.namelist()) == {"uv", "requirements.txt", "installation.json", "manifest.json",
                                           "scripts/ops/nebius_certificates.py", "scripts/ops/nebius_dns_challenge.py",
                                           "scripts/ops/nebius_certificate_gateway.py"}
        assert json.loads(archive.read("installation.json")) == config


def test_transfer_preserves_host_verification_and_passes_only_data_stdin(monkeypatch):
    report = {"status": "qualified", "installation_id": "024cfbfb-a7e8-4d85-9c60-c1d838730f9a",
              "fingerprint_sha256": "a" * 64, "generation": "b" * 64,
              "expires_at": "2026-12-02T12:00:00+00:00",
              "sans": ["*.dev.example.test", "management.example.test"]}

    def run(args, **kwargs):
        assert args[0] == "ssh"
        assert "StrictHostKeyChecking=yes" in args and "IdentitiesOnly=yes" in args
        assert "UserKnownHostsFile=/private/known_hosts" in args
        assert args[-2] == "codex@192.0.2.1"
        assert args[-1] == "loom-nebius-certificate-v1"
        assert kwargs["input"] == b"tooling-only"
        assert b"tooling-only" not in args[-1].encode()
        return subprocess.CompletedProcess(args, 0, json.dumps(report).encode(), b"")

    monkeypatch.setattr(subprocess, "run", run)
    result = module().transfer(b"tooling-only", target="codex@192.0.2.1", key=Path("/private/key"),
                               known_hosts=Path("/private/known_hosts"))
    assert result == report


@pytest.mark.parametrize("target", ["-oProxyCommand=evil", "codex@host;echo injected", "host", "codex@host\nother"])
def test_invalid_ssh_target_is_rejected_before_process_creation(target, monkeypatch):
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: pytest.fail("unsafe SSH target executed"))
    with pytest.raises(module().RolloutError):
        module().transfer(b"data", target=target, key=Path("/private/key"), known_hosts=Path("/private/known_hosts"))


def test_remote_failure_is_not_retried_or_forwarded(monkeypatch):
    calls = []

    def failure(args, **kwargs):
        calls.append(args)
        return subprocess.CompletedProcess(args, 1, b"private-response", b"private-error")

    monkeypatch.setattr(subprocess, "run", failure)
    with pytest.raises(module().RolloutError) as error:
        module().transfer(b"data", target="codex@192.0.2.1", key=Path("/private/key"), known_hosts=Path("/private/known_hosts"))
    assert len(calls) == 1 and "private-" not in str(error.value)


def test_forced_command_rejection_has_a_distinct_safe_diagnostic(monkeypatch):
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: subprocess.CompletedProcess(
        a, 126, b"private-output", b"private-authority-path",
    ))
    with pytest.raises(module().CertificateAuthorityDeniedError):
        module().transfer(b"data", target="codex@192.0.2.1", key=Path("/private/key"),
                          known_hosts=Path("/private/known_hosts"))


def test_operator_prepares_exact_bundle_without_contacting_gateway(tmp_path, monkeypatch, capsys):
    import sys

    output = tmp_path / "approved.zip"
    evidence = tmp_path / "evidence"
    monkeypatch.setattr(sys, "argv", ["prepare", "--requirements", str(tmp_path / "requirements"),
                                    "--evidence-dir", str(evidence), "--prepare-bundle", str(output)])
    monkeypatch.setenv("NEBIUS_CERTIFICATE_INSTALLATION_JSON", "{}")
    monkeypatch.setattr(module(), "load_installation", lambda path: {})
    monkeypatch.setattr(module().shutil, "which", lambda name: "/qualified/uv")
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: subprocess.CompletedProcess(
        a, 0, b"uv 0.11.26 (x86_64-unknown-linux-gnu)\n", b"",
    ))
    monkeypatch.setattr(module(), "build_bundle", lambda *a, **k: b"approved-bundle")
    monkeypatch.setattr(module(), "transfer", lambda *a, **k: pytest.fail("preparation contacted gateway"))
    assert module().main() == 0
    report = json.loads(capsys.readouterr().out)
    assert report == {"status": "prepared", "bundle_sha256": "f6bb76de752093a645ae712531636a2f688acfea0a61ca76339f16c7ad576e1f"}
    assert output.read_bytes() == b"approved-bundle"
    assert output.stat().st_mode & 0o077 == 0
    # Never overwrite an operator's existing approved artifact.
    output.write_bytes(b"retained")
    assert module().main() == 1
    assert output.read_bytes() == b"retained"


def test_certificate_operation_is_protected_and_not_a_route_to_application_rollout():
    workflow = yaml.load((Path(__file__).parents[2] / ".github/workflows/nebius-rollout.yml").read_text(), Loader=yaml.BaseLoader)
    assert "certificate" in workflow["on"]["workflow_dispatch"]["inputs"]["operation"]["options"]
    job = workflow["jobs"]["certificate"]
    assert job["environment"]["name"] == "nebius-integration"
    assert job["permissions"] == {"contents": "read"}
    for condition in ("github.repository_id == '1281629473'", "github.ref == 'refs/heads/dev'",
                      "github.event_name == 'workflow_dispatch'", "inputs.operation == 'certificate'"):
        assert condition in job["if"]
    # Automatic application retries never invoke the certificate operation.
    assert "github.event_name == 'workflow_dispatch'" in job["if"]
    assert "inputs.operation == 'rollout'" in workflow["jobs"]["rollout"]["if"]
    commands = "\n".join(step.get("run", "") for step in job["steps"])
    assert "nebius_certificate_rollout.py" in commands
    assert "nebius_idle_rollout.py" not in commands
    assert "--only-group nebius-certificates" in commands
    transport = next(step for step in job["steps"] if step.get("name") == "Qualify certificate on the private gateway")
    assert transport["env"]["DEPLOY_SSH_KEY"] == "${{ secrets.NEBIUS_CERTIFICATE_SSH_KEY }}"
    artifact = next(step for step in job["steps"] if step.get("name") == "Preserve sanitized certificate evidence")
    assert artifact["with"]["path"].endswith("/certificate-result.json")


@pytest.mark.skipif(not os.environ.get("LOOM_TEST_CERTIFICATE_REQUIREMENTS"), reason="isolated locked gateway qualification")
def test_actual_locked_bundle_bootstraps_without_using_existing_operator_environment(tmp_path):
    from scripts.ops.nebius_certificate_gateway import prepare_release, run_private

    config = {"state_dir": str(tmp_path / "nebius-certificates" / "state")}
    content = module().build_bundle(config, uv=Path(os.environ["LOOM_TEST_CERTIFICATE_UV"]),
                                    requirements=Path(os.environ["LOOM_TEST_CERTIFICATE_REQUIREMENTS"]))
    release, _ = prepare_release(content)
    python = str(release / "venv" / "bin" / "python")
    result = run_private([python, "-c", "from certbot.main import main; raise SystemExit(main())", "--version"], timeout=30)
    assert result.strip() == b"certbot 5.8.0"
    result = run_private([python, str(release / "scripts" / "ops" / "nebius_certificates.py"), "--help"], timeout=30)
    assert b"issue" in result and b"hook" in result
    assert not (tmp_path / "nebius-certificates" / "state").exists(), "tooling qualification started live issuance"


@pytest.mark.skipif(not os.environ.get("LOOM_TEST_CERTBOT_PYTHON"), reason="actual pinned Certbot account/lineage compatibility")
def test_actual_certbot_account_and_lineage_pass_private_recovery_audit(tmp_path):
    from tests.ops.test_nebius_certificates import material

    chain, key, _ = material()
    certificate = tmp_path / "certificate.pem"
    private = tmp_path / "key.pem"
    certificate.write_bytes(chain)
    private.write_bytes(key)
    private.chmod(0o600)
    # The real pinned storage implementation creates its normal file modes,
    # directory structure, links, account metadata and renewal configuration.
    script = '''
import os, sys
from pathlib import Path
import josepy
from acme import messages
from cryptography.hazmat.primitives.asymmetric import rsa
from certbot._internal import account, cli, storage
from certbot._internal.plugins.disco import PluginsRegistry
os.umask(0o077)
root = Path(sys.argv[1])
for name in ('acme', 'work', 'logs'):
    (root / name).mkdir(mode=0o700, parents=True, exist_ok=True)
config = cli.prepare_and_parse_args(PluginsRegistry.find_all(), [
    'certonly', '--manual', '--config', '/dev/null', '--config-dir', str(root/'acme'),
    '--work-dir', str(root/'work'), '--logs-dir', str(root/'logs'),
    '--server', 'https://acme-v02.api.letsencrypt.org/directory'])
key = josepy.JWKRSA(key=rsa.generate_private_key(public_exponent=65537, key_size=2048))
registration = messages.RegistrationResource(body=messages.Registration(), uri='https://example.test/account/1')
owner = account.Account(registration, key)
account.AccountFileStorage(config).save(owner, client=None)
storage.RenewableCert.new_lineage('loom-managed', Path(sys.argv[2]).read_bytes(), Path(sys.argv[3]).read_bytes(), b'', config)
'''
    result = subprocess.run([os.environ["LOOM_TEST_CERTBOT_PYTHON"], "-c", script,
                             str(tmp_path / "state"), str(certificate), str(private)],
                            capture_output=True, timeout=30)
    assert result.returncode == 0, result.stderr.decode()
    from scripts.ops.nebius_certificates import _audit_recovery

    _audit_recovery(tmp_path / "state", sync=True)
