from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
from scripts.ops import nebius_legacy_pool_completion as completion
from scripts.ops.deploy_nebius_platform import DeploymentError, Kubectl

OPERATION = "11111111-1111-4111-8111-111111111111"
UID = "22222222-2222-4222-8222-222222222222"
KEY = "Deployment:platform:loom-control-plane"


@pytest.fixture
def saved(tmp_path, monkeypatch):
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    root = tmp_path / ".loom/nebius-management/pool-cutover" / OPERATION
    state = root / "state"
    state.mkdir(parents=True)
    (root / "anchor").mkdir()
    receipt = {"schema": "loom.nebius-pool-completion.v1", "operation_id": OPERATION,
        "state_dir": str(state), "contract_sha256": "a" * 64, "outcome": "legacy",
        "phase_sha256": {}, "workloads": {KEY: {"kind": "Deployment", "metadata": {
            "namespace": "platform", "name": "loom-control-plane", "uid": UID},
            "spec": {"private_setting": "must-not-escape"}}}}
    def save():
        raw = json.dumps(receipt, sort_keys=True).encode()
        path = state / "completion.json"
        path.write_bytes(raw)
        path.chmod(0o600)
        anchor = root / "anchor" / (OPERATION + "-completion.json")
        anchor.write_text(json.dumps({"schema": "loom.nebius-pool-completion-anchor.v1",
            "operation_id": OPERATION, "state_dir": str(state), "completion_sha256": hashlib.sha256(raw).hexdigest()}))
        anchor.chmod(0o600)
        return path, anchor
    return receipt, save, *save()


def test_cli_returns_only_existing_terminal_identities(saved, tmp_path):
    result = subprocess.run([sys.executable, str(Path(completion.__file__)), OPERATION],
        env={**os.environ, "HOME": str(tmp_path)}, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {"operation_id": OPERATION, "outcome": "legacy", "workloads": {KEY: UID}}
    assert "must-not-escape" not in result.stdout + result.stderr


@pytest.mark.parametrize("failure", ["missing-receipt", "missing-anchor", "mismatch", "global", "wrong-operation",
    "wrong-state", "wrong-key", "wrong-uid", "public", "symlink", "oversized"])
def test_incomplete_or_unqualified_completion_is_rejected(saved, failure):
    receipt, save, path, anchor = saved
    if failure == "missing-receipt":
        path.unlink()
    elif failure == "missing-anchor":
        anchor.unlink()
    elif failure == "mismatch":
        path.write_bytes(path.read_bytes() + b" ")
    elif failure == "global":
        receipt["outcome"] = "global"
        save()
    elif failure == "wrong-operation":
        receipt["operation_id"] = UID
        save()
    elif failure == "wrong-state":
        receipt["state_dir"] = "/another/state"
        save()
    elif failure == "wrong-key":
        receipt["workloads"][KEY]["metadata"]["name"] = "another-workload"
        save()
    elif failure == "wrong-uid":
        receipt["workloads"][KEY]["metadata"]["uid"] = "not-a-uid"
        save()
    elif failure == "public":
        path.chmod(0o644)
    elif failure == "symlink":
        original = path.with_suffix(".retained")
        path.rename(original)
        path.symlink_to(original)
    else:
        path.write_bytes(b"x" * (8 * 1024**2 + 1))
    with pytest.raises((ValueError, OSError)):
        completion.read_completion(OPERATION)


def test_invalid_cli_never_reports_private_exception(saved, tmp_path):
    result = subprocess.run([sys.executable, str(Path(completion.__file__)), "../../private"],
        env={**os.environ, "HOME": str(tmp_path)}, capture_output=True, text=True)
    assert result.returncode == 1
    assert result.stdout == ""
    assert result.stderr == "legacy pool completion unavailable\n"


def test_ssh_probe_uses_fixed_command_and_rejects_untrusted_projection(monkeypatch, tmp_path):
    monkeypatch.setenv("LOOM_DEPLOY_SSH_TARGET", "operator@gateway.test")
    monkeypatch.setenv("LOOM_DEPLOY_SSH_KNOWN_HOSTS_FILE", "/private/known-hosts")
    monkeypatch.setenv("LOOM_DEPLOY_SSH_KEY_FILE", "/private/key")
    calls = []
    result = {"operation_id": OPERATION, "outcome": "legacy", "workloads": {KEY: UID}}
    def run(command, **kwargs):
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, json.dumps(result), "")
    monkeypatch.setattr(subprocess, "run", run)
    kube = Kubectl(tmp_path / "unused")
    assert kube.legacy_pool_completion(OPERATION) == result
    assert calls[-1][-2:] == ["operator@gateway.test", "loom-nebius-legacy-pool-completion-v1 " + OPERATION]
    result["unexpected_private_field"] = "must-not-escape"
    with pytest.raises(DeploymentError, match="completion unavailable"):
        kube.legacy_pool_completion(OPERATION)
    with pytest.raises(DeploymentError, match="completion unavailable"):
        kube.legacy_pool_completion("../../private")
    assert len(calls) == 2
