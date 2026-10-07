"""Protected publication delivers reviewed dev tooling, never workload secrets."""
from __future__ import annotations

import importlib
import json
import os
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml
from tests.ops.test_nebius_development_gateway import archive, members, operation, report


def module():
    return importlib.import_module("scripts.ops.nebius_development_rollout")


def test_bundle_packages_exact_source_reproducibly_without_private_material(tmp_path):
    from scripts.ops.nebius_development_gateway import unpack_bundle

    uv, requirements, wheels = tmp_path / "uv", tmp_path / "requirements", tmp_path / "wheels"
    uv.write_bytes(b"tool")
    requirements.write_bytes(b"dependencies")
    wheels.mkdir()
    for name in ("loom-0.1.0-py3-none-any.whl", "loom_bundle_checksum-0.1.0-py3-none-any.whl"):
        (wheels / name).write_bytes(b"wheel")
    source = {"source_sha": "a" * 40, "source_archive_sha256": "sha256:" + "b" * 64}
    raw = module().build_bundle(operation(tmp_path), source=source, uv=uv, requirements=requirements, wheels=wheels)
    assert raw == module().build_bundle(operation(tmp_path), source=source, uv=uv, requirements=requirements, wheels=wheels)
    files, metadata = unpack_bundle(raw)
    assert metadata == operation(tmp_path) and json.loads(files["development-source.json"]) == source
    assert "inputs.json" not in files and not any("credential" in name for name in files)
    assert files["scripts/ops/nebius_development_entry.py"] == (
        Path(__file__).resolve().parents[2] / "scripts/ops/nebius_development_entry.py").read_bytes()


@pytest.mark.parametrize("damage", [None, "dirty", "untracked", "wrong-head", "not-integrated"])
def test_source_selection_requires_exact_clean_integrated_checkout(tmp_path, monkeypatch, damage):
    repository = tmp_path / "checkout"
    repository.mkdir()

    def git(*args):
        return subprocess.run(["git", *args], cwd=repository, check=True, capture_output=True, text=True).stdout.strip()

    git("init", "-q")
    git("config", "user.name", "Test")
    git("config", "user.email", "test@example.invalid")
    (repository / "source").write_text("approved source\n")
    git("add", "source")
    git("commit", "-qm", "initial")
    sha = git("rev-parse", "HEAD")
    if damage != "not-integrated":
        git("update-ref", "refs/remotes/origin/dev", sha)
    metadata = operation(tmp_path) | {"source_sha": sha, "candidate": sha}
    if damage == "dirty":
        (repository / "source").write_text("changed")
    elif damage == "untracked":
        (repository / "untracked").write_text("unexpected")
    elif damage == "wrong-head":
        metadata.update(source_sha="b" * 40, candidate="b" * 40)
    monkeypatch.setattr(module(), "ROOT", repository)
    if damage:
        with pytest.raises(module().RolloutError):
            module().verify_source(metadata)
    else:
        module().verify_source(metadata)


@pytest.mark.parametrize("action", ["preflight", "install"])
def test_transport_uses_only_fixed_dev_command_and_filters_report(tmp_path, monkeypatch, action):
    raw = archive(members(tmp_path))
    value = report(tmp_path, "development_preflight_qualified" if action == "preflight" else "pending", phase="storage")
    invocations = []

    def ssh(args, **kwargs):
        invocations.append((args, kwargs))
        return SimpleNamespace(returncode=0, stdout=json.dumps(value | {"private": "not-exported"}).encode())

    monkeypatch.setattr(module().subprocess, "run", ssh)
    result = module().transfer(raw, action=action, target="operator@host", key=tmp_path / "key", known_hosts=tmp_path / "hosts")
    assert result["status"] == value["status"] and "private" not in result
    args, kwargs = invocations[0]
    assert args[-1] == "loom-nebius-development-" + action + "-v1"
    assert "StrictHostKeyChecking=yes" in args and "IdentitiesOnly=yes" in args
    assert kwargs["input"] == raw
    with pytest.raises(module().RolloutError):
        module().transfer(raw, action="rollback", target="operator@host", key=tmp_path / "key", known_hosts=tmp_path / "hosts")
    assert len(invocations) == 1


def workflow():
    return yaml.safe_load((Path(__file__).resolve().parents[2] / ".github/workflows/nebius-rollout.yml").read_text())


@pytest.mark.parametrize("action", ["preflight", "install"])
def test_workflow_selects_dev_only_authority_and_removes_ephemeral_key(tmp_path, action):
    flow = workflow()
    choices = flow.get("on", flow.get(True))["workflow_dispatch"]["inputs"]["operation"]["options"]
    assert "development-" + action in choices
    job = flow["jobs"]["development"]
    assert job["permissions"] == {"contents": "read"}
    assert job["environment"] == {"name": "nebius-integration", "deployment": False}
    assert "workflow_dispatch" in job["if"] and "refs/heads/dev" in job["if"]
    selector = next(s for s in job["steps"] if s.get("id") == "tooling")
    runner = next(s for s in job["steps"] if s.get("name") == "Run fixed development operation")
    assert runner["env"]["DEPLOY_SSH_KEY"] == "${{ secrets.NEBIUS_DEVELOPMENT_SSH_KEY }}"
    assert runner["env"]["NEBIUS_DEVELOPMENT_OPERATION_JSON"] == "${{ vars.NEBIUS_DEVELOPMENT_OPERATION_JSON }}"
    bindir = tmp_path / "bin"
    bindir.mkdir()
    git = bindir / "git"
    git.write_text('#!/bin/sh\ntest "$1 $2 $3 $4" = "merge-base --is-ancestor $EXPECTED_SHA refs/remotes/origin/dev"\n')
    git.chmod(0o700)
    uv = bindir / "uv"
    uv.write_text('''#!/usr/bin/env python3
import json, os, pathlib, sys
assert sys.argv[sys.argv.index('--operation') + 1] == os.environ['EXPECTED_ACTION']
assert 'scripts.ops.nebius_development_rollout' in sys.argv
assert pathlib.Path(os.environ['LOOM_DEPLOY_SSH_KEY_FILE']).read_text().strip() == 'private-dev-key'
assert json.loads(os.environ['NEBIUS_DEVELOPMENT_OPERATION_JSON'])['namespace'] == 'loom-dev'
pathlib.Path(os.environ['RUNNER_TEMP'], 'transport-invoked').write_text('selected')
''')
    uv.chmod(0o700)
    env = os.environ | {"PATH": str(bindir) + ":" + os.environ["PATH"], "DEVELOPMENT_OPERATION": "development-" + action,
        "NEBIUS_DEVELOPMENT_OPERATION_JSON": json.dumps(operation(tmp_path)), "DEPLOY_SSH_KEY": "private-dev-key",
        "DEPLOY_KNOWN_HOSTS": "test-host", "LOOM_DEPLOY_SSH_TARGET": "test-target", "RUNNER_TEMP": str(tmp_path),
        "GITHUB_OUTPUT": str(tmp_path / "outputs"), "EXPECTED_SHA": "a" * 40, "EXPECTED_ACTION": action}
    for step in (selector, runner):
        result = subprocess.run(["bash", "-e", "-c", step["run"]], env=env, capture_output=True, text=True, timeout=20)
        assert result.returncode == 0, result.stderr
        assert "private-dev-key" not in result.stdout + result.stderr
    assert (tmp_path / "outputs").read_text() == "sha=" + "a" * 40 + "\n"
    assert (tmp_path / "transport-invoked").read_text() == "selected"
    assert not (tmp_path / "nebius-development-key").exists()
    (tmp_path / "transport-invoked").unlink()
    for damage in ({"DEPLOY_SSH_KEY": ""}, {"DEVELOPMENT_OPERATION": "management-install"}):
        result = subprocess.run(["bash", "-e", "-c", runner["run"]], env=env | damage, capture_output=True, timeout=20)
        assert result.returncode != 0 and not (tmp_path / "transport-invoked").exists()
    wrong = operation(tmp_path) | {"namespace": "loom-nebius-platform"}
    result = subprocess.run(["bash", "-e", "-c", selector["run"]],
        env=env | {"NEBIUS_DEVELOPMENT_OPERATION_JSON": json.dumps(wrong)}, capture_output=True, timeout=20)
    assert result.returncode != 0
