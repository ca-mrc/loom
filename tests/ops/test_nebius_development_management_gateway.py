"""Dev tooling cannot inherit management authority or execute unauthenticated bytes."""
from __future__ import annotations

import ast
import hashlib
import importlib
import io
import json
import stat
import sys
import zipfile
from pathlib import Path
from types import SimpleNamespace

import pytest


def module():
    return importlib.import_module("scripts.ops.nebius_development_management_gateway")


def operation(tmp_path):
    identity = "18718d96-d389-40b3-a79b-11489924d0d4"
    owner = tmp_path if tmp_path.name == ".loom" else tmp_path / ".loom"
    root = owner / "nebius-development-management" / identity
    return {"schema": "loom.nebius-development-management-operation.v1", "source_sha": "a" * 40,
        "candidate": "a" * 40, "installation_id": identity, "namespace": "loom-nebius-management-dev",
        "state_dir": str(root / "state"), "anchor_dir": str(owner / "nebius-development-management-anchors" / identity),
        "inputs_path": str(root / "inputs.json"), "inputs_sha256": "c" * 64}


def members(tmp_path):
    return {**dict.fromkeys(module().SOURCES, b"# trusted test source\n"),
        "uv": b"test-uv", "requirements.txt": b"test==1 --hash=sha256:" + b"a" * 64,
        "wheels/loom-0.1.0-py3-none-any.whl": b"test-wheel",
        "wheels/loom_bundle_checksum-0.1.0-py3-none-any.whl": b"test-wheel",
        "operation.json": json.dumps(operation(tmp_path), sort_keys=True).encode(),
        "development-management-source.json": json.dumps({"source_sha": "a" * 40,
            "source_archive_sha256": "sha256:" + "b" * 64}, sort_keys=True).encode()}


def archive(files, *, duplicate=False, symlink=False):
    files = {**files, "manifest.json": json.dumps({name: hashlib.sha256(value).hexdigest()
        for name, value in files.items()}, sort_keys=True).encode()}
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as bundle:
        for name, value in files.items():
            info = zipfile.ZipInfo(name)
            info.external_attr = ((stat.S_IFLNK | 0o600) if symlink else 0o100600) << 16
            bundle.writestr(info, value)
        if duplicate:
            with pytest.warns(UserWarning, match="Duplicate name"):
                bundle.writestr("operation.json", files["operation.json"])
    return buffer.getvalue()


def report(tmp_path, status="development_management_installed", **extra):
    op = operation(tmp_path)
    return {key: op[key] for key in ("candidate", "installation_id", "namespace")} | {"status": status} | extra


def test_bundle_binds_source_and_is_closed_over_script_imports(tmp_path):
    files = members(tmp_path)
    actual, op = module().unpack_bundle(archive(files))
    assert actual["development-management-source.json"] == files["development-management-source.json"]
    assert op == operation(tmp_path)
    root = Path(__file__).resolve().parents[2]
    pending, seen = ["scripts.ops.nebius_development_management_entry"], set()
    while pending:
        name = pending.pop()
        if name in seen:
            continue
        seen.add(name)
        path = name.replace(".", "/") + ".py"
        assert path in module().SOURCES, f"Missing runtime import: {path}"
        for node in ast.walk(ast.parse((root / path).read_text())):
            if isinstance(node, ast.ImportFrom) and node.module:
                if node.module.startswith("scripts.ops."):
                    pending.append(node.module)
                elif node.module == "scripts.ops":
                    pending.extend("scripts.ops." + alias.name for alias in node.names)
    assert "deploy/k8s/nebius-execution-actuator.yaml" in actual
    assert "deploy/k8s/nebius-capacity-collector.yaml" in actual


@pytest.mark.parametrize("damage", ["traversal", "missing", "duplicate", "symlink", "oversize", "source", "namespace", "manifest"])
def test_invalid_bundle_is_rejected_before_extraction(tmp_path, damage):
    files = members(tmp_path)
    if damage == "traversal":
        files["../unexpected"] = b"no"
    elif damage == "missing":
        del files["requirements.txt"]
    elif damage == "oversize":
        files["development-management-source.json"] = b" " * 4097
    elif damage == "source":
        files["development-management-source.json"] = json.dumps({"source_sha": "d" * 40,
            "source_archive_sha256": "sha256:" + "b" * 64}).encode()
    elif damage == "namespace":
        files["operation.json"] = json.dumps(operation(tmp_path) | {"namespace": "loom-nebius-platform"}).encode()
    raw = archive(files, duplicate=damage == "duplicate", symlink=damage == "symlink")
    if damage == "manifest":
        raw = raw.replace(b"test-uv", b"evil-uv")
    with pytest.raises(module().GatewayError):
        module().unpack_bundle(raw)
    assert not (tmp_path / "nebius-development-management").exists()


@pytest.mark.parametrize("command", ["", "loom-nebius-management-install-v1", "loom-nebius-development-install-v1", "loom-nebius-pool-rollback-v1",
    "loom-nebius-development-management-install-v1; id", "loom-nebius-development-management-install-v1 "])
def test_wrong_command_never_reads_or_prepares_bundle(tmp_path, command, monkeypatch):
    monkeypatch.setenv("SSH_ORIGINAL_COMMAND", command)
    monkeypatch.setattr(sys, "stdin", None)
    assert module().authorized_main("a" * 64) == 126


def test_unapproved_bytes_never_prepare_release(tmp_path, monkeypatch):
    raw = archive(members(tmp_path))
    monkeypatch.setenv("SSH_ORIGINAL_COMMAND", "loom-nebius-development-management-install-v1")
    monkeypatch.setattr(sys, "stdin", SimpleNamespace(buffer=io.BytesIO(raw)))
    monkeypatch.setattr(module(), "prepare_release", lambda *_: pytest.fail("executed before authentication"))
    assert module().authorized_main("0" * 64) == 126


@pytest.mark.parametrize("action,status,extra", [
    ("preflight", "development_management_preflight_qualified", {}),
    ("install", "pending", {"phase": "database"}),
    ("install", "development_management_installed", {}),
    ("install", "blocked", {"stage": "provider_disk"}),
])
def test_only_bounded_dev_reports_leave_authenticated_gateway(tmp_path, monkeypatch, capsys, action, status, extra):
    raw = archive(members(tmp_path))
    monkeypatch.setenv("SSH_ORIGINAL_COMMAND", "loom-nebius-development-management-" + action + "-v1")
    monkeypatch.setattr(sys, "stdin", SimpleNamespace(buffer=io.BytesIO(raw)))
    monkeypatch.setattr(module(), "prepare_release", lambda *_: tmp_path)
    value = report(tmp_path, status, **extra) | {"private": "secret-not-exported"}
    monkeypatch.setattr(module(), "run_private", lambda *_args, **_kw: json.dumps(value).encode())
    assert module().authorized_main(hashlib.sha256(raw).hexdigest()) == 0
    visible = json.loads(capsys.readouterr().out)
    assert visible == report(tmp_path, status, **extra) | {"source_sha": "a" * 40}
    assert module().safe_report(json.dumps(visible).encode(), operation(tmp_path), action) == visible


@pytest.mark.parametrize("damage", [{"namespace": "loom-nebius-platform"}, {"candidate": "b" * 40},
    {"source_sha": "b" * 40}, {"status": "management_installed"}, {"status": "development_management_preflight_qualified"},
    {"status": "pending", "phase": "activate"}, {"status": "blocked", "stage": "secret-value"}])
def test_report_cannot_retarget_or_claim_wrong_action(tmp_path, damage):
    with pytest.raises(module().GatewayError):
        module().safe_report(json.dumps(report(tmp_path) | damage).encode(), operation(tmp_path), "install")


def test_private_release_qualifies_once_and_replay_detects_tampering(tmp_path, monkeypatch):
    gateway = module()
    op = operation(tmp_path)
    Path(op["inputs_path"]).parent.mkdir(parents=True, mode=0o700)
    raw = archive(members(tmp_path))
    calls = []

    def run(args, *, timeout):
        calls.append(args)
        return b'{"status":"tooling_qualified"}' if args[-1] == "qualify" else b""

    monkeypatch.setattr(gateway, "run_private", run)
    release = gateway.prepare_release(raw)
    assert release.name == hashlib.sha256(raw).hexdigest()
    assert len(calls) == 4 and "--require-hashes" in calls[1] and "--offline" in calls[2]
    assert (release / "complete").read_bytes() == b"complete"
    assert not Path(op["state_dir"]).exists()
    assert gateway.prepare_release(raw) == release and len(calls) == 4
    (release / "development-management-source.json").write_bytes(b"changed")
    with pytest.raises(gateway.GatewayError):
        gateway.prepare_release(raw)
    assert len(calls) == 4


def test_failed_release_is_retained_without_automatic_retry(tmp_path, monkeypatch):
    gateway = module()
    op = operation(tmp_path)
    Path(op["inputs_path"]).parent.mkdir(parents=True, mode=0o700)
    raw = archive(members(tmp_path))

    def fail(*args, **kwargs):
        raise RuntimeError("private provider output")

    monkeypatch.setattr(gateway, "run_private", fail)
    with pytest.raises(gateway.GatewayError, match="retain private state"):
        gateway.prepare_release(raw)
    release = Path(op["state_dir"]).parent / "releases" / hashlib.sha256(raw).hexdigest()
    assert release.is_dir() and not (release / "complete").exists()
    monkeypatch.setattr(gateway, "run_private", lambda *_args, **_kw: pytest.fail("retried uncertain preparation"))
    with pytest.raises(gateway.GatewayError):
        gateway.prepare_release(raw)


def test_command_uses_only_isolated_dev_entry(tmp_path):
    command = module().command(tmp_path, "install")
    assert command[:3] == [str(tmp_path / "venv/bin/python"), "-I", "-c"]
    assert command[-3:] == [str(tmp_path), str(tmp_path / "operation.json"), "install"]
    with pytest.raises(module().GatewayError):
        module().command(tmp_path, "rollback")

