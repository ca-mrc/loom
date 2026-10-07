"""Exact ingress tooling cannot inherit local Python or certificate authority."""
from __future__ import annotations

import hashlib
import importlib
import io
import json
import os
import signal
import subprocess
import sys
import time
import zipfile
from pathlib import Path

import pytest
from tests.support.process_observation import process_exited


def module():
    return importlib.import_module("scripts.ops.nebius_ingress_bootstrap")


def configuration(tmp_path):
    return {
        "schema": "loom.nebius-ingress-installation.v1",
        "source_sha": "a" * 40,
        "candidate": "b" * 40,
        "state_dir": str(tmp_path / "nebius-ingress" / "state"),
        "certificate_config": str(tmp_path / "nebius-certificates" / "installation.json"),
        "kubeconfig": str(tmp_path / "kubeconfig"), "kubectl": str(tmp_path / "kubectl"),
        "cluster_id": "mk8scluster-e00fixture", "api_server": "https://192.0.2.1:443",
        "ingress_class": "loom-shared",
        "image": "cr.eu-north1.nebius.cloud/registry/loom-shared-ingress@sha256:" + "c" * 64,
        "binding": {
            "installation_id": "18718d96-d389-40b3-a79b-11489924d0d4",
            "certificate_installation_id": "d67c8115-1311-49e8-80f7-6f614361db9a",
            "namespace": "loom-nebius-development",
            "namespace_uid": "2a83a179-ff51-46ce-bfdd-e6a547618edd",
            "kube_system_uid": "52f5b18c-7dd3-4095-bd7e-49f6a6330391",
            "child_domain": "dev.example.test", "management_host": "management.example.test",
        },
    }


def archive(files, *, bad_hash=False):
    files = dict(files)
    files["manifest.json"] = json.dumps({key: hashlib.sha256(value).hexdigest() for key, value in files.items()}).encode()
    if bad_hash:
        files["manifest.json"] = b"{}"
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w", compression=zipfile.ZIP_DEFLATED) as target:
        for name, content in files.items():
            target.writestr(name, content)
    return stream.getvalue()


def inputs(tmp_path):
    # Break caught: omitted first-party dependency or code in the installed bundle.
    files = {name: b"fixture source" for name in module().SCRIPTS}
    files.update({"uv": b"fixture installer", "requirements.txt": b"httpx==0.28.1\n",
                  "installation.json": json.dumps(configuration(tmp_path)).encode(),
                  "wheels/loom-0.0.0-py3-none-any.whl": b"loom wheel",
                  "wheels/loom_bundle_checksum-0.1.0-py3-none-any.whl": b"checksum wheel"})
    return files


@pytest.mark.parametrize("change", ["extra", "missing_checksum", "bad_hash", "oversize", "duplicate"])
def test_unqualified_archive_rejected_before_storage(tmp_path, change):
    files = inputs(tmp_path)
    if change == "extra":
        files["../foreign"] = b"bad"
    elif change == "missing_checksum":
        del files["wheels/loom_bundle_checksum-0.1.0-py3-none-any.whl"]
    elif change == "oversize":
        files["installation.json"] = b"x" * 16385
    content = archive(files, bad_hash=change == "bad_hash")
    if change == "duplicate":
        stream = io.BytesIO(content)
        with zipfile.ZipFile(stream, "a") as target:
            with pytest.warns(UserWarning, match="Duplicate name"):
                target.writestr("uv", b"other")
        content = stream.getvalue()
    with pytest.raises(module().BootstrapError):
        module().prepare_release(content)
    assert not (tmp_path / "nebius-ingress").exists()


def test_separate_release_installs_both_wheels_offline_and_probes_isolated_imports(tmp_path, monkeypatch):
    calls = []
    content = archive(inputs(tmp_path))
    monkeypatch.setattr(module(), "run_private", lambda args, **kw: calls.append(args) or b"")
    release, config = module().prepare_release(content)
    assert release.parent == tmp_path / "nebius-ingress" / "releases"
    assert json.loads(config.read_bytes()) == configuration(tmp_path)
    assert len(calls) == 4
    assert "--require-hashes" in calls[1] and "--only-binary" in calls[1]
    assert "--offline" in calls[2] and "--no-deps" in calls[2]
    assert {Path(arg).name for arg in calls[2] if arg.endswith(".whl")} == {
        "loom-0.0.0-py3-none-any.whl", "loom_bundle_checksum-0.1.0-py3-none-any.whl",
    }
    assert calls[3][1:3] == ["-I", "-c"]
    assert calls[3][-1] == "qualify"
    assert module().prepare_release(content) == (release, config)
    assert len(calls) == 4
    assert not (tmp_path / "nebius-certificates").exists()
    assert all(path.stat().st_mode & 0o077 == 0 for path in release.rglob("*"))


def test_failed_import_probe_cannot_publish_complete_or_retry_bootstrap(tmp_path, monkeypatch):
    calls = []

    def fail_probe(args, **kwargs):
        calls.append(args)
        if "-I" in args:
            raise RuntimeError("private import failure")
        return b""

    monkeypatch.setattr(module(), "run_private", fail_probe)
    content = archive(inputs(tmp_path))
    for _ in range(2):
        with pytest.raises(module().BootstrapError):
            module().prepare_release(content)
    assert len(calls) == 4, "incomplete release was silently retried"
    assert not list((tmp_path / "nebius-ingress").rglob("complete"))


@pytest.mark.parametrize("field,value", [("state_dir", "/tmp/nebius-certificates/state"),
                                          ("source_sha", "branch-name"), ("candidate", "HEAD"),
                                          ("private_key", "secret"), ("binding", {})])
def test_metadata_fails_closed_before_extraction(tmp_path, field, value):
    files = inputs(tmp_path)
    config = configuration(tmp_path)
    config[field] = value
    files["installation.json"] = json.dumps(config).encode()
    with pytest.raises(module().BootstrapError):
        module().prepare_release(archive(files))
    assert not (tmp_path / "nebius-ingress").exists()


@pytest.mark.parametrize("cluster_id", ["mk8s-e00fixture", "mk8scluster-", "mk8scluster-UPPER", "mk8scluster-a/other"])
def test_invalid_cluster_identity_rejected_before_creating_private_release(tmp_path, cluster_id):
    files = inputs(tmp_path)
    config = configuration(tmp_path)
    config["cluster_id"] = cluster_id
    files["installation.json"] = json.dumps(config).encode()
    with pytest.raises(module().BootstrapError):
        module().prepare_release(archive(files))
    assert not (tmp_path / "nebius-ingress").exists()


def test_modified_release_is_not_reused(tmp_path, monkeypatch):
    monkeypatch.setattr(module(), "run_private", lambda *a, **kw: b"")
    content = archive(inputs(tmp_path))
    release, _ = module().prepare_release(content)
    (release / "wheels/loom-0.0.0-py3-none-any.whl").write_bytes(b"modified")
    with pytest.raises(module().BootstrapError):
        module().prepare_release(content)


@pytest.mark.parametrize("command", ["", "loom-nebius-ingress-v1 extra", "python -c pass",
                                      "loom-nebius-certificate-v1", "loom-nebius-ingress-dns-v1 evil.test 1.1.1.1"])
def test_forced_ingress_command_rejects_other_authority_before_input(command, monkeypatch):
    import sys

    monkeypatch.setenv("SSH_ORIGINAL_COMMAND", command)
    monkeypatch.setattr(sys, "stdin", object())
    assert module().authorized_main("a" * 64) == 126


def test_force_command_cannot_execute_different_bundle(tmp_path, monkeypatch):
    import sys
    from types import SimpleNamespace

    content = archive(inputs(tmp_path))
    monkeypatch.setenv("SSH_ORIGINAL_COMMAND", "loom-nebius-ingress-v1")
    monkeypatch.setattr(sys, "stdin", SimpleNamespace(buffer=io.BytesIO(content)))
    assert module().authorized_main("a" * 64) == 126
    assert not (tmp_path / "nebius-ingress").exists()


@pytest.mark.parametrize("change", [None, "private_ip", "multicast", "foreign_zone", "child_management",
                                   "extra_record", "wrong_name", "bad_id", "bad_origin", "missing_fingerprint"])
def test_dns_report_rejects_unqualified_targets_and_strips_private_fields(change):
    report = {
        "status": "dns_published", "installation_id": "18718d96-d389-40b3-a79b-11489924d0d4",
        "candidate": "b" * 40, "namespace": "loom-dev", "service_uid": "2a83a179-ff51-46ce-bfdd-e6a547618edd",
        "fingerprint_sha256": "c" * 64, "zone": "example.test", "child_domain": "dev.example.test",
        "management_host": "management.example.test", "address": "8.8.8.8", "private": "fixture-secret",
        "records": [{"name": "*.dev", "record_id": "record-1", "origin": "created", "private": "fixture-secret"},
                    {"name": "management", "record_id": "record-2", "origin": "external"}],
    }
    if change == "private_ip":
        report["address"] = "10.0.0.1"
    elif change == "multicast":
        report["address"] = "224.0.0.1"
    elif change == "foreign_zone":
        report["zone"] = "foreign.test"
    elif change == "child_management":
        report["management_host"] = "owner.dev.example.test"
    elif change == "extra_record":
        report["records"].append(dict(report["records"][0]))
    elif change == "wrong_name":
        report["records"][0]["name"] = "@"
    elif change == "bad_id":
        report["records"][0]["record_id"] = "private\ntext"
    elif change == "bad_origin":
        report["records"][0]["origin"] = "owned"
    elif change == "missing_fingerprint":
        del report["fingerprint_sha256"]
    if change:
        with pytest.raises(module().BootstrapError):
            module().safe_report(json.dumps(report).encode())
    else:
        clean = module().safe_report(json.dumps(report).encode())
        assert clean["records"] == [{"name": "*.dev", "record_id": "record-1", "origin": "created"},
                                    {"name": "management", "record_id": "record-2", "origin": "external"}]
        assert "fixture-secret" not in json.dumps(clean)


def qualify_installed_watchdog(release, python, tmp_path):
    """Use actual installed module bytes, including for the full-bundle probe."""
    observed = tmp_path / "children.json"
    # Existence signals a complete snapshot; never expose a partially written JSON file.
    child = (
        "import json,os,subprocess,sys,time; from pathlib import Path; "
        "p=subprocess.Popen([sys.executable,'-c','import time; time.sleep(60)']); "
        "pending=Path(sys.argv[1]+'.tmp'); "
        "pending.write_text(json.dumps([os.getpid(),p.pid,dict(os.environ)])); "
        "pending.replace(sys.argv[1]); time.sleep(60)"
    )
    parent = (
        "import sys; sys.path.insert(0,sys.argv[1]); "
        "from scripts.ops.nebius_ingress_bootstrap import run_private; "
        "run_private([sys.executable,'-c',sys.argv[2],sys.argv[3]],timeout=50)"
    )
    process = subprocess.Popen([str(python), "-I", "-c", parent, str(release), child, str(observed)],
                               env={**os.environ, "INGRESS_TEST_SECRET": "must-not-inherit"},
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    pids = []
    try:
        deadline = time.monotonic() + 10
        while not observed.exists():
            assert process.poll() is None, "installed supervisor failed to start"
            assert time.monotonic() < deadline, "installed child did not start"
            time.sleep(0.01)
        first, second, environment = json.loads(observed.read_text())
        pids = [first, second]
        assert "INGRESS_TEST_SECRET" not in environment
        process.kill()
        process.wait(timeout=5)
        deadline = time.monotonic() + 5
        while not all(process_exited(pid) for pid in pids):
            assert time.monotonic() < deadline, "operation child outlived dead owner"
            time.sleep(0.01)
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)
        for pid in pids:
            if not process_exited(pid):
                os.kill(pid, signal.SIGKILL)


def test_isolated_installed_supervisor_kills_children_on_owner_death(tmp_path):
    release = tmp_path / "release"
    (release / "scripts/ops").mkdir(parents=True)
    root = Path(__file__).resolve().parents[2]
    for name in ("nebius_ingress_bootstrap.py", "nebius_certificate_gateway.py"):
        (release / "scripts/ops" / name).write_bytes((root / "scripts/ops" / name).read_bytes())
    qualify_installed_watchdog(release, Path(sys.executable), tmp_path)
