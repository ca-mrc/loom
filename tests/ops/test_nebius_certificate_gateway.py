"""Protected certificate transfer is bounded, private and cannot unpack arbitrary paths."""
from __future__ import annotations

import hashlib
import importlib
import io
import json
import os
import stat
import subprocess
import sys
import time
import zipfile
from pathlib import Path

import pytest
from tests.support.process_observation import process_exited


def module():
    return importlib.import_module("scripts.ops.nebius_certificate_gateway")


@pytest.mark.parametrize("command", ["", "python3 -c pass", "loom-nebius-certificate-v1; id",
                                      "loom-nebius-certificate-v1 extra"])
def test_forced_entrypoint_rejects_other_commands_before_reading_input(command, monkeypatch):
    monkeypatch.setenv("SSH_ORIGINAL_COMMAND", command)
    monkeypatch.setattr(sys, "stdin", object())
    assert module().authorized_main("a" * 64) == 126


def test_forced_entrypoint_rejects_unapproved_bundle_before_preparation(tmp_path, monkeypatch):
    from types import SimpleNamespace

    content, root = bundle(tmp_path)
    monkeypatch.setenv("SSH_ORIGINAL_COMMAND", "loom-nebius-certificate-v1")
    monkeypatch.setattr(sys, "stdin", SimpleNamespace(buffer=io.BytesIO(content)))
    assert module().authorized_main("a" * 64) == 126
    assert not root.exists()


def test_forced_entrypoint_accepts_only_the_exact_installed_bundle(tmp_path, monkeypatch, capsys):
    from types import SimpleNamespace

    content, _ = bundle(tmp_path)
    report = {"status": "qualified", "installation_id": "024cfbfb-a7e8-4d85-9c60-c1d838730f9a",
              "fingerprint_sha256": "a" * 64, "generation": "b" * 64,
              "expires_at": "2026-12-02T12:00:00+00:00",
              "sans": ["*.dev.example.test", "management.example.test"]}
    monkeypatch.setenv("SSH_ORIGINAL_COMMAND", "loom-nebius-certificate-v1")
    monkeypatch.setattr(sys, "stdin", SimpleNamespace(buffer=io.BytesIO(content)))
    monkeypatch.setattr(module(), "qualify_bundle", lambda payload: report if payload == content else None)
    assert module().authorized_main(hashlib.sha256(content).hexdigest()) == 0
    assert json.loads(capsys.readouterr().out) == report


def bundle(tmp_path, *, extra=None, altered_hash=False):
    root = tmp_path / "nebius-certificates"
    config = {"state_dir": str(root / "state"), "schema": "loom.nebius-certificate-installation.v1"}
    files = {"uv": b"qualified tool", "requirements.txt": b"certbot==5.8.0 --hash=sha256:fixture\n",
             "scripts/ops/nebius_certificates.py": b"qualified issuer",
             "scripts/ops/nebius_dns_challenge.py": b"qualified hook",
             "scripts/ops/nebius_certificate_gateway.py": b"qualified watchdog",
             "installation.json": json.dumps(config).encode()}
    manifest = {name: hashlib.sha256(value).hexdigest() for name, value in files.items()}
    if altered_hash:
        manifest["uv"] = "0" * 64
    files["manifest.json"] = json.dumps(manifest).encode()
    if extra:
        files.update(extra)
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, content in files.items():
            archive.writestr(name, content)
    return stream.getvalue(), root


def test_invalid_bundle_cannot_create_or_overwrite_files(tmp_path):
    for options in [{"extra": {"../foreign": b"bad"}}, {"extra": {"/tmp/foreign": b"bad"}},
                    {"extra": {"unknown": b"bad"}}, {"altered_hash": True}]:
        content, root = bundle(tmp_path, **options)
        with pytest.raises(module().GatewayError):
            module().prepare_release(content)
        assert not root.exists()


def test_archive_size_limit_checked_before_decompression(tmp_path):
    content, root = bundle(tmp_path, extra={"requirements.txt": b"x" * 262145})
    with pytest.raises(module().GatewayError):
        module().prepare_release(content)
    assert not root.exists()


def test_install_is_content_addressed_private_and_hash_locked(tmp_path, monkeypatch):
    content, root = bundle(tmp_path)
    calls = []

    def command(args, **kwargs):
        calls.append(args)
        if "venv" in args:
            target = root / "releases" / hashlib.sha256(content).hexdigest() / "venv"
            target.mkdir(mode=0o700)
        if "sync" in args:
            assert "--require-hashes" in args and "--only-binary" in args
            assert "--no-config" in args and "--no-cache" in args
        return b""

    monkeypatch.setattr(module(), "run_private", command)
    release, config = module().prepare_release(content)
    assert release.parent == root / "releases"
    assert config == release / "installation.json"
    assert (release / "uv").read_bytes() == b"qualified tool"
    assert len(calls) == 2
    assert module().prepare_release(content) == (release, config)
    assert len(calls) == 2, "completed tooling was reinstalled"
    for path in [root, *root.rglob("*")]:
        assert stat.S_IMODE(path.stat().st_mode) & 0o077 == 0


def test_modified_release_is_not_executed_on_replay(tmp_path, monkeypatch):
    content, _ = bundle(tmp_path)
    monkeypatch.setattr(module(), "run_private", lambda *args, **kwargs: b"")
    release, _ = module().prepare_release(content)
    (release / "uv").write_bytes(b"replaced")
    with pytest.raises(module().GatewayError):
        module().prepare_release(content)


def test_shared_or_symlink_tooling_root_is_refused(tmp_path):
    content, root = bundle(tmp_path)
    root.mkdir(mode=0o755)
    with pytest.raises(module().GatewayError):
        module().prepare_release(content)
    root.rmdir()
    foreign = tmp_path / "foreign"
    foreign.mkdir(mode=0o700)
    root.symlink_to(foreign, target_is_directory=True)
    with pytest.raises(module().GatewayError):
        module().prepare_release(content)
    assert not list(foreign.iterdir())


def test_release_limit_preserves_state_without_deleting_old_tooling(tmp_path):
    content, root = bundle(tmp_path)
    root.mkdir(mode=0o700)
    releases = root / "releases"
    releases.mkdir(mode=0o700)
    for number in range(8):
        (releases / str(number)).mkdir(mode=0o700)
    with pytest.raises(module().GatewayError):
        module().prepare_release(content)
    assert len(list(releases.iterdir())) == 8


def test_report_does_not_forward_remote_payloads():
    value = {"status": "qualified", "installation_id": "024cfbfb-a7e8-4d85-9c60-c1d838730f9a",
             "fingerprint_sha256": "a" * 64, "generation": "b" * 64,
             "expires_at": "2026-12-02T12:00:00+00:00", "private": "private-key",
             "sans": ["*.dev.example.test", "management.example.test"]}
    assert "private-key" not in json.dumps(module().safe_report(json.dumps(value).encode()))
    value["fingerprint_sha256"] = "private-key"
    with pytest.raises(module().GatewayError):
        module().safe_report(json.dumps(value).encode())


@pytest.mark.parametrize("gateway_name", ["nebius_certificate_gateway", "nebius_development_management_certificate_gateway"])
def test_private_command_has_no_ambient_credentials_or_public_logs(tmp_path, monkeypatch, gateway_name):
    import sys

    monkeypatch.setenv("PRIVATE_CREDENTIAL", "private-key")
    path = tmp_path / "child.json"
    gateway = importlib.import_module("scripts.ops." + gateway_name)
    result = gateway.run_private([sys.executable, "-c", "import os,json,sys; "
                                  "open(sys.argv[1],'w').write(json.dumps(dict(os.environ))); print('ok')", str(path)], timeout=10)
    assert result.strip() == b"ok"
    assert "private-key" not in path.read_text()
    assert os.stat(path).st_mode & 0o077 == 0


@pytest.mark.parametrize("termination", ["outer_timeout", "owner_sigkill"])
@pytest.mark.parametrize("gateway_name", ["nebius_certificate_gateway", "nebius_development_management_certificate_gateway"])
def test_owner_death_or_outer_timeout_does_not_leave_detached_certificate_client(tmp_path, termination, gateway_name):
    pid = tmp_path / "certificate.pid"
    script = (
        "import subprocess,sys; from pathlib import Path; "
        "from scripts.ops.nebius_certificates import _run_client,_locked_state; "
        "root=Path(sys.argv[1]); "
        "\nwith _locked_state(root):\n"
        " _run_client([sys.executable,'-c',"
        "\"import os,sys,time; open(sys.argv[1],'w').write(str(os.getpid())); time.sleep(90)\",sys.argv[2]],"
        "timeout=80,start_new_session=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)"
    )
    root = tmp_path / "state"
    args = [sys.executable, "-c", script, str(root), str(pid)]
    worker = None
    if termination == "outer_timeout":
        with pytest.raises(subprocess.TimeoutExpired):
            importlib.import_module("scripts.ops." + gateway_name).run_private(args, timeout=2)
    else:
        worker = subprocess.Popen(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        for _ in range(200):
            if pid.exists():
                break
            assert worker.poll() is None, "issuer failed to start"
            time.sleep(0.01)
        worker.kill()
        worker.wait(timeout=5)
    assert pid.exists(), "substitute certificate client was never started"
    client_pid = int(pid.read_text())
    try:
        for _ in range(200):
            if process_exited(client_pid):
                break
            time.sleep(0.01)
        else:
            pytest.fail("certificate client outlived terminated issuer")
    finally:
        # Also clean the intentionally reproduced pre-fix orphan.
        try:
            os.killpg(client_pid, 9)
        except ProcessLookupError:
            pass


@pytest.mark.parametrize("error", [FileNotFoundError, ProcessLookupError])
def test_process_exit_observation_handles_proc_disappearing_during_read(monkeypatch, error):
    monkeypatch.setattr(Path, "exists", lambda self: True)

    def disappeared(path, *args, **kwargs):
        assert str(path) == "/proc/123456/stat"
        raise error()

    monkeypatch.setattr(Path, "read_text", disappeared)
    assert process_exited(123456)


def test_process_exit_observation_does_not_hide_live_process_or_read_errors(monkeypatch):
    monkeypatch.setattr(Path, "exists", lambda self: True)
    monkeypatch.setattr(Path, "read_text", lambda self: "123456 (python) S 1 2 3")
    assert not process_exited(123456)

    def denied(path, *args, **kwargs):
        raise PermissionError()

    monkeypatch.setattr(Path, "read_text", denied)
    with pytest.raises(PermissionError):
        process_exited(123456)
