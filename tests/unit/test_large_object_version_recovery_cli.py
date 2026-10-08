"""Installed operator bindings and explicit, bounded command modes."""
from __future__ import annotations

import importlib
import json
import subprocess
import sys
from types import SimpleNamespace

import pytest

from loom.db.schema_startup import service_schema_head
from loom_control_plane.object_version_recovery import (
    RecoveryConflictError,
    SingleObjectRecoveryRequest,
)
from tests.unit.test_single_object_version_recovery import operator_payload


@pytest.fixture
def platform(tmp_path, monkeypatch):
    monkeypatch.setenv("LOOM_ENV", "development")
    monkeypatch.setenv("LOOM_NAMESPACE", "loom")
    (tmp_path / "environment.json").write_text(json.dumps({"environment": "development", "namespace": "loom"}))
    (tmp_path / "profile.json").write_text(json.dumps({"candidate_sha": "c" * 40}))
    return tmp_path


def request(**changes):
    return SingleObjectRecoveryRequest.model_validate({
        **operator_payload(), "schema_head": service_schema_head(), **changes,
    })


def target():
    return importlib.import_module("loom_control_plane.large_object_version_recovery")


@pytest.mark.parametrize("damage", ["candidate", "schema", "namespace", "environment", "missing", "malformed"])
def test_platform_drift_refuses(platform, damage):
    document = request()
    if damage == "schema":
        document = request(schema_head="0001")
    elif damage == "candidate":
        (platform / "profile.json").write_text(json.dumps({"candidate_sha": "d" * 40}))
    elif damage == "missing":
        (platform / "profile.json").unlink()
    elif damage == "malformed":
        (platform / "profile.json").write_text('[]')
    else:
        environment = json.loads((platform / "environment.json").read_text())
        environment[damage] = "foreign"
        (platform / "environment.json").write_text(json.dumps(environment))
    with pytest.raises(RecoveryConflictError, match="platform_binding_changed"):
        target().qualify_platform(document, platform)


def test_platform_accepts_current_binding_and_later_candidate_readback(platform):
    target().qualify_platform(request(), platform)
    (platform / "profile.json").write_text(json.dumps({"candidate_sha": "d" * 40}))
    target().qualify_platform(request(), platform, readback=True)
    (platform / "environment.json").write_text(json.dumps({"environment": "foreign", "namespace": "loom"}))
    with pytest.raises(RecoveryConflictError, match="platform_binding_changed"):
        target().qualify_platform(request(), platform, readback=True)


@pytest.mark.parametrize("apply,readback,document_apply", [
    (True, False, False), (False, False, True), (False, True, False),
])
def test_cli_mode_mismatch_precedes_live_settings(platform, monkeypatch, capsys, apply, readback, document_apply):
    module = target()
    payload = request(apply=document_apply, plan_sha256="sha256:" + "e" * 64).model_dump_json()
    arguments = ["--platform", str(platform), "--request-json", payload]
    if apply:
        arguments.append("--apply")
    if readback:
        arguments.append("--readback")
    monkeypatch.setattr(module, "ControlPlaneSettings", lambda: pytest.fail("must reject before loading credentials"))
    assert module.main(arguments) == 1
    assert json.loads(capsys.readouterr().out) == {"status": "blocked", "reason": "command_mode_conflict"}


@pytest.mark.parametrize("mode", ["preview", "apply", "readback"])
def test_cli_dispatches_only_explicit_mode(platform, monkeypatch, capsys, mode):
    module = target()
    payload = request(apply=mode != "preview", plan_sha256="sha256:" + "e" * 64).model_dump_json()
    seen = []

    async def run(document, settings, *, platform, readback):
        seen.append((document.apply, readback, settings))
        return {"status": mode}

    settings = SimpleNamespace()
    monkeypatch.setattr(module, "ControlPlaneSettings", lambda: settings)
    monkeypatch.setattr(module, "run_recovery", run)
    arguments = ["--platform", str(platform), "--request-json", payload]
    if mode != "preview":
        arguments.append("--" + mode)
    assert module.main(arguments) == 0
    assert seen == [(mode != "preview", mode == "readback", settings)]
    assert json.loads(capsys.readouterr().out) == {"status": mode}


@pytest.mark.parametrize("raw", ["{", "x" * (16384 + 1)], ids=["invalid_json", "oversized"])
def test_cli_bounds_and_validates_request_without_echo(platform, monkeypatch, capsys, raw):
    module = target()
    monkeypatch.setattr(module, "ControlPlaneSettings", lambda: pytest.fail("must reject before loading credentials"))
    assert module.main(["--platform", str(platform), "--request-json", raw]) == 1
    result = json.loads(capsys.readouterr().out)
    assert result == {"status": "blocked", "reason": "request_size" if len(raw) > 16384 else "invalid_request"}


@pytest.mark.parametrize("error", [RuntimeError("postgres://secret"), RecoveryConflictError("operator_team_conflict")])
def test_cli_emits_only_closed_error_codes(platform, monkeypatch, capsys, error):
    module = target()

    async def run(*args, **kwargs):
        raise error

    monkeypatch.setattr(module, "ControlPlaneSettings", SimpleNamespace)
    monkeypatch.setattr(module, "run_recovery", run)
    assert module.main(["--platform", str(platform), "--request-json", request().model_dump_json()]) == 1
    output = capsys.readouterr()
    assert "secret" not in output.out + output.err and "Traceback" not in output.out + output.err
    assert json.loads(output.out) == {"status": "blocked", "reason": (
        "operator_team_conflict" if isinstance(error, RecoveryConflictError) else "recovery_incomplete")}


def test_dedicated_process_hard_deadline_terminates_stuck_reader(platform):
    script = '''
import asyncio
import sys
from loom_control_plane import large_object_version_recovery as target
target.HARD_TIMEOUT = 1
target.ControlPlaneSettings = lambda: None
async def stuck(*args, **kwargs):
    await asyncio.to_thread(__import__('time').sleep, 60)
target.run_recovery = stuck
raise SystemExit(target.main(sys.argv[1:]))
'''
    result = subprocess.run(
        [sys.executable, "-c", script, "--platform", str(platform), "--request-json", request().model_dump_json()],
        capture_output=True, text=True, timeout=15,
    )
    assert result.returncode == 124
    assert result.stdout == result.stderr == ""
