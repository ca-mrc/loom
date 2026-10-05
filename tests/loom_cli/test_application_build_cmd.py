"""Daily personal builds preserve captured source and uncertain-write identity."""
from __future__ import annotations

import io
import json
import shlex
import tarfile

import httpx
import pytest

from loom_cli.__main__ import main
from loom_cli.config import LoomConfig, save_config
from loom_cli.contexts import selected_context
from tests.loom_cli.test_application_source import commit
from tests.loom_cli.test_application_source import repo as repo
from tests.loom_cli.test_application_source_client import UPLOAD
from tests.loom_cli.test_application_source_client import source_http as source_http

BUILD = "50000000-0000-4000-8000-000000000001"


def status(phase="queued", **changes):
    release = None
    if phase == "ready":
        release = {"release_id": BUILD, "source_digest": "sha256:" + "a" * 64, "schema_revision": "0173",
            "service_image_ref": "cr.eu-north1.nebius.cloud/test/app@sha256:" + "b" * 64,
            "web_image_ref": "cr.eu-north1.nebius.cloud/test/app@sha256:" + "c" * 64}
    return {"build_id": BUILD, "upload_id": UPLOAD, "attempt": 1, "phase": phase, "desired_state": "running",
        "source_digest": "sha256:" + "a" * 64, "recipe_digest": "sha256:" + "d" * 64,
        "created_at": "2026-10-02T00:00:00Z", "release": release, **changes}


def source_handlers(handlers, *, fail=None):
    saved = {}

    def prepare(request):
        saved.update(json.loads(request.content), upload_id=UPLOAD, phase="awaiting_source",
            expires_at="2026-12-01T00:00:00Z")
        if fail == "prepare":
            raise httpx.ReadTimeout("lost private upstream response")
        return httpx.Response(201, json=saved)

    def upload(request):
        saved["phase"] = "source_verified"
        if fail == "upload":
            raise httpx.ReadTimeout("lost private upstream response")
        return httpx.Response(200, json=saved)

    def build(request):
        if fail == "build":
            raise httpx.ReadTimeout("lost private upstream response")
        return httpx.Response(201, json=status(source_digest=saved["source_digest"]))

    handlers["POST", "/api/v1/application-sources"] = prepare
    handlers["PUT", f"/api/v1/application-sources/{UPLOAD}/content"] = upload
    handlers["GET", f"/api/v1/application-sources/{UPLOAD}"] = lambda _: httpx.Response(200, json=saved)
    handlers["POST", "/api/v1/application-builds"] = build
    return saved


def test_build_captures_dirty_and_untracked_source_then_queues_no_deployment(repo, source_http, capsys):
    handlers, requests = source_http
    source_handlers(handlers)
    (repo / "app.py").write_text("committed")
    commit(repo)
    (repo / "app.py").write_text("dirty")
    (repo / "feature.py").write_text("untracked")
    assert main(["dev", "app", "build", "--source", str(repo), "--idempotency-key", "feature-one"]) == 0
    assert [r.method for r in requests] == ["POST", "PUT", "POST"]
    assert requests[0].headers["Idempotency-Key"] == requests[2].headers["Idempotency-Key"] == "feature-one"
    assert json.loads(requests[2].content) == {"upload_id": UPLOAD}
    with tarfile.open(fileobj=io.BytesIO(requests[1].content)) as archive:
        files = {row.name: archive.extractfile(row).read() for row in archive.getmembers() if row.isfile()}
    assert b"dirty" in files.values() and b"untracked" in files.values()
    output = capsys.readouterr()
    assert json.loads(output.out)["build_id"] == BUILD
    assert "--source-digest" in output.err and f"--upload-id {UPLOAD}" in output.err
    assert "not CI-approved" in output.err


@pytest.mark.parametrize("stage", ["prepare", "upload", "build"])
def test_unknown_build_requests_print_replay_and_never_resubmit_changed_source(repo, source_http, capsys, stage):
    handlers, requests = source_http
    saved = source_handlers(handlers, fail=stage)
    (repo / "app.py").write_text("first")
    assert main(["dev", "app", "build", "--source", str(repo), "--idempotency-key", "same-key"]) == 1
    output = capsys.readouterr()
    assert "lost private upstream" not in output.err
    assert len(requests) == {"prepare": 1, "upload": 2, "build": 3}[stage]
    commands = [shlex.split(line.removeprefix("Retry: "))[1:] for line in output.err.splitlines() if line.startswith("Retry: ")]
    assert commands and "same-key" in commands[-1]
    requests.clear()
    (repo / "app.py").write_text("second")
    # The earlier source-bound command cannot silently reinterpret this retry.
    assert main(commands[0]) == 1
    assert requests == []
    if stage == "build":
        assert "--source" not in commands[-1] and "--upload-id" in commands[-1]
        handlers["POST", "/api/v1/application-builds"] = lambda _: httpx.Response(201,
            json=status(source_digest=saved["source_digest"]))
        assert main(commands[-1]) == 0
        assert [request.method for request in requests] == ["GET", "POST"]
        assert json.loads(requests[-1].content) == {"upload_id": UPLOAD}
        assert requests[-1].headers["Idempotency-Key"] == "same-key"


@pytest.mark.parametrize("phase,code", [("queued", 2), ("running", 2), ("settling", 2), ("ready", 0), ("failed", 1), ("cancelled", 1)])
def test_build_wait_is_read_only_and_timeout_does_not_cancel(source_http, capsys, phase, code):
    handlers, requests = source_http
    handlers["GET", f"/api/v1/application-builds/{BUILD}"] = lambda _: httpx.Response(200, json=status(phase))
    assert main(["dev", "app", "build-wait", BUILD, "--timeout", "0"]) == code
    assert [request.method for request in requests] == ["GET"]
    assert json.loads(capsys.readouterr().out)["phase"] == phase


@pytest.mark.parametrize("command,phase,attempt", [("build-cancel", "running", 1), ("build-retry", "queued", 2)])
def test_build_generation_controls_print_exact_replay(source_http, capsys, command, phase, attempt):
    handlers, requests = source_http
    action = command.removeprefix("build-")
    handlers["POST", f"/api/v1/application-builds/{BUILD}/{action}"] = lambda _: httpx.Response(202,
        json=status(phase, attempt=attempt))
    assert main(["dev", "app", command, BUILD, "--attempt", "1"]) == 0
    assert len(requests) == 1 and json.loads(requests[0].content) == {"attempt": 1}
    assert f"{command} {BUILD} --attempt 1" in capsys.readouterr().err


@pytest.mark.parametrize("damage", [{"build_id": UPLOAD}, {"release": None}, {"source_digest": "sha256:" + "f" * 64}])
def test_status_rejects_wrong_build_or_unqualified_ready_response(source_http, capsys, damage):
    handlers, _ = source_http
    handlers["GET", f"/api/v1/application-builds/{BUILD}"] = lambda _: httpx.Response(200, json=status("ready", **damage))
    assert main(["dev", "app", "build-status", BUILD]) == 1
    assert not capsys.readouterr().out


def test_upload_only_replay_preserves_selected_context_and_leading_hyphen_key(repo, source_http, capsys):
    handlers, requests = source_http
    saved = source_handlers(handlers, fail="build")
    (repo / "app.py").write_text("feature")
    with selected_context("feature-manager"):
        save_config(LoomConfig(server_url="https://manage.example.com", auth_token="management-secret"))
    args = ["--context", "feature-manager", "dev", "app", "build", "--source", str(repo), "--idempotency-key=-feature"]
    assert main(args) == 1
    retry = [shlex.split(line.removeprefix("Retry: "))[1:] for line in capsys.readouterr().err.splitlines()
        if line.startswith("Retry: ")][-1]
    assert retry[:2] == ["--context", "feature-manager"]
    handlers["POST", "/api/v1/application-builds"] = lambda _: httpx.Response(201, json=status(source_digest=saved["source_digest"]))
    requests.clear()
    assert main(retry) == 0
    assert requests[-1].headers["Idempotency-Key"] == "-feature"


@pytest.mark.parametrize("damage", [{"upload_id": BUILD}, {"source_digest": "sha256:" + "f" * 64}])
def test_build_response_cannot_substitute_verified_upload(repo, source_http, capsys, damage):
    handlers, _ = source_http
    saved = source_handlers(handlers)
    handlers["POST", "/api/v1/application-builds"] = lambda _: httpx.Response(201,
        json=status(source_digest=saved["source_digest"]) | damage)
    (repo / "app.py").write_text("feature")
    assert main(["dev", "app", "build", "--source", str(repo)]) == 1
    assert not capsys.readouterr().out


@pytest.mark.parametrize("command,attempt", [("build-cancel", 2), ("build-retry", 1)])
def test_generation_control_rejects_another_attempt(source_http, capsys, command, attempt):
    handlers, requests = source_http
    handlers["POST", f"/api/v1/application-builds/{BUILD}/{command.removeprefix('build-')}"] = lambda _: httpx.Response(202,
        json=status(attempt=attempt))
    assert main(["dev", "app", command, BUILD, "--attempt", "1"]) == 1
    assert len(requests) == 1 and not capsys.readouterr().out
