"""`loom tasksets` CLI against a mocked server (httpx MockTransport)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import httpx
import pytest

from loom_cli.__main__ import main

_MANIFEST_YAML = """
apiVersion: loom.taskset/v1
kind: UserTaskSet
metadata:
  name: sample-tasks
  display_name: Sample Tasks
source:
  type: https
  locator: https://example.com/data.jsonl
instance_mapping:
  prompt: row.question
  task_id: row.id
task_template:
  task:
    id: "{{ instance.task_id }}"
    name: "{{ metadata.display_name }}"
  environment:
    os: linux
  agent:
    name: default
  steps:
    - artifacts: [out.txt]
"""

_BUNDLE_UPLOAD_MANIFEST_YAML = """
apiVersion: loom.taskset/v1
kind: UserTaskSet
metadata:
  name: bundle-tasks
  display_name: Bundle Tasks
intents:
  - evaluation
source:
  type: bundle-upload
  locator: bundle.tar.gz
  subset: tasks
"""

_SUBMIT_RESPONSE = {
    "task_set_id": "ts/team-uuid/sample-tasks",
    "status": "materializing",
    "intents": ["trajectory_generation"],
    "manifest_intents": ["trajectory_generation"],
    "inferred_intents": [],
    "capabilities": ["trajectory-only"],
    "warnings": [],
    "evaluation_ready": False,
    "task_count": 0,
    "materialization_job_id": "job-1",
}

_LIST_RESPONSE = {
    "items": [
        {
            "task_set_id": "ts/team-uuid/sample-tasks",
            "display_name": "Sample Tasks",
            "status": "materializing",
            "intents": ["trajectory_generation"],
            "evaluation_ready": False,
            "task_count": 0,
            "created_at": "2026-07-02T12:00:00+00:00",
        },
    ],
}

_STATUS_RESPONSE = {
    **_SUBMIT_RESPONSE,
    "status_reason": None,
    "error_summary": [],
    "materialization_job_state": "queued",
    "materialization_fence": {
        "lease_epoch": 4,
        "lease_heartbeat_at": "2026-07-10T00:00:00Z",
        "lease_heartbeat_state": "fresh",
        "owner_fingerprint": "sha256:0123456789ab",
        "published_generation": 4,
    },
}


@pytest.fixture(autouse=True)
def _isolated_logged_in_config(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path))
    monkeypatch.setenv("MY_TOK", "loom_admin_test123456")
    main([
        "auth", "login",
        "--server", "https://loom.test",
        "--token", "env:MY_TOK",
    ])


class MockServer:
    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []
        self.canned: dict[tuple[str, str], httpx.Response] = {}

    def __getitem__(self, idx: int) -> httpx.Request:
        return self.requests[idx]


@pytest.fixture
def mock_server(monkeypatch: pytest.MonkeyPatch) -> MockServer:
    server = MockServer()

    def _handler(request: httpx.Request) -> httpx.Response:
        server.requests.append(request)
        key = (request.method, request.url.path)
        if key in server.canned:
            return server.canned[key]
        return httpx.Response(404, json={"detail": f"no mock for {key}"})

    transport = httpx.MockTransport(_handler)

    def _patched_authed_client(cfg: Any, *, timeout: float = 30.0) -> httpx.Client:
        return httpx.Client(
            base_url=cfg.server_url,
            headers={"Authorization": f"Bearer {cfg.auth_token}"},
            transport=transport,
            timeout=timeout,
        )

    monkeypatch.setattr(
        "loom_cli.tasksets_cmd.authed_client", _patched_authed_client,
    )
    return server


def _write_bundle(tmp_path: Path, *, with_verifier: bool = False) -> Path:
    bundle = tmp_path / "my-taskset"
    bundle.mkdir()
    (bundle / "manifest.yaml").write_text(_MANIFEST_YAML, encoding="utf-8")
    if with_verifier:
        manifest = _MANIFEST_YAML.replace(
            "task_template:",
            "verifier:\n  type: pytest\n  file: verifier/test.py\ntask_template:",
            1,
        )
        (bundle / "manifest.yaml").write_text(manifest, encoding="utf-8")
        vdir = bundle / "verifier"
        vdir.mkdir()
        (vdir / "test.py").write_text("def test_x(): pass", encoding="utf-8")
    return bundle


def _write_bundle_upload(tmp_path: Path, *, include_bundle: bool = True) -> Path:
    bundle = tmp_path / "bundle-upload"
    bundle.mkdir()
    (bundle / "manifest.yaml").write_text(
        _BUNDLE_UPLOAD_MANIFEST_YAML,
        encoding="utf-8",
    )
    if include_bundle:
        (bundle / "bundle.tar.gz").write_bytes(b"not a real tar for cli")
    return bundle


def test_submit_sends_manifest_only(mock_server: MockServer, tmp_path: Path) -> None:
    bundle = _write_bundle(tmp_path)
    mock_server.canned[("POST", "/api/v1/tasksets")] = httpx.Response(
        202, json=_SUBMIT_RESPONSE,
    )
    rc = main(["tasksets", "submit", str(bundle)])
    assert rc == 0
    assert len(mock_server.requests) == 1
    req = mock_server.requests[0]
    assert req.method == "POST"
    assert req.url.path == "/api/v1/tasksets"
    assert b"manifest.yaml" in req.content


def test_submit_sends_verifier_when_present(
    mock_server: MockServer, tmp_path: Path,
) -> None:
    bundle = _write_bundle(tmp_path, with_verifier=True)
    mock_server.canned[("POST", "/api/v1/tasksets")] = httpx.Response(
        202, json=_SUBMIT_RESPONSE,
    )
    rc = main(["tasksets", "submit", str(bundle)])
    assert rc == 0
    assert b"verifier/test.py" in mock_server.requests[0].content


def test_submit_sends_bundle_upload_part(
    mock_server: MockServer, tmp_path: Path,
) -> None:
    bundle = _write_bundle_upload(tmp_path)
    mock_server.canned[("POST", "/api/v1/tasksets")] = httpx.Response(
        202, json={**_SUBMIT_RESPONSE, "task_set_id": "ts/team-uuid/bundle-tasks"},
    )
    rc = main(["tasksets", "submit", str(bundle)])
    assert rc == 0
    content = mock_server.requests[0].content
    assert b'name="bundle"' in content
    assert b"bundle.tar.gz" in content


def test_submit_bundle_upload_requires_bundle_file(tmp_path: Path) -> None:
    bundle = _write_bundle_upload(tmp_path, include_bundle=False)
    rc = main(["tasksets", "submit", str(bundle)])
    assert rc == 1


def test_submit_missing_manifest_exits_1(tmp_path: Path) -> None:
    empty = tmp_path / "empty"
    empty.mkdir()
    rc = main(["tasksets", "submit", str(empty)])
    assert rc == 1


def test_list_json(mock_server: MockServer, capsys: pytest.CaptureFixture[str]) -> None:
    mock_server.canned[("GET", "/api/v1/tasksets")] = httpx.Response(
        200, json=_LIST_RESPONSE,
    )
    rc = main(["tasksets", "list", "--format", "json"])
    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert len(out["items"]) == 1


def test_status_by_slug_prints_only_safe_fence_fields(
    mock_server: MockServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    mock_server.canned[("GET", "/api/v1/tasksets")] = httpx.Response(
        200, json=_LIST_RESPONSE,
    )
    raw_owner = "cli-raw-owner-must-not-print"
    mock_server.canned[("GET", "/api/v1/tasksets/ts/team-uuid/sample-tasks")] = (
        httpx.Response(200, json={**_STATUS_RESPONSE, "claimed_by": raw_owner})
    )
    rc = main(["tasksets", "status", "sample-tasks"])
    assert rc == 0
    output = capsys.readouterr().out
    assert "fence_lease_epoch:           4" in output
    assert "fence_lease_heartbeat_at:    2026-07-10T00:00:00Z" in output
    assert "fence_lease_heartbeat_state: fresh" in output
    assert "fence_owner_fingerprint:     sha256:0123456789ab" in output
    assert "fence_published_generation:  4" in output
    assert raw_owner not in output
    assert "claimed_by" not in output
    methods_paths = [(r.method, r.url.path) for r in mock_server.requests]
    assert ("GET", "/api/v1/tasksets") in methods_paths
    assert ("GET", "/api/v1/tasksets/ts/team-uuid/sample-tasks") in methods_paths


def test_status_json_preserves_safe_api_body(
    mock_server: MockServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    mock_server.canned[("GET", "/api/v1/tasksets")] = httpx.Response(
        200, json=_LIST_RESPONSE,
    )
    mock_server.canned[("GET", "/api/v1/tasksets/ts/team-uuid/sample-tasks")] = (
        httpx.Response(200, json=_STATUS_RESPONSE)
    )

    rc = main(["tasksets", "status", "sample-tasks", "--format", "json"])

    assert rc == 0
    assert json.loads(capsys.readouterr().out) == _STATUS_RESPONSE


def test_rebuild_by_full_id(mock_server: MockServer) -> None:
    mock_server.canned[("POST", "/api/v1/tasksets/ts/team-uuid/sample-tasks/rebuild")] = (
        httpx.Response(202, json=_SUBMIT_RESPONSE)
    )
    rc = main(["tasksets", "rebuild", "ts/team-uuid/sample-tasks"])
    assert rc == 0
    assert mock_server.requests[0].method == "POST"


def test_delete_by_full_id(mock_server: MockServer) -> None:
    mock_server.canned[("DELETE", "/api/v1/tasksets/ts/team-uuid/sample-tasks")] = (
        httpx.Response(204)
    )
    rc = main(["tasksets", "delete", "ts/team-uuid/sample-tasks"])
    assert rc == 0
    assert mock_server.requests[0].method == "DELETE"


def test_not_logged_in_exits_2(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "empty"))
    rc = main(["tasksets", "list"])
    assert rc == 2


def test_lifecycle_preserves_unspecified_fields_and_sends_precondition(mock_server, capsys) -> None:
    path = "/api/v1/tasksets/ts/team-uuid/sample-tasks"
    current = {**_STATUS_RESPONSE, "purpose": "original", "expires_at": "2026-10-06T12:00:00Z",
               "hold": False, "updated_at": "2026-09-29T12:00:00Z"}
    mock_server.canned[("GET", path)] = httpx.Response(200, json=current)
    mock_server.canned[("PATCH", path + "/lifecycle")] = httpx.Response(200, json={
        "task_set_id": current["task_set_id"], "purpose": "original",
        "expires_at": None, "hold": True, "updated_at": "2026-09-29T12:01:00Z",
    })
    assert main(["tasksets", "lifecycle", current["task_set_id"], "--no-expiry", "--hold"]) == 0
    assert json.loads(mock_server.requests[-1].content) == {
        "purpose": "original", "expires_at": None, "hold": True,
        "expected_updated_at": current["updated_at"],
    }
    assert '"hold": true' in capsys.readouterr().out
