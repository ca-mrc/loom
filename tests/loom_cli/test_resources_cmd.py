"""`loom resources status` exposes the Monitor slot summary for scripts."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import httpx
import pytest

from loom_cli.__main__ import main

_SUMMARY = {
    "resources": {
        "aggregate": {
            "current_active_slots": 12,
            "active_workers": 2,
            "draining_workers": 1,
            "total_slots": 12,
            "draining_slots": 2,
            "occupied_slots": 2,
            "free_slots": 10,
            "running_tasks": 1,
            "starting_tasks": 1,
            "queued_tasks": 1,
        },
        "pools": [
            {
                "pool_name": "local-arm",
                "backend": "docker",
                "cpu_arch": "arm64",
                "current_active_slots": 10,
                "active_workers": 1,
                "draining_workers": 1,
                "total_slots": 10,
                "draining_slots": 2,
                "occupied_slots": 0,
                "free_slots": 10,
                "running_tasks": 0,
                "starting_tasks": 0,
                "queued_tasks": 1,
            },
            {
                "pool_name": "staging-x86",
                "backend": "docker",
                "cpu_arch": "x86_64",
                "current_active_slots": 2,
                "active_workers": 1,
                "draining_workers": 0,
                "total_slots": 2,
                "draining_slots": 0,
                "occupied_slots": 2,
                "free_slots": 0,
                "running_tasks": 1,
                "starting_tasks": 1,
                "queued_tasks": 1,
            },
        ],
    },
    "queue": {
        "queued": 1,
        "claimed": 1,
        "running": 1,
        "waiting": 2,
        "active_workers": 2,
        "available_backends": ["docker"],
        "has_default_backend": True,
        "status": "waiting",
    },
}


class _MockServer:
    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []
        self.summary: dict[str, Any] = _SUMMARY


@pytest.fixture
def _logged_in(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path))
    monkeypatch.setenv("LOOM_TEST_TOKEN", "loom_api_resources")
    main([
        "auth",
        "login",
        "--server",
        "https://loom.test",
        "--token",
        "env:LOOM_TEST_TOKEN",
    ])


@pytest.fixture
def mock_resources_server(
    monkeypatch: pytest.MonkeyPatch,
    _logged_in: None,
) -> _MockServer:
    import loom_cli.resources_cmd as resources_cmd

    server = _MockServer()

    def _handler(request: httpx.Request) -> httpx.Response:
        server.requests.append(request)
        if request.method == "GET" and request.url.path == "/api/v1/monitor/summary":
            return httpx.Response(200, json=server.summary)
        return httpx.Response(404, json={"detail": "not found"})

    transport = httpx.MockTransport(_handler)

    def _patched(cfg: Any, *, timeout: float = 30.0) -> httpx.Client:
        return httpx.Client(
            base_url=cfg.server_url,
            headers={"Authorization": f"Bearer {cfg.auth_token}"},
            transport=transport,
            timeout=timeout,
        )

    monkeypatch.setattr(resources_cmd, "authed_client", _patched)
    return server


def test_resources_status_json_matches_monitor_resource_summary(
    mock_resources_server: _MockServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    rc = main(["resources", "status", "--json"])

    assert rc == 0
    assert json.loads(capsys.readouterr().out) == _SUMMARY["resources"]
    req = mock_resources_server.requests[0]
    assert req.url.path == "/api/v1/monitor/summary"
    assert req.url.params["view"] == "trials"


def test_resources_status_text_shows_slots_and_pool_breakdown(
    mock_resources_server: _MockServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    rc = main(["resources", "status"])

    assert rc == 0
    out = capsys.readouterr().out
    assert "Concurrent tasks: 2 / 12" in out
    assert "Running: 1" in out
    assert "Starting: 1" in out
    assert "Queued: 1" in out
    assert "Draining: 2 slots / 1 workers" in out
    assert "local-arm" in out
    assert "0/10" in out
    assert "staging-x86" in out
    assert "2/2" in out
    assert "Autoscaler" not in out
    assert "desired" not in out


def _native_target(target_id: str, **overrides: Any) -> dict[str, Any]:
    return {
        "target_id": target_id,
        "capacity_owner_target_id": target_id,
        "region": "eu-north1",
        "pool_id": "nebius-cpu",
        "desired_state": "active",
        "policy": {"max_nodes": 8},
        "observation": {
            "is_fresh": True,
            "active_nodes": 3,
            "occupied_nodes": 2,
            "pending_jobs": 1,
        },
        **overrides,
    }


_NATIVE_SUMMARY = {
    "resources": {"aggregate": {"current_active_slots": 0, "occupied_slots": 0}, "pools": []},
    "progress": {
        "stages": {
            "image_preparation": 4,
            "execution_wait": 3,
            "starting": 2,
            "running": 20,
            "archiving": 1,
        },
    },
    "service_execution": {
        "targets": [
            _native_target("owner"),
            # Shares owner's nodes: one row, not double-counted capacity.
            _native_target("web", capacity_owner_target_id="owner", observation={}),
            _native_target("old", region="eu-west1", desired_state="retired"),
        ],
    },
}


def test_resources_status_text_reports_native_trials_not_empty_worker_slots(
    mock_resources_server: _MockServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    mock_resources_server.summary = _NATIVE_SUMMARY

    assert main(["resources", "status"]) == 0

    out = capsys.readouterr().out
    assert "Running: 20 · Starting: 2 · Waiting: 3 · Preparing images: 4 · Archiving: 1" in out
    assert out.count("nebius-cpu") == 1
    assert "eu-west1" not in out
    row = next(line for line in out.splitlines() if "nebius-cpu" in line).split()
    assert row == ["eu-north1", "nebius-cpu", "3", "2", "8", "1", "fresh"]
    assert "Concurrent tasks" not in out
    assert "no active resource pools" not in out


def test_resources_status_text_keeps_worker_pools_beside_native_capacity(
    mock_resources_server: _MockServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    mock_resources_server.summary = {**_SUMMARY, **{
        key: _NATIVE_SUMMARY[key] for key in ("progress", "service_execution")
    }}

    assert main(["resources", "status"]) == 0

    out = capsys.readouterr().out
    assert "Native execution:" in out
    assert "Concurrent tasks: 2 / 12" in out


def test_resources_status_json_adds_native_capacity(
    mock_resources_server: _MockServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    mock_resources_server.summary = _NATIVE_SUMMARY

    assert main(["resources", "status", "--json"]) == 0

    body = json.loads(capsys.readouterr().out)
    assert body["pools"] == []
    assert body["service_execution"] == _NATIVE_SUMMARY["service_execution"]
    assert body["progress"]["stages"]["running"] == 20
