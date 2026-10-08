"""Owner provisioning CLI sends explicit remote requests and never falls back."""

from __future__ import annotations

import json
from uuid import UUID

import httpx
import pytest

from loom_cli.__main__ import main
from loom_cli.config import LoomConfig, save_config

CANDIDATE = "10000000-0000-4000-8000-000000000001"
ENVIRONMENT = "20000000-0000-4000-8000-000000000001"
OPERATION = "30000000-0000-4000-8000-000000000001"


def test_top_level_help_recommends_application_workflow(capsys):
    with pytest.raises(SystemExit) as result:
        main(["--help"])
    assert result.value.code == 0
    assert "loom dev app" in " ".join(capsys.readouterr().out.split())


def test_dev_help_puts_recommended_app_before_explicit_legacy_controls(capsys):
    with pytest.raises(SystemExit) as result:
        main(["dev", "--help"])
    assert result.value.code == 0
    output = capsys.readouterr().out
    assert "Recommended: loom dev app" in " ".join(output.split())
    command_rows = [line.strip() for line in output.splitlines() if line.startswith("    ")]
    assert command_rows[0].startswith("app ")
    for command in ("create", "list", "destroy", "login", "status", "retry", "wait"):
        assert any(row.startswith(f"{command} ") and "Legacy" in row for row in command_rows)


@pytest.fixture
def management_http(monkeypatch, tmp_path):
    from loom_cli import environment_client
    from loom_cli.server_client import authed_client

    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path))
    save_config(LoomConfig(server_url="https://manage.example.com", auth_token="test-management-token"))
    responses = {}
    requests = []

    def handle(request):
        requests.append(request)
        value = responses.get((request.method, request.url.path))
        if value is None:
            return httpx.Response(404, json={"detail": "not found"})
        return value

    monkeypatch.setattr(environment_client, "authed_client", lambda cfg, **kwargs: authed_client(
        cfg, transport=httpx.MockTransport(handle), **kwargs,
    ))
    return requests, responses


def operation(phase="pending"):
    return {"operation_id": OPERATION, "environment_id": ENVIRONMENT,
            "deployment_generation": 1, "action": "create", "phase": phase,
            "error_code": None, "execution_enabled": False}


def test_create_uses_selected_candidate_and_replay_key(management_http, capsys):
    requests, responses = management_http
    responses["POST", "/api/v1/environments"] = httpx.Response(202, json=operation())
    assert main(["dev", "create", "alice", "--candidate", CANDIDATE, "--idempotency-key", "same-request"]) == 0
    assert len(requests) == 1
    assert requests[0].url.path == "/api/v1/environments"
    assert json.loads(requests[0].content) == {"slug": "alice", "candidate_id": CANDIDATE}
    assert requests[0].headers["Idempotency-Key"] == "same-request"
    assert json.loads(capsys.readouterr().out)["operation_id"] == OPERATION


def test_legacy_list_keeps_environment_endpoint_and_json_shape(management_http, capsys):
    requests, responses = management_http
    responses["GET", "/api/v1/environments"] = httpx.Response(200, json={"items": []})
    assert main(["dev", "list"]) == 0
    assert [(request.method, request.url.path) for request in requests] == [("GET", "/api/v1/environments")]
    assert json.loads(capsys.readouterr().out) == {"items": []}


def test_create_prints_replay_key_before_failed_network_response(management_http, capsys):
    _, responses = management_http
    responses["POST", "/api/v1/environments"] = httpx.Response(503, json={"detail": {"code": "unavailable"}})
    assert main(["dev", "create", "alice", "--candidate", CANDIDATE]) == 1
    output = capsys.readouterr()
    key = output.err.split("Idempotency-Key: ", 1)[1].splitlines()[0]
    assert UUID(key)
    assert "not logged" not in output.err


def test_destroy_is_retained_generation_fenced_and_prints_exact_retry_command(management_http, capsys):
    requests, responses = management_http
    responses["POST", f"/api/v1/environments/{ENVIRONMENT}/operations"] = httpx.Response(202, json={
        **operation(), "action": "destroy_retained", "deployment_generation": 2,
    })
    args = ["dev", "destroy", ENVIRONMENT, "--expected-generation", "1", "--idempotency-key", "destroy-1"]
    assert main(args) == 0
    assert len(requests) == 1
    assert json.loads(requests[0].content) == {"action": "destroy_retained", "expected_generation": 1}
    assert requests[0].headers["Idempotency-Key"] == "destroy-1"
    output = capsys.readouterr()
    assert "loom " + " ".join(args) in output.err
    assert json.loads(output.out)["action"] == "destroy_retained"


def test_destroy_without_generation_reads_current_state_then_fences_request(management_http):
    requests, responses = management_http
    responses["GET", f"/api/v1/environments/{ENVIRONMENT}"] = httpx.Response(200, json={
        "registration": {"environment_id": ENVIRONMENT, "incarnation": ENVIRONMENT,
                         "owner_user_id": ENVIRONMENT, "owner_team_id": ENVIRONMENT,
                         "scope": "personal", "kind": "development", "slug": "alice",
                         "cluster_id": "cluster", "physical_pool_id": "pool", "application_namespace": "loom-dev-alice",
                         "execution_namespace": "loom-run-" + ENVIRONMENT.replace("-", ""),
                         "build_namespace": "loom-run-" + ENVIRONMENT.replace("-", "") + "-build",
                         "public_host": "alice.dev.example.com", "target_id": "env-" + ENVIRONMENT.replace("-", ""),
                         "deployment_generation": 1, "desired_state": "active"}, "operation": operation(),
    })
    responses["POST", f"/api/v1/environments/{ENVIRONMENT}/operations"] = httpx.Response(503, json={"detail": "unavailable"})
    assert main(["dev", "destroy", ENVIRONMENT, "--idempotency-key", "destroy-1"]) == 1
    assert [request.method for request in requests] == ["GET", "POST"]
    assert json.loads(requests[1].content) == {"action": "destroy_retained", "expected_generation": 1}


def test_explicit_retry_keeps_the_original_operation_identity(management_http, capsys):
    requests, responses = management_http
    responses["POST", f"/api/v1/environment-operations/{OPERATION}/retry"] = httpx.Response(202, json=operation())
    assert main(["dev", "retry", OPERATION]) == 0
    assert len(requests) == 1 and requests[0].method == "POST"
    assert json.loads(capsys.readouterr().out)["operation_id"] == OPERATION


def test_wait_timeout_does_not_cancel_operation(management_http, capsys):
    requests, responses = management_http
    responses["GET", f"/api/v1/environment-operations/{OPERATION}"] = httpx.Response(200, json=operation())
    assert main(["dev", "wait", OPERATION, "--timeout", "0"]) == 2
    assert all(request.method == "GET" for request in requests)
    assert OPERATION in capsys.readouterr().err


def test_wait_completed_operation_reports_execution_still_disabled(management_http, capsys):
    _, responses = management_http
    responses["GET", f"/api/v1/environment-operations/{OPERATION}"] = httpx.Response(200, json=operation("completed"))
    assert main(["dev", "wait", OPERATION]) == 0
    assert json.loads(capsys.readouterr().out)["execution_enabled"] is False


def test_service_up_explicit_personal_environment_dispatches_remote(management_http):
    requests, responses = management_http
    responses["POST", "/api/v1/environments"] = httpx.Response(503, json={"detail": "not configured"})
    assert main(["service", "up", "--environment", "dev-alice", "--candidate", CANDIDATE]) == 1
    assert len(requests) == 1
    assert json.loads(requests[0].content)["slug"] == "alice"


@pytest.mark.parametrize("flags", [
    ["--environment", "dev-alice"], ["--candidate", CANDIDATE],
    ["--environment", "prod", "--candidate", CANDIDATE],
    ["--environment", "dev-alice", "--candidate", CANDIDATE, "--compose-file", "local.yml"],
    ["--environment", "dev-alice", "--candidate", CANDIDATE, "--db-url", "postgresql://local/db"],
])
def test_service_up_rejects_target_source_conflicts_without_requests(management_http, flags):
    requests, _ = management_http
    assert main(["service", "up", *flags]) == 1
    assert requests == []
