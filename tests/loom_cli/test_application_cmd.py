"""Personal application commands use the application contract, never legacy fallback."""

from __future__ import annotations

import json
import shlex

import httpx
import pytest

from loom_cli.__main__ import main
from loom_cli.config import LoomConfig, save_config
from loom_cli.contexts import selected_context

APPLICATION = "20000000-0000-4000-8000-000000000001"
OPERATION = "30000000-0000-4000-8000-000000000001"
RELEASE = "40000000-0000-4000-8000-000000000001"


def operation(phase="pending", action="create"):
    return {"operation_id": OPERATION, "application_id": APPLICATION,
            "deployment_generation": 2, "access_generation": 2, "action": action,
            "phase": phase, "error_code": "example_blocked" if phase == "blocked" else None}


def registration():
    return {"application_id": APPLICATION, "incarnation": OPERATION,
            "owner_user_id": APPLICATION, "owner_team_id": OPERATION,
            "data_environment_id": RELEASE, "cluster_id": "cluster", "slug": "alice",
            "application_namespace": "loom-dev-alice", "public_host": "alice.example.com",
            "release_id": RELEASE, "deployment_generation": 2, "access_generation": 2,
            "desired_state": "active"}


@pytest.fixture
def application_http(monkeypatch, tmp_xdg_home):
    from loom_cli import server_client

    save_config(LoomConfig(server_url="https://manage.example.com", auth_token="management-secret"))
    responses, requests = {}, []
    authed = server_client.authed_client

    def handle(request):
        requests.append(request)
        assert request.url.host == "manage.example.com"
        assert request.headers["authorization"] == "Bearer management-secret"
        value = responses[request.method, request.url.path]
        if isinstance(value, Exception):
            raise value
        return value

    monkeypatch.setattr(server_client, "authed_client", lambda cfg: authed(cfg, transport=httpx.MockTransport(handle)))
    return responses, requests


def test_application_create_selects_release_and_prints_context_bound_replay(application_http, capsys):
    responses, requests = application_http
    responses["POST", "/api/v1/applications"] = httpx.Response(202, json=operation())
    with selected_context("management-alice"):
        save_config(LoomConfig(server_url="https://manage.example.com", auth_token="management-secret"))
    args = ["--context", "management-alice", "dev", "app", "create", "alice",
            "--release", RELEASE, "--idempotency-key", "create-alice"]
    assert main(args) == 0
    assert len(requests) == 1
    assert json.loads(requests[0].content) == {"slug": "alice", "release_id": RELEASE}
    assert requests[0].headers["Idempotency-Key"] == "create-alice"
    output = capsys.readouterr()
    assert "loom " + " ".join(args) in output.err
    assert json.loads(output.out)["operation_id"] == OPERATION


@pytest.mark.parametrize("command,action", [("update", "update"), ("suspend", "suspend"),
                                           ("resume", "resume"), ("destroy", "destroy_retained")])
def test_transition_uses_generation_and_exact_application_action(application_http, capsys, command, action):
    responses, requests = application_http
    responses["POST", f"/api/v1/applications/{APPLICATION}/operations"] = httpx.Response(202, json=operation(action=action))
    flags = ["--release", RELEASE] if command == "update" else []
    args = ["dev", "app", command, APPLICATION, "--expected-generation", "1", *flags, "--idempotency-key", "same-request"]
    assert main(args) == 0
    assert len(requests) == 1 and requests[0].headers["Idempotency-Key"] == "same-request"
    assert json.loads(requests[0].content) == {"action": action, "expected_generation": 1,
                                             "release_id": RELEASE if command == "update" else None}
    output = capsys.readouterr()
    assert "--expected-generation 1" in output.err and "--idempotency-key same-request" in output.err
    assert json.loads(output.out)["action"] == action


def test_transition_reads_generation_once_and_preserves_retry_after_lost_response(application_http, capsys):
    responses, requests = application_http
    responses["GET", f"/api/v1/applications/{APPLICATION}"] = httpx.Response(200, json={
        "registration": registration(), "operation": operation("completed"),
    })
    responses["POST", f"/api/v1/applications/{APPLICATION}/operations"] = httpx.ReadTimeout("uncertain response")
    assert main(["dev", "app", "suspend", APPLICATION, "--idempotency-key", "stop-1"]) == 1
    assert [r.method for r in requests] == ["GET", "POST"]
    assert json.loads(requests[-1].content)["expected_generation"] == 2
    output = capsys.readouterr()
    assert "--expected-generation 2" in output.err and "--idempotency-key stop-1" in output.err
    assert "uncertain response" not in output.err


@pytest.mark.parametrize("phase,code", [("pending", 2), ("running", 2), ("completed", 0), ("blocked", 1), ("superseded", 1)])
def test_wait_reports_terminal_state_without_cancel_or_retry(application_http, capsys, phase, code):
    responses, requests = application_http
    responses["GET", f"/api/v1/application-operations/{OPERATION}"] = httpx.Response(200, json=operation(phase))
    assert main(["dev", "app", "wait", OPERATION, "--timeout", "0"]) == code
    assert len(requests) == 1 and requests[0].method == "GET"
    assert json.loads(capsys.readouterr().out)["phase"] == phase


def test_list_status_and_explicit_retry_use_only_application_endpoints(application_http, capsys):
    responses, requests = application_http
    responses["GET", "/api/v1/applications"] = httpx.Response(200, json={"items": [registration()]})
    responses["GET", f"/api/v1/applications/{APPLICATION}"] = httpx.Response(200, json={
        "registration": registration(), "operation": operation(),
    })
    responses["POST", f"/api/v1/application-operations/{OPERATION}/retry"] = httpx.Response(202, json=operation())
    assert main(["dev", "app", "list"]) == 0
    assert json.loads(capsys.readouterr().out)["items"][0]["application_id"] == APPLICATION
    assert main(["dev", "app", "status", APPLICATION]) == 0
    assert json.loads(capsys.readouterr().out)["registration"]["application_id"] == APPLICATION
    assert main(["dev", "app", "retry", OPERATION]) == 0
    assert json.loads(capsys.readouterr().out)["operation_id"] == OPERATION
    assert [r.method for r in requests] == ["GET", "GET", "POST"]


@pytest.mark.parametrize("args", [
    ["create", "alice", "--release", "bad"],
    ["create", "alice", "--release", RELEASE, "--idempotency-key", "bad key"],
    ["suspend", APPLICATION, "--expected-generation", "0"],
    ["update", APPLICATION, "--release", "bad"],
    ["wait", OPERATION, "--timeout", "nan"],
])
def test_invalid_arguments_make_no_request(application_http, args):
    _, requests = application_http
    assert main(["dev", "app", *args]) == 1
    assert requests == []


def test_foreign_status_cannot_supply_generation_for_mutation(application_http):
    responses, requests = application_http
    responses["GET", f"/api/v1/applications/{APPLICATION}"] = httpx.Response(200, json={
        "registration": {**registration(), "application_id": RELEASE}, "operation": operation(),
    })
    assert main(["dev", "app", "destroy", APPLICATION]) == 1
    assert len(requests) == 1 and requests[0].method == "GET"


def test_foreign_operation_response_cannot_satisfy_wait(application_http):
    responses, requests = application_http
    responses["GET", f"/api/v1/application-operations/{OPERATION}"] = httpx.Response(200, json={
        **operation("completed"), "operation_id": RELEASE,
    })
    assert main(["dev", "app", "wait", OPERATION]) == 1
    assert len(requests) == 1


@pytest.mark.parametrize("foreign", [False, True])
def test_evidence_reads_only_its_exact_operation_and_never_issues_mutation(application_http, capsys, foreign):
    responses, requests = application_http
    responses["GET", f"/api/v1/application-operations/{OPERATION}/evidence"] = httpx.Response(200, json={
        "schema_version": "loom.nebius-application-operation-evidence.v1",
        "operation": {**operation("running"), "operation_id": RELEASE if foreign else OPERATION},
        "runner_epoch": 1, "lease_active": True, "completion_recorded": False,
        "kubernetes": [], "cloud": [{"kind": "access_key", "action": "delete", "phase": "observed", "count": 1}],
    })
    assert main(["dev", "app", "evidence", OPERATION]) == (1 if foreign else 0)
    assert [(request.method, request.url.path) for request in requests] == [
        ("GET", f"/api/v1/application-operations/{OPERATION}/evidence"),
    ]
    output = capsys.readouterr()
    if not foreign:
        assert json.loads(output.out)["completion_recorded"] is False
        assert "Retry:" not in output.err


@pytest.mark.parametrize("command", ["create", "suspend"])
def test_printed_retry_command_round_trips_leading_hyphen_key(application_http, capsys, command):
    responses, requests = application_http
    if command == "create":
        responses["POST", "/api/v1/applications"] = httpx.Response(202, json=operation())
        args = ["create", "alice", "--release", RELEASE]
    else:
        responses["POST", f"/api/v1/applications/{APPLICATION}/operations"] = httpx.Response(202, json=operation(action="suspend"))
        args = ["suspend", APPLICATION, "--expected-generation", "1"]
    assert main(["dev", "app", *args, "--idempotency-key=-retry"]) == 0
    printed = capsys.readouterr().err.split("Retry: ", 1)[1].splitlines()[0]
    assert main(shlex.split(printed)[1:]) == 0
    assert len(requests) == 2
    assert requests[0].content == requests[1].content
    assert requests[0].headers["Idempotency-Key"] == requests[1].headers["Idempotency-Key"] == "-retry"


def test_evidence_network_failure_guides_read_only_retry(application_http, capsys):
    responses, requests = application_http
    responses["GET", f"/api/v1/application-operations/{OPERATION}/evidence"] = httpx.ReadTimeout("private upstream detail")
    assert main(["dev", "app", "evidence", OPERATION]) == 1
    assert len(requests) == 1 and requests[0].method == "GET"
    output = capsys.readouterr()
    assert output.out == ""
    assert "read-only" in output.err
    assert "printed retry command" not in output.err
    assert "private upstream detail" not in output.err


def capabilities():
    return {"schema_version": "loom.nebius-application-capabilities.v1", "scope": "management_process",
            "application_lifecycle": "worker_healthy", "source_upload": "configured",
            "image_builds": "not_configured", "execution": "not_checked"}


@pytest.mark.parametrize("as_json", [False, True])
def test_capabilities_show_partial_configuration_with_one_read_only_request(application_http, capsys, as_json):
    responses, requests = application_http
    responses["GET", "/api/v1/application-capabilities"] = httpx.Response(200, json=capabilities())
    assert main(["dev", "app", "capabilities", *(["--json"] if as_json else [])]) == 0
    assert [(request.method, request.url.path) for request in requests] == [
        ("GET", "/api/v1/application-capabilities"),
    ]
    output = capsys.readouterr()
    if as_json:
        assert json.loads(output.out) == capabilities()
    else:
        assert "Source upload: configured" in output.out
        assert "Image builds: not configured" in output.out
        assert "Task execution: not checked" in output.out
    assert "Retry:" not in output.err


@pytest.mark.parametrize("damage", ["unsupported_execution", "missing_builds", "secret_field"])
def test_capabilities_reject_malformed_or_overclaiming_server_responses(application_http, capsys, damage):
    responses, requests = application_http
    body = capabilities()
    if damage == "unsupported_execution":
        body["execution"] = "ready"
    elif damage == "missing_builds":
        del body["image_builds"]
    else:
        body["credential"] = "must-not-print"
    responses["GET", "/api/v1/application-capabilities"] = httpx.Response(200, json=body)
    assert main(["dev", "app", "capabilities", "--json"]) == 1
    assert len(requests) == 1
    output = capsys.readouterr()
    assert output.out == "" and "must-not-print" not in output.err


def test_capabilities_network_failure_recommends_read_only_retry(application_http, capsys):
    responses, requests = application_http
    responses["GET", "/api/v1/application-capabilities"] = httpx.ReadTimeout("private upstream detail")
    assert main(["dev", "app", "capabilities"]) == 1
    assert len(requests) == 1 and requests[0].method == "GET"
    output = capsys.readouterr()
    assert output.out == "" and "read-only" in output.err
    assert "printed retry command" not in output.err and "private upstream detail" not in output.err
