"""Teammates can inspect immutable app versions and check shared-schema fit."""
from __future__ import annotations

import json

import httpx
import pytest

from loom_cli.__main__ import main
from tests.loom_cli.test_application_cmd import (
    APPLICATION,
    OPERATION,
    RELEASE,
    application_http as application_http,
    operation,
    registration,
)


def release():
    return {"release_id": RELEASE, "source_digest": "sha256:" + "a" * 64,
            "schema_revision": "0174", "service_image_ref": "registry.example/service@sha256:" + "b" * 64,
            "web_image_ref": "registry.example/web@sha256:" + "c" * 64}


def versions():
    return {"schema_version": "loom.nebius-application-versions.v1", "scope": "deployment_journal",
            "status": {"registration": registration(), "operation": operation(action="update")},
            "requested_release": release(), "last_completed_deployment": {
                "operation_id": "30000000-0000-4000-8000-000000000002", "deployment_generation": 1,
                "completed_at": "2026-10-08T00:00:00Z", "release": release() | {
                    "release_id": "40000000-0000-4000-8000-000000000002", "source_digest": "sha256:" + "d" * 64}},
            "shared_schema_revision": "0174", "schema_compatibility": "compatible"}


@pytest.mark.parametrize("as_json", [False, True])
def test_versions_distinguish_pending_request_from_completed_deployment(application_http, capsys, as_json):
    responses, requests = application_http
    responses["GET", f"/api/v1/applications/{APPLICATION}/versions"] = httpx.Response(200, json=versions())
    assert main(["dev", "app", "versions", APPLICATION, *(["--json"] if as_json else [])]) == 0
    assert len(requests) == 1 and requests[0].method == "GET"
    out = capsys.readouterr()
    if as_json:
        report = json.loads(out.out)
        assert report["requested_release"]["source_digest"] == release()["source_digest"]
        assert report["last_completed_deployment"]["deployment_generation"] == 1
    else:
        assert "Requested release" in out.out and "Last completed deployment: generation 1" in out.out
        assert "pending" in out.out and "not a live readiness check" in out.out
        assert "sha256:" + "a" * 64 in out.out and "sha256:" + "d" * 64 in out.out
        assert "loom eval trial show" in out.out
    assert "Retry:" not in out.err


@pytest.mark.parametrize("compatible", [True, False])
@pytest.mark.parametrize("as_json", [False, True])
def test_check_release_reports_schema_fit_without_deploying(application_http, capsys, compatible, as_json):
    responses, requests = application_http
    responses["GET", f"/api/v1/application-releases/{RELEASE}/compatibility"] = httpx.Response(200, json={
        "schema_version": "loom.nebius-application-release-compatibility.v1", "scope": "configured_shared_schema",
        "release": release(), "shared_schema_revision": "0174" if compatible else "0173",
        "compatibility": "compatible" if compatible else "schema_mismatch",
    })
    assert main(["dev", "app", "check-release", RELEASE, *(["--json"] if as_json else [])]) == (0 if compatible else 1)
    assert len(requests) == 1 and requests[0].method == "GET"
    out = capsys.readouterr()
    if as_json:
        assert json.loads(out.out)["compatibility"] == ("compatible" if compatible else "schema_mismatch")
    else:
        assert "0174" in out.out and "configured shared schema" in out.out
        if not compatible:
            assert "0173" in out.out and "disposable local" in out.out
    assert "Retry:" not in out.err


@pytest.mark.parametrize("command,identity,path", [
    ("versions", APPLICATION, f"/applications/{APPLICATION}/versions"),
    ("check-release", RELEASE, f"/application-releases/{RELEASE}/compatibility"),
])
def test_read_diagnostics_bound_http_errors_and_retries(application_http, capsys, command, identity, path):
    responses, requests = application_http
    responses["GET", "/api/v1" + path] = httpx.Response(500, json={"detail": "fixture-private-material"})
    assert main(["dev", "app", command, identity]) == 1
    out = capsys.readouterr()
    assert out.out == "" and "fixture-private-material" not in out.err and "HTTP 500" in out.err
    responses["GET", "/api/v1" + path] = httpx.ReadTimeout("fixture-private-material")
    assert main(["dev", "app", command, identity]) == 1
    out = capsys.readouterr()
    assert "read-only" in out.err and "fixture-private-material" not in out.err
    assert len(requests) == 2 and all(r.method == "GET" for r in requests)


@pytest.mark.parametrize("damage", ["wrong_owner_application", "future_completion", "secret", "inconsistent_schema"])
def test_versions_reject_inconsistent_or_private_responses(application_http, capsys, damage):
    responses, _ = application_http
    payload = versions()
    if damage == "wrong_owner_application":
        payload["status"]["registration"]["application_id"] = OPERATION
    elif damage == "future_completion":
        payload["last_completed_deployment"]["deployment_generation"] = 3
    elif damage == "inconsistent_schema":
        payload["shared_schema_revision"] = "0173"
    else:
        payload["credential"] = "fixture-private-material"
    responses["GET", f"/api/v1/applications/{APPLICATION}/versions"] = httpx.Response(200, json=payload)
    assert main(["dev", "app", "versions", APPLICATION, "--json"]) == 1
    out = capsys.readouterr()
    assert out.out == "" and "fixture-private-material" not in out.err
