"""The fixed startup diagnostic reports stages, never credentials or cleanup."""
from __future__ import annotations

import json
import os
import ssl
import subprocess
import sys
from pathlib import Path
from uuid import uuid4

import certifi
import httpx
import pytest
from tests.ops.test_nebius_application_setup import setup_request as setup_request
from tests.ops.test_nebius_management_retirement import retirement_request as retirement_request
from tests.unit.test_nebius_management_render import (
    application_management_inputs as application_management_inputs,
)
from tests.unit.test_nebius_management_render import management_inputs as management_inputs
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


@pytest.fixture
def startup(retirement_request, tmp_path):
    from scripts.ops.nebius_management_retirement import retirement_documents

    from loom_service.environment_management.retirement import RetirementSettings

    request, _ = retirement_request
    cm, = [doc for doc in retirement_documents(request)["job"].values() if doc["kind"] == "ConfigMap"]
    settings = RetirementSettings.model_validate_json(cm["data"]["retirement.json"])
    ca, token = tmp_path / "ca.crt", tmp_path / "token"
    ca.write_bytes(Path(certifi.where()).read_bytes())
    token.write_text("private-token")
    token.chmod(0o440)
    settings = settings.model_copy(update={"kubernetes": settings.kubernetes.model_copy(update={"ca_file": ca, "token_file": token})})
    url = (f"postgresql://loom_service:private-password@loom-postgres.{settings.namespace}.svc:5432/loom"
        "?sslmode=verify-full&sslrootcert=/var/run/loom-db/ca.crt")
    return settings, url


def test_fixed_probe_without_mount_reports_settings_failure_without_traceback():
    path = Path(__file__).parents[2] / "scripts/ops/nebius_retirement_startup_probe.py"
    assert path.is_file(), "fixed startup probe is not implemented"
    result = subprocess.run([sys.executable, "-c", path.read_text()], capture_output=True, text=True, timeout=15,
        env=os.environ | {"LOOM_RETIREMENT_DB_URL": "private-password"})
    assert result.returncode == 0  # Successful observation, not successful retirement.
    assert json.loads(result.stdout) == {"schema": "loom.nebius-retirement-startup-probe.v1",
        "status": "unavailable", "stage": "settings", "checks": [], "operations": [],
        "error_type": "FileNotFoundError", "http_status": None}
    assert result.stderr == "" and "private-" not in result.stdout


@pytest.mark.parametrize("raw,error", [(b"private-invalid-json", "ValidationError"), (b"[]", "ValidationError"),
    (b"x" * 262145, "ValueError")])
def test_malformed_or_oversized_settings_never_start_network_checks(tmp_path, monkeypatch, capsys, raw, error):
    from scripts.ops import nebius_retirement_startup_probe as probe

    path = tmp_path / "retirement.json"
    path.write_bytes(raw)
    monkeypatch.setattr(probe, "SETTINGS_PATH", path)
    monkeypatch.setattr(probe, "observe_startup", lambda *args: pytest.fail("unexpected network startup"))
    assert probe.main() == 0
    captured = capsys.readouterr()
    result = json.loads(captured.out)
    assert result["stage"] == "settings" and result["error_type"] == error
    assert result["status"] == "unavailable" and result["checks"] == []
    assert captured.err == "" and "private-" not in captured.out


@pytest.mark.parametrize("damage,stage,error", [
    ("database", "database_binding", "ValueError"),
    ("ca", "kubernetes_ca", "FileNotFoundError"),
    ("token_missing", "kubernetes_token", "ValueError"),
    ("token_mode", "kubernetes_token", "ValueError"),
])
async def test_local_validation_precedes_database_or_api(startup, monkeypatch, damage, stage, error):
    from scripts.ops import nebius_retirement_startup_probe as probe

    settings, url = startup
    if damage == "database":
        url = url.replace("loom-postgres.", "foreign.")
    elif damage == "ca":
        settings.kubernetes.ca_file.unlink()
    elif damage == "token_missing":
        settings.kubernetes.token_file.unlink()
    else:
        settings.kubernetes.token_file.chmod(0o444)
    monkeypatch.setattr(probe, "database_snapshot", lambda *args: pytest.fail("unexpected database access"))
    monkeypatch.setattr(probe.httpx, "AsyncClient", lambda *args, **kwargs: pytest.fail("unexpected API access"))
    result = await probe.observe_startup(settings, url)
    assert result["status"] == "unavailable" and result["stage"] == stage and result["error_type"] == error
    assert "private-" not in json.dumps(result)


@pytest.mark.parametrize("damage,status,stage,error,code", [
    (None, "observed", "complete", None, None),
    ("denied", "unavailable", "kubernetes_get", "HTTPStatusError", 403),
    ("redirect", "unavailable", "kubernetes_get", "HTTPStatusError", 302),
    ("oversized", "unavailable", "kubernetes_get", "ValueError", None),
    ("uid", "unavailable", "kubernetes_identity", "ProviderBlockedError", None),
    ("timeout", "unavailable", "kubernetes_get", "ReadTimeout", None),
])
async def test_only_exact_namespace_gets_and_sanitized_failures(startup, monkeypatch, damage, status, stage, error, code):
    from scripts.ops import nebius_retirement_startup_probe as probe

    settings, url = startup
    target, = settings.targets
    seen = []

    async def database(selected_url, targets):
        assert selected_url.host == f"loom-postgres.{settings.namespace}.svc" and targets == settings.targets
        return []

    def transport(request):
        assert request.method == "GET" and request.headers["Authorization"] == "Bearer private-token"
        name = request.url.path.removeprefix("/api/v1/namespaces/")
        assert name in target.namespace_uids
        seen.append(name)
        if damage == "denied":
            return httpx.Response(403, text="private-provider-body")
        if damage == "redirect":
            return httpx.Response(302, headers={"Location": "https://foreign.invalid/private-url"})
        if damage == "oversized":
            return httpx.Response(200, content=b"x" * (65536 + 1))
        if damage == "timeout":
            raise httpx.ReadTimeout("private-provider-error", request=request)
        return httpx.Response(200, json={"apiVersion": "v1", "kind": "Namespace", "metadata": {
            "name": name, "uid": str(uuid4()) if damage == "uid" else str(target.namespace_uids[name]), "labels": {
                "loom.nebius/environment-id": str(target.registration.environment_id),
                "loom.nebius/incarnation": str(target.registration.incarnation)}}})

    real_client = httpx.AsyncClient

    def client(**kwargs):
        assert kwargs["trust_env"] is False and kwargs["follow_redirects"] is False
        assert kwargs["verify"].verify_mode == ssl.CERT_REQUIRED and kwargs["verify"].check_hostname
        return real_client(**kwargs, transport=httpx.MockTransport(transport))

    monkeypatch.setattr(probe, "database_snapshot", database)
    monkeypatch.setattr(probe.httpx, "AsyncClient", client)
    result = await probe.observe_startup(settings, url)
    assert result["status"] == status and result["stage"] == stage
    assert result.get("error_type") == error and result.get("http_status") == code
    assert "private-" not in json.dumps(result)
    assert len(seen) == (3 if damage is None else 1)
    if damage is None:
        assert result["checks"] == ["database_binding", "kubernetes_ca", "kubernetes_token", "database", "kubernetes"]


async def test_database_error_is_sanitized_and_prevents_api_calls(startup, monkeypatch):
    from scripts.ops import nebius_retirement_startup_probe as probe
    from sqlalchemy.exc import OperationalError

    settings, url = startup

    async def failed(*args):
        raise OperationalError("private-sql", {"password": "private-password"}, RuntimeError("private-host"))

    monkeypatch.setattr(probe, "database_snapshot", failed)
    monkeypatch.setattr(probe.httpx, "AsyncClient", lambda *args, **kwargs: pytest.fail("unexpected API access"))
    result = await probe.observe_startup(settings, url)
    assert result["stage"] == "database" and result["error_type"] == "OperationalError"
    assert result["status"] == "unavailable" and "private-" not in json.dumps(result)
