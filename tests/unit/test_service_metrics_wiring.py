"""Unit tests for #81 slice B-3 — service /metrics endpoint + HTTP
middleware instrumenting every request."""

from __future__ import annotations

import pytest
from prometheus_client import REGISTRY


@pytest.fixture(autouse=True)
def _isolate_env_from_dotenv(monkeypatch: pytest.MonkeyPatch) -> None:
    for k in (
        "LOOM_WORKER_TOKEN", "LOOM_TEAM_TOKEN", "LOOM_ADMIN_TOKEN",
    ):
        monkeypatch.delenv(k, raising=False)


def _make_settings():  # type: ignore[no-untyped-def]
    from loom_service.config import LoomServiceSettings
    return LoomServiceSettings(
        _env_file=None,  # type: ignore[call-arg]
        db_url="postgresql+asyncpg://x@y/z",
        gateway_url="http://gw.x",
        control_plane_url="http://cp.x",
        minio_endpoint="http://minio.x",
        minio_access_key="x",
        minio_secret_key="x",
    )


def test_metrics_endpoint_mounted_on_service_app() -> None:
    from loom_service.app import create_app
    app = create_app(_make_settings())
    mounts = [
        r for r in app.routes
        if hasattr(r, "path") and r.path.startswith("/metrics")
    ]
    assert mounts, "no /metrics route on service app"


def test_service_metric_objects_registered() -> None:
    from loom_service import metrics  # noqa: F401

    names = {m.name for m in REGISTRY.collect()}
    expected = {
        "loom_svc_http_requests",
        "loom_svc_http_request_latency_sec",
        "loom_svc_batch_runner_ticks",
        "loom_svc_batch_runner_trials_dispatched",
        "loom_svc_auth_failures",
        "loom_svc_invites",
        "loom_svc_submission_rejects",
        "loom_svc_artifact_download_bytes",
        "loom_svc_team_emergency_actions",
        "loom_svc_tokens_issued",
        "loom_svc_tokens_revoked",
    }
    for stem in expected:
        assert stem in names or f"{stem}_total" in names or any(
            n.startswith(stem) for n in names
        ), f"metric stem {stem!r} missing"


def test_http_middleware_records_one_observation_per_request() -> None:
    """The middleware must observe ONE request, not zero (forgot
    middleware) or many (double-instrumented). We hit `/` which is
    handled by the inline _root() — no DB needed."""
    from fastapi.testclient import TestClient

    from loom_service.app import create_app

    request_labels = {"route": "/", "method": "GET", "status_class": "2xx"}
    latency_labels = {"route": "/", "method": "GET"}
    request_name = "loom_svc_http_requests_total"
    latency_name = "loom_svc_http_request_latency_sec_count"
    before_requests = REGISTRY.get_sample_value(request_name, request_labels) or 0
    before_latency = REGISTRY.get_sample_value(latency_name, latency_labels) or 0

    # A bare client exercises the real middleware without entering the
    # database lifespan; `/` is the inline, database-free service manifest.
    client = TestClient(create_app(_make_settings()))
    try:
        response = client.get("/")
        assert response.status_code == 200
        assert response.json()["service"] == "loom-service"
    finally:
        client.close()

    assert REGISTRY.get_sample_value(request_name, request_labels) == before_requests + 1
    assert REGISTRY.get_sample_value(latency_name, latency_labels) == before_latency + 1
