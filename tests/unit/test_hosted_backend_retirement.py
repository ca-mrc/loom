"""Hosted admission never falls back to retired worker capacity."""

from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException

from loom.service_execution_backend import local_execution_enabled
from loom_service.execution_admission import admit_execution_backend
from loom_service.routes.backends import list_backends


@pytest.mark.parametrize("environment", [None, "", "development", "staging", "production"])
def test_local_execution_requires_explicit_opt_in(monkeypatch, environment):
    monkeypatch.delenv("LOOM_LOCAL_EXECUTION", raising=False)
    if environment is None:
        monkeypatch.delenv("LOOM_ENV", raising=False)
    else:
        monkeypatch.setenv("LOOM_ENV", environment)
    assert not local_execution_enabled()


def test_disposable_local_execution_is_explicit(monkeypatch):
    monkeypatch.setenv("LOOM_ENV", "development")
    monkeypatch.setenv("LOOM_LOCAL_EXECUTION", "1")
    assert local_execution_enabled()


@pytest.mark.parametrize("backend", ["docker", "slurm", "modal", "fake", "unknown"])
async def test_hosted_admission_rejects_non_nebius_before_capacity_lookup(monkeypatch, backend):
    monkeypatch.setenv("LOOM_ENV", "production")
    session = AsyncMock()
    with pytest.raises(HTTPException) as error:
        await admit_execution_backend(
            session, backend=backend, task_ids=[], trial_config={}, combinations=[],
            runtime_profile_json="",
        )
    assert error.value.status_code == 400
    assert error.value.detail["reason"] == "unsupported_hosted_backend"
    session.execute.assert_not_awaited()


async def test_hosted_catalog_does_not_consult_legacy_workers(monkeypatch):
    monkeypatch.setenv("LOOM_ENV", "production")
    monkeypatch.setattr("loom_service.routes.backends.get_service_execution_backend_pools", AsyncMock(return_value=()))
    session = AsyncMock()
    result = await list_backends((session, None))
    assert [item["name"] for item in result["items"]] == ["nebius"]
    session.execute.assert_not_awaited()


@pytest.mark.parametrize("backend", ["docker", "fake", "modal"])
def test_control_plane_cannot_route_hosted_work_to_workers(monkeypatch, backend):
    from loom_control_plane.routes.trials import _resolve_required_worker_pool_for_backend

    monkeypatch.setenv("LOOM_ENV", "production")
    with pytest.raises(HTTPException, match="Hosted execution supports Nebius only"):
        _resolve_required_worker_pool_for_backend(
            batch_backend=backend, requested_pool=None, task_service_pool=None,
        )


@pytest.mark.parametrize("environment", ["staging", "production"])
def test_hosted_environment_cannot_enable_local_workers(monkeypatch, environment):
    monkeypatch.setenv("LOOM_ENV", environment)
    monkeypatch.setenv("LOOM_LOCAL_EXECUTION", "1")
    assert not local_execution_enabled()


@pytest.mark.parametrize("environment,opt_in,expected", [
    ("development", None, False), ("development", "1", True),
    ("staging", "1", False), ("production", "1", False),
])
def test_worker_registration_and_claim_are_local_only(monkeypatch, environment, opt_in, expected):
    from loom_control_plane.app import create_app
    from loom_control_plane.config import ControlPlaneSettings

    monkeypatch.setenv("LOOM_ENV", environment)
    if opt_in is None:
        monkeypatch.delenv("LOOM_LOCAL_EXECUTION", raising=False)
    else:
        monkeypatch.setenv("LOOM_LOCAL_EXECUTION", opt_in)
    app = create_app(ControlPlaneSettings(
        _env_file=None, db_url="postgresql+psycopg://test:test@localhost/test",
        minio_endpoint="http://localhost", minio_access_key="test", minio_secret_key="test",
        llm_gateway_url="http://localhost",
    ))
    paths = app.openapi()["paths"]
    assert not any("slurm-worker" in path or "gb10-worker" in path or "worker-pool-autoscaler" in path for path in paths)
    for path in ("/workers/register", "/trials/claim", "/work/claim",
                 "/api/v1/internal/task-image-materializations/claim"):
        assert (path in paths) is expected


def test_personal_hosted_fleet_routes_are_retired():
    from loom_service.app import create_app
    from loom_service.config import LoomServiceSettings

    app = create_app(LoomServiceSettings(
        _env_file=None, db_url="postgresql+psycopg://test:test@localhost/test",
        minio_access_key="test", minio_secret_key="test",
    ))
    paths = app.openapi()["paths"]
    assert not any("stage1-smoke" in path for path in paths)
    assert "/api/v1/dev-instances" not in paths
    assert "/api/v1/health/personal-dev-acceptance" not in paths
    assert "/api/v1/health/personal-dev-operational" not in paths


@pytest.mark.parametrize("local", [False, True])
def test_pipeline_submission_is_local_only_and_retained_results_remain(monkeypatch, local):
    from loom_service.app import create_app
    from loom_service.config import LoomServiceSettings

    monkeypatch.setenv("LOOM_ENV", "development")
    monkeypatch.setenv("LOOM_LOCAL_EXECUTION", "1" if local else "0")
    app = create_app(LoomServiceSettings(
        _env_file=None, db_url="postgresql+psycopg://test:test@localhost/test",
        minio_access_key="test", minio_secret_key="test",
    ))
    paths = app.openapi()["paths"]
    assert ("post" in paths["/api/v1/pipeline-runs"]) is local
    assert ("/api/v1/pipeline-stage-runs/{stage_run_id}/retry" in paths) is local
    assert "get" in paths["/api/v1/pipeline-runs"]
    assert "get" in paths["/api/v1/pipeline-runs/{run_id}"]
    assert "/api/v1/pipeline-runs/{run_id}/cancel" in paths


@pytest.mark.parametrize("handler", ["issue_task_image_builder_token", "issue_task_image_registry_gc_token"])
async def test_hosted_cannot_mint_local_task_image_credentials(monkeypatch, handler):
    from types import SimpleNamespace

    from loom_control_plane.routes import admin

    monkeypatch.setenv("LOOM_ENV", "production")
    monkeypatch.setenv("LOOM_LOCAL_EXECUTION", "1")
    monkeypatch.setattr(admin, "_require_admin_scope", AsyncMock())
    with pytest.raises(HTTPException) as error:
        await getattr(admin, handler)(
            SimpleNamespace(), admin._TaskImageServiceTokenPayload(expires_in_days=1), None,
        )
    assert error.value.status_code == 409


@pytest.mark.parametrize("local", [False, True])
async def test_retired_local_model_catalog_is_absent_from_api_and_settings(monkeypatch, local):
    import httpx

    from loom_service.app import create_app
    from loom_service.config import LoomServiceSettings

    monkeypatch.setenv("LOOM_ENV", "development")
    monkeypatch.setenv("LOOM_LOCAL_EXECUTION", "1" if local else "0")
    settings = LoomServiceSettings(
        _env_file=None, db_url="postgresql+psycopg://test:test@localhost/test",
        minio_access_key="test", minio_secret_key="test",
    )
    app = create_app(settings)
    assert "local_servers_json" not in LoomServiceSettings.model_fields
    assert "huggingface_api_key" not in LoomServiceSettings.model_fields
    assert "/api/v1/local-servers" not in app.openapi()["paths"]
    assert "/api/v1/models" in app.openapi()["paths"]
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test",
    ) as client:
        assert (await client.get("/api/v1/local-servers")).status_code == 404
