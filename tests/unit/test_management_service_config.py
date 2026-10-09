"""Management must not require child workload credentials or enable task routes."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from loom_service.app import create_app
from loom_service.config import LoomServiceSettings


@pytest.fixture(autouse=True)
def clean_storage_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in ("LOOM_SVC_MINIO_ACCESS_KEY", "LOOM_SVC_MINIO_SECRET_KEY"):
        monkeypatch.delenv(name, raising=False)


def test_management_config_requires_no_child_storage_secrets() -> None:
    settings = LoomServiceSettings(
        _env_file=None, service_mode="management",
        db_url="postgresql+psycopg://u:p@localhost/management",
    )
    assert settings.minio_access_key is None
    assert settings.minio_secret_key is None


@pytest.mark.parametrize("secrets", [{}, {"minio_access_key": "access"}, {"minio_secret_key": "secret"}])
def test_application_config_still_requires_storage_secrets(secrets: dict[str, str]) -> None:
    with pytest.raises(ValidationError, match="storage credentials"):
        LoomServiceSettings(
            _env_file=None, db_url="postgresql+psycopg://u:p@localhost/application", **secrets,
        )


def test_unknown_service_mode_is_rejected() -> None:
    with pytest.raises(ValidationError, match="service_mode"):
        LoomServiceSettings(
            _env_file=None, service_mode="managment",
            db_url="postgresql+psycopg://u:p@localhost/management",
            minio_access_key="access", minio_secret_key="secret",
        )


@pytest.mark.parametrize("mode,token", [("application", "token"), ("management", None)])
def test_provisioning_configuration_requires_management_mode_and_publication_credential(mode, token):
    with pytest.raises(ValidationError, match="environment management"):
        LoomServiceSettings(
            _env_file=None, service_mode=mode, db_url="postgresql+psycopg://u:p@localhost/db",
            minio_access_key="access", minio_secret_key="secret",
            environment_management_config_file="/protected/install.json", environment_management_github_token=token,
        )


def test_management_ignores_unused_workload_execution_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    # A local execution flag or a stale workload profile cannot enable workload
    # APIs in the separately configured management service.
    monkeypatch.setenv("LOOM_LOCAL_EXECUTION", "1")
    monkeypatch.setenv("LOOM_ENV", "development")
    settings = LoomServiceSettings(
        _env_file=None, service_mode="management",
        db_url="postgresql+psycopg://u:p@localhost/management",
        service_execution_runtime_profile_json="not a workload profile",
        workload_trust_mode="untrusted", gateway_url="https://offline.invalid/prefix",
    )
    paths = create_app(settings).openapi()["paths"]
    assert "/api/v1/auth/login" in paths
    assert "/api/v1/auth/me" in paths
    assert "/api/v1/health/ready" in paths
    assert "/api/v1/application-builds" in paths
    assert "/api/v1/application-sources" in paths
    assert "/api/v1/application-capabilities" in paths
    assert "/api/v1/application-releases/{release_id}/compatibility" in paths
    for path in paths:
        assert path.startswith((
            "/api/v1/auth/", "/api/v1/admin/", "/api/v1/invites",
            "/api/v1/tokens", "/api/v1/teams", "/api/v1/team-registrations", "/api/v1/health",
            "/api/v1/environments", "/api/v1/environment-operations",
            "/api/v1/applications", "/api/v1/application-operations",
            "/api/v1/application-builds", "/api/v1/application-sources",
            "/api/v1/application-capabilities", "/api/v1/application-releases/",
        )), path


def test_application_still_rejects_invalid_execution_profile() -> None:
    settings = LoomServiceSettings(
        _env_file=None, db_url="postgresql+psycopg://u:p@localhost/application",
        minio_access_key="access", minio_secret_key="secret",
        service_execution_runtime_profile_json="not a workload profile",
    )
    with pytest.raises(ValueError):
        create_app(settings)
