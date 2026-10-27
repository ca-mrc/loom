"""The shipped lifespan configures management without tests attaching a manager."""

from __future__ import annotations

import asyncio
import json
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import select

from loom.db.nebius_environment_schema import NebiusPlatformBudget
from loom.db.schema import Team, TeamMembership, User
from loom_service.app import create_app
from loom_service.config import LoomServiceSettings
from loom_service.password_auth import hash_password
from tests.support.nebius_platform import platform_inputs as platform_inputs
from tests.unit.test_nebius_environment_contract import foundation_from
from tests.unit.test_nebius_kubernetes import FakeSDK
from tests.unit.test_nebius_kubernetes import connection as connection


@pytest.fixture
def installation_file(tmp_path, platform_inputs):
    path = tmp_path / "installation.json"
    path.write_text(json.dumps({
        "schema_version": "loom.nebius-management-installation.v1",
        "foundation": foundation_from(platform_inputs[0]).model_dump(mode="json"),
        "registry_prefix": "cr.eu-north1.nebius.cloud/e00example",
        "keyring": {"schema_version": 1, "keys": []}, "publications": [],
        "platform_budget": {"cpu_millis": 10000, "memory_mib": 20000,
                            "storage_mib": 100000, "ephemeral_storage_mib": 100000},
    }))
    return path


async def test_configured_lifespan_exposes_registry_and_reopens_same_platform_budget(
    isolated_migration_postgres_url, installation_file,
):
    settings = LoomServiceSettings(
        _env_file=None, service_mode="management", db_url=isolated_migration_postgres_url,
        public_base_url="https://manage.example.com", auth_local_http=False,
        environment_management_config_file=installation_file, environment_management_github_token="test-token",
    )
    team, owner = uuid4(), uuid4()
    for first in (True, False):
        app = create_app(settings)
        async with app.router.lifespan_context(app):
            async with app.state.session_factory.begin() as session:
                budget = (await session.execute(select(NebiusPlatformBudget))).scalar_one()
                assert budget.cpu_millis == 10000
                assert budget.memory_mib == 20000
                if first:
                    session.add(Team(id=team, name="managed-team"))
                    session.add(User(id=owner, username="alice", username_normalized="alice", status="active",
                                     password_hash=hash_password("owner-passphrase")))
                    await session.flush()
                    session.add(TeamMembership(team_id=team, user_id=owner, role="owner"))
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                         base_url="https://manage.example.com") as client:
                login = await client.post("/api/v1/auth/login", json={"username": "alice", "password": "owner-passphrase"})
                assert login.status_code == 200
                client.headers["X-Loom-CSRF"] = login.json()["csrf_token"]
                assert (await client.get("/api/v1/environments")).json() == {"items": []}
                response = await client.post("/api/v1/environments", json={"slug": "alice", "candidate_id": str(uuid4())},
                                             headers={"Idempotency-Key": "not-published"})
                assert response.status_code == 404
                assert response.json() == {"detail": {"code": "candidate_not_available"}}
        assert not hasattr(app.state, "environment_manager")
        assert app.state._owned_management_http_client.is_closed


async def test_configured_budget_change_requires_explicit_migration_not_implicit_resize(
    isolated_migration_postgres_url, installation_file,
):
    settings = LoomServiceSettings(
        _env_file=None, service_mode="management", db_url=isolated_migration_postgres_url,
        environment_management_config_file=installation_file, environment_management_github_token="test-token",
    )
    first = create_app(settings)
    async with first.router.lifespan_context(first):
        pass
    data = json.loads(installation_file.read_text())
    data["platform_budget"]["cpu_millis"] += 1
    installation_file.write_text(json.dumps(data))
    second = create_app(settings)
    with pytest.raises(ValueError, match="platform_budget_configuration_changed"):
        async with second.router.lifespan_context(second):
            pytest.fail("silently resized platform admission allowance")
    assert not hasattr(second.state, "environment_manager")


async def test_invalid_installation_fails_startup_without_logging_its_contents(
    isolated_migration_postgres_url, installation_file,
):
    installation_file.write_text('{"private-configuration": "not-for-logs"}')
    app = create_app(LoomServiceSettings(
        _env_file=None, service_mode="management", db_url=isolated_migration_postgres_url,
        environment_management_config_file=installation_file, environment_management_github_token="test-token",
    ))
    with pytest.raises(ValueError, match="invalid_environment_management_installation") as caught:
        async with app.router.lifespan_context(app):
            pytest.fail("accepted invalid installation")
    assert "not-for-logs" not in str(caught.value)
    assert not hasattr(app.state, "environment_manager")


@pytest.mark.parametrize("authentication", ["nebius", "projected"])
async def test_provider_runtime_is_supervised_and_closed_before_database_shutdown(
    isolated_migration_postgres_url, installation_file, connection, monkeypatch, authentication,
):
    import nebius.sdk

    created = []

    def sdk_factory(**kwargs):
        assert kwargs["credentials_file_name"] == str(connection.credentials_file)
        instance = FakeSDK()
        created.append(instance)
        return instance

    monkeypatch.setattr(nebius.sdk, "SDK", sdk_factory)
    data = json.loads(installation_file.read_text())
    data["foundation"]["provisioning_project_id"] = "project-managed-storage"
    data["provider_runtime"] = {
        "kubernetes": connection.model_dump(mode="json"),
        "cloud_credentials_file": str(connection.credentials_file),
        "poll_seconds": 1,
    }
    if authentication == "projected":
        token = connection.credentials_file.parent / "token"
        token.write_text("explicit-projected-token")
        token.chmod(0o440)
        data["provider_runtime"]["kubernetes"] = {
            "kind": "projected_service_account", "endpoint": connection.endpoint,
            "ca_file": str(connection.ca_file), "token_file": str(token),
        }
    installation_file.write_text(json.dumps(data))
    app = create_app(LoomServiceSettings(
        _env_file=None, service_mode="management", db_url=isolated_migration_postgres_url,
        environment_management_config_file=installation_file, environment_management_github_token="test-token",
    ))
    async with app.router.lifespan_context(app):
        runtime = app.state.environment_runtime
        async with asyncio.timeout(5):
            while not runtime.ready:
                await asyncio.sleep(0.01)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="https://management.example") as client:
            ready = await client.get("/api/v1/health/ready")
            assert ready.status_code == 200
            assert ready.json()["provisioner"] == "ready"
            runtime.task.cancel()
            await asyncio.gather(runtime.task, return_exceptions=True)
            stopped = await client.get("/api/v1/health/ready")
            assert stopped.status_code == 503
            assert stopped.json() == {"status": "not-ready", "mode": "management",
                                      "postgres": "ready", "provisioner": "not-ready"}
            assert (await client.get("/api/v1/health")).status_code == 200
    assert runtime.task.done()
    assert runtime.kubernetes.http.is_closed
    assert len(created) == (1 if authentication == "projected" else 2)
    assert all(instance.closed for instance in created)
    assert not hasattr(app.state, "environment_runtime")
    assert not hasattr(app.state, "session_factory")


async def test_invalid_provider_credentials_close_clients_and_do_not_start_worker(
    isolated_migration_postgres_url, installation_file, connection, monkeypatch,
):
    import nebius.sdk

    created = []

    def sdk_factory(**kwargs):
        instance = FakeSDK()
        created.append(instance)
        return instance

    monkeypatch.setattr(nebius.sdk, "SDK", sdk_factory)
    data = json.loads(installation_file.read_text())
    data["foundation"]["provisioning_project_id"] = "project-managed-storage"
    data["provider_runtime"] = {
        "kubernetes": connection.model_dump(mode="json"),
        "cloud_credentials_file": str(connection.credentials_file.parent / "missing-private-file"),
    }
    installation_file.write_text(json.dumps(data))
    app = create_app(LoomServiceSettings(
        _env_file=None, service_mode="management", db_url=isolated_migration_postgres_url,
        environment_management_config_file=installation_file, environment_management_github_token="test-token",
    ))
    with pytest.raises(ValueError, match="invalid_environment_provider_credentials"):
        async with app.router.lifespan_context(app):
            pytest.fail("started without explicitly configured provider credentials")
    assert all(instance.closed for instance in created)
    assert not hasattr(app.state, "environment_runtime")
    assert not hasattr(app.state, "session_factory")
    assert app.state._owned_management_http_client.is_closed
