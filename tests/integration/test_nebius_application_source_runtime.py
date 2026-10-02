"""Installed configuration must own real source storage, not an app-state seam."""
from __future__ import annotations

import hashlib
import io
import json
from pathlib import Path
from uuid import uuid4

import boto3
import httpx
import pytest
from botocore.response import StreamingBody
from botocore.stub import Stubber

from loom.db.schema import Team, TeamMembership, User
from loom_service.app import create_app
from loom_service.config import LoomServiceSettings
from loom_service.environment_management.installation import ManagementInstallation
from loom_service.password_auth import hash_password
from tests.integration.test_nebius_application_installation import (
    application_installation as application_installation,
)
from tests.integration.test_nebius_management_installation import (
    installation_file as installation_file,
)
from tests.unit.test_application_source_archive import archive_bytes
from tests.unit.test_application_source_archive import source as source
from tests.unit.test_nebius_kubernetes import FakeSDK
from tests.unit.test_nebius_kubernetes import connection as connection
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


@pytest.fixture
def source_installation(application_installation, tmp_path):
    path, data = application_installation
    credentials = tmp_path / "source-credentials.json"
    credentials.write_text(json.dumps({"access-key": "explicit-source-access", "secret-key": "explicit-source-secret"}))
    credentials.chmod(0o600)
    spool = tmp_path / "source-spool"
    spool.mkdir(mode=0o700)
    data["applications"]["runtime"]["source_upload"] = {
        "credentials_file": str(credentials), "spool_directory": str(spool),
        "max_inflight": 1, "receive_timeout_seconds": 61, "storage_timeout_seconds": 62,
        "upload_ttl_seconds": 600,
    }
    path.write_text(json.dumps(data))
    return path, data


def _app(path, database):
    return create_app(LoomServiceSettings(_env_file=None, service_mode="management", db_url=database,
        environment_management_config_file=path, environment_management_github_token="fixture-token",
        public_base_url="https://management.example.com", auth_local_http=False,
        management_http_max_body_bytes=512))


def test_omitted_upload_configuration_preserves_installation_serialization(application_installation):
    path, _ = application_installation
    installation = ManagementInstallation.load(path)
    assert "source_upload" not in installation.model_dump(mode="json")["applications"]["runtime"]


@pytest.mark.parametrize("field,value", [
    ("max_inflight", 0), ("max_inflight", 17), ("max_inflight", True),
    ("receive_timeout_seconds", 0), ("storage_timeout_seconds", 3601),
    ("upload_ttl_seconds", 59), ("source_bucket", "caller-bucket"),
    ("credentials_file", "relative-credential"), ("spool_directory", "relative-spool"),
])
def test_upload_settings_reject_unbounded_or_caller_storage_authority(source_installation, field, value):
    path, data = source_installation
    data["applications"]["runtime"]["source_upload"][field] = value
    path.write_text(json.dumps(data))
    with pytest.raises(ValueError, match="invalid_environment_management_installation"):
        ManagementInstallation.load(path)


async def test_configured_lifespan_uploads_with_explicit_s3_identity_and_closes_storage(
        source_installation, isolated_migration_postgres_url, source, monkeypatch):
    import nebius.sdk

    path, data = source_installation
    _, captured = source
    body = archive_bytes(captured)
    digest = hashlib.sha256(body).hexdigest()
    config = json.loads(data["foundation"]["platform_config_json"])
    key = f"application-sources/v1/sha256/{digest}.tar"
    sdk = FakeSDK()
    monkeypatch.setattr(nebius.sdk, "SDK", lambda **kwargs: sdk)
    real_client = boto3.client
    clients, stubs, closed = [], [], []

    def client_factory(**kwargs):
        assert kwargs["endpoint_url"] == config["storage_endpoint"]
        assert kwargs["region_name"] == config["region"]
        assert kwargs["aws_access_key_id"] == "explicit-source-access"
        assert kwargs["aws_secret_access_key"] == "explicit-source-secret"
        client = real_client(**kwargs)
        stub = Stubber(client)
        stub.add_response("put_object", {}, {"Bucket": config["buckets"]["source"], "Key": key,
            "Body": body, "ChecksumAlgorithm": "SHA256"})
        stub.add_response("get_object", {"Body": StreamingBody(io.BytesIO(body), len(body))},
            {"Bucket": config["buckets"]["source"], "Key": key})
        stub.activate()
        close = client.close

        def close_client():
            closed.append(client)
            close()

        client.close = close_client
        clients.append(client)
        stubs.append(stub)
        return client

    monkeypatch.setattr(boto3, "client", client_factory)
    # Ambient credentials must never replace the explicitly installed identity.
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "wrong-ambient-access")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "wrong-ambient-secret")
    app = _app(path, isolated_migration_postgres_url)
    async with app.router.lifespan_context(app):
        uploader = app.state.application_source_uploader
        assert uploader is app.state.application_runtime.source_uploader
        assert uploader.max_inflight == 1 and uploader.receive_timeout == 61 and uploader.storage_timeout == 62
        binding = uploader.registry.binding
        assert str(binding.installation_id) == data["applications"]["authority"]["installation_id"]
        assert str(binding.data_environment_id) == data["applications"]["shared"]["data_environment_id"]
        assert binding.cluster_id == config["cluster_id"] and binding.upload_ttl_seconds == 600
        assert binding.source_bucket == config["buckets"]["source"]
        team, owner = uuid4(), uuid4()
        async with app.state.session_factory.begin() as session:
            session.add(Team(id=team, name="source-owners"))
            session.add(User(id=owner, username="source-alice", username_normalized="source-alice", status="active",
                password_hash=hash_password("fixture-source-passphrase")))
            await session.flush()
            session.add(TeamMembership(team_id=team, user_id=owner, role="owner"))
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="https://management.example.com") as client:
            login = await client.post("/api/v1/auth/login", json={"username": "source-alice", "password": "fixture-source-passphrase"})
            assert login.status_code == 200, login.text
            client.headers["X-Loom-CSRF"] = login.json()["csrf_token"]
            receipt = await client.post("/api/v1/application-sources", headers={"Idempotency-Key": "installed-source"},
                json={"source_digest": captured.digest, "archive_sha256": digest, "archive_size_bytes": len(body)})
            assert receipt.status_code == 201, receipt.text
            response = await client.put(f'/api/v1/application-sources/{receipt.json()["upload_id"]}/content',
                content=body, headers={"Content-Type": "application/octet-stream"})
            assert response.status_code == 200 and response.json()["phase"] == "source_verified", response.text
        assert closed == []
        assert list(uploader.directory.iterdir()) == []
    assert len(clients) == 1 and closed == clients and sdk.closed
    stubs[0].assert_no_pending_responses()
    for name in ("application_source_uploader", "application_runtime", "session_factory"):
        assert not hasattr(app.state, name)


@pytest.mark.parametrize("damage", ["missing", "public", "invalid", "extra", "empty", "spool"])
async def test_invalid_source_material_fails_startup_without_exposing_admission_or_leaking_clients(
        source_installation, isolated_migration_postgres_url, monkeypatch, damage):
    import nebius.sdk

    path, data = source_installation
    settings = data["applications"]["runtime"]["source_upload"]
    credentials = Path(settings["credentials_file"])
    if damage == "missing":
        credentials.unlink()
    elif damage == "public":
        credentials.chmod(0o644)
    elif damage == "invalid":
        credentials.write_text("explicit-source-secret")
    elif damage == "extra":
        credentials.write_text(json.dumps({"access-key": "a", "secret-key": "b", "endpoint": "https://foreign.invalid"}))
    elif damage == "empty":
        credentials.write_text(json.dumps({"access-key": "", "secret-key": "explicit-source-secret"}))
    else:
        Path(settings["spool_directory"]).chmod(0o755)
    sdk, closed, created = FakeSDK(), [], []
    monkeypatch.setattr(nebius.sdk, "SDK", lambda **kwargs: sdk)
    real_client = boto3.client

    def client_factory(**kwargs):
        client = real_client(**kwargs)
        close = client.close
        def close_client():
            closed.append(client)
            close()
        client.close = close_client
        created.append(client)
        return client

    monkeypatch.setattr(boto3, "client", client_factory)
    app = _app(path, isolated_migration_postgres_url)
    with pytest.raises(ValueError, match="invalid_application_source_runtime") as caught:
        async with app.router.lifespan_context(app):
            pytest.fail("unsafe source upload runtime started")
    assert "explicit-source-secret" not in str(caught.value)
    assert sdk.closed and closed == created
    for name in ("application_source_uploader", "application_manager", "application_runtime", "session_factory"):
        assert not hasattr(app.state, name)
