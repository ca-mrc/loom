"""Real management auth/middleware must precede private source body consumption."""
from __future__ import annotations

import hashlib
from uuid import uuid4

import httpx
import pytest

from loom.application_source_archive import MAX_APPLICATION_SOURCE_ARCHIVE_BYTES
from loom.application_source_upload import ApplicationSourceUploadRequestV1
from loom.db.schema import TeamMembership, User
from loom.trajectory.storage import FakeObjectStore
from loom_service.app import create_app, register_api_routes
from loom_service.application_management.source_upload import ApplicationSourceUploader
from loom_service.config import LoomServiceSettings
from loom_service.password_auth import hash_password
from tests.integration.test_nebius_application_source_upload import (
    environment_registry as environment_registry,
)
from tests.integration.test_nebius_application_source_upload import (
    platform_inputs as platform_inputs,
)
from tests.integration.test_nebius_application_source_upload import upload_registry
from tests.unit.test_application_source_archive import archive_bytes
from tests.unit.test_application_source_archive import source as source


@pytest.fixture
async def source_api(environment_registry, isolated_migration_postgres_url, source, tmp_path):
    _, factory, owners, _ = environment_registry
    registry, store = upload_registry(factory), FakeObjectStore()
    async with factory.begin() as session:
        for owner, name in zip(owners, ("alice", "bob"), strict=True):
            user = await session.get(User, owner.user_id)
            user.password_hash = hash_password(name + "-source-passphrase")
            session.add(TeamMembership(user_id=owner.user_id, team_id=owner.team_id, role="owner"))
    app = create_app(LoomServiceSettings(_env_file=None, service_mode="management",
        db_url=isolated_migration_postgres_url, auth_local_http=False,
        public_base_url="https://management.example.com", management_http_max_body_bytes=512))
    _, model = source
    body = archive_bytes(model)
    intent = ApplicationSourceUploadRequestV1(source_digest=model.digest,
        archive_sha256=hashlib.sha256(body).hexdigest(), archive_size_bytes=len(body))
    async with app.router.lifespan_context(app):
        app.state.application_source_uploader = ApplicationSourceUploader(registry, store, spool_directory=tmp_path)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="https://management.example.com") as alice, \
                httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="https://management.example.com") as bob:
            for client, name in ((alice, "alice"), (bob, "bob")):
                login = await client.post("/api/v1/auth/login", json={"username": name, "password": name + "-source-passphrase"})
                assert login.status_code == 200, login.text
                client.headers["X-Loom-CSRF"] = login.json()["csrf_token"]
            yield app, alice, bob, registry, owners, store, intent, body


async def test_source_routes_upload_larger_than_json_cap_with_owner_isolation_and_replay(source_api):
    _, alice, bob, _, _, store, intent, body = source_api
    response = await alice.post("/api/v1/application-sources", json=intent.model_dump(mode="json"),
                                headers={"Idempotency-Key": "current-source"})
    assert response.status_code == 201, response.text
    receipt = response.json()
    path = "/api/v1/application-sources/" + receipt["upload_id"]
    assert response.headers["cache-control"] == "no-store"
    replay = await alice.post("/api/v1/application-sources", json=intent.model_dump(mode="json"),
                              headers={"Idempotency-Key": "current-source"})
    assert replay.json() == receipt
    assert (await bob.get(path)).status_code == 403
    assert (await alice.get(path)).json() == receipt
    uploaded = await alice.put(path + "/content", content=body, headers={"Content-Type": "application/octet-stream"})
    assert uploaded.status_code == 200, uploaded.text
    assert uploaded.json()["phase"] == "source_verified"
    assert uploaded.headers["cache-control"] == "no-store"
    assert list(store.objects.values()) == [body]
    assert (await alice.get(path)).json() == uploaded.json()
    assert (await alice.post(path + "/complete")).status_code == 404
    assert (await alice.post("/api/v1/application-sources", content=b"x" * 513)).status_code == 413


@pytest.mark.parametrize("caller", ["anonymous", "foreign-owner", "missing-csrf"])
async def test_upload_auth_rejects_before_any_body_read(source_api, caller):
    app, alice, bob, registry, owners, store, intent, body = source_api
    receipt = await registry.create(principal=owners[0], request=intent, idempotency_key="auth-first")
    consumed = []
    async def chunks():
        consumed.append(True)
        yield body
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="https://management.example.com") as anonymous:
        client = anonymous if caller == "anonymous" else bob if caller == "foreign-owner" else alice
        if caller == "missing-csrf":
            del client.headers["X-Loom-CSRF"]
        response = await client.put(f"/api/v1/application-sources/{receipt.upload_id}/content", content=chunks(),
            headers={"Content-Length": str(len(body)), "Content-Type": "application/octet-stream"})
    assert response.status_code == (401 if caller == "anonymous" else 403), response.text
    assert consumed == [] and store.objects == {}
    assert response.headers["connection"] == "close"
    assert response.headers["cache-control"] == "no-store"


@pytest.mark.parametrize("headers,status", [
    ({"Content-Length": "1"}, 400),
    ({"Content-Length": "10241"}, 400),
    ({"Content-Length": str(MAX_APPLICATION_SOURCE_ARCHIVE_BYTES + 1)}, 413),
    ({"Content-Encoding": "gzip"}, 415),
    ({"Content-Type": "text/plain"}, 415),
])
async def test_streaming_source_keeps_framing_and_encoding_bounds(source_api, headers, status):
    _, alice, _, registry, owners, store, intent, body = source_api
    receipt = await registry.create(principal=owners[0], request=intent, idempotency_key="framing")
    response = await alice.put(f"/api/v1/application-sources/{receipt.upload_id}/content", content=body,
        headers={"Content-Type": "application/octet-stream", **headers})
    assert response.status_code == status, response.text
    assert store.objects == {}
    assert await registry.status(receipt.upload_id, principal=owners[0]) == receipt


async def test_source_routes_fail_closed_without_configured_verifier(source_api):
    app, alice, _, _, _, _, intent, _ = source_api
    del app.state.application_source_uploader
    assert (await alice.post("/api/v1/application-sources", json=intent.model_dump(mode="json"),
        headers={"Idempotency-Key": "disabled"})).status_code == 503
    assert (await alice.put(f"/api/v1/application-sources/{uuid4()}/content", content=b"x" * 513)).status_code == 413


async def test_source_upload_endpoints_are_not_registered_on_personal_applications():
    from fastapi import FastAPI

    app = FastAPI()
    register_api_routes(app, management=False, include_local_execution=False)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="https://personal.example.com") as client:
        assert (await client.post("/api/v1/application-sources", json={})).status_code == 404
        assert (await client.get(f"/api/v1/application-sources/{uuid4()}")).status_code == 404
        assert (await client.put(f"/api/v1/application-sources/{uuid4()}/content", content=b"x")).status_code == 404
