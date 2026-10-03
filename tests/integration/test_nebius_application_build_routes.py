"""Real owner authentication reaches retained personal-build controls only."""
from __future__ import annotations

from uuid import UUID, uuid4

import httpx
import pytest
from sqlalchemy import func, select

from loom.db.nebius_application_build_schema import NebiusApplicationBuildAttempt
from loom_service.app import register_api_routes
from loom_service.application_management.build_journal import ApplicationBuildJournal
from tests.integration.test_nebius_application_build_registry import build_registry
from tests.integration.test_nebius_application_source_routes import source_api as source_api
from tests.integration.test_nebius_application_source_upload import (
    environment_registry as environment_registry,
)
from tests.integration.test_nebius_application_source_upload import (
    platform_inputs as platform_inputs,
)
from tests.unit.test_application_source_archive import source as source
from tests.unit.test_nebius_application_image_renderer import build_inputs as build_inputs


@pytest.fixture
async def build_api(source_api, build_inputs):
    app, alice, bob, sources, _, _, intent, body = source_api
    registry = build_registry(sources.session_factory, sources, build_inputs[0])
    app.state.application_build_registry = registry
    upload = await alice.post("/api/v1/application-sources", json=intent.model_dump(mode="json"),
        headers={"Idempotency-Key": "source-for-build"})
    assert upload.status_code == 201, upload.text
    upload_id = upload.json()["upload_id"]
    response = await alice.put(f"/api/v1/application-sources/{upload_id}/content", content=body,
        headers={"Content-Type": "application/octet-stream"})
    assert response.status_code == 200, response.text
    yield app, alice, bob, registry, upload_id


async def test_owner_build_creation_status_and_replay_retain_server_selected_source(build_api):
    _, alice, bob, registry, upload_id = build_api
    response = await alice.post("/api/v1/application-builds", json={"upload_id": upload_id},
        headers={"Idempotency-Key": "my-build"})
    assert response.status_code == 201, response.text
    build = response.json()
    assert build["phase"] == "queued" and build["attempt"] == 1 and build["upload_id"] == upload_id
    assert response.headers["cache-control"] == "no-store"
    path = "/api/v1/application-builds/" + build["build_id"]
    replay = await alice.post("/api/v1/application-builds", json={"upload_id": upload_id},
        headers={"Idempotency-Key": "my-build"})
    assert replay.json() == build and (await alice.get(path)).json() == build
    assert (await bob.get(path)).status_code == 403
    assert (await bob.post("/api/v1/application-builds", json={"upload_id": upload_id},
        headers={"Idempotency-Key": "foreign-build"})).status_code == 403
    async with registry.session_factory() as session:
        row = await session.get(NebiusApplicationBuildAttempt, (UUID(build["build_id"]), 1))
        assert row.pool_request_json is None
        assert row.claim_json["recipe"] == registry.binding.recipe.model_dump(mode="json")


@pytest.mark.parametrize("field,value", [("priority", 0), ("target_id", "production"),
    ("registry_images", {"service": "foreign:image"}), ("owner_user_id", str(uuid4()))])
async def test_owner_cannot_supply_build_authority(build_api, field, value):
    _, alice, _, _, upload_id = build_api
    response = await alice.post("/api/v1/application-builds", json={"upload_id": upload_id, field: value},
        headers={"Idempotency-Key": "forged"})
    assert response.status_code == 422, response.text


async def test_cancel_and_retry_are_owner_scoped_generation_checked_and_cleanup_qualified(build_api):
    _, alice, bob, registry, upload_id = build_api
    response = await alice.post("/api/v1/application-builds", json={"upload_id": upload_id},
        headers={"Idempotency-Key": "cancel-me"})
    assert response.status_code == 201, response.text
    build_id = UUID(response.json()["build_id"])
    path = f"/api/v1/application-builds/{build_id}"
    assert (await bob.post(path + "/cancel", json={"attempt": 1})).status_code == 403
    assert (await alice.post(path + "/cancel", json={"attempt": 2})).status_code == 409
    cancelled = await alice.post(path + "/cancel", json={"attempt": 1})
    assert cancelled.status_code == 202 and cancelled.json()["desired_state"] == "cancelled", cancelled.text
    assert cancelled.json()["phase"] == "queued"  # Intent alone is not cleanup.
    assert (await alice.post(path + "/retry", json={"attempt": 1})).status_code == 409
    journal = ApplicationBuildJournal(registry)
    lease = await journal.claim(build_id, attempt=1)
    await journal.cancelled_unstarted(lease, None)
    assert (await alice.get(path)).json()["phase"] == "cancelled"
    assert (await bob.post(path + "/retry", json={"attempt": 1})).status_code == 403
    retry = await alice.post(path + "/retry", json={"attempt": 1})
    assert retry.status_code == 202, retry.text
    assert retry.json()["attempt"] == 2 and retry.json()["phase"] == "queued"
    assert retry.json()["desired_state"] == "running"
    assert (await alice.post(path + "/retry", json={"attempt": 1})).json() == retry.json()
    assert (await alice.post(path + "/cancel", json={"attempt": 1})).status_code == 409
    async with registry.session_factory() as session:
        assert await session.scalar(select(func.count()).select_from(NebiusApplicationBuildAttempt)) == 2
        old, new = [await session.get(NebiusApplicationBuildAttempt, (build_id, attempt)) for attempt in (1, 2)]
        assert old.phase == "cancelled" and new.pool_request_json is None
        assert new.claim_json == {**old.claim_json, "attempt": 2}


async def test_build_controls_require_csrf_and_configured_manager(build_api):
    app, alice, _, _, upload_id = build_api
    csrf = alice.headers.pop("X-Loom-CSRF")
    response = await alice.post("/api/v1/application-builds", json={"upload_id": upload_id},
        headers={"Idempotency-Key": "csrf"})
    assert response.status_code == 403, response.text
    alice.headers["X-Loom-CSRF"] = csrf
    del app.state.application_build_registry
    response = await alice.post("/api/v1/application-builds", json={"upload_id": upload_id},
        headers={"Idempotency-Key": "disabled"})
    assert response.status_code == 503, response.text


async def test_build_endpoints_are_not_registered_on_personal_application():
    from fastapi import FastAPI

    app = FastAPI()
    register_api_routes(app, management=False, include_local_execution=False)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="https://personal.example.com") as client:
        assert (await client.post("/api/v1/application-builds", json={})).status_code == 404
        assert (await client.get(f"/api/v1/application-builds/{uuid4()}")).status_code == 404
