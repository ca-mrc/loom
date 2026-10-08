"""Process capabilities must not masquerade as installed execution readiness."""
from __future__ import annotations

import hashlib

import httpx
import pytest
from sqlalchemy import update

from loom.db.schema import Token
from loom_service.app import register_api_routes
from tests.integration.test_nebius_application_source_routes import source_api as source_api
from tests.integration.test_nebius_application_source_upload import (
    environment_registry as environment_registry,
)
from tests.integration.test_nebius_application_source_upload import (
    platform_inputs as platform_inputs,
)
from tests.unit.test_application_source_archive import source as source

PATH = "/api/v1/application-capabilities"


async def test_capabilities_distinguish_source_intake_from_lifecycle_builds_and_execution(source_api):
    app, alice, _, _, _, _, _, _ = source_api
    response = await alice.get(PATH)
    assert response.status_code == 200, response.text
    assert response.headers["cache-control"] == "no-store"
    assert response.json() == {
        "schema_version": "loom.nebius-application-capabilities.v1",
        "scope": "management_process",
        "application_lifecycle": "not_configured",
        "source_upload": "configured",
        "image_builds": "not_configured",
        "execution": "not_checked",
    }
    del app.state.application_source_uploader
    unavailable = await alice.get(PATH)
    assert unavailable.status_code == 200
    assert unavailable.json()["source_upload"] == "not_configured"
    assert unavailable.json()["execution"] == "not_checked"


async def test_capabilities_require_authentication(source_api):
    app, _, _, _, _, _, _, _ = source_api
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                 base_url="https://management.example.com") as anonymous:
        assert (await anonymous.get(PATH)).status_code == 401


@pytest.mark.parametrize("scope,attributed,status", [
    ("read:own", True, 200), ("submit", True, 403), ("read:own", False, 403),
])
async def test_capabilities_require_attributed_read_permission(source_api, scope, attributed, status):
    app, alice, _, registry, _, _, _, _ = source_api
    minted = await alice.post("/api/v1/tokens", json={
        "name": "capabilities", "type": "team", "scopes": [scope], "expires_in_days": 1,
    })
    assert minted.status_code == 201, minted.text
    raw = minted.json()["token"]
    if not attributed:
        async with registry.session_factory.begin() as session:
            await session.execute(update(Token).where(Token.token_hash == hashlib.sha256(raw.encode()).digest())
                                  .values(created_by_user_id=None))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
            base_url="https://management.example.com", headers={"Authorization": "Bearer " + raw}) as bearer:
        response = await bearer.get(PATH)
        assert response.status_code == status, response.text
        assert raw not in response.text
        if status == 403:
            assert "source_upload" not in response.json()


async def test_personal_application_does_not_expose_management_capabilities():
    from fastapi import FastAPI

    app = FastAPI()
    register_api_routes(app, management=False, include_local_execution=False)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                 base_url="https://personal.example.com") as client:
        assert (await client.get(PATH)).status_code == 404
