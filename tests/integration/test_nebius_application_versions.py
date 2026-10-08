"""Owner-scoped app version evidence and non-mutating schema compatibility."""
from __future__ import annotations

from dataclasses import replace
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import func, select

from loom.db.nebius_application_operation_schema import NebiusApplicationOperation
from loom.nebius_application_contract import ApplicationCreateRequestV1, ApplicationOperationRequestV1
from loom_service.environment_management.registry import ManagementError
from tests.integration.test_nebius_application_credentials import database_access as database_access
from tests.integration.test_nebius_application_credentials import shared_ca as shared_ca
from tests.integration.test_nebius_application_manager import manager
from tests.integration.test_nebius_application_material import management_key as management_key
from tests.integration.test_nebius_application_operations import applications as applications
from tests.integration.test_nebius_application_preparation import preparation as preparation
from tests.integration.test_nebius_application_ready import ready_context as ready_context
from tests.integration.test_nebius_environment_management import environment_registry as environment_registry
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


@pytest.fixture
async def application_api(applications, platform_inputs, isolated_migration_postgres_url):
    from loom.db.schema import TeamMembership, User
    from loom_service.app import create_app
    from loom_service.config import LoomServiceSettings
    from loom_service.password_auth import hash_password

    registry, factory, (alice, bob), _, _, _ = applications
    service, release = manager(registry, platform_inputs)
    async with factory.begin() as session:
        for principal, name in ((alice, "alice"), (bob, "bob")):
            user = await session.get(User, principal.user_id)
            user.password_hash = hash_password(name + "-owner-passphrase")
            session.add(TeamMembership(user_id=principal.user_id, team_id=principal.team_id, role="owner"))
    app = create_app(LoomServiceSettings(_env_file=None, service_mode="management",
        db_url=isolated_migration_postgres_url, auth_local_http=False, public_base_url="https://management.example.com"))
    async with app.router.lifespan_context(app):
        app.state.application_manager = service
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="https://management.example.com") as a, \
                httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="https://management.example.com") as b:
            for client, name in ((a, "alice"), (b, "bob")):
                login = await client.post("/api/v1/auth/login", json={"username": name, "password": name + "-owner-passphrase"})
                assert login.status_code == 200
                client.headers["X-Loom-CSRF"] = login.json()["csrf_token"]
            yield app, a, b, service, release, alice


async def test_release_compatibility_and_versions_are_read_only_and_owner_scoped(application_api):
    app, alice, bob, service, release, principal = application_api
    path = f"/api/v1/application-releases/{release.release_id}/compatibility"
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="https://management.example.com") as anonymous:
        assert (await anonymous.get(path)).status_code == 401
    response = await alice.get(path)
    assert response.status_code == 200, response.text
    assert response.headers["cache-control"] == "no-store"
    assert response.json()["compatibility"] == "compatible"
    assert response.json()["release"] == release.model_dump(mode="json")
    async with service.registry.session_factory() as session:
        assert await session.scalar(select(func.count()).select_from(NebiusApplicationOperation)) == 0
    operation = await service.create(principal, ApplicationCreateRequestV1(slug="alice", release_id=release.release_id),
                                     idempotency_key="version-test")
    version_path = f"/api/v1/applications/{operation.application_id}/versions"
    response = await alice.get(version_path)
    assert response.status_code == 200, response.text
    assert response.headers["cache-control"] == "no-store"
    assert response.json()["requested_release"] == release.model_dump(mode="json")
    assert response.json()["last_completed_deployment"] is None
    assert response.json()["scope"] == "deployment_journal"
    assert (await bob.get(version_path)).status_code == 403
    assert set((await alice.get(f"/api/v1/applications/{operation.application_id}")).json()) == {"registration", "operation"}
    assert (await service.registry.get_operation(operation.operation_id, principal=principal)) == operation


async def test_schema_mismatch_can_be_checked_before_create_and_cannot_mutate(application_api):
    _, client, _, service, release, principal = application_api
    incompatible = release.model_copy(update={"release_id": uuid4(), "schema_revision": "different_schema"})
    service._releases[incompatible.release_id] = incompatible
    response = await client.get(f"/api/v1/application-releases/{incompatible.release_id}/compatibility")
    assert response.status_code == 200, response.text
    assert response.json()["compatibility"] == "schema_mismatch"
    assert response.json()["shared_schema_revision"] == service.shared.schema_revision
    response = await client.post("/api/v1/applications", json={"slug": "incompatible", "release_id": str(incompatible.release_id)},
                                 headers={"Idempotency-Key": "mismatch"})
    assert response.status_code == 409
    assert response.json()["detail"]["code"] == "application_schema_mismatch"
    assert not await service.registry.list_applications(principal=principal)


async def test_pending_update_and_suspend_preserve_last_completed_version(ready_context, platform_inputs):
    from loom_service.application_management.versions import read_application_versions

    coordinator, registry, factory, alice, lease, _, _, _ = ready_context
    plan = await registry.frozen_plan(lease)
    await coordinator.start(lease)
    service, original = manager(registry, platform_inputs, plan=plan, authority=coordinator.runtime.authority)
    updated = original.model_copy(update={"release_id": uuid4(), "source_digest": "sha256:" + "f" * 64})
    service._releases[updated.release_id] = updated
    operation = await service.transition(alice, lease.application_id,
        ApplicationOperationRequestV1(action="update", expected_generation=1, release_id=updated.release_id),
        idempotency_key="version-update")
    # Catalog removal must not remove historical code/version evidence.
    service._releases.clear()
    report = await read_application_versions(factory, lease.application_id, principal=alice,
                                             shared_schema_revision=service.shared.schema_revision)
    assert report.status.operation == operation and report.requested_release == updated
    assert report.last_completed_deployment.release == original
    assert report.last_completed_deployment.deployment_generation == 1
    assert report.last_completed_deployment.operation_id == lease.operation_id
    assert report.last_completed_deployment.completed_at is not None
    stop = await service.transition(alice, lease.application_id,
        ApplicationOperationRequestV1(action="suspend", expected_generation=2), idempotency_key="version-stop")
    stopped = await read_application_versions(factory, lease.application_id, principal=alice,
                                               shared_schema_revision=service.shared.schema_revision)
    assert stopped.status.operation == stop and stopped.status.registration.desired_state == "suspended"
    assert stopped.last_completed_deployment == report.last_completed_deployment
    assert stopped.requested_release == updated
    for principal in (replace(alice, team_id=uuid4()), replace(alice, scopes=["submit"])):
        with pytest.raises(ManagementError):
            await read_application_versions(factory, lease.application_id, principal=principal,
                                             shared_schema_revision=service.shared.schema_revision)


async def test_personal_app_has_no_version_or_compatibility_management_route():
    from fastapi import FastAPI

    from loom_service.app import register_api_routes

    app = FastAPI()
    register_api_routes(app, management=False, include_local_execution=False)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="https://personal.example.com") as client:
        for path in (f"/api/v1/applications/{uuid4()}/versions", f"/api/v1/application-releases/{uuid4()}/compatibility"):
            assert (await client.get(path)).status_code == 404
