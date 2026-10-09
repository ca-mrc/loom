"""Owner-scoped app version evidence and non-mutating schema compatibility."""
from __future__ import annotations

import asyncio
import copy
import hashlib
from dataclasses import replace
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import func, select, update

from loom.db.nebius_application_operation_schema import NebiusApplicationOperation
from loom.db.schema import Token
from loom.nebius_application_contract import (
    ApplicationCreateRequestV1,
    ApplicationOperationRequestV1,
)
from loom_service.environment_management.registry import ManagementError
from tests.integration.test_nebius_application_credentials import database_access as database_access
from tests.integration.test_nebius_application_credentials import shared_ca as shared_ca
from tests.integration.test_nebius_application_manager import manager
from tests.integration.test_nebius_application_material import management_key as management_key
from tests.integration.test_nebius_application_operations import applications as applications
from tests.integration.test_nebius_application_preparation import preparation as preparation
from tests.integration.test_nebius_application_ready import ready_context as ready_context
from tests.integration.test_nebius_environment_management import (
    environment_registry as environment_registry,
)
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


@pytest.mark.parametrize("scope,attributed,status", [("read:own", True, 200), ("submit", True, 403), ("read:own", False, 403)])
async def test_version_and_compatibility_reads_require_attributed_read_permission(application_api, scope, attributed, status):
    app, client, _, service, release, principal = application_api
    operation = await service.create(principal, ApplicationCreateRequestV1(slug="alice", release_id=release.release_id),
                                     idempotency_key="read-permissions")
    minted = await client.post("/api/v1/tokens", json={"name": "versions", "type": "team", "scopes": [scope], "expires_in_days": 1})
    assert minted.status_code == 201, minted.text
    token = minted.json()["token"]
    if not attributed:
        async with service.registry.session_factory.begin() as session:
            await session.execute(update(Token).where(Token.token_hash == hashlib.sha256(token.encode()).digest())
                                  .values(created_by_user_id=None))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="https://management.example.com",
                                headers={"Authorization": "Bearer " + token}) as bearer:
        for path in (f"/api/v1/application-releases/{release.release_id}/compatibility",
                     f"/api/v1/applications/{operation.application_id}/versions"):
            response = await bearer.get(path)
            assert response.status_code == status, response.text
            assert token not in response.text


async def test_unqualified_frozen_release_is_not_exposed(application_api):
    _, client, _, service, release, principal = application_api
    operation = await service.create(principal, ApplicationCreateRequestV1(slug="alice", release_id=release.release_id),
                                     idempotency_key="damaged-version")
    async with service.registry.session_factory.begin() as session:
        row = await session.get(NebiusApplicationOperation, operation.operation_id)
        plan = copy.deepcopy(row.plan_json)
        plan["release"]["credential"] = "fixture-private-material"
        row.plan_json = plan
    response = await client.get(f"/api/v1/applications/{operation.application_id}/versions")
    assert response.status_code == 503
    assert response.json()["detail"]["code"] == "application_versions_unqualified"
    assert "fixture-private-material" not in response.text


async def test_two_teammates_keep_distinct_versions_and_lifecycle_intents(application_api):
    _, alice, bob, service, alice_release, principal = application_api
    bob_release = alice_release.model_copy(update={"release_id": uuid4(), "source_digest": "sha256:" + "b" * 64,
        "service_image_ref": "registry.example/bob@sha256:" + "c" * 64})
    service._releases[bob_release.release_id] = bob_release
    replies = await asyncio.gather(*[
        client.post("/api/v1/applications", json={"slug": slug, "release_id": str(release.release_id)},
                    headers={"Idempotency-Key": "concurrent-onboarding"})
        for client, slug, release in ((alice, "alice", alice_release), (bob, "bob", bob_release))
    ])
    assert all(reply.status_code == 202 for reply in replies), [reply.text for reply in replies]
    ids = [reply.json()["application_id"] for reply in replies]
    before = await asyncio.gather(*[client.get(f"/api/v1/applications/{identity}/versions")
                                   for client, identity in zip((alice, bob), ids, strict=True)])
    reports = [reply.json() for reply in before]
    assert reports[0]["requested_release"] != reports[1]["requested_release"]
    assert reports[0]["status"]["registration"]["data_environment_id"] == reports[1]["status"]["registration"]["data_environment_id"]
    assert (await alice.get(f"/api/v1/applications/{ids[1]}/versions")).status_code == 403
    assert (await bob.get(f"/api/v1/applications/{ids[0]}/versions")).status_code == 403
    stopped, peer = await asyncio.gather(
        alice.post(f"/api/v1/applications/{ids[0]}/operations", json={"action": "suspend", "expected_generation": 1},
                   headers={"Idempotency-Key": "alice-stop"}),
        bob.get(f"/api/v1/applications/{ids[1]}/versions"),
    )
    assert stopped.status_code == 202, stopped.text
    assert peer.json() == reports[1]
    assert (await bob.get(f"/api/v1/applications/{ids[1]}/versions")).json() == reports[1]
    assert (await service.registry.list_applications(principal=principal))[0].desired_state == "suspended"


async def test_fifth_owner_onboards_without_replacing_four_existing_applications(applications, platform_inputs):
    from loom.db.nebius_application_operation_schema import NebiusApplicationReservation
    from loom.db.schema import User
    from loom_service.application_management.versions import read_application_versions

    registry, factory, (alice, bob), _, _, _ = applications
    service, release = manager(registry, platform_inputs)
    principals = [alice, bob]
    async with factory.begin() as session:
        for name in ("charlie", "diana", "eve"):
            identity = uuid4()
            session.add(User(id=identity, username=name, username_normalized=name, status="active"))
            principals.append(replace(alice, user_id=identity))
    releases = [release.model_copy(update={"release_id": uuid4(), "source_digest": "sha256:" + str(i + 1) * 64})
                for i in range(5)]
    service._releases = {item.release_id: item for item in releases}

    async def create(index):
        return await service.create(principals[index],
            ApplicationCreateRequestV1(slug=f"teammate-{index}", release_id=releases[index].release_id),
            idempotency_key="same-owner-local-key")

    existing = await asyncio.gather(*[create(index) for index in range(4)])
    await create(4)
    for principal, original in zip(principals, existing, strict=False):
        assert (await registry.status(original.application_id, principal=principal)).operation == original
        report = await read_application_versions(factory, original.application_id, principal=principal,
                                                 shared_schema_revision=service.shared.schema_revision)
        assert report.last_completed_deployment is None  # No deployed-worker evidence is fabricated.
    async with factory() as session:
        operations = list(await session.scalars(select(NebiusApplicationOperation)))
        assert len(operations) == 5
        assert len({op.plan_json["registration"]["application_namespace"] for op in operations}) == 5
        assert len({op.plan_json["shared"]["data_environment_id"] for op in operations}) == 1
        assert all(value == 0 for value in await session.scalars(select(NebiusApplicationReservation.storage_mib)))
        assert all(doc["kind"] not in {"StatefulSet", "PersistentVolumeClaim"}
                   for op in operations for group in op.plan_json["files"].values() for doc in group)


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
