"""Management transactions use real PostgreSQL, not an in-memory owner registry."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from uuid import uuid4

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.auth import AuthContext
from loom.db.schema import Team, User
from loom.nebius_environment_contract import EnvironmentRegistrationV1, new_environment_registration
from loom.nebius_environment_render import render_environment
from tests.unit.test_nebius_environment_contract import foundation_from
from tests.unit.test_nebius_platform_render import ROOT
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


@pytest.fixture
async def environment_registry(isolated_migration_postgres_url, platform_inputs, request):
    from loom.db.nebius_environment_schema import NebiusPlatformBudget
    from loom_service.environment_management.registry import EnvironmentRegistry

    if getattr(request, 'param', None) == '0176':
        from tests.integration.conftest import retain_pool_schema

        await asyncio.to_thread(retain_pool_schema, isolated_migration_postgres_url)
    engine = create_async_engine(isolated_migration_postgres_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    foundation = foundation_from(platform_inputs[0])
    team, alice, bob = uuid4(), uuid4(), uuid4()
    async with factory.begin() as session:
        session.add(Team(id=team, name="environment-owners"))
        for identity, name in ((alice, "alice"), (bob, "bob")):
            session.add(User(id=identity, username=name, username_normalized=name, status="active"))
        session.add(NebiusPlatformBudget(
            cluster_id=foundation.platform_config["cluster_id"], cpu_millis=100000,
            memory_mib=1000000, storage_mib=1000000, ephemeral_storage_mib=1000000,
        ))
    principals = [AuthContext(
        token_hash=b"", type="user", scopes=["read:own", "submit"], team_id=team,
        expires_at=None, user_id=user, auth_kind="session",
    ) for user in (alice, bob)]

    def prepare(slug="alice", principal=principals[0], *, candidate_id=None):
        row = new_environment_registration(
            foundation, environment_id=uuid4(), incarnation=uuid4(),
            owner_user_id=principal.user_id, owner_team_id=principal.team_id, slug=slug,
        )
        row = EnvironmentRegistrationV1.model_validate({
            **row.model_dump(), "candidate_id": candidate_id or candidate_identity,
        })
        return render_environment(row, platform_inputs[1], foundation,
                                  profile=platform_inputs[2], keyring={}, repo_root=ROOT)

    candidate_identity = uuid4()
    try:
        yield EnvironmentRegistry(factory), factory, principals, prepare
    finally:
        await engine.dispose()


async def test_create_replay_commits_one_environment_complete_namespaces_and_one_budget_hold(environment_registry):
    from loom.db.nebius_environment_schema import (
        NebiusEnvironment,
        NebiusEnvironmentNamespace,
        NebiusPlatformReservation,
    )

    registry, factory, (alice, _), prepare = environment_registry
    first = await registry.create(principal=alice, idempotency_key="create-1", prepared=prepare())
    replay = await registry.create(principal=alice, idempotency_key="create-1", prepared=prepare())
    assert replay == first
    assert first.phase == "pending"
    assert first.execution_enabled is False
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(NebiusEnvironment)) == 1
        assert await session.scalar(select(func.count()).select_from(NebiusEnvironmentNamespace)) == 3
        reservation = (await session.execute(select(NebiusPlatformReservation))).scalar_one()
        assert reservation.cpu_millis == 1050
        assert reservation.memory_mib == 2688


async def test_concurrent_retry_has_one_identity_and_different_payload_conflicts(environment_registry):
    from loom_service.environment_management.registry import ManagementError

    registry, _, (alice, _), prepare = environment_registry
    results = await asyncio.gather(*[
        registry.create(principal=alice, idempotency_key="same-key", prepared=prepare())
        for _ in range(3)
    ])
    assert len({item.operation_id for item in results}) == 1
    with pytest.raises(ManagementError, match="idempotency_conflict"):
        await registry.create(principal=alice, idempotency_key="same-key", prepared=prepare("another"))


async def test_generated_storage_reservation_and_plan_remain_frozen_on_replay(
    environment_registry, platform_inputs,
):
    from loom.db.nebius_environment_schema import (
        NebiusEnvironmentOperation,
        NebiusPlatformReservation,
    )
    from loom.nebius_environment_contract import EnvironmentCreateRequestV1, FoundationBinding
    from loom_service.environment_management.manager import (
        EnvironmentManager,
        EnvironmentPlanFactory,
    )

    registry, factory, (alice, _), prepare = environment_registry
    config, candidate, profile = platform_inputs
    foundation = FoundationBinding.model_validate({
        **foundation_from(config).model_dump(), "generated_postgres_storage_gi": 10,
    })
    prepared = render_environment(prepare().registration, candidate, foundation,
                                  profile=profile, keyring={}, repo_root=ROOT)
    first = await registry.create(principal=alice, idempotency_key="sized", prepared=prepared)

    class UnavailableCatalog:
        async def resolve(self, identity):
            raise ConnectionError("Publication unavailable after creation")

    changed = FoundationBinding.model_validate({
        **foundation.model_dump(), "generated_postgres_storage_gi": 100,
    })
    manager = EnvironmentManager(registry, EnvironmentPlanFactory(
        changed, UnavailableCatalog(), keyring={}, repo_root=ROOT,
    ))
    request = EnvironmentCreateRequestV1(slug="alice", candidate_id=prepared.registration.candidate_id)
    assert await manager.create(alice, request, idempotency_key="sized") == first
    async with factory() as session:
        reservation = (await session.execute(select(NebiusPlatformReservation))).scalar_one()
        operation = await session.get(NebiusEnvironmentOperation, first.operation_id)
        assert reservation.storage_mib == 10240
        assert reservation.ephemeral_storage_mib == prepared.platform_envelope.ephemeral_storage_mib
        assert operation.plan_json["config"]["postgres_storage_gi"] == 10
        database = next(doc for doc in operation.plan_json["files"]["20-database.yaml"]
                        if doc["kind"] == "StatefulSet")
        assert database["spec"]["volumeClaimTemplates"][0]["spec"]["resources"]["requests"]["storage"] == "10Gi"


async def test_owner_conflict_and_cross_owner_status_do_not_change_registration(environment_registry):
    from loom_service.environment_management.registry import ManagementError

    registry, _, (alice, bob), prepare = environment_registry
    first = await registry.create(principal=alice, idempotency_key="create-1", prepared=prepare())
    with pytest.raises(ManagementError, match="environment_name_conflict"):
        await registry.create(principal=bob, idempotency_key="create-1", prepared=prepare("alice", bob))
    with pytest.raises(ManagementError, match="environment_forbidden"):
        await registry.get_operation(first.operation_id, principal=bob)
    assert await registry.get_operation(first.operation_id, principal=alice) == first
    assert await registry.list_environments(principal=bob) == []
    assert len(await registry.list_environments(principal=alice)) == 1


async def test_namespace_authority_is_frozen_across_changed_installation_replay(environment_registry, platform_inputs):
    from loom.nebius_environment_contract import FoundationBinding

    registry, _, (alice, _), prepare = environment_registry
    config, candidate, profile = platform_inputs
    installation = "30000000-0000-4000-8000-000000000001"
    foundation = FoundationBinding.model_validate({
        **foundation_from(config).model_dump(), "namespace_authority": {
            "installation_id": installation, "namespace": "loom-nebius-management",
        },
    })
    registration = prepare().registration
    prepared = render_environment(registration, candidate, foundation, profile=profile, keyring={}, repo_root=ROOT)
    created = await registry.create(principal=alice, idempotency_key="owned-namespaces", prepared=prepared)
    changed = FoundationBinding.model_validate({**foundation.model_dump(), "namespace_authority": {
        "installation_id": "30000000-0000-4000-8000-000000000002", "namespace": "loom-nebius-management-other",
    }})
    replay = render_environment(registration, candidate, changed, profile=profile, keyring={}, repo_root=ROOT)
    assert await registry.create(principal=alice, idempotency_key="owned-namespaces", prepared=replay) == created
    context = await registry.provisioning_context(await registry.claim(created.operation_id))
    namespaces = [doc for doc in context.documents.values() if doc.get("kind") == "Namespace"]
    assert len(namespaces) == 3
    assert all(doc["metadata"]["labels"]["loom.nebius/namespace-installation"] == installation for doc in namespaces)
    bindings = [doc for doc in context.documents.values() if doc.get("kind") == "RoleBinding"
                and doc["roleRef"]["kind"] == "ClusterRole"]
    assert len(bindings) == 3
    assert all(doc["subjects"][0]["namespace"] == "loom-nebius-management" for doc in bindings)
    destroyed = await registry.destroy_retained(created.environment_id, principal=alice,
                                                expected_generation=1, idempotency_key="retain-authority")
    retained = await registry.provisioning_context(await registry.claim(destroyed.operation_id))
    assert retained.source.documents == context.documents


@pytest.mark.parametrize("scope", [None, "project-provisioning-a"])
async def test_provisioning_scope_is_frozen_for_replay_and_retained_cleanup(environment_registry, scope):
    from loom.db.nebius_environment_schema import NebiusEnvironmentOperation
    from loom_service.environment_management.cloud_provider import NebiusEnvironmentCloudProvider
    from loom_service.environment_management.steps import ProvisioningStep
    from tests.unit.test_nebius_environment_cloud_provider import CloudApi

    registry, factory, (alice, _), prepare = environment_registry
    prepared = replace(prepare(), provisioning_project_id=scope)
    created = await registry.create(principal=alice, idempotency_key="scoped", prepared=prepared)
    if scope is None:
        # Model a historical persisted plan, before the optional field existed.
        async with factory.begin() as session:
            operation = await session.get(NebiusEnvironmentOperation, created.operation_id)
            historical = dict(operation.plan_json)
            historical.pop("provisioning_project_id", None)
            operation.plan_json = historical
    assert await registry.create(principal=alice, idempotency_key="scoped", prepared=replace(
        prepared, provisioning_project_id="project-provisioning-b",
    )) == created
    lease = await registry.claim(created.operation_id)
    context = await registry.provisioning_context(lease)
    assert context.provisioning_project_id == scope
    destroyed = await registry.destroy_retained(created.environment_id, principal=alice,
                                                expected_generation=1, idempotency_key="cleanup-scoped")
    cleanup = await registry.provisioning_context(await registry.claim(destroyed.operation_id))
    assert cleanup.provisioning_project_id == cleanup.source.provisioning_project_id == scope
    provider = NebiusEnvironmentCloudProvider(CloudApi())
    for kind in ("group", "service_account", "access_key"):
        step = ProvisioningStep("iam:source:" + kind, "credentials", {"purpose": "source", "action": kind})
        cleanup.source.identities["iam:source:service_account"] = "retained-account"
        expected = scope or prepared.config["quota_parent_id" if kind == "group" else "project_id"]
        assert provider.intent(cleanup.source, step)[1]["metadata"]["parent_id"] == expected


async def test_supplied_owner_or_generic_team_credentials_cannot_create(environment_registry):
    from loom_service.environment_management.registry import ManagementError

    registry, _, (alice, bob), prepare = environment_registry
    with pytest.raises(ManagementError, match="environment_owner_mismatch"):
        await registry.create(principal=bob, idempotency_key="create-1", prepared=prepare())
    with pytest.raises(ManagementError, match="user_identity_required"):
        await registry.create(principal=replace(alice, user_id=None), idempotency_key="create-1", prepared=prepare())


async def test_manager_recovers_lost_reply_without_publication_and_rejects_changed_request(
    environment_registry, platform_inputs,
):
    from loom.nebius_environment_contract import EnvironmentCreateRequestV1
    from loom_service.environment_management.manager import (
        EnvironmentManager,
        EnvironmentPlanFactory,
    )
    from loom_service.environment_management.registry import ManagementError

    registry, _, (alice, bob), prepare = environment_registry
    prepared = prepare()
    first = await registry.create(principal=alice, idempotency_key="lost-reply", prepared=prepared)

    class OfflinePublicationApi:
        async def resolve(self, identity):
            raise ConnectionError("Publication temporarily unavailable")

    # A restarted process must answer from its durable operation, not re-render
    # or revalidate an artifact which might have expired since the first request.
    manager = EnvironmentManager(registry, EnvironmentPlanFactory(
        foundation_from(platform_inputs[0]), OfflinePublicationApi(), keyring={}, repo_root=ROOT,
    ))
    request = EnvironmentCreateRequestV1(slug="alice", candidate_id=prepared.registration.candidate_id)
    assert await manager.create(alice, request, idempotency_key="lost-reply") == first
    with pytest.raises(ManagementError, match="idempotency_conflict"):
        await manager.create(alice, request.model_copy(update={"slug": "changed"}), idempotency_key="lost-reply")
    with pytest.raises(ConnectionError):
        await manager.create(bob, request, idempotency_key="lost-reply")


async def test_two_owners_cannot_overbook_last_platform_capacity(environment_registry):
    from sqlalchemy import update

    from loom.db.nebius_environment_schema import NebiusEnvironment, NebiusPlatformBudget
    from loom_service.environment_management.registry import ManagementError

    registry, factory, (alice, bob), prepare = environment_registry
    async with factory.begin() as session:
        await session.execute(update(NebiusPlatformBudget).values(cpu_millis=1050))
    results = await asyncio.gather(
        registry.create(principal=alice, idempotency_key="a", prepared=prepare()),
        registry.create(principal=bob, idempotency_key="b", prepared=prepare("bob", bob)),
        return_exceptions=True,
    )
    failures = [value for value in results if isinstance(value, ManagementError)]
    assert len(failures) == 1
    assert failures[0].code == "platform_capacity_exhausted"
    assert failures[0].details["needed"]["cpu_millis"] == 1050
    assert failures[0].details["available"]["cpu_millis"] == 0
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(NebiusEnvironment)) == 1


@pytest.mark.parametrize("dimension", ["memory_mib", "storage_mib", "ephemeral_storage_mib"])
async def test_platform_admission_checks_non_cpu_dimensions(environment_registry, dimension):
    from sqlalchemy import update

    from loom.db.nebius_environment_schema import NebiusEnvironment, NebiusPlatformBudget
    from loom_service.environment_management.registry import ManagementError

    registry, factory, (alice, _), prepare = environment_registry
    async with factory.begin() as session:
        await session.execute(update(NebiusPlatformBudget).values(**{dimension: 0}))
    with pytest.raises(ManagementError, match="platform_capacity_exhausted") as caught:
        await registry.create(principal=alice, idempotency_key="no-space", prepared=prepare())
    assert caught.value.details["available"][dimension] == 0
    assert caught.value.details["needed"][dimension] > 0
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(NebiusEnvironment)) == 0


@pytest.mark.parametrize("changes,code", [
    ({"scopes": ["read:own"]}, "environment_scope_required"),
    ({"type": "worker"}, "user_identity_required"),
    ({"type": "step_session"}, "user_identity_required"),
    ({"team_id": None}, "user_identity_required"),
])
async def test_unprivileged_principal_cannot_reserve_platform_capacity(environment_registry, changes, code):
    from loom_service.environment_management.registry import ManagementError

    registry, _, (alice, _), prepare = environment_registry
    with pytest.raises(ManagementError, match=code):
        await registry.create(principal=replace(alice, **changes), idempotency_key="forbidden", prepared=prepare())


async def test_resource_namespace_collision_rolls_back_every_claim(environment_registry):
    from loom.db.nebius_environment_schema import NebiusEnvironment, NebiusEnvironmentNamespace
    from loom_service.environment_management.registry import ManagementError

    registry, factory, (alice, bob), prepare = environment_registry
    first = await registry.create(principal=alice, idempotency_key="a", prepared=prepare())
    second = prepare("bob", bob)
    async with factory.begin() as session:
        claim = (await session.execute(select(NebiusEnvironmentNamespace).where(
            NebiusEnvironmentNamespace.environment_id == first.environment_id,
            NebiusEnvironmentNamespace.role == "execution",
        ))).scalar_one()
        claim.namespace_name = second.registration.application_namespace
    with pytest.raises(ManagementError, match="environment_name_conflict"):
        await registry.create(principal=bob, idempotency_key="b", prepared=second)
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(NebiusEnvironment)) == 1
        assert await session.scalar(select(func.count()).select_from(NebiusEnvironmentNamespace)) == 3


async def test_restart_replays_prepared_resource_and_rejects_stale_finalizer(environment_registry):
    from datetime import UTC, datetime, timedelta

    from sqlalchemy import update

    from loom.db.nebius_environment_schema import NebiusEnvironmentOperation
    from loom_service.environment_management.registry import EnvironmentRegistry, ManagementError

    registry, factory, (alice, _), prepare = environment_registry
    operation = await registry.create(principal=alice, idempotency_key="a", prepared=prepare())
    old = await registry.claim(operation.operation_id)
    assert old is not None
    assert await registry.claim(operation.operation_id) is None
    step = await registry.next_step(old)
    assert step is not None and step.kind == "kubernetes"
    assert step.payload["kind"] == "Namespace"
    # The external create happened but its reply was lost: the durable intent
    # exists before it and a new manager must see exactly that same intent.
    async with factory.begin() as session:
        await session.execute(update(NebiusEnvironmentOperation).where(
            NebiusEnvironmentOperation.operation_id == operation.operation_id,
        ).values(lease_expires_at=datetime.now(UTC) - timedelta(seconds=1)))
    restarted = EnvironmentRegistry(factory)
    current = await restarted.claim(operation.operation_id)
    assert current is not None and current.runner_epoch > old.runner_epoch
    assert await restarted.next_step(current) == step
    with pytest.raises(ManagementError, match="stale_operation_lease"):
        await registry.confirm_step(old, step.key, provider_identity="namespace-uid-1")
    await restarted.confirm_step(current, step.key, provider_identity="namespace-uid-1")
    following = await restarted.next_step(current)
    assert following is not None and following.key != step.key
    with pytest.raises(ManagementError, match="resource_identity_conflict"):
        await restarted.confirm_step(current, step.key, provider_identity="different-uid")
    with pytest.raises(ManagementError, match="operation_resources_incomplete"):
        await restarted.complete(current)


async def test_completion_requires_all_durable_steps_including_health_and_auth(environment_registry):
    registry, _, (alice, _), prepare = environment_registry
    operation = await registry.create(principal=alice, idempotency_key="a", prepared=prepare())
    lease = await registry.claim(operation.operation_id)
    assert lease is not None
    kinds = []
    while (step := await registry.next_step(lease)) is not None:
        kinds.append(step.kind)
        await registry.confirm_step(lease, step.key, provider_identity="confirmed-" + step.key)
    assert {"kubernetes", "object_bucket", "credentials", "database_ready", "job_ready", "application_ready"} <= set(kinds)
    assert kinds[-1] == "application_ready"
    await registry.complete(lease)
    assert (await registry.get_operation(operation.operation_id, principal=alice)).phase == "completed"
    assert await registry.claim(operation.operation_id) is None


async def test_real_management_api_derives_owner_and_rejects_other_users(
    environment_registry, isolated_migration_postgres_url, platform_inputs,
):
    import httpx

    from loom.db.schema import TeamMembership
    from loom_service.app import create_app
    from loom_service.config import LoomServiceSettings
    from loom_service.environment_management.manager import (
        CandidateBundle,
        EnvironmentManager,
        EnvironmentPlanFactory,
    )
    from loom_service.password_auth import hash_password

    registry, factory, (alice, bob), prepare = environment_registry
    candidate_id = prepare().registration.candidate_id

    class PublicationApi:
        async def resolve(self, identity):
            assert identity == candidate_id
            return CandidateBundle(identity, platform_inputs[1], platform_inputs[2])

    async with factory.begin() as session:
        for principal, name in ((alice, "alice"), (bob, "bob")):
            user = await session.get(User, principal.user_id)
            user.password_hash = hash_password(name + "-owner-passphrase")
            session.add(TeamMembership(user_id=principal.user_id, team_id=principal.team_id, role="owner"))
    app = create_app(LoomServiceSettings(
        _env_file=None, service_mode="management", db_url=isolated_migration_postgres_url,
        auth_local_http=False, public_base_url="https://management.example.com",
    ))
    async with app.router.lifespan_context(app):
        app.state.environment_manager = EnvironmentManager(registry, EnvironmentPlanFactory(
            foundation_from(platform_inputs[0]), PublicationApi(), keyring={}, repo_root=ROOT,
        ))
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="https://management.example.com") as a, \
                httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="https://management.example.com") as b:
            for client, name in ((a, "alice"), (b, "bob")):
                response = await client.post("/api/v1/auth/login", json={
                    "username": name, "password": name + "-owner-passphrase",
                })
                assert response.status_code == 200
                client.headers["X-Loom-CSRF"] = response.json()["csrf_token"]
            request = {"slug": "alice", "candidate_id": str(candidate_id)}
            first = await a.post("/api/v1/environments", json=request, headers={"Idempotency-Key": "api-create-1"})
            assert first.status_code == 202, first.text
            repeated = await a.post("/api/v1/environments", json=request, headers={"Idempotency-Key": "api-create-1"})
            assert repeated.json() == first.json()
            environment_id = first.json()["environment_id"]
            operation_id = first.json()["operation_id"]
            status = await a.get(f"/api/v1/environments/{environment_id}")
            assert status.status_code == 200
            assert status.json()["registration"]["owner_user_id"] == str(alice.user_id)
            assert status.json()["operation"]["phase"] == "pending"
            assert (await b.get(f"/api/v1/environments/{environment_id}")).status_code == 403
            assert (await b.get(f"/api/v1/environment-operations/{operation_id}")).status_code == 403
            assert (await b.get("/api/v1/environments")).json() == {"items": []}
            impersonation = await b.post("/api/v1/environments", json={
                **request, "owner_user_id": str(alice.user_id),
            }, headers={"Idempotency-Key": "impersonate"})
            assert impersonation.status_code == 422
            destroy_path = f"/api/v1/environments/{environment_id}/operations"
            destroy_body = {"action": "destroy_retained", "expected_generation": 1}
            destroy_headers = {"Idempotency-Key": "api-destroy-1"}
            assert (await b.post(destroy_path, json=destroy_body, headers=destroy_headers)).status_code == 403
            assert (await a.post(destroy_path, json={**destroy_body, "action": "purge"}, headers=destroy_headers)).status_code == 422
            destroyed = await a.post(destroy_path, json=destroy_body, headers=destroy_headers)
            assert destroyed.status_code == 202, destroyed.text
            assert destroyed.json()["action"] == "destroy_retained"
            assert destroyed.json()["deployment_generation"] == 2
            assert (await a.post(destroy_path, json=destroy_body, headers=destroy_headers)).json() == destroyed.json()
            assert (await b.post(f"/api/v1/environment-operations/{operation_id}/retry")).status_code == 403
            assert (await a.post(f"/api/v1/environment-operations/{operation_id}/retry")).status_code == 409
            retry = await a.post(f"/api/v1/environment-operations/{destroyed.json()['operation_id']}/retry")
            assert retry.status_code == 202 and retry.json() == destroyed.json()
            a.headers.pop("X-Loom-CSRF")
            assert (await a.post("/api/v1/environments", json=request,
                                 headers={"Idempotency-Key": "no-csrf"})).status_code == 403
            assert (await a.post(destroy_path, json=destroy_body, headers=destroy_headers)).status_code == 403


async def test_same_login_can_poll_while_publication_waits_without_pool_deadlock(
    environment_registry, isolated_migration_postgres_url, platform_inputs, monkeypatch,
):
    import httpx

    from loom.db.schema import TeamMembership
    from loom_service.app import create_app
    from loom_service.config import LoomServiceSettings
    from loom_service.environment_management.manager import (
        CandidateBundle,
        EnvironmentManager,
        EnvironmentPlanFactory,
    )
    from loom_service.environment_management.registry import EnvironmentRegistry
    from loom_service.password_auth import hash_password

    _, factory, (alice, _), _ = environment_registry
    async with factory.begin() as session:
        user = await session.get(User, alice.user_id)
        user.password_hash = hash_password("owner-passphrase")
        session.add(TeamMembership(user_id=alice.user_id, team_id=alice.team_id, role="owner"))
    # Use a small REAL pool to reproduce the same resource cycle as many
    # concurrent requests on the default pool. No registry/auth method is mocked.
    def engine(url, **kwargs):
        return create_async_engine(url, pool_size=2, max_overflow=0, pool_timeout=0.5, **kwargs)

    monkeypatch.setattr("loom_service.app.create_async_engine", engine)
    entered, release = asyncio.Event(), asyncio.Event()

    class SlowPublicationApi:
        async def resolve(self, identity):
            entered.set()
            await release.wait()
            return CandidateBundle(identity, platform_inputs[1], platform_inputs[2])

    app = create_app(LoomServiceSettings(
        _env_file=None, service_mode="management", db_url=isolated_migration_postgres_url,
        public_base_url="https://management.example.com", auth_local_http=False,
    ))
    async with app.router.lifespan_context(app):
        app.state.environment_manager = EnvironmentManager(
            EnvironmentRegistry(app.state.session_factory), EnvironmentPlanFactory(
                foundation_from(platform_inputs[0]), SlowPublicationApi(), keyring={}, repo_root=ROOT,
            ),
        )
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app, raise_app_exceptions=False),
            base_url="https://management.example.com",
        ) as client:
            login = await client.post("/api/v1/auth/login", json={"username": "alice", "password": "owner-passphrase"})
            assert login.status_code == 200
            client.headers["X-Loom-CSRF"] = login.json()["csrf_token"]
            create = asyncio.create_task(client.post("/api/v1/environments", json={
                "slug": "alice", "candidate_id": str(uuid4()),
            }, headers={"Idempotency-Key": "concurrent"}))
            poll = None
            try:
                await asyncio.wait_for(entered.wait(), timeout=3)
                poll = asyncio.create_task(client.get("/api/v1/environments"))
                async with asyncio.timeout(3):
                    # Before the fix: auth holds one connection and the poll
                    # blocks on its session row with the second. After the fix:
                    # the poll can complete while publication remains paused.
                    while not poll.done() and app.state._owned_service_engine.pool.checkedout() < 2:
                        await asyncio.sleep(0.01)
                release.set()
                first, second = await asyncio.wait_for(asyncio.gather(create, poll), timeout=5)
                assert first.status_code == 202, first.text
                assert second.status_code == 200, second.text
            finally:
                release.set()
                for task in (create, poll):
                    if task is not None and not task.done():
                        task.cancel()
                await asyncio.gather(*(task for task in (create, poll) if task is not None), return_exceptions=True)
