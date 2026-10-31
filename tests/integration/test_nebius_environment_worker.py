"""The real durable registry advances only after verified external effects."""

from __future__ import annotations

import asyncio
import hashlib
from datetime import UTC, datetime

import pytest

from tests.integration.test_nebius_environment_management import (
    environment_registry as environment_registry,
)
from tests.support.nebius_platform import platform_inputs as platform_inputs


class ExternalProvider:
    def __init__(self):
        self.resources = {}
        self.fail_after_create = False
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.pause = False

    async def apply(self, context, step):
        self.entered.set()
        if self.pause:
            await self.release.wait()
        identity = self.resources.setdefault(step.key, "provider-" + str(len(self.resources)))
        if self.fail_after_create:
            self.fail_after_create = False
            from loom_service.environment_management.provider import ProviderRetryError

            raise ProviderRetryError("provider_unavailable")
        return identity


async def test_worker_loop_recovers_database_outage_and_resumes_durable_work(environment_registry, monkeypatch):
    from sqlalchemy import func, select, text
    from sqlalchemy.exc import SQLAlchemyError
    from sqlalchemy.ext.asyncio import create_async_engine

    from loom.db.nebius_environment_schema import (
        NebiusEnvironmentOperation,
        NebiusEnvironmentResource,
    )
    from loom_service.environment_management.worker import EnvironmentWorker

    registry, factory, (alice, _), prepare = environment_registry
    operation = await registry.create(principal=alice, idempotency_key="outage", prepared=prepare())
    engine = factory.kw["bind"]
    url = engine.url
    assert url.database.startswith("loom_migration_")
    admin = create_async_engine(url.set(database="postgres"), isolation_level="AUTOCOMMIT")
    database = admin.dialect.identifier_preparer.quote(url.database)
    outage_observed = asyncio.Event()
    original_poll = registry.runnable_operations

    async def observe_poll(**kwargs):
        try:
            return await original_poll(**kwargs)
        except (SQLAlchemyError, OSError):
            outage_observed.set()
            raise

    monkeypatch.setattr(registry, "runnable_operations", observe_poll)
    provider = ExternalProvider()
    worker = EnvironmentWorker(registry, provider)
    task = None
    try:
        async with admin.connect() as connection:
            await connection.execute(text(f"ALTER DATABASE {database} ALLOW_CONNECTIONS false"))
        await engine.dispose()
        task = asyncio.create_task(worker.run(poll_seconds=1))
        # healthy starts False; wait for an actual rejected poll before
        # restoring the DB so this exercises the intended pre-claim outage.
        await asyncio.wait_for(outage_observed.wait(), timeout=10)
        assert not task.done(), "database outage permanently killed the operation worker"
        assert worker.healthy is False
        async with admin.connect() as connection:
            await connection.execute(text(f"ALTER DATABASE {database} ALLOW_CONNECTIONS true"))
        try:
            async with asyncio.timeout(10):
                while (await registry.get_operation(operation.operation_id, principal=alice)).phase != "completed":
                    await asyncio.sleep(0.1)
        except TimeoutError:
            details = f"provider_steps={len(provider.resources)}, healthy={worker.healthy}"
            try:
                async with asyncio.timeout(1):
                    async with factory() as session:
                        row = await session.get(NebiusEnvironmentOperation, operation.operation_id)
                        pending = await session.scalar(select(func.count()).select_from(NebiusEnvironmentResource).where(
                            NebiusEnvironmentResource.operation_id == operation.operation_id,
                            NebiusEnvironmentResource.phase == "planned",
                        ))
                        details += (f", phase={row.phase}, epoch={row.runner_epoch}, "
                                    f"leased={row.lease_expires_at is not None}, pending={pending}")
            except Exception as error:
                # Diagnostics must not hide the original timeout or print a DSN.
                details += f", state_unavailable={type(error).__name__}"
            pytest.fail("worker recovery timed out: " + details)
        assert worker.healthy is True
    finally:
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        async with admin.connect() as connection:
            await connection.execute(text(f"ALTER DATABASE {database} ALLOW_CONNECTIONS true"))
        await admin.dispose()


async def test_slow_owner_does_not_block_other_available_worker_slots(environment_registry):
    from loom_service.environment_management.worker import EnvironmentWorker

    registry, _, (alice, bob), prepare = environment_registry
    first = await registry.create(principal=alice, idempotency_key="slow", prepared=prepare())
    entered, release = asyncio.Event(), asyncio.Event()

    class OneSlowOwner(ExternalProvider):
        async def apply(self, context, step):
            if context.lease.environment_id == first.environment_id:
                entered.set()
                await release.wait()
            return await super().apply(context, step)

    task = asyncio.create_task(EnvironmentWorker(registry, OneSlowOwner()).run(concurrency=2, poll_seconds=1))
    try:
        await asyncio.wait_for(entered.wait(), timeout=2)
        second = await registry.create(principal=bob, idempotency_key="fast", prepared=prepare("bob", bob))
        async with asyncio.timeout(5):
            while (await registry.get_operation(second.operation_id, principal=bob)).phase != "completed":
                await asyncio.sleep(0.1)
        assert (await registry.get_operation(first.operation_id, principal=alice)).phase == "running"
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_worker_recovers_lost_reply_and_finishes_existing_intents(environment_registry):
    from loom_service.environment_management.worker import EnvironmentWorker

    registry, _, (alice, _), prepare = environment_registry
    operation = await registry.create(principal=alice, idempotency_key="worker", prepared=prepare())
    provider = ExternalProvider()
    provider.fail_after_create = True
    worker = EnvironmentWorker(registry, provider)
    await worker.reconcile_once(operation.operation_id)
    first = await registry.get_operation(operation.operation_id, principal=alice)
    assert first.phase == "pending" and first.error_code == "provider_unavailable"
    assert len(provider.resources) == 1
    await EnvironmentWorker(registry, provider).reconcile_once(operation.operation_id)
    assert (await registry.get_operation(operation.operation_id, principal=alice)).phase == "completed"
    assert "ready:application" in provider.resources


async def test_concurrent_worker_does_not_duplicate_mutations(environment_registry):
    from loom_service.environment_management.worker import EnvironmentWorker

    registry, _, (alice, _), prepare = environment_registry
    operation = await registry.create(principal=alice, idempotency_key="worker", prepared=prepare())
    provider = ExternalProvider()
    provider.pause = True
    first = asyncio.create_task(EnvironmentWorker(registry, provider).reconcile_once(operation.operation_id))
    await provider.entered.wait()
    await EnvironmentWorker(registry, ExternalProvider()).reconcile_once(operation.operation_id)
    assert (await registry.get_operation(operation.operation_id, principal=alice)).phase == "running"
    provider.release.set()
    await first
    assert (await registry.get_operation(operation.operation_id, principal=alice)).phase == "completed"


async def test_worker_has_bounded_retries_and_scrubs_unexpected_errors(environment_registry):
    from loom_service.environment_management.provider import ProviderRetryError
    from loom_service.environment_management.worker import EnvironmentWorker

    registry, _, (alice, _), prepare = environment_registry
    operation = await registry.create(principal=alice, idempotency_key="worker", prepared=prepare())

    class Unavailable:
        async def apply(self, context, step):
            raise ProviderRetryError("provider_unavailable")

    worker = EnvironmentWorker(registry, Unavailable(), max_attempts=2)
    await worker.reconcile_once(operation.operation_id)
    await worker.reconcile_once(operation.operation_id)
    state = await registry.get_operation(operation.operation_id, principal=alice)
    assert state.phase == "blocked" and state.error_code == "provider_unavailable"
    assert await registry.claim(operation.operation_id) is None
    assert await registry.runnable_operations() == []

    other = await registry.create(principal=alice, idempotency_key="worker2", prepared=prepare("other"))

    class Broken:
        async def apply(self, context, step):
            raise RuntimeError("password=must-not-be-journaled")

    await EnvironmentWorker(registry, Broken()).reconcile_once(other.operation_id)
    state = await registry.get_operation(other.operation_id, principal=alice)
    assert state.phase == "blocked" and state.error_code == "provider_internal_error"


async def test_shutdown_does_not_report_completion_or_release_ambiguous_intent(environment_registry):
    from loom_service.environment_management.worker import EnvironmentWorker

    registry, _, (alice, _), prepare = environment_registry
    operation = await registry.create(principal=alice, idempotency_key="worker", prepared=prepare())
    provider = ExternalProvider()
    provider.pause = True
    task = asyncio.create_task(EnvironmentWorker(registry, provider).reconcile_once(operation.operation_id))
    await provider.entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    state = await registry.get_operation(operation.operation_id, principal=alice)
    assert state.phase == "running"
    assert await registry.claim(operation.operation_id) is None


async def test_credential_material_is_encrypted_atomic_and_stable_across_restart(environment_registry, monkeypatch):
    import base64
    import json

    from sqlalchemy import select

    from loom.db.nebius_environment_schema import NebiusEnvironmentOperation
    from loom.db.schema import Secret
    from loom_service.environment_management.registry import ManagementError

    monkeypatch.setenv("LOOM_SECRET_STORE_MASTER_KEY", base64.b64encode(b"m" * 32).decode())
    monkeypatch.delenv("LOOM_SECRET_STORE_MASTER_KEYS", raising=False)
    registry, factory, (alice, _), prepare = environment_registry
    operation = await registry.create(principal=alice, idempotency_key="worker", prepared=prepare())
    lease = await registry.claim(operation.operation_id)
    assert lease is not None
    with pytest.raises(ManagementError, match="resource_step_out_of_order"):
        await registry.store_material(lease, "credentials:material", {"private": "not-in-journal"})
    while (step := await registry.next_step(lease)).key != "credentials:material":
        await registry.confirm_step(lease, step.key, provider_identity="owned:" + step.key)
    ref = await registry.store_material(lease, step.key, {"private": "not-in-journal"})
    assert await registry.store_material(lease, step.key, {"private": "different"}) == ref
    assert await registry.load_material(lease, step.key) == {"private": "not-in-journal"}
    async with factory() as session:
        row = await session.get(NebiusEnvironmentOperation, operation.operation_id)
        assert "not-in-journal" not in json.dumps(row.plan_json)
        secrets = (await session.scalars(select(Secret))).all()
        assert len(secrets) == 1
        assert b"not-in-journal" not in secrets[0].ciphertext


async def test_readiness_wait_does_not_exhaust_transient_retry_budget(environment_registry):
    from loom_service.environment_management.provider import ProviderWaitingError
    from loom_service.environment_management.worker import EnvironmentWorker

    registry, _, (alice, _), prepare = environment_registry
    operation = await registry.create(principal=alice, idempotency_key="worker", prepared=prepare())

    class Starting(ExternalProvider):
        calls = 0

        async def apply(self, context, step):
            self.calls += 1
            if self.calls < 3:
                raise ProviderWaitingError("kubernetes_not_ready")
            return await super().apply(context, step)

    await EnvironmentWorker(registry, Starting(), max_attempts=1, readiness_poll_seconds=0.01).reconcile_once(operation.operation_id)
    assert (await registry.get_operation(operation.operation_id, principal=alice)).phase == "completed"


async def test_worker_renews_lease_and_stops_external_work_if_lease_is_lost(environment_registry):
    from datetime import UTC, datetime, timedelta

    from sqlalchemy import update

    from loom.db.nebius_environment_schema import NebiusEnvironmentOperation
    from loom_service.environment_management.worker import EnvironmentWorker

    registry, factory, (alice, _), prepare = environment_registry
    operation = await registry.create(principal=alice, idempotency_key="worker", prepared=prepare())
    provider = ExternalProvider()
    provider.pause = True
    task = asyncio.create_task(EnvironmentWorker(registry, provider, lease_seconds=3).reconcile_once(operation.operation_id))
    await provider.entered.wait()
    # Waiting across the original lease verifies that actual renewals happen.
    await asyncio.sleep(3.1)
    assert await registry.claim(operation.operation_id) is None
    async with factory.begin() as session:
        await session.execute(update(NebiusEnvironmentOperation).where(
            NebiusEnvironmentOperation.operation_id == operation.operation_id,
        ).values(lease_expires_at=datetime.now(UTC) - timedelta(seconds=1)))
    successor = await registry.claim(operation.operation_id)
    assert successor is not None
    await asyncio.wait_for(task, timeout=2)
    provider.release.set()
    assert provider.resources == {}
    assert (await registry.provisioning_context(successor)).identities == {}


async def test_only_ready_owner_can_obtain_scoped_child_control_material(environment_registry, monkeypatch):
    import base64

    from loom_service.environment_management.registry import ManagementError

    monkeypatch.setenv("LOOM_SECRET_STORE_MASTER_KEY", base64.b64encode(b"m" * 32).decode())
    monkeypatch.delenv("LOOM_SECRET_STORE_MASTER_KEYS", raising=False)
    registry, _, (alice, bob), prepare = environment_registry
    operation = await registry.create(principal=alice, idempotency_key="login", prepared=prepare())
    with pytest.raises(ManagementError, match="environment_not_ready"):
        await registry.ready_access(operation.environment_id, principal=alice)
    lease = await registry.claim(operation.operation_id)
    while (step := await registry.next_step(lease)) is not None:
        if step.key == "credentials:material":
            await registry.store_material(lease, step.key, {"loom-admin-secret": {"secrets.toml": "child-private-material"}})
        else:
            await registry.confirm_step(lease, step.key, provider_identity="owned:" + step.key)
    await registry.complete(lease)
    with pytest.raises(ManagementError, match="environment_forbidden"):
        await registry.ready_access(operation.environment_id, principal=bob)
    row, material = await registry.ready_access(operation.environment_id, principal=alice)
    assert row.environment_id == operation.environment_id
    assert material == {"loom-admin-secret": {"secrets.toml": "child-private-material"}}


async def test_management_login_issues_only_target_child_proof_with_real_auth(
    environment_registry, platform_inputs, monkeypatch, isolated_migration_postgres_url,
):
    import base64

    import httpx

    from loom.db.schema import TeamMembership, Token, User
    from loom_service.app import create_app
    from loom_service.config import LoomServiceSettings
    from loom_service.environment_management.child_client import ChildEnvironmentClient
    from loom_service.environment_management.manager import (
        EnvironmentManager,
        EnvironmentPlanFactory,
    )
    from loom_service.password_auth import hash_password
    from tests.support.nebius_platform import ROOT
    from tests.unit.test_nebius_environment_contract import foundation_from

    monkeypatch.setenv("LOOM_SECRET_STORE_MASTER_KEY", base64.b64encode(b"m" * 32).decode())
    monkeypatch.delenv("LOOM_SECRET_STORE_MASTER_KEYS", raising=False)
    registry, factory, (alice, bob), prepare = environment_registry
    prepared = prepare()
    operation = await registry.create(principal=alice, idempotency_key="login", prepared=prepared)
    lease = await registry.claim(operation.operation_id)
    while (step := await registry.next_step(lease)) is not None:
        if step.key == "credentials:material":
            await registry.store_material(lease, step.key, {"loom-admin-secret": {"secrets.toml": '[admin]\ntoken="child-only-admin"\n'}})
        else:
            await registry.confirm_step(lease, step.key, provider_identity="owned:" + step.key)
    await registry.complete(lease)
    async with factory.begin() as session:
        for principal in (alice, bob):
            user = await session.get(User, principal.user_id)
            user.password_hash = hash_password("owner-passphrase")
            session.add(TeamMembership(user_id=principal.user_id, team_id=principal.team_id, role="owner"))

    class NoCatalog:
        async def resolve(self, candidate_id):
            pytest.fail("login tried to resolve a publication")

    def remote_child(request):
        assert request.headers["authorization"] == "Bearer child-only-admin"
        assert not request.headers.get("cookie")
        assert request.url.host == prepared.registration.public_host
        row = prepared.registration
        return httpx.Response(200, json={"environment_id": str(row.environment_id), "incarnation": str(row.incarnation),
                                        "owner_user_id": str(row.owner_user_id), "owner_team_id": str(row.owner_team_id),
                                        "origin": "https://" + row.public_host, "login_token": "loom_env_login_" + "a" * 43,
                                        "expires_in": 90})

    settings = LoomServiceSettings(_env_file=None, service_mode="management", public_base_url="https://manage.example.com",
                                   db_url=isolated_migration_postgres_url)
    app = create_app(settings)
    app.state.settings = settings
    app.state.session_factory = factory
    async with httpx.AsyncClient(transport=httpx.MockTransport(remote_child)) as remote:
        app.state.environment_manager = EnvironmentManager(registry, EnvironmentPlanFactory(
            foundation_from(platform_inputs[0]), NoCatalog(), keyring={}, repo_root=ROOT,
        ), child=ChildEnvironmentClient(remote))
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="https://manage.example.com") as client:
            for user, expected in (("bob", 403), ("alice", 200)):
                login = await client.post("/api/v1/auth/login", json={"username": user, "password": "owner-passphrase"})
                assert login.status_code == 200
                client.headers["X-Loom-CSRF"] = login.json()["csrf_token"]
                response = await client.post(f"/api/v1/environments/{operation.environment_id}/login")
                assert response.status_code == expected, response.text
                if expected == 200:
                    assert response.json()["login_token"] == "loom_env_login_" + "a" * 43
                    assert response.headers["cache-control"] == "no-store"
                    assert "child-only-admin" not in response.text
            client.headers.pop("X-Loom-CSRF")
            assert (await client.post(f"/api/v1/environments/{operation.environment_id}/login")).status_code == 403

            # User attribution on a team token is not permission to exchange
            # deliberately attenuated credentials for a full child owner.
            client.cookies.clear()
            for scopes, expected in ((["read:own"], 403), (["read:own", "submit"], 403),
                                     (["read:own", "submit", "tokens:manage", "providers:manage", "team:manage"], 200)):
                token = "management-test-" + str(len(scopes))
                async with factory.begin() as session:
                    session.add(Token(token_hash=hashlib.sha256(token.encode()).digest(), type="team",
                                      scopes=scopes, team_id=alice.team_id, created_by_user_id=alice.user_id,
                                      issued_at=datetime.now(UTC)))
                client.headers["Authorization"] = "Bearer " + token
                assert (await client.get(f"/api/v1/environments/{operation.environment_id}")).status_code == 200
                response = await client.post(f"/api/v1/environments/{operation.environment_id}/login")
                assert response.status_code == expected, response.text
            client.headers.pop("Authorization")
            async with factory.begin() as session:
                membership = await session.get(TeamMembership, {"user_id": alice.user_id, "team_id": alice.team_id})
                membership.role = "viewer"
            login = await client.post("/api/v1/auth/login", json={"username": "alice", "password": "owner-passphrase"})
            assert login.status_code == 200
            client.headers["X-Loom-CSRF"] = login.json()["csrf_token"]
            response = await client.post(f"/api/v1/environments/{operation.environment_id}/login")
            assert response.status_code == 403, response.text
