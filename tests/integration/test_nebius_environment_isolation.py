"""Child onboarding issues a new, one-use proof; management sessions never copy."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import httpx
import pytest
from fastapi import HTTPException
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.admin_secret import AdminSecretVerifier
from loom.db.schema import LoginChallenge, Team, TeamMembership, User, UserSession
from loom.nebius_environment_contract import new_environment_registration
from loom_service.app import create_app
from loom_service.config import LoomServiceSettings
from loom_service.session_auth import consume_login_challenge, hash_secret
from tests.support.nebius_platform import platform_inputs as platform_inputs
from tests.unit.test_nebius_environment_contract import foundation_from


async def test_login_challenge_cannot_be_consumed_twice_under_concurrency(isolated_migration_postgres_url):
    engine = create_async_engine(isolated_migration_postgres_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    user, team = uuid4(), uuid4()
    token = "loom_env_login_" + "x" * 43
    now = datetime.now(UTC)
    try:
        async with factory.begin() as session:
            session.add_all([User(id=user, username="owner", username_normalized="owner", status="active"), Team(id=team, name="owner")])
            await session.flush()
            session.add_all([TeamMembership(user_id=user, team_id=team, role="owner"), LoginChallenge(
                challenge_hash=hash_secret(token), user_id=user, issued_at=now, expires_at=now + timedelta(minutes=1),
            )])

        async def second_exchange():
            try:
                async with factory.begin() as session:
                    await consume_login_challenge(session, raw_token=token, session_ttl_seconds=300)
                return "accepted"
            except HTTPException:
                return "rejected"

        async with factory.begin() as first:
            await consume_login_challenge(first, raw_token=token, session_ttl_seconds=300)
            second = asyncio.create_task(second_exchange())
            await asyncio.sleep(0.1)
        assert await second == "rejected"
        async with factory() as session:
            assert await session.scalar(select(func.count()).select_from(UserSession)) == 1
    finally:
        await engine.dispose()


@pytest.fixture
async def child_app(isolated_migration_postgres_url, platform_inputs, tmp_path):
    binding = new_environment_registration(foundation_from(platform_inputs[0]), environment_id=uuid4(),
                                            incarnation=uuid4(), owner_user_id=uuid4(), owner_team_id=uuid4(), slug="alice")
    path = tmp_path / "environment.json"
    path.write_text(json.dumps({"schema_version": "loom.nebius-managed-environment.v1",
                                "registration": binding.model_dump(mode="json"),
                                "namespace": binding.application_namespace, "public_host": binding.public_host}))
    settings = LoomServiceSettings(_env_file=None, db_url=isolated_migration_postgres_url,
                                   minio_access_key="test-only", minio_secret_key="test-only",
                                   public_base_url="https://" + binding.public_host,
                                   managed_environment_config_file=path)
    app = create_app(settings)
    engine = create_async_engine(isolated_migration_postgres_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    admin = "loom_admin_" + "a" * 43
    app.state.settings = settings
    app.state.session_factory = factory
    app.state.admin_secret_verifier = AdminSecretVerifier.from_token(admin)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="https://" + binding.public_host) as http:
        try:
            yield http, binding, admin, factory
        finally:
            await engine.dispose()


async def test_child_enrollment_is_idempotent_and_only_singleton_admin_can_issue_owner_proof(child_app):
    http, binding, admin, factory = child_app
    async with factory() as session:
        baseline_users = await session.scalar(select(func.count()).select_from(User))
    body = {"environment_id": str(binding.environment_id), "incarnation": str(binding.incarnation)}
    path = "/api/v1/admin/managed-environment/owner"
    assert (await http.post(path, json=body)).status_code == 401
    headers = {"Authorization": "Bearer " + admin}
    for _ in range(2):
        enrolled = await http.post(path, json=body, headers=headers)
        assert enrolled.status_code == 200, enrolled.text
        assert enrolled.json()["owner_user_id"] == str(binding.owner_user_id)
    observed = await http.get(path, params=body, headers=headers)
    assert observed.status_code == 200
    assert observed.json() == enrolled.json()
    bad = await http.post(path, json={**body, "incarnation": str(uuid4())}, headers=headers)
    assert bad.status_code == 409
    ticket = await http.post("/api/v1/admin/managed-environment/login", json=body, headers=headers)
    assert ticket.status_code == 200, ticket.text
    assert ticket.headers["cache-control"] == "no-store"
    assert ticket.json()["origin"] == "https://" + binding.public_host
    token = ticket.json()["login_token"]
    assert token != admin
    login = await http.post("/api/v1/auth/login/complete", json={"token": token})
    assert login.status_code == 200, login.text
    assert "__Host-loom_session=" in login.headers["set-cookie"]
    assert (await http.post("/api/v1/auth/login/complete", json={"token": token})).status_code == 400
    me = await http.get("/api/v1/auth/whoami")
    assert me.status_code == 200
    assert me.json()["user_id"] == str(binding.owner_user_id)
    # A child owner session must not acquire the protected bootstrap authority.
    assert (await http.post(path, json=body, headers={"X-Loom-CSRF": login.json()["csrf_token"]})).status_code == 403
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(User)) == baseline_users + 1
        assert await session.scalar(select(func.count()).select_from(UserSession)) == 1


async def test_child_ticket_does_not_reactivate_disabled_owner(child_app):
    http, binding, admin, factory = child_app
    body = {"environment_id": str(binding.environment_id), "incarnation": str(binding.incarnation)}
    headers = {"Authorization": "Bearer " + admin}
    assert (await http.post("/api/v1/admin/managed-environment/owner", json=body, headers=headers)).status_code == 200
    ticket = await http.post("/api/v1/admin/managed-environment/login", json=body, headers=headers)
    async with factory.begin() as session:
        user = await session.get(User, binding.owner_user_id)
        user.disabled_at = datetime.now(UTC)
        user.status = "disabled"
    assert (await http.post("/api/v1/auth/login/complete", json={"token": ticket.json()["login_token"]})).status_code == 403
    assert (await http.post("/api/v1/admin/managed-environment/login", json=body, headers=headers)).status_code == 403


@pytest.mark.parametrize("enrolled", [True, False])
async def test_retained_destroy_revokes_sessions_proofs_and_delayed_enrollment(child_app, enrolled):
    http, binding, admin, _ = child_app
    body = {"environment_id": str(binding.environment_id), "incarnation": str(binding.incarnation)}
    headers = {"Authorization": "Bearer " + admin}
    prefix = "/api/v1/admin/managed-environment/"
    proof = None
    if enrolled:
        assert (await http.post(prefix + "owner", json=body, headers=headers)).status_code == 200
        first = await http.post(prefix + "login", json=body, headers=headers)
        assert (await http.post("/api/v1/auth/login/complete", json={"token": first.json()["login_token"]})).status_code == 200
        proof = (await http.post(prefix + "login", json=body, headers=headers)).json()["login_token"]
    # Destroy cannot be called by the child owner, only the protected manager.
    assert (await http.post(prefix + "revoke", json=body)).status_code in {401, 403}
    assert (await http.post(prefix + "revoke", json={**body, "incarnation": str(uuid4())}, headers=headers)).status_code == 409
    for _ in range(2):
        revoked = await http.post(prefix + "revoke", json=body, headers=headers)
        assert revoked.status_code == 200, revoked.text
        assert revoked.json()["incarnation"] == str(binding.incarnation)
    if proof is not None:
        assert (await http.post("/api/v1/auth/login/complete", json={"token": proof})).status_code == 403
        assert (await http.get("/api/v1/auth/whoami")).status_code in {401, 403}
    assert (await http.post(prefix + "owner", json=body, headers=headers)).status_code == 403
    assert (await http.post(prefix + "login", json=body, headers=headers)).status_code == 403


async def test_child_login_proof_expiry_is_checked_after_waiting_for_database_lock(child_app):
    http, binding, admin, factory = child_app
    body = {"environment_id": str(binding.environment_id), "incarnation": str(binding.incarnation)}
    headers = {"Authorization": "Bearer " + admin}
    assert (await http.post("/api/v1/admin/managed-environment/owner", json=body, headers=headers)).status_code == 200
    ticket = await http.post("/api/v1/admin/managed-environment/login", json=body, headers=headers)
    token = ticket.json()["login_token"]
    async with factory.begin() as session:
        challenge = await session.get(LoginChallenge, hash_secret(token))
        challenge.expires_at = datetime.now(UTC) + timedelta(seconds=0.5)
    async with factory.begin() as lock:
        await lock.get(LoginChallenge, hash_secret(token), with_for_update=True)
        request = asyncio.create_task(http.post("/api/v1/auth/login/complete", json={"token": token}))
        await asyncio.sleep(0.6)
    assert (await request).status_code == 400


@pytest.fixture
def second_child_database(migration_template_postgres_url):
    from sqlalchemy.engine import make_url

    from tests.integration.conftest import _isolated_migration_database

    yield from _isolated_migration_database(migration_template_postgres_url,
                                            template_name=make_url(migration_template_postgres_url).database,
                                            prepare_template=False)


async def test_two_independent_child_databases_reject_each_others_login_proof_and_session(
    child_app, second_child_database, platform_inputs, tmp_path,
):
    alice, a, a_admin, _ = child_app
    a_body = {"environment_id": str(a.environment_id), "incarnation": str(a.incarnation)}
    a_headers = {"Authorization": "Bearer " + a_admin}
    assert (await alice.post("/api/v1/admin/managed-environment/owner", json=a_body, headers=a_headers)).status_code == 200
    a_proof = (await alice.post("/api/v1/admin/managed-environment/login", json=a_body, headers=a_headers)).json()["login_token"]
    b = new_environment_registration(foundation_from(platform_inputs[0]), environment_id=uuid4(), incarnation=uuid4(),
                                     owner_user_id=uuid4(), owner_team_id=uuid4(), slug="bob")
    path = tmp_path / "bob.json"
    path.write_text(json.dumps({"schema_version": "loom.nebius-managed-environment.v1", "registration": b.model_dump(mode="json"),
                                "namespace": b.application_namespace, "public_host": b.public_host}))
    settings = LoomServiceSettings(_env_file=None, db_url=second_child_database, minio_access_key="test", minio_secret_key="test",
                                   public_base_url="https://" + b.public_host, managed_environment_config_file=path)
    app = create_app(settings)
    engine = create_async_engine(second_child_database)
    app.state.settings = settings
    app.state.session_factory = async_sessionmaker(engine, expire_on_commit=False)
    b_admin = "loom_admin_" + "b" * 43
    app.state.admin_secret_verifier = AdminSecretVerifier.from_token(b_admin)
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="https://" + b.public_host) as bob:
            body = {"environment_id": str(b.environment_id), "incarnation": str(b.incarnation)}
            headers = {"Authorization": "Bearer " + b_admin}
            assert (await bob.post("/api/v1/admin/managed-environment/owner", json=body, headers=headers)).status_code == 200
            assert (await bob.post("/api/v1/auth/login/complete", json={"token": a_proof})).status_code == 400
            assert (await alice.post("/api/v1/auth/login/complete", json={"token": a_proof})).status_code == 200
            bob.cookies.set("__Host-loom_session", alice.cookies.get("__Host-loom_session"), domain=b.public_host, path="/")
            assert (await bob.get("/api/v1/auth/whoami")).status_code == 401
            b_proof = (await bob.post("/api/v1/admin/managed-environment/login", json=body, headers=headers)).json()["login_token"]
            assert (await bob.post("/api/v1/auth/login/complete", json={"token": b_proof})).status_code == 200
            assert (await bob.get("/api/v1/auth/whoami")).json()["user_id"] == str(b.owner_user_id)
            assert (await alice.get("/api/v1/auth/whoami")).json()["user_id"] == str(a.owner_user_id)
    finally:
        await engine.dispose()
