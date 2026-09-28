"""Management sessions enter a ready application using its real shared SQL role."""
from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import httpx
import pytest
from sqlalchemy import select, update
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.application_session import ApplicationSessionAudienceV1
from loom.db.schema import LoginChallenge, Team, TeamMembership, User
from loom.nebius_application_contract import SharedDevelopmentBindingV1
from loom_service.app import create_app
from loom_service.config import LoomServiceSettings
from loom_service.environment_management.registry import ManagementError
from loom_service.session_auth import hash_browser_secret
from tests.integration.test_nebius_application_credentials import database_access as database_access
from tests.integration.test_nebius_application_credentials import shared_ca as shared_ca
from tests.integration.test_nebius_application_material import management_key as management_key
from tests.integration.test_nebius_application_operations import applications as applications
from tests.integration.test_nebius_application_preparation import preparation as preparation
from tests.integration.test_nebius_application_ready import ready_context as ready_context
from tests.integration.test_nebius_environment_management import (
    environment_registry as environment_registry,
)
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


@pytest.fixture
async def login_context(ready_context, database_access, tmp_path, monkeypatch):
    from loom_service.application_management import login

    coordinator, registry, _, alice, lease, _, _, row = ready_context
    plan = await registry.frozen_plan(lease)
    await coordinator.start(lease)
    ca = tmp_path / 'shared-ca.crt'
    ca.write_text(coordinator.credentials.shared.ca_pem)
    local = make_url(database_access[1])
    opened = []

    def local_engine(url, **kwargs):
        # Replace only unavailable cluster DNS/TLS with the disposable SQL route.
        # Keep actual generation username/password so grants and revocation are real.
        assert url.host == 'loom-postgres.loom-nebius-platform.svc' and url.port == 5432
        assert url.query == {'sslmode': 'verify-full', 'sslrootcert': str(ca)}
        assert url.username == f'lap_{row.incarnation.hex}_g1'
        opened.append(url)
        return create_async_engine(url.set(host=local.host, port=local.port, query={}), **kwargs)

    monkeypatch.setattr(login, 'create_async_engine', local_engine)
    bridge = login.ApplicationLogin(registry, shared=SharedDevelopmentBindingV1.model_validate(plan['shared']),
        credentials=coordinator.credentials.shared, ca_file=ca)
    alice = replace(alice, auth_kind='session')
    # Add an alphabetically earlier team, to catch accidental first-membership login.
    admin_engine = create_async_engine(local.set(drivername='postgresql+psycopg',
        username=database_access[0].info.user, password=database_access[0].info.password))
    factory = async_sessionmaker(admin_engine, expire_on_commit=False)
    other_team = uuid4()
    try:
        async with factory.begin() as session:
            session.add(Team(id=other_team, name='AAA unrelated membership'))
            await session.flush()
            session.add(TeamMembership(user_id=alice.user_id, team_id=other_team, role='owner'))
        yield bridge, alice, row, factory, opened, registry
    finally:
        await admin_engine.dispose()


def audience(row):
    return {'application_id': str(row.application_id), 'origin': 'https://' + row.public_host,
            'access_generation': row.access_generation}


def child(factory, row, override=None):
    scope = audience(row) if override is None else override
    origin = scope['origin'] if scope else 'https://' + row.public_host
    settings = LoomServiceSettings(_env_file=None, minio_access_key='x', minio_secret_key='y',
        db_url=factory.kw['bind'].url.render_as_string(hide_password=False),
        public_base_url=origin, auth_local_http=False,
        auth_session_audience_json=json.dumps(scope) if scope else None)
    app = create_app(settings)
    app.state.settings, app.state.session_factory = settings, factory
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=origin)


async def test_ready_owner_without_password_enters_exact_team_with_current_shared_role(login_context):
    bridge, alice, row, factory, opened, _ = login_context
    proof = await bridge.issue(alice, row.application_id)
    assert set(proof) == {'application_id', 'incarnation', 'deployment_generation', 'access_generation',
                         'owner_user_id', 'owner_team_id', 'origin', 'login_token', 'expires_in'}
    assert proof['expires_in'] == 90 and proof['origin'] == 'https://' + row.public_host
    assert proof['application_id'] == str(row.application_id) and proof['owner_team_id'] == str(alice.team_id)
    assert len(opened) == 1 and opened[0].password not in json.dumps(proof)
    async with factory() as session:
        user = await session.get(User, alice.user_id)
        assert user.password_hash is None and user.email is None
        stored = (await session.scalars(select(LoginChallenge))).one()
        assert stored.challenge_hash == hash_browser_secret(proof['login_token'],
            audience=ApplicationSessionAudienceV1(**audience(row)), purpose='login_challenge')
        assert stored.expires_at - stored.issued_at == timedelta(seconds=90)
    async with child(factory, row) as http:
        response = await http.post('/api/v1/auth/login/complete', json={'token': proof['login_token']})
        assert response.status_code == 200, response.text
        assert response.json()['user']['id'] == str(alice.user_id)
        assert response.json()['current_team']['id'] == str(alice.team_id)
        assert response.json()['role'] == 'member' and not response.json()['is_platform_admin']
        assert '__Host-loom_session' in http.cookies
        assert (await http.get('/api/v1/auth/me')).status_code == 200
        assert (await http.post('/api/v1/auth/login/complete', json={'token': proof['login_token']})).status_code == 400


@pytest.mark.parametrize('wrong', ['application', 'origin', 'generation', 'legacy', 'team'])
async def test_managed_proof_rejects_foreign_consumers_and_team_tampering(login_context, wrong):
    bridge, alice, row, factory, _, _ = login_context
    proof = await bridge.issue(alice, row.application_id)
    scope, token = audience(row), proof['login_token']
    if wrong == 'application':
        scope['application_id'] = str(uuid4())
    elif wrong == 'origin':
        scope['origin'] = 'https://other.dev.example.com'
    elif wrong == 'generation':
        scope['access_generation'] = 2
    elif wrong == 'legacy':
        scope = {}
    else:
        token = token.replace(alice.team_id.hex, uuid4().hex)
    async with child(factory, row, scope) as http:
        response = await http.post('/api/v1/auth/login/complete', json={'token': token})
        assert response.status_code == 400, response.text
        assert not http.cookies
    async with child(factory, row) as http:
        assert (await http.post('/api/v1/auth/login/complete', json={'token': proof['login_token']})).status_code == 200


@pytest.mark.parametrize('change', ['user-disabled', 'team-disabled', 'membership-removed', 'admin', 'expired', 'downgraded'])
async def test_managed_proof_rechecks_shared_identity_and_expiry_at_consumption(login_context, change):
    bridge, alice, row, factory, _, _ = login_context
    proof = await bridge.issue(alice, row.application_id)
    async with factory.begin() as session:
        if change == 'user-disabled':
            (await session.get(User, alice.user_id)).status = 'disabled'
        elif change == 'team-disabled':
            (await session.get(Team, alice.team_id)).disabled_at = datetime.now(UTC)
        elif change == 'admin':
            (await session.get(User, alice.user_id)).is_platform_admin = True
        elif change == 'expired':
            await session.execute(update(LoginChallenge).values(expires_at=datetime.now(UTC) - timedelta(seconds=1)))
        else:
            member = await session.get(TeamMembership, (alice.team_id, alice.user_id))
            if change == 'membership-removed':
                await session.delete(member)
            else:
                member.role = 'viewer'
    async with child(factory, row) as http:
        response = await http.post('/api/v1/auth/login/complete', json={'token': proof['login_token']})
        if change == 'downgraded':
            assert response.status_code == 200 and response.json()['role'] == 'viewer'
        else:
            assert response.status_code == (400 if change == 'expired' else 403), response.text
            assert not http.cookies


@pytest.mark.parametrize('change', ['foreign-user', 'foreign-team', 'bearer', 'stopped'])
async def test_login_admission_rejects_foreign_delegated_and_stopped_access_before_sql(login_context, change):
    bridge, alice, row, _, opened, registry = login_context
    principal = alice
    if change == 'foreign-user':
        principal = replace(alice, user_id=uuid4())
    elif change == 'foreign-team':
        principal = replace(alice, team_id=uuid4())
    elif change == 'bearer':
        principal = replace(alice, auth_kind='bearer')
    else:
        await registry.transition(row.application_id, principal=alice, idempotency_key='stop-before-login',
            action='suspend', expected_generation=1)
    with pytest.raises(ManagementError):
        await bridge.issue(principal, row.application_id)
    assert not opened


async def test_stop_after_challenge_commit_never_returns_stale_login(login_context, monkeypatch):
    bridge, alice, row, factory, _, registry = login_context
    original = registry.ready_access
    calls = 0

    async def stopped_before_recheck(application_id: UUID, *, principal):
        nonlocal calls
        calls += 1
        if calls == 2:
            await registry.transition(row.application_id, principal=alice, idempotency_key='stop-during-login',
                action='suspend', expected_generation=1)
        return await original(application_id, principal=principal)

    monkeypatch.setattr(registry, 'ready_access', stopped_before_recheck)
    with pytest.raises(ManagementError, match='application_not_ready'):
        await bridge.issue(alice, row.application_id)
    assert calls == 2
    async with factory() as session:
        assert (await session.scalars(select(LoginChallenge))).one().consumed_at is None


async def test_pending_lifecycle_has_no_login_material(applications):
    registry, _, (alice, _), prepare, _, _ = applications
    operation = await registry.create(principal=alice, idempotency_key='pending-login', **prepare())
    with pytest.raises(ManagementError, match='application_not_ready'):
        await registry.ready_access(operation.application_id, principal=replace(alice, auth_kind='session'))


async def test_retired_sql_role_blocks_issuance_with_secret_free_error(login_context, database_access):
    bridge, alice, row, factory, _, _ = login_context
    database_access[2].revoke(row.application_id, row.incarnation, 1)
    with pytest.raises(ManagementError) as caught:
        await bridge.issue(alice, row.application_id)
    assert str(caught.value) == 'application_login_unavailable'
    async with factory() as session:
        assert list(await session.scalars(select(LoginChallenge))) == []


async def test_management_login_route_uses_existing_auth_csrf_and_no_store(login_context):
    from loom_service.password_auth import hash_password

    bridge, alice, row, _, _, registry = login_context
    async with registry.session_factory.begin() as session:
        (await session.get(User, alice.user_id)).password_hash = hash_password('management-test-passphrase')
    settings = LoomServiceSettings(_env_file=None, service_mode='management',
        db_url=registry.session_factory.kw['bind'].url.render_as_string(hide_password=False),
        auth_local_http=False, public_base_url='https://management.example.com')
    app = create_app(settings)
    app.state.settings, app.state.session_factory = settings, registry.session_factory
    app.state.application_login = bridge
    path = f'/api/v1/applications/{row.application_id}/login'
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='https://management.example.com') as http:
        assert (await http.post(path)).status_code == 401
        response = await http.post('/api/v1/auth/login', json={'username': 'alice', 'password': 'management-test-passphrase'})
        assert response.status_code == 200, response.text
        assert (await http.post(path)).status_code == 403
        proof = await http.post(path, headers={'X-Loom-CSRF': response.json()['csrf_token']})
        assert proof.status_code == 200 and proof.headers['Cache-Control'] == 'no-store'
        assert proof.json()['application_id'] == str(row.application_id)
        del app.state.application_login
        assert (await http.post(path, headers={'X-Loom-CSRF': response.json()['csrf_token']})).status_code == 503
