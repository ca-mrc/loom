"""Cancellation keeps browser identity across the Service -> CP boundary."""

from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID, uuid4

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy import delete, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from loom.db.schema import Batch, Task, Team, TeamMembership, Token, Trial, User, UserSession
from loom_control_plane.routes.trials import router as cp_trials_router
from loom_service.app import create_app
from loom_service.config import LoomServiceSettings
from loom_service.session_auth import create_session_for_user


@dataclass
class CancelStack:
    service: FastAPI
    control_plane: FastAPI
    sessions: async_sessionmaker[AsyncSession]
    team_id: UUID
    user_id: UUID
    trial_id: UUID
    other_trial_id: UUID
    batch_id: UUID
    other_batch_id: UUID
    session_cookie: str
    csrf: str
    bearer: str
    cp_requests: list[str]


@pytest.fixture
async def cancel_stack(postgres_url: str, request: pytest.FixtureRequest) -> AsyncIterator[CancelStack]:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    scoped = getattr(request, "param", None) == "scoped"
    audience = {
        "schema_version": "loom.application-session-audience.v1",
        "application_id": str(uuid4()), "origin": "https://alice.dev.example.com",
        "access_generation": 1,
    }
    # Custom public cookie/header names must still work across the internal hop.
    settings = LoomServiceSettings(
        _env_file=None, db_url=postgres_url,
        minio_endpoint="http://minio:9000", minio_access_key="x", minio_secret_key="y",
        control_plane_url="http://cp/", gateway_url="http://gw/",
        auth_session_cookie_name="test_session", auth_csrf_header_name="X-Test-CSRF",
        public_base_url="https://alice.dev.example.com" if scoped else None,
        auth_local_http=not scoped,
        auth_session_audience_json=json.dumps(audience) if scoped else None,
    )
    service = create_app(settings)
    service.state.settings = settings
    service.state.session_factory = sessions
    cp = FastAPI()
    cp.include_router(cp_trials_router)
    cp.state.session_factory = sessions
    cp_requests: list[str] = []

    async def record_request(request: httpx.Request) -> None:
        cp_requests.append(request.url.path)

    cp_client = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=cp), base_url="http://cp",
        event_hooks={"request": [record_request]},
    )
    service.state.http_client = cp_client
    team_id, other_team_id, user_id = uuid4(), uuid4(), uuid4()
    trial_id, other_trial_id, batch_id, other_batch_id = (uuid4() for _ in range(4))
    task_id = f"cancel-session-{uuid4()}"
    bearer = f"test-cancel-{uuid4()}"
    async with sessions() as session:
        session.add_all([
            Team(id=team_id, name=f"cancel-{team_id}"),
            Team(id=other_team_id, name=f"cancel-{other_team_id}"),
            Task(id=task_id, checksum="0" * 64, config={}),
        ])
        user = User(id=user_id, username=f"cancel-{user_id}",
                    username_normalized=f"cancel-{user_id}", status="active",
                    is_platform_admin=False)
        session.add(user)
        await session.flush()
        session.add(TeamMembership(team_id=team_id, user_id=user_id, role="member"))
        for bid, tid in ((batch_id, team_id), (other_batch_id, other_team_id)):
            session.add(Batch(id=bid, team_id=tid, name="cancel", task_filter={},
                              trial_config={}, created_by_token_prefix="test"))
        session.add(Token(token_hash=hashlib.sha256(bearer.encode()).digest(),
                          type="team", scopes=["submit"], team_id=team_id,
                          created_by_user_id=user_id, issued_at=datetime.now(UTC)))
        await session.flush()
        session.add_all([
            Trial(id=trial_id, team_id=team_id, batch_id=batch_id, task_id=task_id,
                  config={}, requires_caps={}, state="queued"),
            Trial(id=other_trial_id, team_id=other_team_id, batch_id=other_batch_id,
                  task_id=task_id, config={}, requires_caps={}, state="queued"),
        ])
        created = await create_session_for_user(
            session, user=user, current_team_id=team_id, session_ttl_seconds=3600,
            audience=settings.session_audience,
        )
        await session.commit()
    try:
        yield CancelStack(service, cp, sessions, team_id, user_id, trial_id,
                          other_trial_id, batch_id, other_batch_id,
                          created.raw_session, created.raw_csrf, bearer, cp_requests)
    finally:
        await cp_client.aclose()
        async with sessions() as session:
            await session.execute(delete(Trial).where(Trial.id.in_([trial_id, other_trial_id])))
            await session.execute(delete(Batch).where(Batch.id.in_([batch_id, other_batch_id])))
            await session.execute(delete(Token).where(Token.created_by_user_id == user_id))
            await session.execute(delete(UserSession).where(UserSession.user_id == user_id))
            await session.execute(delete(TeamMembership).where(TeamMembership.user_id == user_id))
            await session.execute(delete(User).where(User.id == user_id))
            await session.execute(delete(Team).where(Team.id.in_([team_id, other_team_id])))
            await session.execute(delete(Task).where(Task.id == task_id))
            await session.commit()
        await engine.dispose()


@pytest.mark.parametrize("resource", ["trials", "batches"])
@pytest.mark.parametrize("cancel_stack", [None, "scoped"], indirect=True)
@pytest.mark.parametrize("token_source", ["login", "me"])
async def test_other_tab_identity_reads_do_not_invalidate_cancel(cancel_stack, resource, token_source):
    stack = cancel_stack
    settings = stack.service.state.settings
    identifier = stack.trial_id if resource == "trials" else stack.batch_id
    async with stack.sessions() as session:
        await session.execute(update(Trial).where(Trial.id == stack.trial_id).values(
            state="running", batch_id=stack.batch_id if resource == "batches" else None,
        ))
        await session.commit()
    client_options = dict(transport=httpx.ASGITransport(app=stack.service),
                          base_url="https://alice.dev.example.com",
                          cookies={settings.session_cookie_name: stack.session_cookie})
    async with httpx.AsyncClient(**client_options) as first, httpx.AsyncClient(**client_options) as second:
        csrf = stack.csrf
        if token_source == "me":
            identity = await first.get("/api/v1/auth/me")
            assert identity.status_code == 200, identity.text
            csrf = identity.json()["csrf_token"]
        for _ in range(2):
            identity = await second.get("/api/v1/auth/me")
            assert identity.status_code == 200, identity.text
        path = f"/api/v1/{resource}/{identifier}/cancel"
        for headers in ({}, {"X-Test-CSRF": "wrong-token"}):
            denied = await first.post(path, headers=headers)
            assert denied.status_code == 403
        assert stack.cp_requests == []
        response = await asyncio.wait_for(first.post(path, headers={"X-Test-CSRF": csrf}), 5)
        assert response.status_code == 200, response.text
    assert stack.cp_requests == [f"/trials/{stack.trial_id}/cancel"]
    async with stack.sessions() as session:
        assert (await session.get(Trial, stack.trial_id)).cancellation_requested_at is not None
        assert (await session.get(Trial, stack.other_trial_id)).state == "queued"


@pytest.mark.parametrize("resource", ["trials", "batches"])
@pytest.mark.parametrize("auth_kind", ["cookie", "bearer"])
@pytest.mark.parametrize("trial_state", ["queued", "running"])
async def test_cancel_crosses_real_service_cp_boundary(
    cancel_stack: CancelStack, resource: str, auth_kind: str, trial_state: str,
) -> None:
    stack = cancel_stack
    identifier = stack.trial_id if resource == "trials" else stack.batch_id
    headers = ({"X-Test-CSRF": stack.csrf} if auth_kind == "cookie"
               else {"Authorization": f"Bearer {stack.bearer}"})
    cookies = {"test_session": stack.session_cookie} if auth_kind == "cookie" else {}
    async with stack.sessions() as session:
        await session.execute(update(Trial).where(Trial.id == stack.trial_id).values(
            state=trial_state, batch_id=stack.batch_id if resource == "batches" else None,
        ))
        await session.commit()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=stack.service), base_url="http://svc",
        cookies=cookies,
    ) as client:
        # A nested session verifier must not wait on the Service's auth-row lock.
        response = await asyncio.wait_for(
            client.post(f"/api/v1/{resource}/{identifier}/cancel", headers=headers), 5,
        )
    assert response.status_code == 200, response.text
    assert stack.cp_requests == [f"/trials/{stack.trial_id}/cancel"]
    async with stack.sessions() as session:
        trial = await session.get(Trial, stack.trial_id)
        assert trial is not None
        assert trial.state == ("cancelled" if trial_state == "queued" else "running")
        assert trial.cancellation_requested_at is not None
        if trial_state == "running":
            assert trial.cancellation_observed_at is None
            assert trial.finished_at is None
        other = await session.get(Trial, stack.other_trial_id)
        assert other is not None and other.state == "queued"


async def test_hosted_cookie_cancellation_keeps_identity_across_internal_http(cancel_stack):
    stack = cancel_stack
    stack.service.state.settings.auth_local_http = False
    async with stack.sessions() as session:
        await session.execute(update(Trial).where(Trial.id == stack.trial_id).values(batch_id=None))
        await session.commit()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=stack.service),
                                 base_url="https://alice.dev.example.com") as client:
        response = await client.post(f"/api/v1/trials/{stack.trial_id}/cancel", headers={
            "Cookie": f"__Host-test_session={stack.session_cookie}",
            "X-Test-CSRF": stack.csrf,
            "Origin": "https://alice.dev.example.com",
        })
    assert response.status_code == 200, response.text
    async with stack.sessions() as session:
        trial = await session.get(Trial, stack.trial_id)
        assert trial.state == "cancelled"
        assert (await session.get(Trial, stack.other_trial_id)).state == "queued"


@pytest.mark.parametrize("cancel_stack", ["scoped"], indirect=True)
@pytest.mark.parametrize("resource", ["trials", "batches"])
async def test_audience_cancellation_uses_configured_identity_not_supplied_header(cancel_stack, resource):
    stack = cancel_stack
    identifier = stack.trial_id if resource == "trials" else stack.batch_id
    if resource == "trials":
        async with stack.sessions() as session:
            await session.execute(update(Trial).where(Trial.id == stack.trial_id).values(batch_id=None))
            await session.commit()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=stack.service),
                                 base_url="https://alice.dev.example.com") as client:
        response = await asyncio.wait_for(client.post(f"/api/v1/{resource}/{identifier}/cancel", headers={
            "Cookie": f"__Host-test_session={stack.session_cookie}", "X-Test-CSRF": stack.csrf,
            # A caller cannot replace the protected process audience on the hop.
            "X-Loom-Session-Audience": "untrusted-client-supplied-audience",
        }), 5)
    assert response.status_code == 200, response.text
    async with stack.sessions() as session:
        assert (await session.get(Trial, stack.trial_id)).state == "cancelled"
        assert (await session.get(Trial, stack.other_trial_id)).state == "queued"


@pytest.mark.parametrize("cancel_stack", ["scoped"], indirect=True)
@pytest.mark.parametrize("denial,status", [
    ("missing-audience", 401), ("wrong-audience", 401), ("invalid-audience", 401),
    ("oversized-audience", 401), ("missing-csrf", 403), ("cross-team", 409),
    ("revoked-session", 401),
])
async def test_control_plane_revalidates_audience_and_normal_authority(cancel_stack, denial, status):
    stack = cancel_stack
    audience = stack.service.state.settings.session_audience.model_dump(mode="json")
    if denial == "wrong-audience":
        audience["access_generation"] = 2
    headers = {"X-Loom-CSRF": stack.csrf, "X-Loom-Session-Audience": json.dumps(audience)}
    if denial == "missing-audience":
        headers.pop("X-Loom-Session-Audience")
    elif denial == "invalid-audience":
        headers["X-Loom-Session-Audience"] = "null"
    elif denial == "oversized-audience":
        headers["X-Loom-Session-Audience"] = " " * 4097
    elif denial == "missing-csrf":
        headers.pop("X-Loom-CSRF")
    elif denial == "revoked-session":
        async with stack.sessions() as session:
            await session.execute(update(UserSession).where(UserSession.user_id == stack.user_id)
                                  .values(revoked_at=datetime.now(UTC)))
            await session.commit()
    trial_id = stack.other_trial_id if denial == "cross-team" else stack.trial_id
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=stack.control_plane),
                                 base_url="http://cp", cookies={"loom_session": stack.session_cookie}) as client:
        response = await client.post(f"/trials/{trial_id}/cancel", headers=headers)
    assert response.status_code == status, response.text
    async with stack.sessions() as session:
        assert (await session.get(Trial, stack.trial_id)).state == "queued"
        assert (await session.get(Trial, stack.other_trial_id)).state == "queued"


@pytest.mark.parametrize("resource", ["trials", "batches"])
@pytest.mark.parametrize("denial", ["cross-team", "missing-csrf", "invalid-csrf", "disabled-team"])
async def test_service_rejects_cookie_cancellation_before_forward(
    cancel_stack: CancelStack, resource: str, denial: str,
) -> None:
    stack = cancel_stack
    identifier = (stack.other_trial_id if resource == "trials" else stack.other_batch_id)
    headers = {"X-Test-CSRF": stack.csrf}
    if denial != "cross-team":
        identifier = stack.trial_id if resource == "trials" else stack.batch_id
        if denial == "disabled-team":
            async with stack.sessions() as session:
                await session.execute(update(Team).where(Team.id == stack.team_id).values(
                    disabled_at=datetime.now(UTC),
                ))
                await session.commit()
        else:
            headers = {} if denial == "missing-csrf" else {"X-Test-CSRF": "invalid"}
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=stack.service), base_url="http://svc",
        cookies={"test_session": stack.session_cookie},
    ) as client:
        response = await client.post(f"/api/v1/{resource}/{identifier}/cancel", headers=headers)
    assert response.status_code == 403
    assert stack.cp_requests == []
    async with stack.sessions() as session:
        states = (await session.execute(select(Trial.state).where(
            Trial.id.in_([stack.trial_id, stack.other_trial_id]),
        ))).scalars().all()
        assert states == ["queued", "queued"]


@pytest.mark.parametrize("denial,status", [
    ("cross-team", 409), ("missing-csrf", 403), ("invalid-csrf", 403),
    ("viewer", 403), ("disabled-team", 403), ("revoked-session", 401),
    ("invalid-bearer", 401),
])
async def test_control_plane_revalidates_cookie_authority(
    cancel_stack: CancelStack, denial: str, status: int,
) -> None:
    stack = cancel_stack
    trial_id = stack.other_trial_id if denial == "cross-team" else stack.trial_id
    headers = {"X-Loom-CSRF": stack.csrf}
    if denial == "missing-csrf":
        headers = {}
    elif denial == "invalid-csrf":
        headers["X-Loom-CSRF"] = "invalid"
    elif denial == "invalid-bearer":
        headers["Authorization"] = "Bearer invalid"
    async with stack.sessions() as session:
        if denial == "viewer":
            await session.execute(update(TeamMembership).where(
                TeamMembership.user_id == stack.user_id,
            ).values(role="viewer"))
        elif denial == "disabled-team":
            await session.execute(update(Team).where(Team.id == stack.team_id).values(
                disabled_at=datetime.now(UTC),
            ))
        elif denial == "revoked-session":
            await session.execute(update(UserSession).where(
                UserSession.user_id == stack.user_id,
            ).values(revoked_at=datetime.now(UTC)))
        await session.commit()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=stack.control_plane), base_url="http://cp",
        cookies={"loom_session": stack.session_cookie},
    ) as client:
        response = await client.post(f"/trials/{trial_id}/cancel", headers=headers)
    assert response.status_code == status, response.text
    async with stack.sessions() as session:
        trial = await session.get(Trial, trial_id)
        assert trial is not None and trial.state == "queued"
        assert trial.cancellation_requested_at is None
