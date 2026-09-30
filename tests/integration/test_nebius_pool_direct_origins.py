"""Actual service-to-CP submission keeps protected origin across the HTTP hop."""
from __future__ import annotations

import asyncio
import hashlib
import json
from datetime import UTC, datetime
from uuid import UUID, uuid4

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy import delete, func, select

from loom.db.base import Base
from loom.db.schema import DataLifecycleAuthority, Team, Token, Trial, User, UserSession
from loom_control_plane.config import ControlPlaneSettings
from loom_control_plane.routes.trials import router
from loom_service.session_auth import create_session_for_user
from tests.integration.test_service_trial_forwarder import fwd_setup as fwd_setup
from tests.unit.test_nebius_pool_submission_source import source

HEADER = "X-Loom-Submission-ID"
BODY = {"task_id": "local/task-1", "config": {"agent_name": "oracle", "agent_model": None}}


def configure(app, installed):
    audience = None if installed is None or installed["kind"] == "environment" else {
        "schema_version": "loom.application-session-audience.v1",
        "application_id": installed["application"]["application_id"],
        "origin": "https://alice.dev.example.com", "access_generation": 1}
    app.state.settings = app.state.settings.model_copy(update={
        "pool_submission_source_json": None if installed is None else json.dumps(installed),
        "auth_session_audience_json": None if audience is None else json.dumps(audience),
        "public_base_url": None if audience is None else audience["origin"],
        "auth_local_http": audience is None,
    })


@pytest.fixture
async def direct_stack(fwd_setup, monkeypatch, postgres_url):
    app, token, team_id, _ = fwd_setup
    monkeypatch.setenv("LOOM_LOCAL_EXECUTION", "1")
    cp = FastAPI()
    cp.include_router(router)
    cp.state.session_factory = app.state.session_factory
    cp.state.settings = ControlPlaneSettings(_env_file=None, db_url=postgres_url,
        minio_endpoint="http://minio:9000", minio_access_key="x", minio_secret_key="x",
        llm_gateway_url="http://gw:9100")
    await app.state.http_client.aclose()
    captured = []
    async def capture(request):
        captured.append(request)
    app.state.http_client = httpx.AsyncClient(transport=httpx.ASGITransport(app=cp),
        base_url="http://cp", event_hooks={"request": [capture]})
    async with app.state.session_factory() as session:
        user_id = await session.scalar(select(Token.created_by_user_id).where(
            Token.token_hash == hashlib.sha256(token.encode()).digest()))
    try:
        yield app, cp, token, team_id, user_id, captured
    finally:
        async with app.state.session_factory.begin() as session:
            # Only this fixture's submissions; borrowed fixture owns seed cleanup.
            table = Base.metadata.tables.get("nebius_pool_submissions")
            if table is not None:
                await session.execute(delete(table))
            await session.execute(delete(Trial))
            await session.execute(delete(DataLifecycleAuthority))
            await session.execute(delete(UserSession))


async def public_submit(stack, body=None, headers=None, cookies=None):
    app, _, token, _, _, _ = stack
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
            base_url="https://alice.dev.example.com", cookies=cookies or {}) as client:
        return await asyncio.wait_for(client.post("/api/v1/trials", json=body or BODY,
            headers=headers if headers is not None else {"Authorization": "Bearer " + token}), 5)


async def stored(stack, response):
    assert response.status_code == 201, response.text
    async with stack[0].state.session_factory() as session:
        return await session.get(Trial, UUID(response.json()["trial_id"]))


@pytest.mark.parametrize("kind", ["application", "environment"])
async def test_direct_trial_uses_process_origin_not_public_body_or_header(direct_stack, kind):
    app, _, token, _, _, captured = direct_stack
    installed = source(kind)
    configure(app, installed)
    response = await public_submit(direct_stack, BODY | {"pool_origin": source("environment"), "priority": 0},
        {"Authorization": "Bearer " + token, HEADER: str(uuid4())})
    trial = await stored(direct_stack, response)
    assert trial.pool_origin is not None
    assert trial.pool_origin == {"schema_version": "loom.pool-work-origin.v1",
        "submission_id": captured[0].headers[HEADER], "kind": kind,
        "data_environment_id": installed["data_environment_id"], "application": installed["application"]}
    assert json.loads(captured[0].content)["idempotency_key"]


async def test_cookie_submission_crosses_real_cp_with_application_origin_without_locking_it(direct_stack):
    app, _, _, team_id, user_id, _ = direct_stack
    configure(app, source())
    settings = app.state.settings
    async with app.state.session_factory.begin() as session:
        user = await session.get(User, user_id)
        created = await create_session_for_user(session, user=user, current_team_id=team_id,
            session_ttl_seconds=3600, audience=settings.session_audience)
    response = await public_submit(direct_stack, headers={settings.auth_csrf_header_name: created.raw_csrf},
        cookies={settings.session_cookie_name: created.raw_session})
    trial = await stored(direct_stack, response)
    assert trial.pool_origin["application"]["application_id"] == str(settings.session_audience.application_id)
    assert trial.submitted_by_user_id == user_id


async def test_origin_replay_after_new_app_version_keeps_original_trial(direct_stack):
    app = direct_stack[0]
    first = source()
    configure(app, first)
    body = BODY | {"idempotency_key": "origin-direct-" + uuid4().hex}
    initial = await stored(direct_stack, await public_submit(direct_stack, body))
    configure(app, source())
    replay = await stored(direct_stack, await public_submit(direct_stack, body))
    assert replay.id == initial.id
    assert replay.pool_origin is not None
    assert replay.pool_origin["application"] == first["application"]


@pytest.mark.parametrize("damage", ["payload", "unknown-id", "malformed-id", "other-user", "other-team"])
async def test_cp_rejects_unqualified_handoff_even_on_existing_idempotency_replay(direct_stack, damage):
    app, cp, token, team_id, _, captured = direct_stack
    configure(app, source())
    await stored(direct_stack, await public_submit(direct_stack))
    original = captured[0]
    body = json.loads(original.content)
    headers = {"Authorization": "Bearer " + token, HEADER: original.headers.get(HEADER, str(uuid4()))}
    if damage == "payload":
        body["config"]["submit_priority"] = 50
    elif damage == "unknown-id":
        headers[HEADER] = str(uuid4())
    elif damage == "malformed-id":
        headers[HEADER] = "not-a-uuid"
    else:
        other_user, raw = uuid4(), "other-" + uuid4().hex
        async with app.state.session_factory.begin() as session:
            if damage == "other-team":
                team_id = uuid4()
                session.add(Team(id=team_id, name=str(team_id)))
            session.add(User(id=other_user, username=str(other_user), username_normalized=str(other_user), status="active"))
            await session.flush()
            session.add(Token(token_hash=hashlib.sha256(raw.encode()).digest(), type="team", scopes=["submit"],
                team_id=team_id, created_by_user_id=other_user, issued_at=datetime.now(UTC)))
        headers["Authorization"] = "Bearer " + raw
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=cp), base_url="http://cp") as client:
        response = await client.post("/trials", json=body, headers=headers)
    assert response.status_code == 409, response.text
    async with app.state.session_factory() as session:
        assert await session.scalar(select(func.count()).select_from(Trial)) == 1


async def test_lost_reply_and_concurrent_replay_keep_one_trial_and_origin(direct_stack):
    app, cp, token, _, _, captured = direct_stack
    configure(app, source())
    async def lose_reply(response):
        raise httpx.ReadError("discard committed reply")
    app.state.http_client.event_hooks["response"] = [lose_reply]
    response = await public_submit(direct_stack)
    assert response.status_code == 502
    original = captured[0]
    assert HEADER in original.headers
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=cp), base_url="http://cp") as client:
        responses = await asyncio.gather(*(client.post("/trials", json=json.loads(original.content),
            headers={"Authorization": "Bearer " + token, HEADER: original.headers[HEADER]}) for _ in range(2)))
    first, second = [await stored(direct_stack, response) for response in responses]
    assert first.id == second.id and first.pool_origin == second.pool_origin
    async with app.state.session_factory() as session:
        assert await session.scalar(select(func.count()).select_from(Trial)) == 1


async def test_unconfigured_service_does_not_forward_client_origin(direct_stack):
    app, _, token, _, _, captured = direct_stack
    configure(app, None)
    trial = await stored(direct_stack, await public_submit(direct_stack,
        BODY | {"pool_origin": source("environment")}, {"Authorization": "Bearer " + token, HEADER: str(uuid4())}))
    assert trial.pool_origin is None
    assert HEADER not in captured[0].headers
