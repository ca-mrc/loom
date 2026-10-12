"""Invite links and membership onboarding for issue #327."""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy import create_engine, delete, insert, select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import sessionmaker

from loom.admin_secret import AdminSecretVerifier
from loom.db.schema import (
    AdminAuditEvent,
    LoginChallenge,
    Team,
    TeamInvite,
    TeamMembership,
    TeamQuota,
    User,
    UserSession,
)
from loom.system_identities import (
    TASKSET_FENCE_CANARY_TEAM_ID,
    TASKSET_FENCE_CANARY_TEAM_NAME,
)
from loom_service.app import create_app
from loom_service.config import LoomServiceSettings

RAW_ADMIN_TOKEN = "loom_admin_" + "I" * 43


@pytest.fixture
async def invite_app(
    monkeypatch: pytest.MonkeyPatch,
    postgres_url: str,
) -> AsyncIterator[tuple[FastAPI, UUID, UUID, UUID]]:
    for k, v in {
        "LOOM_SVC_DB_URL": postgres_url,
        "LOOM_SVC_MINIO_ENDPOINT": "http://minio:9000",
        "LOOM_SVC_MINIO_ACCESS_KEY": "x",
        "LOOM_SVC_MINIO_SECRET_KEY": "y",
        "LOOM_SVC_CONTROL_PLANE_URL": "http://cp:8080/",
        "LOOM_SVC_GATEWAY_URL": "http://gw:9100/",
        "LOOM_SVC_AUTH_RETURN_LOGIN_TOKEN": "1",
        "LOOM_PUBLIC_BASE_URL": "https://loom.example.com",
    }.items():
        monkeypatch.setenv(k, v)
    settings = LoomServiceSettings(_env_file=None)
    app = create_app(settings)
    engine = create_async_engine(str(settings.db_url))
    app.state.settings = settings
    app.state.session_factory = async_sessionmaker(
        engine,
        expire_on_commit=False,
    )
    app.state.admin_secret_verifier = AdminSecretVerifier.from_token(
        RAW_ADMIN_TOKEN,
    )

    team_a, team_b, owner_id = uuid4(), uuid4(), uuid4()
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(insert(Team).values(id=team_a, name="Invite Alpha"))
        s.execute(insert(Team).values(id=team_b, name="Invite Beta"))
        s.execute(insert(User).values(
            id=owner_id,
            email="owner@example.com",
            username="owner",
            username_normalized="owner",
            display_name="Owner Example",
            status="active",
            is_platform_admin=False,
            created_at=datetime.now(UTC),
        ))
        s.execute(insert(TeamMembership).values(
            team_id=team_a,
            user_id=owner_id,
            role="owner",
        ))
        s.commit()
    try:
        yield app, team_a, team_b, owner_id
    finally:
        await engine.dispose()
        with sl() as s:
            table_exists = s.execute(
                text("SELECT to_regclass('team_invites')"),
            ).scalar_one()
            if table_exists is not None:
                s.execute(text("DELETE FROM team_invites"))
            s.execute(delete(AdminAuditEvent))
            s.execute(delete(UserSession))
            s.execute(delete(LoginChallenge))
            s.execute(delete(TeamMembership))
            s.execute(
                delete(TeamQuota).where(
                    TeamQuota.team_id != TASKSET_FENCE_CANARY_TEAM_ID,
                )
            )
            s.execute(delete(User))
            s.execute(
                delete(Team).where(
                    Team.id != TASKSET_FENCE_CANARY_TEAM_ID),
            )
            s.commit()
        sync_engine.dispose()


async def _login(
    ac: httpx.AsyncClient,
    email: str = "owner@example.com",
) -> tuple[dict[str, object], str]:
    start = await ac.post("/api/v1/auth/login/start", json={"email": email})
    assert start.status_code == 200, start.text
    token = start.json().get("login_token")
    assert isinstance(token, str)
    complete = await ac.post("/api/v1/auth/login/complete", json={"token": token})
    assert complete.status_code == 200, complete.text
    body = complete.json()
    csrf = body.get("csrf_token")
    assert isinstance(csrf, str)
    return body, csrf


def _admin_headers(actor: str = "qianyi") -> dict[str, str]:
    return {
        "Authorization": f"Bearer {RAW_ADMIN_TOKEN}",
        "X-Loom-Admin-Actor": actor,
    }


async def test_owner_creates_lists_and_revokes_invite_without_revealing_code(
    invite_app: tuple[FastAPI, UUID, UUID, UUID],
) -> None:
    app, team_a, _team_b, _owner_id = invite_app
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="https://loom.example.com",
    ) as ac:
        _body, csrf = await _login(ac)
        created = await ac.post(
            "/api/v1/invites",
            headers={"X-Loom-CSRF": csrf},
            json={
                "email": "New.User@Example.com",
                "team_id": str(team_a),
                "role": "member",
                "expires_in_days": 7,
                "max_uses": 1,
            },
        )
        assert created.status_code == 201, created.text
        created_body = created.json()
        invite_code = created_body["invite_code"]
        assert invite_code.startswith("loom_invite_")
        assert created_body["invite_link"].startswith(
            "https://loom.example.com/invites/accept?code=loom_invite_",
        )
        invite_id = created_body["invite"]["id"]
        assert created_body["invite"]["email"] == "new.user@example.com"
        assert created_body["invite"]["status"] == "pending"
        assert created_body["invite"]["code_prefix"]

        listed = await ac.get(f"/api/v1/invites?team_id={team_a}")
        revoked = await ac.post(
            f"/api/v1/invites/{invite_id}/revoke",
            headers={"X-Loom-CSRF": csrf},
            json={"reason": "wrong recipient"},
        )
        after_revoke = await ac.get(f"/api/v1/invites?team_id={team_a}")

    assert listed.status_code == 200, listed.text
    listed_item = listed.json()["items"][0]
    assert listed_item["id"] == invite_id
    assert listed_item["email"] == "new.user@example.com"
    assert "invite_code" not in listed_item
    assert "invite_link" not in listed_item

    assert revoked.status_code == 200, revoked.text
    assert revoked.json()["status"] == "revoked"
    assert after_revoke.json()["items"][0]["status"] == "revoked"


async def test_admin_cannot_invite_users_to_the_system_canary_team(
    invite_app: tuple[FastAPI, UUID, UUID, UUID],
) -> None:
    """The deployment-owned Team cannot acquire a human member via admin invite."""
    app, _team_a, _team_b, _owner_id = invite_app
    async with app.state.session_factory() as session:
        system_team = await session.get(Team, TASKSET_FENCE_CANARY_TEAM_ID)
        if system_team is None:
            system_team = Team(
                id=TASKSET_FENCE_CANARY_TEAM_ID,
                name=TASKSET_FENCE_CANARY_TEAM_NAME,
            )
            session.add(system_team)
            await session.flush()
            session.add(TeamQuota(team_id=system_team.id))
            await session.commit()
    assert system_team is not None
    assert system_team.name == TASKSET_FENCE_CANARY_TEAM_NAME

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="https://loom.example.com") as ac:
        response = await ac.post(
            "/api/v1/invites",
            headers=_admin_headers(),
            json={
                "email": "canary-member@example.com",
                "team_id": str(TASKSET_FENCE_CANARY_TEAM_ID),
                "role": "member",
                "expires_in_days": 7,
            },
        )

    assert response.status_code == 403, response.text


async def test_resend_rotates_invite_code_and_old_code_fails(
    invite_app: tuple[FastAPI, UUID, UUID, UUID],
) -> None:
    app, team_a, _team_b, _owner_id = invite_app
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="https://loom.example.com",
    ) as ac:
        _body, csrf = await _login(ac)
        created = await ac.post(
            "/api/v1/invites",
            headers={"X-Loom-CSRF": csrf},
            json={
                "email": "rotate@example.com",
                "team_id": str(team_a),
                "role": "viewer",
                "expires_in_days": 7,
            },
        )
        assert created.status_code == 201, created.text
        old_code = created.json()["invite_code"]
        invite_id = created.json()["invite"]["id"]

        resent = await ac.post(
            f"/api/v1/invites/{invite_id}/resend",
            headers={"X-Loom-CSRF": csrf},
        )
        assert resent.status_code == 200, resent.text
        new_code = resent.json()["invite_code"]
        assert new_code.startswith("loom_invite_")
        assert new_code != old_code

    async with httpx.AsyncClient(
        transport=transport,
        base_url="https://loom.example.com",
    ) as accept_client:
        old_accept = await accept_client.post(
            "/api/v1/invites/accept",
            json={"code": old_code, "email": "rotate@example.com"},
        )
        new_accept = await accept_client.post(
            "/api/v1/invites/accept",
            json={"code": new_code, "email": "rotate@example.com"},
        )

    assert old_accept.status_code == 400, old_accept.text
    assert old_accept.json()["detail"] == "invalid invite"
    assert new_accept.status_code == 200, new_accept.text
    assert new_accept.json()["current_team"]["id"] == str(team_a)
    assert new_accept.json()["current_team"]["role"] == "viewer"


async def test_accept_invite_creates_user_membership_session_and_safe_audit(
    invite_app: tuple[FastAPI, UUID, UUID, UUID],
) -> None:
    app, team_a, _team_b, _owner_id = invite_app
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="https://loom.example.com",
    ) as ac:
        created = await ac.post(
            "/api/v1/invites",
            headers=_admin_headers(),
            json={
                "email": "beta@example.com",
                "team_id": str(team_a),
                "role": "member",
                "expires_in_days": 7,
                "max_uses": 1,
            },
        )
        assert created.status_code == 201, created.text
        code = created.json()["invite_code"]
        prefix = created.json()["invite"]["code_prefix"]

        accepted = await ac.post(
            "/api/v1/invites/accept",
            json={"code": code, "email": "beta@example.com"},
        )
        assert accepted.status_code == 200, accepted.text
        me = await ac.get("/api/v1/auth/me")
        duplicate = await ac.post(
            "/api/v1/invites/accept",
            json={"code": code, "email": "beta@example.com"},
        )
        audit = await ac.get(
            "/api/v1/admin/audit-events",
            headers=_admin_headers(),
        )

    accepted_body = accepted.json()
    assert accepted_body["user"]["email"] == "beta@example.com"
    assert accepted_body["current_team"]["id"] == str(team_a)
    assert accepted_body["current_team"]["role"] == "member"
    assert "__Host-loom_session" in ac.cookies
    assert me.status_code == 200, me.text
    assert me.json()["user"]["email"] == "beta@example.com"
    assert duplicate.status_code == 409, duplicate.text

    audit_events = audit.json()["items"]
    accept_events = [
        event for event in audit_events if event["action"] == "invite.accept"
    ]
    assert len(accept_events) == 1
    metadata = accept_events[0]["metadata"]
    assert metadata["team_id"] == str(team_a)
    assert metadata["role"] == "member"
    assert metadata["invite_prefix"] == prefix
    assert code not in str(metadata)


@pytest.mark.parametrize("platform_admin", [False, True])
async def test_existing_invitee_requires_own_authenticated_session(
    invite_app: tuple[FastAPI, UUID, UUID, UUID], platform_admin: bool,
) -> None:
    app, team_a, team_b, _owner_id = invite_app
    user_id = uuid4()
    async with app.state.session_factory() as session:
        session.add(User(id=user_id, email="existing@example.com", username="existing",
                         username_normalized="existing", status="active",
                         is_platform_admin=platform_admin, created_at=datetime.now(UTC)))
        await session.flush()
        session.add(TeamMembership(team_id=team_b, user_id=user_id, role="member"))
        await session.commit()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="https://loom.example.com") as owner:
        _, csrf = await _login(owner)
        created = await owner.post("/api/v1/invites", headers={"X-Loom-CSRF": csrf},
                                   json={"email": "existing@example.com", "team_id": str(team_a)})
        assert created.status_code == 201, created.text
    code = created.json()["invite_code"]
    invite_id = UUID(created.json()["invite"]["id"])
    async with httpx.AsyncClient(transport=transport, base_url="https://loom.example.com") as recipient:
        denied = await recipient.post("/api/v1/invites/accept",
                                      json={"code": code, "email": "existing@example.com"})
        assert denied.status_code == 403, denied.text
        assert denied.headers.get("x-loom-invite-rejection") == "sign-in-required"
        assert not denied.headers.get("set-cookie")
        async with app.state.session_factory() as session:
            invite = await session.get(TeamInvite, invite_id)
            assert invite.status == "pending" and invite.accepted_uses == 0
            assert await session.get(TeamMembership, (team_a, user_id)) is None
            assert not (await session.execute(select(UserSession).where(
                UserSession.user_id == user_id))).scalars().all()

        # The same unconsumed invite still works once its recipient signs in.
        await _login(recipient, "existing@example.com")
        accepted = await recipient.post("/api/v1/invites/accept", json={"code": code})
        assert accepted.status_code == 200, accepted.text
        assert accepted.json()["user"]["id"] == str(user_id)
        assert accepted.json()["current_team"]["id"] == str(team_a)


async def test_invite_acceptance_denies_expired_and_wrong_email_without_leaks(
    invite_app: tuple[FastAPI, UUID, UUID, UUID],
    postgres_url: str,
) -> None:
    app, team_a, _team_b, _owner_id = invite_app
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="https://loom.example.com",
    ) as ac:
        created = await ac.post(
            "/api/v1/invites",
            headers=_admin_headers(),
            json={
                "email": "expires@example.com",
                "team_id": str(team_a),
                "role": "member",
                "expires_in_days": 7,
            },
        )
        assert created.status_code == 201, created.text
        code = created.json()["invite_code"]
        invite_id = created.json()["invite"]["id"]

        wrong_email = await ac.post(
            "/api/v1/invites/accept",
            json={"code": code, "email": "intruder@example.com"},
        )

    sync_engine = create_engine(postgres_url)
    with sync_engine.begin() as conn:
        conn.execute(
            text("UPDATE team_invites SET expires_at = :expires_at WHERE id = :id"),
            {
                "expires_at": datetime.now(UTC) - timedelta(minutes=1),
                "id": invite_id,
            },
        )
    sync_engine.dispose()

    async with httpx.AsyncClient(
        transport=transport,
        base_url="https://loom.example.com",
    ) as ac:
        expired = await ac.post(
            "/api/v1/invites/accept",
            json={"code": code, "email": "expires@example.com"},
        )

    assert wrong_email.status_code == 403, wrong_email.text
    assert wrong_email.headers.get("x-loom-invite-rejection") == "email-mismatch"
    assert "Invite Alpha" not in wrong_email.text
    assert str(team_a) not in wrong_email.text
    assert expired.status_code == 410, expired.text
    assert expired.headers.get("x-loom-invite-rejection") == "expired"
    assert "Invite Alpha" not in expired.text
    assert str(team_a) not in expired.text


async def test_domain_invite_allows_matching_email_domain_only(
    invite_app: tuple[FastAPI, UUID, UUID, UUID],
) -> None:
    app, team_a, _team_b, _owner_id = invite_app
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="https://loom.example.com",
    ) as ac:
        created = await ac.post(
            "/api/v1/invites",
            headers=_admin_headers("beta-admin"),
            json={
                "email": "first@allowed.example",
                "allowed_domain": "allowed.example",
                "team_id": str(team_a),
                "role": "viewer",
                "expires_in_days": 7,
                "max_uses": 2,
            },
        )
        assert created.status_code == 201, created.text
        code = created.json()["invite_code"]

        denied = await ac.post(
            "/api/v1/invites/accept",
            json={"code": code, "email": "person@other.example"},
        )
        accepted = await ac.post(
            "/api/v1/invites/accept",
            json={"code": code, "email": "second@allowed.example"},
        )

    assert denied.status_code == 403, denied.text
    assert accepted.status_code == 200, accepted.text
    assert accepted.json()["user"]["email"] == "second@allowed.example"
    assert accepted.json()["current_team"]["role"] == "viewer"
