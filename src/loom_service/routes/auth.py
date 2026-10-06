"""Browser user session routes for invite-only public Loom (#326)."""

from __future__ import annotations

import secrets
from datetime import UTC, datetime, timedelta
from typing import Annotated, Any, Literal
from uuid import UUID

from fastapi import APIRouter, Header, HTTPException, Query, Request, Response
from pydantic import BaseModel, Field, field_validator
from sqlalchemy import func, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from loom.auth import AuthContext
from loom.db.schema import (
    AccountActionToken,
    PasswordResetRequest,
    Team,
    TeamMembership,
    Token,
    User,
    UserRegistrationRequest,
    UserSession,
)
from loom.system_identities import TASKSET_FENCE_CANARY_TEAM_ID
from loom_service import wire_responses as wire
from loom_service.admin_audit import (
    actor_from_context,
    hash_optional,
    write_admin_audit_event,
)
from loom_service.dependencies import AdminSessionAndCtx, SessionAndCtx
from loom_service.password_auth import (
    hash_password,
    needs_rehash,
    normalize_username,
    validate_password_pair,
    verify_password,
)
from loom_service.public_links import public_base_url
from loom_service.session_auth import (
    accessible_teams,
    consume_login_challenge,
    create_login_challenge,
    create_session_for_user,
    hash_secret,
    refresh_session,
    revoke_session,
    rotate_csrf_token,
    session_cookie_options,
    session_csrf_token,
    switch_session_team,
    verify_session_cookie,
)

router = APIRouter(prefix="/auth", tags=["auth"])
admin_router = APIRouter(prefix="/admin", tags=["auth-admin"])


class _LoginStartReq(BaseModel):
    email: str = Field(min_length=3, max_length=320)

    @field_validator("email", mode="before")
    @classmethod
    def _strip(cls, value: object) -> object:
        return value.strip() if isinstance(value, str) else value

    @field_validator("email")
    @classmethod
    def _require_email_shape(cls, value: str) -> str:
        if "@" not in value or value.startswith("@") or value.endswith("@"):
            raise ValueError("email must look like an email address")
        return value


class _LoginCompleteReq(BaseModel):
    token: str = Field(min_length=16)


class _SwitchTeamReq(BaseModel):
    team_id: UUID


class _UsernamePasswordLoginReq(BaseModel):
    username: str = Field(min_length=2, max_length=64)
    password: str = Field(min_length=1, max_length=1024)

    @field_validator("username", mode="before")
    @classmethod
    def _strip_username(cls, value: object) -> object:
        return value.strip() if isinstance(value, str) else value


class _RegistrationRequestReq(BaseModel):
    username: str = Field(min_length=2, max_length=64)
    team_id: UUID
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("username", mode="before")
    @classmethod
    def _strip_username(cls, value: object) -> object:
        return value.strip() if isinstance(value, str) else value


class _RegistrationApproveReq(BaseModel):
    role: Literal["owner", "member", "viewer"] = "member"


class _RejectReq(BaseModel):
    reason: str | None = Field(default=None, max_length=500)

    @field_validator("reason", mode="before")
    @classmethod
    def _strip_reason(cls, value: object) -> object:
        return value.strip() if isinstance(value, str) else value


class _SetupCompleteReq(BaseModel):
    token: str = Field(min_length=16)
    password: str = Field(min_length=1, max_length=1024)
    confirm_password: str = Field(min_length=1, max_length=1024)


class _PasswordResetRequestReq(BaseModel):
    username: str = Field(min_length=2, max_length=64)

    @field_validator("username", mode="before")
    @classmethod
    def _strip_username(cls, value: object) -> object:
        return value.strip() if isinstance(value, str) else value


class _ResetCompleteReq(BaseModel):
    token: str = Field(min_length=16)
    password: str = Field(min_length=1, max_length=1024)
    confirm_password: str = Field(min_length=1, max_length=1024)


_ACTION_TOKEN_TTL = timedelta(days=14)


def _raw_action_token(purpose: str) -> str:
    return f"loom_{purpose}_{secrets.token_urlsafe(32)}"


def _action_link(request: Request, route: str, raw_token: str) -> str:
    return f"{public_base_url(request)}/auth/{route}?token={raw_token}"


def _token_prefix(raw_token: str) -> str:
    return raw_token[:16]


def _serialize_registration_request(row: UserRegistrationRequest) -> dict[str, Any]:
    return {
        "id": str(row.id),
        "username": row.username,
        "team_id": str(row.team_id),
        "role": row.role,
        "status": row.status,
        "requested_at": row.requested_at.isoformat(),
        "reviewed_at": row.reviewed_at.isoformat() if row.reviewed_at else None,
        "reviewed_by_actor": row.reviewed_by_actor,
        "user_id": str(row.user_id) if row.user_id else None,
        "setup_token_prefix": row.setup_token_prefix,
        "rejection_reason": row.rejection_reason,
    }


def _serialize_password_reset_request(row: PasswordResetRequest) -> dict[str, Any]:
    return {
        "id": str(row.id),
        "username": row.username,
        "status": row.status,
        "requested_at": row.requested_at.isoformat(),
        "reviewed_at": row.reviewed_at.isoformat() if row.reviewed_at else None,
        "reviewed_by_actor": row.reviewed_by_actor,
        "user_id": str(row.user_id) if row.user_id else None,
        "reset_token_prefix": row.reset_token_prefix,
        "rejection_reason": row.rejection_reason,
    }


async def _find_user_by_username(session: AsyncSession, username: str) -> User | None:
    normalized = normalize_username(username)
    return (await session.execute(
        select(User).where(User.username_normalized == normalized),
    )).scalar_one_or_none()


async def _mint_account_action_token(
    session: AsyncSession,
    *,
    purpose: Literal["setup_password", "reset_password"],
    user_id: UUID,
    created_by_user_id: UUID | None,
    registration_request_id: UUID | None = None,
    password_reset_request_id: UUID | None = None,
) -> str:
    raw = _raw_action_token("setup" if purpose == "setup_password" else "reset")
    now = datetime.now(UTC)
    token = AccountActionToken(
        token_hash=hash_secret(raw),
        token_prefix=_token_prefix(raw),
        purpose=purpose,
        user_id=user_id,
        registration_request_id=registration_request_id,
        password_reset_request_id=password_reset_request_id,
        created_by_user_id=created_by_user_id,
        issued_at=now,
        expires_at=now + _ACTION_TOKEN_TTL,
    )
    session.add(token)
    return raw


async def _load_action_token(
    session: AsyncSession,
    *,
    raw_token: str,
    purpose: Literal["setup_password", "reset_password"],
) -> AccountActionToken:
    now = datetime.now(UTC)
    row = (await session.execute(
        select(AccountActionToken)
        .where(
            AccountActionToken.token_hash == hash_secret(raw_token),
            AccountActionToken.purpose == purpose,
            AccountActionToken.consumed_at.is_(None),
            AccountActionToken.revoked_at.is_(None),
        )
        .with_for_update(),
    )).scalar_one_or_none()
    if row is None or row.expires_at < now:
        raise HTTPException(status_code=400, detail="invalid or expired token")
    return row


async def _require_user(session: AsyncSession, ctx: AuthContext) -> User:
    if ctx.type != "user" or ctx.user_id is None:
        raise HTTPException(status_code=403, detail="browser session required")
    user = (await session.execute(
        select(User).where(User.id == ctx.user_id),
    )).scalar_one_or_none()
    if user is None:
        raise HTTPException(status_code=401, detail="missing user")
    return user


async def _serialize_me(
    session: AsyncSession,
    ctx: AuthContext,
    *,
    csrf_token: str | None = None,
) -> dict[str, Any]:
    user = await _require_user(session, ctx)

    teams = [
        {"id": str(team.id), "name": team.name, "role": role}
        for team, role in await accessible_teams(session, user)
    ]

    current_team = None
    if ctx.team_id is not None:
        for team in teams:
            if team["id"] == str(ctx.team_id):
                current_team = team
                break

    payload: dict[str, Any] = {
        "user": {
            "id": str(user.id),
            "username": user.username,
            "email": user.email,
            "display_name": user.display_name,
            "is_platform_admin": user.is_platform_admin,
        },
        "teams": teams,
        "current_team": current_team,
        "role": ctx.role,
        "scopes": list(ctx.scopes),
        "is_platform_admin": user.is_platform_admin,
    }
    if csrf_token is not None:
        payload["csrf_token"] = csrf_token
    return payload


def _credential_type(ctx: AuthContext) -> str:
    if ctx.auth_kind == "session" and ctx.type == "user":
        return "browser_session"
    if ctx.type == "team" and ctx.user_id is not None:
        return "user_owned_api_token"
    if ctx.type == "team" and ctx.team_id is None and "submit:batch" in ctx.scopes:
        return "service_credential"
    if ctx.type == "team":
        return "legacy_team_token"
    if ctx.type == "readonly_probe":
        return "staging_readonly_probe"
    if ctx.type == "admin":
        return "admin_bearer_token"
    if ctx.type == "worker":
        return "worker_token"
    if ctx.type == "step_session":
        return "step_session"
    return f"{ctx.type}_credential"


def team_allows_public_registration(team: Team) -> bool:
    """Shared eligibility for public discovery and registration submission.

    All failures must look identical to clients (404 team not found) so a
    remembered private UUID cannot enumerate policy (#775).
    """
    return (
        bool(team.public_registration_enabled)
        and team.disabled_at is None
        and team.name.lower() != "admin"
        and team.id != TASKSET_FENCE_CANARY_TEAM_ID
    )


@router.get("/public-teams")
async def public_teams(request: Request) -> dict[str, list[dict[str, str]]]:
    # Only Teams with an explicit public-registration opt-in are discoverable.
    # System Teams (``admin`` by name, reserved canary UUID) never qualify.
    async with request.app.state.session_factory() as session:
        rows = (await session.execute(
            select(Team)
            .where(Team.public_registration_enabled.is_(True))
            .where(Team.disabled_at.is_(None))
            .where(func.lower(Team.name) != "admin")
            .where(Team.id != TASKSET_FENCE_CANARY_TEAM_ID)
            .order_by(func.lower(Team.name).asc(), Team.id.asc()),
        )).scalars().all()
    return {"items": [{"id": str(team.id), "name": team.name} for team in rows]}


@router.post("/registration-requests", status_code=202, response_model=wire.UserRegistrationEntry, response_model_exclude_unset=True)
async def request_registration(
    request: Request,
    payload: _RegistrationRequestReq,
) -> dict[str, Any]:
    username_normalized = normalize_username(payload.username)
    async with request.app.state.session_factory() as session:
        # Lock the Team row so a concurrent admin policy change cannot race
        # past a stale client selection.
        team = (await session.execute(
            select(Team)
            .where(Team.id == payload.team_id)
            .with_for_update(),
        )).scalar_one_or_none()
        if team is None or not team_allows_public_registration(team):
            raise HTTPException(status_code=404, detail="team not found")
        existing_user = (await session.execute(
            select(User.id).where(User.username_normalized == username_normalized),
        )).scalar_one_or_none()
        if existing_user is not None:
            raise HTTPException(status_code=409, detail="username is already registered")
        existing_request = (await session.execute(
            select(UserRegistrationRequest.id)
            .where(UserRegistrationRequest.username_normalized == username_normalized)
            .where(UserRegistrationRequest.status.in_(("pending", "approved"))),
        )).scalar_one_or_none()
        if existing_request is not None:
            raise HTTPException(status_code=409, detail="username already has an active request")
        now = datetime.now(UTC)
        registration = UserRegistrationRequest(
            username=payload.username.strip(),
            username_normalized=username_normalized,
            team_id=team.id,
            role="member",
            status="pending",
            requested_at=now,
            source_ip_hash=hash_optional(request.client.host if request.client else None),
            user_agent_hash=hash_optional(request.headers.get("user-agent")),
            request_metadata=payload.metadata,
        )
        session.add(registration)
        try:
            await session.commit()
        except IntegrityError as exc:
            await session.rollback()
            raise HTTPException(status_code=409, detail="username already has an active request") from exc
        return _serialize_registration_request(registration)


@router.get("/setup/lookup")
async def setup_lookup(
    request: Request,
    token: Annotated[str, Query(min_length=16)],
) -> dict[str, Any]:
    async with request.app.state.session_factory() as session:
        action = await _load_action_token(session, raw_token=token, purpose="setup_password")
        user = (await session.execute(
            select(User).where(User.id == action.user_id),
        )).scalar_one()
        registration = None
        team = None
        if action.registration_request_id is not None:
            registration = (await session.execute(
                select(UserRegistrationRequest)
                .where(UserRegistrationRequest.id == action.registration_request_id),
            )).scalar_one_or_none()
        if registration is not None:
            team = (await session.execute(
                select(Team).where(Team.id == registration.team_id),
            )).scalar_one_or_none()
        body = {
            "username": user.username,
            "team": (
                {"id": str(team.id), "name": team.name}
                if team is not None else None
            ),
            "expires_at": action.expires_at.isoformat(),
        }
        await session.rollback()
    return body


@router.post("/setup/complete")
async def setup_complete(
    request: Request,
    payload: _SetupCompleteReq,
) -> dict[str, Any]:
    validate_password_pair(payload.password, payload.confirm_password)
    async with request.app.state.session_factory() as session:
        action = await _load_action_token(
            session, raw_token=payload.token, purpose="setup_password",
        )
        user = (await session.execute(
            select(User).where(User.id == action.user_id).with_for_update(),
        )).scalar_one()
        user.password_hash = hash_password(payload.password)
        user.password_set_at = datetime.now(UTC)
        user.status = "active"
        user.disabled_at = None
        action.consumed_at = datetime.now(UTC)
        await session.execute(
            update(AccountActionToken)
            .where(AccountActionToken.user_id == user.id)
            .where(AccountActionToken.purpose == "setup_password")
            .where(AccountActionToken.consumed_at.is_(None))
            .where(AccountActionToken.token_hash != action.token_hash)
            .values(revoked_at=datetime.now(UTC)),
        )
        await session.commit()
        return {
            "status": "active",
            "user": {
                "id": str(user.id),
                "username": user.username,
            },
        }


@router.post("/login")
async def password_login(
    request: Request,
    response: Response,
    payload: _UsernamePasswordLoginReq,
) -> dict[str, Any]:
    async with request.app.state.session_factory() as session:
        user = await _find_user_by_username(session, payload.username)
        if (
            user is None
            or user.password_hash is None
            or user.status != "active"
            or user.disabled_at is not None
            or not verify_password(payload.password, user.password_hash)
        ):
            raise HTTPException(status_code=401, detail="invalid username or password")
        if needs_rehash(user.password_hash):
            user.password_hash = hash_password(payload.password)
            user.password_set_at = datetime.now(UTC)
        created = await create_session_for_user(
            session,
            user=user,
            session_ttl_seconds=request.app.state.settings.auth_session_ttl_sec,
            audience=request.app.state.settings.session_audience,
        )
        await session.flush()
        body = await _serialize_me(session, created.ctx, csrf_token=created.raw_csrf)
        await session.commit()
    _set_auth_cookies(response, request, raw_session=created.raw_session)
    return body


@admin_router.get("/registration-requests")
async def list_registration_requests(
    sc: AdminSessionAndCtx,
    status: Annotated[str, Query()] = "pending",
) -> dict[str, list[dict[str, Any]]]:
    session, _ctx = sc
    rows = (await session.execute(
        select(UserRegistrationRequest)
        .where(UserRegistrationRequest.status == status)
        .order_by(UserRegistrationRequest.requested_at.asc(), UserRegistrationRequest.id.asc()),
    )).scalars().all()
    return {"items": [_serialize_registration_request(row) for row in rows]}


@admin_router.post("/registration-requests/{registration_id}/approve", response_model=wire.AccountActionApproval, response_model_exclude_unset=True)
async def approve_registration_request(
    request: Request,
    sc: AdminSessionAndCtx,
    registration_id: UUID,
    payload: _RegistrationApproveReq,
    x_loom_admin_actor: str | None = Header(default=None),
) -> dict[str, Any]:
    session, ctx = sc
    actor = await actor_from_context(session, ctx, x_loom_admin_actor)
    registration = (await session.execute(
        select(UserRegistrationRequest)
        .where(UserRegistrationRequest.id == registration_id)
        .with_for_update(),
    )).scalar_one_or_none()
    if registration is None:
        raise HTTPException(status_code=404, detail="registration request not found")
    if registration.status != "pending":
        raise HTTPException(status_code=409, detail=f"registration request is {registration.status}")
    existing_user = (await session.execute(
        select(User).where(User.username_normalized == registration.username_normalized),
    )).scalar_one_or_none()
    if existing_user is not None:
        raise HTTPException(status_code=409, detail="username is already registered")
    team = (await session.execute(
        select(Team).where(Team.id == registration.team_id),
    )).scalar_one_or_none()
    if team is None:
        raise HTTPException(status_code=404, detail="team not found")

    now = datetime.now(UTC)
    user = User(
        email=None,
        username=registration.username,
        username_normalized=registration.username_normalized,
        display_name=registration.username,
        password_hash=None,
        status="pending_setup",
        disabled_at=None,
        is_platform_admin=False,
        created_at=now,
    )
    session.add(user)
    await session.flush()
    session.add(TeamMembership(team_id=team.id, user_id=user.id, role=payload.role))
    raw_token = await _mint_account_action_token(
        session,
        purpose="setup_password",
        user_id=user.id,
        created_by_user_id=ctx.user_id,
        registration_request_id=registration.id,
    )
    registration.status = "approved"
    registration.role = payload.role
    registration.reviewed_at = now
    registration.reviewed_by_user_id = ctx.user_id
    registration.reviewed_by_actor = actor
    registration.user_id = user.id
    registration.setup_token_prefix = _token_prefix(raw_token)
    await write_admin_audit_event(
        session,
        actor=actor,
        action="registration_request.approve",
        target_type="registration_request",
        target_id=str(registration.id),
        request=request,
        metadata={
            "username": registration.username,
            "team_id": str(team.id),
            "team_name": team.name,
            "role": payload.role,
            "setup_token_prefix": registration.setup_token_prefix,
        },
    )
    await session.commit()
    return {
        "registration": _serialize_registration_request(registration),
        "user": {"id": str(user.id), "username": user.username},
        "team": {"id": str(team.id), "name": team.name},
        "setup_token_prefix": registration.setup_token_prefix,
        "setup_link": _action_link(request, "setup", raw_token),
    }


@admin_router.post("/registration-requests/{registration_id}/reject", response_model=wire.UserRegistrationEntry, response_model_exclude_unset=True)
async def reject_registration_request(
    request: Request,
    sc: AdminSessionAndCtx,
    registration_id: UUID,
    payload: _RejectReq | None = None,
    x_loom_admin_actor: str | None = Header(default=None),
) -> dict[str, Any]:
    session, ctx = sc
    actor = await actor_from_context(session, ctx, x_loom_admin_actor)
    registration = (await session.execute(
        select(UserRegistrationRequest)
        .where(UserRegistrationRequest.id == registration_id)
        .with_for_update(),
    )).scalar_one_or_none()
    if registration is None:
        raise HTTPException(status_code=404, detail="registration request not found")
    if registration.status != "pending":
        raise HTTPException(status_code=409, detail=f"registration request is {registration.status}")
    registration.status = "rejected"
    registration.reviewed_at = datetime.now(UTC)
    registration.reviewed_by_user_id = ctx.user_id
    registration.reviewed_by_actor = actor
    registration.rejection_reason = payload.reason if payload else None
    await write_admin_audit_event(
        session,
        actor=actor,
        action="registration_request.reject",
        target_type="registration_request",
        target_id=str(registration.id),
        request=request,
        metadata={
            "username": registration.username,
            "reason_present": bool(registration.rejection_reason),
        },
    )
    await session.commit()
    return _serialize_registration_request(registration)


@router.post("/password-reset-requests", status_code=202)
async def request_password_reset(
    request: Request,
    payload: _PasswordResetRequestReq,
) -> dict[str, str]:
    username_normalized = normalize_username(payload.username)
    async with request.app.state.session_factory() as session:
        user = (await session.execute(
            select(User).where(User.username_normalized == username_normalized),
        )).scalar_one_or_none()
        if user is not None:
            active = (await session.execute(
                select(PasswordResetRequest.id)
                .where(PasswordResetRequest.username_normalized == username_normalized)
                .where(PasswordResetRequest.status.in_(("pending", "approved"))),
            )).scalar_one_or_none()
            if active is None:
                reset_request = PasswordResetRequest(
                    username=user.username,
                    username_normalized=user.username_normalized,
                    user_id=user.id,
                    status="pending",
                    requested_at=datetime.now(UTC),
                    source_ip_hash=hash_optional(request.client.host if request.client else None),
                    user_agent_hash=hash_optional(request.headers.get("user-agent")),
                )
                session.add(reset_request)
                await session.commit()
    return {"status": "pending"}


@admin_router.get("/password-reset-requests")
async def list_password_reset_requests(
    sc: AdminSessionAndCtx,
    status: Annotated[str, Query()] = "pending",
) -> dict[str, list[dict[str, Any]]]:
    session, _ctx = sc
    rows = (await session.execute(
        select(PasswordResetRequest)
        .where(PasswordResetRequest.status == status)
        .order_by(PasswordResetRequest.requested_at.asc(), PasswordResetRequest.id.asc()),
    )).scalars().all()
    return {"items": [_serialize_password_reset_request(row) for row in rows]}


@admin_router.post("/password-reset-requests/{reset_request_id}/approve", response_model=wire.AccountActionApproval, response_model_exclude_unset=True)
async def approve_password_reset_request(
    request: Request,
    sc: AdminSessionAndCtx,
    reset_request_id: UUID,
    x_loom_admin_actor: str | None = Header(default=None),
) -> dict[str, Any]:
    session, ctx = sc
    actor = await actor_from_context(session, ctx, x_loom_admin_actor)
    reset_request = (await session.execute(
        select(PasswordResetRequest)
        .where(PasswordResetRequest.id == reset_request_id)
        .with_for_update(),
    )).scalar_one_or_none()
    if reset_request is None:
        raise HTTPException(status_code=404, detail="password reset request not found")
    if reset_request.status != "pending":
        raise HTTPException(status_code=409, detail=f"password reset request is {reset_request.status}")
    if reset_request.user_id is None:
        raise HTTPException(status_code=404, detail="user not found")
    user = (await session.execute(
        select(User).where(User.id == reset_request.user_id),
    )).scalar_one_or_none()
    if user is None:
        raise HTTPException(status_code=404, detail="user not found")
    raw_token = await _mint_account_action_token(
        session,
        purpose="reset_password",
        user_id=user.id,
        created_by_user_id=ctx.user_id,
        password_reset_request_id=reset_request.id,
    )
    reset_request.status = "approved"
    reset_request.reviewed_at = datetime.now(UTC)
    reset_request.reviewed_by_user_id = ctx.user_id
    reset_request.reviewed_by_actor = actor
    reset_request.reset_token_prefix = _token_prefix(raw_token)
    await write_admin_audit_event(
        session,
        actor=actor,
        action="password_reset_request.approve",
        target_type="password_reset_request",
        target_id=str(reset_request.id),
        request=request,
        metadata={
            "username": user.username,
            "reset_token_prefix": reset_request.reset_token_prefix,
        },
    )
    await session.commit()
    return {
        "request": _serialize_password_reset_request(reset_request),
        "user": {"id": str(user.id), "username": user.username},
        "reset_token_prefix": reset_request.reset_token_prefix,
        "reset_link": _action_link(request, "reset", raw_token),
    }


@admin_router.post("/password-reset-requests/{reset_request_id}/reject", response_model=wire.PasswordResetRequestEntry, response_model_exclude_unset=True)
async def reject_password_reset_request(
    request: Request,
    sc: AdminSessionAndCtx,
    reset_request_id: UUID,
    payload: _RejectReq | None = None,
    x_loom_admin_actor: str | None = Header(default=None),
) -> dict[str, Any]:
    session, ctx = sc
    actor = await actor_from_context(session, ctx, x_loom_admin_actor)
    reset_request = (await session.execute(
        select(PasswordResetRequest)
        .where(PasswordResetRequest.id == reset_request_id)
        .with_for_update(),
    )).scalar_one_or_none()
    if reset_request is None:
        raise HTTPException(status_code=404, detail="password reset request not found")
    if reset_request.status != "pending":
        raise HTTPException(status_code=409, detail=f"password reset request is {reset_request.status}")
    reset_request.status = "rejected"
    reset_request.reviewed_at = datetime.now(UTC)
    reset_request.reviewed_by_user_id = ctx.user_id
    reset_request.reviewed_by_actor = actor
    reset_request.rejection_reason = payload.reason if payload else None
    await write_admin_audit_event(
        session,
        actor=actor,
        action="password_reset_request.reject",
        target_type="password_reset_request",
        target_id=str(reset_request.id),
        request=request,
        metadata={
            "username": reset_request.username,
            "reason_present": bool(reset_request.rejection_reason),
        },
    )
    await session.commit()
    return _serialize_password_reset_request(reset_request)


@router.get("/reset/lookup")
async def reset_lookup(
    request: Request,
    token: Annotated[str, Query(min_length=16)],
) -> dict[str, Any]:
    async with request.app.state.session_factory() as session:
        action = await _load_action_token(session, raw_token=token, purpose="reset_password")
        user = (await session.execute(
            select(User).where(User.id == action.user_id),
        )).scalar_one()
        body = {
            "username": user.username,
            "expires_at": action.expires_at.isoformat(),
        }
        await session.rollback()
    return body


@router.post("/reset/complete")
async def reset_complete(
    request: Request,
    payload: _ResetCompleteReq,
) -> dict[str, Any]:
    validate_password_pair(payload.password, payload.confirm_password)
    async with request.app.state.session_factory() as session:
        action = await _load_action_token(
            session, raw_token=payload.token, purpose="reset_password",
        )
        user = (await session.execute(
            select(User).where(User.id == action.user_id).with_for_update(),
        )).scalar_one()
        user.password_hash = hash_password(payload.password)
        user.password_set_at = datetime.now(UTC)
        user.status = "active"
        user.disabled_at = None
        action.consumed_at = datetime.now(UTC)
        await session.execute(
            update(UserSession)
            .where(UserSession.user_id == user.id)
            .where(UserSession.revoked_at.is_(None))
            .values(revoked_at=datetime.now(UTC)),
        )
        await session.execute(
            update(Token)
            .where(Token.created_by_user_id == user.id)
            .where(Token.revoked_at.is_(None))
            .values(revoked_at=datetime.now(UTC)),
        )
        await session.execute(
            update(AccountActionToken)
            .where(AccountActionToken.user_id == user.id)
            .where(AccountActionToken.purpose == "reset_password")
            .where(AccountActionToken.consumed_at.is_(None))
            .where(AccountActionToken.token_hash != action.token_hash)
            .values(revoked_at=datetime.now(UTC)),
        )
        await session.commit()
    return {
        "status": "active",
        "user": {
            "id": str(user.id),
            "username": user.username,
        },
    }


def _set_auth_cookies(
    response: Response, request: Request, *, raw_session: str,
) -> None:
    settings = request.app.state.settings
    response.set_cookie(
        value=raw_session, **session_cookie_options(settings),
    )


def _clear_auth_cookies(response: Response, request: Request) -> None:
    settings = request.app.state.settings
    response.delete_cookie(
        settings.session_cookie_name, path="/", secure=settings.hosted_session_cookie,
        httponly=True, samesite="lax",
    )
    response.delete_cookie(settings.auth_csrf_cookie_name, path="/")


@router.post("/login/start")
async def login_start(
    request: Request, payload: _LoginStartReq,
) -> dict[str, Any]:
    settings = request.app.state.settings
    async with request.app.state.session_factory() as session:
        raw = await create_login_challenge(
            session,
            email=payload.email,
            ttl_seconds=settings.auth_login_challenge_ttl_sec,
            audience=settings.session_audience,
        )
        await session.commit()
    body: dict[str, Any] = {"status": "sent"}
    if settings.auth_return_login_token and raw is not None:
        body["login_token"] = raw
    return body


@router.post("/login/complete")
async def login_complete(
    request: Request, response: Response, payload: _LoginCompleteReq,
) -> dict[str, Any]:
    settings = request.app.state.settings
    async with request.app.state.session_factory() as session:
        created = await consume_login_challenge(
            session,
            raw_token=payload.token,
            session_ttl_seconds=settings.auth_session_ttl_sec,
            audience=settings.session_audience,
        )
        await session.flush()
        body = await _serialize_me(
            session, created.ctx, csrf_token=created.raw_csrf,
        )
        await session.commit()
    _set_auth_cookies(
        response, request,
        raw_session=created.raw_session,
    )
    return body


@router.get("/me")
async def me(request: Request, sc: SessionAndCtx) -> dict[str, Any]:
    session, ctx = sc
    csrf_token = session_csrf_token(
        ctx, request.cookies.get(request.app.state.settings.session_cookie_name),
    )
    await session.commit()
    return await _serialize_me(session, ctx, csrf_token=csrf_token)


@router.get("/whoami")
async def whoami(sc: SessionAndCtx) -> dict[str, Any]:
    session, ctx = sc
    team_name = None
    if ctx.team_id is not None:
        team = (await session.execute(
            select(Team).where(Team.id == ctx.team_id),
        )).scalar_one_or_none()
        team_name = team.name if team is not None else None

    username = None
    if ctx.user_id is not None:
        user = (await session.execute(
            select(User).where(User.id == ctx.user_id),
        )).scalar_one_or_none()
        username = user.username if user is not None else None

    await session.commit()
    result = {
        "auth_kind": ctx.auth_kind,
        "credential_type": _credential_type(ctx),
        "principal_type": ctx.type,
        "user_id": str(ctx.user_id) if ctx.user_id else None,
        "username": username,
        "team_id": str(ctx.team_id) if ctx.team_id else None,
        "team_name": team_name,
        "role": ctx.role,
        "scopes": list(ctx.scopes),
        "token_prefix": (
            ctx.token_hash.hex()[:8] if ctx.token_hash else None
        ),
        "expires_at": (
            ctx.expires_at.isoformat() if ctx.expires_at else None
        ),
    }
    if ctx.type == "readonly_probe":
        result["allowed_http_methods"] = ["GET", "HEAD"]
        result["readonly_authority_version"] = "v1"
    return result


@router.post("/team")
async def switch_team(
    request: Request, payload: _SwitchTeamReq, sc: SessionAndCtx,
) -> dict[str, Any]:
    session, ctx = sc
    await switch_session_team(session, ctx=ctx, team_id=payload.team_id)
    await session.flush()
    refreshed = await verify_session_cookie(
        session, request.cookies.get(request.app.state.settings.session_cookie_name),
        audience=request.app.state.settings.session_audience,
    )
    if refreshed is None:
        raise HTTPException(status_code=401, detail="missing or invalid session")
    csrf_token = await rotate_csrf_token(session, refreshed)
    await session.commit()
    return await _serialize_me(session, refreshed, csrf_token=csrf_token)


@router.post("/refresh")
async def refresh(
    request: Request, response: Response, sc: SessionAndCtx,
) -> dict[str, Any]:
    session, ctx = sc
    settings = request.app.state.settings
    refreshed_tokens = await refresh_session(
        session, ctx=ctx, session_ttl_seconds=settings.auth_session_ttl_sec,
        audience=settings.session_audience,
    )
    await session.commit()
    _set_auth_cookies(
        response, request, raw_session=refreshed_tokens.raw_session,
    )
    refreshed = await verify_session_cookie(
        session, refreshed_tokens.raw_session, audience=settings.session_audience,
    )
    if refreshed is None:
        raise HTTPException(status_code=401, detail="missing or invalid session")
    return await _serialize_me(
        session, refreshed, csrf_token=refreshed_tokens.raw_csrf,
    )


@router.post("/logout", status_code=204)
async def logout(
    request: Request, response: Response, sc: SessionAndCtx,
) -> None:
    session, ctx = sc
    await revoke_session(session, ctx)
    await session.commit()
    _clear_auth_cookies(response, request)
