"""Invite link onboarding routes for issue #327."""

from __future__ import annotations

import secrets
from datetime import UTC, datetime, timedelta
from typing import Annotated, Any, Literal
from uuid import UUID, uuid4

from fastapi import APIRouter, Header, HTTPException, Query, Request, Response
from pydantic import BaseModel, Field, field_validator
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.auth import AuthContext
from loom.db.schema import Team, TeamInvite, TeamMembership, User
from loom.system_identities import TASKSET_FENCE_CANARY_TEAM_ID
from loom_service import wire_responses as wire
from loom_service.admin_audit import require_admin_actor, write_admin_audit_event
from loom_service.auth_guards import is_admin, require_scope
from loom_service.dependencies import SessionAndCtx
from loom_service.metrics import INVITES_TOTAL
from loom_service.password_auth import normalize_username
from loom_service.public_links import public_base_url
from loom_service.routes.auth import _serialize_me, _set_auth_cookies
from loom_service.session_auth import (
    create_session_for_user,
    hash_secret,
    normalize_email,
    verify_session_cookie,
)

router = APIRouter(prefix="/invites", tags=["invites"])

_INVITE_ROLES = Literal["owner", "member", "viewer"]
_INVITE_STATUSES = Literal["pending", "accepted", "revoked", "expired"]
_INVITE_REJECTION = Literal[
    "sign-in-required", "email-mismatch", "email-required", "invalid",
    "already-used", "expired", "unavailable",
]


def _reject_acceptance(status: int, detail: str, reason: _INVITE_REJECTION) -> HTTPException:
    # Only definitive rejections before session creation carry this marker.
    # The browser can preserve its session without consuming an error body.
    return HTTPException(
        status_code=status, detail=detail, headers={"X-Loom-Invite-Rejection": reason},
    )


class _CreateInviteReq(BaseModel):
    email: str = Field(min_length=3, max_length=320)
    team_id: UUID | None = None
    role: _INVITE_ROLES = "member"
    expires_in_days: int = Field(default=7, gt=0, le=365)
    max_uses: int | None = Field(default=1, ge=1, le=100)
    allowed_domain: str | None = Field(default=None, min_length=1, max_length=255)

    @field_validator("email", mode="before")
    @classmethod
    def _strip_email(cls, value: object) -> object:
        return value.strip() if isinstance(value, str) else value

    @field_validator("email")
    @classmethod
    def _normalize_email(cls, value: str) -> str:
        if "@" not in value or value.startswith("@") or value.endswith("@"):
            raise ValueError("email must look like an email address")
        return normalize_email(value)

    @field_validator("allowed_domain", mode="before")
    @classmethod
    def _strip_domain(cls, value: object) -> object:
        return value.strip().lower() if isinstance(value, str) else value

    @field_validator("allowed_domain")
    @classmethod
    def _validate_domain(cls, value: str | None) -> str | None:
        if value is None:
            return None
        if "@" in value or "/" in value or " " in value or value.startswith("."):
            raise ValueError("allowed_domain must be a bare DNS domain")
        return value


class _RevokeInviteReq(BaseModel):
    reason: str | None = Field(default=None, max_length=500)

    @field_validator("reason", mode="before")
    @classmethod
    def _strip_reason(cls, value: object) -> object:
        return value.strip() if isinstance(value, str) else value


class _ResendInviteReq(BaseModel):
    expires_in_days: int = Field(default=7, gt=0, le=365)


class _AcceptInviteReq(BaseModel):
    code: str = Field(min_length=16, max_length=256)
    email: str | None = Field(default=None, min_length=3, max_length=320)

    @field_validator("code", "email", mode="before")
    @classmethod
    def _strip(cls, value: object) -> object:
        return value.strip() if isinstance(value, str) else value

    @field_validator("email")
    @classmethod
    def _normalize_email(cls, value: str | None) -> str | None:
        if value is None:
            return None
        if "@" not in value or value.startswith("@") or value.endswith("@"):
            raise ValueError("email must look like an email address")
        return normalize_email(value)


def _mint_invite_code() -> tuple[str, bytes, str]:
    raw = f"loom_invite_{secrets.token_urlsafe(32)}"
    code_hash = hash_secret(raw)
    return raw, code_hash, code_hash.hex()[:8]


def _invite_link(request: Request, raw_code: str) -> str:
    return f"{public_base_url(request)}/invites/accept?code={raw_code}"


def _display_status(invite: TeamInvite, now: datetime | None = None) -> str:
    current = now or datetime.now(UTC)
    if invite.status == "pending" and invite.expires_at < current:
        return "expired"
    return invite.status


def _serialize_invite(
    invite: TeamInvite,
    *,
    team: Team | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    return {
        "id": str(invite.id),
        "team_id": str(invite.team_id),
        "team_name": team.name if team else None,
        "email": invite.email,
        "allowed_domain": invite.allowed_domain,
        "role": invite.role,
        "status": _display_status(invite, now),
        "code_prefix": invite.code_prefix,
        "max_uses": invite.max_uses,
        "accepted_uses": invite.accepted_uses,
        "created_by_actor": invite.created_by_actor,
        "created_at": invite.created_at.isoformat(),
        "expires_at": invite.expires_at.isoformat(),
        "last_sent_at": invite.last_sent_at.isoformat() if invite.last_sent_at else None,
        "accepted_at": invite.accepted_at.isoformat() if invite.accepted_at else None,
        "revoked_at": invite.revoked_at.isoformat() if invite.revoked_at else None,
    }


async def _load_ctx_user(session: AsyncSession, ctx: AuthContext) -> User | None:
    if ctx.user_id is None:
        return None
    return (await session.execute(
        select(User).where(User.id == ctx.user_id),
    )).scalar_one_or_none()


async def _actor_for_team_management(
    session: AsyncSession,
    *,
    ctx: AuthContext,
    team_id: UUID,
    x_loom_admin_actor: str | None,
) -> str:
    if ctx.type == "admin":
        return require_admin_actor(x_loom_admin_actor)

    if is_admin(ctx):
        user = await _load_ctx_user(session, ctx)
        return f"user:{user.email}" if user else "platform_admin"

    require_scope(ctx, "team:manage")
    if ctx.team_id != team_id:
        raise HTTPException(status_code=403, detail="cannot manage another team")
    user = await _load_ctx_user(session, ctx)
    return f"user:{user.email}" if user else "user"


async def _require_visible_team_invite(
    session: AsyncSession,
    *,
    invite_id: UUID,
) -> tuple[TeamInvite, Team]:
    row = (await session.execute(
        select(TeamInvite, Team)
        .join(Team, Team.id == TeamInvite.team_id)
        .where(TeamInvite.id == invite_id)
        .with_for_update(),
    )).first()
    if row is None:
        raise HTTPException(status_code=404, detail="invite not found")
    invite, team = row
    return invite, team


async def _find_user_by_email(session: AsyncSession, email: str) -> User | None:
    return (await session.execute(
        select(User).where(func.lower(User.email) == normalize_email(email)),
    )).scalar_one_or_none()


def _legacy_username_candidate(email: str) -> str:
    local = email.split("@", 1)[0]
    candidate = "".join(
        ch if ch.isalnum() or ch in "_.-" else "."
        for ch in local
    ).strip("._-")
    if not candidate:
        candidate = "user"
    if len(candidate) == 1:
        candidate = f"{candidate}1"
    return candidate[:64]


async def _unique_legacy_username(session: AsyncSession, email: str) -> str:
    base = _legacy_username_candidate(email)
    for idx in range(1000):
        candidate = base if idx == 0 else f"{base[: max(1, 64 - len(str(idx)) - 1)]}-{idx}"
        normalized = normalize_username(candidate)
        exists = (await session.execute(
            select(User.id).where(User.username_normalized == normalized),
        )).scalar_one_or_none()
        if exists is None:
            return candidate
    raise HTTPException(status_code=409, detail="could not allocate username")


def _email_matches_invite(invite: TeamInvite, email: str) -> bool:
    normalized = normalize_email(email)
    if normalized == invite.email:
        return True
    if invite.allowed_domain:
        domain = normalized.rsplit("@", 1)[-1]
        return domain == invite.allowed_domain
    return False


@router.post("", status_code=201, response_model=wire.InviteReveal, response_model_exclude_unset=True)
async def create_invite(
    request: Request,
    payload: _CreateInviteReq,
    sc: SessionAndCtx,
    x_loom_admin_actor: str | None = Header(default=None),
) -> dict[str, Any]:
    session, ctx = sc
    team_id = payload.team_id or ctx.team_id
    if team_id is None:
        raise HTTPException(status_code=400, detail="team_id required")

    team = (await session.execute(
        select(Team).where(Team.id == team_id),
    )).scalar_one_or_none()
    if team is None:
        raise HTTPException(status_code=404, detail="team not found")
    if team.id == TASKSET_FENCE_CANARY_TEAM_ID:
        raise HTTPException(status_code=403, detail="team is deployment-only")

    actor = await _actor_for_team_management(
        session,
        ctx=ctx,
        team_id=team_id,
        x_loom_admin_actor=x_loom_admin_actor,
    )
    raw_code, code_hash, code_prefix = _mint_invite_code()
    now = datetime.now(UTC)
    invite = TeamInvite(
        id=uuid4(),
        team_id=team_id,
        email=payload.email,
        allowed_domain=payload.allowed_domain,
        role=payload.role,
        status="pending",
        code_hash=code_hash,
        code_prefix=code_prefix,
        max_uses=payload.max_uses,
        accepted_uses=0,
        created_by_actor=actor,
        created_by_user_id=ctx.user_id if ctx.type == "user" else None,
        created_at=now,
        expires_at=now + timedelta(days=payload.expires_in_days),
        last_sent_at=now,
    )
    session.add(invite)
    await session.flush()
    await write_admin_audit_event(
        session,
        actor=actor,
        action="invite.create",
        target_type="invite",
        target_id=str(invite.id),
        request=request,
        metadata={
            "team_id": str(team_id),
            "role": invite.role,
            "invite_prefix": invite.code_prefix,
            "max_uses": invite.max_uses,
            "allowed_domain": invite.allowed_domain,
        },
    )
    await session.commit()
    INVITES_TOTAL.labels(action="create", result="success").inc()

    return {
        "invite": _serialize_invite(invite, team=team, now=now),
        "invite_code": raw_code,
        "invite_link": _invite_link(request, raw_code),
    }


@router.get("")
async def list_invites(
    sc: SessionAndCtx,
    team_id: Annotated[UUID | None, Query()] = None,
    status: Annotated[_INVITE_STATUSES | None, Query()] = None,
) -> dict[str, list[dict[str, Any]]]:
    session, ctx = sc
    target_team_id = team_id or ctx.team_id
    if not is_admin(ctx):
        if target_team_id is None:
            raise HTTPException(status_code=400, detail="team_id required")
        require_scope(ctx, "team:manage")
        if ctx.team_id != target_team_id:
            raise HTTPException(status_code=403, detail="cannot view another team")

    stmt = (
        select(TeamInvite, Team)
        .join(Team, Team.id == TeamInvite.team_id)
        .order_by(TeamInvite.created_at.desc(), TeamInvite.id.desc())
    )
    if target_team_id is not None:
        stmt = stmt.where(TeamInvite.team_id == target_team_id)
    if status is not None and status != "expired":
        stmt = stmt.where(TeamInvite.status == status)

    rows = (await session.execute(stmt)).all()
    now = datetime.now(UTC)
    items = [
        _serialize_invite(invite, team=team, now=now)
        for invite, team in rows
        if status is None or _display_status(invite, now) == status
    ]
    return {"items": items}


@router.get("/lookup", response_model=wire.InviteLookup, response_model_exclude_unset=True)
async def lookup_invite(
    request: Request,
    code: str = Query(min_length=16, max_length=256),
) -> dict[str, Any]:
    async with request.app.state.session_factory() as session:
        row = (await session.execute(
            select(TeamInvite, Team)
            .join(Team, Team.id == TeamInvite.team_id)
            .where(TeamInvite.code_hash == hash_secret(code)),
        )).first()
        if row is None:
            raise HTTPException(status_code=400, detail="invalid invite")
        invite, team = row
        status = _display_status(invite)
        return {
            "team_name": team.name,
            "role": invite.role,
            "status": status,
            "code_prefix": invite.code_prefix,
        }


@router.post("/accept")
async def accept_invite(
    request: Request,
    response: Response,
    payload: _AcceptInviteReq,
) -> dict[str, Any]:
    settings = request.app.state.settings
    async with request.app.state.session_factory() as session:
        ctx = await verify_session_cookie(
            session,
            request.cookies.get(settings.session_cookie_name),
            audience=settings.session_audience,
        )
        session_user = None
        if ctx is not None and ctx.type == "user":
            session_user = await _load_ctx_user(session, ctx)

        row = (await session.execute(
            select(TeamInvite, Team)
            .join(Team, Team.id == TeamInvite.team_id)
            .where(TeamInvite.code_hash == hash_secret(payload.code))
            .with_for_update(),
        )).first()
        if row is None:
            raise _reject_acceptance(400, "invalid invite", "invalid")
        invite, _team = row

        now = datetime.now(UTC)
        if invite.status == "revoked":
            raise _reject_acceptance(410, "invite unavailable", "unavailable")
        if invite.status == "accepted" or (
            invite.max_uses is not None and invite.accepted_uses >= invite.max_uses
        ):
            raise _reject_acceptance(409, "invite already accepted", "already-used")
        if invite.expires_at < now:
            invite.status = "expired"
            await session.commit()
            raise _reject_acceptance(410, "invite expired", "expired")

        session_email = (
            normalize_email(session_user.email)
            if session_user is not None and session_user.email is not None
            else None
        )
        accepted_email = session_email if session_user is not None else payload.email
        if accepted_email is None:
            raise _reject_acceptance(400, "email required", "email-required")
        if session_user is not None and payload.email is not None:
            if session_email is None or normalize_email(payload.email) != session_email:
                raise _reject_acceptance(403, "invite is not valid for this user", "email-mismatch")
        if not _email_matches_invite(invite, accepted_email):
            raise _reject_acceptance(403, "invite is not valid for this user", "email-mismatch")

        user = session_user or await _find_user_by_email(session, accepted_email)
        if session_user is None and user is not None:
            # Invite issuers know the code. It authorizes team membership,
            # never authentication as an existing recipient account.
            raise _reject_acceptance(403, "sign in before accepting this invite", "sign-in-required")
        if user is None:
            username = await _unique_legacy_username(session, accepted_email)
            user = User(
                id=uuid4(),
                email=accepted_email,
                username=username,
                username_normalized=normalize_username(username),
                display_name=None,
                status="active",
                is_platform_admin=False,
                created_at=now,
            )
            session.add(user)
            await session.flush()

        membership = (await session.execute(
            select(TeamMembership).where(
                TeamMembership.team_id == invite.team_id,
                TeamMembership.user_id == user.id,
            ),
        )).scalar_one_or_none()
        if membership is None:
            session.add(TeamMembership(
                team_id=invite.team_id,
                user_id=user.id,
                role=invite.role,
                created_at=now,
            ))

        invite.accepted_uses += 1
        invite.accepted_at = now
        if invite.max_uses is not None and invite.accepted_uses >= invite.max_uses:
            invite.status = "accepted"
        await write_admin_audit_event(
            session,
            actor=f"user:{accepted_email}",
            action="invite.accept",
            target_type="invite",
            target_id=str(invite.id),
            request=request,
            metadata={
                "team_id": str(invite.team_id),
                "role": invite.role,
                "invite_prefix": invite.code_prefix,
                "accepted_email": accepted_email,
            },
        )
        created_session = await create_session_for_user(
            session,
            user=user,
            session_ttl_seconds=settings.auth_session_ttl_sec,
            current_team_id=invite.team_id,
            audience=settings.session_audience,
        )
        await session.flush()
        body = await _serialize_me(
            session,
            created_session.ctx,
            csrf_token=created_session.raw_csrf,
        )
        await session.commit()
        INVITES_TOTAL.labels(action="accept", result="success").inc()

    _set_auth_cookies(response, request, raw_session=created_session.raw_session)
    return body


@router.post("/{invite_id}/revoke", response_model=wire.InviteEntry, response_model_exclude_unset=True)
async def revoke_invite(
    request: Request,
    invite_id: UUID,
    payload: _RevokeInviteReq | None,
    sc: SessionAndCtx,
    x_loom_admin_actor: str | None = Header(default=None),
) -> dict[str, Any]:
    session, ctx = sc
    invite, team = await _require_visible_team_invite(session, invite_id=invite_id)
    actor = await _actor_for_team_management(
        session,
        ctx=ctx,
        team_id=invite.team_id,
        x_loom_admin_actor=x_loom_admin_actor,
    )
    if invite.status not in {"pending", "expired"}:
        raise HTTPException(status_code=409, detail=f"invite is {invite.status}")
    now = datetime.now(UTC)
    invite.status = "revoked"
    invite.revoked_at = now
    invite.revoked_reason = payload.reason if payload else None
    await write_admin_audit_event(
        session,
        actor=actor,
        action="invite.revoke",
        target_type="invite",
        target_id=str(invite.id),
        request=request,
        metadata={
            "team_id": str(invite.team_id),
            "role": invite.role,
            "invite_prefix": invite.code_prefix,
            "reason_present": bool(payload and payload.reason),
        },
    )
    await session.commit()
    INVITES_TOTAL.labels(action="revoke", result="success").inc()
    return _serialize_invite(invite, team=team, now=now)


@router.post("/{invite_id}/resend", response_model=wire.InviteReveal, response_model_exclude_unset=True)
async def resend_invite(
    request: Request,
    invite_id: UUID,
    sc: SessionAndCtx,
    payload: _ResendInviteReq | None = None,
    x_loom_admin_actor: str | None = Header(default=None),
) -> dict[str, Any]:
    session, ctx = sc
    invite, team = await _require_visible_team_invite(session, invite_id=invite_id)
    actor = await _actor_for_team_management(
        session,
        ctx=ctx,
        team_id=invite.team_id,
        x_loom_admin_actor=x_loom_admin_actor,
    )
    if invite.status == "accepted":
        raise HTTPException(status_code=409, detail="invite already accepted")
    if invite.status == "revoked":
        raise HTTPException(status_code=409, detail="invite revoked")

    raw_code, code_hash, code_prefix = _mint_invite_code()
    now = datetime.now(UTC)
    resend_payload = payload or _ResendInviteReq()
    invite.code_hash = code_hash
    invite.code_prefix = code_prefix
    invite.status = "pending"
    invite.expires_at = now + timedelta(days=resend_payload.expires_in_days)
    invite.last_sent_at = now
    await write_admin_audit_event(
        session,
        actor=actor,
        action="invite.resend",
        target_type="invite",
        target_id=str(invite.id),
        request=request,
        metadata={
            "team_id": str(invite.team_id),
            "role": invite.role,
            "invite_prefix": invite.code_prefix,
        },
    )
    await session.commit()
    INVITES_TOTAL.labels(action="resend", result="success").inc()
    return {
        "invite": _serialize_invite(invite, team=team, now=now),
        "invite_code": raw_code,
        "invite_link": _invite_link(request, raw_code),
    }
