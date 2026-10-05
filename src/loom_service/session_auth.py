"""Browser session authentication helpers for loom_service (#326)."""

from __future__ import annotations

import hashlib
import hmac
import json
import re
import secrets
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Literal, TypedDict
from urllib.parse import SplitResult, urlsplit
from uuid import UUID

from fastapi import HTTPException, Request
from sqlalchemy import func, insert, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from loom.application_session import ApplicationSessionAudienceV1
from loom.auth import AuthContext, role_scopes
from loom.db.schema import LoginChallenge, Team, TeamMembership, User, UserSession
from loom_service.config import LoomServiceSettings
from loom_service.public_links import configured_public_base_url

_SAFE_HTTP_METHODS = frozenset({"GET", "HEAD", "OPTIONS"})


@dataclass(frozen=True)
class CreatedSession:
    raw_session: str
    raw_csrf: str
    ctx: AuthContext


@dataclass(frozen=True)
class RefreshedSession:
    raw_session: str
    raw_csrf: str


class CookieOptions(TypedDict):
    key: str
    httponly: bool
    secure: bool
    samesite: Literal["lax", "strict", "none"]
    max_age: int
    path: str


def hash_secret(raw: str) -> bytes:
    """Hash raw session, CSRF, and login challenge secrets for storage."""
    return hashlib.sha256(raw.encode()).digest()


def hash_browser_secret(
    raw: str, *, audience: ApplicationSessionAudienceV1 | None,
    purpose: Literal["session", "login_challenge"],
) -> bytes:
    """Bind a proof to protected process identity without a legacy fallback.

    The non-UTF-8 prefix is intentional: a scoped preimage cannot be submitted
    as a raw string to a legacy endpoint to obtain the same unscoped hash.
    This changes neither API bearer tokens nor the per-session CSRF proof.
    """
    if audience is None:
        return hash_secret(raw)
    encoded = json.dumps([
        audience.schema_version, str(audience.application_id), audience.origin,
        audience.access_generation, purpose, raw,
    ], separators=(",", ":")).encode()
    return hashlib.sha256(b"\xffloom-browser-audience\x00" + encoded).digest()


def normalize_email(email: str) -> str:
    return email.strip().lower()


def _raw_secret(prefix: str) -> str:
    return f"{prefix}_{secrets.token_urlsafe(32)}"


def session_cookie_options(settings: LoomServiceSettings) -> CookieOptions:
    """Use the ordinary session lifetime and host-cookie security policy."""
    return {
        "key": settings.session_cookie_name,
        "httponly": True,
        "secure": settings.hosted_session_cookie,
        "samesite": "lax",
        "max_age": settings.auth_session_ttl_sec,
        "path": "/",
    }


def browser_origin_allowed(request: Request, settings: LoomServiceSettings) -> bool:
    """Reject cross-origin browser writes, including unauthenticated login.

    SameSite does not isolate sibling subdomains. Do not trust forwarded Host
    headers as the public origin. Non-browser clients without Origin/Fetch
    Metadata still require the normal authentication and session CSRF token.
    """
    if not settings.hosted_session_cookie or request.method.upper() in _SAFE_HTTP_METHODS:
        return True
    origin = request.headers.get("origin")
    if origin is None:
        return request.headers.get("sec-fetch-site", "none") in {"none", "same-origin"}
    if origin.strip() != origin or any(ord(char) < 32 for char in origin):
        return False
    try:
        supplied = urlsplit(origin)
        expected = urlsplit(configured_public_base_url(settings.public_base_url) or str(request.base_url))
        if (supplied.scheme not in {"http", "https"} or not supplied.hostname
                or supplied.username is not None or supplied.password is not None
                or supplied.path or supplied.query or supplied.fragment):
            return False
        def identity(url: SplitResult) -> tuple[str, str | None, int]:
            port = url.port if url.port is not None else (443 if url.scheme == "https" else 80)
            return url.scheme, url.hostname, port
        return identity(supplied) == identity(expected)
    except ValueError:
        return False


def session_csrf_token(ctx: AuthContext, raw_session: str | None) -> str:
    """Return a stable read proof bound to the cookie and current CSRF nonce.

    Hashes stored in the database cannot mint this proof: its HMAC key is the
    actual HttpOnly cookie. Team switches and refreshes rotate the nonce.
    """
    if ctx.session_hash is None or ctx.csrf_hash is None or not raw_session:
        raise HTTPException(status_code=401, detail="missing browser session")
    digest = hmac.new(
        raw_session.encode(), b"loom-session-csrf-v1\x00" + ctx.csrf_hash, hashlib.sha256,
    ).hexdigest()
    return f"loom_csrf_{digest}"


def verify_csrf(
    ctx: AuthContext, header_value: str | None, *, raw_session: str | None = None,
) -> None:
    """Require a matching CSRF header for browser-session mutations.

    Bearer-token callers remain exempt because they are not authenticated by
    ambient cookies. Route dependencies call this only on unsafe HTTP methods.
    """
    if ctx.auth_kind != "session":
        return
    if ctx.csrf_hash is None or not header_value:
        raise HTTPException(status_code=403, detail="CSRF token required")
    # Keep the random proof returned by login/team/refresh valid until an auth
    # transition, including clients opened before stable /me proofs existed.
    if hmac.compare_digest(hash_secret(header_value), ctx.csrf_hash):
        return
    if raw_session and ctx.session_hash is not None and hmac.compare_digest(
        header_value.encode(), session_csrf_token(ctx, raw_session).encode(),
    ):
        return
    raise HTTPException(status_code=403, detail="CSRF token invalid")


async def create_login_challenge(
    session: AsyncSession,
    *,
    email: str,
    ttl_seconds: int,
    audience: ApplicationSessionAudienceV1 | None = None,
) -> str | None:
    """Create a one-time login challenge for an existing user.

    Unknown emails return None so the route can keep the same outward
    response without disclosing account existence.
    """
    normalized = normalize_email(email)
    user = (await session.execute(
        select(User).where(func.lower(User.email) == normalized),
    )).scalar_one_or_none()
    if user is None:
        return None

    raw = _raw_secret("loom_login")
    now = datetime.now(UTC)
    await session.execute(insert(LoginChallenge).values(
        challenge_hash=hash_browser_secret(raw, audience=audience, purpose="login_challenge"),
        user_id=user.id,
        issued_at=now,
        expires_at=now + timedelta(seconds=ttl_seconds),
    ))
    return raw


async def _first_membership(
    session: AsyncSession, user_id: UUID,
) -> tuple[TeamMembership, Team] | None:
    row = (await session.execute(
        select(TeamMembership, Team)
        .join(Team, Team.id == TeamMembership.team_id)
        .where(TeamMembership.user_id == user_id)
        .order_by(Team.name.asc(), Team.id.asc()),
    )).first()
    if row is None:
        return None
    membership, team = row
    return membership, team


async def _membership_for_team(
    session: AsyncSession, *, user_id: UUID, team_id: UUID,
) -> tuple[TeamMembership, Team] | None:
    row = (await session.execute(
        select(TeamMembership, Team)
        .join(Team, Team.id == TeamMembership.team_id)
        .where(
            TeamMembership.user_id == user_id,
            TeamMembership.team_id == team_id,
        ),
    )).first()
    if row is None:
        return None
    membership, team = row
    return membership, team


async def create_application_login_challenge(
    session: AsyncSession, *, user_id: UUID, team_id: UUID, audience: ApplicationSessionAudienceV1,
) -> str:
    """Internal owner-session exchange; never enroll or widen shared authority.

    The exact starting team travels inside the hashed proof, not an untrusted
    completion-body selector. Existing challenge/session tables remain unchanged.
    """
    user = await session.get(User, user_id, with_for_update=True)
    membership = await _membership_for_team(session, user_id=user_id, team_id=team_id)
    if (user is None or user.status != "active" or user.disabled_at is not None or user.is_platform_admin
            or membership is None or membership[1].disabled_at is not None
            or membership[0].role not in {"owner", "member", "viewer"}):
        raise HTTPException(status_code=403, detail="application owner unavailable")
    now = (await session.execute(select(func.clock_timestamp()))).scalar_one()
    raw = _raw_secret("loom_app_login_" + team_id.hex)
    session.add(LoginChallenge(challenge_hash=hash_browser_secret(raw, audience=audience, purpose="login_challenge"),
        user_id=user_id, issued_at=now, expires_at=now + timedelta(seconds=90)))
    return raw


def _ctx_from_session(
    *,
    user: User,
    user_session: UserSession,
    role: str,
    team_id: UUID | None,
) -> AuthContext:
    return AuthContext(
        token_hash=b"",
        type="user",
        scopes=role_scopes(role),
        team_id=team_id,
        expires_at=user_session.expires_at,
        user_id=user.id,
        role=role,
        session_hash=user_session.session_hash,
        csrf_hash=user_session.csrf_hash,
        auth_kind="session",
    )


async def consume_login_challenge(
    session: AsyncSession,
    *,
    raw_token: str,
    session_ttl_seconds: int,
    audience: ApplicationSessionAudienceV1 | None = None,
) -> CreatedSession:
    starting_team: UUID | None = None
    if raw_token.startswith("loom_app_login_"):
        match = re.fullmatch(r"loom_app_login_([0-9a-f]{32})_[A-Za-z0-9_-]{43}", raw_token)
        if audience is None or match is None:
            raise HTTPException(status_code=400, detail="invalid login token")
        starting_team = UUID(hex=match.group(1))
    challenge = (await session.execute(
        select(LoginChallenge).where(
            LoginChallenge.challenge_hash == hash_browser_secret(
                raw_token, audience=audience, purpose="login_challenge",
            ),
            LoginChallenge.consumed_at.is_(None),
        ).with_for_update(),
    )).scalar_one_or_none()
    if challenge is None:
        raise HTTPException(status_code=400, detail="invalid login token")

    user = (await session.execute(
        select(User).where(User.id == challenge.user_id).with_for_update(),
    )).scalar_one()
    # Lock waits can exceed a short proof's TTL. Use the database clock only
    # after acquiring both locks, never a pre-wait process timestamp.
    now = (await session.execute(select(func.clock_timestamp()))).scalar_one()
    if challenge.expires_at <= now:
        raise HTTPException(status_code=400, detail="invalid login token")
    if user.status != "active" or user.disabled_at is not None:
        raise HTTPException(status_code=403, detail="user is disabled or inactive")
    if starting_team is not None and user.is_platform_admin:
        raise HTTPException(status_code=403, detail="application owner unavailable")
    role = "platform_admin" if user.is_platform_admin else None
    team_id: UUID | None = None
    if role is None:
        first = (await _membership_for_team(session, user_id=user.id, team_id=starting_team)
                 if starting_team is not None else await _first_membership(session, user.id))
        if first is None:
            raise HTTPException(status_code=403, detail="user has no teams")
        membership, _team = first
        if starting_team is not None and membership.role not in {"owner", "member", "viewer"}:
            raise HTTPException(status_code=403, detail="application owner unavailable")
        if _team.disabled_at is not None:
            raise HTTPException(status_code=403, detail="team is disabled")
        role = membership.role
        team_id = membership.team_id

    raw_session = _raw_secret("loom_session")
    raw_csrf = _raw_secret("loom_csrf")
    session_hash = hash_browser_secret(raw_session, audience=audience, purpose="session")
    user_session = UserSession(
        session_hash=session_hash,
        user_id=user.id,
        current_team_id=team_id,
        csrf_hash=hash_secret(raw_csrf),
        issued_at=now,
        expires_at=now + timedelta(seconds=session_ttl_seconds),
        revoked_at=None,
        last_seen_at=now,
    )
    session.add(user_session)
    await session.execute(
        update(LoginChallenge)
        .where(LoginChallenge.challenge_hash == challenge.challenge_hash)
        .values(consumed_at=now),
    )
    await session.execute(
        update(User).where(User.id == user.id).values(last_login_at=now),
    )
    ctx = _ctx_from_session(
        user=user, user_session=user_session, role=role, team_id=team_id,
    )
    return CreatedSession(raw_session=raw_session, raw_csrf=raw_csrf, ctx=ctx)


async def create_session_for_user(
    session: AsyncSession,
    *,
    user: User,
    session_ttl_seconds: int,
    current_team_id: UUID | None = None,
    audience: ApplicationSessionAudienceV1 | None = None,
) -> CreatedSession:
    """Create a browser session after a trusted onboarding action.

    Invite acceptance uses the invite secret as the bootstrap proof. It creates
    or finds the user, creates the membership, then calls this helper so the
    user lands in the invited team without receiving a raw team token.
    """
    now = datetime.now(UTC)
    role = "platform_admin" if user.is_platform_admin else None
    team_id: UUID | None = None
    if current_team_id is not None:
        membership_row = await _membership_for_team(
            session, user_id=user.id, team_id=current_team_id,
        )
    else:
        membership_row = await _first_membership(session, user.id)
    if membership_row is None:
        raise HTTPException(status_code=403, detail="user has no teams")
    membership, _team = membership_row
    team_id = membership.team_id
    if role is None:
        role = membership.role

    raw_session = _raw_secret("loom_session")
    raw_csrf = _raw_secret("loom_csrf")
    user_session = UserSession(
        session_hash=hash_browser_secret(raw_session, audience=audience, purpose="session"),
        user_id=user.id,
        current_team_id=team_id,
        csrf_hash=hash_secret(raw_csrf),
        issued_at=now,
        expires_at=now + timedelta(seconds=session_ttl_seconds),
        revoked_at=None,
        last_seen_at=now,
    )
    session.add(user_session)
    await session.execute(
        update(User).where(User.id == user.id).values(last_login_at=now),
    )
    ctx = _ctx_from_session(
        user=user, user_session=user_session, role=role, team_id=team_id,
    )
    return CreatedSession(raw_session=raw_session, raw_csrf=raw_csrf, ctx=ctx)


async def accessible_teams(
    session: AsyncSession, user: User, *, team_id: UUID | None = None,
    include_disabled: bool = False,
) -> list[tuple[Team, str]]:
    """Share membership/admin authority, optionally retaining a disabled context.

    Existing sessions retain their identity so route authorization can return
    the established disabled-team 403. New team selections must be enabled.
    """
    if user.disabled_at is not None or user.status != "active":
        return []
    if user.is_platform_admin:
        stmt = select(Team)
        if not include_disabled:
            stmt = stmt.where(Team.disabled_at.is_(None))
        if team_id is not None:
            stmt = stmt.where(Team.id == team_id)
        teams = (await session.execute(stmt.order_by(Team.name.asc(), Team.id.asc()))).scalars().all()
        return [(team, "platform_admin") for team in teams]
    membership_stmt = (
        select(Team, TeamMembership.role)
        .join(TeamMembership, TeamMembership.team_id == Team.id)
        .where(TeamMembership.user_id == user.id)
    )
    if not include_disabled:
        membership_stmt = membership_stmt.where(Team.disabled_at.is_(None))
    if team_id is not None:
        membership_stmt = membership_stmt.where(Team.id == team_id)
    rows = (await session.execute(membership_stmt.order_by(Team.name.asc(), Team.id.asc()))).all()
    return [(team, role) for team, role in rows]


async def verify_session_cookie(
    session: AsyncSession, raw_cookie: str | None,
    *, audience: ApplicationSessionAudienceV1 | None = None,
) -> AuthContext | None:
    # Retired credentials must never gain ordinary-session write authority.
    if not raw_cookie or raw_cookie.startswith("loom_session_staging_admin_"):
        return None
    now = datetime.now(UTC)
    row = (await session.execute(
        select(UserSession, User)
        .join(User, User.id == UserSession.user_id)
        .where(UserSession.session_hash == hash_browser_secret(
            raw_cookie, audience=audience, purpose="session",
        )),
    )).first()
    if row is None:
        return None
    user_session, user = row
    if user_session.revoked_at is not None or user_session.expires_at < now:
        return None
    team_id = user_session.current_team_id
    teams = await accessible_teams(
        session, user, team_id=team_id, include_disabled=team_id is not None,
    )
    if team_id is None:
        if not teams:
            return None
        first = await _first_membership(session, user.id)
        team_id = (
            first[1].id if first is not None and any(team.id == first[1].id for team, _ in teams)
            else teams[0][0].id
        )
        await session.execute(
            update(UserSession)
            .where(UserSession.session_hash == user_session.session_hash)
            .values(current_team_id=team_id),
        )
    role = next((role for team, role in teams if team.id == team_id), None)
    if role is None:
        return None
    await session.execute(
        update(UserSession)
        .where(UserSession.session_hash == user_session.session_hash)
        .values(last_seen_at=now),
    )
    return _ctx_from_session(
        user=user, user_session=user_session, role=role, team_id=team_id,
    )


async def switch_session_team(
    session: AsyncSession, *, ctx: AuthContext, team_id: UUID,
) -> None:
    if ctx.session_hash is None or ctx.user_id is None:
        raise HTTPException(status_code=401, detail="missing browser session")
    user = await session.get(User, ctx.user_id)
    if user is None:
        raise HTTPException(status_code=401, detail="missing user")
    teams = await accessible_teams(session, user, team_id=team_id)
    if not any(team.id == team_id for team, _role in teams):
        if user.is_platform_admin and await session.get(Team, team_id) is None:
            raise HTTPException(status_code=404, detail="team not found")
        raise HTTPException(status_code=403, detail="team is disabled or not accessible")
    await session.execute(
        update(UserSession)
        .where(UserSession.session_hash == ctx.session_hash)
        .values(current_team_id=team_id),
    )


async def refresh_session(
    session: AsyncSession,
    *,
    ctx: AuthContext,
    session_ttl_seconds: int,
    audience: ApplicationSessionAudienceV1 | None = None,
) -> RefreshedSession:
    if ctx.session_hash is None:
        raise HTTPException(status_code=401, detail="missing browser session")
    raw_session = _raw_secret("loom_session")
    raw_csrf = _raw_secret("loom_csrf")
    now = datetime.now(UTC)
    await session.execute(
        update(UserSession)
        .where(UserSession.session_hash == ctx.session_hash)
        .values(
            session_hash=hash_browser_secret(raw_session, audience=audience, purpose="session"),
            csrf_hash=hash_secret(raw_csrf),
            expires_at=now + timedelta(seconds=session_ttl_seconds),
            last_seen_at=now,
        ),
    )
    return RefreshedSession(raw_session=raw_session, raw_csrf=raw_csrf)


async def rotate_csrf_token(session: AsyncSession, ctx: AuthContext) -> str:
    if ctx.session_hash is None:
        raise HTTPException(status_code=401, detail="missing browser session")
    raw_csrf = _raw_secret("loom_csrf")
    await session.execute(
        update(UserSession)
        .where(UserSession.session_hash == ctx.session_hash)
        .values(csrf_hash=hash_secret(raw_csrf), last_seen_at=datetime.now(UTC)),
    )
    return raw_csrf


async def revoke_session(session: AsyncSession, ctx: AuthContext) -> None:
    if ctx.session_hash is None:
        return
    await session.execute(
        update(UserSession)
        .where(UserSession.session_hash == ctx.session_hash)
        .values(revoked_at=datetime.now(UTC)),
    )
