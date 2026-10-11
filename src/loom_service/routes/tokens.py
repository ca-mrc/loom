"""GET/POST/DELETE /api/v1/tokens (spec §5.5 / issue #328).

Routes:
- GET    /tokens                 — list token metadata; never raw secrets.
- POST   /tokens                 — mint one scoped API token and reveal the raw
                                   secret exactly once.
- POST   /tokens/{prefix}/rotate — revoke one visible token and reveal a fresh
                                   replacement exactly once.
- DELETE /tokens/{prefix}        — revoke a token by its 8-char hash prefix.

The hex-prefix lookup is O(N) over all tokens — fine for the v1 scale
(low thousands at most). A composite index would let us do an indexed
range query, but the prefix-as-identifier choice is per spec §5.5 to
keep the raw secret out of the response after revocation."""

from __future__ import annotations

import hashlib
import secrets
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Header, HTTPException, Request
from pydantic import BaseModel, Field
from sqlalchemy import insert, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from loom.auth import AuthContext
from loom.db.schema import Token
from loom_service import wire_responses as wire
from loom_service.admin_audit import require_admin_actor, write_admin_audit_event
from loom_service.auth_guards import (
    is_admin,
    require_scope,
)
from loom_service.dependencies import SessionAndCtx

router = APIRouter()


class _CreateTokenReq(BaseModel):
    name: str = Field(min_length=1, max_length=120)
    type: str = Field(pattern=r"^(team|admin)$")
    scopes: list[str] = Field(min_length=1)
    expires_in_days: int = Field(gt=0, le=365)
    team_id: UUID | None = None  # admin-only cross-team mint


# Every recognized scope across the whole product. Updates here are deliberate:
# a caller minting a token with a never-defined scope is almost certainly a
# typo, so the route rejects it instead of silently storing it. Admin scopes are
# listed only so legacy rows/migration paths remain explicit; new DB-backed
# tokens carrying admin authority are rejected below.
_KNOWN_SCOPES = frozenset({
    "read:own",
    "submit",
    "providers:manage",
    "tokens:manage",
    "team:manage",
    "submit:batch",
    "family:evolve",
    "admin:platform",
    "admin:tokens",
    "admin:rate_cards",
    "worker:claim",
    "worker:report",
    "worker:index",
})

_INTERNAL_SCOPES = frozenset({
    "submit:batch",
    "family:evolve",
    "worker:claim",
    "worker:report",
    "worker:index",
})


def _hash_prefix(token_hash: bytes) -> str:
    return token_hash.hex()[:8]


def _raw_api_token() -> str:
    return f"loom_api_{secrets.token_urlsafe(32)}"


def _serialize(row: Token) -> dict[str, Any]:
    return {
        "token_hash_prefix": _hash_prefix(row.token_hash),
        "name": row.name or "Unnamed token",
        "type": row.type,
        "scopes": list(row.scopes),
        "team_id": str(row.team_id) if row.team_id else None,
        "created_by_actor": row.created_by_actor,
        "created_by_user_id": (
            str(row.created_by_user_id) if row.created_by_user_id else None
        ),
        "issued_at": row.issued_at.isoformat(),
        "expires_at": (
            row.expires_at.isoformat() if row.expires_at else None
        ),
        "revoked_at": (
            row.revoked_at.isoformat() if row.revoked_at else None
        ),
        "last_used_at": (
            row.last_used_at.isoformat() if row.last_used_at else None
        ),
    }


def _validate_prefix(prefix: str) -> None:
    if len(prefix) != 8 or not all(c in "0123456789abcdef" for c in prefix):
        raise HTTPException(
            status_code=400, detail="prefix must be 8 lowercase hex chars",
        )


def _require_token_management(ctx: Any) -> None:
    if not is_admin(ctx):
        require_scope(ctx, "tokens:manage")


def _validate_requested_scopes(ctx: Any, scopes: list[str], *, admin: bool) -> None:
    unknown = [scope for scope in scopes if scope not in _KNOWN_SCOPES]
    if unknown:
        raise HTTPException(
            status_code=400,
            detail=f"unrecognized scopes: {sorted(unknown)}",
        )

    if any(scope.startswith("admin:") for scope in scopes):
        raise HTTPException(
            status_code=400,
            detail=(
                "database-backed admin scopes are disabled; admin "
                "authority must come from the singleton admin secret file"
            ),
        )

    if not admin:
        internal = sorted(set(scopes).intersection(_INTERNAL_SCOPES))
        if internal:
            raise HTTPException(
                status_code=403,
                detail=f"internal scopes require admin:tokens: {internal}",
            )
        missing = sorted(scope for scope in scopes if scope not in ctx.scopes)
        if missing:
            raise HTTPException(
                status_code=403,
                detail=f"cannot grant scopes not held by caller: {missing}",
            )


async def _find_visible_token(
    s: AsyncSession, ctx: AuthContext, prefix: str,
) -> Token:
    _validate_prefix(prefix)
    stmt = select(Token).where(Token.revoked_at.is_(None))
    if not is_admin(ctx):
        stmt = stmt.where(Token.team_id == ctx.team_id)
    rows = (await s.execute(stmt)).scalars().all()
    matches = [
        row for row in rows if row.token_hash.hex().startswith(prefix)
    ]
    if not matches:
        raise HTTPException(status_code=404, detail="token not found")
    if len(matches) > 1:
        raise HTTPException(
            status_code=409,
            detail=(
                f"prefix {prefix!r} matches {len(matches)} tokens; "
                "ambiguous"
            ),
        )
    return matches[0]


def _created_by_actor(ctx: AuthContext, admin_actor: str | None) -> str | None:
    if admin_actor is not None:
        return f"admin:{admin_actor}"
    if ctx.user_id is not None:
        return f"user:{ctx.user_id}"
    return f"{ctx.type}:{ctx.token_hash.hex()[:8]}"


@router.get("/tokens", response_model=wire.TokenList, response_model_exclude_unset=True)
async def list_tokens(
    request: Request,
    sc: SessionAndCtx,
) -> dict[str, list[dict[str, Any]]]:
    s, ctx = sc
    stmt = select(Token).order_by(Token.issued_at.desc())
    if not is_admin(ctx):
        stmt = stmt.where(Token.team_id == ctx.team_id)
    rows = (await s.execute(stmt)).scalars().all()
    return {"items": [_serialize(r) for r in rows]}


@router.post("/tokens", status_code=201, response_model=wire.PostTokensResponse, response_model_exclude_unset=True)
async def create_token(
    request: Request,
    sc: SessionAndCtx,
    payload: _CreateTokenReq,
    x_loom_admin_actor: str | None = Header(default=None),
) -> dict[str, Any]:
    s, ctx = sc

    if payload.type == "admin":
        raise HTTPException(
            status_code=400,
            detail=(
                "database-backed admin tokens are disabled; use the "
                "singleton admin secret file managed by `loom service "
                "init-admin` or `loom service rotate-admin`"
            ),
        )

    admin = is_admin(ctx)
    admin_actor = require_admin_actor(x_loom_admin_actor) if admin else None
    _validate_requested_scopes(ctx, payload.scopes, admin=admin)
    if not admin:
        _require_token_management(ctx)
        if payload.type != "team":
            raise HTTPException(
                status_code=403,
                detail="only admins may mint admin tokens",
            )
        if payload.team_id is not None and payload.team_id != ctx.team_id:
            raise HTTPException(
                status_code=403,
                detail="cannot mint into another team",
            )

    target_team = (
        payload.team_id if payload.team_id else ctx.team_id
    )
    if payload.type == "team" and target_team is None:
        raise HTTPException(
            status_code=400, detail="team_id required for team token",
        )

    raw = _raw_api_token()
    token_hash = hashlib.sha256(raw.encode()).digest()
    token_hash_prefix = _hash_prefix(token_hash)
    expires_at = datetime.now(UTC) + timedelta(
        days=payload.expires_in_days,
    )

    await s.execute(insert(Token).values(
        token_hash=token_hash,
        name=payload.name.strip(),
        type=payload.type,
        scopes=list(payload.scopes),
        team_id=target_team if payload.type == "team" else None,
        created_by_user_id=ctx.user_id,
        created_by_actor=_created_by_actor(ctx, admin_actor),
        issued_at=datetime.now(UTC),
        expires_at=expires_at,
    ))
    if admin_actor is not None:
        await write_admin_audit_event(
            s,
            actor=admin_actor,
            action="token.create",
            target_type="token",
            target_id=token_hash_prefix,
            request=request,
            metadata={
                "token_hash_prefix": token_hash_prefix,
                "token_name": payload.name.strip(),
                "token_type": payload.type,
                "team_id": str(target_team) if target_team else None,
                "scopes": list(payload.scopes),
                "expires_at": expires_at.isoformat(),
            },
        )
    row = (await s.execute(
        select(Token).where(Token.token_hash == token_hash),
    )).scalar_one()
    await s.commit()

    return {
        "token": raw,
        "token_hash_prefix": token_hash_prefix,
        "expires_at": expires_at.isoformat(),
        "item": _serialize(row),
    }


@router.delete("/tokens/{prefix}", status_code=204)
async def revoke_token(
    request: Request,
    sc: SessionAndCtx,
    prefix: str,
    x_loom_admin_actor: str | None = Header(default=None),
) -> None:
    _validate_prefix(prefix)
    s, ctx = sc
    admin = is_admin(ctx)
    admin_actor = require_admin_actor(x_loom_admin_actor) if admin else None
    if not admin:
        _require_token_management(ctx)

    target = await _find_visible_token(s, ctx, prefix)
    if target.team_id is None and not is_admin(ctx):
        # Defense-in-depth: a non-admin shouldn't have reached the
        # prefix-scan over a global-admin token (team_id filter
        # above), but guard anyway in case the scope filter ever
        # changes shape.
        raise HTTPException(
            status_code=403, detail="admin scope required",
        )
    await s.execute(
        update(Token)
        .where(Token.token_hash == target.token_hash)
        .values(revoked_at=datetime.now(UTC)),
    )
    if admin_actor is not None:
        await write_admin_audit_event(
            s,
            actor=admin_actor,
            action="token.revoke",
            target_type="token",
            target_id=prefix,
            request=request,
            metadata={
                "token_hash_prefix": prefix,
                "token_name": target.name,
                "token_type": target.type,
                "team_id": str(target.team_id) if target.team_id else None,
                "scopes": list(target.scopes),
            },
        )
    await s.commit()


@router.post("/tokens/{prefix}/rotate", response_model=wire.PostTokensPrefixRotateResponse, response_model_exclude_unset=True)
async def rotate_token(
    request: Request,
    sc: SessionAndCtx,
    prefix: str,
    x_loom_admin_actor: str | None = Header(default=None),
) -> dict[str, Any]:
    s, ctx = sc
    admin = is_admin(ctx)
    admin_actor = require_admin_actor(x_loom_admin_actor) if admin else None
    if not admin:
        _require_token_management(ctx)

    target = await _find_visible_token(s, ctx, prefix)
    if target.team_id is None and not is_admin(ctx):
        raise HTTPException(status_code=403, detail="admin scope required")

    _validate_requested_scopes(ctx, list(target.scopes), admin=admin)
    raw = _raw_api_token()
    token_hash = hashlib.sha256(raw.encode()).digest()
    token_hash_prefix = _hash_prefix(token_hash)
    now = datetime.now(UTC)
    await s.execute(
        update(Token)
        .where(Token.token_hash == target.token_hash)
        .values(revoked_at=now),
    )
    await s.execute(insert(Token).values(
        token_hash=token_hash,
        name=target.name,
        type=target.type,
        scopes=list(target.scopes),
        team_id=target.team_id,
        created_by_user_id=ctx.user_id,
        created_by_actor=_created_by_actor(ctx, admin_actor),
        issued_at=now,
        expires_at=target.expires_at,
    ))
    if admin_actor is not None:
        await write_admin_audit_event(
            s,
            actor=admin_actor,
            action="token.rotate",
            target_type="token",
            target_id=prefix,
            request=request,
            metadata={
                "old_token_hash_prefix": prefix,
                "new_token_hash_prefix": token_hash_prefix,
                "token_name": target.name,
                "token_type": target.type,
                "team_id": str(target.team_id) if target.team_id else None,
                "scopes": list(target.scopes),
            },
        )
    row = (await s.execute(
        select(Token).where(Token.token_hash == token_hash),
    )).scalar_one()
    await s.commit()
    return {
        "token": raw,
        "token_hash_prefix": token_hash_prefix,
        "expires_at": row.expires_at.isoformat() if row.expires_at else None,
        "item": _serialize(row),
    }
