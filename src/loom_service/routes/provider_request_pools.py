"""Opt-in request rotation behind an existing, stable provider connection."""

from __future__ import annotations

from datetime import datetime
from typing import Literal
from uuid import UUID

from fastapi import APIRouter, Header, HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from sqlalchemy import delete, select, text

from loom.db.schema import (
    ProviderConnectionShare,
    ProviderModelCache,
    ProviderRequestPool,
    ProviderRequestPoolMember,
)
from loom.provider_model_preflight import preflight_failure_kind
from loom_service.admin_audit import write_admin_audit_event
from loom_service.dependencies import SessionAndCtx
from loom_service.routes.provider_connections import (
    _get_active_connection,
    _provider_audit_actor,
    _require_provider_management,
    _require_provider_owner_or_admin,
)

router = APIRouter()


class PoolMemberConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    connection_id: UUID
    quota_group: str = Field(min_length=1, max_length=64, pattern=r"^[A-Za-z0-9_.-]+$")
    enabled: bool = True


class RequestPoolConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    required_models: list[str] = Field(min_length=1, max_length=64)
    equivalent_models: Literal[True]
    members: list[PoolMemberConfig] = Field(min_length=2, max_length=32)
    attempt_timeout_sec: float = Field(default=20, gt=0, le=200, allow_inf_nan=False)
    cooldown_sec: float = Field(default=30, gt=0, le=3600, allow_inf_nan=False)

    @field_validator("required_models")
    @classmethod
    def _model_names(cls, value: list[str]) -> list[str]:
        if any(not v.strip() or len(v) > 256 or v != v.strip() for v in value):
            raise ValueError("required_models must contain nonempty, trimmed model IDs")
        if len(value) != len(set(value)):
            raise ValueError("required_models must be unique")
        return value

    @model_validator(mode="after")
    def _members(self) -> RequestPoolConfig:
        if len({m.connection_id for m in self.members}) != len(self.members):
            raise ValueError("pool members must be unique")
        if not any(m.enabled for m in self.members):
            raise ValueError("at least one pool member must be enabled")
        return self


class PoolMemberResponse(PoolMemberConfig):
    ordinal: int
    cooldown_until: datetime | None


class RequestPoolResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    connection_id: UUID
    enabled: bool
    required_models: list[str]
    attempt_timeout_sec: float
    cooldown_sec: float
    members: list[PoolMemberResponse]


def _response(
    pool: ProviderRequestPool, members: list[ProviderRequestPoolMember]
) -> RequestPoolResponse:
    return RequestPoolResponse(
        connection_id=pool.connection_id,
        enabled=pool.enabled,
        required_models=pool.required_models,
        attempt_timeout_sec=pool.attempt_timeout_sec,
        cooldown_sec=pool.cooldown_sec,
        members=[
            PoolMemberResponse(
                connection_id=m.connection_id,
                quota_group=m.quota_group,
                enabled=m.enabled,
                ordinal=m.ordinal,
                cooldown_until=m.cooldown_until,
            )
            for m in members
        ],
    )


@router.get(
    "/provider-connections/{connection_id}/request-pool", response_model=RequestPoolResponse
)
async def get_request_pool(connection_id: UUID, sc: SessionAndCtx) -> RequestPoolResponse:
    session, ctx = sc
    await _get_active_connection(session, connection_id, ctx)
    pool = await session.get(ProviderRequestPool, connection_id)
    if pool is None:
        raise HTTPException(404, "provider request pool not configured")
    members = list(
        (
            await session.scalars(
                select(ProviderRequestPoolMember)
                .where(
                    ProviderRequestPoolMember.pool_connection_id == connection_id,
                )
                .order_by(ProviderRequestPoolMember.ordinal)
            )
        ).all()
    )
    # Sharing the logical route does not reveal otherwise inaccessible members.
    for member in members:
        await _get_active_connection(session, member.connection_id, ctx)
    return _response(pool, members)


@router.put(
    "/provider-connections/{connection_id}/request-pool", response_model=RequestPoolResponse
)
async def put_request_pool(
    request: Request,
    connection_id: UUID,
    payload: RequestPoolConfig,
    sc: SessionAndCtx,
    x_loom_admin_actor: str | None = Header(default=None),
) -> RequestPoolResponse:
    session, ctx = sc
    _require_provider_management(ctx)
    # Serialize rare membership edits, including concurrent attempts to create
    # cycles between two previously unpooled connections.
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtext('provider_request_pool_config'))")
    )
    root = await _get_active_connection(session, connection_id, ctx, for_update=True)
    _require_provider_owner_or_admin(ctx, root)
    member_ids = [m.connection_id for m in payload.members]
    if connection_id not in member_ids:
        raise HTTPException(400, "the logical connection must be a pool member")
    nested = (
        await session.scalars(
            select(ProviderRequestPool.connection_id).where(
                ProviderRequestPool.enabled.is_(True),
                ProviderRequestPool.connection_id.in_(member_ids),
                ProviderRequestPool.connection_id != connection_id,
            )
        )
    ).first()
    parent = (
        await session.scalars(
            select(ProviderRequestPoolMember.pool_connection_id)
            .join(
                ProviderRequestPool,
                ProviderRequestPool.connection_id == ProviderRequestPoolMember.pool_connection_id,
            )
            .where(
                ProviderRequestPool.enabled.is_(True),
                ProviderRequestPoolMember.connection_id == connection_id,
                ProviderRequestPoolMember.pool_connection_id != connection_id,
            )
        )
    ).first()
    if nested is not None or parent is not None:
        raise HTTPException(400, "nested provider request pools are unsupported")

    for member in payload.members:
        row = await _get_active_connection(session, member.connection_id, ctx)
        # Even an admin must establish use authority for the owning team.
        if row.team_id != root.team_id:
            shared = await session.scalar(
                select(ProviderConnectionShare.provider_connection_id).where(
                    ProviderConnectionShare.provider_connection_id == row.id,
                    ProviderConnectionShare.target_team_id == root.team_id,
                )
            )
            if shared is None:
                raise HTTPException(404, "provider_connection not found")
        if row.status != "valid" or row.provider_type not in {"openai-compatible", "custom"}:
            raise HTTPException(
                400, "every pool member must be a valid OpenAI-compatible connection"
            )
        allowed = set(row.allowed_models or [])
        cached = (
            await session.scalars(
                select(ProviderModelCache).where(
                    ProviderModelCache.provider_connection_id == row.id,
                    ProviderModelCache.model_id.in_(payload.required_models),
                    ProviderModelCache.visible.is_(True),
                    ProviderModelCache.upstream_present.is_(True),
                )
            )
        ).all()
        models = {
            m.model_id
            for m in cached
            if preflight_failure_kind(
                m.last_preflight_status,
                m.last_preflight_error_code,
                m.last_preflight_http_status,
            )
            != "rejected"
        }
        if set(payload.required_models) - models or (
            allowed and set(payload.required_models) - allowed
        ):
            raise HTTPException(
                400,
                "every member requires the complete visible model set without definitive preflight rejection",
            )

    pool = await session.scalar(
        select(ProviderRequestPool)
        .where(
            ProviderRequestPool.connection_id == connection_id,
        )
        .with_for_update()
    )
    if pool is None:
        pool = ProviderRequestPool(
            connection_id=connection_id,
            next_member=0,
            required_models=payload.required_models,
            attempt_timeout_sec=payload.attempt_timeout_sec,
            cooldown_sec=payload.cooldown_sec,
            enabled=True,
        )
        session.add(pool)
    previous = list(
        (
            await session.scalars(
                select(ProviderRequestPoolMember).where(
                    ProviderRequestPoolMember.pool_connection_id == connection_id,
                )
            )
        ).all()
    )
    cooldowns = {m.connection_id: m.cooldown_until for m in previous}
    await session.execute(
        delete(ProviderRequestPoolMember).where(
            ProviderRequestPoolMember.pool_connection_id == connection_id,
        )
    )
    pool.required_models = payload.required_models
    pool.attempt_timeout_sec = payload.attempt_timeout_sec
    pool.cooldown_sec = payload.cooldown_sec
    pool.enabled = True
    await session.flush()
    members = [
        ProviderRequestPoolMember(
            pool_connection_id=connection_id,
            connection_id=m.connection_id,
            ordinal=i,
            quota_group=m.quota_group,
            enabled=m.enabled,
            cooldown_until=cooldowns.get(m.connection_id),
        )
        for i, m in enumerate(payload.members)
    ]
    session.add_all(members)
    await write_admin_audit_event(
        session,
        actor=_provider_audit_actor(ctx, x_loom_admin_actor),
        action="provider_request_pool.configure",
        target_type="provider_connection",
        target_id=str(connection_id),
        request=request,
        metadata={
            "member_connection_ids": [str(i) for i in member_ids],
            "required_models": payload.required_models,
        },
    )
    await session.commit()
    return _response(pool, members)


@router.delete(
    "/provider-connections/{connection_id}/request-pool", response_model=RequestPoolResponse
)
async def disable_request_pool(
    request: Request,
    connection_id: UUID,
    sc: SessionAndCtx,
    x_loom_admin_actor: str | None = Header(default=None),
) -> RequestPoolResponse:
    session, ctx = sc
    _require_provider_management(ctx)
    root = await _get_active_connection(session, connection_id, ctx, for_update=True)
    _require_provider_owner_or_admin(ctx, root)
    pool = await session.scalar(
        select(ProviderRequestPool)
        .where(
            ProviderRequestPool.connection_id == connection_id,
        )
        .with_for_update()
    )
    if pool is None:
        raise HTTPException(404, "provider request pool not configured")
    pool.enabled = False
    members = list(
        (
            await session.scalars(
                select(ProviderRequestPoolMember)
                .where(
                    ProviderRequestPoolMember.pool_connection_id == connection_id,
                )
                .order_by(ProviderRequestPoolMember.ordinal)
            )
        ).all()
    )
    await write_admin_audit_event(
        session,
        actor=_provider_audit_actor(ctx, x_loom_admin_actor),
        action="provider_request_pool.disable",
        target_type="provider_connection",
        target_id=str(connection_id),
        request=request,
    )
    await session.commit()
    return _response(pool, members)
