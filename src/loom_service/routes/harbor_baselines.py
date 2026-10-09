"""Create, inspect and revoke independent, bounded Harbor run credentials."""

from __future__ import annotations

import secrets
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID, uuid4

from fastapi import APIRouter, Header, HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field, field_validator
from sqlalchemy import Text, and_, select

from loom.db.schema import HarborBaselineDispatch, HarborBaselineSession, LlmCall
from loom.harbor_baseline import PREFIX, model_context_bound, token_hash
from loom.provider_pricing import validate_model_id
from loom_llm_gateway.dialect import TokenUsage
from loom_llm_gateway.provider_pricing import configured_cost
from loom_llm_gateway.routes._facade_common import resolve_facade_connection
from loom_service.admin_audit import write_admin_audit_event
from loom_service.auth_guards import is_admin, require_scope
from loom_service.dependencies import SessionAndCtx
from loom_service.routes.provider_connections import _provider_audit_actor

router = APIRouter()


class CreateHarborBaseline(BaseModel):
    model_config = ConfigDict(extra="forbid")
    label: str = Field(min_length=1, max_length=120)
    provider_connection_id: UUID
    model: str = Field(min_length=1, max_length=256)
    team_id: UUID | None = None
    ttl_seconds: int = Field(ge=60, le=21600)
    max_calls: int = Field(ge=1, le=5000)
    max_output_tokens: int = Field(ge=1, le=32768)
    max_total_tokens: int = Field(ge=1, le=1_000_000_000)
    budget_usd: Decimal | None = Field(default=None, gt=0, le=1000, max_digits=12, decimal_places=6)

    @field_validator("model")
    @classmethod
    def exact_model(cls, value: str) -> str:
        return validate_model_id(value)


def metadata(row: HarborBaselineSession) -> dict[str, object]:
    return {
        "id": str(row.id),
        "label": row.label,
        "team_id": str(row.team_id),
        "provider_connection_id": str(row.provider_connection_id),
        "model": row.model,
        "expires_at": row.expires_at.isoformat(),
        "revoked": row.revoked_at is not None,
        "blocked_reason": row.blocked_reason,
        "max_calls": row.max_calls,
        "max_input_tokens": row.max_input_tokens,
        "max_output_tokens": row.max_output_tokens,
        "max_total_tokens": row.max_total_tokens,
        "budget_usd": str(row.budget_usd) if row.budget_usd is not None else None,
        "calls_reserved": row.calls_reserved,
        "tokens_reserved": row.tokens_reserved,
        "cost_reserved_usd": str(row.cost_reserved_usd),
        "subject_kind": "harbor_baseline",
        "configured_price_quota": row.budget_usd is not None,
        "supplier_invoice_cap": False,
    }


@router.post("/harbor-baselines", status_code=201)
async def create_baseline(
    request: Request,
    payload: CreateHarborBaseline,
    dep: SessionAndCtx,
    x_loom_admin_actor: str | None = Header(default=None),
) -> dict[str, object]:
    session, ctx = dep
    if not is_admin(ctx):
        require_scope(ctx, "submit")
        if payload.team_id is not None and payload.team_id != ctx.team_id:
            raise HTTPException(404, "team not found")
    team_id = payload.team_id if is_admin(ctx) else ctx.team_id
    if team_id is None:
        raise HTTPException(400, "baseline_team_required")
    provider = await resolve_facade_connection(
        session,
        payload.provider_connection_id,
        team_id,
        supported_types=frozenset({"openai-compatible", "custom"}),
        dialect_label="Harbor baseline",
    )
    context = await model_context_bound(session, provider, payload.model)
    if payload.max_total_tokens < context + payload.max_output_tokens:
        raise HTTPException(400, "baseline_budget_cannot_reserve_one_context")
    if payload.budget_usd is not None:
        # Creation checks usability; the Gateway re-resolves current pricing
        # before reserving every call. Never interpret usage-only as free.
        if provider.pricing_config is None:
            raise HTTPException(409, "baseline_usd_price_unavailable")
        estimate = await configured_cost(
            session,
            provider,
            payload.model,
            TokenUsage(context, payload.max_output_tokens, {"_loom_cache_usage_known": True}),
        )
        if (
            estimate.currency != "USD"
            or not estimate.price_basis
            or "prices" not in estimate.price_basis
        ):
            raise HTTPException(409, "baseline_usd_price_unavailable")
    token = PREFIX + secrets.token_urlsafe(32)
    row = HarborBaselineSession(
        id=uuid4(),
        team_id=team_id,
        provider_connection_id=provider.id,
        model=payload.model,
        label=payload.label,
        token_hash=token_hash(token),
        expires_at=datetime.now(UTC) + timedelta(seconds=payload.ttl_seconds),
        max_calls=payload.max_calls,
        max_input_tokens=context,
        max_output_tokens=payload.max_output_tokens,
        max_total_tokens=payload.max_total_tokens,
        budget_usd=payload.budget_usd,
        calls_reserved=0,
        tokens_reserved=0,
        cost_reserved_usd=Decimal(0),
    )
    session.add(row)
    await write_admin_audit_event(
        session,
        actor=_provider_audit_actor(ctx, x_loom_admin_actor),
        action="harbor_baseline.create",
        target_type="harbor_baseline",
        target_id=str(row.id),
        request=request,
        metadata={
            "provider_connection_id": str(provider.id),
            "model": row.model,
            "max_calls": row.max_calls,
            "max_total_tokens": row.max_total_tokens,
        },
    )
    await session.commit()
    # Only this create response contains the restricted bearer. No upstream
    # credential or private provider endpoint leaves the SecretStore boundary.
    return {**metadata(row), "token": token, "gateway_base_path": "/harbor-baseline/v1"}


async def visible_baseline(
    baseline_id: UUID, dep: SessionAndCtx, *, locked: bool = False
) -> HarborBaselineSession:
    session, ctx = dep
    query = select(HarborBaselineSession).where(HarborBaselineSession.id == baseline_id)
    if not is_admin(ctx):
        query = query.where(HarborBaselineSession.team_id == ctx.team_id)
    if locked:
        query = query.with_for_update()
    row = await session.scalar(query)
    if row is None:
        raise HTTPException(404, "Harbor baseline not found")
    return row


@router.get("/harbor-baselines/{baseline_id}")
async def get_baseline(baseline_id: UUID, dep: SessionAndCtx) -> dict[str, object]:
    if not is_admin(dep[1]):
        require_scope(dep[1], "read:own")
    return metadata(await visible_baseline(baseline_id, dep))


@router.delete("/harbor-baselines/{baseline_id}", status_code=204)
async def revoke_baseline(
    request: Request,
    baseline_id: UUID,
    dep: SessionAndCtx,
    x_loom_admin_actor: str | None = Header(default=None),
) -> None:
    _, ctx = dep
    if not is_admin(ctx):
        require_scope(ctx, "submit")
    row = await visible_baseline(baseline_id, dep, locked=True)
    row.revoked_at = datetime.now(UTC)
    await write_admin_audit_event(
        dep[0],
        actor=_provider_audit_actor(ctx, x_loom_admin_actor),
        action="harbor_baseline.revoke",
        target_type="harbor_baseline",
        target_id=str(row.id),
        request=request,
    )
    await dep[0].commit()


@router.get("/harbor-baselines/{baseline_id}/calls")
async def get_baseline_calls(baseline_id: UUID, dep: SessionAndCtx) -> dict[str, object]:
    """Return bounded call evidence without prompts, responses or credentials."""
    if not is_admin(dep[1]):
        require_scope(dep[1], "read:own")
    await visible_baseline(baseline_id, dep)
    rows = (
        await dep[0].execute(
            select(HarborBaselineDispatch, LlmCall)
            .outerjoin(
                LlmCall,
                and_(
                    LlmCall.step_id == HarborBaselineDispatch.id.cast(Text),
                    LlmCall.baseline_session_id == baseline_id,
                ),
            )
            .where(HarborBaselineDispatch.baseline_session_id == baseline_id)
            .order_by(HarborBaselineDispatch.created_at, HarborBaselineDispatch.id)
            .limit(5000)
        )
    ).all()
    return {
        "items": [
            {
                "dispatch_id": str(dispatch.id),
                "outcome": dispatch.outcome,
                "reserved_tokens": dispatch.reserved_tokens,
                "reserved_cost_usd": str(dispatch.reserved_cost_usd),
                "llm_call_id": str(call.id) if call is not None else None,
                "model": call.model if call is not None else None,
                "response_model": call.response_model if call is not None else None,
                "input_tokens": call.input_tokens if call is not None else None,
                "output_tokens": call.output_tokens if call is not None else None,
                "cost_usd": str(call.cost_usd)
                if call is not None and call.provider_extras.get("_loom_cost_currency") == "USD"
                else None,
                "cost_source": call.provider_extras.get("_loom_cost_source")
                if call is not None
                else None,
                "cost_confidence": call.provider_extras.get("_loom_cost_confidence")
                if call is not None
                else None,
                "request_params": call.request_params if call is not None else None,
                "usage_status": call.provider_extras.get("_loom_usage_status", "available")
                if call is not None
                else "pending",
            }
            for dispatch, call in rows
        ]
    }
