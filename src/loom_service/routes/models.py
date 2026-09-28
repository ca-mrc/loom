"""Model catalog — GET /api/v1/models.

Returns legacy rate-card model tuples plus BYO provider-connection
models visible to the caller's team (owned or shared with it). Default
view is launch-safe: agent-capable/recommended models only. `view=raw` keeps noisy provider
entries for debugging with classifier reasons.
"""

from __future__ import annotations

from typing import Any, Literal

from fastapi import APIRouter, Query
from sqlalchemy import or_, select

from loom.db.schema import (
    ProviderConnection,
    ProviderConnectionShare,
    ProviderModelCache,
    RateCard,
)
from loom_service.auth_guards import is_admin
from loom_service.dependencies import SessionAndCtx
from loom_service.provider_connection_lookup import OPENAI_SHAPED_PROVIDER_TYPES
from loom_service.provider_connections_service import preflight_failure_kind
from loom_service.provider_model_classifier import classify_model_id

router = APIRouter()


def _provider_namespace(row: ProviderConnection) -> str:
    if row.rate_card_provider:
        return row.rate_card_provider
    if row.provider_type == "openai-compatible":
        return "openai"
    return row.provider_type


def _responses_route(row: ProviderConnection) -> str:
    """How Gateway serves a Responses request (Codex) on this connection.

    Mirrors loom_llm_gateway.routes.responses._resolve_responses_support:
    `unprobed` means Gateway probes the upstream on the first call.
    """
    if row.provider_type not in OPENAI_SHAPED_PROVIDER_TYPES:
        return "unsupported"
    if row.responses_api_supported is None:
        return "unprobed"
    return "native" if row.responses_api_supported else "translated"


def _byo_model_item(
    conn: ProviderConnection,
    cache: ProviderModelCache,
) -> dict[str, Any]:
    capabilities = dict(cache.capabilities or {})
    source = capabilities.get("source")
    source_str = source if isinstance(source, str) else "discovered"
    classification = classify_model_id(
        cache.model_id, source=source_str, family=cache.family,
    )
    if not cache.visible:
        visibility = "hidden"
        recommended = False
        hidden_reason = cache.hidden_reason
    else:
        visibility = classification.visibility
        recommended = classification.recommended
        hidden_reason = cache.hidden_reason or classification.reason
    return {
        "provider": _provider_namespace(conn),
        "name": cache.model_id,
        "provider_connection_id": str(conn.id),
        "provider_connection_name": conn.display_name,
        "provider_connection_type": conn.provider_type,
        "responses_route": _responses_route(conn),
        "source": source_str,
        "family": cache.family,
        "context_length": cache.context_length,
        "capabilities": capabilities,
        "visible": cache.visible,
        "upstream_present": cache.upstream_present,
        "last_seen_at": cache.last_seen_at.isoformat(),
        "agent_capable": classification.agent_capable,
        "recommended": recommended,
        "visibility": visibility,
        "hidden_reason": hidden_reason,
        "last_preflight_status": cache.last_preflight_status,
        "last_preflight_at": (
            cache.last_preflight_at.isoformat()
            if cache.last_preflight_at is not None else None
        ),
        "last_preflight_http_status": cache.last_preflight_http_status,
        "last_preflight_error_code": cache.last_preflight_error_code,
        "last_preflight_error_message": cache.last_preflight_error_message,
        "last_preflight_failure_kind": preflight_failure_kind(
            cache.last_preflight_status,
            cache.last_preflight_error_code,
            cache.last_preflight_http_status,
        ),
    }


@router.get("/models")
async def list_models(
    sc: SessionAndCtx,
    view: Literal["default", "raw"] = Query(default="default"),
) -> dict[str, Any]:
    """Return launch model metadata for the authenticated caller."""
    s, ctx = sc
    rows = (await s.execute(select(RateCard.table))).scalars().all()
    seen: set[tuple[str, str]] = set()
    items: list[dict[str, Any]] = []
    for card in rows:
        entries = (card or {}).get("entries") or []
        for e in entries:
            provider = e.get("provider")
            name = e.get("model")
            if not isinstance(provider, str) or not isinstance(name, str):
                continue
            if (provider, name) in seen:
                continue
            seen.add((provider, name))
            items.append({
                "provider": provider,
                "name": name,
                "source": "rate-card",
                "agent_capable": True,
                "recommended": True,
                "visibility": "default",
                "hidden_reason": None,
                "last_preflight_status": None,
                "last_preflight_at": None,
                "last_preflight_http_status": None,
                "last_preflight_error_code": None,
                "last_preflight_error_message": None,
                "last_preflight_failure_kind": None,
            })

    stmt = (
        select(ProviderConnection, ProviderModelCache)
        .join(
            ProviderModelCache,
            ProviderModelCache.provider_connection_id == ProviderConnection.id,
        )
        .where(ProviderConnection.deleted_at.is_(None))
    )
    if not is_admin(ctx):
        # Same visibility as GET /provider-connections: the team's own
        # connections plus those explicitly shared with it (#2054).
        shared_ids = select(ProviderConnectionShare.provider_connection_id).where(
            ProviderConnectionShare.target_team_id == ctx.team_id,
        )
        stmt = stmt.where(
            or_(
                ProviderConnection.team_id == ctx.team_id,
                ProviderConnection.id.in_(shared_ids),
            ),
        )
    byo_rows = (await s.execute(stmt)).all()
    for conn, cache in byo_rows:
        item = _byo_model_item(conn, cache)
        if view == "default" and not item["recommended"]:
            continue
        items.append(item)

    # Stable ordering: provider asc, name asc.
    items.sort(key=lambda x: (
        x.get("provider_connection_name") or "",
        x["provider"],
        x["name"],
    ))
    return {"items": items}
