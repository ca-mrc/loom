"""Validate a `provider_connection_id` on a submitted Trial / Batch
payload.

The Trial / Batch routes accept an optional `provider_connection_id`.
The user supplies a UUID; the route must check that:

1. The UUID parses (handled by Pydantic before this is called).
2. The row exists.
3. The row belongs to the caller's team or is explicitly shared with
   that team (unshared cross-team → 404, matching provider route +
   gateway existence-hiding behavior).
4. The row is not soft-deleted (deleted_at IS NULL).
5. For agent submissions, the row speaks a protocol accepted by every
   selection using it. Claude Code also accepts native Anthropic Messages.

Centralized here so the Trial and Batch routes use the same shape +
error messages.
"""

from __future__ import annotations

from typing import Any
from uuid import UUID

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import ProviderConnection, ProviderConnectionShare
from loom.provider_connection_routes import ProviderConnectionRouting
from loom_service.agent_catalog import get_agent

# Provider types the gateway routes with the OpenAI wire protocol (see
# loom_llm_gateway.routes.responses._OPENAI_SHAPED_TYPES).
OPENAI_SHAPED_PROVIDER_TYPES = frozenset({"openai-compatible", "custom"})


def effective_provider_type(
    config: dict[str, Any], routing: str | ProviderConnectionRouting,
) -> str:
    if isinstance(routing, str):
        return routing
    selected = config.get("agent_model")
    model = selected.get("name") if isinstance(selected, dict) else None
    agent_name = config.get("agent_name")
    mode = config.get("claude_code_mode")
    if agent_name == "claude-code":
        if mode != "chat" and routing.verified_alternate("anthropic_messages", model):
            return "anthropic"
        if mode == "chat" and routing.verified_alternate("openai_chat", model):
            return "openai-compatible"
    elif routing.verified_alternate("openai_chat", model):
        agent = get_agent(agent_name) if isinstance(agent_name, str) else None
        if agent and agent.runtime_contract.endpoint_dialect == "openai_chat":
            return "openai-compatible"
    return routing.primary_type


def connection_runtime_config(
    config: dict[str, Any], provider_type: str | ProviderConnectionRouting,
) -> dict[str, Any]:
    """Derive the runtime protocol from a connection already authorized to the team."""
    agent = get_agent(config.get("agent_name", ""))
    provider_type = effective_provider_type(config, provider_type)
    supported = agent.supported_connection_types if agent else OPENAI_SHAPED_PROVIDER_TYPES
    if provider_type not in supported:
        raise HTTPException(status_code=400, detail=(
            f"agent {config.get('agent_name')!r} cannot use {provider_type!r}; "
            f"select a compatible Provider Connection ({', '.join(sorted(supported))}). "
            "OpenAI-compatible connections remain supported."
        ))
    out = dict(config)
    if config.get("agent_name") == "claude-code":
        if config.get("request_params"):
            raise HTTPException(status_code=400, detail="Claude Code does not support request_params overrides")
        mode = "native" if provider_type == "anthropic" else "chat"
        if config.get("claude_code_mode") not in {None, mode}:
            raise HTTPException(status_code=400, detail="claude_code_mode conflicts with the selected connection")
        out["claude_code_mode"] = mode
    elif config.get("claude_code_mode") is not None:
        raise HTTPException(status_code=400, detail="claude_code_mode requires claude-code")
    return out


async def validate_provider_connection(
    session: AsyncSession,
    provider_connection_id: UUID,
    *,
    team_id: UUID,
    agent_submission: bool = False,
    submission_configs: list[dict[str, Any]] | None = None,
) -> str | ProviderConnectionRouting:
    """Raise HTTPException if the connection is missing, soft-deleted,
    or owned by a different team. With `agent_submission`, also reject
    connections incompatible with the selected agents. Returns the authorized
    provider type for freezing per-selection runtime modes."""
    row = (await session.execute(
        select(
            ProviderConnection.team_id,
            ProviderConnection.deleted_at,
            ProviderConnection.provider_type,
            ProviderConnection.base_url,
            ProviderConnection.alternate_route,
        ).where(ProviderConnection.id == provider_connection_id),
    )).one_or_none()
    if row is None:
        raise HTTPException(
            status_code=400,
            detail=(
                f"provider_connection {provider_connection_id} not found. "
                f"Run `loom providers list` to see what's available."
            ),
        )
    found_team_id, deleted_at, provider_type, base_url, alternate = row
    if deleted_at is not None:
        raise HTTPException(
            status_code=400,
            detail=(
                f"provider_connection {provider_connection_id} has been "
                f"deleted. Re-create with `loom providers create` or "
                f"pick a different one."
            ),
        )
    if found_team_id != team_id:
        shared = (await session.execute(
            select(ProviderConnectionShare.provider_connection_id).where(
                ProviderConnectionShare.provider_connection_id == provider_connection_id,
                ProviderConnectionShare.target_team_id == team_id,
            ),
        )).scalar_one_or_none()
        if shared is None:
            raise HTTPException(
                status_code=404, detail="provider_connection not found",
            )
    routing = ProviderConnectionRouting.from_storage(provider_type, base_url, alternate)
    authorized_routing = routing if routing.alternate else str(provider_type)
    if agent_submission:
        for config in submission_configs or [{}]:
            connection_runtime_config(config, authorized_routing)

    # Pool selection keeps the existing logical connection id, while every
    # physical member must satisfy the new submission's agent/model contract.
    from loom_service.provider_pool_validation import validate_request_pool_submission

    await validate_request_pool_submission(
        session, provider_connection_id, team_id=team_id,
        submission_configs=submission_configs,
    )
    return authorized_routing
