"""Submission admission for request-level provider rotation pools."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, NoReturn
from uuid import UUID

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import (
    ProviderConnection,
    ProviderConnectionShare,
    ProviderModelCache,
    ProviderRequestPool,
    ProviderRequestPoolMember,
)
from loom.provider_model_preflight import preflight_failure_kind


def provider_submission_configs(
    *,
    trial_config: Mapping[str, Any],
    combinations: list[dict[str, Any]],
    connection_id: UUID,
    indices: set[int] | None = None,
) -> list[dict[str, Any]]:
    """Materialize each resolved selection using this logical connection.

    Callers resolve connection/model inheritance first. Keeping the common
    trial controls while overlaying a combination's agent and model matches
    the batch fan-out contract; reruns validate only selected combinations.
    """
    if not combinations:
        return [dict(trial_config)]
    return [
        {
            **trial_config,
            "agent_name": combo.get("agent_name"),
            "agent_model": combo.get("agent_model"),
        }
        for index, combo in enumerate(combinations)
        if (indices is None or index in indices)
        and str(combo.get("provider_connection_id")) == str(connection_id)
    ]


def _reject(detail: str, *, status_code: int = 400) -> NoReturn:
    raise HTTPException(status_code=status_code, detail=detail)


def _selection_models(config: Mapping[str, Any]) -> set[str]:
    if config.get("agent_name") != "terminus-2":
        _reject("provider request pools support only hosted terminus-2 submissions")
    # New submissions cannot select these modes in the typed TrialConfig,
    # but stored legacy/replay configs must not bypass the pool boundary.
    sources = [config]
    for key in ("request_params", "harbor_config", "agent_kwargs", "agent_config"):
        value = config.get(key)
        if isinstance(value, Mapping):
            sources.append(value)
    forbidden = {
        "previous_response_id",
        "conversation",
        "conversation_id",
        "file_id",
        "vector_store_ids",
        "assistant_id",
        "thread_id",
    }
    if any(source.get("stream") or source.get("use_responses_api") for source in sources):
        _reject("provider request pools require non-streaming complete-messages Chat")
    if any(any(source.get(key) is not None for key in forbidden) for source in sources):
        _reject("provider request pools do not support upstream conversation/resource references")
    models: set[str] = set()
    primary = config.get("agent_model")
    if not isinstance(primary, Mapping) or not isinstance(primary.get("name"), str):
        _reject("provider request pool submission requires a concrete agent model")
    models.add(primary["name"])
    multi = config.get("multi_model")
    secondary = multi.get("secondary_model") if isinstance(multi, Mapping) else None
    if isinstance(multi, Mapping) and multi.get("enabled"):
        if isinstance(secondary, Mapping) and isinstance(secondary.get("name"), str):
            models.add(secondary["name"])
        else:
            _reject("provider request pool submission requires the complete teacher model")
    if not models or "" in models:
        _reject("provider request pool submission requires a concrete agent model")
    return models


async def validate_request_pool_submission(
    session: AsyncSession,
    connection_id: UUID,
    *,
    team_id: UUID,
    submission_configs: list[dict[str, Any]] | None,
) -> None:
    """Validate all models/members without changing ordinary connections.

    A pool is a logical connection selected by the existing submission wire
    contract. Every enabled member must remain authorized and able to serve
    the complete declared model set; a partial pool is never silently used.
    Temporary cooldown is runtime availability, not submission incompatibility.
    """
    pool = await session.get(ProviderRequestPool, connection_id)
    if pool is None or not pool.enabled:
        return
    if not submission_configs:
        _reject("provider request pools require an explicit hosted terminus-2 selection")
    required = set(pool.required_models or [])
    if not required:
        _reject("provider request pool has no declared required models")
    for config in submission_configs:
        missing = _selection_models(config) - required
        if missing:
            _reject(f"provider request pool does not declare selected models: {sorted(missing)}")
    members = (
        (
            await session.execute(
                select(ProviderRequestPoolMember)
                .where(
                    ProviderRequestPoolMember.pool_connection_id == connection_id,
                    ProviderRequestPoolMember.enabled.is_(True),
                )
                .order_by(ProviderRequestPoolMember.ordinal)
            )
        )
        .scalars()
        .all()
    )
    if not members:
        _reject("provider request pool has no enabled members")
    for member in members:
        row = await session.get(ProviderConnection, member.connection_id)
        if row is None or row.deleted_at is not None:
            _reject("provider request pool member is unavailable")
        if row.team_id != team_id:
            shared = await session.get(ProviderConnectionShare, (row.id, team_id))
            if shared is None:
                _reject(
                    "provider request pool member is not available to this team", status_code=404
                )
        if row.provider_type not in {"openai-compatible", "custom"} or row.status != "valid":
            _reject("provider request pool members must be valid OpenAI-compatible connections")
        if row.allowed_models and required - set(row.allowed_models):
            _reject("provider request pool member does not allow the complete required model set")
        cache = (
            (
                await session.execute(
                    select(ProviderModelCache).where(
                        ProviderModelCache.provider_connection_id == row.id,
                        ProviderModelCache.model_id.in_(required),
                        ProviderModelCache.visible.is_(True),
                        ProviderModelCache.upstream_present.is_(True),
                    )
                )
            )
            .scalars()
            .all()
        )
        if {model.model_id for model in cache} != required:
            _reject("provider request pool member lacks a visible complete required model cache")
        if any(
            preflight_failure_kind(
                model.last_preflight_status,
                model.last_preflight_error_code,
                model.last_preflight_http_status,
            )
            == "rejected"
            for model in cache
        ):
            _reject("provider request pool member has a rejected required model preflight")
