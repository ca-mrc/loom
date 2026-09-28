"""Resolve each submitted agent/model selection to one provider route (#2054).

A selection names its model twice: `agent_model.name` is what the runtime
actually calls, while `provider_model_id` drives preflight, budget and
summaries. It may also inherit the batch-level connection/model defaults.
Resolving here, once, at the submission boundary means every later reader
(preflight, budget, persistence, fan-out, rerun, summaries) sees the same
connection and model.

Rules:
- A no-model agent (oracle) takes no provider fields at all.
- A model-backed agent must use an explicitly selected Provider
  Connection: its own, or the batch-level one inherited as a pair. There
  is no fallback to platform credentials.
- The effective model is always `agent_model.name`. A supplied
  `provider_model_id` (own or inherited) must equal it; omitted values
  are filled from it.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from pydantic import ValidationError

from loom.models.types import ModelSpec
from loom_service.agent_catalog import validate_agent_model_compat


class ProviderRouteError(ValueError):
    """The selection's provider fields conflict or are missing."""


@dataclass(frozen=True)
class ProviderRoute:
    connection_id: UUID | None
    model_id: str | None


def resolve_provider_route(
    *,
    context: str,
    agent_model: ModelSpec | None,
    connection_id: UUID | None,
    model_id: str | None,
    default_connection_id: UUID | None = None,
    default_model_id: str | None = None,
) -> ProviderRoute:
    """Return the one connection/model pair this selection runs on.

    `connection_id`/`model_id` are the selection's own fields; the
    `default_*` values are batch-level fields it may inherit. Raises
    ProviderRouteError with an actionable message naming `context`.
    """
    if agent_model is None:
        if connection_id is not None or model_id:
            raise ProviderRouteError(
                f"{context}: this agent does not take a model; remove "
                "provider_connection_id and provider_model_id",
            )
        return ProviderRoute(connection_id=None, model_id=None)

    effective_connection = connection_id or default_connection_id
    if effective_connection is None:
        raise ProviderRouteError(
            f"{context}: model {agent_model.name!r} requires a Provider "
            "Connection; set provider_connection_id to an OpenAI-compatible "
            "connection owned by or shared with your team",
        )
    supplied_model_id = model_id or default_model_id
    if supplied_model_id and supplied_model_id != agent_model.name:
        source = "provider_model_id" if model_id else "the batch-level provider_model_id"
        raise ProviderRouteError(
            f"{context}: agent_model.name {agent_model.name!r} conflicts with "
            f"{source} {supplied_model_id!r}; they must name the same model",
        )
    return ProviderRoute(connection_id=effective_connection, model_id=agent_model.name)


def stored_selection_error(
    selection: Mapping[str, Any],
    *,
    batch_connection_id: UUID | None,
) -> str | None:
    """Check a stored selection (a batch's `trial_config` or one of its
    `combinations`) against current submission policy before new trials
    are created from it by a rerun, clone or artifact reuse.

    Batches accepted under older rules stay readable, but new trials
    require a supported agent and, for model-backed agents, an explicit
    Provider Connection (the selection's own or the batch-level one).
    """
    agent_name = selection.get("agent_name")
    if not isinstance(agent_name, str) or not agent_name:
        return None
    model_raw = selection.get("agent_model")
    try:
        model = None if model_raw is None else ModelSpec.model_validate(model_raw)
    except ValidationError as exc:
        return f"agent_model failed to validate: {exc}"
    err = validate_agent_model_compat(agent_name, model)
    if err is not None:
        return err
    if model is not None and not (selection.get("provider_connection_id") or batch_connection_id):
        return (
            f"model {model.name!r} requires a Provider Connection; set "
            "provider_connection_id to an OpenAI-compatible connection owned by "
            "or shared with your team"
        )
    return None


__all__ = [
    "ProviderRoute",
    "ProviderRouteError",
    "resolve_provider_route",
    "stored_selection_error",
]
