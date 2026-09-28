"""Resolve each submitted agent/model selection to one provider route (#2054).

A selection names its model twice: `agent_model.name` is what the runtime
actually calls, while `provider_model_id` drives preflight, budget and
summaries. It may also inherit the batch-level connection/model defaults.
Resolving here, once, at the submission boundary means every later reader
(preflight, budget, persistence, fan-out, rerun, summaries) sees the same
connection and model.

Rules:
- A no-model agent (oracle) takes no provider fields at all.
- A model-backed agent inherits the batch-level connection when it names
  none of its own.
- The effective model is always `agent_model.name`. A supplied
  `provider_model_id` (own or inherited) must equal it; omitted values
  are filled from it.
"""

from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

from loom.models.types import ModelSpec


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

    # A missing connection still means the platform-credential route; making
    # a Provider Connection mandatory is a separate, pending decision.
    effective_connection = connection_id or default_connection_id
    supplied_model_id = model_id or default_model_id
    if supplied_model_id and supplied_model_id != agent_model.name:
        source = "provider_model_id" if model_id else "the batch-level provider_model_id"
        raise ProviderRouteError(
            f"{context}: agent_model.name {agent_model.name!r} conflicts with "
            f"{source} {supplied_model_id!r}; they must name the same model",
        )
    return ProviderRoute(connection_id=effective_connection, model_id=agent_model.name)


__all__ = ["ProviderRoute", "ProviderRouteError", "resolve_provider_route"]
