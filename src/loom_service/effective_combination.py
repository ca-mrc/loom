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


@dataclass(frozen=True)
class DerivedRoutes:
    """Resolved routes for new trials derived from a stored batch."""

    combinations: list[dict[str, Any]]
    batch_connection_id: UUID | None
    batch_model_id: str | None
    # Connections the new trials run on; only these need authorization.
    connection_ids: frozenset[UUID]


def _stored_uuid(value: Any) -> UUID | None:
    if value is None or value == "":
        return None
    return value if isinstance(value, UUID) else UUID(str(value))


def _stored_model(selection: Mapping[str, Any], context: str) -> ModelSpec | None:
    model_raw = selection.get("agent_model")
    try:
        return None if model_raw is None else ModelSpec.model_validate(model_raw)
    except ValidationError as exc:
        raise ProviderRouteError(f"{context}: agent_model failed to validate: {exc}") from exc


def derive_stored_routes(
    *,
    trial_config: Mapping[str, Any],
    combinations: list[dict[str, Any]],
    source_connection_id: UUID | None,
    source_model_id: str | None,
    replacement_connection_id: UUID | None = None,
    replacement_model_id: str | None = None,
    replace_connections: bool = False,
    indices: set[int] | None = None,
) -> DerivedRoutes:
    """Resolve the route of every stored selection new trials come from.

    A rerun, clone or artifact reuse creates new trials from a stored
    batch's `trial_config`/`combinations`, so each selection goes through
    the same contract as a fresh submission (#2054): a supported agent, a
    Provider Connection for model-backed agents, `provider_model_id` equal
    to `agent_model.name`, and no provider fields on no-model agents. The
    stored batch itself is never changed.

    With `replace_connections` (clone and reuse), every model-backed
    selection runs on `replacement_connection_id`; source connections are
    never carried into the new batch. When the source spread its
    selections across several connections, one replacement would silently
    merge them, so that is rejected. `replacement_model_id`, when given,
    must name each model-backed selection's model. `indices` limits a
    rerun to the combinations it re-dispatches. Raises ProviderRouteError.
    """
    selections = [(f"combinations[{i}]", c) for i, c in enumerate(combinations)] or [
        ("trial_config", dict(trial_config)),
    ]
    if indices is not None and combinations:
        selections = [(ctx, c) for i, (ctx, c) in enumerate(selections) if i in indices]

    parsed = [(ctx, c, _stored_model(c, ctx)) for ctx, c in selections]
    model_backed = [(ctx, c) for ctx, c, model in parsed if model is not None]
    if replace_connections and model_backed:
        if replacement_connection_id is None:
            raise ProviderRouteError(
                "select a provider_connection_id owned by or shared with your "
                "team; the source batch's connections are not reused",
            )
        source_connections = {
            _stored_uuid(c.get("provider_connection_id")) or source_connection_id
            for _, c in model_backed
        } - {None}
        if len(source_connections) > 1:
            raise ProviderRouteError(
                f"the source batch runs on {len(source_connections)} different "
                "Provider Connections; one provider_connection_id cannot replace "
                "them without merging its comparisons. Submit a new batch that "
                "sets a connection per combination",
            )

    routes: dict[str, ProviderRoute] = {}
    for ctx, selection, model in parsed:
        agent_name = selection.get("agent_name")
        if not isinstance(agent_name, str) or not agent_name:
            # Legacy shape without an agent selection: nothing to resolve.
            routes[ctx] = ProviderRoute(
                connection_id=replacement_connection_id or source_connection_id,
                model_id=replacement_model_id or source_model_id,
            )
            continue
        err = validate_agent_model_compat(agent_name, model)
        if err is not None:
            raise ProviderRouteError(f"{ctx}: {err}")
        own_connection = _stored_uuid(selection.get("provider_connection_id"))
        own_model = selection.get("provider_model_id") or None
        default_connection, default_model = source_connection_id, source_model_id
        if model is not None and replace_connections:
            own_connection, default_connection = replacement_connection_id, None
        if model is not None and replacement_model_id:
            own_model, default_model = replacement_model_id, None
        routes[ctx] = resolve_provider_route(
            context=ctx,
            agent_model=model,
            connection_id=own_connection,
            model_id=own_model,
            default_connection_id=default_connection if model is not None else None,
            default_model_id=default_model if model is not None else None,
        )

    resolved = list(routes.values())
    connections = frozenset(r.connection_id for r in resolved if r.connection_id is not None)
    if not combinations:
        route = routes["trial_config"]
        return DerivedRoutes(
            combinations=[],
            batch_connection_id=route.connection_id,
            batch_model_id=route.model_id,
            connection_ids=connections,
        )
    derived: list[dict[str, Any]] = []
    for i, combo in enumerate(combinations):
        combo_route = routes.get(f"combinations[{i}]")
        if combo_route is None:
            derived.append(dict(combo))
            continue
        derived.append(
            {
                **combo,
                "provider_connection_id": (
                    str(combo_route.connection_id) if combo_route.connection_id else None
                ),
                "provider_model_id": combo_route.model_id,
            },
        )
    models = {r.model_id for r in resolved if r.model_id is not None}
    # Batch-level fields summarize the route only when it is unique; every
    # dispatched combination carries its own resolved pair.
    return DerivedRoutes(
        combinations=derived,
        batch_connection_id=next(iter(connections)) if len(connections) == 1 else None,
        batch_model_id=next(iter(models)) if len(models) == 1 else None,
        connection_ids=connections,
    )


__all__ = [
    "DerivedRoutes",
    "ProviderRoute",
    "ProviderRouteError",
    "derive_stored_routes",
    "resolve_provider_route",
]
