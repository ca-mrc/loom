"""#2054: every selection resolves to one connection/model pair at submission."""

from __future__ import annotations

from uuid import uuid4

import pytest

from loom.models.types import ModelSpec
from loom_service.effective_combination import (
    ProviderRoute,
    ProviderRouteError,
    resolve_provider_route,
)

CONN_A = uuid4()
CONN_B = uuid4()
GPT = ModelSpec(provider="openai", name="gpt-4o")


def test_own_fields_resolve_to_the_model_that_runs() -> None:
    route = resolve_provider_route(
        context="c",
        agent_model=GPT,
        connection_id=CONN_A,
        model_id="gpt-4o",
    )

    assert route == ProviderRoute(connection_id=CONN_A, model_id="gpt-4o")


def test_missing_provider_model_id_is_filled_from_agent_model() -> None:
    route = resolve_provider_route(
        context="c",
        agent_model=GPT,
        connection_id=CONN_A,
        model_id=None,
    )

    assert route.model_id == "gpt-4o"


def test_conflicting_own_model_is_rejected() -> None:
    with pytest.raises(ProviderRouteError, match="conflicts with provider_model_id 'gpt-4o-mini'"):
        resolve_provider_route(
            context="combinations[0]",
            agent_model=GPT,
            connection_id=CONN_A,
            model_id="gpt-4o-mini",
        )


def test_conflicting_inherited_model_is_rejected() -> None:
    with pytest.raises(ProviderRouteError, match="batch-level provider_model_id 'gpt-4o-mini'"):
        resolve_provider_route(
            context="combinations[1]",
            agent_model=GPT,
            connection_id=None,
            model_id=None,
            default_connection_id=CONN_A,
            default_model_id="gpt-4o-mini",
        )


def test_selection_inherits_the_batch_connection() -> None:
    route = resolve_provider_route(
        context="c",
        agent_model=GPT,
        connection_id=None,
        model_id=None,
        default_connection_id=CONN_A,
        default_model_id="gpt-4o",
    )

    assert route == ProviderRoute(connection_id=CONN_A, model_id="gpt-4o")


def test_own_connection_overrides_the_batch_connection() -> None:
    route = resolve_provider_route(
        context="c",
        agent_model=GPT,
        connection_id=CONN_B,
        model_id=None,
        default_connection_id=CONN_A,
    )

    assert route.connection_id == CONN_B


def test_no_connection_keeps_the_platform_route() -> None:
    route = resolve_provider_route(
        context="c",
        agent_model=GPT,
        connection_id=None,
        model_id=None,
    )

    assert route == ProviderRoute(connection_id=None, model_id="gpt-4o")


def test_no_model_agent_takes_no_provider_fields() -> None:
    route = resolve_provider_route(
        context="c",
        agent_model=None,
        connection_id=None,
        model_id=None,
        default_connection_id=CONN_A,
        default_model_id="gpt-4o",
    )

    assert route == ProviderRoute(connection_id=None, model_id=None)


@pytest.mark.parametrize(
    ("connection_id", "model_id"),
    [(CONN_A, None), (None, "gpt-4o"), (CONN_A, "gpt-4o")],
)
def test_no_model_agent_rejects_stale_provider_fields(connection_id, model_id) -> None:
    with pytest.raises(ProviderRouteError, match="does not take a model"):
        resolve_provider_route(
            context="combinations[0]",
            agent_model=None,
            connection_id=connection_id,
            model_id=model_id,
        )
