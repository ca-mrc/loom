"""#2054: rerun, clone and artifact reuse create new trials from a stored
batch, so each stored selection goes through the fresh-submission contract."""

from __future__ import annotations

from uuid import uuid4

import pytest

from loom_service.effective_combination import ProviderRouteError, derive_stored_routes

CONN_A = uuid4()
CONN_B = uuid4()
CONN_C = uuid4()
GPT = {"provider": "openai", "name": "gpt-4o"}
MINI = {"provider": "openai", "name": "gpt-4o-mini"}


def _combo(model: dict | None, *, agent: str = "terminus-2", conn=None, model_id=None) -> dict:
    return {
        "agent_name": agent,
        "agent_model": model,
        "n_per_task": 1,
        "provider_connection_id": str(conn) if conn else None,
        "provider_model_id": model_id,
    }


# --- rerun: keeps the source routes, validated ---------------------------


def test_rerun_keeps_a_supported_single_selection_route() -> None:
    routes = derive_stored_routes(
        trial_config={"agent_name": "terminus-2", "agent_model": GPT},
        combinations=[],
        source_connection_id=CONN_A,
        source_model_id="gpt-4o",
    )

    assert (routes.batch_connection_id, routes.batch_model_id) == (CONN_A, "gpt-4o")
    assert routes.connection_ids == {CONN_A}


def test_rerun_resolves_legacy_inherited_combination_and_persists_it() -> None:
    routes = derive_stored_routes(
        trial_config={},
        combinations=[_combo(GPT)],
        source_connection_id=CONN_A,
        source_model_id=None,
    )

    assert routes.combinations[0]["provider_connection_id"] == str(CONN_A)
    assert routes.combinations[0]["provider_model_id"] == "gpt-4o"


def test_rerun_rejects_an_old_conflicting_selection() -> None:
    with pytest.raises(ProviderRouteError, match="conflicts with provider_model_id 'gpt-4o-mini'"):
        derive_stored_routes(
            trial_config={},
            combinations=[_combo(GPT, conn=CONN_A, model_id="gpt-4o-mini")],
            source_connection_id=None,
            source_model_id=None,
        )


def test_rerun_rejects_selection_that_conflicts_with_old_batch_model() -> None:
    with pytest.raises(ProviderRouteError, match="batch-level provider_model_id"):
        derive_stored_routes(
            trial_config={"agent_name": "terminus-2", "agent_model": GPT},
            combinations=[],
            source_connection_id=CONN_A,
            source_model_id="gpt-4o-mini",
        )


def test_rerun_resolves_only_redispatched_combinations() -> None:
    routes = derive_stored_routes(
        trial_config={},
        combinations=[
            _combo(GPT, agent="swe-agent", conn=CONN_A),  # deferred, not rerun
            _combo(MINI, conn=CONN_B),
        ],
        source_connection_id=None,
        source_model_id=None,
        indices={1},
    )

    assert routes.combinations[0]["agent_name"] == "swe-agent"
    assert routes.connection_ids == {CONN_B}
    assert routes.batch_connection_id == CONN_B


def test_rerun_rejects_deferred_agent() -> None:
    with pytest.raises(ProviderRouteError, match="not available for new submissions"):
        derive_stored_routes(
            trial_config={"agent_name": "swe-agent", "agent_model": GPT},
            combinations=[],
            source_connection_id=CONN_A,
            source_model_id=None,
        )


def test_rerun_rejects_model_backed_selection_without_connection() -> None:
    with pytest.raises(ProviderRouteError, match="requires a Provider Connection"):
        derive_stored_routes(
            trial_config={"agent_name": "terminus-2", "agent_model": GPT},
            combinations=[],
            source_connection_id=None,
            source_model_id=None,
        )


def test_rerun_rejects_oracle_with_stale_provider_fields() -> None:
    with pytest.raises(ProviderRouteError, match="does not take a model"):
        derive_stored_routes(
            trial_config={},
            combinations=[_combo(None, agent="oracle", conn=CONN_A)],
            source_connection_id=None,
            source_model_id=None,
        )


def test_oracle_needs_no_connection() -> None:
    routes = derive_stored_routes(
        trial_config={"agent_name": "oracle", "agent_model": None},
        combinations=[],
        source_connection_id=CONN_A,
        source_model_id=None,
    )

    assert (routes.batch_connection_id, routes.batch_model_id) == (None, None)


def test_legacy_openhands_name_resolves_through_alias() -> None:
    routes = derive_stored_routes(
        trial_config={"agent_name": "openhands", "agent_model": GPT},
        combinations=[],
        source_connection_id=CONN_A,
        source_model_id=None,
    )

    assert routes.batch_connection_id == CONN_A


# --- clone / reuse: the selected connection replaces the source's --------


def test_clone_replaces_combination_connection_a_with_b() -> None:
    routes = derive_stored_routes(
        trial_config={},
        combinations=[_combo(GPT, conn=CONN_A, model_id="gpt-4o"), _combo(None, agent="oracle")],
        source_connection_id=None,
        source_model_id=None,
        replacement_connection_id=CONN_B,
        replace_connections=True,
    )

    assert routes.combinations[0]["provider_connection_id"] == str(CONN_B)
    assert routes.combinations[0]["provider_model_id"] == "gpt-4o"
    assert routes.combinations[1]["provider_connection_id"] is None
    assert routes.connection_ids == {CONN_B}
    assert routes.batch_connection_id == CONN_B


def test_clone_replaces_single_selection_connection() -> None:
    routes = derive_stored_routes(
        trial_config={"agent_name": "terminus-2", "agent_model": GPT},
        combinations=[],
        source_connection_id=CONN_A,
        source_model_id="gpt-4o",
        replacement_connection_id=CONN_B,
        replace_connections=True,
    )

    assert routes.connection_ids == {CONN_B}


def test_clone_requires_a_selected_connection() -> None:
    with pytest.raises(ProviderRouteError, match="select a provider_connection_id"):
        derive_stored_routes(
            trial_config={"agent_name": "terminus-2", "agent_model": GPT},
            combinations=[],
            source_connection_id=CONN_A,
            source_model_id=None,
            replace_connections=True,
        )


def test_clone_of_oracle_only_batch_needs_no_connection() -> None:
    routes = derive_stored_routes(
        trial_config={"agent_name": "oracle", "agent_model": None},
        combinations=[],
        source_connection_id=None,
        source_model_id=None,
        replacement_connection_id=CONN_B,
        replace_connections=True,
    )

    assert routes.connection_ids == set()


def test_clone_rejects_ambiguous_multi_connection_remapping() -> None:
    with pytest.raises(ProviderRouteError, match="2 different Provider Connections"):
        derive_stored_routes(
            trial_config={},
            combinations=[_combo(GPT, conn=CONN_A), _combo(GPT, conn=CONN_B)],
            source_connection_id=None,
            source_model_id=None,
            replacement_connection_id=CONN_C,
            replace_connections=True,
        )


def test_clone_rejects_conflicting_requested_model() -> None:
    with pytest.raises(ProviderRouteError, match="conflicts with provider_model_id 'gpt-4o-mini'"):
        derive_stored_routes(
            trial_config={"agent_name": "terminus-2", "agent_model": GPT},
            combinations=[],
            source_connection_id=CONN_A,
            source_model_id="gpt-4o",
            replacement_connection_id=CONN_B,
            replacement_model_id="gpt-4o-mini",
            replace_connections=True,
        )
