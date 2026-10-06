"""#2054: hosted (Nebius native) execution cannot run every supported product
entry yet; the catalog and submission paths must say so truthfully."""

from __future__ import annotations

import pytest

from loom.models.batch import Combination
from loom.service_execution_backend import NEBIUS_BACKEND
from loom.service_execution_materialization import NATIVE_EXECUTION_AGENT_NAMES
from loom_service.agent_catalog import (
    native_execution_error,
    native_selections_error,
    selection_agents,
)


@pytest.mark.parametrize("name", ["direct-completion", "litellm", "terminus-2", "oracle", "codex"])
def test_natively_runnable_agents_have_no_error(name: str) -> None:
    assert native_execution_error(name) is None


@pytest.mark.parametrize("name", ["openhands-sdk", "openhands"])
def test_unconnected_agents_explain_why(name: str) -> None:
    err = native_execution_error(name)

    assert err is not None
    assert "not yet runnable on hosted (Nebius) execution" in err
    assert "#2054" in err


def test_alias_resolves_to_display_name() -> None:
    assert "'openhands'" in (native_execution_error("openhands-sdk") or "")


def test_unknown_agent_is_left_to_catalog_validation() -> None:
    assert native_execution_error("does-not-exist") is None


def test_native_set_is_the_admission_set() -> None:
    assert frozenset({"direct-completion", "litellm", "terminus-2", "oracle", "codex"}) == NATIVE_EXECUTION_AGENT_NAMES


def test_selection_agents_reads_models_dicts_and_single_selection() -> None:
    combos = [
        Combination(agent_name="terminus-2", agent_model=None),
        {"agent_name": "codex", "agent_model": None},
    ]

    assert selection_agents({}, combos) == [
        ("combinations[0]", "terminus-2"),
        ("combinations[1]", "codex"),
    ]
    assert selection_agents({}, combos, {1}) == [("combinations[1]", "codex")]
    assert selection_agents({"agent_name": "oracle"}, []) == [("trial_config", "oracle")]
    assert selection_agents({}, []) == []


def test_native_selections_error_names_the_first_blocked_selection() -> None:
    selections = [("combinations[0]", "oracle"), ("combinations[1]", "openhands")]

    err = native_selections_error(NEBIUS_BACKEND, selections)

    assert err is not None and err.startswith("combinations[1]: agent 'openhands'")


def test_other_backends_are_unaffected() -> None:
    assert native_selections_error("docker", [("trial_config", "openhands")]) is None
