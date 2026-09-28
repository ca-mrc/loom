"""#2054: a failed-case rerun submits new trials, so each rerun selection
must pass current submission policy instead of inheriting retired choices."""

from __future__ import annotations

from uuid import uuid4

from loom_service.effective_combination import stored_selection_error

CONN = uuid4()


def test_supported_selection_passes() -> None:
    selection = {
        "agent_name": "terminus-2",
        "agent_model": {"provider": "openai", "name": "gpt-4o"},
    }

    assert stored_selection_error(selection, batch_connection_id=CONN) is None


def test_oracle_without_model_passes() -> None:
    assert stored_selection_error({"agent_name": "oracle", "agent_model": None}, batch_connection_id=CONN) is None


def test_deferred_agent_is_rejected() -> None:
    selection = {
        "agent_name": "swe-agent",
        "agent_model": {"provider": "openai", "name": "gpt-4o"},
    }

    err = stored_selection_error(selection, batch_connection_id=CONN)

    assert err is not None
    assert "not available for new submissions" in err


def test_retired_model_source_is_rejected() -> None:
    selection = {
        "agent_name": "direct-completion",
        "agent_model": {
            "provider": "local",
            "name": "llama3",
            "source": "local-server",
            "local_server": "ollama",
        },
    }

    err = stored_selection_error(selection, batch_connection_id=CONN)

    assert err is not None
    assert "retired" in err


def test_legacy_openhands_name_resolves_through_alias() -> None:
    selection = {
        "agent_name": "openhands",
        "agent_model": {"provider": "openai", "name": "gpt-4o"},
    }

    assert stored_selection_error(selection, batch_connection_id=CONN) is None


def test_malformed_model_is_rejected() -> None:
    selection = {"agent_name": "codex", "agent_model": {"provider": "openai"}}

    err = stored_selection_error(selection, batch_connection_id=CONN)

    assert err is not None
    assert "agent_model failed to validate" in err


def test_model_backed_selection_without_connection_is_rejected() -> None:
    selection = {
        "agent_name": "terminus-2",
        "agent_model": {"provider": "openai", "name": "gpt-4o"},
    }

    err = stored_selection_error(selection, batch_connection_id=None)

    assert err is not None
    assert "requires a Provider Connection" in err


def test_combination_connection_satisfies_the_requirement() -> None:
    selection = {
        "agent_name": "terminus-2",
        "agent_model": {"provider": "openai", "name": "gpt-4o"},
        "provider_connection_id": str(CONN),
    }

    assert stored_selection_error(selection, batch_connection_id=None) is None


def test_oracle_rerun_needs_no_connection() -> None:
    selection = {"agent_name": "oracle", "agent_model": None}

    assert stored_selection_error(selection, batch_connection_id=None) is None
