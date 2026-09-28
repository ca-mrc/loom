"""Fan-out reads each combination's provider route (#2054)."""

from __future__ import annotations

from types import SimpleNamespace
from uuid import uuid4

from loom_service.batch_runner import _effective_provider_fields

BATCH_CONN = uuid4()
COMBO_CONN = uuid4()


def _batch() -> SimpleNamespace:
    return SimpleNamespace(provider_connection_id=BATCH_CONN, provider_model_id="gpt-4o")


def test_resolved_combination_route_is_used_as_is() -> None:
    combination = {
        "agent_model": {"provider": "openai", "name": "gpt-4o-mini"},
        "provider_connection_id": str(COMBO_CONN),
        "provider_model_id": "gpt-4o-mini",
    }

    assert _effective_provider_fields(_batch(), combination) == (COMBO_CONN, "gpt-4o-mini")


def test_legacy_combination_inherits_batch_route() -> None:
    combination = {"agent_model": {"provider": "openai", "name": "gpt-4o"}}

    assert _effective_provider_fields(_batch(), combination) == (BATCH_CONN, "gpt-4o")


def test_no_model_combination_never_inherits_batch_route() -> None:
    combination = {"agent_name": "oracle", "agent_model": None}

    assert _effective_provider_fields(_batch(), combination) == (None, None)


def test_single_combination_batch_uses_batch_route() -> None:
    assert _effective_provider_fields(_batch(), None) == (BATCH_CONN, "gpt-4o")
