"""Gateway ledger unit tests (#744 Gate 2)."""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest

from loom.agent.terminus2.gateway_ledger import (
    CheckpointBridgeError,
    GatewayCallLedger,
    harbor_metrics_tokens,
)


def test_harbor_metrics_tokens_normalizes_prompt_and_completion() -> None:
    assert harbor_metrics_tokens(
        {"prompt_tokens": 10, "completion_tokens": 5},
    ) == (10, 5)
    assert harbor_metrics_tokens(
        {"input_tokens": 3, "output_tokens": 2},
    ) == (3, 2)


@pytest.mark.asyncio
async def test_gateway_ledger_resolves_unique_llm_call_row() -> None:
    trial_id = uuid4()
    ledger = GatewayCallLedger(trial_id=trial_id, step_id="agent")

    class _Cp:
        async def get_trial_llm_calls(self, tid: UUID) -> list[dict[str, object]]:
            assert tid == trial_id
            return [
                {
                    "id": "gw-real-1",
                    "step_id": "agent",
                    "input_tokens": 10,
                    "output_tokens": 5,
                    "dialect": "openai_chat",
                    "model": "gpt-4",
                    "cost_usd": 0.01,
                    "rate_card_hash": "abc",
                    "captured_at": "2026-07-10T00:00:00Z",
                },
            ]

    await ledger.refresh(_Cp())
    row = ledger.resolve_for_metrics({"prompt_tokens": 10, "completion_tokens": 5})
    assert row["id"] == "gw-real-1"


@pytest.mark.asyncio
async def test_gateway_ledger_fail_closed_on_missing_row() -> None:
    ledger = GatewayCallLedger(trial_id=uuid4(), step_id="agent")

    class _Cp:
        async def get_trial_llm_calls(self, trial_id: UUID) -> list[dict[str, object]]:
            return []

    await ledger.refresh(_Cp())
    with pytest.raises(CheckpointBridgeError, match="no llm_calls row"):
        ledger.resolve_for_metrics({"prompt_tokens": 1, "completion_tokens": 1})


@pytest.mark.asyncio
async def test_gateway_ledger_fail_closed_on_ambiguous_row() -> None:
    ledger = GatewayCallLedger(trial_id=uuid4(), step_id="agent")

    class _Cp:
        async def get_trial_llm_calls(self, trial_id: UUID) -> list[dict[str, object]]:
            return [
                {
                    "id": "a",
                    "step_id": "agent",
                    "input_tokens": 1,
                    "output_tokens": 1,
                },
                {
                    "id": "b",
                    "step_id": "agent",
                    "input_tokens": 1,
                    "output_tokens": 1,
                },
            ]

    await ledger.refresh(_Cp())
    with pytest.raises(CheckpointBridgeError, match="ambiguous"):
        ledger.resolve_for_metrics({"prompt_tokens": 1, "completion_tokens": 1})


def _correlated_row(
    name: str, ordinal: int, *, input_tokens: int = 100, output_tokens: int = 10,
    episode: int = 2,
) -> dict[str, object]:
    return {
        "id": name,
        "client_call_id": f"client-{name}",
        "step_id": "agent",
        "correlation_status": "correlated",
        "episode": episode,
        "call_ordinal": ordinal,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
    }


async def _ledger_with_rows(rows: list[dict[str, object]]) -> GatewayCallLedger:
    class _Cp:
        async def get_trial_llm_calls(self, trial_id: UUID) -> list[dict[str, object]]:
            return rows

    ledger = GatewayCallLedger(trial_id=uuid4(), step_id="agent")
    await ledger.refresh(_Cp())
    return ledger


@pytest.mark.asyncio
@pytest.mark.parametrize("reverse", [False, True])
async def test_correlated_summary_calls_do_not_steal_main_turn_join(reverse: bool) -> None:
    rows = [
        _correlated_row("summary", 2, input_tokens=102, output_tokens=12),
        _correlated_row("questions", 3, input_tokens=103, output_tokens=13),
        _correlated_row("answers", 4, input_tokens=104, output_tokens=14),
        _correlated_row("main", 5, input_tokens=105, output_tokens=15),
    ]
    ledger = await _ledger_with_rows(list(reversed(rows)) if reverse else rows)
    row = ledger.resolve_for_metrics({"prompt_tokens": 105, "completion_tokens": 15}, episode=2)
    assert row["id"] == "main"


@pytest.mark.asyncio
@pytest.mark.parametrize("matching_calls", [0, 2])
async def test_multiple_correlated_calls_require_unique_metrics_match(matching_calls: int) -> None:
    rows = [
        _correlated_row("summary", 2),
        _correlated_row("questions", 3),
        *[
            _correlated_row(f"matching-{index}", 4 + index, input_tokens=105, output_tokens=15)
            for index in range(matching_calls)
        ],
        # Neither a different correlated episode nor a legacy row may rescue
        # an absent or ambiguous join inside the requested episode.
        _correlated_row("other-episode", 6, input_tokens=105, output_tokens=15, episode=3),
        {"id": "legacy", "step_id": "agent", "input_tokens": 105, "output_tokens": 15},
    ]
    ledger = await _ledger_with_rows(rows)
    message = "ambiguous correlated" if matching_calls else "no correlated.*metrics"
    with pytest.raises(CheckpointBridgeError, match=message):
        ledger.resolve_for_metrics({"prompt_tokens": 105, "completion_tokens": 15}, episode=2)
    # Rejection must leave the candidate rows available for an explicit join.
    row = ledger.resolve_for_metrics({}, client_call_id="client-summary")
    assert row["id"] == "summary"


@pytest.mark.asyncio
async def test_single_correlated_episode_keeps_authoritative_join_without_metrics_match() -> None:
    ledger = await _ledger_with_rows([
        _correlated_row("main", 1),
        {"id": "legacy", "step_id": "agent", "input_tokens": 105, "output_tokens": 15},
    ])
    row = ledger.resolve_for_metrics({"prompt_tokens": 105, "completion_tokens": 15}, episode=2)
    assert row["id"] == "main"


@pytest.mark.asyncio
async def test_explicit_client_call_id_precedes_episode_and_metrics_disambiguation() -> None:
    ledger = await _ledger_with_rows([
        _correlated_row("summary", 2),
        _correlated_row("main", 5, input_tokens=105, output_tokens=15),
    ])
    row = ledger.resolve_for_metrics(
        {"prompt_tokens": 105, "completion_tokens": 15},
        episode=3, client_call_id="client-summary",
    )
    assert row["id"] == "summary"
