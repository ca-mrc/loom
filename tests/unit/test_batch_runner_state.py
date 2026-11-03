"""next_batch_state state-machine rules (Plan 19 Task 4)."""

from __future__ import annotations

from types import SimpleNamespace

from loom_service.batch_runner import _compute_result_status, next_batch_state
from loom_service.routes.batches import _empty_trial_summary, _result_status_from_trials


def _empty() -> dict[str, int]:
    return {
        "queued": 0, "claimed": 0, "running": 0,
        "succeeded": 0, "failed": 0, "cancelled": 0,
    }


def test_no_trials_yet_remains_submitted() -> None:
    assert next_batch_state(
        current="submitted", expected=5, counts=_empty(),
    ) == "submitted"


def test_some_in_flight_running() -> None:
    counts = _empty() | {"queued": 1, "claimed": 1, "succeeded": 1}
    assert next_batch_state(
        current="submitted", expected=5, counts=counts,
    ) == "running"


def test_all_terminal_finished() -> None:
    counts = _empty() | {"succeeded": 2, "failed": 1}
    assert next_batch_state(
        current="running", expected=3, counts=counts,
    ) == "finished"


def test_terminal_cancelled_counts_toward_finished() -> None:
    """A cancelled trial is terminal — a batch whose expected
    count is fully accounted for by succeeded+failed+cancelled is
    finished."""
    counts = _empty() | {"succeeded": 1, "cancelled": 2}
    assert next_batch_state(
        current="running", expected=3, counts=counts,
    ) == "finished"


def test_cancelled_is_absorbing() -> None:
    """No matter what trial counts say, a cancelled batch stays
    cancelled."""
    counts = _empty() | {"succeeded": 1, "running": 1}
    assert next_batch_state(
        current="cancelled", expected=3, counts=counts,
    ) == "cancelled"


def test_in_flight_blocks_finished_even_if_terminal_count_reaches_expected() -> None:
    """3 expected, 3 succeeded, but 1 still queued — the queued one
    must drain (likely a stragglers-from-retry case) before finished."""
    counts = _empty() | {"succeeded": 3, "queued": 1}
    assert next_batch_state(
        current="running", expected=3, counts=counts,
    ) == "running"


def test_zero_expected_stays_submitted_with_no_trials() -> None:
    """Edge: a batch whose task_filter matched zero tasks shouldn't
    auto-finish on the first tick — the operator likely intends to
    re-edit the filter. This is a defensive flag for the rollup."""
    assert next_batch_state(
        current="submitted", expected=0, counts=_empty(),
    ) == "submitted"


def test_first_in_flight_promotes_submitted_to_running() -> None:
    counts = _empty() | {"queued": 1}
    assert next_batch_state(
        current="submitted", expected=2, counts=counts,
    ) == "running"


def test_protected_pending_is_in_flight() -> None:
    counts = _empty() | {"protected-pending": 1}
    assert next_batch_state(
        current="submitted", expected=1, counts=counts,
    ) == "running"


def test_protected_pending_batch_result_remains_active() -> None:
    trial = SimpleNamespace(state="protected-pending")

    assert "protected-pending" in _empty_trial_summary()
    assert _result_status_from_trials([trial]) is None


def test_result_status_uses_terminal_state_not_reward() -> None:
    assert _compute_result_status(["succeeded"]) == "succeeded"


def test_result_status_mixed_terminal_states_is_partial_failed() -> None:
    assert _compute_result_status([
        "succeeded", "failed", "cancelled",
    ]) == "partial_failed"


def test_result_status_without_succeeded_trials_is_all_failed() -> None:
    assert _compute_result_status(["failed", "cancelled"]) == "all_failed"


def test_cancelling_stays_open_until_the_drain_helper_closes_it() -> None:
    counts = _empty() | {"succeeded": 2, "cancelled": 1}
    assert next_batch_state(
        current="cancelling", expected=3, counts=counts,
    ) == "cancelling"
