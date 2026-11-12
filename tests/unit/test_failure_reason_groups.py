"""Whole-batch failure_reason groups for the Monitor diagnostics card."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import UUID

from loom_service.routes.batches import _failure_reason_groups_from_trials

_T0 = datetime(2026, 11, 1, 12, 0, tzinfo=UTC)


def _trial(
    n: int,
    *,
    state: str = "failed",
    reason: str | None = "timed_out",
    message: str | None = "agent timeout",
    finished_at: datetime | None = _T0,
) -> SimpleNamespace:
    return SimpleNamespace(
        id=UUID(int=n),
        state=state,
        failure_reason=reason,
        failure_message=message,
        finished_at=finished_at,
    )


def test_unknown_failure_when_reason_is_missing() -> None:
    groups = _failure_reason_groups_from_trials([_trial(1, reason=None)])
    assert groups == [
        {
            "reason": "unknown_failure",
            "count": 1,
            "sample_trial_id": str(UUID(int=1)),
            "messages": ["agent timeout"],
        }
    ]


def test_counts_match_failed_trials_and_skip_other_states() -> None:
    trials = [
        _trial(1, reason="timed_out"),
        _trial(2, reason="timed_out"),
        _trial(3, reason="task_error", message="upstream 504"),
        _trial(4, state="succeeded", reason="timed_out", message="should not count"),
        _trial(5, state="cancelled", reason="task_error", message="cancelled"),
    ]
    groups = _failure_reason_groups_from_trials(trials)
    assert sum(group["count"] for group in groups) == 3
    assert [group["reason"] for group in groups] == ["timed_out", "task_error"]
    assert groups[0]["count"] == 2
    assert groups[1]["count"] == 1
    assert groups[1]["messages"] == ["upstream 504"]


def test_sample_and_messages_come_from_latest_finished() -> None:
    trials = [
        _trial(1, message="oldest", finished_at=_T0),
        _trial(2, message="middle", finished_at=_T0 + timedelta(minutes=1)),
        _trial(3, message="newest", finished_at=_T0 + timedelta(minutes=2)),
        _trial(4, message="tie-break", finished_at=_T0 + timedelta(minutes=2)),
    ]
    groups = _failure_reason_groups_from_trials(trials)
    assert len(groups) == 1
    assert groups[0]["sample_trial_id"] == str(UUID(int=4))
    assert groups[0]["messages"] == ["tie-break", "newest", "middle"]
    assert groups[0]["count"] == 4


def test_redacts_secret_material_in_messages() -> None:
    groups = _failure_reason_groups_from_trials(
        [
            _trial(
                1,
                message="provider returned error with Authorization: Bearer loom_api_supersecret",
            )
        ]
    )
    assert groups[0]["messages"] == [
        "provider returned error with Authorization: Bearer [REDACTED:bearer]"
    ]
    assert "loom_api_supersecret" not in groups[0]["messages"][0]
