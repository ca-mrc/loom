"""FIFO overflow and pause credit, without a database."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from loom.attempt_deadline import AttemptDeadline
from loom.quota_queue import GroupView, choose_admission, pause_credit_seconds
from loom.trial.stale_running import evaluate_stale_running_trial


def _group(name: str, *, cap: int = 1, in_flight: int = 0, waiter=None) -> GroupView:
    return GroupView(name, cap, in_flight, waiter)


def _choice(**overrides: object):
    values = dict(
        bound_group="loom-runs",
        groups=(_group("loom-runs"), _group("yibu-shared")),
        request_id=uuid4(),
        front=False,
        idle_sec=0,
        stall_sec=200,
    )
    values.update(overrides)
    return choose_admission(**values)  # type: ignore[arg-type]


def test_bound_group_with_room_and_no_waiters_admits() -> None:
    choice = _choice()
    assert choice.kind == "admit"
    assert choice.group == "loom-runs"


def test_full_group_overflows_without_cutting_a_line() -> None:
    choice = _choice(groups=(_group("loom-runs", in_flight=1), _group("yibu-shared")))
    assert choice.kind == "overflow"
    assert choice.group == "yibu-shared"


def test_overflow_does_not_pass_a_waiter() -> None:
    choice = _choice(
        groups=(
            _group("loom-runs", in_flight=1),
            _group("yibu-shared", waiter=uuid4()),
        )
    )
    assert choice.kind == "wait"


def test_older_waiter_keeps_a_freed_slot() -> None:
    older = uuid4()
    groups = (_group("loom-runs", waiter=older),)
    assert _choice(groups=groups, request_id=uuid4()).kind == "wait"
    assert _choice(groups=groups, request_id=older).kind == "admit"


def test_idle_full_group_stalls() -> None:
    choice = _choice(
        groups=(_group("loom-runs", in_flight=1), _group("yibu-shared", in_flight=1)),
        idle_sec=200,
    )
    assert choice.kind == "stall"


def test_pause_credit_caps_committed_spans() -> None:
    start = datetime(2026, 11, 5, tzinfo=UTC)
    credit = pause_credit_seconds(
        (
            (start, start + timedelta(seconds=40), start + timedelta(seconds=40)),
            (start + timedelta(seconds=40), None, start + timedelta(seconds=45)),
        ),
        now=start + timedelta(seconds=80),
        max_pause_sec=50,
    )
    assert credit == 50


def test_open_queue_credit_stops_when_the_waiter_is_dead() -> None:
    start = datetime(2026, 11, 5, tzinfo=UTC)
    credit = pause_credit_seconds(
        ((start, None, start),),
        now=start + timedelta(seconds=100),
        max_pause_sec=1000,
    )
    assert credit == 15


def test_late_close_does_not_credit_past_the_dead_waiter_window() -> None:
    start = datetime(2026, 11, 5, tzinfo=UTC)
    credit = pause_credit_seconds(
        ((start, start + timedelta(seconds=100), start),),
        now=start + timedelta(seconds=200),
        max_pause_sec=1000,
    )
    assert credit == 15


def test_attempt_deadline_moves_forward_only() -> None:
    deadline = AttemptDeadline.after(30)
    original = deadline.monotonic_deadline
    wall = deadline.wall_deadline_epoch_sec or 0
    assert deadline.extend_to(wall + 15)
    assert deadline.monotonic_deadline == pytest.approx(original + 15)
    assert not deadline.extend_to(wall + 10)
    assert deadline.monotonic_deadline == pytest.approx(original + 15)


def test_open_queue_keeps_a_trial_past_its_working_budget() -> None:
    now = datetime(2026, 11, 5, tzinfo=UTC)
    decision = evaluate_stale_running_trial(
        state="running",
        started_at=now - timedelta(seconds=5000),
        finished_at=None,
        trial_config={"override_agent_timeout_sec": 3600},
        task_config={"agent": {"timeout_sec": 3600}},
        last_event_at=now - timedelta(seconds=2000),
        last_llm_call_at=now - timedelta(seconds=2000),
        worker_last_seen_at=now,
        now=now,
        worker_heartbeat_expiry_sec=60,
        timeout_multiplier=1,
        grace_sec=0,
        silence_sec=900,
        queue_waiting=True,
        pause_credit_sec=120,
    )
    assert decision.decision == "keep"
    assert decision.reason == "provider_queue_waiting"
