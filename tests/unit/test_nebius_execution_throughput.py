from copy import deepcopy
from datetime import UTC, datetime, timedelta

import pytest
from scripts.ops.nebius_execution_throughput import compare, summarize

START = datetime(2026, 10, 31, tzinfo=UTC)


def stamp(seconds):
    return (START + timedelta(seconds=seconds)).isoformat()


def sample():
    # Two deliberately requested repetitions of the same task are two Trials;
    # retries of a Trial must not become additional completed tasks.
    return {
        "controls": {"expected_tasks": {"task": 2}, "task_revisions": {"task": "revision"}},
        "observed_at": stamp(300), "batch": {"id": "batch", "submitted_at": stamp(0)},
        "trials": [
            {"id": "t1", "task_id": "task", "state": "succeeded", "finished_at": stamp(60),
             "reward": 1, "attempt_count": 2},
            {"id": "t2", "task_id": "task", "state": "succeeded", "finished_at": stamp(150),
             "reward": 0, "attempt_count": 1},
        ],
        "leases": [
            {"id": "l1", "trial_id": "t1", "task_revision_sha256": "revision",
             "materialization_state": "committed", "materialization_committed_at": stamp(65),
             "cleanup_state": "complete", "deleted_at": stamp(90),
             "pod_started_at": stamp(10), "pod_terminated_at": stamp(50)},
            {"id": "l2", "trial_id": "t2", "task_revision_sha256": "revision",
             "materialization_state": "committed", "materialization_committed_at": stamp(155),
             "cleanup_state": "complete", "deleted_at": stamp(180),
             "pod_started_at": stamp(40), "pod_terminated_at": stamp(140)},
        ],
    }


def test_fixed_window_counts_canonical_and_cleaned_completion_separately():
    result = summarize(sample(), [70, 120, 300])
    first, second, last = result["windows"]
    assert (first["canonical_result_completions"], first["fully_cleaned_completions"]) == (1, 0)
    assert second["fully_cleaned_completions"] == 1
    assert second["actual_running_pods"]["peak"] == 2
    assert last["fully_cleaned_completions"] == 2
    assert last["positive_oracle_results"] == 1
    assert result["trials_with_retries"] == 1
    assert result["successful_settled_makespan_seconds"] == 180


def test_does_not_report_unobserved_future_as_zero_throughput():
    assert summarize(sample(), [600])["windows"] == [{"seconds": 600, "observed": False}]


@pytest.mark.parametrize("damage", ["duplicate_trial", "duplicate_lease", "revision", "missing_repetition"])
def test_rejects_inputs_that_could_create_an_unfair_gain(damage):
    data = sample()
    if damage == "duplicate_trial":
        data["trials"][1]["id"] = "t1"
    elif damage == "duplicate_lease":
        data["leases"][1]["id"] = "l1"
    elif damage == "revision":
        data["leases"][0]["task_revision_sha256"] = "other-revision"
    else:
        data["controls"]["expected_tasks"]["task"] = 3
    with pytest.raises(ValueError):
        summarize(data, [120])


def test_pending_cleanup_or_non_numeric_reward_does_not_count_as_settled():
    data = sample()
    data["leases"][0].update(cleanup_state="pending", deleted_at=None)
    data["trials"][1]["reward"] = None
    row = summarize(data, [300])["windows"][0]
    assert row["canonical_result_completions"] == 1
    assert row["fully_cleaned_completions"] == 0


def test_comparison_requires_same_workload_and_reports_fixed_window_gain():
    before = sample()
    after = deepcopy(before)
    after["trials"][1]["finished_at"] = stamp(90)
    after["leases"][1].update(materialization_committed_at=stamp(95), deleted_at=stamp(110))
    assert compare(before, after, [120])["fixed_window_comparison"] == [{
        "seconds": 120, "before_result_completed": 1, "after_result_completed": 2,
        "additional_results": 1, "result_throughput_ratio": 2,
        "before_fully_cleaned": 1, "after_fully_cleaned": 2,
        "additional_fully_cleaned": 1, "settled_throughput_ratio": 2,
    }]
    after["controls"]["task_revisions"]["task"] = "different-workload"
    with pytest.raises(ValueError, match="controls differ"):
        compare(before, after, [120])


def test_failed_batch_reports_drain_without_inventing_success_or_running_pods():
    data = sample()
    data["trials"][1].update(state="failed", reward=None)
    data["leases"][1].update(pod_terminated_at=None, materialization_state="not_started",
                             materialization_committed_at=None)
    result = summarize(data, [300])
    assert result["all_terminal_makespan_seconds"] == 150
    assert result["all_drained_makespan_seconds"] == 180
    assert result["successful_settled_makespan_seconds"] is None
    window = result["windows"][0]
    assert window["canonical_result_completions"] == 1
    assert window["failed"] == 1
    assert window["actual_running_pods"]["average"] == (40 + 140) / 300
    assert window["actual_running_pods"]["unknown_termination_intervals"] == 1
