from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest

from loom_service.failure_taxonomy import (
    build_supplemental_rerun_plan,
    classification_counts,
    classify_trial_outcome,
    rerun_recommendation_counts,
)


def _trial(
    *,
    task_id: str,
    state: str,
    failure_reason: str | None = None,
    result: dict[str, object] | None = None,
    sample_idx: int = 0,
    combination_idx: int = 0,
) -> SimpleNamespace:
    return SimpleNamespace(
        id=uuid4(),
        batch_id=uuid4(),
        task_id=task_id,
        state=state,
        failure_reason=failure_reason,
        failure_message=f"{failure_reason} message" if failure_reason else None,
        result=result,
        sample_idx=sample_idx,
        combination_idx=combination_idx,
    )


def test_reward_zero_is_score_failure_not_platform_failure() -> None:
    classification = classify_trial_outcome(
        _trial(
            task_id="source-useful/task-score-zero",
            state="succeeded",
            result={"aggregate_reward": 0.0},
        )
    )

    assert classification["reason_code"] == "trial.score_failure"
    assert classification["failure_class"] == "score_failure"
    assert classification["platform_outcome"] == "success"
    assert classification["score_outcome"] == "failed"
    assert classification["root_cause"] == "model_or_task_score"
    assert classification["rerun_recommendation"] == "not_rerunnable"


def test_protected_pending_is_classified_as_active() -> None:
    classification = classify_trial_outcome(
        _trial(
            task_id="local/protected-pending",
            state="protected-pending",
        )
    )

    assert classification["reason_code"] == "trial.protected-pending"
    assert classification["platform_outcome"] == "active"


def test_missing_required_artifacts_are_retryable_invalid_evidence() -> None:
    classification = classify_trial_outcome(
        _trial(
            task_id="skilllearnbench/anthropic-poster-design/anthropic-poster-design-5",
            state="failed",
            failure_reason="missing_required_artifacts",
        )
    )

    assert classification["reason_code"] == "trial.missing_required_artifacts"
    assert classification["failure_class"] == "artifact_failure"
    assert classification["root_cause"] == "missing_required_artifacts"
    assert classification["platform_outcome"] == "invalid_evidence"
    assert classification["score_outcome"] == "unscored"
    assert classification["rerun_recommendation"] == "auto_safe"
    assert classification["rerunnable"] is True


@pytest.mark.parametrize(
    (
        "failure_reason",
        "failure_class",
        "root_cause",
        "rerun_recommendation",
        "requires_task_change",
    ),
    [
        (
            "verifier_missing_output",
            "verifier_failure",
            "verifier_missing_output",
            "operator_approval",
            False,
        ),
        (
            "missing_verifier_output",
            "verifier_failure",
            "verifier_missing_output",
            "operator_approval",
            False,
        ),
        (
            "missing_trajectory",
            "artifact_failure",
            "missing_trajectory",
            "auto_safe",
            False,
        ),
        (
            "missing_atif",
            "artifact_failure",
            "missing_atif",
            "auto_safe",
            False,
        ),
        (
            "provider_no_call",
            "provider_failure",
            "provider_no_call",
            "operator_approval",
            False,
        ),
        (
            "provider_timeout",
            "provider_failure",
            "provider_timeout",
            "auto_safe",
            False,
        ),
        (
            "step_credential_invalid_or_expired",
            "platform_failure",
            "step_credential",
            "operator_approval",
            False,
        ),
        (
            "step_credential_scope_invalid",
            "platform_failure",
            "step_credential",
            "operator_approval",
            False,
        ),
        (
            "setup_failure",
            "platform_failure",
            "platform_setup",
            "operator_approval",
            False,
        ),
        (
            "node_setup_health",
            "platform_failure",
            "platform_setup",
            "operator_approval",
            False,
        ),
        (
            "task_image_build_failed",
            "task_failure",
            "task_image_build",
            "not_rerunnable",
            True,
        ),
        (
            "preflight_failed",
            "task_failure",
            "task_preflight",
            "not_rerunnable",
            True,
        ),
    ],
)
def test_required_failure_reasons_are_typed_distinctly(
    failure_reason: str,
    failure_class: str,
    root_cause: str,
    rerun_recommendation: str,
    requires_task_change: bool,
) -> None:
    classification = classify_trial_outcome(
        _trial(
            task_id=f"source-useful/{failure_reason}",
            state="failed",
            failure_reason=failure_reason,
        )
    )

    assert classification["reason_code"] == f"trial.{failure_reason}"
    assert classification["failure_class"] == failure_class
    assert classification["root_cause"] == root_cause
    assert classification["platform_outcome"] == "failed"
    assert classification["score_outcome"] == "unscored"
    assert classification["rerun_recommendation"] == rerun_recommendation
    assert classification["requires_task_change"] is requires_task_change


def test_failed_platform_gateway_is_auto_safe_rerun() -> None:
    classification = classify_trial_outcome(
        _trial(
            task_id="source-useful/task-provider-flake",
            state="failed",
            failure_reason="provider_transport_disconnect",
        )
    )

    assert classification["failure_class"] == "platform_failure"
    assert classification["root_cause"] == "provider_transport"
    assert classification["rerun_recommendation"] == "auto_safe"
    assert classification["rerunnable"] is True


def test_task_compatibility_requires_task_change_before_rerun() -> None:
    classification = classify_trial_outcome(
        _trial(
            task_id="source-useful/task-incompatible",
            state="failed",
            failure_reason="task_compatibility",
        )
    )

    assert classification["failure_class"] == "task_failure"
    assert classification["root_cause"] == "task_compatibility"
    assert classification["rerun_recommendation"] == "not_rerunnable"
    assert classification["requires_task_change"] is True


def test_supplemental_plan_is_deterministic_and_task_filtered() -> None:
    batch = SimpleNamespace(id=uuid4(), team_id=uuid4())
    trials = [
        _trial(
            task_id="source-useful/task-score-zero",
            state="succeeded",
            result={"aggregate_reward": 0.0},
        ),
        _trial(
            task_id="source-useful/task-auto",
            state="failed",
            failure_reason="gateway_error",
        ),
        _trial(
            task_id="source-useful/task-operator",
            state="failed",
            failure_reason="verifier_timeout",
        ),
        _trial(
            task_id="source-useful/task-incompatible",
            state="failed",
            failure_reason="task_compatibility",
        ),
    ]

    plan = build_supplemental_rerun_plan(
        batch,
        trials,
        task_ids=[
            "source-useful/task-incompatible",
            "source-useful/task-auto",
            "source-useful/task-score-zero",
            "source-useful/task-operator",
        ],
    )

    assert plan["supplemental_task_ids"] == ["source-useful/task-auto"]
    assert [item["task_id"] for item in plan["auto_safe"]] == [
        "source-useful/task-auto"
    ]
    assert [item["task_id"] for item in plan["operator_approval"]] == [
        "source-useful/task-operator"
    ]
    assert [item["task_id"] for item in plan["not_rerunnable"]] == [
        "source-useful/task-incompatible",
        "source-useful/task-score-zero",
    ]
    assert plan["summary"] == {
        "auto_safe": 1,
        "operator_approval": 1,
        "not_rerunnable": 2,
        "already_covered": 0,
        "selected_final_trials": 4,
    }


def test_supplemental_plan_preserves_duplicate_task_coordinates() -> None:
    batch = SimpleNamespace(id=uuid4(), team_id=uuid4())
    first = _trial(
        task_id="source-useful/task-auto",
        state="failed",
        failure_reason="gateway_error",
        sample_idx=1,
        combination_idx=0,
    )
    second = _trial(
        task_id="source-useful/task-auto",
        state="failed",
        failure_reason="missing_trajectory",
        sample_idx=0,
        combination_idx=2,
    )
    operator = _trial(
        task_id="source-useful/task-auto",
        state="failed",
        failure_reason="verifier_timeout",
        sample_idx=2,
        combination_idx=0,
    )

    plan = build_supplemental_rerun_plan(batch, [first, second, operator])

    assert plan["supplemental_task_ids"] == ["source-useful/task-auto"]
    assert [
        (item["task_id"], item["sample_idx"], item["combination_idx"])
        for item in plan["auto_safe"]
    ] == [
        ("source-useful/task-auto", 0, 2),
        ("source-useful/task-auto", 1, 0),
    ]
    assert plan["supplemental_coordinates"] == [
        {
            "task_id": "source-useful/task-auto",
            "sample_idx": 0,
            "combination_idx": 2,
        },
        {
            "task_id": "source-useful/task-auto",
            "sample_idx": 1,
            "combination_idx": 0,
        },
    ]
    assert [
        (item["task_id"], item["sample_idx"], item["combination_idx"])
        for item in plan["operator_approval"]
    ] == [("source-useful/task-auto", 2, 0)]


def test_successful_supplemental_trial_becomes_final_selection() -> None:
    batch_id = uuid4()
    child_batch_id = uuid4()
    original = _trial(
        task_id="source-useful/task-auto",
        state="failed",
        failure_reason="gateway_error",
    )
    original.batch_id = batch_id
    rerun = _trial(
        task_id="source-useful/task-auto",
        state="succeeded",
        result={"aggregate_reward": 1.0},
    )
    rerun.batch_id = child_batch_id

    plan = build_supplemental_rerun_plan(
        SimpleNamespace(id=batch_id, team_id=uuid4()),
        [original],
        supplemental_trials=[rerun],
    )

    assert plan["summary"]["already_covered"] == 1
    assert plan["supplemental_task_ids"] == []
    assert plan["final_trial_selection"] == [
        {
            "task_id": original.task_id,
            "sample_idx": 0,
            "combination_idx": 0,
            "selected_trial_id": str(rerun.id),
            "selected_batch_id": str(child_batch_id),
            "selected_source": "supplemental",
            "original_trial_id": str(original.id),
            "original_failure_class": "platform_failure",
        }
    ]


def test_5003_manual_classification_fixture_replays() -> None:
    fixture = (
        Path(__file__).parents[1]
        / "fixtures"
        / "source_useful_5003_failure_classification.json"
    )
    rows = json.loads(fixture.read_text(encoding="utf-8"))["items"]

    actual = {
        item["task_id"]: classify_trial_outcome(
            _trial(
                task_id=item["task_id"],
                state=item["state"],
                failure_reason=item.get("failure_reason"),
                result=item.get("result"),
            )
        )["failure_class"]
        for item in rows
    }

    # The historical annotation called a claimed row stale, but the stored
    # state alone supplies no deadline/heartbeat evidence for that conclusion.
    expected = {item["task_id"]: item["manual_failure_class"] for item in rows}
    expected["source-useful-5003/main/stale-claimed"] = "active"
    assert actual == expected


def test_5003_production_supplemental_targets_replay() -> None:
    fixture = (
        Path(__file__).parents[1]
        / "fixtures"
        / "source_useful_5003_failure_classification.json"
    )
    data = json.loads(fixture.read_text(encoding="utf-8"))
    evidence = data["production_evidence"]
    rows = data["production_items"]
    batch = SimpleNamespace(id=evidence["main_batch_id"], team_id=uuid4())
    trials = [
        _trial(
            task_id=item["task_id"],
            state=item["state"],
            failure_reason=item.get("failure_reason"),
            result=item.get("result"),
        )
        for item in rows
    ]

    assert sum(1 for item in rows if item["state"] == "failed") == evidence[
        "main_failed_count"
    ]
    assert sum(1 for item in rows if item["state"] == "cancelled") == evidence[
        "main_cancelled_count"
    ]
    actual_classes = {
        item["task_id"]: classify_trial_outcome(trial)["failure_class"]
        for item, trial in zip(rows, trials, strict=True)
    }
    assert actual_classes == {
        item["task_id"]: item["manual_failure_class"] for item in rows
    }

    plan = build_supplemental_rerun_plan(
        batch,
        trials,
        include_operator_approval=True,
    )

    assert plan["supplemental_task_ids"] == evidence["supplemental_target_task_ids"]
    assert plan["summary"] == {
        "auto_safe": 0,
        "operator_approval": 32,
        "not_rerunnable": 0,
        "already_covered": 0,
        "selected_final_trials": 32,
    }


@pytest.mark.parametrize("state", ["queued", "protected-pending", "claimed", "running", "submitted", "materializing"])
def test_active_work_is_not_a_failure_or_supplemental_target(state: str) -> None:
    active = _trial(task_id="local/active", state=state)
    failed = _trial(task_id="local/failed", state="failed", failure_reason="agent_timeout")
    classification = classify_trial_outcome(active)
    assert classification["platform_outcome"] == "active"
    assert classification["failure_class"] == "active"
    assert classification["root_cause"] == "none"
    assert classification["attribution"] == "pending"
    assert classification["rerunnable"] is False
    assert classification_counts([active, failed]) == {"agent_failure": 1}
    assert rerun_recommendation_counts([active]) == {}
    plan = build_supplemental_rerun_plan(
        SimpleNamespace(id=uuid4()), [active, failed], include_operator_approval=True,
    )
    assert plan["supplemental_task_ids"] == ["local/failed"]
    replacement = _trial(task_id="local/active", state="succeeded", result={"aggregate_reward": 1})
    plan = build_supplemental_rerun_plan(
        SimpleNamespace(id=uuid4()), [active], supplemental_trials=[replacement],
    )
    assert plan["final_trial_selection"][0]["selected_trial_id"] == str(active.id)


@pytest.mark.parametrize("reason,attribution,retryable", [
    ("image_preparation_base_unsupported", "platform", False),
    ("image_preparation_source_unavailable", "platform", True),
    ("image_preparation_input_invalid", "unknown", False),
])
def test_typed_preparation_failure_is_unscored_without_invented_task_fault(reason, attribution, retryable):
    trial = _trial(task_id="synthetic/preparation", state="failed", failure_reason="task_image_build_failed",
        result={"task_image_preparation_failure": {"reason": reason, "message": "private raw detail"}})
    result = classify_trial_outcome(trial)
    assert result["attribution"] == attribution
    assert result["score_outcome"] == "unscored"
    assert result["requires_task_change"] is False
    assert result["rerunnable"] is retryable
    assert "private raw detail" not in str(result)


@pytest.mark.parametrize(
    ("runtime_status", "failure_reason", "attribution"),
    [
        ("verifier_error", "verifier_error", "benchmark"),
        ("timed_out", "verifier_timeout", "benchmark"),
        ("setup_error", "setup_error", "platform"),
        ("runtime_error", "runtime_error", "platform"),
        ("missing_required_artifacts", "missing_required_artifacts", "platform"),
        ("artifact_upload_failed", "artifact_upload_failed", "platform"),
        ("trajectory_flush_failed", "trajectory_flush_failed", "platform"),
    ],
)
def test_deferred_verifier_platform_failures_are_not_benchmark_errors(
    runtime_status: str, failure_reason: str, attribution: str,
) -> None:
    # #2538: a verifier pod that could not reach its sandbox (setup_error) or
    # lacked a plan-declared output was reported as a benchmark verifier error.
    from loom_control_plane.service_execution import deferred_verifier_failure_reason
    from loom_service.diagnosis import build_trial_diagnosis

    assert deferred_verifier_failure_reason(runtime_status) == failure_reason
    classification = classify_trial_outcome(
        _trial(task_id="tb2/bn-fit-modify", state="failed", failure_reason=failure_reason)
    )
    diagnosis = build_trial_diagnosis({"entity": {"type": "trial"}, "failure": classification})
    assert classification["attribution"] == attribution
    assert diagnosis["primary_cause"]["attribution"] == attribution
