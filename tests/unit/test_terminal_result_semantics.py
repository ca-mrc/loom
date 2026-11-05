from copy import deepcopy

import pytest

from loom.terminal_result_semantics import (
    aggregate_reward_scalar,
    is_scored_agent_timeout,
    is_ungraded_agent_trajectory,
    projected_result_conflicts,
    terminal_result_conflicts,
)


def _scored_timeout_result() -> dict:
    return {
        "reward": {"passed": 0.0},
        "runtime_result": {
            "schema_version": "loom.execution-runtime-result.v1",
            "execution_role": "attempt",
            "status": "timed_out",
            "failure_reason": None,
            "verifier_rewards": {"passed": 0.0},
            "partial_evidence": True,
            "phases": [
                {"role": "agent", "exit_code": 124, "timed_out": True},
                {"role": "verifier", "exit_code": 0, "timed_out": False},
            ],
            "outputs": [{"kind": "verifier", "required": True, "state": "captured"}],
        },
    }


@pytest.mark.parametrize("with_setup", [False, True])
@pytest.mark.parametrize("embedded_state", [None, "failed"])
def test_scored_deadline_keeps_truthful_failure_and_zero_reward(
    with_setup: bool, embedded_state: str | None
) -> None:
    result = _scored_timeout_result()
    if embedded_state is not None:
        result["state"] = embedded_state
    if with_setup:
        result["runtime_result"]["phases"].insert(
            0, {"role": "setup", "exit_code": 0, "timed_out": False}
        )
    original = deepcopy(result)
    assert is_scored_agent_timeout(state="failed", result=result, failure_reason="timed_out")
    assert result == original


@pytest.mark.parametrize(
    "defect",
    [
        "legacy_score",
        "verifier_timeout",
        "verifier_signal",
        "runtime_failure",
        "reward_mismatch",
        "nan_reward",
        "bool_reward",
        "required_output_missing",
        "verifier_output_missing",
        "agent_not_timed_out",
        "cancelled",
        "contradictory_state",
    ],
)
def test_cached_score_does_not_hide_incomplete_or_failed_execution(defect: str) -> None:
    result = _scored_timeout_result()
    runtime = result["runtime_result"]
    state, reason = "failed", "timed_out"
    if defect == "legacy_score":
        del result["runtime_result"]
    elif defect == "verifier_timeout":
        runtime["phases"][-1].update(exit_code=124, timed_out=True)
    elif defect == "verifier_signal":
        runtime["phases"][-1]["signal"] = 9
    elif defect == "runtime_failure":
        runtime["failure_reason"] = "verifier_error"
    elif defect == "reward_mismatch":
        result["reward"] = {"passed": 1.0}
    elif defect in {"nan_reward", "bool_reward"}:
        reward = {"passed": float("nan") if defect == "nan_reward" else False}
        result["reward"] = runtime["verifier_rewards"] = reward
    elif defect == "required_output_missing":
        runtime["outputs"].append({"kind": "workspace", "required": True, "state": "missing"})
    elif defect == "verifier_output_missing":
        runtime["outputs"] = []
    elif defect == "agent_not_timed_out":
        runtime["phases"][0].update(exit_code=1, timed_out=False)
    elif defect == "cancelled":
        state, reason = "cancelled", "cancelled"
    elif defect == "contradictory_state":
        result["state"] = "succeeded"
    assert not is_scored_agent_timeout(state=state, result=result, failure_reason=reason)


@pytest.mark.parametrize(
    ("rewards", "expected"),
    [
        (None, None),
        ({}, None),
        ({"artifact_complete": 1.0}, 1.0),
        ({"passed": 0.0}, 0.0),
        ({"a": 0.0, "b": 1.0}, 0.5),
    ],
)
def test_named_rewards_have_a_shared_scalar_projection(
    rewards: dict[str, float] | None,
    expected: float | None,
) -> None:
    original = None if rewards is None else dict(rewards)
    assert aggregate_reward_scalar(rewards) == expected
    assert rewards == original


def _captured_trajectory() -> dict:
    return {
        "relative_path": "artifacts/harbor/trajectory.json",
        "kind": "agent_native",
        "state": "captured",
    }


def _ungraded_timeout_result() -> dict:
    return {
        "runtime_result": {
            "schema_version": "loom.execution-runtime-result.v1",
            "execution_role": "attempt",
            "status": "timed_out",
            "failure_reason": None,
            "phases": [{"role": "agent", "exit_code": 124, "timed_out": True}],
            "outputs": [_captured_trajectory()],
        },
    }


def _ungraded_verifier_result() -> dict:
    result = _ungraded_timeout_result()
    runtime = result["runtime_result"]
    runtime["status"] = "succeeded"
    runtime["phases"] = [{"role": "agent", "exit_code": 0, "timed_out": False}]
    return result


def test_ungraded_timeout_and_verifier_error_keep_a_committed_trajectory() -> None:
    config = {"agent_name": "terminus-2"}
    timeout = _ungraded_timeout_result()
    verifier = _ungraded_verifier_result()
    assert is_ungraded_agent_trajectory(
        state="failed", result=timeout, failure_reason="timed_out", config=config,
    )
    assert is_ungraded_agent_trajectory(
        state="failed", result=verifier, failure_reason="verifier_error", config=config,
    )
    assert is_ungraded_agent_trajectory(
        state="failed", result=timeout, failure_reason="verifier_error", config=config,
    )
    assert not is_scored_agent_timeout(state="failed", result=timeout, failure_reason="timed_out")
    assert not is_ungraded_agent_trajectory(
        state="failed", result=_scored_timeout_result(), failure_reason="timed_out", config=config,
    )


@pytest.mark.parametrize(
    ("state", "reason", "mutate"),
    [
        ("failed", "task_error", None),
        ("cancelled", "cancelled", None),
        ("failed", "verifier_unavailable", None),
        ("failed", "oom_killed", None),
        ("failed", "timed_out", "reward"),
        ("failed", "timed_out", "drop_trajectory"),
        ("failed", "verifier_error", "other_agent"),
    ],
)
def test_other_failures_stay_out_of_ungraded_delivery(
    state: str, reason: str, mutate: str | None,
) -> None:
    result = _ungraded_timeout_result() if reason == "timed_out" else _ungraded_verifier_result()
    config = {"agent_name": "terminus-2"}
    if mutate == "reward":
        result["reward"] = {"passed": 0.0}
    elif mutate == "drop_trajectory":
        result["runtime_result"]["outputs"] = []
    elif mutate == "other_agent":
        config = {"agent_name": "oracle"}
    assert not is_ungraded_agent_trajectory(
        state=state, result=result, failure_reason=reason, config=config,
    )


def test_projection_rejects_success_with_terminal_failure_reason() -> None:
    conflicts = projected_result_conflicts(
        {
            "state": "succeeded",
            "failure_reason": "verifier_error",
            "reward": None,
        }
    )
    assert {conflict["field"] for conflict in conflicts} == {
        "result.reward",
        "result.failure_reason",
    }


def test_terminal_transition_rejects_result_state_mismatch() -> None:
    assert terminal_result_conflicts(
        state="succeeded",
        result={
            "state": "failed",
            "failure_reason": "verifier_error",
            "aggregate_reward": 0.0,
        },
        failure_reason=None,
        config={},
    ) == [
        {"field": "result.state", "expected": "succeeded", "actual": "failed"},
        {
            "field": "result.failure_reason",
            "expected": None,
            "actual": "verifier_error",
        },
    ]


def test_unscored_success_and_numeric_zero_are_both_valid() -> None:
    assert (
        terminal_result_conflicts(
            state="succeeded",
            result={"state": "succeeded", "reward": None},
            failure_reason=None,
            config={"skip_verifier": True},
        )
        == []
    )


def test_unscored_success_requires_explicit_skip_verifier() -> None:
    assert terminal_result_conflicts(
        state="succeeded",
        result={"state": "succeeded", "reward": None},
        failure_reason=None,
        config={},
    ) == [
        {
            "field": "result.reward",
            "expected": "numeric reward or config.skip_verifier=true",
            "actual": None,
        }
    ]
    assert (
        terminal_result_conflicts(
            state="succeeded",
            result={"state": "succeeded", "aggregate_reward": 0.0},
            failure_reason=None,
            config={},
        )
        == []
    )


def test_scored_success_keeps_nested_verifier_diagnostics() -> None:
    assert (
        terminal_result_conflicts(
            state="succeeded",
            result={
                "state": "succeeded",
                "aggregate_reward": 0.0,
                "steps": [
                    {
                        "verifier_result": {
                            "rewards": {"passed": 0.0},
                            "error": {"kind": "diagnostic", "message": "task failed"},
                        }
                    }
                ],
            },
            failure_reason=None,
            config={},
        )
        == []
    )


def test_legacy_failed_result_without_embedded_state_remains_valid() -> None:
    assert (
        terminal_result_conflicts(
            state="failed",
            result={"aggregate_reward": None},
            failure_reason="gateway_error",
            config={},
        )
        == []
    )
    assert (
        terminal_result_conflicts(
            state="failed",
            result=None,
            failure_reason="task_compatibility",
            config={},
        )
        == []
    )
