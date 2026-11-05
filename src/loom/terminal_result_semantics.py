"""Shared reward projection and consistency checks for persisted trial results.

The worker projects ``TrialResult`` before it reports the terminal row state.
That split write is intentional, but neither write boundary may accept a
payload that can later make a successful trial carry terminal failure
semantics.  Delivery uses the same checks so historical drift fails closed.
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from numbers import Real
from typing import Any

TERMINAL_TRIAL_STATES = frozenset({"succeeded", "failed", "cancelled"})

TerminalResultConflict = dict[str, Any]


def aggregate_reward_scalar(reward: dict[str, float] | None) -> float | None:
    """Project named rewards for Trial/Batch summaries without discarding them."""

    if not reward:
        return None
    if len(reward) == 1:
        return float(next(iter(reward.values())))
    return sum(float(value) for value in reward.values()) / len(reward)


def _conflict(*, field: str, expected: Any, actual: Any) -> TerminalResultConflict:
    return {"field": field, "expected": expected, "actual": actual}


def explicit_skip_verifier(config: Any) -> bool:
    """Return true only for the persisted, explicit verifier-skip contract."""

    return isinstance(config, Mapping) and config.get("skip_verifier") is True


def has_numeric_reward(value: Any) -> bool:
    """Accept scalar or reward-map numbers, including zero, but not booleans/NaN."""

    if isinstance(value, Real) and not isinstance(value, bool):
        return math.isfinite(float(value))
    if isinstance(value, Mapping) and value:
        return all(has_numeric_reward(item) for item in value.values())
    return False


def result_has_numeric_reward(result: Mapping[str, Any]) -> bool:
    return any(
        has_numeric_reward(result.get(key)) for key in ("reward", "aggregate_reward", "score")
    )


def is_scored_agent_timeout(*, state: str, result: Any, failure_reason: Any) -> bool:
    """A task deadline can still produce a complete, independently scored attempt.

    Use the persisted runtime evidence, already validated by its owner, rather
    than treating any failed row with a cached score as deliverable. Storage and
    trajectory completeness remain the export boundary's responsibility.
    """
    if state != "failed" or failure_reason != "timed_out" or not isinstance(result, Mapping):
        return False
    runtime = result.get("runtime_result")
    if (
        # Native finalization stores state on the Trial row; an embedded state
        # is optional, but must agree when supplied by another result producer.
        result.get("state") not in (None, "failed")
        or result.get("failure_reason") not in (None, "timed_out")
        or not isinstance(runtime, Mapping)
        or runtime.get("schema_version") != "loom.execution-runtime-result.v1"
        or runtime.get("execution_role") != "attempt"
        or runtime.get("status") != "timed_out"
        or runtime.get("failure_reason") is not None
        or not has_numeric_reward(runtime.get("verifier_rewards"))
        or not has_numeric_reward(result.get("reward"))
        or result["reward"] != runtime["verifier_rewards"]
    ):
        return False
    phases = runtime.get("phases")
    if not isinstance(phases, list) or not all(isinstance(item, Mapping) for item in phases):
        return False
    roles = [item.get("role") for item in phases]
    if roles not in (["agent", "verifier"], ["setup", "agent", "verifier"]):
        return False
    agent, verifier = phases[-2:]
    if (
        agent.get("exit_code") != 124
        or agent.get("timed_out") is not True
        or any(
            item.get("exit_code") != 0
            or item.get("timed_out") is not False
            or item.get("signal") is not None
            for item in [*phases[:-2], verifier]
        )
    ):
        return False
    outputs = runtime.get("outputs")
    return (
        isinstance(outputs, list)
        and all(isinstance(item, Mapping) for item in outputs)
        and all(not item.get("required") or item.get("state") == "captured" for item in outputs)
        and any(
            item.get("kind") == "verifier" and item.get("state") == "captured" for item in outputs
        )
    )


def agent_phase_timed_out(result: Any) -> bool:
    """The attempt's agent phase stopped on the task deadline."""

    if not isinstance(result, Mapping):
        return False
    runtime = result.get("runtime_result")
    if not isinstance(runtime, Mapping):
        return False
    phases = runtime.get("phases")
    if not isinstance(phases, list) or not phases or not isinstance(phases[-1], Mapping):
        return False
    agent = phases[-1]
    return (
        runtime.get("status") == "timed_out"
        and agent.get("role") == "agent"
        and agent.get("exit_code") == 124
        and agent.get("timed_out") is True
    )


def is_ungraded_agent_trajectory(
    *, state: str, result: Any, failure_reason: Any, config: Any,
) -> bool:
    """A finished terminus-2 attempt can be delivered with its trajectory and no score.

    An agent-budget timeout and a verifier error stay separate labels. A verifier
    error whose agent phase hit that same budget is still deliverable. Neither
    shape is a scored timeout, and a missing Harbor trajectory stays ineligible.
    """
    if (
        state != "failed"
        or not isinstance(config, Mapping)
        or config.get("agent_name") != "terminus-2"
        or not isinstance(result, Mapping)
        or result_has_numeric_reward(result)
        or result.get("state") not in (None, "failed")
        or result.get("failure_reason") not in (None, failure_reason)
    ):
        return False
    runtime = result.get("runtime_result")
    phases = runtime.get("phases") if isinstance(runtime, Mapping) else None
    if (
        not isinstance(runtime, Mapping)
        or runtime.get("schema_version") != "loom.execution-runtime-result.v1"
        or runtime.get("execution_role") != "attempt"
        or runtime.get("failure_reason") is not None
        or not isinstance(phases, list)
        or not phases
        or not all(isinstance(item, Mapping) for item in phases)
        or not _captured_harbor_trajectory(runtime)
    ):
        return False
    roles = [item.get("role") for item in phases]
    if roles not in (["agent"], ["setup", "agent"]):
        return False
    if any(
        item.get("exit_code") != 0 or item.get("timed_out") is not False
        for item in phases[:-1]
    ):
        return False
    agent = phases[-1]
    if failure_reason == "timed_out":
        return agent_phase_timed_out(result)
    if failure_reason == "verifier_error":
        return agent_phase_timed_out(result) or (
            runtime.get("status") == "succeeded"
            and agent.get("exit_code") == 0
            and agent.get("timed_out") is False
        )
    return False


def _captured_harbor_trajectory(runtime: Mapping[str, Any]) -> bool:
    outputs = runtime.get("outputs")
    return isinstance(outputs, list) and any(
        isinstance(item, Mapping)
        and item.get("relative_path") == "artifacts/harbor/trajectory.json"
        and item.get("kind") == "agent_native"
        and item.get("state") == "captured"
        for item in outputs
    )


def projected_result_conflicts(result: Any) -> list[TerminalResultConflict]:
    """Return contradictions contained within a projected terminal result.

    Output projection is the modern worker write path and must carry its own
    terminal ``state``.  This stricter entry-point check does not change the
    legacy terminal PATCH contract, where an older result may lack ``state``.
    """

    if not isinstance(result, Mapping):
        return [_conflict(field="result", expected="object", actual=type(result).__name__)]

    result_state = result.get("state")
    if result_state not in TERMINAL_TRIAL_STATES:
        return [
            _conflict(
                field="result.state",
                expected=sorted(TERMINAL_TRIAL_STATES),
                actual=result_state,
            )
        ]
    return terminal_result_conflicts(
        state=str(result_state),
        result=result,
        failure_reason=None,
        config=result.get("config"),
    )


def terminal_result_conflicts(
    *,
    state: str,
    result: Any,
    failure_reason: Any,
    config: Any,
) -> list[TerminalResultConflict]:
    """Compare a terminal row transition with its effective persisted result.

    A missing result remains valid for failed/cancelled setup paths.  Legacy
    result objects without an embedded ``state`` also remain valid.  Numeric
    zero is a valid score; a missing score is valid only when verifier skipping
    was explicitly persisted in the trial config.
    """

    conflicts: list[TerminalResultConflict] = []
    if state not in TERMINAL_TRIAL_STATES:
        conflicts.append(
            _conflict(
                field="state",
                expected=sorted(TERMINAL_TRIAL_STATES),
                actual=state,
            )
        )
        return conflicts

    if result is None:
        if state == "succeeded":
            conflicts.append(_conflict(field="result", expected="object", actual=None))
        return conflicts
    if not isinstance(result, Mapping):
        conflicts.append(_conflict(field="result", expected="object", actual=type(result).__name__))
        return conflicts

    result_state = result.get("state")
    if result_state is not None and result_state != state:
        conflicts.append(_conflict(field="result.state", expected=state, actual=result_state))

    result_failure_reason = result.get("failure_reason")
    if state == "succeeded":
        if failure_reason is not None:
            conflicts.append(
                _conflict(field="failure_reason", expected=None, actual=failure_reason)
            )

        if not result_has_numeric_reward(result) and not explicit_skip_verifier(config):
            conflicts.append(
                _conflict(
                    field="result.reward",
                    expected="numeric reward or config.skip_verifier=true",
                    actual=result.get("reward"),
                )
            )
        if result_failure_reason is not None:
            conflicts.append(
                _conflict(
                    field="result.failure_reason",
                    expected=None,
                    actual=result_failure_reason,
                )
            )
    elif (
        failure_reason is not None
        and result_failure_reason is not None
        and result_failure_reason != failure_reason
    ):
        conflicts.append(
            _conflict(
                field="result.failure_reason",
                expected=failure_reason,
                actual=result_failure_reason,
            )
        )

    return conflicts
