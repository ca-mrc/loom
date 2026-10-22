"""Normalized failure taxonomy and supplemental rerun planning.

The service stores low-level trial states and failure_reason values. This
module projects them into the user-facing issue #388 contract: platform/task/
score classes, root cause, rerun recommendation, and deterministic target lists.
It intentionally accepts trial-like objects so routes, debug evidence, tests,
and offline replay scripts can share the same classification rules.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from decimal import Decimal
from typing import Any, cast

from loom.task_image_failure import image_preparation_failure

_ACTIVE_TRIAL_STATES = {
    "queued",
    "protected-pending",
    "claimed",
    "running",
    "submitted",
    "materializing",
}

_AUTO_SAFE_REASONS: frozenset[str] = frozenset(
    {
        "artifact_upload_failed",
        "exhausted_retries",
        "gateway_error",
        "provider_transport_disconnect",
        "retry_exhausted",
        "trajectory_flush_failed",
        "worker_lost_claim",
    }
)

_TASK_FAILURE_REASONS: Mapping[str, str] = {
    "preflight_failed": "task_preflight",
    "task_compatibility": "task_compatibility",
    "task_image_build_failed": "task_image_build",
    "task_image_build_timeout": "task_image_build",
}

_VERIFIER_FAILURE_REASONS: Mapping[str, str] = {
    "missing_verifier_output": "verifier_missing_output",
    "verifier_error": "verifier_harness",
    "verifier_missing_output": "verifier_missing_output",
    "verifier_timeout": "verifier_harness",
}

_ARTIFACT_FAILURE_REASONS: Mapping[str, str] = {
    "missing_atif": "missing_atif",
    "missing_required_artifacts": "missing_required_artifacts",
    "missing_trajectory": "missing_trajectory",
}

_AGENT_FAILURE_REASONS: frozenset[str] = frozenset(
    {"agent_error", "agent_timeout"}
)

_STEP_CREDENTIAL_REASONS: frozenset[str] = frozenset(
    {
        "step_credential_invalid_or_expired",
        "step_credential_scope_invalid",
    }
)

_PLATFORM_SETUP_REASONS: frozenset[str] = frozenset(
    {
        "env_start_failure",
        "env_healthcheck_failed",
        "internal_error",
        "node_setup_health",
        "setup_failure",
        # Hosted runtime statuses: a setup phase or the runtime itself failed.
        "runtime_error",
        "setup_error",
    }
)

_PROVIDER_APPROVAL_REASONS: frozenset[str] = frozenset({"provider_error"})
_PROVIDER_NO_CALL_REASONS: frozenset[str] = frozenset({"provider_no_call"})
_PROVIDER_TIMEOUT_REASONS: frozenset[str] = frozenset({"provider_timeout"})


def aggregate_reward(result: Any) -> float | None:
    if not isinstance(result, dict):
        return None
    raw = result.get("aggregate_reward")
    if raw is None:
        raw = result.get("reward")
    if isinstance(raw, dict):
        values = [v for v in raw.values() if isinstance(v, int | float | Decimal)]
        if not values:
            return None
        return float(sum(float(v) for v in values) / len(values))
    if isinstance(raw, int | float | str | Decimal):
        try:
            return float(raw)
        except (TypeError, ValueError):
            return None
    return None


def _trial_key(trial: Any) -> tuple[str, int, int]:
    return (
        str(trial.task_id),
        int(getattr(trial, "sample_idx", 0) or 0),
        int(getattr(trial, "combination_idx", 0) or 0),
    )


def _common(
    *,
    reason_code: str,
    failure_class: str,
    root_cause: str,
    platform_outcome: str,
    score_outcome: str,
    rerun_recommendation: str,
    message: str | None,
    reason: str | None = None,
    category: str | None = None,
    attribution: str | None = None,
    rerunnable: bool = False,
    requires_operator_approval: bool = False,
    requires_task_change: bool = False,
) -> dict[str, Any]:
    return {
        "reason_code": reason_code,
        "reason": reason,
        "category": category or root_cause,
        "attribution": attribution or failure_class,
        "message": message,
        "failure_class": failure_class,
        "root_cause": root_cause,
        "platform_outcome": platform_outcome,
        "score_outcome": score_outcome,
        "rerun_recommendation": rerun_recommendation,
        "rerunnable": rerunnable,
        "requires_operator_approval": requires_operator_approval,
        "requires_task_change": requires_task_change,
    }


def classify_trial_outcome(trial: Any) -> dict[str, Any]:
    """Classify one trial-like object for #388 debug/rerun surfaces."""

    state = str(getattr(trial, "state", "unknown") or "unknown")
    reason = getattr(trial, "failure_reason", None)
    reason = str(reason) if reason else None
    message = getattr(trial, "failure_message", None)
    result = getattr(trial, "result", None)
    reward = aggregate_reward(result)

    if state == "succeeded":
        if reward == 0.0:
            return _common(
                reason_code="trial.score_failure",
                failure_class="score_failure",
                root_cause="model_or_task_score",
                platform_outcome="success",
                score_outcome="failed",
                rerun_recommendation="not_rerunnable",
                message=None,
                reason="score_failure",
                category="score",
                attribution="model_or_task",
            )
        return _common(
            reason_code="trial.succeeded",
            failure_class="platform_success",
            root_cause="none",
            platform_outcome="success",
            score_outcome=("passed" if reward is not None else "unscored"),
            rerun_recommendation="not_needed",
            message=None,
            reason=None,
            category="none",
            attribution="none",
        )

    if state == "cancelled":
        return _common(
            reason_code="trial.cancelled",
            reason="cancelled",
            failure_class="operator_cancelled",
            root_cause="operator_cancelled",
            platform_outcome="cancelled",
            score_outcome="unscored",
            rerun_recommendation="operator_approval",
            message=message,
            category="cancelled",
            attribution="user_or_platform",
            rerunnable=True,
            requires_operator_approval=True,
        )

    if state in _ACTIVE_TRIAL_STATES:
        # State alone cannot establish staleness. Reclaim/deadline evidence is
        # owned by stale-running diagnostics and the lifecycle controller.
        return _common(
            reason_code=f"trial.{state}",
            reason=None,
            failure_class="active",
            root_cause="none",
            platform_outcome="active",
            score_outcome="unscored",
            rerun_recommendation="wait",
            message=None,
            category="active",
            attribution="pending",
        )

    if reason == "task_image_build_failed" and isinstance(result, dict):
        snapshot = result.get("task_image_preparation_failure")
        typed = image_preparation_failure(snapshot.get("reason")) if isinstance(snapshot, dict) else None
        if typed is not None:
            return _common(
                reason_code="trial." + typed["reason"], reason=typed["reason"],
                failure_class="platform_failure" if typed["attribution"] == "platform" else "unknown_failure",
                root_cause="image_preparation", platform_outcome="failed", score_outcome="unscored",
                rerun_recommendation="operator_approval" if typed["retryable"] else "not_rerunnable",
                message=typed["message"], category="image_preparation", attribution=typed["attribution"],
                rerunnable=typed["retryable"], requires_operator_approval=typed["retryable"],
            )

    if reason is not None and reason in _TASK_FAILURE_REASONS:
        return _common(
            reason_code=f"trial.{reason}",
            reason=reason,
            failure_class="task_failure",
            root_cause=_TASK_FAILURE_REASONS[reason],
            platform_outcome="failed",
            score_outcome="unscored",
            rerun_recommendation="not_rerunnable",
            message=message,
            category="task",
            attribution="benchmark",
            requires_task_change=True,
        )

    if reason == "missing_required_artifacts":
        return _common(
            reason_code="trial.missing_required_artifacts",
            reason=reason,
            failure_class="artifact_failure",
            root_cause="missing_required_artifacts",
            platform_outcome="invalid_evidence",
            score_outcome="unscored",
            rerun_recommendation="auto_safe",
            message=message,
            category="artifact",
            attribution="platform",
            rerunnable=True,
        )

    if reason in _AUTO_SAFE_REASONS:
        root = "provider_transport" if reason in {
            "gateway_error",
            "provider_transport_disconnect",
        } else "platform_transient"
        return _common(
            reason_code=f"trial.{reason}",
            reason=reason,
            failure_class="platform_failure",
            root_cause=root,
            platform_outcome="failed",
            score_outcome="unscored",
            rerun_recommendation="auto_safe",
            message=message,
            category=("gateway" if root == "provider_transport" else "platform"),
            attribution=("provider" if root == "provider_transport" else "platform"),
            rerunnable=True,
        )

    if reason is not None and reason in _VERIFIER_FAILURE_REASONS:
        return _common(
            reason_code=f"trial.{reason}",
            reason=reason,
            failure_class="verifier_failure",
            root_cause=_VERIFIER_FAILURE_REASONS[reason],
            platform_outcome="failed",
            score_outcome="unscored",
            rerun_recommendation="operator_approval",
            message=message,
            category="verifier",
            attribution="benchmark",
            rerunnable=True,
            requires_operator_approval=True,
        )

    if reason is not None and reason in _ARTIFACT_FAILURE_REASONS:
        return _common(
            reason_code=f"trial.{reason}",
            reason=reason,
            failure_class="artifact_failure",
            root_cause=_ARTIFACT_FAILURE_REASONS[reason],
            platform_outcome="failed",
            score_outcome="unscored",
            rerun_recommendation="auto_safe",
            message=message,
            category="artifact",
            attribution="platform",
            rerunnable=True,
        )

    if reason == "oom_killed":
        return _common(
            reason_code="trial.oom_killed", reason=reason,
            failure_class="platform_failure", root_cause="memory_limit",
            platform_outcome="failed", score_outcome="unscored",
            rerun_recommendation="operator_approval", message=message,
            category="resource", attribution="resource_limit", rerunnable=True,
            requires_operator_approval=True,
        )

    if reason in _AGENT_FAILURE_REASONS:
        return _common(
            reason_code=f"trial.{reason}",
            reason=reason,
            failure_class="agent_failure",
            root_cause="agent_runtime",
            platform_outcome="failed",
            score_outcome="unscored",
            rerun_recommendation="operator_approval",
            message=message,
            category="agent",
            attribution="model",
            rerunnable=True,
            requires_operator_approval=True,
        )

    if reason in _STEP_CREDENTIAL_REASONS:
        return _common(
            reason_code=f"trial.{reason}",
            reason=reason,
            failure_class="platform_failure",
            root_cause="step_credential",
            platform_outcome="failed",
            score_outcome="unscored",
            rerun_recommendation="operator_approval",
            message=message,
            category="gateway",
            attribution="platform",
            rerunnable=True,
            requires_operator_approval=True,
        )

    if reason in _PLATFORM_SETUP_REASONS:
        return _common(
            reason_code=f"trial.{reason}",
            reason=reason,
            failure_class="platform_failure",
            root_cause="platform_setup",
            platform_outcome="failed",
            score_outcome="unscored",
            rerun_recommendation="operator_approval",
            message=message,
            category="platform",
            attribution="platform",
            rerunnable=True,
            requires_operator_approval=True,
        )

    if reason in _PROVIDER_NO_CALL_REASONS:
        return _common(
            reason_code=f"trial.{reason}",
            reason=reason,
            failure_class="provider_failure",
            root_cause="provider_no_call",
            platform_outcome="failed",
            score_outcome="unscored",
            rerun_recommendation="operator_approval",
            message=message,
            category="provider",
            attribution="provider",
            rerunnable=True,
            requires_operator_approval=True,
        )

    if reason in _PROVIDER_TIMEOUT_REASONS:
        return _common(
            reason_code=f"trial.{reason}",
            reason=reason,
            failure_class="provider_failure",
            root_cause="provider_timeout",
            platform_outcome="failed",
            score_outcome="unscored",
            rerun_recommendation="auto_safe",
            message=message,
            category="provider",
            attribution="provider",
            rerunnable=True,
        )

    if reason in _PROVIDER_APPROVAL_REASONS:
        return _common(
            reason_code=f"trial.{reason}",
            reason=reason,
            failure_class="provider_failure",
            root_cause="provider_upstream",
            platform_outcome="failed",
            score_outcome="unscored",
            rerun_recommendation="operator_approval",
            message=message,
            category="provider",
            attribution="provider",
            rerunnable=True,
            requires_operator_approval=True,
        )

    return _common(
        reason_code=(f"trial.{reason}" if reason else "trial.failed_unknown"),
        reason=reason,
        failure_class="unknown_failure",
        root_cause="unknown",
        platform_outcome=("failed" if state == "failed" else state),
        score_outcome="unscored",
        rerun_recommendation="operator_approval" if state == "failed" else "inspect",
        message=message,
        category="unknown",
        attribution="unknown",
        rerunnable=state == "failed",
        requires_operator_approval=state == "failed",
    )


def is_auto_safe_rerun(trial: Any) -> bool:
    return str(classify_trial_outcome(trial).get("rerun_recommendation")) == "auto_safe"


def is_replaceable_by_successful_supplemental(trial: Any) -> bool:
    classification = classify_trial_outcome(trial)
    return classification["failure_class"] not in {
        "active",
        "platform_success",
        "score_failure",
        "task_failure",
    }


def _target_record(trial: Any, classification: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "task_id": str(trial.task_id),
        "sample_idx": int(getattr(trial, "sample_idx", 0) or 0),
        "combination_idx": int(getattr(trial, "combination_idx", 0) or 0),
        "original_trial_id": str(trial.id),
        "failure_reason": getattr(trial, "failure_reason", None),
        "reason_code": classification["reason_code"],
        "failure_class": classification["failure_class"],
        "root_cause": classification["root_cause"],
        "platform_outcome": classification["platform_outcome"],
        "score_outcome": classification["score_outcome"],
        "rerun_recommendation": classification["rerun_recommendation"],
        "requires_operator_approval": classification["requires_operator_approval"],
        "requires_task_change": classification["requires_task_change"],
    }


def _coordinate_record(target: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "task_id": target["task_id"],
        "sample_idx": target["sample_idx"],
        "combination_idx": target["combination_idx"],
    }


def _sort_trials(trials: Iterable[Any]) -> list[Any]:
    return sorted(
        trials,
        key=lambda trial: (
            _trial_key(trial),
            str(getattr(trial, "batch_id", "")),
            str(getattr(trial, "id", "")),
        ),
    )


def _successful_supplemental_by_key(trials: Sequence[Any]) -> dict[tuple[str, int, int], Any]:
    selected: dict[tuple[str, int, int], Any] = {}
    for trial in _sort_trials(trials):
        if str(getattr(trial, "state", "")) != "succeeded":
            continue
        selected[_trial_key(trial)] = trial
    return selected


def _task_id_filter(task_ids: Sequence[str] | None) -> set[str] | None:
    if task_ids is None:
        return None
    values = {str(task_id) for task_id in task_ids if str(task_id)}
    return values or set()


def build_supplemental_rerun_plan(
    batch: Any,
    trials: Sequence[Any],
    *,
    task_ids: Sequence[str] | None = None,
    supplemental_trials: Sequence[Any] | None = None,
    include_operator_approval: bool = False,
) -> dict[str, Any]:
    """Build a deterministic supplemental rerun plan for a batch.

    ``supplemental_task_ids`` is the list safe for automatic creation by
    default. Operator-approval rows remain visible in the plan and are only
    included when a caller explicitly opts into ``include_operator_approval``.
    """

    task_filter = _task_id_filter(task_ids)
    supplemental_by_key = _successful_supplemental_by_key(supplemental_trials or [])
    auto_safe: list[dict[str, Any]] = []
    operator_approval: list[dict[str, Any]] = []
    not_rerunnable: list[dict[str, Any]] = []
    final_selection: list[dict[str, Any]] = []
    already_covered = 0

    for trial in _sort_trials(trials):
        task_id = str(trial.task_id)
        if task_filter is not None and task_id not in task_filter:
            continue
        classification = classify_trial_outcome(trial)
        key = _trial_key(trial)
        supplemental = supplemental_by_key.get(key)
        selected = trial
        selected_source = "main"
        if supplemental is not None and is_replaceable_by_successful_supplemental(trial):
            selected = supplemental
            selected_source = "supplemental"
            already_covered += 1

        selected_batch_id = getattr(selected, "batch_id", None)
        final_selection.append(
            {
                "task_id": task_id,
                "sample_idx": key[1],
                "combination_idx": key[2],
                "selected_trial_id": str(selected.id),
                "selected_batch_id": str(selected_batch_id) if selected_batch_id is not None else None,
                "selected_source": selected_source,
                "original_trial_id": str(trial.id),
                "original_failure_class": classification["failure_class"],
            }
        )

        if selected_source == "supplemental":
            continue
        target = _target_record(trial, classification)
        recommendation = classification["rerun_recommendation"]
        if recommendation == "auto_safe":
            auto_safe.append(target)
        elif recommendation == "operator_approval":
            operator_approval.append(target)
        elif classification["failure_class"] in {"score_failure", "task_failure"}:
            not_rerunnable.append(target)

    supplemental_targets = list(auto_safe)
    if include_operator_approval:
        supplemental_targets.extend(operator_approval)
    supplemental_task_ids = sorted({target["task_id"] for target in supplemental_targets})
    supplemental_coordinates = [
        _coordinate_record(target)
        for target in sorted(
            supplemental_targets,
            key=lambda target: (
                str(target["task_id"]),
                int(target["sample_idx"]),
                int(target["combination_idx"]),
                str(target["original_trial_id"]),
            ),
        )
    ]

    return {
        "schema_version": "1",
        "batch_id": str(batch.id),
        "rerun_of_batch_id": str(batch.id),
        "supplemental_task_ids": supplemental_task_ids,
        "supplemental_coordinates": supplemental_coordinates,
        "summary": {
            "auto_safe": len(auto_safe),
            "operator_approval": len(operator_approval),
            "not_rerunnable": len(not_rerunnable),
            "already_covered": already_covered,
            "selected_final_trials": len(final_selection),
        },
        "auto_safe": auto_safe,
        "operator_approval": operator_approval,
        "not_rerunnable": not_rerunnable,
        "final_trial_selection": final_selection,
    }


def classification_counts(trials: Sequence[Any]) -> dict[str, int]:
    counts = Counter(
        cast(str, classify_trial_outcome(trial)["failure_class"])
        for trial in trials
        if classify_trial_outcome(trial)["failure_class"] not in {"platform_success", "active"}
    )
    return dict(sorted(counts.items()))


def rerun_recommendation_counts(trials: Sequence[Any]) -> dict[str, int]:
    counts = Counter(
        cast(str, classify_trial_outcome(trial)["rerun_recommendation"])
        for trial in trials
        if classify_trial_outcome(trial)["rerun_recommendation"] not in {
            "not_needed",
            "inspect",
            "wait",
        }
    )
    return dict(sorted(counts.items()))
