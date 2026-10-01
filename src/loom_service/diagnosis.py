"""Human-readable diagnosis reports derived from debug evidence.

The diagnosis layer is intentionally deterministic. It summarizes the
redacted debug evidence that normal users can already fetch, without reading
operator-only logs or calling an LLM.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, cast

from loom.security.redaction import redact_mapping


@dataclass(frozen=True)
class _ReasonMeta:
    label: str
    category: str
    attribution: str
    trial_summary: str
    batch_summary: str
    impact: str
    actions: tuple[str, ...]


_GENERIC_ACTION = "Inspect trajectory, artifacts, and debug evidence."

_BATCH_FAILURE_SUMMARIES: dict[str, str] = {
    "batch.fanout_submit_failed": (
        "The batch could not submit child trials during fan-out."
    ),
    "batch.all_failed": "The batch failed without a dominant child-trial cluster.",
    "batch.partial_failed": (
        "The batch partially failed without a dominant child-trial cluster."
    ),
    "batch.cancelled": "The batch was cancelled before all work completed.",
    "batch.no_llm_calls": (
        "The batch finished terminal model-backed trials but did not record any LLM calls."
    ),
}

_BATCH_FAILURE_ACTIONS: dict[str, tuple[str, ...]] = {
    "batch.fanout_submit_failed": ("inspect_fanout",),
    "batch.all_failed": ("generic",),
    "batch.partial_failed": ("generic",),
    "batch.cancelled": ("clone_config",),
    "batch.no_llm_calls": ("provider_preflight", "inspect_trajectory", "rerun_failed"),
}

_REASON_META: dict[str, _ReasonMeta] = {
    "trial.oom_killed": _ReasonMeta(
        label="Container memory limit exceeded", category="resource", attribution="resource_limit",
        trial_summary="A container exceeded its memory limit and was terminated by the system.",
        batch_summary="Failed trials include confirmed container memory-limit terminations.",
        impact="Execution stopped; retained partial trajectories and usage remain available.",
        actions=("inspect_trajectory", "generic"),
    ),
    "trial.gateway_error": _ReasonMeta(
        label="Provider gateway failure",
        category="gateway",
        attribution="provider",
        trial_summary=(
            "The trial failed before scoring because the provider gateway "
            "returned an error."
        ),
        batch_summary=(
            "The batch failed because most failed child trials hit provider "
            "gateway errors before scoring."
        ),
        impact=(
            "The aggregate score is not reliable for model-quality comparison "
            "because affected trials did not reach benchmark scoring."
        ),
        actions=("provider_preflight", "rerun_failed"),
    ),
    "trial.provider_error": _ReasonMeta(
        label="Provider request failure",
        category="provider",
        attribution="provider",
        trial_summary=(
            "The trial failed before scoring because the model provider "
            "reported an error."
        ),
        batch_summary=(
            "The batch failed because most failed child trials hit provider "
            "request errors before scoring."
        ),
        impact=(
            "The aggregate score is not reliable for model-quality comparison "
            "until provider-side errors are separated from model quality."
        ),
        actions=("provider_preflight", "rerun_failed"),
    ),
    "trial.provider_transport_disconnect": _ReasonMeta(
        label="Provider transport disconnect",
        category="gateway",
        attribution="provider",
        trial_summary=(
            "The trial failed before scoring because the provider transport "
            "disconnected before returning a response."
        ),
        batch_summary=(
            "The batch has provider transport disconnects before benchmark scoring."
        ),
        impact=(
            "The aggregate score is not reliable for model-quality comparison "
            "because affected trials did not receive model responses."
        ),
        actions=("provider_preflight", "rerun_failed"),
    ),
    "trial.step_credential_invalid_or_expired": _ReasonMeta(
        label="Invalid or expired step credential",
        category="gateway",
        attribution="platform",
        trial_summary=(
            "The trial stopped before scoring because Loom rejected its step credential."
        ),
        batch_summary=(
            "The batch has step credentials that became invalid or expired before completion."
        ),
        impact=(
            "The affected score is platform-failure evidence, not a model-quality result."
        ),
        actions=("inspect_trajectory",),
    ),
    "trial.step_credential_scope_invalid": _ReasonMeta(
        label="Invalid step credential scope",
        category="gateway",
        attribution="platform",
        trial_summary=(
            "The trial stopped before scoring because its step credential lacked llm:call."
        ),
        batch_summary=(
            "The batch has step credentials with an invalid Loom gateway scope."
        ),
        impact=(
            "The affected score is platform-failure evidence, not a model-quality result."
        ),
        actions=("inspect_trajectory",),
    ),
    "trial.verifier_error": _ReasonMeta(
        label="Benchmark verifier failure",
        category="verifier",
        attribution="benchmark",
        trial_summary=(
            "The trial reached the benchmark verifier, but the verifier "
            "reported an error."
        ),
        batch_summary=(
            "The batch failed because most failed child trials hit benchmark "
            "verifier errors."
        ),
        impact=(
            "The aggregate score is not reliable for affected tasks until the "
            "verifier output and benchmark assets are inspected."
        ),
        actions=("inspect_verifier",),
    ),
    "trial.verifier_timeout": _ReasonMeta(
        label="Benchmark verifier timeout",
        category="verifier",
        attribution="benchmark",
        trial_summary="The benchmark verifier timed out before producing reward.",
        batch_summary=(
            "The batch failed because most failed child trials timed out in "
            "the benchmark verifier."
        ),
        impact=(
            "The aggregate score is not reliable for timed-out tasks because "
            "reward production did not complete."
        ),
        actions=("inspect_verifier",),
    ),
    "trial.env_start_failure": _ReasonMeta(
        label="Sandbox start failure",
        category="sandbox",
        attribution="platform",
        trial_summary="The trial failed while starting the task environment.",
        batch_summary=(
            "The batch failed because most failed child trials could not start "
            "their task environments."
        ),
        impact=(
            "The aggregate score is not reliable for affected tasks because "
            "the model did not get a runnable environment."
        ),
        actions=("inspect_sandbox",),
    ),
    "trial.env_healthcheck_failed": _ReasonMeta(
        label="Sandbox healthcheck failure",
        category="sandbox",
        attribution="platform",
        trial_summary=(
            "The trial environment started but failed its healthcheck before "
            "agent execution."
        ),
        batch_summary=(
            "The batch failed because most failed child trials failed sandbox "
            "healthchecks."
        ),
        impact=(
            "The aggregate score is not reliable for affected tasks because "
            "the benchmark environment was not healthy."
        ),
        actions=("inspect_sandbox",),
    ),
    "trial.task_image_build_timeout": _ReasonMeta(
        label="Task image build timeout",
        category="sandbox",
        attribution="platform",
        trial_summary=(
            "The trial timed out while building the task Docker image before "
            "agent execution."
        ),
        batch_summary=(
            "The batch has task-image build timeouts before model execution."
        ),
        impact=(
            "The score is not model-quality evidence for affected tasks "
            "because the runtime environment was not ready."
        ),
        actions=("inspect_sandbox", "rerun_failed"),
    ),
    "trial.task_compatibility": _ReasonMeta(
        label="Task compatibility failure",
        category="task",
        attribution="benchmark",
        trial_summary=(
            "The trial hit a task-bundle compatibility problem before model "
            "execution."
        ),
        batch_summary=(
            "The batch has task-bundle compatibility failures that should be "
            "fixed in the task assets before rerun."
        ),
        impact=(
            "The score is not model-quality evidence for affected tasks "
            "because the task environment or agent layer could not be prepared."
        ),
        actions=("inspect_sandbox",),
    ),
    "trial.agent_error": _ReasonMeta(
        label="Agent execution failure",
        category="agent",
        attribution="model",
        trial_summary="The agent failed while attempting the task.",
        batch_summary=(
            "The batch failed because most failed child trials hit agent "
            "execution errors."
        ),
        impact=(
            "The score may reflect agent/runtime behavior rather than only "
            "model capability."
        ),
        actions=("inspect_trajectory",),
    ),
    "trial.agent_timeout": _ReasonMeta(
        label="Agent timeout",
        category="agent",
        attribution="model",
        trial_summary="The agent timed out before completing the task.",
        batch_summary=(
            "The batch failed because most failed child trials timed out during "
            "agent execution."
        ),
        impact=(
            "The score may reflect timeout policy or task/runtime latency "
            "rather than only model capability."
        ),
        actions=("inspect_trajectory",),
    ),
    "trial.artifact_upload_failed": _ReasonMeta(
        label="Artifact upload failure",
        category="artifact",
        attribution="platform",
        trial_summary="The trial finished execution but failed artifact upload.",
        batch_summary=(
            "The batch has repeated artifact upload failures after trial "
            "execution."
        ),
        impact=(
            "Reward may exist, but run evidence and reusable artifacts are "
            "incomplete for affected trials."
        ),
        actions=("inspect_artifacts",),
    ),
    "trial.trajectory_flush_failed": _ReasonMeta(
        label="Trajectory upload failure",
        category="artifact",
        attribution="platform",
        trial_summary=(
            "The trial finished execution but failed to flush trajectory data."
        ),
        batch_summary=(
            "The batch has repeated trajectory flush failures after trial "
            "execution."
        ),
        impact=(
            "Reward may exist, but traceability is incomplete for affected "
            "trials."
        ),
        actions=("inspect_artifacts",),
    ),
    "trial.retry_exhausted": _ReasonMeta(
        label="Retries exhausted",
        category="retry",
        attribution="platform",
        trial_summary="The trial exhausted retry attempts before succeeding.",
        batch_summary=(
            "The batch failed because most failed child trials exhausted retry "
            "attempts."
        ),
        impact=(
            "The aggregate score is not reliable until the underlying repeated "
            "failure reason is inspected."
        ),
        actions=("inspect_retries", "rerun_failed"),
    ),
    "trial.exhausted_retries": _ReasonMeta(
        label="Retries exhausted",
        category="retry",
        attribution="platform",
        trial_summary="The trial exhausted retry attempts before succeeding.",
        batch_summary=(
            "The batch failed because most failed child trials exhausted retry "
            "attempts."
        ),
        impact=(
            "The aggregate score is not reliable until the underlying repeated "
            "failure reason is inspected."
        ),
        actions=("inspect_retries", "rerun_failed"),
    ),
    "trial.no_llm_calls": _ReasonMeta(
        label="No LLM calls recorded",
        category="gateway",
        attribution="platform",
        trial_summary=(
            "The terminal model-backed trial did not record any LLM calls."
        ),
        batch_summary=(
            "The batch has terminal model-backed trials that did not record "
            "any LLM calls."
        ),
        impact=(
            "The reward is not valid benchmark evidence for model-quality "
            "comparison because the model path did not reach the gateway."
        ),
        actions=("provider_preflight", "inspect_trajectory", "rerun_failed"),
    ),
    "trial.succeeded": _ReasonMeta(
        label="Succeeded",
        category="none",
        attribution="none",
        trial_summary="The trial completed and produced a terminal result.",
        batch_summary="The batch completed without a dominant failure cluster.",
        impact="The reward is usable for model-quality comparison.",
        actions=("inspect_result",),
    ),
    "trial.cancelled": _ReasonMeta(
        label="Cancelled",
        category="cancelled",
        attribution="user_or_platform",
        trial_summary="The trial was cancelled before completion.",
        batch_summary="The batch was cancelled before all work completed.",
        impact=(
            "The aggregate score is not reliable because cancelled work did "
            "not complete normally."
        ),
        actions=("clone_config",),
    ),
}

_ACTION_LABELS: dict[str, dict[str, str]] = {
    "follow_progress": {
        "label": "Follow task progress and live trajectory events.",
        "kind": "manual",
    },
    "inspect_fanout": {
        "label": "Inspect batch fan-out errors",
        "kind": "manual",
    },
    "inspect_verifier": {
        "label": "Inspect verifier output and benchmark task assets",
        "kind": "manual",
    },
    "inspect_sandbox": {
        "label": "Inspect sandbox image, healthcheck, and worker capacity",
        "kind": "manual",
    },
    "inspect_trajectory": {
        "label": "Inspect trajectory events around the agent step",
        "kind": "manual",
    },
    "inspect_artifacts": {
        "label": "Inspect artifact and trajectory upload references",
        "kind": "manual",
    },
    "inspect_retries": {
        "label": "Inspect previous attempts and repeated retry causes",
        "kind": "manual",
    },
    "inspect_result": {
        "label": "Use reward and artifacts to interpret model quality",
        "kind": "manual",
    },
    "clone_config": {
        "label": "Clone the same config if cancellation was accidental",
        "kind": "web_action",
        "action": "clone_config",
    },
    "rerun_failed": {
        "label": "Rerun failed trials after the provider path is healthy",
        "kind": "web_action",
        "action": "rerun_failed",
    },
}


def _iso_now() -> str:
    return datetime.now(UTC).isoformat()


def _mapping(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    return {}


def _sequence(value: Any) -> Sequence[Any]:
    if isinstance(value, Sequence) and not isinstance(value, str | bytes):
        return value
    return ()


def _reason_meta(
    reason_code: str,
    *,
    category: str | None = None,
    attribution: str | None = None,
) -> _ReasonMeta:
    if reason_code in _REASON_META:
        return _REASON_META[reason_code]
    label = reason_code.replace("trial.", "").replace("_", " ")
    if label:
        label = label[0].upper() + label[1:]
    return _ReasonMeta(
        label=label or "Unknown failure",
        category=category or "unknown",
        attribution=attribution or "unknown",
        trial_summary=(
            f"The trial ended with {reason_code}; inspect debug evidence for "
            "the specific failure context."
        ),
        batch_summary=(
            f"The batch has repeated {reason_code} failures across child "
            "trials."
        ),
        impact=(
            "The aggregate score may not be reliable until the affected "
            "failures are inspected."
        ),
        actions=("generic",),
    )


def _provider_model(evidence: Mapping[str, Any]) -> str | None:
    provider = _mapping(evidence.get("provider"))
    provider_model_id = provider.get("provider_model_id")
    if isinstance(provider_model_id, str) and provider_model_id:
        return provider_model_id
    models = _sequence(provider.get("models"))
    for model in models:
        if isinstance(model, str) and model:
            return model.split("/", 1)[-1] if "/" in model else model
    return None


def _action(action_id: str, *, evidence: Mapping[str, Any]) -> dict[str, str]:
    if action_id == "provider_preflight":
        model = _provider_model(evidence)
        command = "loom providers models --preflight"
        if model:
            command = f"{command} {model}"
        return {
            "label": "Run provider preflight",
            "kind": "cli_command",
            "command": command,
        }
    if action_id == "generic":
        return {"label": _GENERIC_ACTION, "kind": "manual"}
    return dict(_ACTION_LABELS[action_id])


def _dedupe_actions(actions: Sequence[dict[str, str]]) -> list[dict[str, str]]:
    seen: set[tuple[tuple[str, str], ...]] = set()
    out: list[dict[str, str]] = []
    for action in actions:
        key = tuple(sorted(action.items()))
        if key in seen:
            continue
        seen.add(key)
        out.append(action)
    return out


def _evidence_bullets(
    evidence: Mapping[str, Any],
    *,
    reason_code: str,
    affected: int,
    total: int,
) -> list[str]:
    bullets = [f"{affected}/{total} affected trial(s) matched {reason_code}"] if affected else []
    lifecycle = _mapping(evidence.get("lifecycle"))
    state = lifecycle.get("state")
    if isinstance(state, str) and state:
        bullets.append(f"Lifecycle state: {state}")
    provider = _mapping(evidence.get("provider"))
    calls = provider.get("llm_calls_count")
    if isinstance(calls, int):
        bullets.append(f"LLM calls observed: {calls}")
    model = _provider_model(evidence)
    if model:
        bullets.append(f"Provider model: {model}")
    failure = _mapping(evidence.get("failure"))
    message = failure.get("message")
    if isinstance(message, str) and message:
        bullets.append(f"Failure message: {message}")
    return bullets


def build_trial_diagnosis(evidence: Mapping[str, Any]) -> dict[str, Any]:
    """Build a deterministic diagnosis report for one trial."""
    entity = _mapping(evidence.get("entity"))
    failure = _mapping(evidence.get("failure"))
    reason_code = str(failure.get("reason_code") or "trial.failed_unknown")
    meta = _reason_meta(
        reason_code,
        category=(
            str(failure["category"]) if isinstance(failure.get("category"), str)
            else None
        ),
        attribution=(
            str(failure["attribution"])
            if isinstance(failure.get("attribution"), str)
            else None
        ),
    )
    active = failure.get("platform_outcome") == "active"
    if active:
        meta = _ReasonMeta(
            label="In progress",
            category="active",
            attribution="pending",
            trial_summary=(
                f"The trial is still active ({_mapping(evidence.get('lifecycle')).get('state')}); "
                "follow task progress for the current stage."
            ),
            batch_summary="The batch is still active.",
            impact="Outcome and reward are pending until execution and output archival complete.",
            actions=("follow_progress",),
        )
    affected = 0 if active else 1
    actions = [_action(action_id, evidence=evidence) for action_id in meta.actions]
    report: dict[str, Any] = {
        "schema_version": "1",
        "generated_at": _iso_now(),
        "entity": {
            "type": str(entity.get("type") or "trial"),
            "id": str(entity.get("id") or ""),
        },
        "summary": meta.trial_summary,
        "primary_cause": {
            "reason_code": reason_code,
            "category": meta.category,
            "attribution": meta.attribution,
            "confidence": "high",
            "affected_trials": affected,
            "affected_ratio": float(affected),
        },
        "impact": meta.impact,
        "evidence": _evidence_bullets(
            evidence,
            reason_code=reason_code,
            affected=affected,
            total=1,
        ),
        "next_actions": _dedupe_actions(actions),
        "reason_clusters": [] if active else [
            {
                "reason_code": reason_code,
                "category": meta.category,
                "attribution": meta.attribution,
                "count": 1,
                "affected_ratio": float(affected),
                "representative_trial_id": str(entity.get("id") or ""),
                "representative_task_id": _mapping(evidence.get("task")).get(
                    "task_id",
                ),
            }
        ],
    }
    native = _mapping(evidence.get("execution_failure"))
    if native.get("reason") == "oom_killed" and reason_code == "trial.oom_killed" and not active:
        report["summary"] = str(native["message"])
        report["evidence"].extend([
            f"Stage: {native['stage']}; container: {native['container_role']}; incarnation: {native['container_incarnation']}.",
            f"Termination: {native.get('terminated_at') or 'unknown'}; exit code: {native.get('exit_code')}; memory limit: {native.get('memory_limit_mib')} MiB.",
            "Kernel termination evidence is independent of sampled peaks; missing peak data is not zero usage.",
        ])
    elif native.get("message") and reason_code == "trial.native_execution_failed" and not active:
        report["summary"] = str(native["message"])
        report["evidence"].append(
            f"Container: {native.get('container_role') or 'unattributed'}; "
            f"exit code: {native.get('exit_code')}; signal: {native.get('signal')}."
        )
    return cast(dict[str, Any], redact_mapping(report))


def _trial_summary_total(evidence: Mapping[str, Any]) -> int:
    trials = _mapping(evidence.get("trials"))
    summary = _mapping(trials.get("summary"))
    total = sum(value for value in summary.values() if isinstance(value, int))
    if total:
        return int(total)
    task_selection = _mapping(evidence.get("task_selection"))
    expected = task_selection.get("expected_trial_count")
    if isinstance(expected, int) and expected > 0:
        return expected
    failed_count = trials.get("failed_count")
    if isinstance(failed_count, int) and failed_count > 0:
        return failed_count
    return 1


def _cluster_trial_failures(
    trial_failures: Sequence[Mapping[str, Any]],
    *,
    total_trials: int,
) -> list[dict[str, Any]]:
    counts: Counter[str] = Counter()
    representatives: dict[str, Mapping[str, Any]] = {}
    for failure in trial_failures:
        reason_code = str(failure.get("reason_code") or "trial.failed_unknown")
        counts[reason_code] += 1
        representatives.setdefault(reason_code, failure)

    clusters: list[dict[str, Any]] = []
    denominator = max(total_trials, 1)
    for reason_code, count in counts.most_common():
        representative = representatives[reason_code]
        meta = _reason_meta(reason_code)
        clusters.append({
            "reason_code": reason_code,
            "category": meta.category,
            "attribution": meta.attribution,
            "count": count,
            "affected_ratio": count / denominator,
            "representative_trial_id": representative.get("id"),
            "representative_task_id": representative.get("task_id"),
        })
    return clusters


def trial_failure_records(trials: Sequence[Any]) -> list[dict[str, Any]]:
    """Project ORM-like trial rows into the diagnosis cluster input shape."""
    records: list[dict[str, Any]] = []
    for trial in trials:
        state = getattr(trial, "state", None)
        failure_reason = getattr(trial, "failure_reason", None)
        if state != "failed" and not failure_reason:
            continue
        records.append({
            "id": str(getattr(trial, "id", "")),
            "task_id": getattr(trial, "task_id", None),
            "state": state,
            "reason_code": (
                f"trial.{failure_reason}"
                if failure_reason else "trial.failed_unknown"
            ),
            "failure_reason": failure_reason,
        })
    return records


def _batch_summary(
    evidence: Mapping[str, Any],
    *,
    dominant: dict[str, Any] | None,
) -> str:
    lifecycle = _mapping(evidence.get("lifecycle"))
    status = lifecycle.get("terminal_status")
    state = lifecycle.get("state")
    if state in {"submitted", "queued", "claimed", "running"}:
        return "The batch is still active; diagnosis may change as trials finish."
    if not dominant:
        failure = _mapping(evidence.get("failure"))
        reason_code = str(failure.get("reason_code") or "")
        if reason_code in _BATCH_FAILURE_SUMMARIES:
            return _BATCH_FAILURE_SUMMARIES[reason_code]
    if status == "succeeded" or not dominant:
        return "The batch completed without a dominant failure cluster."
    reason_code = str(dominant.get("reason_code") or "batch.failed_unknown")
    meta = _reason_meta(reason_code)
    return meta.batch_summary


def _batch_impact(
    evidence: Mapping[str, Any],
    *,
    failure_count: int,
    total_trials: int,
) -> str:
    lifecycle = _mapping(evidence.get("lifecycle"))
    status = lifecycle.get("terminal_status")
    failure = _mapping(evidence.get("failure"))
    if failure.get("reason_code") == "batch.fanout_submit_failed":
        return (
            "The aggregate score is not reliable because the batch did not "
            "submit child trials for scoring."
        )
    if failure.get("reason_code") == "batch.no_llm_calls":
        return (
            "The aggregate score is invalid benchmark evidence because "
            "terminal model-backed trials did not record provider calls."
        )
    if status == "succeeded" and failure_count == 0:
        return "The aggregate score is reliable for model-quality comparison."
    if failure_count >= total_trials:
        return (
            "The aggregate score is not reliable for model-quality comparison "
            "because all observed trials failed before usable scoring."
        )
    if failure_count > 0:
        return (
            "The aggregate score is not reliable by itself for model-quality "
            "comparison until platform/provider failures are separated from "
            "scored trials."
        )
    if status == "cancelled":
        return (
            "The aggregate score is not reliable because the batch was "
            "cancelled before all work completed."
        )
    return "The aggregate score is still pending until more trials finish."


def build_batch_diagnosis(
    evidence: Mapping[str, Any],
    *,
    trial_failures: Sequence[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    """Build a deterministic diagnosis report for one batch."""
    entity = _mapping(evidence.get("entity"))
    provider = _mapping(evidence.get("provider"))
    if provider.get("llm_evidence_status") == "no_calls_invalid":
        total_trials = _trial_summary_total(evidence)
        affected = int(provider.get("no_call_trial_count") or 0)
        affected_ratio = affected / max(total_trials, 1)
        reason_code = "batch.no_llm_calls"
        action_ids = _BATCH_FAILURE_ACTIONS[reason_code]
        actions = [
            _action(action_id, evidence=evidence)
            for action_id in action_ids
        ]
        report = {
            "schema_version": "1",
            "generated_at": _iso_now(),
            "entity": {
                "type": str(entity.get("type") or "batch"),
                "id": str(entity.get("id") or ""),
            },
            "summary": _BATCH_FAILURE_SUMMARIES[reason_code],
            "primary_cause": {
                "reason_code": reason_code,
                "category": "gateway",
                "attribution": "platform",
                "confidence": "high",
                "affected_trials": affected,
                "affected_ratio": affected_ratio,
            },
            "impact": _batch_impact(
                {
                    **dict(evidence),
                    "failure": {
                        "reason_code": reason_code,
                        "category": "gateway",
                        "attribution": "platform",
                    },
                },
                failure_count=affected,
                total_trials=total_trials,
            ),
            "evidence": _evidence_bullets(
                evidence,
                reason_code=reason_code,
                affected=affected,
                total=total_trials,
            ),
            "next_actions": _dedupe_actions(actions),
            "reason_clusters": [
                {
                    "reason_code": reason_code,
                    "category": "gateway",
                    "attribution": "platform",
                    "count": affected,
                    "affected_ratio": affected_ratio,
                    "representative_trial_id": None,
                    "representative_task_id": None,
                }
            ],
        }
        return cast(dict[str, Any], redact_mapping(report))

    failures = trial_failures
    if failures is None:
        failures = cast(
            Sequence[Mapping[str, Any]],
            [
                item for item in _sequence(_mapping(evidence.get("trials")).get(
                    "failed",
                ))
                if isinstance(item, Mapping)
            ],
        )
    total_trials = _trial_summary_total(evidence)
    clusters = _cluster_trial_failures(failures, total_trials=total_trials)
    dominant = clusters[0] if clusters else None
    failure_count = sum(int(cluster["count"]) for cluster in clusters)
    if dominant:
        affected = int(dominant["count"])
        affected_ratio = float(dominant["affected_ratio"])
        confidence = (
            "high" if affected_ratio >= 0.8
            else "medium" if affected_ratio >= 0.5
            else "low"
        )
        reason_code = str(dominant["reason_code"])
        category = str(dominant["category"])
        attribution = str(dominant["attribution"])
        meta = _reason_meta(reason_code)
        action_ids = meta.actions
    else:
        failure = _mapping(evidence.get("failure"))
        reason_code = str(failure.get("reason_code") or "batch.succeeded")
        category = str(failure.get("category") or "none")
        attribution = str(failure.get("attribution") or "none")
        affected = 0
        affected_ratio = 0.0
        confidence = "high"
        action_ids = _BATCH_FAILURE_ACTIONS.get(reason_code, ("inspect_result",))

    actions = [_action(action_id, evidence=evidence) for action_id in action_ids]
    report = {
        "schema_version": "1",
        "generated_at": _iso_now(),
        "entity": {
            "type": str(entity.get("type") or "batch"),
            "id": str(entity.get("id") or ""),
        },
        "summary": _batch_summary(evidence, dominant=dominant),
        "primary_cause": {
            "reason_code": reason_code,
            "category": category,
            "attribution": attribution,
            "confidence": confidence,
            "affected_trials": affected,
            "affected_ratio": affected_ratio,
        },
        "impact": _batch_impact(
            evidence,
            failure_count=failure_count,
            total_trials=total_trials,
        ),
        "evidence": _evidence_bullets(
            evidence,
            reason_code=reason_code,
            affected=affected,
            total=total_trials,
        ),
        "next_actions": _dedupe_actions(actions),
        "reason_clusters": clusters,
    }
    return cast(dict[str, Any], redact_mapping(report))
