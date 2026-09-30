"""Declared task semantics missing from current runtimes; shared admission boundary."""

from __future__ import annotations

from loom.models.task import TaskConfig


def task_runtime_rejections(task: TaskConfig, *, agent_name: str | None = None) -> tuple[str, ...]:
    reasons = [f"{issue.path}:{issue.line}: {issue.code}" for issue in task.import_blockers]
    if task.solution_environment and agent_name == "oracle":
        reasons.append("solution_environment: oracle_environment_variables_runtime_unavailable")
    environments = [("environment", task.environment)]
    if task.verifier.environment is not None:
        environments.append(("verifier.environment", task.verifier.environment))
    for path, env in environments:
        if env.gpu_types:
            reasons.append(f"{path}.gpu_types: exact_gpu_model_runtime_unavailable")
        if env.compose_files:
            reasons.append(f"{path}.compose_files: native_compose_runtime_unavailable")
        for index, sidecar in enumerate(env.sidecars):
            if sidecar.healthcheck and sidecar.healthcheck.start_interval_sec is not None:
                reasons.append(
                    f"{path}.sidecars.{index}.healthcheck.start_interval_sec: healthcheck_start_interval_runtime_unavailable"
                )
        if env.healthcheck and env.healthcheck.start_interval_sec is not None:
            reasons.append(
                f"{path}.healthcheck.start_interval_sec: healthcheck_start_interval_runtime_unavailable"
            )
    if task.verifier.environment is not None:
        reasons.append("verifier.environment: independent_verifier_environment_runtime_unavailable")
    if task.verifier.environment_vars:
        reasons.append(
            "verifier.environment_vars: verifier_environment_variables_runtime_unavailable"
        )
    if task.verifier.collect:
        reasons.append("verifier.collect: service_collection_runtime_unavailable")
    for index, step in enumerate(task.steps):
        if step.artifact_sources:
            reasons.append(
                f"steps.{index}.artifact_sources: structured_artifact_collection_runtime_unavailable"
            )
    return tuple(reasons)
