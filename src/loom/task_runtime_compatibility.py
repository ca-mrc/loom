"""Declared task semantics missing from current runtimes; shared admission boundary."""

from __future__ import annotations

from typing import Literal

from loom.execution_architecture import execution_cpu_arch
from loom.models.task import TaskConfig


def task_hardware_rejections(task: TaskConfig) -> tuple[str, ...]:
    """Known unsupported hardware contracts, independent of queue capacity.

    No selected-target evidence is invented: this only rejects contracts the
    current runtime cannot satisfy on any execution target. Transient capacity
    and quota remain with the existing selected-backend admission.
    """
    reasons = list(_exact_accelerator_rejections(task))
    environments = [("environment", task.environment)]
    if task.verifier.environment is not None:
        environments.append(("verifier.environment", task.verifier.environment))
    for path, environment in environments:
        try:
            execution_cpu_arch(environment.cpu_arch)
        except ValueError:
            reasons.append(f"{path}.cpu_arch: unsupported_cpu_architecture")
    return tuple(reasons)


def _exact_accelerator_rejections(task: TaskConfig) -> tuple[str, ...]:
    environments = [("environment", task.environment)]
    if task.verifier.environment is not None:
        environments.append(("verifier.environment", task.verifier.environment))
    return tuple(
        f"{path}.gpu_types: exact_gpu_model_runtime_unavailable"
        for path, environment in environments if environment.gpu_types
    )


def task_runtime_rejections(
    task: TaskConfig, *, agent_name: str | None = None,
    runtime: Literal["hosted", "legacy"] = "hosted",
    check_target_capabilities: bool = True,
) -> tuple[str, ...]:
    reasons = [f"{issue.path}:{issue.line}: {issue.code}" for issue in task.import_blockers]
    if check_target_capabilities:
        reasons.extend(_exact_accelerator_rejections(task))
    if runtime == "hosted" and task.verifier.environment is not None:
        from loom.models.trial import TrialConfig
        from loom.verifier_runtime import independent_verifier_rejections

        reasons.extend(independent_verifier_rejections(
            task, TrialConfig(agent_name=agent_name or task.agent.name, agent_model=None), allow_image_preparation=True,
        ))
    environments = [("environment", task.environment)]
    if task.verifier.environment is not None:
        environments.append(("verifier.environment", task.verifier.environment))
    for path, env in environments:
        if env.compose_files and env.single_service_compose is None:
            if runtime == "legacy" or path != "environment" or env.native_compose is None:
                reasons.append(f"{path}.compose_files: native_compose_runtime_unavailable")
            elif task.verifier.environment is None or task.verifier.env_mode != "separate":
                reasons.append(f"{path}.compose_files: independent_verifier_required")
            if env.native_compose is not None and env.preserve_acls:
                reasons.append(f"{path}.preserve_acls: native_compose_acl_transfer_unavailable")
        for index, sidecar in enumerate(env.sidecars):
            if sidecar.healthcheck and sidecar.healthcheck.start_interval_sec is not None:
                reasons.append(
                    f"{path}.sidecars.{index}.healthcheck.start_interval_sec: healthcheck_start_interval_runtime_unavailable"
                )
    if task.verifier.environment is not None and runtime == "legacy":
        reasons.append("verifier.environment: independent_verifier_environment_runtime_unavailable")
    if task.verifier.environment_vars and runtime == "legacy":
        reasons.append(
            "verifier.environment_vars: verifier_environment_variables_runtime_unavailable"
        )
    service_names = {"main"}
    if runtime == "hosted" and task.environment.native_compose is not None:
        service_names.update(service.name for service in task.environment.native_compose.services)
    if task.verifier.collect and (runtime == "legacy" or any(
        hook.service not in service_names for hook in task.verifier.collect
    )):
        reasons.append("verifier.collect: service_collection_runtime_unavailable")
    for index, step in enumerate(task.steps):
        if runtime == "hosted":
            from loom.artifact_contract import artifact_contract_rejections

            reasons.extend(artifact_contract_rejections(step.artifact_sources, task.environment.workdir))
        if step.artifact_sources and (runtime == "legacy" or any(
            source.service is not None and source.service not in service_names for source in step.artifact_sources
        )):
            reasons.append(
                f"steps.{index}.artifact_sources: structured_artifact_service_runtime_unavailable"
            )
    return tuple(reasons)
