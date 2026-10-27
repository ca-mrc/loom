"""Which machine grades a trial.

``separate`` is the default and the historical Nebius path: tar the workdir
and grade in ``verifier-sandbox``. ``shared`` is opt-in: inject tests into
``task-sandbox`` after the agent phase returns.

Batch ``trial.verifier_env_mode`` wins. Otherwise the task field is used.
A task that omits the field is ``separate``; Harbor's omitted-means-shared
default is not applied here.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from loom.models.task import TaskConfig
from loom.models.trial import TrialConfig
from loom.models.types import VerifierEnvMode

if TYPE_CHECKING:
    from loom.execution_runtime_contract import ExecutionRuntimePlanV1


def resolve_verifier_env_mode(task: TaskConfig, trial: TrialConfig) -> VerifierEnvMode:
    if trial.verifier_env_mode is not None:
        return trial.verifier_env_mode
    return task.verifier.env_mode


def apply_legacy_verifier_default(
    task: TaskConfig,
    trial: TrialConfig,
    *,
    task_checksum: str,
    legacy_separate_verifier_checksum: str | None,
    source_provenance: Mapping[str, Any],
) -> TrialConfig:
    """Freeze the pre-0167 separate behavior without rewriting a task revision.

    Migration 0169 restores only catalogs whose frozen image proves the exact
    0167 rewrite. The marker is bound to that legacy checksum, so later task
    revisions and manifest-backed publications cannot inherit the fallback.
    Explicit user overrides always win. Persist the returned TrialConfig at
    submission; historical task/image/grant snapshots remain unchanged.
    """
    if (
        trial.verifier_env_mode is None
        and task.verifier.env_mode == "shared"
        and legacy_separate_verifier_checksum is not None
        and task_checksum.removeprefix("sha256:") == legacy_separate_verifier_checksum
        and "bundle_content_manifest_sha256" not in source_provenance
    ):
        return trial.model_copy(update={"verifier_env_mode": "separate"})
    return trial


def native_independent_verifier_handoff(task: TaskConfig) -> bool:
    """Converted native tasks transfer declared artifacts for independent grading.

    The controller additionally checks the immutable wrapper bytes before using
    this lifecycle; a generic separate verifier keeps the full workdir contract.
    """
    from loom.harbor_verifier_script import VERIFIER_SCRIPT_PATH
    return bool(task.upstream_conversion is not None
                and task.upstream_conversion.importer == "loom.harbor-native.v1"
                and task.verifier.environment is not None
                and task.verifier.args.get("script_path") == VERIFIER_SCRIPT_PATH)


def verifier_execution_task(task: TaskConfig) -> TaskConfig:
    """Project only the explicit grading environment for its independent lease."""
    if task.verifier.environment is None:
        return task
    return task.model_copy(update={"environment": task.verifier.environment})


def validate_prepared_sandbox_images(plan: ExecutionRuntimePlanV1, prepared_task: TaskConfig) -> None:
    """Bind every private sandbox image to the Trial's locked materialization."""
    from loom.task_environment_handoff import (
        RESOLVED_TASK_ENVIRONMENT,
        RESOLVED_VERIFIER_ENVIRONMENT,
        decode_resolved_task_environment,
    )
    raw = plan.main.environment.get(RESOLVED_TASK_ENVIRONMENT)
    if raw is not None:
        resolved = decode_resolved_task_environment(raw)
        if resolved != prepared_task.environment:
            raise ValueError("runtime environment does not match the trial's prepared environment")
    raw_verifier = plan.main.environment.get(RESOLVED_VERIFIER_ENVIRONMENT)
    verifier_environment = prepared_task.verifier.environment
    if ((raw_verifier is None) != (verifier_environment is None)
            or (raw_verifier is not None
                and decode_resolved_task_environment(raw_verifier) != verifier_environment)):
        raise ValueError("runtime verifier environment does not match the trial's prepared environment")
    for sidecar in plan.sidecars:
        if not sidecar.private_sandbox:
            continue
        environment = (prepared_task.verifier.environment
                       if sidecar.role_name == "verifier-sandbox" and prepared_task.verifier.environment is not None
                       else prepared_task.environment)
        if sidecar.image_ref != environment.docker_image:
            raise ValueError("runtime sandbox image does not match the trial's prepared environment")


def independent_verifier_rejections(task: TaskConfig, trial: TrialConfig, *, allow_image_preparation: bool = False) -> tuple[str, ...]:
    """Field-specific limits of the shared hosted independent grading path."""
    from loom.execution_contract import effective_guest_capabilities
    from loom.runtime_environment import validate_task_environment
    env = task.verifier.environment
    if env is None or trial.skip_verifier:
        return ()
    reasons: list[str] = []
    if resolve_verifier_env_mode(task, trial) != "separate":
        reasons.append("verifier.environment: separate_mode_required")
    if (effective_guest_capabilities(task, trial) is not None
            and (task.environment.native_compose is None
                 or effective_guest_capabilities(verifier_execution_task(task), trial) is not None)):
        reasons.append("verifier.environment: independent_guest_environment_unavailable")
    if env.os != task.environment.os or env.cpu_arch not in {"any", task.environment.cpu_arch}:
        reasons.append("verifier.environment.cpu_arch: independent_architecture_unavailable")
    if env.cpus is None or env.memory_mb is None or env.storage_mb is None:
        reasons.append("verifier.environment: explicit_resources_required")
    if env.sidecars or env.compose_files:
        reasons.append("verifier.environment.sidecars: independent_services_unavailable")
    if (env.user == "image" or env.image_workdir or env.image_entrypoint) and not (
            allow_image_preparation and env.dockerfile is not None):
        reasons.append("verifier.environment: image_runtime_metadata_required")
    if env.dockerfile is not None and not allow_image_preparation:
        reasons.append("verifier.environment.dockerfile: prepared_image_required")
    try:
        validate_task_environment(env.environment)
        validate_task_environment(task.verifier.environment_vars)
    except ValueError:
        reasons.append("verifier.environment: unsafe_environment_variables")
    return tuple(reasons)
