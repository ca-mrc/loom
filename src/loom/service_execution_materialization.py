"""Durable compilation of ordinary TaskSets into Nebius execution plans."""

from __future__ import annotations

import hashlib
import json
import re
import stat
from collections.abc import Iterable
from pathlib import Path, PurePosixPath
from typing import Any, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_serializer,
    model_validator,
)

from loom.agent_runtime import AgentRuntimeBindingV1, AgentRuntimeReleaseV1
from loom.execution_contract import (
    GUEST_LAUNCHER_STORAGE_MIB,
    effective_guest_capabilities,
    evaluate_execution_admission,
    nebius_cpu_execution_class,
    nebius_guest_execution_class,
    workload_requirements_from_task,
)
from loom.execution_image_admission import ExecutionImageAdmissionBundleV1
from loom.execution_requirements import (
    ALL_GUEST_EXECUTION_CAPABILITIES,
    GUEST_EXECUTION_CAPABILITIES,
    GuestExecutionCapability,
    execution_requirement_diagnostics,
)
from loom.execution_runtime_contract import (
    TASK_EGRESS_OUTPUT,
    ContainerResourcesV1,
    ExecutionResourceRequestsV1,
    ExecutionRuntimePlanV1,
    ProcessPhaseV1,
    RuntimeHandoffInputV1,
    RuntimeOutputDeclarationV1,
    RuntimeTaskInputV1,
    TaskExecutionResourceRequestsV1,
)
from loom.hosted_harness import (
    GUEST_SANDBOX_DRIVER_CAPABILITIES,
    NATIVE_EXECUTION_AGENT_NAMES,
    harnesses_supporting,
    hosted_harness,
    is_workspace_harness,
)
from loom.models.networking import (
    UnsupportedNetworkPolicyOverrideError,
    hosted_http_egress,
    resolve_effective_network_policy,
)
from loom.models.task import TaskConfig, normalize_steps
from loom.models.trial import TrialConfig
from loom.mutable_paths import validate_task_workdir
from loom.pipeline.keys import canonical_digest
from loom.sandbox_identity import resolve_sandbox_identity
from loom.task_image_materialization import TaskImageExecutionGrantV1, resolve_prepared_task
from loom.task_sandbox_planner import (
    TaskSandboxPlanRequest,
    compile_deferred_verifier_plan,
    compile_task_sandbox_plan,
    guest_capabilities,
    plan_admissions,
)
from loom.verifier_runtime import resolve_verifier_env_mode


def uses_runner_task_image(task: TaskConfig, trial: TrialConfig) -> bool:
    """Whether this direct-completion task runs in the platform runner image.

    The completion runner is Loom code shipped in the service image, so a
    response-only task that names no image and no Dockerfile needs nothing
    else from its environment. Leaving `docker_image` unset lets it survive
    service upgrades; the execution plan freezes the concrete runner image
    (`ExecutionRuntimePlanV1.task_image_ref`) per run. A task that pins an
    image or declares a Dockerfile keeps exact-image semantics (#2054).
    """
    env = task.environment
    return (
        not is_workspace_harness(trial.agent_name)
        and env.docker_image is None
        and env.dockerfile is None
    )


def resolve_runner_task_image(task: TaskConfig, task_image_ref: str) -> TaskConfig:
    """The task as it executes: a runner-image task takes the image frozen
    into its execution plan, so workload requirements and the plan agree."""
    env = task.environment
    if env.docker_image is not None or env.dockerfile is not None:
        return task
    return task.model_copy(
        update={"environment": env.model_copy(update={"docker_image": task_image_ref})},
    )


_DIGEST_REF = re.compile(r"^.+@sha256:[0-9a-f]{64}$")
_SHA256 = re.compile(r"^sha256:[0-9a-f]{64}$")
_GLOB_MAGIC = re.compile(r"[*?[]")
MAX_INPUT_MANIFEST_BYTES = 16 * 1024 * 1024
MAX_INPUT_FILES = 10_000
MAX_INPUT_BYTES = 10 * 1024**3


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ServiceExecutionInputFileV1(_Strict):
    relative_path: str = Field(min_length=1, max_length=4096)
    size_bytes: int = Field(ge=0)
    sha256: str = Field(pattern=_SHA256.pattern)
    mode: Literal["0644", "0755"]

    @field_validator("relative_path")
    @classmethod
    def safe_relative_path(cls, value: str) -> str:
        path = PurePosixPath(value)
        if path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
            raise ValueError("service execution input path must be safe and relative")
        return value


class ServiceExecutionInputManifestV1(_Strict):
    schema_version: Literal["loom.service-execution-input-manifest.v1"] = (
        "loom.service-execution-input-manifest.v1"
    )
    task_revision_sha256: str = Field(pattern=_SHA256.pattern)
    files: tuple[ServiceExecutionInputFileV1, ...] = Field(
        min_length=1,
        max_length=MAX_INPUT_FILES,
    )

    @model_validator(mode="after")
    def canonical_inventory(self) -> ServiceExecutionInputManifestV1:
        paths = [item.relative_path for item in self.files]
        if paths != sorted(paths, key=lambda item: item.encode("utf-8")):
            raise ValueError("service execution input files must be sorted")
        if len(paths) != len(set(paths)):
            raise ValueError("service execution input paths must be unique")
        return self

    def canonical_bytes(self) -> bytes:
        return json.dumps(
            self.model_dump(mode="json"),
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")


class ServiceExecutionInputBindingV1(_Strict):
    schema_version: Literal["loom.service-execution-input.v1"] = "loom.service-execution-input.v1"
    manifest_uri: str = Field(pattern=r"^s3://[^/]+/.+$", max_length=4096)
    manifest_sha256: str = Field(pattern=_SHA256.pattern)
    file_count: int = Field(gt=0, le=MAX_INPUT_FILES)
    total_bytes: int = Field(ge=0, le=MAX_INPUT_BYTES)


class ControllerComputeResourcesV1(_Strict):
    """Trusted harness compute, independent of the task sandbox allocation."""

    cpu_millis: int = Field(gt=0, le=128_000)
    memory_mib: int = Field(gt=0, le=1_048_576)


class ServiceExecutionRuntimeProfileV1(_Strict):
    """Deployment-owned immutable inputs for automatic plan compilation."""

    schema_version: Literal["loom.service-execution-runtime-profile.v1"] = (
        "loom.service-execution-runtime-profile.v1"
    )
    logical_pool_id: Literal["nebius-cpu"] = "nebius-cpu"
    candidate_sha: str = Field(pattern=r"^[0-9a-f]{40}$")
    execution_class_id: str = Field(pattern=r"^[a-z0-9][a-z0-9-]{0,79}$")
    task_image_ref: str
    agent_image_ref: str | None = None
    agent_runtime_bindings: tuple[AgentRuntimeBindingV1, ...] = ()
    supports_task_web_egress: bool = False
    supports_task_artifact_inputs: bool = Field(default=False, exclude_if=lambda value: not value)
    controller_resources: ControllerComputeResourcesV1 | None = None
    resource_allocation_policy: Literal["node-share-v1"] | None = None
    default_task_resource_requests: ExecutionResourceRequestsV1 | None = None
    task_resource_requests: dict[str, TaskExecutionResourceRequestsV1] = Field(default_factory=dict)
    supports_task_identity: bool = False
    guest_runtime: Literal["qemu-tcg-v1"] | None = Field(default=None, exclude_if=lambda value: value is None)
    supports_emulated_pkcs11: bool = Field(default=False, exclude_if=lambda value: not value)
    guest_runtime_volume_mib: int | None = Field(default=None, ge=1024, le=4096, exclude_if=lambda value: value is None)
    guest_max_artifact_bytes: int | None = Field(default=None, gt=0, le=10 * 1024**3, exclude_if=lambda value: value is None)
    runtime_image_ref: str
    runtime_binary_sha256: str = Field(pattern=_SHA256.pattern)
    image_admission: ExecutionImageAdmissionBundleV1
    run_as_user: int = Field(default=65532, gt=0)
    run_as_group: int = Field(default=65532, gt=0)
    fs_group: int = Field(default=65532, gt=0)
    runtime_volume_mib: int = Field(default=32, gt=0, le=4096)
    termination_grace_seconds: int = Field(default=30, ge=1, le=300)
    max_log_bytes_per_stream: int = Field(default=10 * 1024 * 1024, gt=0)
    max_artifact_bytes: int = Field(default=1024 * 1024 * 1024, gt=0)
    service_lifecycle_ready: bool = False

    @property
    def supported_guest_capabilities(self) -> frozenset[GuestExecutionCapability]:
        if self.guest_runtime is None:
            return frozenset()
        return ALL_GUEST_EXECUTION_CAPABILITIES if self.supports_emulated_pkcs11 else GUEST_EXECUTION_CAPABILITIES

    @model_serializer(mode="wrap")
    def _omit_empty_requests(self, handler: Any) -> dict[str, Any]:
        payload: dict[str, Any] = handler(self)
        if self.resource_allocation_policy is None:
            payload.pop("resource_allocation_policy", None)
        if not self.supports_task_web_egress:
            payload.pop("supports_task_web_egress", None)
        if not self.task_resource_requests:
            payload.pop("task_resource_requests", None)
        if self.default_task_resource_requests is None:
            payload.pop("default_task_resource_requests", None)
        if not self.service_lifecycle_ready:
            payload.pop("service_lifecycle_ready", None)
        if not self.supports_task_identity:
            payload.pop("supports_task_identity", None)
        return payload

    @model_validator(mode="after")
    def consistent_execution_class(self) -> ServiceExecutionRuntimeProfileV1:
        if self.guest_runtime is None and (
            self.guest_runtime_volume_mib is not None or self.guest_max_artifact_bytes is not None
            or self.supports_emulated_pkcs11
        ):
            raise ValueError("guest budgets require an explicit guest runtime")
        execution_class = nebius_cpu_execution_class(
            supports_task_web_egress=self.supports_task_web_egress,
        )
        if self.execution_class_id != execution_class.class_id:
            raise ValueError(
                "runtime profile execution class must match its task egress capability; "
                "new capabilities require distinct class and target identities"
            )
        return self

    @model_validator(mode="after")
    def unique_agent_versions(self) -> ServiceExecutionRuntimeProfileV1:
        keys = [(item.agent_name, item.agent_version) for item in self.agent_runtime_bindings]
        if len(set(keys)) != len(keys):
            raise ValueError("runtime profile has duplicate agent version bindings")
        return self

    @field_validator("task_image_ref", "runtime_image_ref", "agent_image_ref")
    @classmethod
    def immutable_images(cls, value: str | None) -> str | None:
        if value is None:
            return value
        if _DIGEST_REF.fullmatch(value) is None:
            raise ValueError("runtime profile images must be digest-pinned")
        return value


def build_nebius_runtime_profile(
    *,
    candidate_sha: str,
    task_image_ref: str,
    runtime_image_ref: str,
    runtime_binary_sha256: str,
    image_admission: ExecutionImageAdmissionBundleV1,
    agent_image_ref: str | None = None,
    controller_resources: ControllerComputeResourcesV1 | None = None,
    supports_task_web_egress: bool = False,
    supports_task_artifact_inputs: bool = False,
    service_lifecycle_ready: bool = False,
    supports_task_identity: bool = False,
    guest_runtime: Literal["qemu-tcg-v1"] | None = None,
    supports_emulated_pkcs11: bool = False,
    guest_runtime_volume_mib: int | None = None,
    guest_max_artifact_bytes: int | None = None,
    runtime_volume_mib: int = 32,
    resource_allocation_policy: Literal["node-share-v1"] | None = None,
) -> ServiceExecutionRuntimeProfileV1:
    """Construct publisher profiles with the class matching explicit capabilities.

    Existing profiles must be loaded and validated, never passed through this
    builder to silently replace a declared catalog identity.
    """
    return ServiceExecutionRuntimeProfileV1(
        resource_allocation_policy=resource_allocation_policy,
        candidate_sha=candidate_sha,
        execution_class_id=nebius_cpu_execution_class(
            supports_task_web_egress=supports_task_web_egress,
        ).class_id,
        task_image_ref=task_image_ref,
        runtime_image_ref=runtime_image_ref,
        runtime_binary_sha256=runtime_binary_sha256,
        image_admission=image_admission,
        agent_image_ref=agent_image_ref,
        controller_resources=controller_resources,
        supports_task_web_egress=supports_task_web_egress,
        service_lifecycle_ready=service_lifecycle_ready,
        supports_task_artifact_inputs=supports_task_artifact_inputs,
        supports_task_identity=supports_task_identity,
        guest_runtime=guest_runtime,
        supports_emulated_pkcs11=supports_emulated_pkcs11,
        guest_runtime_volume_mib=guest_runtime_volume_mib,
        guest_max_artifact_bytes=guest_max_artifact_bytes,
        runtime_volume_mib=runtime_volume_mib,
    )


def load_service_execution_runtime_profile(
    raw: str,
) -> ServiceExecutionRuntimeProfileV1 | None:
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError("service execution runtime profile is not valid JSON") from exc
    if value == {}:
        return None
    return ServiceExecutionRuntimeProfileV1.model_validate(value)


def build_service_execution_input_manifest(
    bundle_dir: Path,
    *,
    task_checksum: str,
) -> ServiceExecutionInputManifestV1:
    files: list[ServiceExecutionInputFileV1] = []
    # Canonical inventory order is UTF-8 of the relative POSIX path — not
    # pathlib component order (e.g. tests/test.sh sorts before tests/test/).
    file_paths = [path for path in bundle_dir.rglob("*") if not path.is_dir()]
    file_paths.sort(
        key=lambda path: path.relative_to(bundle_dir).as_posix().encode("utf-8"),
    )
    for path in file_paths:
        if path.is_symlink() or not path.is_file():
            raise ValueError("service execution input contains a non-regular file")
        body = path.read_bytes()
        mode = "0755" if stat.S_IMODE(path.stat().st_mode) & 0o111 else "0644"
        files.append(
            ServiceExecutionInputFileV1(
                relative_path=path.relative_to(bundle_dir).as_posix(),
                size_bytes=len(body),
                sha256="sha256:" + hashlib.sha256(body).hexdigest(),
                mode=mode,
            )
        )
    return ServiceExecutionInputManifestV1(
        task_revision_sha256="sha256:" + task_checksum.removeprefix("sha256:"),
        files=tuple(files),
    )


def prepare_service_execution_input_manifest(
    bundle_dir: Path,
    *,
    task_checksum: str,
    bucket: str,
    manifest_key: str,
) -> tuple[bytes, dict[str, Any]]:
    """Build canonical manifest bytes and the ``source_provenance`` binding.

    Shared by TaskSet materialization and benchmark ``publish-local`` so both
    catalog parents attach the same ``service_execution_input`` shape (#1978).
    Does not upload; callers put ``body`` at ``manifest_key``.
    """

    if not manifest_key or manifest_key.endswith("/"):
        raise ValueError("manifest_key must be a non-empty object key")
    manifest = build_service_execution_input_manifest(
        bundle_dir,
        task_checksum=task_checksum,
    )
    body = manifest.canonical_bytes()
    provenance = {
        "service_execution_input": {
            "schema_version": "loom.service-execution-input.v1",
            "manifest_uri": f"s3://{bucket}/{manifest_key}",
            "manifest_sha256": "sha256:" + hashlib.sha256(body).hexdigest(),
            "file_count": len(manifest.files),
            "total_bytes": sum(item.size_bytes for item in manifest.files),
        },
    }
    return body, provenance


def service_execution_input_binding(
    provenance: dict[str, Any],
) -> ServiceExecutionInputBindingV1 | None:
    raw = provenance.get("service_execution_input")
    if raw is None:
        return None
    return ServiceExecutionInputBindingV1.model_validate(raw)


def automatic_service_execution_rejections(
    task: TaskConfig,
    trial: TrialConfig,
    *,
    source_provenance: dict[str, Any],
    allow_task_image_preparation: bool = False,
    supported_capabilities: frozenset[GuestExecutionCapability] = frozenset(),
) -> tuple[str, ...]:
    """Return stable reasons why the v1 ordinary-TaskSet compiler cannot run a task."""

    from loom.task_runtime_compatibility import task_runtime_rejections

    task = normalize_steps(task)
    declared_rejections = task_runtime_rejections(task, agent_name=trial.agent_name)
    env = task.environment
    # The harness spec supplies only harness-owned facts; checks keyed on
    # `controller` belong to the common private-sandbox path (#2288).
    spec = hosted_harness(trial.agent_name)
    controller = spec is not None and spec.workspace
    reasons: list[str] = list(declared_rejections)
    try:
        effective_network_policy = resolve_effective_network_policy(
            baseline=env.baseline_network_policy,
            supported=env.network_policies_supported,
            override=trial.baseline_network_policy_override,
        )
        network_override_supported = True
    except UnsupportedNetworkPolicyOverrideError:
        reasons.append("network_policy_override_not_supported")
        effective_network_policy = env.baseline_network_policy
        network_override_supported = False
    if task.agent.continue_until_timeout and not (spec and spec.supports("agent_continuation")):
        reasons.append("agent_continuation_unsupported")
    reasons.extend(item.code for item in execution_requirement_diagnostics(
        env.execution_requirements, supported_capabilities=supported_capabilities,
    ))
    if trial.isolation == "container" and guest_capabilities(task):
        # A task that needs a guest kernel cannot run safely in a shared kernel.
        reasons.append("isolation_container_unsatisfiable")
    if effective_guest_capabilities(task, trial) is not None:
        if not controller:
            reasons.append("isolation_guest_response_only" if trial.isolation == "guest"
                           else "guest_private_sandboxes_required")
        elif spec is not None and not spec.required_driver_capabilities <= GUEST_SANDBOX_DRIVER_CAPABILITIES:
            reasons.append("guest_driver_capabilities_unsupported")
        # The guest path grades in a fresh verifier guest, not the agent's live
        # sandbox. Reject an explicit request for that until it is supported;
        # task-authored shared guest tasks keep their historical topology.
        if resolve_verifier_env_mode(task, trial) == "shared" and (
            trial.isolation == "guest" or trial.verifier_env_mode == "shared"
        ):
            reasons.append("isolation_guest_shared_unsupported")
        if env.sidecars:
            reasons.append("guest_sidecars_unsupported")
        for user in (env.user, task.verifier.user):
            try:
                identity = resolve_sandbox_identity(user, env.environment.get("HOME")) if user is not None else None
            except ValueError:
                identity = None
            if identity is None or identity.run_as_user != 0 or identity.run_as_group != 0:
                reasons.append("guest_root_identity_required")
        admission = evaluate_execution_admission(
            workload_requirements_from_task(task, trial if network_override_supported else trial.model_copy(
                update={"baseline_network_policy_override": None})),
            nebius_guest_execution_class(
                supports_emulated_pkcs11="emulated_pkcs11_authentication" in supported_capabilities,
            ),
        )
        reasons.extend(reason.code for reason in admission.reasons if reason.code.startswith("guest_"))
    if service_execution_input_binding(source_provenance) is None:
        reasons.append("immutable_task_input_unavailable")
    if env.os != "linux" or env.cpu_arch not in {"x86_64", "any"}:
        reasons.append("linux_x86_64_required")
    if env.gpu_vendor != "none" or env.gpus:
        reasons.append("gpu_unsupported")
    if env.mutable_paths and not controller:
        reasons.append("mutable_paths_require_terminus")
    if env.workspace_reference_files and not controller:
        reasons.append("workspace_references_require_terminus")
    if env.preserve_acls and not controller:
        reasons.append("acl_snapshots_require_terminus")
    if env.service_lifecycle is not None and not controller:
        reasons.append("service_lifecycle_requires_terminus")
    if not (allow_task_image_preparation and controller and env.dockerfile is not None) and not (
        uses_runner_task_image(task, trial)
    ) and (
        env.dockerfile is not None
        or env.docker_image is None
        or _DIGEST_REF.fullmatch(env.docker_image) is None
    ):
        reasons.append("immutable_task_image_required")
    if env.cpus is None or env.memory_mb is None or env.storage_mb is None:
        reasons.append("resource_limits_required")
    elif env.cpus > 128 or env.memory_mb > 1_048_576 or env.storage_mb > 1_048_576:
        reasons.append("resource_limits_out_of_range")
    elif spec is not None and spec.setup is not None and spec.setup.disk_mib:
        # The install lands in the task sandbox's own disk; a guest's disk is
        # its storage less the launcher's share (#2362).
        reserve = GUEST_LAUNCHER_STORAGE_MIB if effective_guest_capabilities(task, trial) is not None else 0
        if env.storage_mb - reserve < spec.setup.disk_mib:
            reasons.append("harness_setup_storage_insufficient")
    if not controller and (env.workdir != PurePosixPath("/workspace") or env.user != "agent"):
        reasons.append("standard_workspace_identity_required")
    if controller:
        try:
            validate_task_workdir(env.workdir)
        except ValueError:
            reasons.append("standard_workspace_identity_required")
        try:
            resolve_sandbox_identity(env.user, env.environment.get("HOME"))
            if task.verifier.user is not None:
                resolve_sandbox_identity(task.verifier.user, env.environment.get("HOME"))
        except ValueError:
            reasons.append("unsupported_task_identity")
    if effective_network_policy.kind not in {"gateway-only", "web-allowlist", "public-web"}:
        reasons.append("gateway_only_network_required")
    if (
        hosted_http_egress(effective_network_policy) is not None
        and not controller
    ):
        reasons.append("task_network_consumer_unavailable")
    if (
        (set(env.environment) - ({"HOME"} if controller else set()))
        or (env.sidecars and (not controller or not all(sidecar.fixture for sidecar in env.sidecars)))
        or env.extra_hosts
        or env.dns
        or env.tmpfs
        or env.healthcheck is not None
        or env.skills_dir is not None
        or env.mcp_servers
    ):
        reasons.append("extended_environment_unsupported")
    if any(sidecar.fixture for sidecar in env.sidecars):
        if allow_task_image_preparation:
            if env.dockerfile is None or any(sidecar.dockerfile is None for sidecar in env.sidecars):
                reasons.append("fixture_build_inputs_required")
        elif any(sidecar.docker_image is None or _DIGEST_REF.fullmatch(sidecar.docker_image) is None
                 or sidecar.dockerfile is not None for sidecar in env.sidecars):
            reasons.append("immutable_fixture_images_required")
    if task.required_agent_capabilities:
        reasons.append("agent_capabilities_unsupported")
    if task.agent.extra_mcp_servers or task.agent.skills or task.agent.user is not None:
        reasons.append("extended_agent_runtime_unsupported")
    if task.verifier.user is not None and not controller:
        reasons.append("custom_verifier_identity_unsupported")
    if len(task.steps) != 1 or task.multi_step is not None:
        reasons.append("single_step_required")
    if spec is None or not spec.natively_runnable:
        reasons.append("direct_completion_required")
    if spec is not None and spec.model == "forbidden":
        # e.g. Oracle's reference solver never calls a model (#2054).
        if trial.agent_model is not None or trial.request_params:
            reasons.append("harness_model_forbidden")
    elif trial.agent_model is None or trial.agent_model.source != "api":
        reasons.append("api_model_required")
    if trial.extra_mcp_servers or trial.extra_skills or trial.multi_model is not None:
        reasons.append("extended_agent_runtime_unsupported")
    if task.verifier.name != "script" or task.verifier.env_mode not in {"shared", "separate"}:
        reasons.append("script_verifier_required")
    verifier_path = task.verifier.args.get("script_path")
    if (
        not isinstance(verifier_path, str)
        or not verifier_path
        or PurePosixPath(verifier_path).is_absolute()
        or ".." in PurePosixPath(verifier_path).parts
        or _GLOB_MAGIC.search(verifier_path)
    ):
        reasons.append("exact_verifier_path_required")
    if trial.skip_verifier or trial.verifier_env_mode not in {None, "shared", "separate"}:
        reasons.append("verifier_mode_required")
    if controller:
        if not isinstance(verifier_path, str) or not verifier_path.startswith("verifier/"):
            reasons.append("private_verifier_directory_required")
        if trial.workspace_staging_policy_name == "none":
            reasons.append("private_workspace_isolation_required")
    if task.steps:
        step = task.steps[0]
        if (
            step.agent is not None
            or step.verifier is not None
            or step.network is not None
            or step.healthcheck is not None
        ):
            reasons.append("step_overrides_unsupported")
        instruction_path = PurePosixPath(step.instruction_file)
        if (
            instruction_path.is_absolute()
            or ".." in instruction_path.parts
            or _GLOB_MAGIC.search(str(instruction_path))
        ):
            reasons.append("exact_instruction_path_required")
        artifacts = [*step.artifacts, *step.required_artifacts]
        if any(
            PurePosixPath(item).is_absolute()
            or ".." in PurePosixPath(item).parts
            or _GLOB_MAGIC.search(item)
            or PurePosixPath(item).parts[:1] == (".loom",)
            for item in artifacts
        ):
            reasons.append("exact_artifact_paths_required")
    return tuple(dict.fromkeys(reasons))


def compile_service_execution_plan(
    *,
    task: TaskConfig,
    trial: TrialConfig,
    task_revision_sha256: str,
    source_provenance: dict[str, Any],
    profile: ServiceExecutionRuntimeProfileV1,
    task_image_grant: TaskImageExecutionGrantV1 | None = None,
    task_id: str | None = None,
) -> ExecutionRuntimePlanV1:
    if profile.task_resource_requests and task_id is None:
        raise ValueError("task resource requests require the selected task identity")
    override = profile.task_resource_requests.get(task_id) if task_id is not None else None
    resource_requests = (
        validate_task_resource_requests(
            task=task, trial=trial, profile=profile,
            task_revision_sha256=task_revision_sha256, override=override,
        ) if override is not None else None
    )
    if any(sidecar.fixture for sidecar in task.environment.sidecars) and (
        task_image_grant is None or any(sidecar.dockerfile is None for sidecar in task.environment.sidecars)
    ):
        raise ValueError("fixture execution requires the frozen task's prepared component grant")
    if task_image_grant is not None:
        if (not is_workspace_harness(trial.agent_name)
            or task.environment.dockerfile is None
            or task_revision_sha256 != "sha256:" + task_image_grant.task_checksum
            or task != TaskConfig.model_validate(task_image_grant.task_config)
            or source_provenance != task_image_grant.task_source_provenance):
            raise ValueError("prepared task image does not match the frozen task")
        task = resolve_prepared_task(task, task_image_grant)
    task = normalize_steps(task)
    if (task.environment.service_lifecycle is not None or task.environment.sidecars) and not profile.service_lifecycle_ready:
        raise ValueError("service_lifecycle runtime is not ready")
    reasons = automatic_service_execution_rejections(
        task,
        trial,
        source_provenance=source_provenance,
        supported_capabilities=profile.supported_guest_capabilities,
    )
    if reasons:
        raise ValueError("automatic service execution is incompatible: " + ",".join(reasons))
    spec = hosted_harness(trial.agent_name)
    assert spec is not None  # admission rejected unknown harnesses above
    effective_network_policy = resolve_effective_network_policy(
        baseline=task.environment.baseline_network_policy,
        supported=task.environment.network_policies_supported,
        override=trial.baseline_network_policy_override,
    )
    if hosted_http_egress(effective_network_policy) is not None and not profile.supports_task_web_egress:
        raise ValueError("task_egress_runtime_unavailable")
    profile_reasons = runtime_profile_rejections(task, trial, profile)
    selected_agent_image = controller_image_for_trial(profile, trial)
    if task_image_grant is not None and selected_agent_image is not None:
        admitted = {item.statement.image_ref for item in profile.image_admission.admissions}
        if selected_agent_image in admitted:
            profile_reasons = tuple(reason for reason in profile_reasons
                                    if reason != "task_image_not_in_runtime_profile")
    if "terminus_controller_unavailable" in profile_reasons:
        raise ValueError("active runtime profile has no Terminus controller image")
    if profile_reasons:
        raise ValueError("task image is not provided by the active runtime profile: " + ",".join(profile_reasons))
    binding = service_execution_input_binding(source_provenance)
    assert binding is not None
    assert task.environment.cpus is not None
    assert task.environment.memory_mb is not None
    assert task.environment.storage_mb is not None
    step = task.steps[0]
    output_paths = list(dict.fromkeys((*step.artifacts, *step.required_artifacts)))
    command_identity = canonical_digest(
        {
            "schema_version": "loom.automatic-service-execution-command.v1",
            "task_revision_sha256": task_revision_sha256,
            "agent": trial.agent_name,
            **({"agent_version": trial.agent_version, "agent_image_ref": selected_agent_image}
               if trial.agent_version is not None else {}),
            "model": trial.agent_model.model_dump(mode="json") if trial.agent_model else None,
            "request_params": trial.request_params,
            "effective_network_policy": effective_network_policy.model_dump(mode="json"),
            "instruction_file": str(step.instruction_file),
            "artifacts": step.artifacts,
            "required_artifacts": step.required_artifacts,
            "verifier": task.verifier.model_dump(mode="json"),
        }
    )
    if spec.workspace:
        assert selected_agent_image is not None  # runtime_profile_rejections checked it
        return compile_task_sandbox_plan(TaskSandboxPlanRequest(
            spec=spec, task=task, trial=trial, profile=profile, binding=binding,
            task_revision_sha256=task_revision_sha256, command_identity=command_identity,
            output_paths=tuple(output_paths), effective_network_policy=effective_network_policy,
            controller_image=selected_agent_image,
            task_image_materialization_id=(
                task_image_grant.materialization_id if task_image_grant else None
            ),
            resource_requests=resource_requests,
        ))
    assert trial.agent_model is not None
    # The in-Pod runner targets Loom's attributed chat route.  It revalidates
    # the service-execution lease and supports both a JWT-bound provider
    # connection and the platform route, whose model identity is provider/name.
    model = trial.agent_model.to_gateway_model_string()
    effective_network_policy_json = json.dumps(
        effective_network_policy.model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
    )
    main_environment = {
        "LOOM_TASK_MODEL": model,
        "LOOM_TASK_INSTRUCTION_FILE": str(step.instruction_file),
        "LOOM_TASK_ARTIFACTS_JSON": json.dumps(output_paths, separators=(",", ":")),
        "LOOM_TASK_REQUEST_PARAMS_JSON": json.dumps(
            trial.request_params, sort_keys=True, separators=(",", ":")
        ),
        "LOOM_EFFECTIVE_NETWORK_POLICY_JSON": effective_network_policy_json,
    }
    if (profile.supports_task_artifact_inputs
            and len(main_environment["LOOM_TASK_ARTIFACTS_JSON"].encode("utf-8")) > ProcessPhaseV1.MAX_ENV_VALUE_BYTES):
        del main_environment["LOOM_TASK_ARTIFACTS_JSON"]
        main_environment["LOOM_TASK_ARTIFACTS_FROM_INPUT"] = "1"
    verifier_path = str(task.verifier.args.get("script_path", ""))
    verifier = ProcessPhaseV1(
        role="verifier",
        argv=("/bin/sh", verifier_path),
        working_directory="/workspace",
        timeout_seconds=round(
            (trial.override_verifier_timeout_sec or task.verifier.timeout_sec)
            * trial.verifier_timeout_multiplier
        ),
        environment={
            "LOOM_TASK_DIR": "/workspace",
            "LOOM_VERIFIER_OUTPUT": "/workspace/.loom/verifier/output.json",
            "LOOM_EFFECTIVE_NETWORK_POLICY_JSON": effective_network_policy_json,
            **(
                {"LOOM_AGENT_OUTPUT": f"/workspace/{step.artifacts[0]}"}
                if len(step.artifacts) == 1
                else {}
            ),
        },
    )
    output_declarations = (
        *(
            RuntimeOutputDeclarationV1(
                source_path=path,
                relative_path=f"artifacts/{path}",
                kind="task_artifact",
                required=True,
            )
            for path in output_paths
        ),
        RuntimeOutputDeclarationV1(
            source_path=".loom/agent/trajectory.jsonl",
            relative_path="trajectory/events.jsonl",
            kind="trajectory",
            required=True,
        ),
        RuntimeOutputDeclarationV1(
            source_path=".loom/agent/usage.json",
            relative_path="accounting/usage.json",
            kind="usage",
            required=True,
        ),
        RuntimeOutputDeclarationV1(
            source_path=".loom/verifier/output.json",
            relative_path="verifier/output.json",
            kind="verifier",
            required=True,
        ),
    )
    if hosted_http_egress(effective_network_policy) is not None:
        output_declarations = (TASK_EGRESS_OUTPUT, *output_declarations)
    return ExecutionRuntimePlanV1(
        effective_network_policy=effective_network_policy,
        task_egress=hosted_http_egress(effective_network_policy),
        candidate_sha=profile.candidate_sha,
        task_revision_sha256=task_revision_sha256,
        command_identity_sha256=command_identity,
        execution_class_id=profile.execution_class_id,
        composition="init_payload",
        task_image_ref=profile.task_image_ref,
        runtime_image_ref=profile.runtime_image_ref,
        runtime_binary_sha256=profile.runtime_binary_sha256,
        image_admission=plan_admissions(profile, {profile.task_image_ref, profile.runtime_image_ref}),
        run_as_user=profile.run_as_user,
        run_as_group=profile.run_as_group,
        fs_group=profile.fs_group,
        task_resources=ContainerResourcesV1(
            cpu_millis=round(task.environment.cpus * 1000),
            memory_mib=task.environment.memory_mb,
            ephemeral_storage_mib=task.environment.storage_mb,
        ),
        workspace_mib=task.environment.storage_mb,
        runtime_volume_mib=profile.runtime_volume_mib,
        termination_grace_seconds=profile.termination_grace_seconds,
        task_input=RuntimeTaskInputV1(
            manifest_sha256=binding.manifest_sha256,
            file_count=binding.file_count,
            total_bytes=binding.total_bytes,
        ),
        output_declarations=output_declarations,
        main=ProcessPhaseV1(
            role="agent",
            argv=("python", "-m", spec.controller_module, spec.controller_phase),
            working_directory="/workspace",
            timeout_seconds=round(
                (trial.override_agent_timeout_sec or task.agent.timeout_sec)
                * trial.agent_timeout_multiplier
            ),
            environment=main_environment,
        ),
        verifier_execution="in_attempt",
        verifier=verifier,
        max_log_bytes_per_stream=profile.max_log_bytes_per_stream,
        max_artifact_bytes=profile.max_artifact_bytes,
    )


def controller_image_for_trial(
    profile: ServiceExecutionRuntimeProfileV1, trial: TrialConfig,
) -> str | None:
    """The trusted controller image the harness spec binds (#2295).

    None fails closed: an unknown harness, a harness that runs in the service
    runner image, a missing deployment controller or an unpinned version.
    """
    spec = hosted_harness(trial.agent_name)
    if spec is None or spec.controller_image != "harness-controller":
        return None
    if trial.agent_version is None:
        return profile.agent_image_ref
    for binding in profile.agent_runtime_bindings:
        if (binding.agent_name, binding.agent_version) == (trial.agent_name, trial.agent_version):
            return binding.agent_image_ref
    return None


def validate_task_resource_requests(
    task: TaskConfig, trial: TrialConfig, profile: ServiceExecutionRuntimeProfileV1,
    task_revision_sha256: str, override: TaskExecutionResourceRequestsV1,
) -> ExecutionResourceRequestsV1:
    """Validate a batch-scoped request against the unchanged source task limits."""
    if override.task_revision_sha256 != task_revision_sha256:
        raise ValueError("task resource requests source revision does not match")
    spec = hosted_harness(trial.agent_name)
    if (spec is None or not spec.supports("task_resource_requests")
            or task.service_execution is not None
            or controller_image_for_trial(profile, trial) is None):
        raise ValueError(
            "task resource requests require automatic native "
            + ", ".join(harnesses_supporting("task_resource_requests")) + " execution",
        )
    env = task.environment
    if env.cpus is None or env.memory_mb is None or env.storage_mb is None:
        raise ValueError("task resource requests require explicit task limits")
    task_limits = ContainerResourcesV1(
        cpu_millis=round(env.cpus * 1000), memory_mib=env.memory_mb,
        ephemeral_storage_mib=env.storage_mb,
    )
    controller_limits = (
        ContainerResourcesV1(
            cpu_millis=profile.controller_resources.cpu_millis,
            memory_mib=profile.controller_resources.memory_mib,
            ephemeral_storage_mib=env.storage_mb,
        ) if profile.controller_resources is not None else task_limits
    )
    override.requests.validate_limits(controller=controller_limits, task=task_limits)
    return override.requests


def freeze_agent_runtime_releases(
    profile: ServiceExecutionRuntimeProfileV1,
    releases: tuple[AgentRuntimeReleaseV1, ...],
) -> ServiceExecutionRuntimeProfileV1:
    admissions = {item.statement.image_ref: item for item in profile.image_admission.admissions}
    for release in releases:
        admissions[release.agent_image_ref] = release.image_admission
    return ServiceExecutionRuntimeProfileV1.model_validate({
        **profile.model_dump(mode="json"),
        "agent_runtime_bindings": [release.binding().model_dump(mode="json") for release in releases],
        "image_admission": {
            "schema_version": profile.image_admission.schema_version,
            "admissions": [item.model_dump(mode="json") for item in admissions.values()],
        },
    })


def _requires_task_identity(task: TaskConfig) -> bool:
    return (task.environment.user != "agent" or "HOME" in task.environment.environment
            or task.verifier.user is not None)


def runtime_profile_rejections(
    task: TaskConfig, trial: TrialConfig, profile: ServiceExecutionRuntimeProfileV1,
    *, allow_task_image_preparation: bool = False,
) -> tuple[str, ...]:
    """Submission and scheduling share the profile's image/agent compatibility.

    Every applicable reason is returned, in a fixed order whose first element
    is the most specific one; callers that surface a single error use it.
    """
    reasons: list[str] = []
    try:
        effective_network_policy = resolve_effective_network_policy(
            baseline=task.environment.baseline_network_policy,
            supported=task.environment.network_policies_supported,
            override=trial.baseline_network_policy_override,
        )
    except UnsupportedNetworkPolicyOverrideError:
        reasons.append("network_policy_override_not_supported")
        effective_network_policy = task.environment.baseline_network_policy
    guest = effective_guest_capabilities(task, trial) is not None
    if guest:
        if profile.guest_runtime is None:
            reasons.append("guest_runtime_unavailable")
        elif (profile.guest_runtime_volume_mib or profile.runtime_volume_mib) < 1024:
            reasons.append("guest_runtime_volume_too_small")
        if not profile.supports_task_identity:
            reasons.append("task_identity_runtime_unavailable")
    if hosted_http_egress(effective_network_policy) is not None and not profile.supports_task_web_egress:
        reasons.append("task_egress_runtime_unavailable")
    spec = hosted_harness(trial.agent_name)
    if trial.agent_version is not None and (
        spec is None or not spec.supports("pinned_versions")
        or controller_image_for_trial(profile, trial) is None
    ):
        reasons.append("agent_version_not_in_runtime_profile")
    if trial.agent_version is not None and any(
        binding.compatibility_error() is not None
        for binding in profile.agent_runtime_bindings
        if (binding.agent_name, binding.agent_version) == (trial.agent_name, trial.agent_version)
    ):
        reasons.append("agent_runtime_bridge_incompatible")
    if spec is None or spec.controller_image == "service-runner":
        # A pinned image must be the deployed runner image; a task that leaves
        # it unset runs in whichever runner image the plan freezes (#2054).
        if not (uses_runner_task_image(task, trial) or task.environment.docker_image == profile.task_image_ref):
            reasons.append("task_image_not_in_runtime_profile")
        return tuple(dict.fromkeys(reasons))
    if _requires_task_identity(task) and not profile.supports_task_identity:
        reasons.append("task_identity_runtime_unavailable")
    if (task.environment.service_lifecycle is not None or task.environment.sidecars) and not profile.service_lifecycle_ready:
        reasons.append("service_lifecycle_runtime_unavailable")
    agent_image = controller_image_for_trial(profile, trial)
    # An unpinnable version already explains the missing controller image.
    if agent_image is None and "agent_version_not_in_runtime_profile" not in reasons:
        reasons.append("terminus_controller_unavailable")
    admitted = {item.statement.image_ref for item in profile.image_admission.admissions}
    preparing = allow_task_image_preparation and task.environment.dockerfile is not None
    if (not preparing and task.environment.docker_image not in admitted) or (
        agent_image is not None and agent_image not in admitted
    ):
        reasons.append("task_image_not_in_runtime_profile")
    return tuple(dict.fromkeys(reasons))


def execution_selection_rejections(
    task: TaskConfig,
    trial: TrialConfig,
    profile: ServiceExecutionRuntimeProfileV1 | None,
    *,
    source_provenance: dict[str, Any],
    allow_task_image_preparation: bool = False,
) -> tuple[str, ...]:
    """Every reason the selected harness, network policy, verification and
    isolation cannot run together for this task on this deployment (#2314)."""

    reasons = list(automatic_service_execution_rejections(
        task, trial, source_provenance=source_provenance,
        allow_task_image_preparation=allow_task_image_preparation,
        supported_capabilities=profile.supported_guest_capabilities if profile is not None else frozenset(),
    ))
    if profile is None:
        reasons.append("runtime_profile_unavailable")
    else:
        reasons.extend(runtime_profile_rejections(
            task, trial, profile, allow_task_image_preparation=allow_task_image_preparation,
        ))
    return tuple(dict.fromkeys(reasons))


_HANDOFF_ARCHIVE = ".loom/workspace.tar"


def verifier_handoff_path(committed_path: str) -> str | None:
    """Where a committed agent output is staged for the deferred verifier.

    The verifier reads these beside its private controller state; `.loom/` is
    never copied into the graded sandbox.
    """
    if committed_path in {"artifacts/workspace.tar", "artifacts/workspace-references.json"}:
        return ".loom/" + committed_path.removeprefix("artifacts/")
    if committed_path.startswith("artifacts/mutable-paths/"):
        return ".loom/" + committed_path.removeprefix("artifacts/")
    return None


def build_verifier_handoff_manifest(
    *, task_revision_sha256: str, committed_files: Iterable[tuple[str, int, str]],
) -> ServiceExecutionInputManifestV1:
    """Bind the exact committed agent workspace that a deferred verifier grades."""
    files = []
    for relative_path, size_bytes, sha256 in committed_files:
        target = verifier_handoff_path(relative_path)
        if target is not None:
            files.append(ServiceExecutionInputFileV1(
                relative_path=target, size_bytes=size_bytes, sha256=sha256, mode="0644",
            ))
    files.sort(key=lambda item: item.relative_path.encode("utf-8"))
    if _HANDOFF_ARCHIVE not in {item.relative_path for item in files}:
        raise ValueError("verifier handoff archive was not committed")
    return ServiceExecutionInputManifestV1(
        task_revision_sha256=task_revision_sha256, files=tuple(files),
    )


def verifier_handoff_input(manifest: ServiceExecutionInputManifestV1) -> RuntimeHandoffInputV1:
    return RuntimeHandoffInputV1(
        manifest_sha256="sha256:" + hashlib.sha256(manifest.canonical_bytes()).hexdigest(),
        file_count=len(manifest.files),
        total_bytes=sum(item.size_bytes for item in manifest.files),
    )


__all__ = [
    "MAX_INPUT_BYTES",
    "MAX_INPUT_FILES",
    "MAX_INPUT_MANIFEST_BYTES",
    # Re-exported for existing importers; derived from the typed hosted harness
    # specs (#2288), so admission and catalog readiness cannot disagree.
    "NATIVE_EXECUTION_AGENT_NAMES",
    "ControllerComputeResourcesV1",
    "RuntimeTaskInputV1",
    "ServiceExecutionInputBindingV1",
    "ServiceExecutionInputFileV1",
    "ServiceExecutionInputManifestV1",
    "ServiceExecutionRuntimeProfileV1",
    "TaskExecutionResourceRequestsV1",
    "automatic_service_execution_rejections",
    "build_service_execution_input_manifest",
    "build_verifier_handoff_manifest",
    "compile_deferred_verifier_plan",
    "compile_service_execution_plan",
    "load_service_execution_runtime_profile",
    "prepare_service_execution_input_manifest",
    "service_execution_input_binding",
    "validate_task_resource_requests",
    "verifier_handoff_input",
    "verifier_handoff_path",
]
