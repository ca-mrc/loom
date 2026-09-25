"""Task schema — parsed `task.toml` (spec §4.1).

This module defines the on-disk task config that lives in a task directory:
metadata + environment + per-step config + multi-step aggregation strategy.
"""

from __future__ import annotations

from pathlib import PurePosixPath
from typing import TYPE_CHECKING, Annotated, Any, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_serializer,
    field_validator,
    model_serializer,
    model_validator,
)

from loom.execution_requirements import TaskExecutionRequirementsV1
from loom.models.healthcheck import HealthcheckSpec
from loom.models.mcp import MCPConnection
from loom.models.networking import NetworkPolicy, Public
from loom.models.skill import SkillRef
from loom.models.types import (
    OS,
    GPUVendor,
    ModelSpec,
    MultiStepRewardStrategy,
    NetworkPolicyKind,
    RequiredCPUArch,
    VerifierEnvMode,
)

if TYPE_CHECKING:
    from loom.execution_runtime_contract import ExecutionRuntimePlanV1

_SERVICE_EXECUTION_REVISION_SENTINEL = "sha256:" + "0" * 64


class TaskMetadata(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    id: str
    name: str
    description: str | None = None
    labels: list[str] = []


class TaskSidecarConfig(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    name: str
    docker_image: str | None = None
    dockerfile: PurePosixPath | None = None
    docker_build_context: PurePosixPath | None = None
    command: str | list[str] | None = None
    environment: dict[str, str] = {}
    hostname: str | None = None
    healthcheck: HealthcheckSpec | None = None
    depends_on: list[str] = []
    fixture: bool = Field(default=False, strict=True, exclude_if=lambda value: not value)
    ports: tuple[int, ...] = Field(default=(), exclude_if=lambda value: not value)
    cpus: float | None = Field(default=None, gt=0, le=4, allow_inf_nan=False,
                               exclude_if=lambda value: value is None)
    memory_mb: int | None = Field(default=None, gt=0, le=4096, exclude_if=lambda value: value is None)
    storage_mb: int | None = Field(default=None, gt=0, le=4096, exclude_if=lambda value: value is None)

    @model_validator(mode="after")
    def _fixture_contract(self) -> TaskSidecarConfig:
        from loom.task_fixtures import validate_fixture_config

        validate_fixture_config(self)
        return self


class ServiceLifecycleConfig(BaseModel):
    """Keep task-owned services alive until the independent verifier finishes.

    Startup is an optional, returning environment initializer, not an agent
    solution. Agent-owned services omit it and supply a readiness check only.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")
    startup_command: tuple[str, ...] = Field(default=(), max_length=64)
    startup_timeout_sec: float = Field(default=60, gt=0, le=300, allow_inf_nan=False)
    readiness: HealthcheckSpec
    readiness_timeout_sec: float = Field(default=30, gt=0, le=300, allow_inf_nan=False)
    readiness_scope: Literal["startup_and_handoff", "startup_only"] = Field(
        default="startup_and_handoff", exclude_if=lambda value: value == "startup_and_handoff",
    )

    @field_validator("startup_command")
    @classmethod
    def _startup_argv(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if any("\x00" in item or len(item) > 4096 for item in value) or (value and not value[0]):
            raise ValueError("service startup requires valid argv")
        return value

    @model_validator(mode="after")
    def _startup_only_requires_initializer(self) -> ServiceLifecycleConfig:
        if self.readiness_scope == "startup_only" and not self.startup_command:
            raise ValueError("startup_only readiness requires startup_command")
        return self


class EnvironmentConfig(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    os: OS
    cpu_arch: RequiredCPUArch = "x86_64"
    gpu_vendor: GPUVendor = "none"
    docker_image: str | None = None
    dockerfile: PurePosixPath | None = None
    docker_build_context: PurePosixPath | None = None
    # Public build inputs, not a mechanism for passing secrets to Docker.
    docker_build_args: dict[
        Annotated[str, Field(pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")], str
    ] = Field(default_factory=dict)
    docker_build_target: Annotated[
        str, Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")
    ] | None = None
    environment: dict[str, str] = {}
    extra_hosts: dict[str, str] = {}
    dns: list[str] = []
    tmpfs: list[str] = []
    healthcheck: HealthcheckSpec | None = None
    workdir: PurePosixPath = PurePosixPath("/workspace")
    mutable_paths: tuple[PurePosixPath, ...] = Field(default=(), max_length=16)
    mutable_path_reference_files: tuple[PurePosixPath, ...] = Field(default=(), max_length=16)
    workspace_reference_files: tuple[PurePosixPath, ...] = Field(default=(), max_length=16)
    reference_file_symlinks: dict[str, str] = Field(default_factory=dict, max_length=16)
    preserve_acls: bool = Field(default=False, strict=True, exclude_if=lambda value: not value)
    service_lifecycle: ServiceLifecycleConfig | None = None
    execution_requirements: TaskExecutionRequirementsV1 | None = Field(
        default=None, exclude_if=lambda value: value is None,
    )
    user: str | int = "agent"
    network_policies_supported: frozenset[NetworkPolicyKind] = frozenset({"public"})
    baseline_network_policy: NetworkPolicy = Public()
    skills_dir: PurePosixPath | None = None
    mcp_servers: list[MCPConnection] = []
    build_timeout_sec: float = Field(default=1200, gt=0)
    # Runtime resource contract. These values are optional for ordinary Loom
    # tasks, but audited benchmark profiles preserve their native values here.
    # CPU/memory/GPU values are executable driver policy. ``storage_mb`` is
    # retained for provenance, capacity planning, and service-execution
    # backends; the Docker backend intentionally does not request a
    # storage-driver-specific writable-layer quota.
    cpus: float | None = Field(default=None, gt=0)
    memory_mb: int | None = Field(default=None, gt=0)
    storage_mb: int | None = Field(default=None, gt=0)
    gpus: int = Field(default=0, ge=0)
    sidecars: list[TaskSidecarConfig] = []

    @model_serializer(mode="wrap")
    def _omit_unused_handoff_declarations(self, handler: Any) -> dict[str, Any]:
        payload: dict[str, Any] = handler(self)
        if not self.mutable_paths:
            payload.pop("mutable_paths", None)
        if not self.mutable_path_reference_files:
            payload.pop("mutable_path_reference_files", None)
        if not self.workspace_reference_files:
            payload.pop("workspace_reference_files", None)
        if not self.reference_file_symlinks:
            payload.pop("reference_file_symlinks", None)
        if self.service_lifecycle is None:
            payload.pop("service_lifecycle", None)
        return payload

    @model_validator(mode="after")
    def _validate_mutable_paths(self) -> EnvironmentConfig:
        from loom.mutable_paths import (
            validate_mutable_paths,
            validate_mutable_reference_files,
            validate_reference_file_symlinks,
            validate_workspace_reference_files,
        )

        validate_mutable_paths(self.mutable_paths, workdir=self.workdir)
        validate_mutable_reference_files(self.mutable_path_reference_files,
                                         paths=self.mutable_paths, workdir=self.workdir)
        validate_workspace_reference_files(self.workspace_reference_files,
                                           paths=self.mutable_paths, workdir=self.workdir)
        validate_reference_file_symlinks(
            self.reference_file_symlinks,
            groups=(self.mutable_path_reference_files, self.workspace_reference_files),
        )
        return self

    @model_validator(mode="after")
    def _docker_build_options_require_dockerfile(self) -> EnvironmentConfig:
        if (self.docker_build_args or self.docker_build_target) and self.dockerfile is None:
            raise ValueError("docker_build_args and docker_build_target require dockerfile")
        if any("\x00" in value for value in self.docker_build_args.values()):
            raise ValueError("docker_build_args values cannot contain NUL")
        from loom.task_fixtures import validate_fixture_contexts

        validate_fixture_contexts(self)
        return self


class AgentDefaults(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    name: str
    version: str | None = None
    model: ModelSpec | None = None
    timeout_sec: float = Field(default=1800, gt=0)
    setup_timeout_sec: float = Field(default=360, gt=0)
    continue_until_timeout: bool = Field(
        default=False, strict=True, exclude_if=lambda value: value is False,
    )
    user: str | int | None = None
    extra_mcp_servers: list[MCPConnection] = []
    skills: list[SkillRef] = []


class VerifierDefaults(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    name: str
    args: dict[str, Any] = {}
    timeout_sec: float = Field(default=300, gt=0)
    env_mode: VerifierEnvMode = "separate"
    user: str | int | None = None


class AgentOverrides(BaseModel):
    """Per-step partial override of AgentDefaults. None fields inherit task default."""

    model_config = ConfigDict(frozen=True, extra="forbid")
    model: ModelSpec | None = None
    timeout_sec: float | None = None
    user: str | int | None = None
    extra_mcp_servers: list[MCPConnection] | None = None


class VerifierOverrides(BaseModel):
    """Per-step partial override of VerifierDefaults."""

    model_config = ConfigDict(frozen=True, extra="forbid")
    name: str | None = None
    args: dict[str, Any] | None = None
    timeout_sec: float | None = None
    env_mode: VerifierEnvMode | None = None
    user: str | int | None = None


class StepNetworkPlan(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    agent_phase: NetworkPolicy | None = None
    verifier_phase: NetworkPolicy | None = None


class StepConfig(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    name: str
    instruction_file: PurePosixPath = PurePosixPath("instruction.md")
    agent: AgentOverrides | None = None
    verifier: VerifierOverrides | None = None
    artifacts: list[str] = []  # POSIX globs
    required_artifacts: list[str] = []  # verifier-required POSIX globs
    min_reward: dict[str, float] | float | None = None
    network: StepNetworkPlan | None = None
    healthcheck: HealthcheckSpec | None = None


class MultiStepConfig(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    reward_strategy: MultiStepRewardStrategy = "mean"
    weights: dict[str, float] | None = None

    @model_validator(mode="after")
    def _weights_required_for_weighted(self) -> MultiStepConfig:
        if self.reward_strategy == "weighted" and not self.weights:
            raise ValueError(
                "reward_strategy='weighted' requires non-empty `weights`",
            )
        return self


class TaskServiceExecutionV1(BaseModel):
    """Opt-in, immutable service-execution binding carried by a task revision.

    The runtime template deliberately omits ``task_revision_sha256`` because
    that digest covers the complete task directory, including task.toml. The
    scheduler binds the published Task checksum after materialization, avoiding
    an impossible self-referential digest while retaining strict validation.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    schema_version: Literal["loom.task-service-execution.v1"] = "loom.task-service-execution.v1"
    logical_pool_id: str = Field(pattern=r"^[a-z0-9][a-z0-9-]{0,79}$")
    runtime_template: dict[str, Any]

    @field_validator("runtime_template")
    @classmethod
    def _runtime_template_is_canonical(cls, value: dict[str, Any]) -> dict[str, Any]:
        if "task_revision_sha256" in value:
            raise ValueError("runtime template cannot contain its enclosing task revision")
        plan = bind_service_execution_runtime_plan(
            value,
            task_revision_sha256=_SERVICE_EXECUTION_REVISION_SENTINEL,
        )
        if plan.execution_role != "attempt":
            raise ValueError("task service execution requires an attempt runtime plan")
        if plan.task_egress is not None or any(
            sidecar.identity is not None or sidecar.task_fixture for sidecar in plan.sidecars
        ):
            raise ValueError("task identity, fixtures and web egress require automatic native execution")
        payload = plan.canonical_payload()
        del payload["task_revision_sha256"]
        return payload


def bind_service_execution_runtime_plan(
    runtime_template: dict[str, Any],
    *,
    task_revision_sha256: str,
) -> ExecutionRuntimePlanV1:
    """Bind a published task revision to its immutable runtime template."""

    from loom.execution_runtime_contract import ExecutionRuntimePlanV1

    return ExecutionRuntimePlanV1.model_validate(
        {**runtime_template, "task_revision_sha256": task_revision_sha256}
    )


class TaskConfig(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    schema_version: Literal["1"] = "1"
    required_agent_capabilities: frozenset[str] = frozenset()
    task: TaskMetadata
    environment: EnvironmentConfig
    agent: AgentDefaults
    verifier: VerifierDefaults
    service_execution: TaskServiceExecutionV1 | None = None
    steps: list[StepConfig] = []
    multi_step: MultiStepConfig | None = None

    @field_serializer("required_agent_capabilities", when_used="json")
    def _serialize_required_agent_capabilities(
        self,
        value: frozenset[str],
    ) -> list[str]:
        return sorted(value)

    @model_validator(mode="after")
    def _service_execution_matches_task(self) -> TaskConfig:
        if self.service_execution is None:
            return self
        if (self.environment.user != "agent" or self.verifier.user is not None
                or "HOME" in self.environment.environment or self.environment.mutable_paths
                or self.environment.service_lifecycle is not None
                or any(sidecar.fixture for sidecar in self.environment.sidecars)
                or self.environment.preserve_acls
                or self.environment.baseline_network_policy.kind in {"web-allowlist", "public-web"}):
            raise ValueError("declared sandbox capabilities require automatic native execution")

        from loom.execution_contract import workload_requirements_from_task
        from loom.execution_runtime_contract import validate_runtime_plan_requirements

        plan = bind_service_execution_runtime_plan(
            self.service_execution.runtime_template,
            task_revision_sha256=_SERVICE_EXECUTION_REVISION_SENTINEL,
        )
        requirements = workload_requirements_from_task(self)
        validate_runtime_plan_requirements(plan, requirements)
        return self


def normalize_steps(cfg: TaskConfig) -> TaskConfig:
    """Implicit single-step synthesis (spec §4.1).

    Tasks without explicit `steps` get a single synthesized step named "main"
    so the trial loop has exactly one code path to follow.
    """
    if cfg.steps:
        return cfg
    return cfg.model_copy(update={"steps": [StepConfig(name="main")]})
