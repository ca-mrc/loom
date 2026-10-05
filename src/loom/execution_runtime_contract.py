"""Immutable Pod-native execution plan shared by admission, actuator, and runtime."""

from __future__ import annotations

import math
import re
from datetime import datetime
from enum import StrEnum
from pathlib import PurePosixPath
from typing import Any, ClassVar, Literal
from uuid import UUID

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    TypeAdapter,
    field_validator,
    model_serializer,
    model_validator,
)

from loom.execution_contract import (
    IsolationLevel,
    VerifierTopology,
    WorkloadRequirementsV1,
    evaluate_execution_admission,
    nebius_guest_class_by_id,
)
from loom.execution_image_admission import ExecutionImageAdmissionBundleV1
from loom.execution_requirements import ALL_GUEST_EXECUTION_CAPABILITIES, GuestExecutionCapability
from loom.models.networking import NetworkPolicy, TaskHttpEgress, hosted_http_egress
from loom.sandbox_identity import SandboxIdentityV1

_DIGEST_REF = re.compile(r"^.+@sha256:[0-9a-f]{64}$")
_SHA256 = re.compile(r"^sha256:[0-9a-f]{64}$")
_CANDIDATE = re.compile(r"^[0-9a-f]{40}$")
_ENV_NAME = re.compile(r"^[A-Z_][A-Z0-9_]{0,127}$")
_ROLE_NAME = re.compile(r"^[a-z][a-z0-9-]{0,62}$")
_SECRET_ENV = re.compile(r"(?:KEY|TOKEN|SECRET|PASSWORD|CREDENTIAL|KUBECONFIG)")
_NETWORK_POLICY_ADAPTER: TypeAdapter[NetworkPolicy] = TypeAdapter(NetworkPolicy)


def _confined_relative_path(value: str) -> str:
    path = PurePosixPath(value)
    if (
        path.is_absolute()
        or path.as_posix() != value
        or any(part in {"", ".", ".."} for part in path.parts)
    ):
        raise ValueError("runtime output path must be a confined relative POSIX path")
    return value


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class RuntimeComposition(StrEnum):
    PRECOMPOSED = "precomposed"
    INIT_PAYLOAD = "init_payload"


class VerifierExecution(StrEnum):
    IN_ATTEMPT = "in_attempt"
    SEPARATE_EXECUTION = "separate_execution"
    SKIPPED = "skipped"


class ContainerResourcesV1(_Strict):
    cpu_millis: int = Field(gt=0, le=128_000)
    memory_mib: int = Field(gt=0, le=1_048_576)
    ephemeral_storage_mib: int = Field(gt=0, le=1_048_576)


class _ContainerResourceRequestsV1(ContainerResourcesV1):
    model_config = ConfigDict(strict=True)


class ExecutionResourceRequestsV1(_Strict):
    """Optional scheduling requests; the task's executable limits stay intact."""

    controller: _ContainerResourceRequestsV1 | None = None
    task_sandbox: _ContainerResourceRequestsV1 | None = None
    verifier_sandbox: _ContainerResourceRequestsV1 | None = None

    @model_serializer(mode="wrap")
    def _omit_unconfigured_roles(self, handler: Any) -> dict[str, Any]:
        payload: dict[str, Any] = handler(self)
        return {role: value for role, value in payload.items() if value is not None}

    @model_validator(mode="after")
    def _nonempty(self) -> ExecutionResourceRequestsV1:
        if all(value is None for value in (
            self.controller, self.task_sandbox, self.verifier_sandbox,
        )):
            raise ValueError("resource requests must configure at least one container")
        return self

    def validate_limits(
        self, *, controller: ContainerResourcesV1, task: ContainerResourcesV1,
    ) -> None:
        for role, requested, limit in (
            ("controller", self.controller, controller),
            ("task_sandbox", self.task_sandbox, task),
            ("verifier_sandbox", self.verifier_sandbox, task),
        ):
            if requested is not None and any(
                getattr(requested, field) > getattr(limit, field)
                for field in ContainerResourcesV1.model_fields
            ):
                raise ValueError(f"{role} resource requests exceed hard limits")


class TaskExecutionResourceRequestsV1(_Strict):
    """Measured requests bound to a task revision; usable by offline renderers."""

    task_revision_sha256: str = Field(pattern=_SHA256.pattern)
    requests: ExecutionResourceRequestsV1


class NodeResourceAllocationV1(_Strict):
    """Frozen task minima and usable target-node budget, after resident overhead."""

    policy: Literal["node-share-v1"] = "node-share-v1"
    target_id: str = Field(min_length=1, max_length=80)
    usable_node: ContainerResourcesV1
    baseline_slots: int = Field(gt=0)
    declared_task: ContainerResourcesV1


class ProcessPhaseV1(_Strict):
    # Shared by plan validation and compiler transport selection.
    MAX_ENV_VALUE_BYTES: ClassVar[int] = 4096
    role: Literal["setup", "agent", "verifier"]
    argv: tuple[str, ...] = Field(min_length=1, max_length=128)
    working_directory: str = Field(pattern=r"^(?:/app|/workspace(?:/[-A-Za-z0-9._]+)*)$")
    timeout_seconds: int = Field(gt=0, le=86_400)
    environment: dict[str, str] = Field(default_factory=dict)

    @field_validator("working_directory")
    @classmethod
    def _working_directory_is_canonical(cls, value: str) -> str:
        if any(part in {".", ".."} for part in value.split("/")):
            raise ValueError("process working directory must be canonical")
        return value

    @field_validator("argv")
    @classmethod
    def _argv_is_bounded(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if any(not item or "\x00" in item or len(item.encode("utf-8")) > 4096 for item in value):
            raise ValueError("process argv contains an invalid item")
        if sum(len(item.encode("utf-8")) for item in value) > 32_768:
            raise ValueError("process argv exceeds 32 KiB")
        return value

    @field_validator("environment")
    @classmethod
    def _environment_is_nonsecret(cls, value: dict[str, str]) -> dict[str, str]:
        if len(value) > 64:
            raise ValueError("process environment exceeds 64 entries")
        for name, item in value.items():
            if _ENV_NAME.fullmatch(name) is None or _SECRET_ENV.search(name):
                raise ValueError("process environment contains a forbidden name")
            if "\x00" in item or len(item.encode("utf-8")) > cls.MAX_ENV_VALUE_BYTES:
                raise ValueError("process environment value is invalid")
        return value


class ProbeV1(_Strict):
    kind: Literal["http", "tcp", "exec"]
    initial_delay_seconds: int = Field(default=0, ge=0, le=300, exclude_if=lambda value: value == 0)
    timeout_seconds: int = Field(default=2, gt=0, le=30)
    period_seconds: int = Field(default=2, gt=0, le=60)
    failure_threshold: int = Field(default=30, gt=0, le=300)
    port: int | None = Field(default=None, gt=0, le=65535)
    path: str | None = Field(default=None, pattern=r"^/[ -~]{0,1023}$")
    argv: tuple[str, ...] = Field(default=(), max_length=32)

    @field_validator("argv")
    @classmethod
    def _argv_is_bounded(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return ProcessPhaseV1._argv_is_bounded(value)

    @model_validator(mode="after")
    def _shape_matches_kind(self) -> ProbeV1:
        if self.kind == "http" and (self.port is None or self.path is None or self.argv):
            raise ValueError("HTTP probe requires port/path only")
        if self.kind == "tcp" and (self.port is None or self.path is not None or self.argv):
            raise ValueError("TCP probe requires port only")
        if self.kind == "exec" and (
            not self.argv or self.port is not None or self.path is not None
        ):
            raise ValueError("exec probe requires argv only")
        return self


class GuestExecutionV1(_Strict):
    """Exact task-declared capabilities implemented inside one private guest.

    Empty capabilities describe a plain guest selected by ``isolation=guest``.
    """

    schema_version: Literal["loom.guest-execution.v1"] = "loom.guest-execution.v1"
    runtime: Literal["qemu-tcg-v1"] = "qemu-tcg-v1"
    capabilities: tuple[GuestExecutionCapability, ...] = Field(max_length=4)

    @field_validator("capabilities")
    @classmethod
    def _canonical_capabilities(cls, value: tuple[GuestExecutionCapability, ...]) -> tuple[GuestExecutionCapability, ...]:
        if tuple(sorted(set(value))) != value:
            raise ValueError("guest capabilities must be sorted and unique")
        return value


class SidecarContainerV1(_Strict):
    role_name: str = Field(pattern=_ROLE_NAME.pattern)
    image_ref: str
    argv: tuple[str, ...] = Field(min_length=1, max_length=128)
    environment: dict[str, str] = Field(default_factory=dict)
    resources: ContainerResourcesV1
    startup_probe: ProbeV1
    readiness_probe: ProbeV1
    depends_on: tuple[str, ...] = Field(default=(), max_length=32)
    private_sandbox: bool = False
    identity: SandboxIdentityV1 | None = None
    guest_execution: GuestExecutionV1 | None = Field(default=None, exclude_if=lambda value: value is None)
    task_fixture: bool = Field(default=False, strict=True, exclude_if=lambda value: not value)
    task_image_component: str | None = Field(default=None, exclude_if=lambda value: value is None)
    hostname: str | None = Field(default=None, exclude_if=lambda value: value is None)

    @model_validator(mode="after")
    def _identity_is_private(self) -> SidecarContainerV1:
        if self.task_fixture:
            from loom.task_fixtures import validate_fixture_component, validate_fixture_hostname

            validate_fixture_component(self.role_name, self.task_image_component)
            validate_fixture_hostname(self.hostname)
            if self.private_sandbox or self.identity is not None or self.depends_on or self.environment:
                raise ValueError("fixture cannot share a sandbox identity or trusted sidecar contract")
        elif self.task_image_component is not None or self.hostname is not None or self.role_name.startswith("fixture-"):
            raise ValueError("fixture metadata and roles require explicit fixture isolation")
        if self.identity is not None and not self.private_sandbox:
            raise ValueError("task identity requires a private sandbox")
        if self.identity is not None and "HOME" in self.environment:
            raise ValueError("sandbox HOME must be declared by its identity")
        if self.guest_execution is not None:
            if (not self.private_sandbox or self.identity is None
                    or self.identity.run_as_user != 0 or self.identity.run_as_group != 0):
                raise ValueError("guest execution requires an explicit root private sandbox")
            binary = "/loom/bin/loom-sandbox-runtime"
            socket = f"/loom/sandboxes/{self.role_name}/sandbox.sock"
            if (len(self.argv) != 5 or self.argv[:4] != (
                    binary, "--socket", socket, "--exec-timeout-seconds")
                    or not self.argv[4].isascii() or not self.argv[4].isdigit()
                    or not 1 <= int(self.argv[4]) <= 86400 or str(int(self.argv[4])) != self.argv[4]):
                raise ValueError("guest execution requires the canonical bounded sandbox command")
            if any(probe.kind != "exec" or probe.argv != (binary, "--check-socket", socket)
                   for probe in (self.startup_probe, self.readiness_probe)):
                raise ValueError("guest execution requires its private socket probes")
            if (self.resources.cpu_millis < 1000 or self.resources.memory_mib < 512
                    or self.resources.ephemeral_storage_mib < 160):
                raise ValueError("guest execution resources are below the admitted minimum")
        return self

    @field_validator("image_ref")
    @classmethod
    def _immutable_image(cls, value: str) -> str:
        if _DIGEST_REF.fullmatch(value) is None:
            raise ValueError("sidecar image must be digest-pinned")
        return value

    @field_validator("argv")
    @classmethod
    def _argv_is_bounded(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return ProcessPhaseV1._argv_is_bounded(value)

    @field_validator("environment")
    @classmethod
    def _nonsecret_environment(cls, value: dict[str, str]) -> dict[str, str]:
        ProcessPhaseV1._environment_is_nonsecret(value)
        return value


class RuntimeTaskInputV1(_Strict):
    schema_version: Literal["loom.runtime-task-input.v1"] = "loom.runtime-task-input.v1"
    manifest_sha256: str = Field(pattern=_SHA256.pattern)
    file_count: int = Field(gt=0, le=10_000)
    total_bytes: int = Field(ge=0, le=10 * 1024**3)


class RuntimeHandoffInputV1(_Strict):
    """The parent attempt's committed workspace, staged under .loom/ for grading."""

    schema_version: Literal["loom.runtime-handoff-input.v1"] = "loom.runtime-handoff-input.v1"
    manifest_sha256: str = Field(pattern=_SHA256.pattern)
    file_count: int = Field(gt=0, le=10_000)
    total_bytes: int = Field(ge=0, le=10 * 1024**3)


class RuntimeOutputDeclarationV1(_Strict):
    """One immutable workspace file expected in the complete Trial bundle."""

    source_path: str = Field(min_length=1, max_length=4096)
    relative_path: str = Field(min_length=1, max_length=4096)
    kind: Literal[
        "task_artifact",
        "trajectory",
        "agent_native",
        "verifier",
        "usage",
        "diagnostic",
        "checkpoint",
    ]
    required: bool

    @field_validator("source_path", "relative_path")
    @classmethod
    def _safe_path(cls, value: str) -> str:
        return _confined_relative_path(value)

    @model_validator(mode="after")
    def _bundle_namespace(self) -> RuntimeOutputDeclarationV1:
        namespace = self.relative_path.split("/", 1)[0]
        if namespace not in {
            "artifacts",
            "trajectory",
            "agent",
            "verifier",
            "accounting",
            "diagnostics",
            "checkpoints",
        }:
            raise ValueError("runtime output has an unknown bundle namespace")
        return self


TASK_EGRESS_OUTPUT = RuntimeOutputDeclarationV1(
    source_path=".loom/task-egress.jsonl", relative_path="diagnostics/task-egress.jsonl",
    kind="diagnostic", required=True,
)


class RuntimeOutputEvidenceV1(RuntimeOutputDeclarationV1):
    state: Literal["captured", "missing"]
    size_bytes: int | None = Field(default=None, ge=0)
    sha256: str | None = Field(default=None, pattern=_SHA256.pattern)

    @model_validator(mode="after")
    def _state_matches_evidence(self) -> RuntimeOutputEvidenceV1:
        populated = self.size_bytes is not None and self.sha256 is not None
        if populated != (self.state == "captured"):
            raise ValueError("runtime output state does not match its evidence")
        return self


class ExecutionRuntimePlanV1(_Strict):
    schema_version: Literal["loom.execution-runtime-plan.v1"] = "loom.execution-runtime-plan.v1"
    candidate_sha: str = Field(pattern=_CANDIDATE.pattern)
    task_revision_sha256: str = Field(pattern=_SHA256.pattern)
    command_identity_sha256: str = Field(pattern=_SHA256.pattern)
    execution_role: Literal["attempt", "verifier"] = "attempt"
    execution_class_id: str = Field(pattern=r"^[a-z0-9][a-z0-9-]{0,79}$")
    composition: RuntimeComposition
    task_image_ref: str
    agent_image_ref: str | None = None
    task_image_materialization_id: UUID | None = None
    runtime_image_ref: str
    runtime_binary_sha256: str = Field(pattern=_SHA256.pattern)
    image_admission: ExecutionImageAdmissionBundleV1
    run_as_user: int = Field(default=65532, gt=0, le=2_147_483_647)
    run_as_group: int = Field(default=65532, gt=0, le=2_147_483_647)
    fs_group: int = Field(default=65532, gt=0, le=2_147_483_647)
    task_resources: ContainerResourcesV1
    effective_network_policy: NetworkPolicy | None = None
    task_egress: TaskHttpEgress | None = None
    controller_resources: ContainerResourcesV1 | None = None
    resource_requests: ExecutionResourceRequestsV1 | None = None
    node_resource_allocation: NodeResourceAllocationV1 | None = None
    workspace_mib: int = Field(gt=0, le=1_048_576)
    runtime_volume_mib: int = Field(gt=0, le=4096)
    termination_grace_seconds: int = Field(default=30, ge=1, le=300)
    setup: tuple[ProcessPhaseV1, ...] = Field(default=(), max_length=32)
    main: ProcessPhaseV1
    verifier_execution: VerifierExecution
    verifier: ProcessPhaseV1 | None = None
    verifier_after_agent_timeout: bool = False
    in_place_verifier: bool = False
    sidecars: tuple[SidecarContainerV1, ...] = Field(default=(), max_length=32)
    max_log_bytes_per_stream: int = Field(default=10 * 1024 * 1024, gt=0, le=100 * 1024 * 1024)
    max_artifact_bytes: int = Field(default=1024 * 1024 * 1024, gt=0, le=10 * 1024**3)
    task_input: RuntimeTaskInputV1 | None = None
    handoff_input: RuntimeHandoffInputV1 | None = None
    output_declarations: tuple[RuntimeOutputDeclarationV1, ...] = Field(
        default=(),
        max_length=10_000,
    )

    @field_validator("task_image_ref", "runtime_image_ref")
    @classmethod
    def _images_are_immutable(cls, value: str) -> str:
        if _DIGEST_REF.fullmatch(value) is None:
            raise ValueError("runtime plan images must be digest-pinned")
        return value

    @field_validator("agent_image_ref")
    @classmethod
    def _agent_image_is_immutable(cls, value: str | None) -> str | None:
        if value is not None:
            cls._images_are_immutable(value)
        return value

    def resident_private_roles(self) -> set[str]:
        """Sandboxes that exist in this pod, not ones admitted later."""
        if self.execution_role == "verifier":
            return {"verifier-sandbox"}
        if (
            self.in_place_verifier
            or self.verifier_execution == VerifierExecution.SEPARATE_EXECUTION
        ):
            return {"task-sandbox"}
        return {"task-sandbox", "verifier-sandbox"}

    @model_validator(mode="after")
    def _roles_and_dependencies_are_closed(self) -> ExecutionRuntimePlanV1:
        guests = [sidecar for sidecar in self.sidecars if sidecar.guest_execution is not None]
        guest_definition = nebius_guest_class_by_id(self.execution_class_id)
        guest_class = guest_definition is not None
        if guest_definition is not None and guest_definition.guest_execution is not None:
            supported = guest_definition.guest_execution.supported_capabilities
            for sidecar in guests:
                assert sidecar.guest_execution is not None
                declared_guest = frozenset(sidecar.guest_execution.capabilities)
                if not declared_guest <= supported or (
                    ("emulated_pkcs11_authentication" in declared_guest)
                    != ("emulated_pkcs11_authentication" in supported)
                ):
                    raise ValueError("guest capabilities do not match the immutable class")
        if guest_class != bool(guests):
            raise ValueError("guest execution class and guest plan must be selected together")
        if guests and (
            {sidecar.role_name for sidecar in guests} != {"task-sandbox", "verifier-sandbox"}
            or len(self.sidecars) != 2
            or self.execution_role != "attempt" or self.composition != RuntimeComposition.INIT_PAYLOAD
            or self.verifier_execution != VerifierExecution.IN_ATTEMPT
            or self.runtime_volume_mib < 1024
            or self.controller_resources is None
            or any(sidecar.guest_execution != guests[0].guest_execution
                   or sidecar.resources != self.task_resources
                   or sidecar.image_ref != self.task_image_ref for sidecar in guests)
        ):
            raise ValueError("guest execution requires matching private task/verifier plans and payload storage")
        fixtures = [sidecar for sidecar in self.sidecars if sidecar.task_fixture]
        private = {sidecar.role_name for sidecar in self.sidecars if sidecar.private_sandbox}
        expected = self.resident_private_roles()
        if fixtures and (
            len(fixtures) != 1 or self.task_image_materialization_id is None
            or self.agent_image_ref is None or self.execution_role != "attempt"
            or self.composition != RuntimeComposition.INIT_PAYLOAD
            or private != expected
            or len(self.sidecars) != 1 + len(expected) or not self.sidecars[0].task_fixture
        ):
            raise ValueError("one prepared fixture requires an isolated attempt controller and both sandboxes")
        if self.task_egress is not None and TASK_EGRESS_OUTPUT not in self.output_declarations:
            raise ValueError("task egress requires its immutable diagnostic output declaration")
        if (
            self.effective_network_policy is not None
            and hosted_http_egress(self.effective_network_policy) != self.task_egress
        ):
            raise ValueError("task egress does not match the effective network policy")
        phases = (self.main, *((self.verifier,) if self.verifier is not None else ()))
        for phase in phases:
            raw_policy = phase.environment.get("LOOM_EFFECTIVE_NETWORK_POLICY_JSON")
            if self.effective_network_policy is None:
                if raw_policy is not None:
                    raise ValueError("phase network policy has no immutable plan authority")
                continue
            if raw_policy is None:
                raise ValueError("phase is missing the effective network policy")
            try:
                phase_policy = _NETWORK_POLICY_ADAPTER.validate_json(raw_policy)
            except ValueError:
                raise ValueError("phase effective network policy is invalid") from None
            if phase_policy != self.effective_network_policy:
                raise ValueError("phase effective network policy differs from the plan")
        if self.task_image_materialization_id is not None and (
            self.task_image_materialization_id.int == 0
            or self.agent_image_ref is None
            or self.composition != RuntimeComposition.INIT_PAYLOAD
        ):
            raise ValueError("prepared task images require a separate trusted attempt controller")
        if self.handoff_input is not None and (
            self.execution_role != "verifier" or self.task_input is None
        ):
            raise ValueError("a workspace handoff belongs only to a deferred verifier with task input")
        if (
            any(sidecar.private_sandbox for sidecar in self.sidecars)
            and self.agent_image_ref is None
        ):
            raise ValueError("private sandboxes require a separate agent image reference")
        if any(phase.role != "setup" for phase in self.setup):
            raise ValueError("runtime phase roles do not match their positions")
        if self.execution_role == "attempt":
            if self.main.role != "agent":
                raise ValueError("attempt execution requires an agent main phase")
            if self.verifier_execution == VerifierExecution.IN_ATTEMPT:
                if self.verifier is None or self.verifier.role != "verifier":
                    raise ValueError("in-attempt verifier requires a verifier phase")
            elif self.verifier is not None:
                raise ValueError("separate/skipped verifier cannot run in the primary attempt")
        elif (
            self.main.role != "verifier"
            or self.verifier_execution != VerifierExecution.SKIPPED
            or self.verifier is not None
        ):
            raise ValueError("verifier execution requires one verifier main phase")
        names = [sidecar.role_name for sidecar in self.sidecars]
        if len(names) != len(set(names)):
            raise ValueError("sidecar role names must be unique")
        if {"execution", "runtime-materializer", "setup", "agent", "verifier"} & set(names):
            raise ValueError("sidecar role name collides with a reserved container role")
        known: set[str] = set()
        for sidecar in self.sidecars:
            if sidecar.private_sandbox and sidecar.role_name not in {
                "task-sandbox",
                "verifier-sandbox",
            }:
                raise ValueError("private sandbox role must be task-sandbox or verifier-sandbox")
            if (
                sidecar.role_name in {"task-sandbox", "verifier-sandbox"}
                and not sidecar.private_sandbox
            ):
                raise ValueError("sandbox roles require private mounts")
            if any(item not in known for item in sidecar.depends_on):
                raise ValueError("sidecar dependencies must reference earlier sidecars")
            known.add(sidecar.role_name)
        private_roles = {sidecar.role_name for sidecar in self.sidecars if sidecar.private_sandbox}
        expected_roles = self.resident_private_roles()
        if self.verifier_after_agent_timeout and (
            self.execution_role != "attempt"
            or self.composition != RuntimeComposition.INIT_PAYLOAD
            or self.agent_image_ref is None
            or self.verifier_execution != "in_attempt"
            or private_roles != expected_roles
        ):
            raise ValueError("timeout verification requires an isolated attempt controller and in-attempt verifier")
        if self.controller_resources is not None:
            sandboxes = [sidecar for sidecar in self.sidecars if sidecar.private_sandbox]
            if (
                self.agent_image_ref is None
                or self.execution_role not in {"attempt", "verifier"}
                or self.composition != RuntimeComposition.INIT_PAYLOAD
                or {sidecar.role_name for sidecar in sandboxes} != self.resident_private_roles()
            ):
                raise ValueError("controller resources require an isolated attempt controller")
            if any(sidecar.resources != self.task_resources for sidecar in sandboxes):
                raise ValueError("controller sizing must preserve task and verifier resources")
            if (self.node_resource_allocation is None
                    and self.controller_resources.ephemeral_storage_mib
                    != self.task_resources.ephemeral_storage_mib + (self.runtime_volume_mib if guests else 0)):
                raise ValueError("controller sizing must preserve task-derived storage")
        if guests:
            declared = (self.node_resource_allocation.declared_task
                        if self.node_resource_allocation else self.task_resources)
            if (self.execution_resources.ephemeral_storage_mib
                    < declared.ephemeral_storage_mib + self.runtime_volume_mib
                    or self.container_request("execution").ephemeral_storage_mib <= self.runtime_volume_mib):
                raise ValueError("guest payload storage must be reserved in the controller allocation and request")
        if self.resource_requests is not None:
            sandboxes = [sidecar for sidecar in self.sidecars if sidecar.private_sandbox]
            if (
                self.agent_image_ref is None
                or self.execution_role not in {"attempt", "verifier"}
                or self.composition != RuntimeComposition.INIT_PAYLOAD
                or {sidecar.role_name for sidecar in sandboxes} != self.resident_private_roles()
                or any(sidecar.resources != self.task_resources for sidecar in sandboxes)
            ):
                raise ValueError("resource requests require an isolated attempt controller")
            self.resource_requests.validate_limits(
                controller=self.execution_resources, task=self.task_resources,
            )
        if self.node_resource_allocation is not None:
            allocation = self.node_resource_allocation
            if allocation.baseline_slots != max(1, allocation.usable_node.cpu_millis // 1000):
                raise ValueError("node allocation slot count does not match usable CPU")
            if any(getattr(self.task_resources, key) < getattr(allocation.declared_task, key)
                   for key in ContainerResourcesV1.model_fields):
                raise ValueError("node allocation cannot reduce declared task requirements")
            for role, limits in [("execution", self.execution_resources), *(
                (sidecar.role_name, sidecar.resources) for sidecar in self.sidecars
            )]:
                if self.container_request(role).memory_mib != limits.memory_mib:
                    raise ValueError("node allocation memory requests must equal limits")
        source_paths = [item.source_path for item in self.output_declarations]
        bundle_paths = [item.relative_path for item in self.output_declarations]
        if len(source_paths) != len(set(source_paths)) or len(bundle_paths) != len(
            set(bundle_paths)
        ):
            raise ValueError("runtime output declarations must be unique")
        return self

    def canonical_payload(self) -> dict[str, object]:
        payload = self.model_dump(mode="json")
        if self.effective_network_policy is None:
            payload.pop("effective_network_policy")
        if self.task_egress is None:
            payload.pop("task_egress")
        # Keep existing published plans byte-compatible when new fields are unused.
        if not self.verifier_after_agent_timeout:
            payload.pop("verifier_after_agent_timeout")
        if not self.in_place_verifier:
            payload.pop("in_place_verifier")
        if self.controller_resources is None:
            payload.pop("controller_resources")
        if self.resource_requests is None:
            payload.pop("resource_requests")
        if self.node_resource_allocation is None:
            payload.pop("node_resource_allocation")
        if self.agent_image_ref is None:
            payload.pop("agent_image_ref")
        if self.task_image_materialization_id is None:
            payload.pop("task_image_materialization_id")
        if self.handoff_input is None:
            payload.pop("handoff_input")
        for sidecar in payload["sidecars"]:
            if not sidecar["private_sandbox"]:
                sidecar.pop("private_sandbox")
            if sidecar["identity"] is None:
                sidecar.pop("identity")
        return payload

    @property
    def execution_resources(self) -> ContainerResourcesV1:
        return self.controller_resources or self.task_resources

    def container_request(self, role_name: str) -> ContainerResourcesV1:
        """Resolve the same requests for rendering, admission, and finance."""
        if role_name == "execution":
            requested = self.resource_requests.controller if self.resource_requests else None
            return requested or self.execution_resources
        sidecar = next(item for item in self.sidecars if item.role_name == role_name)
        field = {"task-sandbox": "task_sandbox", "verifier-sandbox": "verifier_sandbox"}.get(role_name)
        requested = getattr(self.resource_requests, field) if self.resource_requests and field else None
        return requested or sidecar.resources

    def published_image_refs(self) -> tuple[str, ...]:
        """Images executed with platform trust, separate from prepared task sandboxes.

        The Control Plane checks the materialization against the Trial before
        saving a lease. The authenticated lease carries that decision to the
        actuator; a caller-provided UUID alone never authorizes an image.
        """
        refs = {self.runtime_image_ref, self.agent_image_ref or self.task_image_ref}
        if self.task_image_materialization_id is None:
            refs.add(self.task_image_ref)
        refs.update(
            sidecar.image_ref for sidecar in self.sidecars
            if not (
                self.task_image_materialization_id is not None
                and (sidecar.task_fixture or (
                    sidecar.private_sandbox and sidecar.image_ref == self.task_image_ref
                ))
            )
        )
        return tuple(sorted(refs))


def runtime_pod_resources(plan: ExecutionRuntimePlanV1) -> ContainerResourcesV1:
    """Effective Kubernetes request: materializer runs before native sidecars."""

    requests = [plan.container_request("execution"), *(
        plan.container_request(sidecar.role_name) for sidecar in plan.sidecars
    )]
    return ContainerResourcesV1(
        cpu_millis=max(
            50, sum(item.cpu_millis for item in requests)
        ),
        memory_mib=max(
            64, sum(item.memory_mib for item in requests)
        ),
        ephemeral_storage_mib=max(
            32,
            sum(item.ephemeral_storage_mib for item in requests),
        ),
    )


class RuntimeStreamEvidenceV1(_Strict):
    path: str = Field(pattern=r"^[0-9]{2}-(?:setup|agent|verifier)\.(?:stdout|stderr)$")
    sha256: str = Field(pattern=_SHA256.pattern)
    bytes_seen: int = Field(ge=0)
    bytes_saved: int = Field(ge=0)
    truncated: bool

    @model_validator(mode="after")
    def _saved_bytes_are_bounded(self) -> RuntimeStreamEvidenceV1:
        if self.bytes_saved > self.bytes_seen or self.truncated != (
            self.bytes_saved < self.bytes_seen
        ):
            raise ValueError("stream truncation evidence is inconsistent")
        return self


class RuntimePhaseEvidenceV1(_Strict):
    role: Literal["setup", "agent", "verifier"]
    ordinal: int = Field(gt=0, le=64)
    started_at: datetime
    finished_at: datetime
    exit_code: int
    signal: str | None = Field(default=None, max_length=32)
    timed_out: bool
    stdout: RuntimeStreamEvidenceV1
    stderr: RuntimeStreamEvidenceV1

    @model_validator(mode="after")
    def _phase_time_is_ordered(self) -> RuntimePhaseEvidenceV1:
        if self.finished_at < self.started_at:
            raise ValueError("runtime phase timestamps are reversed")
        return self


class ExecutionRuntimeResultV1(_Strict):
    schema_version: Literal["loom.execution-runtime-result.v1"]
    runtime_contract_sha256: str = Field(pattern=_SHA256.pattern)
    candidate_sha: str = Field(pattern=_CANDIDATE.pattern)
    task_revision_sha256: str = Field(pattern=_SHA256.pattern)
    command_identity_sha256: str = Field(pattern=_SHA256.pattern)
    execution_role: Literal["attempt", "verifier"]
    container_roles: tuple[str, ...] = Field(min_length=2, max_length=66)
    task_image_ref: str
    runtime_image_ref: str
    runtime_binary_sha256: str = Field(pattern=_SHA256.pattern)
    execution_class_id: str
    status: Literal[
        "succeeded",
        "setup_error",
        "task_error",
        "verifier_error",
        "timed_out",
        "cancelled",
        "runtime_error",
        "artifact_upload_failed",
        "missing_required_artifacts",
        "trajectory_flush_failed",
    ]
    started_at: datetime
    finished_at: datetime
    phases: tuple[RuntimePhaseEvidenceV1, ...] = Field(max_length=64)
    outputs: tuple[RuntimeOutputEvidenceV1, ...] = Field(default=(), max_length=10_000)
    verifier_rewards: dict[str, float] | None = None
    failure_reason: Literal["sandbox_lost"] | None = None
    partial_evidence: bool

    @field_validator("task_image_ref", "runtime_image_ref")
    @classmethod
    def _result_images_are_immutable(cls, value: str) -> str:
        if _DIGEST_REF.fullmatch(value) is None:
            raise ValueError("runtime result images must be digest-pinned")
        return value

    @model_validator(mode="after")
    def _terminal_result_is_consistent(self) -> ExecutionRuntimeResultV1:
        if self.finished_at < self.started_at:
            raise ValueError("runtime result timestamps are reversed")
        if self.partial_evidence != (self.status != "succeeded"):
            raise ValueError("runtime partial-evidence flag does not match status")
        if self.failure_reason is not None and self.status != "runtime_error":
            raise ValueError("sandbox loss must remain a runtime failure")
        if [phase.ordinal for phase in self.phases] != list(range(1, len(self.phases) + 1)):
            raise ValueError("runtime phase ordinals are not contiguous")
        sources = [item.source_path for item in self.outputs]
        paths = [item.relative_path for item in self.outputs]
        if len(sources) != len(set(sources)) or len(paths) != len(set(paths)):
            raise ValueError("runtime output evidence must be unique")
        if self.status == "succeeded" and any(
            item.required and item.state != "captured" for item in self.outputs
        ):
            raise ValueError("successful runtime result is missing required output")
        if self.verifier_rewards is not None:
            if not self.verifier_rewards or any(
                not key or len(key.encode("utf-8")) > 256 or not math.isfinite(value)
                for key, value in self.verifier_rewards.items()
            ):
                raise ValueError("runtime verifier rewards are invalid")
        return self


def validate_runtime_plan_requirements(
    plan: ExecutionRuntimePlanV1,
    requirements: WorkloadRequirementsV1,
) -> None:
    """Reject semantic drift between admission requirements and the runtime plan."""

    declared_capabilities = (requirements.execution_requirements.capabilities
                             if requirements.execution_requirements else ())
    expected_guest = ALL_GUEST_EXECUTION_CAPABILITIES.intersection(declared_capabilities)
    guests = [sidecar.guest_execution for sidecar in plan.sidecars if sidecar.guest_execution is not None]
    # A forced plain guest has a guest kernel without declared capabilities;
    # declared capabilities always require one.
    if bool(guests) != (requirements.isolation_level == IsolationLevel.DEDICATED_GUEST_KERNEL) or (
        expected_guest and not guests
    ):
        raise ValueError("runtime guest plan does not match workload isolation and capabilities")
    if guests:
        execution_class = nebius_guest_class_by_id(plan.execution_class_id)
        if execution_class is None:
            raise ValueError("runtime guest class is unavailable")
        admission = evaluate_execution_admission(requirements, execution_class)
        if not admission.compatible or any(frozenset(guest.capabilities) != expected_guest for guest in guests):
            raise ValueError("runtime guest plan does not match admitted workload requirements")
    if requirements.task_egress != plan.task_egress:
        raise ValueError("runtime plan network policy does not match workload requirements")
    # Historical task-authored/static plans predate the frozen policy field.
    # Their task_egress equality above remains authoritative. New automatic
    # hosted plans always populate this field and must match it exactly.
    if (
        plan.effective_network_policy is not None
        and requirements.effective_network_policy != plan.effective_network_policy
    ):
        raise ValueError("runtime plan effective network policy does not match workload requirements")
    if requirements.image_ref != plan.task_image_ref:
        raise ValueError("runtime plan task image does not match workload requirements")
    expected_resources = (
        requirements.cpu_millis,
        requirements.memory_mib,
        requirements.ephemeral_storage_mib,
    )
    declared = (plan.node_resource_allocation.declared_task
                if plan.node_resource_allocation else plan.task_resources)
    actual_resources = (declared.cpu_millis, declared.memory_mib, declared.ephemeral_storage_mib)
    if expected_resources != actual_resources:
        raise ValueError("runtime plan resources do not match workload requirements")
    if requirements.sidecar_count != sum(not sidecar.private_sandbox for sidecar in plan.sidecars):
        raise ValueError("runtime plan sidecars do not match workload requirements")
    if plan.execution_role == "verifier":
        if plan.verifier_execution != VerifierExecution.SKIPPED:
            raise ValueError("a verifier execution unit cannot schedule another verifier")
    else:
        expected_verifier = {
            VerifierTopology.IN_ATTEMPT: VerifierExecution.IN_ATTEMPT,
            VerifierTopology.SEPARATE_EXECUTION: VerifierExecution.SEPARATE_EXECUTION,
        }[requirements.verifier_topology]
        if plan.verifier_execution != expected_verifier:
            raise ValueError("runtime plan verifier topology does not match workload requirements")


__all__ = [
    "ContainerResourcesV1",
    "ExecutionResourceRequestsV1",
    "ExecutionRuntimePlanV1",
    "ExecutionRuntimeResultV1",
    "GuestExecutionV1",
    "ProbeV1",
    "ProcessPhaseV1",
    "RuntimeComposition",
    "RuntimeHandoffInputV1",
    "RuntimeOutputDeclarationV1",
    "RuntimeOutputEvidenceV1",
    "RuntimePhaseEvidenceV1",
    "RuntimeStreamEvidenceV1",
    "RuntimeTaskInputV1",
    "SidecarContainerV1",
    "VerifierExecution",
    "validate_runtime_plan_requirements",
]
