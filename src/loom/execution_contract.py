"""Provider-neutral service execution and workload admission contracts.

The contracts in this module define what a service workload needs without
choosing a provider or physical target.  Provider/region binding belongs to an
execution target (and, later, a durable execution lease), while admission is a
strict comparison between immutable workload requirements and an execution
class.

Issue #1548 deliberately keeps these models independent from the current
``backend``/worker-pool scheduler fields.  They are the versioned boundary that
the durable execution control plane introduced by #1540 will persist.
"""

from __future__ import annotations

import re
from datetime import datetime
from enum import StrEnum
from typing import Any, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_serializer,
    model_serializer,
    model_validator,
)

from loom.execution_requirements import (
    ALL_GUEST_EXECUTION_CAPABILITIES,
    GUEST_EXECUTION_CAPABILITIES,
    GuestExecutionCapability,
    TaskExecutionRequirementsV1,
    execution_requirement_diagnostics,
)
from loom.hosted_harness import is_workspace_harness
from loom.models.networking import (
    NetworkPolicy,
    TaskHttpEgress,
    hosted_http_egress,
    resolve_effective_network_policy,
)
from loom.models.task import TaskConfig
from loom.models.trial import TrialConfig
from loom.verifier_runtime import resolve_verifier_env_mode

_IMMUTABLE_OCI_REF = re.compile(r"^.+@sha256:[0-9a-f]{64}$")


class _StrictContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class IsolationLevel(StrEnum):
    """Container isolation supplied by an execution class."""

    SHARED_KERNEL = "shared_kernel"
    SANDBOXED_RUNTIME = "sandboxed_runtime"
    DEDICATED_EPHEMERAL_NODE = "dedicated_ephemeral_node"
    DEDICATED_GUEST_KERNEL = "dedicated_guest_kernel"


class NetworkAccess(StrEnum):
    NONE = "none"
    GATEWAY_ONLY = "gateway_only"
    APPROVED_ALLOWLIST = "approved_allowlist"
    UNRESTRICTED_PUBLIC = "unrestricted_public"


class ImageMaterialization(StrEnum):
    IMMUTABLE_OCI = "immutable_oci"
    TASK_DOCKERFILE = "task_dockerfile"
    MUTABLE_OCI = "mutable_oci"
    UNSPECIFIED = "unspecified"


class VerifierTopology(StrEnum):
    IN_ATTEMPT = "in_attempt"
    SEPARATE_EXECUTION = "separate_execution"


class ExecutionAdapterKind(StrEnum):
    LEGACY_WORKER_CLAIM = "legacy_worker_claim"
    KUBERNETES_JOB = "kubernetes_job"


class CapacityEvidenceKind(StrEnum):
    FRESH_EXECUTABLE = "fresh_executable_capacity"
    CONFIGURED_SCALE_HEADROOM = "configured_scale_headroom"
    UNAVAILABLE = "unavailable"
    PREEXISTING_ASSIGNMENT = "preexisting_assignment"


class ExecutionRoutingReason(StrEnum):
    FRESH_EXECUTABLE_CAPACITY = "fresh_executable_capacity"
    CONFIGURED_SCALE_HEADROOM = "configured_scale_headroom"
    OPERATOR_PIN = "operator_pin"
    PREEXISTING_ASSIGNMENT = "preexisting_assignment"
    ADMIN_TARGET_BINDING = "admin_target_binding"


class PoolCapacityV1(_StrictContract):
    """Normalized capacity evidence without upgrading configuration to capacity."""

    schema_version: Literal["loom.pool-capacity.v1"] = "loom.pool-capacity.v1"
    logical_pool_id: str = Field(min_length=1, max_length=80)
    adapter_kind: ExecutionAdapterKind
    environment: str | None
    region: str | None
    data_residency: str | None
    configured_ceiling_slots: int = Field(ge=0)
    configured_scale_headroom_slots: int = Field(ge=0)
    observed_active_slots: int = Field(ge=0)
    observed_occupied_slots: int = Field(ge=0)
    observed_pending_slots: int = Field(ge=0)
    assigned_queued_slots: int = Field(ge=0)
    executable_free_slots: int = Field(ge=0)
    capacity_evidence_kind: CapacityEvidenceKind
    capacity_observed_at: datetime | None
    capacity_fresh_until: datetime | None
    capacity_is_fresh: bool
    capacity_freshness_seconds: int = Field(gt=0)
    aggregate_executable_eligible: bool
    enabled: bool
    healthy: bool
    draining: bool
    budget_eligible: bool
    estimated_cost_microusd_per_slot_hour: int | None = Field(default=None, ge=0)
    operator_weight: int = Field(ge=-1_000, le=1_000)
    blockers: tuple[str, ...]

    @model_validator(mode="after")
    def _capacity_evidence_is_consistent(self) -> PoolCapacityV1:
        if self.configured_scale_headroom_slots > self.configured_ceiling_slots:
            raise ValueError("configured scale headroom exceeds the configured ceiling")
        is_executable = (
            self.capacity_is_fresh
            and self.capacity_evidence_kind == CapacityEvidenceKind.FRESH_EXECUTABLE
        )
        if self.aggregate_executable_eligible != is_executable:
            raise ValueError("aggregate executable eligibility does not match fresh evidence")
        if not self.capacity_is_fresh and self.executable_free_slots != 0:
            raise ValueError("stale capacity cannot expose executable free slots")
        if self.executable_free_slots > 0 and not is_executable:
            raise ValueError("executable free slots require fresh executable evidence")
        if (self.capacity_observed_at is None) != (self.capacity_fresh_until is None):
            raise ValueError("capacity observation and freshness deadline must be grouped")
        return self


class GuestExecutionClassV1(_StrictContract):
    """Guest-local capabilities, without authority over the trusted host."""

    schema_version: Literal["loom.guest-execution-class.v1"] = "loom.guest-execution-class.v1"
    runtime: Literal["qemu-tcg-v1"] = "qemu-tcg-v1"
    supported_capabilities: frozenset[GuestExecutionCapability] = GUEST_EXECUTION_CAPABILITIES

    @field_serializer("supported_capabilities", when_used="json")
    def _serialize_capabilities(self, value: frozenset[GuestExecutionCapability]) -> list[str]:
        return sorted(value)


class ExecutionClassV1(_StrictContract):
    """Versioned, provider-neutral capabilities for one execution class."""

    schema_version: Literal["loom.execution-class.v1"] = "loom.execution-class.v1"
    class_id: str = Field(pattern=r"^[a-z0-9][a-z0-9-]{0,79}$")
    operating_system: Literal["linux"]
    cpu_architecture: Literal["x86_64", "arm64"]
    gpu_vendor: Literal["none", "nvidia"]
    isolation_level: IsolationLevel
    network_access: frozenset[NetworkAccess]
    supports_task_web_egress: bool = False
    maximum_cpu_millis: int | None = Field(default=None, gt=0)
    maximum_memory_mib: int | None = Field(default=None, gt=0)
    maximum_ephemeral_storage_mib: int | None = Field(default=None, gt=0)
    maximum_sidecars: int = Field(ge=0)
    supports_separate_verifier: bool
    supports_custom_dns: bool
    supports_extra_hosts: bool
    supports_tmpfs: bool
    supports_task_image_build: bool
    requires_immutable_image: bool
    permits_privileged: bool
    permits_host_path: bool
    permits_host_network: bool
    permits_nested_containers: bool
    permits_host_devices: bool
    guest_execution: GuestExecutionClassV1 | None = Field(
        default=None, exclude_if=lambda value: value is None,
    )

    @model_serializer(mode="wrap")
    def _omit_unused_web_egress(self, handler: Any) -> dict[str, Any]:
        payload: dict[str, Any] = handler(self)
        if not self.supports_task_web_egress:
            payload.pop("supports_task_web_egress", None)
        return payload

    @field_serializer("network_access", when_used="json")
    def _serialize_network_access(self, value: frozenset[NetworkAccess]) -> list[str]:
        return sorted(item.value for item in value)

    @model_validator(mode="after")
    def _forbid_host_escape_capabilities(self) -> ExecutionClassV1:
        forbidden = {
            "permits_privileged": self.permits_privileged,
            "permits_host_path": self.permits_host_path,
            "permits_host_network": self.permits_host_network,
            "permits_nested_containers": self.permits_nested_containers,
            "permits_host_devices": self.permits_host_devices,
        }
        enabled = sorted(name for name, value in forbidden.items() if value)
        if enabled:
            raise ValueError(
                "service execution classes cannot enable host-escape capabilities: "
                + ", ".join(enabled),
            )
        return self

    @model_validator(mode="after")
    def _guest_capabilities_require_an_independent_kernel(self) -> ExecutionClassV1:
        dedicated_guest = self.isolation_level == IsolationLevel.DEDICATED_GUEST_KERNEL
        if dedicated_guest != (self.guest_execution is not None):
            raise ValueError("guest_execution requires exactly dedicated_guest_kernel isolation")
        if dedicated_guest and (self.cpu_architecture != "x86_64" or self.gpu_vendor != "none"):
            raise ValueError("guest execution supports only Linux x86_64 CPU classes")
        return self


class ExecutionTargetV1(_StrictContract):
    """Environment binding onto one provider-bound physical cluster scope."""

    schema_version: Literal["loom.execution-target.v1"] = "loom.execution-target.v1"
    target_id: str = Field(pattern=r"^[a-z0-9][a-z0-9-]{0,79}$")
    logical_pool_id: str = Field(pattern=r"^[a-z0-9][a-z0-9-]{0,79}$")
    execution_class_id: str = Field(pattern=r"^[a-z0-9][a-z0-9-]{0,79}$")
    capacity_owner_target_id: str | None = Field(
        default=None, pattern=r"^[a-z0-9][a-z0-9-]{0,79}$", exclude_if=lambda value: value is None,
    )
    # Optional at the standalone V1 parsing boundary so already-persisted
    # regional records remain readable. A current topology requires an explicit
    # physical cluster scope for every target.
    cluster_scope_id: str | None = Field(
        default=None,
        pattern=r"^[a-z0-9][a-z0-9-]{0,79}$",
    )
    environment: Literal["development", "staging", "production"]
    provider: Literal["nebius"]
    region: str = Field(min_length=1, max_length=80)
    failure_domain: str = Field(min_length=1, max_length=120)
    data_residency: Literal["eu"]
    namespace_name: str = Field(pattern=r"^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$")
    pod_identity_audience: str | None = Field(default=None, min_length=1, max_length=120)
    service_account_name: str = Field(
        default="loom-execution-attempt", pattern=r"^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$"
    )
    health_role: Literal["primary", "secondary"]
    health_check_id: str = Field(pattern=r"^[a-z0-9][a-z0-9-]{0,79}$")
    health_check_interval_seconds: int = Field(ge=5, le=300)
    health_stale_after_seconds: int = Field(ge=10, le=900)

    @model_validator(mode="after")
    def _health_freshness_exceeds_probe_interval(self) -> ExecutionTargetV1:
        if self.capacity_owner_target_id is not None and (
            self.capacity_owner_target_id == self.target_id
            or self.execution_class_id not in {
                "linux-amd64-cpu-guest-v1", "linux-amd64-cpu-guest-web-v1",
                "linux-amd64-cpu-guest-auth-v1", "linux-amd64-cpu-guest-auth-web-v1",
            }
        ):
            raise ValueError("only a guest target may share a distinct capacity owner")
        if self.health_stale_after_seconds <= self.health_check_interval_seconds:
            raise ValueError(
                "health_stale_after_seconds must exceed the independent probe interval",
            )
        return self


class ExecutionTopologyV1(_StrictContract):
    """Explicit regional bindings within one pool and residency boundary."""

    schema_version: Literal["loom.execution-topology.v1"] = "loom.execution-topology.v1"
    logical_pool_id: str = Field(pattern=r"^[a-z0-9][a-z0-9-]{0,79}$")
    execution_class_id: str = Field(pattern=r"^[a-z0-9][a-z0-9-]{0,79}$")
    placement_policy: Literal["environment-local-health-first"]
    targets: tuple[ExecutionTargetV1, ...] = Field(min_length=1, max_length=64)

    @model_validator(mode="after")
    def _targets_form_explicit_regional_topology(self) -> ExecutionTopologyV1:
        target_ids = [target.target_id for target in self.targets]
        health_ids = [target.health_check_id for target in self.targets]
        namespaces = [(target.cluster_scope_id, target.namespace_name) for target in self.targets]
        if len(target_ids) != len(set(target_ids)):
            raise ValueError("execution target ids must be unique")
        if len(health_ids) != len(set(health_ids)):
            raise ValueError("every execution target needs an independent health check")
        if len(namespaces) != len(set(namespaces)):
            raise ValueError("every execution target needs an isolated namespace")
        for target in self.targets:
            if target.logical_pool_id != self.logical_pool_id:
                raise ValueError("every target must bind the declared logical pool")
            if target.execution_class_id != self.execution_class_id:
                raise ValueError("every target must bind the declared execution class")

        clusters: dict[str, tuple[str, str]] = {}
        for target in self.targets:
            if target.cluster_scope_id is None:
                raise ValueError("every target must bind an explicit physical cluster scope")
            scope = (target.region, target.failure_domain)
            if clusters.setdefault(target.cluster_scope_id, scope) != scope:
                raise ValueError("one physical cluster must have one region and failure domain")
        for environment in {target.environment for target in self.targets}:
            targets = [target for target in self.targets if target.environment == environment]
            if sum(target.health_role == "primary" for target in targets) != 1:
                raise ValueError("every environment needs exactly one primary target")
            if len({target.data_residency for target in targets}) != 1:
                raise ValueError("regional targets must preserve data residency")
            if len({target.cluster_scope_id for target in targets}) != len(targets):
                raise ValueError("an environment may bind a physical cluster only once")
            if any(
                target.health_role == "secondary" and target.pod_identity_audience is None
                for target in targets
            ):
                raise ValueError("regional targets require native Pod identity")
        return self


class WorkloadRequirementsV1(_StrictContract):
    """Complete material capability declaration for one execution unit."""

    schema_version: Literal["loom.workload-requirements.v1"] = "loom.workload-requirements.v1"
    operating_system: Literal["linux", "windows"]
    cpu_architecture: Literal["x86_64", "arm64", "any"]
    data_residency: str | None = Field(default=None, pattern=r"^[a-z]{2}$")
    gpu_vendor: Literal["none", "nvidia"]
    gpu_count: int = Field(ge=0)
    cpu_millis: int | None = Field(gt=0)
    memory_mib: int | None = Field(gt=0)
    ephemeral_storage_mib: int | None = Field(gt=0)
    isolation_level: IsolationLevel
    network_access: NetworkAccess
    effective_network_policy: NetworkPolicy | None = None
    task_egress: TaskHttpEgress | None = None
    image_materialization: ImageMaterialization
    image_ref: str | None
    sidecar_count: int = Field(ge=0)
    verifier_topology: VerifierTopology
    custom_dns: bool
    extra_hosts: bool
    tmpfs: bool
    privileged: bool
    host_path: bool
    host_network: bool
    nested_containers: bool
    host_devices: bool
    host_specialized: bool
    execution_requirements: TaskExecutionRequirementsV1 | None = Field(
        default=None, exclude_if=lambda value: value is None,
    )

    @model_serializer(mode="wrap")
    def _omit_unused_egress(self, handler: Any) -> dict[str, Any]:
        payload: dict[str, Any] = handler(self)
        if self.effective_network_policy is None:
            payload.pop("effective_network_policy", None)
        if self.task_egress is None:
            payload.pop("task_egress", None)
        return payload

    @model_validator(mode="after")
    def _image_identity_matches_materialization(self) -> WorkloadRequirementsV1:
        if self.task_egress is not None and self.network_access != NetworkAccess.APPROVED_ALLOWLIST:
            raise ValueError("task egress requires approved_allowlist networking")
        if self.image_materialization == ImageMaterialization.IMMUTABLE_OCI:
            if self.image_ref is None or not _IMMUTABLE_OCI_REF.fullmatch(self.image_ref):
                raise ValueError(
                    "immutable_oci requires an image_ref pinned by @sha256:<64 hex>",
                )
        return self


class CompatibilityReasonV1(_StrictContract):
    code: str = Field(pattern=r"^[a-z0-9][a-z0-9_]{0,79}$")
    message: str = Field(min_length=1, max_length=500)


class ExecutionAdmissionV1(_StrictContract):
    schema_version: Literal["loom.execution-admission.v1"] = "loom.execution-admission.v1"
    execution_class_id: str
    compatible: bool
    reasons: tuple[CompatibilityReasonV1, ...]

    @model_validator(mode="after")
    def _decision_matches_reasons(self) -> ExecutionAdmissionV1:
        if self.compatible == bool(self.reasons):
            raise ValueError("compatible must be true exactly when reasons is empty")
        return self


class ExecutionRouteCandidateV1(_StrictContract):
    """One independently evidenced physical-pool candidate."""

    logical_pool_id: str = Field(pattern=r"^[a-z0-9][a-z0-9-]{0,79}$")
    adapter_kind: ExecutionAdapterKind
    target_id: str | None = Field(default=None, pattern=r"^[a-z0-9][a-z0-9-]{0,79}$")
    execution_class_id: str | None = Field(
        default=None,
        pattern=r"^[a-z0-9][a-z0-9-]{0,79}$",
    )
    environment: Literal["development", "staging", "production"] | None = None
    region: str | None = Field(default=None, min_length=1, max_length=80)
    data_residency: str | None = Field(default=None, pattern=r"^[a-z]{2}$")
    operator_weight: int = Field(default=0, ge=-1_000, le=1_000)
    budget_eligible: bool = True
    estimated_cost_microusd_per_slot_hour: int | None = Field(default=None, ge=0)
    enabled: bool
    healthy: bool
    draining: bool
    configured_slots: int = Field(ge=0)
    active_slots: int = Field(ge=0)
    occupied_slots: int = Field(ge=0)
    pending_slots: int = Field(ge=0)
    assigned_queued_slots: int = Field(ge=0)
    available_slots: int = Field(ge=0)
    capacity_evidence_kind: CapacityEvidenceKind
    capacity_observed_at: datetime | None = None
    blockers: tuple[str, ...] = Field(default=(), max_length=32)

    @model_validator(mode="after")
    def _candidate_evidence_is_coherent(self) -> ExecutionRouteCandidateV1:
        if self.adapter_kind == ExecutionAdapterKind.KUBERNETES_JOB and (
            self.target_id is None or self.execution_class_id is None
        ):
            raise ValueError("Kubernetes routing candidates require target and class identity")
        if self.capacity_evidence_kind == CapacityEvidenceKind.FRESH_EXECUTABLE and (
            self.capacity_observed_at is None or self.available_slots <= 0
        ):
            raise ValueError("fresh executable capacity requires timestamped available slots")
        if self.capacity_evidence_kind == CapacityEvidenceKind.CONFIGURED_SCALE_HEADROOM and (
            self.available_slots <= 0
        ):
            raise ValueError("configured scale headroom requires a positive bounded slot count")
        if self.capacity_evidence_kind == CapacityEvidenceKind.UNAVAILABLE and self.available_slots:
            raise ValueError("unavailable routing candidates cannot advertise slots")
        if not self.budget_eligible and (
            "budget_ineligible" not in self.blockers or self.available_slots != 0
        ):
            raise ValueError("budget-ineligible candidates require a blocker and zero slots")
        if self.budget_eligible and "budget_ineligible" in self.blockers:
            raise ValueError("budget blocker conflicts with an eligible candidate")
        if len(self.blockers) != len(set(self.blockers)) or tuple(sorted(self.blockers)) != (
            self.blockers
        ):
            raise ValueError("routing candidate blockers must be unique and sorted")
        return self


class ExecutionRoutingDecisionV1(_StrictContract):
    """Immutable reason and evidence for one selected physical pool."""

    schema_version: Literal["loom.execution-routing-decision.v1"] = (
        "loom.execution-routing-decision.v1"
    )
    generation: int = Field(gt=0)
    requirements_sha256: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    selected_pool_id: str = Field(pattern=r"^[a-z0-9][a-z0-9-]{0,79}$")
    selected_adapter_kind: ExecutionAdapterKind
    selected_target_id: str | None = Field(
        default=None,
        pattern=r"^[a-z0-9][a-z0-9-]{0,79}$",
    )
    selected_execution_class_id: str | None = Field(
        default=None,
        pattern=r"^[a-z0-9][a-z0-9-]{0,79}$",
    )
    reason: ExecutionRoutingReason
    decided_at: datetime
    candidates: tuple[ExecutionRouteCandidateV1, ...] = Field(min_length=1, max_length=64)

    @model_validator(mode="after")
    def _selection_matches_exact_candidate(self) -> ExecutionRoutingDecisionV1:
        keys = [(item.logical_pool_id, item.target_id or "") for item in self.candidates]
        if keys != sorted(keys) or len(keys) != len(set(keys)):
            raise ValueError("routing candidates must have unique canonical pool/target order")
        selected = [
            item
            for item in self.candidates
            if item.logical_pool_id == self.selected_pool_id
            and item.target_id == self.selected_target_id
        ]
        if len(selected) != 1:
            raise ValueError("routing decision must select exactly one candidate")
        candidate = selected[0]
        if (
            candidate.adapter_kind != self.selected_adapter_kind
            or candidate.execution_class_id != self.selected_execution_class_id
        ):
            raise ValueError("selected routing identity drift")
        if self.reason not in {
            ExecutionRoutingReason.PREEXISTING_ASSIGNMENT,
            ExecutionRoutingReason.ADMIN_TARGET_BINDING,
        } and (
            not candidate.enabled
            or not candidate.healthy
            or candidate.draining
            or candidate.blockers
            or candidate.available_slots <= 0
        ):
            raise ValueError("selected candidate is not eligible for the recorded reason")
        if (
            self.reason
            in {
                ExecutionRoutingReason.FRESH_EXECUTABLE_CAPACITY,
                ExecutionRoutingReason.CONFIGURED_SCALE_HEADROOM,
            }
            and candidate.capacity_evidence_kind.value != self.reason.value
        ):
            raise ValueError("selected candidate evidence does not match the routing reason")
        if self.reason == ExecutionRoutingReason.OPERATOR_PIN and (
            candidate.capacity_evidence_kind
            not in {
                CapacityEvidenceKind.FRESH_EXECUTABLE,
                CapacityEvidenceKind.CONFIGURED_SCALE_HEADROOM,
            }
        ):
            raise ValueError("operator pin requires eligible capacity evidence")
        return self


def _task_declares_guest_execution(task: TaskConfig) -> bool:
    declared = task.environment.execution_requirements
    return bool(ALL_GUEST_EXECUTION_CAPABILITIES.intersection(declared.capabilities if declared else ()))


def effective_guest_capabilities(
    task: TaskConfig, trial: TrialConfig | None,
) -> frozenset[GuestExecutionCapability] | None:
    """Guest capabilities of the selected isolation class, or ``None`` for the ordinary class.

    A forced guest without declared capabilities is a plain guest (empty set).
    ``container`` resolves to the ordinary class even when the task declares
    guest capabilities; admission rejects that combination before compile.
    """

    declared = task.environment.execution_requirements
    capabilities = ALL_GUEST_EXECUTION_CAPABILITIES.intersection(declared.capabilities if declared else ())
    isolation = trial.isolation if trial is not None else None
    if isolation == "container":
        return None
    if isolation == "guest" or capabilities:
        return capabilities
    return None


def workload_requirements_from_task(
    task: TaskConfig, trial: TrialConfig | None = None,
) -> WorkloadRequirementsV1:
    """Project a version-1 task into the explicit service workload contract.

    Existing task fields called ``public`` mean unrestricted network access;
    the Nebius service class therefore rejects them until the task is converted
    to gateway-only or an approved allowlist.  Mutable tags and Dockerfile
    builds are likewise represented honestly instead of being auto-repaired.
    """

    env = task.environment
    capabilities = env.execution_requirements.capabilities if env.execution_requirements else ()
    needs_guest_kernel = effective_guest_capabilities(task, trial) is not None
    if env.dockerfile is not None:
        materialization = ImageMaterialization.TASK_DOCKERFILE
        image_ref: str | None = None
    elif env.docker_image is None:
        materialization = ImageMaterialization.UNSPECIFIED
        image_ref = None
    elif _IMMUTABLE_OCI_REF.fullmatch(env.docker_image):
        materialization = ImageMaterialization.IMMUTABLE_OCI
        image_ref = env.docker_image
    else:
        materialization = ImageMaterialization.MUTABLE_OCI
        image_ref = env.docker_image

    effective_policy = resolve_effective_network_policy(
        baseline=env.baseline_network_policy,
        supported=env.network_policies_supported,
        override=trial.baseline_network_policy_override if trial is not None else None,
    )
    policy_kind = effective_policy.kind
    network_access = {
        "no-network": NetworkAccess.NONE,
        "gateway-only": NetworkAccess.GATEWAY_ONLY,
        "allowlist": NetworkAccess.APPROVED_ALLOWLIST,
        "web-allowlist": NetworkAccess.APPROVED_ALLOWLIST,
        "public-web": NetworkAccess.APPROVED_ALLOWLIST,
        "public": NetworkAccess.UNRESTRICTED_PUBLIC,
    }[policy_kind]
    # A later verifier pod exists only for workspace-harness separate grading.
    # Callers that have the trial pass it, because that is what the compiler
    # uses. Task-only callers keep their historical projection (declared agent
    # and env_mode) so stored comparisons that do not know the trial stay stable.
    if trial is None:
        separate = (
            task.agent.name == "terminus-2"
            and task.verifier.env_mode == "separate"
            and not _task_declares_guest_execution(task)
        )
    else:
        # Retained services cannot outlive the agent pod, so the compiler
        # grades them in it with a separate verifier sandbox.
        separate = (
            is_workspace_harness(trial.agent_name)
            and resolve_verifier_env_mode(task, trial) == "separate"
            and not needs_guest_kernel
            and task.environment.service_lifecycle is None
        )
    verifier_topology = (
        VerifierTopology.SEPARATE_EXECUTION if separate else VerifierTopology.IN_ATTEMPT
    )
    return WorkloadRequirementsV1(
        operating_system=env.os,
        cpu_architecture=env.cpu_arch,
        gpu_vendor=env.gpu_vendor,
        gpu_count=env.gpus,
        cpu_millis=round(env.cpus * 1000) if env.cpus is not None else None,
        memory_mib=env.memory_mb,
        ephemeral_storage_mib=env.storage_mb,
        isolation_level=(
            IsolationLevel.DEDICATED_GUEST_KERNEL if needs_guest_kernel
            else IsolationLevel.SHARED_KERNEL
        ),
        network_access=network_access,
        effective_network_policy=effective_policy,
        task_egress=hosted_http_egress(effective_policy),
        image_materialization=materialization,
        image_ref=image_ref,
        sidecar_count=len(env.sidecars),
        verifier_topology=verifier_topology,
        custom_dns=bool(env.dns),
        extra_hosts=bool(env.extra_hosts),
        tmpfs=bool(env.tmpfs),
        privileged=False,
        host_path=False,
        host_network=False,
        nested_containers=False,
        host_devices="dpdk_networking" in capabilities,
        host_specialized=False,
        execution_requirements=env.execution_requirements,
    )


def evaluate_execution_admission(
    requirements: WorkloadRequirementsV1,
    execution_class: ExecutionClassV1,
) -> ExecutionAdmissionV1:
    """Return every material incompatibility; never weaken requirements."""

    reasons: list[CompatibilityReasonV1] = []

    def reject(code: str, message: str) -> None:
        reasons.append(CompatibilityReasonV1(code=code, message=message))

    supported_capabilities = (
        execution_class.guest_execution.supported_capabilities
        if execution_class.guest_execution is not None else frozenset()
    )
    for diagnostic in execution_requirement_diagnostics(
        requirements.execution_requirements, supported_capabilities=supported_capabilities,
    ):
        reject(diagnostic.code, diagnostic.reason)

    if requirements.operating_system != execution_class.operating_system:
        reject("operating_system_unsupported", "execution class only supports Linux")
    if requirements.cpu_architecture not in {"any", execution_class.cpu_architecture}:
        reject(
            "cpu_architecture_unsupported",
            f"execution class provides {execution_class.cpu_architecture}",
        )
    if requirements.gpu_vendor != execution_class.gpu_vendor or requirements.gpu_count:
        reject("gpu_unsupported", "CPU execution class cannot satisfy a GPU workload")
    if requirements.isolation_level != execution_class.isolation_level:
        reject(
            "isolation_level_unsupported",
            "workload isolation requirement does not exactly match the admitted class",
        )
    if requirements.task_egress is not None and not execution_class.supports_task_web_egress:
        reject("task_web_egress_unsupported", "execution class does not support task HTTP(S) egress")
    if requirements.network_access not in execution_class.network_access:
        reject(
            "network_access_unsupported",
            f"network mode {requirements.network_access.value} is not admitted",
        )
    if requirements.image_materialization == ImageMaterialization.TASK_DOCKERFILE:
        if not execution_class.supports_task_image_build:
            reject("task_image_build_unsupported", "runtime Dockerfile builds are not admitted")
    if execution_class.requires_immutable_image:
        if requirements.image_materialization != ImageMaterialization.IMMUTABLE_OCI:
            reject(
                "immutable_image_required", "attempt image must be digest-pinned before admission"
            )
    if requirements.sidecar_count > execution_class.maximum_sidecars:
        reject("sidecar_limit_exceeded", "declared sidecars exceed the execution class limit")
    if (
        requirements.verifier_topology == VerifierTopology.SEPARATE_EXECUTION
        and not execution_class.supports_separate_verifier
    ):
        reject("separate_verifier_unsupported", "separate verifier execution is required")

    resource_pairs = (
        ("cpu", requirements.cpu_millis, execution_class.maximum_cpu_millis),
        ("memory", requirements.memory_mib, execution_class.maximum_memory_mib),
        (
            "ephemeral_storage",
            requirements.ephemeral_storage_mib,
            execution_class.maximum_ephemeral_storage_mib,
        ),
    )
    for name, requested, maximum in resource_pairs:
        if requested is None:
            reject(f"{name}_limit_required", f"{name} must have an explicit positive limit")
        elif maximum is not None and requested > maximum:
            reject(f"{name}_limit_exceeded", f"{name} exceeds the execution class maximum")

    if execution_class.guest_execution is not None:
        for name, requested, minimum, field, unit in (
            ("cpu", requirements.cpu_millis, 1000, "cpus", "CPU millis"),
            ("memory", requirements.memory_mib, 512, "memory_mb", "MiB"),
            (
                "ephemeral_storage", requirements.ephemeral_storage_mib, 160,
                "storage_mb", "MiB",
            ),
        ):
            if requested is not None and requested < minimum:
                reject(
                    f"guest_{name}_limit_too_small",
                    f"guest execution requires at least {minimum} {unit}; "
                    f"increase environment.{field} before submitting this workload",
                )

    feature_pairs = (
        (requirements.custom_dns, execution_class.supports_custom_dns, "custom_dns"),
        (requirements.extra_hosts, execution_class.supports_extra_hosts, "extra_hosts"),
        (requirements.tmpfs, execution_class.supports_tmpfs, "tmpfs"),
        (requirements.privileged, execution_class.permits_privileged, "privileged"),
        (requirements.host_path, execution_class.permits_host_path, "host_path"),
        (requirements.host_network, execution_class.permits_host_network, "host_network"),
        (
            requirements.nested_containers,
            execution_class.permits_nested_containers,
            "nested_containers",
        ),
        (requirements.host_devices, execution_class.permits_host_devices, "host_devices"),
    )
    for required, supported, name in feature_pairs:
        if required and not supported:
            reject(f"{name}_unsupported", f"{name} is forbidden by the execution class")
    if requirements.host_specialized:
        reject("host_specialized_unsupported", "host-specialized workloads are not portable Pods")

    return ExecutionAdmissionV1(
        execution_class_id=execution_class.class_id,
        compatible=not reasons,
        reasons=tuple(reasons),
    )


NEBIUS_CPU_EXECUTION_CLASS_V1 = ExecutionClassV1(
    class_id="linux-amd64-cpu-pod-v1",
    operating_system="linux",
    cpu_architecture="x86_64",
    gpu_vendor="none",
    isolation_level=IsolationLevel.SHARED_KERNEL,
    network_access=frozenset(
        {
            NetworkAccess.NONE,
            NetworkAccess.GATEWAY_ONLY,
            NetworkAccess.APPROVED_ALLOWLIST,
        }
    ),
    maximum_sidecars=8,
    supports_separate_verifier=True,
    supports_custom_dns=False,
    supports_extra_hosts=False,
    supports_tmpfs=True,
    supports_task_image_build=False,
    requires_immutable_image=True,
    permits_privileged=False,
    permits_host_path=False,
    permits_host_network=False,
    permits_nested_containers=False,
    permits_host_devices=False,
)

# Execution classes and target bindings are immutable catalog identities.
# A newly qualified capability must never change the already installed CPU V1.
NEBIUS_CPU_WEB_EXECUTION_CLASS_V1 = NEBIUS_CPU_EXECUTION_CLASS_V1.model_copy(update={
    "class_id": "linux-amd64-cpu-web-pod-v1",
    "supports_task_web_egress": True,
})

NEBIUS_CPU_GUEST_EXECUTION_CLASS_V1 = ExecutionClassV1.model_validate({
    **NEBIUS_CPU_EXECUTION_CLASS_V1.model_dump(),
    "class_id": "linux-amd64-cpu-guest-v1",
    "isolation_level": IsolationLevel.DEDICATED_GUEST_KERNEL,
    "guest_execution": GuestExecutionClassV1(),
})
NEBIUS_CPU_GUEST_WEB_EXECUTION_CLASS_V1 = ExecutionClassV1.model_validate({
    **NEBIUS_CPU_GUEST_EXECUTION_CLASS_V1.model_dump(),
    "class_id": "linux-amd64-cpu-guest-web-v1",
    "supports_task_web_egress": True,
})
NEBIUS_CPU_GUEST_AUTH_EXECUTION_CLASS_V1 = ExecutionClassV1.model_validate({
    **NEBIUS_CPU_GUEST_EXECUTION_CLASS_V1.model_dump(),
    "class_id": "linux-amd64-cpu-guest-auth-v1",
    "guest_execution": GuestExecutionClassV1(supported_capabilities=ALL_GUEST_EXECUTION_CAPABILITIES),
})
NEBIUS_CPU_GUEST_AUTH_WEB_EXECUTION_CLASS_V1 = ExecutionClassV1.model_validate({
    **NEBIUS_CPU_GUEST_AUTH_EXECUTION_CLASS_V1.model_dump(),
    "class_id": "linux-amd64-cpu-guest-auth-web-v1",
    "supports_task_web_egress": True,
})


def nebius_cpu_execution_class(*, supports_task_web_egress: bool = False) -> ExecutionClassV1:
    """Select an immutable CPU catalog identity from explicit runtime readiness."""
    return (
        NEBIUS_CPU_WEB_EXECUTION_CLASS_V1 if supports_task_web_egress
        else NEBIUS_CPU_EXECUTION_CLASS_V1
    )


def nebius_guest_execution_class(
    *, supports_task_web_egress: bool = False, supports_emulated_pkcs11: bool = False,
) -> ExecutionClassV1:
    """Select guest-local contract support; deployment readiness is checked separately."""
    if supports_emulated_pkcs11:
        return (NEBIUS_CPU_GUEST_AUTH_WEB_EXECUTION_CLASS_V1 if supports_task_web_egress
                else NEBIUS_CPU_GUEST_AUTH_EXECUTION_CLASS_V1)
    return (
        NEBIUS_CPU_GUEST_WEB_EXECUTION_CLASS_V1 if supports_task_web_egress
        else NEBIUS_CPU_GUEST_EXECUTION_CLASS_V1
    )


def nebius_guest_class_by_id(class_id: str) -> ExecutionClassV1 | None:
    """Resolve only registered guest catalog definitions, without inferring readiness."""
    return next((item for item in (
        NEBIUS_CPU_GUEST_EXECUTION_CLASS_V1, NEBIUS_CPU_GUEST_WEB_EXECUTION_CLASS_V1,
        NEBIUS_CPU_GUEST_AUTH_EXECUTION_CLASS_V1, NEBIUS_CPU_GUEST_AUTH_WEB_EXECUTION_CLASS_V1,
    ) if item.class_id == class_id), None)
