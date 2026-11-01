"""Strict contracts for the Nebius/Kubernetes capacity collector."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    SerializerFunctionWrapHandler,
    field_validator,
    model_serializer,
    model_validator,
)


class ResourceTotals(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    cpu_millis: int = Field(ge=0)
    memory_mib: int = Field(ge=0)
    storage_mib: int = Field(ge=0)


class QuotaResource(BaseModel):
    """The provider identity and normalized limit/usage for one resource."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    parent_id: str = Field(min_length=1)
    region: str = Field(min_length=1)
    service: str = Field(min_length=1)
    name: str = Field(min_length=1)
    # Provider unit is part of identity. Numeric values use ResourceTotals units
    # (milli-vCPU, MiB), or integer nodes, matching existing observation columns.
    unit: str = Field(min_length=1)
    limit: int = Field(ge=0)
    used: int = Field(ge=0)


class PodPendingCounts(BaseModel):
    """This Pod's exact contribution to the collector's admission counters."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)
    pending: bool
    unschedulable: bool
    image_pull_backoff: bool

    @model_validator(mode="after")
    def _pending_subcategories(self) -> PodPendingCounts:
        if (self.unschedulable or self.image_pull_backoff) and not self.pending:
            raise ValueError("pending subcategories require a pending Pod")
        return self


class ManagedPodPlacement(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    uid: str = Field(min_length=1)
    lease_id: str = Field(min_length=1)
    generation: int = Field(gt=0)
    requests: ResourceTotals
    # Omission preserves the exact canonical payload of historical observations.
    pending_counts: PodPendingCounts | None = None

    @model_serializer(mode="wrap")
    def _omit_historical_pending_counts(self, handler: SerializerFunctionWrapHandler) -> dict[str, Any]:
        value: dict[str, Any] = handler(self)
        if self.pending_counts is None:
            value.pop("pending_counts", None)
        return value


class NodeIOPressure(BaseModel):
    """Measured node stall pressure; absent telemetry is never numeric zero."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    sampled_at: AwareDatetime
    full_avg10: float = Field(ge=0, le=100, allow_inf_nan=False)

    @field_validator("sampled_at", mode="before")
    @classmethod
    def _stored_time(cls, value: Any) -> Any:
        # Placement is persisted as JSONB and revalidated in Python mode. Keep
        # strict aware-datetime validation after decoding its ISO wire format.
        return datetime.fromisoformat(value) if isinstance(value, str) else value


class NodeIOWindowPressure(BaseModel):
    """Full host stall percentage over two real provider counter timestamps.

    This rectangular window is not the kernel's exponentially weighted avg10.
    Both endpoints are retained so admission can require a complete post-start
    feedback window rather than diluting a new build with earlier idle time.
    """

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    source: Literal["nebius_io_stalled_counter"] = "nebius_io_stalled_counter"
    window_started_at: AwareDatetime
    sampled_at: AwareDatetime
    full_window_percent: float = Field(ge=0, le=100, allow_inf_nan=False)

    @field_validator("window_started_at", "sampled_at", mode="before")
    @classmethod
    def _stored_time(cls, value: Any) -> Any:
        return datetime.fromisoformat(value) if isinstance(value, str) else value

    @model_validator(mode="after")
    def _bounded_window(self) -> NodeIOWindowPressure:
        if not 10 <= (self.sampled_at - self.window_started_at).total_seconds() <= 60:
            raise ValueError("node I/O pressure needs a real ten-to-sixty-second window")
        return self


class NodePlacement(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    uid: str = Field(min_length=1)
    name: str | None = Field(
        default=None, min_length=1, max_length=253, exclude_if=lambda value: value is None,
    )
    io_pressure: NodeIOPressure | None = Field(
        default=None, exclude_if=lambda value: value is None,
    )
    io_stall_window: NodeIOWindowPressure | None = Field(
        default=None, exclude_if=lambda value: value is None,
    )
    created_at: AwareDatetime | None = Field(
        default=None, exclude_if=lambda value: value is None,
    )
    provider_id: str = Field(min_length=1)
    ready: bool
    unschedulable: bool
    deleting: bool
    # None means the observation predates explicit drain detection.
    draining: bool | None = None
    allocatable: ResourceTotals
    requested: ResourceTotals
    pod_slots: int = Field(gt=0)
    used_pod_slots: int = Field(ge=0)
    managed_pods: list[ManagedPodPlacement]

    @field_validator("created_at", mode="before")
    @classmethod
    def _stored_creation_time(cls, value: Any) -> Any:
        return datetime.fromisoformat(value) if isinstance(value, str) else value


class DaemonSetPlacement(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    uid: str = Field(min_length=1)
    generation: int = Field(gt=0)
    requests: ResourceTotals
    scheduling: dict[str, Any]


class NodeTemplateSample(BaseModel):
    """Observed facts only; CP decides whether historical samples remain usable."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    node_uid: str = Field(min_length=1)
    allocatable: ResourceTotals
    pod_slots: int = Field(gt=0)
    daemonset_requests: ResourceTotals
    daemonset_slots: int = Field(ge=0)
    kubelet_version: str = Field(min_length=1)
    # Only matching, fully observed controller revisions can back a cold sample.
    daemonsets: dict[str, int]


class NodeGroupPlacement(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    id: str = Field(min_length=1)
    max_nodes: int = Field(ge=0)
    node_count: int = Field(ge=0)
    template: dict[str, Any]
    raw_node: ResourceTotals


class CapacityTargetScopeV1(BaseModel):
    """Authoritative catalog membership captured with one physical inventory."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    schema_version: Literal["loom.execution-capacity-target-scope.v1"] = "loom.execution-capacity-target-scope.v1"
    owner_target_id: str = Field(pattern=r"^[a-z0-9][a-z0-9-]{0,79}$")
    namespace_name: str = Field(pattern=r"^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$")
    target_ids: list[str] = Field(min_length=2, max_length=64)

    @model_validator(mode="after")
    def _exact_membership(self) -> CapacityTargetScopeV1:
        import re

        if (self.target_ids != sorted(set(self.target_ids)) or self.owner_target_id not in self.target_ids
                or any(re.fullmatch(r"[a-z0-9][a-z0-9-]{0,79}", item) is None for item in self.target_ids)):
            raise ValueError("capacity scope requires sorted unique target membership including its owner")
        return self


class CapacityPlacement(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    # Absent in historical observations or when no native builder is configured.
    build_concurrency_limit: int | None = Field(default=None, ge=1)
    target_scope: CapacityTargetScopeV1 | None = Field(default=None, exclude_if=lambda value: value is None)
    quota_resources: dict[Literal["nodes", "vcpu", "memory", "storage"], QuotaResource]
    node_group: NodeGroupPlacement
    nodes: list[NodePlacement]
    pending_pods: list[ManagedPodPlacement]
    daemonsets: list[DaemonSetPlacement]
    template_samples: list[NodeTemplateSample]


class NodeStateCounts(BaseModel):
    """Provider/Kubernetes node lifecycle counts safe for operator projection."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    desired: int = Field(ge=0)
    creating: int = Field(ge=0)
    ready: int = Field(ge=0)
    failed: int = Field(ge=0)
    deleting: int = Field(ge=0)


class CapacityPolicyBinding(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    target_id: str = Field(min_length=1, max_length=120)
    target_scope: CapacityTargetScopeV1 | None = Field(default=None, exclude_if=lambda value: value is None)
    pool_id: str = Field(min_length=1, max_length=120)
    enabled: bool
    max_nodes: int = Field(gt=0)
    node_cpu_millis: int = Field(gt=0)
    node_memory_mib: int = Field(gt=0)
    node_storage_mib: int = Field(gt=0)
    version: int = Field(gt=0)

    @model_validator(mode="after")
    def _scope_owner(self) -> CapacityPolicyBinding:
        if self.target_scope is not None and self.target_scope.owner_target_id != self.target_id:
            raise ValueError("capacity collector policy must bind the owner target")
        return self


class ProviderCapacitySnapshot(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    source_versions: dict[str, str]
    provider_capacity_state: Literal["available", "insufficient", "unknown"]
    provider_capacity_reason: str | None = Field(default=None, max_length=500)
    autoscaler_state: Literal["ready", "scaling", "stalled", "unknown"]
    autoscaler_reason: str | None = Field(default=None, max_length=500)
    quota_nodes: int = Field(ge=0)
    quota_vcpu_millis: int = Field(ge=0)
    quota_memory_mib: int = Field(ge=0)
    quota_storage_mib: int = Field(ge=0)
    used_nodes: int = Field(ge=0)
    used_vcpu_millis: int = Field(ge=0)
    used_memory_mib: int = Field(ge=0)
    used_storage_mib: int = Field(ge=0)
    node_count: int = Field(ge=0)
    target_node_count: int = Field(ge=0)
    ready_node_count: int = Field(ge=0)
    quota_resources: dict[Literal["nodes", "vcpu", "memory", "storage"], QuotaResource] = Field(
        default_factory=dict
    )
    node_group: NodeGroupPlacement | None = None


class KubernetesCapacitySnapshot(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    source_versions: dict[str, str]
    active_nodes: int = Field(ge=0)
    ready_nodes: int = Field(ge=0)
    provisioned: ResourceTotals
    allocatable: ResourceTotals
    requested: ResourceTotals
    pending_jobs: int = Field(ge=0)
    unschedulable_jobs: int = Field(ge=0)
    image_pull_backoff_jobs: int = Field(ge=0)
    pending_reasons: dict[str, int]
    nodes: list[NodePlacement] = Field(default_factory=list)
    pending_pods: list[ManagedPodPlacement] = Field(default_factory=list)
    daemonsets: list[DaemonSetPlacement] = Field(default_factory=list)
    template_samples: list[NodeTemplateSample] = Field(default_factory=list)
    # Collector-internal readback, not copied into placement/observation JSON.
    node_templates: dict[str, dict[str, Any]] = Field(default_factory=dict)


class CapacityObservationV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    target_id: str = Field(min_length=1, max_length=120)
    source: str = Field(min_length=1, max_length=120)
    source_version: str = Field(min_length=1, max_length=160)
    observed_at: datetime
    provider_capacity_state: Literal["available", "insufficient", "unknown"]
    provider_capacity_reason: str | None = Field(default=None, max_length=500)
    autoscaler_state: Literal["ready", "scaling", "stalled", "unknown"]
    autoscaler_reason: str | None = Field(default=None, max_length=500)
    provider_quota_nodes: int = Field(ge=0)
    provider_quota_vcpu_millis: int = Field(ge=0)
    provider_quota_memory_mib: int = Field(ge=0)
    provider_quota_storage_mib: int = Field(ge=0)
    provider_used_nodes: int = Field(ge=0)
    provider_used_vcpu_millis: int = Field(ge=0)
    provider_used_memory_mib: int = Field(ge=0)
    provider_used_storage_mib: int = Field(ge=0)
    active_nodes: int = Field(ge=0)
    node_states: NodeStateCounts
    provisioned_vcpu_millis: int = Field(ge=0)
    provisioned_memory_mib: int = Field(ge=0)
    provisioned_storage_mib: int = Field(ge=0)
    allocatable_cpu_millis: int = Field(ge=0)
    allocatable_memory_mib: int = Field(ge=0)
    allocatable_storage_mib: int = Field(ge=0)
    requested_cpu_millis: int = Field(ge=0)
    requested_memory_mib: int = Field(ge=0)
    requested_storage_mib: int = Field(ge=0)
    pending_jobs: int = Field(ge=0)
    unschedulable_jobs: int = Field(ge=0)
    image_pull_backoff_jobs: int = Field(ge=0)
    pending_reasons: dict[str, int]
    placement: CapacityPlacement | None = None


class CapacityObservationReceipt(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    id: str
    created: bool
    target_id: str
    source: str
    source_version: str
    observed_at: datetime
    provider_capacity_state: Literal["available", "insufficient", "unknown"]
    autoscaler_state: Literal["ready", "scaling", "stalled", "unknown"]
    observation_sha256: str


__all__ = [
    "CapacityObservationReceipt",
    "CapacityObservationV1",
    "CapacityPolicyBinding",
    "KubernetesCapacitySnapshot",
    "NodeIOPressure",
    "NodeStateCounts",
    "ProviderCapacitySnapshot",
    "ResourceTotals",
]
