"""Versioned per-execution resource accounting contracts (#1503)."""

from __future__ import annotations

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

Completeness = Literal["complete", "partial", "unavailable"]
ContainerRole = Literal["agent", "verifier", "sidecar", "controller", "task", "pod"]


class ResourceCounters(BaseModel):
    """Monotonic counters and observed peaks from one execution container."""

    model_config = ConfigDict(extra="forbid")

    cpu_sampled_max_nanocores: int | None = Field(default=None, ge=0)
    memory_sampled_max_bytes: int | None = Field(default=None, ge=0)
    filesystem_sampled_max_bytes: int | None = Field(default=None, ge=0)
    ephemeral_storage_sampled_max_bytes: int | None = Field(default=None, ge=0)
    cpu_usage_usec: int | None = Field(default=None, ge=0)
    cpu_user_usec: int | None = Field(default=None, ge=0)
    cpu_system_usec: int | None = Field(default=None, ge=0)
    cpu_throttled_usec: int | None = Field(default=None, ge=0)
    cpu_periods: int | None = Field(default=None, ge=0)
    cpu_throttled_periods: int | None = Field(default=None, ge=0)
    memory_current_bytes: int | None = Field(default=None, ge=0)
    memory_peak_bytes: int | None = Field(default=None, ge=0)
    memory_events_low: int | None = Field(default=None, ge=0)
    memory_events_high: int | None = Field(default=None, ge=0)
    memory_events_max: int | None = Field(default=None, ge=0)
    memory_events_oom: int | None = Field(default=None, ge=0)
    memory_events_oom_kill: int | None = Field(default=None, ge=0)
    pids_current: int | None = Field(default=None, ge=0)
    pids_peak: int | None = Field(default=None, ge=0)
    io_read_bytes: int | None = Field(default=None, ge=0)
    io_write_bytes: int | None = Field(default=None, ge=0)
    io_read_ops: int | None = Field(default=None, ge=0)
    io_write_ops: int | None = Field(default=None, ge=0)


class ResourceLimits(BaseModel):
    model_config = ConfigDict(extra="forbid")

    cpu_cores: float | None = Field(default=None, gt=0)
    memory_bytes: int | None = Field(default=None, gt=0)
    pids: int | None = Field(default=None, gt=0)
    resource_profile: str | None = Field(default=None, max_length=120)


class TrialResourceUsageReport(BaseModel):
    """Idempotent worker-to-control-plane accounting report."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1] = 1
    trial_id: UUID
    attempt_count: int = Field(gt=0)
    worker_id: UUID | None = None
    execution_lease_id: UUID | None = None
    resource_generation: int | None = Field(default=None, gt=0)
    target_id: str | None = None
    pod_uid: str | None = Field(default=None, min_length=1, max_length=128)
    execution_key: str = Field(pattern=r"^[0-9a-f]{64}$")
    runtime_id_hash: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    container_role: ContainerRole
    role_name: str = Field(min_length=1, max_length=120, pattern=r"^[A-Za-z0-9_.:-]+$")
    backend: str = Field(min_length=1, max_length=40, pattern=r"^[a-z0-9_.-]+$")
    architecture: str | None = Field(default=None, max_length=40)
    candidate_sha: str | None = Field(default=None, pattern=r"^[0-9a-f]{40}$")
    image_digest: str | None = Field(
        default=None,
        pattern=r"^(?:sha256:)?[0-9a-f]{64}$",
    )
    source: Literal["docker_stats", "provider", "unsupported", "kubelet_summary", "native_node_observer"]
    observation_seq: int = Field(ge=0)
    container_started_at: datetime | None = None
    first_observed_at: datetime
    last_observed_at: datetime
    finalized_at: datetime | None = None
    terminal_reason: str | None = Field(default=None, max_length=80)
    completeness: Completeness
    diagnostic_code: str | None = Field(
        default=None,
        max_length=80,
        pattern=r"^[a-z0-9_.:-]+$",
    )
    limits: ResourceLimits = Field(default_factory=ResourceLimits)
    counters: ResourceCounters = Field(default_factory=ResourceCounters)

    @field_validator("last_observed_at")
    @classmethod
    def _last_not_before_first(cls, value: datetime, info: object) -> datetime:
        data = getattr(info, "data", {})
        first = data.get("first_observed_at")
        if isinstance(first, datetime) and value < first:
            raise ValueError("last_observed_at precedes first_observed_at")
        return value

    @field_validator(
        "container_started_at",
        "first_observed_at",
        "last_observed_at",
        "finalized_at",
    )
    @classmethod
    def _timestamps_are_timezone_aware(cls, value: datetime | None) -> datetime | None:
        if value is not None and value.utcoffset() is None:
            raise ValueError("resource usage timestamps must be timezone-aware")
        return value

    @model_validator(mode="after")
    def _finalization_is_consistent(self) -> TrialResourceUsageReport:
        native = (self.execution_lease_id, self.resource_generation, self.target_id, self.pod_uid)
        if self.worker_id is None:
            if not all(value is not None for value in native):
                raise ValueError(
                    "native resource usage requires lease, generation, target and Pod UID"
                )
        elif any(value is not None for value in native):
            raise ValueError("worker resource usage cannot carry native identity")
        if self.finalized_at is None and self.completeness != "partial":
            raise ValueError("unfinished resource usage must be partial")
        if self.finalized_at is not None and self.finalized_at < self.last_observed_at:
            raise ValueError("finalized_at precedes last_observed_at")
        return self


def aggregate_resource_usage(
    reports: list[TrialResourceUsageReport],
) -> dict[str, object]:
    """Return an explicit, conservative multi-container projection.

    Cumulative counters are summed across distinct incarnations. Overlapping
    native sources for one known incarnation contribute their field maximum,
    preserving original provenance without double counting. Per-container peaks
    remain an upper bound, not a synchronized whole-trial peak.
    """

    def total(name: str) -> int | None:
        values: dict[tuple[object, ...], int] = {}
        for report in reports:
            if (report.container_role == "pod") != (name == "ephemeral_storage_sampled_max_bytes"):
                continue
            value = getattr(report.counters, name)
            if value is None:
                continue
            if report.execution_lease_id is not None and (
                report.container_started_at is not None or report.container_role == "pod"
            ):
                # Two sources may observe the same physical incarnation. Keep
                # both provenance records, but never add their cumulative usage.
                key: tuple[object, ...] = (
                    "native", report.trial_id, report.attempt_count,
                    report.execution_lease_id, report.resource_generation,
                    report.target_id, report.pod_uid, report.container_role,
                    report.role_name, report.container_started_at,
                )
            else:
                # Missing incarnation identity cannot prove two rows overlap.
                key = ("record", report.trial_id, report.attempt_count, report.execution_key)
            values[key] = max(values.get(key, 0), int(value))
        return sum(values.values()) if values else None

    complete = sum(report.completeness == "complete" for report in reports)
    partial = sum(report.completeness == "partial" for report in reports)
    unavailable = sum(report.completeness == "unavailable" for report in reports)
    return {
        "schema_version": 1,
        "records": len(reports),
        "complete_records": complete,
        "partial_records": partial,
        "unavailable_records": unavailable,
        "telemetry_status": (
            "unavailable"
            if not reports or unavailable == len(reports)
            else "complete"
            if complete == len(reports)
            else "partial"
        ),
        "cpu_usage_usec": total("cpu_usage_usec"),
        "cpu_user_usec": total("cpu_user_usec"),
        "cpu_system_usec": total("cpu_system_usec"),
        "cpu_throttled_usec": total("cpu_throttled_usec"),
        "cpu_periods": total("cpu_periods"),
        "cpu_throttled_periods": total("cpu_throttled_periods"),
        "memory_sampled_max_sum_bytes": total("memory_sampled_max_bytes"),
        "pod_ephemeral_storage_sampled_max_sum_bytes": total("ephemeral_storage_sampled_max_bytes"),
        "memory_peak_upper_bound_bytes": total("memory_peak_bytes"),
        "pids_peak_upper_bound": total("pids_peak"),
        "io_read_bytes": total("io_read_bytes"),
        "io_write_bytes": total("io_write_bytes"),
        "io_read_ops": total("io_read_ops"),
        "io_write_ops": total("io_write_ops"),
        "oom_events": total("memory_events_oom"),
        "oom_kill_events": total("memory_events_oom_kill"),
    }
