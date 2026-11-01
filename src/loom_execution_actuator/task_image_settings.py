"""Native builder configuration shared by offline rendering and the actuator.

Keep service/database imports out of this module: deployment preflight only
needs to validate settings, not instantiate a running controller.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    SerializerFunctionWrapHandler,
    model_serializer,
    model_validator,
)

if TYPE_CHECKING:
    from loom_execution_actuator.task_image_renderer import TaskImageJobConfig


class NativeTaskImageSettings(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    namespace: str = Field(pattern=r"^[a-z0-9][a-z0-9-]{0,61}[a-z0-9]$")
    pool_id: str = "nebius-cpu"
    service_image: str
    source_secret_name: str = "loom-task-build-source"
    cache_secret_name: str | None = None
    archive_secret_name: str | None = Field(default=None, exclude_if=lambda value: value is None)
    registry_secret_name: str = "loom-task-build-registry"
    registry_auth_kind: Literal["docker-config", "nebius"] = "nebius"
    storage_endpoint: str
    storage_region: str
    source_bucket: str
    cache_bucket: str | None = None
    registry_repository: str
    cpu_millis: int = Field(default=4000, ge=100, le=64000)
    memory_mib: int = Field(default=4096, ge=512, le=262144)
    ephemeral_storage_mib: int = Field(default=32768, ge=16384, le=524288)
    # Omission preserves the historical Guaranteed envelope. Memory and scratch
    # retain requests equal to limits; CPU sharing is an explicit operator choice.
    cpu_request_millis: int | None = Field(default=None, ge=100, strict=True, exclude_if=lambda value: value is None)
    max_processes: int = Field(default=512, ge=64, le=4096)
    active_deadline_seconds: int = Field(default=1800, ge=60, le=7200)
    # Optional operator guard; physical resource/quota admission always applies.
    max_concurrent: int | None = Field(default=None, ge=1, strict=True)
    # Preserve filesystem semantics for unchanged authored Dockerfiles. The
    # faster OverlayFS backend is opt-in after qualifying the task workload.
    snapshotter: Literal["overlayfs", "native"] = "native"
    # A pressure policy, not a build-count limit. Qualify for the installed
    # node/storage shape; actual resource and shared-cloud quota admission apply.
    io_full_avg10_max: float = Field(default=50.0, ge=0, le=100, allow_inf_nan=False, strict=True)
    # When "same_task", prepare may import BuildKit cache from a prior ready
    # revision of the same task_id+cpu_arch. Results still use this mat key.
    compatible_revision_cache: Literal["off", "same_task"] = "off"
    # BuildKit local export mode; keep max until Nebius timing compares say otherwise.
    export_cache_mode: Literal["max", "min"] = "max"
    # Incremental content-addressed blobs (default) or whole-archive tar rollback.
    cache_transfer: Literal["tar", "blobs"] = "blobs"
    # BuildKit OCI export shape; archive is today's path, directory is measure-gated.
    oci_export_format: Literal["archive", "directory"] = "archive"

    @model_validator(mode="after")
    def bounded_requests(self) -> NativeTaskImageSettings:
        if self.cpu_request_millis is not None and self.cpu_request_millis > self.cpu_millis:
            raise ValueError("native build request exceeds its hard limit")
        return self

    @model_serializer(mode="wrap")
    def serialize_settings(self, handler: SerializerFunctionWrapHandler) -> dict[str, Any]:
        settings: dict[str, Any] = handler(self)
        # Old pool contracts predate this field. Keep their omission while the
        # actuator still uses the runtime default; explicit policies stay frozen.
        if "io_full_avg10_max" not in self.model_fields_set:
            settings.pop("io_full_avg10_max", None)
        return settings

    def job_config(self) -> TaskImageJobConfig:
        from loom_execution_actuator.task_image_renderer import TaskImageJobConfig

        return TaskImageJobConfig(**{key: getattr(self, key) for key in (
            "service_image", "source_secret_name", "cache_secret_name", "archive_secret_name", "registry_secret_name", "registry_auth_kind",
            "cpu_millis", "memory_mib", "ephemeral_storage_mib", "max_processes", "active_deadline_seconds",
            "cpu_request_millis",
            "snapshotter", "export_cache_mode", "oci_export_format",
        )})

    def runtime_configuration(self) -> dict[str, Any]:
        return {key: getattr(self, key) for key in (
            "storage_endpoint", "storage_region", "source_bucket", "cache_bucket", "registry_repository",
            "cache_transfer", "oci_export_format",
        )}
