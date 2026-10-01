"""Environment configuration for one read-only capacity collection pass."""

from pathlib import Path
from typing import Literal
from uuid import UUID

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from loom.nebius_kubernetes import NebiusKubernetesConnection, connection_from_fields


class NebiusCapacitySourceSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="LOOM_EXECUTION_CAPACITY_COLLECTOR_",
        extra="ignore",
    )

    kubernetes_endpoint: str | None = None
    kubernetes_ca_file: Path | None = None
    kubernetes_nebius_credentials_file: Path | None = None

    @model_validator(mode="after")
    def _remote_kubernetes_complete(self) -> "NebiusCapacitySourceSettings":
        _ = self.kubernetes_connection
        return self

    @property
    def kubernetes_connection(self) -> NebiusKubernetesConnection | None:
        return connection_from_fields(
            self.kubernetes_endpoint,
            self.kubernetes_ca_file,
            self.kubernetes_nebius_credentials_file,
        )

    nebius_project_id: str = Field(min_length=1, max_length=160)
    nebius_quota_parent_id: str | None = Field(default=None, min_length=1, max_length=160)
    nebius_node_group_id: str = Field(min_length=1, max_length=160)
    nebius_region: str = Field(min_length=1, max_length=80)
    nebius_credentials_file: Path
    quota_nodes_name: str = Field(min_length=1, max_length=255)
    quota_vcpu_name: str = Field(min_length=1, max_length=255)
    quota_memory_name: str | None = Field(default=None, min_length=1, max_length=255)
    quota_storage_name: str = Field(min_length=1, max_length=255)
    quota_nodes_unit: str = Field(min_length=1, max_length=40)
    quota_vcpu_unit: str = Field(min_length=1, max_length=40)
    quota_memory_unit: str | None = Field(default=None, min_length=1, max_length=40)
    quota_storage_unit: str = Field(min_length=1, max_length=40)
    quota_service: str = Field(default="compute", min_length=1, max_length=120)
    request_timeout_seconds: float = Field(default=15.0, ge=1.0, le=60.0)

    @model_validator(mode="after")
    def _optional_memory_quota_is_complete(self) -> "NebiusCapacitySourceSettings":
        if (self.quota_memory_name is None) != (self.quota_memory_unit is None):
            raise ValueError("memory quota name and unit must be configured together")
        return self


class ExecutionCapacityCollectorSettings(NebiusCapacitySourceSettings):
    """Retained single-target mode; not fallback authority for a pool collector."""

    target_id: str = Field(min_length=1, max_length=120)
    pool_id: str = Field(min_length=1, max_length=120)
    build_concurrency_limit: int | None = Field(default=None, ge=1)
    namespace: str = Field(pattern=r"^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$")
    node_label_selector: str = Field(min_length=1, max_length=500)
    control_plane_url: str = Field(pattern=r"^https?://")
    control_plane_bearer_token_file: Path
    source: str = Field(default="nebius-kubernetes-capacity-collector", min_length=1, max_length=120)
    request_attempts: int = Field(default=3, ge=1, le=5)


class PoolCapacityCollectorSettings(NebiusCapacitySourceSettings):
    """Protected pool identity and separate management observer credential."""

    pool_id: UUID
    management_url: str = Field(pattern=r"^https://")
    management_bearer_token_file: Path

    @model_validator(mode="after")
    def non_nil_pool(self) -> "PoolCapacityCollectorSettings":
        if not self.pool_id.int:
            raise ValueError("nil pool identity")
        return self


class CapacityCollectorModeSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="LOOM_EXECUTION_CAPACITY_COLLECTOR_", extra="ignore")

    collection_mode: Literal["target", "pool"] = "target"


__all__ = ["CapacityCollectorModeSettings", "ExecutionCapacityCollectorSettings", "PoolCapacityCollectorSettings"]
