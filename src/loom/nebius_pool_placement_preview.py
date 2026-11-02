"""Scoped, read-only incoming fit; these documents confer no authority."""
from __future__ import annotations

from typing import Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

from loom.nebius_pool_allocation import PoolNodeAllocationV1
from loom.nebius_pool_workload import PoolExecutionPrepareV1
from loom_execution_capacity_collector.contracts import ResourceTotals


class PoolExecutionPreviewRequestV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    schema_version: Literal["loom.pool-execution-preview-request.v1"] = "loom.pool-execution-preview-request.v1"
    request: PoolExecutionPrepareV1
    allocate_resources: bool = Field(default=False, strict=True)
    routing_generation: int = Field(default=0, ge=0, strict=True)

    @property
    def pool_id(self) -> UUID:
        return self.request.pool_id


class PoolExecutionPreviewV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    schema_version: Literal["loom.pool-execution-preview.v1"] = "loom.pool-execution-preview.v1"
    pool_id: UUID
    participant_id: UUID
    target_id: str
    admission_epoch: int = Field(gt=0, strict=True)
    participant_revision: int = Field(gt=0, strict=True)
    execution_namespace_uid: UUID
    observation_id: UUID
    observed_at: AwareDatetime
    valid_until: AwareDatetime
    node_allocation: PoolNodeAllocationV1 | None = None
    effective_requests: ResourceTotals
    request_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    prefer_incoming_warm_fit: bool = Field(strict=True)
    policy_source: Literal["management-db"] = "management-db"
    eligible: bool = Field(strict=True)
    warm_node_uids: tuple[str, ...] = ()
    reason: str = Field(pattern=r"^[a-z0-9_]{1,100}$")
