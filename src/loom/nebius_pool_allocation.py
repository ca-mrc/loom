"""Scoped node-share sizing evidence, not a capacity reservation or Job permit."""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

from loom.execution_runtime_contract import ContainerResourcesV1

_Generation = Annotated[int, Field(gt=0, le=2**63 - 1, strict=True)]


class PoolNodeAllocationRequestV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    pool_id: UUID
    participant_id: UUID
    admission_epoch: _Generation
    participant_revision: _Generation
    target_id: str = Field(pattern=r"^[a-z0-9][a-z0-9-]{0,79}$")
    workload_kind: Literal["trial", "verifier"] = "trial"

    @model_validator(mode="after")
    def identity(self) -> PoolNodeAllocationRequestV1:
        if not self.pool_id.int or not self.participant_id.int:
            raise ValueError("invalid_pool_allocation_scope")
        return self


class PoolNodeAllocationV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["loom.pool-node-allocation.v1"] = "loom.pool-node-allocation.v1"
    scope: PoolNodeAllocationRequestV1
    observation_id: UUID
    observed_at: datetime
    valid_until: datetime
    usable_node: ContainerResourcesV1

    @model_validator(mode="after")
    def evidence(self) -> PoolNodeAllocationV1:
        if (not self.observation_id.int or self.observed_at.utcoffset() is None
                or self.valid_until.utcoffset() is None
                or not self.observed_at < self.valid_until <= self.observed_at + timedelta(seconds=900)):
            raise ValueError("invalid_pool_allocation_evidence")
        return self
