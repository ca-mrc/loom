"""Typed execution intake, never caller-supplied Kubernetes or capacity authority."""
from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

from loom.execution_contract import WorkloadRequirementsV1
from loom.execution_runtime_contract import (
    ExecutionRuntimePlanV1,
    validate_runtime_plan_requirements,
)
from loom.nebius_pool_contract import PoolRequestKeyV1
from loom.nebius_pool_priority import PoolWorkOriginV1

_Generation = Annotated[int, Field(gt=0, le=2**63 - 1, strict=True)]


class PoolExecutionWorkloadV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    lease_generation: _Generation
    execution_unit_key: UUID
    parent_lease_id: UUID | None
    requirements: WorkloadRequirementsV1
    runtime: ExecutionRuntimePlanV1

    @model_validator(mode="after")
    def consistent_workload(self) -> PoolExecutionWorkloadV1:
        if not self.execution_unit_key.int or (self.parent_lease_id is not None and not self.parent_lease_id.int):
            raise ValueError("nil_pool_execution_identity")
        validate_runtime_plan_requirements(self.runtime, self.requirements)
        return self


class PoolExecutionPrepareV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["loom.pool-execution-prepare.v1"] = "loom.pool-execution-prepare.v1"
    pool_id: UUID
    admission_epoch: _Generation
    participant_revision: _Generation
    key: PoolRequestKeyV1
    target_id: str = Field(pattern=r"^[a-z0-9][a-z0-9-]{0,79}$")
    deadline_at: datetime
    origin: PoolWorkOriginV1
    execution: PoolExecutionWorkloadV1

    @model_validator(mode="after")
    def consistent_request(self) -> PoolExecutionPrepareV1:
        if not self.pool_id.int or self.deadline_at.utcoffset() is None:
            raise ValueError("invalid_pool_execution_identity")
        role = {"trial": "attempt", "verifier": "verifier"}.get(self.key.workload_kind)
        if role is None or role != self.execution.runtime.execution_role or self.origin.kind == "personal_build":
            raise ValueError("pool_execution_kind_mismatch")
        object.__setattr__(self, "deadline_at", self.deadline_at.astimezone(UTC))
        return self
