"""Bounded execution readback; no manifests, credentials or current profiles."""
from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

from loom.nebius_pool_contract import PoolNamespaceBindingV1, PoolReceiptV1

_Generation = Annotated[int, Field(gt=0, le=2**63 - 1, strict=True)]


class PoolExecutionRuntimeV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["loom.pool-execution-runtime.v1"] = "loom.pool-execution-runtime.v1"
    receipt: PoolReceiptV1
    target_id: str = Field(pattern=r"^[a-z0-9][a-z0-9-]{0,79}$")
    namespace: PoolNamespaceBindingV1
    job_name: str
    resource_generation: _Generation
    lease_generation: _Generation
    execution_unit_key: UUID
    deadline_at: datetime
    job_effect_id: UUID | None

    @model_validator(mode="after")
    def bound_runtime(self) -> PoolExecutionRuntimeV1:
        if (self.receipt.request_key.workload_kind not in {"trial", "verifier"}
                or self.receipt.phase not in {"create_intent", "observed", "cleanup_intent", "released"}
                or self.receipt.plan_sha256 is None
                or self.job_name != f"loom-pool-{self.receipt.reservation_id.hex}"
                or self.resource_generation != self.receipt.request_key.generation
                or not self.execution_unit_key.int or self.deadline_at.utcoffset() is None
                or ((self.receipt.job_uid is None) != (self.job_effect_id is None))
                or (self.job_effect_id is not None and not self.job_effect_id.int)):
            raise ValueError("pool_execution_runtime_identity_mismatch")
        object.__setattr__(self, "deadline_at", self.deadline_at.astimezone(UTC))
        return self
