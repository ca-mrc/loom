"""Bounded native-result readback; contains neither manifests nor credentials."""
from __future__ import annotations

from datetime import UTC, datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

from loom.nebius_pool_contract import PoolNamespaceBindingV1, PoolReceiptV1


class PoolNativeRuntimeV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["loom.pool-native-runtime.v1"] = "loom.pool-native-runtime.v1"
    receipt: PoolReceiptV1
    target_id: str = Field(pattern=r"^[a-z0-9][a-z0-9-]{0,79}$")
    namespace: PoolNamespaceBindingV1
    job_name: str
    lease_epoch: int = Field(gt=0, le=2**63 - 1, strict=True)
    deadline_at: datetime
    registry_repository: str = Field(max_length=200, pattern=r"^[a-z0-9.-]+(?::[0-9]+)?/[a-z0-9/_.-]+$")
    job_effect_id: UUID | None

    @model_validator(mode="after")
    def bound_runtime(self) -> PoolNativeRuntimeV1:
        if (self.receipt.request_key.workload_kind not in {"task_image_build", "application_image_build"} or self.receipt.plan_sha256 is None
                or self.job_name != f"loom-pool-{self.receipt.reservation_id.hex}"
                or self.deadline_at.utcoffset() is None
                or ((self.receipt.job_uid is None) != (self.job_effect_id is None))
                or (self.job_effect_id is not None and not self.job_effect_id.int)):
            raise ValueError("pool_native_runtime_identity_mismatch")
        object.__setattr__(self, "deadline_at", self.deadline_at.astimezone(UTC))
        return self
