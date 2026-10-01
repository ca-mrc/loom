"""Machine-only lifecycle attestations; none asserts Kubernetes absence/release."""
from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

from loom.nebius_pool_contract import PoolRequestActionV1

_Digest = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
_Generation = Annotated[int, Field(gt=0, le=2**63 - 1, strict=True)]


class _Lifecycle(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    action: PoolRequestActionV1
    reservation_id: UUID
    plan_sha256: _Digest
    lease_generation: _Generation

    @model_validator(mode="after")
    def identity(self) -> _Lifecycle:
        if not self.reservation_id.int:
            raise ValueError("pool_lifecycle_identity_required")
        return self


class PoolStopV1(_Lifecycle):
    schema_version: Literal["loom.pool-stop.v1"] = "loom.pool-stop.v1"
    cause: Literal["completed", "failed", "cancelled", "lease_lost", "deadline"]
    grace_deadline_at: datetime

    @model_validator(mode="after")
    def aware_deadline(self) -> PoolStopV1:
        if self.grace_deadline_at.utcoffset() is None:
            raise ValueError("pool_stop_requires_absolute_deadline")
        object.__setattr__(self, "grace_deadline_at", self.grace_deadline_at.astimezone(UTC))
        return self


class PoolDrainV1(_Lifecycle):
    schema_version: Literal["loom.pool-drain.v1"] = "loom.pool-drain.v1"
    stop_sha256: _Digest
    output_generation: _Generation
    output_state: Literal["committed", "unavailable"]
    evidence_sha256: _Digest
