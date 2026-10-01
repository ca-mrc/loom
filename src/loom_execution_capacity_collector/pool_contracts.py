"""Versioned observer transport. These snapshots confer no Job-write authority."""
from __future__ import annotations

from datetime import UTC
from typing import Annotated, Any, Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

from loom.pipeline.keys import canonical_digest
from loom_execution_capacity_collector.contracts import (
    KubernetesCapacitySnapshot,
    ProviderCapacitySnapshot,
)
from loom_execution_capacity_collector.pool import PoolObservationScope

_Digest = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
MAX_POOL_OBSERVATION_BYTES = 1024 * 1024


class _ObserverContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    @model_validator(mode="after")
    def non_nil(self) -> _ObserverContract:
        if any(isinstance(value, UUID) and not value.int for value in self.__dict__.values()):
            raise ValueError("nil observer identity")
        return self


class PoolCaptureV1(_ObserverContract):
    schema_version: Literal["loom.pool-capture.v1"] = "loom.pool-capture.v1"
    capture_id: UUID
    pool_id: UUID
    admission_epoch: int = Field(gt=0, strict=True)
    registration_sha256: _Digest
    created_at: AwareDatetime
    scope: PoolObservationScope


class PoolObservationV1(_ObserverContract):
    schema_version: Literal["loom.pool-observation.v1"] = "loom.pool-observation.v1"
    capture_id: UUID
    observed_at: AwareDatetime
    provider: ProviderCapacitySnapshot
    kubernetes: KubernetesCapacitySnapshot

    def payload(self) -> dict[str, Any]:
        # Retain the journal's UTC +00:00 serialization, including exact replay.
        return self.model_dump(mode="json") | {"observed_at": self.observed_at.astimezone(UTC).isoformat()}

    def digest(self) -> str:
        return canonical_digest(self.payload()).removeprefix("sha256:")


class PoolObservationReceiptV1(_ObserverContract):
    schema_version: Literal["loom.pool-observation-receipt.v1"] = "loom.pool-observation-receipt.v1"
    observation_id: UUID
    capture_id: UUID
    observation_sha256: _Digest
