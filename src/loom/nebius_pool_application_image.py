"""Frozen management build intent submitted to the common capacity authority."""
from __future__ import annotations

from datetime import UTC, datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

from loom.application_image_build import ApplicationImageBuildClaimV1
from loom.nebius_pool_contract import PoolRequestKeyV1
from loom.nebius_pool_priority import PoolWorkOriginV1


class PoolApplicationImagePrepareV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)

    schema_version: Literal["loom.pool-application-image-prepare.v1"] = "loom.pool-application-image-prepare.v1"
    pool_id: UUID
    admission_epoch: int = Field(gt=0, le=2**63 - 1, strict=True)
    participant_revision: int = Field(gt=0, le=2**63 - 1, strict=True)
    key: PoolRequestKeyV1
    target_id: str = Field(pattern=r"^[a-z0-9][a-z0-9-]{0,79}$")
    deadline_at: datetime
    origin: PoolWorkOriginV1
    build: ApplicationImageBuildClaimV1

    @model_validator(mode="after")
    def consistent_request(self) -> PoolApplicationImagePrepareV1:
        if (not self.pool_id.int or self.deadline_at.utcoffset() is None
                or self.key.workload_kind != "application_image_build" or self.origin.kind != "personal_build"
                or (self.key.local_work_id, self.key.generation, self.origin.submission_id,
                    self.origin.data_environment_id) != (
                    self.build.build_id, self.build.attempt, self.build.build_id, self.build.data_environment_id)):
            raise ValueError("pool_application_build_request_identity")
        object.__setattr__(self, "deadline_at", self.deadline_at.astimezone(UTC))
        return self
