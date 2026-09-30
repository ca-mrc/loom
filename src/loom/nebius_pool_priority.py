"""Pure class ordering, not proof of a caller's origin or admission authority.

Protected registration supplies the participant. Trusted submission code records
the origin; the management registry must qualify application/build references
before calling this policy. Parsing these values never authorizes a workload.
No fixed shares, idle reservations or preemption are introduced by this ordering.
"""
from __future__ import annotations

from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

from loom.nebius_pool_contract import PoolParticipantV1, PoolWorkloadKind


class PoolApplicationOriginV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    application_id: UUID
    incarnation: UUID
    deployment_generation: int = Field(gt=0, le=2**63 - 1, strict=True)
    release_id: UUID
    source_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")

    @model_validator(mode="after")
    def non_nil(self) -> PoolApplicationOriginV1:
        if not all(value.int for value in (self.application_id, self.incarnation, self.release_id)):
            raise ValueError("nil_pool_origin_identity")
        return self


class PoolWorkOriginV1(BaseModel):
    """One persisted submission; missing historical origin is not shared work.

    A personal_build submission ID identifies the management-owned source build,
    which can precede application creation. Application origins retain the exact
    submitted version even after a later update, suspend or destroy.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["loom.pool-work-origin.v1"] = "loom.pool-work-origin.v1"
    data_environment_id: UUID
    submission_id: UUID
    kind: Literal["environment", "application", "personal_build"]
    application: PoolApplicationOriginV1 | None

    @model_validator(mode="after")
    def explicit_origin(self) -> PoolWorkOriginV1:
        if not self.data_environment_id.int or not self.submission_id.int:
            raise ValueError("nil_pool_origin_identity")
        if (self.kind == "application") != (self.application is not None):
            raise ValueError("pool_origin_scope")
        return self


def pool_request_priority(participant: PoolParticipantV1, origin: PoolWorkOriginV1, *,
                          workload_kind: PoolWorkloadKind) -> int:
    """Order already-qualified new demand; lower numbers take precedence.

    The caller still owns origin lookup, fitting/staleness/fairness decisions,
    immutable replay and transactional admission. Existing grants are untouched.
    """
    participant = PoolParticipantV1.model_validate(participant.model_dump())
    origin = PoolWorkOriginV1.model_validate(origin.model_dump())
    if (origin.data_environment_id != participant.environment_id
            or workload_kind not in {"trial", "verifier", "task_image_build", "application_image_build"}
            or (workload_kind == "application_image_build") != (origin.kind == "personal_build")
            or (origin.kind != "environment" and participant.environment_class != "development")):
        raise ValueError("pool_origin_scope")
    if origin.kind != "environment":
        return 3
    return {"production": 0, "staging": 1, "development": 2}[participant.environment_class]
