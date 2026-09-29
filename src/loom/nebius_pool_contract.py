"""Global pool identity contracts; validation alone confers no runtime authority.

Protected registration, credentials and current admission epochs must qualify
these values before use. Target IDs are local to a participant, never global
reservation keys. Runtime profiles and namespace ownership are registry-bound.
"""
from __future__ import annotations

from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

PoolWorkloadKind = Literal["trial", "verifier", "task_image_build", "application_image_build"]
_Generation = Annotated[int, Field(gt=0, le=2**63 - 1, strict=True)]
_NAMESPACE = r"^[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?$"
_TARGET = r"^[a-z0-9][a-z0-9-]{0,79}$"


def _non_nil(*identities: UUID) -> None:
    if any(not identity.int for identity in identities):
        raise ValueError("nil_pool_identity")


class _PoolContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class PoolNamespaceBindingV1(_PoolContract):
    name: str = Field(pattern=_NAMESPACE)
    uid: UUID

    @model_validator(mode="after")
    def identity(self) -> PoolNamespaceBindingV1:
        _non_nil(self.uid)
        return self


class PoolTargetBindingV1(_PoolContract):
    target_id: str = Field(pattern=_TARGET)
    profile_id: UUID
    workload_kinds: tuple[PoolWorkloadKind, ...] = Field(min_length=1, max_length=4)

    @model_validator(mode="after")
    def identity(self) -> PoolTargetBindingV1:
        _non_nil(self.profile_id)
        if len(set(self.workload_kinds)) != len(self.workload_kinds):
            raise ValueError("duplicate_pool_workload_kind")
        return self


class PoolParticipantV1(_PoolContract):
    schema_version: Literal["loom.pool-participant.v1"] = "loom.pool-participant.v1"
    participant_id: UUID
    installation_id: UUID
    environment_id: UUID
    incarnation: UUID
    pool_id: UUID
    binding_revision: _Generation
    admission_epoch: _Generation
    execution_namespace: PoolNamespaceBindingV1
    build_namespace: PoolNamespaceBindingV1
    targets: tuple[PoolTargetBindingV1, ...] = Field(min_length=1, max_length=64)

    @model_validator(mode="after")
    def unambiguous_identity(self) -> PoolParticipantV1:
        _non_nil(self.participant_id, self.installation_id, self.environment_id, self.incarnation, self.pool_id)
        if (self.execution_namespace.name == self.build_namespace.name
                or self.execution_namespace.uid == self.build_namespace.uid):
            raise ValueError("duplicate_pool_namespace")
        if len({target.target_id for target in self.targets}) != len(self.targets):
            raise ValueError("duplicate_pool_target")
        return self

    def target(self, target_id: str, workload_kind: PoolWorkloadKind) -> PoolTargetBindingV1:
        for target in self.targets:
            if target.target_id == target_id and workload_kind in target.workload_kinds:
                return target
        raise ValueError("target_workload_unavailable")


class PoolRequestKeyV1(_PoolContract):
    schema_version: Literal["loom.pool-request-key.v1"] = "loom.pool-request-key.v1"
    participant_id: UUID
    workload_kind: PoolWorkloadKind
    local_work_id: UUID
    generation: _Generation

    @model_validator(mode="after")
    def identity(self) -> PoolRequestKeyV1:
        _non_nil(self.participant_id, self.local_work_id)
        return self

    def storage_key(self) -> tuple[UUID, PoolWorkloadKind, UUID, int]:
        return self.participant_id, self.workload_kind, self.local_work_id, self.generation
