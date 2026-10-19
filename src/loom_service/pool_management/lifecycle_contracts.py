"""Inputs to protected platform administration, never public runtime authority."""
from __future__ import annotations

from datetime import timedelta
from typing import Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

from loom.nebius_pool_contract import PoolParticipantV1
from loom_service.pool_management.capacity import PoolCapacityPolicyV1
from loom_service.pool_management.installation import PoolMachineInstallation
from loom_service.pool_management.profiles import PoolProfileCatalog


class _Strict(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True)


class PoolPlatformInstallationV2(_Strict):
    schema_version: Literal['loom.pool-platform-installation.v2']
    operation_id: UUID
    pool_id: UUID
    installation_id: UUID
    cluster_id: str = Field(min_length=1, max_length=253)
    node_group_id: str = Field(min_length=1, max_length=253)
    admission_epoch: int = Field(gt=0, strict=True)
    policy_revision: int = Field(gt=0, strict=True)
    node_selector: dict[str, str]
    admission: PoolCapacityPolicyV1
    quota_identities: dict[str, tuple[str, str, str, str, str]]
    machines: tuple[PoolMachineInstallation, ...] = Field(min_length=2, max_length=2)

    @model_validator(mode='after')
    def physical_authority(self) -> PoolPlatformInstallationV2:
        if (not all(value.int for value in (self.operation_id, self.pool_id, self.installation_id))
                or self.node_selector.get('nebius.com/node-group-id') != self.node_group_id
                or not {'nodes', 'vcpu', 'storage'} <= set(self.quota_identities) <= {'nodes', 'vcpu', 'storage', 'memory'}
                or any(not value for parts in self.quota_identities.values() for value in parts)
                or {row.role for row in self.machines} != {'observer', 'gateway'}
                or len({row.machine_id for row in self.machines}) != 2
                or len({row.token_sha256 for row in self.machines}) != 2):
            raise ValueError('pool_platform_authority_invalid')
        return self


class PoolParticipantEnrollmentV2(_Strict):
    schema_version: Literal['loom.pool-participant-enrollment.v2']
    operation_id: UUID
    pool_authority_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')
    # Digest of the protected provision/import receipt, not proof by itself.
    foundation_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')
    participant: PoolParticipantV1
    machines: tuple[PoolMachineInstallation, ...] = Field(min_length=1, max_length=2)
    profiles: PoolProfileCatalog

    @model_validator(mode='after')
    def scoped_machines(self) -> PoolParticipantEnrollmentV2:
        builders = any('application_image_build' in row.workload_kinds for row in self.participant.targets)
        scopes = ['environment', 'application_builder'] if builders else ['environment']
        if (not self.operation_id.int or self.participant.binding_revision != 1
                or any(row.role != 'participant' or row.participant_id != self.participant.participant_id for row in self.machines)
                or sorted(row.workload_scope for row in self.machines) != sorted(scopes)
                or len({row.machine_id for row in self.machines}) != len(self.machines)
                or len({row.token_sha256 for row in self.machines}) != len(self.machines)):
            raise ValueError('pool_enrollment_authority_invalid')
        return self


class PoolParticipantReadinessV2(_Strict):
    """Protected installer's authenticated readback; no HTTP client can submit it."""
    observed_at: AwareDatetime
    valid_until: AwareDatetime
    pool_authority_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')
    participant_binding_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')
    catalog_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')
    observer_machine_id: UUID
    gateway_machine_id: UUID
    observer_credential_epoch: int = Field(gt=0, strict=True)
    gateway_credential_epoch: int = Field(gt=0, strict=True)
    capabilities: tuple[str, ...]

    @model_validator(mode='after')
    def bounded(self) -> PoolParticipantReadinessV2:
        if (not self.observer_machine_id.int or not self.gateway_machine_id.int
                or not timedelta(0) < self.valid_until - self.observed_at <= timedelta(minutes=5)):
            raise ValueError('pool_participant_readiness_invalid')
        return self


class PoolWaitingCancellationV2(_Strict):
    request_id: UUID
    request_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')


class PoolParticipantOperationV2(_Strict):
    schema_version: Literal['loom.pool-participant-operation.v2']
    operation_id: UUID
    pool_id: UUID
    participant_id: UUID
    incarnation: UUID
    expected_head: UUID
    expected_binding_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')
    pool_authority_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')
    action: Literal['ready', 'activate', 'suspend', 'resume', 'retire', 'complete_retirement']
    readiness: PoolParticipantReadinessV2 | None = None
    cancel_waiting: tuple[PoolWaitingCancellationV2, ...] = Field(default=(), max_length=10000)

    @model_validator(mode='after')
    def scope(self) -> PoolParticipantOperationV2:
        if (not all(value.int for value in (self.operation_id, self.pool_id, self.participant_id, self.incarnation, self.expected_head))
                or (self.readiness is not None) != (self.action in {'ready', 'activate', 'resume'})
                or (self.cancel_waiting and self.action != 'retire')
                or len({row.request_id for row in self.cancel_waiting}) != len(self.cancel_waiting)):
            raise ValueError('pool_participant_operation_invalid')
        return self
