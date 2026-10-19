"""Bounded read-only ledger inventory for the protected installer, not runtime consent.

The fixed Job proves which code/schema read the ledger. It does not prove that
the observer/gateway processes are running or that exclusive writer transfer
occurred. Those installed runtime observations remain activation requirements.
"""
from __future__ import annotations

from datetime import timedelta
from typing import Any, Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.nebius_pool_catalog_schema import NebiusPoolParticipantCatalog
from loom.db.nebius_pool_lifecycle_schema import NebiusPoolParticipantLifecycle
from loom.db.nebius_pool_schema import NebiusPoolBinding, NebiusPoolMachine, NebiusPoolParticipant
from loom.nebius_pool_contract import PoolParticipantV1
from loom_service.pool_management.capacity import digest
from loom_service.pool_management.control import _clock
from loom_service.pool_management.lifecycle_contracts import PoolRuntimeReadinessV2
from loom_service.pool_management.observation_binding import physical_binding_digest
from loom_service.pool_management.profile_registry import _validate_catalog, registry_enabled
from loom_service.pool_management.profiles import PoolProfileCatalog
from loom_service.pool_management.readiness import qualify_runtime_readiness


class _Strict(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True, hide_input_in_errors=True)


class PoolInspectionV2(_Strict):
    schema_version: Literal['loom.pool-inspection.v2']
    operation_id: UUID
    installation_id: UUID
    pool_id: UUID
    participant_id: UUID | None = None
    environment_id: UUID | None = None
    incarnation: UUID | None = None
    environment_class: Literal['development', 'staging', 'production'] | None = None

    @model_validator(mode='after')
    def identity(self) -> PoolInspectionV2:
        ids = (self.participant_id, self.environment_id, self.incarnation)
        if (not all(row.int for row in (self.operation_id, self.installation_id, self.pool_id))
                or (self.environment_class is None and any(row is not None for row in ids))
                or (self.environment_class is not None and any(row is None or not row.int for row in ids))):
            raise ValueError('pool_inspection_scope_unqualified')
        return self


class PoolLedgerIdentityV2(_Strict):
    pool_id: UUID
    installation_id: UUID
    cluster_id: str = Field(min_length=1, max_length=253)
    node_group_id: str = Field(min_length=1, max_length=253)
    admission_epoch: int = Field(gt=0, strict=True)
    policy_revision: int = Field(gt=0, strict=True)
    mode: Literal['closed', 'global']
    binding: dict[str, Any]
    binding_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')

    def row(self) -> NebiusPoolBinding:
        value = self.model_dump(exclude={'binding'})
        return NebiusPoolBinding(**value, binding_json=self.binding)

    @model_validator(mode='after')
    def qualified(self) -> PoolLedgerIdentityV2:
        if (not self.pool_id.int or not self.installation_id.int or digest(self.binding) != self.binding_sha256
                or not registry_enabled(self.row()) or self.binding.get('lifecycle_source') != 'scoped-v1'
                or self.binding.get('node_selector', {}).get('nebius.com/node-group-id') != self.node_group_id):
            raise ValueError('pool_inspection_binding_unqualified')
        return self


class _Machine(_Strict):
    machine_id: UUID
    role: Literal['observer', 'gateway']
    credential_epoch: int = Field(gt=0, strict=True)


class _Head(_Strict):
    operation_id: UUID
    generation: int = Field(gt=0, strict=True)
    state: Literal['enrolled', 'ready', 'active', 'suspended', 'retiring', 'retired']
    binding_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')


class _Participant(_Strict):
    binding: PoolParticipantV1
    head: _Head
    profiles: PoolProfileCatalog
    catalog_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')


class PoolInspectionSnapshotV2(_Strict):
    pool: PoolLedgerIdentityV2
    pool_authority_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')
    observed_at: AwareDatetime
    valid_until: AwareDatetime
    capabilities: tuple[str, ...]
    machines: tuple[_Machine, ...] = Field(min_length=2, max_length=2)
    participant: _Participant | None

    @model_validator(mode='after')
    def coherence(self) -> PoolInspectionSnapshotV2:
        if (not timedelta(0) < self.valid_until - self.observed_at <= timedelta(minutes=5)
                or self.pool_authority_sha256 != physical_binding_digest(self.pool.row())
                or set(self.capabilities) != {'physical-v1', 'registry-v1', 'scoped-lifecycle-v1'}
                or {row.role for row in self.machines} != {'gateway', 'observer'}
                or len({row.machine_id for row in self.machines if row.machine_id.int}) != 2):
            raise ValueError('pool_inspection_snapshot_unqualified')
        selected = self.participant
        if selected is not None:
            binding = selected.binding.model_dump(mode='json')
            catalog = {'participant': binding, 'profiles': selected.profiles.model_dump(mode='json')}
            if (selected.head.binding_sha256 != digest(binding) or not selected.head.operation_id.int
                    or selected.catalog_sha256 != digest(catalog)):
                raise ValueError('pool_inspection_participant_unqualified')
            _validate_catalog(self.pool.row(), selected.binding, selected.profiles)
        return self


class PoolInspectionResultV2(_Strict):
    schema_version: Literal['loom.pool-inspection-result.v2'] = 'loom.pool-inspection-result.v2'
    authorization_id: UUID
    request_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')
    snapshot: PoolInspectionSnapshotV2


def qualify_inspection(result: Any, request: PoolInspectionV2, authorization_id: UUID) -> PoolInspectionSnapshotV2:
    report = PoolInspectionResultV2.model_validate(result)
    snapshot, selected = report.snapshot, report.snapshot.participant
    if (report.authorization_id != authorization_id or report.request_sha256 != digest(request.model_dump(mode='json'))
            or (snapshot.pool.installation_id, snapshot.pool.pool_id) != (request.installation_id, request.pool_id)
            or (request.participant_id is None and selected is not None)):
        raise ValueError('pool_inspection_report_unqualified')
    if selected is not None and (selected.binding.participant_id, selected.binding.environment_id,
            selected.binding.incarnation, selected.binding.environment_class) != (
                request.participant_id, request.environment_id, request.incarnation, request.environment_class):
        raise ValueError('pool_inspection_selection_unqualified')
    return snapshot


async def inspect_pool(session: AsyncSession, request: PoolInspectionV2, authorization_id: UUID) -> dict[str, Any]:
    now = await _clock(session)
    pool = await session.get(NebiusPoolBinding, request.pool_id, populate_existing=True)
    if pool is None or pool.installation_id != request.installation_id:
        raise ValueError('pool_inspection_binding_unavailable')
    identity = PoolLedgerIdentityV2(**{key: getattr(pool, key) for key in PoolLedgerIdentityV2.model_fields if key != 'binding'},
        binding=pool.binding_json)
    rows = list(await session.scalars(select(NebiusPoolMachine).where(NebiusPoolMachine.pool_id == request.pool_id,
        NebiusPoolMachine.participant_id.is_(None), NebiusPoolMachine.phase == 'active').order_by(NebiusPoolMachine.role)))
    machines = tuple(_Machine(machine_id=row.machine_id, role=row.role, credential_epoch=row.credential_epoch) for row in rows)
    participant = None
    if request.participant_id is not None:
        selected = await session.get(NebiusPoolParticipant, request.participant_id, populate_existing=True)
        if selected is not None:
            head = await session.get(NebiusPoolParticipantLifecycle, request.participant_id, populate_existing=True)
            catalog = await session.get(NebiusPoolParticipantCatalog, (request.participant_id, selected.binding_revision), populate_existing=True)
            if (head is None or catalog is None or head.pool_id != request.pool_id or catalog.pool_id != request.pool_id
                    or selected.binding_sha256 != digest(selected.binding_json)
                    or catalog.document_json.get('participant') != selected.binding_json):
                raise ValueError('pool_inspection_participant_unavailable')
            participant = _Participant(binding=PoolParticipantV1.model_validate(selected.binding_json),
                head=_Head(operation_id=head.head_operation_id, generation=head.generation, state=head.state,
                    binding_sha256=head.binding_sha256), profiles=PoolProfileCatalog.model_validate(catalog.document_json['profiles']),
                catalog_sha256=catalog.document_sha256)
    snapshot = PoolInspectionSnapshotV2(pool=identity, pool_authority_sha256=physical_binding_digest(pool), observed_at=now,
        valid_until=now + timedelta(minutes=5), capabilities=('physical-v1', 'registry-v1', 'scoped-lifecycle-v1'),
        machines=machines, participant=participant)
    observer, = (row for row in machines if row.role == 'observer')
    gateway, = (row for row in machines if row.role == 'gateway')
    await qualify_runtime_readiness(session, pool, PoolRuntimeReadinessV2(observed_at=now, valid_until=snapshot.valid_until,
        pool_authority_sha256=snapshot.pool_authority_sha256, capabilities=snapshot.capabilities,
        observer_machine_id=observer.machine_id, observer_credential_epoch=observer.credential_epoch,
        gateway_machine_id=gateway.machine_id, gateway_credential_epoch=gateway.credential_epoch))
    result = PoolInspectionResultV2(authorization_id=authorization_id,
        request_sha256=digest(request.model_dump(mode='json')), snapshot=snapshot).model_dump(mode='json')
    qualify_inspection(result, request, authorization_id)
    return result
