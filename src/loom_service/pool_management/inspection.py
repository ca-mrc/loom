"""Bounded read-only ledger inventory for the protected installer, not runtime consent.

The fixed Job proves which code/schema read the ledger. It does not prove that
the observer/gateway processes are running or that exclusive writer transfer
occurred. Those installed runtime observations remain activation requirements.
"""
from __future__ import annotations

from datetime import timedelta
from typing import Any, Literal
from uuid import UUID

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    SerializerFunctionWrapHandler,
    model_serializer,
    model_validator,
)
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.nebius_pool_catalog_schema import NebiusPoolParticipantCatalog
from loom.db.nebius_pool_lifecycle_schema import (
    NebiusPoolLifecycleOperation,
    NebiusPoolParticipantLifecycle,
)
from loom.db.nebius_pool_schema import NebiusPoolBinding, NebiusPoolMachine, NebiusPoolParticipant
from loom.nebius_pool_contract import PoolParticipantV1
from loom_service.pool_management.capacity import digest, observe_connected_capacity
from loom_service.pool_management.control import _clock
from loom_service.pool_management.lifecycle_contracts import PoolRuntimeReadinessV2
from loom_service.pool_management.locks import require_pool_read_snapshot
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
    require_capacity: bool = Field(default=False, strict=True)
    platform_authorization_id: UUID | None = None

    @model_serializer(mode='wrap')
    def retained_inventory_bytes(self, handler: SerializerFunctionWrapHandler) -> dict[str, Any]:
        value: dict[str, Any] = handler(self)
        if not self.require_capacity:
            value.pop('require_capacity', None)
        if self.platform_authorization_id is None:
            value.pop('platform_authorization_id', None)
        return value

    @model_validator(mode='after')
    def identity(self) -> PoolInspectionV2:
        ids = (self.participant_id, self.environment_id, self.incarnation)
        if (not all(row.int for row in (self.operation_id, self.installation_id, self.pool_id))
                or (self.platform_authorization_id is not None and not self.platform_authorization_id.int)
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


class PoolCapacityObservationV2(_Strict):
    """Authenticated physical evidence, not free slots or an allocation grant."""
    pool_id: UUID
    pool_authority_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')
    observation_id: UUID
    observed_at: AwareDatetime
    valid_until: AwareDatetime
    placement_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')

    @model_validator(mode='after')
    def bounded(self) -> PoolCapacityObservationV2:
        if (not self.pool_id.int or not self.observation_id.int
                or not timedelta(seconds=10) <= self.valid_until - self.observed_at <= timedelta(seconds=900)):
            raise ValueError('pool_inspection_capacity_unqualified')
        return self


class PoolPlatformAuthorityObservationV2(_Strict):
    """Committed protected authority reference, not a new sole-writer assertion."""
    operation_id: UUID
    authorization_id: UUID
    authorization_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')
    request_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')
    receipt_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')


class PoolInspectionSnapshotV2(_Strict):
    pool: PoolLedgerIdentityV2
    pool_authority_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')
    observed_at: AwareDatetime
    valid_until: AwareDatetime
    capabilities: tuple[str, ...]
    machines: tuple[_Machine, ...] = Field(min_length=2, max_length=2)
    participant: _Participant | None
    capacity: tuple[PoolCapacityObservationV2, ...] | None = Field(default=None, min_length=1, max_length=1000)
    platform_authority: PoolPlatformAuthorityObservationV2 | None = None

    @model_serializer(mode='wrap')
    def retained_inventory_bytes(self, handler: SerializerFunctionWrapHandler) -> dict[str, Any]:
        value: dict[str, Any] = handler(self)
        if self.capacity is None:
            value.pop('capacity', None)
        if self.platform_authority is None:
            value.pop('platform_authority', None)
        return value

    @model_validator(mode='after')
    def coherence(self) -> PoolInspectionSnapshotV2:
        if (not timedelta(0) < self.valid_until - self.observed_at <= timedelta(minutes=5)
                or self.pool_authority_sha256 != physical_binding_digest(self.pool.row())
                or set(self.capabilities) != {'physical-v1', 'registry-v1', 'scoped-lifecycle-v1'}
                or {row.role for row in self.machines} != {'gateway', 'observer'}
                or len({row.machine_id for row in self.machines if row.machine_id.int}) != 2):
            raise ValueError('pool_inspection_snapshot_unqualified')
        selected = self.participant
        if self.platform_authority is not None and (
                not self.platform_authority.authorization_id.int or self.pool.mode != 'global'
                or str(self.platform_authority.operation_id) != self.pool.binding.get('lifecycle_head_operation_id')):
            raise ValueError('pool_inspection_platform_authority_unqualified')
        if self.capacity is not None:
            own = [row for row in self.capacity if row.pool_id == self.pool.pool_id]
            if (len(own) != 1 or own[0].pool_authority_sha256 != self.pool_authority_sha256
                    or len({row.pool_id for row in self.capacity}) != len(self.capacity)
                    or any(self.valid_until > row.valid_until or row.observed_at > self.observed_at + timedelta(seconds=60)
                        for row in self.capacity)):
                raise ValueError('pool_inspection_capacity_unqualified')
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
            or (request.participant_id is None and selected is not None)
            or request.require_capacity != (snapshot.capacity is not None)
            or request.platform_authorization_id != (
                snapshot.platform_authority.authorization_id if snapshot.platform_authority is not None else None)):
        raise ValueError('pool_inspection_report_unqualified')
    if selected is not None and (selected.binding.participant_id, selected.binding.environment_id,
            selected.binding.incarnation, selected.binding.environment_class) != (
                request.participant_id, request.environment_id, request.incarnation, request.environment_class):
        raise ValueError('pool_inspection_selection_unqualified')
    return snapshot


async def _platform_authority(session: AsyncSession, pool: NebiusPoolBinding,
        authorization_id: UUID) -> PoolPlatformAuthorityObservationV2:
    # Import at the boundary: administration owns authorization parsing and also
    # dispatches inspection. No duplicate interpretation of protected grant scope.
    from loom_service.pool_management.administration import (
        _REQUEST,
        PoolAdministrationAuthorizationV2,
    )
    from loom_service.pool_management.legacy_adoption import PoolLegacyAdoptionV2
    from loom_service.pool_management.lifecycle_contracts import PoolPlatformOperationV2

    try:
        await require_pool_read_snapshot(session)
        identity = UUID(pool.binding_json['lifecycle_head_operation_id'])
        operation = await session.get(NebiusPoolLifecycleOperation, identity, populate_existing=True)
        audit = await session.get(NebiusPoolLifecycleOperation, authorization_id, populate_existing=True)
        if pool.mode != 'global' or operation is None or audit is None:
            raise ValueError
        grant = PoolAdministrationAuthorizationV2.model_validate(audit.request_json['authorization'])
        expected_audit = dict(schema_version='loom.pool-administration-audit.v2', operation_id=str(authorization_id),
            authorization=grant.model_dump(mode='json'), authorized_operation_id=str(identity))
        if (grant.authorization_id != authorization_id or grant.authority != 'platform'
                or (grant.installation_id, grant.pool_id) != (pool.installation_id, pool.pool_id)
                or not grant.issued_at <= audit.created_at < grant.expires_at
                or audit.request_json != expected_audit
                or audit.receipt_json != dict(operation_id=str(identity), request_sha256=grant.request_sha256)
                or any(row.pool_id != pool.pool_id or row.participant_id is not None
                    or row.request_sha256 != digest(row.request_json) for row in (audit, operation))
                or operation.request_sha256 != grant.request_sha256):
            raise ValueError
        request = _REQUEST.validate_python(operation.request_json)
        if isinstance(request, PoolPlatformOperationV2):
            allowed = request.action == 'open'
            installation, selected_pool = request.installation_id, request.pool_id
        elif isinstance(request, PoolLegacyAdoptionV2):
            allowed = request.expected_mode == 'global'
            installation, selected_pool = request.before.installation_id, request.before.pool_id
        else:
            raise ValueError
        receipt = operation.receipt_json
        if (not allowed or request.operation_id != identity
                or (installation, selected_pool) != (pool.installation_id, pool.pool_id)
                or receipt.get('operation_id') != str(identity) or receipt.get('pool_id') != str(pool.pool_id)
                or receipt.get('mode') != 'global' or receipt.get('admission_epoch') != pool.admission_epoch
                or receipt.get('pool_authority_sha256') != physical_binding_digest(pool)):
            raise ValueError
        return PoolPlatformAuthorityObservationV2(operation_id=identity, authorization_id=authorization_id,
            authorization_sha256=digest(grant.model_dump(mode='json')), request_sha256=operation.request_sha256,
            receipt_sha256=digest(receipt))
    except Exception:
        raise ValueError('pool_inspection_platform_authority_unqualified') from None


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
    capacity = None
    valid_until = now + timedelta(minutes=5)
    if request.require_capacity:
        current = await observe_connected_capacity(session, pool.pool_id, now)
        capacity = tuple(PoolCapacityObservationV2(pool_id=row.pool.pool_id, pool_authority_sha256=physical_binding_digest(row.pool),
            observation_id=row.observation_id, observed_at=row.observed_at,
            valid_until=row.observed_at + timedelta(seconds=row.policy.observation_max_age_seconds),
            placement_sha256=digest(row.placement.model_dump(mode='json'))) for row in current.values())
        valid_until = min(valid_until, *(row.valid_until for row in capacity))
    platform_authority = (await _platform_authority(session, pool, request.platform_authorization_id)
        if request.platform_authorization_id is not None else None)
    snapshot = PoolInspectionSnapshotV2(pool=identity, pool_authority_sha256=physical_binding_digest(pool), observed_at=now,
        valid_until=valid_until, capabilities=('physical-v1', 'registry-v1', 'scoped-lifecycle-v1'),
        machines=machines, participant=participant, capacity=capacity, platform_authority=platform_authority)
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
