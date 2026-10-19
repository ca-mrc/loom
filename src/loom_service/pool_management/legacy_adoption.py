"""Explicit v1 import; ordinary lifecycle never loads combined migration inputs.

The protected caller must qualify installed manager/collector/gateway capability
and completed legacy sole-writer evidence before this transaction. A closed
registration may be imported closed, but never proves operational readiness.
No guard, drain, writer transfer, credential rotation or resource install occurs.
"""
from __future__ import annotations

from typing import Any, Literal
from uuid import UUID, uuid5

from pydantic import BaseModel, ConfigDict, Field, model_validator
from sqlalchemy import insert, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.nebius_pool_lifecycle_schema import NebiusPoolParticipantLifecycle
from loom.db.nebius_pool_schema import NebiusPoolBinding, NebiusPoolParticipant, NebiusPoolRequest
from loom_service.pool_management.capacity import CHARGED_PHASES, digest
from loom_service.pool_management.installation import PoolInstallation
from loom_service.pool_management.lifecycle_journal import replay, retain_operation
from loom_service.pool_management.observation_binding import physical_binding_digest
from loom_service.pool_management.participant_refresh import _authority, _binding
from loom_service.pool_management.profile_registry import store_participant_catalog


class PoolLegacyAdoptionV2(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True)

    schema_version: Literal['loom.pool-legacy-adoption.v2']
    operation_id: UUID
    expected_pool_authority_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')
    expected_mode: Literal['closed', 'global']
    before: PoolInstallation

    @model_validator(mode='after')
    def identity(self) -> PoolLegacyAdoptionV2:
        if not self.operation_id.int:
            raise ValueError('pool_legacy_adoption_identity_invalid')
        return self


async def adopt_legacy_platform(session: AsyncSession, request: PoolLegacyAdoptionV2) -> dict[str, Any]:
    request = PoolLegacyAdoptionV2.model_validate(request.model_dump())
    document = request.model_dump(mode='json')
    retained = await replay(session, request.operation_id, document)
    if retained is not None:
        return retained
    before = request.before
    pool = await session.get(NebiusPoolBinding, before.pool_id, with_for_update=True, populate_existing=True)
    if (pool is None or pool.mode != request.expected_mode
            or physical_binding_digest(pool) != request.expected_pool_authority_sha256
            or pool.binding_json != _binding(before) or digest(pool.binding_json) != pool.binding_sha256
            or (pool.installation_id, pool.cluster_id, pool.node_group_id, pool.policy_revision, pool.admission_epoch)
                != (before.installation_id, before.cluster_id, before.node_group_id, before.policy_revision, before.admission_epoch)):
        raise ValueError('pool_legacy_adoption_authority_changed')
    await _authority(session, before)
    rows = (await session.scalars(select(NebiusPoolParticipant).where(NebiusPoolParticipant.pool_id == pool.pool_id)
        .with_for_update().execution_options(populate_existing=True))).all()
    expected = {row.participant_id: row for row in before.participants}
    if {row.participant_id for row in rows} != set(expected):
        raise ValueError('pool_legacy_adoption_roster_changed')
    for row in rows:
        participant = expected[row.participant_id]
        if (row.binding_json != participant.model_dump(mode='json') or digest(row.binding_json) != row.binding_sha256
                or (row.environment_id, row.incarnation, row.binding_revision, row.admission_epoch)
                    != (participant.environment_id, participant.incarnation, participant.binding_revision, participant.admission_epoch)
                or row.phase not in {'active', 'fenced'}
                or await session.get(NebiusPoolParticipantLifecycle, row.participant_id) is not None):
            raise ValueError('pool_legacy_adoption_participant_changed')
    if pool.mode == 'closed' and await session.scalar(select(NebiusPoolRequest.request_id).where(
            NebiusPoolRequest.pool_id == pool.pool_id, NebiusPoolRequest.phase.in_(CHARGED_PHASES)).limit(1)) is not None:
        raise ValueError('pool_legacy_adoption_closed_work_requires_recovery')
    binding = dict(pool.binding_json, observation_binding='physical-v1', profile_source='registry-v1', lifecycle_source='scoped-v1')
    await session.execute(update(NebiusPoolBinding).where(NebiusPoolBinding.pool_id == pool.pool_id).values(
        binding_json=binding, binding_sha256=digest(binding), policy_revision=pool.policy_revision + 1))
    children = []
    for row in rows:
        participant = expected[row.participant_id]
        wanted = {target.profile_id for target in participant.targets}
        catalog = before.profiles.model_copy(update={family: tuple(profile for profile in getattr(before.profiles, family)
            if profile.profile_id in wanted) for family in ('execution', 'task_images', 'application_images')})
        await store_participant_catalog(session, participant, catalog)
        state = 'suspended' if row.phase == 'fenced' else ('active' if pool.mode == 'global' else 'enrolled')
        if state == 'enrolled':
            await session.execute(update(NebiusPoolParticipant).where(NebiusPoolParticipant.participant_id == row.participant_id)
                .values(phase='fenced'))
        child_id = uuid5(request.operation_id, 'participant:' + str(row.participant_id))
        child = dict(schema_version='loom.pool-participant-adoption.v2', operation_id=str(child_id),
            parent_operation_id=str(request.operation_id), participant=participant.model_dump(mode='json'),
            previous_pool_authority_sha256=request.expected_pool_authority_sha256)
        receipt = dict(schema_version='loom.pool-participant-receipt.v2', operation_id=str(child_id),
            pool_id=str(pool.pool_id), participant_id=str(row.participant_id), incarnation=str(row.incarnation),
            generation=1, state=state, binding_sha256=row.binding_sha256)
        await retain_operation(session, child, receipt, pool_id=pool.pool_id, participant_id=row.participant_id)
        await session.execute(insert(NebiusPoolParticipantLifecycle).values(participant_id=row.participant_id,
            pool_id=pool.pool_id, head_operation_id=child_id, generation=1, state=state, binding_sha256=row.binding_sha256))
        children.append(receipt)
    receipt = dict(schema_version='loom.pool-legacy-adoption-receipt.v2', operation_id=str(request.operation_id),
        pool_id=str(pool.pool_id), mode=pool.mode, admission_epoch=pool.admission_epoch,
        pool_authority_sha256=physical_binding_digest(pool), participants=sorted(children, key=lambda row: str(row['participant_id'])))
    await retain_operation(session, document, receipt, pool_id=pool.pool_id, participant_id=None)
    return receipt
