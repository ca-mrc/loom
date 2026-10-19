"""Protected, participant-scoped state transitions with exact replay and CAS.

These are database transactions, not public routes or Kubernetes readiness probes.
The protected installer owns authorization and external evidence qualification.
No transaction performs external IO, changes a peer, or opens the physical pool.
"""
from __future__ import annotations

from typing import Any
from uuid import UUID

from sqlalchemy import insert, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.nebius_pool_catalog_schema import NebiusPoolParticipantCatalog
from loom.db.nebius_pool_lifecycle_schema import (
    NebiusPoolLifecycleOperation,
    NebiusPoolParticipantLifecycle,
)
from loom.db.nebius_pool_schema import (
    NebiusPoolBinding,
    NebiusPoolMachine,
    NebiusPoolParticipant,
    NebiusPoolRequest,
)
from loom.nebius_pool_contract import PoolParticipantV1
from loom_service.pool_management.capacity import CHARGED_PHASES, digest, read_connected_capacity
from loom_service.pool_management.control import _clock
from loom_service.pool_management.intake import scoped_lifecycle
from loom_service.pool_management.lifecycle_contracts import (
    PoolParticipantEnrollmentV2 as PoolParticipantEnrollmentV2,
)
from loom_service.pool_management.lifecycle_contracts import (
    PoolParticipantOperationV2 as PoolParticipantOperationV2,
)
from loom_service.pool_management.lifecycle_contracts import (
    PoolParticipantReadinessV2,
    PoolParticipantReleaseV2,
)
from loom_service.pool_management.lifecycle_contracts import (
    PoolPlatformInstallationV2 as PoolPlatformInstallationV2,
)
from loom_service.pool_management.lifecycle_journal import (
    install_machines,
    replay,
    retain_operation,
)
from loom_service.pool_management.observation_binding import physical_binding_digest
from loom_service.pool_management.platform_installation import (
    register_platform as register_platform,
)
from loom_service.pool_management.profile_registry import store_participant_catalog
from loom_service.pool_management.readiness import qualify_runtime_readiness
from loom_service.pool_management.release_compatibility import qualify_release


async def _pool(session: AsyncSession, pool_id: UUID, sha: str) -> NebiusPoolBinding:
    pool = await session.get(NebiusPoolBinding, pool_id, with_for_update=True, populate_existing=True)
    if (pool is None or not scoped_lifecycle(pool) or pool.mode not in {'closed', 'global'}
            or physical_binding_digest(pool) != sha or digest(pool.binding_json) != pool.binding_sha256):
        raise ValueError('pool_lifecycle_authority_changed')
    return pool


async def enroll_participant(session: AsyncSession, spec: PoolParticipantEnrollmentV2) -> dict[str, Any]:
    spec = PoolParticipantEnrollmentV2.model_validate(spec.model_dump())
    document = spec.model_dump(mode='json')
    retained = await replay(session, spec.operation_id, document)
    if retained is not None:
        return retained
    participant = spec.participant
    pool = await _pool(session, participant.pool_id, spec.pool_authority_sha256)
    if (participant.installation_id != pool.installation_id or participant.admission_epoch != pool.admission_epoch
            or await session.get(NebiusPoolParticipant, participant.participant_id) is not None):
        raise ValueError('pool_participant_enrollment_conflict')
    rows = (await session.scalars(select(NebiusPoolParticipant).where(NebiusPoolParticipant.pool_id == pool.pool_id))).all()
    wanted = (participant.execution_namespace, participant.build_namespace)
    for row in rows:
        peer = PoolParticipantV1.model_validate(row.binding_json)
        history = await session.get(NebiusPoolParticipantLifecycle, row.participant_id)
        retired = history is not None and history.state == 'retired'
        if ((row.environment_id == participant.environment_id and (not retired or row.incarnation == participant.incarnation))
                or any(left.uid == right.uid or (left.name == right.name and not retired) for left in wanted
                    for right in (peer.execution_namespace, peer.build_namespace))):
            raise ValueError('pool_participant_enrollment_conflict')
    binding = participant.model_dump(mode='json')
    checksum = digest(binding)
    await session.execute(insert(NebiusPoolParticipant).values(participant_id=participant.participant_id,
        pool_id=pool.pool_id, environment_id=participant.environment_id, incarnation=participant.incarnation,
        binding_revision=participant.binding_revision, admission_epoch=pool.admission_epoch, phase='fenced',
        binding_json=binding, binding_sha256=checksum))
    await store_participant_catalog(session, participant, spec.profiles)
    await install_machines(session, pool.pool_id, spec.machines)
    receipt = dict(schema_version='loom.pool-participant-receipt.v2', operation_id=str(spec.operation_id),
        pool_id=str(pool.pool_id), participant_id=str(participant.participant_id), incarnation=str(participant.incarnation),
        generation=1, state='enrolled', binding_sha256=checksum)
    await retain_operation(session, document, receipt, pool_id=pool.pool_id, participant_id=participant.participant_id)
    await session.execute(insert(NebiusPoolParticipantLifecycle).values(participant_id=participant.participant_id,
        pool_id=pool.pool_id, head_operation_id=spec.operation_id, generation=1, state='enrolled', binding_sha256=checksum))
    return receipt


async def _readiness(session: AsyncSession, pool: NebiusPoolBinding, selected: NebiusPoolParticipant,
                     proof: PoolParticipantReadinessV2 | None) -> None:
    if proof is None:
        raise ValueError('pool_participant_readiness_missing')
    await qualify_runtime_readiness(session, pool, proof)
    catalog = await session.get(NebiusPoolParticipantCatalog, (selected.participant_id, selected.binding_revision),
        populate_existing=True)
    if (proof.participant_binding_sha256 != selected.binding_sha256 or catalog is None
            or catalog.pool_id != pool.pool_id or catalog.document_sha256 != proof.catalog_sha256
            or digest(catalog.document_json) != proof.catalog_sha256):
        raise ValueError('pool_participant_readiness_unqualified')


async def transition_participant(session: AsyncSession, spec: PoolParticipantOperationV2) -> dict[str, Any]:
    spec = PoolParticipantOperationV2.model_validate(spec.model_dump())
    document = spec.model_dump(mode='json')
    retained = await replay(session, spec.operation_id, document)
    if retained is not None:
        return retained
    pool = await _pool(session, spec.pool_id, spec.pool_authority_sha256)
    selected = await session.get(NebiusPoolParticipant, spec.participant_id, with_for_update=True, populate_existing=True)
    head = await session.get(NebiusPoolParticipantLifecycle, spec.participant_id, with_for_update=True, populate_existing=True)
    if (selected is None or head is None or selected.pool_id != pool.pool_id or head.pool_id != pool.pool_id
            or selected.incarnation != spec.incarnation or selected.admission_epoch != pool.admission_epoch
            or selected.binding_sha256 != spec.expected_binding_sha256 or head.binding_sha256 != selected.binding_sha256
            or digest(selected.binding_json) != selected.binding_sha256):
        raise ValueError('pool_participant_operation_binding_changed')
    if head.head_operation_id != spec.expected_head:
        raise ValueError('pool_participant_predecessor_changed')
    allowed = {'ready': ({'enrolled', 'suspended'}, 'ready'), 'activate': ({'ready'}, 'active'),
        'resume': ({'suspended'}, 'active'), 'suspend': ({'active'}, 'suspended'),
        'retire': ({'enrolled', 'ready', 'suspended'}, 'retiring'), 'complete_retirement': ({'retiring'}, 'retired'),
        'begin_release': ({'active'}, 'suspended'), 'commit_release': ({'suspended'}, 'ready')}
    sources, destination = allowed[spec.action]
    if head.state not in sources:
        raise ValueError('pool_participant_state_conflict')
    outstanding = (await session.scalars(select(NebiusPoolRequest).where(
        NebiusPoolRequest.participant_id == spec.participant_id,
        NebiusPoolRequest.phase.in_((*CHARGED_PHASES, 'waiting'))).with_for_update())).all()
    charged = [row for row in outstanding if row.phase in CHARGED_PHASES]
    waiting = [row for row in outstanding if row.phase == 'waiting']
    if spec.action in {'ready', 'complete_retirement', 'commit_release'} and charged:
        raise ValueError('pool_participant_outstanding_work')
    if spec.action == 'complete_retirement' and waiting:
        raise ValueError('pool_participant_waiting_work')
    if spec.action == 'retire':
        if {(row.request_id, row.request_sha256) for row in waiting} != {
                (row.request_id, row.request_sha256) for row in spec.cancel_waiting}:
            raise ValueError('pool_participant_waiting_disposition_required')
        if waiting:
            await session.execute(update(NebiusPoolRequest).where(
                NebiusPoolRequest.request_id.in_([row.request_id for row in waiting]), NebiusPoolRequest.phase == 'waiting'
            ).values(phase='cancelled_unstarted'))
    if spec.action == 'begin_release':
        assert spec.replacement is not None
        await qualify_release(session, pool, PoolParticipantV1.model_validate(selected.binding_json), spec.replacement)
    if spec.action == 'commit_release':
        predecessor = await session.get(NebiusPoolLifecycleOperation, head.head_operation_id)
        if predecessor is None or predecessor.request_json.get('action') != 'begin_release':
            raise ValueError('pool_release_predecessor_unqualified')
        release = PoolParticipantReleaseV2.model_validate(predecessor.request_json['replacement'])
        await qualify_release(session, pool, PoolParticipantV1.model_validate(selected.binding_json), release)
        following = release.participant.model_dump(mode='json')
        await session.execute(update(NebiusPoolParticipant).where(NebiusPoolParticipant.participant_id == selected.participant_id)
            .values(binding_revision=release.participant.binding_revision, binding_json=following, binding_sha256=digest(following)))
        await store_participant_catalog(session, release.participant, release.profiles)
    if spec.action in {'ready', 'activate', 'resume', 'commit_release'}:
        await _readiness(session, pool, selected, spec.readiness)
    if spec.action in {'activate', 'resume'}:
        if pool.mode != 'global':
            raise ValueError('pool_participant_requires_global_authority')
        await read_connected_capacity(session, pool.pool_id, await _clock(session))
    # Intake suspension must leave already-charged execution/cleanup authority on.
    phase = selected.phase if destination == 'retiring' else ('active' if destination in {'active', 'suspended'} else 'fenced')
    await session.execute(update(NebiusPoolParticipant).where(NebiusPoolParticipant.participant_id == selected.participant_id)
        .values(phase=phase))
    if destination == 'retired':
        await session.execute(update(NebiusPoolMachine).where(NebiusPoolMachine.participant_id == selected.participant_id)
            .values(phase='revoked'))
    receipt = dict(schema_version='loom.pool-participant-receipt.v2', operation_id=str(spec.operation_id),
        pool_id=str(pool.pool_id), participant_id=str(selected.participant_id), incarnation=str(selected.incarnation),
        generation=head.generation + 1, state=destination, binding_sha256=selected.binding_sha256)
    await retain_operation(session, document, receipt, pool_id=pool.pool_id, participant_id=selected.participant_id)
    await session.execute(update(NebiusPoolParticipantLifecycle).where(
        NebiusPoolParticipantLifecycle.participant_id == selected.participant_id,
        NebiusPoolParticipantLifecycle.head_operation_id == spec.expected_head,
    ).values(head_operation_id=spec.operation_id, generation=receipt['generation'], state=destination,
        binding_sha256=selected.binding_sha256))
    return receipt
