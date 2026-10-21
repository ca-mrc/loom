"""Protected platform intake changes, never an environment release side effect.

Opening requires qualified sole-writer evidence from the installed host. Closing
is only allowed after charged work drains: it must not strand reserved creates.
Both changes advance platform policy binding, requiring fresh physical observation
before further admission. Ordinary participant releases never call this path.
"""
from __future__ import annotations

from typing import Any

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.nebius_pool_schema import NebiusPoolBinding, NebiusPoolRequest
from loom_service.pool_management.capacity import CHARGED_PHASES, digest, read_connected_capacity
from loom_service.pool_management.control import _clock
from loom_service.pool_management.intake import scoped_lifecycle
from loom_service.pool_management.lifecycle_contracts import (
    PoolPlatformOperationV2 as PoolPlatformOperationV2,
)
from loom_service.pool_management.lifecycle_journal import replay, retain_operation
from loom_service.pool_management.observation_binding import physical_binding_digest
from loom_service.pool_management.readiness import qualify_runtime_readiness


async def transition_platform(session: AsyncSession, spec: PoolPlatformOperationV2) -> dict[str, Any]:
    spec = PoolPlatformOperationV2.model_validate(spec.model_dump())
    document = spec.model_dump(mode='json')
    retained = await replay(session, spec.operation_id, document)
    if retained is not None:
        return retained
    pool = await session.get(NebiusPoolBinding, spec.pool_id, with_for_update=True, populate_existing=True)
    source, destination = ('closed', 'global') if spec.action == 'open' else ('global', 'closed')
    if (pool is None or pool.installation_id != spec.installation_id or not scoped_lifecycle(pool)
            or pool.mode != source or digest(pool.binding_json) != pool.binding_sha256
            or physical_binding_digest(pool) != spec.pool_authority_sha256
            or pool.binding_json.get('lifecycle_head_operation_id') != str(spec.expected_head)):
        raise ValueError('pool_platform_predecessor_unqualified')
    if await session.scalar(select(NebiusPoolRequest.request_id).where(
            NebiusPoolRequest.pool_id == pool.pool_id, NebiusPoolRequest.phase.in_(CHARGED_PHASES)).limit(1)) is not None:
        raise ValueError('pool_platform_outstanding_work')
    if spec.action == 'open':
        assert spec.readiness is not None
        await qualify_runtime_readiness(session, pool, spec.readiness)
        await read_connected_capacity(session, pool.pool_id, await _clock(session))
    binding = dict(pool.binding_json, lifecycle_head_operation_id=str(spec.operation_id))
    await session.execute(update(NebiusPoolBinding).where(NebiusPoolBinding.pool_id == pool.pool_id).values(
        mode=destination, policy_revision=pool.policy_revision + 1, binding_json=binding, binding_sha256=digest(binding)))
    receipt = dict(schema_version='loom.pool-platform-operation-receipt.v2', operation_id=str(spec.operation_id),
        pool_id=str(pool.pool_id), mode=destination, pool_authority_sha256=physical_binding_digest(pool),
        policy_revision=pool.policy_revision, admission_epoch=pool.admission_epoch)
    await retain_operation(session, document, receipt, pool_id=pool.pool_id, participant_id=None)
    return receipt
