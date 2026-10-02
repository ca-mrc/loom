"""Read one globally qualified node shape without granting any capacity."""
from __future__ import annotations

from datetime import timedelta

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.nebius_pool_schema import NebiusPoolBinding
from loom.execution_runtime_contract import ContainerResourcesV1
from loom.nebius_pool_allocation import PoolNodeAllocationRequestV1, PoolNodeAllocationV1
from loom_service.pool_management.auth import PoolPrincipal, authorize_pool_machine
from loom_service.pool_management.capacity import _read_capacity
from loom_service.pool_management.locks import acquire_pool_mutation_lock


async def node_allocation(session: AsyncSession, principal: PoolPrincipal,
                          request: PoolNodeAllocationRequestV1) -> PoolNodeAllocationV1:
    request = PoolNodeAllocationRequestV1.model_validate_json(request.model_dump_json())
    if (request.pool_id, request.participant_id) != (principal.pool_id, principal.participant_id):
        raise ValueError("pool_allocation_scope_unavailable")
    await acquire_pool_mutation_lock(session)
    pool = await session.scalar(select(NebiusPoolBinding).where(NebiusPoolBinding.pool_id == request.pool_id)
        .with_for_update().execution_options(populate_existing=True))
    await authorize_pool_machine(session, principal, role="participant", pool_id=request.pool_id,
        participant_id=request.participant_id, workload_kind=request.workload_kind)
    if pool is None or pool.mode != "global" or pool.admission_epoch != request.admission_epoch:
        raise ValueError("pool_allocation_scope_unavailable")
    now = (await session.execute(select(func.clock_timestamp()))).scalar_one()
    capacity = await _read_capacity(session, pool, now)
    participant = next((item for item in capacity.participants if item.participant_id == request.participant_id), None)
    if (participant is None or participant.binding_revision != request.participant_revision
            or participant.admission_epoch != request.admission_epoch or capacity.sample is None):
        raise ValueError("pool_allocation_evidence_unavailable")
    participant.target(request.target_id, request.workload_kind)
    sample = capacity.sample
    usable = ContainerResourcesV1(cpu_millis=sample.allocatable.cpu_millis - sample.daemonset_requests.cpu_millis,
        memory_mib=sample.allocatable.memory_mib - sample.daemonset_requests.memory_mib,
        ephemeral_storage_mib=sample.allocatable.storage_mib - sample.daemonset_requests.storage_mib)
    return PoolNodeAllocationV1(scope=request, observation_id=capacity.observation_id,
        observed_at=capacity.observed_at,
        valid_until=capacity.observed_at + timedelta(seconds=capacity.policy.observation_max_age_seconds),
        usable_node=usable)
