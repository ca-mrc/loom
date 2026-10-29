"""Finite gateway passes retain journal authority and reach past broken effects."""
from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest
from sqlalchemy import func, select, update

from loom.db.nebius_pool_schema import NebiusPoolRequest
from loom.pipeline.keys import canonical_digest
from loom_service.pool_management.gateway_journal import PoolGatewayJournal
from loom_service.pool_management.worker import PoolGatewayWorker
from tests.integration.test_nebius_pool_admission_scale import seed_waiters
from tests.integration.test_nebius_pool_registry import machine, setup
from tests.integration.test_nebius_pool_registry import sessions as sessions


async def pending_gateway(sessions, *, count=1000, batch_size=3, concurrency=2):
    participants, _, requests, _, _ = await setup(sessions)
    await seed_waiters(sessions, participants[0], requests[0], count)
    plan = {"job": {"metadata": {"name": "scale-queue", "namespace": participants[0].execution_namespace.name}}}
    async with sessions.begin() as session:
        ids = list(await session.scalars(select(NebiusPoolRequest.request_id)
            .order_by(NebiusPoolRequest.created_at, NebiusPoolRequest.request_id)))
        await session.execute(update(NebiusPoolRequest).values(phase="reserved", granted_at=func.clock_timestamp()))
        await session.execute(update(NebiusPoolRequest).values(phase="create_intent", plan_json=plan,
            plan_sha256=canonical_digest(plan).removeprefix("sha256:")))
        await session.execute(update(NebiusPoolRequest).where(NebiusPoolRequest.request_id == ids[-1])
            .values(phase="cleanup_intent"))
    gateway = SimpleNamespace(journal=PoolGatewayJournal(sessions))
    principal = await machine(sessions, participants[0].pool_id, role="gateway")
    return PoolGatewayWorker(gateway=gateway, principal=principal,
        batch_size=batch_size, concurrency=concurrency), ids


async def test_gateway_bounds_each_phase_rotates_errors_and_replays_on_restart(sessions):
    worker, ids = await pending_gateway(sessions)
    visited, active, peak = [], 0, 0

    async def reconcile(identity):
        nonlocal active, peak
        visited.append(identity)
        active += 1
        peak = max(peak, active)
        try:
            await asyncio.sleep(0)
            if identity == ids[0]:
                raise RuntimeError("effect unavailable")
        finally:
            active -= 1

    worker._reconcile = reconcile
    with pytest.raises(RuntimeError, match="unavailable"):
        await worker.run_once()
    assert visited[0] == ids[-1]  # Cleanup remains visible behind 999 creates.
    assert set(visited) == {ids[-1], *ids[:3]} and peak == 2
    visited.clear()
    await worker.run_once()
    assert set(visited) == {ids[-1], *ids[3:6]}
    restarted = PoolGatewayWorker(gateway=worker.gateway, principal=worker.principal, batch_size=3)
    assert [identity async for identity in restarted._pending()] == [ids[-1], *ids[:3]]
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(NebiusPoolRequest)) == 1000
        assert await session.scalar(select(func.count()).select_from(NebiusPoolRequest)
            .where(NebiusPoolRequest.phase.in_(("create_intent", "cleanup_intent")))) == 1000


@pytest.mark.parametrize("batch_size,concurrency", [(0, 1), (101, 1), (1, 0), (1, 17), (True, 1), (1, True)])
def test_gateway_rejects_unbounded_or_invalid_worker_settings(batch_size, concurrency):
    with pytest.raises(ValueError):
        PoolGatewayWorker(gateway=None, principal=None, batch_size=batch_size, concurrency=concurrency)


@pytest.mark.parametrize("phase", ["create_intent", "cleanup_intent"])
async def test_gateway_retries_old_effect_under_continuous_new_arrivals(sessions, phase):
    from loom.db.nebius_pool_schema import NebiusPoolParticipant
    from loom.nebius_pool_contract import PoolParticipantV1
    from loom.nebius_pool_workload import PoolExecutionPrepareV1

    worker, ids = await pending_gateway(sessions, count=5, batch_size=2)
    if phase == "cleanup_intent":
        async with sessions.begin() as session:
            await session.execute(update(NebiusPoolRequest).where(NebiusPoolRequest.phase == "create_intent")
                .values(phase="cleanup_intent"))
    attempts, recovered = 0, False
    async def reconcile(identity):
        nonlocal attempts, recovered
        if identity == ids[0]:
            attempts += 1
            if attempts == 1:
                raise RuntimeError("temporary outage")
            recovered = True
    worker._reconcile = reconcile
    with pytest.raises(RuntimeError, match="temporary"):
        await worker.run_once()
    async with sessions() as session:
        original = await session.get(NebiusPoolRequest, ids[0])
        binding = await session.get(NebiusPoolParticipant, original.participant_id)
        participant = PoolParticipantV1.model_validate(binding.binding_json)
        request = PoolExecutionPrepareV1.model_validate(original.request_json)
        plan, sha = original.plan_json, original.plan_sha256
    # Every turn adds a full batch beyond the original scan's high-water key.
    # An unbounded advancing cursor would never return to the first failure.
    for _ in range(4):
        await seed_waiters(sessions, participant, request, 2)
        async with sessions.begin() as session:
            await session.execute(update(NebiusPoolRequest).where(NebiusPoolRequest.phase == "waiting")
                .values(phase="reserved", granted_at=func.clock_timestamp()))
            await session.execute(update(NebiusPoolRequest).where(NebiusPoolRequest.phase == "reserved")
                .values(phase="create_intent", plan_json=plan, plan_sha256=sha))
            if phase == "cleanup_intent":
                await session.execute(update(NebiusPoolRequest).where(NebiusPoolRequest.phase == "create_intent")
                    .values(phase="cleanup_intent"))
        await worker.run_once()
    assert recovered and attempts >= 2
