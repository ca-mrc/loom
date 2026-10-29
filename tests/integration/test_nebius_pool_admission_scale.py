"""Large durable backlogs must not turn a valid prepare into a generic error."""
from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import func, insert, select

from loom.db.nebius_pool_schema import NebiusPoolRequest
from loom.pipeline.keys import canonical_digest
from tests.integration.test_nebius_pool_registry import (
    prepare,
    setup,
)
from tests.integration.test_nebius_pool_registry import (
    sessions as sessions,
)


async def seed_waiters(sessions, participant, request, count, *, cpu_millis=1500, invalid_target=False, pod_slots=1):
    now = datetime.now(UTC) - timedelta(seconds=5)
    rows = []
    for _ in range(count):
        payload = request.model_dump(mode="json")
        payload["key"]["local_work_id"] = str(uuid4())
        if invalid_target:
            payload["target_id"] = "unknown-target"
        payload["execution"]["requirements"]["cpu_millis"] = cpu_millis
        payload["execution"]["runtime"]["task_resources"]["cpu_millis"] = cpu_millis
        rows.append(dict(request_id=uuid4(), pool_id=participant.pool_id,
            participant_id=participant.participant_id, namespace_uid=participant.execution_namespace.uid,
            workload_kind=request.key.workload_kind, local_work_id=payload["key"]["local_work_id"],
            generation=1, admission_epoch=request.admission_epoch, target_id=payload["target_id"],
            request_sha256=canonical_digest(payload).removeprefix("sha256:"), request_json=payload,
            deadline_at=request.deadline_at, cpu_millis=cpu_millis, memory_mib=2048,
            ephemeral_storage_mib=4096, pod_slots=pod_slots, priority=2, created_at=now, renewed_at=now,
            phase="waiting"))
    async with sessions.begin() as session:
        for offset in range(0, len(rows), 500):
            await session.execute(insert(NebiusPoolRequest), rows[offset:offset + 500])


@pytest.mark.parametrize("count", [1000, 10000])
async def test_higher_priority_prepare_skips_large_younger_class_backlog(sessions, count, monkeypatch):
    import loom_service.pool_management.registry as registry

    participants, principals, requests, profiles, _ = await setup(
        sessions, environment_classes=("development", "production"))
    await seed_waiters(sessions, participants[0], requests[0], count)
    renders = 0
    original = registry._render

    def render(*args, **kwargs):
        nonlocal renders
        renders += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(registry, "_render", render)
    result = await prepare(sessions, principals[1], requests[1], profiles)
    assert result.phase == "reserved"
    assert renders == 1
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(NebiusPoolRequest)) == count + 1


@pytest.mark.parametrize("count", [1000, 10000])
async def test_impossible_prefix_does_not_hide_fitting_tail(sessions, count, monkeypatch):
    import loom_service.pool_management.registry as registry

    participants, principals, requests, profiles, _ = await setup(sessions)
    await seed_waiters(sessions, participants[0], requests[0], count, cpu_millis=4000)
    calls = {"fit": 0, "render": 0, "profiles": 0}
    for name, key in (("require_fit", "fit"), ("_render", "render")):
        original = getattr(registry, name)
        def counted(*args, _original=original, _key=key, **kwargs):
            calls[_key] += 1
            return _original(*args, **kwargs)
        monkeypatch.setattr(registry, name, counted)
    original_profiles = registry.resolve_profiles
    async def resolve(*args, **kwargs):
        calls["profiles"] += 1
        return await original_profiles(*args, **kwargs)
    monkeypatch.setattr(registry, "resolve_profiles", resolve)
    started = time.perf_counter()
    result = await prepare(sessions, principals[1], requests[1], profiles)
    print(f"impossible-prefix count={count} prepare_seconds={time.perf_counter() - started:.4f} calls={calls}")
    assert result.phase == "reserved"
    assert calls["fit"] <= 3 and calls["render"] == 1 and calls["profiles"] == 1
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(NebiusPoolRequest)
            .where(NebiusPoolRequest.phase == "waiting")) == count


async def test_fit_memo_rechecks_when_valid_prior_waiters_protect_more_capacity(sessions, monkeypatch):
    import loom_service.pool_management.registry as registry

    participants, principals, requests, profiles, _ = await setup(sessions)
    await seed_waiters(sessions, participants[0], requests[0], 2)
    generations = []
    original = registry.require_fit
    def fit(*args, **kwargs):
        generations.append(len(kwargs["protected_waits"]))
        return original(*args, **kwargs)
    monkeypatch.setattr(registry, "require_fit", fit)
    result = await prepare(sessions, principals[1], requests[1], profiles)
    assert result.phase == "waiting" and result.reason == "pool_max_nodes_exceeded"
    assert generations == [0, 1, 2]


async def test_waiting_request_cannot_supply_an_observed_reservation_discount(sessions):
    from loom_execution_capacity_collector.contracts import (
        CapacityPlacement,
        ManagedPodPlacement,
        ResourceTotals,
    )
    from tests.execution_placement_fixtures import placement_fixture
    from tests.integration.test_nebius_pool_registry import publish_placement

    participants, principals, requests, profiles, observer = await setup(sessions)
    await seed_waiters(sessions, participants[0], requests[0], 1, invalid_target=True)
    async with sessions() as session:
        prior_id = await session.scalar(select(NebiusPoolRequest.request_id))
    placement = CapacityPlacement.model_validate(placement_fixture(target_id="pool-test", node_cpu=3000,
        node_memory=8192, node_storage=32768, requested_cpu=3000, quota_nodes=1))
    node = placement.nodes[0].model_copy(update={"managed_pods": [ManagedPodPlacement(
        uid="observed-prior", lease_id=f"reservation:{prior_id}", generation=1,
        requests=ResourceTotals(cpu_millis=1500, memory_mib=2048, storage_mib=4096))]})
    from loom_service.pool_management.observations import PoolObservationError
    with pytest.raises(PoolObservationError):
        await publish_placement(sessions, observer, placement.model_copy(update={"nodes": [node]}))
    # The rejected observation cannot replace the legitimate empty-node sample.
    result = await prepare(sessions, principals[1], requests[1], profiles)
    assert result.phase == "reserved"


async def test_retained_invalid_envelope_does_not_block_valid_caller(sessions):
    participants, principals, requests, profiles, _ = await setup(sessions)
    await seed_waiters(sessions, participants[0], requests[0], 1, pod_slots=2)
    result = await prepare(sessions, principals[1], requests[1], profiles)
    assert result.phase == "reserved"


async def test_over_boundary_retry_retains_age_and_reloads_changed_capacity(sessions):
    from loom_execution_capacity_collector.contracts import CapacityPlacement
    from tests.execution_placement_fixtures import placement_fixture
    from tests.integration.test_nebius_pool_registry import publish_placement

    participants, principals, requests, profiles, observer = await setup(sessions, occupied_cpu=3000,
        environment_classes=("development", "production"))
    await seed_waiters(sessions, participants[0], requests[0], 10000)
    first = await prepare(sessions, principals[1], requests[1], profiles)
    assert first.phase == "waiting" and first.reason == "pool_max_nodes_exceeded"
    async with sessions() as session:
        retained = await session.scalar(select(NebiusPoolRequest).where(
            NebiusPoolRequest.local_work_id == requests[1].key.local_work_id,
            NebiusPoolRequest.participant_id == participants[1].participant_id))
        identity, created, renewed = retained.request_id, retained.created_at, retained.renewed_at
    await publish_placement(sessions, observer, CapacityPlacement.model_validate(placement_fixture(
        target_id="pool-test", node_cpu=3000, node_memory=8192, node_storage=32768, quota_nodes=1)))
    second = await prepare(sessions, principals[1], requests[1], profiles)
    assert second.phase == "reserved" and second.reservation_id == identity
    async with sessions() as session:
        saved = await session.get(NebiusPoolRequest, identity)
        assert saved.created_at == created and saved.renewed_at > renewed
        assert await session.scalar(select(func.count()).select_from(NebiusPoolRequest)) == 10001
