from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import pytest
from sqlalchemy import delete, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from loom.db.schema import (
    Batch,
    Task,
    TaskImageMaterialization,
    TaskImageMaterializationAttempt,
    Team,
    Trial,
    TrialTaskImageMaterialization,
)
from loom_control_plane.task_image_materializations import (
    TaskImageLeaseConflictError,
    claim_task_image_materialization,
    has_nebius_task_image_demand,
    heartbeat_task_image_materialization,
)

POOL = "nebius-cpu"


@pytest.fixture
async def claim_setup(postgres_url: str) -> AsyncIterator[tuple[async_sessionmaker[AsyncSession], UUID]]:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    team_id = uuid4()
    async with sessions() as session, session.begin():
        session.add(Team(id=team_id, name=f"nebius-image-claims-{team_id}"))
    try:
        yield sessions, team_id
    finally:
        async with sessions() as session, session.begin():
            task_ids = select(Task.id).where(Task.id.startswith(f"nebius-image-claims/{team_id}/"))
            image_ids = select(TaskImageMaterialization.id).where(TaskImageMaterialization.task_id.in_(task_ids))
            await session.execute(delete(TrialTaskImageMaterialization).where(
                TrialTaskImageMaterialization.materialization_id.in_(image_ids),
            ))
            await session.execute(delete(TaskImageMaterializationAttempt).where(
                TaskImageMaterializationAttempt.materialization_id.in_(image_ids),
            ))
            await session.execute(delete(TaskImageMaterialization).where(TaskImageMaterialization.id.in_(image_ids)))
            await session.execute(delete(Trial).where(Trial.team_id == team_id))
            await session.execute(delete(Batch).where(Batch.team_id == team_id))
            await session.execute(delete(Task).where(Task.id.in_(task_ids)))
            await session.execute(delete(Team).where(Team.id == team_id))
        await engine.dispose()


async def _seed(
    session: AsyncSession,
    team_id: UUID,
    *,
    consumer: bool = True,
    trial_values: dict[str, Any] | None = None,
    batch_values: dict[str, Any] | None = None,
    snapshot_values: dict[str, Any] | None = None,
) -> tuple[UUID, UUID | None]:
    task_id = f"nebius-image-claims/{team_id}/{uuid4()}"
    session.add(Task(id=task_id, checksum="a" * 64, config={}, source="test"))
    image = TaskImageMaterialization(**{
        "id": uuid4(), "materialization_key": uuid4().hex * 2, "task_id": task_id,
        "task_checksum": "a" * 64, "cpu_arch": "x86_64", "task_config": {},
        **(snapshot_values or {}),
    })
    session.add(image)
    await session.flush()
    if not consumer:
        return image.id, None
    batch = Batch(**{
        "id": uuid4(), "team_id": team_id, "name": "image-demand", "task_filter": {},
        "trial_config": {}, "created_by_token_prefix": "test", "backend": "nebius",
        "service_execution_runtime_profile": {"logical_pool_id": POOL},
        **(batch_values or {}),
    })
    session.add(batch)
    await session.flush()
    trial = Trial(**{
        "id": uuid4(), "team_id": team_id, "task_id": task_id, "batch_id": batch.id,
        "config": {}, "requires_caps": {"worker_pool": POOL}, "state": "queued",
        **(trial_values or {}),
    })
    session.add(trial)
    await session.flush()
    session.add(TrialTaskImageMaterialization(trial_id=trial.id, materialization_id=image.id))
    await session.flush()
    return image.id, trial.id


async def test_native_claim_ignores_unusable_consumers_and_preserves_legacy_default(claim_setup) -> None:
    sessions, team_id = claim_setup
    async with sessions() as session, session.begin():
        orphan_id, _ = await _seed(session, team_id, consumer=False, snapshot_values={
            "created_at": datetime.now(UTC) - timedelta(days=1),
        })
        for kwargs in (
            {"trial_values": {"state": "cancelled"}},
            {"trial_values": {"state": "failed"}},
            {"trial_values": {"cancellation_requested_at": datetime.now(UTC)}},
            {"trial_values": {"requires_caps": {"worker_pool": "oldlab"}}},
            {"trial_values": {"family_key": "long-horizon"}},
            {"batch_values": {"backend": "docker", "service_execution_runtime_profile": None}},
            {"batch_values": {"state": "cancelled"}},
            {"batch_values": {"state": "finished"}},
            {"batch_values": {"service_execution_runtime_profile": None}},
            {"snapshot_values": {"cpu_arch": "arm64"}},
            {"snapshot_values": {"task_config": {"service_execution": {"logical_pool_id": "other"}}}},
            {"trial_values": {"batch_id": None}},
            {"trial_values": _route_values("oldlab", "legacy_worker_claim")},
            {"trial_values": _route_values(POOL, "legacy_worker_claim")},
        ):
            image_id, _ = await _seed(session, team_id, **kwargs)
            assert not await has_nebius_task_image_demand(session, materialization_id=image_id, pool_id=POOL)
        live_id, _ = await _seed(session, team_id)
        assert not await has_nebius_task_image_demand(session, materialization_id=live_id, pool_id="other")
    async with sessions() as session, session.begin():
        claimed = await claim_task_image_materialization(
            session, builder_id="native", cpu_arch="x86_64", nebius_pool_id=POOL,
        )
        assert claimed is not None and claimed.id == live_id
        assert await claim_task_image_materialization(
            session, builder_id="native", cpu_arch="x86_64", nebius_pool_id=POOL,
        ) is None
        legacy = await claim_task_image_materialization(session, builder_id="legacy", cpu_arch="x86_64")
        assert legacy is not None and legacy.id == orphan_id
        with pytest.raises(ValueError, match="x86_64"):
            await claim_task_image_materialization(session, builder_id="native", cpu_arch="arm64", nebius_pool_id=POOL)


def _route_values(pool_id: str, adapter: str) -> dict[str, Any]:
    return {
        "execution_route_pool_name": pool_id, "execution_route_generation": 1,
        "execution_route_sha256": "sha256:" + "b" * 64,
        "execution_route_json": {
            "schema_version": "loom.execution-routing-decision.v1",
            "selected_pool_id": pool_id, "selected_adapter_kind": adapter,
        },
    }


async def test_native_claim_uses_frozen_explicit_binding_during_scheduling_backoff(claim_setup) -> None:
    sessions, team_id = claim_setup
    async with sessions() as session, session.begin():
        image_id, trial_id = await _seed(
            session, team_id,
            batch_values={"service_execution_runtime_profile": None},
            snapshot_values={"task_config": {"service_execution": {"logical_pool_id": POOL}}},
            trial_values={**_route_values(POOL, "kubernetes_job"),
                          "next_attempt_at": datetime.now(UTC) + timedelta(minutes=5)},
        )
        trial = await session.get(Trial, trial_id)
        await session.execute(update(Task).where(Task.id == trial.task_id).values(config={"new_revision": True}))
    async with sessions() as session, session.begin():
        assert await has_nebius_task_image_demand(session, materialization_id=image_id, pool_id=POOL)
        claimed = await claim_task_image_materialization(session, builder_id="explicit", cpu_arch="x86_64", nebius_pool_id=POOL)
        assert claimed is not None and claimed.id == image_id


async def test_native_claim_skips_locked_materialization(claim_setup) -> None:
    sessions, team_id = claim_setup
    async with sessions() as session, session.begin():
        ids = {(await _seed(session, team_id))[0] for _ in range(2)}
    async with sessions() as first, sessions() as second:
        claimed = await claim_task_image_materialization(first, builder_id="one", cpu_arch="x86_64", nebius_pool_id=POOL)
        other = await asyncio.wait_for(claim_task_image_materialization(
            second, builder_id="two", cpu_arch="x86_64", nebius_pool_id=POOL,
        ), timeout=2)
        assert claimed is not None and other is not None
        assert {claimed.id, other.id} == ids
        await first.commit()
        await second.commit()


async def test_shared_demand_survives_one_consumer_cancellation(claim_setup) -> None:
    sessions, team_id = claim_setup
    async with sessions() as session, session.begin():
        image_id, first_id = await _seed(session, team_id)
        first = await session.get(Trial, first_id)
        second = Trial(id=uuid4(), team_id=team_id, task_id=first.task_id, batch_id=first.batch_id,
                       config={}, requires_caps={"worker_pool": POOL}, state="queued")
        session.add(second)
        await session.flush()
        session.add(TrialTaskImageMaterialization(trial_id=second.id, materialization_id=image_id))
        second_id = second.id
        claimed = await claim_task_image_materialization(session, builder_id="shared", cpu_arch="x86_64", nebius_pool_id=POOL)
        assert claimed is not None
        epoch = claimed.lease_epoch
    async with sessions() as session, session.begin():
        await session.execute(update(Trial).where(Trial.id == first_id).values(cancellation_requested_at=datetime.now(UTC)))
    async with sessions() as session, session.begin():
        assert await has_nebius_task_image_demand(session, materialization_id=image_id, pool_id=POOL)
        await heartbeat_task_image_materialization(session, materialization_id=image_id, builder_id="shared", lease_epoch=epoch)
    async with sessions() as session, session.begin():
        await session.execute(update(Trial).where(Trial.id == second_id).values(state="cancelled"))
    async with sessions() as session:
        assert not await has_nebius_task_image_demand(session, materialization_id=image_id, pool_id=POOL)
        await session.execute(update(TaskImageMaterialization).where(TaskImageMaterialization.id == image_id).values(
            lease_expires_at=datetime.now(UTC) - timedelta(seconds=1),
        ))
        await session.commit()
        assert await claim_task_image_materialization(session, builder_id="unneeded", cpu_arch="x86_64", nebius_pool_id=POOL) is None


async def test_native_claim_recovers_expired_lease_and_fences_previous_builder(claim_setup) -> None:
    sessions, team_id = claim_setup
    async with sessions() as session, session.begin():
        image_id, _ = await _seed(session, team_id)
        first = await claim_task_image_materialization(session, builder_id="old", cpu_arch="x86_64", nebius_pool_id=POOL)
        assert first is not None
        old_epoch = first.lease_epoch
    async with sessions() as session, session.begin():
        await session.execute(update(TaskImageMaterialization).where(TaskImageMaterialization.id == image_id).values(
            lease_expires_at=datetime.now(UTC) - timedelta(seconds=1),
        ))
    async with sessions() as session, session.begin():
        current = await claim_task_image_materialization(session, builder_id="new", cpu_arch="x86_64", nebius_pool_id=POOL)
        assert current is not None and current.id == image_id
        assert (current.lease_epoch, current.attempt_count) == (old_epoch + 1, 2)
        epoch = current.lease_epoch
    async with sessions() as session:
        with pytest.raises(TaskImageLeaseConflictError):
            await heartbeat_task_image_materialization(session, materialization_id=image_id, builder_id="old", lease_epoch=old_epoch)
        await session.rollback()
        await heartbeat_task_image_materialization(session, materialization_id=image_id, builder_id="new", lease_epoch=epoch)
        await session.commit()


@pytest.mark.parametrize("claim_kind", ["native", "default", "exact"])
async def test_claim_waits_for_every_prior_native_reservation(claim_setup, claim_kind) -> None:
    sessions, team_id = claim_setup
    now = datetime.now(UTC)
    async with sessions() as session, session.begin():
        image_id, _ = await _seed(session, team_id, snapshot_values={
            "lease_epoch": 2, "attempt_count": 2,
        })
        old = TaskImageMaterializationAttempt(
            materialization_id=image_id, attempt_number=1, lease_epoch=1,
            builder_id="old", claimed_at=now,
            native_build={"capacity_reserved_at": now.isoformat(), "capacity_released_at": None},
        )
        session.add_all([old, TaskImageMaterializationAttempt(
            materialization_id=image_id, attempt_number=2, lease_epoch=2,
            builder_id="newer", claimed_at=now,
            native_build={"capacity_reserved_at": now.isoformat(), "capacity_released_at": now.isoformat()},
        )])
        await session.flush()
        old_id = old.id
    options = {"nebius_pool_id": POOL} if claim_kind == "native" else (
        {"materialization_id": image_id, "expected_lease_epoch": 2} if claim_kind == "exact" else {}
    )
    async with sessions() as session, session.begin():
        assert await claim_task_image_materialization(
            session, builder_id="successor", cpu_arch="x86_64", **options,
        ) is None
        row = await session.get(TaskImageMaterialization, image_id)
        assert (row.state, row.lease_epoch, row.attempt_count) == ("queued", 2, 2)
    async with sessions() as session, session.begin():
        old = await session.get(TaskImageMaterializationAttempt, old_id)
        old.native_build = {**old.native_build, "capacity_released_at": now.isoformat()}
    async with sessions() as session, session.begin():
        row = await claim_task_image_materialization(
            session, builder_id="successor", cpu_arch="x86_64", **options,
        )
        assert row is not None and row.id == image_id
        assert (row.state, row.lease_epoch, row.attempt_count) == ("claimed", 3, 3)


async def test_native_expiry_limit_cleanup_keeps_unrelated_builds_untouched(claim_setup) -> None:
    sessions, team_id = claim_setup
    expired = {
        "state": "running", "claimed_by": "expired", "lease_epoch": 1, "attempt_count": 3,
        "max_attempts": 3, "lease_expires_at": datetime.now(UTC) - timedelta(seconds=1),
    }
    async with sessions() as session, session.begin():
        live_id, _ = await _seed(session, team_id, snapshot_values=expired)
        orphan_id, _ = await _seed(session, team_id, consumer=False, snapshot_values=expired)
    async with sessions() as session, session.begin():
        assert await claim_task_image_materialization(session, builder_id="native", cpu_arch="x86_64", nebius_pool_id=POOL) is None
        live = await session.get(TaskImageMaterialization, live_id)
        orphan = await session.get(TaskImageMaterialization, orphan_id)
        assert (live.state, live.failure_reason) == ("failed", "lease_expired")
        assert (orphan.state, orphan.claimed_by) == ("running", "expired")
