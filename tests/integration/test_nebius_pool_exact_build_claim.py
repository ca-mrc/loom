"""A granted immutable selection can claim only its local row and observed epoch."""
from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest
from sqlalchemy import func, select, update

from loom.db.schema import TaskImageMaterialization, TaskImageMaterializationAttempt, Trial
from loom_control_plane.task_image_materializations import claim_task_image_materialization
from tests.integration.test_nebius_task_image_claims import POOL, _seed
from tests.integration.test_nebius_task_image_claims import claim_setup as claim_setup


async def claim(session, identity, epoch=0, **changes):
    return await claim_task_image_materialization(session, **{
        "builder_id": "pool-native", "cpu_arch": "x86_64", "nebius_pool_id": POOL,
        "materialization_id": identity, "expected_lease_epoch": epoch, **changes})


async def test_exact_grant_claim_never_substitutes_an_older_queue_head(claim_setup):
    sessions, team = claim_setup
    async with sessions.begin() as session:
        older, _ = await _seed(session, team, snapshot_values={"created_at": datetime.now(UTC) - timedelta(hours=1)})
        selected, _ = await _seed(session, team)
    async with sessions.begin() as session:
        row = await claim(session, selected)
        assert row.id == selected and (row.lease_epoch, row.attempt_count) == (1, 1)
    async with sessions() as session:
        untouched = await session.get(TaskImageMaterialization, older)
        assert (untouched.state, untouched.lease_epoch, untouched.attempt_count) == ("queued", 0, 0)
        attempts = (await session.scalars(select(TaskImageMaterializationAttempt))).all()
        assert len(attempts) == 1 and attempts[0].materialization_id == selected and attempts[0].lease_epoch == 1


@pytest.mark.parametrize("damage", ["missing", "epoch", "arch", "no-demand", "cancelled", "wrong-pool", "backoff", "owned", "exhausted"])
async def test_changed_selection_consumes_no_attempt_and_cannot_fall_back_to_other_work(claim_setup, damage):
    sessions, team = claim_setup
    now = datetime.now(UTC)
    async with sessions.begin() as session:
        selected, trial = await _seed(session, team, consumer=damage != "no-demand")
        other, _ = await _seed(session, team)
        changes = {
            "epoch": {"lease_epoch": 2}, "arch": {"cpu_arch": "arm64"},
            "backoff": {"next_attempt_at": now + timedelta(minutes=2)},
            "owned": {"state": "claimed", "claimed_by": "someone-else", "lease_epoch": 1,
                      "attempt_count": 1, "lease_expires_at": now + timedelta(minutes=5)},
            "exhausted": {"state": "claimed", "claimed_by": "old", "lease_epoch": 1,
                          "attempt_count": 3, "max_attempts": 3, "lease_expires_at": now - timedelta(minutes=1)},
        }.get(damage, {})
        if changes:
            await session.execute(update(TaskImageMaterialization).where(TaskImageMaterialization.id == selected).values(**changes))
        if damage == "cancelled":
            await session.execute(update(Trial).where(Trial.id == trial).values(cancellation_requested_at=now))
    async with sessions.begin() as session:
        before = await session.get(TaskImageMaterialization, selected)
        snapshot = (before.state, before.lease_epoch, before.attempt_count, before.claimed_by, before.failure_reason)
        assert await claim(session, uuid4() if damage == "missing" else selected,
            epoch=1 if damage in {"owned", "exhausted"} else 0,
            nebius_pool_id="other" if damage == "wrong-pool" else POOL) is None
    async with sessions() as session:
        row = await session.get(TaskImageMaterialization, selected)
        assert (row.state, row.lease_epoch, row.attempt_count, row.claimed_by, row.failure_reason) == snapshot
        other_row = await session.get(TaskImageMaterialization, other)
        assert (other_row.state, other_row.lease_epoch, other_row.attempt_count) == ("queued", 0, 0)
        assert await session.scalar(select(func.count()).select_from(TaskImageMaterializationAttempt)) == 0


async def test_exact_claim_has_no_unrelated_queue_maintenance_side_effect(claim_setup):
    sessions, team = claim_setup
    async with sessions.begin() as session:
        expired, _ = await _seed(session, team, snapshot_values={"state": "claimed", "claimed_by": "other-builder",
            "lease_epoch": 3, "attempt_count": 3, "max_attempts": 3,
            "lease_expires_at": datetime.now(UTC) - timedelta(minutes=1)})
        selected, _ = await _seed(session, team)
    async with sessions.begin() as session:
        assert (await claim(session, selected)).id == selected
    async with sessions() as session:
        row = await session.get(TaskImageMaterialization, expired)
        assert row.state == "claimed" and row.claimed_by == "other-builder" and row.failure_reason is None


async def test_concurrent_same_selection_has_one_claim_and_no_substitution(claim_setup):
    sessions, team = claim_setup
    async with sessions.begin() as session:
        selected, _ = await _seed(session, team)
        other, _ = await _seed(session, team)
    async with sessions() as first, sessions() as second:
        assert (await claim(first, selected)).id == selected
        assert await asyncio.wait_for(claim(second, selected), timeout=2) is None
        await first.commit()
        await second.commit()
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(TaskImageMaterializationAttempt)) == 1
        assert (await session.get(TaskImageMaterialization, other)).attempt_count == 0


async def test_claim_rollback_keeps_selected_epoch_and_attempt_budget_available(claim_setup):
    sessions, team = claim_setup
    async with sessions.begin() as session:
        selected, _ = await _seed(session, team)
    async with sessions() as session:
        assert (await claim(session, selected)).attempt_count == 1
        await session.rollback()
    async with sessions.begin() as session:
        row = await claim(session, selected)
        assert row.attempt_count == row.lease_epoch == 1


@pytest.mark.parametrize("identity,epoch", [(None, 0), (uuid4(), None), (UUID(int=0), 0),
    (uuid4(), True), (uuid4(), -1), (uuid4(), 2**63 - 1), (uuid4(), "0")])
async def test_incomplete_or_invalid_exact_selection_never_becomes_legacy_queue_claim(claim_setup, identity, epoch):
    sessions, team = claim_setup
    async with sessions.begin() as session:
        await _seed(session, team)
    async with sessions.begin() as session:
        with pytest.raises(ValueError):
            await claim(session, identity, epoch)
        assert await session.scalar(select(func.count()).select_from(TaskImageMaterializationAttempt)) == 0
