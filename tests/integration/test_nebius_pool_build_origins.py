"""Shared task builds derive class from current eligible consumers, not callers."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import update

from loom.db.schema import Batch, TaskImageMaterialization, Trial, TrialTaskImageMaterialization
from loom_control_plane.task_image_materializations import has_nebius_task_image_demand
from tests.integration.test_nebius_pool_build_outbox import counts, grant, local_setup, outbox
from tests.integration.test_nebius_pool_observation_registry import sessions as sessions
from tests.integration.test_nebius_task_image_claims import POOL, _route_values


async def consumer(sessions, identity, template_trial, origin, **changes):
    async with sessions.begin() as session:
        parent = await session.get(Trial, template_trial)
        trial = Trial(id=uuid4(), team_id=parent.team_id, batch_id=parent.batch_id, task_id=parent.task_id,
            config={}, requires_caps=parent.requires_caps, state="queued", pool_origin=origin,
            submitted_at=datetime.now(UTC) + timedelta(seconds=1))
        for name, value in changes.items():
            setattr(trial, name, value)
        session.add(trial)
        await session.flush()
        session.add(TrialTaskImageMaterialization(trial_id=trial.id, materialization_id=identity))
        return trial.id


async def preferred(sessions, participant, identity):
    from loom_execution_actuator.pool_origins import preferred_task_image_origin

    async with sessions() as session:
        return await preferred_task_image_origin(session, materialization_id=identity,
            participant=participant, logical_pool_id=POOL)


async def test_build_uses_highest_live_class_without_duplicating_shared_image(sessions):
    participant, request, trial = await local_setup(sessions, origin_kind="application")
    identity = request.key.local_work_id
    assert await preferred(sessions, participant, identity) == request.origin
    shared = request.origin.model_copy(update={"kind": "environment", "application": None, "submission_id": uuid4()})
    new_trial = await consumer(sessions, identity, trial, shared.model_dump(mode="json"))
    assert await preferred(sessions, participant, identity) == shared
    async with sessions.begin() as session:
        await session.execute(update(Trial).where(Trial.id == new_trial).values(cancellation_requested_at=datetime.now(UTC)))
    assert await preferred(sessions, participant, identity) == request.origin
    assert await counts(sessions, identity) == (0, 0, 0)


@pytest.mark.parametrize("damage", ["unknown", "foreign", "malformed", "cancelled", "terminal", "wrong-pool", "legacy-route", "family"])
async def test_invalid_or_ineligible_higher_consumer_cannot_promote_shared_build(sessions, damage):
    participant, request, trial = await local_setup(sessions, origin_kind="application")
    origin = request.origin.model_copy(update={"kind": "environment", "application": None, "submission_id": uuid4()}).model_dump(mode="json")
    changes = {}
    if damage == "unknown":
        origin = None
    elif damage == "foreign":
        origin["data_environment_id"] = str(uuid4())
    elif damage == "malformed":
        origin = {"kind": "environment"}
    else:
        changes = {"cancelled": {"cancellation_requested_at": datetime.now(UTC)}, "terminal": {"state": "failed"},
            "wrong-pool": {"requires_caps": {"worker_pool": "other"}},
            "legacy-route": _route_values(POOL, "legacy_worker_claim"),
            "family": {"family_key": "other-runtime"}}[damage]
    await consumer(sessions, request.key.local_work_id, trial, origin, **changes)
    assert await preferred(sessions, participant, request.key.local_work_id) == request.origin


async def test_outbox_refuses_unknown_origin_instead_of_trusting_request_class(sessions):
    participant, request, _ = await local_setup(sessions, origin_kind=None)
    with pytest.raises(ValueError):
        await outbox(sessions, participant).remember(request)
    assert await counts(sessions, request.key.local_work_id) == (0, 0, 0)


async def test_request_cannot_promote_personal_consumers_and_equal_class_keeps_oldest(sessions):
    participant, request, trial = await local_setup(sessions, origin_kind="application")
    another = request.origin.model_copy(update={"submission_id": uuid4()})
    await consumer(sessions, request.key.local_work_id, trial, another.model_dump(mode="json"))
    assert await preferred(sessions, participant, request.key.local_work_id) == request.origin
    spoofed = request.model_copy(update={"origin": request.origin.model_copy(update={
        "kind": "environment", "application": None})})
    with pytest.raises(ValueError):
        await outbox(sessions, participant).remember(spoofed)
    assert await counts(sessions, request.key.local_work_id) == (0, 0, 0)


async def test_higher_consumer_arrival_cancels_unstarted_grant_before_reselection(sessions):
    participant, request, trial = await local_setup(sessions, origin_kind="application")
    journal = outbox(sessions, participant)
    await journal.remember(request)
    shared = request.origin.model_copy(update={"kind": "environment", "application": None, "submission_id": uuid4()})
    await consumer(sessions, request.key.local_work_id, trial, shared.model_dump(mode="json"))
    receipt = grant(request)
    result = await journal.accept_grant(request.key, receipt)
    assert result.phase == "cancel_pending" and result.attempt_id is None
    assert await counts(sessions, request.key.local_work_id) == (0, 0, 0)
    replacement = request.model_copy(update={"origin": shared,
        "key": request.key.model_copy(update={"generation": request.key.generation + 1})})
    with pytest.raises(ValueError):
        await journal.remember(replacement)
    await journal.confirm_cancel(request.key, receipt.model_copy(update={"phase": "cancelled_unstarted"}))
    assert (await journal.remember(replacement)).request.origin == shared
    assert (await journal.accept_grant(replacement.key, grant(replacement))).phase == "attached"
    assert await counts(sessions, request.key.local_work_id) == (1, 1, 1)


async def test_explicitly_bound_direct_nebius_trial_has_build_demand_and_origin(sessions):
    participant, request, trial = await local_setup(sessions)
    identity = request.key.local_work_id
    async with sessions.begin() as session:
        row = await session.get(TaskImageMaterialization, identity)
        row.task_config = {**row.task_config, "service_execution": {"logical_pool_id": POOL}}
        await session.execute(update(Trial).where(Trial.id == trial).values(
            batch_id=None, requires_caps={"backend": "nebius", "worker_pool": POOL}))
    async with sessions() as session:
        assert await has_nebius_task_image_demand(session, materialization_id=identity, pool_id=POOL)
    assert await preferred(sessions, participant, identity) == request.origin


async def test_cancelled_parent_batch_removes_consumer_even_with_valid_origin(sessions):
    participant, request, trial = await local_setup(sessions)
    async with sessions.begin() as session:
        parent = await session.get(Trial, trial)
        await session.execute(update(Batch).where(Batch.id == parent.batch_id).values(state="cancelled"))
    assert await preferred(sessions, participant, request.key.local_work_id) is None
