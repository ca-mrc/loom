"""Real native queue selection captures immutable requests, not attempts."""
from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import httpx
import pytest
from sqlalchemy import select, update

from loom.db.nebius_pool_outbox_schema import NebiusPoolBuildOutbox
from loom.db.schema import TaskImageMaterialization, TaskImageMaterializationAttempt, Trial
from tests.integration.test_nebius_pool_build_outbox import counts, local_setup, outbox
from tests.integration.test_nebius_pool_build_runtime import Reader
from tests.integration.test_nebius_pool_observation_registry import sessions as sessions
from tests.integration.test_nebius_pool_participant_http import client, setup


def selector(journal):
    from loom_execution_actuator.pool_build_selection import PoolBuildSelector

    return PoolBuildSelector(outbox=journal, target_id="native", deadline_seconds=1800)


async def test_concurrent_queue_selection_retains_one_source_and_no_attempt(sessions):
    participant, original, _ = await local_setup(sessions)
    journal = outbox(sessions, participant)
    results = await asyncio.gather(selector(journal).select_next(), selector(journal).select_next())
    selected = [item for item in results if item is not None]
    assert len(selected) == 1
    handoff = selected[0]
    assert handoff.phase == "selected" and handoff.request.key.local_work_id == original.key.local_work_id
    assert handoff.request.key.generation == 1 and handoff.request.build == original.build
    assert handoff.request.origin == original.origin
    assert 1750 < (handoff.request.deadline_at - datetime.now(UTC)).total_seconds() <= 1800
    assert await counts(sessions, original.key.local_work_id) == (0, 0, 0)
    assert await selector(journal).select_next() is None
    assert (await journal.pending()) == (handoff,)


@pytest.mark.parametrize("invalid", ["cancelled", "foreign", "no-origin", "backoff", "exhausted", "ready", "unsupported", "legacy-effect"])
async def test_ineligible_or_unsupported_head_does_not_hide_later_native_demand(sessions, invalid):
    participant, first, trial = await local_setup(sessions, origin_kind=None if invalid == "no-origin" else "environment")
    async with sessions.begin() as session:
        if invalid in {"cancelled", "foreign", "no-origin"}:
            values = {"cancelled": {"cancellation_requested_at": datetime.now(UTC)},
                "foreign": {"requires_caps": {"worker_pool": "oldlab"}}, "no-origin": {"pool_origin": None}}[invalid]
            if invalid != "no-origin":
                await session.execute(update(Trial).where(Trial.id == trial).values(**values))
        else:
            values = {"backoff": {"next_attempt_at": datetime.now(UTC) + timedelta(hours=1)},
                "exhausted": {"attempt_count": 3}, "ready": {"state": "ready"},
                "unsupported": {"task_source_provenance": {}}, "legacy-effect": {}}[invalid]
            if values:
                await session.execute(update(TaskImageMaterialization).where(TaskImageMaterialization.id == first.key.local_work_id).values(**values))
            if invalid == "legacy-effect":
                session.add(TaskImageMaterializationAttempt(materialization_id=first.key.local_work_id,
                    attempt_number=1, lease_epoch=1, builder_id="legacy", claimed_at=datetime.now(UTC),
                    native_build={"target_id": "native", "state": "running"}))
    _, wanted, _ = await local_setup(sessions, environment_id=participant.environment_id)
    journal = outbox(sessions, participant)
    selected = await selector(journal).select_next()
    assert selected.request.key.local_work_id == wanted.key.local_work_id
    assert selected.request.build == wanted.build
    assert await counts(sessions, wanted.key.local_work_id) == (0, 0, int(invalid == "legacy-effect"))


async def test_new_shared_demand_is_selected_before_older_personal_demand(sessions):
    participant, personal, _ = await local_setup(sessions, origin_kind="application")
    _, shared, _ = await local_setup(sessions, environment_id=participant.environment_id)
    journal = outbox(sessions, participant)
    assert (await selector(journal).select_next()).request.key.local_work_id == shared.key.local_work_id
    assert (await selector(journal).select_next()).request.key.local_work_id == personal.key.local_work_id


async def test_closed_intake_cannot_persist_selection_or_consume_attempt(sessions):
    from loom.nebius_rollout_guard import acquire

    participant, request, _ = await local_setup(sessions)
    async with sessions.begin() as session:
        await acquire(session, owner="queue-test", candidate="a" * 40)
    journal = outbox(sessions, participant)
    assert await selector(journal).select_next() is None
    assert not await journal.pending() and await counts(sessions, request.key.local_work_id) == (0, 0, 0)


async def test_real_queue_drives_management_activation_without_direct_job_writer(sessions, tmp_path):
    from loom_execution_actuator.pool_build_driver import PoolBuildDriver
    from loom_execution_actuator.pool_build_runtime import PoolNativeBuildController

    app, _, token, participants, _, _ = await setup(sessions, tmp_path)
    participant = participants[0]
    _, request, _ = await local_setup(sessions, environment_id=participant.environment_id)
    journal = outbox(sessions, participant)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app)) as http:
        controller = PoolNativeBuildController(driver=PoolBuildDriver(outbox=journal, management=client(http, token)),
            kubernetes=Reader(), selector=selector(journal))
        await controller.run_once()
    async with sessions() as session:
        row = (await session.scalars(select(NebiusPoolBuildOutbox))).one()
        assert row.materialization_id == request.key.local_work_id and row.phase == "active"
        assert row.activated_json["phase"] == "create_intent"
    assert await counts(sessions, request.key.local_work_id) == (1, 1, 1)


async def test_reselection_generation_comes_from_history_not_build_lease(sessions):
    from tests.integration.test_nebius_pool_build_outbox import grant

    participant, original, _ = await local_setup(sessions)
    journal = outbox(sessions, participant)
    saved = await journal.remember(original)
    assert saved.request.key.generation == 7
    await journal.request_cancel(original.key)
    await journal.confirm_cancel(original.key, grant(original, phase="cancelled_unstarted"))
    following = await selector(journal).select_next()
    assert following.request.key.generation == 8 and following.request.build.expected_lease_epoch == 0
    assert following.request.origin == original.origin


async def test_source_drift_before_persist_never_selects_a_different_snapshot(sessions, monkeypatch):
    participant, request, _ = await local_setup(sessions)
    journal = outbox(sessions, participant)
    original = journal.remember

    async def raced(selected):
        async with sessions.begin() as session:
            await session.execute(update(TaskImageMaterialization).where(
                TaskImageMaterialization.id == request.key.local_work_id).values(task_source="s3://other/changed/"))
        return await original(selected)

    monkeypatch.setattr(journal, "remember", raced)
    assert await selector(journal).select_next() is None
    assert not await journal.pending() and await counts(sessions, request.key.local_work_id) == (0, 0, 0)


@pytest.mark.parametrize("retired", [False, True])
async def test_registered_source_selection_uses_real_registration_and_pins_available_source(sessions, tmp_path, retired):
    from loom.db.schema import Task, TaskBundleSourceReference, TrialTaskImageMaterialization
    from loom.task_image_materialization import ensure_task_image_materializations
    from tests.integration.test_task_bundle_source_journal import (
        NOW,
        _module,
        _publish,
        _receipts,
        _spec,
        _upload,
    )

    participant, _request, trial = await local_setup(sessions)
    spec = _spec(tmp_path)
    ticket = await _upload(sessions, spec)
    await _receipts(sessions, ticket)
    await _publish(sessions, ticket)
    async with sessions.begin() as session:
        task = Task(id=spec.catalog_task_id, checksum=spec.manifest.task_checksum,
            config=spec.task_config, source=spec.source_uri, source_provenance=spec.provenance)
        session.add(task)
        await session.flush()
        image = (await ensure_task_image_materializations(session, task_row=task))[0]
        materialization_id = image.id
        await session.execute(update(Trial).where(Trial.id == trial).values(task_id=spec.catalog_task_id))
        session.add(TrialTaskImageMaterialization(trial_id=trial, materialization_id=materialization_id))
        await session.flush()
        await _module().release_task_bundle_reference(session, source_id=spec.id,
            reference_kind="materialization", owner_id=str(materialization_id))
        if retired:
            await _module().release_task_bundle_reference(session, source_id=spec.id, reference_kind="catalog", owner_id="catalog")
            assert await _module().retire_task_bundle_source(session, incarnation_id=ticket.incarnation_id, now=NOW)
    journal = outbox(sessions, participant)
    saved = await selector(journal).select_next()
    if retired:
        assert saved is None and not await journal.pending()
    else:
        assert saved.request.build.source.kind == "registered"
        assert saved.request.build.source.registration == spec
        async with sessions() as session:
            pin = await session.scalar(select(TaskBundleSourceReference).where(
                TaskBundleSourceReference.source_id == spec.id, TaskBundleSourceReference.kind == "materialization"))
            assert pin.owner_id == str(materialization_id)
    assert await counts(sessions, materialization_id) == (0, 0, 0)
