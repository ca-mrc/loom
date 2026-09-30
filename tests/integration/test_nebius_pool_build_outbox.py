"""Real local SQL preserves the selection→grant→exact-claim crash boundary."""
from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import delete, func, inspect, select, update
from sqlalchemy.exc import DBAPIError

from loom.db.schema import TaskImageMaterialization, TaskImageMaterializationAttempt, Team, Trial
from loom.nebius_pool_contract import PoolReceiptV1
from loom.nebius_pool_task_image import PoolTaskImagePrepareV1
from loom.pipeline.keys import canonical_digest
from loom.task_image_materialization import task_image_materialization_key
from tests.integration.test_nebius_pool_observation_registry import sessions as sessions
from tests.integration.test_nebius_task_image_claims import POOL, _seed
from tests.unit.test_nebius_pool_task_image_render import build_inputs


async def local_setup(sessions):
    participant, body, _ = build_inputs()
    team = uuid4()
    async with sessions.begin() as session:
        session.add(Team(id=team, name=str(team)))
        await session.flush()
        identity, trial = await _seed(session, team)
        row = await session.get(TaskImageMaterialization, identity)
        body["key"]["local_work_id"] = identity
        body["build"].update(task_id=row.task_id, expected_lease_epoch=0,
            materialization_key=task_image_materialization_key(task_id=row.task_id,
                task_checksum=body["build"]["task_checksum"], cpu_arch="x86_64"))
        request = PoolTaskImagePrepareV1.model_validate(body)
        for name, value in request.build.claim_snapshot().items():
            setattr(row, name, value)
    return participant, request, trial


def outbox(sessions, participant, **changes):
    from loom_execution_actuator.pool_outbox import PoolBuildOutbox

    return PoolBuildOutbox(sessions=sessions, participant=participant, logical_pool_id=POOL,
                          builder_id="native-global", **changes)


def grant(request, **changes):
    return PoolReceiptV1.model_validate({"reservation_id": uuid4(), "pool_id": request.pool_id,
        "request_key": request.key, "admission_epoch": request.admission_epoch,
        "request_sha256": canonical_digest(request).removeprefix("sha256:"), "phase": "reserved", **changes})


async def counts(sessions, identity):
    async with sessions() as session:
        row = await session.get(TaskImageMaterialization, identity)
        count = await session.scalar(select(func.count()).select_from(TaskImageMaterializationAttempt))
        return row.lease_epoch, row.attempt_count, count


async def test_selection_survives_restart_without_claim_or_attempt(sessions):
    participant, request, _ = await local_setup(sessions)
    saved = await outbox(sessions, participant).remember(request)
    assert saved.phase == "selected" and saved.reservation_id is None and saved.attempt_id is None
    recovered = await outbox(sessions, participant).get(request.key)
    assert recovered == saved and recovered.request == request
    assert await outbox(sessions, participant).remember(request) == saved
    assert await counts(sessions, request.key.local_work_id) == (0, 0, 0)


async def test_lost_grant_reply_then_concurrent_replay_commits_only_one_exact_attempt(sessions):
    participant, request, _ = await local_setup(sessions)
    first = outbox(sessions, participant)
    await first.remember(request)
    receipt = grant(request)
    # Both replicas recover the same durable request and management receipt.
    a, b = await asyncio.wait_for(asyncio.gather(first.accept_grant(request.key, receipt),
        outbox(sessions, participant).accept_grant(request.key, receipt)), timeout=5)
    assert a == b and a.phase == "attached" and a.reservation_id == receipt.reservation_id
    assert await counts(sessions, request.key.local_work_id) == (1, 1, 1)
    async with sessions() as session:
        attempt = await session.get(TaskImageMaterializationAttempt, a.attempt_id)
        assert (attempt.materialization_id, attempt.lease_epoch, attempt.builder_id) == (
            request.key.local_work_id, 1, "native-global")
    assert (await outbox(sessions, participant).get(request.key)).attempt_id == a.attempt_id


@pytest.mark.parametrize("change", ["config", "source", "provenance", "epoch", "cancelled", "backoff", "closed"])
async def test_stale_selection_keeps_grant_for_cancellation_without_consuming_an_attempt(sessions, change):
    participant, request, trial = await local_setup(sessions)
    journal = outbox(sessions, participant)
    await journal.remember(request)
    async with sessions.begin() as session:
        if change == "cancelled":
            await session.execute(update(Trial).where(Trial.id == trial).values(cancellation_requested_at=datetime.now(UTC)))
        elif change == "closed":
            from loom.nebius_rollout_guard import acquire

            assert (await acquire(session, owner="outbox-test", candidate="a" * 40))["status"] == "acquired"
        else:
            values = {"config": {"task_config": {}}, "source": {"task_source": "s3://other/source/"},
                "provenance": {"task_source_provenance": {"bundle_file_metadata_sha256": "sha256:" + "f" * 64}},
                "epoch": {"lease_epoch": 2}, "backoff": {"next_attempt_at": datetime.now(UTC) + timedelta(minutes=5)}}[change]
            await session.execute(update(TaskImageMaterialization).where(TaskImageMaterialization.id == request.key.local_work_id).values(**values))
    receipt = grant(request)
    result = await journal.accept_grant(request.key, receipt)
    assert result.phase == "cancel_pending" and result.reservation_id == receipt.reservation_id
    assert result.attempt_id is None
    assert await counts(sessions, request.key.local_work_id) == (2 if change == "epoch" else 0, 0, 0)


@pytest.mark.parametrize("change", ["digest", "pool", "key", "epoch", "activated"])
async def test_unrelated_or_already_activated_grant_cannot_claim_local_work(sessions, change):
    participant, request, _ = await local_setup(sessions)
    journal = outbox(sessions, participant)
    await journal.remember(request)
    changes = {"digest": {"request_sha256": "c" * 64}, "pool": {"pool_id": uuid4()},
        "key": {"request_key": request.key.model_copy(update={"local_work_id": uuid4()})},
        "epoch": {"admission_epoch": 99}, "activated": {"phase": "create_intent", "plan_sha256": "d" * 64}}[change]
    with pytest.raises(ValueError):
        await journal.accept_grant(request.key, grant(request, **changes))
    assert await counts(sessions, request.key.local_work_id) == (0, 0, 0)
    assert (await journal.get(request.key)).reservation_id is None


async def test_changed_replay_and_overlapping_selection_are_not_priority_promotion(sessions):
    participant, request, _ = await local_setup(sessions)
    journal = outbox(sessions, participant)
    await journal.remember(request)
    changed = request.model_copy(update={"deadline_at": request.deadline_at + timedelta(seconds=1)})
    with pytest.raises(ValueError):
        await journal.remember(changed)
    following = request.model_copy(update={"key": request.key.model_copy(update={"generation": 8})})
    with pytest.raises(ValueError):
        await journal.remember(following)
    # Durable cancel intent also wins against a late successful prepare reply.
    assert (await journal.request_cancel(request.key)).phase == "cancel_pending"
    reserved = grant(request)
    assert (await journal.accept_grant(request.key, reserved)).phase == "cancel_pending"
    cancelled = reserved.model_copy(update={"phase": "cancelled_unstarted"})
    assert (await journal.confirm_cancel(request.key, cancelled)).phase == "cancelled"
    assert (await journal.remember(following)).phase == "selected"
    assert await counts(sessions, request.key.local_work_id) == (0, 0, 0)


async def test_lost_unstarted_cancellation_reply_survives_restart(sessions):
    participant, request, _ = await local_setup(sessions)
    journal = outbox(sessions, participant)
    await journal.remember(request)
    await journal.request_cancel(request.key)
    receipt = grant(request, phase="cancelled_unstarted")
    cancelled = await journal.confirm_cancel(request.key, receipt)
    assert await outbox(sessions, participant).confirm_cancel(request.key, receipt) == cancelled
    with pytest.raises(ValueError):
        await journal.accept_grant(request.key, receipt.model_copy(update={"phase": "reserved"}))


async def test_attached_claim_cannot_cancel_as_unstarted_or_swap_grants(sessions):
    participant, request, _ = await local_setup(sessions)
    journal = outbox(sessions, participant)
    await journal.remember(request)
    receipt = grant(request)
    attached = await journal.accept_grant(request.key, receipt)
    with pytest.raises(ValueError):
        await journal.accept_grant(request.key, grant(request))
    with pytest.raises(ValueError):
        await journal.request_cancel(request.key)
    assert await journal.get(request.key) == attached


async def test_request_and_claim_are_atomic_when_final_journal_write_fails(sessions):
    from sqlalchemy import text

    participant, request, _ = await local_setup(sessions)
    journal = outbox(sessions, participant)
    await journal.remember(request)
    async with sessions.begin() as session:
        await session.execute(text("CREATE FUNCTION fail_outbox_attach() RETURNS trigger LANGUAGE plpgsql AS $$ "
            "BEGIN IF NEW.phase = 'attached' THEN RAISE EXCEPTION 'injected disk failure'; END IF; RETURN NEW; END $$"))
        await session.execute(text("CREATE TRIGGER fail_outbox_attach BEFORE UPDATE ON nebius_pool_build_outbox "
                                   "FOR EACH ROW EXECUTE FUNCTION fail_outbox_attach()"))
    with pytest.raises(DBAPIError):
        await journal.accept_grant(request.key, grant(request))
    assert await counts(sessions, request.key.local_work_id) == (0, 0, 0)
    assert (await journal.get(request.key)).phase == "selected"


async def test_installed_schema_matches_orm_and_retains_selection_evidence(sessions):
    from loom.db.nebius_pool_outbox_schema import NebiusPoolBuildOutbox

    participant, request, _ = await local_setup(sessions)
    await outbox(sessions, participant).remember(request)
    async with sessions.begin() as session:
        connection = await session.connection()
        columns = await connection.run_sync(lambda conn: inspect(conn).get_columns("nebius_pool_build_outbox"))
        assert {column["name"] for column in columns} == set(NebiusPoolBuildOutbox.__table__.columns.keys())
        for mutation in (delete(NebiusPoolBuildOutbox),
            update(NebiusPoolBuildOutbox).values(request_json={}),
            update(NebiusPoolBuildOutbox).values(phase="attached", reservation_id=uuid4())):
            with pytest.raises(DBAPIError):
                async with session.begin_nested():
                    await session.execute(mutation)
