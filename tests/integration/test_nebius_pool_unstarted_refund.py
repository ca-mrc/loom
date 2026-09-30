"""Only confirmed manager cancellation can refund an attached native claim."""
from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import httpx
import pytest
from sqlalchemy import select, text, update
from sqlalchemy.exc import DBAPIError

from loom.db.schema import TaskImageMaterialization, TaskImageMaterializationAttempt
from tests.integration.test_nebius_pool_build_outbox import counts, grant, local_setup, outbox
from tests.integration.test_nebius_pool_observation_registry import sessions as sessions
from tests.integration.test_nebius_pool_participant_http import client, setup


async def attached_setup(sessions):
    participant, request, _ = await local_setup(sessions)
    journal = outbox(sessions, participant)
    await journal.remember(request)
    receipt = grant(request)
    attached = await journal.accept_grant(request.key, receipt)
    return participant, request, journal, receipt, attached


@pytest.mark.parametrize("expired", [False, True])
async def test_confirmed_unstarted_cancel_refunds_once_and_never_reuses_epoch(sessions, expired):
    participant, request, journal, receipt, attached = await attached_setup(sessions)
    if expired:
        async with sessions.begin() as session:
            await session.execute(update(TaskImageMaterialization).where(TaskImageMaterialization.id == request.key.local_work_id)
                .values(lease_expires_at=datetime.now(UTC) - timedelta(seconds=1)))
    pending = await journal.request_cancel(request.key)
    assert pending.attempt_id == attached.attempt_id and await counts(sessions, request.key.local_work_id) == (1, 1, 1)
    cancelled = receipt.model_copy(update={"phase": "cancelled_unstarted"})
    a, b = await asyncio.gather(journal.confirm_cancel(request.key, cancelled),
        outbox(sessions, participant).confirm_cancel(request.key, cancelled))
    assert a == b and a.phase == "cancelled" and a.attempt_id == attached.attempt_id
    assert await counts(sessions, request.key.local_work_id) == (1, 0, 1)
    async with sessions() as session:
        row = await session.get(TaskImageMaterialization, request.key.local_work_id)
        attempt = await session.get(TaskImageMaterializationAttempt, attached.attempt_id)
        assert (row.state, row.claimed_by, row.lease_expires_at, row.next_attempt_at) == ("queued", None, None, None)
        assert attempt.native_build["retry_budget_refunded"] is True
        assert attempt.native_build["failure_reason"] == "build_cancelled"
        assert attempt.native_build["pool_reservation_id"] == str(receipt.reservation_id)
        assert "job_uid" not in attempt.native_build and "job" not in attempt.native_build
    following = request.model_copy(update={"key": request.key.model_copy(update={"generation": request.key.generation + 1}),
        "build": request.build.model_copy(update={"expected_lease_epoch": 1})})
    await journal.remember(following)
    following_attempt = await journal.accept_grant(following.key, grant(following))
    assert following_attempt.attempt_id != attached.attempt_id
    assert await counts(sessions, request.key.local_work_id) == (2, 1, 2)


@pytest.mark.parametrize("damage", ["reserved", "activated", "other-grant"])
async def test_unconfirmed_or_foreign_receipt_cannot_refund_attempt(sessions, damage):
    _, request, journal, receipt, _ = await attached_setup(sessions)
    await journal.request_cancel(request.key)
    invalid = {"reserved": receipt,
        "activated": receipt.model_copy(update={"phase": "create_intent", "plan_sha256": "e" * 64}),
        "other-grant": grant(request, phase="cancelled_unstarted")}[damage]
    with pytest.raises(ValueError):
        await journal.confirm_cancel(request.key, invalid)
    assert await counts(sessions, request.key.local_work_id) == (1, 1, 1)
    assert (await journal.get(request.key)).phase == "cancel_pending"


async def test_confirmed_old_cancellation_does_not_change_a_superseding_claim(sessions):
    _, request, journal, receipt, attached = await attached_setup(sessions)
    await journal.request_cancel(request.key)
    async with sessions.begin() as session:
        await session.execute(update(TaskImageMaterialization).where(TaskImageMaterialization.id == request.key.local_work_id)
            .values(lease_epoch=2, attempt_count=2, claimed_by="new-builder"))
    result = await journal.confirm_cancel(request.key, receipt.model_copy(update={"phase": "cancelled_unstarted"}))
    assert result.phase == "cancelled" and result.attempt_id == attached.attempt_id
    assert await counts(sessions, request.key.local_work_id) == (2, 2, 1)
    async with sessions() as session:
        row = await session.get(TaskImageMaterialization, request.key.local_work_id)
        assert row.state == "claimed" and row.claimed_by == "new-builder"
        attempt = await session.get(TaskImageMaterializationAttempt, attached.attempt_id)
        assert attempt.native_build["retry_budget_refunded"] is False


async def test_refund_and_final_journal_write_are_one_transaction(sessions):
    _, request, journal, receipt, attached = await attached_setup(sessions)
    await journal.request_cancel(request.key)
    async with sessions.begin() as session:
        await session.execute(text("CREATE FUNCTION fail_pool_cancel_finish() RETURNS trigger LANGUAGE plpgsql AS $$ "
            "BEGIN IF NEW.phase = 'cancelled' THEN RAISE EXCEPTION 'injected cancel commit failure'; END IF; RETURN NEW; END $$"))
        await session.execute(text("CREATE TRIGGER fail_pool_cancel_finish BEFORE UPDATE ON nebius_pool_build_outbox "
            "FOR EACH ROW EXECUTE FUNCTION fail_pool_cancel_finish()"))
    with pytest.raises(DBAPIError, match="injected cancel commit failure"):
        await journal.confirm_cancel(request.key, receipt.model_copy(update={"phase": "cancelled_unstarted"}))
    assert await counts(sessions, request.key.local_work_id) == (1, 1, 1)
    assert (await journal.get(request.key)).phase == "cancel_pending"
    async with sessions() as session:
        attempt = await session.get(TaskImageMaterializationAttempt, attached.attempt_id)
        assert attempt.native_build is None


async def test_cancellation_cannot_hide_already_recorded_native_effects(sessions):
    _, request, journal, receipt, attached = await attached_setup(sessions)
    await journal.request_cancel(request.key)
    async with sessions.begin() as session:
        await session.execute(update(TaskImageMaterializationAttempt).where(TaskImageMaterializationAttempt.id == attached.attempt_id)
            .values(native_build={"state": "running", "job_uid": "must-not-hide"}))
    with pytest.raises(ValueError):
        await journal.confirm_cancel(request.key, receipt.model_copy(update={"phase": "cancelled_unstarted"}))
    assert await counts(sessions, request.key.local_work_id) == (1, 1, 1)
    async with sessions() as session:
        assert (await session.scalar(select(TaskImageMaterializationAttempt.native_build))) == {"state": "running", "job_uid": "must-not-hide"}


@pytest.mark.parametrize("activated", [False, True])
async def test_real_manager_decides_whether_attached_claim_can_be_refunded(sessions, tmp_path, activated):
    from loom_execution_actuator.pool_client import PoolRequestUnconfirmedError

    app, _, token, participants, _, _ = await setup(sessions, tmp_path)
    participant = participants[0]
    _, selected, _ = await local_setup(sessions, environment_id=participant.environment_id)
    request = selected.model_copy(update={"pool_id": participant.pool_id,
        "admission_epoch": participant.admission_epoch, "participant_revision": participant.binding_revision,
        "key": selected.key.model_copy(update={"participant_id": participant.participant_id})})
    journal = outbox(sessions, participant)
    await journal.remember(request)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app)) as http:
        management = client(http, token)
        receipt = await management.prepare(request)
        attached = await journal.accept_grant(request.key, receipt)
        if activated:
            await management.activate(attached.action)
        pending = await journal.request_cancel(request.key)
        if activated:
            with pytest.raises(PoolRequestUnconfirmedError):
                await management.cancel_unstarted(pending.action)
            current = await management.status(pending.action)
            assert current.phase == "create_intent" and current.capacity_charged
            with pytest.raises(ValueError):
                await journal.confirm_cancel(request.key, current)
            assert await counts(sessions, request.key.local_work_id) == (1, 1, 1)
            assert (await journal.get(request.key)).phase == "cancel_pending"
        else:
            cancelled = await management.cancel_unstarted(pending.action)
            result = await outbox(sessions, participant).confirm_cancel(request.key, cancelled)
            assert result.phase == "cancelled" and result.attempt_id == attached.attempt_id
            assert await counts(sessions, request.key.local_work_id) == (1, 0, 1)
