"""A terminal cancellation fences delayed prepare, even if prepare never arrived."""
from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import httpx
import pytest
from sqlalchemy import func, select, update

from loom.db.nebius_pool_schema import NebiusPoolBinding, NebiusPoolRequest
from loom_service.pool_management.registry import PoolProfiles
from tests.integration.test_nebius_pool_build_admission import mixed_setup, prepare_build
from tests.integration.test_nebius_pool_build_outbox import counts, local_setup, outbox
from tests.integration.test_nebius_pool_control import action, operate
from tests.integration.test_nebius_pool_participant_http import client, setup
from tests.integration.test_nebius_pool_registry import prepare
from tests.integration.test_nebius_pool_registry import sessions as sessions


@pytest.mark.parametrize("kind,expired", [("execution", False), ("execution", True), ("build", False), ("build", True)])
async def test_cancel_before_prepare_never_creates_capacity_or_loses_terminal_receipt(sessions, kind, expired):
    _, principals, executions, builds, _, _ = await mixed_setup(sessions)
    body = builds[0] if kind == "build" else executions[0]
    preparing = prepare_build if kind == "build" else prepare
    if expired:
        body = body.model_copy(update={"deadline_at": datetime.now(UTC) - timedelta(seconds=1)})
    receipt = await operate(sessions, principals[0], action(body), operation="cancel")
    assert receipt.phase == "cancelled_unstarted" and not receipt.capacity_charged
    assert await operate(sessions, principals[0], action(body), operation="status") == receipt
    assert await operate(sessions, principals[0], action(body), operation="cancel") == receipt
    async with sessions.begin() as session:
        await session.execute(update(NebiusPoolBinding).where(NebiusPoolBinding.pool_id == body.pool_id).values(mode="closed"))
    # Replay requires neither capacity nor a live renderer/deadline/intake.
    assert await preparing(sessions, replace(principals[0], pool_mode="closed"), body, PoolProfiles()) == receipt
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(NebiusPoolRequest)) == 0


@pytest.mark.parametrize("change", ["digest", "epoch"])
async def test_early_cancellation_rejects_changed_action_and_changed_prepare(sessions, change):
    from loom_service.pool_management.control import PoolControlError
    from loom_service.pool_management.registry import PoolAdmissionError

    _, principals, executions, _, profiles, _ = await mixed_setup(sessions)
    body = executions[0]
    reference = action(body)
    receipt = await operate(sessions, principals[0], reference, operation="cancel")
    changed = reference.model_copy(update={"request_sha256": "f" * 64} if change == "digest" else {"admission_epoch": 99})
    for operation in ("status", "cancel", "activate"):
        with pytest.raises(PoolControlError):
            await operate(sessions, principals[0], changed, profiles=profiles, operation=operation)
    with pytest.raises(PoolAdmissionError):
        await prepare(sessions, principals[0], body.model_copy(update={"deadline_at": body.deadline_at + timedelta(seconds=1)}), profiles)
    assert await operate(sessions, principals[0], reference, operation="status") == receipt


@pytest.mark.parametrize("kind", ["execution", "build"])
async def test_concurrent_prepare_and_early_cancel_finish_with_one_terminal_identity(sessions, kind):
    _, principals, executions, builds, profiles, _ = await mixed_setup(sessions)
    body = builds[0] if kind == "build" else executions[0]
    preparing = prepare_build if kind == "build" else prepare
    first, cancelled = await asyncio.wait_for(asyncio.gather(
        preparing(sessions, principals[0], body, profiles),
        operate(sessions, principals[0], action(body), operation="cancel")), 10)
    assert first.reservation_id == cancelled.reservation_id
    assert cancelled.phase == "cancelled_unstarted"
    assert await preparing(sessions, principals[0], body, profiles) == cancelled
    assert await operate(sessions, principals[0], action(body), operation="status") == cancelled


async def test_unqualified_early_cancellation_does_not_poison_another_participant(sessions):
    from loom_service.pool_management.control import PoolControlError

    _, principals, executions, _, profiles, _ = await mixed_setup(sessions)
    with pytest.raises(PoolControlError):
        await operate(sessions, principals[1], action(executions[0]), operation="cancel")
    assert (await prepare(sessions, principals[0], executions[0], profiles)).phase == "reserved"


async def test_lost_early_cancel_http_reply_is_recovered_without_preparing_work(sessions, tmp_path):
    from loom_execution_actuator.pool_client import PoolRequestUnconfirmedError

    app, _, token, _, _, builds = await setup(sessions, tmp_path)
    body = builds[0]
    paths = []
    async def lose_reply(response):
        paths.append(response.request.url.path)
        if len(paths) == 1:
            assert response.status_code == 200
            raise httpx.ReadError("discard committed cancellation")
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), event_hooks={"response": [lose_reply]}) as http:
        management = client(http, token)
        with pytest.raises(PoolRequestUnconfirmedError):
            await management.cancel_unstarted(action(body))
        receipt = await client(http, token).status(action(body))
        assert receipt.phase == "cancelled_unstarted"
        assert await client(http, token).cancel_unstarted(action(body)) == receipt
    assert [path.rsplit("/", 1)[-1] for path in paths] == ["cancel-unstarted", "status", "cancel-unstarted"]
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(NebiusPoolRequest)) == 0


async def test_local_selection_recovery_cancels_before_prepare_without_any_build_attempt(sessions, tmp_path):
    app, _, token, participants, _, _ = await setup(sessions, tmp_path)
    participant = participants[0]
    _, selected, _ = await local_setup(sessions, environment_id=participant.environment_id)
    request = selected.model_copy(update={"pool_id": participant.pool_id,
        "admission_epoch": participant.admission_epoch, "participant_revision": participant.binding_revision,
        "key": selected.key.model_copy(update={"participant_id": participant.participant_id})})
    journal = outbox(sessions, participant)
    await journal.remember(request)
    pending = await journal.request_cancel(request.key)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app)) as http:
        management = client(http, token)
        cancelled = await management.cancel_unstarted(pending.action)
        # Both a restarted local controller and a delayed manager prepare
        # converge on the terminal record, without first buying a reservation.
        recovered = await outbox(sessions, participant).confirm_cancel(request.key, cancelled)
        assert recovered.phase == "cancelled"
        assert await management.prepare(request) == cancelled
    assert await counts(sessions, request.key.local_work_id) == (0, 0, 0)
    following = request.model_copy(update={"key": request.key.model_copy(update={"generation": request.key.generation + 1})})
    assert (await journal.remember(following)).phase == "selected"


async def test_early_cancellation_does_not_commit_the_callers_transaction(sessions):
    from loom.db.nebius_pool_schema import NebiusPoolCancellation
    from loom_service.pool_management.control import cancel_unstarted_pool_request

    _, principals, executions, _, _, _ = await mixed_setup(sessions)
    async with sessions() as session:
        result = await cancel_unstarted_pool_request(session, principals[0], action(executions[0]))
        assert result.phase == "cancelled_unstarted"
        async with sessions() as observer:
            assert await observer.scalar(select(func.count()).select_from(NebiusPoolCancellation)) == 0
        await session.rollback()
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(NebiusPoolCancellation)) == 0
