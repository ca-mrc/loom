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
