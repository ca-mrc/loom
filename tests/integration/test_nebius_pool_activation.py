"""A retained grant/claim is not permission to activate after the claim expires."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import select, text, update
from sqlalchemy.exc import DBAPIError

from loom.db.schema import TaskImageMaterialization, Trial
from tests.integration.test_nebius_pool_build_outbox import counts, grant, local_setup, outbox
from tests.integration.test_nebius_pool_control import action, operate
from tests.integration.test_nebius_pool_observation_registry import sessions as sessions
from tests.integration.test_nebius_pool_participant_http import client, setup


def consent(request, *, not_after=None):
    from loom.nebius_pool_contract import PoolActivationV1

    return PoolActivationV1(action=action(request), not_after=not_after or request.deadline_at)


async def attached(sessions):
    participant, request, trial = await local_setup(sessions)
    journal = outbox(sessions, participant)
    await journal.remember(request)
    receipt = grant(request)
    await journal.accept_grant(request.key, receipt)
    return participant, request, trial, journal, receipt


async def test_activation_intent_is_durable_and_never_extends_the_original_lease(sessions):
    participant, request, _, journal, _ = await attached(sessions)
    saved = await journal.begin_activation(request.key)
    assert saved.phase == "activation_pending" and saved.activation.action == saved.action
    async with sessions() as session:
        row = await session.get(TaskImageMaterialization, request.key.local_work_id)
        assert saved.activation.not_after == min(row.lease_expires_at, request.deadline_at)
    async with sessions.begin() as session:
        await session.execute(update(TaskImageMaterialization).values(lease_expires_at=request.deadline_at))
    assert await outbox(sessions, participant).begin_activation(request.key) == saved
    assert await counts(sessions, request.key.local_work_id) == (1, 1, 1)


@pytest.mark.parametrize("damage", ["lease", "epoch", "owner", "state", "source", "cancelled", "closed", "participant"])
async def test_retained_attachment_does_not_authorize_a_stale_activation(sessions, damage):
    participant, request, trial, journal, receipt = await attached(sessions)
    async with sessions.begin() as session:
        if damage == "cancelled":
            await session.execute(update(Trial).where(Trial.id == trial).values(cancellation_requested_at=datetime.now(UTC)))
        elif damage == "closed":
            await session.execute(text("INSERT INTO nebius_rollout_guard(id,owner,candidate_sha) VALUES(1,'test',:sha)"), {"sha": "a" * 40})
        elif damage != "participant":
            changes = {"lease": {"lease_expires_at": datetime.now(UTC) - timedelta(seconds=1)},
                "epoch": {"lease_epoch": 2}, "owner": {"claimed_by": "other"}, "state": {"state": "queued"},
                "source": {"task_source": "s3://foreign/"}}[damage]
            await session.execute(update(TaskImageMaterialization).where(TaskImageMaterialization.id == request.key.local_work_id).values(**changes))
    if damage == "participant":
        journal = outbox(sessions, participant.model_copy(update={"admission_epoch": participant.admission_epoch + 1}))
    result = await journal.begin_activation(request.key)
    assert result.phase == "cancel_pending" and result.activation is None
    assert result.reservation_id == receipt.reservation_id
    assert (await counts(sessions, request.key.local_work_id))[1:] == (1, 1)


async def test_expired_saved_consent_cannot_be_renewed_by_a_heartbeat(sessions, monkeypatch):
    _, request, _, journal, _ = await attached(sessions)
    saved = await journal.begin_activation(request.key)
    # Re-check against actual DB time, not the request's much later runtime deadline.
    from loom_execution_actuator import pool_outbox

    assert saved.activation is not None
    later = saved.activation.not_after + timedelta(seconds=1)

    async def clock(_session):
        return later

    monkeypatch.setattr(pool_outbox, "_clock", clock)
    async with sessions.begin() as session:
        await session.execute(update(TaskImageMaterialization).values(lease_expires_at=later + timedelta(minutes=1)))
    result = await journal.begin_activation(request.key)
    assert result.phase == "cancel_pending" and result.activation == saved.activation


async def test_activation_receipt_after_local_cancel_requires_stop_not_refund(sessions):
    _, request, _, journal, reserved = await attached(sessions)
    pending = await journal.begin_activation(request.key)
    await journal.request_cancel(request.key)
    activated = reserved.model_copy(update={"phase": "create_intent", "plan_sha256": "d" * 64})
    result = await journal.confirm_activation(request.key, activated)
    assert result.phase == "stop_pending" and result.activation == pending.activation
    assert result.activated == activated and result.attempt_id == pending.attempt_id
    assert await journal.confirm_activation(request.key, activated) == result
    with pytest.raises(ValueError):
        await journal.confirm_cancel(request.key, reserved.model_copy(update={"phase": "cancelled_unstarted"}))
    assert await counts(sessions, request.key.local_work_id) == (1, 1, 1)


@pytest.mark.parametrize("damage", ["reserved", "foreign", "no_intent"])
async def test_activation_confirmation_requires_exact_grant_and_saved_intent(sessions, damage):
    _, request, _, journal, reserved = await attached(sessions)
    if damage != "no_intent":
        await journal.begin_activation(request.key)
    receipt = reserved.model_copy(update={"phase": "create_intent", "plan_sha256": "d" * 64})
    if damage == "reserved":
        receipt = reserved
    elif damage == "foreign":
        receipt = receipt.model_copy(update={"reservation_id": uuid4()})
    with pytest.raises(ValueError):
        await journal.confirm_activation(request.key, receipt)


async def test_sql_retains_activation_consent_and_receipt(sessions):
    from loom.db.nebius_pool_outbox_schema import NebiusPoolBuildOutbox

    _, request, _, journal, reserved = await attached(sessions)
    await journal.begin_activation(request.key)
    await journal.confirm_activation(request.key, reserved.model_copy(update={"phase": "create_intent", "plan_sha256": "d" * 64}))
    async with sessions.begin() as session:
        for changes in ({"activation_json": None}, {"activation_json": {}}, {"activated_json": {}}, {"phase": "attached"}):
            with pytest.raises(DBAPIError):
                async with session.begin_nested():
                    await session.execute(update(NebiusPoolBuildOutbox).values(**changes))


@pytest.mark.parametrize("bad", ["expired", "past_deadline", "naive", "missing"])
async def test_real_manager_rejects_missing_or_invalid_activation_consent(sessions, tmp_path, bad):
    app, raw, token, _, executions, _ = await setup(sessions, tmp_path)
    request = executions[0]
    body = {"schema_version": "loom.pool-activation.v1", "action": action(request).model_dump(mode="json"),
        "not_after": {"expired": datetime.now(UTC) - timedelta(seconds=1),
            "past_deadline": request.deadline_at + timedelta(seconds=1),
            "naive": datetime.now().replace(tzinfo=None), "missing": request.deadline_at}[bad].isoformat()}
    if bad == "missing":
        body = body["action"]
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="https://management.example") as http:
        management = client(http, token)
        await management.prepare(request)
        response = await http.post(f"/internal/pools/v1/{request.pool_id}/activate", json=body,
            headers={"Authorization": "Bearer " + raw})
        assert response.status_code in {409, 422}
        assert (await management.status(action(request))).phase == "reserved"


async def test_manager_freezes_consent_and_replay_does_not_extend_it(sessions, tmp_path):
    from loom.db.nebius_pool_schema import NebiusPoolRequest
    from loom_execution_actuator.pool_client import PoolRequestUnconfirmedError

    app, _, token, _, executions, _ = await setup(sessions, tmp_path)
    request = executions[0]
    activation = consent(request, not_after=request.deadline_at - timedelta(seconds=1))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app)) as http:
        management = client(http, token)
        await management.prepare(request)
        receipt = await management.activate(activation)
        assert await management.activate(activation) == receipt
        with pytest.raises(PoolRequestUnconfirmedError):
            await management.activate(consent(request))
    async with sessions() as session:
        row = await session.get(NebiusPoolRequest, receipt.reservation_id)
        assert row.plan_json["activation"] == activation.model_dump(mode="json")


async def test_expiry_between_preflight_and_intent_write_cannot_activate(sessions, monkeypatch):
    from loom_service.pool_management import control
    from tests.integration.test_nebius_pool_build_admission import mixed_setup
    from tests.integration.test_nebius_pool_registry import prepare

    _, principals, executions, _, profiles, _ = await mixed_setup(sessions)
    request = executions[0]
    await prepare(sessions, principals[0], request, profiles)
    expired = datetime.now(UTC) - timedelta(seconds=1)

    async def earlier_clock(_session):
        return expired - timedelta(seconds=1)

    monkeypatch.setattr(control, "_clock", earlier_clock)
    with pytest.raises(control.PoolControlError):
        await operate(sessions, principals[0], consent(request, not_after=expired), profiles=profiles)
    assert (await operate(sessions, principals[0], action(request), operation="status")).phase == "reserved"
