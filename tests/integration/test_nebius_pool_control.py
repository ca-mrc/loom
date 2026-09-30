"""One frozen activation or an unstarted cancellation, never both or a Job write."""
from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import timedelta
from uuid import uuid4

import pytest
from sqlalchemy import select, update

from loom.db.nebius_pool_schema import NebiusPoolBinding, NebiusPoolRequest
from loom.pipeline.keys import canonical_digest
from tests.integration.test_nebius_pool_build_admission import mixed_setup, prepare_build
from tests.integration.test_nebius_pool_registry import prepare
from tests.integration.test_nebius_pool_registry import sessions as sessions


def action(request):
    from loom.nebius_pool_contract import PoolRequestActionV1

    return PoolRequestActionV1(pool_id=request.pool_id, request_key=request.key,
        admission_epoch=request.admission_epoch,
        request_sha256=canonical_digest(request.model_dump(mode="json")).removeprefix("sha256:"))


async def operate(sessions, principal, request, *, profiles=None, operation="activate"):
    from loom_service.pool_management.control import (
        activate_pool_request,
        cancel_unstarted_pool_request,
        pool_request_status,
    )

    async with sessions.begin() as session:
        if operation == "activate":
            return await activate_pool_request(session, principal, request, profiles=profiles)
        if operation == "cancel":
            return await cancel_unstarted_pool_request(session, principal, request)
        return await pool_request_status(session, principal, request)


@pytest.mark.parametrize("kind", ["execution", "build"])
async def test_activation_freezes_actual_documents_once_and_replay_never_renews_deadline(sessions, monkeypatch, kind):
    from loom_service.pool_management import control
    from loom_service.pool_management.registry import PoolProfiles

    _, principals, executions, builds, profiles, _ = await mixed_setup(sessions)
    body = builds[0] if kind == "build" else executions[0]
    preparing = prepare_build if kind == "build" else prepare
    receipt = await preparing(sessions, principals[0], body, profiles)
    activation_time = body.deadline_at - timedelta(seconds=45)

    async def clock(_session):
        return activation_time

    monkeypatch.setattr(control, "_clock", clock)
    activated = await operate(sessions, principals[0], action(body), profiles=profiles)
    assert activated.phase == "create_intent" and activated.reservation_id == receipt.reservation_id
    assert activated.capacity_charged and activated.job_uid is None
    async with sessions() as session:
        row = await session.get(NebiusPoolRequest, receipt.reservation_id)
        frozen = row.plan_json
        assert canonical_digest(frozen).removeprefix("sha256:") == activated.plan_sha256
        assert frozen["job"]["spec"]["activeDeadlineSeconds"] == 45
        assert frozen["job"]["metadata"]["name"] == f"loom-pool-{receipt.reservation_id.hex}"
        assert (frozen.get("configmap") is not None) is (kind == "build")
        if kind == "build":
            assert frozen["job"]["metadata"]["labels"]["loom.lease-epoch"] == "3"
    activation_time = body.deadline_at + timedelta(hours=1)
    replay = await operate(sessions, principals[0], action(body), profiles=PoolProfiles())
    assert replay == activated
    async with sessions() as session:
        assert (await session.get(NebiusPoolRequest, receipt.reservation_id)).plan_json == frozen


@pytest.mark.parametrize("kind", ["execution", "build"])
async def test_expired_unactivated_grant_is_still_charged_and_never_gets_an_intent(sessions, monkeypatch, kind):
    from loom_service.pool_management import control

    _, principals, executions, builds, profiles, _ = await mixed_setup(sessions)
    body = builds[0] if kind == "build" else executions[0]
    preparing = prepare_build if kind == "build" else prepare
    receipt = await preparing(sessions, principals[0], body, profiles)

    async def clock(_session):
        return body.deadline_at + timedelta(seconds=1)

    monkeypatch.setattr(control, "_clock", clock)
    with pytest.raises(control.PoolControlError):
        await operate(sessions, principals[0], action(body), profiles=profiles)
    async with sessions() as session:
        row = await session.get(NebiusPoolRequest, receipt.reservation_id)
        assert row.phase == "reserved" and row.plan_json is None


@pytest.mark.parametrize("occupied", [0, 3000])
async def test_status_does_not_renew_and_cancelled_wait_or_grant_cannot_activate(sessions, occupied):
    from loom_service.pool_management.control import PoolControlError

    _, principals, executions, _, profiles, _ = await mixed_setup(sessions, occupied_cpu=occupied)
    body = executions[0]
    prepared = await prepare(sessions, principals[0], body, profiles)
    async with sessions() as session:
        row = (await session.scalars(select(NebiusPoolRequest))).one()
        identity, renewed = row.request_id, row.renewed_at
    status = await operate(sessions, principals[0], action(body), operation="status")
    assert status.phase == prepared.phase
    cancelled = await operate(sessions, principals[0], action(body), operation="cancel")
    assert cancelled.phase == "cancelled_unstarted" and not cancelled.capacity_charged
    assert await operate(sessions, principals[0], action(body), operation="cancel") == cancelled
    with pytest.raises(PoolControlError):
        await operate(sessions, principals[0], action(body), profiles=profiles)
    async with sessions() as session:
        row = await session.get(NebiusPoolRequest, identity)
        assert row.renewed_at == renewed and row.plan_json is None


async def test_concurrent_activation_and_cancellation_cannot_both_win(sessions):
    from loom_service.pool_management.control import PoolControlError

    _, principals, executions, _, profiles, _ = await mixed_setup(sessions)
    receipt = await prepare(sessions, principals[0], executions[0], profiles)
    request = action(executions[0])
    results = await asyncio.wait_for(asyncio.gather(
        operate(sessions, principals[0], request, profiles=profiles),
        operate(sessions, principals[0], request, operation="cancel"), return_exceptions=True), timeout=10)
    assert sum(isinstance(result, PoolControlError) for result in results) == 1
    async with sessions() as session:
        row = await session.get(NebiusPoolRequest, receipt.reservation_id)
        assert row.phase in {"create_intent", "cancelled_unstarted"}
        assert (row.plan_json is not None) is (row.phase == "create_intent")


async def test_activation_never_commits_the_callers_transaction(sessions):
    from loom_service.pool_management.control import activate_pool_request

    _, principals, executions, _, profiles, _ = await mixed_setup(sessions)
    receipt = await prepare(sessions, principals[0], executions[0], profiles)
    async with sessions() as session:
        assert (await activate_pool_request(session, principals[0], action(executions[0]), profiles=profiles)).phase == "create_intent"
        async with sessions() as observer:
            assert (await observer.get(NebiusPoolRequest, receipt.reservation_id)).phase == "reserved"
        await session.rollback()
    async with sessions() as session:
        assert (await session.get(NebiusPoolRequest, receipt.reservation_id)).phase == "reserved"


@pytest.mark.parametrize("damage", ["digest", "epoch", "pool", "other_participant", "closed", "selector", "missing_profile", "waiting"])
async def test_unqualified_activation_creates_no_intent(sessions, damage):
    from loom_service.pool_management.control import PoolControlError
    from loom_service.pool_management.registry import PoolProfiles

    participants, principals, executions, _, profiles, _ = await mixed_setup(sessions,
        occupied_cpu=3000 if damage == "waiting" else 0)
    await prepare(sessions, principals[0], executions[0], profiles)
    request, principal = action(executions[0]), principals[0]
    if damage == "digest":
        request = request.model_copy(update={"request_sha256": "f" * 64})
    elif damage == "epoch":
        request = request.model_copy(update={"admission_epoch": 8})
    elif damage == "pool":
        request = request.model_copy(update={"pool_id": uuid4()})
    elif damage == "other_participant":
        principal = principals[1]
    elif damage == "closed":
        async with sessions.begin() as session:
            await session.execute(update(NebiusPoolBinding).where(NebiusPoolBinding.pool_id == request.pool_id).values(mode="closed"))
        principal = replace(principal, pool_mode="closed")
    elif damage == "missing_profile":
        profiles = PoolProfiles()
    elif damage == "selector":
        key = participants[0].targets[0].profile_id
        original = profiles.execution[key]
        profiles = PoolProfiles(profiles.execution | {key: replace(original,
            runtime=replace(original.runtime, node_selector={"nebius.com/node-group-id": "foreign"}))}, profiles.task_images)
    with pytest.raises(PoolControlError):
        await operate(sessions, principal, request, profiles=profiles)
    async with sessions() as session:
        row = (await session.scalars(select(NebiusPoolRequest))).one()
        assert row.phase in {"reserved", "waiting"} and row.plan_json is None
