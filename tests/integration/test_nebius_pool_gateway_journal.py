"""Gateway dispatch permission commits once; fixed documents are never caller input."""
from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from sqlalchemy import select, update

from loom.db.nebius_pool_schema import NebiusPoolBinding, NebiusPoolEffect, NebiusPoolRequest
from loom.db.schema import Token
from tests.integration.test_nebius_pool_build_admission import mixed_setup, prepare_build
from tests.integration.test_nebius_pool_control import action, operate
from tests.integration.test_nebius_pool_registry import machine, prepare
from tests.integration.test_nebius_pool_registry import sessions as sessions


async def setup(sessions, *, build=False, activate=True):
    from loom_service.pool_management.gateway_journal import PoolGatewayJournal

    participants, principals, executions, builds, profiles, _ = await mixed_setup(sessions)
    body = builds[0] if build else executions[0]
    receipt = await (prepare_build if build else prepare)(sessions, principals[0], body, profiles)
    if activate:
        receipt = await operate(sessions, principals[0], action(body, activation=True), profiles=profiles)
    gateway = await machine(sessions, participants[0].pool_id, role="gateway")
    return PoolGatewayJournal(sessions), gateway, receipt, principals[0]


async def test_prepared_document_is_derived_from_frozen_plan_and_replay_is_exact(sessions):
    journal, gateway, receipt, _ = await setup(sessions)
    effect = await journal.prepare_create(gateway, receipt.reservation_id, kind="Job")
    assert effect.phase == "prepared" and effect.dispatch_id is None
    assert effect.document["metadata"]["name"] == f"loom-pool-{receipt.reservation_id.hex}"
    assert effect.document["metadata"]["annotations"]["loom.nebius/pool-effect-id"] == str(effect.effect_id)
    assert await journal.prepare_create(gateway, receipt.reservation_id, kind="Job") == effect
    effect.document["spec"]["parallelism"] = 900  # Mutating a returned copy grants no authority.
    dispatched = await journal.dispatch_create(gateway, effect.effect_id)
    assert dispatched.document["spec"]["parallelism"] == 1
    async with sessions() as session:
        row = await session.get(NebiusPoolEffect, effect.effect_id)
        assert row.phase == "dispatched" and row.dispatch_id == dispatched.dispatch_id
        assert (await session.get(NebiusPoolRequest, receipt.reservation_id)).job_uid is None


async def test_concurrent_dispatch_and_lost_reply_never_issue_a_second_permit(sessions):
    from loom_service.pool_management.gateway_journal import PoolGatewayJournal

    journal, gateway, receipt, _ = await setup(sessions)
    effect = await journal.prepare_create(gateway, receipt.reservation_id, kind="Job")
    permits = await asyncio.wait_for(asyncio.gather(*(journal.dispatch_create(gateway, effect.effect_id) for _ in range(3))), timeout=10)
    assert sum(permit is not None for permit in permits) == 1
    # A new process cannot reinterpret committed-but-unacknowledged dispatch as a retry.
    assert await PoolGatewayJournal(sessions).dispatch_create(gateway, effect.effect_id) is None
    assert (await journal.get_effect(gateway, effect.effect_id)).phase == "dispatched"


async def test_build_job_waits_for_configmap_observation_and_uses_actual_attempt_epoch(sessions):
    from loom_service.pool_management.gateway_journal import PoolGatewayError

    journal, gateway, receipt, _ = await setup(sessions, build=True)
    with pytest.raises(PoolGatewayError):
        await journal.prepare_create(gateway, receipt.reservation_id, kind="Job")
    auxiliary = await journal.prepare_create(gateway, receipt.reservation_id, kind="ConfigMap")
    await journal.dispatch_create(gateway, auxiliary.effect_id)
    with pytest.raises(PoolGatewayError):
        await journal.prepare_create(gateway, receipt.reservation_id, kind="Job")
    await journal.observe_create(gateway, auxiliary.effect_id, uid=uuid4(), resource_version="11")
    job = await journal.prepare_create(gateway, receipt.reservation_id, kind="Job")
    assert job.document["metadata"]["labels"]["loom.lease-epoch"] == "3"
    assert job.document["metadata"]["name"] == auxiliary.document["metadata"]["name"]
    async with sessions() as session:
        assert (await session.get(NebiusPoolRequest, receipt.reservation_id)).phase == "create_intent"


async def test_observation_preserves_first_uid_and_receipt_without_releasing_capacity(sessions):
    from loom_service.pool_management.gateway_journal import PoolGatewayError

    journal, gateway, receipt, _ = await setup(sessions)
    effect = await journal.prepare_create(gateway, receipt.reservation_id, kind="Job")
    uid = uuid4()
    with pytest.raises(PoolGatewayError):
        await journal.observe_create(gateway, effect.effect_id, uid=uid, resource_version="11")
    await journal.dispatch_create(gateway, effect.effect_id)
    observed = await journal.observe_create(gateway, effect.effect_id, uid=uid, resource_version="11")
    assert observed.phase == "observed"
    assert await journal.observe_create(gateway, effect.effect_id, uid=uid, resource_version="12") == observed
    with pytest.raises(PoolGatewayError):
        await journal.observe_create(gateway, effect.effect_id, uid=uuid4(), resource_version="13")
    async with sessions() as session:
        row = await session.get(NebiusPoolRequest, receipt.reservation_id)
        assert row.phase == "observed" and row.job_uid == uid and row.cleanup_observation_id is None


@pytest.mark.parametrize("status", [409, 422, 403, 429, 500])
async def test_only_definite_rejection_is_recorded_and_never_authorizes_resend(sessions, status):
    from loom_service.pool_management.gateway_journal import PoolGatewayError

    journal, gateway, receipt, _ = await setup(sessions)
    effect = await journal.prepare_create(gateway, receipt.reservation_id, kind="Job")
    await journal.dispatch_create(gateway, effect.effect_id)
    if status in {409, 422}:
        rejected = await journal.reject_create(gateway, effect.effect_id, status_code=status)
        assert rejected.phase == "rejected" and rejected.rejection_status == status
    else:
        with pytest.raises(PoolGatewayError):
            await journal.reject_create(gateway, effect.effect_id, status_code=status)
        assert (await journal.get_effect(gateway, effect.effect_id)).phase == "dispatched"
    assert await journal.dispatch_create(gateway, effect.effect_id) is None
    async with sessions() as session:
        assert (await session.get(NebiusPoolRequest, receipt.reservation_id)).phase == "create_intent"


@pytest.mark.parametrize("damage", ["participant", "reserved", "closed", "revoked", "foreign_request", "unsupported"])
async def test_unqualified_gateway_cannot_prepare_fixed_effect(sessions, damage):
    from loom_service.pool_management.gateway_journal import PoolGatewayError

    journal, gateway, receipt, participant = await setup(sessions, activate=damage != "reserved")
    reservation_id, kind = receipt.reservation_id, "Job"
    if damage == "participant":
        gateway = participant
    elif damage == "closed":
        async with sessions.begin() as session:
            await session.execute(update(NebiusPoolBinding).where(NebiusPoolBinding.pool_id == gateway.pool_id).values(mode="closed"))
        gateway = replace(gateway, pool_mode="closed")
    elif damage == "revoked":
        async with sessions.begin() as session:
            await session.execute(update(Token).where(Token.token_hash == gateway.token_hash).values(revoked_at=datetime.now(UTC)))
    elif damage == "foreign_request":
        reservation_id = uuid4()
    elif damage == "unsupported":
        kind = "Secret"
    with pytest.raises(PoolGatewayError):
        await journal.prepare_create(gateway, reservation_id, kind=kind)
    async with sessions() as session:
        assert (await session.scalars(select(NebiusPoolEffect))).all() == []


async def test_intake_closure_allows_readback_not_a_new_create_dispatch(sessions):
    from loom_service.pool_management.gateway_journal import PoolGatewayError

    journal, gateway, receipt, _ = await setup(sessions)
    effect = await journal.prepare_create(gateway, receipt.reservation_id, kind="Job")
    async with sessions.begin() as session:
        await session.execute(update(NebiusPoolBinding).where(NebiusPoolBinding.pool_id == gateway.pool_id).values(mode="closed"))
    gateway = replace(gateway, pool_mode="closed")
    assert (await journal.get_effect(gateway, effect.effect_id)).phase == "prepared"
    with pytest.raises(PoolGatewayError):
        await journal.dispatch_create(gateway, effect.effect_id)
