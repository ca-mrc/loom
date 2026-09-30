"""Retained-UID deletion intent is one-use and is never capacity release."""
from __future__ import annotations

import asyncio
from dataclasses import replace
from uuid import uuid4

import pytest
from sqlalchemy import select, update

from loom.db.nebius_pool_schema import (
    NebiusPoolBinding,
    NebiusPoolEffect,
    NebiusPoolParticipant,
    NebiusPoolRequest,
)
from tests.integration.test_nebius_pool_gateway_journal import setup
from tests.integration.test_nebius_pool_registry import sessions as sessions


async def begin_cleanup(sessions, reservation_id):
    # Test setup only. Production cleanup must originate from the connected
    # environment outbox after durable output drain, not an owner boolean.
    async with sessions.begin() as session:
        await session.execute(update(NebiusPoolRequest).where(NebiusPoolRequest.request_id == reservation_id).values(phase="cleanup_intent"))


async def observed(sessions, *, kind="Job", cleanup=True):
    journal, principal, receipt, _ = await setup(sessions, build=kind == "ConfigMap")
    create = await journal.prepare_create(principal, receipt.reservation_id, kind=kind)
    await journal.dispatch_create(principal, create.effect_id)
    create = await journal.observe_create(principal, create.effect_id, uid=uuid4(), resource_version="11")
    if cleanup:
        await begin_cleanup(sessions, receipt.reservation_id)
    return journal, principal, receipt, create


@pytest.mark.parametrize("kind", ["Job", "ConfigMap"])
async def test_delete_uses_only_retained_create_uid_and_commits_before_permission(sessions, kind):
    journal, principal, receipt, created = await observed(sessions, kind=kind)
    deletion = await journal.prepare_delete(principal, receipt.reservation_id, kind=kind)
    assert deletion.created == created
    assert deletion.document == {"apiVersion": "v1", "kind": "DeleteOptions", "propagationPolicy": "Background",
                                 "preconditions": {"uid": str(created.observed_uid)}}
    assert deletion.phase == "prepared" and deletion.dispatch_id is None
    assert await journal.prepare_delete(principal, receipt.reservation_id, kind=kind) == deletion
    deletion.document["preconditions"]["uid"] = str(uuid4())
    dispatched = await journal.dispatch_delete(principal, deletion.effect_id)
    assert dispatched.document["preconditions"]["uid"] == str(created.observed_uid)
    async with sessions() as session:
        row = await session.get(NebiusPoolEffect, deletion.effect_id)
        assert row.phase == "dispatched" and row.dispatch_id == dispatched.dispatch_id


async def test_concurrent_delete_dispatch_and_process_loss_cannot_resend(sessions):
    from loom_service.pool_management.gateway_journal import PoolGatewayJournal

    journal, principal, receipt, _ = await observed(sessions)
    deletion = await journal.prepare_delete(principal, receipt.reservation_id, kind="Job")
    permits = await asyncio.gather(*(journal.dispatch_delete(principal, deletion.effect_id) for _ in range(3)))
    assert sum(permit is not None for permit in permits) == 1
    assert await PoolGatewayJournal(sessions).dispatch_delete(principal, deletion.effect_id) is None


@pytest.mark.parametrize("damage", ["not-stopped", "unobserved", "missing", "unsupported"])
async def test_delete_requires_cleanup_intent_and_observed_owned_create(sessions, damage):
    from loom_service.pool_management.gateway_journal import PoolGatewayError

    if damage == "not-stopped":
        journal, principal, receipt, _ = await observed(sessions, cleanup=False)
    else:
        journal, principal, receipt, _ = await setup(sessions)
        if damage == "unobserved":
            create = await journal.prepare_create(principal, receipt.reservation_id, kind="Job")
            await journal.dispatch_create(principal, create.effect_id)
        await begin_cleanup(sessions, receipt.reservation_id)
    with pytest.raises(PoolGatewayError):
        await journal.prepare_delete(principal, receipt.reservation_id, kind="Secret" if damage == "unsupported" else "Job")
    async with sessions() as session:
        assert (await session.scalars(select(NebiusPoolEffect).where(NebiusPoolEffect.effect_key.startswith("delete:")))).all() == []


async def test_delete_reconciliation_retains_history_without_releasing_request(sessions):
    from loom_service.pool_management.gateway_journal import PoolGatewayError

    journal, principal, receipt, created = await observed(sessions)
    deletion = await journal.prepare_delete(principal, receipt.reservation_id, kind="Job")
    with pytest.raises(PoolGatewayError):
        await journal.observe_delete(principal, deletion.effect_id)
    await journal.dispatch_delete(principal, deletion.effect_id)
    deleted = await journal.observe_delete(principal, deletion.effect_id)
    assert deleted.phase == "observed"
    assert await journal.observe_delete(principal, deletion.effect_id) == deleted
    assert await journal.get_delete(principal, deletion.effect_id) == deleted
    assert await journal.dispatch_delete(principal, deletion.effect_id) is None
    async with sessions() as session:
        row = await session.get(NebiusPoolEffect, deletion.effect_id)
        assert row.observed_uid == created.observed_uid and row.observed_resource_version is None
        request = await session.get(NebiusPoolRequest, receipt.reservation_id)
        assert request.phase == "cleanup_intent" and request.cleanup_observation_id is None


@pytest.mark.parametrize("status", [409, 422, 403, 429, 500])
async def test_only_definite_delete_rejection_is_terminal_not_retry_or_release(sessions, status):
    from loom_service.pool_management.gateway_journal import PoolGatewayError

    journal, principal, receipt, _ = await observed(sessions)
    deletion = await journal.prepare_delete(principal, receipt.reservation_id, kind="Job")
    await journal.dispatch_delete(principal, deletion.effect_id)
    if status in {409, 422}:
        rejected = await journal.reject_delete(principal, deletion.effect_id, status_code=status)
        assert rejected.phase == "rejected" and rejected.rejection_status == status
    else:
        with pytest.raises(PoolGatewayError):
            await journal.reject_delete(principal, deletion.effect_id, status_code=status)
        assert (await journal.get_delete(principal, deletion.effect_id)).phase == "dispatched"
    assert await journal.dispatch_delete(principal, deletion.effect_id) is None


async def test_closed_pool_and_fenced_participant_do_not_strand_owned_cleanup(sessions):
    journal, principal, receipt, _ = await observed(sessions)
    deletion = await journal.prepare_delete(principal, receipt.reservation_id, kind="Job")
    async with sessions.begin() as session:
        await session.execute(update(NebiusPoolBinding).where(NebiusPoolBinding.pool_id == principal.pool_id).values(mode="closed"))
        await session.execute(update(NebiusPoolParticipant).where(NebiusPoolParticipant.pool_id == principal.pool_id).values(phase="fenced"))
    principal = replace(principal, pool_mode="closed")
    assert (await journal.dispatch_delete(principal, deletion.effect_id)).phase == "dispatched"
    assert (await journal.observe_delete(principal, deletion.effect_id)).phase == "observed"


async def test_create_and_delete_methods_cannot_confuse_each_others_effects(sessions):
    from loom_service.pool_management.gateway_journal import PoolGatewayError

    journal, principal, receipt, created = await observed(sessions)
    deletion = await journal.prepare_delete(principal, receipt.reservation_id, kind="Job")
    with pytest.raises(PoolGatewayError):
        await journal.dispatch_create(principal, deletion.effect_id)
    with pytest.raises(PoolGatewayError):
        await journal.observe_create(principal, deletion.effect_id, uid=uuid4(), resource_version="11")
    with pytest.raises(PoolGatewayError):
        await journal.dispatch_delete(principal, created.effect_id)
