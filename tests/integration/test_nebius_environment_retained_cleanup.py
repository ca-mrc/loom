"""Destroy fences create immediately; resource charges wait for cleanup proof."""

from __future__ import annotations

import asyncio

import pytest
from sqlalchemy import select

from loom.db.nebius_environment_schema import NebiusPlatformReservation
from loom_service.environment_management.registry import ManagementError
from tests.integration.test_nebius_environment_management import (
    environment_registry as environment_registry,
)
from tests.support.nebius_platform import platform_inputs as platform_inputs


async def test_destroy_fences_inflight_create_and_keeps_data_names_and_charges(environment_registry):
    registry, factory, (alice, bob), prepare = environment_registry
    created = await registry.create(principal=alice, idempotency_key="create", prepared=prepare())
    lease = await registry.claim(created.operation_id)
    first = await registry.next_step(lease)
    await registry.confirm_step(lease, first.key, provider_identity="original-namespace")
    async with factory() as session:
        reservation = await session.get(NebiusPlatformReservation, created.environment_id)
        before = (reservation.cpu_millis, reservation.memory_mib, reservation.storage_mib)
    with pytest.raises(ManagementError, match="environment_forbidden"):
        await registry.destroy_retained(created.environment_id, principal=bob, expected_generation=1, idempotency_key="destroy")
    destroyed, replay = await asyncio.gather(*[
        registry.destroy_retained(created.environment_id, principal=alice, expected_generation=1, idempotency_key="destroy")
        for _ in range(2)
    ])
    assert destroyed == replay
    assert destroyed.action == "destroy_retained" and destroyed.phase == "pending"
    assert destroyed.deployment_generation == 2
    state = await registry.status(created.environment_id, principal=alice)
    assert state.registration.desired_state == "destroyed"
    assert state.registration.application_namespace == "loom-dev-alice"
    assert (await registry.get_operation(created.operation_id, principal=alice)).phase == "blocked"
    with pytest.raises(ManagementError, match="stale_operation_lease"):
        await registry.confirm_step(lease, first.key, provider_identity="original-namespace")
    with pytest.raises(ManagementError, match="environment_not_ready"):
        await registry.ready_access(created.environment_id, principal=alice)
    with pytest.raises(ManagementError, match="idempotency_conflict"):
        await registry.destroy_retained(created.environment_id, principal=alice, expected_generation=2, idempotency_key="destroy")
    cleanup_lease = await registry.claim(destroyed.operation_id)
    context = await registry.provisioning_context(cleanup_lease)
    assert context.action == "destroy_retained"
    assert context.source.lease.operation_id == created.operation_id
    assert context.source.registration["desired_state"] == "active"
    assert context.source.identities[first.key] == "original-namespace"
    assert any(doc["kind"] == "Deployment" for doc in context.source.documents.values())
    with pytest.raises(ManagementError, match="operation_resources_incomplete"):
        await registry.complete(cleanup_lease)
    async with factory() as session:
        reservation = (await session.scalars(select(NebiusPlatformReservation))).one()
        assert (reservation.cpu_millis, reservation.memory_mib, reservation.storage_mib) == before
    with pytest.raises(ManagementError, match="environment_name_conflict"):
        await registry.create(principal=alice, idempotency_key="reuse-slug", prepared=prepare())


async def test_destroy_rejects_stale_generation_without_fencing_current_create(environment_registry):
    registry, _, (alice, _), prepare = environment_registry
    created = await registry.create(principal=alice, idempotency_key="create", prepared=prepare())
    with pytest.raises(ManagementError, match="environment_generation_conflict"):
        await registry.destroy_retained(created.environment_id, principal=alice, expected_generation=2, idempotency_key="destroy")
    assert (await registry.status(created.environment_id, principal=alice)).registration.desired_state == "active"
    assert (await registry.get_operation(created.operation_id, principal=alice)).phase == "pending"


async def test_explicit_retry_preserves_intents_and_epoch_but_cannot_revive_destroyed_generation(environment_registry):
    from loom_service.environment_management.worker import EnvironmentWorker
    from tests.integration.test_nebius_environment_worker import ExternalProvider

    registry, _, (alice, bob), prepare = environment_registry
    created = await registry.create(principal=alice, idempotency_key="create", prepared=prepare())
    lease = await registry.claim(created.operation_id)
    first = await registry.next_step(lease)
    await registry.confirm_step(lease, first.key, provider_identity="first-namespace")
    await registry.finish_attempt(lease, error_code="provider_unavailable", retry=False)
    with pytest.raises(ManagementError, match="environment_forbidden"):
        await registry.retry(created.operation_id, principal=bob)
    a, b = await asyncio.gather(registry.retry(created.operation_id, principal=alice), registry.retry(created.operation_id, principal=alice))
    assert a == b and a.operation_id == created.operation_id and a.phase == "pending"
    successor = await registry.claim(created.operation_id)
    assert successor.runner_epoch > lease.runner_epoch
    assert (await registry.provisioning_context(successor)).identities[first.key] == "first-namespace"
    assert (await registry.next_step(successor)).key != first.key
    await registry.finish_attempt(successor, error_code="provider_unavailable", retry=False)
    await registry.retry(created.operation_id, principal=alice)
    await EnvironmentWorker(registry, ExternalProvider(), max_attempts=1).reconcile_once(created.operation_id)
    assert (await registry.get_operation(created.operation_id, principal=alice)).phase == "completed"
    await registry.destroy_retained(created.environment_id, principal=alice, expected_generation=1, idempotency_key="destroy")
    with pytest.raises(ManagementError, match="stale_operation_generation"):
        await registry.retry(created.operation_id, principal=alice)
