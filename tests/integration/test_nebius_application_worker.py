"""The bounded worker drives concrete lifecycle completion and preserves leases."""
from __future__ import annotations

import asyncio

import pytest

from loom.db.nebius_application_operation_schema import (
    NebiusApplicationOperation,
    NebiusApplicationReservation,
)
from loom_service.environment_management.provider import (
    ProviderBlockedError,
    ProviderRetryError,
    ProviderWaitingError,
)
from loom_service.environment_management.registry import ManagementError
from tests.integration.test_nebius_application_credentials import database_access as database_access
from tests.integration.test_nebius_application_credentials import shared_ca as shared_ca
from tests.integration.test_nebius_application_effects import expire
from tests.integration.test_nebius_application_material import management_key as management_key
from tests.integration.test_nebius_application_operations import applications as applications
from tests.integration.test_nebius_application_preparation import preparation as preparation
from tests.integration.test_nebius_application_ready import ready_context as ready_context
from tests.integration.test_nebius_environment_management import (
    environment_registry as environment_registry,
)
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


class HoldingCoordinator:
    """Controlled slow lifecycle boundary, never a fabricated success receipt."""
    def __init__(self, registry, error=None):
        self.registry, self.error = registry, error
        self.entered, self.cancelled = asyncio.Event(), asyncio.Event()
        self.calls = []
        self.cancelled_operations = set()

    async def start(self, lease):
        await self.registry.frozen_plan(lease)
        self.calls.append(('start', lease))
        self.entered.set()
        if self.error is not None:
            raise self.error
        try:
            await asyncio.Event().wait()
        finally:
            self.cancelled.set()
            self.cancelled_operations.add(lease.operation_id)

    async def stop(self, lease):
        await self.registry.frozen_plan(lease)
        self.calls.append(('stop', lease))
        raise ProviderBlockedError('test_stop_observed')


async def test_poll_excludes_claimed_terminal_and_superseded_generations(applications):
    registry, factory, (alice, bob), prepare, _, _ = applications
    first = await registry.create(principal=alice, idempotency_key='first', **prepare())
    second = await registry.create(principal=bob, idempotency_key='second', **prepare('bob', bob))
    assert await registry.runnable_operations(limit=1) == [first.operation_id]
    lease = await registry.claim(first.operation_id)
    assert await registry.runnable_operations() == [second.operation_id]
    await expire(factory, lease)
    assert await registry.runnable_operations() == [first.operation_id, second.operation_id]
    lease = await registry.claim(first.operation_id)
    await registry.finish_attempt(lease, error_code='blocked', retry=False)
    successor = await registry.transition(second.application_id, principal=bob, idempotency_key='stop',
        action='suspend', expected_generation=1)
    # Defensive current-generation filter even if an old row remains runnable.
    async with factory.begin() as session:
        (await session.get(NebiusApplicationOperation, second.operation_id)).phase = 'pending'
    assert await registry.runnable_operations() == [successor.operation_id]
    for invalid in (True, 0, 17, 1.5):
        with pytest.raises(ValueError):
            await registry.runnable_operations(limit=invalid)


async def test_worker_completes_concrete_ready_lifecycle_and_retains_charge(ready_context, monkeypatch):
    from loom_service.application_management.worker import ApplicationWorker

    coordinator, registry, factory, alice, lease, _, _, _ = ready_context
    await expire(factory, lease)
    original, calls = coordinator.runtime.read_ready, 0

    async def briefly_wait(current):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise ProviderWaitingError('test_controller_wait')
        return await original(current)

    monkeypatch.setattr(coordinator.runtime, 'read_ready', briefly_wait)
    await ApplicationWorker(registry, coordinator, max_attempts=1, readiness_poll_seconds=0.01).reconcile_once(lease.operation_id)
    result = await registry.get_operation(lease.operation_id, principal=alice)
    assert result.phase == 'completed'
    async with factory() as session:
        row = await session.get(NebiusApplicationOperation, lease.operation_id)
        assert row.completion_json is not None and row.lease_token is None
        reservation = await session.get(NebiusApplicationReservation, lease.application_id)
        assert reservation.cpu_millis > 0 and reservation.storage_mib == 0


async def test_concurrent_workers_claim_only_one_lifecycle_and_cancellation_drains_it(applications):
    from loom_service.application_management.worker import ApplicationWorker

    registry, factory, (alice, _), prepare, _, _ = applications
    operation = await registry.create(principal=alice, idempotency_key='one', **prepare())
    coordinator = HoldingCoordinator(registry)
    worker = ApplicationWorker(registry, coordinator)
    task = asyncio.create_task(worker.reconcile_once(operation.operation_id))
    try:
        await asyncio.wait_for(coordinator.entered.wait(), 2)
        await worker.reconcile_once(operation.operation_id)
        assert len(coordinator.calls) == 1
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    assert coordinator.cancelled.is_set()
    assert (await registry.get_operation(operation.operation_id, principal=alice)).phase == 'running'
    assert await registry.claim(operation.operation_id) is None
    async with factory() as session:
        assert (await session.get(NebiusApplicationReservation, operation.application_id)).cpu_millis > 0


async def test_heartbeat_renews_then_cancels_stale_work_without_changing_successor(applications):
    from loom_service.application_management.worker import ApplicationWorker

    registry, factory, (alice, _), prepare, _, _ = applications
    operation = await registry.create(principal=alice, idempotency_key='lease', **prepare())
    coordinator = HoldingCoordinator(registry)
    task = asyncio.create_task(ApplicationWorker(registry, coordinator, lease_seconds=3).reconcile_once(operation.operation_id))
    try:
        await asyncio.wait_for(coordinator.entered.wait(), 2)
        await asyncio.sleep(3.1)
        assert await registry.claim(operation.operation_id) is None
        await expire(factory, coordinator.calls[0][1])
        successor = await registry.claim(operation.operation_id)
        await asyncio.wait_for(task, 2)
        assert coordinator.cancelled.is_set()
        assert await registry.frozen_plan(successor)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.parametrize('error,code', [(ProviderRetryError('test_unavailable'), 'test_unavailable'),
    (ProviderBlockedError('test_conflict'), 'test_conflict'),
    (ManagementError('application_completion_evidence_conflict'), 'application_completion_evidence_conflict'),
    (ManagementError('password=never-journal'), 'provider_internal_error'),
    (RuntimeError('password=never-journal'), 'provider_internal_error')])
async def test_failure_budget_is_bounded_and_exception_details_are_not_persisted(applications, error, code):
    from loom_service.application_management.worker import ApplicationWorker

    registry, _, (alice, _), prepare, _, _ = applications
    operation = await registry.create(principal=alice, idempotency_key='failure', **prepare())
    worker = ApplicationWorker(registry, HoldingCoordinator(registry, error), max_attempts=2)
    await worker.reconcile_once(operation.operation_id)
    if type(error) is ProviderRetryError:
        assert (await registry.get_operation(operation.operation_id, principal=alice)).phase == 'pending'
        await worker.reconcile_once(operation.operation_id)
    result = await registry.get_operation(operation.operation_id, principal=alice)
    assert result.phase == 'blocked' and result.error_code == code
    assert await registry.runnable_operations() == []


async def test_timeout_drains_work_before_releasing_lease_and_keeps_charge(applications, monkeypatch):
    from loom_service.application_management.worker import ApplicationWorker

    registry, factory, (alice, _), prepare, _, _ = applications
    operation = await registry.create(principal=alice, idempotency_key='timeout', **prepare())
    coordinator = HoldingCoordinator(registry)
    finish = registry.finish_attempt

    async def after_drain(*args, **kwargs):
        assert coordinator.cancelled.is_set()
        return await finish(*args, **kwargs)

    monkeypatch.setattr(registry, 'finish_attempt', after_drain)
    await ApplicationWorker(registry, coordinator, attempt_timeout=0.05).reconcile_once(operation.operation_id)
    result = await registry.get_operation(operation.operation_id, principal=alice)
    assert result.phase == 'pending' and result.error_code == 'provider_timeout'
    async with factory() as session:
        assert (await session.get(NebiusApplicationReservation, operation.application_id)).cpu_millis > 0


async def test_readiness_wait_has_deadline_and_stop_dispatch_uses_current_desired_state(applications):
    from loom_service.application_management.worker import ApplicationWorker

    registry, _, (alice, _), prepare, _, _ = applications
    operation = await registry.create(principal=alice, idempotency_key='waiting', **prepare())
    coordinator = HoldingCoordinator(registry, ProviderWaitingError('test_waiting'))
    worker = ApplicationWorker(registry, coordinator, readiness_poll_seconds=0.01, readiness_timeout=0.05)
    await worker.reconcile_once(operation.operation_id)
    result = await registry.get_operation(operation.operation_id, principal=alice)
    assert result.phase == 'blocked' and result.error_code == 'application_readiness_timeout'
    stopped = await registry.transition(operation.application_id, principal=alice, idempotency_key='stop',
        action='suspend', expected_generation=1)
    await worker.reconcile_once(stopped.operation_id)
    result = await registry.get_operation(stopped.operation_id, principal=alice)
    assert result.error_code == 'test_stop_observed' and coordinator.calls[-1][0] == 'stop'


async def test_loop_progresses_other_owner_and_shutdown_clears_health_and_drains(applications):
    from loom_service.application_management.worker import ApplicationWorker

    registry, _, (alice, bob), prepare, _, _ = applications
    first = await registry.create(principal=alice, idempotency_key='slow', **prepare())
    coordinator = HoldingCoordinator(registry)
    worker = ApplicationWorker(registry, coordinator)
    task = asyncio.create_task(worker.run(concurrency=2, poll_seconds=1))
    try:
        await asyncio.wait_for(coordinator.entered.wait(), 2)
        second = await registry.create(principal=bob, idempotency_key='second', **prepare('bob', bob))
        async with asyncio.timeout(4):
            while (await registry.get_operation(second.operation_id, principal=bob)).phase != 'running':
                await asyncio.sleep(0.05)
        assert worker.healthy
        assert (await registry.get_operation(first.operation_id, principal=alice)).phase == 'running'
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    assert not worker.healthy and coordinator.cancelled.is_set()
    assert {call[1].operation_id for call in coordinator.calls} == {first.operation_id, second.operation_id}
    assert coordinator.cancelled_operations == {first.operation_id, second.operation_id}


async def test_loop_database_outage_cancels_active_work_and_recovers_health(applications):
    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import create_async_engine

    from loom_service.application_management.worker import ApplicationWorker

    registry, factory, (alice, _), prepare, _, _ = applications
    operation = await registry.create(principal=alice, idempotency_key='outage', **prepare())
    engine = factory.kw['bind']
    assert engine.url.database.startswith('loom_migration_')
    admin = create_async_engine(engine.url.set(database='postgres'), isolation_level='AUTOCOMMIT')
    database = admin.dialect.identifier_preparer.quote(engine.url.database)
    coordinator = HoldingCoordinator(registry)
    worker = ApplicationWorker(registry, coordinator)
    task = asyncio.create_task(worker.run(poll_seconds=1))
    try:
        await asyncio.wait_for(coordinator.entered.wait(), 2)
        assert worker.healthy
        async with admin.connect() as connection:
            await connection.execute(text(f'ALTER DATABASE {database} ALLOW_CONNECTIONS false'))
        await engine.dispose()
        await asyncio.wait_for(coordinator.cancelled.wait(), 4)
        assert not worker.healthy and not task.done()
        async with admin.connect() as connection:
            await connection.execute(text(f'ALTER DATABASE {database} ALLOW_CONNECTIONS true'))
        async with asyncio.timeout(4):
            while not worker.healthy:
                await asyncio.sleep(0.05)
        result = await registry.get_operation(operation.operation_id, principal=alice)
        assert result.phase == 'running' and await registry.claim(operation.operation_id) is None
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        async with admin.connect() as connection:
            await connection.execute(text(f'ALTER DATABASE {database} ALLOW_CONNECTIONS true'))
        await admin.dispose()


@pytest.mark.parametrize('method', ['runnable_operations', 'renew'])
async def test_stalled_database_poll_or_renewal_cancels_work_before_lease_expiry(applications, monkeypatch, method):
    from loom_service.application_management.worker import ApplicationWorker

    registry, _, (alice, _), prepare, _, _ = applications
    operation = await registry.create(principal=alice, idempotency_key='stalled', **prepare())
    coordinator = HoldingCoordinator(registry)
    worker = ApplicationWorker(registry, coordinator, lease_seconds=3)
    original = getattr(registry, method)
    stalled, recovered = asyncio.Event(), asyncio.Event()

    async def stall_after_start(*args, **kwargs):
        if coordinator.entered.is_set() and not recovered.is_set():
            stalled.set()
            await asyncio.Event().wait()
        return await original(*args, **kwargs)

    monkeypatch.setattr(registry, method, stall_after_start)
    task = asyncio.create_task(worker.run(poll_seconds=1))
    try:
        await asyncio.wait_for(coordinator.entered.wait(), 2)
        assert worker.healthy
        await asyncio.wait_for(stalled.wait(), 2)
        # A DB stall must stop the real lifecycle before its three-second lease
        # can expire, without relying on the 45-second provider-attempt timeout.
        await asyncio.wait_for(coordinator.cancelled.wait(), 1)
        assert not worker.healthy and not task.done()
        result = await registry.get_operation(operation.operation_id, principal=alice)
        assert result.phase == 'running' and result.error_code is None
        recovered.set()
        async with asyncio.timeout(3):
            while not worker.healthy:
                await asyncio.sleep(0.01)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.parametrize('method,phase', [('claim', 'pending'), ('frozen_plan', 'running'),
    ('finish_attempt', 'running')])
async def test_stalled_database_attempt_boundaries_preserve_uncertain_state(applications, monkeypatch, method, phase):
    from loom_service.application_management.worker import ApplicationWorker

    registry, factory, (alice, _), prepare, _, _ = applications
    operation = await registry.create(principal=alice, idempotency_key='stalled-attempt', **prepare())
    coordinator = HoldingCoordinator(registry, ProviderBlockedError('test_conflict'))
    worker = ApplicationWorker(registry, coordinator, lease_seconds=3)
    stalled = asyncio.Event()

    async def stall(*args, **kwargs):
        stalled.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(registry, method, stall)
    task = asyncio.create_task(worker.reconcile_once(operation.operation_id))
    try:
        await asyncio.wait_for(stalled.wait(), 2)
        done, _ = await asyncio.wait({task}, timeout=1)
        assert task in done, f'{method} must have a bounded database wait'
        error = task.exception()
        assert isinstance(error, OSError) and not isinstance(error, TimeoutError)
        result = await registry.get_operation(operation.operation_id, principal=alice)
        assert result.phase == phase and result.error_code is None
        async with factory() as session:
            assert (await session.get(NebiusApplicationReservation, operation.application_id)).cpu_millis > 0
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
