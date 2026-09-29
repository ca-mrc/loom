"""Actual PostgreSQL enforces refresh inspection without touching owner work."""
from __future__ import annotations

import copy

import pytest
from sqlalchemy import event, select, text

from loom.db.nebius_application_operation_schema import (
    NebiusApplicationOperation,
    NebiusApplicationReservation,
)
from tests.integration.test_nebius_application_operations import applications as applications
from tests.integration.test_nebius_environment_management import (
    environment_registry as environment_registry,
)
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


def settings(prepared, *, mode='manager'):
    from loom.nebius_management_refresh_probe import RefreshProbeSettings

    shared = prepared['shared']
    return RefreshProbeSettings(mode=mode, namespace='loom-nebius-management', expected_revision='0168',
        shared=shared)


@pytest.mark.parametrize('running', [False, True])
async def test_refresh_probe_preserves_claims_frozen_plans_and_reservations(applications, running):
    from loom.nebius_management_refresh_probe import database_snapshot

    registry, factory, (alice, _), prepare, _, _ = applications
    prepared = prepare()
    operation = await registry.create(principal=alice, idempotency_key='refresh-preserves-owner', **prepared)
    if running:
        await registry.claim(operation.operation_id)

    async def retained():
        async with factory() as session:
            row = await session.get(NebiusApplicationOperation, operation.operation_id)
            reservation = (await session.scalars(select(NebiusApplicationReservation))).one()
            return copy.deepcopy((row.plan_json, row.phase, row.runner_epoch, row.lease_token, row.lease_expires_at,
                reservation.cpu_millis, reservation.memory_mib, reservation.storage_mib, reservation.ephemeral_storage_mib))

    before = await retained()
    result = await database_snapshot(factory.kw['bind'].url, settings(prepared))
    assert result == {'schema': 'loom.nebius-management-refresh-probe.v1', 'status': 'qualified', 'mode': 'manager',
        'revision': '0168', 'operations_checked': 1}
    assert await retained() == before
    assert str(operation.operation_id) not in str(result)


async def test_probe_connection_rejects_injected_write_at_the_server(applications, monkeypatch):
    from loom import nebius_management_refresh_probe as probe

    registry, factory, (alice, _), prepare, _, _ = applications
    prepared = prepare()
    operation = await registry.create(principal=alice, idempotency_key='read-only-probe', **prepared)
    original = probe.create_async_engine
    attempts = []

    def instrumented(url, **kwargs):
        engine = original(url, **kwargs)

        def inject(connection, cursor, statement, parameters, context, many):
            if statement.startswith('SHOW transaction_read_only'):
                attempts.append(True)
                cursor.execute('UPDATE nebius_application_operations SET runner_epoch=runner_epoch+1')

        event.listen(engine.sync_engine, 'before_cursor_execute', inject)
        return engine

    monkeypatch.setattr(probe, 'create_async_engine', instrumented)
    with pytest.raises(ValueError, match='refresh_probe_unqualified'):
        await probe.database_snapshot(factory.kw['bind'].url, settings(prepared))
    assert attempts == [True]
    async with factory() as session:
        assert (await session.get(NebiusApplicationOperation, operation.operation_id)).runner_epoch == 0


@pytest.mark.parametrize('fault', ['schema', 'plan_version', 'shared_identity', 'shared_schema', 'release_binding', 'unknown_field'])
async def test_incompatible_retained_state_blocks_refresh_without_claims(applications, fault):
    from loom.nebius_management_refresh_probe import database_snapshot

    registry, factory, (alice, _), prepare, _, _ = applications
    prepared = prepare()
    operation = await registry.create(principal=alice, idempotency_key='incompatible', **prepared)
    config = settings(prepared)
    if fault == 'schema':
        config = config.model_copy(update={'expected_revision': '0000'})
    else:
        async with factory.begin() as session:
            row = await session.get(NebiusApplicationOperation, operation.operation_id)
            plan = copy.deepcopy(row.plan_json)
            if fault == 'plan_version':
                plan['schema_version'] = 'loom.nebius-application-plan.v99'
            elif fault == 'shared_identity':
                plan['shared']['platform_namespace'] = 'another-shared-namespace'
            elif fault == 'shared_schema':
                plan['shared']['schema_revision'] = plan['release']['schema_revision'] = '0000'
            elif fault == 'release_binding':
                plan['registration']['release_id'] = '12345678-1234-4234-8234-123456789012'
            else:
                plan['unknown_contract'] = True
            row.plan_json = plan
    with pytest.raises(ValueError, match='refresh_probe_unqualified'):
        await database_snapshot(factory.kw['bind'].url, config)
    async with factory() as session:
        row = await session.get(NebiusApplicationOperation, operation.operation_id)
        assert row.runner_epoch == 0 and row.lease_token is None and row.phase == 'pending'


async def test_shared_probe_only_reads_the_expected_schema(applications):
    from loom.nebius_management_refresh_probe import database_snapshot

    _, factory, _, prepare, _, _ = applications
    prepared = prepare()
    prepared['shared'] = prepared['shared'].model_copy(update={'schema_revision': '0168'})
    config = settings(prepared, mode='shared')
    result = await database_snapshot(factory.kw['bind'].url, config)
    assert result['revision'] == '0168' and result['operations_checked'] == 0
    async with factory() as session:
        assert await session.scalar(text('SHOW transaction_read_only')) == 'off'


async def test_shared_probe_rejects_schema_not_bound_to_the_selected_release(applications):
    from loom.nebius_management_refresh_probe import database_snapshot

    _, factory, _, prepare, _, _ = applications
    with pytest.raises(ValueError, match='refresh_probe_unqualified'):
        await database_snapshot(factory.kw['bind'].url, settings(prepare(), mode='shared'))


@pytest.mark.parametrize('action', ['suspend', 'destroy_retained'])
async def test_cleanup_retains_old_schema_plans_while_new_release_advances(applications, action):
    from loom.nebius_management_refresh_probe import database_snapshot

    registry, factory, (alice, _), prepare, _, _ = applications
    prepared = prepare()
    first = await registry.create(principal=alice, idempotency_key='old-version', **prepared)
    await registry.claim(first.operation_id)
    stopped = await registry.transition(first.application_id, principal=alice, idempotency_key='cleanup',
        action=action, expected_generation=1)
    config = settings(prepared)
    config = config.model_copy(update={'shared': config.shared.model_copy(update={'schema_revision': '0168'})})
    async with factory() as session:
        plan = copy.deepcopy((await session.get(NebiusApplicationOperation, stopped.operation_id)).plan_json)
    report = await database_snapshot(factory.kw['bind'].url, config)
    assert report['operations_checked'] == 1
    async with factory() as session:
        row = await session.get(NebiusApplicationOperation, stopped.operation_id)
        assert row.plan_json == plan and row.phase == 'pending' and row.runner_epoch == 0


async def test_probe_fails_when_read_only_enforcement_is_disabled(applications, monkeypatch):
    from loom import nebius_management_refresh_probe as probe

    _, factory, _, prepare, _, _ = applications
    original = probe.create_async_engine

    def unenforced(url, **kwargs):
        kwargs['connect_args']['options'] = '-c default_transaction_read_only=off'
        return original(url, **kwargs)

    monkeypatch.setattr(probe, 'create_async_engine', unenforced)
    with pytest.raises(ValueError, match='refresh_probe_unqualified'):
        await probe.database_snapshot(factory.kw['bind'].url, settings(prepare()))


@pytest.mark.parametrize('bound', ['_MAX_OPERATIONS', '_MAX_TOTAL_BYTES', '_MAX_PLAN_BYTES'])
async def test_probe_rejects_unbounded_snapshot_before_fetching_private_plans(applications, monkeypatch, bound):
    from loom import nebius_management_refresh_probe as probe

    registry, factory, (alice, _), prepare, _, _ = applications
    prepared = prepare()
    await registry.create(principal=alice, idempotency_key='bounded', **prepared)
    monkeypatch.setattr(probe, bound, 0)
    original = probe.create_async_engine
    queries = []

    def instrumented(url, **kwargs):
        engine = original(url, **kwargs)
        def observe(connection, cursor, statement, parameters, context, many):
            queries.append(statement)
        event.listen(engine.sync_engine, 'before_cursor_execute', observe)
        return engine

    monkeypatch.setattr(probe, 'create_async_engine', instrumented)
    with pytest.raises(ValueError, match='refresh_probe_unqualified'):
        await probe.database_snapshot(factory.kw['bind'].url, settings(prepared))
    assert any('octet_length' in query for query in queries)
    assert not any(query.startswith('SELECT nebius_application_operations.operation_id') for query in queries)
