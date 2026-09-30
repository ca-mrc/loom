"""Actual PostgreSQL enforces refresh inspection without touching owner work."""
from __future__ import annotations

import asyncio
import copy
import socket
from pathlib import Path
from uuid import uuid4

import pytest
from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy import event, select, text

from loom.db.nebius_application_operation_schema import (
    NebiusApplicationOperation,
    NebiusApplicationReservation,
)
from tests.integration.test_nebius_application_operations import applications as applications
from tests.integration.test_nebius_environment_management import (
    environment_registry as environment_registry,
)
from tests.integration.test_nebius_platform_bootstrap import platform_database as platform_database
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs

# These fixtures bootstrap the current source tree, not a historical release.
CURRENT_REVISION = ScriptDirectory.from_config(
    Config(str(Path(__file__).resolve().parents[2] / "database/migrations/alembic.ini"))
).get_current_head()
assert CURRENT_REVISION is not None


def settings(prepared, *, mode='manager'):
    from loom.nebius_management_refresh_probe import RefreshProbeSettings

    shared = prepared['shared']
    return RefreshProbeSettings(mode=mode, namespace='loom-nebius-management', expected_revision=CURRENT_REVISION,
        shared=shared)


async def test_connection_refusal_is_classified_before_read_only_query(platform_inputs):
    from sqlalchemy.engine import URL

    from loom.nebius_management_refresh_probe import RefreshProbeError, database_snapshot
    from tests.unit.test_nebius_management_refresh_probe import config

    # Reserve a real local port without listening: no fixture database or mock
    # may accidentally turn a failed connection into a query failure.
    with socket.socket() as reserved:
        reserved.bind(('127.0.0.1', 0))
        url = URL.create('postgresql+psycopg', username='loom_service', password='disposable-only',
            host='127.0.0.1', port=reserved.getsockname()[1], database='loom')
        with pytest.raises(RefreshProbeError) as failure:
            await database_snapshot(url, config(platform_inputs))
    assert failure.value.stage == 'database'
    assert failure.value.error_type == 'OperationalError'
    assert str(failure.value) == 'refresh_probe_unqualified'


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
        'revision': CURRENT_REVISION, 'operations_checked': 1}
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
    with pytest.raises(ValueError, match='refresh_probe_unqualified') as failure:
        await database_snapshot(factory.kw['bind'].url, config)
    assert failure.value.stage == ('schema' if fault == 'schema' else 'operations')
    assert failure.value.error_type == 'ValueError'
    async with factory() as session:
        row = await session.get(NebiusApplicationOperation, operation.operation_id)
        assert row.runner_epoch == 0 and row.lease_token is None and row.phase == 'pending'


async def test_shared_probe_only_reads_the_expected_schema(applications):
    from loom.nebius_management_refresh_probe import database_snapshot

    _, factory, _, prepare, _, _ = applications
    prepared = prepare()
    prepared['shared'] = prepared['shared'].model_copy(update={'schema_revision': CURRENT_REVISION})
    config = settings(prepared, mode='shared')
    result = await database_snapshot(factory.kw['bind'].url, config)
    assert result['revision'] == CURRENT_REVISION and result['operations_checked'] == 0
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
    config = config.model_copy(update={'shared': config.shared.model_copy(update={'schema_revision': CURRENT_REVISION})})
    async with factory() as session:
        plan = copy.deepcopy((await session.get(NebiusApplicationOperation, stopped.operation_id)).plan_json)
    report = await database_snapshot(factory.kw['bind'].url, config)
    assert report['operations_checked'] == 1
    async with factory() as session:
        row = await session.get(NebiusApplicationOperation, stopped.operation_id)
        assert row.plan_json == plan and row.phase == 'pending' and row.runner_epoch == 0


@pytest.mark.parametrize('fault', ['historical_version', 'missing_source', 'source_generation'])
async def test_refresh_qualifies_historical_plans_needed_by_active_cleanup(applications, fault):
    from loom.nebius_management_refresh_probe import database_snapshot

    registry, factory, (alice, _), prepare, _, _ = applications
    prepared = prepare()
    first = await registry.create(principal=alice, idempotency_key='historical-version', **prepared)
    stopped = await registry.transition(first.application_id, principal=alice, idempotency_key='historical-stop',
        action='suspend', expected_generation=1)
    async with factory.begin() as session:
        identity = first.operation_id if fault == 'historical_version' else stopped.operation_id
        row = await session.get(NebiusApplicationOperation, identity)
        plan = copy.deepcopy(row.plan_json)
        if fault == 'historical_version':
            plan['schema_version'] = 'loom.nebius-application-plan.v99'
        elif fault == 'missing_source':
            plan['source_operation_id'] = str(uuid4())
        else:
            # Consistent current row/plan counters, but no longer an immediate
            # successor to the source from which cleanup inherited its effects.
            row.access_generation = 3
            plan['registration']['access_generation'] = 3
        row.plan_json = plan
    with pytest.raises(ValueError, match='refresh_probe_unqualified'):
        await database_snapshot(factory.kw['bind'].url, settings(prepared))
    async with factory() as session:
        assert (await session.get(NebiusApplicationOperation, stopped.operation_id)).phase == 'pending'


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


def test_probe_uses_the_real_bootstrapped_nonadmin_service_role_over_tls(platform_database, platform_inputs, monkeypatch):
    from sqlalchemy.engine import make_url

    from loom import nebius_platform_bootstrap as bootstrap
    from loom.nebius_management_refresh_probe import RefreshProbeSettings, database_snapshot
    from tests.unit.test_nebius_application_render import inputs

    monkeypatch.setattr(bootstrap, 'database_url', lambda _value, _namespace: platform_database)
    password = 'disposable-probe-service-' + 'x' * 30
    monkeypatch.setenv('LOOM_DB_URL', platform_database)
    monkeypatch.setenv('LOOM_DB_SERVICE_PASSWORD', password)
    bootstrap.bootstrap_management_database({'namespace': 'loom-nebius-management'})
    service = make_url(platform_database).set(drivername='postgresql+psycopg', username='loom_service', password=password)
    shared = inputs(platform_inputs)[2].model_copy(update={'schema_revision': CURRENT_REVISION})
    for mode in ('manager', 'shared'):
        config = RefreshProbeSettings(mode=mode, namespace='loom-nebius-management', expected_revision=CURRENT_REVISION, shared=shared)
        assert asyncio.run(database_snapshot(service, config)) == {
            'schema': 'loom.nebius-management-refresh-probe.v1', 'status': 'qualified', 'mode': mode,
            'revision': CURRENT_REVISION, 'operations_checked': 0}
