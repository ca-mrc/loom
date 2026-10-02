"""Recovery drain reads real journals; it neither cancels nor fabricates cleanup."""
from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import httpx
import psycopg
import pytest
from sqlalchemy import text, update

from loom.db.nebius_pool_schema import NebiusPoolBinding
from loom.nebius_pool_workload import PoolExecutionPrepareV1
from loom_execution_capacity_collector.contracts import CapacityPlacement
from loom_service.pool_management.auth import resolve_pool_machine
from tests.execution_placement_fixtures import placement_fixture
from tests.integration.test_nebius_pool_activation_fence import read_sql
from tests.integration.test_nebius_pool_registry import prepare, publish_placement
from tests.integration.test_nebius_pool_registry import sessions as sessions
from tests.integration.test_nebius_pool_startup_capacity import prepare_startup_capacity
from tests.unit.test_nebius_pool_execution_render import inputs


async def request_setup(sessions, tmp_path):
    spec, tokens, environment, *_ = await prepare_startup_capacity(sessions, tmp_path, executable=True)
    async with sessions.begin() as session:
        await session.execute(update(NebiusPoolBinding).values(mode='global'))

    async def principal(role):
        machine, = (row for row in spec.machines if row.role == role and
            (role != 'participant' or row.participant_id == spec.participants[0].participant_id))
        async with sessions() as session:
            return await resolve_pool_machine(session, 'Bearer ' + tokens[machine.machine_id])

    placement = placement_fixture(target_id=spec.node_group_id, parent_id='parent', quota_nodes=2,
        node_cpu=4000, node_memory=8192, node_storage=32768)
    del placement['quota_resources']['memory']
    await publish_placement(sessions, await principal('observer'), CapacityPlacement.model_validate(placement))
    participant = spec.participants[0]
    _, body = inputs()
    body.update(pool_id=spec.pool_id, key=body['key'] | {'participant_id': participant.participant_id},
        origin=body['origin'] | {'data_environment_id': participant.environment_id})
    request = PoolExecutionPrepareV1.model_validate(body)
    receipt = await prepare(sessions, await principal('participant'), request, spec.profiles.profiles())
    assert receipt.phase == 'reserved'
    return spec, environment['LOOM_POOL_GATEWAY_DB_URL'], principal, request


async def test_drain_requires_terminal_fence_and_never_cancels_a_reservation(sessions, tmp_path):
    from scripts.ops.nebius_pool_activation_database import fence_pool_activation_sql
    from scripts.ops.nebius_pool_recovery_database import pool_recovery_drain_sql, qualify_pool_recovery_drain

    from loom_service.pool_management.control import cancel_unstarted_pool_request
    from tests.integration.test_nebius_pool_control import action

    spec, url, principal, request = await request_setup(sessions, tmp_path)
    query = pool_recovery_drain_sql(spec)
    with pytest.raises(psycopg.Error):
        read_sql(url, query)
    read_sql(url, fence_pool_activation_sql(spec))
    first = read_sql(url, query)
    assert first['counts'] == {'unstarted_requests': 1, 'active_requests': 0,
        'unconfirmed_creates': 0, 'unqualified_releases': 0}
    assert qualify_pool_recovery_drain(spec, first) is False
    assert read_sql(url, query) == first
    async with sessions.begin() as session:
        await cancel_unstarted_pool_request(session, await principal('participant'), action(request))
    assert qualify_pool_recovery_drain(spec, read_sql(url, query)) is True


@pytest.mark.parametrize('effect', ['absent', 'prepared', 'observed', 'unknown'])
async def test_only_real_cleanup_can_satisfy_global_drain(sessions, tmp_path, effect):
    from scripts.ops.nebius_pool_activation_database import fence_pool_activation_sql
    from scripts.ops.nebius_pool_recovery_database import pool_recovery_drain_sql, qualify_pool_recovery_drain

    from loom_service.pool_management.gateway_journal import PoolGatewayJournal
    from loom_service.pool_management.kubernetes import KubernetesPoolGateway
    from loom_service.pool_management.worker import PoolGatewayWorker
    from tests.integration.test_nebius_pool_control import action, operate
    from tests.integration.test_nebius_pool_kubernetes import KubernetesAPI
    from tests.integration.test_nebius_pool_pod_inventory import InventoryAPI
    from tests.integration.test_nebius_pool_stop_drain import accept_drain, accept_stop, drain_input, stop_input

    spec, url, principal, request = await request_setup(sessions, tmp_path)
    receipt = await operate(sessions, await principal('participant'), action(request, activation=True), profiles=spec.profiles.profiles())
    journal = PoolGatewayJournal(sessions)
    api = KubernetesAPI(spec.participants[0].execution_namespace.uid)
    async with httpx.AsyncClient(base_url='https://kubernetes.example', transport=httpx.MockTransport(InventoryAPI(api, []))) as http:
        gateway = KubernetesPoolGateway(journal, http)
        if effect == 'prepared':
            await journal.prepare_create(await principal('gateway'), receipt.reservation_id, kind='Job')
        elif effect in {'observed', 'unknown'}:
            api.lose_reply = effect == 'unknown'
            await PoolGatewayWorker(gateway=gateway, principal=await principal('gateway')).run_once()
        read_sql(url, fence_pool_activation_sql(spec))
        query = pool_recovery_drain_sql(spec)
        before = read_sql(url, query)
        assert before['counts']['active_requests'] == 1
        assert before['counts']['unconfirmed_creates'] == int(effect == 'unknown')
        assert qualify_pool_recovery_drain(spec, before) is False
        stop = (await stop_input(sessions, receipt)).model_copy(update={'grace_deadline_at': datetime.now(UTC)})
        await accept_stop(sessions, await principal('participant'), stop)
        await accept_drain(sessions, await principal('participant'), drain_input(stop))
        fixed = PoolGatewayWorker(gateway=gateway, principal=await principal('gateway'))
        if effect == 'unknown':
            api.hide_objects = True
            await fixed.run_once()
            assert read_sql(url, query) == before
            assert not api.deletes and len(api.writes) == 1
            api.hide_objects = False
        await fixed.run_once()
        assert qualify_pool_recovery_drain(spec, read_sql(url, query)) is True
        assert len(api.writes) == len(api.deletes) == int(effect in {'observed', 'unknown'})


@pytest.mark.parametrize('phase', ['selected', 'reserved', 'attached'])
async def test_local_drain_waits_for_outbox_even_when_no_pod_exists(sessions, tmp_path, phase):
    from scripts.ops.nebius_pool_guard_activation import pool_guard_activation_sql
    from scripts.ops.nebius_pool_recovery_database import participant_recovery_drain_sql, qualify_participant_recovery_drain

    from loom_execution_actuator.pool_build_driver import PoolBuildDriver
    from tests.integration.test_nebius_pool_build_driver import selected
    from tests.integration.test_nebius_pool_build_outbox import counts
    from tests.integration.test_nebius_pool_participant_http import client

    app, token, participant, request, journal = await selected(sessions, tmp_path)
    operation, candidate = uuid4(), 'a' * 40
    url = sessions.kw['bind'].url.render_as_string(hide_password=False)
    query = participant_recovery_drain_sql(operation, participant.participant_id, candidate)
    with pytest.raises(psycopg.Error):
        read_sql(url, query)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app)) as http:
        management = client(http, token)
        if phase != 'selected':
            receipt = await management.prepare(request)
            if phase == 'attached':
                await journal.accept_grant(request.key, receipt)
        read_sql(url, pool_guard_activation_sql(operation, participant.participant_id, candidate, action='fence'))
        first = read_sql(url, query)
        assert first['counts']['build_outboxes'] == 1
        assert first['counts']['builds'] == int(phase == 'attached')
        assert qualify_participant_recovery_drain(operation, participant.participant_id, candidate, first) is False
        assert read_sql(url, query) == first
        driver = PoolBuildDriver(outbox=journal, management=management)
        assert (await driver.advance(request.key)).phase == 'cancelled'
        assert qualify_participant_recovery_drain(operation, participant.participant_id, candidate, read_sql(url, query)) is True
    assert (await counts(sessions, request.key.local_work_id))[1] == 0


@pytest.mark.parametrize('damage', ['binding', 'revision', 'schema'])
async def test_global_drain_refuses_changed_retained_authority(sessions, tmp_path, damage):
    from scripts.ops.nebius_pool_activation_database import fence_pool_activation_sql
    from scripts.ops.nebius_pool_recovery_database import pool_recovery_drain_sql

    prepared = await prepare_startup_capacity(sessions, tmp_path)
    spec, _, environment, *_ = prepared
    url = environment['LOOM_POOL_GATEWAY_DB_URL']
    read_sql(url, fence_pool_activation_sql(spec))
    async with sessions.begin() as session:
        if damage == 'schema':
            await session.execute(text("UPDATE alembic_version SET version_num='0171'"))
        else:
            await session.execute(update(NebiusPoolBinding).values(**(
                {'binding_sha256': '0' * 64} if damage == 'binding' else {'policy_revision': spec.policy_revision + 2})))
    with pytest.raises(psycopg.Error):
        read_sql(url, pool_recovery_drain_sql(spec))
