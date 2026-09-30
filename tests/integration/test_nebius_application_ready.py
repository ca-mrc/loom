"""Ready completion retains active charges and requires concrete current evidence."""
from __future__ import annotations

from uuid import uuid4

import pytest

from loom.db.nebius_application_operation_schema import (
    NebiusApplicationOperation,
    NebiusApplicationReservation,
)
from loom_service.environment_management.provider import ProviderWaitingError
from loom_service.environment_management.registry import ManagementError
from tests.integration.test_nebius_application_activation import ready_backends
from tests.integration.test_nebius_application_credentials import database_access as database_access
from tests.integration.test_nebius_application_credentials import shared_ca as shared_ca
from tests.integration.test_nebius_application_effects import expire
from tests.integration.test_nebius_application_material import management_key as management_key
from tests.integration.test_nebius_application_operations import applications as applications
from tests.integration.test_nebius_application_preparation import close_quota
from tests.integration.test_nebius_application_preparation import preparation as preparation
from tests.integration.test_nebius_environment_management import (
    environment_registry as environment_registry,
)
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


async def ready_evidence(context, lease=None):
    from loom_service.application_management.proofs import ApplicationReadyEvidence

    coordinator = context[0]
    lease = lease or context[4]
    return ApplicationReadyEvidence(
        workloads=await coordinator.runtime.read_ready(lease),
        database=await coordinator.credentials.retire_database(lease),
        objects=await coordinator.credentials.retire_cloud(lease, coordinator.object_verifier),
        prepared=await coordinator.runtime.read_prepared(lease),
        access=await coordinator.credentials.qualify(lease, coordinator.object_verifier),
        network=await coordinator.runtime.read_shared_network(lease))


@pytest.fixture
async def ready_context(preparation):
    coordinator, _, _, _, lease, api, _, _ = preparation
    await close_quota(preparation)
    await coordinator.activate(lease)
    with pytest.raises(ProviderWaitingError, match='application_workloads_readiness_pending'):
        await coordinator.runtime.start_workloads(lease)
    ready_backends(api)
    await coordinator.runtime.start_workloads(lease)
    return preparation


async def test_ready_completion_retains_actual_charge_shared_records_and_immutable_replay(ready_context, database_access):
    _, registry, factory, _, lease, _, _, _ = ready_context
    plan = await registry.frozen_plan(lease)
    target = plan['platform_envelope']
    database_access[0].execute("INSERT INTO shared_records(value) VALUES ('accepted shared work')")
    # The update path holds max(old,new); completion alone may release excess.
    async with factory.begin() as session:
        reservation = await session.get(NebiusApplicationReservation, lease.application_id)
        reservation.cpu_millis += 100
        reservation.memory_mib += 100
    proof = await ready_evidence(ready_context)
    await registry.complete_ready(lease, proof)
    async with factory() as session:
        operation = await session.get(NebiusApplicationOperation, lease.operation_id)
        reservation = await session.get(NebiusApplicationReservation, lease.application_id)
        assert operation.phase == 'completed' and operation.completed_at is not None
        assert operation.completion_json == proof.model_dump(mode='json')
        assert operation.plan_json == plan and operation.lease_token is operation.lease_expires_at is None
        assert all(getattr(reservation, name) == value for name, value in target.items())
        assert reservation.cpu_millis > 0 and reservation.memory_mib > 0 and reservation.storage_mib == 0
        completed_at = operation.completed_at
    await registry.complete_ready(lease, proof)
    async with factory() as session:
        assert (await session.get(NebiusApplicationOperation, lease.operation_id)).completed_at == completed_at
    assert database_access[0].execute('SELECT value FROM shared_records').fetchall() == [('accepted shared work',)]


@pytest.mark.parametrize('damage', ['namespace', 'activation', 'deployment', 'replicas', 'services', 'static',
    'network', 'access-key', 'owner', 'schema', 'retired-through', 'lease'])
async def test_invalid_ready_evidence_never_completes_or_changes_charge(ready_context, damage):
    from loom_service.application_management.proofs import ApplicationReadyEvidence

    _, registry, factory, _, lease, _, _, _ = ready_context
    proof = (await ready_evidence(ready_context)).model_dump()
    if damage == 'namespace':
        proof['prepared']['namespace']['uid'] = 'replacement'
    elif damage == 'activation':
        proof['workloads']['retired_quota_uid'] = 'replacement'
    elif damage == 'deployment':
        proof['workloads']['deployments'][0]['uid'] = 'replacement'
    elif damage == 'replicas':
        proof['workloads']['deployments'][0]['replicas'] = 2
    elif damage in {'services', 'static', 'network'}:
        if damage == 'services':
            proof['workloads']['services'] = ()
        elif damage == 'static':
            proof['prepared']['resources'] = ()
        else:
            proof['network'] = ()
    elif damage == 'access-key':
        proof['access']['access_key_sha256'] = '0' * 64
    elif damage == 'owner':
        proof['access']['user_id'] = uuid4()
    elif damage == 'schema':
        proof['access']['schema_revision'] = 'wrong'
    elif damage == 'retired-through':
        proof['database']['retired_through'] = 1
    else:
        proof['access']['identity']['lease_sha256'] = '0' * 64
    with pytest.raises(ManagementError, match='application_completion_evidence_conflict'):
        await registry.complete_ready(lease, ApplicationReadyEvidence.model_validate(proof))
    async with factory() as session:
        operation = await session.get(NebiusApplicationOperation, lease.operation_id)
        reservation = await session.get(NebiusApplicationReservation, lease.application_id)
        assert operation.phase == 'running' and operation.completion_json is None
        assert reservation.cpu_millis > 0


async def test_ready_completion_rejects_new_unsettled_effect(ready_context):
    _, registry, factory, _, lease, _, _, _ = ready_context
    proof = await ready_evidence(ready_context)
    await registry.prepare_effect(lease, 'unexpected-pending', dict(api_version='v1', kind='Pod',
        namespace='loom-dev-alice', name='pending', action='delete', uid='pod', resource_version='1', request_sha256='a' * 64))
    with pytest.raises(ManagementError, match='application_completion_effects_pending'):
        await registry.complete_ready(lease, proof)
    async with factory() as session:
        assert (await session.get(NebiusApplicationOperation, lease.operation_id)).phase == 'running'


async def test_ready_completion_requires_fresh_proof_after_lease_takeover(ready_context):
    _, registry, factory, _, lease, _, _, _ = ready_context
    proof = await ready_evidence(ready_context)
    await expire(factory, lease)
    current = await registry.claim(lease.operation_id)
    with pytest.raises(ManagementError, match='stale_operation_lease'):
        await registry.complete_ready(lease, proof)
    with pytest.raises(ManagementError, match='application_completion_evidence_conflict'):
        await registry.complete_ready(current, proof)
    await registry.complete_ready(current, await ready_evidence(ready_context, current))


async def test_concrete_start_waits_for_current_backends_then_completes(preparation):
    coordinator, _, factory, _, lease, api, _, _ = preparation
    await close_quota(preparation)
    with pytest.raises(ProviderWaitingError, match='application_workloads_readiness_pending'):
        await coordinator.start(lease)
    async with factory() as session:
        assert (await session.get(NebiusApplicationOperation, lease.operation_id)).phase == 'running'
    ready_backends(api)
    await coordinator.start(lease)
    async with factory() as session:
        operation = await session.get(NebiusApplicationOperation, lease.operation_id)
        assert operation.phase == 'completed' and operation.completion_json is not None
        assert (await session.get(NebiusApplicationReservation, lease.application_id)).cpu_millis > 0
