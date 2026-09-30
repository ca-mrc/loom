"""Durable stopped completion uses real journals, SQL access and provider adapters."""
from __future__ import annotations

import asyncio
from dataclasses import replace
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import select

from loom.db.nebius_application_operation_schema import (
    NebiusApplicationOperation,
    NebiusApplicationReservation,
)
from loom.db.nebius_application_schema import NebiusApplication, NebiusDeploymentNameClaim
from loom.nebius_application_authority import ApplicationNamespaceAuthorityV1
from loom_service.application_management.kubernetes import ApplicationKubernetesProvider
from loom_service.application_management.proofs import ApplicationStopEvidence
from loom_service.application_management.runtime import ApplicationRuntimeProvider
from loom_service.environment_management.provider import ProviderWaitingError
from loom_service.environment_management.registry import ManagementError
from tests.integration.test_nebius_application_credentials import database_access as database_access
from tests.integration.test_nebius_application_credentials import setup
from tests.integration.test_nebius_application_credentials import shared_ca as shared_ca
from tests.integration.test_nebius_application_effects import expire
from tests.integration.test_nebius_application_kubernetes import KubernetesAPI
from tests.integration.test_nebius_application_material import management_key as management_key
from tests.integration.test_nebius_application_operations import applications as applications
from tests.integration.test_nebius_application_retirement import API, PODS, WEB
from tests.integration.test_nebius_application_runtime import FENCE_PATH
from tests.integration.test_nebius_environment_management import (
    environment_registry as environment_registry,
)
from tests.unit.test_nebius_application_render import inputs
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


@pytest.fixture
async def stopped_context(applications, platform_inputs, database_access, shared_ca, request):
    _, _, shared, _ = inputs(platform_inputs)
    authority = ApplicationNamespaceAuthorityV1(installation_id=uuid4(), namespace="loom-nebius-management",
        cluster_id=shared.cluster_id, data_environment_id=database_access[3], shared_namespace=shared.platform_namespace)
    credentials, registry, factory, alice, row, original, cloud, _ = await setup(
        applications, platform_inputs, database_access, shared_ca, authority=authority)
    early = getattr(request, 'param', None) == 'early'
    if not early:
        await credentials.prepare(original)
    api = KubernetesAPI()
    async with httpx.AsyncClient(base_url="https://kubernetes.test", transport=httpx.MockTransport(api.handle)) as http:
        kubernetes = ApplicationKubernetesProvider(registry, http)
        runtime = ApplicationRuntimeProvider(registry, kubernetes, authority=authority)
        plan = await registry.frozen_plan(original)
        if not early:
            await runtime.ensure_namespace(original)
            for docs in plan['files'].values():
                for doc in docs:
                    if doc['kind'] in {'Deployment', 'Service', 'Ingress'}:
                        await kubernetes.create(original, doc['kind'] + ':' + doc['metadata']['name'], doc)
            for path in (API, WEB):
                api.objects[path]['metadata']['generation'] = 1
                api.objects[path]['status'] = {'observedGeneration': 1}
        api.objects[PODS] = {'kind': 'PodList', 'metadata': {'resourceVersion': '10'}, 'items': []}
        stopped = await registry.transition(row.application_id, principal=alice, action='suspend',
            idempotency_key='stop', expected_generation=1)
        lease = await registry.claim(stopped.operation_id, lease_seconds=300)
        with pytest.raises(ProviderWaitingError):
            await runtime.stop_workloads(lease)
        api.objects[FENCE_PATH]['status'] = {'hard': {'pods': '0'}}
        deployment = next(doc for docs in plan['files'].values() for doc in docs
                          if doc['kind'] == 'Deployment' and doc['metadata']['name'] == 'loom-service')
        env = deployment['spec']['template']['spec']['containers'][0]['env']
        endpoint = next(item['value'] for item in env if item['name'] == 'LOOM_SVC_MINIO_ENDPOINT')
        async with httpx.AsyncClient(base_url=endpoint, transport=httpx.MockTransport(
            lambda request: httpx.Response(403, text='<Error><Code>InvalidAccessKeyId</Code></Error>'),
        )) as storage_http:
            from tests.integration.test_nebius_application_credentials import object_verifier

            verifier = object_verifier(storage_http, credentials, platform_inputs, plan)
            yield registry, factory, alice, lease, runtime, credentials, verifier, api, cloud, original


async def evidence(context):
    _, _, _, lease, runtime, credentials, verifier, *_ = context
    await runtime.stop_workloads(lease)
    database = await credentials.retire_database(lease)
    objects = await credentials.retire_cloud(lease, verifier)
    return ApplicationStopEvidence(workloads=await runtime.stop_workloads(lease), database=database, objects=objects)


async def charged(context):
    _, factory, _, lease, *_ = context
    async with factory() as session:
        row = await session.get(NebiusApplicationReservation, lease.application_id)
        return row.cpu_millis, row.memory_mib, row.storage_mib, row.ephemeral_storage_mib


async def test_completion_atomically_retains_evidence_names_and_shared_data(stopped_context, database_access):
    registry, factory, alice, lease, *_ = stopped_context
    before = await registry.frozen_plan(lease)
    assert (await charged(stopped_context))[0] > 0
    database_access[0].execute("INSERT INTO shared_records(value) VALUES ('retained accepted work')")
    proof = await evidence(stopped_context)
    await registry.complete_stopped(lease, proof)
    async with factory() as session:
        operation = await session.get(NebiusApplicationOperation, lease.operation_id)
        assert operation.phase == 'completed'
        assert operation.lease_token is operation.lease_expires_at is None
        assert operation.completed_at is not None
        assert operation.completion_json == proof.model_dump(mode='json')
        assert operation.plan_json == before
        completed_at = operation.completed_at
        assert (await session.get(NebiusApplication, lease.application_id)).desired_state == 'suspended'
        assert len((await session.scalars(select(NebiusDeploymentNameClaim).where(
            NebiusDeploymentNameClaim.application_id == lease.application_id))).all()) == 3
    assert await charged(stopped_context) == (0, 0, 0, 0)
    assert database_access[0].execute('SELECT value FROM shared_records').fetchall() == [('retained accepted work',)]
    await registry.complete_stopped(lease, proof)
    async with factory() as session:
        assert (await session.get(NebiusApplicationOperation, lease.operation_id)).completed_at == completed_at
    assert (await registry.get_operation(lease.operation_id, principal=alice)).phase == 'completed'


@pytest.mark.parametrize('damage', ['namespace', 'fence', 'deployment', 'omitted_deployment', 'key', 'omitted_key',
                                  'database', 'generation', 'lease', 'operation'])
async def test_invalid_retirement_evidence_never_releases_reservation(stopped_context, damage):
    registry, _, _, lease, *_ = stopped_context
    proof = (await evidence(stopped_context)).model_dump()
    if damage in {'namespace', 'fence'}:
        proof['workloads'][damage]['uid'] = 'different-uid'
    elif damage == 'deployment':
        proof['workloads']['deployments'][0]['uid'] = 'different-uid'
    elif damage == 'omitted_deployment':
        proof['workloads']['deployments'] = ()
    elif damage == 'key':
        proof['objects']['keys'][0]['access_key_sha256'] = '0' * 64
    elif damage == 'omitted_key':
        proof['objects']['keys'] = ()
    elif damage == 'database':
        proof['database']['identity']['data_environment_id'] = uuid4()
    elif damage == 'generation':
        proof['database']['retired_through'] = 1
    else:
        proof['workloads']['identity']['lease_sha256' if damage == 'lease' else 'operation_id'] = (
            '0' * 64 if damage == 'lease' else uuid4())
    with pytest.raises(ManagementError, match='application_completion_evidence_conflict'):
        await registry.complete_stopped(lease, ApplicationStopEvidence.model_validate(proof))
    assert (await charged(stopped_context))[0] > 0


async def test_old_lease_proof_cannot_complete_after_takeover_or_transition(stopped_context):
    registry, factory, alice, lease, *_ = stopped_context
    proof = await evidence(stopped_context)
    await expire(factory, lease)
    current = await registry.claim(lease.operation_id)
    with pytest.raises(ManagementError, match='stale_operation_lease'):
        await registry.complete_stopped(lease, proof)
    with pytest.raises(ManagementError, match='application_completion_evidence_conflict'):
        await registry.complete_stopped(current, proof)
    await registry.transition(lease.application_id, principal=alice, action='destroy_retained',
        idempotency_key='destroy', expected_generation=2)
    with pytest.raises(ManagementError, match='stale_operation_lease'):
        await registry.complete_stopped(current, proof)
    assert (await charged(stopped_context))[0] > 0


async def test_completion_replay_rejects_changed_proof_token_or_supersession(stopped_context):
    registry, _, alice, lease, *_ = stopped_context
    proof = await evidence(stopped_context)
    await registry.complete_stopped(lease, proof)
    altered = proof.model_dump()
    altered['workloads']['pods_resource_version'] = '999'
    with pytest.raises(ManagementError):
        await registry.complete_stopped(lease, ApplicationStopEvidence.model_validate(altered))
    with pytest.raises(ManagementError):
        await registry.complete_stopped(replace(lease, lease_token=uuid4()), proof)
    await registry.transition(lease.application_id, principal=alice, action='destroy_retained',
        idempotency_key='destroy', expected_generation=2)
    with pytest.raises(ManagementError, match='stale_operation_lease'):
        await registry.complete_stopped(lease, proof)


@pytest.mark.parametrize('phase', ['prepared', 'dispatched'])
async def test_unresolved_current_effect_cannot_release_capacity(stopped_context, phase):
    registry, _, _, lease, *_ = stopped_context
    proof = await evidence(stopped_context)
    fence = proof.workloads.fence
    await registry.prepare_effect(lease, 'unresolved', dict(api_version='v1', kind='ResourceQuota',
        namespace='loom-dev-alice', name='loom-application-retired', action='patch', uid=fence.uid,
        resource_version=fence.resource_version, request_sha256='a' * 64))
    if phase == 'dispatched':
        await registry.dispatch_effect(lease, 'unresolved')
    with pytest.raises(ManagementError, match='application_completion_effects_pending'):
        await registry.complete_stopped(lease, proof)
    assert (await charged(stopped_context))[0] > 0


async def test_completion_and_admission_preserve_sibling_and_single_budget(stopped_context, applications):
    from loom.db.nebius_environment_schema import NebiusPlatformBudget

    registry, factory, alice, lease, *_ = stopped_context
    _, _, _, prepare, _, _ = applications
    proof = await evidence(stopped_context)
    sibling_plan = prepare('sibling')
    sibling = await registry.create(principal=alice, idempotency_key='sibling', **sibling_plan)
    async with factory.begin() as session:
        budget = await session.get(NebiusPlatformBudget, sibling_plan['prepared'].registration.cluster_id)
        budget.cpu_millis = (await charged(stopped_context))[0] + sibling_plan['prepared'].platform_envelope.cpu_millis
    results = await asyncio.gather(registry.complete_stopped(lease, proof), *[
        registry.create(principal=alice, idempotency_key=slug, **prepare(slug)) for slug in ('new-a', 'new-b')
    ], return_exceptions=True)
    assert results[0] is None
    assert sum(not isinstance(result, Exception) for result in results[1:]) <= 1
    async with factory() as session:
        row = await session.get(NebiusApplicationReservation, sibling.application_id)
        assert row.cpu_millis == sibling_plan['prepared'].platform_envelope.cpu_millis
        assert row.memory_mib == sibling_plan['prepared'].platform_envelope.memory_mib
    if all(isinstance(result, Exception) for result in results[1:]):
        await registry.create(principal=alice, idempotency_key='new-a', **prepare('new-a'))


async def test_stop_coordinator_composes_retirement_before_durable_completion(stopped_context, database_access):
    from loom_service.application_management.coordinator import ApplicationLifecycleCoordinator

    registry, factory, _, lease, runtime, credentials, verifier, api, cloud, _ = stopped_context
    coordinator = ApplicationLifecycleCoordinator(registry, runtime, credentials, verifier)
    api.objects[PODS]['items'] = [{'metadata': {'name': 'still-running'}}]
    with pytest.raises(ProviderWaitingError, match='application_workloads_retirement_pending'):
        await coordinator.stop(lease)
    assert database_access[0].execute('SELECT retired_through FROM loom_application_access.applications').fetchone() == (0,)
    assert len(cloud.resources) == 4
    assert (await charged(stopped_context))[0] > 0
    api.objects[PODS]['items'] = []
    await coordinator.stop(lease)
    assert cloud.resources == {}
    assert database_access[0].execute('SELECT retired_through FROM loom_application_access.applications').fetchone() == (2,)
    assert await charged(stopped_context) == (0, 0, 0, 0)
    async with factory() as session:
        operation = await session.get(NebiusApplicationOperation, lease.operation_id)
        assert operation.phase == 'completed'
        assert operation.completion_json['workloads']['pods_resource_version'] == '10'


@pytest.mark.parametrize('boundary', ['sql', 'cloud', 'object', 'last_process_check', 'cancel'])
async def test_coordinator_failure_never_completes_or_releases(stopped_context, monkeypatch, boundary):
    from loom_service.application_management.coordinator import ApplicationLifecycleCoordinator

    registry, factory, _, lease, runtime, credentials, verifier, api, cloud, _ = stopped_context
    coordinator = ApplicationLifecycleCoordinator(registry, runtime, credentials, verifier)
    async def fail(*args, **kwargs):
        if boundary == 'cancel':
            raise asyncio.CancelledError
        raise ProviderWaitingError('injected_provider_failure')

    if boundary in {'sql', 'cancel'}:
        monkeypatch.setattr(credentials.database, 'drain', fail)
    elif boundary == 'cloud':
        monkeypatch.setattr(cloud, 'delete_resource', fail)
    elif boundary == 'object':
        monkeypatch.setattr(verifier, 'verify_retired', fail)
    else:
        verify = verifier.verify_retired
        async def new_pod(*args, **kwargs):
            await verify(*args, **kwargs)
            api.objects[PODS]['items'] = [{'metadata': {'name': 'late-terminating-pod'}}]
        monkeypatch.setattr(verifier, 'verify_retired', new_pod)
    with pytest.raises(asyncio.CancelledError if boundary == 'cancel' else ProviderWaitingError):
        await coordinator.stop(lease)
    assert (await charged(stopped_context))[0] > 0
    async with factory() as session:
        operation = await session.get(NebiusApplicationOperation, lease.operation_id)
        assert operation.phase == 'running' and operation.completion_json is operation.completed_at is None


@pytest.mark.parametrize('stopped_context', ['early'], indirect=True)
async def test_stop_before_any_dispatch_completes_without_grants_or_workloads(stopped_context, database_access):
    from loom_service.application_management.coordinator import ApplicationLifecycleCoordinator

    registry, factory, _, lease, runtime, credentials, verifier, api, cloud, _ = stopped_context
    await ApplicationLifecycleCoordinator(registry, runtime, credentials, verifier).stop(lease)
    assert cloud.mutations == []
    assert [body['kind'] for method, _, body in api.mutations if method == 'POST'] == ['Namespace', 'RoleBinding', 'ResourceQuota']
    assert database_access[0].execute('SELECT count(*) FROM loom_application_access.generations').fetchone() == (0,)
    assert await charged(stopped_context) == (0, 0, 0, 0)
    async with factory() as session:
        receipt = (await session.get(NebiusApplicationOperation, lease.operation_id)).completion_json
        assert receipt['objects']['keys'] == receipt['workloads']['deployments'] == []


async def test_coordinator_cannot_stop_active_generation(stopped_context, applications):
    from loom_service.application_management.coordinator import ApplicationLifecycleCoordinator
    from loom_service.environment_management.provider import ProviderBlockedError

    registry, _, alice, _, runtime, credentials, verifier, api, cloud, _ = stopped_context
    active = await registry.create(principal=alice, idempotency_key='active', **applications[3]('active'))
    lease = await registry.claim(active.operation_id)
    mutations = len(api.mutations), len(cloud.mutations)
    with pytest.raises(ProviderBlockedError, match='application_stop_not_requested'):
        await ApplicationLifecycleCoordinator(registry, runtime, credentials, verifier).stop(lease)
    assert (len(api.mutations), len(cloud.mutations)) == mutations


@pytest.mark.parametrize('phase', ['prepared', 'dispatched'])
async def test_historical_dispatch_is_not_confused_with_unsent_intent(stopped_context, phase):
    registry, _, alice, lease, runtime, credentials, verifier, *_ = stopped_context
    proof = await evidence(stopped_context)
    created = next(item for item in await registry.effect_history(lease)
        if item.intent.kind == 'Service' and item.intent.name == 'loom-service' and item.intent.action == 'create')
    await registry.prepare_effect(lease, 'late-delete', dict(api_version='v1', kind='Service',
        namespace='loom-dev-alice', name='loom-service', action='delete', uid=created.observed_uid,
        resource_version=created.observed_resource_version, request_sha256='c' * 64))
    if phase == 'dispatched':
        await registry.dispatch_effect(lease, 'late-delete')
    operation = await registry.transition(lease.application_id, principal=alice, action='destroy_retained',
        idempotency_key='destroy', expected_generation=2)
    current = await registry.claim(operation.operation_id)
    database = await credentials.retire_database(current)
    objects = await credentials.retire_cloud(current, verifier)
    # Independently pin the registry gate: a stale process snapshot cannot hide
    # an unresolved predecessor even when all its identity fields look current.
    workloads = proof.workloads.model_dump() | {
        'identity': database.identity, 'fence': await runtime.close_admission(current)}
    candidate = ApplicationStopEvidence.model_validate(dict(workloads=workloads, database=database, objects=objects))
    if phase == 'dispatched':
        with pytest.raises(ManagementError, match='application_completion_effects_pending'):
            await registry.complete_stopped(current, candidate)
        assert (await charged(stopped_context))[0] > 0
    else:
        await registry.complete_stopped(current, candidate)
        assert await charged(stopped_context) == (0, 0, 0, 0)
