"""Concrete preparation composes real databases and journal-backed provider I/O."""
from __future__ import annotations

import copy
from uuid import uuid4

import httpx
import pytest

from loom.db.nebius_application_operation_schema import (
    NebiusApplicationOperation,
    NebiusApplicationReservation,
)
from loom.nebius_application_authority import ApplicationNamespaceAuthorityV1
from loom.nebius_application_network import application_shared_network_policies
from loom_service.application_management.coordinator import ApplicationLifecycleCoordinator
from loom_service.application_management.kubernetes import ApplicationKubernetesProvider
from loom_service.application_management.object_access import ApplicationObjectAccessVerifier
from loom_service.application_management.runtime import ApplicationRuntimeProvider
from loom_service.environment_management.provider import ProviderBlockedError, ProviderWaitingError
from loom_service.environment_management.registry import ManagementError
from tests.integration.test_nebius_application_credentials import database_access as database_access
from tests.integration.test_nebius_application_credentials import setup
from tests.integration.test_nebius_application_credentials import shared_ca as shared_ca
from tests.integration.test_nebius_application_kubernetes import KubernetesAPI
from tests.integration.test_nebius_application_material import management_key as management_key
from tests.integration.test_nebius_application_operations import applications as applications
from tests.integration.test_nebius_application_runtime import FENCE_PATH
from tests.integration.test_nebius_environment_management import (
    environment_registry as environment_registry,
)
from tests.unit.test_nebius_application_render import inputs
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


@pytest.fixture
async def preparation(applications, platform_inputs, database_access, shared_ca):
    shared = inputs(platform_inputs)[2]
    authority = ApplicationNamespaceAuthorityV1(installation_id=uuid4(), namespace="loom-nebius-management",
        cluster_id=shared.cluster_id, data_environment_id=database_access[3], shared_namespace=shared.platform_namespace)
    credentials, registry, factory, alice, row, lease, cloud, _ = await setup(
        applications, platform_inputs, database_access, shared_ca, authority=authority)
    api = KubernetesAPI()
    api.objects['/api/v1/namespaces/loom-dev-alice/pods'] = {
        'kind': 'PodList', 'metadata': {'resourceVersion': '10'}, 'items': []}
    for index, doc in enumerate(application_shared_network_policies(authority)):
        doc['metadata'].update(uid=f'shared-{index}', resourceVersion='1')
        api.objects[f"/apis/networking.k8s.io/v1/namespaces/{shared.platform_namespace}/networkpolicies/{doc['metadata']['name']}"] = doc
    async with httpx.AsyncClient(base_url='https://kubernetes.test', transport=httpx.MockTransport(api.handle)) as http:
        runtime = ApplicationRuntimeProvider(registry, ApplicationKubernetesProvider(registry, http), authority=authority)
        # Fresh generation has no retired key to probe. Any unexpected access
        # reaches a real verifier with controlled I/O, never a success callback.
        async with httpx.AsyncClient(base_url='https://storage.eu-north1.nebius.cloud',
                transport=httpx.MockTransport(lambda request: httpx.Response(500))) as storage_http:
            coordinator = ApplicationLifecycleCoordinator(registry, runtime, credentials, ApplicationObjectAccessVerifier(storage_http))
            yield coordinator, registry, factory, alice, lease, api, cloud, row


async def close_quota(context):
    coordinator, _, _, _, lease, api, cloud, _ = context
    with pytest.raises(ProviderWaitingError, match='application_pod_fence_pending'):
        await coordinator.prepare(lease)
    assert cloud.mutations == []
    api.objects[FENCE_PATH]['status'] = {'hard': {'pods': '0'}}


async def test_concrete_preparation_returns_live_evidence_without_starting_or_completing(preparation, database_access):
    coordinator, registry, factory, _, lease, api, cloud, row = preparation
    database_access[0].execute("INSERT INTO public.shared_records(value) VALUES ('retained work')")
    await close_quota(preparation)
    proof = await coordinator.prepare(lease)
    assert len(proof.prepared.resources) == 8 and len(proof.network) == 3
    assert proof.workloads.deployments == () and proof.database.retired_through == 0 and proof.objects.keys == ()
    assert proof.access.membership_role == 'member'
    assert all(item.identity == proof.prepared.identity for item in (
        proof.workloads, proof.database, proof.objects, proof.access))
    assert api.objects[FENCE_PATH]['spec']['hard'] == {'pods': '0'}
    assert not any(obj['kind'] in {'Deployment', 'Service', 'Ingress', 'Pod'} for obj in api.objects.values())
    assert database_access[0].execute('SELECT value FROM public.shared_records').fetchall() == [('retained work',)]
    async with factory() as session:
        operation = await session.get(NebiusApplicationOperation, lease.operation_id)
        reservation = await session.get(NebiusApplicationReservation, row.application_id)
        assert operation.phase != 'completed' and operation.completion_json is None
        assert reservation.cpu_millis > 0 and reservation.memory_mib > 0 and reservation.storage_mib == 0
    assert not await registry.activation_started(lease)
    before = copy.deepcopy(api.mutations), copy.deepcopy(cloud.mutations)
    replay = await coordinator.prepare(lease)
    assert replay == proof
    assert (api.mutations, cloud.mutations) == before


@pytest.mark.parametrize('condition', ['missing-network', 'running-pod'])
async def test_concrete_preparation_keeps_admission_closed_when_prerequisites_are_missing(preparation, condition):
    coordinator, _, _, _, lease, api, cloud, _ = preparation
    await close_quota(preparation)
    if condition == 'missing-network':
        path = next(path for path, value in api.objects.items() if value['kind'] == 'NetworkPolicy')
        del api.objects[path]
        expected = ProviderBlockedError
    else:
        api.objects['/api/v1/namespaces/loom-dev-alice/pods']['items'] = [{'metadata': {'name': 'still-running'}}]
        expected = ProviderWaitingError
    with pytest.raises(expected):
        await coordinator.prepare(lease)
    assert api.objects[FENCE_PATH]['spec']['hard'] == {'pods': '0'}
    assert cloud.mutations == []


async def test_concrete_preparation_does_not_return_proof_after_source_supersession(preparation, monkeypatch):
    coordinator, registry, _, alice, lease, api, _, _ = preparation
    await close_quota(preparation)
    qualify = coordinator.credentials.qualify

    async def supersede(current):
        result = await qualify(current)
        await registry.transition(lease.application_id, principal=alice, idempotency_key='stop',
            action='suspend', expected_generation=1)
        return result

    monkeypatch.setattr(coordinator.credentials, 'qualify', supersede)
    with pytest.raises(ManagementError, match='stale_operation_lease'):
        await coordinator.prepare(lease)
    assert api.objects[FENCE_PATH]['spec']['hard'] == {'pods': '0'}
    assert not any(obj['kind'] == 'Deployment' for obj in api.objects.values())


async def test_stopped_generation_cannot_enter_concrete_preparation(preparation):
    coordinator, registry, _, alice, lease, api, cloud, _ = preparation
    stopped = await registry.transition(lease.application_id, principal=alice, idempotency_key='stop',
        action='suspend', expected_generation=1)
    current = await registry.claim(stopped.operation_id)
    with pytest.raises(ProviderBlockedError, match='application_preparation_not_requested'):
        await coordinator.prepare(current)
    assert api.mutations == cloud.mutations == []
