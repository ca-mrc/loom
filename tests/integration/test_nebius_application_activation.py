"""Active lifecycle recovery observes retirement without re-entering mutations."""
from __future__ import annotations

import asyncio
import copy
import hashlib

import pytest

from loom_service.environment_management.provider import ProviderBlockedError, ProviderWaitingError
from loom_service.environment_management.registry import ManagementError
from tests.integration.test_nebius_application_effects import expire
from tests.integration.test_nebius_application_operations import applications as applications
from tests.integration.test_nebius_application_retirement import API, PODS, WEB
from tests.integration.test_nebius_application_retirement import retirement as retirement
from tests.integration.test_nebius_application_runtime import FENCE_PATH, close_ready
from tests.integration.test_nebius_application_runtime import runtime_context as runtime_context
from tests.integration.test_nebius_application_startup import next_static_generation
from tests.integration.test_nebius_environment_management import (
    environment_registry as environment_registry,
)
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


@pytest.fixture
async def active_retired(runtime_context):
    api = runtime_context[3]
    api.objects[PODS] = {'kind': 'PodList', 'metadata': {'resourceVersion': '14'}, 'items': []}
    provider = await close_ready(runtime_context)
    proof = await provider.stop_workloads(runtime_context[4])
    return runtime_context, provider, proof


async def test_retirement_readback_does_not_close_admission_or_make_authorization_posts(active_retired, monkeypatch):
    context, provider, proof = active_retired
    client, api, lease = context[1], context[3], context[4]
    request = client._request

    async def only_read(method, path, body=None):
        assert method == 'GET'
        return await request(method, path, body)

    monkeypatch.setattr(client, '_request', only_read)
    writes = copy.deepcopy(api.mutations)
    assert await provider.read_retired(lease) == proof
    assert api.mutations == writes


async def test_prepared_unfence_can_revalidate_retirement_without_dispatching_or_reclosing(active_retired):
    context, provider, proof = active_retired
    registry, _, _, api, lease, _ = context
    metadata = api.objects[FENCE_PATH]['metadata']
    key = 'activate:unfence:' + hashlib.sha256(f"{metadata['uid']}:{metadata['resourceVersion']}".encode()).hexdigest()
    await registry.prepare_effect(lease, key, dict(api_version='v1', kind='ResourceQuota', namespace='loom-dev-alice',
        name='loom-application-retired', action='delete', uid=metadata['uid'], resource_version=metadata['resourceVersion'],
        request_sha256='a' * 64))
    writes = copy.deepcopy(api.mutations)
    assert await provider.read_retired(lease) == proof
    assert (await registry.effect_history(lease))[-1].phase == 'prepared'
    assert api.mutations == writes


@pytest.mark.parametrize('change', ['missing-quota', 'replaced-quota', 'quota-unready', 'running-pod'])
async def test_read_retired_rejects_live_drift_without_repair(active_retired, change):
    context, provider, _ = active_retired
    api, lease = context[3:5]
    if change == 'missing-quota':
        del api.objects[FENCE_PATH]
    elif change == 'replaced-quota':
        api.objects[FENCE_PATH]['metadata']['uid'] = 'foreign'
    elif change == 'quota-unready':
        api.objects[FENCE_PATH]['status']['hard']['pods'] = '1'
    else:
        api.objects[PODS]['items'] = [{'metadata': {'name': 'terminating', 'deletionTimestamp': '2026-09-28T00:00:00Z'}}]
    writes = copy.deepcopy(api.mutations)
    with pytest.raises((ProviderBlockedError, ProviderWaitingError)):
        await provider.read_retired(lease)
    assert api.mutations == writes


async def test_stopped_read_retired_still_requires_observed_zero_replica_controller(retirement):
    context, provider = retirement
    _, _, _, api, lease, _ = context
    proof = await provider.stop_workloads(lease)
    assert await provider.read_retired(lease) == proof
    api.objects[API]['status']['observedGeneration'] = 0
    writes = copy.deepcopy(api.mutations)
    with pytest.raises(ProviderWaitingError, match='application_workloads_retirement_pending'):
        await provider.read_retired(lease)
    assert api.mutations == writes


async def test_read_retired_rejects_expired_lease_and_recovers_under_takeover(active_retired):
    context, provider, proof = active_retired
    registry, _, _, api, lease, _ = context
    await expire(registry.session_factory, lease)
    current = await registry.claim(lease.operation_id)
    writes = copy.deepcopy(api.mutations)
    with pytest.raises(ManagementError, match='stale_operation_lease'):
        await provider.read_retired(lease)
    fresh = await provider.read_retired(current)
    assert fresh.namespace == proof.namespace and fresh.fence == proof.fence
    assert fresh.identity.runner_epoch == current.runner_epoch > proof.identity.runner_epoch
    assert api.mutations == writes


async def test_open_admission_deletes_exact_owned_quota_once_and_replay_reads_live_absence(active_retired):
    context, provider, _ = active_retired
    api, lease = context[3:5]
    metadata = copy.deepcopy(api.objects[FENCE_PATH]['metadata'])
    before = len(api.mutations)
    effect = await provider.open_admission(lease)
    assert effect.phase == 'observed' and effect.intent.action == 'delete'
    assert effect.intent.uid == metadata['uid'] and effect.intent.resource_version == metadata['resourceVersion']
    assert FENCE_PATH not in api.objects
    assert await provider.open_admission(lease) == effect
    assert len(api.mutations) == before + 1
    assert api.mutations[-1][0:2] == ('DELETE', FENCE_PATH)


async def test_prepared_open_retains_original_preconditions_after_takeover_and_rejection(active_retired, monkeypatch):
    context, provider, _ = active_retired
    registry, _, _, api, lease, _ = context
    dispatch = registry.dispatch_effect

    async def interrupted(*args, **kwargs):
        raise asyncio.CancelledError

    monkeypatch.setattr(registry, 'dispatch_effect', interrupted)
    with pytest.raises(asyncio.CancelledError):
        await provider.open_admission(lease)
    prepared = (await registry.effect_history(lease))[-1]
    assert prepared.phase == 'prepared'
    monkeypatch.setattr(registry, 'dispatch_effect', dispatch)
    await expire(registry.session_factory, lease)
    current = await registry.claim(lease.operation_id)
    api.objects[FENCE_PATH]['metadata']['resourceVersion'] = '99'
    api.reject_next = 422
    with pytest.raises(ProviderWaitingError):
        await provider.open_admission(current)
    assert api.mutations[-1][2]['preconditions']['resourceVersion'] == '1'
    rejected = (await registry.effect_history(current))[-1]
    assert rejected.key == prepared.key and rejected.phase == 'rejected'
    observed = await provider.open_admission(current)
    assert observed.key != rejected.key and observed.phase == 'observed'
    assert api.mutations[-1][2]['preconditions']['resourceVersion'] == '99'


async def test_uncertain_open_waits_for_original_delete_without_resending(active_retired):
    context, provider, _ = active_retired
    registry, _, _, api, lease, _ = context
    api.lose_response = api.pending_delete = True
    with pytest.raises(ProviderWaitingError):
        await provider.open_admission(lease)
    before = copy.deepcopy(api.mutations)
    await expire(registry.session_factory, lease)
    current = await registry.claim(lease.operation_id)
    api.lose_response = False
    with pytest.raises(ProviderWaitingError):
        await provider.open_admission(current)
    assert api.mutations == before
    del api.objects[FENCE_PATH]
    assert (await provider.open_admission(current)).phase == 'observed'
    assert api.mutations == before


@pytest.mark.parametrize('uncertain', [False, True])
async def test_open_never_deletes_replacement_quota_even_if_old_delete_is_observed(active_retired, uncertain):
    context, provider, _ = active_retired
    api, lease = context[3:5]
    replacement = copy.deepcopy(api.objects[FENCE_PATH])
    replacement['metadata']['uid'] = 'replacement'
    if uncertain:
        api.lose_response = True
        with pytest.raises(ProviderWaitingError):
            await provider.open_admission(lease)
        api.lose_response = False
    else:
        await provider.open_admission(lease)
    api.objects[FENCE_PATH] = replacement
    before = copy.deepcopy(api.mutations)
    with pytest.raises(ProviderBlockedError, match='application_pod_fence_conflict'):
        await provider.open_admission(lease)
    assert api.mutations == before


async def test_open_refuses_running_pods_and_stopped_successor(active_retired):
    context, provider, _ = active_retired
    registry, _, _, api, lease, alice = context
    api.objects[PODS]['items'] = [{'metadata': {'name': 'still-running'}}]
    before = copy.deepcopy(api.mutations)
    with pytest.raises(ProviderWaitingError, match='application_workloads_retirement_pending'):
        await provider.open_admission(lease)
    api.objects[PODS]['items'] = []
    stopped = await registry.transition(lease.application_id, principal=alice, idempotency_key='stop',
        action='suspend', expected_generation=1)
    current = await registry.claim(stopped.operation_id)
    with pytest.raises(ProviderBlockedError, match='application_activation_not_requested'):
        await provider.open_admission(current)
    with pytest.raises(ManagementError, match='stale_operation_lease'):
        await provider.open_admission(lease)
    assert api.mutations == before


def ready_backends(api):
    for path in (API, WEB):
        api.objects[path]['metadata']['generation'] = 1
        api.objects[path]['status'] = {'observedGeneration': 1, 'replicas': 1,
            'updatedReplicas': 1, 'readyReplicas': 1, 'availableReplicas': 1}


async def pending_backends(active_retired):
    context, provider, _ = active_retired
    await provider.open_admission(context[4])
    with pytest.raises(ProviderWaitingError, match='application_workloads_readiness_pending'):
        await provider.start_workloads(context[4])
    return context, provider


async def test_start_requires_observed_activation_before_any_workload_mutation(active_retired):
    context, provider, _ = active_retired
    api, lease = context[3:5]
    before = copy.deepcopy(api.mutations)
    with pytest.raises(ProviderWaitingError, match='application_activation_pending'):
        await provider.start_workloads(lease)
    assert api.mutations == before


async def test_start_waits_for_backends_then_publishes_exact_current_route_once(active_retired):
    context, provider = await pending_backends(active_retired)
    registry, _, _, api, lease, _ = context
    assert sum(obj['kind'] == 'Deployment' for obj in api.objects.values()) == 2
    assert sum(obj['kind'] == 'Service' for obj in api.objects.values()) == 2
    assert not any(obj['kind'] == 'Ingress' for obj in api.objects.values())
    ready_backends(api)
    proof = await provider.start_workloads(lease)
    assert {item.name for item in proof.deployments} == {'loom-service', 'loom-web'}
    assert {item.name for item in proof.services} == {'loom-service', 'loom-web'}
    assert proof.ingress.name == 'loom-web' and proof.identity.operation_id == lease.operation_id
    assert all(item.replicas == 1 and item.generation == item.observed_generation == 1 for item in proof.deployments)
    assert all(item.operation_id == lease.operation_id for item in (*proof.deployments, *proof.services, proof.ingress))
    activation = next(item for item in await registry.effect_history(lease) if item.key == proof.activation_key)
    assert activation.phase == 'observed' and activation.observed_uid == proof.retired_quota_uid
    before = copy.deepcopy(api.mutations)
    assert await provider.read_ready(lease) == proof
    assert await provider.start_workloads(lease) == proof
    assert api.mutations == before


@pytest.mark.parametrize('field,value', [('observedGeneration', 0), ('updatedReplicas', 0),
    ('readyReplicas', 0), ('availableReplicas', 0), ('replicas', 2), ('unavailableReplicas', 1),
    ('terminatingReplicas', 1)])
async def test_stale_or_partial_backend_readiness_never_publishes_route(active_retired, field, value):
    context, provider = await pending_backends(active_retired)
    api, lease = context[3:5]
    ready_backends(api)
    api.objects[API]['status'][field] = value
    before = copy.deepcopy(api.mutations)
    with pytest.raises(ProviderWaitingError, match='application_workloads_readiness_pending'):
        await provider.start_workloads(lease)
    assert api.mutations == before
    assert not any(obj['kind'] == 'Ingress' for obj in api.objects.values())


@pytest.mark.parametrize('damage', ['image', 'uid', 'quota'])
async def test_ready_readback_rejects_live_drift_without_repair(active_retired, damage):
    context, provider = await pending_backends(active_retired)
    api, lease = context[3:5]
    ready_backends(api)
    await provider.start_workloads(lease)
    if damage == 'image':
        api.objects[API]['spec']['template']['spec']['containers'][0]['image'] = 'other@sha256:' + '9' * 64
    elif damage == 'uid':
        api.objects[API]['metadata']['uid'] = 'replacement'
    else:
        api.objects[FENCE_PATH] = {'metadata': {'name': 'loom-application-retired', 'uid': 'replacement', 'resourceVersion': '1'}}
    before = copy.deepcopy(api.mutations)
    with pytest.raises(ProviderBlockedError):
        await provider.read_ready(lease)
    assert api.mutations == before


@pytest.mark.parametrize('prepared', [True, False])
async def test_interrupted_workload_create_recovers_original_request_without_duplicate(active_retired, monkeypatch, prepared):
    context, provider, _ = active_retired
    registry, _, _, api, lease, _ = context
    await provider.open_admission(lease)
    dispatch = registry.dispatch_effect

    async def interrupt(*args):
        raise asyncio.CancelledError

    if prepared:
        monkeypatch.setattr(registry, 'dispatch_effect', interrupt)
        expected = asyncio.CancelledError
    else:
        api.lose_response = True
        expected = ProviderWaitingError
    with pytest.raises(expected):
        await provider.start_workloads(lease)
    interrupted = (await registry.effect_history(lease))[-1]
    assert interrupted.phase == ('prepared' if prepared else 'dispatched')
    monkeypatch.setattr(registry, 'dispatch_effect', dispatch)
    api.lose_response = False
    await expire(registry.session_factory, lease)
    current = await registry.claim(lease.operation_id)
    with pytest.raises(ProviderWaitingError, match='application_workloads_readiness_pending'):
        await provider.start_workloads(current)
    ready_backends(api)
    await provider.start_workloads(current)
    creates = [body['metadata']['name'] for method, _, body in api.mutations
               if method == 'POST' and body.get('kind') == 'Deployment']
    assert sorted(creates) == ['loom-service', 'loom-web']


async def test_update_patches_retained_deployment_uids_and_recreates_only_retired_routes(active_retired, platform_inputs):
    context, provider = await pending_backends(active_retired)
    registry, _, _, api, lease, _ = context
    ready_backends(api)
    await provider.start_workloads(lease)
    original = {path: api.objects[path]['metadata']['uid'] for path in (API, WEB)}
    current = await next_static_generation(context, platform_inputs)
    with pytest.raises(ProviderWaitingError, match='application_pod_fence_pending'):
        await provider.stop_workloads(current)
    api.objects[FENCE_PATH]['status'] = {'hard': {'pods': '0'}}
    await provider.stop_workloads(current)
    await provider.open_admission(current)
    proof = await provider.start_workloads(current)
    assert proof.identity.deployment_generation == 2
    for path in (API, WEB):
        assert api.objects[path]['metadata']['uid'] == original[path]
        assert api.objects[path]['spec']['replicas'] == 1
        assert api.objects[path]['metadata']['annotations']['loom.nebius/operation-id'] == str(current.operation_id)
    assert sum(method == 'POST' and body.get('kind') == 'Deployment' for method, _, body in api.mutations) == 2
    assert sum(method == 'POST' and body.get('kind') == 'Service' for method, _, body in api.mutations) == 4
    assert all(item.phase == 'observed' for item in await registry.effect_history(current))
