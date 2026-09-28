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
from tests.integration.test_nebius_application_retirement import API, PODS
from tests.integration.test_nebius_application_retirement import retirement as retirement
from tests.integration.test_nebius_application_runtime import FENCE_PATH, close_ready
from tests.integration.test_nebius_application_runtime import runtime_context as runtime_context
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
