"""Active lifecycle recovery observes retirement without re-entering mutations."""
from __future__ import annotations

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
