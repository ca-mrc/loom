"""Actual application journal coordinates the fixed Kubernetes Pod-admission gate."""
from __future__ import annotations

import asyncio
from uuid import uuid4

import httpx
import pytest

from loom.nebius_application_authority import ApplicationNamespaceAuthorityV1
from loom.nebius_application_render import render_application
from loom_service.application_management.kubernetes import ApplicationKubernetesProvider
from loom_service.environment_management.provider import ProviderBlockedError, ProviderWaitingError
from loom_service.environment_management.registry import ManagementError
from tests.integration.test_nebius_application_kubernetes import KubernetesAPI
from tests.integration.test_nebius_application_operations import applications as applications
from tests.integration.test_nebius_environment_management import (
    environment_registry as environment_registry,
)
from tests.unit.test_nebius_application_render import inputs, named
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs

FENCE_PATH = "/api/v1/namespaces/loom-dev-alice/resourcequotas/loom-application-retired"


async def runtime_inputs(applications, platform_inputs, *, fixture_image=None):
    registry, _, (alice, _), _, _, _ = applications
    row, release, shared, foundation = inputs(platform_inputs)
    if fixture_image is not None:
        release = release.model_copy(update={"service_image_ref": fixture_image, "web_image_ref": fixture_image})
    row = row.model_copy(update={"owner_user_id": alice.user_id, "owner_team_id": alice.team_id})
    authority = ApplicationNamespaceAuthorityV1(installation_id=uuid4(), namespace="loom-nebius-management",
        cluster_id=shared.cluster_id, data_environment_id=shared.data_environment_id,
        shared_namespace=shared.platform_namespace)
    rendered = render_application(row, release, shared, foundation, authority=authority)
    if fixture_image is not None:
        # Freeze harmless runnable templates for disposable controller tests;
        # never alter an already-journaled manifest or any production renderer.
        for docs in rendered.files.values():
            for doc in docs:
                if doc["kind"] == "Deployment":
                    pod = doc["spec"]["template"]["spec"]
                    pod.pop("nodeSelector")
                    pod.pop("volumes", None)
                    pod["terminationGracePeriodSeconds"] = 1
                    container = pod["containers"][0]
                    container.pop("volumeMounts", None)
                    container.pop("readinessProbe")
                    container.update(env=[], command=["/fixture", "idle"])
    operation = await registry.create(principal=alice, idempotency_key="create",
        prepared=rendered, release=release, shared=shared)
    lease = await registry.claim(operation.operation_id)
    return registry, authority, lease, alice, rendered


@pytest.fixture
async def runtime_context(applications, platform_inputs):
    registry, authority, lease, alice, rendered = await runtime_inputs(applications, platform_inputs)
    api = KubernetesAPI()
    async with httpx.AsyncClient(base_url="https://kubernetes.test", transport=httpx.MockTransport(api.handle)) as http:
        kubernetes = ApplicationKubernetesProvider(registry, http)
        await kubernetes.create(lease, "namespace", named(rendered, "Namespace", rendered.registration.application_namespace))
        yield registry, kubernetes, authority, api, lease, alice


def runtime(context):
    from loom_service.application_management.runtime import ApplicationRuntimeProvider

    return ApplicationRuntimeProvider(context[0], context[1], authority=context[2])


async def close_ready(context):
    provider = runtime(context)
    with pytest.raises(ProviderWaitingError, match="application_pod_fence_pending"):
        await provider.close_admission(context[4])
    context[3].objects[FENCE_PATH]["status"] = {"hard": {"pods": "0"}}
    await provider.close_admission(context[4])
    return provider


async def test_close_waits_for_live_quota_status_and_replay_never_reposts(runtime_context):
    provider = await close_ready(runtime_context)
    _, _, _, api, lease, _ = runtime_context
    await provider.close_admission(lease)
    assert [method for method, _, _ in api.mutations] == ["POST", "POST"]
    assert api.objects[FENCE_PATH]["spec"] == {"hard": {"pods": "0"}}
    api.objects[FENCE_PATH]["status"]["hard"]["pods"] = "4"
    with pytest.raises(ProviderWaitingError, match="application_pod_fence_pending"):
        await provider.close_admission(lease)


async def test_lost_fence_create_reply_reconciles_without_a_second_create(runtime_context):
    provider = runtime(runtime_context)
    _, _, _, api, lease, _ = runtime_context
    api.lose_response = True
    with pytest.raises(ProviderWaitingError):
        await provider.close_admission(lease)
    late = api.objects.pop(FENCE_PATH)
    with pytest.raises(ProviderWaitingError):
        await provider.close_admission(lease)
    assert len(api.mutations) == 2
    late["status"] = {"hard": {"pods": "0"}}
    api.objects[FENCE_PATH] = late
    await provider.close_admission(lease)
    assert len(api.mutations) == 2


async def test_suspend_advances_exact_fence_and_rejects_old_lease(runtime_context):
    provider = await close_ready(runtime_context)
    registry, _, _, api, lease, alice = runtime_context
    original_uid = api.objects[FENCE_PATH]["metadata"]["uid"]
    stopped = await registry.transition(lease.application_id, principal=alice, idempotency_key="stop",
        action="suspend", expected_generation=1)
    current = await registry.claim(stopped.operation_id)
    await provider.close_admission(current)
    quota = api.objects[FENCE_PATH]
    assert quota["metadata"]["uid"] == original_uid
    assert quota["metadata"]["annotations"]["loom.nebius/deployment-generation"] == "2"
    assert quota["metadata"]["annotations"]["loom.nebius/operation-id"] == str(current.operation_id)
    assert [method for method, _, _ in api.mutations] == ["POST", "POST", "PATCH"]
    with pytest.raises(ManagementError, match="stale_operation_lease"):
        await provider.close_admission(lease)
    assert len(api.mutations) == 3


@pytest.mark.parametrize("damage", ["uid", "scope", "future", "operation", "identity", "pods", "missing"])
async def test_altered_or_missing_fence_never_grants_permission_to_recreate_or_patch(runtime_context, damage):
    provider = await close_ready(runtime_context)
    _, _, _, api, lease, _ = runtime_context
    quota = api.objects[FENCE_PATH]
    if damage == "uid":
        quota["metadata"]["uid"] = "replacement"
    elif damage == "scope":
        quota["spec"]["scopes"] = ["BestEffort"]
    elif damage == "future":
        quota["metadata"]["annotations"]["loom.nebius/deployment-generation"] = "2"
    elif damage == "operation":
        quota["metadata"]["annotations"]["loom.nebius/operation-id"] = str(uuid4())
    elif damage == "identity":
        quota["metadata"]["labels"]["loom.nebius/application-id"] = str(uuid4())
    elif damage == "pods":
        quota["spec"]["hard"]["pods"] = "5"
    else:
        del api.objects[FENCE_PATH]
    with pytest.raises(ProviderBlockedError, match="application_pod_fence_conflict"):
        await provider.close_admission(lease)
    assert len(api.mutations) == 2


async def test_wrong_installation_cannot_close_admission(runtime_context):
    from loom_service.application_management.runtime import ApplicationRuntimeProvider

    registry, kubernetes, authority, api, lease, _ = runtime_context
    provider = ApplicationRuntimeProvider(registry, kubernetes,
        authority=authority.model_copy(update={"installation_id": uuid4()}))
    with pytest.raises(ProviderBlockedError, match="application_runtime_authority_conflict"):
        await provider.close_admission(lease)
    assert len(api.mutations) == 1


async def test_destroy_reconciles_uncertain_suspend_patch_before_advancing_again(runtime_context):
    provider = await close_ready(runtime_context)
    registry, _, _, api, lease, alice = runtime_context
    stopped = await registry.transition(lease.application_id, principal=alice, idempotency_key="stop",
        action="suspend", expected_generation=1)
    current = await registry.claim(stopped.operation_id)
    api.lose_response = True
    with pytest.raises(ProviderWaitingError):
        await provider.close_admission(current)
    destroyed = await registry.transition(lease.application_id, principal=alice, idempotency_key="destroy",
        action="destroy_retained", expected_generation=2)
    latest = await registry.claim(destroyed.operation_id)
    api.lose_response = False
    await provider.close_admission(latest)
    assert [method for method, _, _ in api.mutations] == ["POST", "POST", "PATCH", "PATCH"]
    assert all(effect.phase == "observed" for effect in await registry.effect_history(latest))
    assert api.objects[FENCE_PATH]["metadata"]["annotations"]["loom.nebius/deployment-generation"] == "3"


async def test_prepared_current_patch_survives_version_churn_before_dispatch(runtime_context, monkeypatch):
    provider = await close_ready(runtime_context)
    registry, _, _, api, lease, alice = runtime_context
    stopped = await registry.transition(lease.application_id, principal=alice, idempotency_key="stop",
        action="suspend", expected_generation=1)
    current = await registry.claim(stopped.operation_id)
    dispatch = registry.dispatch_effect

    async def interrupted(*args, **kwargs):
        raise asyncio.CancelledError  # prepare_effect already committed in the real DB.

    monkeypatch.setattr(registry, "dispatch_effect", interrupted)
    with pytest.raises(asyncio.CancelledError):
        await provider.close_admission(current)
    prepared = (await registry.effect_history(current))[-1]
    assert prepared.phase == "prepared" and prepared.intent.resource_version == "1"
    monkeypatch.setattr(registry, "dispatch_effect", dispatch)
    api.objects[FENCE_PATH]["metadata"]["resourceVersion"] = "99"
    api.reject_next = 422  # Kubernetes rejects the original, now-stale exact RV.
    with pytest.raises(ProviderWaitingError):
        await provider.close_admission(current)
    rejected = (await registry.effect_history(current))[-1]
    assert rejected.key == prepared.key and rejected.phase == "rejected"
    assert api.mutations[-1][2][1] == {"op": "test", "path": "/metadata/resourceVersion", "value": "1"}
    await provider.close_admission(current)
    assert api.mutations[-1][2][1] == {"op": "test", "path": "/metadata/resourceVersion", "value": "99"}
    assert [effect.phase for effect in (await registry.effect_history(current))[-2:]] == ["rejected", "observed"]
    assert len(api.mutations) == 4


@pytest.mark.parametrize("lost_reply", [False, True])
async def test_peer_fence_creation_during_read_is_not_a_permanent_identity_conflict(
    runtime_context, monkeypatch, lost_reply,
):
    provider = runtime(runtime_context)
    _, kubernetes, _, api, lease, _ = runtime_context
    read = provider._read
    first = True

    async def peer_writes_before_read(current, namespace):
        nonlocal first
        if first:
            first = False
            api.lose_response = lost_reply
            try:
                await kubernetes.create(current, "pod-fence:create", await provider._fence(current))
            except ProviderWaitingError:
                assert lost_reply
            api.lose_response = False
            api.objects[FENCE_PATH]["status"] = {"hard": {"pods": "0"}}
        return await read(current, namespace)

    monkeypatch.setattr(provider, "_read", peer_writes_before_read)
    if lost_reply:
        with pytest.raises(ProviderWaitingError):
            await provider.close_admission(lease)
    await provider.close_admission(lease)
    assert len(api.mutations) == 2


async def test_peer_prepares_patch_after_initial_history_scan_remains_pending(runtime_context, monkeypatch):
    provider = await close_ready(runtime_context)
    registry, kubernetes, _, api, lease, alice = runtime_context
    stopped = await registry.transition(lease.application_id, principal=alice, idempotency_key="stop",
        action="suspend", expected_generation=1)
    current = await registry.claim(stopped.operation_id)
    read, dispatch = provider._read, registry.dispatch_effect
    first = True

    async def interrupted(*args, **kwargs):
        raise asyncio.CancelledError

    async def peer_prepares_before_read(active, namespace):
        nonlocal first
        if first:
            first = False
            monkeypatch.setattr(registry, "dispatch_effect", interrupted)
            with pytest.raises(asyncio.CancelledError):
                await kubernetes.patch_spec(active, "peer-prepared-patch", await provider._fence(active),
                    uid=api.objects[FENCE_PATH]["metadata"]["uid"], resource_version="1")
            monkeypatch.setattr(registry, "dispatch_effect", dispatch)
            api.objects[FENCE_PATH]["metadata"]["resourceVersion"] = "99"
        return await read(active, namespace)

    monkeypatch.setattr(provider, "_read", peer_prepares_before_read)
    with pytest.raises(ProviderWaitingError):
        await provider.close_admission(current)
    assert len(api.mutations) == 2
    api.reject_next = 422
    with pytest.raises(ProviderWaitingError):
        await provider.close_admission(current)
    await provider.close_admission(current)
    assert len(api.mutations) == 4
