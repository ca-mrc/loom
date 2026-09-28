"""Personal process retirement with a real journal and controlled API races."""
from __future__ import annotations

import asyncio
import copy

import httpx
import pytest

from loom_service.application_management.kubernetes import ApplicationKubernetesProvider
from loom_service.application_management.runtime import ApplicationRuntimeProvider
from loom_service.environment_management.provider import ProviderBlockedError, ProviderWaitingError
from tests.integration.test_nebius_application_kubernetes import KubernetesAPI
from tests.integration.test_nebius_application_operations import applications as applications
from tests.integration.test_nebius_application_runtime import (
    FENCE_PATH,
    close_ready,
    runtime_inputs,
)
from tests.integration.test_nebius_application_runtime import (
    runtime_context as runtime_context,
)
from tests.integration.test_nebius_environment_management import (
    environment_registry as environment_registry,
)
from tests.unit.test_nebius_application_render import named
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs

NS = "loom-dev-alice"
PODS = f"/api/v1/namespaces/{NS}/pods"
API = f"/apis/apps/v1/namespaces/{NS}/deployments/loom-service"
WEB = f"/apis/apps/v1/namespaces/{NS}/deployments/loom-web"


@pytest.fixture
async def retirement(runtime_context):
    registry, kubernetes, _, api, lease, alice = runtime_context
    plan = await registry.frozen_plan(lease)
    for docs in plan["files"].values():
        for doc in docs:
            if doc["kind"] in {"Deployment", "Service", "Ingress"}:
                await kubernetes.create(lease, doc["kind"] + ":" + doc["metadata"]["name"], doc)
    api.objects[PODS] = {"apiVersion": "v1", "kind": "PodList",
                         "metadata": {"resourceVersion": "1"}, "items": []}
    for path in (API, WEB):
        api.objects[path]["metadata"]["generation"] = 1
        api.objects[path]["status"] = {"observedGeneration": 1}
    stopped = await registry.transition(lease.application_id, principal=alice,
        idempotency_key="stop", action="suspend", expected_generation=1)
    current = await registry.claim(stopped.operation_id)
    context = (*runtime_context[:4], current, alice)
    provider = await close_ready(context)
    return context, provider


async def test_stop_removes_only_personal_routes_and_preserves_deployment_templates(retirement):
    context, provider = retirement
    _, _, _, api, lease, _ = context
    templates = {path: copy.deepcopy(api.objects[path]["spec"]["template"]) for path in (API, WEB)}
    await provider.stop_workloads(lease)
    assert api.objects[FENCE_PATH]["spec"] == {"hard": {"pods": "0"}}
    for path in (API, WEB):
        assert api.objects[path]["spec"]["replicas"] == 0
        assert api.objects[path]["spec"]["template"] == templates[path]
    deleted = [path for method, path, _ in api.mutations if method == "DELETE"]
    assert set(deleted) == {f"/api/v1/namespaces/{NS}/services/loom-service",
        f"/api/v1/namespaces/{NS}/services/loom-web",
        f"/apis/networking.k8s.io/v1/namespaces/{NS}/ingresses/loom-web"}
    count = len(api.mutations)
    await provider.stop_workloads(lease)
    assert len(api.mutations) == count


async def test_stop_returns_exact_live_process_and_fence_evidence(retirement):
    context, provider = retirement
    registry, _, _, api, lease, _ = context
    proof = await provider.stop_workloads(lease)
    assert proof.identity.operation_id == lease.operation_id
    assert proof.identity.application_id == lease.application_id
    assert proof.identity.runner_epoch == lease.runner_epoch
    assert proof.namespace.uid == api.objects['/api/v1/namespaces/' + NS]['metadata']['uid']
    assert proof.fence.uid == api.objects[FENCE_PATH]['metadata']['uid']
    assert proof.fence.operation_id == lease.operation_id
    assert proof.pods_resource_version == '1'
    assert {item.name for item in proof.deployments} == {'loom-service', 'loom-web'}
    history = {(item.operation_id, item.key): item for item in await registry.effect_history(lease)}
    for item in (proof.namespace, proof.fence, *proof.deployments):
        assert history[item.operation_id, item.key].observed_uid == item.uid
    for item in proof.deployments:
        actual = api.objects[API if item.name == 'loom-service' else WEB]
        assert item.resource_version == actual['metadata']['resourceVersion']
        assert item.generation == item.observed_generation == 1


@pytest.mark.parametrize("condition", ["running", "terminating", "foreign", "controller"])
async def test_stop_waits_for_every_pod_and_deployment_controller(retirement, condition):
    context, provider = retirement
    _, _, _, api, lease, _ = context
    if condition == "controller":
        api.objects[API]["metadata"]["generation"] = 2
    else:
        pod = {"metadata": {"name": "remaining", "uid": "pod-uid", "resourceVersion": "1"}}
        if condition == "terminating":
            pod["metadata"]["deletionTimestamp"] = "2026-09-28T00:00:00Z"
        if condition != "foreign":
            pod["metadata"]["labels"] = {"loom.nebius/application-id": str(lease.application_id)}
        api.objects[PODS]["items"] = [pod]
    with pytest.raises(ProviderWaitingError, match="application_workloads_retirement_pending"):
        await provider.stop_workloads(lease)
    assert not any(method == "DELETE" and "/pods/" in path for method, path, _ in api.mutations)
    api.objects[PODS]["items"] = []
    api.objects[API]["status"]["observedGeneration"] = api.objects[API]["metadata"]["generation"]
    await provider.stop_workloads(lease)


async def test_replaced_deployment_is_not_scaled_or_adopted(retirement):
    context, provider = retirement
    _, _, _, api, lease, _ = context
    api.objects[API]["metadata"]["uid"] = "foreign-replacement"
    with pytest.raises(ProviderBlockedError, match="application_workload_identity_conflict"):
        await provider.stop_workloads(lease)
    assert api.objects[API]["spec"]["replicas"] == 1


async def test_lost_scale_reply_reconciles_under_destroy_without_repeating_write(retirement):
    context, provider = retirement
    registry, kubernetes, _, api, lease, alice = context
    handle = api.handle

    def lose_scale(request):
        if request.method == "PATCH" and request.url.path == API:
            handle(request)
            raise httpx.ReadTimeout("lost scale reply", request=request) from None
        return handle(request)

    async with httpx.AsyncClient(base_url="https://kubernetes.test",
                                transport=httpx.MockTransport(lose_scale)) as http:
        original_http, kubernetes.http = kubernetes.http, http
        with pytest.raises(ProviderWaitingError):
            await provider.stop_workloads(lease)
        kubernetes.http = original_http
    stopped = await registry.transition(lease.application_id, principal=alice,
        idempotency_key="destroy", action="destroy_retained", expected_generation=2)
    current = await registry.claim(stopped.operation_id)
    await provider.stop_workloads(current)
    assert len([1 for method, path, _ in api.mutations if method == "PATCH" and path == API]) == 1
    assert all(e.phase == "observed" for e in await registry.effect_history(current))


async def test_prepared_scale_resumes_original_version_before_using_new_preconditions(retirement, monkeypatch):
    context, provider = retirement
    registry, _, _, api, lease, _ = context
    dispatch = registry.dispatch_effect

    async def interrupt_scale(current, key):
        if key.startswith("retire:scale:"):
            raise asyncio.CancelledError
        return await dispatch(current, key)

    monkeypatch.setattr(registry, "dispatch_effect", interrupt_scale)
    with pytest.raises(asyncio.CancelledError):
        await provider.stop_workloads(lease)
    prepared = (await registry.effect_history(lease))[-1]
    assert prepared.phase == "prepared" and prepared.intent.resource_version == "1"
    monkeypatch.setattr(registry, "dispatch_effect", dispatch)
    api.objects[API]["metadata"]["resourceVersion"] = "99"
    api.reject_next = 422
    with pytest.raises(ProviderWaitingError):
        await provider.stop_workloads(lease)
    assert api.mutations[-1][2][1]["value"] == "1"
    assert (await registry.effect_history(lease))[-1].phase == "rejected"
    await provider.stop_workloads(lease)
    patches = [body for method, path, body in api.mutations if method == "PATCH" and path == API]
    assert [body[1]["value"] for body in patches] == ["1", "99"]


@pytest.mark.parametrize("body", [{}, {"kind": "PodList", "items": []},
    {"kind": "PodList", "items": [], "metadata": {"resourceVersion": "9", "continue": "next"}}])
async def test_invalid_or_partial_pod_list_is_not_process_retirement(retirement, body):
    context, provider = retirement
    _, _, _, api, lease, _ = context
    api.objects[PODS] = body
    with pytest.raises(ProviderBlockedError, match="application_kubernetes_invalid_response"):
        await provider.stop_workloads(lease)


async def test_uncertain_old_create_is_reconciled_only_then_retired(runtime_context):
    registry, kubernetes, _, api, lease, alice = runtime_context
    plan = await registry.frozen_plan(lease)
    document = next(doc for docs in plan["files"].values() for doc in docs
                    if doc["kind"] == "Deployment" and doc["metadata"]["name"] == "loom-service")
    api.lose_response = True
    with pytest.raises(ProviderWaitingError):
        await kubernetes.create(lease, "Deployment:loom-service", document)
    late = api.objects.pop(API)
    late["metadata"]["generation"] = 1
    late["status"] = {"observedGeneration": 1}
    api.lose_response = False
    stopped = await registry.transition(lease.application_id, principal=alice,
        idempotency_key="stop", action="suspend", expected_generation=1)
    current = await registry.claim(stopped.operation_id)
    context = (*runtime_context[:4], current, alice)
    provider = await close_ready(context)
    api.objects[PODS] = {"kind": "PodList", "metadata": {"resourceVersion": "1"}, "items": []}
    with pytest.raises(ProviderWaitingError):
        await provider.stop_workloads(current)
    before = len(api.mutations)
    api.objects[API] = late
    await provider.stop_workloads(current)
    assert len(api.mutations) == before + 1
    assert api.mutations[-1][0:2] == ("PATCH", API)
    assert api.objects[API]["spec"]["replicas"] == 0


async def test_stop_recovers_lost_namespace_create_before_closing_admission(applications, platform_inputs):
    registry, authority, lease, alice, rendered = await runtime_inputs(applications, platform_inputs)
    api = KubernetesAPI()
    async with httpx.AsyncClient(base_url="https://kubernetes.test", transport=httpx.MockTransport(api.handle)) as http:
        kubernetes = ApplicationKubernetesProvider(registry, http)
        provider = ApplicationRuntimeProvider(registry, kubernetes, authority=authority)
        api.lose_response = True
        with pytest.raises(ProviderWaitingError):
            await kubernetes.create(lease, "namespace", named(rendered, "Namespace", NS))
        api.lose_response = False
        stopped = await registry.transition(lease.application_id, principal=alice,
            idempotency_key="stop", action="suspend", expected_generation=1)
        current = await registry.claim(stopped.operation_id)
        namespace = api.objects.pop("/api/v1/namespaces/" + NS)
        with pytest.raises(ProviderWaitingError, match="application_kubernetes_unconfirmed"):
            await provider.stop_workloads(current)
        assert len(api.mutations) == 1
        api.objects["/api/v1/namespaces/" + NS] = namespace
        with pytest.raises(ProviderWaitingError, match="application_pod_fence_pending"):
            await provider.stop_workloads(current)
        api.objects[FENCE_PATH]["status"] = {"hard": {"pods": "0"}}
        api.objects[PODS] = {"kind": "PodList", "metadata": {"resourceVersion": "1"}, "items": []}
        await provider.stop_workloads(current)
        assert [method for method, _, _ in api.mutations] == ["POST", "POST"]
        assert all(effect.phase == "observed" for effect in await registry.effect_history(current))


@pytest.mark.parametrize("prepared", [False, True])
async def test_stop_before_first_namespace_dispatch_retires_without_sending_old_intent(
    applications, platform_inputs, monkeypatch, prepared,
):
    registry, authority, lease, alice, rendered = await runtime_inputs(applications, platform_inputs)
    api = KubernetesAPI()
    async with httpx.AsyncClient(base_url="https://kubernetes.test", transport=httpx.MockTransport(api.handle)) as http:
        kubernetes = ApplicationKubernetesProvider(registry, http)
        provider = ApplicationRuntimeProvider(registry, kubernetes, authority=authority)
        dispatch = registry.dispatch_effect

        async def interrupt(*args, **kwargs):
            raise asyncio.CancelledError

        if prepared:
            monkeypatch.setattr(registry, "dispatch_effect", interrupt)
            with pytest.raises(asyncio.CancelledError):
                await kubernetes.create(lease, "namespace", named(rendered, "Namespace", NS))
            monkeypatch.setattr(registry, "dispatch_effect", dispatch)
        stopped = await registry.transition(lease.application_id, principal=alice,
            idempotency_key="stop", action="suspend", expected_generation=1)
        current = await registry.claim(stopped.operation_id)
        with pytest.raises(ProviderWaitingError, match="application_pod_fence_pending"):
            await provider.stop_workloads(current)
        api.objects[FENCE_PATH]["status"] = {"hard": {"pods": "0"}}
        api.objects[PODS] = {"kind": "PodList", "metadata": {"resourceVersion": "1"}, "items": []}
        await provider.stop_workloads(current)
        assert len(api.mutations) == 2
        assert all(body["metadata"]["annotations"]["loom.nebius/operation-id"] == str(current.operation_id)
                   for _, _, body in api.mutations)
        if prepared:
            assert (await registry.effect_history(current))[0].phase == "prepared"


async def test_early_stop_does_not_adopt_a_foreign_namespace(applications, platform_inputs):
    registry, authority, lease, alice, _ = await runtime_inputs(applications, platform_inputs)
    api = KubernetesAPI()
    api.objects["/api/v1/namespaces/" + NS] = {"metadata": {"name": NS, "uid": "foreign", "resourceVersion": "1"}}
    stopped = await registry.transition(lease.application_id, principal=alice,
        idempotency_key="stop", action="suspend", expected_generation=1)
    current = await registry.claim(stopped.operation_id)
    async with httpx.AsyncClient(base_url="https://kubernetes.test", transport=httpx.MockTransport(api.handle)) as http:
        provider = ApplicationRuntimeProvider(registry, ApplicationKubernetesProvider(registry, http), authority=authority)
        with pytest.raises(ProviderBlockedError):
            await provider.stop_workloads(current)
    assert api.mutations == []


async def test_runtime_bootstraps_exact_rolebinding_before_resource_access(runtime_context):
    registry, _, authority, api, lease, _ = runtime_context
    from tests.integration.test_nebius_application_runtime import runtime

    provider = runtime(runtime_context)
    api.resource_access_ready = False
    with pytest.raises(ProviderWaitingError, match='application_resource_authority_pending'):
        await provider.ensure_resource_authority(lease)
    role_path = f'/apis/rbac.authorization.k8s.io/v1/namespaces/{NS}/rolebindings/{authority.name}'
    binding = api.objects[role_path]
    assert binding['roleRef']['name'] == authority.name + '-resources'
    assert binding['subjects'] == [{'kind': 'ServiceAccount', 'name': 'loom-application-provisioner',
                                   'namespace': authority.namespace}]
    assert len(api.mutations) == 2  # Namespace and binding, never quota/workloads.
    api.resource_access_ready = True
    await provider.ensure_resource_authority(lease)
    assert len(api.mutations) == 2
    assert (await registry.effect_history(lease))[-1].observed_uid == binding['metadata']['uid']


async def test_binding_lost_reply_reconciles_after_supersession_without_repost(runtime_context):
    registry, _, authority, api, lease, alice = runtime_context
    from tests.integration.test_nebius_application_runtime import runtime

    provider = runtime(runtime_context)
    api.lose_response = True
    with pytest.raises(ProviderWaitingError):
        await provider.ensure_resource_authority(lease)
    path = f'/apis/rbac.authorization.k8s.io/v1/namespaces/{NS}/rolebindings/{authority.name}'
    late = api.objects.pop(path)
    api.lose_response = False
    stopped = await registry.transition(lease.application_id, principal=alice, action='suspend',
        idempotency_key='stop', expected_generation=1)
    current = await registry.claim(stopped.operation_id)
    with pytest.raises(ProviderWaitingError):
        await provider.ensure_resource_authority(current)
    assert len(api.mutations) == 2
    api.objects[path] = late
    await provider.ensure_resource_authority(current)
    assert len(api.mutations) == 2


@pytest.mark.parametrize('damage', ['foreign', 'missing', 'subject', 'role'])
async def test_resource_authority_is_never_adopted_replaced_or_broadened(runtime_context, damage):
    _, _, authority, api, lease, _ = runtime_context
    from tests.integration.test_nebius_application_runtime import runtime

    provider = runtime(runtime_context)
    await provider.ensure_resource_authority(lease)
    path = f'/apis/rbac.authorization.k8s.io/v1/namespaces/{NS}/rolebindings/{authority.name}'
    if damage == 'missing':
        del api.objects[path]
    elif damage == 'foreign':
        api.objects[path]['metadata']['uid'] = 'foreign'
    elif damage == 'subject':
        api.objects[path]['subjects'][0]['name'] = 'other'
    else:
        api.objects[path]['roleRef']['name'] = 'cluster-admin'
    with pytest.raises(ProviderBlockedError, match='application_resource_authority_conflict'):
        await provider.ensure_resource_authority(lease)
    assert len(api.mutations) == 2
