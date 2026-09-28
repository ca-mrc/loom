"""Personal process retirement with a real journal and controlled API races."""
from __future__ import annotations

import asyncio
import copy

import httpx
import pytest

from loom_service.environment_management.provider import ProviderBlockedError, ProviderWaitingError
from tests.integration.test_nebius_application_operations import applications as applications
from tests.integration.test_nebius_application_runtime import (
    FENCE_PATH,
    close_ready,
)
from tests.integration.test_nebius_application_runtime import (
    runtime_context as runtime_context,
)
from tests.integration.test_nebius_environment_management import (
    environment_registry as environment_registry,
)
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
