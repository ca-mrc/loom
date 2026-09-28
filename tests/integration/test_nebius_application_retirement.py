"""Personal process retirement with a real journal and controlled API races."""
from __future__ import annotations

import copy

import httpx
import pytest

from loom_service.environment_management.provider import ProviderBlockedError, ProviderWaitingError
from tests.integration.test_nebius_application_operations import applications as applications
from tests.integration.test_nebius_application_runtime import (
    FENCE_PATH,
    close_ready,
    runtime,
    runtime_context as runtime_context,
)
from tests.integration.test_nebius_environment_management import environment_registry as environment_registry
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
            response = handle(request)
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
