"""Real journal transactions bound to controlled Kubernetes HTTP outcomes."""
from __future__ import annotations

import asyncio
import copy
import json

import httpx
import pytest

from loom_service.environment_management.provider import ProviderBlockedError, ProviderWaitingError
from loom_service.environment_management.registry import ManagementError
from tests.integration.test_nebius_application_effects import expire, started
from tests.integration.test_nebius_application_operations import applications as applications
from tests.integration.test_nebius_environment_management import (
    environment_registry as environment_registry,
)
from tests.unit.test_nebius_application_render import named
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


class KubernetesAPI:
    def __init__(self):
        self.objects = {}
        self.mutations = []
        self.lose_response = False
        self.hide_objects = False
        self.pending_delete = False
        self.reject_next = None
        self.changing_read_versions = False
        self.replace_uid_on_read = False
        self.read_version = 0

    def handle(self, request):
        path = request.url.path
        if request.method == "GET":
            value = None if self.hide_objects else self.objects.get(path)
            if value is not None and self.changing_read_versions:
                self.read_version += 1
                value["metadata"]["resourceVersion"] = str(self.read_version)
                if self.replace_uid_on_read and self.read_version > 1:
                    value["metadata"]["uid"] = "replacement-uid"
            return httpx.Response(200 if value else 404, json=value or {})
        body = json.loads(request.content)
        self.mutations.append((request.method, path, body))
        if self.reject_next is not None:
            status = self.reject_next
            self.reject_next = None
            return httpx.Response(status, json={"kind": "Status", "code": status})
        if request.method == "POST":
            path += "/" + body["metadata"]["name"]
            if path in self.objects:
                return httpx.Response(409, json={})
            value = copy.deepcopy(body)
            value["metadata"].update(uid=f"uid-{len(self.mutations)}", resourceVersion="1")
            self.objects[path] = value
            if self.lose_response:
                raise httpx.ReadTimeout("must-not-escape-protected-detail")
            return httpx.Response(201, json=value)
        value = self.objects.get(path)
        if value is None:
            return httpx.Response(404, json={})
        if request.method == "PATCH":
            assert request.headers["content-type"] == "application/json-patch+json"
            assert body[:2] == [
                {"op": "test", "path": "/metadata/uid", "value": value["metadata"]["uid"]},
                {"op": "test", "path": "/metadata/resourceVersion", "value": value["metadata"]["resourceVersion"]},
            ]
            for patch in body[2:]:
                if patch["path"] == "/spec":
                    value["spec"] = patch["value"]
                elif patch["path"] == "/metadata/annotations":
                    value["metadata"]["annotations"] = patch["value"]
                elif patch["path"] == "/metadata/labels":
                    value["metadata"]["labels"] = patch["value"]
                else:
                    raise AssertionError("unexpected patch field")
            value["metadata"]["resourceVersion"] = "2"
            return httpx.Response(200, json=value)
        assert request.method == "DELETE"
        assert body == {"apiVersion": "v1", "kind": "DeleteOptions", "propagationPolicy": "Background",
                        "preconditions": {"uid": value["metadata"]["uid"],
                                          "resourceVersion": value["metadata"]["resourceVersion"]}}
        if not self.pending_delete:
            del self.objects[path]
        return httpx.Response(202, json={"kind": "Status", "status": "Success"})


@pytest.fixture
async def provider(applications):
    from loom_service.application_management.kubernetes import ApplicationKubernetesProvider

    registry, _, _, plan, _, lease = await started(applications)
    api = KubernetesAPI()
    async with httpx.AsyncClient(base_url="https://kubernetes.test", transport=httpx.MockTransport(api.handle)) as http:
        client = ApplicationKubernetesProvider(registry, http)
        yield client, api, registry, plan, lease


async def namespace_ready(provider):
    client, api, registry, plan, lease = provider
    document = named(plan["prepared"], "Namespace", "loom-dev-alice")
    effect = await client.create(lease, "namespace", document)
    assert effect.phase == "observed" and effect.observed_uid == "uid-1"
    return client, api, registry, plan, lease


async def test_namespace_create_is_single_winner_and_observed_replay_never_resends(provider):
    client, api, registry, plan, lease = provider
    document = named(plan["prepared"], "Namespace", "loom-dev-alice")
    original = copy.deepcopy(document)
    results = await asyncio.gather(*[client.create(lease, "namespace", document) for _ in range(3)],
                                   return_exceptions=True)
    assert all(not isinstance(value, Exception) or isinstance(value, ProviderWaitingError) for value in results)
    observed = await client.create(lease, "namespace", document)
    assert observed.phase == "observed" and observed.observed_uid == "uid-1"
    assert document == original
    assert len(api.mutations) == 1
    history = await registry.effect_history(lease)
    assert history == [observed]


async def test_stop_reconciles_late_predecessor_create_without_resending(provider, applications):
    client, api, registry, plan, lease = await namespace_ready(provider)
    document = named(plan["prepared"], "Deployment", "loom-service")
    api.lose_response = True
    with pytest.raises(ProviderWaitingError):
        await client.create(lease, "api", document)
    alice = applications[2][0]
    stopped = await registry.transition(lease.application_id, principal=alice, idempotency_key="stop",
        action="suspend", expected_generation=1)
    current = await registry.claim(stopped.operation_id)
    path = "/apis/apps/v1/namespaces/loom-dev-alice/deployments/loom-service"
    late = api.objects.pop(path)  # Keep the already-recorded namespace visible.
    with pytest.raises(ProviderWaitingError):
        await client.reconcile(current, lease.operation_id, "api", document=document)
    assert len(api.mutations) == 2
    api.objects[path] = late
    damaged = copy.deepcopy(document)
    damaged["spec"]["replicas"] = 9
    with pytest.raises(ProviderBlockedError, match="application_kubernetes_request_conflict"):
        await client.reconcile(current, lease.operation_id, "api", document=damaged)
    result = await client.reconcile(current, lease.operation_id, "api", document=document)
    assert result.operation_id == lease.operation_id and result.phase == "observed"
    assert result.observed_uid == "uid-2"
    assert len(api.mutations) == 2
    assert (await registry.frozen_plan(current, operation_id=lease.operation_id))["registration"]["desired_state"] == "active"
    with pytest.raises(ManagementError, match="stale_operation_lease"):
        await client.reconcile(lease, lease.operation_id, "api", document=document)


async def test_same_owner_sibling_cannot_load_or_observe_predecessor_effects(provider, applications):
    client, _, registry, _, lease = await namespace_ready(provider)
    _, _, (alice, _), prepare, _, _ = applications
    sibling = await registry.create(principal=alice, idempotency_key="sibling", **prepare("alice-second", alice))
    other = await registry.claim(sibling.operation_id)
    with pytest.raises(ManagementError, match="application_history_forbidden"):
        await registry.frozen_plan(other, operation_id=lease.operation_id)
    with pytest.raises(ManagementError, match="application_history_forbidden"):
        await registry.observe_effect(other, "namespace", uid="uid-1", resource_version="1",
                                      operation_id=lease.operation_id)
    with pytest.raises(ProviderBlockedError, match="application_kubernetes_effect_missing"):
        await client.reconcile(other, lease.operation_id, "namespace")


@pytest.mark.parametrize("replacement", [False, True])
async def test_concurrent_readbacks_preserve_first_observation_and_reject_different_uid(provider, monkeypatch, replacement):
    client, api, registry, plan, lease = provider
    api.changing_read_versions = True
    api.replace_uid_on_read = replacement
    barrier = asyncio.Event()
    arrivals, committed_versions = [], []
    observe = registry.observe_effect

    async def synchronized_observe(*args, **kwargs):
        arrivals.append(kwargs["resource_version"])
        if len(arrivals) == 2:
            barrier.set()
        await asyncio.wait_for(barrier.wait(), timeout=5)
        await observe(*args, **kwargs)  # Real transaction, no fake observation.
        committed_versions.append(kwargs["resource_version"])

    monkeypatch.setattr(registry, "observe_effect", synchronized_observe)
    document = named(plan["prepared"], "Namespace", "loom-dev-alice")
    results = await asyncio.gather(*[client.create(lease, "namespace", document) for _ in range(2)],
                                   return_exceptions=True)
    assert len(set(arrivals)) == 2
    assert len(committed_versions) == 1
    history = await registry.effect_history(lease)
    if replacement:
        conflicts = [value for value in results if isinstance(value, ManagementError)]
        assert len(conflicts) == 1 and conflicts[0].code == "application_effect_observation_conflict"
        assert sum(value == history[0] for value in results) == 1
    else:
        assert all(result == history[0] for result in results), results
    assert history[0].observed_resource_version == committed_versions[0]
    assert len(api.mutations) == 1


async def test_uncertain_create_and_absence_never_authorize_resend(provider, applications):
    client, api, registry, plan, lease = provider
    document = named(plan["prepared"], "Namespace", "loom-dev-alice")
    api.lose_response = True
    with pytest.raises(ProviderWaitingError) as error:
        await client.create(lease, "namespace", document)
    assert "protected-detail" not in str(error.value)
    await expire(applications[1], lease)
    stale = lease
    lease = await registry.claim(lease.operation_id)
    with pytest.raises(ManagementError, match="stale_operation_lease"):
        await client.create(stale, "namespace", document)
    api.hide_objects = True
    with pytest.raises(ProviderWaitingError):
        await client.create(lease, "namespace", document)
    assert (await registry.effect_history(lease))[0].phase == "dispatched"
    assert len(api.mutations) == 1
    api.hide_objects = False
    result = await client.create(lease, "namespace", document)
    assert result.phase == "observed" and len(api.mutations) == 1


@pytest.mark.parametrize("change", ["uid", "data", "pod_security"])
async def test_namespaced_create_requires_recorded_unchanged_namespace(provider, change):
    client, api, _, plan, lease = provider
    document = named(plan["prepared"], "Deployment", "loom-service")
    with pytest.raises(ProviderBlockedError, match="namespace_identity"):
        await client.create(lease, "api", document)
    assert api.mutations == []
    await namespace_ready(provider)
    metadata = api.objects["/api/v1/namespaces/loom-dev-alice"]["metadata"]
    if change == "uid":
        metadata["uid"] = "replacement"
    elif change == "data":
        metadata["labels"]["loom.nebius/data-environment-id"] = "foreign"
    else:
        metadata["labels"]["pod-security.kubernetes.io/enforce"] = "privileged"
    with pytest.raises(ProviderBlockedError, match="namespace_identity"):
        await client.create(lease, "api", document)
    assert len(api.mutations) == 1


async def test_credential_contents_never_enter_effect_journal(provider):
    client, api, _, plan, lease = await namespace_ready(provider)
    namespace = plan["prepared"].registration.application_namespace
    name = f"loom-application-auth-{lease.incarnation.hex}-g1"
    document = {"apiVersion": "v1", "kind": "Secret", "immutable": True, "type": "Opaque",
                "metadata": {"name": name, "namespace": namespace}, "data": {"key": "dGVzdC1vbmx5LW1hdGVyaWFs"}}
    created = await client.create(lease, "auth", document)
    assert created.phase == "observed"
    serialized = json.dumps(created.intent.model_dump())
    assert "dGVzdC1vbmx5LW1hdGVyaWFs" not in serialized and "data" not in created.intent.model_dump()
    assert api.objects[f"/api/v1/namespaces/{namespace}/secrets/{name}"]["data"] == document["data"]


async def test_patch_and_delete_send_exact_preconditions_and_wait_for_retirement(provider):
    client, api, registry, plan, lease = await namespace_ready(provider)
    document = named(plan["prepared"], "Deployment", "loom-service")
    created = await client.create(lease, "api", document)
    stopped = copy.deepcopy(document)
    stopped["spec"]["replicas"] = 0
    patched = await client.patch_spec(lease, "stop-api", stopped,
        uid=created.observed_uid, resource_version=created.observed_resource_version)
    assert patched.observed_uid == created.observed_uid and patched.observed_resource_version == "2"
    target = dict(api_version="apps/v1", kind="Deployment", namespace="loom-dev-alice", name="loom-service",
                  uid=patched.observed_uid, resource_version=patched.observed_resource_version)
    api.pending_delete = True
    with pytest.raises(ProviderWaitingError):
        await client.delete(lease, "delete-api", **target)
    assert (await registry.effect_history(lease))[-1].phase == "dispatched"
    path = "/apis/apps/v1/namespaces/loom-dev-alice/deployments/loom-service"
    # A replacement proves the old UID is gone, never authorizes deleting it.
    api.objects[path]["metadata"]["uid"] = "replacement-api"
    retired = await client.delete(lease, "delete-api", **target)
    assert retired.phase == "observed" and retired.observed_resource_version is None
    assert api.objects[path]["metadata"]["uid"] == "replacement-api"
    assert [method for method, _, _ in api.mutations] == ["POST", "POST", "PATCH", "DELETE"]


async def test_wrong_reconciliation_identity_is_not_adopted(provider):
    client, api, registry, plan, lease = provider
    document = named(plan["prepared"], "Namespace", "loom-dev-alice")
    api.lose_response = True
    with pytest.raises(ProviderWaitingError):
        await client.create(lease, "namespace", document)
    api.objects["/api/v1/namespaces/loom-dev-alice"]["metadata"]["labels"]["loom.nebius/application-id"] = "foreign"
    with pytest.raises(ProviderBlockedError, match="resource_identity"):
        await client.create(lease, "namespace", document)
    assert (await registry.effect_history(lease))[0].phase == "dispatched"
    assert len(api.mutations) == 1


async def test_malformed_delete_readback_cannot_prove_old_resource_absence(provider):
    client, api, registry, plan, lease = await namespace_ready(provider)
    created = await client.create(lease, "api", named(plan["prepared"], "Deployment", "loom-service"))
    target = dict(api_version="apps/v1", kind="Deployment", namespace="loom-dev-alice", name="loom-service",
                  uid=created.observed_uid, resource_version=created.observed_resource_version)
    api.pending_delete = True
    with pytest.raises(ProviderWaitingError):
        await client.delete(lease, "delete-api", **target)
    del api.objects["/apis/apps/v1/namespaces/loom-dev-alice/deployments/loom-service"]["metadata"]["uid"]
    with pytest.raises(ProviderBlockedError):
        await client.delete(lease, "delete-api", **target)
    assert (await registry.effect_history(lease))[-1].phase == "dispatched"
    assert len(api.mutations) == 3


@pytest.mark.parametrize("status", [409, 422])
async def test_definite_rejection_never_resends_old_key_but_allows_fresh_effect(provider, status):
    from loom_service.application_management.kubernetes import KubernetesEffectRejectedError

    client, api, registry, plan, lease = await namespace_ready(provider)
    document = named(plan["prepared"], "Deployment", "loom-service")
    api.reject_next = status
    for _ in range(2):
        with pytest.raises(KubernetesEffectRejectedError) as error:
            await client.create(lease, "rejected-api", document)
        assert error.value.status_code == status
    assert len(api.mutations) == 2  # One namespace and ONE rejected attempt.
    assert (await registry.effect_history(lease))[-1].phase == "rejected"
    created = await client.create(lease, "corrected-api", document)
    assert created.phase == "observed" and len(api.mutations) == 3


async def test_stale_lease_cannot_issue_a_kubernetes_write(provider, applications):
    client, api, registry, plan, lease = provider
    alice = applications[2][0]
    await registry.transition(lease.application_id, principal=alice, idempotency_key="stop",
                             action="suspend", expected_generation=1)
    with pytest.raises(ManagementError, match="stale_operation_lease"):
        await client.create(lease, "namespace", named(plan["prepared"], "Namespace", "loom-dev-alice"))
    assert api.mutations == []
