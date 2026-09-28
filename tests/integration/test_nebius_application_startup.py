"""Durable startup phase exclusion using real application/effect journals."""
from __future__ import annotations

import asyncio
import base64
import copy
import hashlib
from uuid import uuid4

import pytest

from loom_service.environment_management.provider import ProviderBlockedError, ProviderWaitingError
from loom_service.environment_management.registry import ManagementError
from tests.integration.test_nebius_application_effects import expire, intent, started
from tests.integration.test_nebius_application_material import management_key as management_key
from tests.integration.test_nebius_application_material import material
from tests.integration.test_nebius_application_operations import _observed_complete
from tests.integration.test_nebius_application_operations import applications as applications
from tests.integration.test_nebius_application_runtime import FENCE_PATH, close_ready, runtime
from tests.integration.test_nebius_application_runtime import runtime_context as runtime_context
from tests.integration.test_nebius_environment_management import (
    environment_registry as environment_registry,
)
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


def unfence(plan, *, uid="fence-uid", rv="9"):
    key = "activate:unfence:" + hashlib.sha256((uid + ":" + rv).encode()).hexdigest()
    return key, intent(plan, api_version="v1", kind="ResourceQuota", name="loom-application-retired",
                       action="delete", uid=uid, resource_version=rv)


@pytest.mark.parametrize("phase", ["prepared", "dispatched", "rejected", "observed"])
async def test_activation_boundary_survives_every_request_phase_and_lease_takeover(applications, phase):
    registry, factory, _, plan, operation, lease = await started(applications)
    assert not await registry.activation_started(lease)
    key, value = unfence(plan)
    await registry.prepare_effect(lease, key, value)
    if phase != "prepared":
        assert await registry.dispatch_effect(lease, key)
    if phase == "rejected":
        await registry.reject_effect(lease, key, status_code=409)
    elif phase == "observed":
        await registry.observe_effect(lease, key, uid="fence-uid", resource_version=None)
    assert await registry.activation_started(lease)
    for retirement_key, retirement in (
        ("retire:scale:old", intent(plan, action="patch", uid="old-api", resource_version="7")),
        ("pod-fence:create", intent(plan, kind="ResourceQuota", api_version="v1", name="loom-application-retired")),
        ("pod-fence:advance:old", intent(plan, kind="ResourceQuota", api_version="v1", name="loom-application-retired", action="patch", uid="fence-uid", resource_version="8")),
        ("prepare:network", intent(plan, kind="NetworkPolicy", api_version="networking.k8s.io/v1", name="default-deny")),
    ):
        with pytest.raises(ManagementError, match="application_activation_started"):
            await registry.prepare_effect(lease, retirement_key, retirement)
    assert len(await registry.effect_history(lease)) == 1
    await expire(factory, lease)
    replacement = await registry.claim(operation.operation_id)
    with pytest.raises(ManagementError, match="stale_operation_lease"):
        await registry.activation_started(lease)
    assert await registry.activation_started(replacement)


@pytest.mark.parametrize("change", ["target", "action", "kind", "key", "unqualified_delete"])
async def test_activation_marker_cannot_name_another_mutation(applications, change):
    registry, _, _, plan, _, lease = await started(applications)
    key, value = unfence(plan)
    if change == "target":
        value["name"] = "other-quota"
    elif change == "action":
        value["action"] = "patch"
    elif change == "kind":
        value.update(kind="Service", name="loom-service")
    elif change == "key":
        key = "activate:unfence:" + "0" * 64
    else:
        key = "arbitrary-delete"
    with pytest.raises(ManagementError, match="invalid_application_effect"):
        await registry.prepare_effect(lease, key, value)
    assert await registry.effect_history(lease) == []


async def test_definitive_rejection_allows_fresh_unfence_but_not_return_to_retirement(applications):
    registry, _, _, plan, _, lease = await started(applications)
    first, first_value = unfence(plan)
    await registry.prepare_effect(lease, first, first_value)
    await registry.dispatch_effect(lease, first)
    second, second_value = unfence(plan, rv="10")
    with pytest.raises(ManagementError):
        await registry.prepare_effect(lease, second, second_value)
    await registry.reject_effect(lease, first, status_code=409)
    assert (await registry.prepare_effect(lease, second, second_value)).phase == "prepared"
    await registry.dispatch_effect(lease, second)
    await registry.observe_effect(lease, second, uid="fence-uid", resource_version=None)
    third, third_value = unfence(plan, rv="11")
    with pytest.raises(ManagementError, match="application_activation_started"):
        await registry.prepare_effect(lease, third, third_value)
    assert (await registry.prepare_effect(lease, second, second_value)).phase == "observed"
    assert not await registry.dispatch_effect(lease, second)
    assert (await registry.prepare_effect(lease, "start:api", intent(plan))).phase == "prepared"


async def test_stop_successor_can_close_admission_without_inheriting_active_phase(applications):
    registry, _, alice, plan, operation, lease = await started(applications)
    key, value = unfence(plan)
    await registry.prepare_effect(lease, key, value)
    await registry.dispatch_effect(lease, key)
    stopped = await registry.transition(operation.application_id, principal=alice, idempotency_key="stop",
        action="suspend", expected_generation=1)
    successor = await registry.claim(stopped.operation_id)
    assert not await registry.activation_started(successor)
    with pytest.raises(ManagementError, match="invalid_application_effect"):
        await registry.prepare_effect(successor, key, value)
    closed = intent(plan, kind="ResourceQuota", api_version="v1", name="loom-application-retired")
    assert (await registry.prepare_effect(successor, "pod-fence:create", closed)).phase == "prepared"
    with pytest.raises(ManagementError, match="stale_operation_lease"):
        await registry.dispatch_effect(lease, key)


async def test_same_lease_preparation_race_has_one_durable_phase_winner(applications):
    registry, _, _, plan, _, lease = await started(applications)
    key, value = unfence(plan)
    retirement = intent(plan, action="patch", uid="old-api", resource_version="8")
    results = await asyncio.gather(registry.prepare_effect(lease, key, value),
        registry.prepare_effect(lease, "retire:scale:old", retirement), return_exceptions=True)
    assert sum(isinstance(result, ManagementError) for result in results) == 1
    effects = await registry.effect_history(lease)
    assert len(effects) == 1
    winner = effects[0]
    assert await registry.dispatch_effect(lease, winner.key)
    if winner.key == key:
        await registry.observe_effect(lease, key, uid="fence-uid", resource_version=None)
        with pytest.raises(ManagementError, match="application_activation_started"):
            await registry.prepare_effect(lease, "retire:scale:old", retirement)
    else:
        with pytest.raises(ManagementError, match="application_effect_unresolved"):
            await registry.prepare_effect(lease, key, value)
        await registry.observe_effect(lease, winner.key, uid="old-api", resource_version="9")
        assert (await registry.prepare_effect(lease, key, value)).phase == "prepared"


async def test_one_application_activation_never_blocks_another_owner(applications):
    registry, _, _, plan, _, lease = await started(applications)
    await registry.prepare_effect(lease, *unfence(plan))
    _, _, (_, bob), prepare, _, _ = applications
    other = prepare("bob", principal=bob)
    operation = await registry.create(principal=bob, idempotency_key="bob", **other)
    sibling = await registry.claim(operation.operation_id)
    assert not await registry.activation_started(sibling)
    assert (await registry.prepare_effect(sibling, "retire:scale:old",
        intent(other, action="patch", uid="bobs-api", resource_version="8"))).phase == "prepared"


async def document(registry, lease, kind):
    if kind == "Secret":
        bundles = await registry.ensure_material(lease, material)
        name = next(iter(bundles))
        return {"apiVersion": "v1", "kind": "Secret", "immutable": True, "type": "Opaque",
                "metadata": {"name": name, "namespace": "loom-dev-alice"},
                "data": {key: base64.b64encode(value.encode()).decode() for key, value in bundles[name].items()}}
    plan = await registry.frozen_plan(lease)
    return next(doc for docs in plan["files"].values() for doc in docs if doc["kind"] == kind)


async def interrupted_create(registry, client, lease, key, doc, monkeypatch):
    async def crash(*args):
        raise InterruptedError("crash after prepare")

    with monkeypatch.context() as patch:
        patch.setattr(registry, "dispatch_effect", crash)
        with pytest.raises(InterruptedError):
            await client.create(lease, key, doc)


@pytest.mark.parametrize("kind", ["ServiceAccount", "NetworkPolicy", "Secret"])
@pytest.mark.parametrize("lost_reply", [False, True])
async def test_preparation_recovers_current_static_and_secret_writes_once(runtime_context, monkeypatch, kind, lost_reply):
    registry, client, _, api, lease, _ = runtime_context
    doc = await document(registry, lease, kind)
    key = "prepare:" + kind.lower()
    if lost_reply:
        api.lose_response = True
        with pytest.raises(ProviderWaitingError):
            await client.create(lease, key, doc)
    else:
        await interrupted_create(registry, client, lease, key, doc, monkeypatch)
    await expire(registry.session_factory, lease)
    replacement = await registry.claim(lease.operation_id)
    with pytest.raises(ManagementError, match="stale_operation_lease"):
        await runtime(runtime_context).resume_preparation(lease)
    await runtime(runtime_context).resume_preparation(replacement)
    await runtime(runtime_context).resume_preparation(replacement)
    assert [method for method, _, _ in api.mutations] == ["POST", "POST"]
    effect = (await registry.effect_history(replacement))[-1]
    assert effect.key == key and effect.phase == "observed"
    actual = next(obj for obj in api.objects.values() if obj["kind"] == kind)
    if kind == "Secret":
        assert actual["immutable"] is True and actual["data"] == doc["data"]


async def test_preparation_resumes_original_network_patch_preconditions(runtime_context, monkeypatch):
    registry, client, _, api, lease, _ = runtime_context
    doc = await document(registry, lease, "NetworkPolicy")
    original = await client.create(lease, "network:create", doc)

    async def crash(*args):
        raise InterruptedError("crash after prepare")

    with monkeypatch.context() as patch:
        patch.setattr(registry, "dispatch_effect", crash)
        with pytest.raises(InterruptedError):
            await client.patch_spec(lease, "network:patch", doc, uid=original.observed_uid,
                                    resource_version=original.observed_resource_version)
    api.reject_next = 409
    with pytest.raises(ProviderWaitingError, match="application_preparation_pending"):
        await runtime(runtime_context).resume_preparation(lease)
    assert api.mutations[-1][2][:2] == [
        {"op": "test", "path": "/metadata/uid", "value": original.observed_uid},
        {"op": "test", "path": "/metadata/resourceVersion", "value": original.observed_resource_version},
    ]
    await runtime(runtime_context).resume_preparation(lease)
    assert len(api.mutations) == 3
    assert (await registry.effect_history(lease))[-1].phase == "rejected"


@pytest.mark.parametrize("kind", ["Deployment", "Service", "Ingress"])
async def test_preparation_cannot_start_workloads_or_publish_routes(runtime_context, monkeypatch, kind):
    registry, client, _, api, lease, _ = runtime_context
    await interrupted_create(registry, client, lease, "forbidden", await document(registry, lease, kind), monkeypatch)
    with pytest.raises(ProviderBlockedError, match="application_preparation_effect_conflict"):
        await runtime(runtime_context).resume_preparation(lease)
    assert len(api.mutations) == 1
    assert (await registry.effect_history(lease))[-1].phase == "prepared"


async def test_preparation_never_dispatches_a_stopped_predecessors_unsent_secret(runtime_context, monkeypatch):
    registry, client, _, api, lease, alice = runtime_context
    await interrupted_create(registry, client, lease, "credential:db", await document(registry, lease, "Secret"), monkeypatch)
    stopped = await registry.transition(lease.application_id, principal=alice, idempotency_key="stop",
        action="suspend", expected_generation=1)
    current = await registry.claim(stopped.operation_id)
    with pytest.raises(ProviderBlockedError, match="application_preparation_not_requested"):
        await runtime(runtime_context).resume_preparation(current)
    assert len(api.mutations) == 1


async def test_preparation_cannot_run_after_the_activation_boundary(runtime_context):
    registry, _, _, api, lease, _ = runtime_context
    key = "activate:unfence:" + hashlib.sha256(b"fence-uid:9").hexdigest()
    await registry.prepare_effect(lease, key, dict(api_version="v1", kind="ResourceQuota",
        namespace="loom-dev-alice", name="loom-application-retired", action="delete",
        uid="fence-uid", resource_version="9", request_sha256="a" * 64))
    with pytest.raises(ProviderBlockedError, match="application_activation_started"):
        await runtime(runtime_context).resume_preparation(lease)
    assert len(api.mutations) == 1


SA_PATH = "/api/v1/namespaces/loom-dev-alice/serviceaccounts/loom-platform"
NETWORK_PATH = "/apis/networking.k8s.io/v1/namespaces/loom-dev-alice/networkpolicies/"


async def test_static_preparation_waits_for_observed_quota_before_installing(runtime_context):
    _, _, _, api, lease, _ = runtime_context
    with pytest.raises(ProviderWaitingError, match="application_pod_fence_pending"):
        await runtime(runtime_context).prepare_static(lease)
    assert {body["kind"] for method, _, body in api.mutations if method == "POST"} == {
        "Namespace", "RoleBinding", "ResourceQuota"}
    assert api.objects[FENCE_PATH]["spec"]["hard"] == {"pods": "0"}


async def test_static_preparation_installs_only_frozen_resources_and_replays_without_writes(runtime_context):
    _, _, _, api, lease, _ = runtime_context
    provider = await close_ready(runtime_context)
    await provider.prepare_static(lease)
    assert api.objects[SA_PATH]["automountServiceAccountToken"] is False
    policies = {obj["metadata"]["name"] for obj in api.objects.values() if obj["kind"] == "NetworkPolicy"}
    assert policies == {"default-deny", "public-api", "public-web", "application-egress"}
    assert {obj["kind"] for obj in api.objects.values()} == {
        "Namespace", "RoleBinding", "ResourceQuota", "ServiceAccount", "NetworkPolicy"}
    writes = copy.deepcopy(api.mutations)
    await provider.prepare_static(lease)
    assert api.mutations == writes


@pytest.mark.parametrize("damage", ["deleted", "replaced", "broad-policy", "token-mount", "terminating"])
async def test_static_preparation_blocks_live_drift_instead_of_repairing_it(runtime_context, damage):
    _, _, _, api, lease, _ = runtime_context
    provider = await close_ready(runtime_context)
    await provider.prepare_static(lease)
    actual = api.objects[SA_PATH if damage == "token-mount" else NETWORK_PATH + "default-deny"]
    if damage == "deleted":
        del api.objects[NETWORK_PATH + "default-deny"]
    elif damage == "replaced":
        actual["metadata"]["uid"] = "foreign"
    elif damage == "broad-policy":
        actual["spec"]["ingress"] = [{}]
    elif damage == "token-mount":
        actual["automountServiceAccountToken"] = True
    else:
        actual["metadata"]["deletionTimestamp"] = "2026-09-28T00:00:00Z"
    writes = copy.deepcopy(api.mutations)
    with pytest.raises(ProviderBlockedError, match="application_static_resource_conflict"):
        await provider.prepare_static(lease)
    assert api.mutations == writes


async def test_static_preparation_lost_network_reply_recovers_without_another_post(runtime_context, monkeypatch):
    registry, client, _, api, lease, _ = runtime_context
    provider = await close_ready(runtime_context)
    request = client._request

    async def lose_network_reply(method, path, body=None):
        if method == "POST" and path.endswith("/networkpolicies"):
            api.lose_response = True
        try:
            return await request(method, path, body)
        finally:
            api.lose_response = False

    with monkeypatch.context() as patch:
        patch.setattr(client, "_request", lose_network_reply)
        with pytest.raises(ProviderWaitingError):
            await provider.prepare_static(lease)
    await expire(registry.session_factory, lease)
    replacement = await registry.claim(lease.operation_id)
    await provider.prepare_static(replacement)
    assert sum(method == "POST" and body.get("kind") == "NetworkPolicy"
               and body["metadata"]["name"] == "default-deny" for method, _, body in api.mutations) == 1


async def next_static_generation(context, platform_inputs):
    from loom.nebius_application_contract import (
        ApplicationRegistrationV1,
        ApplicationReleaseV1,
        SharedDevelopmentBindingV1,
    )
    from loom.nebius_application_render import render_application
    from tests.unit.test_nebius_application_render import inputs

    registry, _, authority, _, lease, alice = context
    plan = await registry.frozen_plan(lease)
    release = ApplicationReleaseV1.model_validate(plan["release"]).model_copy(update={"release_id": uuid4()})
    row = ApplicationRegistrationV1.model_validate(plan["registration"]).model_copy(update={
        "deployment_generation": 2, "access_generation": 2, "release_id": release.release_id})
    shared = SharedDevelopmentBindingV1.model_validate(plan["shared"])
    rendered = render_application(row, release, shared, inputs(platform_inputs)[3], authority=authority)
    # A different frozen policy is supplied through the trusted planner, not an
    # owner raw-manifest endpoint. This checks an actual spec replacement.
    for docs in rendered.files.values():
        for doc in docs:
            if doc["kind"] == "NetworkPolicy" and doc["metadata"]["name"] == "public-api":
                doc["spec"]["ingress"][0]["ports"].append({"protocol": "TCP", "port": 8091})
    await _observed_complete(registry.session_factory, lease.operation_id)
    operation = await registry.transition(lease.application_id, principal=alice, idempotency_key="update",
        action="update", expected_generation=1, release_id=release.release_id,
        prepared=rendered, release=release, shared=shared)
    return await registry.claim(operation.operation_id)


async def test_static_update_retains_account_and_patches_owned_network_with_exact_preconditions(runtime_context, platform_inputs):
    _, _, _, api, lease, _ = runtime_context
    provider = await close_ready(runtime_context)
    await provider.prepare_static(lease)
    before = copy.deepcopy(api.objects)
    current = await next_static_generation(runtime_context, platform_inputs)
    await provider.prepare_static(current)
    assert api.objects[SA_PATH] == before[SA_PATH]
    for method, path, body in api.mutations:
        if method == "PATCH" and path.startswith(NETWORK_PATH):
            assert body[:2] == [
                {"op": "test", "path": "/metadata/uid", "value": before[path]["metadata"]["uid"]},
                {"op": "test", "path": "/metadata/resourceVersion", "value": before[path]["metadata"]["resourceVersion"]},
            ]
    assert sum(method == "PATCH" and path.startswith(NETWORK_PATH) for method, path, _ in api.mutations) == 4
    updated = api.objects[NETWORK_PATH + "public-api"]
    assert updated["spec"]["ingress"][0]["ports"] == [{"protocol": "TCP", "port": 8090}, {"protocol": "TCP", "port": 8091}]
    assert updated["metadata"]["annotations"]["loom.nebius/operation-id"] == str(current.operation_id)
    writes = copy.deepcopy(api.mutations)
    await provider.prepare_static(current)
    assert api.mutations == writes


async def test_static_update_never_sends_a_historical_prepared_request(runtime_context, platform_inputs, monkeypatch):
    registry, client, _, api, lease, _ = runtime_context
    provider = await close_ready(runtime_context)
    await interrupted_create(registry, client, lease, "old-account", await document(registry, lease, "ServiceAccount"), monkeypatch)
    current = await next_static_generation(runtime_context, platform_inputs)
    await provider.prepare_static(current)
    previous = next(item for item in await registry.effect_history(current) if item.key == "old-account")
    assert previous.phase == "prepared"
    assert api.objects[SA_PATH]["metadata"]["annotations"]["loom.nebius/operation-id"] == str(current.operation_id)
    assert sum(method == "POST" and body.get("kind") == "ServiceAccount" for method, _, body in api.mutations) == 1


@pytest.mark.parametrize("phase", ["stopped", "activated"])
async def test_static_preparation_cannot_cross_stopped_or_activation_boundary(runtime_context, phase):
    registry, _, _, api, lease, alice = runtime_context
    if phase == "stopped":
        operation = await registry.transition(lease.application_id, principal=alice, idempotency_key="stop",
            action="suspend", expected_generation=1)
        lease = await registry.claim(operation.operation_id)
    else:
        await registry.prepare_effect(lease, "activate:unfence:" + hashlib.sha256(b"fence-uid:9").hexdigest(),
            dict(api_version="v1", kind="ResourceQuota", namespace="loom-dev-alice", name="loom-application-retired",
                 action="delete", uid="fence-uid", resource_version="9", request_sha256="a" * 64))
    with pytest.raises(ProviderBlockedError, match=r"application_preparation_not_requested|application_activation_started"):
        await runtime(runtime_context).prepare_static(lease)
    assert len(api.mutations) == 1


def shared_network_objects(context, platform_inputs):
    from loom.nebius_application_network import render_application_shared_access
    from tests.unit.test_nebius_application_render import inputs

    authority, api = context[2:4]
    _, _, shared, foundation = inputs(platform_inputs)
    policies = render_application_shared_access(authority, shared, foundation)
    paths = []
    for index, policy in enumerate(policies):
        policy["metadata"].update(uid=f"shared-policy-{index}", resourceVersion=str(index + 10))
        path = f"/apis/networking.k8s.io/v1/namespaces/{shared.platform_namespace}/networkpolicies/{policy['metadata']['name']}"
        api.objects[path] = policy
        paths.append(path)
    return paths


async def test_shared_network_preflight_reads_only_exact_protected_policies(runtime_context, platform_inputs, monkeypatch):
    _, client, authority, api, lease, _ = runtime_context
    paths = shared_network_objects(runtime_context, platform_inputs)
    request, reads = client._request, []

    async def track(method, path, body=None):
        if f"/namespaces/{authority.shared_namespace}/" in path:
            assert method == "GET"
            reads.append(path)
        return await request(method, path, body)

    monkeypatch.setattr(client, "_request", track)
    proof = await runtime(runtime_context).read_shared_network(lease)
    assert reads == paths
    assert [(item.name, item.uid, item.resource_version) for item in proof] == [
        (authority.name + "-postgres", "shared-policy-0", "10"),
        (authority.name + "-control-plane", "shared-policy-1", "11"),
        (authority.name + "-gateway", "shared-policy-2", "12")]
    assert len(api.mutations) == 1


@pytest.mark.parametrize("damage", ["missing", "broadened", "wrong-owner", "wrong-target", "terminating", "wrong-kind", "bad-version"])
async def test_shared_network_preflight_rejects_missing_or_changed_live_policy(runtime_context, platform_inputs, damage):
    _, _, _, api, lease, _ = runtime_context
    paths = shared_network_objects(runtime_context, platform_inputs)
    policy = api.objects[paths[0]]
    if damage == "missing":
        del api.objects[paths[0]]
    elif damage == "broadened":
        policy["spec"]["ingress"].append({})
    elif damage == "wrong-owner":
        policy["spec"]["ingress"][0]["from"][0]["namespaceSelector"]["matchLabels"]["loom.nebius/application-installation"] = str(uuid4())
    elif damage == "wrong-target":
        policy["spec"]["podSelector"] = {}
    elif damage == "terminating":
        policy["metadata"]["deletionTimestamp"] = "2026-09-28T00:00:00Z"
    elif damage == "wrong-kind":
        policy["kind"] = "Service"
    else:
        policy["metadata"]["resourceVersion"] = ""
    with pytest.raises(ProviderBlockedError, match=r"application_shared_network_conflict|application_kubernetes_invalid_response"):
        await runtime(runtime_context).read_shared_network(lease)
    assert len(api.mutations) == 1


async def test_shared_network_preflight_cannot_return_evidence_after_supersession(runtime_context, platform_inputs, monkeypatch):
    registry, client, _, api, lease, alice = runtime_context
    paths = shared_network_objects(runtime_context, platform_inputs)
    request = client._request

    async def supersede(method, path, body=None):
        result = await request(method, path, body)
        if path == paths[-1]:
            await registry.transition(lease.application_id, principal=alice, idempotency_key="stop",
                action="suspend", expected_generation=1)
        return result

    monkeypatch.setattr(client, "_request", supersede)
    with pytest.raises(ManagementError, match="stale_operation_lease"):
        await runtime(runtime_context).read_shared_network(lease)
    assert len(api.mutations) == 1


async def test_shared_network_preflight_does_not_follow_foreign_authority(runtime_context, platform_inputs):
    from loom_service.application_management.runtime import ApplicationRuntimeProvider

    registry, client, authority, api, lease, _ = runtime_context
    shared_network_objects(runtime_context, platform_inputs)
    other = authority.model_copy(update={"shared_namespace": "loom-foreign"})
    provider = ApplicationRuntimeProvider(registry, client, authority=other)
    with pytest.raises(ProviderBlockedError, match="application_runtime_authority_conflict"):
        await provider.read_shared_network(lease)
    assert len(api.mutations) == 1
