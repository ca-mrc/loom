"""Create-only Kubernetes provider: exact intent/ownership and UID readiness."""

from __future__ import annotations

import copy
import json
from uuid import uuid4

import httpx
import pytest

from loom_service.environment_management.steps import ProvisioningStep


@pytest.mark.parametrize("actual,want", [
    ({"name": "EMPTY"}, True),
    ({"name": "EMPTY", "value": ""}, True),
    ({"name": "EMPTY", "value": "other"}, False),
    ({"name": "EMPTY", "valueFrom": {"secretKeyRef": {"name": "foreign", "key": "token"}}}, False),
    ({"name": "EMPTY", "value": "", "valueFrom": {"secretKeyRef": {"name": "foreign", "key": "token"}}}, False),
])
def test_api_omitted_empty_environment_value_preserves_literal_not_secret_source(actual, want):
    from loom_service.environment_management.kubernetes_provider import _contains

    expected = {"spec": {"template": {"spec": {"containers": [{"env": [{"name": "EMPTY", "value": ""}]}]}}}}
    observed = {"spec": {"template": {"spec": {"containers": [{"env": [actual]}]}}}}
    assert _contains(observed, expected) is want
    assert not _contains({"name": "EMPTY"}, {"name": "EMPTY", "value": ""})


def context():
    from loom_service.environment_management.provider import ProvisioningContext
    from loom_service.environment_management.registry import OperationLease

    return ProvisioningContext(
        OperationLease(uuid4(), uuid4(), 1, 1, uuid4()),
        {"incarnation": str(uuid4()), "application_namespace": "loom-dev-alice",
         "execution_namespace": "loom-execution-alice", "build_namespace": "loom-execution-alice-build"},
        {}, {},
    )


def namespace():
    return ProvisioningStep("ns", "kubernetes", {
        "apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "loom-dev-alice"},
    })


async def test_create_readback_recovery_is_idempotent_and_does_not_patch():
    from loom_service.environment_management.kubernetes_provider import (
        KubernetesEnvironmentProvider,
    )

    stored, writes = {}, []

    def api(request):
        if request.method == "POST":
            writes.append(request.method)
            obj = json.loads(request.content)
            obj["metadata"]["uid"] = "namespace-uid"
            stored.update(obj)
            return httpx.Response(201, json=obj)
        assert request.method == "GET"
        return httpx.Response(200, json=stored) if stored else httpx.Response(404)

    async with httpx.AsyncClient(base_url="https://kubernetes.test", transport=httpx.MockTransport(api)) as http:
        provider = KubernetesEnvironmentProvider(http)
        ctx = context()
        assert await provider.apply(ctx, namespace()) == "namespace-uid"
        assert await provider.apply(ctx, namespace()) == "namespace-uid"
        assert writes == ["POST"]
        assert stored["metadata"]["labels"]["loom.nebius/environment-id"] == str(ctx.lease.environment_id)


@pytest.mark.parametrize("change", ["owner", "body", "deleted", "uid"])
async def test_existing_foreign_changed_deleting_or_replaced_resource_is_never_adopted(change):
    from loom_service.environment_management.kubernetes_provider import (
        KubernetesEnvironmentProvider,
    )
    from loom_service.environment_management.provider import ProviderBlockedError

    stored = {}

    def api(request):
        if request.method == "POST":
            stored.update(json.loads(request.content))
            stored["metadata"]["uid"] = "owned-uid"
            return httpx.Response(201, json=stored)
        return httpx.Response(200, json=stored) if stored else httpx.Response(404)

    async with httpx.AsyncClient(base_url="https://kubernetes.test", transport=httpx.MockTransport(api)) as http:
        provider = KubernetesEnvironmentProvider(http)
        ctx = context()
        assert await provider.apply(ctx, namespace()) == "owned-uid"
        if change == "owner":
            stored["metadata"]["labels"]["loom.nebius/environment-id"] = str(uuid4())
        elif change == "body":
            stored["kind"] = "Secret"
        elif change == "deleted":
            stored["metadata"]["deletionTimestamp"] = "2026-09-22T00:00:00Z"
        else:
            ctx.identities["ns"] = "earlier-uid"
        with pytest.raises(ProviderBlockedError):
            await provider.apply(ctx, namespace())


async def test_job_ready_requires_recorded_uid_and_complete_condition():
    from loom_service.environment_management.kubernetes_provider import (
        KubernetesEnvironmentProvider,
    )
    from loom_service.environment_management.provider import (
        ProviderBlockedError,
        ProviderRetryError,
    )

    ctx = context()
    ctx.identities["job"] = "owned-job"
    job = {"metadata": {"uid": "owned-job"}, "status": {}}
    step = ProvisioningStep("ready", "job_ready", {
        "namespace": "loom-dev-alice", "name": "loom-migrate", "resource_key": "job",
    })
    async with httpx.AsyncClient(base_url="https://kubernetes.test", transport=httpx.MockTransport(
        lambda request: httpx.Response(200, json=copy.deepcopy(job)),
    )) as http:
        provider = KubernetesEnvironmentProvider(http)
        with pytest.raises(ProviderRetryError):
            await provider.apply(ctx, step)
        job["status"] = {"conditions": [{"type": "Complete", "status": "True"}]}
        assert await provider.apply(ctx, step) == "owned-job"
        job["metadata"]["uid"] = "replacement-job"
        with pytest.raises(ProviderBlockedError):
            await provider.apply(ctx, step)


@pytest.mark.parametrize("kind,namespace_name", [("ClusterRole", None), ("Secret", "foreign"), ("Namespace", "foreign")])
async def test_provider_rejects_out_of_scope_intent_without_network_call(kind, namespace_name):
    from loom_service.environment_management.kubernetes_provider import (
        KubernetesEnvironmentProvider,
    )
    from loom_service.environment_management.provider import ProviderBlockedError

    def unexpected(request):
        pytest.fail("out-of-scope intent made a request")

    doc = {"apiVersion": "v1", "kind": kind, "metadata": {"name": "foreign"}}
    if kind != "Namespace":
        doc["metadata"]["namespace"] = namespace_name
    async with httpx.AsyncClient(base_url="https://kubernetes.test", transport=httpx.MockTransport(unexpected)) as http:
        with pytest.raises(ProviderBlockedError):
            await KubernetesEnvironmentProvider(http).apply(context(), ProvisioningStep("bad", "kubernetes", doc))


async def test_namespace_replacement_blocks_creation_into_the_same_name():
    from loom_service.environment_management.kubernetes_provider import (
        KubernetesEnvironmentProvider,
    )
    from loom_service.environment_management.provider import ProviderBlockedError

    ctx = context()
    ctx.identities["k8s:Namespace:-:loom-dev-alice"] = "original-namespace"
    calls = []

    def api(request):
        calls.append(request.method)
        if request.url.path == "/api/v1/namespaces/loom-dev-alice":
            return httpx.Response(200, json={"metadata": {"uid": "foreign-replacement"}})
        if request.method == "GET":
            return httpx.Response(404)
        obj = json.loads(request.content)
        obj["metadata"]["uid"] = "wrongly-created-object"
        return httpx.Response(201, json=obj)

    doc = {"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"namespace": "loom-dev-alice", "name": "test"},
           "data": {"key": "value"}}
    async with httpx.AsyncClient(base_url="https://kubernetes.test", transport=httpx.MockTransport(api)) as http:
        with pytest.raises(ProviderBlockedError):
            await KubernetesEnvironmentProvider(http).apply(ctx, ProvisioningStep("cm", "kubernetes", doc))
    assert "POST" not in calls


async def test_application_readiness_requires_all_frozen_deployments_and_current_rollout():
    from loom_service.environment_management.kubernetes_provider import (
        KubernetesEnvironmentProvider,
    )
    from loom_service.environment_management.provider import (
        ProviderBlockedError,
        ProviderWaitingError,
    )

    ctx = context()
    rows = {}

    def api(request):
        path = request.url.path
        if request.method == "POST":
            doc = json.loads(request.content)
            doc["metadata"].update(uid="uid-" + doc["metadata"]["name"], generation=1)
            if doc["kind"] == "Deployment":
                doc["status"] = {"observedGeneration": 1, "replicas": 1, "readyReplicas": 1,
                                 "updatedReplicas": 1, "availableReplicas": 1}
            rows[path + "/" + doc["metadata"]["name"]] = doc
            return httpx.Response(201, json=doc)
        return httpx.Response(200, json=rows[path]) if path in rows else httpx.Response(404)

    async with httpx.AsyncClient(base_url="https://kubernetes.test", transport=httpx.MockTransport(api)) as http:
        provider = KubernetesEnvironmentProvider(http)
        ctx.identities["k8s:Namespace:-:loom-dev-alice"] = await provider.apply(ctx, namespace())
        for name in ("loom-service", "loom-control-plane", "loom-llm-gateway", "loom-web"):
            key = "k8s:Deployment:loom-dev-alice:" + name
            doc = {"apiVersion": "apps/v1", "kind": "Deployment", "metadata": {"namespace": "loom-dev-alice", "name": name},
                   "spec": {"replicas": 1, "template": {"spec": {"containers": [{"name": name, "image": "approved@sha256:" + "a" * 64}]}}}}
            ctx.identities[key] = await provider.apply(ctx, ProvisioningStep(key, "kubernetes", doc))
            ctx.documents[key] = doc
        step = ProvisioningStep("ready:services", "application_ready", {"namespace": "loom-dev-alice", "phase": "services"})
        service = rows["/apis/apps/v1/namespaces/loom-dev-alice/deployments/loom-service"]
        service["status"]["observedGeneration"] = 0
        with pytest.raises(ProviderWaitingError):
            await provider.apply(ctx, step)
        service["status"]["observedGeneration"] = 1
        identity = await provider.apply(ctx, step)
        assert identity.startswith("deployments:")
        assert await provider.apply(ctx, step) == identity
        service["spec"]["template"]["spec"]["containers"][0]["image"] = "unapproved:changed"
        with pytest.raises(ProviderBlockedError):
            await provider.apply(ctx, step)


async def test_api_empty_list_omission_preserves_default_deny_readback():
    from loom_service.environment_management.kubernetes_provider import (
        KubernetesEnvironmentProvider,
    )

    ctx, rows = context(), {}

    def api(request):
        if request.method == "POST":
            doc = json.loads(request.content)
            doc["metadata"]["uid"] = "uid-" + doc["metadata"]["name"]
            if doc["kind"] == "NetworkPolicy":
                doc["spec"].pop("ingress")
                doc["spec"].pop("egress")
            rows[request.url.path + "/" + doc["metadata"]["name"]] = doc
            return httpx.Response(201, json=doc)
        return httpx.Response(200, json=rows[request.url.path]) if request.url.path in rows else httpx.Response(404)

    doc = {"apiVersion": "networking.k8s.io/v1", "kind": "NetworkPolicy",
           "metadata": {"namespace": "loom-dev-alice", "name": "default-deny"},
           "spec": {"podSelector": {}, "policyTypes": ["Ingress", "Egress"], "ingress": [], "egress": []}}
    async with httpx.AsyncClient(base_url="https://kubernetes.test", transport=httpx.MockTransport(api)) as http:
        provider = KubernetesEnvironmentProvider(http)
        ctx.identities["k8s:Namespace:-:loom-dev-alice"] = await provider.apply(ctx, namespace())
        assert await provider.apply(ctx, ProvisioningStep("policy", "kubernetes", doc)) == "uid-default-deny"


def test_controller_readback_allows_omitted_empty_env_value_but_rejects_value_from():
    from loom_service.environment_management.kubernetes_provider import (
        KubernetesEnvironmentProvider,
    )
    from loom_service.environment_management.provider import ProviderBlockedError

    expected = {"apiVersion": "apps/v1", "kind": "Deployment", "metadata": {"name": "web"},
                "spec": {"template": {"spec": {"containers": [{"name": "web", "image": "fixed",
                    "env": [{"name": "OPTIONAL_SETTING", "value": ""}]}]}}}}
    actual = copy.deepcopy(expected)
    actual["metadata"]["uid"] = "retained"
    variable = actual["spec"]["template"]["spec"]["containers"][0]["env"][0]
    variable.pop("value")  # Kubernetes omits the optional empty string.
    assert KubernetesEnvironmentProvider._identity(actual, expected, "retained") == "retained"
    variable["valueFrom"] = {"secretKeyRef": {"name": "foreign", "key": "credential"}}
    with pytest.raises(ProviderBlockedError, match="identity_conflict"):
        KubernetesEnvironmentProvider._identity(actual, expected, "retained")


@pytest.mark.parametrize("kind,spec,change", [
    ("NetworkPolicy", {"podSelector": {}, "policyTypes": ["Ingress", "Egress"], "ingress": [], "egress": []},
     {"podSelector": {"matchLabels": {"app": "unrelated"}}}),
    ("NetworkPolicy", {"podSelector": {"matchLabels": {"app": "loom"}}, "policyTypes": ["Ingress"]},
     {"podSelector": {"matchLabels": {"app": "loom", "foreign": "true"}}}),
    ("NetworkPolicy", {"podSelector": {}, "policyTypes": ["Ingress"]},
     {"ingress": [{}]}),
    ("NetworkPolicy", {"podSelector": {}, "policyTypes": ["Ingress"], "ingress": [{"from": [{"namespaceSelector": {}}]}]},
     {"ingress": [{"from": [{"namespaceSelector": {}, "podSelector": {"matchLabels": {"app": "unrelated"}}}]}]}),
    ("ResourceQuota", {"hard": {"pods": "0"}}, {"scopes": ["Terminating"]}),
    ("ResourceQuota", {"hard": {"pods": "0"}},
     {"scopeSelector": {"matchExpressions": [{"scopeName": "Terminating", "operator": "Exists"}]}}),
    ("Service", {"selector": {"app": "loom"}, "ports": [{"port": 80}]},
     {"selector": {"app": "loom", "foreign": "true"}}),
])
async def test_readback_rejects_authority_changing_policy_quota_and_selector_additions(kind, spec, change):
    from loom_service.environment_management.kubernetes_provider import (
        KubernetesEnvironmentProvider,
    )
    from loom_service.environment_management.provider import ProviderBlockedError

    ctx, stored = context(), {}

    def api(request):
        path = request.url.path
        if request.method == "POST":
            obj = json.loads(request.content)
            obj["metadata"]["uid"] = "uid-" + obj["metadata"]["name"]
            stored[path + "/" + obj["metadata"]["name"]] = obj
            return httpx.Response(201, json=obj)
        return httpx.Response(200, json=stored[path]) if path in stored else httpx.Response(404)

    doc = {"apiVersion": "networking.k8s.io/v1" if kind == "NetworkPolicy" else "v1", "kind": kind,
           "metadata": {"namespace": "loom-dev-alice", "name": "fence"}, "spec": spec}
    step = ProvisioningStep("fence", "kubernetes", doc)
    async with httpx.AsyncClient(base_url="https://kubernetes.test", transport=httpx.MockTransport(api)) as http:
        provider = KubernetesEnvironmentProvider(http)
        ctx.identities["k8s:Namespace:-:loom-dev-alice"] = await provider.apply(ctx, namespace())
        ctx.identities[step.key] = await provider.apply(ctx, step)
        _, path = provider._path(ctx, doc)
        stored[path]["spec"].update(change)
        stored[path]["status"] = {"hard": {"pods": "0"}, "used": {"pods": "0"}}
        with pytest.raises(ProviderBlockedError, match="kubernetes_resource_identity_conflict"):
            await provider.apply(ctx, step)
