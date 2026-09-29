"""Retained controller names prevent delayed creation from restarting compute."""

from __future__ import annotations

import copy
import json
from uuid import uuid4

import httpx
import pytest

from loom_service.environment_management.kubernetes_provider import KubernetesEnvironmentProvider
from loom_service.environment_management.provider import ProviderBlockedError, ProviderRetryError
from loom_service.environment_management.steps import ProvisioningStep
from tests.unit.test_nebius_environment_kubernetes_provider import context


@pytest.mark.parametrize("action,expected", [
    ("retained_owner_revoke", "retained_child_client_required"),
    ("retained_key_revoke", "retained_cloud_client_required"),
])
async def test_unconfigured_revocation_client_cannot_be_silently_skipped(action, expected):
    from dataclasses import replace
    from types import SimpleNamespace

    from loom_service.environment_management.retained_destroy import EnvironmentRetainedDestroy

    source = context()
    source.identities["credentials:material"] = "delivered"
    ctx = replace(source, action="destroy_retained", source=source)
    def unexpected(request):
        pytest.fail("revocation unexpectedly attempted a Kubernetes call")
    async with httpx.AsyncClient(base_url="https://kubernetes.test", transport=httpx.MockTransport(unexpected)) as http:
        cleanup = EnvironmentRetainedDestroy(SimpleNamespace(), KubernetesEnvironmentProvider(http), None, None)
        with pytest.raises(ProviderBlockedError, match=expected):
            await cleanup.apply(ctx, ProvisioningStep("revoke", "credentials", {"action": action, "purpose": "canonical"}))


def workload(kind):
    spec = {"template": {"metadata": {"labels": {"app": "loom-test"}},
                         "spec": {"containers": [{"name": "test", "image": "test@sha256:" + "a" * 64}]}}}
    if kind in {"Deployment", "StatefulSet"}:
        spec.update(replicas=1, selector={"matchLabels": {"app": "loom-test"}})
    if kind == "StatefulSet":
        spec["persistentVolumeClaimRetentionPolicy"] = {"whenDeleted": "Retain", "whenScaled": "Retain"}
    if kind == "CronJob":
        spec = {"schedule": "17 */6 * * *", "jobTemplate": {"spec": spec}}
    if kind == "Ingress":
        spec = {"rules": [{"host": "alice.example.com", "http": {"paths": [{"path": "/", "pathType": "Prefix",
                 "backend": {"service": {"name": "loom-service", "port": {"number": 8000}}}}]}}]}
    api = {"Deployment": "apps/v1", "StatefulSet": "apps/v1", "Job": "batch/v1", "CronJob": "batch/v1",
           "Ingress": "networking.k8s.io/v1"}[kind]
    return ProvisioningStep("workload", "kubernetes", {"apiVersion": api, "kind": kind,
                          "metadata": {"namespace": "loom-dev-alice", "name": "loom-test"}, "spec": spec})


@pytest.mark.parametrize("kind", ["Deployment", "StatefulSet", "Job", "CronJob", "Ingress"])
@pytest.mark.parametrize("already_exists", [True, False])
async def test_stop_preserves_owned_name_and_fences_patch_against_replacement(kind, already_exists):
    from loom_service.environment_management.retained_cleanup import RetainedKubernetesCleanup

    ctx, step, cleanup_id = context(), workload(kind), uuid4()
    ctx.identities["k8s:Namespace:-:loom-dev-alice"] = "namespace-uid"
    expected = KubernetesEnvironmentProvider._expected(ctx, step)
    stored = copy.deepcopy(expected) if already_exists else None
    if stored is not None:
        stored["metadata"].update(uid="workload-uid", resourceVersion="7")
        ctx.identities[step.key] = "workload-uid"
    mutations = []

    def api(request):
        nonlocal stored
        if request.url.path == "/api/v1/namespaces/loom-dev-alice":
            return httpx.Response(200, json={"metadata": {"uid": "namespace-uid", "labels": expected["metadata"]["labels"]}})
        if request.method == "GET":
            return httpx.Response(200, json=stored) if stored is not None else httpx.Response(404)
        mutations.append(request.method)
        if request.method == "POST":
            assert stored is None
            stored = json.loads(request.content)
            stored["metadata"].update(uid="workload-uid", resourceVersion="8")
        else:
            assert request.method == "PATCH", "cleanup must retain the original names, never delete data/controllers"
            assert request.headers["content-type"] == "application/json-patch+json"
            patch = json.loads(request.content)
            assert patch[:2] == [{"op": "test", "path": "/metadata/uid", "value": "workload-uid"},
                                 {"op": "test", "path": "/metadata/resourceVersion", "value": "7"}]
            for edit in patch[2:]:
                if edit["path"] == "/spec":
                    stored["spec"] = edit["value"]
                elif edit["path"] == "/metadata/annotations/loom.nebius~1retained-by":
                    stored["metadata"]["annotations"]["loom.nebius/retained-by"] = edit["value"]
                else:
                    pytest.fail("unbounded patch " + edit["path"])
            stored["metadata"]["resourceVersion"] = "8"
        return httpx.Response(200 if request.method == "PATCH" else 201, json=stored)

    async with httpx.AsyncClient(base_url="https://kubernetes.test", transport=httpx.MockTransport(api)) as http:
        cleanup = RetainedKubernetesCleanup(KubernetesEnvironmentProvider(http))
        assert await cleanup.stop(ctx, step, cleanup_id=cleanup_id) == "workload-uid"
        assert await cleanup.stop(ctx, step, cleanup_id=cleanup_id) == "workload-uid"
    assert mutations == ["PATCH" if already_exists else "POST"]
    assert stored["metadata"]["uid"] == "workload-uid"
    if kind in {"Deployment", "StatefulSet"}:
        assert stored["spec"]["replicas"] == 0
        if kind == "StatefulSet":
            assert stored["spec"]["persistentVolumeClaimRetentionPolicy"] == {"whenDeleted": "Retain", "whenScaled": "Retain"}
    elif kind in {"Job", "CronJob"}:
        assert stored["spec"]["suspend"] is True
    else:
        assert stored["spec"]["rules"][0]["http"]["paths"][0]["backend"]["service"]["name"].startswith("loom-retained-")


@pytest.mark.parametrize("change", ["owner", "uid", "body", "deleting", "namespace"])
async def test_stop_rejects_foreign_or_drifted_resources_without_mutation(change):
    from loom_service.environment_management.retained_cleanup import RetainedKubernetesCleanup

    ctx, step = context(), workload("Deployment")
    ctx.identities["k8s:Namespace:-:loom-dev-alice"] = "namespace-uid"
    ctx.identities[step.key] = "original-uid"
    expected = KubernetesEnvironmentProvider._expected(ctx, step)
    stored = copy.deepcopy(expected)
    stored["metadata"].update(uid="original-uid", resourceVersion="7")
    namespace = {"metadata": {"uid": "namespace-uid", "labels": copy.deepcopy(expected["metadata"]["labels"])}}
    if change == "namespace":
        namespace["metadata"]["uid"] = "replacement-namespace"
    elif change == "owner":
        stored["metadata"]["labels"]["loom.nebius/environment-id"] = str(uuid4())
    elif change == "uid":
        stored["metadata"]["uid"] = "replacement"
    elif change == "deleting":
        stored["metadata"]["deletionTimestamp"] = "2026-09-22T00:00:00Z"
    else:
        stored["spec"]["template"]["spec"]["containers"][0]["image"] = "other-image"

    def api(request):
        assert request.method == "GET"
        return httpx.Response(200, json=namespace if request.url.path.startswith("/api/v1/namespaces/") else stored)

    async with httpx.AsyncClient(base_url="https://kubernetes.test", transport=httpx.MockTransport(api)) as http:
        with pytest.raises(ProviderBlockedError):
            await RetainedKubernetesCleanup(KubernetesEnvironmentProvider(http)).stop(ctx, step, cleanup_id=uuid4())


async def test_stop_retries_conflict_and_never_treats_it_as_success():
    from loom_service.environment_management.retained_cleanup import RetainedKubernetesCleanup

    ctx, step = context(), workload("Deployment")
    ctx.identities["k8s:Namespace:-:loom-dev-alice"] = "namespace-uid"
    original = KubernetesEnvironmentProvider._expected(ctx, step)
    original["metadata"].update(uid="workload-uid", resourceVersion="7")

    def api(request):
        if request.method == "PATCH":
            return httpx.Response(409)
        if request.url.path == "/api/v1/namespaces/loom-dev-alice":
            return httpx.Response(200, json={"metadata": {"uid": "namespace-uid", "labels": original["metadata"]["labels"]}})
        return httpx.Response(200, json=original)

    async with httpx.AsyncClient(base_url="https://kubernetes.test", transport=httpx.MockTransport(api)) as http:
        with pytest.raises(ProviderRetryError):
            await RetainedKubernetesCleanup(KubernetesEnvironmentProvider(http)).stop(ctx, step, cleanup_id=uuid4())


@pytest.mark.parametrize("change", ["replacement", "running", "changed-owner", "foreign-namespace"])
async def test_terminal_pod_cleanup_cannot_delete_replacement_live_or_foreign_pod(change):
    from loom_service.environment_management.retained_cleanup import RetainedKubernetesCleanup

    ctx = context()
    doc = {"apiVersion": "v1", "kind": "Pod", "metadata": {"name": "done", "namespace": "loom-dev-alice", "uid": "original-pod",
            "ownerReferences": [{"kind": "Job", "uid": "owned-job", "name": "job", "controller": True}]}}
    observed = copy.deepcopy(doc)
    observed["status"] = {"phase": "Succeeded"}
    if change == "replacement":
        observed["metadata"]["uid"] = "replacement-pod"
    elif change == "changed-owner":
        observed["metadata"]["ownerReferences"][0]["uid"] = "foreign-job"
    elif change == "running":
        observed["status"]["phase"] = "Running"
    else:
        doc["metadata"]["namespace"] = "foreign"

    def api(request):
        assert request.method == "GET", "unsafe Pod deletion"
        return httpx.Response(200, json=observed)

    async with httpx.AsyncClient(base_url="https://kubernetes.test", transport=httpx.MockTransport(api)) as http:
        with pytest.raises(ProviderBlockedError):
            await RetainedKubernetesCleanup(KubernetesEnvironmentProvider(http)).terminal_pod(ctx, doc)


async def test_terminal_pod_delete_precondition_and_absence_readback():
    from loom_service.environment_management.provider import ProviderWaitingError
    from loom_service.environment_management.retained_cleanup import RetainedKubernetesCleanup

    ctx = context()
    doc = {"apiVersion": "v1", "kind": "Pod", "metadata": {"name": "done", "namespace": "loom-dev-alice", "uid": "original-pod",
            "ownerReferences": [{"kind": "Job", "uid": "owned-job", "name": "job", "controller": True}]}}
    current = copy.deepcopy(doc)
    current["status"] = {"phase": "Succeeded"}

    def api(request):
        nonlocal current
        if request.method == "GET":
            return httpx.Response(200, json=current) if current else httpx.Response(404)
        assert request.method == "DELETE"
        assert json.loads(request.content)["preconditions"] == {"uid": "original-pod"}
        current = None
        return httpx.Response(202, json={"kind": "Status"})

    async with httpx.AsyncClient(base_url="https://kubernetes.test", transport=httpx.MockTransport(api)) as http:
        cleanup = RetainedKubernetesCleanup(KubernetesEnvironmentProvider(http))
        with pytest.raises(ProviderWaitingError):
            await cleanup.terminal_pod(ctx, doc)
        assert await cleanup.terminal_pod(ctx, doc) == "original-pod"


async def test_inventory_pagination_never_treats_an_unknown_repeated_page_as_empty():
    from loom_service.environment_management.retained_cleanup import RetainedKubernetesCleanup

    seen = []

    def api(request):
        seen.append(request.url.params["continue"])
        return httpx.Response(200, json={"items": [], "metadata": {"continue": "same-page"}})

    async with httpx.AsyncClient(base_url="https://kubernetes.test", transport=httpx.MockTransport(api)) as http:
        with pytest.raises(ProviderBlockedError, match="retained_inventory_incomplete"):
            await RetainedKubernetesCleanup(KubernetesEnvironmentProvider(http)).inventory(context(), "loom-dev-alice", "pods")
    assert seen == ["", "same-page"]


async def test_retained_quota_waits_for_enforcement_and_inflight_usage_readback():
    from dataclasses import replace
    from types import SimpleNamespace

    from loom_service.environment_management.provider import ProviderWaitingError
    from loom_service.environment_management.retained_destroy import EnvironmentRetainedDestroy

    source = context()
    source.identities["k8s:Namespace:-:loom-dev-alice"] = "namespace-uid"
    ctx = replace(source, lease=replace(source.lease, operation_id=uuid4(), deployment_generation=2),
                  action="destroy_retained", source=source)
    quota = None

    def api(request):
        nonlocal quota
        if request.url.path == "/api/v1/namespaces/loom-dev-alice":
            return httpx.Response(200, json={"metadata": {"uid": "namespace-uid", "labels": {
                "loom.nebius/environment-id": str(source.lease.environment_id),
                "loom.nebius/incarnation": source.registration["incarnation"],
            }}})
        if request.method == "POST":
            quota = json.loads(request.content)
            quota["metadata"]["uid"] = "quota-uid"
            return httpx.Response(201, json=quota)
        return httpx.Response(200, json=quota) if quota else httpx.Response(404)

    async with httpx.AsyncClient(base_url="https://kubernetes.test", transport=httpx.MockTransport(api)) as http:
        cleanup = EnvironmentRetainedDestroy(SimpleNamespace(), KubernetesEnvironmentProvider(http), SimpleNamespace(), SimpleNamespace())
        with pytest.raises(ProviderWaitingError):
            await cleanup._close_pod_admission(ctx, source, "loom-dev-alice")
        quota["status"] = {"hard": {"pods": "0"}, "used": {"pods": "1"}}
        assert await cleanup._close_pod_admission(ctx, source, "loom-dev-alice") == "quota-uid"
        with pytest.raises(ProviderWaitingError):
            await cleanup._close_pod_admission(ctx, source, "loom-dev-alice", require_idle=True)
        quota["status"]["used"]["pods"] = "0"
        assert await cleanup._close_pod_admission(ctx, source, "loom-dev-alice", require_idle=True) == "quota-uid"


@pytest.mark.parametrize("change", ["foreign-parent", "changed-template"])
async def test_discovery_rejects_unowned_or_changed_cron_children_before_journaling(change):
    from dataclasses import replace
    from types import SimpleNamespace

    from loom_service.environment_management.retained_destroy import EnvironmentRetainedDestroy

    source = context()
    cron = workload("CronJob").payload
    source.documents["cron"] = cron
    source.identities["cron"] = "owned-cron"
    ctx = replace(source, action="destroy_retained", source=source)
    job = {"apiVersion": "batch/v1", "kind": "Job", "metadata": {
        "name": "backup", "namespace": "loom-dev-alice", "uid": "job-uid",
        "ownerReferences": [{"kind": "CronJob", "uid": "owned-cron", "controller": True}],
    }, "spec": copy.deepcopy(cron["spec"]["jobTemplate"]["spec"])}
    if change == "foreign-parent":
        job["metadata"]["ownerReferences"][0]["uid"] = "foreign-cron"
    else:
        job["spec"]["template"]["spec"]["containers"][0]["image"] = "unrecognized-image"

    def api(request):
        assert request.method == "GET"
        return httpx.Response(200, json={"items": [job]})

    async with httpx.AsyncClient(base_url="https://kubernetes.test", transport=httpx.MockTransport(api)) as http:
        cleanup = EnvironmentRetainedDestroy(SimpleNamespace(), KubernetesEnvironmentProvider(http), SimpleNamespace(), SimpleNamespace())
        with pytest.raises(ProviderBlockedError, match="retained_unowned_job"):
            await cleanup._descendants(ctx, source, "loom-dev-alice")
