"""Real bounded HTTPS retirement transport and complete process-drain evidence."""
from __future__ import annotations

import copy
import json
import ssl
from uuid import uuid4

import httpx
import pytest
from scripts.ops.nebius_pool_retirement import retirement_documents, stopped_document
from tests.ops.test_nebius_pool_retirement import initialize, retire
from tests.ops.test_nebius_pool_retirement import retirement_inputs as retirement_inputs
from tests.ops.test_nebius_pool_runtime import runtime_inputs as runtime_inputs
from tests.unit.test_nebius_management_render import management_inputs as management_inputs
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


def page(kind, items):
    return {"apiVersion": {"Pod": "v1", "ReplicaSet": "apps/v1", "Job": "batch/v1"}[kind],
        "kind": kind + "List", "metadata": {"resourceVersion": "10"}, "items": items}


def drained_inputs(request, kind):
    key, original = next((key, row) for key, row in retirement_documents(request).items() if row["kind"] == kind)
    current = stopped_document(request, key)
    current["metadata"].update(uid=original["metadata"]["uid"], resourceVersion="2", generation=2)
    current["status"] = {"observedGeneration": 2} if kind == "Deployment" else {"active": []}
    return key, current, page("ReplicaSet" if kind == "Deployment" else "Job", []), page("Pod", [])


@pytest.mark.parametrize("kind", ["Deployment", "CronJob"])
def test_actual_drained_workload_does_not_depend_on_old_cp_still_running(retirement_inputs, kind):
    from scripts.ops.nebius_pool_retirement import qualify_pool_drain

    key, current, children, pods = drained_inputs(retirement_inputs, kind)
    assert qualify_pool_drain(retirement_inputs, key=key, current=current, children=children, pods=pods) is True


@pytest.mark.parametrize("damage", ["generation", "counter", "continuation", "foreign_namespace", "terminating_pod", "replica_set"])
def test_missing_or_live_deployment_evidence_cannot_qualify_retirement(retirement_inputs, damage):
    from scripts.ops.nebius_pool_retirement import qualify_pool_drain

    key, current, children, pods = drained_inputs(retirement_inputs, "Deployment")
    if damage == "generation":
        current["status"]["observedGeneration"] = 1
    elif damage == "counter":
        current["status"]["replicas"] = True
    elif damage == "continuation":
        pods["metadata"]["continue"] = "more"
    elif damage == "foreign_namespace":
        pods["items"] = [{"metadata": {"name": "foreign", "namespace": "foreign", "uid": str(uuid4())}}]
    elif damage == "terminating_pod":
        pods["items"] = [{"metadata": {"name": "old", "namespace": current["metadata"]["namespace"], "uid": str(uuid4()),
            "labels": current["spec"]["selector"]["matchLabels"], "deletionTimestamp": "2026-09-30T00:00:00Z"}}]
    else:
        children["items"] = [{"metadata": {"name": "old-rs", "namespace": current["metadata"]["namespace"], "uid": str(uuid4()),
            "generation": 1, "labels": current["spec"]["selector"]["matchLabels"], "ownerReferences": [{
                "apiVersion": "apps/v1", "kind": "Deployment", "name": current["metadata"]["name"], "uid": current["metadata"]["uid"], "controller": True}]},
            "spec": {"replicas": 1}, "status": {"observedGeneration": 1, "replicas": 1}}]
    if damage in {"counter", "continuation", "foreign_namespace"}:
        with pytest.raises(ValueError):
            qualify_pool_drain(retirement_inputs, key=key, current=current, children=children, pods=pods)
    else:
        assert qualify_pool_drain(retirement_inputs, key=key, current=current, children=children, pods=pods) is False


@pytest.mark.parametrize("live", [False, True])
def test_completed_collector_history_is_not_a_running_process(retirement_inputs, live):
    from scripts.ops.nebius_pool_retirement import qualify_pool_drain

    key, current, jobs, pods = drained_inputs(retirement_inputs, "CronJob")
    job_uid, pod_uid = str(uuid4()), str(uuid4())
    namespace = current["metadata"]["namespace"]
    jobs["items"] = [{"metadata": {"name": "collector-old", "namespace": namespace, "uid": job_uid, "ownerReferences": [{
        "apiVersion": "batch/v1", "kind": "CronJob", "name": current["metadata"]["name"], "uid": current["metadata"]["uid"], "controller": True}]},
        "status": {"conditions": [{"type": "Complete", "status": "True"}], "active": 0}}]
    pods["items"] = [{"metadata": {"name": "collector-old-pod", "namespace": namespace, "uid": pod_uid, "ownerReferences": [{
        "apiVersion": "batch/v1", "kind": "Job", "name": "collector-old", "uid": job_uid, "controller": True}]},
        "spec": {"containers": [{"name": "collector"}], "initContainers": [{"name": "prepare-credentials"}]},
        "status": {"phase": "Succeeded", "containerStatuses": [{"name": "collector", "state": {"running": {}} if live else {"terminated": {"exitCode": 0}}}],
            "initContainerStatuses": [{"name": "prepare-credentials", "state": {"terminated": {"exitCode": 0}}}]}}]
    assert qualify_pool_drain(retirement_inputs, key=key, current=current, children=jobs, pods=pods) is (not live)


class Guards:
    def __init__(self, request):
        self.request = request
        self.held = True

    def guard(self, target, action):
        assert target in self.request.migration.guards and action == "observe"
        return {"status": "held" if self.held else "open"}


@pytest.mark.parametrize("failure", [None, "before", "after", "conflict", "namespace", "guard"])
def test_https_patch_has_fixed_scope_uid_rv_preconditions_and_no_uncertain_retry(retirement_inputs, tmp_path, failure):
    from scripts.ops.nebius_pool_retirement_live import HTTPSPoolRetirementAPI

    request = retirement_inputs
    initialize(request, tmp_path)
    documents = retirement_documents(request)
    resources = {}
    for document in documents.values():
        resource = "cronjobs" if document["kind"] == "CronJob" else "deployments"
        resources[f'/apis/{document["apiVersion"]}/namespaces/{document["metadata"]["namespace"]}/{resource}/{document["metadata"]["name"]}'] = copy.deepcopy(document)
    binding = request.migration.registration.binding
    namespaces = {"kube-system": binding.kube_system_uid, binding.namespace: binding.namespace_uid,
        **{row.namespace: str(row.namespace_uid) for row in request.migration.guards},
        **{row.execution_namespace.name: str(row.execution_namespace.uid) for row in request.migration.registration.spec.participants}}
    patches = []

    def respond(message):
        path = message.url.path
        if path in {"/api/v1/namespaces/" + name for name in namespaces}:
            name = path.rsplit("/", 1)[-1]
            return httpx.Response(200, json={"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": name,
                "uid": str(uuid4()) if failure == "namespace" else namespaces[name], "labels": {
                    "loom.nebius/management-installation": binding.installation_id, "pod-security.kubernetes.io/enforce": "restricted"}}})
        if path in resources:
            current = resources[path]
            if message.method == "PATCH":
                patches.append(message)
                if failure == "before":
                    raise httpx.ReadError("private-marker")
                if failure == "conflict":
                    return httpx.Response(409, json={"kind": "Status", "code": 409})
                body = json.loads(message.content)
                assert body[:3] == [{"op": "test", "path": "/metadata/uid", "value": current["metadata"]["uid"]},
                    {"op": "test", "path": "/metadata/resourceVersion", "value": current["metadata"]["resourceVersion"]},
                    {"op": "test", "path": "/spec", "value": current["spec"]}]
                assert len(body) == 5 and message.headers["Content-Type"] == "application/json-patch+json"
                current["metadata"]["annotations"] = body[3]["value"]
                field = "suspend" if current["kind"] == "CronJob" else "replicas"
                assert body[4] == {"op": "replace", "path": "/spec/" + field, "value": True if field == "suspend" else 0}
                current["spec"][field] = body[4]["value"]
                current["metadata"].update(resourceVersion="2", generation=2)
                current["status"] = {"observedGeneration": 2} if field == "replicas" else {"active": []}
                if failure == "after":
                    raise httpx.ReadError("private-marker")
            return httpx.Response(200, json=current)
        for resource, kind in (("replicasets", "ReplicaSet"), ("jobs", "Job"), ("pods", "Pod")):
            if path.endswith("/" + resource):
                return httpx.Response(200, json=page(kind, []))
        raise AssertionError("outside expected read/update scope")

    guards = Guards(request)
    guards.held = failure != "guard"
    api = HTTPSPoolRetirementAPI(request=request, guards=guards, api_server="https://cluster.example", ssl_context=ssl.create_default_context())
    api.client.close()
    api.client = httpx.Client(base_url="https://cluster.example", transport=httpx.MockTransport(respond))
    with api:
        for _ in range(2):
            if failure in {"before", "namespace", "guard"}:
                with pytest.raises(ValueError):
                    retire(request, api, tmp_path)
            else:
                result = retire(request, api, tmp_path)
                assert result["status"] == ("pending_retirement" if failure == "conflict" else "old_pool_workloads_retired")
        assert len(patches) == {None: 9, "before": 1, "after": 9, "conflict": 2, "namespace": 0, "guard": 0}[failure]
