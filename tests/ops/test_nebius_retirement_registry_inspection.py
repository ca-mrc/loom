"""The protected probe selects only the failed retirement's exact manager."""
from __future__ import annotations

import copy
import json
from uuid import uuid4

import pytest
from scripts.ops import nebius_management_preflight as preflight
from scripts.ops.nebius_management_retirement import retirement_documents

from loom_service.environment_management.deployment import render_management
from tests.ops.test_nebius_application_setup import setup_request as setup_request
from tests.ops.test_nebius_management_preflight import Cluster
from tests.ops.test_nebius_management_retirement import retirement_request as retirement_request
from tests.unit.test_nebius_management_render import application_management_inputs as application_management_inputs
from tests.unit.test_nebius_management_render import management_inputs as management_inputs
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


class RegistryCluster(Cluster):
    def __init__(self, request):
        super().__init__()
        self.namespace = request.binding.namespace
        docs = retirement_documents(request)
        self.job = next(copy.deepcopy(doc) for doc in docs["job"].values() if doc["kind"] == "Job")
        self.cm = next(copy.deepcopy(doc) for doc in docs["job"].values() if doc["kind"] == "ConfigMap")
        self.job["metadata"]["uid"] = str(uuid4())
        self.job["status"] = {"conditions": [{"type": "Failed", "status": "True"}]}
        self.pod = copy.deepcopy(self.job["spec"]["template"])
        self.pod["metadata"].update(name=self.job["metadata"]["name"] + "-abcde", namespace=self.namespace,
            uid=str(uuid4()), ownerReferences=[{"kind": "Job", "controller": True,
            "name": self.job["metadata"]["name"], "uid": self.job["metadata"]["uid"]}])
        self.pod["status"] = {"phase": "Failed"}
        rendered = render_management(request.deployment, candidate=request.candidate, profile=request.profile, repo_root=request.repo_root)
        deployment = next(doc for doc in rendered.files["40-services.yaml"] if doc["kind"] == "Deployment")
        self.manager = copy.deepcopy(deployment["spec"]["template"])
        self.manager["metadata"].update(name="loom-service-abcde", namespace=self.namespace, uid=str(uuid4()))
        self.manager["status"] = {"phase": "Running", "containerStatuses": [{"name": "loom-service", "ready": True}]}
        self.lists["pods"] += [self.pod, self.manager]
        self.targets = [target.model_dump(mode="json") for target in request.targets]
        self.executions = 0

    def get(self, kind, name, namespace):
        if kind in {"job", "pod"} or (kind == "configmap" and name == self.cm["metadata"]["name"]):
            self.calls.append(("get", kind, name, namespace))
            selected = {"job": self.job, "pod": self.manager, "configmap": self.cm}[kind]
            assert (name, namespace) == (selected["metadata"]["name"], self.namespace)
            return copy.deepcopy(selected)
        return super().get(kind, name, namespace)

    def run(self, *args, **kwargs):
        if args[0] == "logs":
            return '{"status":"retirement_blocked"}'
        if args[0] == "exec":
            self.executions += 1
            self.calls.append(args)
            assert args[:9] == ("exec", "loom-service-abcde", "-n", self.namespace, "-c", "loom-service", "--", "python", "-c")
            assert args[10] == self.namespace and json.loads(args[11]) == self.targets
            assert kwargs == {"timeout": 90}
            return json.dumps(self.response())
        return super().run(*args, **kwargs)

    def response(self):
        from scripts.ops.nebius_retirement_registry_probe import CHECKS

        return {"status": "observed", "read_only": True, "private": "private-payload", "targets": [
            {"operation_id": target["operation_id"], "checks": {key: True for key in CHECKS}, "private": "private-row"}
            for target in self.targets]}


def inspect(cluster):
    return preflight.inspect(cluster, namespace="loom-nebius-platform", expected_cluster_id="mk8scluster-test")


def test_failed_retirement_gets_fixed_read_only_registry_probe_without_exporting_payloads(retirement_request):
    cluster = RegistryCluster(retirement_request[0])
    result = inspect(cluster)
    assert cluster.executions == 1
    probe = result["failed_retirement_jobs"][0]["registry_probe"]
    assert probe["status"] == "observed" and probe["read_only"] is True
    assert probe["manager_pod_uid"] == cluster.manager["metadata"]["uid"]
    assert all(value is True for value in probe["targets"][0]["checks"].values())
    assert "private-" not in json.dumps(result)
    assert all(call[0] in {"get", "config", "exec"} for call in cluster.calls)


@pytest.mark.parametrize("mutation", ["foreign_manager", "legacy_account", "not_ready", "duplicate_manager", "mutable_config", "wrong_targets"])
def test_unqualified_registry_probe_never_executes(retirement_request, mutation):
    cluster = RegistryCluster(retirement_request[0])
    if mutation == "foreign_manager":
        cluster.manager["metadata"]["labels"]["loom.nebius/management-installation"] = str(uuid4())
    elif mutation == "legacy_account":
        cluster.manager["spec"]["serviceAccountName"] = "loom-management-provisioner"
    elif mutation == "not_ready":
        cluster.manager["status"]["containerStatuses"][0]["ready"] = False
    elif mutation == "duplicate_manager":
        cluster.lists["pods"].append(copy.deepcopy(cluster.manager))
    elif mutation == "mutable_config":
        cluster.cm["immutable"] = False
    else:
        cluster.cm["data"]["retirement.json"] = '{"targets":"private-invalid"}'
    result = inspect(cluster)
    assert cluster.executions == 0
    assert result["failed_retirement_jobs"][0]["registry_probe"]["status"] == "unavailable"


def test_manager_replacement_after_probe_discards_its_observations(retirement_request):
    class Replaced(RegistryCluster):
        def response(self):
            self.manager["metadata"]["uid"] = str(uuid4())
            return super().response()

    cluster = Replaced(retirement_request[0])
    assert inspect(cluster)["failed_retirement_jobs"][0]["registry_probe"] == {"status": "unavailable"}
