"""Startup diagnosis cannot expose payloads or confuse stale/current workloads."""
from __future__ import annotations

import copy
import json
from uuid import uuid4

import pytest
from scripts.ops import nebius_management_preflight as preflight
from tests.ops.test_nebius_management_gateway import operation
from tests.ops.test_nebius_management_preflight import Cluster

MANAGER = "loom-nebius-management"
EXECUTION = "loom-nebius-platform-execution"
LABEL = "loom.nebius/management-installation"
LOG = '''Traceback (most recent call last):
  File "/app/src/loom_service/app.py", line 237, in _management_lifespan
    private_source_line(secret)
  File "/app/src/loom_service/application_management/source_runtime.py", line 51, in open_source_uploader
    raise ValueError("private-source-line")
ValueError: invalid_application_source_runtime
ERROR: Application startup failed. Exiting.
'''


def resource(kind, name, namespace, spec, *, owner=None):
    metadata = {"name": name, "namespace": namespace, "uid": str(uuid4())}
    if owner is not None:
        metadata["ownerReferences"] = [{"apiVersion": owner["apiVersion"], "kind": owner["kind"],
            "name": owner["metadata"]["name"], "uid": owner["metadata"]["uid"], "controller": True}]
    return {"apiVersion": "batch/v1" if kind in {"Job", "CronJob"} else "v1" if kind in {"Pod", "ConfigMap"} else "apps/v1",
        "kind": kind, "metadata": metadata, "spec": copy.deepcopy(spec)}


class StartupCluster(Cluster):
    def __init__(self, installation):
        super().__init__()
        container = {"name": "loom-service", "image": "registry.test/service@sha256:" + "a" * 64,
            "env": [{"name": "LOOM_SVC_SERVICE_MODE", "value": "management"}]}
        template = {"spec": {"containers": [container]}}
        self.manager = resource("Deployment", "loom-service", MANAGER, {"template": template})
        self.manager["metadata"]["labels"] = {LABEL: installation}
        self.rs = resource("ReplicaSet", "loom-service-abcdefgh", MANAGER, {"template": template}, owner=self.manager)
        self.manager_pod = resource("Pod", "loom-service-abcdefgh-12345", MANAGER, template["spec"], owner=self.rs)
        self.manager_pod["status"] = {"phase": "Running", "containerStatuses": [{"name": "loom-service", "ready": False,
            "restartCount": 4, "state": {"waiting": {"reason": "CrashLoopBackOff"}},
            "lastState": {"terminated": {"exitCode": 3, "reason": "Error"}}}]}
        config_name = "loom-pool-collector-" + "b" * 32
        collector = {"name": "collector", "image": "registry.test/collector@sha256:" + "c" * 64,
            "command": ["python", "-m", "loom_execution_capacity_collector"],
            "envFrom": [{"configMapRef": {"name": config_name}}]}
        template = {"spec": {"containers": [collector]}}
        self.cron = resource("CronJob", "loom-execution-capacity-collector", EXECUTION,
            {"jobTemplate": {"spec": {"template": template}}})
        self.job = resource("Job", "loom-execution-capacity-collector-12345", EXECUTION, {"template": template}, owner=self.cron)
        self.collector_pod = resource("Pod", "loom-execution-capacity-collector-12345-abcde", EXECUTION,
            template["spec"], owner=self.job)
        self.collector_pod["status"] = {"phase": "Failed", "containerStatuses": [{"name": "collector", "ready": False,
            "restartCount": 0, "state": {"terminated": {"exitCode": 1, "reason": "Error"}}}]}
        self.cm = resource("ConfigMap", config_name, EXECUTION, {})
        self.cm.update(immutable=True, data={"LOOM_EXECUTION_CAPACITY_COLLECTOR_COLLECTION_MODE": "pool"})
        self.cm["metadata"]["labels"] = {LABEL: installation}
        self.lists["pods"] = [self.manager_pod, self.collector_pod]
        self.log = LOG
        self.log_error = False
        self.replace_on_log = False

    def get(self, kind, name, namespace):
        for doc in (self.manager, self.rs, self.manager_pod, self.cron, self.job, self.collector_pod, self.cm):
            if (kind, name, namespace) == (doc["kind"].lower(), doc["metadata"]["name"], doc["metadata"]["namespace"]):
                self.calls.append(("get", kind, name, namespace))
                return copy.deepcopy(doc)
        return super().get(kind, name, namespace)

    def run(self, *args, **kwargs):
        if args[0] != "logs":
            return super().run(*args, **kwargs)
        self.calls.append(args)
        assert kwargs == {"timeout": 40}
        assert args[-2:] == ("--tail=100", "--limit-bytes=32768")
        if self.log_error:
            raise RuntimeError("private-transport-error")
        if self.replace_on_log:
            self.manager_pod["metadata"]["uid"] = str(uuid4())
        return self.log


@pytest.fixture
def cluster(tmp_path, monkeypatch):
    metadata = operation(tmp_path)
    monkeypatch.setenv("NEBIUS_MANAGEMENT_OPERATION_JSON", json.dumps(metadata))
    return StartupCluster(metadata["installation_id"])


def inspect(cluster):
    return preflight.inspect(cluster, namespace="loom-nebius-platform", expected_cluster_id="mk8scluster-test")["pool_startup_diagnostics"]


def test_startup_inspection_identifies_manager_and_collector_without_payloads(cluster):
    result = inspect(cluster)
    assert result["status"] == "observed"
    manager, collector = result["workloads"]
    assert (manager["role"], manager["pod_uid"], manager["log_instance"]) == (
        "manager", cluster.manager_pod["metadata"]["uid"], "previous")
    assert (collector["role"], collector["pod_uid"], collector["log_instance"]) == (
        "collector", cluster.collector_pod["metadata"]["uid"], "current")
    assert manager["diagnostic"] == {"status": "observed", "errors": ["ValueError"],
        "stages": ["invalid_application_source_runtime"], "locations": [
            {"component": "service_app", "line": 237}, {"component": "application_source_runtime", "line": 51}]}
    assert "private" not in json.dumps(result)
    assert all(call[0] in {"get", "config", "logs"} for call in cluster.calls)
    assert not any(call[:2] == ("get", "secret") for call in cluster.calls)


@pytest.mark.parametrize("mutation", ["pod_owner", "rs_uid", "deployment_uid", "installation", "stale_image",
    "stale_environment", "not_management", "not_failed", "foreign_namespace", "pod_command"])
def test_unbound_manager_never_reads_logs(cluster, mutation):
    pod = cluster.manager_pod
    if mutation == "pod_owner":
        pod["metadata"]["ownerReferences"][0]["controller"] = False
    elif mutation == "rs_uid":
        cluster.rs["metadata"]["uid"] = str(uuid4())
    elif mutation == "deployment_uid":
        cluster.manager["metadata"]["uid"] = str(uuid4())
    elif mutation == "installation":
        cluster.manager["metadata"]["labels"][LABEL] = str(uuid4())
    elif mutation in {"stale_image", "stale_environment", "not_management"}:
        container = cluster.manager["spec"]["template"]["spec"]["containers"][0]
        if mutation == "stale_image":
            container["image"] = "different"
        else:
            container["env"] = [{"name": "LOOM_SVC_SERVICE_MODE", "value": "service"}]
    elif mutation == "not_failed":
        pod["status"]["containerStatuses"][0].update(state={"running": {}}, lastState={}, ready=True)
    elif mutation == "foreign_namespace":
        pod["metadata"]["namespace"] = "foreign"
    else:
        pod["spec"]["containers"][0]["command"] = ["sh", "-c", "private"]
    assert all(row["role"] != "manager" for row in inspect(cluster)["workloads"])
    assert not any(call[:2] == ("logs", pod["metadata"]["name"]) for call in cluster.calls)


@pytest.mark.parametrize("mutation", ["job_uid", "cron_uid", "stale_image", "not_pool", "installation", "mutable"])
def test_unbound_collector_never_reads_logs(cluster, mutation):
    if mutation in {"job_uid", "cron_uid"}:
        (cluster.job if mutation == "job_uid" else cluster.cron)["metadata"]["uid"] = str(uuid4())
    elif mutation == "stale_image":
        cluster.cron["spec"]["jobTemplate"]["spec"]["template"]["spec"]["containers"][0]["image"] = "different"
    elif mutation == "not_pool":
        cluster.cm["data"]["LOOM_EXECUTION_CAPACITY_COLLECTOR_COLLECTION_MODE"] = "environment"
    elif mutation == "installation":
        cluster.cm["metadata"]["labels"][LABEL] = str(uuid4())
    else:
        cluster.cm["immutable"] = False
    assert all(row["role"] != "collector" for row in inspect(cluster)["workloads"])
    assert not any(call[:2] == ("logs", cluster.collector_pod["metadata"]["name"]) for call in cluster.calls)


@pytest.mark.parametrize("raw", ["private log only", 'PrivateError: private-secret',
    '  File "/private/secret.py", line 12, in private\nprivate-payload'])
def test_unsupported_log_content_is_unavailable_not_health(cluster, raw):
    cluster.log = raw
    result = inspect(cluster)
    assert len(result["workloads"]) == 2
    assert all(row["diagnostic"] == {"status": "unavailable"} for row in result["workloads"])
    assert "private" not in json.dumps(result).lower()


def test_log_transport_failure_is_payload_free(cluster):
    cluster.log_error = True
    assert all(row["diagnostic"] == {"status": "unavailable"} for row in inspect(cluster)["workloads"])


def test_replaced_pod_cannot_inherit_old_log_observation(cluster):
    cluster.replace_on_log = True
    manager = next(row for row in inspect(cluster)["workloads"] if row["role"] == "manager")
    assert manager["diagnostic"] == {"status": "unavailable"}


def test_missing_metadata_performs_no_startup_requests(cluster, monkeypatch):
    monkeypatch.delenv("NEBIUS_MANAGEMENT_OPERATION_JSON")
    assert inspect(cluster) == {"status": "not_configured", "workloads": []}
    assert not any(call[0] == "logs" for call in cluster.calls)


def test_startup_requests_are_bounded_even_with_many_stale_candidates(cluster):
    stale = copy.deepcopy(cluster.manager_pod)
    stale["metadata"]["ownerReferences"][0]["uid"] = str(uuid4())
    cluster.lists["pods"] = [copy.deepcopy(stale) for _ in range(100)]
    assert inspect(cluster)["workloads"] == []
    assert sum(call[:2] == ("get", "replicaset") for call in cluster.calls) <= 3
