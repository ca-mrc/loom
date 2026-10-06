"""Startup diagnosis cannot expose payloads or confuse stale/current workloads."""
from __future__ import annotations

import copy
import json
import traceback
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


def test_recovered_manager_is_not_a_current_startup_failure(cluster):
    cluster.manager_pod["status"]["containerStatuses"][0].update(ready=True, state={"running": {}})
    assert all(row["role"] != "manager" for row in inspect(cluster)["workloads"])


def test_old_replicaset_with_same_image_but_changed_volume_is_not_current(cluster):
    cluster.manager["spec"]["template"]["spec"]["volumes"] = [{"name": "source", "configMap": {"name": "new-config"}}]
    assert all(row["role"] != "manager" for row in inspect(cluster)["workloads"])


def test_real_python_exception_group_is_reduced_to_fixed_public_symbols(cluster):
    source = compile('raise ExceptionGroup("private-group", [ValueError("private-value"), PermissionError("private-path")])',
        "/app/src/loom_service/application_management/service_runtime.py", "exec")
    try:
        exec(source, {})
    except ExceptionGroup as exc:
        cluster.log = "".join(traceback.format_exception(exc))
    result = inspect(cluster)
    diagnostic = result["workloads"][0]["diagnostic"]
    assert set(diagnostic["errors"]) == {"ExceptionGroup", "ValueError", "PermissionError"}
    assert diagnostic["locations"] == [{"component": "application_runtime", "line": 1}]
    assert "private" not in json.dumps(result)


def test_restart_during_log_read_is_not_attributed_to_observed_instance(cluster, monkeypatch):
    original = cluster.run

    def changed(*args, **kwargs):
        result = original(*args, **kwargs)
        if args[0] == "logs":
            cluster.manager_pod["status"]["containerStatuses"][0]["restartCount"] += 1
        return result

    monkeypatch.setattr(cluster, "run", changed)
    manager = next(row for row in inspect(cluster)["workloads"] if row["role"] == "manager")
    assert manager["diagnostic"] == {"status": "unavailable"}


@pytest.mark.parametrize("mutation", [None, "unexpected_mount", "wrong_projection", "not_requested"])
def test_collector_standard_serviceaccount_projection_only(cluster, mutation):
    templates = [cluster.job["spec"]["template"]["spec"],
        cluster.cron["spec"]["jobTemplate"]["spec"]["template"]["spec"]]
    for spec in templates:
        spec["automountServiceAccountToken"] = mutation != "not_requested"
    pod = cluster.collector_pod["spec"]
    name = "kube-api-access-abcde"
    pod["volumes"] = [{"name": name, "projected": {"defaultMode": 420, "sources": [
        {"serviceAccountToken": {"expirationSeconds": 3607, "path": "token"}},
        {"configMap": {"name": "kube-root-ca.crt", "items": [{"key": "ca.crt", "path": "ca.crt"}]}},
        {"downwardAPI": {"items": [{"path": "namespace", "fieldRef": {"apiVersion": "v1", "fieldPath": "metadata.namespace"}}]}},
    ]}}]
    pod["containers"][0]["volumeMounts"] = [{"name": name, "readOnly": True,
        "mountPath": "/var/run/secrets/kubernetes.io/serviceaccount"}]
    if mutation == "wrong_projection":
        pod["volumes"][0]["projected"]["sources"][1]["configMap"]["name"] = "private-config"
    elif mutation == "unexpected_mount":
        pod["containers"][0]["volumeMounts"][0]["mountPath"] = "/private"
    result = inspect(cluster)
    collector = [row for row in result["workloads"] if row["role"] == "collector"]
    assert bool(collector) == (mutation is None)
    if collector:
        assert collector[0]["diagnostic"]["status"] == "observed"


@pytest.mark.parametrize("role", ["manager", "collector"])
@pytest.mark.parametrize("change", ["annotation", "label", "native_labels"])
def test_template_metadata_separates_old_rollouts_from_native_controller_labels(cluster, role, change):
    parent = cluster.rs if role == "manager" else cluster.job
    root = cluster.manager["spec"] if role == "manager" else cluster.cron["spec"]["jobTemplate"]["spec"]
    if change == "annotation":
        root["template"]["metadata"] = {"annotations": {"kubectl.kubernetes.io/restartedAt": "new-revision"}}
        parent["spec"]["template"]["metadata"] = {"annotations": {"kubectl.kubernetes.io/restartedAt": "old-revision"}}
    elif change == "label":
        root["template"]["metadata"] = {"labels": {"configuration-version": "new"}}
        parent["spec"]["template"]["metadata"] = {"labels": {"configuration-version": "old"}}
    else:
        labels = {"pod-template-hash": "abc123"} if role == "manager" else {
            "batch.kubernetes.io/controller-uid": parent["metadata"]["uid"],
            "controller-uid": parent["metadata"]["uid"], "job-name": parent["metadata"]["name"],
            "batch.kubernetes.io/job-name": parent["metadata"]["name"]}
        parent["spec"]["template"]["metadata"] = {"labels": labels}
    assert any(row["role"] == role for row in inspect(cluster)["workloads"]) == (change == "native_labels")


@pytest.fixture
def gateway(cluster, monkeypatch):
    installation = cluster.manager["metadata"]["labels"][LABEL]
    container = {"name": "gateway", "image": "registry.test/service@sha256:" + "a" * 64,
        "command": ["python", "-m", "loom_service.pool_management"],
        "env": [{"name": "LOOM_POOL_GATEWAY_INSTALLATION_ID", "value": installation}]}
    template = {"spec": {"containers": [container]}}
    root = resource("Deployment", "loom-pool-gateway", MANAGER, {"template": template})
    root["metadata"]["labels"] = {LABEL: installation}
    replica = resource("ReplicaSet", "loom-pool-gateway-abcdefgh", MANAGER, {"template": template}, owner=root)
    pod = resource("Pod", "loom-pool-gateway-abcdefgh-12345", MANAGER, template["spec"], owner=replica)
    pod["status"] = {"phase": "Running", "containerStatuses": [{"name": "gateway", "ready": False,
        "restartCount": 4, "state": {"waiting": {"reason": "CrashLoopBackOff"}},
        "lastState": {"terminated": {"exitCode": 1, "reason": "Error"}}}]}
    get = cluster.get

    def gateway_get(kind, name, namespace):
        for doc in (root, replica, pod):
            if (kind, name, namespace) == (doc["kind"].lower(), doc["metadata"]["name"], doc["metadata"]["namespace"]):
                cluster.calls.append(("get", kind, name, namespace))
                return copy.deepcopy(doc)
        return get(kind, name, namespace)

    monkeypatch.setattr(cluster, "get", gateway_get)
    cluster.lists["pods"].insert(0, pod)
    return root, replica, pod


def test_gateway_failure_is_independent_of_manager_and_exposes_no_payload(cluster, gateway):
    cluster.log = '''Traceback (most recent call last):
  File "/app/src/loom_service/pool_management/__main__.py", line 118, in _run
    private_source(secret)
  File "/app/src/loom_service/environment_management/kubernetes_credentials.py", line 60, in get_token
    private_source(secret)
PermissionError: private-token-path
'''
    result = inspect(cluster)
    assert {row["role"] for row in result["workloads"]} == {"manager", "gateway", "collector"}
    row, = (row for row in result["workloads"] if row["role"] == "gateway")
    assert row["pod_uid"] == gateway[2]["metadata"]["uid"]
    assert row["log_instance"] == "previous"
    assert row["diagnostic"] == {"status": "observed", "errors": ["PermissionError"], "stages": [],
        "locations": [{"component": "pool_gateway_entry", "line": 118},
            {"component": "projected_kubernetes_credentials", "line": 60}]}
    assert "private" not in json.dumps(result)
    assert all(call[0] in {"get", "config", "logs"} for call in cluster.calls)


@pytest.mark.parametrize("damage", ["installation_label", "installation_env", "command", "args", "stale_image",
    "replica_uid", "root_uid", "root_name", "namespace", "ready"])
def test_unbound_gateway_never_reads_logs(cluster, gateway, damage):
    root, replica, pod = gateway
    container = root["spec"]["template"]["spec"]["containers"][0]
    if damage == "installation_label":
        root["metadata"]["labels"][LABEL] = str(uuid4())
    elif damage == "installation_env":
        for doc in (root, replica):
            doc["spec"]["template"]["spec"]["containers"][0]["env"][0]["value"] = str(uuid4())
    elif damage == "command":
        container["command"] = ["python", "-m", "other"]
    elif damage == "args":
        container["args"] = ["private"]
    elif damage == "stale_image":
        container["image"] = "different"
    elif damage == "replica_uid":
        replica["metadata"]["uid"] = str(uuid4())
    elif damage == "root_uid":
        root["metadata"]["uid"] = str(uuid4())
    elif damage == "root_name":
        root["metadata"]["name"] = "foreign"
        replica["metadata"]["ownerReferences"][0]["name"] = "foreign"
    elif damage == "namespace":
        pod["metadata"]["namespace"] = EXECUTION
    else:
        pod["status"]["containerStatuses"][0].update(ready=True, state={"running": {}})
    assert not any(row["role"] == "gateway" for row in inspect(cluster)["workloads"])
    assert not any(call[:2] == ("logs", pod["metadata"]["name"]) for call in cluster.calls)


def test_fixed_collector_and_gateway_error_types_are_retained_without_messages(cluster):
    cluster.log = "| loom_execution_capacity_collector.kubernetes.KubernetesObservationError: private-pod\n" \
        "SchemaNotAtHeadError: private-database\n" \
        "ValueError: pool_gateway_identity_unavailable\n"
    diagnostic = inspect(cluster)["workloads"][0]["diagnostic"]
    assert diagnostic == {"status": "observed", "errors": ["KubernetesObservationError", "SchemaNotAtHeadError", "ValueError"],
        "stages": ["pool_gateway_identity_unavailable"], "locations": []}
