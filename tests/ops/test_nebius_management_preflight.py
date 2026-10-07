"""Protected installation inventory must be read-only and never export payloads."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml
from scripts.ops import nebius_management_preflight as preflight


class Cluster:
    def __init__(self):
        self.calls = []
        self.config = {"cluster_id": "mk8scluster-test", "namespace": "loom-nebius-platform",
                       "execution_namespace": "loom-nebius-platform-execution", "public_host": "loom.example.test",
                       "execution_node_group_id": "mk8snodegroup-test", "ignored": "private-payload"}
        self.view = {"clusters": [{"name": "cluster-test", "cluster": {
            "server": "https://api.example.test", "certificate-authority-data": "private-ca"}}],
            "users": [{"user": {"token": "private-token"}}]}
        self.config["kubernetes_api_server"] = "https://api.example.test"
        node = {"metadata": {"name": "computeinstance-test", "uid": "node-uid", "labels": {
            "loom.nebius/node-role": "system", "loom.nebius/platform": "integration"}},
            "spec": {"providerID": "nebius://computeinstance-test"},
            "status": {"allocatable": {"cpu": "7900m", "memory": "30000Mi", "ephemeral-storage": "100Gi", "pods": "110"},
                       "conditions": [{"type": "Ready", "status": "True"}]}}
        self.lists = {
            "deployments": [], "cronjobs": [], "roles": [], "rolebindings": [],
            "clusterroles": [], "clusterrolebindings": [],
            "nodes": [node],
            "namespaces": [{"metadata": {"name": "loom-nebius-platform", "uid": "ns-uid"}}],
            "pods": [{"metadata": {"name": "service-1", "namespace": "loom-nebius-platform", "uid": "pod-uid",
                                    "annotations": {"private": "private-annotation"}},
                      "spec": {"nodeName": "computeinstance-test", "containers": [{"name": "service",
                          "env": [{"value": "private-env"}], "resources": {"requests": {"cpu": "500m", "memory": "512Mi"}}}],
                          "initContainers": [{"name": "init", "resources": {"requests": {"cpu": "1"}}}],
                          "overhead": {"memory": "20Mi"}}, "status": {"phase": "Running"}}],
            "services": [{"metadata": {"name": "public", "namespace": "loom-nebius-platform", "uid": "svc-uid"},
                          "spec": {"type": "LoadBalancer"}, "status": {"loadBalancer": {"ingress": [{"ip": "192.0.2.1"}]}}}],
            "ingresses": [], "ingressclasses": [], "persistentvolumeclaims": [],
            "storageclasses": [{"metadata": {"name": "network-ssd", "uid": "sc-uid"},
                                "provisioner": "disk.csi.nebius.ai", "parameters": {"key": "private-storage"}}],
        }

    def get(self, kind, name, namespace):
        self.calls.append(("get", kind, name, namespace))
        assert (kind, name, namespace) == ("configmap", "loom-platform-config", "loom-nebius-platform")
        return {"data": {"environment.json": json.dumps(self.config),
                         "profile.json": json.dumps({"candidate_sha": "a" * 40, "private": "private-profile"}),
                         "keyring.json": "private-keyring"}}

    def run(self, *args, **kwargs):
        self.calls.append(args)
        if args[:2] == ("config", "view"):
            return json.dumps(self.view)
        assert args[0] == "get", "inventory attempted a write"
        return json.dumps({"items": self.lists[args[1]]})


def test_inventory_projects_capacity_and_routes_without_claiming_installation_readiness():
    cluster = Cluster()
    result = preflight.inspect(cluster, namespace="loom-nebius-platform", expected_cluster_id="mk8scluster-test")
    assert result["status"] == "observed"
    # The ConfigMap can advance before workload rollout/migration succeeds.
    assert result["configured_candidate_sha"] == "a" * 40
    assert "running_candidate_correspondence" in result["unverified"]
    assert result["configured_execution_node_group_id"] == "mk8snodegroup-test"
    assert result["nodes"][0]["allocatable"]["memory"] == "30000Mi"
    assert result["pods"][0]["containers"][0]["requests"] == {"cpu": "500m", "memory": "512Mi"}
    assert result["pods"][0]["init_containers"][0]["requests"] == {"cpu": "1"}
    assert result["pods"][0]["overhead"] == {"memory": "20Mi"}
    assert result["ingress_classes"] == []
    assert result["services"][0]["type"] == "LoadBalancer"
    assert result["services"][0]["load_balancer"] == [{"ip": "192.0.2.1"}]
    assert "provider_iam" in result["unverified"] and "wildcard_dns_tls" in result["unverified"]
    assert "private-" not in json.dumps(result)
    assert all(command[0] in {"get", "config"} for command in cluster.calls)
    assert result["ingress_preflight"] == {"status": "not_configured"}
    assert result["controller_inventory"]["controllers"] == []
    assert result["controller_inventory"]["job_write_bindings"] == []
    assert "effective_writer_fencing" in result["controller_inventory"]["unverified"]


@pytest.mark.parametrize("state,expected", [
    ({"waiting": {"reason": "CreateContainerConfigError", "message": "private-message"}},
     {"state": "waiting", "reason": "CreateContainerConfigError"}),
    ({"waiting": {"reason": "private-reason", "message": "private-message"}},
     {"state": "waiting", "reason": "Other"}),
    ({"terminated": {"reason": "OOMKilled", "exitCode": 137, "message": "private-message",
                     "containerID": "private-id"}},
     {"state": "terminated", "reason": "OOMKilled", "exit_code": 137}),
    ({"running": {"startedAt": "private-unchecked"}}, {"state": "running"}),
    ({}, {"state": "unknown"}),
])
def test_inventory_reports_safe_container_states_without_extra_reads(state, expected):
    baseline = Cluster()
    preflight.inspect(baseline, namespace="loom-nebius-platform", expected_cluster_id="mk8scluster-test")
    cluster = Cluster()
    cluster.lists["pods"][0]["status"] = {"phase": "Pending", "message": "private-pod-message",
        "containerStatuses": [{"name": "service", "ready": False, "restartCount": 3,
            "state": state, "lastState": {"terminated": {"reason": "Error", "exitCode": 1,
                "message": "private-last-message"}}, "imageID": "private-image-id"}],
        "initContainerStatuses": [{"name": "init", "ready": True, "restartCount": 0,
            "state": {"terminated": {"reason": "Completed", "exitCode": 0}}, "lastState": {}}]}
    result = preflight.inspect(cluster, namespace="loom-nebius-platform", expected_cluster_id="mk8scluster-test")
    pod = result["pods"][0]
    assert pod["container_statuses"] == [{"name": "service", "ready": False, "restart_count": 3,
        "current": expected, "previous": {"state": "terminated", "reason": "Error", "exit_code": 1}}]
    assert pod["init_container_statuses"] == [{"name": "init", "ready": True, "restart_count": 0,
        "current": {"state": "terminated", "reason": "Completed", "exit_code": 0},
        "previous": {"state": "unknown"}}]
    assert "private-" not in json.dumps(result)
    assert cluster.calls == baseline.calls


def test_inventory_does_not_treat_missing_or_malformed_container_status_as_healthy():
    cluster = Cluster()
    baseline = preflight.inspect(cluster, namespace="loom-nebius-platform", expected_cluster_id="mk8scluster-test")
    assert baseline["pods"][0]["container_statuses"] == []
    assert baseline["pods"][0]["init_container_statuses"] == []
    cluster.lists["pods"][0]["status"]["containerStatuses"] = [{"name": "service",
        "ready": "private-ready", "restartCount": "private-count",
        "state": {"terminated": {"reason": "private-reason", "exitCode": "private-exit"}}}]
    result = preflight.inspect(cluster, namespace="loom-nebius-platform", expected_cluster_id="mk8scluster-test")
    assert result["pods"][0]["container_statuses"] == [{"name": "service", "ready": None,
        "restart_count": None, "current": {"state": "terminated", "reason": "Other"},
        "previous": {"state": "unknown"}}]
    assert "private-" not in json.dumps(result)


@pytest.mark.parametrize("case,changes", [
    ("native", {}),
    ("owned", {"no_owner_references": False}),
    ("extra_selector", {"native_selector_exact": False}),
    ("legacy", {"native_selector_exact": False, "native_selector_required": False,
                "legacy_selector_required": True}),
    ("missing_selector", {"native_selector_exact": False, "native_selector_required": False}),
    ("headless", {"cluster_ip_usable": False}),
    ("tcp_only", {"udp_53": False}),
    ("deleting", {"not_deleting": False}),
])
def test_inventory_reports_dns_binding_checks_without_payloads_or_new_requests(case, changes):
    baseline = Cluster()
    preflight.inspect(baseline, namespace="loom-nebius-platform", expected_cluster_id="mk8scluster-test")
    cluster = Cluster()
    dns = {"metadata": {"name": "coredns", "namespace": "kube-system", "uid": "dns-uid",
                        "annotations": {"private": "private-dns-annotation"}},
           "spec": {"type": "ClusterIP", "clusterIP": "10.43.0.10", "selector": {"k8s-app": "coredns"},
                    "ports": [{"protocol": "TCP", "port": 53, "targetPort": "dns-tcp"},
                              {"protocol": "UDP", "port": 53, "targetPort": "dns-udp"}]}}
    if case == "owned":
        dns["metadata"]["ownerReferences"] = [{"name": "private-addon-owner", "uid": "private-owner-uid"}]
    elif case == "extra_selector":
        dns["spec"]["selector"]["private-key"] = "private-selector-value"
    elif case == "legacy":
        dns["spec"]["selector"] = {"k8s-app": "kube-dns"}
    elif case == "missing_selector":
        dns["spec"].pop("selector")
    elif case == "headless":
        dns["spec"]["clusterIP"] = "None"
    elif case == "tcp_only":
        dns["spec"]["ports"].pop()
    elif case == "deleting":
        dns["metadata"]["deletionTimestamp"] = "2026-09-29T00:00:00Z"
    cluster.lists["services"].append(dns)
    result = preflight.inspect(cluster, namespace="loom-nebius-platform", expected_cluster_id="mk8scluster-test")
    observed = result["services"][1]
    assert observed["uid"] == "dns-uid"
    assert observed["dns_checks"] == {"not_deleting": True, "no_owner_references": True,
        "native_selector_exact": True, "native_selector_required": True, "legacy_selector_required": False,
        "cluster_ip_usable": True, "tcp_53": True, "udp_53": True, **changes}
    assert "dns_checks" not in result["services"][0]
    assert "private-" not in json.dumps(result)
    assert cluster.calls == baseline.calls


def test_inspection_connects_optional_binding_without_exporting_it(monkeypatch):
    monkeypatch.setenv("NEBIUS_INGRESS_INSTALLATION_JSON", '{"private":"invalid-binding"}')
    result = preflight.inspect(Cluster(), namespace="loom-nebius-platform", expected_cluster_id="mk8scluster-test")
    assert result["status"] == "observed"
    assert result["ingress_preflight"] == {"status": "blocked", "phase": "binding", "checks": {},
                                           "reason": "validation_failed"}
    assert "private" not in json.dumps(result)


@pytest.mark.parametrize("parameters,expected,complete", [
    ({}, {}, True),
    ({"type": "NETWORK_SSD"}, {"type": "NETWORK_SSD"}, True),
    ({"type": "NETWORK_SSD", "csi.storage.k8s.io/fstype": "ext4"},
     {"type": "NETWORK_SSD", "csi.storage.k8s.io/fstype": "ext4"}, True),
    ({"type": "NETWORK_SSD_IO_M3", "csi.storage.k8s.io/fstype": "xfs"},
     {"type": "NETWORK_SSD_IO_M3", "csi.storage.k8s.io/fstype": "xfs"}, True),
    ({"type": "NETWORK_SSD", "private-key": "private-storage"}, {"type": "NETWORK_SSD"}, False),
    ({"type": "private-storage"}, {}, False),
    ({"csi.storage.k8s.io/fstype": "private-storage"}, {}, False),
    ({"type": ["private-storage"]}, {}, False),
    (None, {}, False),
    (["private-storage"], {}, False),
])
def test_storage_parameters_observe_only_public_enums_without_hiding_redactions(parameters, expected, complete):
    cluster = Cluster()
    cluster.lists["storageclasses"][0].update(provisioner="compute.csi.nebius.com", parameters=parameters)
    result = preflight.inspect(cluster, namespace="loom-nebius-platform", expected_cluster_id="mk8scluster-test")
    storage = result["storage_classes"][0]
    assert storage["parameters"] == expected
    assert storage["parameters_complete"] is complete
    assert "private-" not in json.dumps(result)
    assert all(command[0] in {"get", "config"} for command in cluster.calls)


@pytest.mark.parametrize("provisioner,complete", [("compute.csi.nebius.com", True), ("foreign.example", False)])
def test_missing_storage_parameters_are_complete_only_for_known_provisioner(provisioner, complete):
    cluster = Cluster()
    storage = cluster.lists["storageclasses"][0]
    storage.pop("parameters")
    storage["provisioner"] = provisioner
    result = preflight.inspect(cluster, namespace="loom-nebius-platform", expected_cluster_id="mk8scluster-test")
    assert result["storage_classes"][0]["parameters"] == {}
    assert result["storage_classes"][0]["parameters_complete"] is complete


@pytest.mark.parametrize("mutation", ["wrong_cluster", "wrong_namespace", "wrong_server", "insecure", "non_nebius", "no_system"])
def test_wrong_target_stops_before_cluster_inventory(mutation):
    cluster = Cluster()
    if mutation == "wrong_cluster":
        cluster.config["cluster_id"] = "mk8scluster-other"
    elif mutation == "wrong_namespace":
        cluster.config["namespace"] = "other"
    elif mutation == "wrong_server":
        cluster.view["clusters"][0]["cluster"]["server"] = "https://wrong.example.test"
    elif mutation == "insecure":
        cluster.view["clusters"][0]["cluster"]["insecure-skip-tls-verify"] = True
    elif mutation == "non_nebius":
        cluster.lists["nodes"][0]["spec"]["providerID"] = "foreign://node"
    else:
        cluster.lists["nodes"] = []
    with pytest.raises(preflight.DeploymentError):
        preflight.inspect(cluster, namespace="loom-nebius-platform", expected_cluster_id="mk8scluster-test")
    assert not any(command[:2] == ("get", "pods") for command in cluster.calls)


@pytest.mark.parametrize("resource", ["pods", "ingresses", "storageclasses"])
def test_incomplete_inventory_is_never_reported_as_empty(resource):
    class Unreadable(Cluster):
        def run(self, *args, **kwargs):
            if args[:2] == ("get", resource):
                return json.dumps({"kind": "Status", "message": "private-diagnostic"})
            return super().run(*args, **kwargs)

    with pytest.raises(preflight.DeploymentError, match="inventory"):
        preflight.inspect(Unreadable(), namespace="loom-nebius-platform", expected_cluster_id="mk8scluster-test")


def test_command_preserves_sanitized_failure_evidence(monkeypatch, tmp_path, capsys):
    class Broken(Cluster):
        def get(self, *args):
            raise RuntimeError("private-provider-secret")

    monkeypatch.setattr(preflight, "Kubectl", lambda path: Broken())
    monkeypatch.setattr("sys.argv", ["preflight", "--kubeconfig", "protected-kubeconfig", "--expected-cluster-id",
                                   "mk8scluster-test", "--evidence-dir", str(tmp_path)])
    assert preflight.main() == 1
    evidence = (tmp_path / "management-preflight.json").read_text()
    assert json.loads(evidence)["status"] == "blocked"
    captured = capsys.readouterr()
    assert "private-provider-secret" not in evidence + captured.out + captured.err


def test_protected_manual_inventory_cannot_select_rollout_or_unprotected_environment():
    workflow = yaml.load((Path(__file__).parents[2] / ".github/workflows/nebius-rollout.yml").read_text(), Loader=yaml.BaseLoader)
    assert workflow["on"]["workflow_dispatch"]["inputs"]["operation"]["options"] == [
        "rollout", "recover", "inspect", "certificate", "ingress", "ingress-rollback", "ingress-dns",
        "management-preflight", "management-install",
        "management-diagnostic-preflight", "management-diagnostic-install",
        "management-recovery-preflight", "management-recovery-install",
        "management-refresh-preflight", "management-refresh-install",
        "management-pool-preflight", "management-pool-install", "management-pool-rollback",
        "management-pool-repair-preflight", "management-pool-repair-install", "management-pool-repair-rollback",
        "development-preflight", "development-install",
        "development-management-preflight", "development-management-install",
    ]
    assert "inputs.operation == 'rollout'" in workflow["jobs"]["rollout"]["if"]
    job = workflow["jobs"]["inspect"]
    assert job["environment"]["name"] == "nebius-integration"
    assert job["permissions"] == {"contents": "read"}
    for condition in ("github.repository == 'qianyi-sun/loom'", "github.ref == 'refs/heads/dev'",
                      "github.event_name == 'workflow_dispatch'", "inputs.operation == 'inspect'"):
        assert condition in job["if"]
    commands = "\n".join(step.get("run", "") for step in job["steps"])
    assert "nebius_management_preflight.py" in commands
    assert "nebius_idle_rollout.py" not in commands
    assert "--apply" not in commands
    inspection = next(step for step in job["steps"] if "nebius_management_preflight.py" in step.get("run", ""))
    assert inspection["env"]["NEBIUS_INGRESS_INSTALLATION_JSON"] == "${{ vars.NEBIUS_INGRESS_INSTALLATION_JSON }}"
    assert inspection["env"]["NEBIUS_MANAGEMENT_OPERATION_JSON"] == "${{ vars.NEBIUS_MANAGEMENT_OPERATION_JSON }}"
