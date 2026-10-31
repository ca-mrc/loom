"""The node observer stays on its execution cluster with bounded authority."""
from __future__ import annotations

from copy import deepcopy
from pathlib import Path

import pytest

from loom.nebius_platform_render import build_platform, build_regional_execution
from tests.support.nebius_platform import platform_inputs as platform_inputs
from tests.support.nebius_platform import regional_inputs as regional_inputs

ROOT = Path(__file__).resolve().parents[2]
NAME = "loom-native-resource-observer"


def _all(files):
    return [doc for group in files.values() for doc in group]


def test_primary_observer_is_node_local_and_actor_reads_its_snapshots(platform_inputs):
    config, candidate, profile = platform_inputs
    config["kubernetes_api_server"] = "https://192.0.2.10"
    original = deepcopy(config)
    docs = _all(build_platform(config, candidate, profile, {}, repo_root=ROOT))
    namespace = config["execution_namespace"] + "-observations"
    observer, = [d for d in docs if d["kind"] == "DaemonSet" and d["metadata"]["name"] == NAME]
    assert observer["metadata"]["namespace"] == namespace
    pod = observer["spec"]["template"]["spec"]
    assert pod["nodeSelector"] == {"loom.nebius/node-role": "integration-execution", "loom.nebius/platform": "integration"}
    assert not any(pod.get(key, False) for key in ("hostPID", "hostIPC", "hostNetwork"))
    container, = pod["containers"]
    assert container["command"] == ["python", "-m", "loom_execution_actuator.node_observer"]
    assert container["image"] == candidate["images"]["execution_actuator"]["image_ref"]
    security = container["securityContext"]
    assert security["runAsUser"] == 0 and security["privileged"] is False
    assert security["allowPrivilegeEscalation"] is False and security["readOnlyRootFilesystem"] is True
    assert security["capabilities"] == {"drop": ["ALL"], "add": ["DAC_READ_SEARCH"]}
    assert {v["hostPath"]["path"] for v in pod["volumes"] if "hostPath" in v} == {
        "/run/containerd/containerd.sock", "/var/log/pods", "/var/lib/kubelet/pods"}
    assert all(m["readOnly"] for m in container["volumeMounts"])
    env = {x["name"]: x.get("value") for x in container["env"]}
    assert env["LOOM_RESOURCE_OBSERVER_EXECUTION_NAMESPACE"] == config["execution_namespace"]
    assert env["LOOM_RESOURCE_OBSERVER_NAMESPACE"] == namespace
    actor = next(d for d in docs if d["kind"] == "Deployment" and d["metadata"]["name"] == "loom-execution-actuator")
    assert {x["name"]: x.get("value") for x in actor["spec"]["template"]["spec"]["containers"][0]["env"]}["LOOM_EXECUTION_ACTUATOR_RESOURCE_OBSERVATION_NAMESPACE"] == namespace
    bindings = [d for d in docs if d["kind"] == "RoleBinding" and d["metadata"].get("namespace") == namespace]
    assert any(b["subjects"] == [{"kind": "ServiceAccount", "name": "loom-execution-actuator", "namespace": config["execution_namespace"]}] for b in bindings)
    policy = next(d for d in docs if d["kind"] == "NetworkPolicy" and d["metadata"].get("namespace") == namespace)
    assert policy["spec"]["ingress"] == []
    assert policy["spec"]["egress"] == [{"to": [{"ipBlock": {"cidr": "192.0.2.10/32"}}], "ports": [{"protocol": "TCP", "port": 443}]}]
    assert config == original


def test_regional_observer_never_moves_to_primary_or_uses_primary_sa(regional_inputs):
    config, candidate, profile = regional_inputs
    config["kubernetes_api_server"] = "https://192.0.2.10"
    target = config["regional_execution_targets"][0]
    target["kubernetes_api_server"] = "https://192.0.2.20:6443"
    namespace = target["execution_namespace"] + "-observations"
    primary = _all(build_platform(config, candidate, profile, {}, repo_root=ROOT))
    remote = build_regional_execution(config, candidate, repo_root=ROOT)[target["target_id"]]
    observer, = [d for d in remote if d["kind"] == "DaemonSet"]
    assert observer["metadata"] == {"name": NAME, "namespace": namespace, "labels": {"app.kubernetes.io/name": NAME}}
    assert not [d for d in primary if d["metadata"].get("namespace") == namespace]
    reader = next(d for d in remote if d["kind"] == "RoleBinding" and d["metadata"].get("namespace") == namespace and d["metadata"]["name"].endswith("-reader"))
    assert reader["subjects"] == [{"kind": "User", "apiGroup": "rbac.authorization.k8s.io", "name": target["service_account_ids"]["actuator"]}]
    account = next(d for d in remote if d["kind"] == "ServiceAccount" and d["metadata"].get("namespace") == namespace)
    assert account["metadata"]["name"] == NAME
    producer = next(d for d in remote if d["kind"] == "RoleBinding" and d["metadata"].get("namespace") == namespace and d["metadata"]["name"] == NAME)
    assert producer["subjects"][0]["kind"] == "ServiceAccount"
    actor = next(d for d in primary if d["kind"] == "Deployment" and d["metadata"]["name"] == target["target_id"] + "-actuator")
    env = {x["name"]: x.get("value") for x in actor["spec"]["template"]["spec"]["containers"][0]["env"]}
    assert env["LOOM_EXECUTION_ACTUATOR_RESOURCE_OBSERVATION_NAMESPACE"] == namespace


def test_dns_api_uses_only_exact_cilium_fqdn_and_dns_queries(platform_inputs):
    config, candidate, profile = platform_inputs
    config.pop("resource_observer_api_cidrs", None)
    docs = _all(build_platform(config, candidate, profile, {}, repo_root=ROOT))
    namespace = config["execution_namespace"] + "-observations"
    policy, = [d for d in docs if d["kind"] == "CiliumNetworkPolicy"]
    assert policy["apiVersion"] == "cilium.io/v2" and policy["metadata"]["namespace"] == namespace
    assert policy["spec"]["egress"][0] == {"toFQDNs": [{"matchName": "api.cluster.test"}], "toPorts": [{"ports": [{"port": "443", "protocol": "TCP"}]}]}
    assert policy["spec"]["egress"][1]["toPorts"][0]["rules"] == {"dns": [{"matchName": "api.cluster.test"}]}
    assert policy["spec"]["egress"][1]["toEndpoints"] == [{"matchLabels": {"k8s:io.kubernetes.pod.namespace": "kube-system"}, "matchExpressions": [{"key": "k8s:k8s-app", "operator": "In", "values": ["kube-dns", "coredns"]}]}]
    assert "matchPattern" not in str(policy)
    generic = next(d for d in docs if d["kind"] == "NetworkPolicy" and d["metadata"].get("namespace") == namespace)
    assert generic["spec"]["egress"] == []


def test_observer_cannot_inherit_primary_api_cidrs(regional_inputs):
    config, candidate, _profile = regional_inputs
    target = config["regional_execution_targets"][0]
    config["resource_observer_api_cidrs"] = ["192.0.2.10/32"]
    target.pop("resource_observer_api_cidrs", None)
    remote = build_regional_execution(config, candidate, repo_root=ROOT)[target["target_id"]]
    policy, = [d for d in remote if d["kind"] == "CiliumNetworkPolicy"]
    assert policy["spec"]["egress"][0]["toFQDNs"] == [{"matchName": "api.west.test"}]
    assert "192.0.2.10" not in str(policy)


def test_explicit_unrestricted_api_cidr_is_rejected(platform_inputs):
    from loom.nebius_platform_render import NebiusPlatformError

    config, candidate, profile = platform_inputs
    config["resource_observer_api_cidrs"] = ["0.0.0.0/0"]
    with pytest.raises(NebiusPlatformError, match="unrestricted"):
        build_platform(config, candidate, profile, {}, repo_root=ROOT)


def test_dns_endpoint_with_explicit_api_cidrs_retains_exact_dns_queries(platform_inputs):
    config, candidate, profile = platform_inputs
    config["resource_observer_api_cidrs"] = ["192.0.2.10/32"]
    docs = _all(build_platform(config, candidate, profile, {}, repo_root=ROOT))
    namespace = config["execution_namespace"] + "-observations"
    generic = next(d for d in docs if d["kind"] == "NetworkPolicy" and d["metadata"].get("namespace") == namespace)
    assert generic["spec"]["egress"] == [{"to": [{"ipBlock": {"cidr": "192.0.2.10/32"}}],
        "ports": [{"protocol": "TCP", "port": 443}]}]
    policy, = [d for d in docs if d["kind"] == "CiliumNetworkPolicy"]
    assert len(policy["spec"]["egress"]) == 1
    dns = policy["spec"]["egress"][0]
    assert dns["toPorts"][0]["rules"] == {"dns": [{"matchName": "api.cluster.test"}]}
    assert dns["toEndpoints"] == [{"matchLabels": {"k8s:io.kubernetes.pod.namespace": "kube-system"}, "matchExpressions": [{"key": "k8s:k8s-app", "operator": "In", "values": ["kube-dns", "coredns"]}]}]
    assert not any("toFQDNs" in rule for rule in policy["spec"]["egress"])


@pytest.mark.parametrize("dns_app", ["coredns", "kube-dns"])
def test_dns_policy_matches_managed_dns_pods_without_selecting_other_apps(platform_inputs, dns_app):
    config, candidate, profile = platform_inputs
    docs = _all(build_platform(config, candidate, profile, {}, repo_root=ROOT))
    policy, = [d for d in docs if d["kind"] == "CiliumNetworkPolicy"]
    selectors = [selector for rule in policy["spec"]["egress"] for selector in rule.get("toEndpoints", [])]

    def matches(selector, namespace, app):
        labels = {"k8s:io.kubernetes.pod.namespace": namespace, "k8s:k8s-app": app}
        return (all(labels.get(key) == value for key, value in selector.get("matchLabels", {}).items())
                and all(item["operator"] == "In" and labels.get(item["key"]) in item["values"]
                        for item in selector.get("matchExpressions", [])))

    # Both managed clusters actually use coredns; kube-dns remains compatible.
    assert any(matches(selector, "kube-system", dns_app) for selector in selectors)
    assert not any(matches(selector, "kube-system", "unrelated") for selector in selectors)
    assert not any(matches(selector, "unrelated", dns_app) for selector in selectors)
