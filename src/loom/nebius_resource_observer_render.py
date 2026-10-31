"""Render the explicitly trusted node stats reader and API-backed snapshots."""
from __future__ import annotations

import re
from ipaddress import ip_address, ip_network
from typing import Any
from urllib.parse import urlsplit

NAME = "loom-native-resource-observer"
LABELS = {"app.kubernetes.io/name": NAME}


def observation_namespace(execution_namespace: str) -> str:
    name = execution_namespace + "-observations"
    if len(name) > 63:
        raise ValueError("execution namespace leaves no room for resource observations")
    return name


def api_egress(config: dict[str, Any]) -> list[dict[str, Any]]:
    endpoint = urlsplit(config["kubernetes_api_server"])
    port = endpoint.port or 443
    if not 1 <= port <= 65535:
        raise ValueError("resource observer requires a valid Kubernetes API port")
    configured = config.get("resource_observer_api_cidrs")
    if configured is None:
        try:
            address = ip_address(endpoint.hostname or "")
        except ValueError:
            hostname = endpoint.hostname or ""
            if len(hostname) > 253 or not re.fullmatch(r"[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?", hostname):
                raise ValueError("resource observer requires an exact API DNS name") from None
            return []
        configured = [str(ip_network(address))]
    if not isinstance(configured, list) or not 1 <= len(configured) <= 16:
        raise ValueError("resource_observer_api_cidrs requires bounded explicit API networks")
    networks = []
    for value in configured:
        if not isinstance(value, str):
            raise ValueError("resource_observer_api_cidrs requires explicit API networks")
        try:
            network = ip_network(value, strict=True)
        except ValueError:
            raise ValueError("resource_observer_api_cidrs requires canonical API networks") from None
        if network.prefixlen == 0 or network.is_multicast or network.is_loopback or network.is_link_local:
            raise ValueError("resource_observer_api_cidrs cannot allow unrestricted or local networks")
        if str(network) not in networks:
            networks.append(str(network))
    return [{"to": [{"ipBlock": {"cidr": value}} for value in networks],
             "ports": [{"protocol": "TCP", "port": port}]}]


def observer_documents(config: dict[str, Any], image: str, *,
        reader_subject: dict[str, str] | None = None) -> list[dict[str, Any]]:
    """No inbound service and no runtime authority in workload containers.

    The observer alone is trusted with the CRI socket. A read-only socket mount
    does not make the CRI protocol read-only; its typed helper exposes stats only.
    DAC_READ_SEARCH permits metadata scans of private workload directories; all
    other capabilities are dropped and host filesystem mounts remain read-only.
    """
    execution = config["execution_namespace"]
    namespace = observation_namespace(execution)

    def obj(kind: str, name: str, ns: str | None = namespace, **values: Any) -> dict[str, Any]:
        api = "apps/v1" if kind == "DaemonSet" else (
            "rbac.authorization.k8s.io/v1" if kind in {"Role", "RoleBinding", "ClusterRole", "ClusterRoleBinding"}
            else "networking.k8s.io/v1" if kind == "NetworkPolicy" else "v1")
        metadata: dict[str, Any] = {"name": name, "labels": dict(LABELS)}
        if ns is not None:
            metadata["namespace"] = ns
        return {"apiVersion": api, "kind": kind, "metadata": metadata, **values}

    subject = {"kind": "ServiceAccount", "name": NAME, "namespace": namespace}
    reader_subject = reader_subject or {
        "kind": "ServiceAccount", "name": "loom-execution-actuator", "namespace": execution}

    def binding(kind: str, name: str, role: str, ns: str | None, who: dict[str, str]) -> dict[str, Any]:
        return obj(kind, name, ns, roleRef={"apiGroup": "rbac.authorization.k8s.io",
                   "kind": "ClusterRole" if kind == "ClusterRoleBinding" else "Role", "name": role},
                   subjects=[who])

    space = obj("Namespace", namespace, None)
    # HostPath is deliberately isolated from the ordinary restricted task namespace.
    space["metadata"]["labels"].update({"loom.nebius/platform": "true",
                                        "pod-security.kubernetes.io/enforce": "privileged"})
    account = obj("ServiceAccount", NAME, automountServiceAccountToken=True)
    node_role = namespace + "-nodes"
    docs = [space, account,
        obj("ClusterRole", node_role, None, rules=[{"apiGroups": [""], "resources": ["nodes"], "verbs": ["get"]}]),
        binding("ClusterRoleBinding", node_role, node_role, None, subject),
        obj("Role", NAME, execution, rules=[{"apiGroups": [""], "resources": ["pods"], "verbs": ["get", "list"]}]),
        binding("RoleBinding", NAME, NAME, execution, subject),
        obj("Role", NAME, rules=[{"apiGroups": [""], "resources": ["configmaps"], "verbs": ["create", "get", "update"]},
                              {"apiGroups": [""], "resources": ["pods"], "verbs": ["get"]}]),
        binding("RoleBinding", NAME, NAME, namespace, subject),
        obj("Role", NAME + "-reader", rules=[{"apiGroups": [""], "resources": ["configmaps", "pods"], "verbs": ["get"]}]),
        binding("RoleBinding", NAME + "-reader", NAME + "-reader", namespace, reader_subject),
    ]
    env: list[dict[str, Any]] = [{"name": "LOOM_RESOURCE_OBSERVER_" + key, "value": value} for key, value in {
        "EXECUTION_NAMESPACE": execution, "NAMESPACE": namespace,
        "KUBERNETES_ENDPOINT": config["kubernetes_api_server"],
        "INTERVAL_SECONDS": "15", "MAX_PODS": "128",
        "HELPER_PATH": "/usr/local/bin/loom-node-resource-stats",
        "RUNTIME_ENDPOINT": "unix:///var/run/loom-node/containerd.sock",
    }.items()]
    env.extend({"name": "LOOM_RESOURCE_OBSERVER_" + key,
                "valueFrom": {"fieldRef": {"fieldPath": path}}} for key, path in {
        "NODE_NAME": "spec.nodeName", "POD_NAME": "metadata.name", "POD_UID": "metadata.uid"}.items())
    docs.append(obj("DaemonSet", NAME, spec={
        "selector": {"matchLabels": dict(LABELS)},
        "template": {"metadata": {"labels": dict(LABELS)}, "spec": {
            "serviceAccountName": NAME, "automountServiceAccountToken": True,
            "nodeSelector": {"loom.nebius/node-role": "integration-execution", "loom.nebius/platform": "integration"},
            "tolerations": [{"key": "loom.nebius/execution", "operator": "Equal", "value": "true", "effect": "NoSchedule"},
                            {"key": "loom.nebius/platform", "operator": "Equal", "value": "integration", "effect": "NoSchedule"}],
            "securityContext": {"seccompProfile": {"type": "RuntimeDefault"}},
            "dnsConfig": {"options": [{"name": "ndots", "value": "1"}]},
            "containers": [{"name": "observer", "image": image,
                "command": ["python", "-m", "loom_execution_actuator.node_observer"], "env": env,
                "securityContext": {"runAsUser": 0, "runAsGroup": 0, "privileged": False,
                    "allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True, "capabilities": {"drop": ["ALL"], "add": ["DAC_READ_SEARCH"]}},
                "resources": {"requests": {"cpu": "50m", "memory": "64Mi"}, "limits": {"cpu": "500m", "memory": "256Mi"}},
                "volumeMounts": [{"name": "runtime", "mountPath": "/var/run/loom-node/containerd.sock", "readOnly": True},
                    {"name": "logs", "mountPath": "/var/log/pods", "readOnly": True},
                    {"name": "volumes", "mountPath": "/var/lib/kubelet/pods", "readOnly": True}],
            }],
            "volumes": [{"name": "runtime", "hostPath": {"path": "/run/containerd/containerd.sock", "type": "Socket"}},
                        {"name": "logs", "hostPath": {"path": "/var/log/pods", "type": "Directory"}},
                        {"name": "volumes", "hostPath": {"path": "/var/lib/kubelet/pods", "type": "Directory"}}],
        }},
    }))
    network_egress = api_egress(config)
    docs.append(obj("NetworkPolicy", NAME, spec={"podSelector": {"matchLabels": dict(LABELS)},
                "policyTypes": ["Ingress", "Egress"], "ingress": [], "egress": network_egress}))
    try:
        ip_address(urlsplit(config["kubernetes_api_server"]).hostname or "")
    except ValueError:
        hostname = urlsplit(config["kubernetes_api_server"]).hostname
        port = urlsplit(config["kubernetes_api_server"]).port or 443
        docs.append({"apiVersion": "cilium.io/v2", "kind": "CiliumNetworkPolicy",
            "metadata": {"name": NAME, "namespace": namespace, "labels": dict(LABELS)},
            "spec": {"endpointSelector": {"matchLabels": dict(LABELS)}, "egress": [
                *([] if network_egress else [{"toFQDNs": [{"matchName": hostname}],
                 "toPorts": [{"ports": [{"port": str(port), "protocol": "TCP"}]}]}]),
                {"toEndpoints": [{"matchLabels": {"k8s:io.kubernetes.pod.namespace": "kube-system",
                                                    "k8s:k8s-app": "kube-dns"}}],
                 "toPorts": [{"ports": [{"port": "53", "protocol": "ANY"}],
                              "rules": {"dns": [{"matchName": hostname}]}}]},
            ]}})
    return docs
