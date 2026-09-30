"""Fixed registration Job; its protected caller owns candidate and namespace proof."""
from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from loom.nebius_platform_render import _env, _mount_secret, _obj, _secret_env
from loom_service.environment_management.kubernetes_credentials import ProjectedKubernetesConnection
from loom_service.pool_management.installation import PoolInstallation


def _runtime(spec: PoolInstallation, namespace: str, service_image: str) -> PoolInstallation:
    spec = PoolInstallation.model_validate(spec.model_dump())
    if (re.fullmatch(r"loom-nebius-management(?:-[a-z0-9]+(?:-[a-z0-9]+)*)?", namespace) is None
            or len(namespace) > 53 or re.fullmatch(r".+@sha256:[0-9a-f]{64}", service_image) is None):
        raise ValueError("unqualified pool registration runtime")
    return spec


def render_registration(spec: PoolInstallation, *, namespace: str, service_image: str) -> tuple[dict[str, Any], dict[str, Any]]:
    spec = _runtime(spec, namespace, service_image)
    name = "loom-pool-registration-" + spec.operation_id.hex[:12]
    labels = {"loom.nebius/management-installation": str(spec.installation_id),
        "loom.nebius/pool-operation": str(spec.operation_id)}
    config = _obj("ConfigMap", name, namespace)
    config.update(immutable=True, data={"installation.json": spec.model_dump_json()})
    job = _obj("Job", name, namespace, api="batch/v1")
    container = {"name": "register", "image": service_image,
        "command": ["python", "-m", "loom_service.pool_management.installation"],
        "env": [*_env({"LOOM_POOL_INSTALLATION_FILE": "/var/run/loom-pool-installation/installation.json"}),
            _secret_env("LOOM_POOL_INSTALLATION_DB_URL", "loom-platform-db", "admin-url")],
        "volumeMounts": [{"name": "installation", "mountPath": "/var/run/loom-pool-installation", "readOnly": True}],
        "securityContext": {"allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True,
            "capabilities": {"drop": ["ALL"]}},
        "resources": {"requests": {"cpu": "100m", "memory": "256Mi", "ephemeral-storage": "64Mi"},
            "limits": {"cpu": "1", "memory": "512Mi", "ephemeral-storage": "128Mi"}}}
    pod = {"restartPolicy": "Never", "automountServiceAccountToken": False,
        "securityContext": {"runAsNonRoot": True, "runAsUser": 1000, "runAsGroup": 1000, "fsGroup": 1000,
            "seccompProfile": {"type": "RuntimeDefault"}},
        "containers": [container], "volumes": [{"name": "installation", "configMap": {"name": name}}],
        "nodeSelector": {"loom.nebius/node-role": "system", "loom.nebius/platform": "integration"},
        "tolerations": [{"key": "loom.nebius/platform", "operator": "Equal", "value": "integration", "effect": "NoSchedule"}]}
    _mount_secret(pod, "db-ca", "loom-platform-db", "/var/run/loom-db", ca_only=True)
    job["spec"] = {"backoffLimit": 0, "activeDeadlineSeconds": 120, "template": {"metadata": {"labels": labels}, "spec": pod}}
    for resource in (config, job):
        resource["metadata"]["labels"] = labels.copy()
    return config, job


def render_gateway(spec: PoolInstallation, *, namespace: str, service_image: str,
                   kubernetes_endpoint: str) -> dict[str, tuple[dict[str, Any], ...]]:
    """Separate disabled workload, configuration and authority migration phases.

    Rendering installs nothing. The parent must retire legacy writers before
    staging authority, qualify the closed gateway and only then open admission.
    Dedicated machine Secret delivery is a separate hash-qualified private stage.
    """
    spec = _runtime(spec, namespace, service_image)
    connection = ProjectedKubernetesConnection(kind="projected_service_account", endpoint=kubernetes_endpoint,
        ca_file=Path("/var/run/loom-pool-kubernetes/ca.crt"), token_file=Path("/var/run/loom-pool-kubernetes/token"))
    machine, = (row for row in spec.machines if row.role == "gateway")
    name = "loom-pool-gateway"
    account = _obj("ServiceAccount", name, namespace)
    account["automountServiceAccountToken"] = False
    catalog = _obj("ConfigMap", "loom-pool-profiles-" + spec.operation_id.hex, namespace)
    catalog.update(immutable=True, data={"profiles.json": spec.profiles.model_dump_json()})
    security = {"allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True, "capabilities": {"drop": ["ALL"]}}
    token_path = "/var/run/loom-pool-token/token"
    container = {"name": "gateway", "image": service_image, "command": ["python", "-m", "loom_service.pool_management"],
        "env": [*_env({"LOOM_POOL_GATEWAY_POOL_ID": spec.pool_id, "LOOM_POOL_GATEWAY_INSTALLATION_ID": spec.installation_id,
            "LOOM_POOL_GATEWAY_MACHINE_ID": machine.machine_id, "LOOM_POOL_GATEWAY_ADMISSION_EPOCH": spec.admission_epoch,
            "LOOM_POOL_GATEWAY_BEARER_TOKEN_FILE": token_path, "LOOM_POOL_GATEWAY_KUBERNETES": connection.model_dump_json()}),
            _secret_env("LOOM_POOL_GATEWAY_DB_URL", "loom-platform-db", "service-url")],
        "securityContext": security, "volumeMounts": [
            {"name": "pool-token", "mountPath": "/var/run/loom-pool-token", "readOnly": True},
            {"name": "pool-kubernetes", "mountPath": "/var/run/loom-pool-kubernetes", "readOnly": True}],
        "ports": [{"name": "health", "containerPort": 9120}],
        "readinessProbe": {"httpGet": {"path": "/readyz", "port": "health"}},
        "livenessProbe": {"httpGet": {"path": "/healthz", "port": "health"}},
        "resources": {"requests": {"cpu": "100m", "memory": "128Mi"}, "limits": {"cpu": "1", "memory": "512Mi"}}}
    pod = {"serviceAccountName": name, "automountServiceAccountToken": False,
        "securityContext": {"runAsNonRoot": True, "runAsUser": 1000, "runAsGroup": 1000, "fsGroup": 1000,
            "seccompProfile": {"type": "RuntimeDefault"}}, "containers": [container],
        "initContainers": [{"name": "prepare-pool-token", "image": service_image,
            "command": ["python", "-c", "import sys; from pathlib import Path; "
                "p=Path(sys.argv[2]); p.write_bytes(Path(sys.argv[1]).read_bytes()); p.chmod(0o600)",
                "/var/run/loom-pool-token-source/token", token_path],
            "securityContext": security,
            "resources": {"requests": {"cpu": "10m", "memory": "32Mi"}, "limits": {"cpu": "100m", "memory": "64Mi"}},
            "volumeMounts": [{"name": "pool-token-source", "mountPath": "/var/run/loom-pool-token-source", "readOnly": True},
                {"name": "pool-token", "mountPath": "/var/run/loom-pool-token"}]}],
        "volumes": [{"name": "pool-token-source", "secret": {"secretName": "loom-pool-machine-" + machine.machine_id.hex,
            "defaultMode": 0o440, "items": [{"key": "token", "path": "token"}]}},
            {"name": "pool-token", "emptyDir": {"medium": "Memory", "sizeLimit": "1Mi"}},
            {"name": "pool-kubernetes", "projected": {"defaultMode": 0o440, "sources": [
                {"serviceAccountToken": {"path": "token", "expirationSeconds": 3600}},
                {"configMap": {"name": "kube-root-ca.crt", "items": [{"key": "ca.crt", "path": "ca.crt"}]}}]}}],
        "nodeSelector": {"loom.nebius/node-role": "system", "loom.nebius/platform": "integration"},
        "tolerations": [{"key": "loom.nebius/platform", "operator": "Equal", "value": "integration", "effect": "NoSchedule"}]}
    _mount_secret(pod, "db-ca", "loom-platform-db", "/var/run/loom-db", ca_only=True)
    labels = {"app.kubernetes.io/name": name}
    deployment = _obj("Deployment", name, namespace, api="apps/v1")
    deployment["spec"] = {"replicas": 0, "strategy": {"type": "Recreate"}, "selector": {"matchLabels": labels.copy()},
        "template": {"metadata": {"labels": labels.copy(), "annotations": {"loom.nebius/pool-operation": str(spec.operation_id)}}, "spec": pod}}
    namespaces = sorted({ns.name for row in spec.participants for ns in (row.execution_namespace, row.build_namespace)})
    subject = {"kind": "ServiceAccount", "name": name, "namespace": namespace}
    role_name = "loom-pool-" + spec.pool_id.hex
    authority: list[dict[str, Any]] = []
    cluster_role = _obj("ClusterRole", role_name, None, api="rbac.authorization.k8s.io/v1")
    cluster_role["rules"] = [{"apiGroups": [""], "resources": ["namespaces"], "verbs": ["get"], "resourceNames": namespaces}]
    cluster_binding = _obj("ClusterRoleBinding", role_name, None, api="rbac.authorization.k8s.io/v1")
    cluster_binding.update(subjects=[subject.copy()], roleRef={"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": role_name})
    authority.extend((cluster_role, cluster_binding))
    for destination in namespaces:
        role = _obj("Role", role_name, destination, api="rbac.authorization.k8s.io/v1")
        role["rules"] = [{"apiGroups": ["batch"], "resources": ["jobs"], "verbs": ["get", "create", "delete"]},
            {"apiGroups": [""], "resources": ["configmaps"], "verbs": ["get", "create", "delete"]},
            {"apiGroups": [""], "resources": ["pods"], "verbs": ["get", "list", "delete"]}]
        binding = _obj("RoleBinding", role_name, destination, api="rbac.authorization.k8s.io/v1")
        binding.update(subjects=[subject.copy()], roleRef={"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": role_name})
        authority.extend((role, binding))
    result = {"configuration": (account, catalog), "authority": tuple(authority), "workload": (deployment,)}
    for resources in result.values():
        for doc in resources:
            doc["metadata"]["labels"] = {"loom.nebius/management-installation": str(spec.installation_id),
                "loom.nebius/pool": str(spec.pool_id), "loom.nebius/pool-operation": str(spec.operation_id)}
    return result
