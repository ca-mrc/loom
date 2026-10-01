"""Pure child-stack rendering; no cloud creation, enrollment or admission authority.

The caller authenticates owners, verifies candidate publication, reserves physical
names and provisions namespace-local credentials before applying these documents.
This initial managed format deliberately cannot execute tasks or native builds.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from loom.nebius_environment_contract import EnvironmentRegistrationV1, FoundationBinding
from loom.nebius_management_authority import INSTALLATION_LABEL, namespace_binding
from loom.nebius_platform_render import (
    NebiusPlatformError,
    _build_platform,
    _namespace,
    _network_policy,
    _obj,
    _requested_quantity,
    _service,
)


@dataclass(frozen=True)
class PlatformEnvelope:
    """Requested resources including concurrent rollout and maintenance headroom."""

    cpu_millis: int
    memory_mib: int
    storage_mib: int
    ephemeral_storage_mib: int


@dataclass(frozen=True)
class RenderedEnvironment:
    registration: EnvironmentRegistrationV1
    config: dict[str, Any]
    files: dict[str, list[dict[str, Any]]]
    platform_envelope: PlatformEnvelope
    execution_enabled: Literal[False] = False
    provisioning_project_id: str | None = None


def _configuration(
    row: EnvironmentRegistrationV1, foundation: FoundationBinding,
) -> dict[str, Any]:
    config = foundation.platform_config
    if row.desired_state != "active":
        raise NebiusPlatformError("only an active desired registration may render a running stack")
    if row.cluster_id != config["cluster_id"] or row.physical_pool_id != config["execution_node_group_id"]:
        raise NebiusPlatformError("registration is outside the protected cluster/pool")
    if row.public_host.partition(".")[2] != foundation.public_dns_zone:
        raise NebiusPlatformError("host is outside the shared wildcard certificate scope")
    if row.binding_mode == "imported":
        for actual, expected in (
            (row.application_namespace, config["namespace"]),
            (row.execution_namespace, config["execution_namespace"]),
            (row.public_host, config["public_host"]),
            (row.target_id, config["target_id"]),
        ):
            if actual != expected:
                raise NebiusPlatformError("import requires exact existing foundation bindings")
    else:
        if row.public_host != row.slug + "." + foundation.public_dns_zone:
            raise NebiusPlatformError("host differs from protected environment route")
        if foundation.generated_postgres_storage_gi is not None:
            config["postgres_storage_gi"] = foundation.generated_postgres_storage_gi
        config["buckets"] = {
            purpose: f"loom-{row.incarnation.hex}-{purpose}"
            for purpose in ("artifacts", "trajectories", "source", "backup")
        }
    config.update(
        schema_version="loom.nebius-managed-environment.v1",
        registration=row.model_dump(mode="json"),
        namespace=row.application_namespace,
        execution_namespace=row.execution_namespace,
        environment=row.kind,
        target_id=row.target_id,
        public_host=row.public_host,
        public_tls_bootstrap=False,
    )
    config["capacity_policy"]["enabled"] = False
    config["capacity_policy"]["reason"] = "Managed execution awaits shared admission and write enforcement"
    # Builder settings include write authority and are activated only with the
    # shared gateway. They cannot be inherited from the standalone installation.
    config.pop("task_image_builder", None)
    # Standalone root-task admission is bound to its target and namespace.
    # A closed child keeps restricted PSS, not another target's policy.
    config.pop("task_identity_policy", None)
    config.pop("shared_ingress_enabled", None)
    return config


def _closed_execution(row: EnvironmentRegistrationV1) -> list[dict[str, Any]]:
    docs = []
    for ns in (row.execution_namespace, row.build_namespace):
        docs.append(_network_policy("default-deny", ns, {}, [], []))
        docs.append(_obj("ResourceQuota", "loom-execution-disabled", ns, {"hard": {"pods": "0"}}))
        account = _obj("ServiceAccount", "loom-execution-actuator", ns)
        account["automountServiceAccountToken"] = False
        docs.append(account)
        role = _obj("Role", "loom-execution-observer", ns, api="rbac.authorization.k8s.io/v1")
        role["rules"] = [
            {"apiGroups": ["batch"], "resources": ["jobs"], "verbs": ["get", "list", "watch"]},
            {"apiGroups": [""], "resources": ["pods"], "verbs": ["get", "list", "watch"]},
        ]
        binding = _obj("RoleBinding", "loom-execution-observer", ns, api=role["apiVersion"])
        binding["subjects"] = [{"kind": "ServiceAccount", "name": account["metadata"]["name"], "namespace": ns}]
        binding["roleRef"] = {"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": role["metadata"]["name"]}
        docs.extend([role, binding])
    return docs


def _shared_ingress(row: EnvironmentRegistrationV1, foundation: FoundationBinding) -> dict[str, Any]:
    ingress = _obj("Ingress", "loom-web", row.application_namespace, api="networking.k8s.io/v1")
    ingress["spec"] = {
        "ingressClassName": foundation.ingress_class_name,
        # The shared controller owns the default wildcard certificate/key.
        "tls": [{"hosts": [row.public_host]}],
        "rules": [{"host": row.public_host, "http": {"paths": [
            {"path": path, "pathType": "Prefix", "backend": {
                "service": {"name": service, "port": {"number": port}},
            }} for path, service, port in (("/api", "loom-service", 8090), ("/", "loom-web", 8080))
        ]}}],
    }
    return ingress


def _envelope(files: dict[str, list[dict[str, Any]]]) -> PlatformEnvelope:
    totals = dict.fromkeys(("cpu", "memory", "ephemeral-storage"), 0)
    storage = 0
    for docs in files.values():
        for doc in docs:
            kind, spec = doc["kind"], doc.get("spec", {})
            if kind not in {"Deployment", "StatefulSet", "Job", "CronJob"}:
                continue
            count = 1
            if kind in {"Deployment", "StatefulSet"}:
                count = spec["replicas"]
                if kind == "Deployment" and spec["strategy"]["type"] != "Recreate":
                    count += spec["strategy"]["rollingUpdate"]["maxSurge"]
                for claim in spec.get("volumeClaimTemplates", []):
                    storage += spec["replicas"] * _requested_quantity(claim["spec"]["resources"]["requests"]["storage"], "storage")
            if kind == "CronJob":
                spec = spec["jobTemplate"]["spec"]
            pod = spec["template"]["spec"]
            for resource in totals:
                regular = sum(_requested_quantity(c["resources"]["requests"][resource], resource) for c in pod["containers"])
                initial = max((_requested_quantity(c["resources"]["requests"][resource], resource) for c in pod.get("initContainers", [])), default=0)
                totals[resource] += count * max(regular, initial)
    return PlatformEnvelope(totals["cpu"], totals["memory"], storage, totals["ephemeral-storage"])


def render_environment(
    registration: EnvironmentRegistrationV1,
    candidate: dict[str, Any],
    foundation: FoundationBinding,
    *,
    profile: dict[str, Any],
    keyring: dict[str, Any],
    repo_root: Path,
) -> RenderedEnvironment:
    """Render only the registered child, preserving standalone platform behavior."""
    row = registration
    if foundation.namespace_authority is not None and row.binding_mode == "imported":
        raise ValueError("namespace authority cannot adopt imported bindings")
    config = _configuration(row, foundation)
    files = _build_platform(config, candidate, profile, keyring, repo_root=repo_root, execution_enabled=False)
    files["00-namespaces.yaml"] = [_namespace(ns) for ns in row.namespaces]
    if foundation.namespace_authority is not None:
        authority = foundation.namespace_authority
        for doc in files["00-namespaces.yaml"]:
            doc["metadata"]["labels"][INSTALLATION_LABEL] = str(authority.installation_id)
        files["00-namespaces.yaml"].extend(namespace_binding(authority, ns) for ns in row.namespaces)
    files["60-execution.yaml"] = _closed_execution(row)
    files["70-public.yaml"] = [
        _service("loom-web", row.application_namespace, 8080), _shared_ingress(row, foundation),
    ]
    peer = {
        "namespaceSelector": {"matchLabels": {"kubernetes.io/metadata.name": foundation.ingress_namespace}},
        "podSelector": {"matchLabels": {"app.kubernetes.io/name": foundation.ingress_controller_label}},
    }
    network = files["10-config-network.yaml"]
    files["10-config-network.yaml"] = [doc for doc in network if doc["metadata"]["name"] != "public-web"]
    for name, app, port in (("public-web", "loom-web", 8080), ("public-api", "loom-service", 8090)):
        files["10-config-network.yaml"].append(_network_policy(
            name, row.application_namespace, {"matchLabels": {"app": app}},
            [{"from": [peer], "ports": [{"protocol": "TCP", "port": port}]}],
        ))
    files["40-services.yaml"] = [doc for doc in files["40-services.yaml"] if doc["kind"] != "PersistentVolumeClaim"]
    for docs in files.values():
        for doc in docs:
            doc["metadata"].setdefault("labels", {}).update({
                "loom.nebius/environment-id": str(row.environment_id),
                "loom.nebius/incarnation": str(row.incarnation),
            })
            if doc["kind"] == "ConfigMap":
                doc["data"].pop("public-tls.json", None)
            if doc["kind"] not in {"Deployment", "StatefulSet", "Job", "CronJob"}:
                continue
            spec = doc["spec"]
            if doc["kind"] == "CronJob":
                spec = spec["jobTemplate"]["spec"]
            pod = spec["template"]["spec"]
            if doc["metadata"]["name"] == "loom-web":
                pod["containers"] = pod["containers"][:1]
                pod.pop("volumes", None)
            if doc["metadata"]["name"] == "loom-service":
                pod.setdefault("volumes", []).append({"name": "managed-environment", "configMap": {
                    "name": "loom-platform-config", "items": [{"key": "environment.json", "path": "environment.json"}],
                }})
                pod["containers"][0].setdefault("volumeMounts", []).append({
                    "name": "managed-environment", "mountPath": "/var/run/loom-managed", "readOnly": True,
                })
                pod["containers"][0]["env"].append({"name": "LOOM_SVC_MANAGED_ENVIRONMENT_CONFIG_FILE",
                                                    "value": "/var/run/loom-managed/environment.json"})
            for container in pod.get("initContainers", []) + pod["containers"]:
                container["env"] = [env for env in container.get("env", []) if env["name"] not in {
                    "LOOM_GW_LOCAL_YIBU_API_KEY", "LOOM_GW_LOCAL_YIBU_BASE_URL",
                }]
                for env in container.get("env", []):
                    if env["name"] == "LOOM_CP_SERVICE_EXECUTION_SCHEDULER_ENABLED":
                        env["value"] = "false"
                    elif env["name"] == "LOOM_FRONTEND_ENVIRONMENT_LABEL":
                        env["value"] = "Loom " + row.slug
                ephemeral = f"{config['postgres_storage_gi']}Gi" if doc["kind"] == "CronJob" else "256Mi"
                container["resources"]["requests"]["ephemeral-storage"] = ephemeral
                container["resources"]["limits"]["ephemeral-storage"] = ephemeral
    return RenderedEnvironment(row, config, files, _envelope(files),
                               provisioning_project_id=foundation.provisioning_project_id)
