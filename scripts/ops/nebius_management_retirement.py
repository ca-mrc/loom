"""Fixed, data-retaining legacy cleanup through existing protected stage journals."""
from __future__ import annotations

import copy
import json
from collections.abc import Callable
from contextlib import AbstractContextManager
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_application_setup import _setup_defaulted
from scripts.ops.nebius_ingress_stage import _key, _snapshot, _uid
from scripts.ops.nebius_management_material import ManagementBinding
from scripts.ops.nebius_management_stage import (
    ManagementStageAPI,
    ManagementStageError,
    _stage_fixed_documents,
    _validate_record,
)

from loom.nebius_platform_render import digest
from loom_service.environment_management.deployment import ManagementDeployment, render_management
from loom_service.environment_management.retirement import RetirementSettings, RetirementTarget


@dataclass(frozen=True)
class RetirementInstallRequest:
    binding: ManagementBinding
    deployment: ManagementDeployment
    candidate: dict[str, Any]
    profile: dict[str, Any]
    targets: tuple[RetirementTarget, ...]
    repo_root: Path


def retirement_documents(request: RetirementInstallRequest) -> dict[str, dict[str, dict[str, Any]]]:
    """No caller-supplied manifests; namespace permissions derive from exact targets."""
    deployment, binding = request.deployment, request.binding
    config = deployment.installation.foundation.platform_config
    namespaces = [name for target in request.targets for name in target.namespace_uids]
    if (deployment.installation.applications is None or deployment.installation.provider_runtime is not None
            or (str(deployment.installation_id), deployment.namespace) != (binding.installation_id, binding.namespace)
            or not 1 <= len(request.targets) <= 16 or len(namespaces) != len(set(namespaces))
            or binding.namespace in namespaces or "loom-nebius-platform" in namespaces
            or any(target.registration.cluster_id != config["cluster_id"] for target in request.targets)):
        raise ManagementStageError("retirement binding differs")
    # Validate model copies as well as decoded protected inputs.
    targets = [RetirementTarget.model_validate(target.model_dump(mode="json")) for target in request.targets]
    rendered = render_management(deployment, candidate=request.candidate, profile=request.profile, repo_root=request.repo_root)
    settings = {"schema_version": "loom.nebius-retirement.v1", "namespace": binding.namespace, "kubernetes": {
        "kind": "projected_service_account", "endpoint": config["kubernetes_api_server"].rstrip("/"),
        "ca_file": "/var/run/loom-retirement-kubernetes/ca.crt", "token_file": "/var/run/loom-retirement-kubernetes/token",
    }, "targets": [target.model_dump(mode="json") for target in targets]}
    validated = RetirementSettings.model_validate(settings)
    name = "loom-retirement-" + digest({"binding": asdict(binding), "settings": settings, "runtime": rendered.revision})[7:19]
    ns = binding.namespace
    labels = {"loom.nebius/management-installation": binding.installation_id, "loom.nebius/retirement": name}

    def obj(kind: str, *, namespace: str | None = ns, api: str = "v1") -> dict[str, Any]:
        return {"apiVersion": api, "kind": kind, "metadata": {"name": name,
            **({"namespace": namespace} if namespace is not None else {}), "labels": dict(labels)}}

    sa = obj("ServiceAccount")
    sa["automountServiceAccountToken"] = False
    cluster = obj("ClusterRole", namespace=None, api="rbac.authorization.k8s.io/v1")
    cluster["rules"] = [{"apiGroups": [""], "resources": ["namespaces"], "resourceNames": sorted(namespaces), "verbs": ["get"]}]
    subject = [{"kind": "ServiceAccount", "name": name, "namespace": ns}]
    cluster_binding = obj("ClusterRoleBinding", namespace=None, api="rbac.authorization.k8s.io/v1")
    cluster_binding.update(subjects=subject, roleRef={"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": name})
    permissions = [sa, cluster, cluster_binding]
    for namespace in namespaces:
        role = obj("Role", namespace=namespace, api="rbac.authorization.k8s.io/v1")
        role["rules"] = [
            {"apiGroups": [""], "resources": ["resourcequotas"], "verbs": ["get", "create"]},
            {"apiGroups": ["apps"], "resources": ["deployments", "statefulsets"], "verbs": ["get", "create", "patch"]},
            {"apiGroups": ["batch"], "resources": ["jobs", "cronjobs"], "verbs": ["get", "list", "create", "patch"]},
            {"apiGroups": ["networking.k8s.io"], "resources": ["ingresses"], "verbs": ["get", "create", "patch"]},
            {"apiGroups": ["apps"], "resources": ["replicasets"], "verbs": ["get", "list"]},
            {"apiGroups": [""], "resources": ["pods"], "verbs": ["get", "list", "delete"]},
        ]
        role_binding = obj("RoleBinding", namespace=namespace, api="rbac.authorization.k8s.io/v1")
        role_binding.update(subjects=subject, roleRef={"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": name})
        permissions.extend((role, role_binding))
    network = obj("NetworkPolicy", api="networking.k8s.io/v1")
    selector = {"matchLabels": {"loom.nebius/retirement": name}}
    network["spec"] = {"podSelector": selector, "policyTypes": ["Ingress", "Egress"], "ingress": [], "egress": [
        {"to": [{"namespaceSelector": {"matchLabels": {"kubernetes.io/metadata.name": "kube-system"}},
                 "podSelector": {"matchLabels": {"k8s-app": "kube-dns"}}}],
         "ports": [{"protocol": protocol, "port": 53} for protocol in ("TCP", "UDP")]},
        {"to": [{"podSelector": {"matchLabels": {"app": "loom-postgres"}}}], "ports": [{"protocol": "TCP", "port": 5432}]},
        {"ports": [{"protocol": "TCP", "port": urlsplit(validated.kubernetes.endpoint).port or 443}]},
    ]}
    database_network = obj("NetworkPolicy", api="networking.k8s.io/v1")
    database_network["metadata"]["name"] += "-db"
    database_network["spec"] = {"podSelector": {"matchLabels": {"app": "loom-postgres"}}, "policyTypes": ["Ingress"],
        "ingress": [{"from": [{"podSelector": selector}], "ports": [{"protocol": "TCP", "port": 5432}]}]}
    cm = obj("ConfigMap")
    cm.update(immutable=True, data={"retirement.json": json.dumps(settings, sort_keys=True)})
    job = copy.deepcopy(rendered.files["30-migrate.yaml"][0])
    job["metadata"] = obj("Job")["metadata"]
    job["spec"].update(backoffLimit=0, activeDeadlineSeconds=1800, completions=1, parallelism=1)
    job["spec"].pop("ttlSecondsAfterFinished", None)
    job["spec"]["template"]["metadata"] = {"labels": dict(labels)}
    pod = job["spec"]["template"]["spec"]
    pod.update(serviceAccountName=name, automountServiceAccountToken=False, restartPolicy="Never")
    container = pod["containers"][0]
    container["command"] = ["python", "-m", "loom_service.environment_management.retirement"]
    container["env"] = [{"name": "LOOM_RETIREMENT_DB_URL", "valueFrom": {"secretKeyRef": {"name": "loom-platform-db", "key": "service-url"}}}]
    container["resources"] = {"requests": {"cpu": "100m", "memory": "256Mi", "ephemeral-storage": "128Mi"},
        "limits": {"cpu": "500m", "memory": "512Mi", "ephemeral-storage": "256Mi"}}
    pod["volumes"] = [volume for volume in pod["volumes"] if volume["name"] == "db-ca"] + [
        {"name": "retirement", "configMap": {"name": name}},
        {"name": "retirement-kubernetes", "projected": {"defaultMode": 0o440, "sources": [
            {"serviceAccountToken": {"path": "token", "expirationSeconds": 3600}},
            {"configMap": {"name": "kube-root-ca.crt", "items": [{"key": "ca.crt", "path": "ca.crt"}]}}]}},
    ]
    container["volumeMounts"] = [mount for mount in container["volumeMounts"] if mount["name"] == "db-ca"] + [
        {"name": "retirement", "mountPath": "/var/run/loom-retirement", "readOnly": True},
        {"name": "retirement-kubernetes", "mountPath": "/var/run/loom-retirement-kubernetes", "readOnly": True},
    ]
    return {phase: {_key(doc): doc for doc in documents} for phase, documents in {
        "permissions": permissions, "network": [network, database_network], "job": [cm, job],
    }.items()}


def stage_retirement(*, request: RetirementInstallRequest, phase: str, api: ManagementStageAPI,
                     state_dir: Path) -> dict[str, Any]:
    phases = retirement_documents(request)
    return _stage_fixed_documents(documents=phases[phase], revision=digest(phases), phase="retirement-" + phase,
        binding=request.binding, api=api, state_dir=state_dir, default_document=_setup_defaulted)


def retirement_ready(*, request: RetirementInstallRequest, api: ManagementStageAPI, state_dir: Path) -> bool:
    phases = retirement_documents(request)
    identity = {"schema": "loom.nebius-management-stage.v1", "binding": asdict(request.binding),
        "revision": digest(phases), "phase": "retirement-job"}
    with private_state._locked_state(state_dir):
        record = json.loads(private_state._private_read(state_dir / "stage.json", limit=4 * 1024**2))
        _validate_record(record, identity, phases["job"])
        ready = False
        for item in record["resources"].values():
            api.verify_identity(request.binding)
            actual = api.get_resource(item["desired"])
            if (item["status"] != "created" or actual is None or _uid(actual) != item["uid"]
                    or _snapshot(actual) != item["observed"]):
                raise ManagementStageError("retirement resource identity or configuration changed")
            if actual["kind"] == "Job":
                status = actual.get("status", {})
                conditions = {row["type"]: row["status"] for row in status.get("conditions", [])}
                if conditions.get("Failed") == "True":
                    raise ManagementStageError("retirement Job failed; explicit recovery required")
                ready = conditions.get("Complete") == "True" and status.get("succeeded", 0) == 1
        return ready


def install_retirement(*, request: RetirementInstallRequest,
                       resources: Callable[[str], AbstractContextManager[ManagementStageAPI]],
                       state_dir: Path, anchor_dir: Path) -> dict[str, Any]:
    """One fixed sequence, retaining independent start evidence across lost state."""
    phases = retirement_documents(request)
    identity = {"schema": "loom.nebius-management-retirement.v1", "binding": asdict(request.binding),
        "revision": digest(phases), "state_dir": str(state_dir)}
    try:
        if (any(not path.is_absolute() or path != path.resolve() for path in (state_dir, anchor_dir))
                or state_dir == anchor_dir or state_dir in anchor_dir.parents or anchor_dir in state_dir.parents):
            raise ValueError
        with private_state._locked_state(anchor_dir):
            marker = anchor_dir / (request.binding.installation_id + ".json")
            progress = state_dir / "retirement.json"
            if marker.exists() or marker.is_symlink():
                if json.loads(private_state._private_read(marker)) != identity:
                    raise ValueError
                record = json.loads(private_state._private_read(progress))
                if (set(record) != {*identity, "started"} or any(record[key] != value for key, value in identity.items())
                        or record["started"] != list(phases)[:len(record["started"])]
                        or any(not (state_dir / phase / "stage.json").is_file() for phase in record["started"])):
                    raise ValueError
            else:
                if state_dir.exists() or state_dir.is_symlink():
                    raise ValueError
                record = {**identity, "started": []}
                private_state._atomic_json(marker, identity)
            with private_state._locked_state(state_dir):
                private_state._atomic_json(progress, record)
                for phase in phases:
                    if phase not in record["started"]:
                        record["started"].append(phase)
                        private_state._atomic_json(progress, record)
                    with resources(phase) as api:
                        stage_retirement(request=request, phase=phase, api=api, state_dir=state_dir / phase)
                with resources("job") as api:
                    complete = retirement_ready(request=request, api=api, state_dir=state_dir / "job")
                return {"status": "management_retired" if complete else "pending",
                    **({} if complete else {"phase": "retirement"}), "namespace_uid": request.binding.namespace_uid,
                    "revision": identity["revision"]}
    except ManagementStageError:
        raise
    except Exception:
        raise ManagementStageError("retirement recovery evidence unavailable; preserve state") from None
