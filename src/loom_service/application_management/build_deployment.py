"""Fixed manager-side source transport and read-only build observation delivery."""
from __future__ import annotations

from typing import Any

from loom.nebius_application_authority import ApplicationNamespaceAuthorityV1
from loom_service.application_management.installation import ApplicationSourceUploadSettings

SOURCE_CREDENTIALS_PATH = "/var/run/loom-application-source-credentials"
SOURCE_VOLUME_PATH = "/var/run/loom-application-source"
SOURCE_SPOOL_PATH = SOURCE_VOLUME_PATH + "/spool"


def source_spool_mib(settings: ApplicationSourceUploadSettings) -> int:
    # Each admitted upload can retain an archive plus its validated extraction.
    # This is transient local transport space, not a PVC or an execution share.
    # Kubernetes' volume/Pod limits also bound filesystem metadata-heavy trees.
    return 2048 * settings.max_inflight


def mount_application_source(pod: dict[str, Any], *, settings: ApplicationSourceUploadSettings,
                             secret_name: str, service_image: str) -> None:
    container, = pod["containers"]
    security = pod.setdefault("securityContext", {})
    uid = container.get("securityContext", {}).get("runAsUser", security.get("runAsUser", 1000))
    if type(uid) is not int or uid <= 0:
        raise ValueError("application source process must be non-root")
    names = {"application-source", "application-source-credentials"}
    if (any(row["name"] in names for row in pod.get("volumes", []))
            or any(row["name"] == "prepare-application-source" for row in pod.get("initContainers", []))
            or any(row["name"] in names or row["mountPath"].startswith(SOURCE_VOLUME_PATH)
                for row in container.get("volumeMounts", []))):
        raise ValueError("application source mounts already configured")
    security.setdefault("runAsUser", uid)
    security.setdefault("runAsGroup", uid)
    security.setdefault("fsGroup", uid)
    security["runAsNonRoot"] = True
    pod.setdefault("volumes", []).extend([
        {"name": "application-source", "emptyDir": {"sizeLimit": f"{source_spool_mib(settings)}Mi"}},
        {"name": "application-source-credentials", "secret": {"secretName": secret_name,
            "defaultMode": 0o440, "items": [{"key": "credentials.json", "path": "credentials.json"}]}},
    ])
    container.setdefault("volumeMounts", []).extend([
        {"name": "application-source", "mountPath": SOURCE_VOLUME_PATH},
        {"name": "application-source-credentials", "mountPath": SOURCE_CREDENTIALS_PATH, "readOnly": True},
    ])
    pod.setdefault("initContainers", []).append({"name": "prepare-application-source", "image": service_image,
        "command": ["python", "-c", "import os,stat,sys; from pathlib import Path; "
            "p=Path(sys.argv[1]); p.mkdir(mode=0o700,exist_ok=True); s=p.lstat(); "
            "assert p.resolve(strict=True)==p and stat.S_ISDIR(s.st_mode) and "
            "s.st_uid==os.getuid() and stat.S_IMODE(s.st_mode)==0o700", SOURCE_SPOOL_PATH],
        "securityContext": {"runAsUser": uid, "runAsGroup": uid, "runAsNonRoot": True,
            "allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True, "capabilities": {"drop": ["ALL"]}},
        "resources": {"requests": {"cpu": "10m", "memory": "32Mi"}, "limits": {"cpu": "100m", "memory": "64Mi"}},
        "volumeMounts": [{"name": "application-source", "mountPath": SOURCE_VOLUME_PATH}]})


def render_application_build_reader(authority: ApplicationNamespaceAuthorityV1, *, namespace: str
                                    ) -> list[dict[str, Any]]:
    # The protected caller supplies the foundation's shared build namespace.
    # The manager already has namespace-get for identity checks, not Job-write.
    name = "loom-application-build-reader-" + authority.installation_id.hex[:12]
    metadata = {"name": name, "namespace": namespace,
        "labels": {"loom.nebius/management-installation": str(authority.installation_id)}}
    return [{"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "Role", "metadata": metadata,
        "rules": [
            {"apiGroups": ["batch"], "resources": ["jobs"], "verbs": ["get"]},
            {"apiGroups": [""], "resources": ["pods"], "verbs": ["get", "list"]},
            {"apiGroups": [""], "resources": ["pods/log"], "verbs": ["get"]},
        ]},
        {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "RoleBinding", "metadata": metadata,
         "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": name},
         "subjects": [{"kind": "ServiceAccount", "name": "loom-application-provisioner", "namespace": authority.namespace}]}]
