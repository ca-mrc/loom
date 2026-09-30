"""Fixed registration Job; its protected caller owns candidate and namespace proof."""
from __future__ import annotations

import re
from typing import Any

from loom.nebius_platform_render import _env, _mount_secret, _obj, _secret_env
from loom_service.pool_management.installation import PoolInstallation


def render_registration(spec: PoolInstallation, *, namespace: str, service_image: str) -> tuple[dict[str, Any], dict[str, Any]]:
    spec = PoolInstallation.model_validate(spec.model_dump())
    if (re.fullmatch(r"loom-nebius-management(?:-[a-z0-9]+(?:-[a-z0-9]+)*)?", namespace) is None
            or len(namespace) > 53 or re.fullmatch(r".+@sha256:[0-9a-f]{64}", service_image) is None):
        raise ValueError("unqualified pool registration runtime")
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
