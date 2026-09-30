"""Bounded failed-probe observations; no SQL, exec, writes or retry authority."""
from __future__ import annotations

import base64
import json
import re
from typing import Any
from uuid import UUID

from scripts.ops.deploy_nebius_platform import Kubectl, job_failed

from loom.nebius_management_refresh_probe import (
    FAILURE_ERRORS,
    FAILURE_STAGES,
    SCHEMA,
    RefreshProbeSettings,
)

_MARKER = "loom.nebius/management-refresh-id"
_INSTALLATION = "loom.nebius/management-installation"
_COMMAND = ["python", "-m", "loom.nebius_management_refresh_probe"]
_ENV = [{"name": "LOOM_REFRESH_DB_URL", "valueFrom": {
    "secretKeyRef": {"name": "loom-platform-db", "key": "service-url"}}}]
_NAME = re.compile(r"loom-refresh-(manager-probe|shared-probe|post-probe)-([0-9a-f]{32})")


def _diagnostic(raw: str) -> dict[str, str]:
    for line in reversed(raw[-16384:].splitlines()[-50:]):
        try:
            value = json.loads(line)
        except (ValueError, RecursionError):
            error = line.partition(":")[0]
            if error in {"ImportError", "ModuleNotFoundError", "SyntaxError", "IndentationError"}:
                return {"error_type": error}
            continue
        if isinstance(value, dict) and value.get("schema") == SCHEMA and value.get("status") == "unqualified":
            result = {"status": "unqualified"}
            for key, allowed in (("stage", FAILURE_STAGES), ("error_type", FAILURE_ERRORS)):
                field = value.get(key)
                if isinstance(field, str) and field in allowed:
                    result[key] = field
            return result
    return {"status": "unavailable"}


def _current_url(kube: Kubectl, job: dict[str, Any], pod: dict[str, Any], phase: str) -> dict[str, Any]:
    """Project current credential shape, never claim it is failure-time evidence."""
    from sqlalchemy.engine import make_url

    try:
        metadata = job["metadata"]
        namespace, name = metadata["namespace"], metadata["name"]
        cm = kube.get("configmap", name, namespace)
        if (cm.get("immutable") is not True or cm["metadata"]["name"] != name
                or cm["metadata"]["namespace"] != namespace
                or cm["metadata"].get("annotations", {}).get(_MARKER) != metadata["annotations"][_MARKER]
                or cm["metadata"].get("labels", {}).get(_INSTALLATION) != metadata["labels"][_INSTALLATION]):
            raise ValueError
        raw = cm["data"]["probe.json"]
        if not isinstance(raw, str) or len(raw.encode()) > 262144:
            raise ValueError
        settings = RefreshProbeSettings.model_validate_json(raw)
        expected_namespace = settings.shared.platform_namespace if settings.mode == "shared" else settings.namespace
        if settings.mode != ("shared" if phase == "shared-probe" else "manager") or namespace != expected_namespace:
            raise ValueError
        for spec in (job["spec"]["template"]["spec"], pod["spec"]):
            container, = spec["containers"]
            if (container.get("command") != _COMMAND or container.get("args") or container.get("env") != _ENV
                    or container.get("envFrom") or spec.get("initContainers")
                    or spec.get("automountServiceAccountToken") is not False
                    or spec.get("serviceAccountName") != "loom-platform"):
                raise ValueError
            config_volume, = [row for row in spec["volumes"] if row["name"] == "refresh-probe"]
            config_map = dict(config_volume["configMap"])
            mode = config_map.pop("defaultMode", 420)
            if (type(mode) is not int or mode != 420 or config_map != {
                    "name": name, "items": [{"key": "probe.json", "path": "probe.json"}]}):
                raise ValueError
            mount, = [row for row in container["volumeMounts"] if row["name"] == "refresh-probe"]
            if mount != {"name": "refresh-probe", "mountPath": "/var/run/loom-management-refresh", "readOnly": True}:
                raise ValueError
        secret = kube.get("secret", "loom-platform-db", namespace)
        if (secret["metadata"]["name"], secret["metadata"]["namespace"]) != ("loom-platform-db", namespace):
            raise ValueError
        encoded = secret["data"]["service-url"]
        if not isinstance(encoded, str) or len(encoded) > 16384:
            raise ValueError
        url = make_url(base64.b64decode(encoded, validate=True).decode())
        return {"status": "observed_current", "checks": {
            "raw_postgresql_driver": url.drivername == "postgresql",
            "psycopg_driver": url.drivername == "postgresql+psycopg",
            "service_role": url.username == "loom_service", "password_present": bool(url.password),
            "namespace_host": url.host == f"loom-postgres.{namespace}.svc", "port": url.port == 5432,
            "database": url.database == "loom", "tls_query": dict(url.query) == {
                "sslmode": "verify-full", "sslrootcert": "/var/run/loom-db/ca.crt"},
        }}
    except Exception:
        return {"status": "unavailable"}


def failed_refresh_probes(kube: Kubectl, pods: list[dict[str, Any]], namespace: str) -> list[dict[str, Any]]:
    """Observe up to three terminal Jobs, retaining failures with absent diagnostics."""
    result: list[dict[str, Any]] = []
    inspected = 0
    for pod in sorted(pods, key=lambda row: row["metadata"].get("creationTimestamp", ""), reverse=True):
        try:
            metadata = pod["metadata"]
            selected_namespace = metadata.get("namespace", "")
            if ((selected_namespace != namespace and not re.fullmatch(
                    r"loom-nebius-management(?:-[a-z0-9]+(?:-[a-z0-9]+)*)?", selected_namespace))
                    or pod.get("status", {}).get("phase") != "Failed"):
                continue
            owner, = metadata["ownerReferences"]
            match = _NAME.fullmatch(owner.get("name", ""))
            if (owner.get("kind") != "Job" or owner.get("apiVersion") != "batch/v1"
                    or owner.get("controller") is not True or match is None or not UUID(owner["uid"]).int):
                continue
            operation = UUID(hex=match[2])
            if not operation.int or metadata.get("annotations", {}).get(_MARKER) != str(operation):
                continue
            container, = pod["spec"]["containers"]
            if container.get("command") != _COMMAND:
                continue
            if inspected == 3:
                break
            inspected += 1
            job = kube.get("job", owner["name"], selected_namespace)
            jm, template = job["metadata"], job["spec"]["template"]
            if (jm.get("uid") != owner["uid"] or jm.get("name") != owner["name"]
                    or jm.get("namespace") != selected_namespace or not job_failed(job)
                    or jm.get("annotations", {}).get(_MARKER) != str(operation)
                    or template["metadata"].get("annotations", {}).get(_MARKER) != str(operation)
                    or not UUID(jm["labels"][_INSTALLATION]).int
                    or template["metadata"].get("labels", {}).get(_INSTALLATION) != jm["labels"][_INSTALLATION]
                    or metadata.get("labels", {}).get(_INSTALLATION) != jm["labels"][_INSTALLATION]):
                continue
            expected, = template["spec"]["containers"]
            if any(expected.get(key) != container.get(key) for key in ("name", "image", "command")):
                continue
        except Exception:
            continue
        termination: dict[str, Any] = {}
        status = next((row for row in pod.get("status", {}).get("containerStatuses", [])
            if row.get("name") == container["name"]), {})
        terminated = status.get("state", {}).get("terminated", {})
        for key, field in (("exit_code", "exitCode"), ("signal", "signal")):
            if type(terminated.get(field)) is int and 0 <= terminated[field] <= 255:
                termination[key] = terminated[field]
        if "reason" in terminated:
            termination["reason"] = terminated["reason"] if terminated["reason"] in (
                "Error", "OOMKilled", "Completed", "ContainerCannotRun") else "Other"
        try:
            diagnostic = _diagnostic(kube.run("logs", metadata["name"], "-n", selected_namespace, "-c",
                container["name"], "--tail=50", "--limit-bytes=16384", timeout=40))
        except Exception:
            diagnostic = {"status": "unavailable"}
        result.append({"namespace": selected_namespace, "job": owner["name"], "job_uid": owner["uid"],
            "pod": metadata["name"], "pod_uid": metadata["uid"], "termination": termination,
            "diagnostic": diagnostic, "current_url": _current_url(kube, job, pod, match[1])})
    return result
