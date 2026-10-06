"""Read-only startup symptoms, not runtime qualification or retry authority."""
from __future__ import annotations

import json
import re
from typing import Any

from scripts.ops.deploy_nebius_platform import Kubectl
from scripts.ops.nebius_management_gateway import validate_operation

_LABEL = "loom.nebius/management-installation"
_COLLECTOR = "loom-execution-capacity-collector"
_WORKLOADS = {"manager": ("loom-service", "loom-service"),
    "gateway": ("gateway", "loom-pool-gateway"), "collector": ("collector", _COLLECTOR)}
_ERRORS = {"ValueError", "RuntimeError", "TypeError", "KeyError", "AttributeError", "AssertionError",
    "FileNotFoundError", "PermissionError", "TimeoutError", "ImportError", "ModuleNotFoundError",
    "OSError", "ConnectionError", "ExceptionGroup", "CapacityCollectionError", "PoolObservationError",
    "OperationalError", "ProgrammingError", "IntegrityError", "ValidationError", "ConnectError",
    "ConnectTimeout", "ReadTimeout", "HTTPStatusError", "SSLCertVerificationError",
    "KubernetesObservationError", "SchemaNotAtHeadError", "PoolAuthenticationError"}
_STAGES = {"invalid_application_provider_credentials", "invalid_environment_provider_credentials",
    "invalid_application_source_runtime", "invalid_application_build_runtime", "invalid_application_runtime_material",
    "pool_gateway_identity_unavailable"}
_FILES = {
    "loom_service/app.py": "service_app",
    "loom_service/config.py": "service_config",
    "loom_service/environment_management/installation.py": "environment_installation",
    "loom_service/environment_management/runtime.py": "environment_runtime",
    "loom_service/application_management/installation.py": "application_installation",
    "loom_service/application_management/service_runtime.py": "application_runtime",
    "loom_service/application_management/source_runtime.py": "application_source_runtime",
    "loom_service/application_management/source_upload.py": "application_source_upload",
    "loom_service/application_management/build_runtime.py": "application_build_runtime",
    "loom_service/pool_management/profiles.py": "pool_profiles",
    "loom_service/pool_management/__main__.py": "pool_gateway_entry",
    "loom_service/pool_management/auth.py": "pool_auth",
    "loom_service/pool_management/kubernetes.py": "pool_kubernetes",
    "loom_service/pool_management/gateway_journal.py": "pool_gateway_journal",
    "loom_service/environment_management/kubernetes_credentials.py": "projected_kubernetes_credentials",
    "loom/db/schema_startup.py": "database_schema",
    "loom/secret_store.py": "secret_store",
    "loom_execution_capacity_collector/__main__.py": "collector_entry",
    "loom_execution_capacity_collector/config.py": "collector_config",
    "loom_execution_capacity_collector/pool_collector.py": "pool_collector",
    "loom_execution_capacity_collector/pool_client.py": "pool_client",
    "loom_execution_capacity_collector/kubernetes.py": "collector_kubernetes",
    "loom_execution_capacity_collector/nebius.py": "collector_nebius",
}


def _diagnostic(raw: str) -> dict[str, Any]:
    """Recognize fixed enums and numeric source locations, never echo log text."""
    errors: list[str] = []
    stages: list[str] = []
    locations: list[dict[str, Any]] = []
    for line in raw[-32768:].splitlines()[-100:]:
        line = line.lstrip(" +|")  # Also recognize Python's ExceptionGroup layout.
        frame = re.fullmatch(r'File "[^"\n]*/([^"\n]+)", line ([0-9]{1,6}), in [^\n]+', line)
        if frame is not None:
            # Match the entire fixed package suffix, not an arbitrary basename.
            for path, component in _FILES.items():
                if re.fullmatch(r'File "[^"\n]*/' + re.escape(path) + r'", line [0-9]{1,6}, in [^\n]+', line):
                    location = {"component": component, "line": int(frame[2])}
                    if location not in locations:
                        locations.append(location)
                    break
        error, _, message = line.partition(":")
        # Common libraries qualify exception class names. Only export the enum.
        name = error.rsplit(".", 1)[-1]
        if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_.]{0,127}", error) and name in _ERRORS:
            if name not in errors:
                errors.append(name)
            if message.strip() in _STAGES and message.strip() not in stages:
                stages.append(message.strip())
    if not (errors or locations or stages):
        return {"status": "unavailable"}
    return {"status": "observed", "errors": errors[-16:], "stages": stages, "locations": locations[-20:]}


def _parent(kube: Kubectl, child: dict[str, Any], kind: str) -> dict[str, Any]:
    owner, = child["metadata"]["ownerReferences"]
    version = "apps/v1" if kind in {"ReplicaSet", "Deployment"} else "batch/v1"
    if (owner.get("controller") is not True or owner.get("kind") != kind
            or owner.get("apiVersion") != version or not owner.get("uid")):
        raise ValueError
    namespace = child["metadata"]["namespace"]
    parent = kube.get(kind.lower(), owner["name"], namespace)
    if (parent.get("kind") != kind or parent.get("apiVersion") != version
            or parent["metadata"].get("namespace") != namespace
            or parent["metadata"].get("name") != owner["name"]
            or parent["metadata"].get("uid") != owner["uid"]):
        raise ValueError
    return parent


def _container(spec: dict[str, Any], name: str) -> dict[str, Any]:
    container, = spec["containers"]
    if container.get("name") != name:
        raise ValueError
    return container


def _bound(kube: Kubectl, pod: dict[str, Any], role: str, installation: str) -> dict[str, Any]:
    from scripts.ops.nebius_pool_migration_guard import _runtime_pod_spec

    name, controller = _WORKLOADS[role]
    deployment = role != "collector"
    parent = _parent(kube, pod, "ReplicaSet" if deployment else "Job")
    root = _parent(kube, parent, "Deployment" if deployment else "CronJob")
    root_spec = root["spec"] if deployment else root["spec"]["jobTemplate"]["spec"]
    if root["metadata"]["name"] != controller:
        raise ValueError
    if parent["spec"]["template"]["spec"] != root_spec["template"]["spec"]:
        raise ValueError
    actual_meta = parent["spec"]["template"].get("metadata", {})
    expected_meta = root_spec["template"].get("metadata", {})
    actual_labels, expected_labels = dict(actual_meta.get("labels", {})), expected_meta.get("labels", {})
    generated = {"pod-template-hash"} if deployment else {
        "batch.kubernetes.io/controller-uid", "controller-uid", "batch.kubernetes.io/job-name", "job-name"}
    for key in generated - expected_labels.keys():
        actual_labels.pop(key, None)
    if (actual_labels != expected_labels
            or actual_meta.get("annotations", {}) != expected_meta.get("annotations", {})):
        raise ValueError
    container = _container(_runtime_pod_spec(pod["spec"], root_spec["template"]["spec"]), name)
    for spec in (parent["spec"]["template"]["spec"], root_spec["template"]["spec"]):
        expected = _container(spec, name)
        if any(container.get(key) != expected.get(key) for key in
               ("name", "image", "command", "args", "env", "envFrom", "volumeMounts")):
            raise ValueError
    if role == "manager":
        if (root["metadata"].get("labels", {}).get(_LABEL) != installation
                or {"name": "LOOM_SVC_SERVICE_MODE", "value": "management"} not in container.get("env", [])
                or container.get("command") not in (None, ["python", "-m", "loom_service"])
                or container.get("args")):
            raise ValueError
    elif role == "gateway":
        identity = [row for row in container.get("env", [])
            if row.get("name", "").upper() == "LOOM_POOL_GATEWAY_INSTALLATION_ID"]
        if (root["metadata"].get("labels", {}).get(_LABEL) != installation
                or identity != [{"name": "LOOM_POOL_GATEWAY_INSTALLATION_ID", "value": installation}]
                or container.get("command") != ["python", "-m", "loom_service.pool_management"]
                or container.get("args")):
            raise ValueError
    else:
        if (container.get("command") != ["python", "-m", "loom_execution_capacity_collector"]
                or container.get("args")):
            raise ValueError
        source, = container["envFrom"]
        config_name = source["configMapRef"]["name"]
        if not re.fullmatch(r"loom-pool-collector-[0-9a-f]{32}", config_name):
            raise ValueError
        config = kube.get("configmap", config_name, pod["metadata"]["namespace"])
        if (config.get("immutable") is not True or config["metadata"].get("name") != config_name
                or config["metadata"].get("namespace") != pod["metadata"]["namespace"]
                or config["metadata"].get("labels", {}).get(_LABEL) != installation
                or config.get("data", {}).get("LOOM_EXECUTION_CAPACITY_COLLECTOR_COLLECTION_MODE") != "pool"):
            raise ValueError
    return root


def _instance(pod: dict[str, Any], name: str) -> str | None:
    status, = [row for row in pod.get("status", {}).get("containerStatuses", []) if row.get("name") == name]
    if status.get("ready") is True:
        return None
    for key, instance in (("state", "current"), ("lastState", "previous")):
        code = status.get(key, {}).get("terminated", {}).get("exitCode")
        if type(code) is int and code != 0:
            return instance
    return None


def _collector_completion(kube: Kubectl, pods: list[dict[str, Any]], *, namespace: str,
                          installation: str) -> dict[str, Any] | None:
    """Bounded read-only observation, not publication or activation authority."""
    from scripts.ops.nebius_pool_migration_guard import _runtime_pod_spec

    attempts = 0
    for pod in sorted(pods, key=lambda row: row['metadata'].get('creationTimestamp', ''), reverse=True):
        if (pod['metadata'].get('namespace') != namespace or pod.get('status', {}).get('phase') != 'Succeeded'
                or not any(row.get('name') == 'collector' for row in pod['spec'].get('containers', []))):
            continue
        if attempts == 3:
            break
        attempts += 1
        try:
            if pod['metadata'].get('deletionTimestamp'):
                raise ValueError
            root = _bound(kube, pod, 'collector', installation)
            job = _parent(kube, pod, 'Job')
            status = job.get('status', {})
            complete = [row for row in status.get('conditions', []) if row.get('type') == 'Complete']
            if len(complete) != 1 or complete[0].get('status') != 'True':
                raise ValueError
            if (status.get('active', 0) != 0 or type(status.get('succeeded')) is not int or status['succeeded'] < 1
                    or any(row.get('type') == 'Failed' and row.get('status') == 'True' for row in status.get('conditions', []))):
                raise ValueError
            expected = root['spec']['jobTemplate']['spec']['template']['spec']
            normalized = _runtime_pod_spec(pod['spec'], expected)
            image = _container(expected, 'collector')['image']
            match = re.fullmatch(r'[^@\s]+@sha256:([0-9a-f]{64})', image)
            if match is None:
                raise ValueError
            for field, statuses in (('containers', 'containerStatuses'), ('initContainers', 'initContainerStatuses')):
                actual, wanted = normalized.get(field, []), expected.get(field, [])
                observed = pod['status'].get(statuses, [])
                names = [row['name'] for row in wanted]
                if (sorted(row['name'] for row in actual) != sorted(names)
                        or sorted(row['name'] for row in observed) != sorted(names)
                        or any(row.get('image') != image for row in (*actual, *wanted))
                        or any(type(row.get('state', {}).get('terminated', {}).get('exitCode')) is not int
                            or row['state']['terminated']['exitCode'] != 0 for row in observed)):
                    raise ValueError
                if actual != wanted:
                    raise ValueError
            current = kube.get('pod', pod['metadata']['name'], namespace)
            if (current != pod or _parent(kube, current, 'Job') != job
                    or _bound(kube, current, 'collector', installation) != root):
                raise ValueError
            return {'status': 'observed', 'namespace': namespace,
                'pod': pod['metadata']['name'], 'pod_uid': pod['metadata']['uid'],
                'job': job['metadata']['name'], 'job_uid': job['metadata']['uid'],
                'controller': root['metadata']['name'], 'controller_uid': root['metadata']['uid'],
                'image_sha256': match[1]}
        except Exception:
            continue
    return None


def pool_startup_diagnostics(kube: Kubectl, pods: list[dict[str, Any]], *, operation_json: str,
                             execution_namespace: str) -> dict[str, Any]:
    if not operation_json:
        return {"status": "not_configured", "workloads": []}
    try:
        operation = json.loads(operation_json)
        validate_operation(operation)
        namespace, installation = operation["namespace"], operation["installation_id"]
    except Exception:
        return {"status": "unavailable", "workloads": []}
    workloads: list[dict[str, Any]] = []
    attempts = dict.fromkeys(_WORKLOADS, 0)
    observed: set[str] = set()
    for pod in sorted(pods, key=lambda row: row["metadata"].get("creationTimestamp", ""), reverse=True):
        try:
            metadata = pod["metadata"]
            role = "manager" if metadata.get("namespace") == namespace else "collector"
            if role == "manager" and any(row.get("name") == "gateway" for row in pod["spec"].get("containers", [])):
                role = "gateway"
            if metadata.get("namespace") not in {namespace, execution_namespace} or role in observed:
                continue
            name = _WORKLOADS[role][0]
            instance = _instance(pod, name)
            if instance is None or attempts[role] == 3:
                continue
            attempts[role] += 1
            root = _bound(kube, pod, role, installation)
        except Exception:
            continue
        observed.add(role)
        try:
            # Read exactly the selected failed instance, then reject replacement
            # or restart drift. Logs are symptoms, never an authorization input.
            raw = kube.run("logs", metadata["name"], "-n", metadata["namespace"], "-c", name,
                *(["--previous"] if instance == "previous" else []), "--tail=100", "--limit-bytes=32768", timeout=40)
            current = kube.get("pod", metadata["name"], metadata["namespace"])
            if (current["metadata"].get("uid") != metadata["uid"]
                    or current["metadata"].get("ownerReferences") != metadata.get("ownerReferences")
                    or current.get("status", {}).get("containerStatuses") != pod.get("status", {}).get("containerStatuses")):
                raise ValueError
            if _bound(kube, current, role, installation)["metadata"]["uid"] != root["metadata"]["uid"]:
                raise ValueError
            diagnostic = _diagnostic(raw)
        except Exception:
            diagnostic = {"status": "unavailable"}
        workloads.append({"role": role, "namespace": metadata["namespace"], "pod": metadata["name"],
            "pod_uid": metadata["uid"], "controller": root["metadata"]["name"], "controller_uid": root["metadata"]["uid"],
            "container": name, "log_instance": instance, "diagnostic": diagnostic})
    return {"status": "observed", "workloads": workloads,
        "collector_completion": _collector_completion(kube, pods, namespace=execution_namespace, installation=installation)}
