#!/usr/bin/env python3
"""Read-only, payload-free inventory before qualifying managed Nebius environments."""
from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID

ROOT = Path(__file__).resolve().parents[2]
if __package__ in {None, ""}:
    sys.path[:0] = [str(ROOT), str(ROOT / "src")]

from scripts.ops.deploy_nebius_platform import (  # noqa: E402
    DeploymentError,
    Kubectl,
    job_failed,
    verify_cluster_identity,
)
from scripts.ops.nebius_controller_inventory import controller_inventory  # noqa: E402
from scripts.ops.nebius_ingress_preflight import inspect_ingress  # noqa: E402
from scripts.ops.nebius_refresh_probe_inspection import failed_refresh_probes  # noqa: E402

RESOURCE_KEYS = ("cpu", "memory", "ephemeral-storage", "pods")
_BOOTSTRAP_ERRORS = {"ConfigurationRequestError", "MigrationError", "ValueError", "KeyError",
                     "FileNotFoundError", "TimeoutError", "OperationalError", "ProgrammingError",
                     "IntegrityError", "InsufficientPrivilege", "UndefinedTable", "UniqueViolation"}
_OPERATIONS = {"/admin/service-execution/catalog": "catalog",
               "/admin/execution-price-snapshots": "price-snapshot",
               "/admin/execution-capacity/status": "capacity-status",
               "/admin/execution-admission/status": "admission-status"}
_OPERATION_PREFIXES = {"/admin/execution-target-price-bindings/": "target-price-binding",
                       "/admin/execution-capacity-policies/": "capacity-policy",
                       "/admin/execution-admission-policies/": "admission-policy"}


def _bootstrap_diagnostic(raw: str, phase: str) -> dict[str, Any]:
    """Project existing bootstrap JSON; never forward free text or payloads."""
    for line in reversed(raw[-16_384:].splitlines()[-50:]):
        try:
            value = json.loads(line)
        except (ValueError, RecursionError):
            continue
        if not isinstance(value, dict) or value.get("phase") != phase or "error_type" not in value:
            continue
        error = value["error_type"]
        result: dict[str, Any] = {"phase": phase, "error_type": error
                                 if isinstance(error, str) and error in _BOOTSTRAP_ERRORS else "OtherError"}
        if error == "ConfigurationRequestError":
            if value.get("method") in ("GET", "POST", "PUT"):
                result["method"] = value["method"]
            if type(value.get("http_status")) is int and 400 <= value["http_status"] <= 599:
                result["http_status"] = value["http_status"]
            route = value.get("route")
            if isinstance(route, str):
                operation = _OPERATIONS.get(route)
                if operation is None:
                    operation = next((name for prefix, name in _OPERATION_PREFIXES.items()
                                      if route.startswith(prefix)), None)
                if operation is not None:
                    result["operation"] = operation
        # Do not export arbitrary reason/SQL/traceback/error-class strings.
        return result
    return {"status": "unavailable"}


def _failed_bootstrap_jobs(kube: Kubectl, pods: list[dict[str, Any]], namespace: str) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    candidates = sorted(pods, key=lambda p: p["metadata"].get("creationTimestamp", ""), reverse=True)
    inspected = 0
    for pod in candidates:
        metadata = pod["metadata"]
        if metadata.get("namespace") != namespace or pod.get("status", {}).get("phase") != "Failed":
            continue
        owner = next((owner for owner in metadata.get("ownerReferences", [])
                      if owner.get("kind") == "Job" and owner.get("controller") is True
                      and re.fullmatch(r"loom-platform-(?:configure|migrate)-[0-9a-f]{12}", owner.get("name", ""))), None)
        if owner is None or not isinstance(owner.get("uid"), str) or not owner["uid"]:
            continue
        phase = "configure" if owner["name"].startswith("loom-platform-configure-") else "database"
        container = next((c for c in pod.get("spec", {}).get("containers", [])
                          if c.get("name") == owner["name"] and c.get("command") == [
                              "python", "-m", "loom.nebius_platform_bootstrap", phase,
                          ]), None)
        if container is None:
            continue
        # Bound API/log requests even if every retained candidate is stale.
        if inspected == 3:
            break
        inspected += 1
        job = kube.get("job", owner["name"], namespace)
        if job.get("metadata", {}).get("uid") != owner.get("uid") or not job_failed(job):
            continue
        try:
            raw = kube.run("logs", metadata["name"], "-n", namespace, "-c", container["name"],
                           "--tail=50", "--limit-bytes=16384", timeout=40)
            diagnostic = _bootstrap_diagnostic(raw, phase)
        except Exception:
            diagnostic = {"status": "unavailable"}
        result.append({"namespace": namespace, "job": owner["name"], "job_uid": owner["uid"],
                       "pod": metadata["name"], "pod_uid": metadata["uid"], "diagnostic": diagnostic})
    return result


def _retirement_diagnostic(raw: str) -> dict[str, str]:
    for line in reversed(raw[-16_384:].splitlines()[-50:]):
        try:
            value = json.loads(line)
        except (ValueError, RecursionError):
            # Import failures occur before the CLI's sanitized exception handler.
            error = line.partition(":")[0]
            if error in {"ImportError", "ModuleNotFoundError", "SyntaxError", "IndentationError"}:
                return {"error_type": error}
            continue
        if isinstance(value, dict) and value.get("status") in ("retirement_blocked", "retirement_completed"):
            return {"status": value["status"]}
    return {"status": "unavailable"}


def _retirement_journal_bound(job: dict[str, Any], cm: dict[str, Any], namespace_uids: dict[str, str]) -> bool:
    """Compare live identities with private create receipts without exporting them."""
    from scripts.ops.nebius_ingress_stage import _key, _snapshot, _uid
    from scripts.ops.nebius_management_gateway import validate_operation

    from loom.nebius_platform_render import digest

    operation = json.loads(os.environ["NEBIUS_MANAGEMENT_OPERATION_JSON"])
    validate_operation(operation)
    namespace = job["metadata"]["namespace"]
    installation = job["metadata"]["labels"]["loom.nebius/management-installation"]
    if (operation["schema"] != "loom.nebius-management-retirement-operation.v1"
            or (operation["namespace"], operation["installation_id"]) != (namespace, installation)):
        raise ValueError
    expected = {"binding": {"installation_id": installation, "namespace": namespace,
        "namespace_uid": namespace_uids[namespace], "kube_system_uid": namespace_uids["kube-system"]},
        "resources": {_key(doc): {"uid": _uid(doc), "snapshot": digest(_snapshot(doc))} for doc in (job, cm)}}
    target = os.environ["LOOM_DEPLOY_SSH_TARGET"]
    if re.fullmatch(r"[a-zA-Z0-9_.-]+@[a-zA-Z0-9_.-]+", target) is None:
        raise ValueError
    arguments = ["python3", "-", operation["state_dir"], json.dumps(expected)]
    command = ["ssh", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes", "-o", "IdentitiesOnly=yes",
        "-o", "ConnectTimeout=15", "-o", "ServerAliveInterval=15", "-o", "ServerAliveCountMax=3",
        "-o", "UserKnownHostsFile=" + os.environ["LOOM_DEPLOY_SSH_KNOWN_HOSTS_FILE"],
        "-i", os.environ["LOOM_DEPLOY_SSH_KEY_FILE"], target, shlex.join(arguments)]
    result = subprocess.run(command, input=Path(__file__).with_name("nebius_retirement_journal_probe.py").read_text(),
        capture_output=True, text=True, timeout=40, check=False)
    return result.returncode == 0 and len(result.stdout) <= 256 and json.loads(result.stdout) == {"status": "matched"}


def _retirement_registry_probe(kube: Kubectl, pods: list[dict[str, Any]], job: dict[str, Any], namespace: str,
                               namespace_uids: dict[str, str]) -> dict[str, Any]:
    """Fixed protected exec reads only the failed Job's bound registry records."""
    from scripts.ops.nebius_retirement_registry_probe import CHECKS, ERRORS

    from loom_service.environment_management.retirement import RetirementSettings

    stage = "configuration_identity"
    try:
        name = job["metadata"]["name"]
        installation = job["metadata"]["labels"]["loom.nebius/management-installation"]
        if str(UUID(installation)) != installation or not UUID(installation).int:
            raise ValueError
        cm = kube.get("configmap", name, namespace)
        if (cm.get("immutable") is not True or cm["metadata"]["labels"].get("loom.nebius/retirement") != name
                or cm["metadata"]["labels"].get("loom.nebius/management-installation") != installation):
            raise ValueError
        if os.environ.get("NEBIUS_MANAGEMENT_OPERATION_JSON"):
            # Exact UID/snapshot receipts also work when server timestamps tie.
            # A selected but mismatched journal must never fall back to a guess.
            stage = "configuration_journal"
            if not _retirement_journal_bound(job, cm, namespace_uids):
                raise ValueError
        else:
            # Without protected journal metadata retain the conservative check.
            stage = "configuration_lineage"
            config_created = datetime.fromisoformat(cm["metadata"]["creationTimestamp"])
            job_created = datetime.fromisoformat(job["metadata"]["creationTimestamp"])
            if config_created.tzinfo is None or job_created.tzinfo is None or config_created >= job_created:
                raise ValueError
        stage = "settings"
        settings = RetirementSettings.model_validate_json(cm["data"]["retirement.json"])
        if (settings.namespace != namespace or not any(volume.get("name") == "retirement"
                and volume.get("configMap", {}).get("name") == name for volume in job["spec"]["template"]["spec"]["volumes"])):
            raise ValueError
        stage = "manager_selection"
        managers = [pod for pod in pods if pod["metadata"].get("namespace") == namespace
            and pod["metadata"].get("labels", {}).get("loom.nebius/management-installation") == installation
            and pod["metadata"].get("labels", {}).get("app") == "loom-service"
            and pod.get("spec", {}).get("serviceAccountName") == "loom-application-provisioner"
            and not pod["metadata"].get("deletionTimestamp") and pod.get("status", {}).get("phase") == "Running"
            and any(row.get("name") == "loom-service" and row.get("ready") is True
                    for row in pod.get("status", {}).get("containerStatuses", []))]
        if len(managers) != 1:
            raise ValueError
        manager = managers[0]

        def same_manager(current: dict[str, Any]) -> bool:
            return (all(current["metadata"].get(key) == manager["metadata"].get(key)
                        for key in ("name", "namespace", "uid", "labels", "deletionTimestamp"))
                    and current.get("spec") == manager["spec"]
                    and current.get("status", {}).get("phase") == "Running"
                    and any(row.get("name") == "loom-service" and row.get("ready") is True
                            for row in current.get("status", {}).get("containerStatuses", [])))

        stage = "manager_before"
        before = kube.get("pod", manager["metadata"]["name"], namespace)
        if not same_manager(before):
            raise ValueError
        stage = "payload"
        targets = [target.model_dump(mode="json") for target in settings.targets]
        payload = json.dumps(targets)
        if len(payload.encode()) > 65536:
            raise ValueError
        source = Path(__file__).with_name("nebius_retirement_registry_probe.py").read_text()
        stage = "exec"
        raw = kube.run("exec", manager["metadata"]["name"], "-n", namespace, "-c", "loom-service", "--",
                       "python", "-c", source, namespace, payload, timeout=90)
        stage = "output"
        if len(raw.encode()) > 16384:
            raise ValueError
        stage = "manager_after"
        if not same_manager(kube.get("pod", manager["metadata"]["name"], namespace)):
            raise ValueError
        stage = "output"
        value = json.loads(raw)
        if value.get("status") == "unavailable":
            return {"status": "unavailable", "stage": value["stage"] if value.get("stage") in ("inputs", "database", "registry") else "unknown",
                "error_type": value["error_type"] if value.get("error_type") in ERRORS else "OtherError"}
        if value.get("status") != "observed" or value.get("read_only") is not True or len(value["targets"]) != len(targets):
            raise ValueError
        rows = []
        for expected, row in zip(targets, value["targets"], strict=True):
            if row["operation_id"] != expected["operation_id"] or any(type(row["checks"].get(key)) is not bool for key in CHECKS):
                raise ValueError
            rows.append({"operation_id": expected["operation_id"], "checks": {key: row["checks"][key] for key in CHECKS}})
        return {"status": "observed", "read_only": True, "manager_pod_uid": manager["metadata"]["uid"], "targets": rows}
    except Exception as error:
        kind = type(error).__name__
        result = {"status": "unavailable", "stage": stage, "error_type": kind if kind in {
            *ERRORS, "DeploymentError", "JSONDecodeError", "TypeError", "TimeoutExpired", "FileNotFoundError",
        } else "OtherError"}
        # Kubectl already sanitizes API failures. Project only known reason
        # codes; never return its message or an arbitrary exception's payload.
        if isinstance(error, DeploymentError):
            reason = str(error).rpartition(": ")[2]
            if reason in {"Forbidden", "Unauthorized", "NotFound", "BadRequest", "CommandFailed", "DeadlineExceeded"}:
                result["api_reason"] = reason
        return result


def _failed_retirement_jobs(kube: Kubectl, pods: list[dict[str, Any]], namespace_uids: dict[str, str]) -> list[dict[str, Any]]:
    """Read-only failed-Job evidence; never export log text or termination messages."""
    result: list[dict[str, Any]] = []
    inspected = 0
    for pod in sorted(pods, key=lambda p: p["metadata"].get("creationTimestamp", ""), reverse=True):
        metadata = pod["metadata"]
        namespace = metadata.get("namespace", "")
        if (not re.fullmatch(r"loom-nebius-management(?:-[a-z0-9]+(?:-[a-z0-9]+)*)?", namespace)
                or pod.get("status", {}).get("phase") != "Failed"):
            continue
        owner = next((row for row in metadata.get("ownerReferences", [])
                      if row.get("kind") == "Job" and row.get("controller") is True
                      and re.fullmatch(r"loom-retirement-[0-9a-f]{12}", row.get("name", ""))), None)
        if (owner is None or not isinstance(owner.get("uid"), str) or not owner["uid"]
                or metadata.get("labels", {}).get("loom.nebius/retirement") != owner["name"]):
            continue
        container = next((row for row in pod.get("spec", {}).get("containers", []) if row.get("command") == [
            "python", "-m", "loom_service.environment_management.retirement"]), None)
        if container is None:
            continue
        if inspected == 3:
            break
        inspected += 1
        job = kube.get("job", owner["name"], namespace)
        if job.get("metadata", {}).get("uid") != owner["uid"] or not job_failed(job):
            continue
        status: dict[str, Any] = next((row for row in pod.get("status", {}).get("containerStatuses", [])
                       if row.get("name") == container["name"]), {})
        terminated = status.get("state", {}).get("terminated", {})
        termination = {key: terminated[field] for key, field in (("exit_code", "exitCode"), ("signal", "signal"))
                       if type(terminated.get(field)) is int and 0 <= terminated[field] <= 255}
        if "reason" in terminated:
            reason = terminated["reason"]
            termination["reason"] = reason if reason in ("Error", "OOMKilled", "Completed", "ContainerCannotRun") else "Other"
        try:
            raw = kube.run("logs", metadata["name"], "-n", namespace, "-c", container["name"],
                           "--tail=50", "--limit-bytes=16384", timeout=40)
            diagnostic = _retirement_diagnostic(raw)
        except Exception:
            diagnostic = {"status": "unavailable"}
        result.append({"namespace": namespace, "job": owner["name"], "job_uid": owner["uid"],
                       "pod": metadata["name"], "pod_uid": metadata["uid"], "container": container["name"],
                       "termination": termination, "diagnostic": diagnostic})
        if diagnostic.get("status") == "retirement_blocked":
            result[-1]["registry_probe"] = _retirement_registry_probe(kube, pods, job, namespace, namespace_uids)
    return result


def _fields(value: dict[str, Any], keys: tuple[str, ...]) -> dict[str, Any]:
    return {key: value[key] for key in keys if key in value}


def _identity(item: dict[str, Any]) -> dict[str, Any]:
    return _fields(item["metadata"], ("name", "namespace", "uid"))


def _dns_service_checks(item: dict[str, Any]) -> dict[str, Any]:
    """Explain the known DNS binding without exporting labels or provider data."""
    metadata, spec = item["metadata"], item["spec"]
    if metadata.get("namespace") != "kube-system" or metadata.get("name") != "coredns":
        return {}
    selector = spec.get("selector")
    ports = spec.get("ports") or []
    return {"dns_checks": {
        "not_deleting": not bool(metadata.get("deletionTimestamp")),
        "no_owner_references": not bool(metadata.get("ownerReferences")),
        "native_selector_exact": selector == {"k8s-app": "coredns"},
        "native_selector_required": isinstance(selector, dict) and selector.get("k8s-app") == "coredns",
        "legacy_selector_required": isinstance(selector, dict) and selector.get("k8s-app") == "kube-dns",
        "cluster_ip_usable": bool(spec.get("clusterIP")) and spec["clusterIP"] != "None",
        "tcp_53": any(row.get("protocol") == "TCP" and row.get("port") == 53 for row in ports),
        "udp_53": any(row.get("protocol") == "UDP" and row.get("port") == 53 for row in ports),
    }}


def _resources(value: dict[str, Any]) -> dict[str, Any]:
    return _fields(value, RESOURCE_KEYS)


def _list(kube: Kubectl, kind: str, *, namespaced: bool = False) -> list[dict[str, Any]]:
    value = json.loads(kube.run("get", kind, *(["--all-namespaces"] if namespaced else []), "-o", "json"))
    # Missing/failed inventory is not an empty list. Never expose API diagnostics.
    items = value.get("items")
    if (not isinstance(items, list) or value.get("metadata", {}).get("continue")
            or any(not isinstance(item, dict) or "metadata" not in item for item in items)):
        raise DeploymentError("incomplete resource inventory")
    return items


def _containers(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{"name": item["name"], "requests": _resources(item.get("resources", {}).get("requests", {})),
             **_fields(item, ("restartPolicy",))} for item in items]


def _container_state(value: Any) -> dict[str, Any]:
    kinds = [key for key in ("waiting", "running", "terminated")
             if isinstance(value, dict) and isinstance(value.get(key), dict)]
    if len(kinds) != 1:
        return {"state": "unknown"}
    kind, = kinds
    result: dict[str, Any] = {"state": kind}
    if kind == "running":
        return result
    reason = value[kind].get("reason")
    known = {"PodInitializing", "ContainerCreating", "CreateContainerConfigError", "CreateContainerError",
             "RunContainerError", "ErrImagePull", "ImagePullBackOff", "InvalidImageName", "CrashLoopBackOff",
             "Error", "OOMKilled", "Completed", "ContainerCannotRun", "StartError"}
    result["reason"] = reason if isinstance(reason, str) and reason in known else "Other"
    exit_code = value[kind].get("exitCode")
    if kind == "terminated" and type(exit_code) is int and 0 <= exit_code < 2**31:
        result["exit_code"] = exit_code
    return result


def _container_statuses(items: Any, *, names: set[str]) -> list[dict[str, Any]]:
    """Use only existing Pod reads; never export messages, IDs or arbitrary reasons."""
    if not isinstance(items, list):
        return []
    result = []
    for row in items:
        if (not isinstance(row, dict) or not isinstance(row.get("name"), str)
                or row["name"] not in names):
            continue
        ready, restarts = row.get("ready"), row.get("restartCount")
        result.append({"name": row["name"], "ready": ready if type(ready) is bool else None,
            "restart_count": restarts if type(restarts) is int and 0 <= restarts < 2**31 else None,
            "current": _container_state(row.get("state")), "previous": _container_state(row.get("lastState"))})
    return result


def _storage_class(item: dict[str, Any]) -> dict[str, Any]:
    """Observe public driver options, never arbitrary parameter/credential data."""
    allowed = {"type": {"NETWORK_SSD", "NETWORK_SSD_IO_M3"}, "csi.storage.k8s.io/fstype": {"ext4", "xfs"}}
    raw = item.get("parameters", {})
    known = item.get("provisioner") == "compute.csi.nebius.com" and isinstance(raw, dict)
    parameters = {key: value for key, value in raw.items()
                  if isinstance(value, str) and value in allowed.get(key, set())} if known else {}
    return {**_identity(item), **_fields(item, ("provisioner", "reclaimPolicy", "volumeBindingMode")),
            "parameters": parameters, "parameters_complete": known and len(parameters) == len(raw)}


def inspect(kube: Kubectl, *, namespace: str, expected_cluster_id: str) -> dict[str, Any]:
    if not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", namespace):
        raise DeploymentError("invalid namespace")
    data = kube.get("configmap", "loom-platform-config", namespace)["data"]
    config, profile = json.loads(data["environment.json"]), json.loads(data["profile.json"])
    if config["namespace"] != namespace:
        raise DeploymentError("configured namespace does not match selected target")
    verify_cluster_identity(kube, config, expected_cluster_id)
    candidate = profile["candidate_sha"]
    if not isinstance(candidate, str) or not re.fullmatch(r"[0-9a-f]{40}", candidate):
        raise DeploymentError("invalid installed candidate identity")

    nodes = _list(kube, "nodes")
    namespaces = _list(kube, "namespaces")
    pods = _list(kube, "pods", namespaced=True)
    services = _list(kube, "services", namespaced=True)
    ingresses = _list(kube, "ingresses", namespaced=True)
    ingress_classes = _list(kube, "ingressclasses")
    volumes = _list(kube, "persistentvolumeclaims", namespaced=True)
    storage_classes = _list(kube, "storageclasses")
    # All cluster Pods count for platform sizing, including system/foreign Pods.
    # Preserve requests as declared, including init/sidecar/overhead, without
    # claiming an available budget from a naive sum or instantaneous usage.
    return {
        "schema_version": "loom.nebius-management-preflight.v1",
        "status": "observed", "observed_at": datetime.now(UTC).isoformat(),
        "cluster_id": expected_cluster_id, "namespace": namespace,
        "execution_namespace": config["execution_namespace"], "configured_candidate_sha": candidate,
        "failed_bootstrap_jobs": _failed_bootstrap_jobs(kube, pods, namespace),
        "failed_refresh_probes": failed_refresh_probes(kube, pods, namespace),
        "failed_retirement_jobs": _failed_retirement_jobs(kube, pods,
            {row["metadata"]["name"]: row["metadata"]["uid"] for row in namespaces}),
        "ingress_preflight": inspect_ingress(kube, os.environ.get("NEBIUS_INGRESS_INSTALLATION_JSON", ""),
                                             namespace=namespace, expected_cluster_id=expected_cluster_id),
        "public_host": config["public_host"],
        "configured_execution_node_group_id": config["execution_node_group_id"],
        "controller_inventory": controller_inventory(kube, lambda kind, namespaced: _list(kube, kind, namespaced=namespaced)),
        "nodes": [{**_identity(item), "role": item["metadata"].get("labels", {}).get("loom.nebius/node-role"),
            "provider_id": item.get("spec", {}).get("providerID"),
            "unschedulable": item.get("spec", {}).get("unschedulable", False),
            "allocatable": _resources(item.get("status", {}).get("allocatable", {})),
            "ready": any(condition.get("type") == "Ready" and condition.get("status") == "True"
                         for condition in item.get("status", {}).get("conditions", [])),
            "taints": [_fields(taint, ("key", "value", "effect")) for taint in item.get("spec", {}).get("taints", [])],
        } for item in nodes],
        "namespaces": [_identity(item) for item in namespaces],
        "pods": [{**_identity(item), "node": item["spec"].get("nodeName"),
            "phase": item.get("status", {}).get("phase"),
            "containers": _containers(item["spec"].get("containers", [])),
            "init_containers": _containers(item["spec"].get("initContainers", [])),
            "container_statuses": _container_statuses(item.get("status", {}).get("containerStatuses"),
                names={row["name"] for row in item["spec"].get("containers", [])}),
            "init_container_statuses": _container_statuses(item.get("status", {}).get("initContainerStatuses"),
                names={row["name"] for row in item["spec"].get("initContainers", [])}),
            "overhead": _resources(item["spec"].get("overhead", {})),
            "pod_requests": _resources(item["spec"].get("resources", {}).get("requests", {})),
        } for item in pods],
        "services": [{**_identity(item), **_dns_service_checks(item), "type": item["spec"].get("type"),
            "load_balancer": [_fields(address, ("ip", "hostname")) for address in
                              item.get("status", {}).get("loadBalancer", {}).get("ingress", [])],
        } for item in services],
        "ingresses": [{**_identity(item), "class": item["spec"].get("ingressClassName"),
            "hosts": [rule.get("host") for rule in item["spec"].get("rules", [])],
            "tls_hosts": [host for tls in item["spec"].get("tls", []) for host in tls.get("hosts", [])],
        } for item in ingresses],
        "ingress_classes": [{**_identity(item), "controller": item["spec"]["controller"]} for item in ingress_classes],
        "volumes": [{**_identity(item), "storage_class": item["spec"].get("storageClassName"),
            "requested_storage": item["spec"].get("resources", {}).get("requests", {}).get("storage"),
            "phase": item.get("status", {}).get("phase"),
        } for item in volumes],
        "storage_classes": [_storage_class(item) for item in storage_classes],
        "unverified": ["running_candidate_correspondence", "provider_iam", "wildcard_dns_tls", "management_installation", "platform_child_allowance",
                       "live_nebius_pool_limits_and_quota", "concurrent_owner_acceptance"],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kubeconfig", type=Path, required=True)
    parser.add_argument("--expected-cluster-id", required=True)
    parser.add_argument("--namespace", default="loom-nebius-platform")
    parser.add_argument("--evidence-dir", type=Path, required=True)
    parser.add_argument('--prepare-shared-inputs', action='store_true',
        help='Retain required shared inputs privately on the gateway; no cluster changes')
    args = parser.parse_args()
    try:
        result = inspect(Kubectl(args.kubeconfig), namespace=args.namespace, expected_cluster_id=args.expected_cluster_id)
        if args.prepare_shared_inputs:
            result['shared_input_observation'] = prepare_shared_inputs(result, kubeconfig=args.kubeconfig)
    except Exception as exc:
        # Never print raw config, kubeconfig, API errors or exception messages.
        result = {"schema_version": "loom.nebius-management-preflight.v1", "status": "blocked",
                  "error_type": type(exc).__name__}
    args.evidence_dir.mkdir(parents=True, exist_ok=True)
    (args.evidence_dir / "management-preflight.json").write_text(json.dumps(result, sort_keys=True) + "\n")
    print(json.dumps({"status": result["status"]}))
    return 0 if result["status"] == "observed" else 1


def prepare_shared_inputs(report: dict[str, Any], *, kubeconfig: Path) -> dict[str, str]:
    """Run the fixed collector remotely so no Secret payload enters Actions."""
    target = os.environ['LOOM_DEPLOY_SSH_TARGET']
    if not re.fullmatch(r'[a-zA-Z0-9_.-]+@[a-zA-Z0-9_.-]+', target):
        raise DeploymentError('invalid observation SSH target')
    identities = {row['name']: row['uid'] for row in report['namespaces']}
    arguments = ['python3', '-', '--kubeconfig', str(kubeconfig), '--cluster-id', report['cluster_id'],
        '--namespace', report['namespace'], '--namespace-uid', identities[report['namespace']],
        '--kube-system-uid', identities['kube-system']]
    command = ['ssh', '-o', 'BatchMode=yes', '-o', 'StrictHostKeyChecking=yes', '-o', 'IdentitiesOnly=yes',
        '-o', 'ConnectTimeout=15', '-o', 'ServerAliveInterval=15', '-o', 'ServerAliveCountMax=3',
        '-o', 'UserKnownHostsFile=' + os.environ['LOOM_DEPLOY_SSH_KNOWN_HOSTS_FILE'],
        '-i', os.environ['LOOM_DEPLOY_SSH_KEY_FILE'], target, shlex.join(arguments)]
    result = subprocess.run(command, input=Path(__file__).with_name('nebius_application_snapshot.py').read_text(),
        capture_output=True, text=True, timeout=240, check=False)
    if result.returncode or len(result.stdout) > 4096:
        raise DeploymentError('shared input observation unavailable')
    value = json.loads(result.stdout)
    if (not isinstance(value, dict) or set(value) != {'status', 'observation_id', 'candidate_sha'}
            or value['status'] != 'shared_inputs_observed' or str(UUID(value['observation_id'])) != value['observation_id']
            or not UUID(value['observation_id']).int or not re.fullmatch(r'[0-9a-f]{40}', value['candidate_sha'])):
        raise DeploymentError('shared input observation unqualified')
    return {key: str(value[key]) for key in ('status', 'observation_id', 'candidate_sha')}


if __name__ == "__main__":
    raise SystemExit(main())
