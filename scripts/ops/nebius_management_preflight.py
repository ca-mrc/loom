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
from scripts.ops.nebius_ingress_preflight import inspect_ingress  # noqa: E402

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


def _fields(value: dict[str, Any], keys: tuple[str, ...]) -> dict[str, Any]:
    return {key: value[key] for key in keys if key in value}


def _identity(item: dict[str, Any]) -> dict[str, Any]:
    return _fields(item["metadata"], ("name", "namespace", "uid"))


def _resources(value: dict[str, Any]) -> dict[str, Any]:
    return _fields(value, RESOURCE_KEYS)


def _list(kube: Kubectl, kind: str, *, namespaced: bool = False) -> list[dict[str, Any]]:
    value = json.loads(kube.run("get", kind, *(["--all-namespaces"] if namespaced else []), "-o", "json"))
    # Missing/failed inventory is not an empty list. Never expose API diagnostics.
    items = value.get("items")
    if not isinstance(items, list) or any(not isinstance(item, dict) or "metadata" not in item for item in items):
        raise DeploymentError("incomplete resource inventory")
    return items


def _containers(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{"name": item["name"], "requests": _resources(item.get("resources", {}).get("requests", {})),
             **_fields(item, ("restartPolicy",))} for item in items]


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
        "ingress_preflight": inspect_ingress(kube, os.environ.get("NEBIUS_INGRESS_INSTALLATION_JSON", ""),
                                             namespace=namespace, expected_cluster_id=expected_cluster_id),
        "public_host": config["public_host"],
        "configured_execution_node_group_id": config["execution_node_group_id"],
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
            "overhead": _resources(item["spec"].get("overhead", {})),
            "pod_requests": _resources(item["spec"].get("resources", {}).get("requests", {})),
        } for item in pods],
        "services": [{**_identity(item), "type": item["spec"].get("type"),
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
