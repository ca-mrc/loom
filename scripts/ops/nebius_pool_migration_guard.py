"""Fixed idle-guard commands in an exact retained participant controller Pod.

Callable only by the protected migration, not an operator CLI. The parent owns
publication/predecessor qualification. This adapter neither releases intake nor
changes controller or Kubernetes authority. Ambiguous exec outcomes only read back.
"""
from __future__ import annotations

import hashlib
import os
import re
import subprocess
from pathlib import Path
from typing import Any

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_ingress_stage import _snapshot, _uid
from scripts.ops.nebius_management_evidence import _matches_backup_template
from scripts.ops.nebius_pool_migration import (
    PoolGuardTarget,
    PoolMigrationError,
    PoolMigrationRequest,
    migration_contract,
)

from loom.nebius_platform_render import digest
from loom_service.environment_management.candidates import _json


class KubectlPoolGuardAPI:
    def __init__(self, *, request: PoolMigrationRequest, kubeconfig: Path, executable: Path):
        try:
            if (not kubeconfig.is_absolute() or kubeconfig != kubeconfig.resolve()
                    or not executable.is_absolute()):
                raise ValueError
            self.request = request
            self.contract_sha256 = digest(migration_contract(request))
            self.kubeconfig = kubeconfig
            self.kubeconfig_sha256 = hashlib.sha256(private_state._private_read(kubeconfig, limit=512 * 1024)).hexdigest()
            cache = kubeconfig.parent / ".loom-pool-kubectl-cache"
            private_state._private_directory(cache)
            self.prefix = [str(executable), "--kubeconfig", str(kubeconfig), "--request-timeout=30s", "--cache-dir", str(cache)]
        except Exception:
            raise PoolMigrationError("guard_configuration") from None

    def _run(self, args: list[str]) -> dict[str, Any]:
        value = subprocess.run([*self.prefix, *args], capture_output=True, timeout=40, check=False,
            env={"PATH": os.defpath, "LANG": "C.UTF-8"})
        if value.returncode or len(value.stdout) > 4 * 1024**2:
            raise ValueError
        result = _json(value.stdout)
        if not isinstance(result, dict):
            raise ValueError
        return result

    def _get(self, kind: str, name: str, namespace: str | None = None) -> dict[str, Any]:
        return self._run(["get", kind, name, *(["-n", namespace] if namespace else []), "-o", "json"])

    def _namespaces(self, target: PoolGuardTarget) -> None:
        for name, uid in (("kube-system", self.request.registration.binding.kube_system_uid),
                (target.namespace, str(target.namespace_uid))):
            namespace = self._get("namespace", name)
            if (namespace.get("apiVersion") != "v1" or namespace.get("kind") != "Namespace"
                    or namespace["metadata"].get("name") != name or _uid(namespace) != uid):
                raise ValueError
            _snapshot(namespace)  # Reject deletion or a foreign owner.

    @staticmethod
    def _owner(document: dict[str, Any], *, kind: str, name: str, uid: str) -> None:
        owners = document["metadata"].get("ownerReferences", [])
        if len(owners) != 1:
            raise ValueError
        actual = dict(owners[0])
        actual.pop("blockOwnerDeletion", None)
        if actual != {"apiVersion": "apps/v1", "kind": kind, "name": name, "uid": uid, "controller": True}:
            raise ValueError

    def _runtime(self, target: PoolGuardTarget) -> dict[str, Any]:
        self._namespaces(target)
        original = target.controller
        controller = self._get("deployment", "loom-control-plane", target.namespace)
        if _uid(controller) != _uid(original) or _snapshot(controller) != _snapshot(original):
            raise ValueError
        status = controller.get("status", {})
        if (status.get("observedGeneration", 0) < controller["metadata"].get("generation", 1)
                or any(type(status.get(key)) is not int or status[key] != 1
                    for key in ("replicas", "updatedReplicas", "availableReplicas", "readyReplicas"))):
            raise ValueError
        listing = self._run(["get", "pods", "-n", target.namespace, "-l", "app=loom-control-plane", "--chunk-size=100", "-o", "json"])
        if (listing.get("apiVersion") != "v1" or listing.get("kind") != "PodList"
                or listing.get("metadata", {}).get("continue") or not listing.get("metadata", {}).get("resourceVersion")
                or len(listing.get("items", [])) != 1):
            raise ValueError
        pod = {"apiVersion": "v1", "kind": "Pod", **listing["items"][0]}
        meta = pod["metadata"]
        _uid(pod)
        if (pod["apiVersion"] != "v1" or pod["kind"] != "Pod" or meta.get("namespace") != target.namespace
                or meta.get("deletionTimestamp") or not re.fullmatch(r"[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?", meta["name"])
                or meta.get("labels", {}).get("app") != "loom-control-plane"):
            raise ValueError
        owners = meta.get("ownerReferences", [])
        if len(owners) != 1 or not re.fullmatch(r"[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?", owners[0]["name"]):
            raise ValueError
        replica = self._get("replicaset", owners[0]["name"], target.namespace)
        if (replica.get("apiVersion") != "apps/v1" or replica.get("kind") != "ReplicaSet"
                or replica["metadata"].get("namespace") != target.namespace
                or replica["metadata"].get("name") != owners[0]["name"] or replica["metadata"].get("deletionTimestamp")):
            raise ValueError
        self._owner(replica, kind="Deployment", name="loom-control-plane", uid=_uid(controller))
        self._owner(pod, kind="ReplicaSet", name=replica["metadata"]["name"], uid=_uid(replica))
        expected, actual = controller["spec"]["template"]["spec"], pod["spec"]
        if (not _matches_backup_template(replica["spec"]["template"]["spec"], expected)
                or not _matches_backup_template(actual, expected)
                or actual.get("securityContext", {}) != expected.get("securityContext", {})
                or actual.get("ephemeralContainers", []) != expected.get("ephemeralContainers", [])
                or actual.get("serviceAccountName", "default") != expected.get("serviceAccountName", "default")
                or any(actual.get(field, False) != expected.get(field, False)
                    for field in ("hostNetwork", "hostPID", "hostIPC", "shareProcessNamespace"))
                or len(expected["containers"]) != 1 or expected["containers"][0]["name"] != "loom-control-plane"
                or pod.get("status", {}).get("phase") != "Running"):
            raise ValueError
        for container, wanted in zip(actual["containers"], expected["containers"], strict=True):
            if (container.keys() - wanted.keys() - {"imagePullPolicy", "terminationMessagePath", "terminationMessagePolicy"}
                    or container.get("securityContext", {}) != wanted.get("securityContext", {})):
                raise ValueError
        states = pod["status"].get("containerStatuses", [])
        if len(states) != 1 or states[0].get("name") != "loom-control-plane" or states[0].get("ready") is not True:
            raise ValueError
        self._namespaces(target)
        return pod

    def guard(self, target: PoolGuardTarget, action: str) -> dict[str, Any]:
        try:
            allowed = {"observe": {"open", "held", "skipped_locked"}, "acquire": {"acquired", "skipped_busy", "skipped_locked"}}
            if (action not in allowed or target not in self.request.guards
                    or digest(migration_contract(self.request)) != self.contract_sha256
                    or hashlib.sha256(private_state._private_read(self.kubeconfig, limit=512 * 1024)).hexdigest() != self.kubeconfig_sha256):
                raise ValueError
            before = self._runtime(target)
            report = self._run(["exec", "-n", target.namespace, "pod/" + before["metadata"]["name"], "-c", "loom-control-plane", "--",
                "python", "-m", "loom.nebius_rollout_guard", action, "--owner", str(self.request.registration.spec.operation_id),
                "--candidate", self.request.registration.candidate["candidate_sha"]])
            if report.get("status") not in allowed[action] or _uid(self._runtime(target)) != _uid(before):
                raise ValueError
            return {"status": report["status"]}
        except Exception:
            raise PoolMigrationError("guard_" + action if action in {"observe", "acquire"} else "guard_scope") from None
