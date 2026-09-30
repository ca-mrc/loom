"""Fixed idle-guard commands bound to retained controller and database Pods.

Callable only by the protected migration, not an operator CLI. The parent owns
publication/predecessor qualification. This adapter neither releases intake nor
changes controller or Kubernetes authority. Ambiguous exec outcomes only read back.
"""
from __future__ import annotations

import base64
import copy
import hashlib
import os
import re
import subprocess
from pathlib import Path
from typing import Any

from scripts.ops import nebius_certificates as private_state
from scripts.ops.deploy_nebius_platform import rollout_guard_observation_sql
from scripts.ops.nebius_ingress_stage import _snapshot, _uid
from scripts.ops.nebius_management_evidence import _matches_backup_template
from scripts.ops.nebius_pool_migration import (
    PoolGuardTarget,
    PoolMigrationError,
    PoolMigrationRequest,
    migration_contract,
)
from sqlalchemy.engine import make_url

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

    def _pods(self, namespace: str, app: str) -> dict[str, Any]:
        # kubectl's generic list printer drops resourceVersion/continuation.
        # Read the fixed API collection so completeness remains verifiable.
        return self._run(["get", "--raw", f"/api/v1/namespaces/{namespace}/pods?labelSelector=app%3D{app}&limit=100"])

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
        listing = self._pods(target.namespace, "loom-control-plane")
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
                or actual.get("initContainers", []) != expected.get("initContainers", [])
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

    def _database(self, target: PoolGuardTarget) -> dict[str, Any]:
        """Bind a read to the original controller's namespace-local database."""
        self._namespaces(target)
        binding = target.database
        if binding is None:
            raise ValueError
        containers = target.controller["spec"]["template"]["spec"]["containers"]
        if len(containers) != 1 or containers[0].get("envFrom"):
            raise ValueError
        environment = containers[0]["env"]
        if len({row["name"] for row in environment}) != len(environment):
            raise ValueError
        entry, = (row for row in environment if row["name"] == "LOOM_CP_DB_URL")
        if set(entry) != {"name", "valueFrom"} or set(entry["valueFrom"]) != {"secretKeyRef"}:
            raise ValueError
        reference = entry["valueFrom"]["secretKeyRef"]
        if (reference.keys() - {"name", "key", "optional"} or reference.get("optional", False) is not False
                or any(not isinstance(reference[key], str) or not re.fullmatch(r"[a-zA-Z0-9._-]{1,253}", reference[key])
                    for key in ("name", "key"))):
            raise ValueError
        secret = self._get("secret", reference["name"], target.namespace)
        if (secret.get("apiVersion") != "v1" or secret.get("kind") != "Secret"
                or secret["metadata"].get("namespace") != target.namespace or secret["metadata"].get("name") != reference["name"]
                or _uid(secret) != str(binding.credential_uid)
                or secret["metadata"].get("resourceVersion") != binding.credential_resource_version
                or secret["metadata"].get("deletionTimestamp") or secret["metadata"].get("ownerReferences")):
            raise ValueError
        url = make_url(base64.b64decode(secret["data"][reference["key"]], validate=True).decode())
        if (url.drivername not in {"postgresql", "postgresql+psycopg", "postgresql+asyncpg"}
                or url.host != f"loom-postgres.{target.namespace}.svc" or url.port not in (None, 5432)
                or url.database != "loom" or not url.username or not url.password
                or url.query.keys() - {"sslmode", "sslrootcert", "sslcert", "sslkey", "connect_timeout", "application_name"}
                or any(not isinstance(value, str) for value in url.query.values())):
            raise ValueError
        database = self._get("statefulset", "loom-postgres", target.namespace)
        service = self._get("service", "loom-postgres", target.namespace)
        for actual, wanted in ((database, binding.statefulset), (service, binding.service)):
            if _uid(actual) != _uid(wanted) or _snapshot(actual) != _snapshot(wanted):
                raise ValueError
        spec, status = database["spec"], database.get("status", {})
        if (type(spec.get("replicas")) is not int or spec["replicas"] != 1
                or spec.get("serviceName") != "loom-postgres" or spec.get("ordinals", {}).get("start", 0) != 0
                or spec["selector"] != {"matchLabels": {"app": "loom-postgres"}}
                or service["spec"]["selector"] != {"app": "loom-postgres"}
                or service["spec"].get("type", "ClusterIP") != "ClusterIP"
                or len(service["spec"]["ports"]) != 1 or service["spec"]["ports"][0]["port"] != 5432
                or service["spec"]["ports"][0]["targetPort"] != 5432
                or status.get("observedGeneration", 0) < database["metadata"].get("generation", 1)
                or any(type(status.get(key)) is not int or status[key] != 1
                    for key in ("replicas", "readyReplicas", "currentReplicas", "updatedReplicas"))
                or not status.get("currentRevision") or status["currentRevision"] != status.get("updateRevision")):
            raise ValueError
        listing = self._pods(target.namespace, "loom-postgres")
        if (listing.get("apiVersion") != "v1" or listing.get("kind") != "PodList"
                or listing.get("metadata", {}).get("continue") or not listing.get("metadata", {}).get("resourceVersion")
                or len(listing.get("items", [])) != 1):
            raise ValueError
        pod = {"apiVersion": "v1", "kind": "Pod", **listing["items"][0]}
        meta = pod["metadata"]
        _uid(pod)
        if (pod["apiVersion"] != "v1" or pod["kind"] != "Pod" or meta.get("namespace") != target.namespace
                or meta.get("name") != "loom-postgres-0" or meta.get("deletionTimestamp")
                or meta.get("labels", {}).get("app") != "loom-postgres"
                or meta["labels"].get("controller-revision-hash") != status["currentRevision"]):
            raise ValueError
        self._owner(pod, kind="StatefulSet", name="loom-postgres", uid=_uid(database))
        expected = copy.deepcopy(spec["template"]["spec"])
        claims = spec.get("volumeClaimTemplates", [])
        if len(claims) != 1 or claims[0]["metadata"].get("name") != "data":
            raise ValueError
        expected.setdefault("volumes", []).append({"name": "data", "persistentVolumeClaim": {"claimName": "data-loom-postgres-0"}})
        actual = pod["spec"]
        # StatefulSet may order its injected PVC volume before other volumes.
        actual = {**actual, "volumes": sorted(actual.get("volumes", []), key=lambda row: row["name"])}
        expected["volumes"].sort(key=lambda row: row["name"])
        if (not _matches_backup_template(actual, expected)
                or actual.get("initContainers", []) != expected.get("initContainers", [])
                or actual.get("securityContext", {}) != expected.get("securityContext", {})
                or actual.get("ephemeralContainers", []) != expected.get("ephemeralContainers", [])
                or actual.get("serviceAccountName", "default") != expected.get("serviceAccountName", "default")
                or any(actual.get(field, False) != expected.get(field, False)
                    for field in ("hostNetwork", "hostPID", "hostIPC", "shareProcessNamespace"))
                or len(expected["containers"]) != 1 or expected["containers"][0]["name"] != "loom-postgres"
                or pod.get("status", {}).get("phase") != "Running"):
            raise ValueError
        container, wanted = actual["containers"][0], expected["containers"][0]
        if (container.keys() - wanted.keys() - {"imagePullPolicy", "terminationMessagePath", "terminationMessagePolicy"}
                or container.get("securityContext", {}) != wanted.get("securityContext", {})):
            raise ValueError
        states = pod["status"].get("containerStatuses", [])
        if len(states) != 1 or states[0].get("name") != "loom-postgres" or states[0].get("ready") is not True:
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
            database = self._database(target) if target.database is not None else None
            if action == "observe" and database is not None:
                query = rollout_guard_observation_sql(owner=str(self.request.registration.spec.operation_id),
                    candidate=self.request.registration.candidate["candidate_sha"])
                report = self._run(["exec", "-n", target.namespace, "pod/" + database["metadata"]["name"], "-c", "loom-postgres", "--",
                    "psql", "-X", "-qAt", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", "loom", "-c", query])
                if set(report) != {"status"} or report["status"] not in allowed[action] or _uid(self._database(target)) != _uid(database):
                    raise ValueError
                return {"status": report["status"]}
            before = self._runtime(target)
            report = self._run(["exec", "-n", target.namespace, "pod/" + before["metadata"]["name"], "-c", "loom-control-plane", "--",
                "python", "-m", "loom.nebius_rollout_guard", action, "--owner", str(self.request.registration.spec.operation_id),
                "--candidate", self.request.registration.candidate["candidate_sha"]])
            if report.get("status") not in allowed[action] or _uid(self._runtime(target)) != _uid(before):
                raise ValueError
            if database is not None and _uid(self._database(target)) != _uid(database):
                raise ValueError
            return {"status": report["status"]}
        except Exception:
            raise PoolMigrationError("guard_" + action if action in {"observe", "acquire"} else "guard_scope") from None
