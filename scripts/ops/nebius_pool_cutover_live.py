"""Fixed HTTPS transport for the connected closed-runtime cutover.

No CLI, activation, deletion, ambient credential discovery or HTTP write retry.
The protected entry supplies independently qualified publication/predecessor/
backend checks and the original database-bound guard adapter.
"""
from __future__ import annotations

import copy
import json
import ssl
from typing import Any, Protocol
from uuid import UUID

from scripts.ops.nebius_ingress_stage import _key, _snapshot, _uid
from scripts.ops.nebius_management_material import ManagementBinding
from scripts.ops.nebius_management_stage import (
    _MARKER,
    HTTPSManagementStageAPI,
    _qualified_defaulted,
)
from scripts.ops.nebius_management_switch import _matches, _stable
from scripts.ops.nebius_management_transport import ManagementKubernetesTransport
from scripts.ops.nebius_pool_cutover import PoolCutoverRequest, _contract, cutover_documents
from scripts.ops.nebius_pool_material import machine_documents
from scripts.ops.nebius_pool_migration import (
    PoolGuardTarget,
    PoolMigrationAPI,
    PoolMigrationRequest,
)
from scripts.ops.nebius_pool_migration_guard import qualify_cutover_readiness_page
from scripts.ops.nebius_pool_retirement import (
    qualify_closed_workload_drain,
    retirement_documents,
    stopped_document,
)
from scripts.ops.nebius_pool_retirement_live import HTTPSPoolRetirementAPI
from scripts.ops.nebius_pool_role_fencing_live import HTTPSPoolRoleFenceAPI

from loom.nebius_platform_render import digest
from loom.nebius_pool_priority import PoolWorkOriginV1


class PoolCutoverChecks(Protocol):
    def preflight(self, request: PoolCutoverRequest) -> None: ...
    def qualify_quiescence(self) -> None: ...


class PoolCutoverHistory(Protocol):
    def qualify_binding(self, request: PoolMigrationRequest, manager: dict[str, Any]) -> None: ...
    def qualify_pending_origins(self, target: PoolGuardTarget, origins: tuple[PoolWorkOriginV1, ...]) -> None:
        """Qualify retained management registration/history, not just JSON shape."""
        ...


class PoolCutoverGuards(Protocol):
    request: PoolMigrationRequest
    def guard(self, target: PoolGuardTarget, action: str) -> dict[str, Any]: ...
    def runtime_role(self, target: PoolGuardTarget, action: str) -> dict[str, Any]: ...
    def cutover_readiness_page(self, target: PoolGuardTarget, *, after: str | None) -> dict[str, Any]: ...


class HTTPSPoolCutoverAPI(HTTPSManagementStageAPI):
    def __init__(self, *, request: PoolCutoverRequest, tokens: dict[UUID, str], migration: PoolMigrationAPI,
                 guards: PoolCutoverGuards, checks: PoolCutoverChecks, history: PoolCutoverHistory, api_server: str,
                 ssl_context: ssl.SSLContext, token: str | None = None):
        registration = request.fencing.retirement.migration.registration
        if guards.request != request.fencing.retirement.migration:
            raise ValueError("pool cutover guard binding differs")
        history.qualify_binding(guards.request, request.manager)
        self.request, self.migration, self.guards, self.checks = request, migration, guards, checks
        self.history = history
        self.binding = registration.binding
        self.catalog = cutover_documents(request)
        self.contract_sha256 = digest(_contract(request, self.catalog))
        self.originals = {**retirement_documents(request.fencing.retirement), **self.catalog["producers"]}
        self.documents = {**machine_documents(request.fencing.retirement.migration, tokens),
            **{_key(row): row for phase in ("configuration", "authority", "workload") for row in self.catalog[phase]}}
        self.namespaces = {row.namespace: str(row.namespace_uid) for row in guards.request.guards}
        self.namespaces.update({ns.name: str(ns.uid) for row in registration.spec.participants
            for ns in (row.execution_namespace, row.build_namespace)})
        ManagementKubernetesTransport.__init__(self, api_server=api_server, ssl_context=ssl_context, token=token)
        self.retirement = HTTPSPoolRetirementAPI(request=request.fencing.retirement, guards=guards,
            api_server=api_server, ssl_context=ssl_context, token=token)
        self.fencing = HTTPSPoolRoleFenceAPI(request=request.fencing, retirement=self.retirement,
            api_server=api_server, ssl_context=ssl_context, token=token)
        self.resources = self

    def __exit__(self, *_args: object) -> None:
        self.client.close()
        self.retirement.client.close()
        self.fencing.client.close()

    def verify_identity(self, binding: ManagementBinding) -> None:
        super().verify_identity(binding)
        for name, uid in self.namespaces.items():
            namespace = self._request("GET", "/api/v1/namespaces/" + name)
            if (namespace is None or namespace.get("apiVersion") != "v1" or namespace.get("kind") != "Namespace"
                    or namespace["metadata"].get("name") != name or _uid(namespace) != uid):
                raise ValueError("pool cutover namespace differs")
            _snapshot(namespace)

    def _scope(self) -> None:
        if self.contract_sha256 != digest(_contract(self.request, cutover_documents(self.request))):
            raise ValueError("pool cutover inputs changed")
        self.verify_identity(self.binding)

    def _approved(self, document: dict[str, Any], *, writing: bool = False) -> str:
        try:
            value = copy.deepcopy(document)
            expected = self.documents[_key(value)]
            annotations = value["metadata"].get("annotations", {})
            operation = annotations.pop(_MARKER, None)
            if writing or operation is not None:
                if str(UUID(operation)) != operation or not UUID(operation).int:
                    raise ValueError
            if not annotations and "annotations" not in expected["metadata"]:
                value["metadata"].pop("annotations", None)
            if value != expected:
                raise ValueError
            version, resource, namespaced = {
                "Secret": ("v1", "secrets", True), "ConfigMap": ("v1", "configmaps", True),
                "ServiceAccount": ("v1", "serviceaccounts", True), "Deployment": ("apps/v1", "deployments", True),
                "Role": ("rbac.authorization.k8s.io/v1", "roles", True),
                "RoleBinding": ("rbac.authorization.k8s.io/v1", "rolebindings", True),
                "ClusterRole": ("rbac.authorization.k8s.io/v1", "clusterroles", False),
                "ClusterRoleBinding": ("rbac.authorization.k8s.io/v1", "clusterrolebindings", False),
            }[value["kind"]]
            if value["apiVersion"] != version:
                raise ValueError
            prefix = "/api/v1" if version == "v1" else "/apis/" + version
            return prefix + ("/namespaces/" + value["metadata"]["namespace"] if namespaced else "") + "/" + resource
        except Exception:
            raise ValueError("resource outside fixed pool cutover stage") from None

    def preflight(self, request: PoolCutoverRequest) -> None:
        if request != self.request:
            raise ValueError("pool cutover request differs")
        self._scope()
        self.checks.preflight(request)

    def qualify_quiescence(self) -> None:
        self._scope()
        for target in self.guards.request.guards:
            participant, = (row for row in self.guards.request.registration.spec.participants
                if row.participant_id == target.participant_id)
            after = None
            while True:
                report = self.guards.cutover_readiness_page(target, after=after)
                origins = qualify_cutover_readiness_page(report, participant=participant, after=after)
                self.history.qualify_pending_origins(target, origins)
                if len(report["rows"]) < 128:
                    break
                after = report["rows"][-1]["key"]
        self.checks.qualify_quiescence()

    def qualify_runtime_access(self, participant_id: UUID, action: str) -> None:
        self._scope()
        guard, = (row for row in self.guards.request.guards if row.participant_id == participant_id)
        if action not in {"stage", "observe"} or self.guards.runtime_role(guard, action) != {"status": "staged" if action == "stage" else "qualified"}:
            raise ValueError("pool cutover runtime role unqualified")

    def _workload_path(self, key: str) -> str:
        original = self.originals[key]
        resource = "cronjobs" if original["kind"] == "CronJob" else "deployments"
        return "/apis/" + str(original["apiVersion"]) + "/namespaces/" + str(original["metadata"]["namespace"]) + "/" + resource + "/" + str(original["metadata"]["name"])

    def read_workload(self, key: str) -> dict[str, Any]:
        self._scope()
        original = self.originals[key]
        actual = self._request("GET", self._workload_path(key))
        if (actual is None or _uid(actual) != _uid(original)
                or any(actual.get(field) != original[field] for field in ("apiVersion", "kind"))
                or any(actual["metadata"].get(field) != original["metadata"][field] for field in ("namespace", "name"))):
            raise ValueError("pool cutover workload identity differs")
        _snapshot(actual)
        return actual

    def _patch(self, key: str, before: dict[str, Any], desired: dict[str, Any], *, preview: bool) -> dict[str, Any] | None:
        try:
            original = self.originals[key]
            freeze = key in self.catalog["stopped"] and desired == self.catalog["stopped"][key]
            expected = (original if freeze else self.catalog["stopped"][key] if key in self.catalog["stopped"]
                else stopped_document(self.request.fencing.retirement, key))
            if ((not freeze and desired != self.catalog["runtime"].get(key))
                    or not _matches(before, expected, _uid(original))):
                raise ValueError
            version = before["metadata"]["resourceVersion"]
            if not isinstance(version, str) or not 0 < len(version) <= 128:
                raise ValueError
            self._scope()
            if not freeze:
                if any(self.guards.guard(target, "observe").get("status") != "held" for target in self.guards.request.guards):
                    raise ValueError
                self.fencing.verify_readonly()
            patches = [{"op": "test", "path": "/metadata/uid", "value": _uid(original)},
                {"op": "test", "path": "/metadata/resourceVersion", "value": version},
                {"op": "test", "path": "/spec", "value": before["spec"]},
                {"op": "add", "path": "/metadata/annotations", "value": desired["metadata"]["annotations"]},
                {"op": "replace", "path": "/spec", "value": desired["spec"]}]
            with self.client.stream("PATCH", self._workload_path(key) + ("?dryRun=All" if preview else ""),
                    json=patches, headers={"Content-Type": "application/json-patch+json"}) as response:
                if response.status_code not in {200, 409, 422} or response.headers.get("content-encoding", "identity").lower() != "identity":
                    raise ValueError
                content = bytearray()
                for chunk in response.iter_bytes(chunk_size=16384):
                    if len(content) + len(chunk) > 4 * 1024**2:
                        raise ValueError
                    content.extend(chunk)
                value = json.loads(content)
                if not isinstance(value, dict):
                    raise ValueError
                if response.status_code in {409, 422}:
                    if (value.get("apiVersion") != "v1" or value.get("kind") != "Status" or value.get("status") != "Failure"
                            or value.get("code") != response.status_code or value.get("reason") != {409: "Conflict", 422: "Invalid"}[response.status_code]):
                        raise ValueError
                    return None
                if _uid(value) != _uid(original):
                    raise ValueError
                _qualified_defaulted(desired, value)
                return value
        except Exception:
            raise ValueError("pool cutover workload update unconfirmed") from None

    def preview_workload(self, key: str, before: dict[str, Any], desired: dict[str, Any]) -> dict[str, Any]:
        value = self._patch(key, before, desired, preview=True)
        if value is None:
            raise ValueError("pool cutover workload preview rejected")
        return value

    def patch_workload(self, key: str, before: dict[str, Any], desired: dict[str, Any]) -> bool:
        return self._patch(key, before, desired, preview=False) is not None

    def drained_workload(self, key: str, desired: dict[str, Any]) -> bool:
        current = self.read_workload(key)
        namespace = current["metadata"]["namespace"]
        prefix = ("/apis/apps/v1/namespaces/" + namespace + "/replicasets" if current["kind"] == "Deployment"
            else "/apis/batch/v1/namespaces/" + namespace + "/jobs")
        children = self._request("GET", prefix + "?limit=1000")
        pods = self._request("GET", "/api/v1/namespaces/" + namespace + "/pods?limit=1000")
        if children is None or pods is None or _stable(self.read_workload(key)) != _stable(current):
            raise ValueError("pool cutover drain changed during observation")
        return qualify_closed_workload_drain(original=self.originals[key], desired=desired,
            current=current, children=children, pods=pods)
