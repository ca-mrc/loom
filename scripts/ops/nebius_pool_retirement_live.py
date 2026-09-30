"""Fixed HTTPS stop/read transport for retained pool controller identities.

No create/delete/activate surface, ambient credentials or automatic HTTP retry.
Guard observation uses the existing qualified original-CP adapter before each
first stop; drain readback no longer needs the retired CP to remain running.
"""
from __future__ import annotations

import json
import ssl
from typing import Any, Protocol

from scripts.ops.nebius_ingress_stage import _snapshot, _uid
from scripts.ops.nebius_management_stage import HTTPSManagementStageAPI
from scripts.ops.nebius_management_switch import _matches, _stable
from scripts.ops.nebius_management_transport import ManagementKubernetesTransport
from scripts.ops.nebius_pool_migration import PoolGuardTarget, migration_contract
from scripts.ops.nebius_pool_retirement import (
    PoolRetirementRequest,
    qualify_pool_drain,
    retirement_documents,
    stopped_document,
)

from loom.nebius_platform_render import digest


class PoolGuardObserver(Protocol):
    def guard(self, target: PoolGuardTarget, action: str) -> dict[str, Any]: ...


class HTTPSPoolRetirementAPI(HTTPSManagementStageAPI):
    def __init__(self, *, request: PoolRetirementRequest, guards: PoolGuardObserver, api_server: str,
                 ssl_context: ssl.SSLContext, token: str | None = None):
        self.request, self.guards = request, guards
        self.originals = retirement_documents(request)
        self.binding = request.migration.registration.binding
        self.documents = {}  # Inherited generic stage create methods have no approved resources.
        self.contract_sha256 = digest({"migration": migration_contract(request.migration), "originals": self.originals})
        self.namespaces = {row.namespace: str(row.namespace_uid) for row in request.migration.guards}
        self.namespaces.update({row.execution_namespace.name: str(row.execution_namespace.uid) for row in request.migration.registration.spec.participants})
        ManagementKubernetesTransport.__init__(self, api_server=api_server, ssl_context=ssl_context, token=token)

    def _scope(self) -> None:
        if self.contract_sha256 != digest({"migration": migration_contract(self.request.migration), "originals": retirement_documents(self.request)}):
            raise ValueError("pool retirement input changed")
        self.verify_identity(self.binding)
        for name, uid in self.namespaces.items():
            namespace = self._request("GET", "/api/v1/namespaces/" + name)
            if (namespace is None or namespace.get("apiVersion") != "v1" or namespace.get("kind") != "Namespace"
                    or namespace["metadata"].get("name") != name or _uid(namespace) != uid):
                raise ValueError("pool retirement namespace differs")
            _snapshot(namespace)

    def _path(self, key: str) -> str:
        original = self.originals[key]
        resource = "cronjobs" if original["kind"] == "CronJob" else "deployments"
        return "/apis/" + str(original["apiVersion"]) + "/namespaces/" + str(original["metadata"]["namespace"]) + "/" + resource + "/" + str(original["metadata"]["name"])

    def verify_guards(self) -> None:
        self._scope()
        if any(self.guards.guard(target, "observe").get("status") != "held" for target in self.request.migration.guards):
            raise ValueError("pool retirement guards are not held")

    def read(self, key: str) -> dict[str, Any]:
        try:
            original = self.originals[key]
            self._scope()
            actual = self._request("GET", self._path(key))
            if (actual is None or actual.get("apiVersion") != original["apiVersion"] or actual.get("kind") != original["kind"]
                    or actual["metadata"].get("namespace") != original["metadata"]["namespace"]
                    or actual["metadata"].get("name") != original["metadata"]["name"] or _uid(actual) != _uid(original)):
                raise ValueError
            _snapshot(actual)
            return actual
        except Exception:
            raise ValueError("pool_retirement_read_unqualified") from None

    def stop(self, key: str, before: dict[str, Any]) -> bool:
        try:
            original = self.originals[key]
            desired = stopped_document(self.request, key)
            version = before["metadata"]["resourceVersion"]
            if (not _matches(before, original, _uid(original)) or not isinstance(version, str) or not 0 < len(version) <= 128):
                raise ValueError
            namespace = original["metadata"]["namespace"]
            participant_ids = {row.participant_id for row in self.request.migration.registration.spec.participants
                if row.execution_namespace.name == namespace}
            guard, = (row for row in self.request.migration.guards if row.namespace == namespace or row.participant_id in participant_ids)
            if self.guards.guard(guard, "observe").get("status") != "held":
                raise ValueError
            self._scope()
            field = "suspend" if original["kind"] == "CronJob" else "replicas"
            patches = [{"op": "test", "path": "/metadata/uid", "value": _uid(before)},
                {"op": "test", "path": "/metadata/resourceVersion", "value": version},
                {"op": "test", "path": "/spec", "value": before["spec"]},
                {"op": "add", "path": "/metadata/annotations", "value": desired["metadata"]["annotations"]},
                {"op": "replace", "path": "/spec/" + field, "value": desired["spec"][field]}]
            with self.client.stream("PATCH", self._path(key), json=patches,
                    headers={"Content-Type": "application/json-patch+json"}) as response:
                if response.status_code not in {200, 409, 422} or response.headers.get("content-encoding", "identity").lower() != "identity":
                    raise ValueError
                raw = bytearray()
                for chunk in response.iter_bytes(chunk_size=16384):
                    if len(raw) + len(chunk) > 4 * 1024**2:
                        raise ValueError
                    raw.extend(chunk)
                value = json.loads(raw)
                if response.status_code in {409, 422}:
                    if (not isinstance(value, dict) or value.get("apiVersion") != "v1" or value.get("kind") != "Status"
                            or value.get("status") != "Failure" or value.get("code") != response.status_code
                            or value.get("reason") != {409: "Conflict", 422: "Invalid"}[response.status_code]):
                        raise ValueError
                    return False
                if not isinstance(value, dict) or not _matches(value, desired, _uid(original)):
                    raise ValueError
            return True
        except Exception:
            raise ValueError("pool_retirement_update_unconfirmed") from None

    def drained(self, key: str) -> bool:
        try:
            current = self.read(key)
            namespace = str(current["metadata"]["namespace"])
            path = ("/apis/apps/v1/namespaces/" + namespace + "/replicasets" if current["kind"] == "Deployment"
                else "/apis/batch/v1/namespaces/" + namespace + "/jobs")
            children = self._request("GET", path + "?limit=1000")
            pods = self._request("GET", "/api/v1/namespaces/" + namespace + "/pods?limit=1000")
            if children is None or pods is None or _stable(self.read(key)) != _stable(current):
                raise ValueError
            return qualify_pool_drain(self.request, key=key, current=current, children=children, pods=pods)
        except Exception:
            raise ValueError("pool_retirement_drain_unconfirmed") from None
