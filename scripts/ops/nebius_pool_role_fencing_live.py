"""Fixed HTTPS reductions of retained Role rules, never bindings or grants."""
from __future__ import annotations

import copy
import ssl
from typing import Any

from scripts.ops.nebius_ingress_stage import _key, _snapshot, _uid
from scripts.ops.nebius_management_stage import HTTPSManagementStageAPI
from scripts.ops.nebius_management_switch import _matches
from scripts.ops.nebius_management_transport import ManagementKubernetesTransport
from scripts.ops.nebius_pool_retirement_live import HTTPSPoolRetirementAPI, _patch_result
from scripts.ops.nebius_pool_role_fencing import PoolRoleFenceRequest, role_fence_documents

from loom.nebius_platform_render import digest


class HTTPSPoolRoleFenceAPI(HTTPSManagementStageAPI):
    def __init__(self, *, request: PoolRoleFenceRequest, retirement: HTTPSPoolRetirementAPI,
                 api_server: str, ssl_context: ssl.SSLContext, token: str | None = None):
        if request.retirement != retirement.request or api_server != retirement.api_server:
            raise ValueError("pool role fencing transport differs")
        self.request, self.retirement = request, retirement
        self.targets = role_fence_documents(request)
        self.roles = {_key(row): copy.deepcopy(row) for row in request.originals}
        self.contract_sha256 = digest({"originals": self.roles, "targets": self.targets})
        self.binding = request.retirement.migration.registration.binding
        self.documents = {}  # No inherited stage create has an approved target.
        ManagementKubernetesTransport.__init__(self, api_server=api_server, ssl_context=ssl_context, token=token)

    def _scope(self) -> None:
        if self.contract_sha256 != digest({"originals": {_key(row): row for row in self.request.originals},
                "targets": role_fence_documents(self.request)}):
            raise ValueError("pool role fencing input changed")
        self.retirement._scope()
        self.verify_identity(self.binding)
        for participant in self.request.retirement.migration.registration.spec.participants:
            for namespace in (participant.execution_namespace, participant.build_namespace):
                actual = self._request("GET", "/api/v1/namespaces/" + namespace.name)
                if (actual is None or actual.get("apiVersion") != "v1" or actual.get("kind") != "Namespace"
                        or actual["metadata"].get("name") != namespace.name or _uid(actual) != str(namespace.uid)):
                    raise ValueError("pool role fencing namespace differs")
                _snapshot(actual)

    def _role_path(self, key: str) -> str:
        original = self.roles[key]
        return "/apis/rbac.authorization.k8s.io/v1/namespaces/" + str(original["metadata"]["namespace"]) + "/roles/" + str(original["metadata"]["name"])

    def read_role(self, key: str) -> dict[str, Any]:
        try:
            original = self.roles[key]
            self._scope()
            actual = self._request("GET", self._role_path(key))
            if (actual is None or actual.get("apiVersion") != "rbac.authorization.k8s.io/v1" or actual.get("kind") != "Role"
                    or actual["metadata"].get("namespace") != original["metadata"]["namespace"]
                    or actual["metadata"].get("name") != original["metadata"]["name"] or _uid(actual) != _uid(original)):
                raise ValueError
            _snapshot(actual)
            return actual
        except Exception:
            raise ValueError("pool_role_fence_read_unqualified") from None

    def restrict_role(self, key: str, before: dict[str, Any], desired: dict[str, Any]) -> bool:
        try:
            original = self.roles[key]
            version = before["metadata"]["resourceVersion"]
            if (desired != self.targets[key] or not _matches(before, original, _uid(original))
                    or not isinstance(version, str) or not 0 < len(version) <= 128):
                raise ValueError
            self._scope()
            patches = [{"op": "test", "path": "/metadata/uid", "value": _uid(before)},
                {"op": "test", "path": "/metadata/resourceVersion", "value": version},
                {"op": "test", "path": "/rules", "value": before["rules"]},
                {"op": "add", "path": "/metadata/annotations", "value": desired["metadata"]["annotations"]},
                {"op": "replace", "path": "/rules", "value": desired["rules"]}]
            with self.client.stream("PATCH", self._role_path(key), json=patches,
                    headers={"Content-Type": "application/json-patch+json"}) as response:
                return _patch_result(response, desired=desired, uid=_uid(original))
        except Exception:
            raise ValueError("pool_role_fence_update_unconfirmed") from None
