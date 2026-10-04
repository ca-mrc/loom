"""Hash-qualified dedicated machine Secrets for the protected pool migration.

No credential generation/rotation or personal namespace delivery. The parent
retains candidate/phase authority; this fixed create-only stage grants no RBAC.
"""
from __future__ import annotations

import base64
import copy
import hashlib
import re
import ssl
from pathlib import Path
from typing import Any
from uuid import UUID

from scripts.ops.nebius_ingress_stage import _key, _snapshot, _uid
from scripts.ops.nebius_management_material import ManagementBinding
from scripts.ops.nebius_management_stage import (
    _MARKER,
    HTTPSManagementStageAPI,
    ManagementStageAPI,
    ManagementStageError,
    _stage_fixed_documents,
)
from scripts.ops.nebius_management_supplied import _defaulted
from scripts.ops.nebius_management_transport import ManagementKubernetesTransport
from scripts.ops.nebius_pool_migration import PoolMigrationRequest, migration_contract

from loom.nebius_platform_render import digest


def machine_documents(request: PoolMigrationRequest, tokens: dict[UUID, str]) -> dict[str, dict[str, Any]]:
    try:
        migration_contract(request)
        spec, binding = request.registration.spec, request.registration.binding
        if set(tokens) != {row.machine_id for row in spec.machines}:
            raise ValueError
        participants = {row.participant_id: row for row in spec.participants}
        platforms = {row.participant_id: row.namespace for row in request.guards}
        documents = {}
        for machine in spec.machines:
            token = tokens[machine.machine_id]
            if (not isinstance(token, str) or not 0 < len(token) <= 512
                    or re.fullmatch(r"[A-Za-z0-9._~+/-]+={0,2}", token) is None
                    or hashlib.sha256(token.encode()).hexdigest() != machine.token_sha256):
                raise ValueError
            destinations = [binding.namespace]
            if machine.role == "observer":
                development, = (row for row in spec.participants if row.environment_class == "development")
                destinations = [development.execution_namespace.name]
            elif machine.participant_id is not None and machine.workload_scope == "environment":
                destinations = [platforms[machine.participant_id], participants[machine.participant_id].execution_namespace.name]
            for namespace in destinations:
                document = {"apiVersion": "v1", "kind": "Secret", "immutable": True, "type": "Opaque",
                    "metadata": {"name": "loom-pool-machine-" + machine.machine_id.hex, "namespace": namespace,
                        "labels": {"loom.nebius/management-installation": binding.installation_id,
                            "loom.nebius/pool": str(spec.pool_id), "loom.nebius/pool-operation": str(spec.operation_id),
                            "loom.nebius/pool-machine": str(machine.machine_id)}},
                    "data": {"token": base64.b64encode(token.encode()).decode()}}
                if _key(document) in documents:
                    raise ValueError
                documents[_key(document)] = document
        return documents
    except Exception:
        raise ValueError("pool_machine_material_unqualified") from None


class HTTPSPoolMaterialAPI(HTTPSManagementStageAPI):
    def __init__(self, *, request: PoolMigrationRequest, tokens: dict[UUID, str], api_server: str,
                 ssl_context: ssl.SSLContext, token: str | None = None):
        self.documents = machine_documents(request, tokens)
        self.binding = request.registration.binding
        self.namespaces = {row.namespace: str(row.namespace_uid) for row in request.guards}
        self.namespaces.update({row.execution_namespace.name: str(row.execution_namespace.uid) for row in request.registration.spec.participants})
        ManagementKubernetesTransport.__init__(self, api_server=api_server, ssl_context=ssl_context, token=token)

    def verify_identity(self, binding: ManagementBinding) -> None:
        super().verify_identity(binding)
        try:
            for name, uid in self.namespaces.items():
                actual = self._request("GET", "/api/v1/namespaces/" + name)
                if (actual is None or actual.get("apiVersion") != "v1" or actual.get("kind") != "Namespace"
                        or actual["metadata"].get("name") != name or _uid(actual) != uid):
                    raise ValueError
                _snapshot(actual)
        except Exception:
            raise ManagementStageError("pool machine namespace identity differs") from None

    def _approved(self, document: dict[str, Any], *, writing: bool = False) -> str:
        try:
            desired = copy.deepcopy(document)
            expected = self.documents[_key(desired)]
            annotations = desired["metadata"].get("annotations", {})
            operation = annotations.pop(_MARKER, None)
            if writing or operation is not None:
                if str(UUID(operation)) != operation or not UUID(operation).int:
                    raise ValueError
            if not annotations and "annotations" not in expected["metadata"]:
                desired["metadata"].pop("annotations", None)
            if desired != expected:
                raise ValueError
            namespace = desired["metadata"]["namespace"]
            if not isinstance(namespace, str):
                raise ValueError
            return "/api/v1/namespaces/" + namespace + "/secrets"
        except Exception:
            raise ManagementStageError("resource outside fixed pool machine material") from None


def deliver_pool_material(*, request: PoolMigrationRequest, tokens: dict[UUID, str], api: ManagementStageAPI,
                          state_dir: Path) -> dict[str, Any]:
    try:
        documents = machine_documents(request, tokens)
        revision = digest({"contract": migration_contract(request), "documents": documents})
        receipt = _stage_fixed_documents(documents=documents, revision=revision, phase="pool-machine-material",
            binding=request.registration.binding, api=api, state_dir=state_dir, default_document=_defaulted)
        return {**receipt, "status": "pool_machine_material_delivered"}
    except Exception:
        raise ValueError("pool_machine_material_unavailable_preserve_evidence") from None
