"""Deliver qualified supplied management credentials without rotation.

Qualification of dedicated provider/publication/backup scopes belongs to the
protected caller. This adapter only accepts the exact independently supplied
Secret shapes, never bootstrap database/admin material or arbitrary manifests.
"""
from __future__ import annotations

import base64
import json
import ssl
from pathlib import Path
from typing import Any

from scripts.ops.nebius_ingress_stage import _key, _snapshot
from scripts.ops.nebius_management_material import ManagementBinding
from scripts.ops.nebius_management_stage import (
    HTTPSManagementStageAPI,
    ManagementStageAPI,
    ManagementStageError,
    _stage_fixed_documents,
)
from scripts.ops.nebius_management_transport import ManagementKubernetesTransport

from loom.nebius_platform_render import digest

_KEYS = {
    "loom-management-cloud": {"credentials.json"},
    "loom-management-publications": {"token"},
    "loom-platform-storage": {"backup-access-key", "backup-secret-key"},
}


def _documents(material: dict[str, dict[str, str]], binding: ManagementBinding, *,
               application_only: bool = False) -> dict[str, dict[str, Any]]:
    try:
        keys_by_name = {name: keys for name, keys in _KEYS.items()
                        if not application_only or name != 'loom-management-cloud'}
        if not isinstance(material, dict) or material.keys() != keys_by_name.keys():
            raise ValueError()
        documents = {}
        for name, keys in keys_by_name.items():
            values = material[name]
            if (not isinstance(values, dict) or values.keys() != keys
                    or any(not isinstance(value, str) or not 0 < len(value.encode()) <= 65_536 for value in values.values())):
                raise ValueError()
            document = {
                "apiVersion": "v1", "kind": "Secret", "immutable": True, "type": "Opaque",
                "metadata": {"name": name, "namespace": binding.namespace,
                             "labels": {"loom.nebius/management-installation": binding.installation_id}},
                "data": {key: base64.b64encode(value.encode()).decode() for key, value in values.items()},
            }
            documents[_key(document)] = document
        if not application_only:
            cloud = json.loads(material["loom-management-cloud"]["credentials.json"])
            if not isinstance(cloud, dict) or not cloud:
                raise ValueError()
        return documents
    except Exception:
        raise ManagementStageError("invalid supplied management material") from None


class HTTPSSuppliedMaterialAPI(HTTPSManagementStageAPI):
    """Reuses exact-document allowlisting and per-write Namespace identity checks."""

    def __init__(self, *, material: dict[str, dict[str, str]], binding: ManagementBinding,
                 api_server: str, ssl_context: ssl.SSLContext, token: str | None = None,
                 application_only: bool = False):
        self.documents = _documents(material, binding, application_only=application_only)
        self.binding = binding
        ManagementKubernetesTransport.__init__(self, api_server=api_server, ssl_context=ssl_context, token=token)


def _defaulted(api: ManagementStageAPI, desired: dict[str, Any]) -> dict[str, Any]:
    observed = _snapshot(api.default_resource(desired))
    if observed != desired:
        raise ManagementStageError("supplied material defaulting changed fixed credentials")
    return observed


def deliver_supplied_material(*, material: dict[str, dict[str, str]], binding: ManagementBinding,
                             api: ManagementStageAPI, state_dir: Path,
                             application_only: bool = False) -> dict[str, Any]:
    documents = _documents(material, binding, application_only=application_only)
    result = _stage_fixed_documents(documents=documents, revision=digest(documents), phase="supplied-material",
                                   binding=binding, api=api, state_dir=state_dir, default_document=_defaulted)
    return {"status": "management_supplied_material_delivered", "installation_id": binding.installation_id,
            "namespace": binding.namespace, "namespace_uid": binding.namespace_uid,
            "secret_uids": {doc["metadata"]["name"]: result["resource_uids"][key] for key, doc in documents.items()}}
