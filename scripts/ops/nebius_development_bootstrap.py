"""Fresh dev Namespace/local credentials behind a protected fixed installer.

The caller qualifies source, candidate, fresh authority and resource fit first.
No CLI, workload, cloud credential, execution permission or staging access exists
here. Preserve both the private journal and its independent anchor after failure;
this phase is not rotation, adoption, application readiness or namespace teardown.
"""
from __future__ import annotations

import base64
import copy
import json
import re
import ssl
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Protocol
from uuid import UUID, uuid4

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_ingress_stage import _snapshot, _uid
from scripts.ops.nebius_management_transport import ManagementKubernetesTransport

from loom.nebius_platform_render import _namespace, digest
from loom_service.environment_management.credentials import generate_material

_LABEL = "loom.nebius/development-installation"
_OPERATION = "loom.nebius/development-bootstrap-operation"
_DB_KEYS = {"ca.crt", "postgres-password", "admin-url", "service-url", "service-password",
            "control-plane-url", "control-plane-password", "gateway-url", "gateway-password"}
_RESERVED = {"loom-platform-db", "loom-platform-auth", "loom-admin-secret", "loom-platform-storage",
             "loom-platform-collector", "loom-platform-batch-runner"}


class DevelopmentBootstrapError(RuntimeError):
    """Payload-free failure; retain all created resources and private evidence."""


def _uuid(value: str) -> None:
    if not isinstance(value, str) or str(UUID(value)) != value or not UUID(value).int:
        raise ValueError()


@dataclass(frozen=True)
class DevelopmentBootstrapBinding:
    installation_id: str
    kube_system_uid: str
    tls_secret_name: str
    namespace: str = "loom-dev"

    def __post_init__(self) -> None:
        try:
            _uuid(self.installation_id)
            _uuid(self.kube_system_uid)
            if (self.namespace != "loom-dev" or not isinstance(self.tls_secret_name, str)
                    or len(self.tls_secret_name) > 63 or self.tls_secret_name in _RESERVED
                    or re.fullmatch(r"[a-z0-9](?:[a-z0-9-]*[a-z0-9])?", self.tls_secret_name) is None):
                raise ValueError()
        except Exception:
            raise DevelopmentBootstrapError("invalid development bootstrap binding") from None


class DevelopmentBootstrapAPI(Protocol):
    def verify_cluster(self, binding: DevelopmentBootstrapBinding) -> None: ...
    def get_namespace(self) -> dict[str, Any] | None: ...
    def create_namespace(self, document: dict[str, Any]) -> None: ...
    def get_secret(self, name: str) -> dict[str, Any] | None: ...
    def create_secret(self, document: dict[str, Any], *, namespace_uid: str) -> None: ...


def _keys(binding: DevelopmentBootstrapBinding) -> dict[str, set[str]]:
    return {"loom-platform-db": _DB_KEYS, binding.tls_secret_name: {"tls.crt", "tls.key"},
            "loom-platform-auth": {"jwt-signing-key", "secret-store-master-key"}, "loom-admin-secret": {"secrets.toml"}}


def _namespace_document(binding: DevelopmentBootstrapBinding, operation: str) -> dict[str, Any]:
    _uuid(operation)
    document = _namespace(binding.namespace)
    document["metadata"]["labels"].update({_LABEL: binding.installation_id,
        "loom.nebius/development-phase": "private-bootstrap"})
    document["metadata"]["annotations"] = {_OPERATION: operation}
    return document


def _secret_documents(material: dict[str, Any], binding: DevelopmentBootstrapBinding, operation: str) -> dict[str, dict[str, Any]]:
    _uuid(operation)
    keys = _keys(binding)
    if not isinstance(material, dict) or material.keys() != keys.keys():
        raise ValueError()
    result = {}
    for name, required in keys.items():
        values = material[name]
        if (not isinstance(values, dict) or values.keys() != required
                or any(not isinstance(value, str) or not 0 < len(value.encode()) <= 65_536 for value in values.values())):
            raise ValueError()
        result[name] = {"apiVersion": "v1", "kind": "Secret", "immutable": True,
            "type": "kubernetes.io/tls" if name == binding.tls_secret_name else "Opaque",
            "metadata": {"name": name, "namespace": binding.namespace,
                         "labels": {_LABEL: binding.installation_id}, "annotations": {_OPERATION: operation}},
            "data": {key: base64.b64encode(value.encode()).decode() for key, value in values.items()}}
    return result


def _observe_namespace(api: DevelopmentBootstrapAPI, binding: DevelopmentBootstrapBinding,
                       operation: str, expected_uid: str | None) -> str:
    api.verify_cluster(binding)
    row = api.get_namespace()
    if row is None:
        raise DevelopmentBootstrapError("development namespace create unresolved; preserve intent")
    uid, snapshot = _uid(row), _snapshot(row)
    name = snapshot["metadata"].get("labels", {}).pop("kubernetes.io/metadata.name", binding.namespace)
    spec = snapshot.pop("spec", {})
    if (name != binding.namespace or spec not in ({}, {"finalizers": ["kubernetes"]})
            or snapshot != _namespace_document(binding, operation) or expected_uid not in (None, uid)):
        raise DevelopmentBootstrapError("development namespace identity or policy differs")
    api.verify_cluster(binding)
    return uid


class HTTPSDevelopmentBootstrapAPI(ManagementKubernetesTransport):
    """Only one fixed Namespace and four fixed immutable local Secret creates."""

    error_type = DevelopmentBootstrapError

    def __init__(self, *, binding: DevelopmentBootstrapBinding, api_server: str,
                 ssl_context: ssl.SSLContext, token: str | None = None):
        self.binding = binding
        super().__init__(api_server=api_server, ssl_context=ssl_context, token=token)

    def verify_cluster(self, binding: DevelopmentBootstrapBinding) -> None:
        try:
            if binding != self.binding:
                raise ValueError()
            row = self._request("GET", "/api/v1/namespaces/kube-system")
            if (row is None or row.get("apiVersion") != "v1" or row.get("kind") != "Namespace"
                    or row["metadata"]["name"] != "kube-system" or _uid(row) != binding.kube_system_uid):
                raise ValueError()
            _snapshot(row)
        except Exception:
            raise DevelopmentBootstrapError("development cluster identity differs") from None

    def get_namespace(self) -> dict[str, Any] | None:
        return self._request("GET", "/api/v1/namespaces/loom-dev")

    def create_namespace(self, document: dict[str, Any]) -> None:
        try:
            operation = document["metadata"]["annotations"][_OPERATION]
            if document != _namespace_document(self.binding, operation):
                raise ValueError()
        except Exception:
            raise DevelopmentBootstrapError("Namespace outside development bootstrap scope") from None
        self.verify_cluster(self.binding)
        self._request("POST", "/api/v1/namespaces", document=document)

    def get_secret(self, name: str) -> dict[str, Any] | None:
        if name not in _keys(self.binding):
            raise DevelopmentBootstrapError("Secret outside development bootstrap scope")
        return self._request("GET", "/api/v1/namespaces/loom-dev/secrets/" + name)

    def create_secret(self, document: dict[str, Any], *, namespace_uid: str) -> None:
        try:
            _uuid(namespace_uid)
            metadata, data = document["metadata"], document["data"]
            name, operation = metadata["name"], metadata["annotations"][_OPERATION]
            _uuid(operation)
            if (name not in _keys(self.binding) or data.keys() != _keys(self.binding)[name]
                    or set(document) != {"apiVersion", "kind", "metadata", "immutable", "type", "data"}
                    or document["apiVersion"] != "v1" or document["kind"] != "Secret" or document["immutable"] is not True
                    or document["type"] != ("kubernetes.io/tls" if name == self.binding.tls_secret_name else "Opaque")
                    or metadata != {"name": name, "namespace": "loom-dev", "labels": {_LABEL: self.binding.installation_id},
                                    "annotations": {_OPERATION: operation}}):
                raise ValueError()
            for value in data.values():
                if not isinstance(value, str) or not 0 < len(value) <= 90_000 or not base64.b64decode(value, validate=True):
                    raise ValueError()
        except Exception:
            raise DevelopmentBootstrapError("Secret outside development bootstrap scope") from None
        _observe_namespace(self, self.binding, operation, namespace_uid)
        self._request("POST", "/api/v1/namespaces/loom-dev/secrets", document=document)


def _validate_record(record: dict[str, Any], marker: dict[str, Any], binding: DevelopmentBootstrapBinding) -> None:
    if (not isinstance(record, dict) or set(record) != {*marker, "material", "namespace", "secrets"}
            or any(record[key] != value for key, value in marker.items())
            or digest(record["material"]) != marker["material_sha256"]
            or not isinstance(record["secrets"], dict) or record["secrets"].keys() != _keys(binding).keys()):
        raise DevelopmentBootstrapError("development bootstrap journal differs")
    _secret_documents(record["material"], binding, marker["operation_id"])
    items = [record["namespace"], *(record["secrets"][name] for name in _keys(binding))]
    unfinished = False
    for item in items:
        if (not isinstance(item, dict) or set(item) != {"status", "uid"}
                or item["status"] not in {"prepared", "create_intent", "created"}
                or (item["status"] == "created") != (item["uid"] is not None)
                or (unfinished and item["status"] != "prepared")):
            raise DevelopmentBootstrapError("development bootstrap journal phase differs")
        if item["uid"] is not None:
            _uuid(item["uid"])
        unfinished |= item["status"] != "created"


def bootstrap_development(*, binding: DevelopmentBootstrapBinding, api: DevelopmentBootstrapAPI,
                          state_dir: Path, anchor_dir: Path) -> dict[str, Any]:
    """Create once and read back uncertain outcomes; never retry, adopt or rotate."""
    try:
        state, anchor = state_dir.absolute(), anchor_dir.absolute()
        if (state != state.resolve() or anchor != anchor.resolve() or state == anchor
                or state in anchor.parents or anchor in state.parents):
            raise DevelopmentBootstrapError("development recovery anchor must be independent of private state")
        identity = {"schema": "loom.nebius-development-local-bootstrap.v1", "binding": asdict(binding), "state_dir": str(state)}
        with private_state._locked_state(anchor):
            marker_path, journal = anchor / (binding.installation_id + ".json"), state / "bootstrap.json"
            if marker_path.exists() or marker_path.is_symlink():
                marker = json.loads(private_state._private_read(marker_path))
                if (not isinstance(marker, dict) or set(marker) != {*identity, "operation_id", "material_sha256"}
                        or any(marker[key] != value for key, value in identity.items())
                        or not state.is_dir() or not journal.is_file() or journal.is_symlink()):
                    raise DevelopmentBootstrapError("development bootstrap recovery evidence missing or changed")
                _uuid(marker["operation_id"])
                record = None
            else:
                if state.exists() or state.is_symlink():
                    raise DevelopmentBootstrapError("untracked development bootstrap state; refusing adoption")
                api.verify_cluster(binding)
                if api.get_namespace() is not None or any(api.get_secret(name) is not None for name in _keys(binding)):
                    raise DevelopmentBootstrapError("untracked development namespace or Secret; refusing adoption")
                generated = generate_material(namespace=binding.namespace, tls_secret_name=binding.tls_secret_name)
                material = {name: {key: generated[name][key] for key in keys} for name, keys in _keys(binding).items()}
                marker = {**identity, "operation_id": str(uuid4()), "material_sha256": digest(material)}
                record = {**marker, "material": material, "namespace": {"status": "prepared", "uid": None},
                          "secrets": {name: {"status": "prepared", "uid": None} for name in _keys(binding)}}
                _validate_record(record, marker, binding)
                # The independent start marker precedes all remote writes. Loss
                # of the working tree, even alongside API absence, cannot grant
                # permission to regenerate material or repeat unknown requests.
                private_state._atomic_json(marker_path, marker)
                # The anchor lock cannot exclude a caller using another anchor.
                # Atomically claim this fresh directory before taking its lock;
                # never overwrite a competing caller's retained journal.
                state.mkdir(mode=0o700)
            with private_state._locked_state(state):
                if record is None:
                    record = json.loads(private_state._private_read(journal, limit=1024 * 1024))
                else:
                    private_state._atomic_json(journal, record)
                _validate_record(record, marker, binding)
                operation = marker["operation_id"]
                namespace = record["namespace"]
                if namespace["status"] == "prepared":
                    api.verify_cluster(binding)
                    if api.get_namespace() is not None:
                        raise DevelopmentBootstrapError("untracked development namespace; refusing adoption")
                    namespace["status"] = "create_intent"
                    private_state._atomic_json(journal, record)
                    try:
                        api.create_namespace(_namespace_document(binding, operation))
                    except Exception:
                        pass  # Only readback resolves an uncertain outcome.
                uid = _observe_namespace(api, binding, operation, namespace["uid"])
                if namespace["status"] != "created":
                    namespace.update(status="created", uid=uid)
                    private_state._atomic_json(journal, record)
                documents = _secret_documents(record["material"], binding, operation)

                def observe(name: str) -> str:
                    actual = api.get_secret(name)
                    if actual is None:
                        raise DevelopmentBootstrapError("development Secret create unresolved; preserve intent")
                    found = _uid(actual)
                    if _snapshot(actual) != documents[name] or record["secrets"][name]["uid"] not in (None, found):
                        raise DevelopmentBootstrapError("development Secret identity or material differs")
                    return found

                # Validate every retained Secret before resuming any writes.
                for name in documents:
                    _observe_namespace(api, binding, operation, uid)
                    if record["secrets"][name]["status"] == "prepared":
                        if api.get_secret(name) is not None:
                            raise DevelopmentBootstrapError("untracked development Secret; refusing adoption")
                    else:
                        observe(name)
                for name, desired in documents.items():
                    item = record["secrets"][name]
                    _observe_namespace(api, binding, operation, uid)
                    if item["status"] == "prepared":
                        if api.get_secret(name) is not None:
                            raise DevelopmentBootstrapError("untracked development Secret; refusing adoption")
                        item["status"] = "create_intent"
                        private_state._atomic_json(journal, record)
                        try:
                            api.create_secret(copy.deepcopy(desired), namespace_uid=uid)
                        except Exception:
                            pass
                    secret_uid = observe(name)
                    _observe_namespace(api, binding, operation, uid)
                    if item["status"] != "created":
                        item.update(status="created", uid=secret_uid)
                        private_state._atomic_json(journal, record)
                for name in documents:
                    observe(name)
                _observe_namespace(api, binding, operation, uid)
                return {"status": "development_local_bootstrap_complete", "installation_id": binding.installation_id,
                        "namespace": binding.namespace, "namespace_uid": uid,
                        "secret_uids": {name: item["uid"] for name, item in record["secrets"].items()}}
    except DevelopmentBootstrapError:
        raise
    except Exception:
        raise DevelopmentBootstrapError("development bootstrap unavailable; preserve recovery evidence") from None
