#!/usr/bin/env python3
"""Create-only initial ingress staging, never public cutover or certificate proof.

The protected orchestrator must deliver/validate TLS before calling this module.
Server dry-run observes trusted cluster defaulting/admission; actual creation must
match that entire observation. A receipt freezes identities, not live readiness.
"""
from __future__ import annotations

import copy
import hashlib
import json
from dataclasses import asdict
from pathlib import Path
from typing import Any, Protocol, TypeVar, cast
from uuid import UUID, uuid4

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_ingress_gateway import KubectlTLSAPI, TLSBinding

from loom.nebius_shared_ingress import SharedIngressInstallation, render_shared_ingress


class StageError(RuntimeError):
    """Fixed, payload-free staging failure; retain the journal for reconciliation."""


class StageAPI(Protocol):
    def verify_identity(self, binding: TLSBinding) -> None: ...
    def get_resource(self, desired: dict[str, Any]) -> dict[str, Any] | None: ...
    def default_resource(self, desired: dict[str, Any]) -> dict[str, Any]:
        """Server dry-run only, never persistence."""
        ...

    def create_resource(self, desired: dict[str, Any]) -> None:
        """One create, never apply/replace/retry."""
        ...


class KubectlStageAPI(KubectlTLSAPI):
    """Allow only this installation's fixed renderer output plus its journal ID."""

    def __init__(self, kubeconfig: Path, *, binding: TLSBinding, executable: Path,
                 installation: SharedIngressInstallation):
        super().__init__(kubeconfig, binding=binding, executable=executable)
        if (str(installation.installation_id) != binding.installation_id
                or installation.foundation.ingress_namespace != binding.namespace):
            raise StageError("staging transport binding differs")
        self.documents = {_key(document): document for document in render_shared_ingress(installation)}

    def _approved(self, desired: dict[str, Any]) -> None:
        try:
            base = copy.deepcopy(desired)
            expected = self.documents[_key(base)]
            annotations = base["metadata"].get("annotations", {})
            operation = annotations.pop("loom.nebius/ingress-stage-id", None)
            # Preflight GETs use the undecorated renderer document. Writes must
            # additionally require a concrete operation identity below.
            if operation is not None and (str(UUID(operation)) != operation or UUID(operation).int == 0):
                raise ValueError()
            if not annotations and "annotations" not in expected["metadata"]:
                base["metadata"].pop("annotations", None)
            if base != expected:
                raise ValueError()
        except (KeyError, ValueError, TypeError, AttributeError):
            raise StageError("resource outside the fixed ingress render") from None

    def get_resource(self, desired: dict[str, Any]) -> dict[str, Any] | None:
        self._approved(desired)
        metadata = desired["metadata"]
        scope = ["-n", metadata["namespace"]] if metadata.get("namespace") is not None else []
        return self._get(["get", desired["kind"], metadata["name"], *scope])

    def _create(self, desired: dict[str, Any], *, dry_run: bool) -> bytes:
        self._approved(desired)
        if not desired["metadata"].get("annotations", {}).get("loom.nebius/ingress-stage-id"):
            raise StageError("ingress create requires durable operation identity")
        self.verify_identity(self.binding)
        return self._run(["create", *(["--dry-run=server"] if dry_run else []), "-f", "-", "-o", "json" if dry_run else "name"],
                         payload=json.dumps(desired).encode())

    def default_resource(self, desired: dict[str, Any]) -> dict[str, Any]:
        try:
            result = json.loads(self._create(desired, dry_run=True))
            if not isinstance(result, dict):
                raise ValueError()
            return result
        except (ValueError, TypeError):
            raise StageError("ingress defaulting readback unavailable") from None

    def create_resource(self, desired: dict[str, Any]) -> None:
        self._create(desired, dry_run=False)


def _key(document: dict[str, Any]) -> str:
    return f"{document['kind']}:{document['metadata'].get('namespace', '-')}:{document['metadata']['name']}"


def _retains_intent(actual: Any, desired: Any) -> bool:
    if isinstance(desired, dict):
        return isinstance(actual, dict) and all(
            _retains_intent(actual[key], value) if key in actual else value == []
            for key, value in desired.items()
        )
    if isinstance(desired, list):
        return (isinstance(actual, list) and len(actual) == len(desired)
                and all(_retains_intent(a, d) for a, d in zip(actual, desired, strict=True)))
    return type(actual) is type(desired) and actual == desired


def _defaulted(api: StageAPI, desired: dict[str, Any]) -> dict[str, Any]:
    observed = api.default_resource(desired)
    if not _retains_intent(observed, desired):
        raise StageError("cluster defaulting changed requested ingress configuration")
    if desired["kind"] == "Deployment":
        pod = observed["spec"]["template"]["spec"]
        wanted = desired["spec"]["template"]["spec"]
        if (any(pod.get(field, False) != wanted.get(field, False)
                for field in ("hostNetwork", "hostPID", "hostIPC", "shareProcessNamespace"))
                or pod.get("initContainers", []) != wanted.get("initContainers", [])
                or pod.get("securityContext", {}) != wanted.get("securityContext", {})
                or any(container.get("securityContext", {}) != expected.get("securityContext", {})
                       for container, expected in zip(pod["containers"], wanted["containers"], strict=True))):
            raise StageError("cluster defaulting changed ingress Pod security")
    return _snapshot(observed, allocation=False)


_Document = TypeVar("_Document")


def _copy_document(document: _Document) -> _Document:
    """Detach JSON-shaped manifests without generic deepcopy work per scalar.

    Exact builtin containers take the hot path; custom types keep the ordinary
    deepcopy protocol. One memo preserves aliases/cycles across both paths.
    This is a copy, not a cache or a qualification of the source document.
    """
    memo: dict[int, Any] = {}
    retained: list[Any] = []  # Prevent id reuse if a custom copier mutates input.

    def clone(value: Any) -> Any:
        kind = type(value)
        if value is None or kind in (str, int, bool, float, bytes):
            return value
        identity = id(value)
        if identity in memo:
            return memo[identity]
        if kind is dict:
            result: dict[Any, Any] = {}
            memo[identity] = result
            retained.append(value)
            for key, item in value.items():
                result[key if type(key) is str else copy.deepcopy(key, memo)] = clone(item)
            return result
        if kind is list:
            items: list[Any] = []
            memo[identity] = items
            retained.append(value)
            items.extend(clone(item) for item in value)
            return items
        return copy.deepcopy(value, memo)

    return cast(_Document, clone(document))


def _snapshot(document: dict[str, Any], *, allocation: bool = True) -> dict[str, Any]:
    result = _copy_document(document)
    metadata = result["metadata"]
    if metadata.get("deletionTimestamp") is not None or metadata.get("ownerReferences"):
        raise StageError("staged resource is deleting or has a foreign owner")
    for field in ("uid", "resourceVersion", "generation", "creationTimestamp", "managedFields", "selfLink"):
        metadata.pop(field, None)
    metadata.pop("deletionTimestamp", None)
    if result["kind"] == "Deployment":
        annotations = metadata.get("annotations", {})
        annotations.pop("deployment.kubernetes.io/revision", None)
        if not annotations:
            metadata.pop("annotations", None)
    result.pop("status", None)
    if result["kind"] == "Service" and not allocation:
        for field in ("clusterIP", "clusterIPs"):
            result["spec"].pop(field, None)
    return result


def _uid(document: dict[str, Any]) -> str:
    value = document["metadata"]["uid"]
    if not isinstance(value, str) or str(UUID(value)) != value or UUID(value).int == 0:
        raise StageError("staged resource identity is invalid")
    return value


def stage_controller(installation: SharedIngressInstallation, *, binding: TLSBinding,
                     api: StageAPI, state_dir: Path) -> dict[str, Any]:
    """Freeze all eight rendered resources and reconcile a single create per item.

    This initial-stage journal is intentionally immutable across configuration
    changes. Certificate rotation has its separate exact controller-switch journal;
    it must not reinterpret/replay an old initial-stage intent as an update.
    """
    try:
        if (str(installation.installation_id) != binding.installation_id
                or installation.foundation.ingress_namespace != binding.namespace
                or installation.foundation.public_dns_zone != binding.child_domain):
            raise StageError("ingress staging binding differs")
        documents = render_shared_ingress(installation)
        rendered = {_key(document): document for document in documents}
        identity = {"schema": "loom.nebius-ingress-stage.v1", "binding": asdict(binding),
                    "render_sha256": hashlib.sha256(json.dumps(documents, sort_keys=True).encode()).hexdigest()}
        with private_state._locked_state(state_dir):
            api.verify_identity(binding)
            path = state_dir / (binding.installation_id + ".json")
            if path.exists() or path.is_symlink():
                record = json.loads(private_state._private_read(path, limit=1024 * 1024))
                if (not isinstance(record, dict) or set(record) != {*identity, "status", "operation_id", "resources"}
                        or any(record[name] != value for name, value in identity.items())
                        or record["status"] not in {"stage_intent", "controller_staged"}
                        or str(UUID(record["operation_id"])) != record["operation_id"]
                        or set(record["resources"]) != set(rendered)):
                    raise StageError("ingress staging journal differs")
            else:
                # Reject all name collisions before any create, including an
                # exact-looking object without our durable operation identity.
                if any(api.get_resource(document) is not None for document in documents):
                    raise StageError("untracked ingress resource exists; refusing adoption")
                operation = str(uuid4())
                resources = {}
                for key, document in rendered.items():
                    desired = copy.deepcopy(document)
                    desired["metadata"].setdefault("annotations", {})["loom.nebius/ingress-stage-id"] = operation
                    resources[key] = {"desired": desired, "expected": _defaulted(api, desired),
                                      "status": "prepared", "uid": None, "observed": None}
                record = {**identity, "status": "stage_intent", "operation_id": operation, "resources": resources}
                private_state._atomic_json(path, record)
            for key, original in rendered.items():
                item = record["resources"][key]
                desired = copy.deepcopy(original)
                desired["metadata"].setdefault("annotations", {})["loom.nebius/ingress-stage-id"] = record["operation_id"]
                if (set(item) != {"desired", "expected", "status", "uid", "observed"}
                        or item["desired"] != desired or item["status"] not in {"prepared", "create_intent", "created"}
                        or (item["status"] == "created") != (item["uid"] is not None and item["observed"] is not None)):
                    raise StageError("ingress resource journal differs")
                api.verify_identity(binding)
                observed = api.get_resource(desired)
                if item["status"] == "prepared":
                    if observed is not None:
                        raise StageError("untracked ingress resource exists; refusing adoption")
                    item["status"] = "create_intent"
                    private_state._atomic_json(path, record)
                    try:
                        api.create_resource(desired)
                    except Exception:
                        pass  # Readback alone can resolve the write; never repeat it.
                    observed = api.get_resource(desired)
                if observed is None:
                    raise StageError("ingress create outcome unresolved; preserve intent")
                uid = _uid(observed)
                snapshot = _snapshot(observed)
                if (_snapshot(observed, allocation=False) != item["expected"]
                        or (item["uid"] is not None and (uid != item["uid"] or snapshot != item["observed"]))):
                    raise StageError("staged ingress resource differs from recorded intent")
                api.verify_identity(binding)
                if item["status"] != "created":
                    item.update(status="created", uid=uid, observed=snapshot)
                    private_state._atomic_json(path, record)
            # Recheck the complete chain before a receipt: a later create can
            # race earlier objects' deletion/ownership/configuration changes.
            for item in record["resources"].values():
                observed = api.get_resource(item["desired"])
                if (observed is None or _uid(observed) != item["uid"] or _snapshot(observed) != item["observed"]):
                    raise StageError("staged ingress changed before final readback")
            api.verify_identity(binding)
            if record["status"] != "controller_staged":
                record["status"] = "controller_staged"
                private_state._atomic_json(path, record)
            return {"status": "controller_staged", "installation_id": binding.installation_id,
                    "render_sha256": identity["render_sha256"],
                    "resource_uids": {key: item["uid"] for key, item in record["resources"].items()}}
    except StageError:
        raise
    except Exception:
        raise StageError("ingress staging unavailable; preserve journal for reconciliation") from None
