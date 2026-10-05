"""Create-only phases from the trusted management renderer, not arbitrary apply.

The installer owns prerequisite qualification and phase ordering. It must retain
independent phase-start evidence so loss of this directory cannot reopen writes.
Receipts freeze resource identities, not database/application/backup readiness.
"""
from __future__ import annotations

import copy
import json
import re
import ssl
from collections.abc import Callable
from dataclasses import asdict
from pathlib import Path
from typing import Any, Protocol
from uuid import UUID, uuid4

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_ingress_stage import _key, _snapshot, _uid
from scripts.ops.nebius_management_material import ManagementBinding
from scripts.ops.nebius_management_transport import ManagementKubernetesTransport

from loom_service.environment_management.deployment import RenderedManagement
from loom_service.environment_management.kubernetes_provider import _contains

_MARKER = "loom.nebius/management-stage-operation"
_LABEL = "loom.nebius/management-installation"
_RESOURCES = {
    "Secret": ("v1", "secrets"),
    "ConfigMap": ("v1", "configmaps"), "ServiceAccount": ("v1", "serviceaccounts"),
    "Service": ("v1", "services"), "NetworkPolicy": ("networking.k8s.io/v1", "networkpolicies"),
    "StatefulSet": ("apps/v1", "statefulsets"), "Deployment": ("apps/v1", "deployments"),
    "Job": ("batch/v1", "jobs"), "CronJob": ("batch/v1", "cronjobs"),
    "Ingress": ("networking.k8s.io/v1", "ingresses"),
}
_PHASES = {
    "10-config-network.yaml": {
        ("ConfigMap", "loom-platform-config"), ("ServiceAccount", "loom-platform"),
        ("NetworkPolicy", "default-deny-ingress"), ("NetworkPolicy", "management-api"),
        ("NetworkPolicy", "postgres-private"),
    },
    "20-database.yaml": {("Service", "loom-postgres"), ("StatefulSet", "loom-postgres")},
    "40-services.yaml": {("Service", "loom-service"), ("Deployment", "loom-service")},
    "70-public.yaml": {("Ingress", "loom-management")},
    "80-backup.yaml": {("CronJob", "loom-platform-backup")},
}


class ManagementStageError(RuntimeError):
    """Payload-free failure; preserve data and private recovery state."""


class ManagementStageAPI(Protocol):
    def verify_identity(self, binding: ManagementBinding) -> None: ...
    def get_resource(self, document: dict[str, Any]) -> dict[str, Any] | None: ...
    def default_resource(self, document: dict[str, Any]) -> dict[str, Any]: ...
    def create_resource(self, document: dict[str, Any]) -> None: ...
    def get_database_claim(self) -> dict[str, Any] | None: ...
    def get_database_volume(self) -> dict[str, Any] | None: ...


def _documents(rendered: RenderedManagement, phase: str, binding: ManagementBinding) -> dict[str, dict[str, Any]]:
    try:
        if re.fullmatch(r"sha256:[0-9a-f]{64}", rendered.revision) is None:
            raise ValueError()
        jobs = {"30-migrate.yaml": "loom-management-migrate-", "85-backup-verify.yaml": "loom-management-backup-"}
        required = ({("Job", jobs[phase] + rendered.revision[7:19])} if phase in jobs else _PHASES[phase])
        documents = rendered.files[phase]
        found = {(doc["kind"], doc["metadata"]["name"]) for doc in documents}
        optional = {("ServiceAccount", "loom-management-provisioner")} if phase == "10-config-network.yaml" else set()
        if not required <= found or found - required - optional or len(found) != len(documents):
            raise ValueError()
        for doc in documents:
            if (doc["metadata"].get("namespace") != binding.namespace
                    or doc["metadata"].get("labels", {}).get(_LABEL) != binding.installation_id
                    or doc["apiVersion"] != _RESOURCES[doc["kind"]][0]
                    or _MARKER in doc["metadata"].get("annotations", {})):
                raise ValueError()
        return {_key(doc): copy.deepcopy(doc) for doc in documents}
    except Exception:
        raise ManagementStageError("resource outside fixed management phase") from None


class HTTPSManagementStageAPI(ManagementKubernetesTransport):
    """Explicit TLS/auth, no retry; only the fixed phase's rendered objects."""

    error_type: type[RuntimeError] = ManagementStageError

    def __init__(self, *, binding: ManagementBinding, rendered: RenderedManagement, phase: str,
                 api_server: str, ssl_context: ssl.SSLContext, token: str | None = None):
        self.documents = _documents(rendered, phase, binding)
        self.binding = binding
        super().__init__(api_server=api_server, ssl_context=ssl_context, token=token)

    def verify_identity(self, binding: ManagementBinding) -> None:
        try:
            if binding != self.binding:
                raise ValueError()
            for name, uid in (("kube-system", binding.kube_system_uid), (binding.namespace, binding.namespace_uid)):
                namespace = self._request("GET", "/api/v1/namespaces/" + name)
                if (namespace is None or namespace.get("kind") != "Namespace" or _uid(namespace) != uid
                        or namespace["metadata"]["name"] != name or namespace["metadata"].get("deletionTimestamp")
                        or namespace["metadata"].get("ownerReferences")):
                    raise ValueError()
                if name == binding.namespace:
                    labels = namespace["metadata"].get("labels", {})
                    if (labels.get(_LABEL) != binding.installation_id
                            or labels.get("pod-security.kubernetes.io/enforce") != "restricted"):
                        raise ValueError()
        except Exception:
            raise ManagementStageError("management namespace identity or policy differs") from None

    def _approved(self, document: dict[str, Any], *, writing: bool = False) -> str:
        try:
            desired = copy.deepcopy(document)
            expected = self.documents[_key(desired)]
            annotations = desired["metadata"].get("annotations", {})
            operation = annotations.pop(_MARKER, None)
            if writing or operation is not None:
                if str(UUID(operation)) != operation or UUID(operation).int == 0:
                    raise ValueError()
            if not annotations and "annotations" not in expected["metadata"]:
                desired["metadata"].pop("annotations", None)
            if desired != expected:
                raise ValueError()
            version, resource = _RESOURCES[desired["kind"]]
            return ("/api/v1" if version == "v1" else "/apis/" + version) + "/namespaces/" + self.binding.namespace + "/" + resource
        except Exception:
            raise ManagementStageError("resource outside fixed management phase") from None

    def get_resource(self, document: dict[str, Any]) -> dict[str, Any] | None:
        return self._request("GET", self._approved(document) + "/" + document["metadata"]["name"])

    def get_database_claim(self) -> dict[str, Any] | None:
        if not any(doc["kind"] == "StatefulSet" and doc["metadata"]["name"] == "loom-postgres" for doc in self.documents.values()):
            raise ManagementStageError("database storage outside fixed management phase")
        self.verify_identity(self.binding)
        return self._request("GET", "/api/v1/namespaces/" + self.binding.namespace + "/persistentvolumeclaims/data-loom-postgres-0")

    def get_database_volume(self) -> dict[str, Any] | None:
        claim = self.get_database_claim()
        name = (claim or {}).get("spec", {}).get("volumeName")
        if name is None:
            return None
        if not isinstance(name, str) or re.fullmatch(r"[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?", name) is None:
            raise ManagementStageError("invalid bound database volume name")
        return self._request("GET", "/api/v1/persistentvolumes/" + name)

    def default_resource(self, document: dict[str, Any]) -> dict[str, Any]:
        path = self._approved(document, writing=True)
        self.verify_identity(self.binding)
        result = self._request("POST", path + "?dryRun=All", document=document)
        assert result is not None
        return result

    def create_resource(self, document: dict[str, Any]) -> None:
        path = self._approved(document, writing=True)
        self.verify_identity(self.binding)
        self._request("POST", path, document=document)


def _defaulted(api: ManagementStageAPI, desired: dict[str, Any]) -> dict[str, Any]:
    return _qualified_defaulted(desired, api.default_resource(desired))


def _qualified_defaulted(desired: dict[str, Any], observed: dict[str, Any]) -> dict[str, Any]:
    """Qualify the same workload defaults for fixed create and upgrade previews."""
    if not _contains(_canonical_quantities(observed), _canonical_quantities(desired)):
        raise ManagementStageError("management defaulting changed requested configuration")
    kind = desired["kind"]
    if kind == "Service":
        actual, wanted = observed["spec"], desired["spec"]
        if (actual.get("type", "ClusterIP") != "ClusterIP"
                or actual.get("selector") != wanted.get("selector")
                or any(actual.get(field) for field in (
                    "externalIPs", "externalName", "loadBalancerIP", "loadBalancerClass", "loadBalancerSourceRanges",
                    "allocateLoadBalancerNodePorts", "healthCheckNodePort", "externalTrafficPolicy",
                ))
                or actual.get("publishNotReadyAddresses", False) != wanted.get("publishNotReadyAddresses", False)
                or any(port.get("nodePort", 0) for port in actual.get("ports", []))):
            raise ManagementStageError("management defaulting changed private Service exposure")
    if kind in {"Deployment", "StatefulSet", "Job", "CronJob"}:
        actual_spec, wanted_spec = observed["spec"], desired["spec"]
        if kind == "CronJob":
            actual_spec, wanted_spec = actual_spec["jobTemplate"]["spec"], wanted_spec["jobTemplate"]["spec"]
        actual, wanted = actual_spec["template"]["spec"], wanted_spec["template"]["spec"]
        if (any(actual.get(field, False) != wanted.get(field, False) for field in (
            "hostNetwork", "hostPID", "hostIPC", "shareProcessNamespace",
        )) or any(actual.get(field) != wanted.get(field) for field in ("nodeName", "priorityClassName", "hostAliases"))
                or actual.get("securityContext", {}) != wanted.get("securityContext", {})
                or actual.get("ephemeralContainers", []) != wanted.get("ephemeralContainers", [])):
            raise ManagementStageError("management defaulting changed Pod security")
        for field in ("containers", "initContainers"):
            for container, expected in zip(actual.get(field, []), wanted.get(field, []), strict=True):
                if (container.get("securityContext", {}) != expected.get("securityContext", {})
                        or container.keys() - expected.keys() - {"imagePullPolicy", "terminationMessagePath", "terminationMessagePolicy"}):
                    raise ManagementStageError("management defaulting changed container security")
    return _comparison_snapshot(observed)


def _canonical_quantities(document: dict[str, Any], *, detached: bool = False) -> dict[str, Any]:
    """Exact decimal equality for API-equivalent resource spellings, no rounding."""
    from kubernetes.utils.quantity import parse_quantity

    # Only callers passing their own fresh snapshot may skip another full copy.
    result = document if detached else copy.deepcopy(document)
    kind = result.get("kind")
    if kind not in {"Deployment", "StatefulSet", "Job", "CronJob"}:
        return result
    spec = result["spec"]
    if kind == "CronJob":
        spec = spec["jobTemplate"]["spec"]
    pod = spec["template"]["spec"]
    resources = [item.get("resources", {}) for item in pod.get("containers", []) + pod.get("initContainers", [])]
    resources += [claim["spec"].get("resources", {}) for claim in spec.get("volumeClaimTemplates", [])]
    maps = [resource.get(section, {}) for resource in resources for section in ("requests", "limits")]
    maps += [volume["emptyDir"] for volume in pod.get("volumes", []) if "sizeLimit" in volume.get("emptyDir", {})]
    for quantities in maps:
        for key in quantities:
            if key == "medium":
                continue
            amount = parse_quantity(quantities[key])
            if not amount.is_finite() or amount < 0:
                raise ManagementStageError("management resource quantity invalid")
            quantities[key] = str(amount.normalize())
    return result


def _comparison_snapshot(document: dict[str, Any]) -> dict[str, Any]:
    """Check server-allocated Job labels before excluding them from dry-run comparison.

    Dry-run and persisted Jobs receive different UIDs. The full persisted snapshot
    still freezes those fields on replay; only the dry-run comparison omits them.
    """
    result = _canonical_quantities(_snapshot(document, allocation=False), detached=True)
    if document["kind"] == "Job":
        uid, name = _uid(document), document["metadata"]["name"]
        spec = result["spec"]
        if spec.get("manualSelector", False) or spec.pop("selector", None) != {
            "matchLabels": {"batch.kubernetes.io/controller-uid": uid},
        }:
            raise ManagementStageError("management Job allocation differs")
        labels = spec["template"]["metadata"]["labels"]
        for key, value in (("controller-uid", uid), ("job-name", name)):
            for prefix in ("", "batch.kubernetes.io/"):
                if labels.pop(prefix + key, None) != value:
                    raise ManagementStageError("management Job allocation differs")
    return result


def _validate_record(record: dict[str, Any], identity: dict[str, Any], documents: dict[str, dict[str, Any]]) -> None:
    if (not isinstance(record, dict) or set(record) != {*identity, "operation_id", "resources"}
            or any(record[key] != value for key, value in identity.items())
            or str(UUID(record["operation_id"])) != record["operation_id"] or UUID(record["operation_id"]).int == 0
            or set(record["resources"]) != set(documents)):
        raise ManagementStageError("management phase journal differs")
    for key, doc in documents.items():
        desired = copy.deepcopy(doc)
        desired["metadata"].setdefault("annotations", {})[_MARKER] = record["operation_id"]
        item = record["resources"][key]
        if (set(item) != {"desired", "expected", "status", "uid", "observed"}
                or item["desired"] != desired or item["status"] not in {"prepared", "create_intent", "created"}
                or not _contains(item["expected"], _canonical_quantities(desired))
                or (item["status"] == "created") != (item["uid"] is not None and item["observed"] is not None)
                or (item["status"] != "created" and (item["uid"] is not None or item["observed"] is not None))):
            raise ManagementStageError("management resource journal differs")


def stage_management_resources(*, rendered: RenderedManagement, phase: str, binding: ManagementBinding,
                               api: ManagementStageAPI, state_dir: Path) -> dict[str, Any]:
    """Freeze all defaults before creating one phase; unknown writes only read back."""
    documents = _documents(rendered, phase, binding)
    return _stage_fixed_documents(documents=documents, revision=rendered.revision, phase=phase,
                                  binding=binding, api=api, state_dir=state_dir)


def _stage_fixed_documents(*, documents: dict[str, dict[str, Any]], revision: str, phase: str,
                           binding: ManagementBinding, api: ManagementStageAPI, state_dir: Path,
                           default_document: Callable[[ManagementStageAPI, dict[str, Any]], dict[str, Any]] = _defaulted,
                           ) -> dict[str, Any]:
    """Internal journal engine; callers must freeze scope with a fixed renderer."""
    try:
        identity = {"schema": "loom.nebius-management-stage.v1", "binding": asdict(binding),
                    "revision": revision, "phase": phase}
        with private_state._locked_state(state_dir):
            api.verify_identity(binding)
            path = state_dir / "stage.json"
            if path.exists() or path.is_symlink():
                record = json.loads(private_state._private_read(path, limit=4 * 1024 * 1024))
            else:
                if any(api.get_resource(doc) is not None for doc in documents.values()):
                    raise ManagementStageError("untracked management resource; refusing adoption")
                operation = str(uuid4())
                resources = {}
                for key, doc in documents.items():
                    desired = copy.deepcopy(doc)
                    desired["metadata"].setdefault("annotations", {})[_MARKER] = operation
                    resources[key] = {"desired": desired, "expected": default_document(api, desired),
                                      "status": "prepared", "uid": None, "observed": None}
                record = {**identity, "operation_id": operation, "resources": resources}
                private_state._atomic_json(path, record)
            # Validate every item before the first write, including late entries.
            _validate_record(record, identity, documents)
            for key in documents:
                item = record["resources"][key]
                api.verify_identity(binding)
                actual = api.get_resource(item["desired"])
                if item["status"] == "prepared":
                    if actual is not None:
                        raise ManagementStageError("untracked management resource; refusing adoption")
                elif actual is None:
                    raise ManagementStageError("management create unresolved; preserve intent")
                elif (_comparison_snapshot(actual) != item["expected"]
                      or (item["uid"] is not None and (_uid(actual) != item["uid"] or _snapshot(actual) != item["observed"]))):
                    raise ManagementStageError("management resource differs from recorded intent")
            # JSON persistence sorts map keys. Only the renderer owns dependency
            # order (in particular admission policies before permission grants).
            for key in documents:
                item = record["resources"][key]
                api.verify_identity(binding)
                actual = api.get_resource(item["desired"])
                if item["status"] == "prepared":
                    if actual is not None:
                        raise ManagementStageError("untracked management resource; refusing adoption")
                    item["status"] = "create_intent"
                    private_state._atomic_json(path, record)
                    try:
                        api.create_resource(item["desired"])
                    except Exception:
                        pass
                    actual = api.get_resource(item["desired"])
                if actual is None:
                    raise ManagementStageError("management create unresolved; preserve intent")
                uid, snapshot = _uid(actual), _snapshot(actual)
                if (_comparison_snapshot(actual) != item["expected"]
                        or (item["uid"] is not None and (item["uid"] != uid or item["observed"] != snapshot))):
                    raise ManagementStageError("management resource differs from recorded intent")
                api.verify_identity(binding)
                if item["status"] != "created":
                    item.update(status="created", uid=uid, observed=snapshot)
                    private_state._atomic_json(path, record)
            for item in record["resources"].values():
                actual = api.get_resource(item["desired"])
                if actual is None or _uid(actual) != item["uid"] or _snapshot(actual) != item["observed"]:
                    raise ManagementStageError("management phase changed before final readback")
            api.verify_identity(binding)
            return {"status": "management_phase_staged", "installation_id": binding.installation_id,
                    "phase": phase, "revision": revision,
                    "resource_uids": {key: item["uid"] for key, item in record["resources"].items()}}
    except ManagementStageError:
        raise
    except Exception:
        raise ManagementStageError("management staging unavailable; preserve recovery evidence") from None


def management_phase_ready(*, rendered: RenderedManagement, phase: str, binding: ManagementBinding,
                           api: ManagementStageAPI, state_dir: Path) -> bool:
    """Observe one already-staged workload; never create, retry or replace it.

    A healthy Deployment is not public authentication proof. A CronJob is not
    backup or restore proof; those require the installer's separate checks.
    """
    try:
        if phase not in {"20-database.yaml", "30-migrate.yaml", "40-services.yaml", "85-backup-verify.yaml"}:
            raise ManagementStageError("phase has no management workload readiness proof")
        documents = _documents(rendered, phase, binding)
        path = state_dir / "stage.json"
        if not path.is_file():
            raise ManagementStageError("management phase recovery evidence missing")
        identity = {"schema": "loom.nebius-management-stage.v1", "binding": asdict(binding),
                    "revision": rendered.revision, "phase": phase}
        with private_state._locked_state(state_dir):
            record = json.loads(private_state._private_read(path, limit=4 * 1024 * 1024))
            _validate_record(record, identity, documents)
            ready = True
            for item in record["resources"].values():
                if item["status"] != "created":
                    raise ManagementStageError("management phase was not fully staged")
                api.verify_identity(binding)
                actual = api.get_resource(item["desired"])
                if actual is None or _uid(actual) != item["uid"] or _snapshot(actual) != item["observed"]:
                    raise ManagementStageError("management workload identity or configuration differs")
                kind, status = actual["kind"], actual.get("status", {})
                if kind == "Job":
                    conditions = {row["type"]: row["status"] for row in status.get("conditions", [])}
                    if conditions.get("Failed") == "True":
                        operation = "migration" if phase == "30-migrate.yaml" else "backup"
                        raise ManagementStageError(f"management {operation} failed; explicit recovery required")
                    ready &= conditions.get("Complete") == "True" and status.get("succeeded", 0) >= actual["spec"].get("completions", 1)
                elif kind in {"StatefulSet", "Deployment"}:
                    replicas = actual["spec"].get("replicas", 1)
                    ready &= (replicas > 0 and status.get("observedGeneration", 0) >= actual["metadata"].get("generation", 1)
                              and all(status.get(field, 0) == replicas for field in ("replicas", "readyReplicas", "updatedReplicas")))
                    if kind == "StatefulSet":
                        ready &= bool(status.get("currentRevision")) and status.get("currentRevision") == status.get("updateRevision")
                    else:
                        ready &= status.get("availableReplicas", 0) == replicas and status.get("unavailableReplicas", 0) == 0
            api.verify_identity(binding)
            return ready
    except ManagementStageError:
        raise
    except Exception:
        raise ManagementStageError("management readiness unavailable; preserve recovery evidence") from None
