"""Fixed private dev phases, consumed only by a protected installation journal.

No generic apply, namespace adoption, patch/delete, public or execution operation.
The caller retains independent phase-start evidence and qualifies supplied storage
identity; these phase receipts prove resources, not public/task/owner readiness.
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
from uuid import uuid4

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_development_bootstrap import (
    DevelopmentBootstrapBinding,
    DevelopmentBootstrapError,
    HTTPSDevelopmentBootstrapAPI,
    _observe_namespace,
    _uuid,
)
from scripts.ops.nebius_ingress_stage import _snapshot, _uid
from scripts.ops.nebius_management_stage import (
    _canonical_quantities,
    _comparison_snapshot,
    _qualified_defaulted,
)

from loom.nebius_development_foundation import render_development_foundation
from loom.nebius_platform_render import digest
from loom_service.environment_management.kubernetes_provider import _contains

_ROOT = Path(__file__).resolve().parents[2]
_LABEL = "loom.nebius/development-installation"
_MARKER = "loom.nebius/development-stage-operation"
_FILES = {"config": "10-config-network.yaml", "database": "20-database.yaml",
          "migration": "30-migrate.yaml", "services": "40-services.yaml"}
_RESOURCES = {"Secret": ("v1", "secrets"), "ConfigMap": ("v1", "configmaps"),
              "ServiceAccount": ("v1", "serviceaccounts"), "Service": ("v1", "services"),
              "NetworkPolicy": ("networking.k8s.io/v1", "networkpolicies"),
              "StatefulSet": ("apps/v1", "statefulsets"), "Deployment": ("apps/v1", "deployments"),
              "Job": ("batch/v1", "jobs")}
_STORAGE_KEYS = {"access-key", "secret-key", "source-access-key", "source-secret-key"}


class DevelopmentStageError(DevelopmentBootstrapError):
    """Closed diagnostic only; preserve private state and all remote resources."""


@dataclass(frozen=True)
class DevelopmentResourceBinding:
    bootstrap: DevelopmentBootstrapBinding
    namespace_uid: str
    operation_id: str

    def __post_init__(self) -> None:
        try:
            if not isinstance(self.bootstrap, DevelopmentBootstrapBinding):
                raise ValueError()
            _uuid(self.namespace_uid)
            _uuid(self.operation_id)
        except Exception:
            raise DevelopmentStageError("invalid development resource binding") from None


@dataclass(frozen=True, repr=False)
class DevelopmentStageInput:
    config: dict[str, Any]
    candidate: dict[str, Any]
    profile: dict[str, Any]
    keyring: dict[str, Any]
    storage: dict[str, str]


class DevelopmentStageAPI(Protocol):
    def verify_identity(self, binding: DevelopmentResourceBinding) -> None: ...
    def get_resource(self, document: dict[str, Any]) -> dict[str, Any] | None: ...
    def default_resource(self, document: dict[str, Any]) -> dict[str, Any]: ...
    def create_resource(self, document: dict[str, Any]) -> None: ...


def _key(document: dict[str, Any]) -> str:
    return str(document["kind"]) + ":" + str(document["metadata"]["name"])


def development_documents(selection: DevelopmentStageInput, binding: DevelopmentResourceBinding,
                          phase: str) -> tuple[str, dict[str, dict[str, Any]]]:
    """Derive scope from source/config, never from an owner-supplied manifest."""
    try:
        if (phase not in {*_FILES, "supplied"} or selection.config.get("db_tls_secret_name") != binding.bootstrap.tls_secret_name
                or set(selection.storage) != _STORAGE_KEYS
                or any(not isinstance(v, str) or not 0 < len(v.encode()) <= 65536 for v in selection.storage.values())):
            raise ValueError()
        rendered = render_development_foundation(selection.config, selection.candidate,
            selection.profile, selection.keyring, repo_root=_ROOT)
        revision = digest({"foundation": rendered.revision, "storage": selection.storage})
        rows: list[dict[str, Any]]
        if phase == "supplied":
            rows = [{"apiVersion": "v1", "kind": "Secret", "metadata": {
                "name": "loom-platform-storage", "namespace": "loom-dev"}, "immutable": True, "type": "Opaque",
                "data": {key: base64.b64encode(value.encode()).decode() for key, value in selection.storage.items()}}]
        else:
            rows = copy.deepcopy(rendered.files[_FILES[phase]])
        for row in rows:
            if row["metadata"].get("namespace") != "loom-dev" or row["apiVersion"] != _RESOURCES[row["kind"]][0]:
                raise ValueError()
            row["metadata"].setdefault("labels", {})[_LABEL] = binding.bootstrap.installation_id
            if row["kind"] == "StatefulSet":
                for claim in row["spec"]["volumeClaimTemplates"]:
                    claim["metadata"].setdefault("labels", {})[_LABEL] = binding.bootstrap.installation_id
            if row["kind"] == "Job":
                row["spec"].pop("ttlSecondsAfterFinished", None)
                if row["spec"]["backoffLimit"] != 0 or row["spec"]["template"]["spec"]["restartPolicy"] != "Never":
                    raise ValueError()
        documents = {_key(row): row for row in rows}
        if len(documents) != len(rows):
            raise ValueError()
        return revision, documents
    except Exception:
        raise DevelopmentStageError("resource outside fixed development phase") from None


class HTTPSDevelopmentStageAPI(HTTPSDevelopmentBootstrapAPI):
    """Explicit-trust connection with fixed renderer-derived resource paths."""

    error_type = DevelopmentStageError

    def __init__(self, *, binding: DevelopmentResourceBinding, selection: DevelopmentStageInput, phase: str,
                 api_server: str, ssl_context: ssl.SSLContext, token: str | None = None):
        self.resource_binding = binding
        self.selection, self.phase = copy.deepcopy(selection), phase
        _, self.documents = development_documents(self.selection, binding, phase)
        super().__init__(binding=binding.bootstrap, api_server=api_server, ssl_context=ssl_context, token=token)

    def verify_identity(self, binding: DevelopmentResourceBinding) -> None:
        try:
            if binding != self.resource_binding:
                raise ValueError()
            _observe_namespace(self, binding.bootstrap, binding.operation_id, binding.namespace_uid)
        except Exception:
            raise DevelopmentStageError("development namespace identity or policy differs") from None

    def _approved(self, document: dict[str, Any], *, writing: bool = False) -> str:
        try:
            desired = copy.deepcopy(document)
            expected = self.documents[_key(desired)]
            annotations = desired["metadata"].get("annotations", {})
            operation = annotations.pop(_MARKER, None)
            if writing or operation is not None:
                _uuid(operation)
            if not annotations and "annotations" not in expected["metadata"]:
                desired["metadata"].pop("annotations", None)
            if desired != expected:
                raise ValueError()
            api, resource = _RESOURCES[desired["kind"]]
            return ("/api/v1" if api == "v1" else "/apis/" + api) + "/namespaces/loom-dev/" + resource
        except Exception:
            raise DevelopmentStageError("resource outside fixed development phase") from None

    def get_resource(self, document: dict[str, Any]) -> dict[str, Any] | None:
        return self._request("GET", self._approved(document) + "/" + document["metadata"]["name"])

    def default_resource(self, document: dict[str, Any]) -> dict[str, Any]:
        path = self._approved(document, writing=True)
        self.verify_identity(self.resource_binding)
        result = self._request("POST", path + "?dryRun=All", document=document)
        assert result is not None
        return result

    def create_resource(self, document: dict[str, Any]) -> None:
        path = self._approved(document, writing=True)
        self.verify_identity(self.resource_binding)
        self._request("POST", path, document=document)

    def get_database_claim(self) -> dict[str, Any] | None:
        if self.phase != "database":
            raise DevelopmentStageError("storage outside fixed development phase")
        self.verify_identity(self.resource_binding)
        return self._request("GET", "/api/v1/namespaces/loom-dev/persistentvolumeclaims/data-loom-postgres-0")

    def get_database_volume(self) -> dict[str, Any] | None:
        claim = self.get_database_claim()
        name = (claim or {}).get("spec", {}).get("volumeName")
        if name is None:
            return None
        if not isinstance(name, str) or re.fullmatch(r"[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?", name) is None:
            raise DevelopmentStageError("invalid development volume identity")
        return self._request("GET", "/api/v1/persistentvolumes/" + name)


def _identity(binding: DevelopmentResourceBinding, phase: str, revision: str) -> dict[str, Any]:
    return {"schema": "loom.nebius-development-stage.v1", "binding": asdict(binding), "phase": phase, "revision": revision}


def _only_defaults(actual: dict[str, Any], wanted: dict[str, Any], defaults: dict[str, Any]) -> None:
    for key, default in defaults.items():
        if key not in wanted and key in actual and actual[key] == default:
            actual.pop(key)
    if actual != wanted:
        raise DevelopmentStageError("development defaulting changed fixed resource behavior")


def qualify_development_default(desired: dict[str, Any], observed: dict[str, Any]) -> dict[str, Any]:
    """Accept API defaults, not admission-added data sources or execution policy."""
    expected = _qualified_defaulted(desired, observed)
    wanted = _canonical_quantities(desired)
    actual = copy.deepcopy(expected)
    kind = desired["kind"]
    if kind == "Secret":
        if expected != desired:
            raise DevelopmentStageError("development defaulting changed supplied material")
    if kind in {"Deployment", "StatefulSet", "Job"}:
        pod, target = actual["spec"]["template"]["spec"], wanted["spec"]["template"]["spec"]
        for field in ("containers", "initContainers"):
            for container, goal in zip(pod.get(field, []), target.get(field, []), strict=True):
                for variable, wanted_variable in zip(container.get("env", []), goal.get("env", []), strict=True):
                    # Kubernetes omits an explicit empty literal value. This
                    # does not permit an injected valueFrom or another variable.
                    if wanted_variable.get("value") == "" and "value" not in variable:
                        variable["value"] = ""
                    _only_defaults(variable, wanted_variable, {})
                for port, wanted_port in zip(container.get("ports", []), goal.get("ports", []), strict=True):
                    _only_defaults(port, wanted_port, {"protocol": "TCP", "hostPort": 0, "hostIP": ""})
                for name in ("readinessProbe", "livenessProbe", "startupProbe"):
                    if name not in container:
                        continue
                    if name not in goal:
                        raise DevelopmentStageError("development defaulting introduced an unplanned probe")
                    probe, wanted_probe = container[name], goal[name]
                    if "httpGet" in probe:
                        _only_defaults(probe["httpGet"], wanted_probe["httpGet"], {"scheme": "HTTP"})
                    _only_defaults(probe, wanted_probe, {"timeoutSeconds": 1, "successThreshold": 1,
                        "failureThreshold": 3, "periodSeconds": 10, "initialDelaySeconds": 0})
                _only_defaults(container, goal, {"imagePullPolicy": "IfNotPresent",
                    "terminationMessagePath": "/dev/termination-log", "terminationMessagePolicy": "File"})
        for volume, wanted_volume in zip(pod.get("volumes", []), target.get("volumes", []), strict=True):
            for source in ("secret", "configMap"):
                if source in volume:
                    _only_defaults(volume[source], wanted_volume[source], {"defaultMode": 0o644, "optional": False})
            _only_defaults(volume, wanted_volume, {})
        _only_defaults(pod, target, {"dnsPolicy": "ClusterFirst", "restartPolicy": "Always",
            "schedulerName": "default-scheduler", "terminationGracePeriodSeconds": 30,
            "serviceAccount": target.get("serviceAccountName", "default")})
    if kind == "StatefulSet":
        for claim, goal in zip(actual["spec"]["volumeClaimTemplates"], wanted["spec"]["volumeClaimTemplates"], strict=True):
            _only_defaults(claim["spec"], goal["spec"], {"volumeMode": "Filesystem"})
            _only_defaults(claim["metadata"], goal["metadata"], {"creationTimestamp": None})
            _only_defaults(claim, goal, {"apiVersion": "v1", "kind": "PersistentVolumeClaim",
                "status": {"phase": "Pending"}})
    if kind == "Job":
        _only_defaults(actual["spec"], wanted["spec"], {"parallelism": 1, "completions": 1,
            "completionMode": "NonIndexed", "suspend": False, "manualSelector": False,
            "podReplacementPolicy": "TerminatingOrFailed"})
    return expected


def _validate(record: dict[str, Any], identity: dict[str, Any], documents: dict[str, dict[str, Any]]) -> None:
    if (not isinstance(record, dict) or set(record) != {*identity, "operation_id", "resources"}
            or any(record[key] != value for key, value in identity.items())
            or not isinstance(record["resources"], dict) or record["resources"].keys() != documents.keys()):
        raise DevelopmentStageError("development phase journal differs")
    _uuid(record["operation_id"])
    for key, document in documents.items():
        desired = copy.deepcopy(document)
        desired["metadata"].setdefault("annotations", {})[_MARKER] = record["operation_id"]
        item = record["resources"][key]
        if (not isinstance(item, dict) or set(item) != {"desired", "expected", "status", "uid", "observed"}
                or item["desired"] != desired or item["status"] not in {"prepared", "create_intent", "created"}
                or not _contains(item["expected"], _canonical_quantities(desired))
                or (item["status"] == "created") != (item["uid"] is not None and item["observed"] is not None)
                or (item["status"] != "created" and (item["uid"] is not None or item["observed"] is not None))):
            raise DevelopmentStageError("development resource journal differs")
        if item["uid"] is not None:
            _uuid(item["uid"])


def _observed(api: DevelopmentStageAPI, item: dict[str, Any]) -> tuple[str, dict[str, Any], dict[str, Any]]:
    actual = api.get_resource(item["desired"])
    if actual is None:
        raise DevelopmentStageError("development create unresolved; preserve intent")
    uid, snapshot = _uid(actual), _snapshot(actual)
    if (_comparison_snapshot(actual) != item["expected"] or (item["uid"] is not None and (
            item["uid"] != uid or item["observed"] != snapshot))):
        raise DevelopmentStageError("development resource identity or configuration differs")
    return uid, snapshot, actual


def stage_development_resources(*, selection: DevelopmentStageInput, binding: DevelopmentResourceBinding,
                                phase: str, api: DevelopmentStageAPI, state_dir: Path) -> dict[str, Any]:
    """Create only derived objects; caller must retain independent phase-start evidence."""
    try:
        revision, documents = development_documents(selection, binding, phase)
        identity = _identity(binding, phase, revision)
        with private_state._locked_state(state_dir):
            api.verify_identity(binding)
            path = state_dir / "stage.json"
            if path.exists() or path.is_symlink():
                record = json.loads(private_state._private_read(path, limit=4 * 1024 * 1024))
            else:
                if any(api.get_resource(doc) is not None for doc in documents.values()):
                    raise DevelopmentStageError("untracked development resource; refusing adoption")
                operation, resources = str(uuid4()), {}
                for key, doc in documents.items():
                    desired = copy.deepcopy(doc)
                    desired["metadata"].setdefault("annotations", {})[_MARKER] = operation
                    expected = qualify_development_default(desired, api.default_resource(desired))
                    resources[key] = {"desired": desired, "expected": expected,
                        "status": "prepared", "uid": None, "observed": None}
                record = {**identity, "operation_id": operation, "resources": resources}
                private_state._atomic_json(path, record)
            _validate(record, identity, documents)
            # Check the whole retained phase before any resumed write.
            for key in documents:
                api.verify_identity(binding)
                item = record["resources"][key]
                if item["status"] == "prepared":
                    if api.get_resource(item["desired"]) is not None:
                        raise DevelopmentStageError("untracked development resource; refusing adoption")
                else:
                    _observed(api, item)
            for key in documents:
                api.verify_identity(binding)
                item = record["resources"][key]
                if item["status"] == "prepared":
                    if api.get_resource(item["desired"]) is not None:
                        raise DevelopmentStageError("untracked development resource; refusing adoption")
                    item["status"] = "create_intent"
                    private_state._atomic_json(path, record)
                    try:
                        api.create_resource(item["desired"])
                    except Exception:
                        pass
                uid, snapshot, _ = _observed(api, item)
                api.verify_identity(binding)
                if item["status"] != "created":
                    item.update(status="created", uid=uid, observed=snapshot)
                    private_state._atomic_json(path, record)
            for item in record["resources"].values():
                _observed(api, item)
            api.verify_identity(binding)
            return {"status": "development_phase_staged", "phase": phase, "revision": revision,
                "installation_id": binding.bootstrap.installation_id, "namespace_uid": binding.namespace_uid,
                "resource_uids": {key: item["uid"] for key, item in record["resources"].items()}}
    except DevelopmentStageError:
        raise
    except Exception:
        raise DevelopmentStageError("development staging unavailable; preserve recovery evidence") from None


def development_phase_ready(*, selection: DevelopmentStageInput, binding: DevelopmentResourceBinding,
                            phase: str, api: DevelopmentStageAPI, state_dir: Path) -> bool:
    """Current controller/Job readiness only; never starts or repairs resources."""
    try:
        if phase not in {"database", "migration", "services"} or not (state_dir / "stage.json").is_file():
            raise DevelopmentStageError("development readiness requires retained workload phase")
        revision, documents = development_documents(selection, binding, phase)
        with private_state._locked_state(state_dir):
            record = json.loads(private_state._private_read(state_dir / "stage.json", limit=4 * 1024 * 1024))
            _validate(record, _identity(binding, phase, revision), documents)
            ready = True
            for item in record["resources"].values():
                if item["status"] != "created":
                    raise DevelopmentStageError("development workload not fully staged")
                api.verify_identity(binding)
                _, _, actual = _observed(api, item)
                kind, status = actual["kind"], actual.get("status", {})
                if kind == "Job":
                    conditions = {row["type"]: row["status"] for row in status.get("conditions", [])}
                    if conditions.get("Failed") == "True":
                        raise DevelopmentStageError("development migration failed; explicit recovery required")
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
    except DevelopmentStageError:
        raise
    except Exception:
        raise DevelopmentStageError("development readiness unavailable; preserve recovery evidence") from None
