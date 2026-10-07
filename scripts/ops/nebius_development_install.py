"""Anchored private dev installation; no public route, runtime writer or CLI.

The fixed protected caller owns actual source/cloud/quota qualification and live
dependency probes. Fresh namespace-absence checks are never used for recovery.
Unknown writes and missing journals cannot be interpreted as fresh installation.
"""
from __future__ import annotations

import copy
import hashlib
import json
from contextlib import AbstractContextManager
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Protocol
from uuid import uuid4

from kubernetes.utils.quantity import parse_quantity
from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_development_bootstrap import (
    DevelopmentBootstrapAPI,
    DevelopmentBootstrapBinding,
    _uuid,
    bootstrap_development,
)
from scripts.ops.nebius_development_stage import (
    DevelopmentResourceBinding,
    DevelopmentStageAPI,
    DevelopmentStageInput,
    development_documents,
    development_phase_ready,
    stage_development_resources,
)
from scripts.ops.nebius_ingress_stage import _uid

from loom.nebius_platform_render import digest

_PHASES = ("bootstrap", "config", "supplied", "database", "storage", "migration", "services")
_LABEL = "loom.nebius/development-installation"
_CLAIM = "data-loom-postgres-0"
_DRIVER = "compute.csi.nebius.com"


class DevelopmentInstallError(RuntimeError):
    """Sanitized failure; keep resources, keys and independent recovery evidence."""


@dataclass(frozen=True, repr=False)
class DevelopmentInstallRequest:
    bootstrap: DevelopmentBootstrapBinding
    selection: DevelopmentStageInput


class DevelopmentStorageAPI(DevelopmentStageAPI, Protocol):
    def get_database_claim(self) -> dict[str, Any] | None: ...
    def get_database_volume(self) -> dict[str, Any] | None: ...


class DevelopmentInstallationAPI(Protocol):
    def qualify(self, request: DevelopmentInstallRequest, *, fresh: bool) -> None:
        """Authenticate candidate/source/cloud credentials, quota and current fit.

        Fresh checks require namespace absence. Retained checks validate the same
        frozen inputs beside already-journaled resources, never adopt or rotate.
        This must perform live reads, not accept caller-supplied readiness flags.
        """
        ...

    def bootstrap_api(self) -> AbstractContextManager[DevelopmentBootstrapAPI]: ...
    def resources(self, binding: DevelopmentResourceBinding, selection: DevelopmentStageInput,
                  phase: str) -> AbstractContextManager[DevelopmentStorageAPI]: ...
    def qualify_volume(self, request: DevelopmentInstallRequest, binding: DevelopmentResourceBinding,
                       observation: dict[str, Any]) -> None:
        """Read actual CSI disk: expected project/region, fresh identity and capacity.

        Kubernetes annotations alone cannot qualify Nebius provider ownership.
        The authenticated provider adapter must reject foreign or reused disks.
        """
        ...

    def verify_private_dependencies(self, request: DevelopmentInstallRequest, binding: DevelopmentResourceBinding,
                                    material_state: Path) -> None:
        """Probe authenticated internal API plus actual database/object dependencies."""
        ...


def _phase_files(phase: str, *, complete: bool) -> tuple[str, ...]:
    if phase == "bootstrap":
        return ("bootstrap.json",)
    if phase == "database":
        return ("storage-intent.json", "stage.json")
    if phase == "storage":
        return ("intent.json", "stage.json") if complete else ("intent.json",)
    return ("stage.json",)


def _hashes(state: Path, phase: str, *, complete: bool) -> dict[str, str]:
    return {name: hashlib.sha256(private_state._private_read(state / phase / name, limit=4 * 1024 * 1024)).hexdigest()
            for name in _phase_files(phase, complete=complete)}


def _history(record: dict[str, Any], identity: dict[str, Any], state: Path) -> None:
    if (not isinstance(record, dict) or set(record) != {*identity, "phases"}
            or any(record[key] != value for key, value in identity.items())
            or not isinstance(record["phases"], dict) or set(record["phases"]) != set(_PHASES[:len(record["phases"])])):
        raise DevelopmentInstallError("development installation history differs")
    for index, phase in enumerate(_PHASES[:len(record["phases"])]):
        item = record["phases"][phase]
        if (not isinstance(item, dict) or set(item) != {"status", "receipt", "journals"}
                or item["status"] not in {"started", "complete"}
                or (item["status"] == "started" and (
                    index != len(record["phases"]) - 1 or item["receipt"] is not None or item["journals"] is not None))):
            raise DevelopmentInstallError("development installation phase differs")
        complete = item["status"] == "complete"
        current = _hashes(state, phase, complete=complete)
        if complete and item["journals"] != current:
            raise DevelopmentInstallError("development retained phase evidence changed")


def _storage_intent(binding: DevelopmentResourceBinding, revision: str) -> dict[str, Any]:
    return {"schema": "loom.nebius-development-storage-intent.v1", "binding": asdict(binding),
        "revision": revision, "claim_name": _CLAIM}


def _prepare_storage(binding: DevelopmentResourceBinding, revision: str, api: DevelopmentStorageAPI, state: Path) -> None:
    intent = _storage_intent(binding, revision)
    with private_state._locked_state(state):
        api.verify_identity(binding)
        path = state / "storage-intent.json"
        if path.exists() or path.is_symlink():
            if json.loads(private_state._private_read(path)) != intent:
                raise DevelopmentInstallError("development storage intent differs")
        else:
            if (state / "stage.json").exists() or api.get_database_claim() is not None:
                raise DevelopmentInstallError("untracked development database claim; refusing adoption")
            private_state._atomic_json(path, intent)


def _storage_observation(request: DevelopmentInstallRequest, binding: DevelopmentResourceBinding,
                         api: DevelopmentStorageAPI) -> dict[str, Any] | None:
    api.verify_identity(binding)
    claim, volume = api.get_database_claim(), api.get_database_volume()
    if claim is None:
        return None
    if (claim.get("apiVersion") != "v1" or claim.get("kind") != "PersistentVolumeClaim"
            or claim["metadata"].get("name") != _CLAIM or claim["metadata"].get("namespace") != "loom-dev"
            or claim["metadata"].get("labels", {}).get(_LABEL) != binding.bootstrap.installation_id
            or claim["metadata"].get("deletionTimestamp") or claim["metadata"].get("ownerReferences")):
        raise DevelopmentInstallError("development database claim identity differs")
    pvc_uid = _uid(claim)
    spec = copy.deepcopy(claim["spec"])
    requested = parse_quantity(spec["resources"]["requests"]["storage"])
    expected = parse_quantity(str(request.selection.config["postgres_storage_gi"]) + "Gi")
    if (not requested.is_finite() or requested != expected or spec.get("volumeMode", "Filesystem") != "Filesystem"
            or spec.get("storageClassName") != request.selection.config["storage_class"]
            or spec.get("accessModes") != ["ReadWriteOnce"]
            or any(spec.get(key) is not None for key in ("dataSource", "dataSourceRef", "selector", "volumeAttributesClassName"))):
        raise DevelopmentInstallError("development database claim configuration differs")
    if claim.get("status", {}).get("phase") != "Bound" or volume is None:
        return None
    if (volume.get("apiVersion") != "v1" or volume.get("kind") != "PersistentVolume"
            or volume["metadata"].get("name") != "pvc-" + pvc_uid or spec.get("volumeName") != "pvc-" + pvc_uid
            or volume["metadata"].get("deletionTimestamp") or volume["metadata"].get("ownerReferences")
            or volume["metadata"].get("annotations", {}).get("pv.kubernetes.io/provisioned-by") != _DRIVER
            or volume.get("status", {}).get("phase") != "Bound"):
        raise DevelopmentInstallError("development dynamically provisioned volume identity differs")
    pv_spec = copy.deepcopy(volume["spec"])
    ref, csi = pv_spec["claimRef"], pv_spec["csi"]
    capacity = parse_quantity(pv_spec["capacity"]["storage"])
    handle = csi.get("volumeHandle")
    if (any(ref.get(key) != value for key, value in {"namespace": "loom-dev", "name": _CLAIM, "uid": pvc_uid}.items())
            or pv_spec.get("storageClassName") != spec["storageClassName"] or csi.get("driver") != _DRIVER
            or pv_spec.get("volumeMode", "Filesystem") != "Filesystem" or pv_spec.get("accessModes") != ["ReadWriteOnce"]
            or not capacity.is_finite() or capacity < requested
            or not isinstance(handle, str) or not 0 < len(handle) <= 1024 or any(char.isspace() for char in handle)):
        raise DevelopmentInstallError("development physical data-volume binding differs")
    ref.pop("resourceVersion", None)
    spec["resources"]["requests"]["storage"] = str(requested.normalize())
    pv_spec["capacity"]["storage"] = str(capacity.normalize())
    api.verify_identity(binding)
    return {"pvc_uid": pvc_uid, "pv_uid": _uid(volume), "pv_name": volume["metadata"]["name"],
        "pvc_spec": spec, "pv_spec": pv_spec}


def _verify_storage(request: DevelopmentInstallRequest, binding: DevelopmentResourceBinding, revision: str,
                    api: DevelopmentStorageAPI, database_state: Path, state: Path) -> dict[str, Any] | None:
    intent = _storage_intent(binding, revision)
    if json.loads(private_state._private_read(database_state / "storage-intent.json")) != intent:
        raise DevelopmentInstallError("development storage absence evidence differs")
    with private_state._locked_state(state):
        start = state / "intent.json"
        if start.exists() or start.is_symlink():
            if json.loads(private_state._private_read(start)) != intent:
                raise DevelopmentInstallError("development storage phase identity differs")
        else:
            private_state._atomic_json(start, intent)
        path = state / "stage.json"
        observed = _storage_observation(request, binding, api)
        if observed is None:
            if path.exists() or path.is_symlink():
                raise DevelopmentInstallError("development retained database storage missing")
            return None
        record = {**intent, **observed}
        if path.exists() or path.is_symlink():
            if json.loads(private_state._private_read(path, limit=1024 * 1024)) != record:
                raise DevelopmentInstallError("development retained database storage changed")
        else:
            private_state._atomic_json(path, record)
        if _storage_observation(request, binding, api) != observed:
            raise DevelopmentInstallError("development storage changed during readback")
        return {"status": "development_storage_verified", "pvc_uid": observed["pvc_uid"], "pv_uid": observed["pv_uid"]}


def _final_readback(request: DevelopmentInstallRequest, api: DevelopmentInstallationAPI,
                    binding: DevelopmentResourceBinding, state: Path, anchor: Path,
                    record: dict[str, Any], revision: str) -> str | None:
    """All phases are complete; replay is a read-only identity/readiness check."""
    if any(item["status"] != "complete" for item in record["phases"].values()) or len(record["phases"]) != len(_PHASES):
        raise DevelopmentInstallError("development final readback requires complete phase evidence")
    _history(record, {key: value for key, value in record.items() if key != "phases"}, state)
    with api.bootstrap_api() as bootstrap_api:
        bootstrap = bootstrap_development(binding=request.bootstrap, api=bootstrap_api,
            state_dir=state / "bootstrap", anchor_dir=anchor / "local-bootstrap")
    if bootstrap != record["phases"]["bootstrap"]["receipt"]:
        raise DevelopmentInstallError("development local credentials changed before final readback")
    for phase in _PHASES[1:]:
        with api.resources(binding, request.selection, "database" if phase == "storage" else phase) as stage_api:
            if phase == "storage":
                receipt = _verify_storage(request, binding, revision, stage_api, state / "database", state / "storage")
                api.qualify_volume(request, binding, json.loads(private_state._private_read(
                    state / "storage" / "stage.json", limit=1024 * 1024)))
            else:
                receipt = stage_development_resources(selection=request.selection, binding=binding,
                    phase=phase, api=stage_api, state_dir=state / phase)
            if receipt != record["phases"][phase]["receipt"]:
                raise DevelopmentInstallError("development resources changed before final readback")
            if phase in {"database", "migration", "services"} and not development_phase_ready(
                    selection=request.selection, binding=binding, phase=phase, api=stage_api, state_dir=state / phase):
                return phase
    return None


def install_private_development(*, request: DevelopmentInstallRequest, api: DevelopmentInstallationAPI,
                                state_dir: Path, anchor_dir: Path) -> dict[str, Any]:
    """Advance a private installation to its next live readiness barrier."""
    try:
        request = copy.deepcopy(request)
        state, anchor = state_dir.absolute(), anchor_dir.absolute()
        if (state != state.resolve() or anchor != anchor.resolve() or state == anchor
                or state in anchor.parents or anchor in state.parents):
            raise DevelopmentInstallError("development installer requires independent recovery anchor")
        # Validate all frozen source/material inputs before any marker or mutation.
        provisional = DevelopmentResourceBinding(request.bootstrap, str(uuid4()), str(uuid4()))
        revision, _ = development_documents(request.selection, provisional, "config")
        identity = {"schema": "loom.nebius-development-install.v1", "input_digest": digest(asdict(request)),
            "state_dir": str(state), "binding": asdict(request.bootstrap)}
        with private_state._locked_state(anchor):
            marker, journal = anchor / (request.bootstrap.installation_id + ".json"), state / "installation.json"
            fresh = not (marker.exists() or marker.is_symlink())
            started: dict[str, Any]
            record: dict[str, Any]
            if fresh:
                if state.exists() or state.is_symlink():
                    raise DevelopmentInstallError("untracked development installation state; refusing adoption")
                api.qualify(request, fresh=True)
                started = {**identity, "operation_id": str(uuid4())}
                record = {**started, "phases": {}}
                private_state._atomic_json(marker, started)
                state.mkdir(mode=0o700)
            else:
                started = json.loads(private_state._private_read(marker))
                if (not isinstance(started, dict) or set(started) != {*identity, "operation_id"}
                        or any(started[key] != value for key, value in identity.items())):
                    raise DevelopmentInstallError("development installation recovery identity differs")
                _uuid(started["operation_id"])
                record = json.loads(private_state._private_read(journal, limit=1024 * 1024))
                _history(record, started, state)
            with private_state._locked_state(state):
                if fresh:
                    private_state._atomic_json(journal, record)
                api.qualify(request, fresh=False)
                binding: DevelopmentResourceBinding | None = None
                storage: dict[str, Any] | None = None

                def pending(phase: str) -> dict[str, Any]:
                    return {"status": "pending", "phase": phase, "installation_id": request.bootstrap.installation_id,
                        "namespace": "loom-dev", "revision": revision}

                for phase in _PHASES:
                    if phase not in record["phases"]:
                        record["phases"][phase] = {"status": "started", "receipt": None, "journals": None}
                        private_state._atomic_json(journal, record)
                    item, phase_state = record["phases"][phase], state / phase
                    if phase == "bootstrap":
                        with api.bootstrap_api() as bootstrap_api:
                            receipt = bootstrap_development(binding=request.bootstrap, api=bootstrap_api,
                                state_dir=phase_state, anchor_dir=anchor / "local-bootstrap")
                        local = json.loads(private_state._private_read(phase_state / "bootstrap.json", limit=1024 * 1024))
                        binding = DevelopmentResourceBinding(request.bootstrap, receipt["namespace_uid"], local["operation_id"])
                    else:
                        assert binding is not None
                        with api.resources(binding, request.selection, "database" if phase == "storage" else phase) as stage_api:
                            if phase == "storage":
                                storage = _verify_storage(request, binding, revision, stage_api, state / "database", phase_state)
                                if storage is None:
                                    return pending("storage")
                                api.qualify_volume(request, binding, json.loads(private_state._private_read(
                                    phase_state / "stage.json", limit=1024 * 1024)))
                                receipt = storage
                            else:
                                if phase == "database":
                                    _prepare_storage(binding, revision, stage_api, phase_state)
                                receipt = stage_development_resources(selection=request.selection, binding=binding,
                                    phase=phase, api=stage_api, state_dir=phase_state)
                    if item["status"] == "complete":
                        if item["receipt"] != receipt:
                            raise DevelopmentInstallError("development installation receipt changed")
                    else:
                        item.update(status="complete", receipt=receipt, journals=_hashes(state, phase, complete=True))
                        private_state._atomic_json(journal, record)
                    if phase in {"storage", "migration", "services"}:
                        assert binding is not None
                        readiness_phase = "database" if phase == "storage" else phase
                        with api.resources(binding, request.selection, readiness_phase) as stage_api:
                            if not development_phase_ready(selection=request.selection, binding=binding, phase=readiness_phase,
                                    api=stage_api, state_dir=state / readiness_phase):
                                return pending(readiness_phase)
                assert binding is not None and storage is not None
                api.verify_private_dependencies(request, binding, state / "bootstrap")
                changed = _final_readback(request, api, binding, state, anchor, record, revision)
                if changed is not None:
                    return pending(changed)
                return {"status": "development_private_installed", "installation_id": request.bootstrap.installation_id,
                    "namespace": "loom-dev", "namespace_uid": binding.namespace_uid, "revision": revision, "storage": storage}
    except DevelopmentInstallError:
        raise
    except Exception:
        raise DevelopmentInstallError("development installation incomplete; preserve recovery evidence") from None
