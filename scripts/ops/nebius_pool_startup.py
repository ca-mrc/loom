"""Start only the anchored cutover successors, with global admission still closed.

This internal stage is not runtime acceptance or permission to open admission.
The same operation lock protects closure and startup. An uncertain PATCH is
observed, never repeated; unchanged replicas alone cannot disprove a late write.
"""
from __future__ import annotations

import copy
import json
from dataclasses import asdict
from pathlib import Path
from typing import Any, Protocol, TypedDict

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_ingress_stage import _copy_document, _key, _snapshot, _uid
from scripts.ops.nebius_management_stage import _qualified_defaulted, _validate_record
from scripts.ops.nebius_management_switch import _matches, _stable
from scripts.ops.nebius_pool_cutover import (
    PoolCutoverRequest,
    _contract,
    _cutover_documents,
    _read_cutover_evidence,
    qualify_cutover_image_admission,
)
from scripts.ops.nebius_pool_migration import _hash, migration_contract
from scripts.ops.nebius_pool_projection import pure_projection
from scripts.ops.nebius_pool_retirement import retirement_documents, stopped_documents

from loom.nebius_platform_render import digest


class PoolWorkloadReader(Protocol):
    def read_workload(self, key: str) -> dict[str, Any]: ...


class PoolStartupAPI(PoolWorkloadReader, Protocol):
    def qualify_closed(self) -> None:
        """Observe exact closed epoch, guards, credentials and retired authority."""
        ...

    def read_workload(self, key: str) -> dict[str, Any]: ...
    def preview_workload(self, key: str, before: dict[str, Any], desired: dict[str, Any]) -> dict[str, Any] | None: ...
    def start_workload(self, key: str, before: dict[str, Any], desired: dict[str, Any]) -> bool:
        """One UID/resourceVersion/spec CAS; False only for definite rejection."""
        ...


class _StartupProjection(TypedDict):
    documents: dict[str, Any]
    originals: dict[str, dict[str, Any]]
    stopped: dict[str, dict[str, Any]]
    contract_sha256: str
    migration_contract_sha256: str


@pure_projection
def _startup_projection(request: PoolCutoverRequest) -> _StartupProjection:
    """Derive related expectations together, never read operational evidence.

    Recovery repeatedly needs this whole bundle. Memoizing siblings separately
    retraverses their shared installation for every ancestor of every phase.
    The existing bounded memo still snapshots the complete raw typed request on
    every call and returns detached documents. Clock admission stays outside.
    """
    documents = _cutover_documents(request)
    return {'documents': documents,
        'originals': {**retirement_documents(request.fencing.retirement), **documents['producers']},
        'stopped': stopped_documents(request.fencing.retirement),
        'contract_sha256': digest(_contract(request, documents)),
        'migration_contract_sha256': digest(migration_contract(request.fencing.retirement.migration))}


def closed_startup_documents(request: PoolCutoverRequest, *, state_dir: Path,
                             anchor_dir: Path) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    """Derive closed roots and fixed starts from complete immutable child evidence.

    Retired collectors and explicitly dormant foreign-target roots stay closed.
    The gateway identity comes from its created child receipt, never its name.
    """
    qualify_cutover_image_admission(request)
    projection = _startup_projection(request)
    documents = projection['documents']
    record = _read_cutover_evidence(request, documents, state_dir, anchor_dir,
        contract_sha256=projection['contract_sha256'],
        migration_contract_sha256=projection['migration_contract_sha256'])
    if (record is None or record["fenced"] is None
            or any(item["phase"] != "stopped" for group in ("producers", "runtime") for item in record[group].values())
            or any(phase != "staged" for phase in record["runtime_access"].values())
            or set(record["phases"]) != {"material", "configuration", "authority", "workload"}
            or any(checksum is None for checksum in record["phases"].values())):
        raise ValueError
    originals = projection['originals']
    closed = _copy_document(projection['stopped'])
    closed.update({key: copy.deepcopy(item["expected"]) for key, item in record["runtime"].items()})
    for key, document in closed.items():
        document["metadata"]["uid"] = _uid(originals[key])
    for phase in ("configuration", "authority", "workload"):
        targets = {_key(row): row for row in documents[phase]}
        child = json.loads(private_state._private_read(state_dir / phase / "stage.json", limit=4 * 1024**2))
        _validate_record(child, {"schema": "loom.nebius-management-stage.v1",
            "binding": asdict(request.fencing.retirement.migration.registration.binding),
            "revision": digest(targets), "phase": "pool-cutover-" + phase}, targets)
        for key, item in child["resources"].items():
            if (item["status"] != "created"
                    or _qualified_defaulted(item["desired"], item["observed"]) != item["expected"]):
                raise ValueError
            if phase == "workload":
                value = copy.deepcopy(item["observed"])
                value["metadata"]["uid"] = item["uid"]
                _uid(value)
                if key in closed or value["kind"] != "Deployment":
                    raise ValueError
                closed[key] = value
    # Renderer order starts the manager first and the fixed gateway second.
    keys = [_key(request.manager), *(_key(row) for row in documents["workload"]),
        *(key for key in documents["runtime"] if key != _key(request.manager))]
    targets = {}
    for key in keys:
        target = _snapshot(closed[key])
        field = "suspend" if target["kind"] == "CronJob" else "replicas"
        expected = True if field == "suspend" else 0
        if type(target["spec"][field]) is not type(expected) or target["spec"][field] != expected:
            raise ValueError
        target["spec"][field] = False if field == "suspend" else 1
        targets[key] = target
    # Filesystem reads can release the GIL. Reject changed retained inputs even
    # if the journal bytes still match the expectation captured at entry.
    if _startup_projection(request) != projection:
        raise ValueError('pool_startup_inputs_changed')
    return closed, targets


def _startup_record(request: PoolCutoverRequest, *, state: Path, anchor: Path,
                    closed: dict[str, dict[str, Any]], targets: dict[str, dict[str, Any]],
                    ) -> tuple[dict[str, Any], dict[str, Any] | None]:
    operation = str(request.fencing.retirement.migration.registration.spec.operation_id)
    identity = {"schema": "loom.nebius-pool-startup.v1", "operation_id": operation,
        "state_dir": str(state), "closure_sha256": _hash(state / "cutover.json"),
        "workloads_sha256": digest({"closed": closed, "targets": targets})}
    marker, path = anchor / (operation + "-startup.json"), state / "startup.json"
    if not marker.exists() and not marker.is_symlink():
        if path.exists() or path.is_symlink():
            raise ValueError
        return identity, None
    if json.loads(private_state._private_read(marker)) != identity:
        raise ValueError
    record = json.loads(private_state._private_read(path, limit=4 * 1024**2))
    if (not isinstance(record, dict) or set(record) != {*identity, "workloads"}
            or any(record[key] != value for key, value in identity.items())
            or set(record["workloads"]) != set(targets)):
        raise ValueError
    for item in record["workloads"].values():
        if set(item) != {"phase", "before_resource_version"} or item["phase"] not in {"prepared", "intent", "started"}:
            raise ValueError
        version = item["before_resource_version"]
        if item["phase"] == "prepared":
            if version is not None:
                raise ValueError
        elif not isinstance(version, str) or not 0 < len(version) <= 128:
            raise ValueError
    return identity, record


def _observe_workloads(api: PoolWorkloadReader, closed: dict[str, dict[str, Any]],
                       targets: dict[str, dict[str, Any]], record: dict[str, Any]) -> dict[str, dict[str, Any]]:
    observed = {}
    for key, original in closed.items():
        actual = api.read_workload(key)
        item = record["workloads"].get(key)
        phase = "prepared" if item is None else item["phase"]
        choices = ([original] if phase == "prepared" else [targets[key]] if phase == "started"
            else [original, targets[key]])
        if not any(_matches(actual, desired, _uid(original)) for desired in choices):
            raise ValueError
        observed[key] = actual
    return observed


def startup_workload_options(request: PoolCutoverRequest, *, state_dir: Path,
                             anchor_dir: Path) -> dict[str, tuple[dict[str, Any], ...]] | None:
    """Read-only recovery projection, including both sides of an uncertain CAS.

    Nothing here asserts health, write rejection or permission to roll back.
    Callers must match their actual retained UID against one of these templates.
    """
    operation = str(request.fencing.retirement.migration.registration.spec.operation_id)
    paths = (state_dir / "startup.json", anchor_dir / (operation + "-startup.json"))
    from scripts.ops.nebius_pool_legacy_restart import (
        legacy_restart_exists,
        restarted_legacy_options,
    )
    from scripts.ops.nebius_pool_shutdown import shutdown_exists, shutdown_workload_options
    from scripts.ops.nebius_pool_startup_fence import fenced_startup_options, startup_fence_exists
    from scripts.ops.nebius_pool_startup_repair import repaired_startup_options
    from scripts.ops.nebius_pool_template_restoration import (
        restored_template_options,
        template_restoration_exists,
    )

    started = any(path.exists() or path.is_symlink() for path in paths)
    if (not started and not startup_fence_exists(request, state=state_dir, anchor=anchor_dir)
            and not shutdown_exists(request, state=state_dir, anchor=anchor_dir)
            and not template_restoration_exists(request, state=state_dir, anchor=anchor_dir)
            and not legacy_restart_exists(request, state=state_dir, anchor=anchor_dir)):
        return None
    closed, targets = closed_startup_documents(request, state_dir=state_dir, anchor_dir=anchor_dir)
    _, record = _startup_record(request, state=state_dir, anchor=anchor_dir, closed=closed, targets=targets)
    if record is None and started:
        raise ValueError
    choices: dict[str, tuple[dict[str, Any], ...]] = {}
    for key, original in closed.items():
        phase = ({} if record is None else record["workloads"]).get(key, {"phase": "prepared"})["phase"]
        choices[key] = ((original,) if phase == "prepared" else (targets[key],) if phase == "started"
            else (original, targets[key]))
    choices = repaired_startup_options(request, state=state_dir, anchor=anchor_dir, choices=choices)
    choices = fenced_startup_options(request, state=state_dir, anchor=anchor_dir,
        closed=closed, targets=targets, startup=record, choices=choices)
    choices = shutdown_workload_options(request, state=state_dir, anchor=anchor_dir, choices=choices)
    choices = restored_template_options(request, state=state_dir, anchor=anchor_dir, choices=choices)
    return restarted_legacy_options(request, state=state_dir, anchor=anchor_dir, choices=choices)


def stage_pool_startup(*, request: PoolCutoverRequest, api: PoolStartupAPI,
                       state_dir: Path, anchor_dir: Path) -> dict[str, Any]:
    """Start fixed successors under closed admission; expose no public activation."""
    try:
        state, anchor = state_dir.absolute(), anchor_dir.absolute()
        with private_state._locked_state(anchor):
            operation = str(request.fencing.retirement.migration.registration.spec.operation_id)
            if any(path.exists() or path.is_symlink() for path in (
                    state / 'activation.json', anchor / (operation + '-activation.json'))):
                raise ValueError
            closed, targets = closed_startup_documents(request, state_dir=state, anchor_dir=anchor)
            identity, record = _startup_record(request, state=state, anchor=anchor, closed=closed, targets=targets)
            if record is None:
                record = {**identity, "workloads": {key: {"phase": "prepared", "before_resource_version": None} for key in targets}}
                api.qualify_closed()
                _observe_workloads(api, closed, targets, record)
                private_state._atomic_json(anchor / (identity["operation_id"] + "-startup.json"), identity)
                private_state._atomic_json(state / "startup.json", record)

            def save() -> None:
                private_state._atomic_json(state / "startup.json", record)

            def result(status: str) -> dict[str, Any]:
                return {"status": status, "operation_id": identity["operation_id"],
                    "admission_open": False, "runtime_verified": False}

            for key, desired in targets.items():
                item = record["workloads"][key]
                if item["phase"] == "started":
                    # Every remaining write and the final barrier qualify all
                    # roots. Replay needs one complete read, not N identical ones.
                    continue
                api.qualify_closed()
                actual = _observe_workloads(api, closed, targets, record)[key]
                if item["phase"] == "prepared":
                    # Preserve the server's exact representation; parent records
                    # canonicalize quantities for comparison, not for PATCHing.
                    field = "suspend" if actual["kind"] == "CronJob" else "replicas"
                    desired = _snapshot(actual)
                    desired["spec"][field] = targets[key]["spec"][field]
                    preview = api.preview_workload(key, actual, desired)
                    if preview is None:
                        return result("pending_startup_update")
                    # Startup changes one scalar, never the retained/defaulted template.
                    if _stable(preview) != _stable(desired):
                        raise ValueError
                    version = actual["metadata"]["resourceVersion"]
                    if not isinstance(version, str) or not 0 < len(version) <= 128:
                        raise ValueError
                    api.qualify_closed()
                    item.update(phase="intent", before_resource_version=version)
                    save()
                    try:
                        accepted = api.start_workload(key, actual, desired)
                    except Exception:
                        accepted = None
                    if accepted is False:
                        item.update(phase="prepared", before_resource_version=None)
                        save()
                        return result("pending_startup_update")
                    actual = api.read_workload(key)
                if not _matches(actual, desired, _uid(closed[key])):
                    if item["phase"] == "intent" and _matches(actual, closed[key], _uid(closed[key])):
                        return result("pending_startup_outcome")
                    raise ValueError
                if item["phase"] != "started":
                    item["phase"] = "started"
                    save()
            api.qualify_closed()
            _observe_workloads(api, closed, targets, record)
            return result("pool_startup_staged_closed")
    except Exception:
        raise ValueError("pool_startup_unconfirmed_preserve_evidence") from None
