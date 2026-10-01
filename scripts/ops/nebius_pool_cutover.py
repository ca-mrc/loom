"""Connected protected cutover through closed runtime staging.

The entry must qualify publication, predecessor, backend and complete writer
inventory. Producer drain is not database-access retirement: quiescence also
qualifies application access, schema readiness and trusted queued origins. This
parent composes the real closure/fencing/material stages and preserves their
receipts across runtime replacement. It never opens admission or starts a Pod.
"""
from __future__ import annotations

import copy
import json
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol
from uuid import UUID

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_ingress_stage import _key, _snapshot, _uid
from scripts.ops.nebius_management_stage import (
    ManagementStageAPI,
    _qualified_defaulted,
    _stage_fixed_documents,
)
from scripts.ops.nebius_management_switch import _matches, _stable
from scripts.ops.nebius_pool_material import deliver_pool_material, machine_documents
from scripts.ops.nebius_pool_migration import (
    PoolMigrationAPI,
    _hash,
    close_and_register_pool,
    migration_contract,
)
from scripts.ops.nebius_pool_retirement import MARKER as RETIREMENT_MARKER
from scripts.ops.nebius_pool_retirement import _closed, retirement_documents, stopped_document
from scripts.ops.nebius_pool_role_fencing import (
    PoolRoleFenceAPI,
    PoolRoleFenceRequest,
    fence_pool_roles,
    role_fence_documents,
)
from scripts.ops.nebius_pool_runtime import (
    participant_readonly_roles,
    wire_collector,
    wire_manager,
    wire_participant,
)

from loom.nebius_platform_render import digest
from loom.service_execution_materialization import ServiceExecutionRuntimeProfileV1
from loom_service.pool_management.installation_render import render_gateway

MARKER = "loom.nebius/pool-cutover-operation"


@dataclass(frozen=True, repr=False)
class PoolCutoverRequest:
    fencing: PoolRoleFenceRequest
    manager: dict[str, Any]
    services: tuple[dict[str, Any], ...]
    collector_config: dict[str, Any]
    profiles: dict[UUID, ServiceExecutionRuntimeProfileV1]
    management_origin: str
    kubernetes_endpoint: str


class PoolCutoverAPI(Protocol):
    migration: PoolMigrationAPI
    fencing: PoolRoleFenceAPI
    resources: ManagementStageAPI

    def preflight(self, request: PoolCutoverRequest) -> None:
        """Qualify immutable publication/predecessor/backend and writer inventory."""
        ...

    def qualify_quiescence(self) -> None:
        """No active application access; ready schema and original backlog provenance."""
        ...

    def qualify_runtime_access(self, participant_id: UUID, action: str) -> None:
        """Fixed stage/observe of every retained participant runtime DB role."""
        ...

    def read_workload(self, key: str) -> dict[str, Any]: ...
    def preview_workload(self, key: str, before: dict[str, Any], desired: dict[str, Any]) -> dict[str, Any] | None:
        """None only for a definite dry-run rejection; no persistent write."""
        ...
    def patch_workload(self, key: str, before: dict[str, Any], desired: dict[str, Any]) -> bool:
        """False only for definite rejection; unknown outcomes are never retried."""
        ...

    def drained_workload(self, key: str, desired: dict[str, Any]) -> bool: ...


def cutover_documents(request: PoolCutoverRequest) -> dict[str, Any]:
    """Generate targets from retained originals, never accept arbitrary manifests."""
    migration = request.fencing.retirement.migration
    spec, binding = migration.registration.spec, migration.registration.binding
    originals = retirement_documents(request.fencing.retirement)
    role_fence_documents(request.fencing)
    participants = {row.participant_id: row for row in spec.participants}
    if (set(request.profiles) != set(participants) or len(request.services) != len(participants)
            or {row["metadata"]["namespace"] for row in request.services} != {row.namespace for row in migration.guards}):
        raise ValueError("pool cutover service roster differs")
    producers = {_key(row): row for row in (request.manager, *request.services)}
    if len(producers) != 1 + len(participants) or len({_uid(row) for row in producers.values()} | {_uid(row) for row in originals.values()}) != len(producers) + len(originals):
        raise ValueError("pool cutover workload identity differs")
    runtime = {_key(request.manager): wire_manager(request=migration, original=request.manager)}
    for guard in migration.guards:
        participant = participants[guard.participant_id]
        service, = (row for row in request.services if row["metadata"]["namespace"] == guard.namespace)
        actuator, = (row for row in request.fencing.retirement.actuators
            if row["metadata"]["namespace"] == participant.execution_namespace.name and row["metadata"]["name"] == "loom-execution-actuator")
        guests = tuple(row for row in request.fencing.retirement.actuators
            if row["metadata"]["namespace"] == participant.execution_namespace.name and row is not actuator)
        wired = wire_participant(request=migration, participant_id=guard.participant_id,
            management_origin=request.management_origin, actuator=actuator, service=service,
            runtime_profile=request.profiles[guard.participant_id], guest_actuators=guests)
        runtime.update({_key(row): row for row in wired.values()})
    development, = (row for row in spec.participants if row.environment_class == "development")
    collector, = (row for row in request.fencing.retirement.collectors if row["metadata"]["namespace"] == development.execution_namespace.name)
    observer = wire_collector(request=migration, original=collector, config_map=request.collector_config,
        management_origin=request.management_origin)
    runtime.update({_key(row): row for row in observer["workload"]})
    gateway = render_gateway(spec, namespace=binding.namespace,
        service_image=migration.registration.candidate["images"]["service"]["image_ref"], kubernetes_endpoint=request.kubernetes_endpoint)
    # Keep retirement markers: normal standalone rollouts must remain fenced.
    for key, row in runtime.items():
        row = _snapshot(row)
        row["metadata"].setdefault("annotations", {})[MARKER] = str(spec.operation_id)
        if key in originals:
            row["metadata"]["annotations"][RETIREMENT_MARKER] = str(spec.operation_id)
        runtime[key] = row
    stopped = {}
    for key, row in producers.items():
        value = _snapshot(row)
        value["spec"]["replicas"] = 0
        value["metadata"].setdefault("annotations", {})[MARKER] = str(spec.operation_id)
        stopped[key] = value
    reader_identity = tuple(row for row in participant_readonly_roles(request=migration)
        if row["kind"] in {"ClusterRole", "ClusterRoleBinding"})
    return {"producers": producers, "stopped": stopped, "runtime": runtime,
        "configuration": (*gateway["configuration"], *observer["configuration"], *reader_identity),
        "authority": gateway["authority"], "workload": gateway["workload"]}


def _contract(request: PoolCutoverRequest, documents: dict[str, Any]) -> dict[str, Any]:
    return {"migration": migration_contract(request.fencing.retirement.migration),
        "roles": {"originals": [_stable(row) for row in request.fencing.originals], "targets": role_fence_documents(request.fencing)},
        "writers": {key: {"uid": _uid(row), "document": _stable(row)} for key, row in retirement_documents(request.fencing.retirement).items()},
        "producers": {key: {"uid": _uid(row), "document": _stable(row)} for key, row in documents["producers"].items()},
        "collector_config": {"uid": _uid(request.collector_config), "document": _stable(request.collector_config)},
        "documents": {key: value for key, value in documents.items() if key != "producers"}}


def _read_cutover_record(request: PoolCutoverRequest, documents: dict[str, Any],
                         state: Path, anchor: Path) -> dict[str, Any] | None:
    """The same anchored recovery validation serves preflight and mutation."""
    if (state != state.resolve() or anchor != anchor.resolve() or state == anchor
            or state in anchor.parents or anchor in state.parents):
        raise ValueError
    migration = request.fencing.retirement.migration
    operation = str(migration.registration.spec.operation_id)
    identity = {"schema": "loom.nebius-pool-cutover.v1", "operation_id": operation,
        "state_dir": str(state), "contract_sha256": digest(_contract(request, documents))}
    marker, path = anchor / (operation + "-cutover.json"), state / "cutover.json"
    if not marker.exists() and not marker.is_symlink():
        if state.exists() or state.is_symlink():
            raise ValueError
        return None
    if json.loads(private_state._private_read(marker)) != identity:
        raise ValueError
    record = json.loads(private_state._private_read(path, limit=4 * 1024**2))
    if (not isinstance(record, dict) or set(record) != {*identity, "producers", "fenced", "runtime_access", "phases", "runtime"}
            or any(record[key] != value for key, value in identity.items())
            or set(record["producers"]) != set(documents["producers"])
            or set(record["runtime"]) != set(documents["runtime"])
            or not isinstance(record["runtime_access"], dict)
            or set(record["runtime_access"]) != {str(row.participant_id) for row in migration.guards}
            or any(value not in {"prepared", "intent", "staged"} for value in record["runtime_access"].values())
            or not set(record["phases"]) <= {"material", "configuration", "authority", "workload"}):
        raise ValueError
    for group, targets in (("producers", documents["stopped"]), ("runtime", documents["runtime"])):
        for key, item in record[group].items():
            if (set(item) != {"phase", "expected"} or item["phase"] not in {"prepared", "intent", "stopped"}
                    or (item["phase"] == "prepared") != (item["expected"] is None)):
                raise ValueError
            if item["expected"] is not None and _qualified_defaulted(targets[key], item["expected"]) != item["expected"]:
                raise ValueError
    writer_state, writer_anchor = state / "writers", state / "writer-anchor"
    if record["fenced"] is not None:
        if (not isinstance(record["fenced"], dict)
                or set(record["fenced"]) != {"migration.json", "retirement.json", "role-fencing.json"}
                or any(_hash(writer_state / name) != checksum for name, checksum in record["fenced"].items())):
            raise ValueError
        _closed(migration, writer_state, writer_anchor)
    elif (any(value != "prepared" for value in record["runtime_access"].values()) or record["phases"]
            or any(item["phase"] != "prepared" for item in record["runtime"].values())):
        raise ValueError
    for phase, checksum in record["phases"].items():
        if checksum is not None and _hash(state / phase / "stage.json") != checksum:
            raise ValueError
    return record


def retained_cutover_workloads(request: PoolCutoverRequest, *, state_dir: Path, anchor_dir: Path) -> dict[str, dict[str, Any]]:
    """Read only: derive each original or exactly journal-qualified successor.

    This is not drain, runtime-health or database evidence. It selects the right
    workload for those checks without requiring a retired Pod to run again.
    An unanchored state file or a zero replica count is never migration evidence.
    """
    try:
        documents = cutover_documents(request)
        originals = retirement_documents(request.fencing.retirement)
        expected = {**originals, **documents["producers"]}
        record = _read_cutover_record(request, documents, state_dir, anchor_dir)
        if record is None:
            return copy.deepcopy(expected)
        for key, item in record["producers"].items():
            if item["phase"] != "prepared":
                expected[key] = item["expected"]
        state, anchor = state_dir / "writers", state_dir / "writer-anchor"
        operation = str(request.fencing.retirement.migration.registration.spec.operation_id)
        marker, path = anchor / (operation + "-retirement.json"), state / "retirement.json"
        if marker.exists() or marker.is_symlink():
            identity = {"schema": "loom.nebius-pool-retirement.v1", "operation_id": operation, "state_dir": str(state),
                "closure_sha256": _closed(request.fencing.retirement.migration, state, anchor),
                "originals_sha256": digest({key: {"uid": _uid(row), "document": _stable(row)} for key, row in originals.items()})}
            child = json.loads(private_state._private_read(path, limit=4 * 1024**2))
            if (json.loads(private_state._private_read(marker)) != identity
                    or set(child) != {*identity, "workloads"}
                    or any(child[key] != value for key, value in identity.items())
                    or set(child["workloads"]) != set(originals)
                    or any(value not in {"prepared", "intent", "stopped"} for value in child["workloads"].values())):
                raise ValueError
            for key, phase in child["workloads"].items():
                if phase != "prepared":
                    expected[key] = stopped_document(request.fencing.retirement, key)
        elif path.exists() or path.is_symlink() or record["fenced"] is not None:
            raise ValueError
        for key, item in record["runtime"].items():
            if item["phase"] != "prepared":
                expected[key] = item["expected"]
        return copy.deepcopy(expected)
    except Exception:
        raise ValueError("pool_workload_recovery_unqualified") from None


def _updates(*, api: PoolCutoverAPI, originals: dict[str, Any], targets: dict[str, Any],
             items: dict[str, Any], save: Callable[[], None], pending: str) -> str | None:
    for key, original in originals.items():
        item = items[key]
        actual = api.read_workload(key)
        if item["phase"] == "prepared":
            if not _matches(actual, original, _uid(original)):
                raise ValueError
            preview = api.preview_workload(key, actual, targets[key])
            if preview is None:
                return pending  # No mutation intent or retry of a rejected dry run.
            item["expected"] = _qualified_defaulted(targets[key], preview)
            item["phase"] = "intent"
            save()
            try:
                accepted = api.patch_workload(key, actual, targets[key])
            except Exception:
                accepted = True
            if accepted is False:
                item.update(phase="prepared", expected=None)
                save()
                return pending
            if accepted is not True:
                raise ValueError
            actual = api.read_workload(key)
        if not _matches(actual, item["expected"], _uid(original)):
            raise ValueError
        if item["phase"] != "stopped":
            item["phase"] = "stopped"
            save()
        if api.drained_workload(key, item["expected"]) is not True:
            return "pending_producer_drain" if pending == "pending_producer_update" else "pending_runtime_drain"
    for key, original in originals.items():
        if not _matches(api.read_workload(key), items[key]["expected"], _uid(original)):
            raise ValueError
        if api.drained_workload(key, items[key]["expected"]) is not True:
            return "pending_producer_drain" if pending == "pending_producer_update" else "pending_runtime_drain"
    return None


def stage_pool_cutover(*, request: PoolCutoverRequest, tokens: dict[UUID, str], api: PoolCutoverAPI,
                       state_dir: Path, anchor_dir: Path) -> dict[str, Any]:
    """Freeze → close → retire/fence → material/ACLs → disabled runtime.

    Runtime replacement changes the original templates. Recovery after that
    boundary qualifies retained child hashes, roles and current target templates;
    it must NOT replay retirement/fencing against the original stopped templates.
    Activation, rollback and durable refresh completion remain subsequent gates.
    """
    try:
        state, anchor = state_dir.absolute(), anchor_dir.absolute()
        if (state != state.resolve() or anchor != anchor.resolve() or state == anchor
                or state in anchor.parents or anchor in state.parents):
            raise ValueError
        documents = cutover_documents(request)
        migration = request.fencing.retirement.migration
        machine_documents(migration, tokens)  # Qualify all hashes before downtime.
        operation = str(migration.registration.spec.operation_id)
        identity = {"schema": "loom.nebius-pool-cutover.v1", "operation_id": operation,
            "state_dir": str(state), "contract_sha256": digest(_contract(request, documents))}
        writer_state, writer_anchor = state / "writers", state / "writer-anchor"
        with private_state._locked_state(anchor):
            path, marker = state / "cutover.json", anchor / (operation + "-cutover.json")
            record = _read_cutover_record(request, documents, state, anchor)
            if record is None:
                api.preflight(request)
                private_state._atomic_json(marker, identity)
                private_state._private_directory(state)
                record = {**identity, "producers": {key: {"phase": "prepared", "expected": None} for key in documents["producers"]},
                    "fenced": None, "runtime_access": dict.fromkeys((str(row.participant_id) for row in migration.guards), "prepared"), "phases": {},
                    "runtime": {key: {"phase": "prepared", "expected": None} for key in documents["runtime"]}}

            def save() -> None:
                private_state._atomic_json(path, record)

            def result(status: str) -> dict[str, Any]:
                return {"status": status, "operation_id": operation, "writer_migration_complete": False}

            save()
            api.preflight(request)
            runtime_started = any(item["phase"] != "prepared" for item in record["runtime"].values())
            if not runtime_started:
                pending = _updates(api=api, originals=documents["producers"], targets=documents["stopped"],
                    items=record["producers"], save=save, pending="pending_producer_update")
                if pending:
                    return result(pending)
            api.qualify_quiescence()
            if record["fenced"] is None:
                closed = close_and_register_pool(request=migration, api=api.migration,
                    state_dir=writer_state, anchor_dir=writer_anchor)
                if closed["status"] != "pool_registered_closed":
                    return closed
                fenced = fence_pool_roles(request=request.fencing, api=api.fencing,
                    state_dir=writer_state, anchor_dir=writer_anchor)
                if fenced["status"] != "participant_roles_restricted":
                    return fenced
                record["fenced"] = {name: _hash(writer_state / name) for name in ("migration.json", "retirement.json", "role-fencing.json")}
                save()

            def verify_fenced() -> None:
                if any(api.migration.guard(target, "observe").get("status") != "held" for target in migration.guards):
                    raise ValueError
                targets = role_fence_documents(request.fencing)
                if any(not _matches(api.fencing.read_role(_key(row)), targets[_key(row)], _uid(row)) for row in request.fencing.originals):
                    raise ValueError
                api.fencing.verify_readonly()
                for key, original in documents["producers"].items():
                    item = record["runtime"][key]
                    desired = item["expected"] if item["phase"] != "prepared" else record["producers"][key]["expected"]
                    if not _matches(api.read_workload(key), desired, _uid(original)) or api.drained_workload(key, desired) is not True:
                        raise ValueError
                # Before each stage, all retained writers must be stopped in
                # precisely the old or journaled new template, never a third one.
                for key, original in retirement_documents(request.fencing.retirement).items():
                    item = record["runtime"].get(key)
                    desired = (item["expected"] if item is not None and item["phase"] != "prepared"
                        else stopped_document(request.fencing.retirement, key))
                    if not _matches(api.read_workload(key), desired, _uid(original)) or api.drained_workload(key, desired) is not True:
                        raise ValueError

            verify_fenced()
            for guard in migration.guards:
                key = str(guard.participant_id)
                if record["runtime_access"][key] == "prepared":
                    record["runtime_access"][key] = "intent"
                    save()
                    try:
                        api.qualify_runtime_access(guard.participant_id, "stage")
                    except Exception:
                        pass  # Unknown SQL outcome: only exact qualification may recover.
                api.qualify_runtime_access(guard.participant_id, "observe")
                record["runtime_access"][key] = "staged"
                save()
            for phase in ("material", "configuration", "authority", "workload"):
                verify_fenced()
                if phase not in record["phases"]:
                    record["phases"][phase] = None
                    save()
                if phase == "material":
                    deliver_pool_material(request=migration, tokens=tokens, api=api.resources, state_dir=state / phase)
                else:
                    stage_documents = {_key(row): row for row in documents[phase]}
                    _stage_fixed_documents(documents=stage_documents, revision=digest(stage_documents), phase="pool-cutover-" + phase,
                        binding=migration.registration.binding, api=api.resources, state_dir=state / phase)
                checksum = _hash(state / phase / "stage.json")
                if record["phases"][phase] not in {None, checksum}:
                    raise ValueError
                record["phases"][phase] = checksum
                save()
            previous = {key: (documents["stopped"][key] if key in documents["stopped"]
                else stopped_document(request.fencing.retirement, key)) for key in documents["runtime"]}
            retained = {**retirement_documents(request.fencing.retirement), **documents["producers"]}
            previous = copy.deepcopy(previous)
            for key, document in previous.items():
                document["metadata"]["uid"] = _uid(retained[key])
            pending = _updates(api=api, originals=previous, targets=documents["runtime"],
                items=record["runtime"], save=save, pending="pending_runtime_update")
            if pending:
                return result(pending)
            verify_fenced()
            api.qualify_quiescence()
            for guard in migration.guards:
                api.qualify_runtime_access(guard.participant_id, "observe")
            # Replacement can take long enough for a previously qualified
            # gateway/catalog/credential to drift. Recheck every retained stage
            # with GETs only; completion must not silently repair or recreate it.
            for phase, checksum in record["phases"].items():
                child = state / phase / "stage.json"
                if checksum is None or _hash(child) != checksum:
                    raise ValueError
                staged = json.loads(private_state._private_read(child, limit=4 * 1024**2))
                for item in staged["resources"].values():
                    if item["status"] != "created":
                        raise ValueError
                    api.resources.verify_identity(migration.registration.binding)
                    actual = api.resources.get_resource(item["desired"])
                    if actual is None or _uid(actual) != item["uid"] or _snapshot(actual) != item["observed"]:
                        raise ValueError
            api.resources.verify_identity(migration.registration.binding)
            return result("pool_runtime_staged_closed")
    except Exception:
        raise ValueError("pool_cutover_unconfirmed_preserve_evidence") from None
