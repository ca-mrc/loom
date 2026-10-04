"""Stop only the retained old pool processes; no activation, RBAC or deletion.

Closed registration and durable idle guards precede this separate phase. It
must not replay initial closure once the original CP Pods have been retired.
The protected parent owns publication, database migrations and later fencing.
"""
from __future__ import annotations

import copy
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_ingress_stage import _key, _snapshot, _uid
from scripts.ops.nebius_management_switch import _matches, _stable
from scripts.ops.nebius_pool_dormant import DormantPoolConsumer, dormant_retirement_documents
from scripts.ops.nebius_pool_migration import (
    PoolMigrationRequest,
    _hash,
    _proof,
    migration_contract,
)
from scripts.ops.nebius_pool_projection import pure_projection
from scripts.ops.nebius_pool_runtime import qualify_participant_actuators

from loom.nebius_platform_render import digest

MARKER = "loom.nebius/pool-retirement-operation"


@dataclass(frozen=True, repr=False)
class PoolRetirementRequest:
    migration: PoolMigrationRequest
    actuators: tuple[dict[str, Any], ...]
    collectors: tuple[dict[str, Any], ...]
    dormant_consumers: tuple[DormantPoolConsumer, ...] = ()


class PoolRetirementAPI(Protocol):
    def verify_guards(self) -> None: ...
    def read(self, key: str) -> dict[str, Any]: ...
    def stop(self, key: str, before: dict[str, Any]) -> bool:
        """False only on a complete definite rejection; exceptions are unknown."""
        ...

    def drained(self, key: str) -> bool: ...


@pure_projection
def retirement_documents(request: PoolRetirementRequest) -> dict[str, dict[str, Any]]:
    try:
        migration_contract(request.migration)
        participants = request.migration.registration.spec.participants
        namespaces = {row.execution_namespace.name for row in participants}
        primary_actuators = tuple(row for row in request.actuators if row["metadata"]["name"] == "loom-execution-actuator")
        if {row["metadata"]["namespace"] for row in request.actuators} != namespaces:
            raise ValueError
        for participant in participants:
            actuator, = (row for row in primary_actuators if row["metadata"]["namespace"] == participant.execution_namespace.name)
            guests = tuple(row for row in request.actuators if row["metadata"]["namespace"] == participant.execution_namespace.name
                and row["metadata"]["name"] != "loom-execution-actuator")
            qualify_participant_actuators(request=request.migration, participant_id=participant.participant_id, actuator=actuator, guests=guests)
        for documents, kind, name in ((primary_actuators, "Deployment", "loom-execution-actuator"),
                (request.collectors, "CronJob", "loom-execution-capacity-collector")):
            if len(documents) != len(participants) or {row["metadata"]["namespace"] for row in documents} != namespaces:
                raise ValueError
            for document in documents:
                if (document.get("apiVersion") != ("apps/v1" if kind == "Deployment" else "batch/v1")
                        or document.get("kind") != kind or document["metadata"].get("name") != name
                        or MARKER in document["metadata"].get("annotations", {})):
                    raise ValueError
                if kind == "Deployment":
                    if (type(document["spec"].get("replicas")) is not int or document["spec"]["replicas"] != 1
                            or document["spec"]["selector"] != {"matchLabels": {"app.kubernetes.io/name": name}}):
                        raise ValueError
                elif type(document["spec"].get("suspend")) is not bool or document["spec"].get("concurrencyPolicy") != "Forbid":
                    raise ValueError
        dormant = dormant_retirement_documents(migration=request.migration, actuators=request.actuators,
            consumers=request.dormant_consumers)
        originals = (*request.collectors, *(row for row in dormant.values() if row["kind"] == "CronJob"),
            *request.actuators, *(row for row in dormant.values() if row["kind"] == "Deployment"),
            *(row.controller for row in request.migration.guards))
        if len({_uid(row) for row in originals}) != len(originals):
            raise ValueError
        result = {}
        for document in originals:
            _snapshot(document)
            if MARKER in document["metadata"].get("annotations", {}):
                raise ValueError
            result[_key(document)] = copy.deepcopy(document)
        return result
    except Exception:
        raise ValueError("pool_retirement_inputs_unqualified") from None


def _stopped_document(original: dict[str, Any], operation: str) -> dict[str, Any]:
    desired = _snapshot(original)
    desired["metadata"].setdefault("annotations", {})[MARKER] = operation
    desired["spec"]["suspend" if desired["kind"] == "CronJob" else "replicas"] = True if desired["kind"] == "CronJob" else 0
    return desired


def stopped_documents(request: PoolRetirementRequest) -> dict[str, dict[str, Any]]:
    """Project one qualified roster without rescanning it for every workload."""
    originals = retirement_documents(request)
    operation = str(request.migration.registration.spec.operation_id)
    return {key: _stopped_document(original, operation) for key, original in originals.items()}


def stopped_document(request: PoolRetirementRequest, key: str) -> dict[str, Any]:
    return _stopped_document(retirement_documents(request)[key], str(request.migration.registration.spec.operation_id))


def _closed(request: PoolMigrationRequest, state: Path, anchor: Path) -> str:
    return _read_closed_migration(request, state, anchor, contract_sha256=digest(migration_contract(request)))


def _read_closed_migration(request: PoolMigrationRequest, state: Path, anchor: Path, *, contract_sha256: str) -> str:
    """Read all closure evidence afresh against an input-derived expectation."""
    operation = str(request.registration.spec.operation_id)
    identity = {"schema": "loom.nebius-pool-migration.v1", "operation_id": operation,
        "state_dir": str(state), "contract_sha256": contract_sha256}
    if json.loads(private_state._private_read(anchor / (operation + ".json"))) != identity:
        raise ValueError
    record = json.loads(private_state._private_read(state / "migration.json", limit=4 * 1024**2))
    if (set(record) != {*identity, "guards", "registration"}
            or any(record[key] != value for key, value in identity.items())
            or record["guards"] != {str(row.participant_id): "held" for row in request.guards}
            or not isinstance(record["registration"], dict)
            or set(record["registration"]) != {"stage_sha256", "proof"}
            or record["registration"]["stage_sha256"] != _hash(state / "registration/stage.json")):
        raise ValueError
    _proof(request, state / "registration", record["registration"]["proof"])
    return digest(record)


def qualify_pool_drain(request: PoolRetirementRequest, *, key: str, current: dict[str, Any],
                       children: dict[str, Any], pods: dict[str, Any]) -> bool:
    return qualify_closed_workload_drain(original=retirement_documents(request)[key],
        desired=stopped_document(request, key), current=current, children=children, pods=pods)


def qualify_closed_workload_drain(*, original: dict[str, Any], desired: dict[str, Any], current: dict[str, Any],
                                  children: dict[str, Any], pods: dict[str, Any]) -> bool:
    """Complete namespace collections; terminating controller Pods still count.

Completed collector history may remain for retention, but every container must
be terminal. No observation here grants or restores any Kubernetes write role.
"""
    try:
        if (original["kind"] not in {"Deployment", "CronJob"}
                or desired["kind"] != original["kind"]
                or not _matches(current, desired, _uid(original))
                or (desired["spec"].get("replicas") != 0 if original["kind"] == "Deployment"
                    else desired["spec"].get("suspend") is not True)):
            raise ValueError
        deployment = original["kind"] == "Deployment"
        namespace = original["metadata"]["namespace"]

        def collection(document: dict[str, Any], kind: str, version: str) -> list[dict[str, Any]]:
            revision = document["metadata"]["resourceVersion"]
            if (document["apiVersion"] != version or document["kind"] != kind + "List"
                    or not isinstance(revision, str) or not 0 < len(revision) <= 128
                    or document["metadata"].get("continue") or not isinstance(document["items"], list)
                    or len(document["items"]) > 1000):
                raise ValueError
            rows = [{"apiVersion": version, "kind": kind, **row} for row in document["items"]]
            if (any(row["apiVersion"] != version or row["kind"] != kind or row["metadata"]["namespace"] != namespace for row in rows)
                    or len({_uid(row) for row in rows}) != len(rows)):
                raise ValueError
            return rows

        def owned(row: dict[str, Any], parent: dict[str, Any]) -> bool:
            owners = row["metadata"].get("ownerReferences", [])
            if not any(owner.get("uid") == _uid(parent) for owner in owners):
                return False
            if len(owners) != 1:
                raise ValueError
            owner = dict(owners[0])
            blocking = owner.pop("blockOwnerDeletion", False)
            if (type(blocking) is not bool or owner != {"apiVersion": parent["apiVersion"], "kind": parent["kind"],
                    "name": parent["metadata"]["name"], "uid": _uid(parent), "controller": True}):
                raise ValueError
            return True

        def observed_zero(row: dict[str, Any]) -> bool:
            generation = row["metadata"]["generation"]
            status = row.get("status", {})
            observed = status.get("observedGeneration", 0)
            counts = [row["spec"].get("replicas", 1), *(status.get(field, 0) for field in (
                "replicas", "readyReplicas", "availableReplicas", "updatedReplicas", "unavailableReplicas", "fullyLabeledReplicas", "terminatingReplicas"))]
            if (type(generation) is not int or generation <= 0 or type(observed) is not int or observed < 0
                    or any(type(value) is not int or value < 0 for value in counts)):
                raise ValueError
            return observed >= generation and all(value == 0 for value in counts)

        descendants = collection(children, "ReplicaSet" if deployment else "Job", "apps/v1" if deployment else "batch/v1")
        workload_pods = collection(pods, "Pod", "v1")
        selected = [row for row in descendants if owned(row, current)]
        labels = (current["spec"]["selector"]["matchLabels"] if deployment
            else current["spec"]["jobTemplate"]["spec"]["template"]["metadata"]["labels"])
        if not labels:
            raise ValueError

        def labelled(row: dict[str, Any]) -> bool:
            return all(row["metadata"].get("labels", {}).get(name) == value for name, value in labels.items())

        selected_pods = [row for row in workload_pods if labelled(row) or any(owned(row, parent) for parent in selected)]
        if deployment:
            if any(labelled(row) and row not in selected for row in descendants):
                raise ValueError
            return observed_zero(current) and all(observed_zero(row) for row in selected) and not selected_pods
        active = current.get("status", {}).get("active", [])
        if not isinstance(active, list):
            raise ValueError
        if active:
            return False
        for job in selected:
            status = job.get("status", {})
            running = status.get("active", 0)
            if type(running) is not int or running < 0:
                raise ValueError
            if running or not any(row.get("type") in {"Complete", "Failed"} and row.get("status") == "True" for row in status.get("conditions", [])):
                return False
        for pod in selected_pods:
            if pod.get("status", {}).get("phase") not in {"Succeeded", "Failed"}:
                return False
            for field, status_field in (("containers", "containerStatuses"), ("initContainers", "initContainerStatuses"),
                    ("ephemeralContainers", "ephemeralContainerStatuses")):
                expected = {row["name"] for row in pod["spec"].get(field, [])}
                statuses = pod["status"].get(status_field, [])
                if len(statuses) != len(expected) or {row["name"] for row in statuses} != expected:
                    return False
                for row in statuses:
                    state = row.get("state", {})
                    if set(state) != {"terminated"} or type(state["terminated"].get("exitCode")) is not int:
                        return False
        return True
    except Exception:
        raise ValueError("pool_retirement_drain_unqualified") from None


def retire_pool_workloads(*, request: PoolRetirementRequest, api: PoolRetirementAPI,
                          state_dir: Path, anchor_dir: Path) -> dict[str, Any]:
    try:
        state, anchor = state_dir.absolute(), anchor_dir.absolute()
        if (state != state.resolve() or anchor != anchor.resolve() or state == anchor
                or state in anchor.parents or anchor in state.parents):
            raise ValueError
        originals = retirement_documents(request)
        operation = str(request.migration.registration.spec.operation_id)
        with private_state._locked_state(anchor):
            identity = {"schema": "loom.nebius-pool-retirement.v1", "operation_id": operation, "state_dir": str(state),
                "closure_sha256": _closed(request.migration, state, anchor),
                "originals_sha256": digest({key: {"uid": _uid(row), "document": _stable(row)} for key, row in originals.items()})}
            marker, path = anchor / (operation + "-retirement.json"), state / "retirement.json"
            if marker.exists() or marker.is_symlink():
                if json.loads(private_state._private_read(marker)) != identity:
                    raise ValueError
                record = json.loads(private_state._private_read(path, limit=4 * 1024**2))
                if (set(record) != {*identity, "workloads"} or any(record[key] != value for key, value in identity.items())
                        or set(record["workloads"]) != set(originals)
                        or any(value not in {"prepared", "intent", "stopped"} for value in record["workloads"].values())):
                    raise ValueError
            else:
                if path.exists() or path.is_symlink():
                    raise ValueError
                api.verify_guards()
                if any(not _matches(api.read(key), row, _uid(row)) for key, row in originals.items()):
                    raise ValueError
                private_state._atomic_json(marker, identity)
                record = {**identity, "workloads": dict.fromkeys(originals, "prepared")}
                private_state._atomic_json(path, record)

            def save(key: str, phase: str) -> None:
                record["workloads"][key] = phase
                private_state._atomic_json(path, record)

            def pending(status: str) -> dict[str, Any]:
                return {"status": status, "operation_id": operation, "writer_migration_complete": False}

            for key, original in originals.items():
                actual = api.read(key)
                if record["workloads"][key] == "prepared":
                    if not _matches(actual, original, _uid(original)):
                        raise ValueError
                    save(key, "intent")
                    try:
                        accepted = api.stop(key, actual)
                    except Exception:
                        accepted = True  # Retain unknown intent; GET is not a retry.
                    if accepted is False:
                        save(key, "prepared")
                        return pending("pending_retirement")
                    if accepted is not True:
                        raise ValueError
                    actual = api.read(key)
                if not _matches(actual, stopped_document(request, key), _uid(original)):
                    raise ValueError
                if record["workloads"][key] != "stopped":
                    save(key, "stopped")
                if api.drained(key) is not True:
                    return pending("pending_drain")
            # Recheck earlier processes after stopping the final controller.
            for key, original in originals.items():
                if not _matches(api.read(key), stopped_document(request, key), _uid(original)):
                    raise ValueError
                if api.drained(key) is not True:
                    return pending("pending_drain")
            return pending("old_pool_workloads_retired")
    except Exception:
        raise ValueError("pool_retirement_unconfirmed_preserve_evidence") from None
