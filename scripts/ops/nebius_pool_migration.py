"""Protected initial migration barriers; closed registration is not activation.

The connected caller qualifies publication, predecessor and the complete installed
participant/writer inventory, including each original controller.
Reuse each environment's durable idle rollout guard. This stage never releases a
guard, stops a controller, grants a write role or opens global admission.
"""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Protocol
from uuid import UUID

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_ingress_stage import _snapshot, _uid
from scripts.ops.nebius_pool_projection import pure_projection
from scripts.ops.nebius_pool_registration import (
    PoolRegistrationRequest,
    registration_documents,
    validate_registration_proof,
)

from loom.nebius_platform_render import digest


class PoolMigrationError(RuntimeError):
    def __init__(self, stage: str):
        super().__init__("pool migration incomplete; preserve guards and recovery evidence")
        self.stage = stage


@dataclass(frozen=True, repr=False)
class PoolGuardDatabase:
    statefulset: dict[str, Any]
    service: dict[str, Any]
    credential_uid: UUID
    credential_resource_version: str
    actuator_credential_uid: UUID | None = None
    actuator_credential_resource_version: str | None = None


@dataclass(frozen=True, repr=False)
class PoolGuardTarget:
    participant_id: UUID
    namespace: str
    namespace_uid: UUID
    controller: dict[str, Any]
    database: PoolGuardDatabase | None = None


@dataclass(frozen=True, repr=False)
class PoolMigrationRequest:
    registration: PoolRegistrationRequest
    guards: tuple[PoolGuardTarget, ...]


class PoolMigrationAPI(Protocol):
    def preflight(self, request: PoolMigrationRequest) -> None:
        """Qualify publication, predecessor and the complete installed writer set."""
        ...

    def guard(self, target: PoolGuardTarget, action: str) -> dict[str, Any]:
        """Only fixed observe/acquire commands, bound to this operation/candidate."""
        ...

    def register(self, state_dir: Path) -> dict[str, Any] | None:
        """Stage the fixed Job and return its qualified runtime proof, or pending."""
        ...


@pure_projection
def migration_contract(request: PoolMigrationRequest) -> dict[str, Any]:
    registration_documents(request.registration)
    participants = request.registration.spec.participants
    # Environment classes describe policy, not the number of installed databases.
    # Registration validates the participant set; every member must be guarded.
    if (len(request.guards) != len(participants)
            or {row.participant_id for row in request.guards} != {row.participant_id for row in participants}
            or len({row.namespace for row in request.guards}) != len(request.guards)
            or len({row.namespace_uid for row in request.guards}) != len(request.guards)):
        raise ValueError("pool migration participants differ")
    guards = []
    for target in request.guards:
        controller = target.controller
        if (not target.namespace_uid.int or not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", target.namespace)
                or target.namespace == request.registration.binding.namespace
                or controller.get("apiVersion") != "apps/v1" or controller.get("kind") != "Deployment"
                or controller["metadata"].get("namespace") != target.namespace
                or controller["metadata"].get("name") != "loom-control-plane"
                or type(controller["spec"].get("replicas")) is not int or controller["spec"]["replicas"] != 1
                or controller["spec"]["selector"] != {"matchLabels": {"app": "loom-control-plane"}}):
            raise ValueError("pool migration controller differs")
        guards.append({"participant_id": str(target.participant_id), "namespace": target.namespace,
            "namespace_uid": str(target.namespace_uid), "controller_uid": _uid(controller), "controller": _snapshot(controller)})
        if target.database is not None:
            database = target.database
            if (not database.credential_uid.int or not isinstance(database.credential_resource_version, str)
                    or not database.credential_resource_version):
                raise ValueError("pool migration database credential identity differs")
            binding: dict[str, Any] = {"credential_uid": str(database.credential_uid),
                "credential_resource_version": database.credential_resource_version}
            if database.actuator_credential_uid is not None or database.actuator_credential_resource_version is not None:
                if (database.actuator_credential_uid is None or not database.actuator_credential_uid.int
                        or not isinstance(database.actuator_credential_resource_version, str)
                        or not 0 < len(database.actuator_credential_resource_version) <= 128):
                    raise ValueError("pool migration actuator credential identity differs")
                binding["actuator_credential"] = {"uid": str(database.actuator_credential_uid),
                    "resource_version": database.actuator_credential_resource_version}
            for kind, version, document in (("StatefulSet", "apps/v1", database.statefulset), ("Service", "v1", database.service)):
                if (document.get("apiVersion") != version or document.get("kind") != kind
                        or document["metadata"].get("name") != "loom-postgres"
                        or document["metadata"].get("namespace") != target.namespace):
                    raise ValueError("pool migration database differs")
                binding[kind] = {"uid": _uid(document), "document": _snapshot(document)}
            guards[-1]["database"] = binding
    return {"installation": request.registration.spec.model_dump(mode="json"),
        "binding": asdict(request.registration.binding), "candidate": request.registration.candidate, "guards": guards}


def _hash(path: Path) -> str:
    return hashlib.sha256(private_state._private_read(path, limit=4 * 1024**2)).hexdigest()


def _proof(request: PoolMigrationRequest, state: Path, proof: Any) -> None:
    validate_registration_proof(request.registration, state, proof)


def close_and_register_pool(*, request: PoolMigrationRequest, api: PoolMigrationAPI,
                            state_dir: Path, anchor_dir: Path) -> dict[str, Any]:
    """Resume exact guards and registration. Never infer a safe writer switchover."""
    stage = "recovery"
    try:
        state, anchor = state_dir.absolute(), anchor_dir.absolute()
        if (state != state.resolve() or anchor != anchor.resolve() or state == anchor
                or state in anchor.parents or anchor in state.parents):
            raise ValueError
        contract = migration_contract(request)
        operation = str(request.registration.spec.operation_id)
        identity = {"schema": "loom.nebius-pool-migration.v1", "operation_id": operation,
            "state_dir": str(state), "contract_sha256": digest(contract)}
        keys = {str(row.participant_id) for row in request.guards}
        with private_state._locked_state(anchor):
            marker, path = anchor / (operation + ".json"), state / "migration.json"
            if marker.exists() or marker.is_symlink():
                if json.loads(private_state._private_read(marker)) != identity:
                    raise ValueError
                record = json.loads(private_state._private_read(path, limit=4 * 1024**2))
                if (set(record) != {*identity, "guards", "registration"}
                        or any(record[key] != value for key, value in identity.items())
                        or set(record["guards"]) != keys
                        or any(value not in {"prepared", "acquire_intent", "held"} for value in record["guards"].values())):
                    raise ValueError
                item = record["registration"]
                if item is not None and (not isinstance(item, dict) or set(item) != {"stage_sha256", "proof"}
                        or not all(value == "held" for value in record["guards"].values())
                        or not (state / "registration/stage.json").is_file()
                        or (item["stage_sha256"] is not None and _hash(state / "registration/stage.json") != item["stage_sha256"])
                        or (item["proof"] is not None and item["stage_sha256"] is None)):
                    raise ValueError
            else:
                if state.exists() or state.is_symlink():
                    raise ValueError
                # Independent intent precedes the parent journal. Losing either
                # is recovery, never permission to create a second operation.
                private_state._atomic_json(marker, identity)
                private_state._private_directory(state)
                record = {**identity, "guards": dict.fromkeys(keys, "prepared"), "registration": None}
                private_state._atomic_json(path, record)
            stage = "preflight"
            api.preflight(request)
            stage = "intake"
            for target in request.guards:
                key = str(target.participant_id)
                current = api.guard(target, "observe").get("status")
                saved = record["guards"][key]
                if saved == "prepared":
                    if current != "open":
                        raise ValueError
                    record["guards"][key] = "acquire_intent"
                    private_state._atomic_json(path, record)
                    try:
                        outcome = api.guard(target, "acquire").get("status")
                    except Exception:
                        outcome = None
                    if outcome == "skipped_busy":
                        record["guards"][key] = "prepared"
                        private_state._atomic_json(path, record)
                        return {"status": "pending_idle", "operation_id": operation, "writer_migration_complete": False}
                    if outcome not in {None, "acquired"}:
                        raise ValueError
                    current = api.guard(target, "observe").get("status")
                if current != "held":
                    raise ValueError
                if saved != "held":
                    record["guards"][key] = "held"
                    private_state._atomic_json(path, record)
            stage = "registration"
            # Recheck every earlier participant after closing the last one.
            if any(api.guard(target, "observe").get("status") != "held" for target in request.guards):
                raise ValueError
            if record["registration"] is None:
                record["registration"] = {"stage_sha256": None, "proof": None}
                private_state._atomic_json(path, record)
            proof = api.register(state / "registration")
            if proof is None:
                return {"status": "pending_registration", "operation_id": operation, "writer_migration_complete": False}
            _proof(request, state / "registration", proof)
            item = {"stage_sha256": _hash(state / "registration/stage.json"), "proof": proof}
            if record["registration"]["proof"] is not None and record["registration"] != item:
                raise ValueError
            if any(api.guard(target, "observe").get("status") != "held" for target in request.guards):
                raise ValueError
            record["registration"] = item
            private_state._atomic_json(path, record)
            return {"status": "pool_registered_closed", "operation_id": operation,
                "pool_id": str(request.registration.spec.pool_id), "writer_migration_complete": False}
    except Exception:
        raise PoolMigrationError(stage) from None
