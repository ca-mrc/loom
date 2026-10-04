"""Protected initial registration only; never opens global admission or RBAC."""
from __future__ import annotations

import asyncio
import json
import os
import stat
from datetime import datetime
from pathlib import Path
from typing import Any, Literal, TypeVar
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from loom.db.base import Base
from loom.db.nebius_pool_schema import (
    NebiusPoolBinding,
    NebiusPoolMachine,
    NebiusPoolMachineCredential,
    NebiusPoolParticipant,
)
from loom.db.schema import Token
from loom.db.schema_startup import assert_schema_at_head
from loom.nebius_pool_contract import PoolParticipantV1
from loom_service.pool_management.capacity import PoolCapacityPolicyV1, digest
from loom_service.pool_management.control import _clock
from loom_service.pool_management.locks import acquire_pool_mutation_lock
from loom_service.pool_management.profiles import PoolProfileCatalog, _object


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class PoolMachineInstallation(_Strict):
    machine_id: UUID
    role: Literal["participant", "observer", "gateway"]
    participant_id: UUID | None
    workload_scope: Literal["environment", "application_builder"] = Field(
        default="environment", exclude_if=lambda value: value == "environment")
    credential_epoch: int = Field(gt=0, strict=True)
    token_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    issued_at: datetime
    expires_at: datetime

    @model_validator(mode="after")
    def identity(self) -> PoolMachineInstallation:
        if (not self.machine_id.int or (self.role == "participant") != (self.participant_id is not None)
                or (self.workload_scope == "application_builder" and self.role != "participant")
                or (self.participant_id is not None and not self.participant_id.int)
                or self.issued_at.utcoffset() is None or self.expires_at.utcoffset() is None
                or self.issued_at >= self.expires_at):
            raise ValueError("invalid initial machine credential")
        return self


class PoolInstallation(_Strict):
    schema_version: Literal["loom.pool-installation.v1"]
    operation_id: UUID
    pool_id: UUID
    installation_id: UUID
    cluster_id: str = Field(min_length=1, max_length=253)
    node_group_id: str = Field(min_length=1, max_length=253)
    admission_epoch: int = Field(gt=0, strict=True)
    policy_revision: int = Field(gt=0, strict=True)
    node_selector: dict[str, str]
    admission: PoolCapacityPolicyV1
    quota_identities: dict[str, tuple[str, str, str, str, str]]
    participants: tuple[PoolParticipantV1, ...] = Field(min_length=1, max_length=128)
    machines: tuple[PoolMachineInstallation, ...] = Field(min_length=3, max_length=258)
    profiles: PoolProfileCatalog

    @model_validator(mode="after")
    def bindings(self) -> PoolInstallation:
        if (not all(value.int for value in (self.operation_id, self.pool_id, self.installation_id))
                or self.node_selector.get("nebius.com/node-group-id") != self.node_group_id
                or not {"nodes", "vcpu", "storage"} <= set(self.quota_identities) <= {"nodes", "vcpu", "memory", "storage"}
                or any(not value for parts in self.quota_identities.values() for value in parts)):
            raise ValueError("invalid physical pool binding")
        participants = {row.participant_id: row for row in self.participants}
        namespaces = [ns for row in self.participants for ns in (row.execution_namespace, row.build_namespace)]
        if (len(participants) != len(self.participants)
                or len({row.environment_id for row in self.participants}) != len(self.participants)
                or len({ns.name for ns in namespaces}) != len(namespaces)
                or len({ns.uid for ns in namespaces}) != len(namespaces)):
            raise ValueError("duplicate pool participant or namespace")
        profiles = self.profiles.profiles()
        used_execution, used_build, used_application, application_participants = set(), set(), set(), set()
        for row in self.participants:
            if (row.installation_id, row.pool_id, row.admission_epoch) != (
                    self.installation_id, self.pool_id, self.admission_epoch):
                raise ValueError("participant differs from installation")
            for target in row.targets:
                if "application_image_build" in target.workload_kinds and (
                        row.environment_class != "development" or target.workload_kinds != ("application_image_build",)):
                    raise ValueError("application build requires a dedicated development target")
                for kind in target.workload_kinds:
                    if kind in {"trial", "verifier"}:
                        execution = profiles.execution.get(target.profile_id)
                        if execution is None:
                            raise ValueError("missing execution profile")
                        runtime, namespace = execution.runtime, row.execution_namespace.name
                        used_execution.add(target.profile_id)
                    elif kind == "task_image_build":
                        build = profiles.task_images.get(target.profile_id)
                        if build is None:
                            raise ValueError("missing build profile")
                        runtime, namespace = build.target, row.build_namespace.name
                        used_build.add(target.profile_id)
                    else:
                        application = profiles.application_images.get(target.profile_id)
                        if application is None:
                            raise ValueError("missing application build profile")
                        runtime, namespace = application.target, row.build_namespace.name
                        used_application.add(target.profile_id)
                        application_participants.add(row.participant_id)
                    if ((runtime.target_id, runtime.namespace) != (target.target_id, namespace)
                            or any((runtime.node_selector or {}).get(key) != value for key, value in self.node_selector.items())):
                        raise ValueError("profile differs from protected target")
        if (used_execution != set(profiles.execution) or used_build != set(profiles.task_images)
                or used_application != set(profiles.application_images)):
            raise ValueError("unbound renderer profile")
        owners = [row.participant_id for row in self.machines if row.role == "participant" and row.workload_scope == "environment"]
        builders = [row.participant_id for row in self.machines if row.workload_scope == "application_builder"]
        if (len({row.machine_id for row in self.machines}) != len(self.machines)
                or len({row.token_sha256 for row in self.machines}) != len(self.machines)
                or len(owners) != len(participants) or set(owners) != set(participants)
                or len(builders) != len(application_participants) or set(builders) != application_participants
                or sum(row.role == "observer" for row in self.machines) != 1
                or sum(row.role == "gateway" for row in self.machines) != 1):
            raise ValueError("incomplete dedicated machine identities")
        return self

    @classmethod
    def load(cls, path: Path) -> PoolInstallation:
        try:
            descriptor = os.open(path, os.O_RDONLY | os.O_NONBLOCK)
            try:
                if not stat.S_ISREG(os.fstat(descriptor).st_mode):
                    raise ValueError
                with os.fdopen(descriptor, "rb", closefd=False) as stream:
                    raw = stream.read(2 * 1024 * 1024 + 1)
            finally:
                os.close(descriptor)
            if len(raw) > 2 * 1024 * 1024:
                raise ValueError
            return cls.model_validate(json.loads(raw, object_pairs_hook=_object))
        except (OSError, ValueError, TypeError):
            raise ValueError("invalid_pool_installation") from None


_Row = TypeVar("_Row", bound=Base)


async def _retain(session: AsyncSession, model: type[_Row], identity: UUID | bytes, values: dict[str, Any]) -> None:
    await session.execute(insert(model).values(**values).on_conflict_do_nothing())
    row = await session.get(model, identity, populate_existing=True, with_for_update=True)
    if row is None or any(getattr(row, name) != value for name, value in values.items()):
        raise ValueError("pool_installation_conflicts_with_retained_authority")


async def register_installation(session: AsyncSession, spec: PoolInstallation) -> dict[str, Any]:
    """Caller owns the transaction; exact replay cannot rotate or reopen anything."""
    spec = PoolInstallation.model_validate(spec.model_dump())
    if session.new or session.dirty or session.deleted:
        raise ValueError("initial pool registration requires a clean transaction")
    await acquire_pool_mutation_lock(session)
    now = await _clock(session)
    if any(not row.issued_at <= now < row.expires_at for row in spec.machines):
        raise ValueError("pool_installation_credentials_not_current")
    checksum = digest(spec.model_dump(mode="json"))
    binding = {"node_selector": spec.node_selector, "admission": spec.admission.model_dump(),
        "quota_identities": {key: list(value) for key, value in spec.quota_identities.items()},
        "installation_sha256": checksum, "profile_catalog_sha256": digest(spec.profiles.model_dump(mode="json"))}
    await _retain(session, NebiusPoolBinding, spec.pool_id, dict(pool_id=spec.pool_id, installation_id=spec.installation_id,
        cluster_id=spec.cluster_id, node_group_id=spec.node_group_id, policy_revision=spec.policy_revision,
        admission_epoch=spec.admission_epoch, mode="closed", binding_json=binding, binding_sha256=digest(binding)))
    for participant in spec.participants:
        payload = participant.model_dump(mode="json")
        await _retain(session, NebiusPoolParticipant, participant.participant_id, dict(participant_id=participant.participant_id,
            pool_id=spec.pool_id, environment_id=participant.environment_id, incarnation=participant.incarnation,
            binding_revision=participant.binding_revision, admission_epoch=spec.admission_epoch, phase="active",
            binding_json=payload, binding_sha256=digest(payload)))
    for machine in spec.machines:
        hashed = bytes.fromhex(machine.token_sha256)
        await _retain(session, NebiusPoolMachine, machine.machine_id, dict(machine_id=machine.machine_id, pool_id=spec.pool_id,
            participant_id=machine.participant_id, role=machine.role, workload_scope=machine.workload_scope,
            credential_epoch=machine.credential_epoch, phase="active"))
        await _retain(session, Token, hashed, dict(token_hash=hashed, type="pool_machine", scopes=[], team_id=None,
            created_by_user_id=None, issued_at=machine.issued_at, expires_at=machine.expires_at, revoked_at=None))
        await _retain(session, NebiusPoolMachineCredential, hashed, dict(token_hash=hashed,
            machine_id=machine.machine_id, credential_epoch=machine.credential_epoch))
    for model, column, expected in ((NebiusPoolParticipant, NebiusPoolParticipant.participant_id,
            {row.participant_id for row in spec.participants}),
            (NebiusPoolMachine, NebiusPoolMachine.machine_id, {row.machine_id for row in spec.machines})):
        if set(await session.scalars(select(column).where(model.pool_id == spec.pool_id))) != expected:
            raise ValueError("pool_installation_contains_foreign_registrations")
    return {"schema_version": "loom.pool-installation-receipt.v1", "operation_id": str(spec.operation_id),
        "pool_id": str(spec.pool_id), "installation_sha256": checksum, "mode": "closed",
        "participants": len(spec.participants), "machines": len(spec.machines)}


async def run_installation(path: Path, *, db_url: str) -> dict[str, Any]:
    spec = PoolInstallation.load(path)
    engine = create_async_engine(db_url, pool_pre_ping=True)
    try:
        await assert_schema_at_head(engine, db_url_env_var="LOOM_POOL_INSTALLATION_DB_URL")
        async with async_sessionmaker(engine, expire_on_commit=False).begin() as session:
            receipt = await register_installation(session, spec)
        return receipt  # Commit succeeded before reporting a receipt.
    finally:
        await engine.dispose()


def main() -> int:
    try:
        result = asyncio.run(run_installation(Path(os.environ["LOOM_POOL_INSTALLATION_FILE"]),
            db_url=os.environ["LOOM_POOL_INSTALLATION_DB_URL"]))
        print(json.dumps(result, sort_keys=True))
        return 0
    except Exception:
        print(json.dumps({"status": "pool_installation_unavailable"}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
