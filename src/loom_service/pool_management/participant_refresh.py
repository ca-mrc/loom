"""Protected single-participant release refresh; caller owns authorization/commit.

No public route invokes this transaction. The protected rollout must retain its
intent, local idle guard, source qualification and readiness barriers separately.
These database states establish neither Kubernetes readiness nor guard ownership.
"""
from __future__ import annotations

import asyncio
import json
import os
import stat
import sys
from datetime import timedelta
from pathlib import Path
from typing import Any, Literal, cast
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator
from sqlalchemy import select, text, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from loom.db.nebius_pool_schema import (
    NebiusPoolBinding,
    NebiusPoolMachine,
    NebiusPoolMachineCredential,
    NebiusPoolParticipant,
    NebiusPoolRequest,
)
from loom.db.schema import Token
from loom.db.schema_startup import assert_schema_at_head
from loom_service.pool_management.capacity import CHARGED_PHASES, digest, read_connected_capacity
from loom_service.pool_management.control import _clock
from loom_service.pool_management.installation import PoolInstallation
from loom_service.pool_management.locks import acquire_pool_mutation_lock
from loom_service.pool_management.profiles import _object

RefreshAction = Literal["observe", "capacity", "fence", "stage", "open"]
_ACTIONS = {"observe", "capacity", "fence", "stage", "open"}


class ParticipantCapacityReceipt(BaseModel):
    """Bounded historical readiness proof; the parent must check expiry on use."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    checked_at: AwareDatetime
    valid_until: AwareDatetime
    selected_observation_id: UUID
    selected_observed_at: AwareDatetime
    connected_pools: int = Field(ge=1, le=1000)
    observations_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def bounded_window(self) -> ParticipantCapacityReceipt:
        if (not self.selected_observation_id.int or self.valid_until <= self.checked_at
                or self.selected_observed_at > self.checked_at + timedelta(seconds=60)
                or self.valid_until > self.selected_observed_at + timedelta(seconds=900)):
            raise ValueError("participant_capacity_receipt_unqualified")
        return self


class PoolParticipantRefresh(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["loom.pool-participant-refresh.v1"]
    participant_id: UUID
    before: PoolInstallation
    after: PoolInstallation

    @classmethod
    def load(cls, path: Path) -> PoolParticipantRefresh:
        """Bounded regular input, including Kubernetes projected ConfigMap links."""
        try:
            descriptor = os.open(path, os.O_RDONLY | os.O_NONBLOCK)
            try:
                if not stat.S_ISREG(os.fstat(descriptor).st_mode):
                    raise ValueError
                with os.fdopen(descriptor, "rb", closefd=False) as stream:
                    raw = stream.read(4 * 1024**2 + 1)
            finally:
                os.close(descriptor)
            if len(raw) > 4 * 1024**2:
                raise ValueError
            return cls.model_validate(json.loads(raw, object_pairs_hook=_object))
        except (OSError, ValueError, TypeError):
            raise ValueError("invalid_pool_participant_refresh") from None

    @model_validator(mode="after")
    def release_only(self) -> PoolParticipantRefresh:
        before, after = self.before, self.after
        mutable = {"operation_id", "policy_revision", "participants", "profiles"}
        if (before.model_dump(exclude=mutable) != after.model_dump(exclude=mutable)
                or before.operation_id == after.operation_id
                or after.policy_revision != before.policy_revision + 1
                or len(before.participants) != len(after.participants)):
            raise ValueError("participant_refresh_changes_pool_authority")
        selected = next((row for row in before.participants if row.participant_id == self.participant_id), None)
        if selected is None:
            raise ValueError("participant_refresh_missing_participant")
        replacements: dict[UUID, UUID] = {}
        old_ids = ({row.profile_id for row in before.profiles.execution}
            | {row.profile_id for row in before.profiles.task_images}
            | {row.profile_id for row in before.profiles.application_images})
        for previous, following in zip(before.participants, after.participants, strict=True):
            if previous.participant_id != self.participant_id:
                if previous != following:
                    raise ValueError("participant_refresh_changes_neighbor")
                continue
            if (previous.model_dump(exclude={"binding_revision", "targets"})
                    != following.model_dump(exclude={"binding_revision", "targets"})
                    or following.binding_revision != previous.binding_revision + 1
                    or len(previous.targets) != len(following.targets)):
                raise ValueError("participant_refresh_changes_identity")
            for old, new in zip(previous.targets, following.targets, strict=True):
                if (old.model_dump(exclude={"profile_id"}) != new.model_dump(exclude={"profile_id"})
                        or "application_image_build" in old.workload_kinds
                        or new.profile_id in old_ids or new.profile_id in replacements.values()
                        or old.profile_id in replacements):
                    raise ValueError("participant_refresh_changes_target")
                replacements[old.profile_id] = new.profile_id
        if (before.profiles.image_admission_keyring != after.profiles.image_admission_keyring
                or before.profiles.application_images != after.profiles.application_images
                or len(before.profiles.execution) != len(after.profiles.execution)
                or len(before.profiles.task_images) != len(after.profiles.task_images)):
            raise ValueError("participant_refresh_changes_catalog_scope")
        candidates = set()
        for old_execution, new_execution in zip(before.profiles.execution, after.profiles.execution, strict=True):
            if old_execution.profile_id not in replacements:
                if old_execution != new_execution:
                    raise ValueError("participant_refresh_changes_neighbor_profile")
                continue
            release_fields = {"profile_id", "candidate_sha", "runtime_image_ref", "runtime_binary_sha256"}
            if (new_execution.profile_id != replacements[old_execution.profile_id]
                    or old_execution.model_dump(exclude=release_fields) != new_execution.model_dump(exclude=release_fields)
                    or old_execution.candidate_sha == new_execution.candidate_sha
                    or old_execution.runtime_image_ref.rsplit("@", 1)[0] != new_execution.runtime_image_ref.rsplit("@", 1)[0]):
                raise ValueError("participant_refresh_changes_execution_contract")
            candidates.add(new_execution.candidate_sha)
        if len(candidates) != 1:
            raise ValueError("participant_refresh_requires_one_execution_candidate")
        for old_build, new_build in zip(before.profiles.task_images, after.profiles.task_images, strict=True):
            if old_build.profile_id not in replacements:
                if old_build != new_build:
                    raise ValueError("participant_refresh_changes_neighbor_profile")
                continue
            if (new_build.profile_id != replacements[old_build.profile_id]
                    or old_build.model_dump(exclude={"profile_id", "settings"})
                        != new_build.model_dump(exclude={"profile_id", "settings"})
                    or old_build.settings.model_dump(exclude={"service_image"})
                        != new_build.settings.model_dump(exclude={"service_image"})
                    or old_build.settings.service_image.rsplit("@", 1)[0]
                        != new_build.settings.service_image.rsplit("@", 1)[0]):
                raise ValueError("participant_refresh_changes_build_contract")
        return self


def _binding(spec: PoolInstallation) -> dict[str, Any]:
    return {"node_selector": spec.node_selector, "admission": spec.admission.model_dump(),
        "quota_identities": {key: list(value) for key, value in spec.quota_identities.items()},
        "installation_sha256": digest(spec.model_dump(mode="json")),
        "profile_catalog_sha256": digest(spec.profiles.model_dump(mode="json"))}


async def _authority(session: AsyncSession, spec: PoolInstallation) -> None:
    """Exact, still-current machine authority; no credential changes in refresh."""
    machines = (await session.scalars(select(NebiusPoolMachine).where(NebiusPoolMachine.pool_id == spec.pool_id)
        .order_by(NebiusPoolMachine.machine_id).with_for_update(read=True)
        .execution_options(populate_existing=True))).all()
    expected = {row.machine_id: row for row in spec.machines}
    if {row.machine_id for row in machines} != set(expected):
        raise ValueError("participant_refresh_machine_drift")
    for row in machines:
        machine = expected[row.machine_id]
        if (row.participant_id, row.role, row.workload_scope, row.credential_epoch, row.phase) != (
                machine.participant_id, machine.role, machine.workload_scope, machine.credential_epoch, "active"):
            raise ValueError("participant_refresh_machine_drift")
    credentials = (await session.scalars(select(NebiusPoolMachineCredential)
        .where(NebiusPoolMachineCredential.machine_id.in_(expected))
        .order_by(NebiusPoolMachineCredential.token_hash).with_for_update(read=True)
        .execution_options(populate_existing=True))).all()
    if {(row.token_hash, row.machine_id, row.credential_epoch) for row in credentials} != {
            (bytes.fromhex(row.token_sha256), row.machine_id, row.credential_epoch) for row in spec.machines}:
        raise ValueError("participant_refresh_credential_drift")
    tokens = (await session.scalars(select(Token).where(Token.token_hash.in_([row.token_hash for row in credentials]))
        .order_by(Token.token_hash).with_for_update(read=True).execution_options(populate_existing=True))).all()
    expected_tokens = {bytes.fromhex(row.token_sha256): row for row in spec.machines}
    if {row.token_hash for row in tokens} != set(expected_tokens):
        raise ValueError("participant_refresh_credential_drift")
    now = await _clock(session)
    for token in tokens:
        owner = expected_tokens[token.token_hash]
        if (token.type != "pool_machine" or token.scopes != [] or token.team_id is not None
                or token.created_by_user_id is not None or token.revoked_at is not None
                or token.issued_at != owner.issued_at or token.expires_at != owner.expires_at
                or not owner.issued_at <= now < owner.expires_at):
            raise ValueError("participant_refresh_credential_drift")


async def refresh_participant(session: AsyncSession, request: PoolParticipantRefresh, *,
        action: RefreshAction) -> dict[str, Any]:
    """Use the existing mutation lock and monotonic revisions, never a new epoch.

    Exact completed readback does not require idleness or refence current work.
    Outstanding waiting/charged requests are never cancelled to make rollout fit.
    """
    request = PoolParticipantRefresh.model_validate(request.model_dump())
    if action not in _ACTIONS or session.new or session.dirty or session.deleted:
        raise ValueError("participant_refresh_requires_clean_transaction")
    await acquire_pool_mutation_lock(session)
    pool = await session.get(NebiusPoolBinding, request.before.pool_id, populate_existing=True, with_for_update=True)
    if pool is None or pool.mode != "global":
        raise ValueError("participant_refresh_requires_global_pool")
    matches = [spec for spec in (request.before, request.after)
        if (pool.installation_id, pool.cluster_id, pool.node_group_id, pool.admission_epoch, pool.policy_revision,
            pool.binding_json, pool.binding_sha256) == (spec.installation_id, spec.cluster_id, spec.node_group_id,
            spec.admission_epoch, spec.policy_revision, _binding(spec), digest(_binding(spec)))]
    if len(matches) != 1:
        raise ValueError("participant_refresh_pool_drift")
    current, = matches
    participants = (await session.scalars(select(NebiusPoolParticipant).where(NebiusPoolParticipant.pool_id == pool.pool_id)
        .order_by(NebiusPoolParticipant.participant_id).with_for_update()
        .execution_options(populate_existing=True))).all()
    expected = {row.participant_id: row for row in current.participants}
    if {row.participant_id for row in participants} != set(expected):
        raise ValueError("participant_refresh_participant_drift")
    for participant in participants:
        spec_participant = expected[participant.participant_id]
        payload = spec_participant.model_dump(mode="json")
        if ((participant.environment_id, participant.incarnation, participant.admission_epoch,
                participant.binding_revision, participant.binding_json, participant.binding_sha256) != (
                spec_participant.environment_id, spec_participant.incarnation, spec_participant.admission_epoch,
                spec_participant.binding_revision, payload, digest(payload))
                or participant.phase not in {"active", "fenced"}
                or (participant.participant_id != request.participant_id and participant.phase != "active")):
            raise ValueError("participant_refresh_participant_drift")
    await _authority(session, current)
    selected, = (row for row in participants if row.participant_id == request.participant_id)
    status = ("ready" if selected.phase == "active" else "fenced") if current is request.before else (
        "complete" if selected.phase == "active" else "staged")
    if action not in {"observe", "capacity"}:
        allowed = {"fence": {"ready", "fenced"}, "stage": {"fenced", "staged"}, "open": {"staged", "complete"}}
        if status not in allowed[action]:
            raise ValueError("participant_refresh_out_of_order")
        if status != "complete":
            outstanding = await session.scalar(select(NebiusPoolRequest.request_id).where(
                NebiusPoolRequest.participant_id == request.participant_id,
                NebiusPoolRequest.phase.in_(("waiting", *CHARGED_PHASES))).limit(1))
            if outstanding is not None:
                status = "pending_work"
            elif action == "stage" and status == "fenced":
                binding = _binding(request.after)
                after_participant = next(row for row in request.after.participants if row.participant_id == request.participant_id)
                payload = after_participant.model_dump(mode="json")
                await session.execute(update(NebiusPoolBinding).where(NebiusPoolBinding.pool_id == pool.pool_id).values(
                    policy_revision=request.after.policy_revision, binding_json=binding, binding_sha256=digest(binding)))
                await session.execute(update(NebiusPoolParticipant).where(NebiusPoolParticipant.participant_id == request.participant_id)
                    .values(binding_revision=after_participant.binding_revision, binding_json=payload, binding_sha256=digest(payload)))
                status = "staged"
            elif (action == "fence" and status == "ready") or (action == "open" and status == "staged"):
                await session.execute(update(NebiusPoolParticipant).where(NebiusPoolParticipant.participant_id == request.participant_id)
                    .values(phase="fenced" if action == "fence" else "active"))
                status = "fenced" if action == "fence" else "complete"
    result: dict[str, Any] = {"schema_version": "loom.pool-participant-refresh-receipt.v1", "operation_id": str(request.after.operation_id),
        "participant_id": str(request.participant_id), "pool_id": str(pool.pool_id), "status": status,
        "before_sha256": digest(request.before.model_dump(mode="json")),
        "after_sha256": digest(request.after.model_dump(mode="json"))}
    if action == "capacity":
        if status != "complete":
            raise ValueError("participant_refresh_capacity_requires_complete")
        now = await _clock(session)
        capacities = await read_connected_capacity(session, pool.pool_id, now)
        selected_capacity = capacities[pool.pool_id]
        proof = ParticipantCapacityReceipt(checked_at=now,
            valid_until=min(row.observed_at + timedelta(seconds=row.policy.observation_max_age_seconds)
                for row in capacities.values()),
            selected_observation_id=selected_capacity.observation_id, selected_observed_at=selected_capacity.observed_at,
            connected_pools=len(capacities), observations_sha256=digest([
                {'pool_id': str(key), 'observation_id': str(row.observation_id), 'observed_at': row.observed_at.isoformat()}
                for key, row in sorted(capacities.items())]))
        result['capacity'] = proof.model_dump(mode='json')
    return result


async def run_refresh(path: Path, *, db_url: str, action: RefreshAction) -> dict[str, Any]:
    """Fixed protected Job entry; report only after its one transaction commits.

    The operator's retained parent supplies guard/phase authority. No write retry,
    schema change or initial-registration fallback belongs in this command.
    """
    if action not in _ACTIONS:
        raise ValueError("invalid_pool_participant_refresh_action")
    request = PoolParticipantRefresh.load(path)
    engine = create_async_engine(db_url, pool_pre_ping=True)
    try:
        await assert_schema_at_head(engine, db_url_env_var="LOOM_POOL_PARTICIPANT_REFRESH_DB_URL")
        async with async_sessionmaker(engine, expire_on_commit=False).begin() as session:
            await session.execute(text("SET LOCAL statement_timeout='10s'"))
            await session.execute(text("SET LOCAL lock_timeout='2s'"))
            result = await refresh_participant(session, request, action=action)
        return result
    finally:
        await engine.dispose()


def main() -> int:
    try:
        if len(sys.argv) != 2 or sys.argv[1] not in _ACTIONS:
            raise ValueError
        result = asyncio.run(run_refresh(Path(os.environ["LOOM_POOL_PARTICIPANT_REFRESH_FILE"]),
            db_url=os.environ["LOOM_POOL_PARTICIPANT_REFRESH_DB_URL"], action=cast(RefreshAction, sys.argv[1])))
        print(json.dumps(result, sort_keys=True))
        return 0
    except Exception:
        print(json.dumps({"status": "pool_participant_refresh_unavailable"}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
