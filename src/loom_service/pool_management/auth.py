"""Dedicated hash-only machine authentication, without hidden commits or writes.

Resolution is not mutation authority. Admission must reauthorize inside its own
transaction, after acquiring the management-wide pool mutation lock. This
module then holds pool, participant, machine, credential and token read locks
until the caller commits/rolls back. No network operation belongs in that window.
The operation itself enforces closed/fenced intake and workload policy.
"""
from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Literal, TypeVar, cast
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.base import Base
from loom.db.nebius_pool_schema import (
    NebiusPoolBinding,
    NebiusPoolMachine,
    NebiusPoolMachineCredential,
    NebiusPoolParticipant,
)
from loom.db.schema import Token
from loom.nebius_pool_contract import PoolWorkloadKind

PoolMachineRole = Literal["participant", "observer", "gateway"]
_Row = TypeVar("_Row", bound=Base)
_CREDENTIAL = re.compile(r"[A-Za-z0-9._~+/-]{1,512}=*")


class PoolAuthenticationError(ValueError):
    def __init__(self) -> None:
        super().__init__("pool_machine_authority_unavailable")


@dataclass(frozen=True)
class PoolPrincipal:
    token_hash: bytes = field(repr=False)
    machine_id: UUID
    pool_id: UUID
    participant_id: UUID | None
    role: PoolMachineRole
    credential_epoch: int
    installation_id: UUID
    pool_epoch: int
    policy_revision: int
    pool_binding_sha256: str
    pool_mode: str
    environment_id: UUID | None
    incarnation: UUID | None
    participant_epoch: int | None
    participant_revision: int | None
    participant_binding_sha256: str | None
    participant_phase: str | None
    workload_scope: Literal["environment", "application_builder"]


async def _row(session: AsyncSession, model: type[_Row], identity: UUID | bytes, *, locked: bool) -> _Row | None:
    column = next(iter(model.__table__.primary_key))
    statement = select(model).where(column == identity).execution_options(populate_existing=True)
    if locked:
        statement = statement.with_for_update(read=True)
    return (await session.execute(statement)).scalar_one_or_none()


async def _resolve_hash(session: AsyncSession, token_hash: bytes, *, locked: bool) -> PoolPrincipal | None:
    with session.no_autoflush:
        # Re-reading must not erase pending ORM revocation/registration edits,
        # and authentication must not flush them on the caller's behalf.
        authority_rows = (Token, NebiusPoolBinding, NebiusPoolParticipant,
                          NebiusPoolMachine, NebiusPoolMachineCredential)
        if any(isinstance(row, authority_rows) for row in session.new | session.dirty | session.deleted):
            return None
        # Immutable scope pointers are read first solely to locate lock targets.
        credential = await _row(session, NebiusPoolMachineCredential, token_hash, locked=False)
        if credential is None:
            return None
        machine = await _row(session, NebiusPoolMachine, credential.machine_id, locked=False)
        if machine is None:
            return None
        pool = await _row(session, NebiusPoolBinding, machine.pool_id, locked=locked)
        participant = (await _row(session, NebiusPoolParticipant, machine.participant_id, locked=locked)
            if machine.participant_id is not None else None)
        if locked:
            machine = await _row(session, NebiusPoolMachine, credential.machine_id, locked=True)
            credential = await _row(session, NebiusPoolMachineCredential, token_hash, locked=True)
        token = await _row(session, Token, token_hash, locked=locked)
        now = datetime.now(UTC)
        if (pool is None or machine is None or credential is None or token is None
                or token.type != "pool_machine" or token.scopes != []
                or token.team_id is not None or token.created_by_user_id is not None
                or token.revoked_at is not None or token.expires_at is None
                or token.expires_at <= now or token.issued_at > now
                or machine.phase != "active" or machine.credential_epoch != credential.credential_epoch
                or machine.role not in {"participant", "observer", "gateway"}
                or machine.workload_scope not in {"environment", "application_builder"}
                or (machine.workload_scope == "application_builder" and machine.role != "participant")
                or machine.pool_id != pool.pool_id or credential.machine_id != machine.machine_id):
            return None
        if machine.role == "participant":
            if (participant is None or participant.participant_id != machine.participant_id
                    or participant.pool_id != pool.pool_id or participant.admission_epoch != pool.admission_epoch):
                return None
        elif machine.participant_id is not None:
            return None
        return PoolPrincipal(
            token_hash=token_hash, machine_id=machine.machine_id, pool_id=pool.pool_id,
            participant_id=machine.participant_id, role=cast(PoolMachineRole, machine.role),
            credential_epoch=machine.credential_epoch, installation_id=pool.installation_id,
            pool_epoch=pool.admission_epoch, policy_revision=pool.policy_revision,
            pool_binding_sha256=pool.binding_sha256, pool_mode=pool.mode,
            environment_id=participant.environment_id if participant else None,
            incarnation=participant.incarnation if participant else None,
            participant_epoch=participant.admission_epoch if participant else None,
            participant_revision=participant.binding_revision if participant else None,
            participant_binding_sha256=participant.binding_sha256 if participant else None,
            participant_phase=participant.phase if participant else None,
            workload_scope=cast(Literal["environment", "application_builder"], machine.workload_scope),
        )


async def resolve_pool_machine(session: AsyncSession, header_value: str | None) -> PoolPrincipal | None:
    """Resolve dedicated identity only; never invoke generic/admin/JWT auth."""
    if header_value is None or len(header_value) > 520:
        return None
    parts = header_value.split()
    if (len(parts) != 2 or parts[0].lower() != "bearer"
            or len(parts[1]) > 512 or _CREDENTIAL.fullmatch(parts[1]) is None):
        return None
    return await _resolve_hash(session, hashlib.sha256(parts[1].encode()).digest(), locked=False)


async def authorize_pool_machine(session: AsyncSession, principal: PoolPrincipal, *,
                                 role: PoolMachineRole, pool_id: UUID,
                                 participant_id: UUID | None = None,
                                 workload_kind: PoolWorkloadKind | None = None) -> PoolPrincipal:
    """Recheck current locked identity at the transaction's authority boundary.

    Caller must still qualify operation-specific mode, workload scope, immutable
    request epochs and priority. This function neither dispatches nor admits work.
    """
    if (principal.role, principal.pool_id, principal.participant_id) != (role, pool_id, participant_id):
        raise PoolAuthenticationError
    current = await _resolve_hash(session, principal.token_hash, locked=True)
    if current is None or current != principal:
        raise PoolAuthenticationError
    if workload_kind is not None and (current.role != "participant" or
            (workload_kind == "application_image_build") != (current.workload_scope == "application_builder")):
        raise PoolAuthenticationError
    return current
