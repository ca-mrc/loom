"""Transaction-local administration receipts; protected caller owns authorization."""
from __future__ import annotations

from typing import Any
from uuid import UUID

from sqlalchemy import insert
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.nebius_pool_lifecycle_schema import NebiusPoolLifecycleOperation
from loom.db.nebius_pool_schema import NebiusPoolMachine, NebiusPoolMachineCredential
from loom.db.schema import Token
from loom_service.pool_management.capacity import digest
from loom_service.pool_management.control import _clock
from loom_service.pool_management.installation import PoolMachineInstallation, _retain
from loom_service.pool_management.locks import acquire_pool_mutation_lock


async def replay(session: AsyncSession, operation_id: UUID, document: dict[str, Any]) -> dict[str, Any] | None:
    if session.new or session.dirty or session.deleted:
        raise ValueError('pool_lifecycle_requires_clean_transaction')
    await acquire_pool_mutation_lock(session)
    row = await session.get(NebiusPoolLifecycleOperation, operation_id, populate_existing=True)
    if row is None:
        return None
    if row.request_json != document or row.request_sha256 != digest(document):
        raise ValueError('pool_lifecycle_operation_conflict')
    return row.receipt_json


async def retain_operation(session: AsyncSession, document: dict[str, Any], receipt: dict[str, Any], *,
                           pool_id: UUID, participant_id: UUID | None) -> None:
    await session.execute(insert(NebiusPoolLifecycleOperation).values(
        operation_id=UUID(document['operation_id']), pool_id=pool_id, participant_id=participant_id,
        request_json=document, request_sha256=digest(document), receipt_json=receipt))


async def install_machines(session: AsyncSession, pool_id: UUID, machines: tuple[PoolMachineInstallation, ...]) -> None:
    now = await _clock(session)
    if any(not row.issued_at <= now < row.expires_at for row in machines):
        raise ValueError('pool_installation_credentials_not_current')
    for machine in machines:
        hashed = bytes.fromhex(machine.token_sha256)
        await _retain(session, NebiusPoolMachine, machine.machine_id, dict(machine_id=machine.machine_id, pool_id=pool_id,
            participant_id=machine.participant_id, role=machine.role, workload_scope=machine.workload_scope,
            credential_epoch=machine.credential_epoch, phase='active'))
        await _retain(session, Token, hashed, dict(token_hash=hashed, type='pool_machine', scopes=[], team_id=None,
            created_by_user_id=None, issued_at=machine.issued_at, expires_at=machine.expires_at, revoked_at=None))
        await _retain(session, NebiusPoolMachineCredential, hashed, dict(token_hash=hashed,
            machine_id=machine.machine_id, credential_epoch=machine.credential_epoch))
