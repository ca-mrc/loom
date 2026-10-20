"""Transactional qualification of protected runtime-readback identities."""
from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.nebius_pool_schema import (
    NebiusPoolBinding,
    NebiusPoolMachine,
    NebiusPoolMachineCredential,
)
from loom.db.schema import Token
from loom_service.pool_management.control import _clock
from loom_service.pool_management.lifecycle_contracts import PoolRuntimeReadinessV2
from loom_service.pool_management.observation_binding import physical_binding_digest


async def qualify_runtime_readiness(session: AsyncSession, pool: NebiusPoolBinding, proof: PoolRuntimeReadinessV2) -> None:
    now = await _clock(session)
    if (not proof.observed_at <= now < proof.valid_until or proof.pool_authority_sha256 != physical_binding_digest(pool)
            or not {'physical-v1', 'registry-v1', 'scoped-lifecycle-v1'} <= set(proof.capabilities)):
        raise ValueError('pool_runtime_readiness_unqualified')
    for role, machine_id, epoch in (('gateway', proof.gateway_machine_id, proof.gateway_credential_epoch),
                                  ('observer', proof.observer_machine_id, proof.observer_credential_epoch)):
        machine = await session.get(NebiusPoolMachine, machine_id, populate_existing=True)
        token = await session.scalar(select(Token).join(NebiusPoolMachineCredential,
            NebiusPoolMachineCredential.token_hash == Token.token_hash).where(
                NebiusPoolMachineCredential.machine_id == machine_id,
                NebiusPoolMachineCredential.credential_epoch == epoch,
                Token.type == 'pool_machine', Token.revoked_at.is_(None), Token.issued_at <= now, Token.expires_at > now))
        if (machine is None or machine.pool_id != pool.pool_id or machine.role != role or machine.participant_id is not None
                or machine.phase != 'active' or machine.credential_epoch != epoch or token is None):
            raise ValueError('pool_runtime_readiness_machine_unqualified')
