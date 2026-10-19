"""Admission lifecycle is separate from charged-work execution authority."""
from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.nebius_pool_lifecycle_schema import NebiusPoolParticipantLifecycle
from loom.db.nebius_pool_schema import NebiusPoolBinding, NebiusPoolParticipant
from loom_service.pool_management.profile_registry import registry_enabled


def scoped_lifecycle(pool: NebiusPoolBinding) -> bool:
    version = pool.binding_json.get('lifecycle_source')
    if version is None:
        return False
    if version != 'scoped-v1' or not registry_enabled(pool):
        raise ValueError('pool_lifecycle_unavailable')
    return True


async def intake_open(session: AsyncSession, pool: NebiusPoolBinding, participant: NebiusPoolParticipant) -> bool:
    if any(isinstance(row, NebiusPoolParticipantLifecycle) for row in session.new | session.dirty | session.deleted):
        raise ValueError('pool_lifecycle_requires_clean_transaction')
    if not scoped_lifecycle(pool):
        return participant.phase == 'active'
    head = await session.get(NebiusPoolParticipantLifecycle, participant.participant_id, populate_existing=True)
    return bool(head is not None and head.pool_id == pool.pool_id and head.binding_sha256 == participant.binding_sha256
                and head.state == 'active' and participant.phase == 'active')
