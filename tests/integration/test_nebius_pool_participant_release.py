"""Actual candidate projection must be consumable by the protected SQL refresh."""
from __future__ import annotations

from sqlalchemy import select, update

from loom.db.nebius_pool_schema import NebiusPoolBinding, NebiusPoolParticipant
from loom_service.pool_management.installation import register_installation
from loom_service.pool_management.participant_refresh import refresh_participant
from tests.integration.test_nebius_pool_registry import sessions as sessions
from tests.ops.test_nebius_pool_participant_release import release_inputs as release_inputs
from tests.ops.test_nebius_staging_pool_attachment import (
    staging_attachment_inputs as staging_attachment_inputs,
)
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


async def test_real_candidate_release_stages_and_opens_only_its_participant(sessions, release_inputs):
    from scripts.ops.nebius_pool_participant_release import prepare_staging_pool_release

    plan = prepare_staging_pool_release(**release_inputs)
    async with sessions.begin() as session:
        await register_installation(session, plan.refresh.before)
        await session.execute(update(NebiusPoolBinding).where(NebiusPoolBinding.pool_id == plan.refresh.before.pool_id)
            .values(mode='global'))
    for action, expected in (('fence', 'fenced'), ('stage', 'staged'), ('open', 'complete')):
        async with sessions.begin() as session:
            result = await refresh_participant(session, plan.refresh, action=action)
            assert result['status'] == expected
    async with sessions() as session:
        rows = {row.participant_id: row for row in await session.scalars(select(NebiusPoolParticipant))}
        assert len(rows) == 2
        for participant in plan.refresh.after.participants:
            assert rows[participant.participant_id].phase == 'active'
            assert rows[participant.participant_id].binding_json == participant.model_dump(mode='json')
        pool = await session.get(NebiusPoolBinding, plan.refresh.after.pool_id)
        assert pool.mode == 'global' and pool.admission_epoch == plan.refresh.before.admission_epoch
