"""Actual candidate projection must be consumable by the protected SQL refresh."""
from __future__ import annotations

import json
import os
import subprocess
import sys

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


async def test_fixed_refresh_command_commits_real_source_derived_transaction_before_reporting(sessions, release_inputs, tmp_path):
    from scripts.ops.nebius_pool_participant_release import prepare_staging_pool_release

    plan = prepare_staging_pool_release(**release_inputs)
    async with sessions.begin() as session:
        await register_installation(session, plan.refresh.before)
        await session.execute(update(NebiusPoolBinding).where(NebiusPoolBinding.pool_id == plan.refresh.before.pool_id)
            .values(mode='global'))
    path = tmp_path / 'refresh.json'
    path.write_text(plan.refresh.model_dump_json())
    env = {**os.environ, 'LOOM_POOL_PARTICIPANT_REFRESH_FILE': str(path),
        'LOOM_POOL_PARTICIPANT_REFRESH_DB_URL': sessions.kw['bind'].url.render_as_string(hide_password=False)}
    for action, expected in (('observe', 'ready'), ('fence', 'fenced'), ('stage', 'staged'), ('open', 'complete')):
        child = subprocess.run([sys.executable, '-m', 'loom_service.pool_management.participant_refresh', action],
            env=env, text=True, capture_output=True, timeout=25, check=False)
        assert child.returncode == 0, child.stderr
        assert json.loads(child.stdout)['status'] == expected
        async with sessions() as session:
            participant = await session.get(NebiusPoolParticipant, plan.refresh.participant_id)
            assert participant.phase == ('fenced' if action in {'fence', 'stage'} else 'active')
            assert participant.binding_revision == (plan.refresh.after.participants[0].binding_revision
                if action in {'stage', 'open'} else plan.refresh.before.participants[0].binding_revision)
    # Malformed input and unsupported action never echo private contents or report success.
    path.write_text('{"private-marker": "not-an-operation"}')
    for action in ('stage', 'register'):
        child = subprocess.run([sys.executable, '-m', 'loom_service.pool_management.participant_refresh', action],
            env=env, text=True, capture_output=True, timeout=25, check=False)
        assert child.returncode == 1 and child.stderr == ''
        assert json.loads(child.stdout) == {'status': 'pool_participant_refresh_unavailable'}
        assert 'private-marker' not in child.stdout
