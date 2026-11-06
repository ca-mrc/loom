from datetime import UTC, datetime

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import Trial
from loom_control_plane.trial_cancellation import cancel_trial_under_authority
from tests.integration.test_service_execution_leases import (
    _cleanup_service_execution_test_rows,  # noqa: F401
    _seed_ready_trial,
)


@pytest.mark.parametrize("state", ["queued", "protected-pending", "claimed", "running"])
async def test_controller_owned_trial_cancel_records_observation(postgres_url, state):
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with sessions() as session:
            trial_id, _ = await _seed_ready_trial(session, now=datetime.now(UTC))
            trial = await session.get(Trial, trial_id)
            trial.state = state
            team_id = trial.team_id
            await session.commit()
        assert await cancel_trial_under_authority(
            session_factory=sessions, trial_id=trial_id, team_id=team_id,
        ) is not None
        async with sessions() as session:
            trial = await session.get(Trial, trial_id)
            assert trial.cancellation_requested_at is not None
            if state in {"queued", "protected-pending"}:
                assert trial.state == "cancelled"
                assert trial.cancellation_observed_at >= trial.cancellation_requested_at
                assert trial.finished_at is not None
            else:
                assert trial.state == state
                assert trial.cancellation_observed_at is None
    finally:
        await engine.dispose()
