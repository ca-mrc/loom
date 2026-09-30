"""Compile the real queued execution before global capacity or a local claim."""
from __future__ import annotations

from datetime import UTC, datetime

import pytest
from sqlalchemy import func, select, update

from loom.db.schema import (
    ExecutionAdmissionReservation,
    ExecutionCapacityPolicy,
    ExecutionCostReservation,
    ExecutionProvisioningAuthorization,
    ServiceExecutionCommand,
    ServiceExecutionLease,
    TeamQuota,
    Trial,
)
from loom_control_plane import service_execution_scheduler as scheduler
from tests.integration.test_nebius_pool_observation_registry import sessions as sessions
from tests.integration.test_service_execution_leases import (
    _configure_scheduler_trial,
    _seed_ready_trial,
)


@pytest.mark.parametrize("capacity_enabled", [True, False])
async def test_candidate_compilation_does_not_claim_attempt_or_spend_capacity(sessions, capacity_enabled):
    now = datetime.now(UTC)
    async with sessions.begin() as session:
        trial_id, target = await _seed_ready_trial(session, now=now)
        await _configure_scheduler_trial(session, trial_id=trial_id, now=now)
        await session.execute(update(ExecutionCapacityPolicy).values(enabled=capacity_enabled))
    async with sessions.begin() as session:
        row = (await session.execute(scheduler._NEXT_SERVICE_TRIAL, {"now": now, "pool_id": "nebius-cpu"})).mappings().one()
        compiled = await scheduler._compile_service_candidate(session, row=row, environment="staging",
            pool_id="nebius-cpu", maximum_deadline_seconds=7200, current_time=now)
        assert compiled is not None
        assert [item.id for item in compiled.targets] == [target.target_id]
        assert compiled.runtime_plan.main.argv == ("/bin/true",)
        assert compiled.runtime_plan.task_revision_sha256 == "sha256:" + "2" * 64
        assert compiled.requirements.cpu_millis == 1000
        assert (compiled.deadline_at - now).total_seconds() == 750  # 60+60+30+600.
        assert compiled.image_mode == "prebuilt" and compiled.image_ready_at == row["submitted_at"]
        assert not compiled.allocate_resources
    async with sessions() as session:
        trial = await session.get(Trial, trial_id)
        quota = await session.get(TeamQuota, trial.team_id)
        assert trial.state == "queued" and trial.attempt_count == 0 and trial.claimed_at is None
        assert trial.execution_route_generation == 0 and quota.in_flight_count == 0
        for model in (ServiceExecutionLease, ServiceExecutionCommand, ExecutionCostReservation,
                      ExecutionAdmissionReservation, ExecutionProvisioningAuthorization):
            assert await session.scalar(select(func.count()).select_from(model)) == 0
