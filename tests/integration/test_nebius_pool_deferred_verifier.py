"""Deferred grading uses a fresh global grant after exact parent cleanup."""
from __future__ import annotations

from datetime import UTC, datetime

import pytest
from sqlalchemy import func, select

from loom.db.nebius_pool_outbox_schema import NebiusPoolExecutionOutbox
from loom.db.schema import ExecutionProvisioningAuthorization, ServiceExecutionLease, Task, Trial
from loom_control_plane.service_execution import enqueue_execution_transition
from loom_execution_actuator.pool_execution_selection import PoolExecutionSelector
from tests.integration import test_nebius_pool_execution_activation as activation
from tests.integration.test_nebius_pool_execution_controller import connected, lease, start
from tests.integration.test_nebius_pool_observation_registry import sessions as sessions
from tests.integration.test_service_execution_leases import (
    _hand_off_to_verifier,
    _stub_verifier_plan as _stub_verifier_plan,
)


@pytest.fixture
async def separate_setup(monkeypatch):
    original = activation.setup

    async def setup(sessions):
        outbox, trial_id, target = await original(sessions)
        async with sessions.begin() as session:
            trial = await session.get(Trial, trial_id)
            task = await session.get(Task, trial.task_id)
            raw = task.config.copy()
            raw["agent"] = {"name": "terminus-2"}
            raw["verifier"] = {"name": "script", "env_mode": "separate"}
            runtime = {**raw["service_execution"]["runtime_template"],
                       "verifier_execution": "separate_execution", "verifier": None}
            raw["service_execution"] = {**raw["service_execution"], "runtime_template": runtime}
            task.config = raw
        return outbox, trial_id, target

    monkeypatch.setattr(activation, "setup", setup)


async def finish_parent(sessions, case):
    await start(sessions, case)
    current = await lease(sessions, case)
    async with sessions.begin() as session:
        await enqueue_execution_transition(session, lease_id=current.id,
            expected_generation=current.generation, desired_state="start")
        await _hand_off_to_verifier(session, parent_id=current.id, now=datetime.now(UTC))
        await enqueue_execution_transition(session, lease_id=current.id,
            expected_generation=current.generation, desired_state="finalize")
        await enqueue_execution_transition(session, lease_id=current.id,
            expected_generation=current.generation, desired_state="delete_pending")
    await case.actuator.reconcile_full_once()
    await case.worker.run_once()
    case.sdk.pods.clear()
    await case.worker.run_once()
    await case.actuator.reconcile_full_once()
    assert (await case.outbox.get(case.proposal.request.key)).phase == "released"
    return await lease(sessions, case)


async def test_global_verifier_proposal_and_grant_preserve_parent_attempt(
    sessions, tmp_path, separate_setup, request,
):
    async with connected(sessions, tmp_path) as case:
        parent = await finish_parent(sessions, case)
        request.getfixturevalue("_stub_verifier_plan")
        selected = await PoolExecutionSelector(outbox=case.outbox).select_next()
        assert selected is not None, "a pending verifier must select a global proposal"
        assert selected.request.key.workload_kind == "verifier"
        assert selected.request.execution.parent_lease_id == parent.id
        active = await case.driver.advance(selected.request.key)
        assert active.phase == "active"
        async with sessions() as session:
            child = await session.get(ServiceExecutionLease, active.lease_id)
            trial = await session.get(Trial, case.trial_id)
            assert (child.execution_role, child.parent_lease_id, child.attempt) == ("verifier", parent.id, 1)
            assert trial.state == "running" and trial.attempt_count == 1
            assert await session.scalar(select(func.count()).select_from(ExecutionProvisioningAuthorization)) == 0
            assert await session.scalar(select(func.count()).select_from(NebiusPoolExecutionOutbox)) == 2
        assert case.resources.key(child) == selected.request.key
        assert await PoolExecutionSelector(outbox=case.outbox).select_next() is None
