"""Real SQL: retained execution proposals claim only their exact admitted work."""
from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from sqlalchemy import func, select, text, update
from sqlalchemy.exc import DBAPIError

from loom.db.schema import (
    ExecutionAdmissionReservation,
    ExecutionBudgetPolicy,
    ExecutionCapacityPolicy,
    ExecutionCostReservation,
    ExecutionProvisioningAuthorization,
    ServiceExecutionCommand,
    ServiceExecutionLease,
    ServiceExecutionTarget,
    Task,
    TeamQuota,
    Trial,
)
from loom.nebius_pool_contract import PoolParticipantV1, PoolReceiptV1
from loom.nebius_pool_priority import PoolWorkOriginV1
from tests.integration.test_nebius_pool_observation_registry import sessions as sessions
from tests.integration.test_service_execution_leases import _configure_scheduler_trial, _seed_ready_trial
from tests.support.execution_image_admission import IMAGE_ADMISSION_KEYRING


async def setup(sessions, *, origin=True):
    from loom_execution_actuator.pool_execution_outbox import PoolExecutionOutbox

    environment_id = uuid4()
    source = PoolWorkOriginV1(data_environment_id=environment_id, submission_id=uuid4(),
        kind="environment", application=None)
    now = datetime.now(UTC)
    async with sessions.begin() as session:
        trial_id, target = await _seed_ready_trial(session, now=now,
            pool_origin=source.model_dump(mode="json") if origin else None)
        await _configure_scheduler_trial(session, trial_id=trial_id, now=now)
        # A global grant replaces this physical allocator, not local cost/admission.
        await session.execute(update(ExecutionCapacityPolicy).values(enabled=False))
    participant = PoolParticipantV1(participant_id=uuid4(), installation_id=uuid4(),
        environment_id=environment_id, environment_class="staging", incarnation=uuid4(),
        pool_id=uuid4(), binding_revision=1, admission_epoch=2,
        execution_namespace={"name": target.namespace_name, "uid": uuid4()},
        build_namespace={"name": "loom-build-staging", "uid": uuid4()},
        targets=[{"target_id": target.target_id, "profile_id": uuid4(), "workload_kinds": ["trial", "verifier"]}])
    journal = PoolExecutionOutbox(sessions=sessions, participant=participant,
        environment="staging", logical_pool_id="nebius-cpu", image_admission_keyring=IMAGE_ADMISSION_KEYRING)
    return journal, trial_id, target


def grant(proposal):
    return PoolReceiptV1(pool_id=proposal.request.pool_id, request_key=proposal.request.key,
        admission_epoch=proposal.request.admission_epoch, request_sha256=proposal.request_sha256,
        reservation_id=uuid4(), phase="reserved", plan_sha256=None, job_uid=None,
        cleanup_observation_id=None)


async def assert_unclaimed(sessions, trial_id):
    async with sessions() as session:
        trial = await session.get(Trial, trial_id)
        assert trial.state == "queued" and trial.attempt_count == 0 and trial.claimed_at is None
        assert trial.execution_route_generation == 0
        assert (await session.get(TeamQuota, trial.team_id)).in_flight_count == 0
        for model in (ServiceExecutionLease, ServiceExecutionCommand, ExecutionAdmissionReservation,
                      ExecutionCostReservation, ExecutionProvisioningAuthorization):
            assert await session.scalar(select(func.count()).select_from(model)) == 0


async def test_proposal_survives_restart_without_consuming_local_attempt_or_budgets(sessions):
    journal, trial_id, target = await setup(sessions)
    proposed = await journal.propose(trial_id=trial_id, target_id=target.target_id)
    replay = await journal.propose(trial_id=trial_id, target_id=target.target_id)
    assert replay == proposed == await journal.get(proposed.request.key)
    assert proposed.phase == "selected" and proposed.lease_id is None
    assert proposed.request.key.local_work_id != trial_id
    assert proposed.request.execution.lease_generation == 1
    assert proposed.request.execution.runtime.main.argv == ("/bin/true",)
    assert proposed.request.execution.runtime.task_revision_sha256 == "sha256:" + "2" * 64
    await assert_unclaimed(sessions, trial_id)


async def test_concurrent_grant_replay_attaches_once_with_final_immutable_job_identity(sessions):
    journal, trial_id, target = await setup(sessions)
    proposed, replay = await asyncio.gather(*(journal.propose(trial_id=trial_id,
        target_id=target.target_id) for _ in range(2)))
    assert proposed == replay
    receipt = grant(proposed)
    first, replay = await asyncio.gather(*(journal.accept_grant(proposed.request.key, receipt) for _ in range(2)))
    assert first == replay and first.phase == "attached"
    assert first.lease_id == proposed.request.key.local_work_id
    async with sessions() as session:
        lease = await session.get(ServiceExecutionLease, first.lease_id)
        assert lease.job_name == "loom-pool-" + receipt.reservation_id.hex
        assert lease.namespace_name == target.namespace_name and lease.trial_id == trial_id
        assert lease.execution_unit_key == proposed.request.execution.execution_unit_key
        assert lease.deadline_at == proposed.request.deadline_at and lease.attempt == 1
        assert lease.runtime_contract_json == proposed.request.execution.runtime.canonical_payload()
        trial = await session.get(Trial, trial_id)
        assert trial.state == "claimed" and trial.attempt_count == 1
        assert (await session.get(TeamQuota, trial.team_id)).in_flight_count == 1
        assert await session.scalar(select(func.count()).select_from(ServiceExecutionLease)) == 1
        assert await session.scalar(select(func.count()).select_from(ExecutionCostReservation)) == 1
        assert await session.scalar(select(func.count()).select_from(ExecutionAdmissionReservation)) == 1
        assert await session.scalar(select(func.count()).select_from(ExecutionProvisioningAuthorization)) == 0
        command = await session.scalar(select(ServiceExecutionCommand))
        assert command.payload_json["job_name"] == lease.job_name
        assert command.payload_json["lease_id"] == str(first.lease_id)


@pytest.mark.parametrize("damage", ["trial-config", "task-config", "target", "cancel", "budget", "attempt-ceiling"])
async def test_changed_selection_or_local_denial_cancels_grant_without_claim(sessions, damage):
    journal, trial_id, target = await setup(sessions)
    proposed = await journal.propose(trial_id=trial_id, target_id=target.target_id)
    async with sessions.begin() as session:
        trial = await session.get(Trial, trial_id)
        if damage == "trial-config":
            trial.config = {**trial.config, "agent_name": "changed"}
        elif damage == "task-config":
            task = await session.get(Task, trial.task_id)
            task.config = {**task.config, "agent": {"name": "changed"}}
        elif damage == "target":
            await session.execute(update(ServiceExecutionTarget).where(ServiceExecutionTarget.id == target.target_id).values(
                desired_state="draining"))
        elif damage == "cancel":
            trial.cancellation_requested_at = datetime.now(UTC)
        elif damage == "budget":
            await session.execute(update(ExecutionBudgetPolicy).values(emergency_stop=True))
        else:
            quota = await session.get(TeamQuota, trial.team_id)
            quota.max_attempts_ceiling = 1
            # A prospective attempt selected under a different ceiling is stale.
            trial.attempt_count = 1
    attached = await journal.accept_grant(proposed.request.key, grant(proposed))
    assert attached.phase == "cancel_pending" and attached.lease_id is None
    if damage != "attempt-ceiling":
        await assert_unclaimed(sessions, trial_id)
    else:
        async with sessions() as session:
            assert (await session.get(Trial, trial_id)).attempt_count == 1
            assert await session.scalar(select(func.count()).select_from(ServiceExecutionLease)) == 0


async def test_missing_submission_origin_cannot_be_promoted_to_shared_work(sessions):
    journal, trial_id, target = await setup(sessions, origin=False)
    with pytest.raises(ValueError):
        await journal.propose(trial_id=trial_id, target_id=target.target_id)
    await assert_unclaimed(sessions, trial_id)


async def test_foreign_receipt_cannot_claim_selected_trial(sessions):
    journal, trial_id, target = await setup(sessions)
    proposed = await journal.propose(trial_id=trial_id, target_id=target.target_id)
    wrong = grant(proposed).model_copy(update={"request_sha256": "f" * 64})
    with pytest.raises(ValueError):
        await journal.accept_grant(proposed.request.key, wrong)
    assert (await journal.get(proposed.request.key)).phase == "selected"
    await assert_unclaimed(sessions, trial_id)


@pytest.mark.parametrize("change", ["request_json", "selection_json", "trial_id", "delete"])
async def test_database_retains_proposal_identity_and_history(sessions, change):
    journal, trial_id, target = await setup(sessions)
    proposed = await journal.propose(trial_id=trial_id, target_id=target.target_id)
    command = ("DELETE FROM nebius_pool_execution_outbox WHERE lease_id=:id" if change == "delete" else
        "UPDATE nebius_pool_execution_outbox SET " + {
            "request_json": "request_json='{}'::jsonb", "selection_json": "selection_json='{}'::jsonb",
            "trial_id": "trial_id=gen_random_uuid()"}.get(change, "") + " WHERE lease_id=:id")
    with pytest.raises(DBAPIError):
        async with sessions.begin() as session:
            await session.execute(text(command), {"id": proposed.request.key.local_work_id})
    assert await journal.get(proposed.request.key) == proposed


async def test_failed_outbox_attach_rolls_back_claim_cost_and_admission_together(sessions):
    journal, trial_id, target = await setup(sessions)
    proposed = await journal.propose(trial_id=trial_id, target_id=target.target_id)
    async with sessions.begin() as session:
        await session.execute(text("""CREATE FUNCTION refuse_execution_attach() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN IF NEW.phase='attached' THEN RAISE EXCEPTION 'test attach crash'; END IF; RETURN NEW; END $$"""))
        await session.execute(text("""CREATE TRIGGER test_attach_failure BEFORE UPDATE ON nebius_pool_execution_outbox
            FOR EACH ROW EXECUTE FUNCTION refuse_execution_attach()"""))
    with pytest.raises(DBAPIError):
        await journal.accept_grant(proposed.request.key, grant(proposed))
    assert await journal.get(proposed.request.key) == proposed
    await assert_unclaimed(sessions, trial_id)
