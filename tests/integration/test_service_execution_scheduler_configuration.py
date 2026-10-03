from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime

import pytest
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import (
    Batch,
    ExecutionAdmissionReservation,
    ExecutionBudgetPolicy,
    ExecutionCostReservation,
    ServiceExecutionLease,
    Task,
    TeamQuota,
    Trial,
)
from loom_control_plane import service_execution_scheduler as scheduler
from loom_control_plane.service_execution_scheduler import (
    ServiceExecutionConfigurationError,
    reserve_next_service_execution,
)
from tests.integration.test_service_execution_leases import (
    _cleanup_service_execution_test_rows,  # noqa: F401
    _configure_scheduler_trial,
    _seed_ready_trial,
)
from tests.support.execution_image_admission import IMAGE_ADMISSION_KEYRING
from tests.unit.test_service_execution_materialization import _profile, _provenance, _task, _trial


@pytest.mark.parametrize("automatic", [False, True])
async def test_invalid_runtime_contract_does_not_block_next_trial_or_leak_input(
    postgres_url: str, caplog: pytest.LogCaptureFixture, automatic: bool,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    private_input = "private-configuration-sentinel-" * 200
    try:
        async with sessions() as session:
            invalid_id, _ = await _seed_ready_trial(session, now=now)
            await _configure_scheduler_trial(session, trial_id=invalid_id, now=now)
            invalid = await session.get(Trial, invalid_id)
            assert invalid is not None
            invalid.submit_priority = 200
            invalid.scheduling_observation = {"reason": "task_image_preparation_pending", "observed_at": now.isoformat()}
            team_id = invalid.team_id
            task = await session.get(Task, invalid.task_id)
            assert task is not None
            config = deepcopy(task.config)
            if automatic:
                # A frozen older controller cannot read declarations from its
                # task input. Reject its oversized plan without reselecting it.
                config = _task().model_dump(mode="json")
                config["steps"][0]["artifacts"] = [f"private-configuration-sentinel-{i:04}.txt" for i in range(515)]
                task.source_provenance = _provenance()
                batch = await session.get(Batch, invalid.batch_id)
                assert batch is not None
                batch.service_execution_runtime_profile = _profile().model_dump(mode="json")
                invalid.config = _trial().model_dump(mode="json")
            else:
                config["service_execution"]["runtime_template"]["main"]["environment"] = {"LOOM_TASK_ARTIFACTS_JSON": private_input}
            task.config = config
            next_id, _ = await _seed_ready_trial(session, now=now)
            await _configure_scheduler_trial(session, trial_id=next_id, now=now)
            await session.commit()
        async with sessions() as session:
            lease = await reserve_next_service_execution(
                session, environment="staging", pool_id="nebius-cpu",
                image_admission_keyring=IMAGE_ADMISSION_KEYRING, now=now,
            )
            assert lease is not None and lease.trial_id == next_id
            await session.commit()
        async with sessions() as session:
            invalid = await session.get(Trial, invalid_id)
            assert invalid is not None and invalid.state == "failed"
            assert invalid.failure_reason == "service_execution_configuration_invalid"
            assert "runtime contract" in invalid.failure_message
            assert "private-configuration-sentinel" not in invalid.failure_message + caplog.text
            assert invalid.attempt_count == 0 and invalid.started_at is None and invalid.claimed_at is None
            assert invalid.finished_at == now and invalid.next_attempt_at is None
            assert invalid.scheduling_observation is None
            quota = await session.get(TeamQuota, team_id)
            assert quota is not None and quota.in_flight_count == 0
            for model in (ServiceExecutionLease, ExecutionCostReservation, ExecutionAdmissionReservation):
                assert await session.scalar(select(func.count()).select_from(model).where(model.trial_id == invalid_id)) == 0
    finally:
        await engine.dispose()


@pytest.mark.parametrize("maximum_seconds", [7200, 14400])
async def test_scheduler_isolates_unsupported_deadline_and_respects_configured_bound(
    postgres_url: str, maximum_seconds: int,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    try:
        async with sessions() as session:
            blocked_id, blocked_target = await _seed_ready_trial(session, now=now)
            await _configure_scheduler_trial(session, trial_id=blocked_id, now=now)
            blocked = await session.get(Trial, blocked_id)
            assert blocked is not None
            blocked.submit_priority = 200
            team_id = blocked.team_id
            task = await session.get(Task, blocked.task_id)
            assert task is not None
            config = deepcopy(task.config)
            template = config["service_execution"]["runtime_template"]
            template["main"]["timeout_seconds"] = 3600
            template["verifier"]["timeout_seconds"] = 3600
            task.config = config
            next_id, next_target = await _seed_ready_trial(session, now=now)
            await _configure_scheduler_trial(session, trial_id=next_id, now=now)
            # Finance has its own duration bound. Explicitly make the fixture
            # compatible; changing a scheduler bound must never bypass it.
            await session.execute(update(ExecutionBudgetPolicy).where(
                ExecutionBudgetPolicy.scope_key.in_((
                    "nebius-cpu", blocked_target.target_id, next_target.target_id,
                )),
            ).values(max_estimate_duration_seconds=14400))
            await session.commit()

        async with sessions() as session:
            lease = await reserve_next_service_execution(
                session, environment="staging", pool_id="nebius-cpu",
                image_admission_keyring=IMAGE_ADMISSION_KEYRING, now=now,
                maximum_deadline_seconds=maximum_seconds,
            )
            if maximum_seconds == 14400:
                assert lease is not None and lease.trial_id == blocked_id
                assert (lease.deadline_at - now).total_seconds() == 7830
                assert lease.runtime_contract_json["main"]["timeout_seconds"] == 3600
                assert lease.runtime_contract_json["verifier"]["timeout_seconds"] == 3600
                lease = await reserve_next_service_execution(
                    session, environment="staging", pool_id="nebius-cpu",
                    image_admission_keyring=IMAGE_ADMISSION_KEYRING, now=now,
                    maximum_deadline_seconds=maximum_seconds,
                )
            assert lease is not None and lease.trial_id == next_id
            assert lease.attempt == 1
            await session.commit()

        async with sessions() as session:
            blocked = await session.get(Trial, blocked_id)
            assert blocked is not None
            if maximum_seconds == 14400:
                assert blocked.state == "claimed" and blocked.attempt_count == 1
                assert blocked.failure_reason is None
                return
            assert blocked.state == "failed"
            assert blocked.failure_reason == "service_execution_configuration_invalid"
            assert "requested_seconds=7830" in blocked.failure_message
            assert "maximum_seconds=7200" in blocked.failure_message
            assert blocked.finished_at == now
            assert blocked.attempt_count == 0
            assert blocked.claimed_at is None and blocked.started_at is None
            assert blocked.next_attempt_at is None
            quota = await session.get(TeamQuota, team_id)
            assert quota is not None and quota.in_flight_count == 0
            for model in (ServiceExecutionLease, ExecutionCostReservation, ExecutionAdmissionReservation):
                assert await session.scalar(
                    select(func.count()).select_from(model).where(model.trial_id == blocked_id)
                ) == 0
            task = await session.get(Task, blocked.task_id)
            assert task is not None and task.config == config
    finally:
        await engine.dispose()


@pytest.mark.parametrize("error", [
    ServiceExecutionConfigurationError("unsupported configured deadline"),
    ValueError("unexpected data failure"),
    RuntimeError("transient platform failure"),
])
async def test_scheduler_rolls_back_candidate_writes_and_only_classifies_known_configuration_errors(
    postgres_url: str, monkeypatch: pytest.MonkeyPatch, error: Exception,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    try:
        async with sessions() as session:
            trial_id, _ = await _seed_ready_trial(session, now=now)
            await _configure_scheduler_trial(session, trial_id=trial_id, now=now)
            trial = await session.get(Trial, trial_id)
            assert trial is not None
            team_id = trial.team_id
            await session.commit()

        async def failing_candidate(session, **kwargs):
            await session.execute(update(Trial).where(Trial.id == trial_id).values(attempt_count=3))
            await session.execute(update(TeamQuota).where(TeamQuota.team_id == team_id).values(in_flight_count=1))
            raise error

        monkeypatch.setattr(scheduler, "_reserve_service_candidate", failing_candidate)
        async with sessions() as session:
            if isinstance(error, ServiceExecutionConfigurationError):
                assert await reserve_next_service_execution(
                    session, environment="staging", pool_id="nebius-cpu",
                    image_admission_keyring=IMAGE_ADMISSION_KEYRING, now=now,
                ) is None
            else:
                with pytest.raises(type(error), match=str(error)):
                    await reserve_next_service_execution(
                        session, environment="staging", pool_id="nebius-cpu",
                        image_admission_keyring=IMAGE_ADMISSION_KEYRING, now=now,
                    )
            await session.commit()
            trial = await session.get(Trial, trial_id)
            quota = await session.get(TeamQuota, team_id)
            assert trial is not None and trial.attempt_count == 0
            assert quota is not None and quota.in_flight_count == 0
            if isinstance(error, ServiceExecutionConfigurationError):
                assert trial.state == "failed"
                assert trial.failure_reason == "service_execution_configuration_invalid"
            else:
                assert trial.state == "queued"
                assert trial.failure_reason is None and trial.finished_at is None
    finally:
        await engine.dispose()
