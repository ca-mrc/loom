from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import delete, func, select, update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import (
    Batch,
    ExecutionAdmissionPolicy,
    ExecutionAdmissionReservation,
    ExecutionBudgetPolicy,
    ExecutionCostReservation,
    ExecutionPriceSnapshot,
    ExecutionProvisioningAuthorization,
    ExecutionTargetPriceBinding,
    ServiceExecutionCommand,
    ServiceExecutionLease,
    Task,
    TeamQuota,
    Trial,
)
from loom_control_plane import service_execution
from loom_control_plane import service_execution_scheduler as scheduler
from loom_control_plane.execution_admission import ExecutionAdmissionBlockedError
from loom_control_plane.execution_finance import (
    ExecutionFinanceBlockedError,
    create_execution_price_snapshot,
    upsert_target_price_binding,
)
from loom_control_plane.service_execution import ServiceExecutionConflict
from loom_control_plane.service_execution_scheduler import (
    ServiceExecutionConfigurationError,
    reserve_next_service_execution,
)
from tests.integration import test_service_execution_leases as fixtures
from tests.integration.test_service_execution_leases import (
    _cleanup_service_execution_test_rows,  # noqa: F401
    _configure_scheduler_trial,
    _reserve,
    _seed_ready_trial,
)
from tests.integration.test_service_execution_regions import _observe
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
    ServiceExecutionConflict("reservation request_id was reused with different inputs"),
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


@pytest.mark.parametrize("denial", ["admission", "finance"])
async def test_scheduler_policy_denial_rolls_back_and_allows_an_unrelated_candidate(
    postgres_url: str, denial: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    policy_id = uuid4()
    next_id = uuid4()
    try:
        async with sessions() as session:
            blocked_id, target = await _seed_ready_trial(session, now=now)
            await _configure_scheduler_trial(session, trial_id=blocked_id, now=now)
            blocked = await session.get(Trial, blocked_id)
            assert blocked is not None
            blocked.submit_priority = 200
            blocked.submitted_at = now - timedelta(minutes=1)
            team_id = blocked.team_id
            task = await session.get(Task, blocked.task_id)
            assert task is not None
            # Keep both candidates in the same fair-share scope. The denied
            # batch sorts first, while the independent batch remains eligible.
            next_task_id = f"service-execution/{uuid4().hex}"
            next_batch_id = uuid4()
            session.add_all((
                Task(id=next_task_id, checksum=task.checksum, config=deepcopy(task.config)),
                Batch(
                    id=next_batch_id, team_id=team_id, name="independent scheduler candidate",
                    task_filter={}, trial_config={}, backend="nebius", state="submitted",
                    created_by_token_prefix="test", expected_trial_count=1,
                ),
                Trial(
                    id=next_id, team_id=team_id, batch_id=next_batch_id, task_id=next_task_id,
                    config=deepcopy(blocked.config), requires_caps=deepcopy(blocked.requires_caps),
                    state="queued", attempt_count=0, submitted_at=now,
                ),
            ))
            if denial == "admission":
                occupied_id = uuid4()
                session.add_all((
                    ExecutionAdmissionPolicy(
                        id=policy_id, scope_kind="batch", scope_key=str(blocked.batch_id),
                        max_concurrent=1, enabled=True, reason="scheduler isolation test",
                    ),
                    Trial(
                        id=occupied_id, team_id=team_id, batch_id=blocked.batch_id,
                        task_id=blocked.task_id, config=deepcopy(blocked.config),
                        requires_caps=deepcopy(blocked.requires_caps), state="queued", attempt_count=0,
                    ),
                ))
                await _reserve(session, trial_id=occupied_id, target=target, now=now)
                reason = "execution_admission_batch_ceiling_reached"
            else:
                config = deepcopy(task.config)
                config["service_execution"]["runtime_template"]["main"]["timeout_seconds"] = 3600
                task.config = config
                await session.execute(update(ExecutionBudgetPolicy).where(
                    ExecutionBudgetPolicy.scope_kind == "pool",
                    ExecutionBudgetPolicy.scope_key == "nebius-cpu",
                ).values(max_estimate_duration_seconds=1000))
                reason = "execution_budget_pool_duration_exceeded"
            await session.commit()

        async with sessions() as session:
            lease = await reserve_next_service_execution(
                session, environment="staging", pool_id="nebius-cpu",
                image_admission_keyring=IMAGE_ADMISSION_KEYRING, now=now,
            )
            assert lease is not None and lease.trial_id == next_id
            await session.commit()

        async with sessions() as session:
            blocked = await session.get(Trial, blocked_id)
            assert blocked is not None and blocked.state == "queued"
            assert blocked.attempt_count == 0 and blocked.claimed_at is None
            assert blocked.started_at is None and blocked.finished_at is None
            assert blocked.failure_reason is None
            assert blocked.execution_route_generation == 0
            assert blocked.execution_route_pool_name is None
            assert blocked.next_attempt_at is not None and blocked.next_attempt_at > now
            retry_at = blocked.next_attempt_at
            assert blocked.scheduling_observation is not None
            assert blocked.scheduling_observation["reason"] == reason
            for model in (ServiceExecutionLease, ExecutionCostReservation, ExecutionAdmissionReservation):
                assert await session.scalar(select(func.count()).select_from(model).where(
                    model.trial_id == blocked_id,
                )) == 0
            quota = await session.get(TeamQuota, team_id)
            assert quota is not None and quota.in_flight_count == (2 if denial == "admission" else 1)
            if denial == "admission":
                policy = await session.get(ExecutionAdmissionPolicy, policy_id)
                assert policy is not None and policy.active_count == 1
                policy.max_concurrent = 2
            else:
                await session.execute(update(ExecutionBudgetPolicy).where(
                    ExecutionBudgetPolicy.scope_kind == "pool",
                    ExecutionBudgetPolicy.scope_key == "nebius-cpu",
                ).values(max_estimate_duration_seconds=7200))
            await session.commit()

        async with sessions() as session:
            assert await reserve_next_service_execution(
                session, environment="staging", pool_id="nebius-cpu",
                image_admission_keyring=IMAGE_ADMISSION_KEYRING, now=now,
            ) is None
            recovered = await reserve_next_service_execution(
                session, environment="staging", pool_id="nebius-cpu",
                image_admission_keyring=IMAGE_ADMISSION_KEYRING, now=retry_at,
            )
            assert recovered is not None and recovered.trial_id == blocked_id
            assert recovered.attempt == 1
            await session.commit()
    finally:
        async with sessions() as session, session.begin():
            await session.execute(delete(ExecutionAdmissionPolicy).where(
                ExecutionAdmissionPolicy.id == policy_id,
            ))
        await engine.dispose()


@pytest.mark.parametrize(("function", "error"), [
    ("reserve_execution_admission", ExecutionAdmissionBlockedError("reservation", "nebius-cpu", 0, 0)),
    ("reserve_execution_cost", ExecutionFinanceBlockedError("execution_cost_price_snapshot_mismatch")),
])
async def test_scheduler_preserves_admission_and_finance_invariant_conflicts(
    postgres_url: str, monkeypatch: pytest.MonkeyPatch, function: str, error: Exception,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    try:
        async with sessions() as session:
            trial_id, _ = await _seed_ready_trial(session, now=now)
            await _configure_scheduler_trial(session, trial_id=trial_id, now=now)
            await session.commit()

        async def reject(*args, **kwargs):
            raise error

        monkeypatch.setattr(service_execution, function, reject)
        async with sessions() as session:
            with pytest.raises(ServiceExecutionConflict) as caught:
                await reserve_next_service_execution(
                    session, environment="staging", pool_id="nebius-cpu",
                    image_admission_keyring=IMAGE_ADMISSION_KEYRING, now=now,
                )
            assert type(caught.value) is ServiceExecutionConflict
            await session.commit()
            trial = await session.get(Trial, trial_id)
            assert trial is not None and trial.state == "queued"
            assert trial.next_attempt_at is None and trial.attempt_count == 0
            assert trial.execution_route_generation == 0
            for model in (ServiceExecutionLease, ExecutionCostReservation, ExecutionAdmissionReservation):
                assert await session.scalar(select(func.count()).select_from(model).where(
                    model.trial_id == trial_id,
                )) == 0
    finally:
        await engine.dispose()


@pytest.mark.parametrize("denial", [
    "target-budget", "region", "price-unavailable", "price-not-effective",
    "capacity-then-policy", "policy-then-capacity", "all-target-budgets",
])
async def test_scheduler_isolates_target_denials_and_preserves_capacity_wait(
    postgres_url: str, monkeypatch: pytest.MonkeyPatch, denial: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    scheduled_at = now + timedelta(seconds=2)
    policy_id = uuid4()
    original_target = fixtures._target
    try:
        async with sessions() as session:
            trial_id, primary = await _seed_ready_trial(session, now=now)
            await _configure_scheduler_trial(session, trial_id=trial_id, now=now)
            with monkeypatch.context() as patch:
                patch.setattr(fixtures, "_target", lambda suffix: original_target(suffix).model_copy(
                    update={"region": "eu-west1", "health_role": "secondary",
                            "failure_domain": "west-b", "cluster_scope_id": "regional-west"},
                ))
                spare_trial_id, secondary = await _seed_ready_trial(session, now=now)
            await _observe(session, primary, now + timedelta(seconds=1),
                           quota=2, stock=denial != "capacity-then-policy")
            await _observe(session, secondary, now + timedelta(seconds=1),
                           quota=2, stock=denial != "policy-then-capacity")
            primary_policy = await session.scalar(select(ExecutionBudgetPolicy).where(
                ExecutionBudgetPolicy.scope_kind == "target",
                ExecutionBudgetPolicy.scope_key == primary.target_id,
            ))
            assert primary_policy is not None
            if denial in {"target-budget", "policy-then-capacity", "all-target-budgets"}:
                primary_policy.emergency_stop = True
            if denial in {"capacity-then-policy", "all-target-budgets"}:
                await session.execute(update(ExecutionBudgetPolicy).where(
                    ExecutionBudgetPolicy.scope_kind == "target",
                    ExecutionBudgetPolicy.scope_key == secondary.target_id,
                ).values(emergency_stop=True))
            if denial == "region":
                session.add(ExecutionAdmissionPolicy(
                    id=policy_id, scope_kind="region", scope_key=primary.region,
                    max_concurrent=1, enabled=True, reason="regional fallback regression",
                ))
                await _reserve(session, trial_id=spare_trial_id, target=primary, now=scheduled_at)
            if denial.startswith("price-"):
                binding = await session.get(ExecutionTargetPriceBinding, primary.target_id)
                assert binding is not None
                if denial == "price-unavailable":
                    binding.enabled = False
                else:
                    price = await session.get(ExecutionPriceSnapshot, binding.price_snapshot_id)
                    assert price is not None
                    future_price, _ = await create_execution_price_snapshot(
                        session,
                        provider=price.provider, region=price.region, sku=price.sku,
                        source=price.source, source_version=f"{primary.target_id}-future",
                        source_uri=price.source_uri,
                        effective_at=scheduled_at + timedelta(hours=1), observed_at=now,
                        base_microusd_per_hour=price.base_microusd_per_hour,
                        vcpu_microusd_per_hour=price.vcpu_microusd_per_hour,
                        memory_gib_microusd_per_hour=price.memory_gib_microusd_per_hour,
                        ephemeral_storage_gib_microusd_per_hour=price.ephemeral_storage_gib_microusd_per_hour,
                    )
                    await upsert_target_price_binding(
                        session, target_id=primary.target_id, price_snapshot_id=future_price.id,
                        enabled=True, reason="future price regression", now=now,
                    )
            primary_reserved = (primary_policy.daily_reserved_microusd,
                                primary_policy.monthly_reserved_microusd)
            await session.commit()

        async with sessions() as session:
            lease = await reserve_next_service_execution(
                session, environment="staging", pool_id="nebius-cpu",
                image_admission_keyring=IMAGE_ADMISSION_KEYRING, now=scheduled_at,
            )
            await session.commit()

        async with sessions() as session:
            trial = await session.get(Trial, trial_id)
            assert trial is not None
            quota = await session.get(TeamQuota, trial.team_id)
            assert quota is not None
            waiting = denial in {"capacity-then-policy", "policy-then-capacity", "all-target-budgets"}
            expected_count = 0 if waiting else 1
            for model in (ServiceExecutionLease, ExecutionCostReservation, ExecutionAdmissionReservation):
                assert await session.scalar(select(func.count()).select_from(model).where(
                    model.trial_id == trial_id,
                )) == expected_count
            assert trial.attempt_count == quota.in_flight_count == expected_count
            if waiting:
                assert lease is None and trial.state == "queued"
                assert trial.execution_route_generation == 0
                assert trial.execution_route_pool_name is None
                assert trial.claimed_at is None and trial.started_at is None
                assert trial.next_attempt_at == scheduled_at + timedelta(seconds=15)
                observation = trial.scheduling_observation
                assert observation is not None
                if denial == "all-target-budgets":
                    assert observation["reason"] == "execution_budget_target_emergency_stop"
                    assert "capacity_wait" not in observation
                else:
                    waiting_target = primary if denial == "capacity-then-policy" else secondary
                    assert observation["reason"].startswith("execution_capacity_")
                    assert observation["capacity_wait"]["target_id"] == waiting_target.target_id
            else:
                assert lease is not None and lease.target_id == secondary.target_id
                assert trial.state == "claimed" and trial.execution_route_generation == 1
                assert trial.execution_route_pool_name == "nebius-cpu"
                cost = await session.scalar(select(ExecutionCostReservation).where(
                    ExecutionCostReservation.trial_id == trial_id,
                ))
                admission = await session.scalar(select(ExecutionAdmissionReservation).where(
                    ExecutionAdmissionReservation.trial_id == trial_id,
                ))
                assert cost is not None and cost.target_id == secondary.target_id
                assert admission is not None and admission.region == secondary.region
                for model in (ExecutionProvisioningAuthorization, ServiceExecutionCommand):
                    assert await session.scalar(select(func.count()).select_from(model).where(
                        model.lease_id == lease.id,
                    )) == 1
            primary_policy = await session.scalar(select(ExecutionBudgetPolicy).where(
                ExecutionBudgetPolicy.scope_kind == "target",
                ExecutionBudgetPolicy.scope_key == primary.target_id,
            ))
            assert primary_policy is not None
            assert (primary_policy.daily_reserved_microusd,
                    primary_policy.monthly_reserved_microusd) == primary_reserved
            if denial == "region":
                policy = await session.get(ExecutionAdmissionPolicy, policy_id)
                assert policy is not None and policy.active_count == 1
    finally:
        async with sessions() as session, session.begin():
            await session.execute(delete(ExecutionAdmissionPolicy).where(
                ExecutionAdmissionPolicy.id == policy_id,
            ))
        await engine.dispose()
