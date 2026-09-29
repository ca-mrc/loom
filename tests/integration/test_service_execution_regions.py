"""Regional scheduler fallback preserves one execution transaction and residency."""

from __future__ import annotations

import inspect
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from loom.db.schema import (
    ExecutionAdmissionReservation,
    ExecutionBudgetPolicy,
    ExecutionCapacityObservation,
    ExecutionCapacityPolicy,
    ExecutionCostReservation,
    ExecutionProvisioningAuthorization,
    ServiceExecutionCommand,
    ServiceExecutionLease,
    TeamQuota,
    Trial,
)
from loom.execution_contract import ExecutionTargetV1
from loom_control_plane import service_execution_scheduler as scheduler
from loom_control_plane.execution_capacity import create_execution_capacity_observation
from tests.execution_placement_fixtures import placement_fixture
from tests.integration import test_service_execution_leases as fixtures
from tests.integration.test_service_execution_leases import (
    _cleanup_service_execution_test_rows,  # noqa: F401 -- shared disposable fixture
)
from tests.support.execution_image_admission import IMAGE_ADMISSION_KEYRING


async def _observe(
    session: AsyncSession,
    target: ExecutionTargetV1,
    now: datetime,
    *,
    quota: int,
    stock: bool = True,
) -> None:
    previous = await session.scalar(
        select(ExecutionCapacityObservation).where(
            ExecutionCapacityObservation.target_id == target.target_id
        )
    )
    assert previous is not None
    fields = inspect.signature(create_execution_capacity_observation).parameters
    payload = {
        key: deepcopy(value) for key, value in previous.observation_json.items() if key in fields
    }
    placement = placement_fixture(
        target_id=target.target_id,
        region=target.region,
        parent_id="regional-tenant",
        nodes=0,
        used_nodes=0,
        quota_nodes=quota,
    )
    payload.update(
        observed_at=now,
        source_version=str(uuid4()),
        placement=placement,
        active_nodes=0,
        node_states=None,
        provider_capacity_state="available" if stock else "insufficient",
    )
    for label, suffix in (
        ("nodes", "nodes"),
        ("vcpu", "vcpu_millis"),
        ("memory", "memory_mib"),
        ("storage", "storage_mib"),
    ):
        payload["provider_quota_" + suffix] = placement["quota_resources"][label]["limit"]
        payload["provider_used_" + suffix] = 0
    for key in payload:
        if key.startswith(("provisioned_", "allocatable_")):
            payload[key] = 0
    await create_execution_capacity_observation(session, **payload)


@pytest.mark.parametrize(
    "case",
    [
        "healthy",
        "quota",
        "stale",
        "stock",
        "all_exhausted",
        "wrong_environment",
        "wrong_pool",
        "internal_residency",
    ],
)
async def test_regional_fallback_keeps_one_claim_and_rolls_back_rejected_target(
    postgres_url: str,
    monkeypatch: pytest.MonkeyPatch,
    case: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    scheduled_at = now + timedelta(seconds=20)
    original_target = fixtures._target
    primary_region = "eu-west1" if case == "healthy" else "eu-north1"
    secondary_region = "eu-north1" if case == "healthy" else "eu-west1"
    try:
        async with sessions() as session:
            # The healthy case reverses lexical order to prove health_role
            # decides preference. Failure cases exercise the former head target.
            with monkeypatch.context() as patch:
                patch.setattr(
                    fixtures,
                    "_target",
                    lambda suffix: original_target(suffix).model_copy(
                        update={
                            "region": primary_region,
                            "failure_domain": primary_region + "-a",
                            "cluster_scope_id": "primary-regional-cluster",
                        }
                    ),
                )
                trial_id, primary = await fixtures._seed_ready_trial(session, now=now)
            await fixtures._configure_scheduler_trial(session, trial_id=trial_id, now=now)
            with monkeypatch.context() as patch:
                patch.setattr(
                    fixtures,
                    "_target",
                    lambda suffix: original_target(suffix).model_copy(
                        update={
                            "region": secondary_region,
                            "health_role": "secondary",
                            "failure_domain": secondary_region + "-b",
                            "cluster_scope_id": "secondary-regional-cluster",
                            "environment": "production"
                            if case == "wrong_environment"
                            else "staging",
                            "logical_pool_id": "other-pool"
                            if case == "wrong_pool"
                            else "nebius-cpu",
                        }
                    ),
                )
                _, secondary = await fixtures._seed_ready_trial(session, now=now)
            await _observe(
                session,
                primary,
                now + timedelta(seconds=1),
                quota=2 if case in {"healthy", "stock", "stale"} else 0,
                stock=case != "stock",
            )
            await _observe(
                session,
                secondary,
                now + timedelta(seconds=2),
                quota=0 if case == "all_exhausted" else 2,
            )
            if case == "stale":
                policy = await session.get(ExecutionCapacityPolicy, primary.target_id)
                assert policy is not None
                policy.observation_max_age_seconds = 10
            await session.commit()
        if case == "internal_residency":
            # TaskConfig currently exposes no residency preference. Exercise
            # the explicit internal workload contract without inventing one.
            project = scheduler.workload_requirements_from_task
            monkeypatch.setattr(
                scheduler,
                "workload_requirements_from_task",
                lambda task, trial=None: project(task, trial).model_copy(update={"data_residency": "us"}),
            )
        async with sessions() as session:
            lease = await scheduler.reserve_next_service_execution(
                session,
                environment="staging",
                pool_id="nebius-cpu",
                image_admission_keyring=IMAGE_ADMISSION_KEYRING,
                now=scheduled_at,
            )
            await session.commit()
        expected = (
            primary
            if case == "healthy"
            else secondary
            if case in {"quota", "stale", "stock"}
            else None
        )
        async with sessions() as session:
            trial = await session.get(Trial, trial_id)
            assert trial is not None
            quota = await session.get(TeamQuota, trial.team_id)
            assert quota is not None
            leases = (
                await session.scalars(
                    select(ServiceExecutionLease).where(ServiceExecutionLease.trial_id == trial_id)
                )
            ).all()
            costs = (
                await session.scalars(
                    select(ExecutionCostReservation).where(
                        ExecutionCostReservation.trial_id == trial_id
                    )
                )
            ).all()
            admission_count = await session.scalar(
                select(func.count())
                .select_from(ExecutionAdmissionReservation)
                .where(ExecutionAdmissionReservation.trial_id == trial_id)
            )
            if expected is None:
                assert lease is None and leases == [] and costs == [] and admission_count == 0
                assert trial.state == "queued" and trial.attempt_count == quota.in_flight_count == 0
                assert trial.claimed_at is None and trial.execution_route_pool_name is None
                assert trial.next_attempt_at == scheduled_at + timedelta(seconds=15)
            else:
                assert lease is not None and lease.target_id == expected.target_id
                assert len(leases) == len(costs) == admission_count == 1
                assert costs[0].target_id == expected.target_id
                assert (
                    trial.state == "claimed" and trial.attempt_count == quota.in_flight_count == 1
                )
                assert trial.execution_route_pool_name == "nebius-cpu"
                assert (
                    await session.scalar(
                        select(func.count())
                        .select_from(ExecutionProvisioningAuthorization)
                        .where(ExecutionProvisioningAuthorization.lease_id == lease.id)
                    )
                    == 1
                )
                assert (
                    await session.scalar(
                        select(func.count())
                        .select_from(ServiceExecutionCommand)
                        .where(ServiceExecutionCommand.lease_id == lease.id)
                    )
                    == 1
                )
            for target in (primary, secondary):
                policy = await session.scalar(
                    select(ExecutionBudgetPolicy).where(
                        ExecutionBudgetPolicy.scope_kind == "target",
                        ExecutionBudgetPolicy.scope_key == target.target_id,
                    )
                )
                assert policy is not None
                if expected is not None and target.target_id == expected.target_id:
                    assert policy.daily_reserved_microusd > 0
                else:
                    assert policy.daily_reserved_microusd == policy.monthly_reserved_microusd == 0
    finally:
        await engine.dispose()
