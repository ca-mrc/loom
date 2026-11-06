"""Regional re-selection follows real unschedulable retry and UID cleanup."""

from copy import deepcopy
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from fastapi import HTTPException
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.auth import AuthContext
from loom.db.schema import (
    ExecutionAdmissionReservation,
    ExecutionCostReservation,
    ExecutionProvisioningAuthorization,
    ServiceExecutionLease,
    ServiceExecutionTarget,
    Trial,
)
from loom.execution_contract import WorkloadRequirementsV1
from loom.execution_runtime_contract import ExecutionRuntimePlanV1
from loom_control_plane.service_execution import (
    ServiceExecutionConflict,
    ServiceExecutionFenceError,
    set_execution_target_health,
    verify_trial_execution_fence,
)
from loom_control_plane.service_execution_scheduler import reserve_next_service_execution
from loom_execution_actuator.contracts import NormalizedJobState
from loom_execution_actuator.controller import ExecutionActuator
from loom_execution_actuator.renderer import ExecutionTargetRuntime
from loom_llm_gateway.execution_attempt_dispatch import authorize_trial_execution_dispatch
from tests.integration import test_service_execution_leases as fixtures
from tests.integration.test_service_execution_leases import (
    _cleanup_service_execution_test_rows,  # noqa: F401 -- disposable owned rows
)
from tests.integration.test_service_execution_regions import _observe
from tests.support.execution_image_admission import IMAGE_ADMISSION_KEYRING


@pytest.mark.parametrize(
    "boundary",
    [
        "eligible",
        "started",
        "other_failure",
        "bound_retry",
        "wrong_environment",
        "wrong_residency",
        "requirements_drift",
    ],
)
async def test_regional_retry_rebinds_only_after_unstarted_infra_cleanup(
    postgres_url: str,
    monkeypatch: pytest.MonkeyPatch,
    boundary: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    kube = fixtures._FakeKubernetesJobApi()
    original_target = fixtures._target
    try:
        async with sessions() as session:
            trial_id, primary = await fixtures._seed_ready_trial(session, now=now)
            await fixtures._configure_scheduler_trial(session, trial_id=trial_id, now=now)
            with monkeypatch.context() as patch:
                patch.setattr(
                    fixtures,
                    "_target",
                    lambda suffix: original_target(suffix).model_copy(
                        update={
                            "region": "eu-west1",
                            "failure_domain": "west-b",
                            "cluster_scope_id": "regional-west",
                            "health_role": "secondary",
                        }
                    ),
                )
                next_trial_id, secondary = await fixtures._seed_ready_trial(session, now=now)
            await _observe(session, primary, now + timedelta(seconds=1), quota=2)
            await _observe(session, secondary, now + timedelta(seconds=1), quota=2)
            first = await reserve_next_service_execution(
                session,
                environment="staging",
                pool_id="nebius-cpu",
                image_admission_keyring=IMAGE_ADMISSION_KEYRING,
                now=now + timedelta(seconds=2),
            )
            assert first is not None and first.target_id == primary.target_id
            trial = await session.get(Trial, trial_id)
            original_route = deepcopy(trial.execution_route_json)
            first_identity = (
                first.target_id,
                first.routing_generation,
                first.routing_decision_sha256,
            )
            await session.commit()
        actuator = ExecutionActuator(
            sessions=sessions,
            kubernetes=kube,
            target=ExecutionTargetRuntime(
                target_id=primary.target_id, namespace=primary.namespace_name
            ),
            controller_id="regional-retry",
        )
        assert await actuator.run_commands_once(now=now + timedelta(seconds=3)) == 1
        async with sessions() as session:
            plan = ExecutionRuntimePlanV1.model_validate(first.runtime_contract_json)
            old_step_context = AuthContext(
                token_hash=b"",
                type="step_session",
                scopes=["llm:call"],
                team_id=first.team_id,
                expires_at=first.deadline_at,
                trial_id=trial_id,
                step_id="agent",
                provider_connection_id=None,
                provider_connection_id_bound=True,
                step_jwt_id=uuid4(),
                service_execution_lease_id=first.id,
                service_execution_generation=1,
                service_execution_role="attempt",
                service_execution_runtime_contract_sha256=first.runtime_contract_sha256,
                service_execution_candidate_sha=plan.candidate_sha,
                service_execution_task_revision_sha256=plan.task_revision_sha256,
                service_execution_command_identity_sha256=plan.command_identity_sha256,
            )
            # These decoded step-token claims are initially a live authority.
            await authorize_trial_execution_dispatch(session, old_step_context)
        kube.jobs[first.job_name] = kube.jobs[first.job_name].model_copy(
            update={
                "normalized_state": NormalizedJobState.UNSCHEDULABLE,
                "resource_version": "2",
                "message": "Insufficient cpu",
                "started_at": now if boundary == "started" else None,
            }
        )
        await actuator.reconcile_full_once(now=first.deadline_at)
        if boundary == "started":
            async with sessions() as session:
                current = await session.get(ServiceExecutionLease, first.id)
                assert current.revoked_at is None and current.desired_state == "create"
                with pytest.raises(ServiceExecutionConflict, match="trial is not reservable"):
                    await fixtures._reserve(
                        session, trial_id=trial_id, target=secondary, now=first.deadline_at
                    )
            return
        after_cleanup = first.deadline_at + timedelta(seconds=302)
        async with sessions() as session:
            current = await session.get(ServiceExecutionLease, first.id)
            assert current.revoked_at is not None and current.cleanup_state == "pending"
            assert current.desired_state == "retry" and current.error_code == "unschedulable"
            with pytest.raises(ServiceExecutionConflict, match="cleanup is not complete"):
                await fixtures._reserve(
                    session,
                    trial_id=trial_id,
                    target=secondary,
                    now=first.deadline_at + timedelta(seconds=16),
                    requirements=WorkloadRequirementsV1.model_validate(
                        first.workload_requirements_json
                    ),
                    runtime_contract=ExecutionRuntimePlanV1.model_validate(
                        first.runtime_contract_json
                    ),
                )
            await session.rollback()
            trial = await session.get(Trial, trial_id)
            assert trial.execution_route_json == original_route
        assert kube.create_count == 1 and len(kube.jobs) == 1
        if boundary in {"other_failure", "bound_retry"}:
            async with sessions() as session:
                current = await session.get(ServiceExecutionLease, first.id)
                current.error_code = "evicted"
                await session.commit()
        await actuator.run_commands_once(now=after_cleanup)
        await actuator.reconcile_full_once(now=after_cleanup)
        assert kube.delete_count == 1 and not kube.jobs
        async with sessions() as session:
            previous = await session.get(ServiceExecutionLease, first.id)
            assert previous.cleanup_state == "complete" and previous.desired_state == "deleted"
            for model, column in (
                (ExecutionProvisioningAuthorization, ExecutionProvisioningAuthorization.lease_id),
                (ExecutionCostReservation, ExecutionCostReservation.lease_id),
                (ExecutionAdmissionReservation, ExecutionAdmissionReservation.owner_id),
            ):
                row = await session.scalar(select(model).where(column == first.id))
                assert row is not None and row.state == "released"
            for target in (primary, secondary):
                await set_execution_target_health(
                    session,
                    target_id=target.target_id,
                    desired_state="active",
                    observed_state="ready",
                    health_status="healthy",
                    observed_at=after_cleanup,
                )
            await _observe(session, primary, after_cleanup, quota=2 if boundary == "bound_retry" else 0)
            await _observe(session, secondary, after_cleanup, quota=2)
            if boundary == "bound_retry":
                # Placement preference can change between attempts. It must
                # still reach the original target without rebinding the route.
                for target, role in ((primary, "secondary"), (secondary, "primary")):
                    row = await session.get(ServiceExecutionTarget, target.target_id)
                    row.spec_json = {**row.spec_json, "health_role": role}
            if boundary == "other_failure":
                # A bound retry waiting for its region must not strand a later
                # unrelated trial that can use the other available region.
                await fixtures._configure_scheduler_trial(session, trial_id=next_trial_id, now=after_cleanup)
                next_trial = await session.get(Trial, next_trial_id)
                next_trial.submitted_at = after_cleanup
            if boundary == "wrong_environment":
                row = await session.get(ServiceExecutionTarget, secondary.target_id)
                row.environment = "production"
            await session.commit()
        async with sessions() as session:
            if boundary in ("requirements_drift", "wrong_residency"):
                change = (
                    {"memory_mib": 2048}
                    if boundary == "requirements_drift"
                    else {"data_residency": "us"}
                )
                requirements = WorkloadRequirementsV1.model_validate(
                    first.workload_requirements_json
                ).model_copy(update=change)
                with pytest.raises(ServiceExecutionConflict):
                    await fixtures._reserve(
                        session,
                        trial_id=trial_id,
                        target=secondary,
                        now=after_cleanup,
                        requirements=requirements,
                        runtime_contract=ExecutionRuntimePlanV1.model_validate(
                            first.runtime_contract_json
                        ),
                    )
                await session.rollback()
                recovered = None
            elif boundary == "other_failure":
                recovered = await reserve_next_service_execution(
                    session,
                    environment="staging",
                    pool_id="nebius-cpu",
                    image_admission_keyring=IMAGE_ADMISSION_KEYRING,
                    now=after_cleanup + timedelta(seconds=1),
                )
                assert recovered is not None and recovered.trial_id == next_trial_id
                assert recovered.target_id == secondary.target_id
                trial = await session.get(Trial, trial_id)
                assert trial.state == "queued" and trial.next_attempt_at > after_cleanup
                assert trial.execution_route_json == original_route and trial.attempt_count == 1
                assert await session.scalar(select(func.count()).select_from(ServiceExecutionLease)
                    .where(ServiceExecutionLease.trial_id == trial_id)) == 1
                await session.commit()
                return
            else:
                recovered = await reserve_next_service_execution(
                    session,
                    environment="staging",
                    pool_id="nebius-cpu",
                    image_admission_keyring=IMAGE_ADMISSION_KEYRING,
                    now=after_cleanup + timedelta(seconds=1),
                )
                await session.commit()
            if boundary == "bound_retry":
                assert recovered is not None and recovered.target_id == primary.target_id
                assert recovered.attempt == 2
                trial = await session.get(Trial, trial_id)
                assert trial.execution_route_json == original_route
                assert await session.scalar(select(func.count()).select_from(ServiceExecutionLease)
                    .where(ServiceExecutionLease.trial_id == trial_id)) == 2
                return
            if boundary != "eligible":
                assert recovered is None
                trial = await session.get(Trial, trial_id)
                assert trial.execution_route_json == original_route and trial.attempt_count == 1
                return
            assert recovered is not None, "eligible retry must select the second region"
            assert recovered.target_id == secondary.target_id and recovered.attempt == 2
            assert recovered.routing_generation == first.routing_generation + 1
            previous = await session.get(ServiceExecutionLease, first.id)
            assert (
                previous.target_id,
                previous.routing_generation,
                previous.routing_decision_sha256,
            ) == first_identity
            with pytest.raises(ServiceExecutionFenceError, match="not authoritative"):
                await verify_trial_execution_fence(
                    session,
                    trial_id=trial_id,
                    lease_id=first.id,
                    generation=1,
                    surface="regional-retry",
                )
            with pytest.raises(HTTPException) as denied:
                await authorize_trial_execution_dispatch(session, old_step_context)
            assert denied.value.status_code == 403
            assert (
                await session.scalar(
                    select(func.count())
                    .select_from(ServiceExecutionLease)
                    .where(ServiceExecutionLease.trial_id == trial_id)
                )
                == 2
            )
            assert (
                await session.scalar(
                    select(func.count())
                    .select_from(ExecutionAdmissionReservation)
                    .where(
                        ExecutionAdmissionReservation.trial_id == trial_id,
                        ExecutionAdmissionReservation.state == "active",
                    )
                )
                == 1
            )
            assert (
                await session.scalar(
                    select(func.count())
                    .select_from(ExecutionCostReservation)
                    .where(
                        ExecutionCostReservation.trial_id == trial_id,
                        ExecutionCostReservation.state == "reserved",
                    )
                )
                == 1
            )
        second_kube = fixtures._FakeKubernetesJobApi()
        second_actuator = ExecutionActuator(
            sessions=sessions,
            kubernetes=second_kube,
            target=ExecutionTargetRuntime(
                target_id=secondary.target_id, namespace=secondary.namespace_name
            ),
            controller_id="regional-retry-west",
        )
        assert (
            await second_actuator.run_commands_once(now=after_cleanup + timedelta(seconds=2)) == 1
        )
        assert (
            await second_actuator.run_commands_once(now=after_cleanup + timedelta(seconds=3)) == 0
        )
        assert second_kube.create_count == 1 and len(second_kube.jobs) == 1 and not kube.jobs
    finally:
        await engine.dispose()
