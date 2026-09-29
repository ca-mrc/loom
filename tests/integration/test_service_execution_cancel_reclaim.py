"""Legacy worker reclaim cannot reopen service-execution cancellation."""

from copy import deepcopy
from datetime import UTC, datetime, timedelta

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.admin_secret import AdminSecretVerifier
from loom.db.schema import (
    ExecutionAdmissionReservation,
    ExecutionCostReservation,
    ExecutionProvisioningAuthorization,
    ServiceExecutionLease,
    TeamQuota,
    Trial,
)
from loom_control_plane.routes.trials import router
from loom_control_plane.scheduler.crash_detector import reclaim_expired_workers
from loom_control_plane.service_execution_scheduler import _NEXT_SERVICE_TRIAL
from loom_control_plane.trial_cancellation import cancel_trial_under_authority
from loom_execution_actuator.controller import ExecutionActuator
from loom_execution_actuator.renderer import ExecutionTargetRuntime
from tests.integration import test_service_execution_leases as fixtures
from tests.integration.test_service_execution_leases import (
    _cleanup_service_execution_test_rows,  # noqa: F401
)


@pytest.mark.parametrize("cancel_requested", [False, True])
async def test_stale_claim_reclaim_preserves_service_execution_owner(
    postgres_url: str,
    cancel_requested: bool,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    old = datetime.now(UTC) - timedelta(minutes=10)
    try:
        async with sessions() as session:
            trial_id, target = await fixtures._seed_ready_trial(session, now=old)
            await fixtures._configure_scheduler_trial(session, trial_id=trial_id, now=old)
            lease = await fixtures._reserve(session, trial_id=trial_id, target=target, now=old)
            legacy_id, _ = await fixtures._seed_ready_trial(session, now=old)
            legacy = await session.get(Trial, legacy_id)
            assert legacy is not None
            legacy.state, legacy.claimed_at = "claimed", old
            await session.commit()
        if cancel_requested:
            assert (
                await cancel_trial_under_authority(
                    session_factory=sessions,
                    trial_id=trial_id,
                    team_id=None,
                )
                is not None
            )
        async with sessions() as session:
            assert (
                await reclaim_expired_workers(
                    session,
                    expiry_sec=30,
                    claimed_without_start_expiry_sec=60,
                )
                == 1
            )
            await session.commit()
        async with sessions() as session:
            trial = await session.get(Trial, trial_id)
            quota = await session.get(TeamQuota, lease.team_id)
            legacy = await session.get(Trial, legacy_id)
            assert trial is not None and trial.state == "claimed"
            assert trial.failure_reason is None and trial.worker_id is None
            assert bool(trial.cancellation_requested_at) == cancel_requested
            assert quota is not None and quota.in_flight_count == 1
            assert legacy is not None and legacy.state == "queued"
            assert legacy.failure_reason == "worker_lost_claim"
    finally:
        await engine.dispose()


@pytest.mark.parametrize("cleanup_before_replay", [True, False])
async def test_normal_cancel_replay_repairs_queued_cancel_with_deleted_lease(
    postgres_url: str,
    cleanup_before_replay: bool,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    token = "loom_admin_" + "e" * 64
    app = FastAPI()
    app.state.session_factory = sessions
    app.state.admin_secret_verifier = AdminSecretVerifier.from_token(token)
    app.include_router(router)
    kube = fixtures._FakeKubernetesJobApi()
    try:
        async with sessions() as session:
            trial_id, target = await fixtures._seed_ready_trial(session, now=now)
            await fixtures._configure_scheduler_trial(session, trial_id=trial_id, now=now)
            lease = await fixtures._reserve(session, trial_id=trial_id, target=target, now=now)
            await fixtures.reserve_execution_provisioning(session, lease_id=lease.id, now=now)
            await session.commit()
        assert (
            await cancel_trial_under_authority(
                session_factory=sessions,
                trial_id=trial_id,
                team_id=None,
            )
            is not None
        )
        async with sessions() as session:
            trial = await session.get(Trial, trial_id)
            assert trial is not None
            requested_at = trial.cancellation_requested_at
            # Historical state left by the legacy stale-claim sweep before this fix.
            trial.state, trial.failure_reason = "queued", "worker_lost_claim"
            await session.commit()
        actuator = ExecutionActuator(
            sessions=sessions,
            kubernetes=kube,
            target=ExecutionTargetRuntime(
                target_id=target.target_id, namespace=target.namespace_name
            ),
            controller_id="historical-cleanup",
        )
        if cleanup_before_replay:
            assert await actuator.reconcile_full_once(now=now + timedelta(seconds=2)) == 1
        async with sessions() as session:
            old_lease = await session.get(ServiceExecutionLease, lease.id)
            assert old_lease is not None
            assert old_lease.cleanup_state == ("complete" if cleanup_before_replay else "pending")
            assert old_lease.job_uid is None and old_lease.pod_uid is None
            before = deepcopy(
                {c.key: getattr(old_lease, c.key) for c in old_lease.__table__.columns}
            )
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://cp.test"
        ) as client:
            for _ in range(2):
                response = await client.post(
                    f"/trials/{trial_id}/cancel", headers={"Authorization": "Bearer " + token}
                )
                assert response.status_code == 200, response.text
                assert response.json()["state"] == "cancelled"
        async with sessions() as session:
            trial = await session.get(Trial, trial_id)
            assert trial is not None and trial.state == "cancelled"
            assert trial.cancellation_requested_at == requested_at
            assert trial.cancellation_observed_at is not None and trial.finished_at is not None
            quota = await session.get(TeamQuota, lease.team_id)
            assert quota is not None and quota.in_flight_count == 0
            old_lease = await session.get(ServiceExecutionLease, lease.id)
            assert old_lease is not None
            assert {c.key: getattr(old_lease, c.key) for c in old_lease.__table__.columns} == before
            for model, key in (
                (ExecutionAdmissionReservation, ExecutionAdmissionReservation.owner_id),
                (ExecutionProvisioningAuthorization, ExecutionProvisioningAuthorization.lease_id),
                (ExecutionCostReservation, ExecutionCostReservation.lease_id),
            ):
                reservation = (
                    await session.execute(select(model).where(key == lease.id))
                ).scalar_one()
                assert (reservation.released_at is not None) == cleanup_before_replay
        if not cleanup_before_replay:
            assert await actuator.reconcile_full_once(now=now + timedelta(seconds=3)) == 1
            async with sessions() as session:
                for model, key in (
                    (ExecutionAdmissionReservation, ExecutionAdmissionReservation.owner_id),
                    (
                        ExecutionProvisioningAuthorization,
                        ExecutionProvisioningAuthorization.lease_id,
                    ),
                    (ExecutionCostReservation, ExecutionCostReservation.lease_id),
                ):
                    reservation = (
                        await session.execute(select(model).where(key == lease.id))
                    ).scalar_one()
                    assert reservation.state == "released" and reservation.released_at is not None
        assert kube.create_count == kube.delete_count == 0
    finally:
        await engine.dispose()


async def test_native_cleanup_acknowledges_cancel_without_client_replay(postgres_url: str) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    try:
        async with sessions() as session:
            trial_id, target = await fixtures._seed_ready_trial(session, now=now)
            await fixtures._configure_scheduler_trial(session, trial_id=trial_id, now=now)
            lease = await fixtures._reserve(session, trial_id=trial_id, target=target, now=now)
            await session.commit()
        await cancel_trial_under_authority(
            session_factory=sessions, trial_id=trial_id, team_id=lease.team_id,
        )
        async with sessions() as session:
            trial = await session.get(Trial, trial_id)
            assert trial is not None and trial.cancellation_requested_at is not None
            assert trial.cancellation_observed_at is None
            requested_at = trial.cancellation_requested_at
        actuator = ExecutionActuator(
            sessions=sessions, kubernetes=fixtures._FakeKubernetesJobApi(),
            target=ExecutionTargetRuntime(target_id=target.target_id, namespace=target.namespace_name),
            controller_id="cancel-ack-test",
        )
        await actuator.reconcile_full_once(now=now + timedelta(seconds=5))
        async with sessions() as session:
            trial = await session.get(Trial, trial_id)
            current = await session.get(ServiceExecutionLease, lease.id)
            assert current is not None and current.cleanup_state == "complete"
            assert trial is not None and trial.cancellation_requested_at == requested_at
            assert trial.cancellation_observed_at == current.deleted_at
            assert trial.cancellation_observed_at is not None
            acknowledged_at = trial.cancellation_observed_at
        await actuator.reconcile_full_once(now=now + timedelta(seconds=8))
        async with sessions() as session:
            trial = await session.get(Trial, trial_id)
            assert trial is not None and trial.cancellation_observed_at == acknowledged_at
    finally:
        await engine.dispose()


async def test_service_scheduler_skips_cancel_requested_queue(postgres_url: str) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    try:
        async with sessions() as session:
            cancelled_id, _ = await fixtures._seed_ready_trial(session, now=now)
            await fixtures._configure_scheduler_trial(session, trial_id=cancelled_id, now=now)
            trial = await session.get(Trial, cancelled_id)
            assert trial is not None
            trial.cancellation_requested_at = now
            await session.commit()
        async with sessions() as session:
            assert (
                await session.execute(_NEXT_SERVICE_TRIAL, {"pool_id": "nebius-cpu", "now": now})
            ).first() is None
            queued_id, _ = await fixtures._seed_ready_trial(session, now=now)
            await fixtures._configure_scheduler_trial(session, trial_id=queued_id, now=now)
            await session.flush()
            selected = (
                await session.execute(_NEXT_SERVICE_TRIAL, {"pool_id": "nebius-cpu", "now": now})
            ).first()
            assert selected is not None and selected.id == queued_id
            await session.commit()
    finally:
        await engine.dispose()
