"""Each actuator consumes only its target's transactional command outbox."""

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import (
    ExecutionAdmissionReservation,
    ExecutionCostReservation,
    ExecutionProvisioningAuthorization,
    ServiceExecutionCommand,
    ServiceExecutionLease,
    TeamQuota,
    Trial,
)
from loom_execution_actuator.contracts import KubernetesJobInventory
from loom_execution_actuator.controller import ExecutionActuator
from loom_execution_actuator.renderer import ExecutionTargetRuntime
from tests.integration import test_service_execution_leases as fixtures
from tests.integration.test_service_execution_leases import (
    _cleanup_service_execution_test_rows,  # noqa: F401
)


async def test_actuator_leaves_other_target_commands_and_leases_untouched(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    try:
        async with sessions() as session:
            trial_a, target_a = await fixtures._seed_ready_trial(session, now=now)
            lease_a = await fixtures._reserve(session, trial_id=trial_a, target=target_a, now=now)
            trial_b, target_b = await fixtures._seed_ready_trial(session, now=now)
            lease_b = await fixtures._reserve(session, trial_id=trial_b, target=target_b, now=now)
            await session.commit()
        kube_a, kube_b = fixtures._FakeKubernetesJobApi(), fixtures._FakeKubernetesJobApi()
        actuator_b = ExecutionActuator(
            sessions=sessions,
            kubernetes=kube_b,
            target=ExecutionTargetRuntime(
                target_id=target_b.target_id, namespace=target_b.namespace_name
            ),
            controller_id="target-b",
        )
        assert await actuator_b.run_commands_once(now=now + timedelta(seconds=1)) == 1
        assert set(kube_b.jobs) == {lease_b.job_name}
        async with sessions() as session:
            untouched = await session.get(ServiceExecutionLease, lease_a.id)
            command = (
                await session.execute(
                    select(ServiceExecutionCommand).where(
                        ServiceExecutionCommand.lease_id == lease_a.id
                    )
                )
            ).scalar_one()
            assert untouched is not None
            assert untouched.revoked_at is None and untouched.desired_state == "create"
            assert untouched.job_uid is None
            assert (command.state, command.claimed_by, command.delivery_count) == (
                "pending",
                None,
                0,
            )
        actuator_a = ExecutionActuator(
            sessions=sessions,
            kubernetes=kube_a,
            target=ExecutionTargetRuntime(
                target_id=target_a.target_id, namespace=target_a.namespace_name
            ),
            controller_id="target-a",
        )
        assert await actuator_a.run_commands_once(now=now + timedelta(seconds=2)) == 1
        assert set(kube_a.jobs) == {lease_a.job_name}
        assert await actuator_a.run_commands_once(now=now + timedelta(seconds=3)) == 0
        assert await actuator_b.run_commands_once(now=now + timedelta(seconds=3)) == 0
        assert kube_a.create_count == kube_b.create_count == 1
    finally:
        await engine.dispose()


async def test_target_claims_preserve_skip_locked_and_expired_redelivery(postgres_url: str) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    try:
        async with sessions() as session:
            trial_a, target_a = await fixtures._seed_ready_trial(session, now=now)
            lease_a = await fixtures._reserve(session, trial_id=trial_a, target=target_a, now=now)
            trial_b, target_b = await fixtures._seed_ready_trial(session, now=now)
            lease_b = await fixtures._reserve(session, trial_id=trial_b, target=target_b, now=now)
            await session.commit()
        async with sessions() as first, sessions() as second:
            claimed_a = await fixtures.claim_execution_commands(
                first,
                consumer_id="a",
                target_id=target_a.target_id,
                limit=1,
                lease_seconds=5,
                now=now,
            )
            # A competing same-target consumer skips the transaction-held row.
            assert (
                await fixtures.claim_execution_commands(
                    second,
                    consumer_id="competing-a",
                    target_id=target_a.target_id,
                    limit=1,
                    lease_seconds=5,
                    now=now,
                )
                == ()
            )
            claimed_b = await fixtures.claim_execution_commands(
                second,
                consumer_id="b",
                target_id=target_b.target_id,
                limit=1,
                lease_seconds=5,
                now=now,
            )
            assert [c.lease_id for c in claimed_a] == [lease_a.id]
            assert [c.lease_id for c in claimed_b] == [lease_b.id]
            await first.commit()
            await second.commit()
        async with sessions() as session:
            assert (
                await fixtures.claim_execution_commands(
                    session,
                    consumer_id="early-a",
                    target_id=target_a.target_id,
                    limit=100,
                    lease_seconds=5,
                    now=now + timedelta(seconds=4),
                )
                == ()
            )
            replay = await fixtures.claim_execution_commands(
                session,
                consumer_id="new-a",
                target_id=target_a.target_id,
                limit=100,
                lease_seconds=5,
                now=now + timedelta(seconds=6),
            )
            assert len(replay) == 1 and replay[0].id == claimed_a[0].id
            assert replay[0].delivery_count == 2
            b = await session.get(ServiceExecutionCommand, claimed_b[0].id)
            assert b is not None and b.claimed_by == "b" and b.delivery_count == 1
            await session.commit()
    finally:
        await engine.dispose()


async def test_admin_claim_requires_target_and_preserves_admin_authority(postgres_url: str) -> None:
    import httpx
    from fastapi import FastAPI

    from loom.admin_secret import AdminSecretVerifier
    from loom_control_plane.routes.service_executions import router

    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    app = FastAPI()
    app.state.session_factory = sessions
    token = "loom_admin_" + "e" * 64
    app.state.admin_secret_verifier = AdminSecretVerifier.from_token(token)
    app.include_router(router)
    now = datetime.now(UTC)
    try:
        async with sessions() as session:
            trial_a, target_a = await fixtures._seed_ready_trial(session, now=now)
            lease_a = await fixtures._reserve(session, trial_id=trial_a, target=target_a, now=now)
            trial_b, target_b = await fixtures._seed_ready_trial(session, now=now)
            lease_b = await fixtures._reserve(session, trial_id=trial_b, target=target_b, now=now)
            await session.commit()
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            url = "/admin/service-execution/commands/claim"
            body = {"consumer_id": "admin-test", "target_id": target_b.target_id}
            assert (await client.post(url, json=body)).status_code == 401
            headers = {"Authorization": "Bearer " + token}
            for invalid in ({"consumer_id": "admin-test"}, {**body, "target_id": ""}):
                assert (await client.post(url, json=invalid, headers=headers)).status_code == 422
            response = await client.post(url, json=body, headers=headers)
            assert response.status_code == 200, response.text
            assert [c["lease_id"] for c in response.json()["items"]] == [str(lease_b.id)]
        async with sessions() as session:
            a = (
                await session.execute(
                    select(ServiceExecutionCommand).where(
                        ServiceExecutionCommand.lease_id == lease_a.id
                    )
                )
            ).scalar_one()
            assert (a.state, a.delivery_count) == ("pending", 0)
    finally:
        await engine.dispose()


@pytest.mark.parametrize("trial_boundary", ["claimed", "running", "succeeded", "new_attempt"])
async def test_reconcile_completes_precreate_cancel_after_dead_letter(
    postgres_url: str, monkeypatch: pytest.MonkeyPatch, trial_boundary: str
) -> None:
    from loom_control_plane.service_execution import defer_execution_command

    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    kube = fixtures._FakeKubernetesJobApi()
    try:
        async with sessions() as session:
            trial_id, target = await fixtures._seed_ready_trial(session, now=now)
            trial = await session.get(Trial, trial_id)
            assert trial is not None
            team_id = trial.team_id
            session.add(TeamQuota(team_id=team_id))
            await session.flush()
            lease = await fixtures._reserve(session, trial_id=trial_id, target=target, now=now)
            provisioning = await fixtures.reserve_execution_provisioning(
                session, lease_id=lease.id, now=now
            )
            assert provisioning is not None and provisioning.state == "authorized"
            await session.commit()
        actuator = ExecutionActuator(
            sessions=sessions,
            kubernetes=kube,
            target=ExecutionTargetRuntime(
                target_id=target.target_id, namespace=target.namespace_name
            ),
            controller_id="cancel-recovery",
        )
        # An unstarted active create must survive an empty authoritative list.
        assert await actuator.reconcile_full_once(now=now + timedelta(seconds=1)) == 0
        async with sessions() as session:
            active = await session.get(ServiceExecutionLease, lease.id)
            assert active is not None and active.observed_state == "reserved"
            quota = await session.get(TeamQuota, team_id)
            assert quota is not None and quota.in_flight_count == 1
            trial = await session.get(Trial, trial_id)
            assert trial is not None
            trial.result = {"preserved": "original-result"}
            if trial_boundary == "new_attempt":
                trial.attempt_count += 1
            elif trial_boundary != "claimed":
                trial.state = trial_boundary
            if trial_boundary == "succeeded":
                trial.finished_at = now + timedelta(seconds=1)
                active.finalized_at = now + timedelta(seconds=1)
                active.observed_state = "finalized"
            await fixtures.enqueue_execution_transition(
                session,
                lease_id=lease.id,
                expected_generation=1,
                desired_state="cancel",
                now=now + timedelta(seconds=2),
            )
            claimed = await fixtures.claim_execution_commands(
                session,
                consumer_id="failed-consumer",
                target_id=target.target_id,
                limit=100,
                lease_seconds=5,
                now=now + timedelta(seconds=2),
            )
            for command in claimed:
                await defer_execution_command(
                    session,
                    command_id=command.id,
                    consumer_id="failed-consumer",
                    error_code="contract_error",
                    error_message="previous wrong-target consumer",
                    retry_after_seconds=1,
                    max_deliveries=1,
                    now=now + timedelta(seconds=2),
                )
            await session.commit()
        # A rejected/malformed listed Job must not be treated as proof of absence.
        with monkeypatch.context() as patch:
            patch.setattr(
                kube,
                "list_jobs",
                AsyncMock(return_value=KubernetesJobInventory((), rejected_count=1)),
            )
            assert await actuator.reconcile_full_once(now=now + timedelta(seconds=3)) == 1
            async with sessions() as session:
                pending = await session.get(ServiceExecutionLease, lease.id)
                assert pending is not None and pending.cleanup_state == "pending"
        assert await actuator.reconcile_full_once(now=now + timedelta(seconds=3)) == 1
        async with sessions() as session:
            closed = await session.get(ServiceExecutionLease, lease.id)
            assert closed is not None
            assert closed.job_uid is None and closed.pod_uid is None
            assert (closed.desired_state, closed.observed_state, closed.cleanup_state) == (
                "deleted",
                "deleted",
                "complete",
            )
            assert closed.output_commit_state == "unavailable"
            assert closed.output_unavailable_reason == "operator_cancelled"
            trial = await session.get(Trial, trial_id)
            assert trial is not None and trial.result == {"preserved": "original-result"}
            if trial_boundary in {"claimed", "running"}:
                assert trial.state == "cancelled"
                assert trial.finished_at == now + timedelta(seconds=3)
                assert closed.finalized_at == trial.finished_at
            elif trial_boundary == "succeeded":
                assert trial.state == "succeeded"
                assert trial.finished_at == closed.finalized_at == now + timedelta(seconds=1)
            else:
                assert trial.state == "claimed" and trial.attempt_count == lease.attempt + 1
                assert trial.finished_at is None and closed.finalized_at is None
            quota = await session.get(TeamQuota, team_id)
            assert quota is not None and quota.in_flight_count == int(
                trial_boundary == "new_attempt"
            )
            for model, key in (
                (ExecutionAdmissionReservation, ExecutionAdmissionReservation.owner_id),
                (ExecutionProvisioningAuthorization, ExecutionProvisioningAuthorization.lease_id),
                (ExecutionCostReservation, ExecutionCostReservation.lease_id),
            ):
                row = (await session.execute(select(model).where(key == lease.id))).scalar_one()
                assert row.state == "released"
                assert row.released_at is not None
        assert await actuator.reconcile_full_once(now=now + timedelta(seconds=4)) == 0
        assert kube.create_count == kube.delete_count == 0
    finally:
        await engine.dispose()


async def test_sibling_actuators_preserve_foreign_jobs_without_false_orphan_drift(postgres_url):
    from uuid import uuid4

    from tests.integration.test_execution_shared_capacity import _family, _guest_reserve

    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    try:
        async with sessions() as session, session.begin():
            ordinary, guest, _ = await _family(session, now)
            native_lease = await fixtures._reserve(session, trial_id=ordinary[0], target=ordinary[1],
                                                   now=now + timedelta(seconds=2))
            guest_lease = await _guest_reserve(session, guest, now + timedelta(seconds=2))
        kube = fixtures._FakeKubernetesJobApi()
        actuators = [ExecutionActuator(
            sessions=sessions, kubernetes=kube,
            target=ExecutionTargetRuntime(target_id=pair[1].target_id, namespace=pair[1].namespace_name),
            controller_id=pair[1].target_id,
        ) for pair in (ordinary, guest)]
        for actuator in actuators:
            assert await actuator.run_commands_once(now=now + timedelta(seconds=3)) == 1
        assert set(kube.jobs) == {native_lease.job_name, guest_lease.job_name}
        before = dict(kube.jobs)
        for actuator in actuators:
            assert await actuator.reconcile_full_once(now=now + timedelta(seconds=4)) == 0
            assert kube.jobs == before and kube.delete_count == 0
        # Unregistered target annotations are still visible drift, never treated
        # as a legitimate sibling or deleted by another target's reconciler.
        foreign = next(iter(kube.jobs.values())).model_copy(update={
            "target_id": "unknown-target", "lease_id": str(uuid4()), "job_name": "foreign-job",
        })
        kube.jobs[foreign.job_name] = foreign
        for actuator in actuators:
            assert await actuator.reconcile_full_once(now=now + timedelta(seconds=5)) == 1
        assert kube.jobs[foreign.job_name] == foreign and kube.delete_count == 0
    finally:
        await engine.dispose()
