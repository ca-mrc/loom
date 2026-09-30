"""Real execution claims, management HTTP and retained activation/cancellation."""
from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta

import httpx
import pytest
from sqlalchemy import func, select, text, update

from loom.db.nebius_pool_outbox_schema import NebiusPoolExecutionOutbox
from loom.db.nebius_pool_schema import NebiusPoolBinding, NebiusPoolRequest
from loom.db.schema import ExecutionAdmissionReservation, ExecutionCostReservation, ServiceExecutionLease, Trial
from loom.execution_contract import nebius_cpu_execution_class
from loom.pipeline.keys import canonical_digest
from loom_execution_actuator.renderer import ExecutionTargetRuntime
from loom_execution_capacity_collector.contracts import CapacityPlacement
from tests.execution_placement_fixtures import placement_fixture
from tests.integration.test_nebius_pool_auth import credential
from tests.integration.test_nebius_pool_execution_outbox import assert_unclaimed, grant, setup
from tests.integration.test_nebius_pool_observation_registry import sessions as sessions
from tests.integration.test_nebius_pool_participant_http import client
from tests.integration.test_nebius_pool_registry import machine, publish_placement
from tests.support.execution_image_admission import IMAGE_ADMISSION_KEYRING


async def selected(sessions, tmp_path, *, occupied_cpu=0):
    from loom_service.app import create_app
    from loom_service.config import LoomServiceSettings
    from loom_service.pool_management.registry import PoolProfiles
    from loom_service.pool_management.render import PoolExecutionProfile

    journal, trial_id, target = await setup(sessions)
    journal.participant = journal.participant.model_copy(update={"admission_epoch": 1})
    participant = journal.participant
    proposed = await journal.propose(trial_id=trial_id, target_id=target.target_id)
    raw, pool_id, _, _ = await credential(sessions, role="participant", participant_config=participant)
    placement = CapacityPlacement.model_validate(placement_fixture(target_id="group-1",
        node_cpu=3000, node_memory=8192, node_storage=32768, requested_cpu=occupied_cpu, quota_nodes=1, used_nodes=1))
    selector = {"nebius.com/node-group-id": "group-1"}
    binding = {"node_selector": selector, "admission": {"observation_max_age_seconds": 120,
        "max_create_per_minute": 10, "max_pending_jobs": 10, "max_unschedulable_jobs": 0,
        "max_image_pull_backoff_jobs": 0, "build_concurrency_limit": 2},
        "quota_identities": {name: [quota.parent_id, quota.region, quota.service, quota.name, quota.unit]
            for name, quota in placement.quota_resources.items()}}
    async with sessions.begin() as session:
        await session.execute(update(NebiusPoolBinding).where(NebiusPoolBinding.pool_id == pool_id).values(
            policy_revision=2, binding_json=binding, binding_sha256=canonical_digest(binding).removeprefix("sha256:")))
    observer = await machine(sessions, pool_id)
    await publish_placement(sessions, observer, placement)
    runtime = proposed.request.execution.runtime
    profile = PoolExecutionProfile(profile_id=participant.targets[0].profile_id,
        runtime=ExecutionTargetRuntime(target_id=target.target_id, namespace=target.namespace_name, node_selector=selector),
        candidate_sha=runtime.candidate_sha, execution_class_id=runtime.execution_class_id,
        runtime_image_ref=runtime.runtime_image_ref, runtime_binary_sha256=runtime.runtime_binary_sha256,
        execution_class=nebius_cpu_execution_class(), image_admission_keyring=IMAGE_ADMISSION_KEYRING)
    app = create_app(LoomServiceSettings(_env_file=None, service_mode="management",
        db_url="postgresql+asyncpg://unused:unused@localhost/unused"))
    app.state.session_factory, app.state.pool_profiles = sessions, PoolProfiles({profile.profile_id: profile}, {})
    token = tmp_path / "pool-execution-token"
    token.write_text(raw)
    token.chmod(0o600)
    return journal, trial_id, proposed, app, token


@pytest.mark.parametrize("lost", [None, "prepare", "activate"])
async def test_real_execution_recovers_committed_grant_and_activation_after_lost_reply(sessions, tmp_path, lost):
    from loom_execution_actuator.pool_client import PoolRequestUnconfirmedError
    from loom_execution_actuator.pool_execution_driver import PoolExecutionDriver

    journal, trial_id, proposed, app, token = await selected(sessions, tmp_path)
    calls = []

    class Boundary(httpx.AsyncBaseTransport):
        inner = httpx.ASGITransport(app=app)
        dropped = False

        async def handle_async_request(self, incoming):
            operation = incoming.url.path.rsplit("/", 1)[-1]
            calls.append(operation)
            async with sessions() as reader:
                row = await reader.get(NebiusPoolExecutionOutbox, proposed.request.key.local_work_id)
                if operation == "activate":
                    assert row.phase == "activation_pending" and row.activation_json is not None
                    assert await reader.get(ServiceExecutionLease, row.attached_lease_id) is not None
            response = await self.inner.handle_async_request(incoming)
            if operation == lost and not self.dropped:
                self.dropped = True
                assert response.status_code == 200
                await response.aclose()
                raise httpx.ReadError("committed response lost")
            return response

    async with httpx.AsyncClient(transport=Boundary()) as http:
        driver = PoolExecutionDriver(outbox=journal, management=client(http, token))
        if lost:
            with pytest.raises(PoolRequestUnconfirmedError):
                await driver.advance(proposed.request.key)
        recovered = PoolExecutionDriver(outbox=journal, management=client(http, token))
        active = await recovered.advance(proposed.request.key)
        assert active.phase == "active" and active.activated.phase == "create_intent"
        assert active.lease_id == proposed.request.key.local_work_id
        assert await recovered.advance(proposed.request.key) == active
    assert calls.count("activate") == 1
    async with sessions() as session:
        assert (await session.get(Trial, trial_id)).attempt_count == 1
        assert await session.scalar(select(func.count()).select_from(ServiceExecutionLease)) == 1
        retained = await session.get(NebiusPoolRequest, active.reservation_id)
        lease = await session.get(ServiceExecutionLease, active.lease_id)
        assert retained.plan_json["job"]["metadata"]["name"] == lease.job_name
        assert retained.request_json == active.request.model_dump(mode="json")


async def test_waiting_withdrawal_cancels_without_attempt_or_local_budget(sessions, tmp_path):
    from loom_execution_actuator.pool_execution_driver import PoolExecutionDriver

    journal, trial_id, proposed, app, token = await selected(sessions, tmp_path, occupied_cpu=3000)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app)) as http:
        driver = PoolExecutionDriver(outbox=journal, management=client(http, token))
        assert (await driver.advance(proposed.request.key)).phase == "selected"
        await assert_unclaimed(sessions, trial_id)
        async with sessions.begin() as session:
            await session.execute(update(Trial).where(Trial.id == trial_id).values(cancellation_requested_at=datetime.now(UTC)))
        assert (await driver.advance(proposed.request.key)).phase == "cancelled"
        await assert_unclaimed(sessions, trial_id)


@pytest.mark.parametrize("damage", ["cancel", "rollout", "participant"])
async def test_attached_selection_loses_activation_authority_without_freeing_capacity(sessions, damage):
    journal, trial_id, target = await setup(sessions)
    proposed = await journal.propose(trial_id=trial_id, target_id=target.target_id)
    reserved = grant(proposed)
    await journal.accept_grant(proposed.request.key, reserved)
    if damage == "participant":
        journal.participant = journal.participant.model_copy(update={"admission_epoch": 99})
    else:
        async with sessions.begin() as session:
            if damage == "cancel":
                await session.execute(update(Trial).where(Trial.id == trial_id).values(cancellation_requested_at=datetime.now(UTC)))
            else:
                await session.execute(text("INSERT INTO nebius_rollout_guard(id,owner,candidate_sha) VALUES(1,'test',:sha)"),
                    {"sha": "a" * 40})
    pending = await journal.begin_activation(proposed.request.key)
    assert pending.phase == "cancel_pending" and pending.activation is None
    assert pending.reservation_id == reserved.reservation_id and pending.lease_id is not None
    async with sessions() as session:
        assert (await session.get(ServiceExecutionLease, pending.lease_id)).cleanup_state == "not_requested"
        assert (await session.scalar(select(ExecutionCostReservation))).state == "reserved"


async def test_expired_activation_consent_cannot_be_renewed(sessions, monkeypatch):
    from loom_execution_actuator import pool_execution_outbox as module

    journal, trial_id, target = await setup(sessions)
    proposed = await journal.propose(trial_id=trial_id, target_id=target.target_id)
    await journal.accept_grant(proposed.request.key, grant(proposed))
    pending = await journal.begin_activation(proposed.request.key)
    assert pending.activation.not_after <= datetime.now(UTC) + timedelta(seconds=30)

    async def later(_session):
        return pending.activation.not_after + timedelta(seconds=1)

    monkeypatch.setattr(module, "_clock", later)
    expired = await journal.begin_activation(proposed.request.key)
    assert expired.phase == "cancel_pending" and expired.activation == pending.activation


async def test_late_accepted_activation_after_cancel_requires_stop_not_unstarted_release(sessions):
    journal, trial_id, target = await setup(sessions)
    proposed = await journal.propose(trial_id=trial_id, target_id=target.target_id)
    reserved = grant(proposed)
    await journal.accept_grant(proposed.request.key, reserved)
    pending = await journal.begin_activation(proposed.request.key)
    await journal.request_cancel(proposed.request.key)
    activated = reserved.model_copy(update={"phase": "create_intent", "plan_sha256": "d" * 64})
    result = await journal.confirm_activation(proposed.request.key, activated)
    assert result.phase == "stop_pending" and result.activation == pending.activation
    assert result.activated == activated
    with pytest.raises(ValueError):
        await journal.confirm_cancel(proposed.request.key, reserved.model_copy(update={"phase": "cancelled_unstarted"}))
    async with sessions() as session:
        assert (await session.get(ServiceExecutionLease, result.lease_id)).cleanup_state == "not_requested"


async def test_confirmed_never_started_grant_releases_local_cost_and_claim_without_erasing_lease(sessions, tmp_path):
    from loom_execution_actuator.pool_execution_driver import PoolExecutionDriver

    journal, trial_id, proposed, app, token = await selected(sessions, tmp_path)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app)) as http:
        management = client(http, token)
        reserved = await management.prepare(proposed.request)
        attached = await journal.accept_grant(proposed.request.key, reserved)
        await journal.request_cancel(proposed.request.key)
        cancelled = await PoolExecutionDriver(outbox=journal, management=management).advance(proposed.request.key)
        assert cancelled.phase == "cancelled" and cancelled.lease_id == attached.lease_id
        assert await journal.confirm_cancel(proposed.request.key, await management.status(proposed.action)) == cancelled
    async with sessions() as session:
        lease = await session.get(ServiceExecutionLease, attached.lease_id)
        assert lease.cleanup_state == "complete" and lease.desired_state == "deleted"
        assert lease.output_commit_state == "unavailable" and lease.job_uid is None and lease.pod_uid is None
        assert (await session.scalar(select(ExecutionCostReservation))).state == "released"
        assert (await session.scalar(select(ExecutionAdmissionReservation))).state == "released"
        trial = await session.get(Trial, trial_id)
        assert trial.state == "queued" and trial.attempt_count == 1
    next_proposal = await journal.propose(trial_id=trial_id, target_id=proposed.request.target_id)
    assert next_proposal.request.key.local_work_id != proposed.request.key.local_work_id


@pytest.mark.parametrize("damage", ["unattached", "foreign-plan", "foreign-grant"])
async def test_activation_receipt_cannot_substitute_local_claim_or_grant(sessions, damage):
    from uuid import uuid4

    journal, trial_id, target = await setup(sessions)
    proposed = await journal.propose(trial_id=trial_id, target_id=target.target_id)
    reserved = grant(proposed)
    activated = reserved.model_copy(update={"phase": "create_intent", "plan_sha256": "d" * 64})
    if damage != "unattached":
        await journal.accept_grant(proposed.request.key, reserved)
        await journal.begin_activation(proposed.request.key)
    if damage == "foreign-plan":
        await journal.confirm_activation(proposed.request.key, activated)
        activated = activated.model_copy(update={"plan_sha256": "e" * 64})
    elif damage == "foreign-grant":
        activated = activated.model_copy(update={"reservation_id": uuid4()})
    with pytest.raises(ValueError):
        await journal.confirm_activation(proposed.request.key, activated)
