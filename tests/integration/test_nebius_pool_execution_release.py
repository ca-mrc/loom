"""Only manager-qualified release may complete local global-execution cleanup."""
from __future__ import annotations

from contextlib import asynccontextmanager
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import func, select
from sqlalchemy.exc import DBAPIError

from loom.db.schema import ServiceExecutionEvent, ServiceExecutionLease, Trial
from loom_control_plane.service_execution import (
    enqueue_execution_transition,
    mark_execution_output_unavailable,
    record_kubernetes_observation,
)
from loom_execution_actuator.pool_execution_driver import PoolExecutionDriver
from loom_service.pool_management.gateway_journal import PoolGatewayJournal
from loom_service.pool_management.kubernetes import KubernetesPoolGateway
from tests.integration.test_nebius_pool_execution_activation import selected
from tests.integration.test_nebius_pool_kubernetes import KubernetesAPI
from tests.integration.test_nebius_pool_observation_registry import sessions as sessions
from tests.integration.test_nebius_pool_participant_http import client
from tests.integration.test_nebius_pool_pod_inventory import InventoryAPI
from tests.integration.test_nebius_pool_registry import machine


@asynccontextmanager
async def released(sessions, tmp_path, *, created=True, retry=False):
    outbox, trial_id, proposal, app, token = await selected(sessions, tmp_path)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app)) as management_http:
        driver = PoolExecutionDriver(outbox=outbox, management=client(management_http, token))
        active = await driver.advance(proposal.request.key)
        gateway_principal = await machine(sessions, proposal.request.pool_id, role="gateway")
        api = KubernetesAPI(outbox.participant.execution_namespace.uid)
        async with httpx.AsyncClient(base_url="https://kubernetes.example",
                transport=httpx.MockTransport(InventoryAPI(api, []))) as kubernetes_http:
            gateway = KubernetesPoolGateway(PoolGatewayJournal(sessions), kubernetes_http)
            if created:
                job = await gateway.create(gateway_principal, active.reservation_id, kind="Job")
                async with sessions.begin() as session:
                    lease = await session.get(ServiceExecutionLease, active.lease_id)
                    await record_kubernetes_observation(session, lease_id=lease.id, generation=lease.generation,
                        payload={"normalized_state": "pending", "job_uid": str(job.observed_uid), "resource_version": "11"},
                        observed_at=lease.created_at)
            async with sessions.begin() as session:
                lease = await session.get(ServiceExecutionLease, active.lease_id)
                await enqueue_execution_transition(session, lease_id=lease.id, expected_generation=lease.generation,
                    desired_state="retry" if retry else "cancel")
                await mark_execution_output_unavailable(session, lease_id=lease.id, expected_generation=lease.generation,
                    reason="test_cleanup_deadline", now=lease.cleanup_deadline_at)
            await driver.stop_and_drain(proposal.request.key)
            if created:
                await gateway.delete(gateway_principal, active.reservation_id, kind="Job")
            receipt = await gateway.verify_cleanup(gateway_principal, active.reservation_id)
            assert receipt.phase == "released" and receipt.cleanup_observation_id is not None
            yield outbox, driver, active, receipt, trial_id


@pytest.mark.parametrize("created", [False, True])
async def test_release_projects_exact_deleted_lease_once_without_inventing_job_identity(sessions, tmp_path, created):
    async with released(sessions, tmp_path, created=created) as (outbox, driver, active, receipt, trial_id):
        async with sessions() as session:
            assert (await session.get(ServiceExecutionLease, active.lease_id)).deleted_at is None
        done = await outbox.confirm_release(active.request.key, receipt)
        assert done.phase == "released" and done.released == receipt
        assert await outbox.confirm_release(active.request.key, receipt) == done
        assert await driver.advance(active.request.key) == done
        async with sessions() as session:
            lease = await session.get(ServiceExecutionLease, active.lease_id)
            assert lease.desired_state == "deleted" and lease.cleanup_state == "complete" and lease.deleted_at is not None
            assert lease.job_uid == (str(receipt.job_uid) if created else None) and lease.pod_uid is None
            assert lease.output_commit_state == "unavailable"
            assert (await session.get(Trial, trial_id)).state == "cancelled"
            assert await session.scalar(select(func.count()).select_from(ServiceExecutionEvent).where(
                ServiceExecutionEvent.lease_id == lease.id,
                ServiceExecutionEvent.payload_json["normalized_state"].astext == "deleted")) == 1


@pytest.mark.parametrize("damage", ["reservation", "plan", "job", "phase", "key"])
async def test_cross_request_or_unreleased_receipt_cannot_close_local_execution(sessions, tmp_path, damage):
    async with released(sessions, tmp_path) as (outbox, _, active, receipt, _):
        changed = receipt.model_dump(mode="json")
        if damage == "reservation":
            changed["reservation_id"] = str(uuid4())
        elif damage == "plan":
            changed["plan_sha256"] = "a" * 64
        elif damage == "job":
            changed["job_uid"] = str(uuid4())
        elif damage == "phase":
            changed.update(phase="cleanup_intent", cleanup_observation_id=None)
        else:
            changed["request_key"]["local_work_id"] = str(uuid4())
        with pytest.raises(ValueError):
            await outbox.confirm_release(active.request.key, type(receipt).model_validate(changed))
        async with sessions() as session:
            assert (await session.get(ServiceExecutionLease, active.lease_id)).deleted_at is None
        assert (await outbox.get(active.request.key)).phase == "stop_pending"


async def test_released_attempt_unblocks_next_trial_selection_and_old_replay_is_read_only(sessions, tmp_path):
    async with released(sessions, tmp_path, retry=True) as (outbox, _, active, receipt, trial_id):
        # Before manager acknowledgment, retry cannot duplicate the live selection.
        assert (await outbox.propose(trial_id=trial_id, target_id=active.request.target_id)).request.key == active.request.key
        done = await outbox.confirm_release(active.request.key, receipt)
        following = await outbox.propose(trial_id=trial_id, target_id=active.request.target_id)
        assert following.request.key != active.request.key and following.phase == "selected"
        assert await outbox.confirm_release(active.request.key, receipt) == done
        assert (await outbox.get(following.request.key)).phase == "selected"
        async with sessions() as session:
            assert (await session.get(Trial, trial_id)).attempt_count == 1


async def test_database_rejects_local_deletion_without_durable_manager_release(sessions, tmp_path):
    async with released(sessions, tmp_path) as (_, _, active, _, _):
        with pytest.raises(DBAPIError):
            async with sessions.begin() as session:
                lease = await session.get(ServiceExecutionLease, active.lease_id)
                await record_kubernetes_observation(session, lease_id=lease.id, generation=lease.generation,
                    payload={"normalized_state": "deleted", "job_uid": lease.job_uid}, observed_at=lease.created_at)
