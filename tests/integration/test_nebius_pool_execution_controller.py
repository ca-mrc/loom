"""Actual actuator + local SQL + management HTTP + gateway, with external K8s doubled."""
from __future__ import annotations

import copy
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import func, select

from loom.db.nebius_pool_schema import NebiusPoolRequest
from loom.db.schema import (
    ExecutionProvisioningAuthorization,
    ServiceExecutionCommand,
    ServiceExecutionLease,
    ServiceExecutionTarget,
    Trial,
)
from loom_control_plane.service_execution import enqueue_execution_transition
from loom_execution_actuator.contracts import ActuatorContractError, KubernetesApiError
from loom_execution_actuator.controller import ExecutionActuator
from loom_execution_actuator.kubernetes_api import InClusterKubernetesJobApi
from loom_execution_actuator.pool_execution_driver import PoolExecutionDriver
from loom_execution_actuator.renderer import ExecutionTargetRuntime
from loom_service.pool_management.gateway_journal import PoolGatewayJournal
from loom_service.pool_management.kubernetes import KubernetesPoolGateway
from loom_service.pool_management.worker import PoolGatewayWorker
from tests.integration.test_nebius_pool_execution_activation import selected
from tests.integration.test_nebius_pool_kubernetes import KubernetesAPI
from tests.integration.test_nebius_pool_observation_registry import sessions as sessions
from tests.integration.test_nebius_pool_participant_http import client
from tests.integration.test_nebius_pool_pod_inventory import InventoryAPI, pod
from tests.integration.test_nebius_pool_registry import machine


class SDKBoundary:
    """The real read-only adapter receives real SDK model shapes, no write methods."""

    def __init__(self, api, namespace):
        from kubernetes import client

        self.api, self.namespace, self.pods, self.reads = api, namespace, [], []
        self.deserialize = client.ApiClient()._ApiClient__deserialize
        self.namespace_uid = str(namespace.uid)

    def read_namespace(self, name, **kwargs):
        assert name == self.namespace.name
        self.reads.append("namespace")
        return self.deserialize({"apiVersion": "v1", "kind": "Namespace",
            "metadata": {"name": name, "uid": self.namespace_uid}}, "V1Namespace")

    def read_namespaced_job(self, name, namespace, **kwargs):
        from kubernetes.client import ApiException

        assert namespace == self.namespace.name
        self.reads.append("job")
        value = self.api.objects.get(f"/apis/batch/v1/namespaces/{namespace}/jobs/{name}")
        if value is None or self.api.hide_objects:
            raise ApiException(status=404)
        return self.deserialize(copy.deepcopy(value), "V1Job")

    def list_namespaced_pod(self, namespace, **kwargs):
        assert namespace == self.namespace.name and kwargs["limit"] == 100
        self.reads.append("pods")
        name = kwargs["label_selector"].removeprefix("job-name=")
        return self.deserialize({"apiVersion": "v1", "kind": "PodList", "metadata": {"resourceVersion": "30"},
            "items": [copy.deepcopy(item) for item in self.pods
                if item["metadata"]["ownerReferences"][0]["name"] == name]}, "V1PodList")


@asynccontextmanager
async def connected(sessions, tmp_path, *, occupied_cpu=0):
    from loom_execution_actuator.pool_execution_resources import PoolExecutionResources

    outbox, trial_id, proposal, app, token = await selected(sessions, tmp_path, occupied_cpu=occupied_cpu)
    api = KubernetesAPI(outbox.participant.execution_namespace.uid)
    sdk = SDKBoundary(api, outbox.participant.execution_namespace)
    kubernetes = InClusterKubernetesJobApi.__new__(InClusterKubernetesJobApi)
    kubernetes._batch, kubernetes._core = sdk, sdk
    inventory = InventoryAPI(api, sdk.pods)
    async with (httpx.AsyncClient(transport=httpx.ASGITransport(app=app)) as management_http,
                httpx.AsyncClient(base_url="https://kubernetes.example", transport=httpx.MockTransport(inventory)) as gateway_http):
        driver = PoolExecutionDriver(outbox=outbox, management=client(management_http, token))
        target = ExecutionTargetRuntime(target_id=proposal.request.target_id, namespace=outbox.participant.execution_namespace.name)
        resources = PoolExecutionResources(driver=driver, kubernetes=kubernetes, target_id=target.target_id)
        actuator = ExecutionActuator(sessions=sessions, kubernetes=kubernetes, target=target,
            controller_id="global-test", pool_resources=resources)
        gateway = KubernetesPoolGateway(PoolGatewayJournal(sessions), gateway_http)
        principal = await machine(sessions, proposal.request.pool_id, role="gateway")
        worker = PoolGatewayWorker(gateway=gateway, principal=principal)
        yield SimpleNamespace(outbox=outbox, trial_id=trial_id, proposal=proposal, driver=driver, api=api, sdk=sdk,
            resources=resources, actuator=actuator, worker=worker, kubernetes=kubernetes, target=target)


async def lease(sessions, case):
    async with sessions() as session:
        return await session.get(ServiceExecutionLease, case.proposal.request.key.local_work_id)


async def start(sessions, case):
    await case.actuator.reconcile_full_once()
    active = await case.outbox.get(case.proposal.request.key)
    assert active.phase == "active" and not case.api.writes
    assert await case.actuator.run_commands_once() == 1
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(ExecutionProvisioningAuthorization)) == 0
        command = await session.scalar(select(ServiceExecutionCommand).where(ServiceExecutionCommand.command_type == "create"))
        assert command.state == "acknowledged"
    await case.worker.run_once()
    job, = case.api.objects.values()
    child = pod(job)
    child["spec"]["nodeName"] = "execution-node"
    child["status"].update(podIP="10.24.7.19", containerStatuses=[{"name": "execution", "image": "test",
        "imageID": "test", "ready": True, "restartCount": 0,
        "state": {"running": {"startedAt": datetime.now(UTC).isoformat()}}}])
    case.sdk.pods.append(child)
    await case.actuator.reconcile_full_once()
    saved = await lease(sessions, case)
    assert saved.job_uid == job["metadata"]["uid"] and saved.pod_uid == child["metadata"]["uid"]
    async with sessions() as session:
        assert (await session.get(Trial, case.trial_id)).state == "running"
    return active


async def test_real_actuator_runs_global_execution_and_cancels_before_output_drain(sessions, tmp_path):
    async with connected(sessions, tmp_path) as case:
        active = await start(sessions, case)
        async with sessions.begin() as session:
            current = await session.get(ServiceExecutionLease, active.lease_id)
            await enqueue_execution_transition(session, lease_id=current.id, expected_generation=current.generation,
                desired_state="cancel")
        await case.actuator.run_commands_once()
        current = await lease(sessions, case)
        assert current.output_commit_state == "not_started" and current.deleted_at is None
        async with sessions() as session:
            remote = await session.get(NebiusPoolRequest, active.reservation_id)
            assert remote.phase == "cleanup_intent" and remote.stop_json is not None and remote.drain_json is None
        await case.worker.run_once()
        assert len(case.api.deletes) == 1
        case.sdk.pods.clear()  # External Job controller completed foreground Pod deletion.
        # Missing Job during output drain does not free the reservation or revive create.
        await case.actuator.reconcile_full_once()
        assert (await lease(sessions, case)).deleted_at is None and len(case.api.writes) == 1
        case.actuator = ExecutionActuator(sessions=sessions, kubernetes=case.kubernetes, target=case.target,
            controller_id="restarted-global", pool_resources=case.resources)
        await case.actuator.reconcile_full_once(now=current.cleanup_deadline_at + timedelta(seconds=1))
        assert (await lease(sessions, case)).output_commit_state == "unavailable"
        assert (await lease(sessions, case)).deleted_at is None
        await case.worker.run_once()
        await case.actuator.reconcile_full_once()
        done = await lease(sessions, case)
        assert done.desired_state == "deleted" and done.cleanup_state == "complete"
        assert (await case.outbox.get(case.proposal.request.key)).phase == "released"
        async with sessions() as session:
            assert (await session.get(Trial, case.trial_id)).state == "cancelled"
        assert len(case.api.writes) == len(case.api.deletes) == 1


async def test_global_controller_reuses_native_terminal_failure_projection(sessions, tmp_path):
    async with connected(sessions, tmp_path) as case:
        await start(sessions, case)
        case.sdk.pods[0]["status"] = {"phase": "Failed", "reason": "NodeLost"}
        observed_at = datetime.now(UTC)
        await case.actuator.reconcile_full_once(now=observed_at)
        assert (await lease(sessions, case)).observed_state == "failed"
        await case.actuator.reconcile_full_once(now=observed_at + timedelta(minutes=6))
        async with sessions() as session:
            trial = await session.get(Trial, case.trial_id)
            assert trial.state == "failed" and trial.failure_reason == "native_execution_failed" and trial.result is None
        assert (await lease(sessions, case)).output_commit_state == "unavailable"
        case.sdk.pods.clear()
        await case.worker.run_once()
        await case.actuator.reconcile_full_once()
        assert (await lease(sessions, case)).cleanup_state == "complete"


async def test_waiting_execution_stays_unclaimed_and_namespace_probe_keeps_empty_target_healthy(sessions, tmp_path):
    async with connected(sessions, tmp_path, occupied_cpu=3000) as case:
        await case.actuator.reconcile_full_once()
        assert (await case.outbox.get(case.proposal.request.key)).phase == "selected"
        assert await lease(sessions, case) is None and not case.api.writes
        async with sessions() as session:
            target = await session.get(ServiceExecutionTarget, case.target.target_id)
            first_health = target.health_observed_at
            assert (await session.get(Trial, case.trial_id)).attempt_count == 0
        case.sdk.namespace_uid = str(uuid4())
        with pytest.raises(KubernetesApiError):
            await case.actuator.reconcile_full_once()
        async with sessions() as session:
            assert (await session.get(ServiceExecutionTarget, case.target.target_id)).health_observed_at == first_health
        with pytest.raises(ActuatorContractError):
            await case.actuator.watch_once(timeout_seconds=1)


async def test_missing_active_job_never_recreates_or_projects_deletion(sessions, tmp_path):
    async with connected(sessions, tmp_path) as case:
        await start(sessions, case)
        case.api.hide_objects = True
        await case.actuator.reconcile_full_once()
        current = await lease(sessions, case)
        assert current.desired_state != "deleted" and current.output_commit_state == "not_started"
        assert len(case.api.writes) == 1 and not case.api.deletes


async def test_stale_accepted_activation_is_locally_revoked_before_global_stop(sessions, tmp_path):
    async with connected(sessions, tmp_path) as case:
        active = await case.driver.advance(case.proposal.request.key)
        await case.outbox.request_cancel(case.proposal.request.key)
        assert (await lease(sessions, case)).desired_state == "create"
        await case.actuator.reconcile_full_once()
        current = await lease(sessions, case)
        assert current.desired_state == "retry" and current.revoked_at is not None
        async with sessions() as session:
            remote = await session.get(NebiusPoolRequest, active.reservation_id)
            assert remote.phase == "cleanup_intent" and remote.stop_json is not None and remote.drain_json is None
        assert not case.api.writes and current.output_commit_state == "not_started"
