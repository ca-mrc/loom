"""Global native results use real handoff SQL/HTTP, never the legacy Job writer."""
from __future__ import annotations

import asyncio
import copy
import json
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import select, update

from loom.db.nebius_pool_schema import NebiusPoolRequest
from loom.db.schema import TaskImageMaterialization, TaskImageMaterializationAttempt, Trial
from loom_execution_actuator.pool_build_driver import PoolBuildDriver
from tests.integration.test_nebius_pool_build_driver import selected
from tests.integration.test_nebius_pool_observation_registry import sessions as sessions
from tests.integration.test_nebius_pool_participant_http import client
from tests.integration.test_nebius_pool_registry import machine


class Reader:
    """Only the external Kubernetes read is doubled; no write method exists."""

    def __init__(self, job=None, during_read=None):
        self.job, self.during_read, self.calls = job, during_read, 0

    async def observe_pool(self, runtime, *, capture_logs=False):
        self.calls += 1
        if self.during_read is not None:
            await self.during_read()
        return copy.deepcopy(self.job)


async def gateway_job(sessions, handoff):
    from loom_service.pool_management.gateway_journal import PoolGatewayJournal

    journal = PoolGatewayJournal(sessions)
    gateway = await machine(sessions, handoff.request.pool_id, role="gateway")
    for kind in ("ConfigMap", "Job"):
        effect = await journal.prepare_create(gateway, handoff.reservation_id, kind=kind)
        await journal.dispatch_create(gateway, effect.effect_id)
        effect = await journal.observe_create(gateway, effect.effect_id, uid=uuid4(), resource_version="1")
    job = copy.deepcopy(effect.document)
    job["metadata"].update(uid=str(effect.observed_uid), resourceVersion="1")
    pod = {"apiVersion": "v1", "kind": "Pod", "metadata": {
        **copy.deepcopy(job["spec"]["template"]["metadata"]), "namespace": job["metadata"]["namespace"],
        "uid": str(uuid4()), "resourceVersion": "2", "name": job["metadata"]["name"] + "-abcde",
        "ownerReferences": [{"apiVersion": "batch/v1", "kind": "Job", "controller": True,
            "uid": job["metadata"]["uid"], "name": job["metadata"]["name"]}]}, "status": {}}
    job["pods"] = [pod]
    return job


def finish(job, request, *, invalid=False):
    job["status"] = {"succeeded": 1}
    job["pods"][0]["status"] = {"containerStatuses": [{"name": "publish", "state": {"terminated": {
        "exitCode": 0, "message": json.dumps({"materialization_id": str(request.key.local_work_id),
            "lease_epoch": request.build.expected_lease_epoch + 1,
            "registry_images": {"task": ("foreign.example/tasks" if invalid else "registry.example/tasks") + "@sha256:" + "a" * 64}})}}}]}
    job["builder_log"] = "build complete password=must-not-persist"


async def local_rows(sessions, request):
    async with sessions() as session:
        row = await session.get(TaskImageMaterialization, request.key.local_work_id)
        attempt = (await session.scalars(select(TaskImageMaterializationAttempt))).one()
        return row, attempt


async def test_waiting_for_gateway_heartbeats_without_inventing_job_or_result(sessions, tmp_path):
    from loom_execution_actuator.pool_build_runtime import PoolNativeBuildController

    app, token, _, request, journal = await selected(sessions, tmp_path)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app)) as http:
        driver = PoolBuildDriver(outbox=journal, management=client(http, token))
        await driver.advance(request.key)
        soon = datetime.now(UTC) + timedelta(seconds=25)
        async with sessions.begin() as session:
            await session.execute(update(TaskImageMaterialization).values(lease_expires_at=soon))
        reader = Reader()
        await PoolNativeBuildController(driver=driver, kubernetes=reader).run_once()
    row, attempt = await local_rows(sessions, request)
    assert row.state == "claimed" and row.lease_expires_at > soon
    assert attempt.native_build["pool_reservation_id"] == str((await journal.get(request.key)).reservation_id)
    assert attempt.native_build["job_uid"] is None and not reader.calls
    assert "configmap" not in attempt.native_build and "job" not in attempt.native_build
    assert (await journal.get(request.key)).phase == "active"


@pytest.mark.parametrize("invalid", [False, True])
async def test_concurrent_recovery_records_exact_result_without_releasing_capacity(sessions, tmp_path, invalid):
    from loom_execution_actuator.pool_build_runtime import PoolNativeBuildController

    app, token, _, request, journal = await selected(sessions, tmp_path)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app)) as http:
        driver = PoolBuildDriver(outbox=journal, management=client(http, token))
        handoff = await driver.advance(request.key)
        job = await gateway_job(sessions, handoff)
        finish(job, request, invalid=invalid)
        reader = Reader(job)
        await asyncio.gather(*(PoolNativeBuildController(driver=driver, kubernetes=reader).run_once() for _ in range(2)))
        await PoolNativeBuildController(driver=driver, kubernetes=reader).run_once()
    row, attempt = await local_rows(sessions, request)
    assert row.state == ("failed" if invalid else "ready")
    assert row.attempt_count == 1 and row.lease_epoch == 1
    if invalid:
        assert row.failure_reason == "build_publication_receipt_invalid" and not row.registry_images
    else:
        assert row.registry_images == {"task": "registry.example/tasks@sha256:" + "a" * 64}
    assert "must-not-persist" not in json.dumps(attempt.native_build)
    assert not attempt.native_build.get("capacity_released_at")
    assert (await journal.get(request.key)).phase == "stop_pending"
    async with sessions() as session:
        global_row = await session.get(NebiusPoolRequest, handoff.reservation_id)
        assert global_row.phase == "observed" and global_row.cleanup_observation_id is None


@pytest.mark.parametrize("damage", ["superseded", "source", "cancelled"])
async def test_changed_local_authority_during_read_never_commits_old_publication(sessions, tmp_path, damage):
    from loom_execution_actuator.pool_build_runtime import PoolNativeBuildController

    app, token, _, request, journal = await selected(sessions, tmp_path)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app)) as http:
        driver = PoolBuildDriver(outbox=journal, management=client(http, token))
        job = await gateway_job(sessions, await driver.advance(request.key))
        finish(job, request)

        async def changed():
            async with sessions.begin() as session:
                if damage == "cancelled":
                    await session.execute(update(Trial).values(cancellation_requested_at=datetime.now(UTC)))
                else:
                    values = ({"lease_epoch": 2, "claimed_by": "successor"} if damage == "superseded"
                              else {"task_source": "s3://other/source/"})
                    await session.execute(update(TaskImageMaterialization).values(**values))

        await PoolNativeBuildController(driver=driver, kubernetes=Reader(job, changed)).run_once()
    row, attempt = await local_rows(sessions, request)
    assert not row.registry_images and row.state != "ready"
    assert (await journal.get(request.key)).phase == "stop_pending"
    assert not attempt.native_build.get("capacity_released_at")
    if damage == "superseded":
        assert row.lease_epoch == 2 and row.claimed_by == "successor"


@pytest.mark.parametrize("damage", ["job_uid", "effect", "pod_owner", "pod_epoch"])
async def test_foreign_kubernetes_identity_cannot_supply_publication(sessions, tmp_path, damage):
    from loom_execution_actuator.pool_build_runtime import PoolNativeBuildController

    app, token, _, request, journal = await selected(sessions, tmp_path)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app)) as http:
        driver = PoolBuildDriver(outbox=journal, management=client(http, token))
        job = await gateway_job(sessions, await driver.advance(request.key))
        finish(job, request)
        if damage == "job_uid":
            job["metadata"]["uid"] = str(uuid4())
        elif damage == "effect":
            job["metadata"]["annotations"]["loom.nebius/pool-effect-id"] = str(uuid4())
        elif damage == "pod_owner":
            job["pods"][0]["metadata"]["ownerReferences"][0]["uid"] = str(uuid4())
        else:
            job["pods"][0]["metadata"]["labels"]["loom.lease-epoch"] = "99"
        with pytest.raises(ValueError):
            await PoolNativeBuildController(driver=driver, kubernetes=Reader(job)).run_once()
    row, _ = await local_rows(sessions, request)
    assert row.state == "claimed" and not row.registry_images
