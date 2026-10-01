"""Gateway orchestration uses retained effects; waiting never authorizes retries."""
from __future__ import annotations

import asyncio
import copy

import httpx
import pytest
from sqlalchemy import select

from loom.db.nebius_pool_schema import NebiusPoolEffect, NebiusPoolRequest
from tests.integration.test_nebius_pool_cleanup_journal import begin_cleanup
from tests.integration.test_nebius_pool_kubernetes import provider
from tests.integration.test_nebius_pool_pod_inventory import InventoryAPI, pod
from tests.integration.test_nebius_pool_registry import sessions as sessions


def worker(gateway, principal):
    from loom_service.pool_management.worker import PoolGatewayWorker

    return PoolGatewayWorker(gateway=gateway, principal=principal)


@pytest.mark.parametrize("build", [False, True])
async def test_worker_creates_then_drains_and_releases_exact_workload(sessions, build):
    gateway, api, principal, receipt, original = await provider(sessions, build=build)
    await original.aclose()
    inventory = InventoryAPI(api, [])
    async with httpx.AsyncClient(base_url="https://kubernetes.example", transport=httpx.MockTransport(inventory)) as http:
        gateway.http = http
        await worker(gateway, principal).run_once()
        async with sessions() as session:
            row = await session.get(NebiusPoolRequest, receipt.reservation_id)
            assert row.phase == "observed" and row.job_uid is not None
        assert [item["kind"] for item in api.writes] == (["ConfigMap", "Job"] if build else ["Job"])
        await worker(gateway, principal).run_once()
        assert len(api.writes) == (2 if build else 1)
        await begin_cleanup(sessions, receipt.reservation_id)
        await worker(gateway, principal).run_once()
        async with sessions() as session:
            row = await session.get(NebiusPoolRequest, receipt.reservation_id)
            assert row.phase == "released" and row.cleanup_observation_id is not None
        assert not api.objects and len(api.deletes) == (2 if build else 1)
        before = list(api.requests)
        await worker(gateway, principal).run_once()
        assert api.requests == before


async def test_worker_signals_stopped_job_before_drain_but_retains_auxiliary_and_charge(sessions):
    gateway, api, principal, receipt, http = await provider(sessions, build=True)
    async with http:
        await worker(gateway, principal).run_once()
        await begin_cleanup(sessions, receipt.reservation_id, drained=False)
        await worker(gateway, principal).run_once()
    assert len(api.deletes) == 1 and next(iter(api.objects.values()))["kind"] == "ConfigMap"
    async with sessions() as session:
        row = await session.get(NebiusPoolRequest, receipt.reservation_id)
        assert row.phase == "cleanup_intent" and row.cleanup_observation_id is None


async def test_worker_does_not_repeat_uncertain_create_and_recovers_visible_object_after_stop(sessions):
    gateway, api, principal, receipt, original = await provider(sessions)
    await original.aclose()
    async with httpx.AsyncClient(base_url="https://kubernetes.example", transport=httpx.MockTransport(InventoryAPI(api, []))) as http:
        gateway.http = http
        api.lose_reply = True
        await worker(gateway, principal).run_once()
        assert len(api.writes) == 1
        await begin_cleanup(sessions, receipt.reservation_id)
        api.hide_objects = True
        await worker(gateway, principal).run_once()
        async with sessions() as session:
            assert (await session.get(NebiusPoolRequest, receipt.reservation_id)).phase == "cleanup_intent"
        assert not api.deletes and len(api.writes) == 1
        api.hide_objects = False
        await worker(gateway, principal).run_once()
        assert len(api.writes) == len(api.deletes) == 1
        async with sessions() as session:
            assert (await session.get(NebiusPoolRequest, receipt.reservation_id)).phase == "released"


async def test_worker_releases_stopped_never_dispatched_work_without_a_create(sessions):
    gateway, api, principal, receipt, original = await provider(sessions, build=True)
    await begin_cleanup(sessions, receipt.reservation_id)
    await original.aclose()
    async with httpx.AsyncClient(base_url="https://kubernetes.example", transport=httpx.MockTransport(InventoryAPI(api, []))) as http:
        gateway.http = http
        await worker(gateway, principal).run_once()
    assert not api.writes and not api.deletes
    async with sessions() as session:
        assert (await session.get(NebiusPoolRequest, receipt.reservation_id)).phase == "released"
        assert not (await session.scalars(select(NebiusPoolEffect))).all()


async def test_worker_cleans_only_owned_residual_pods_after_job_and_drain(sessions):
    gateway, api, principal, receipt, original = await provider(sessions)
    await worker(gateway, principal).run_once()
    job = copy.deepcopy(next(iter(api.objects.values())))
    child = pod(job)
    foreign = pod(job, suffix="foreign")
    foreign["metadata"].update(name="foreign", ownerReferences=[], labels={}, annotations={})
    child_path = "/api/v1/namespaces/" + child["metadata"]["namespace"] + "/pods/" + child["metadata"]["name"]
    api.objects[child_path] = child
    inventory = InventoryAPI(api, [])

    def handler(request):
        inventory.pods = [foreign] + ([child] if child_path in api.objects else [])
        return inventory(request)

    await begin_cleanup(sessions, receipt.reservation_id)
    await original.aclose()
    async with httpx.AsyncClient(base_url="https://kubernetes.example", transport=httpx.MockTransport(handler)) as http:
        gateway.http = http
        await worker(gateway, principal).run_once()
    assert len(api.deletes) == 2 and api.deletes[-1][0] == child_path
    assert api.deletes[-1][1]["preconditions"]["uid"] == child["metadata"]["uid"]
    async with sessions() as session:
        assert (await session.get(NebiusPoolRequest, receipt.reservation_id)).phase == "released"


async def test_concurrent_workers_replay_one_create_and_recover_lost_delete(sessions):
    gateway, api, principal, receipt, original = await provider(sessions)
    await original.aclose()
    async with httpx.AsyncClient(base_url="https://kubernetes.example", transport=httpx.MockTransport(InventoryAPI(api, []))) as http:
        gateway.http = http
        await asyncio.gather(worker(gateway, principal).run_once(), worker(gateway, principal).run_once())
        assert len(api.writes) == 1
        await begin_cleanup(sessions, receipt.reservation_id)
        api.lose_delete_reply = True
        await worker(gateway, principal).run_once()
        async with sessions() as session:
            assert (await session.get(NebiusPoolRequest, receipt.reservation_id)).phase == "cleanup_intent"
        await worker(gateway, principal).run_once()
        assert len(api.deletes) == 1
        async with sessions() as session:
            assert (await session.get(NebiusPoolRequest, receipt.reservation_id)).phase == "released"


async def test_real_queue_gateway_result_and_cleanup_loops_close_the_same_attempt(sessions, tmp_path):
    from loom_execution_actuator.pool_build_driver import PoolBuildDriver
    from loom_execution_actuator.pool_build_runtime import PoolNativeBuildController
    from loom_service.pool_management.gateway_journal import PoolGatewayJournal
    from loom_service.pool_management.kubernetes import KubernetesPoolGateway
    from tests.integration.test_nebius_pool_build_outbox import counts, local_setup, outbox
    from tests.integration.test_nebius_pool_build_runtime import Reader, finish, local_rows
    from tests.integration.test_nebius_pool_build_selection import selector
    from tests.integration.test_nebius_pool_kubernetes import KubernetesAPI
    from tests.integration.test_nebius_pool_participant_http import client, setup
    from tests.integration.test_nebius_pool_registry import machine

    app, _, token, participants, _, _ = await setup(sessions, tmp_path)
    participant = participants[0]
    _, original, _ = await local_setup(sessions, environment_id=participant.environment_id)
    journal = outbox(sessions, participant)
    api = KubernetesAPI(participant.build_namespace.uid)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app)) as management_http, httpx.AsyncClient(
        base_url="https://kubernetes.example", transport=httpx.MockTransport(InventoryAPI(api, []))) as kube_http:
        driver = PoolBuildDriver(outbox=journal, management=client(management_http, token))
        reader = Reader()
        native = PoolNativeBuildController(driver=driver, kubernetes=reader, selector=selector(journal))
        gateway = KubernetesPoolGateway(PoolGatewayJournal(sessions), kube_http)
        fixed = worker(gateway, await machine(sessions, participant.pool_id, role="gateway"))
        await native.run_once()
        handoff, = await journal.pending()
        assert handoff.request.key.local_work_id == original.key.local_work_id
        await fixed.run_once()
        job = copy.deepcopy(next(item for item in api.objects.values() if item["kind"] == "Job"))
        job["pods"] = [pod(job)]
        finish(job, handoff.request)
        reader.job = job
        await native.run_once()
        assert (await journal.get(handoff.request.key)).phase == "stop_pending"
        await fixed.run_once()
        await native.run_once()
        released = await journal.get(handoff.request.key)
        assert released.phase == "released" and not await journal.pending()
        row, attempt = await local_rows(sessions, handoff.request)
        assert row.state == "ready" and row.registry_images == {"task": "registry.example/tasks@sha256:" + "a" * 64}
        assert attempt.id == released.attempt_id and attempt.native_build["capacity_released_at"]
        assert await counts(sessions, row.id) == (1, 1, 1)
        assert len(api.writes) == len(api.deletes) == 2
