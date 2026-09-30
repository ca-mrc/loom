"""Capacity release requires fresh fixed-gateway absence and settled create authority."""
from __future__ import annotations

import copy
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import func, select, update

from loom.db.nebius_pool_schema import NebiusPoolCleanupObservation, NebiusPoolRequest
from loom.db.schema import Token
from tests.integration.test_nebius_pool_cleanup_journal import begin_cleanup
from tests.integration.test_nebius_pool_kubernetes import provider
from tests.integration.test_nebius_pool_pod_inventory import InventoryAPI, pod
from tests.integration.test_nebius_pool_registry import sessions as sessions


async def stopped(sessions, *, build=False, started=True, drained=True, keep_auxiliary=False):
    gateway, api, principal, receipt, original = await provider(sessions, build=build)
    job = None
    if started:
        if build:
            await gateway.create(principal, receipt.reservation_id, kind="ConfigMap")
        await gateway.create(principal, receipt.reservation_id, kind="Job")
        job = copy.deepcopy(next(value for value in api.objects.values() if value["kind"] == "Job"))
    await begin_cleanup(sessions, receipt.reservation_id, drained=drained)
    if started:
        await gateway.delete(principal, receipt.reservation_id, kind="Job")
        if build and drained and not keep_auxiliary:
            await gateway.delete(principal, receipt.reservation_id, kind="ConfigMap")
    await original.aclose()
    inventory = InventoryAPI(api, [])
    http = httpx.AsyncClient(base_url="https://kubernetes.example", transport=httpx.MockTransport(inventory))
    gateway.http = http
    return gateway, api, principal, receipt, http, inventory, job


async def retained(sessions, receipt):
    async with sessions() as session:
        row = await session.get(NebiusPoolRequest, receipt.reservation_id)
        evidence = list((await session.scalars(select(NebiusPoolCleanupObservation).where(
            NebiusPoolCleanupObservation.request_id == receipt.reservation_id))).all())
        return row, evidence


@pytest.mark.parametrize("build,started", [(False, True), (True, True), (False, False), (True, False)])
async def test_verified_absence_releases_only_its_reservation_and_replays_retained_receipt(sessions, build, started):
    gateway, api, principal, receipt, http, inventory, _ = await stopped(sessions, build=build, started=started)
    async with http:
        result = await gateway.verify_cleanup(principal, receipt.reservation_id)
        assert result.phase == "released" and not result.capacity_charged
        assert (result.job_uid is not None) == started
        requests = list(api.requests)
        assert await gateway.verify_cleanup(principal, receipt.reservation_id) == result
        assert api.requests == requests  # Terminal replay is retained history.
    row, evidence = await retained(sessions, receipt)
    assert row.cleanup_observation_id == result.cleanup_observation_id == evidence[0].observation_id
    assert len(evidence) == 1 and inventory.pages == [""]
    assert evidence[0].plan_sha256 == row.plan_sha256 and evidence[0].namespace_uid == row.namespace_uid
    assert evidence[0].evidence_json["schema_version"] == "loom.pool-cleanup-evidence.v1"
    assert evidence[0].evidence_json["pod_list_resource_version"] == "30"


@pytest.mark.parametrize("blocker", ["drain", "pod", "auxiliary", "namespace", "job"])
async def test_incomplete_or_replaced_cleanup_keeps_capacity_charged(sessions, blocker):
    gateway, api, principal, receipt, http, inventory, job = await stopped(sessions,
        build=blocker == "auxiliary", drained=blocker != "drain", keep_auxiliary=blocker == "auxiliary")
    if blocker == "pod":
        child = pod(job, phase="Succeeded")
        child["metadata"]["deletionTimestamp"] = "2026-09-30T01:00:00Z"
        inventory.pods.append(child)
    elif blocker == "namespace":
        api.namespace_uid = uuid4()
    elif blocker == "job":
        job["metadata"]["uid"] = str(uuid4())
        api.objects["/apis/batch/v1/namespaces/" + job["metadata"]["namespace"] + "/jobs/" + job["metadata"]["name"]] = job
    async with http:
        with pytest.raises(ValueError):
            await gateway.verify_cleanup(principal, receipt.reservation_id)
    row, evidence = await retained(sessions, receipt)
    assert row.phase == "cleanup_intent" and row.cleanup_observation_id is None and not evidence


async def test_unrelated_foreign_pods_are_preserved_and_do_not_become_our_cleanup(sessions):
    gateway, api, principal, receipt, http, inventory, job = await stopped(sessions)
    foreign = pod(job)
    foreign["metadata"].update(name="foreign-workload", labels={}, annotations={}, ownerReferences=[])
    inventory.pods.append(foreign)
    deletes = list(api.deletes)
    async with http:
        assert (await gateway.verify_cleanup(principal, receipt.reservation_id)).phase == "released"
    assert inventory.pods == [foreign] and api.deletes == deletes


async def test_uncertain_dispatched_create_is_not_released_by_404_or_drain(sessions):
    from loom_service.pool_management.kubernetes import PoolKubernetesWaitingError

    gateway, api, principal, receipt, http = await provider(sessions)
    async with http:
        api.lose_reply = True
        with pytest.raises(PoolKubernetesWaitingError):
            await gateway.create(principal, receipt.reservation_id, kind="Job")
        await begin_cleanup(sessions, receipt.reservation_id)
        api.objects.clear()
        with pytest.raises(ValueError):
            await gateway.verify_cleanup(principal, receipt.reservation_id)
    row, evidence = await retained(sessions, receipt)
    assert row.phase == "cleanup_intent" and not evidence and len(api.writes) == 1


@pytest.mark.parametrize("change", ["effect", "credential"])
async def test_state_or_authority_change_during_inventory_requires_new_proof(sessions, change):
    from loom_service.pool_management.pod_inventory import PoolPodReference

    gateway, api, principal, receipt, original, inventory, job = await stopped(sessions)
    changed = False

    async def handler(request):
        nonlocal changed
        if request.url.path.endswith("/pods") and not changed:
            changed = True
            if change == "credential":
                async with sessions.begin() as session:
                    await session.execute(update(Token).where(Token.token_hash == principal.token_hash).values(revoked_at=func.clock_timestamp()))
            else:
                await gateway.pod_cleanup.prepare(principal, receipt.reservation_id, pod=PoolPodReference(
                    job["metadata"]["name"] + "-late", uuid4(), "42", False))
        return inventory(request)

    await original.aclose()
    async with httpx.AsyncClient(base_url="https://kubernetes.example", transport=httpx.MockTransport(handler)) as http:
        gateway.http = http
        with pytest.raises(ValueError):
            await gateway.verify_cleanup(principal, receipt.reservation_id)
    assert changed
    row, evidence = await retained(sessions, receipt)
    assert row.phase == "cleanup_intent" and not evidence
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(NebiusPoolCleanupObservation)) == 0
