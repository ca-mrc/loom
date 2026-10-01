"""Retire residual Pods only after their Job is gone and outputs are drained."""
from __future__ import annotations

import asyncio
import copy
from uuid import UUID, uuid4

import httpx
import pytest
from sqlalchemy import select

from loom.db.nebius_pool_schema import NebiusPoolEffect, NebiusPoolRequest
from tests.integration.test_nebius_pool_cleanup_journal import begin_cleanup
from tests.integration.test_nebius_pool_kubernetes import provider
from tests.integration.test_nebius_pool_pod_inventory import pod
from tests.integration.test_nebius_pool_registry import sessions as sessions


async def residual(sessions, *, drained=True, job_gone=True):
    from loom_service.pool_management.pod_inventory import PoolPodReference

    gateway, api, principal, receipt, http = await provider(sessions)
    created = await gateway.create(principal, receipt.reservation_id, kind="Job")
    job = next(value for value in api.objects.values() if value["kind"] == "Job")
    child = pod(job)
    path = "/api/v1/namespaces/" + job["metadata"]["namespace"] + "/pods/" + child["metadata"]["name"]
    api.objects[path] = copy.deepcopy(child)
    await begin_cleanup(sessions, receipt.reservation_id, drained=drained)
    if job_gone:
        await gateway.delete(principal, receipt.reservation_id, kind="Job")
    metadata = child["metadata"]
    ref = PoolPodReference(metadata["name"], UUID(metadata["uid"]), metadata["resourceVersion"], False)
    await http.aclose()
    http = httpx.AsyncClient(base_url="https://kubernetes.example", transport=httpx.MockTransport(api))
    gateway.http = http
    return gateway, api, principal, receipt, http, created, ref, path


async def test_pod_delete_is_bound_to_retained_parent_and_committed_before_http(sessions):
    gateway, api, principal, receipt, original, created, ref, path = await residual(sessions)

    async def handler(request):
        if request.method == "DELETE" and request.url.path == path:
            async with sessions() as session:
                effect = await session.scalar(select(NebiusPoolEffect).where(
                    NebiusPoolEffect.effect_key == "delete:pod:" + ref.uid.hex))
                assert effect.phase == "dispatched" and effect.dispatch_id is not None
                assert effect.intent_json["uid"] == str(ref.uid)
                assert effect.intent_json["create_effect_id"] == str(created.effect_id)
        return api(request)

    await original.aclose()
    async with httpx.AsyncClient(base_url="https://kubernetes.example", transport=httpx.MockTransport(handler)) as http:
        gateway.http = http
        deleted = await gateway.delete_pod(principal, receipt.reservation_id, pod=ref)
        assert deleted.phase == "observed"
        assert await gateway.delete_pod(principal, receipt.reservation_id, pod=ref) == deleted
    assert path not in api.objects
    deletes = [body for name, body in api.deletes if name == path]
    assert deletes == [{"apiVersion": "v1", "kind": "DeleteOptions", "gracePeriodSeconds": 0,
        "preconditions": {"uid": str(ref.uid)}}]
    async with sessions() as session:
        row = await session.get(NebiusPoolRequest, receipt.reservation_id)
        assert row.phase == "cleanup_intent" and row.cleanup_observation_id is None


@pytest.mark.parametrize("drained,job_gone", [(False, True), (True, False)])
async def test_pod_delete_needs_output_drain_and_observed_job_absence(sessions, drained, job_gone):
    gateway, api, principal, receipt, http, _, ref, path = await residual(sessions, drained=drained, job_gone=job_gone)
    async with http:
        with pytest.raises(ValueError):
            await gateway.delete_pod(principal, receipt.reservation_id, pod=ref)
    assert path in api.objects and not any(name == path for name, _ in api.deletes)


@pytest.mark.parametrize("damage", ["uid", "owner", "plan", "namespace"])
async def test_pod_identity_conflict_does_not_gain_delete_authority(sessions, damage):
    gateway, api, principal, receipt, http, _, ref, path = await residual(sessions)
    metadata = api.objects[path]["metadata"]
    if damage == "uid":
        metadata["uid"] = str(uuid4())
    elif damage == "owner":
        metadata["ownerReferences"][0]["uid"] = str(uuid4())
    elif damage == "plan":
        metadata["annotations"]["loom.nebius/pool-plan-sha256"] = "a" * 64
    else:
        api.namespace_uid = uuid4()
    async with http:
        with pytest.raises(ValueError):
            await gateway.delete_pod(principal, receipt.reservation_id, pod=ref)
    assert path in api.objects and not any(name == path for name, _ in api.deletes)


async def test_lost_pod_delete_reply_never_resends_and_recovers_absence(sessions):
    from loom_service.pool_management.kubernetes import PoolKubernetesWaitingError

    gateway, api, principal, receipt, http, _, ref, path = await residual(sessions)
    async with http:
        api.lose_delete_reply = True
        with pytest.raises(PoolKubernetesWaitingError):
            await gateway.delete_pod(principal, receipt.reservation_id, pod=ref)
        assert path not in api.objects
        assert (await gateway.delete_pod(principal, receipt.reservation_id, pod=ref)).phase == "observed"
    assert len([item for item in api.deletes if item[0] == path]) == 1


async def test_concurrent_pod_deletion_has_one_dispatch_and_never_releases_capacity(sessions):
    from loom_service.pool_management.kubernetes import PoolKubernetesWaitingError

    gateway, api, principal, receipt, http, _, ref, path = await residual(sessions)
    async with http:
        outcomes = await asyncio.gather(*(gateway.delete_pod(principal, receipt.reservation_id, pod=ref)
                                         for _ in range(3)), return_exceptions=True)
    assert all(not isinstance(outcome, Exception) or isinstance(outcome, PoolKubernetesWaitingError) for outcome in outcomes)
    assert any(not isinstance(outcome, Exception) and outcome.phase == "observed" for outcome in outcomes)
    assert len([item for item in api.deletes if item[0] == path]) == 1
    async with sessions() as session:
        assert (await session.get(NebiusPoolRequest, receipt.reservation_id)).phase == "cleanup_intent"
