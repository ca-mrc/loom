"""Cleanup reads every bound Pod; Job absence or terminal Pod status is not release."""
from __future__ import annotations

import copy
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import select

from loom.db.nebius_pool_schema import NebiusPoolEffect
from tests.integration.test_nebius_pool_kubernetes import provider
from tests.integration.test_nebius_pool_registry import sessions as sessions


def pod(job, *, suffix="first", phase="Running"):
    metadata = copy.deepcopy(job["spec"]["template"]["metadata"])
    metadata.update(name=job["metadata"]["name"] + "-" + suffix, namespace=job["metadata"]["namespace"],
        uid=str(uuid4()), resourceVersion="21", ownerReferences=[{"kind": "Job", "apiVersion": "batch/v1",
            "name": job["metadata"]["name"], "uid": job["metadata"]["uid"], "controller": True, "blockOwnerDeletion": True}])
    return {"apiVersion": "v1", "kind": "Pod", "metadata": metadata,
            "spec": copy.deepcopy(job["spec"]["template"]["spec"]), "status": {"phase": phase}}


class InventoryAPI:
    def __init__(self, api, pods):
        self.api, self.pods = api, pods
        self.page_size = 1
        self.list_rv = "30"
        self.damage = None
        self.pages = []

    def __call__(self, request):
        if request.url.path.endswith("/pods"):
            assert request.method == "GET" and request.url.params.get("limit")
            cursor = request.url.params.get("continue", "")
            self.pages.append(cursor)
            offset = int(cursor or "0")
            following = offset + self.page_size
            value = {"apiVersion": "v1", "kind": "PodList", "metadata": {"resourceVersion": self.list_rv,
                "continue": str(following) if following < len(self.pods) else ""},
                "items": copy.deepcopy(self.pods[offset:following])}
            if self.damage:
                self.damage(value, offset)
            return httpx.Response(200, json=value)
        return self.api(request)


@pytest.mark.parametrize("build", [False, True])
async def test_all_bound_pods_remain_visible_after_job_deletion_and_across_pages(sessions, build):
    gateway, api, principal, receipt, original = await provider(sessions, build=build)
    if build:
        await gateway.create(principal, receipt.reservation_id, kind="ConfigMap")
    created = await gateway.create(principal, receipt.reservation_id, kind="Job")
    job = next(value for value in api.objects.values() if value["kind"] == "Job")
    children = [pod(job, suffix="pending", phase="Pending"), pod(job, suffix="done", phase="Succeeded")]
    children[1]["metadata"]["deletionTimestamp"] = "2026-09-30T01:00:00Z"
    foreign = pod(job, suffix="foreign")
    foreign["metadata"].update(name="unrelated", labels={}, annotations={}, ownerReferences=[])
    handler = InventoryAPI(api, [children[0], foreign, children[1]])
    api.objects.clear()  # Residual Pods are still charged even with no Job.
    before = list(api.writes)
    async with original, httpx.AsyncClient(base_url="https://kubernetes.example", transport=httpx.MockTransport(handler)) as http:
        gateway.http = http
        inventory = await gateway.pod_inventory(principal, receipt.reservation_id)
        assert inventory.reservation_id == receipt.reservation_id and inventory.job_uid == created.observed_uid
        assert inventory.namespace_uid == created.namespace_uid and inventory.resource_version == "30"
        assert {str(item.uid) for item in inventory.pods} == {item["metadata"]["uid"] for item in children}
        assert len(inventory.pods) == 2 and sum(item.terminating for item in inventory.pods) == 1
        assert handler.pages == ["", "1", "2"] and api.writes == before and not api.deletes
        async with sessions() as session:
            assert all(row.intent_json["action"] == "create" for row in (await session.scalars(select(NebiusPoolEffect))).all())


@pytest.mark.parametrize("damage", ["owner-uid", "owner-name", "owner-kind", "controller", "multiple-owners",
                                    "plan", "effect", "reservation", "claim", "namespace", "name", "identity-erased"])
async def test_spoofed_or_changed_bound_pod_is_not_treated_as_foreign_absence(sessions, damage):
    from loom_service.pool_management.kubernetes import PoolKubernetesError

    gateway, api, principal, receipt, original = await provider(sessions)
    await gateway.create(principal, receipt.reservation_id, kind="Job")
    job, = api.objects.values()
    child = pod(job)
    metadata = child["metadata"]
    owner = metadata["ownerReferences"][0]
    if damage.startswith("owner-"):
        owner[damage.removeprefix("owner-")] = str(uuid4())
    elif damage == "controller":
        owner["controller"] = False
    elif damage == "multiple-owners":
        metadata["ownerReferences"].append(owner | {"uid": str(uuid4())})
    elif damage in {"plan", "effect", "reservation"}:
        key = {"plan": "pool-plan-sha256", "effect": "pool-effect-id", "reservation": "pool-reservation-id"}[damage]
        metadata["annotations"]["loom.nebius/" + key] = "changed"
    elif damage == "claim":
        metadata["labels"]["loom.openai.com/generation"] = "99"
    elif damage == "namespace":
        metadata["namespace"] = "other"
    elif damage == "name":
        metadata["name"] = "not-the-generated-pod"
    else:
        metadata.update(labels={}, annotations={}, ownerReferences=[])
    handler = InventoryAPI(api, [child])
    async with original, httpx.AsyncClient(base_url="https://kubernetes.example", transport=httpx.MockTransport(handler)) as http:
        gateway.http = http
        with pytest.raises(PoolKubernetesError):
            await gateway.pod_inventory(principal, receipt.reservation_id)


@pytest.mark.parametrize("damage", ["changed-rv", "repeat-cursor", "duplicate-uid", "duplicate-name", "missing-items", "namespace-replaced"])
async def test_partial_or_inconsistent_inventory_cannot_prove_absence(sessions, damage):
    from loom_service.pool_management.kubernetes import PoolKubernetesError

    gateway, api, principal, receipt, original = await provider(sessions)
    await gateway.create(principal, receipt.reservation_id, kind="Job")
    job, = api.objects.values()
    children = [pod(job), pod(job, suffix="second")]
    if damage == "duplicate-uid":
        children[1]["metadata"]["uid"] = children[0]["metadata"]["uid"]
    if damage == "duplicate-name":
        children[1]["metadata"]["name"] = children[0]["metadata"]["name"]
    handler = InventoryAPI(api, children)

    def corrupt(value, offset):
        if damage == "changed-rv" and offset:
            value["metadata"]["resourceVersion"] = "31"
        if damage == "repeat-cursor":
            value["metadata"]["continue"] = "1"
        if damage == "missing-items":
            value.pop("items")
        if damage == "namespace-replaced":
            api.namespace_uid = uuid4()

    handler.damage = corrupt
    async with original, httpx.AsyncClient(base_url="https://kubernetes.example", transport=httpx.MockTransport(handler)) as http:
        gateway.http = http
        with pytest.raises(PoolKubernetesError):
            await gateway.pod_inventory(principal, receipt.reservation_id)


async def test_inventory_never_prepares_an_unobserved_job_as_a_side_effect(sessions):
    from loom_service.pool_management.gateway_journal import PoolGatewayError

    gateway, api, principal, receipt, http = await provider(sessions)
    async with http:
        with pytest.raises(PoolGatewayError):
            await gateway.pod_inventory(principal, receipt.reservation_id)
        async with sessions() as session:
            assert (await session.scalars(select(NebiusPoolEffect))).all() == []
        assert not api.writes
