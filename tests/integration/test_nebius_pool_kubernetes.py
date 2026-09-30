"""Real SQL dispatch journal at the Kubernetes HTTP boundary (not RBAC evidence)."""
from __future__ import annotations

import asyncio
import copy
import json
from uuid import UUID, uuid4

import httpx
import pytest
from sqlalchemy import select

from loom.db.nebius_pool_schema import NebiusPoolEffect, NebiusPoolRequest
from tests.integration.test_nebius_pool_gateway_journal import setup
from tests.integration.test_nebius_pool_registry import sessions as sessions


class KubernetesAPI:
    """Only external HTTP is doubled; render, auth, transactions and journal are real."""

    def __init__(self, namespace_uid):
        self.namespace_uid = namespace_uid
        self.objects = {}
        self.writes = []
        self.requests = []
        self.post_status = 201
        self.lose_reply = False
        self.hide_objects = False
        self.damage = None
        self.read_status = 200

    def __call__(self, request):
        self.requests.append((request.method, request.url.path))
        path = request.url.path
        if request.method == "POST":
            document = json.loads(request.content)
            self.writes.append(document)
            if self.post_status != 201:
                return httpx.Response(self.post_status, text="SENSITIVE API ERROR")
            document["metadata"].update(uid=str(uuid4()), resourceVersion="11")
            if document["kind"] == "Job":
                uid, name = document["metadata"]["uid"], document["metadata"]["name"]
                spec = document["spec"]
                spec.update(completionMode="NonIndexed", suspend=False,
                            podReplacementPolicy="TerminatingOrFailed")
                spec["selector"] = {"matchLabels": {"batch.kubernetes.io/controller-uid": uid}}
                spec["template"]["metadata"]["labels"].update({
                    "batch.kubernetes.io/controller-uid": uid, "controller-uid": uid,
                    "batch.kubernetes.io/job-name": name, "job-name": name})
                pod = spec["template"]["spec"]
                pod.update(dnsPolicy="ClusterFirst", schedulerName="default-scheduler",
                           serviceAccount=pod["serviceAccountName"])
                for container in pod["containers"] + pod.get("initContainers", []):
                    container.setdefault("terminationMessagePath", "/dev/termination-log")
                    container.setdefault("terminationMessagePolicy", "File")
                for volume in pod["volumes"]:
                    for source in ("configMap", "secret"):
                        if source in volume:
                            volume[source].setdefault("defaultMode", 420)
            self.objects[path + "/" + document["metadata"]["name"]] = copy.deepcopy(document)
            if self.lose_reply:
                raise httpx.ReadTimeout("SENSITIVE TRANSPORT ERROR", request=request)
            return httpx.Response(201, json=document)
        assert request.method == "GET"
        if self.read_status != 200:
            return httpx.Response(self.read_status, headers={"Location": "https://foreign.invalid/"}, text="SENSITIVE")
        if path.count("/") == 4:
            return httpx.Response(200, json={"apiVersion": "v1", "kind": "Namespace", "metadata": {
                "name": path.rsplit("/", 1)[-1], "uid": str(self.namespace_uid), "resourceVersion": "10"}})
        value = copy.deepcopy(self.objects.get(path))
        if value is None or self.hide_objects:
            return httpx.Response(404)
        if self.damage:
            self.damage(value)
        return httpx.Response(200, json=value)


async def provider(sessions, *, build=False):
    from loom_service.pool_management.kubernetes import KubernetesPoolGateway

    journal, principal, receipt, _ = await setup(sessions, build=build)
    async with sessions() as session:
        row = await session.get(NebiusPoolRequest, receipt.reservation_id)
        api = KubernetesAPI(row.namespace_uid)
    http = httpx.AsyncClient(base_url="https://kubernetes.example", transport=httpx.MockTransport(api))
    return KubernetesPoolGateway(journal, http), api, principal, receipt, http


@pytest.mark.parametrize("build", [False, True])
async def test_fixed_create_commits_before_http_and_observes_real_defaulted_document(sessions, build):
    gateway, api, principal, receipt, http = await provider(sessions, build=build)
    base_handler = api.__call__

    async def handler(request):
        if request.method == "POST":
            body = json.loads(request.content)
            effect_id = UUID(body["metadata"]["annotations"]["loom.nebius/pool-effect-id"])
            async with sessions() as session:
                effect = await session.get(NebiusPoolEffect, effect_id)
                assert effect.phase == "dispatched" and effect.dispatch_id is not None
        return base_handler(request)

    async with httpx.AsyncClient(base_url="https://kubernetes.example", transport=httpx.MockTransport(handler)) as checked_http:
        gateway.http = checked_http
        if build:
            auxiliary = await gateway.create(principal, receipt.reservation_id, kind="ConfigMap")
            assert auxiliary.phase == "observed"
        observed = await gateway.create(principal, receipt.reservation_id, kind="Job")
        replay = await gateway.create(principal, receipt.reservation_id, kind="Job")
        assert observed.phase == "observed" and replay == observed
        assert [value["kind"] for value in api.writes] == (["ConfigMap", "Job"] if build else ["Job"])
        async with sessions() as session:
            row = await session.get(NebiusPoolRequest, receipt.reservation_id)
            assert row.phase == "observed" and row.job_uid == observed.observed_uid
            assert row.cleanup_observation_id is None
    await http.aclose()


async def test_lost_post_reply_then_404_does_not_resend_and_eventually_observes(sessions):
    from loom_service.pool_management.kubernetes import PoolKubernetesWaiting

    gateway, api, principal, receipt, http = await provider(sessions)
    async with http:
        api.lose_reply = True
        with pytest.raises(PoolKubernetesWaiting, match=r"^pool_kubernetes_unconfirmed$"):
            await gateway.create(principal, receipt.reservation_id, kind="Job")
        api.hide_objects = True
        with pytest.raises(PoolKubernetesWaiting):
            await gateway.create(principal, receipt.reservation_id, kind="Job")
        assert len(api.writes) == 1
        api.hide_objects = False
        observed = await gateway.create(principal, receipt.reservation_id, kind="Job")
        assert observed.phase == "observed" and len(api.writes) == 1


async def test_concurrent_reconcilers_create_exactly_one_job(sessions):
    gateway, api, principal, receipt, http = await provider(sessions)
    from loom_service.pool_management.kubernetes import PoolKubernetesWaiting

    async with http:
        outcomes = await asyncio.gather(*(gateway.create(principal, receipt.reservation_id, kind="Job")
                                          for _ in range(3)), return_exceptions=True)
        assert all(not isinstance(result, Exception) or isinstance(result, PoolKubernetesWaiting) for result in outcomes)
        assert any(not isinstance(result, Exception) and result.phase == "observed" for result in outcomes)
        assert len(api.writes) == 1


@pytest.mark.parametrize("status", [409, 422, 403, 429, 500, 302])
async def test_write_failure_is_secret_free_and_never_retried(sessions, status):
    from loom_service.pool_management.kubernetes import PoolKubernetesError

    gateway, api, principal, receipt, http = await provider(sessions)
    async with http:
        api.post_status = status
        for _ in range(2):
            with pytest.raises(PoolKubernetesError) as caught:
                await gateway.create(principal, receipt.reservation_id, kind="Job")
            assert "SENSITIVE" not in str(caught.value)
        assert len(api.writes) == 1
        async with sessions() as session:
            effect = (await session.scalars(select(NebiusPoolEffect))).one()
            assert effect.phase == ("rejected" if status in {409, 422} else "dispatched")
            assert (await session.get(NebiusPoolRequest, receipt.reservation_id)).phase == "create_intent"


@pytest.mark.parametrize("when", ["before", "after"])
async def test_replaced_namespace_never_gains_create_or_observation_authority(sessions, when):
    from loom_service.pool_management.kubernetes import PoolKubernetesError

    gateway, api, principal, receipt, http = await provider(sessions)
    async with http:
        if when == "after":
            await gateway.create(principal, receipt.reservation_id, kind="Job")
        api.namespace_uid = uuid4()
        with pytest.raises(PoolKubernetesError):
            await gateway.create(principal, receipt.reservation_id, kind="Job")
        assert len(api.writes) == (1 if when == "after" else 0)


@pytest.mark.parametrize("damage", ["uid", "marker", "privileged", "host", "mount", "selector", "sidecar",
                                    "resources", "env", "deleting", "deadline", "owner", "job-selector"])
async def test_changed_runtime_or_identity_is_not_adopted(sessions, damage):
    from loom_service.pool_management.kubernetes import PoolKubernetesError

    gateway, api, principal, receipt, http = await provider(sessions)
    async with http:
        await gateway.create(principal, receipt.reservation_id, kind="Job")

        def corrupt(value):
            pod = value["spec"]["template"]["spec"]
            container = pod["containers"][0]
            if damage == "uid":
                value["metadata"]["uid"] = str(uuid4())
            elif damage == "marker":
                value["metadata"]["annotations"]["loom.nebius/pool-effect-id"] = str(uuid4())
            elif damage == "privileged":
                container["securityContext"]["privileged"] = True
            elif damage == "host":
                pod["hostNetwork"] = True
            elif damage == "mount":
                container["volumeMounts"][0]["subPath"] = "other"
            elif damage == "selector":
                pod["nodeSelector"]["unqualified"] = "true"
            elif damage == "sidecar":
                pod["containers"].append({"name": "extra", "image": "untrusted"})
            elif damage == "resources":
                container["resources"]["requests"]["cpu"] = "10"
            elif damage == "env":
                container["env"][0]["valueFrom"] = {"secretKeyRef": {"name": "other", "key": "token"}}
            elif damage == "deleting":
                value["metadata"]["deletionTimestamp"] = "2026-09-30T01:00:00Z"
            elif damage == "deadline":
                value["spec"]["activeDeadlineSeconds"] += 300
            elif damage == "owner":
                value["metadata"]["ownerReferences"] = [{"uid": str(uuid4())}]
            else:
                value["spec"]["selector"]["matchLabels"]["unexpected"] = "foreign"

        api.damage = corrupt
        with pytest.raises(PoolKubernetesError):
            await gateway.create(principal, receipt.reservation_id, kind="Job")
        assert len(api.writes) == 1


@pytest.mark.parametrize("damage", ["uid", "data", "missing", "extra-data"])
async def test_build_rechecks_live_exact_auxiliary_before_first_job_dispatch(sessions, damage):
    from loom_service.pool_management.kubernetes import PoolKubernetesError

    gateway, api, principal, receipt, http = await provider(sessions, build=True)
    async with http:
        await gateway.create(principal, receipt.reservation_id, kind="ConfigMap")
        configmap, = api.objects.values()
        if damage == "uid":
            configmap["metadata"]["uid"] = str(uuid4())
        elif damage == "data":
            configmap["data"]["claim.json"] = "{}"
        elif damage == "extra-data":
            configmap["data"]["credentials"] = "unexpected"
        else:
            api.objects.clear()
        with pytest.raises(PoolKubernetesError):
            await gateway.create(principal, receipt.reservation_id, kind="Job")
        assert len(api.writes) == 1
        async with sessions() as session:
            job = (await session.scalars(select(NebiusPoolEffect).where(NebiusPoolEffect.effect_key == "create:job"))).one()
            assert job.phase == "prepared" and job.dispatch_id is None


@pytest.mark.parametrize("url", ["http://kubernetes.example", "https://user:secret@kubernetes.example",
                               "https://kubernetes.example/prefix", "https://kubernetes.example/?token=secret"])
async def test_endpoint_cannot_redirect_or_embed_authority(sessions, url):
    from loom_service.pool_management.kubernetes import KubernetesPoolGateway

    journal, _, _, _ = await setup(sessions)
    async with httpx.AsyncClient(base_url=url) as http:
        with pytest.raises(ValueError):
            KubernetesPoolGateway(journal, http)
