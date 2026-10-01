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


def error_response(request, status):
    parts = request.url.path.split("/")
    resource = parts[-1] if request.method == "POST" else parts[-2]
    name = json.loads(request.content)["metadata"]["name"] if request.method == "POST" else parts[-1]
    return httpx.Response(status, json={"apiVersion": "v1", "kind": "Status", "status": "Failure", "code": status,
        "reason": {404: "NotFound", 409: "AlreadyExists" if request.method == "POST" else "Conflict", 422: "Invalid"}[status],
        "message": "SENSITIVE API ERROR", "details": {"name": name, "kind": resource,
            "group": "batch" if request.url.path.startswith("/apis/batch/") else ""}})


class KubernetesAPI:
    """Only external HTTP is doubled; render, auth, transactions and journal are real."""

    def __init__(self, namespace_uid):
        self.namespace_uid = namespace_uid
        self.objects = {}
        self.writes = []
        self.deletes = []
        self.requests = []
        self.post_status = 201
        self.lose_reply = False
        self.hide_objects = False
        self.damage = None
        self.read_status = 200
        self.hold_deletion = False
        self.lose_delete_reply = False
        self.qualified_errors = True

    def __call__(self, request):
        self.requests.append((request.method, request.url.path))
        path = request.url.path
        if request.method == "DELETE":
            options = json.loads(request.content)
            self.deletes.append((path, options))
            current = self.objects.get(path)
            if current is None:
                return error_response(request, 404)
            if options["preconditions"]["uid"] != current["metadata"]["uid"]:
                return error_response(request, 409)
            if not self.hold_deletion:
                del self.objects[path]
            if self.lose_delete_reply:
                raise httpx.ReadTimeout("SENSITIVE DELETE ERROR", request=request)
            return httpx.Response(200, json={"apiVersion": "v1", "kind": "Status", "status": "Success"})
        if request.method == "POST":
            document = json.loads(request.content)
            self.writes.append(document)
            if self.post_status != 201:
                if self.qualified_errors and self.post_status in {409, 422}:
                    return error_response(request, self.post_status)
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
            return error_response(request, 404) if self.qualified_errors else httpx.Response(404)
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
    from loom_service.pool_management.kubernetes import PoolKubernetesWaitingError

    gateway, api, principal, receipt, http = await provider(sessions)
    async with http:
        api.lose_reply = True
        with pytest.raises(PoolKubernetesWaitingError, match=r"^pool_kubernetes_unconfirmed$"):
            await gateway.create(principal, receipt.reservation_id, kind="Job")
        api.hide_objects = True
        with pytest.raises(PoolKubernetesWaitingError):
            await gateway.create(principal, receipt.reservation_id, kind="Job")
        assert len(api.writes) == 1
        api.hide_objects = False
        observed = await gateway.create(principal, receipt.reservation_id, kind="Job")
        assert observed.phase == "observed" and len(api.writes) == 1


async def test_concurrent_reconcilers_create_exactly_one_job(sessions):
    gateway, api, principal, receipt, http = await provider(sessions)
    from loom_service.pool_management.kubernetes import PoolKubernetesWaitingError

    async with http:
        outcomes = await asyncio.gather(*(gateway.create(principal, receipt.reservation_id, kind="Job")
                                          for _ in range(3)), return_exceptions=True)
        assert all(not isinstance(result, Exception) or isinstance(result, PoolKubernetesWaitingError) for result in outcomes)
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


@pytest.mark.parametrize("status", [409, 422])
async def test_unqualified_rejection_retains_uncertain_create_without_retry(sessions, status):
    from loom_service.pool_management.kubernetes import PoolKubernetesError

    gateway, api, principal, receipt, http = await provider(sessions)
    api.post_status, api.qualified_errors = status, False
    async with http:
        for _ in range(2):
            with pytest.raises(PoolKubernetesError) as error:
                await gateway.create(principal, receipt.reservation_id, kind="Job")
            assert "SENSITIVE" not in str(error.value)
        async with sessions() as session:
            effect = (await session.scalars(select(NebiusPoolEffect))).one()
            assert effect.phase == "dispatched" and effect.rejection_status is None
        assert len(api.writes) == 1


@pytest.mark.parametrize("status", [404, 409, 422])
@pytest.mark.parametrize("damage", ["code", "reason", "status", "details", "name", "group", "resource"])
async def test_error_response_must_be_kubernetes_failure_for_the_exact_resource(sessions, status, damage):
    from loom_service.pool_management.kubernetes import PoolKubernetesError, PoolKubernetesRejectedError

    gateway, _api, _principal, _receipt, original = await provider(sessions)
    await original.aclose()
    method = "GET" if status == 404 else "POST"
    path = "/apis/batch/v1/namespaces/test/jobs" + ("/expected" if method == "GET" else "")
    document = {"apiVersion": "batch/v1", "kind": "Job", "metadata": {"name": "expected"}}

    def respond(request):
        response = error_response(request, status)
        value = response.json()
        if damage == "code":
            value["code"] = 500
        elif damage == "reason":
            value["reason"] = "InternalError"
        elif damage == "status":
            value["status"] = "Success"
        elif damage == "details":
            del value["details"]
        else:
            value["details"]["kind" if damage == "resource" else damage] = "foreign"
        return httpx.Response(status, json=value)

    async with httpx.AsyncClient(base_url="https://kubernetes.example", transport=httpx.MockTransport(respond)) as http:
        gateway.http = http
        with pytest.raises(PoolKubernetesError) as error:
            await gateway._request(method, path, document if method == "POST" else None)
        assert not isinstance(error.value, PoolKubernetesRejectedError)


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


@pytest.mark.parametrize("damage", ["redirect", "oversize", "invalid-json", "non-object", "timeout"])
async def test_namespace_transport_is_bounded_and_no_failure_authorizes_a_write(sessions, monkeypatch, damage):
    from loom_service.pool_management import kubernetes

    gateway, api, principal, receipt, original = await provider(sessions)
    calls = []

    async def handler(request):
        calls.append(request)
        if damage == "redirect":
            return httpx.Response(302, headers={"Location": "https://foreign.invalid/"})
        if damage == "oversize":
            return httpx.Response(200, content=b" " * (2 * 1024 * 1024 + 1))
        if damage == "invalid-json":
            return httpx.Response(200, text="SENSITIVE malformed")
        if damage == "non-object":
            return httpx.Response(200, json=[])
        await asyncio.sleep(1)
        return api(request)

    monkeypatch.setattr(kubernetes, "_TIMEOUT", 0.05)
    async with original, httpx.AsyncClient(base_url="https://kubernetes.example", follow_redirects=True,
                                           transport=httpx.MockTransport(handler)) as http:
        gateway.http = http
        with pytest.raises(kubernetes.PoolKubernetesError) as caught:
            await gateway.create(principal, receipt.reservation_id, kind="Job")
        assert "SENSITIVE" not in str(caught.value)
        assert len(calls) == 1 and calls[0].method == "GET" and not api.writes
        async with sessions() as session:
            effect = (await session.scalars(select(NebiusPoolEffect))).one()
            assert effect.phase == "prepared" and effect.dispatch_id is None


async def test_namespace_change_between_create_and_observation_remains_charged(sessions):
    from loom_service.pool_management.kubernetes import PoolKubernetesError

    gateway, api, principal, receipt, original = await provider(sessions)

    def handler(request):
        response = api(request)
        if request.method == "POST":
            api.namespace_uid = uuid4()
        return response

    async with original, httpx.AsyncClient(base_url="https://kubernetes.example", transport=httpx.MockTransport(handler)) as http:
        gateway.http = http
        with pytest.raises(PoolKubernetesError):
            await gateway.create(principal, receipt.reservation_id, kind="Job")
        async with sessions() as session:
            effect = (await session.scalars(select(NebiusPoolEffect))).one()
            row = await session.get(NebiusPoolRequest, receipt.reservation_id)
            assert effect.phase == "dispatched" and row.phase == "create_intent"
            assert row.job_uid is None and row.cleanup_observation_id is None


@pytest.mark.parametrize("kind", ["Job", "ConfigMap"])
async def test_exact_uid_delete_never_counts_as_complete_capacity_cleanup(sessions, kind):
    from tests.integration.test_nebius_pool_cleanup_journal import begin_cleanup

    gateway, api, principal, receipt, http = await provider(sessions, build=kind == "ConfigMap")
    async with http:
        created = await gateway.create(principal, receipt.reservation_id, kind=kind)
        await begin_cleanup(sessions, receipt.reservation_id)
        deleted = await gateway.delete(principal, receipt.reservation_id, kind=kind)
        assert deleted.phase == "observed"
        assert len(api.deletes) == 1
        assert api.deletes[0][1]["preconditions"] == {"uid": str(created.observed_uid)}
        assert await gateway.delete(principal, receipt.reservation_id, kind=kind) == deleted
        assert len(api.deletes) == 1
        async with sessions() as session:
            row = await session.get(NebiusPoolRequest, receipt.reservation_id)
            assert row.phase == "cleanup_intent" and row.cleanup_observation_id is None


@pytest.mark.parametrize("uncertainty", ["lost-reply", "still-terminating"])
async def test_delete_uncertainty_reconciles_without_a_second_write(sessions, uncertainty):
    from loom_service.pool_management.kubernetes import PoolKubernetesWaitingError
    from tests.integration.test_nebius_pool_cleanup_journal import begin_cleanup

    gateway, api, principal, receipt, http = await provider(sessions)
    async with http:
        await gateway.create(principal, receipt.reservation_id, kind="Job")
        await begin_cleanup(sessions, receipt.reservation_id)
        api.hold_deletion = uncertainty == "still-terminating"
        api.lose_delete_reply = uncertainty == "lost-reply"
        with pytest.raises(PoolKubernetesWaitingError):
            await gateway.delete(principal, receipt.reservation_id, kind="Job")
        if api.hold_deletion:
            with pytest.raises(PoolKubernetesWaitingError):
                await gateway.delete(principal, receipt.reservation_id, kind="Job")
            assert len(api.deletes) == 1
            path, _ = api.deletes[0]
            del api.objects[path]
        assert (await gateway.delete(principal, receipt.reservation_id, kind="Job")).phase == "observed"
        assert len(api.deletes) == 1


@pytest.mark.parametrize("after_dispatch", [False, True])
async def test_replacement_is_never_deleted_or_used_as_absence_proof(sessions, after_dispatch):
    from loom_service.pool_management.kubernetes import (
        PoolKubernetesError,
        PoolKubernetesWaitingError,
    )
    from tests.integration.test_nebius_pool_cleanup_journal import begin_cleanup

    gateway, api, principal, receipt, http = await provider(sessions)
    async with http:
        await gateway.create(principal, receipt.reservation_id, kind="Job")
        await begin_cleanup(sessions, receipt.reservation_id)
        if after_dispatch:
            api.hold_deletion = True
            with pytest.raises(PoolKubernetesWaitingError):
                await gateway.delete(principal, receipt.reservation_id, kind="Job")
        job, = api.objects.values()
        job["metadata"]["uid"] = str(uuid4())
        with pytest.raises(PoolKubernetesError):
            await gateway.delete(principal, receipt.reservation_id, kind="Job")
        assert len(api.deletes) == int(after_dispatch)


async def test_missing_owned_job_requires_no_delete_but_remains_charged(sessions):
    from tests.integration.test_nebius_pool_cleanup_journal import begin_cleanup

    gateway, api, principal, receipt, http = await provider(sessions)
    async with http:
        await gateway.create(principal, receipt.reservation_id, kind="Job")
        api.objects.clear()  # Does not establish absence of residual Pods.
        await begin_cleanup(sessions, receipt.reservation_id)
        assert (await gateway.delete(principal, receipt.reservation_id, kind="Job")).phase == "observed"
        assert not api.deletes
        async with sessions() as session:
            assert (await session.get(NebiusPoolRequest, receipt.reservation_id)).phase == "cleanup_intent"
