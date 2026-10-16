"""Whole journal through real management/child DBs; only cloud/Kubernetes are fake."""

from __future__ import annotations

import asyncio
import base64
import copy
import json
import tomllib
from uuid import uuid4

import httpx

from loom.admin_secret import AdminSecretVerifier
from loom_service.app import create_app
from loom_service.config import LoomServiceSettings
from tests.integration.test_nebius_environment_isolation import (
    second_child_database as second_child_database,
)
from tests.integration.test_nebius_environment_management import (
    environment_registry as environment_registry,
)
from tests.support.nebius_platform import platform_inputs as platform_inputs


async def test_full_provider_recovers_bucket_reply_loss_and_readies_owner_without_enabling_execution(
    environment_registry, second_child_database, monkeypatch, tmp_path,
):
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from loom_service.environment_management.child_client import ChildEnvironmentClient
    from loom_service.environment_management.cloud_provider import NebiusEnvironmentCloudProvider
    from loom_service.environment_management.credentials import EnvironmentCredentialProvider
    from loom_service.environment_management.kubernetes_provider import (
        KubernetesEnvironmentProvider,
    )
    from loom_service.environment_management.provider import ProviderRetryError
    from loom_service.environment_management.provisioner import EnvironmentProvisioner
    from loom_service.environment_management.worker import EnvironmentWorker

    monkeypatch.setenv("LOOM_SECRET_STORE_MASTER_KEY", base64.b64encode(b"m" * 32).decode())
    monkeypatch.delenv("LOOM_SECRET_STORE_MASTER_KEYS", raising=False)
    registry, _, (alice, _), prepare = environment_registry
    prepared = prepare()
    operation = await registry.create(principal=alice, idempotency_key="full-provider", prepared=prepared)
    path = tmp_path / "environment.json"
    path.write_text(json.dumps(prepared.config))
    settings = LoomServiceSettings(_env_file=None, db_url=second_child_database, minio_access_key="fake-only",
                                   minio_secret_key="fake-only", public_base_url="https://" + prepared.registration.public_host,
                                   managed_environment_config_file=path)
    child = create_app(settings)
    engine = create_async_engine(second_child_database)
    child.state.settings = settings
    child.state.session_factory = async_sessionmaker(engine, expire_on_commit=False)

    class Cloud:
        def __init__(self):
            self.objects = {}
            self.fail = True

        @staticmethod
        def key(kind, expected):
            return (kind, expected["metadata"]["parent_id"], expected["metadata"].get("name", expected["spec"].get("member_id")))

        async def find(self, kind, expected):
            return copy.deepcopy(self.objects.get(self.key(kind, expected)))

        async def create(self, kind, expected, *, idempotency_key):
            value = copy.deepcopy(expected)
            value["metadata"]["id"] = "provider-" + str(uuid4())
            self.objects[self.key(kind, expected)] = value
            if kind == "bucket" and self.fail:
                self.fail = False
                raise ProviderRetryError("lost_bucket_reply")
            return value

        async def access_key_secret(self, identity):
            assert any(row["metadata"]["id"] == identity for row in self.objects.values())
            return {"access-key": "key-" + identity, "secret-key": "secret-" + identity}

        async def revoke_access_key(self, identity, expected, *, idempotency_key):
            key = self.key("access_key", expected)
            assert self.objects[key]["metadata"]["id"] == identity
            del self.objects[key]

    resources = {}
    pod_present = False
    terminal_pod_present = False
    inspected_pods = asyncio.Event()

    def kubernetes(request):
        nonlocal terminal_pod_present
        if request.method == "GET":
            if request.url.path.endswith("/pods"):
                inspected_pods.set()
                pods = []
                if request.url.path == f"/api/v1/namespaces/{prepared.registration.application_namespace}/pods":
                    if pod_present:
                        pods.append({"metadata": {"name": "terminating", "uid": "pod-uid"}, "status": {"phase": "Running"}})
                    if terminal_pod_present:
                        pods.append(resources[request.url.path + "/finished-backup"])
                return httpx.Response(200, json={"items": pods})
            if request.url.path.endswith(("/jobs", "/replicasets")):
                return httpx.Response(200, json={"items": [doc for path, doc in resources.items()
                                                           if path.rsplit("/", 1)[0] == request.url.path]})
            value = resources.get(request.url.path)
            return httpx.Response(200, json=value) if value is not None else httpx.Response(404)
        if request.method == "PATCH":
            value = resources[request.url.path]
            patch = json.loads(request.content)
            assert patch[:2] == [{"op": "test", "path": "/metadata/uid", "value": value["metadata"]["uid"]},
                                 {"op": "test", "path": "/metadata/resourceVersion", "value": value["metadata"]["resourceVersion"]}]
            for edit in patch[2:]:
                if edit["path"] == "/spec":
                    value["spec"] = edit["value"]
                elif edit["path"] == "/metadata/annotations/loom.nebius~1retained-by":
                    value["metadata"]["annotations"]["loom.nebius/retained-by"] = edit["value"]
                elif edit["path"] == "/metadata/annotations":
                    value["metadata"]["annotations"] = edit["value"]
                else:
                    raise AssertionError(edit)
            value["metadata"].update(resourceVersion="2", generation=2)
            value["status"] = {"observedGeneration": 2, "replicas": 0, "readyReplicas": 0,
                               "conditions": [{"type": "Suspended", "status": "True"}]}
            return httpx.Response(200, json=value)
        if request.method == "DELETE":
            assert request.url.path.endswith("/pods/finished-backup")
            assert json.loads(request.content)["preconditions"]["uid"] == "terminal-pod-uid"
            terminal_pod_present = False
            del resources[request.url.path]
            return httpx.Response(200, json={"kind": "Status", "status": "Success"})
        assert request.method == "POST"
        value = json.loads(request.content)
        value["metadata"].update(uid=str(uuid4()), generation=1, resourceVersion="1")
        if value["kind"] == "Job":
            value["status"] = {"conditions": [{"type": "Complete", "status": "True"}]}
        elif value["kind"] in {"Deployment", "StatefulSet"}:
            value["status"] = {"observedGeneration": 1, "replicas": 1, "readyReplicas": 1,
                               "updatedReplicas": 1, "availableReplicas": 1}
        elif value["kind"] == "ResourceQuota":
            value["status"] = {"hard": copy.deepcopy(value["spec"]["hard"]), "used": {"pods": "0"}}
        if value["kind"] == "Secret" and value["metadata"]["name"] == "loom-admin-secret":
            token = tomllib.loads(base64.b64decode(value["data"]["secrets.toml"]).decode())["admin"]["token"]
            child.state.admin_secret_verifier = AdminSecretVerifier.from_token(token)
        resources[request.url.path + "/" + value["metadata"]["name"]] = value
        return httpx.Response(201, json=value)

    cloud = Cloud()
    try:
        async with httpx.AsyncClient(base_url="https://kubernetes.test", transport=httpx.MockTransport(kubernetes)) as kube_http, \
                httpx.AsyncClient(transport=httpx.ASGITransport(app=child)) as child_http:
            kube = KubernetesEnvironmentProvider(kube_http)
            provider = EnvironmentProvisioner(registry, kubernetes=kube, cloud=NebiusEnvironmentCloudProvider(cloud),
                                              credentials=EnvironmentCredentialProvider(registry, cloud, kube),
                                              child=ChildEnvironmentClient(child_http))
            worker = EnvironmentWorker(registry, provider)
            await worker.reconcile_once(operation.operation_id)
            assert (await registry.get_operation(operation.operation_id, principal=alice)).error_code == "lost_bucket_reply"
            await worker.reconcile_once(operation.operation_id)
            status = await registry.get_operation(operation.operation_id, principal=alice)
            assert status.phase == "completed", status
            assert status.execution_enabled is False
            assert len(cloud.objects) == 16  # 3 x account/group/member/key + 4 buckets.
            row, material = await registry.ready_access(operation.environment_id, principal=alice)
            token = tomllib.loads(material["loom-admin-secret"]["secrets.toml"])["admin"]["token"]
            proof = await ChildEnvironmentClient(child_http).login(row, admin_token=token)
            logged_in = await child_http.post("https://" + row.public_host + "/api/v1/auth/login/complete",
                                              json={"token": proof["login_token"]})
            assert logged_in.status_code == 200
            quotas = [doc for doc in resources.values() if doc["kind"] == "ResourceQuota"]
            assert len(quotas) == 2 and all(doc["spec"]["hard"]["pods"] == "0" for doc in quotas)
            storage = next(doc for doc in resources.values() if doc["kind"] == "Secret" and doc["metadata"]["name"] == "loom-platform-storage")
            assert len({storage["data"][name] for name in ("secret-key", "source-secret-key", "backup-secret-key")}) == 3
            from loom.db.nebius_environment_schema import NebiusPlatformReservation

            pod_present = True
            terminal_pod_present = True
            cron = next(doc for doc in resources.values() if doc["kind"] == "CronJob")
            backup_job = {"apiVersion": "batch/v1", "kind": "Job", "metadata": {
                "name": "scheduled-backup", "namespace": prepared.registration.application_namespace,
                "uid": "backup-job-uid", "resourceVersion": "1", "generation": 1,
                "ownerReferences": [{"kind": "CronJob", "name": cron["metadata"]["name"],
                                     "uid": cron["metadata"]["uid"], "controller": True}],
            }, "spec": copy.deepcopy(cron["spec"]["jobTemplate"]["spec"])}
            resources[f"/apis/batch/v1/namespaces/{prepared.registration.application_namespace}/jobs/scheduled-backup"] = backup_job
            resources[f"/api/v1/namespaces/{prepared.registration.application_namespace}/pods/finished-backup"] = {
                "apiVersion": "v1", "kind": "Pod", "metadata": {
                    "name": "finished-backup", "uid": "terminal-pod-uid", "namespace": prepared.registration.application_namespace,
                    "ownerReferences": [{"kind": "Job", "name": "scheduled-backup", "uid": "backup-job-uid", "controller": True}],
                }, "status": {"phase": "Succeeded"},
            }
            destroy = await registry.destroy_retained(operation.environment_id, principal=alice,
                                                       expected_generation=1, idempotency_key="destroy")
            task = asyncio.create_task(EnvironmentWorker(registry, provider, readiness_poll_seconds=0.01).reconcile_once(destroy.operation_id))
            try:
                await asyncio.wait_for(inspected_pods.wait(), timeout=5)
                async with registry.session_factory() as session:
                    reservation = await session.get(NebiusPlatformReservation, operation.environment_id)
                    assert reservation.cpu_millis > 0
                assert not task.done(), "live Pods must keep cleanup running and capacity charged"
                pod_present = False
                await asyncio.wait_for(task, timeout=10)
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            assert (await registry.get_operation(destroy.operation_id, principal=alice)).phase == "completed"
            async with registry.session_factory() as session:
                reservation = await session.get(NebiusPlatformReservation, operation.environment_id)
                assert reservation.cpu_millis == reservation.memory_mib == reservation.ephemeral_storage_mib == 0
                assert reservation.storage_mib > 0
            assert len(cloud.objects) == 13 and sum(kind == "bucket" for kind, _, _ in cloud.objects) == 4
            assert backup_job["spec"]["suspend"] is True and terminal_pod_present is False
            stopped_namespaces = {doc["metadata"]["namespace"] for doc in resources.values()
                                  if doc["kind"] == "ResourceQuota" and doc["metadata"]["name"] == "loom-environment-retained"
                                  and doc["spec"]["hard"] == {"pods": "0"}}
            assert stopped_namespaces == set(row.namespaces)
            assert any(doc["kind"] == "StatefulSet" for doc in resources.values())
            assert (await child_http.get("https://" + row.public_host + "/api/v1/auth/whoami")).status_code in {401, 403}
    finally:
        await engine.dispose()
