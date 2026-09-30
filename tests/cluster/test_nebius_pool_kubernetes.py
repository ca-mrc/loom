"""Fixed CREATE adapter with real API defaulting and restricted credentials.

This is not the protected installer/migration acceptance: the fixture establishes
the intended fixed-resource roles in a fresh disposable cluster. No workload
can schedule on its node (the frozen cloud node-group selector is unmatched).
"""
from __future__ import annotations

import asyncio
import os
import ssl
import time
from uuid import UUID

import httpx
import pytest

from loom_service.pool_management.gateway_journal import PoolGatewayJournal
from loom_service.pool_management.kubernetes import (
    KubernetesPoolGateway,
    PoolKubernetesError,
    PoolKubernetesWaitingError,
)
from tests.integration.conftest import (
    isolated_migration_postgres_url as isolated_migration_postgres_url,
)
from tests.integration.conftest import (
    migration_template_postgres_url as migration_template_postgres_url,
)
from tests.integration.test_execution_actuator_k3s import _load_client, _start_k3s
from tests.integration.test_nebius_pool_build_admission import mixed_setup, prepare_build
from tests.integration.test_nebius_pool_cleanup_journal import begin_cleanup
from tests.integration.test_nebius_pool_control import action, operate
from tests.integration.test_nebius_pool_registry import machine, prepare
from tests.integration.test_nebius_pool_registry import sessions as sessions

pytestmark = pytest.mark.skipif(os.environ.get("LOOM_RUN_DISPOSABLE_K3S") != "1",
                                reason="requires explicitly disposable Kubernetes")


def drift_paths(actual, expected, path=()):
    """Test-only diagnostics expose field paths, never credentials or Pod values."""
    if isinstance(expected, dict) and isinstance(actual, dict):
        return ["/".join((*path, key)) + ": missing" for key in expected.keys() - actual.keys()] + [
            finding for key in expected.keys() & actual.keys()
            for finding in drift_paths(actual[key], expected[key], (*path, key))]
    if isinstance(expected, list) and isinstance(actual, list) and len(actual) == len(expected):
        return [finding for index, (a, e) in enumerate(zip(actual, expected, strict=True))
                for finding in drift_paths(a, e, (*path, str(index)))]
    return ["/".join(path) + ": differs"] if actual != expected else []


@pytest.mark.timeout(240)
async def test_fixed_gateway_real_defaulting_and_restricted_namespace_authority(sessions):
    from kubernetes import client

    container = await asyncio.to_thread(_start_k3s, ephemeral_storage_floor="1Gi")
    try:
        _, core, _ = await asyncio.to_thread(_load_client, container)
        rbac = client.RbacAuthorizationV1Api(core.api_client)
        namespaces = {}
        for name in ["pool-management", *[f"pool-test-{kind}-{index}" for kind in ("execution", "build") for index in range(2)]]:
            created = await asyncio.to_thread(core.create_namespace, {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": name}})
            namespaces[name] = UUID(created.metadata.uid)
        for name in ("gateway", "legacy"):
            await asyncio.to_thread(core.create_namespaced_service_account, "pool-management", {
                "apiVersion": "v1", "kind": "ServiceAccount", "metadata": {"name": name}})
        allowed = ["pool-test-execution-0", "pool-test-build-0"]
        subject = {"kind": "ServiceAccount", "name": "gateway", "namespace": "pool-management"}
        await asyncio.to_thread(rbac.create_cluster_role, {"apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": "ClusterRole", "metadata": {"name": "pool-namespace-read"}, "rules": [
                {"apiGroups": [""], "resources": ["namespaces"], "resourceNames": allowed, "verbs": ["get"]}]})
        await asyncio.to_thread(rbac.create_cluster_role_binding, {"apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": "ClusterRoleBinding", "metadata": {"name": "pool-namespace-read"}, "subjects": [subject],
            "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": "pool-namespace-read"}})
        for namespace in allowed:
            await asyncio.to_thread(rbac.create_namespaced_role, namespace, {"apiVersion": "rbac.authorization.k8s.io/v1",
                "kind": "Role", "metadata": {"name": "pool-create"}, "rules": [
                    {"apiGroups": ["batch"], "resources": ["jobs"], "verbs": ["get", "create", "delete"]},
                    {"apiGroups": [""], "resources": ["configmaps"], "verbs": ["get", "create", "delete"]},
                    {"apiGroups": [""], "resources": ["pods"], "verbs": ["get", "list"]}]})
            await asyncio.to_thread(rbac.create_namespaced_role_binding, namespace, {"apiVersion": "rbac.authorization.k8s.io/v1",
                "kind": "RoleBinding", "metadata": {"name": "pool-create"}, "subjects": [subject],
                "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": "pool-create"}})
        participants, principals, executions, builds, profiles, _ = await mixed_setup(sessions, namespace_uids=namespaces)
        profile_id = participants[0].targets[0].profile_id
        for runtime in (profiles.execution[profile_id].runtime, profiles.task_images[profile_id].target):
            await asyncio.to_thread(core.create_namespaced_service_account, runtime.namespace, {
                "apiVersion": "v1", "kind": "ServiceAccount", "metadata": {"name": runtime.service_account_name},
                "automountServiceAccountToken": False})
        journal = PoolGatewayJournal(sessions)
        principal = await machine(sessions, participants[0].pool_id, role="gateway")
        config = core.api_client.configuration
        trust = ssl.create_default_context(cafile=config.ssl_ca_cert)
        issued = await asyncio.to_thread(core.create_namespaced_service_account_token, "gateway", "pool-management",
            client.AuthenticationV1TokenRequest(spec=client.V1TokenRequestSpec(audiences=[])))
        mutations = []

        async def record(request):
            if request.method in {"POST", "DELETE"}:
                mutations.append((request.method, request.url.path))

        # Token only, no admin certificate and no ambient environment authority.
        async with httpx.AsyncClient(base_url=config.host, verify=trust, trust_env=False,
                                     headers={"Authorization": "Bearer " + issued.status.token},
                                     event_hooks={"request": [record]}) as http:
            gateway = KubernetesPoolGateway(journal, http)
            receipts = []
            for body, preparing in [(executions[0], prepare), (builds[0], prepare_build)]:
                await preparing(sessions, principals[0], body, profiles)
                receipt = await operate(sessions, principals[0], action(body), profiles=profiles)
                if body.key.workload_kind == "task_image_build":
                    await gateway.create(principal, receipt.reservation_id, kind="ConfigMap")
                try:
                    observed = await gateway.create(principal, receipt.reservation_id, kind="Job")
                except PoolKubernetesError:
                    effect = await journal.prepare_create(principal, receipt.reservation_id, kind="Job")
                    document = effect.document
                    response = await http.get("/apis/batch/v1/namespaces/" + document["metadata"]["namespace"] + "/jobs/" + document["metadata"]["name"])
                    print("Frozen Job field drift:", drift_paths(response.json(), document))
                    raise
                assert observed.phase == "observed"
                assert await gateway.create(principal, receipt.reservation_id, kind="Job") == observed
                deadline = time.monotonic() + 20
                while True:
                    inventory = await gateway.pod_inventory(principal, receipt.reservation_id)
                    if inventory.pods:
                        break
                    assert time.monotonic() < deadline, "real Job controller did not create its pending Pod"
                    await asyncio.sleep(0.1)
                assert len(inventory.pods) == 1 and inventory.job_uid == observed.observed_uid
                receipts.append((receipt, body.key.workload_kind))
            foreign = await http.post("/apis/batch/v1/namespaces/pool-test-execution-1/jobs", json=observed.document)
            assert foreign.status_code == 403
            secret = await http.post("/api/v1/namespaces/pool-test-build-0/secrets", json={
                "apiVersion": "v1", "kind": "Secret", "metadata": {"name": "not-allowed"}})
            assert secret.status_code == 403
            for namespace in allowed:
                pods = await asyncio.to_thread(core.list_namespaced_pod, namespace)
                assert all(pod.spec.node_name is None for pod in pods.items)
            for receipt, workload_kind in receipts:
                await begin_cleanup(sessions, receipt.reservation_id)
                for kind in (["Job", "ConfigMap"] if workload_kind == "task_image_build" else ["Job"]):
                    deadline = time.monotonic() + 20
                    while True:
                        try:
                            deleted = await gateway.delete(principal, receipt.reservation_id, kind=kind)
                            break
                        except PoolKubernetesWaitingError:
                            assert time.monotonic() < deadline, "fixed object deletion did not converge"
                            await asyncio.sleep(0.1)
                    assert deleted.phase == "observed"
                deadline = time.monotonic() + 20
                while (await gateway.pod_inventory(principal, receipt.reservation_id)).pods:
                    assert time.monotonic() < deadline, "real residual Pod retirement did not converge"
                    await asyncio.sleep(0.1)
            deletes = [path for method, path in mutations if method == "DELETE"]
            assert len(deletes) == len(set(deletes)) == 3
        legacy = await asyncio.to_thread(core.create_namespaced_service_account_token, "legacy", "pool-management",
            client.AuthenticationV1TokenRequest(spec=client.V1TokenRequestSpec(audiences=[])))
        async with httpx.AsyncClient(base_url=config.host, verify=trust, trust_env=False,
                                     headers={"Authorization": "Bearer " + legacy.status.token}) as http:
            response = await http.post("/apis/batch/v1/namespaces/pool-test-build-0/jobs", json=observed.document)
            assert response.status_code == 403
    finally:
        await asyncio.to_thread(container.stop)
