"""Actual Kubernetes admission and RBAC for the installer's fixed gateway output."""
from __future__ import annotations

import asyncio
import os
import ssl

import httpx
import pytest

from tests.integration.test_execution_actuator_k3s import _load_client, _start_k3s
from tests.unit.test_nebius_pool_gateway_render import rendered

pytestmark = pytest.mark.skipif(os.environ.get("LOOM_RUN_DISPOSABLE_K3S") != "1",
    reason="requires explicitly disposable Kubernetes")


@pytest.mark.timeout(180)
async def test_rendered_gateway_is_disabled_and_its_real_identity_has_only_fixed_namespace_writes():
    from kubernetes import client

    container = await asyncio.to_thread(_start_k3s, ephemeral_storage_floor="1Gi")
    try:
        _, core, _ = await asyncio.to_thread(_load_client, container)
        rbac = client.RbacAuthorizationV1Api(core.api_client)
        apps = client.AppsV1Api(core.api_client)
        spec, _, documents = rendered()
        management = "loom-nebius-management"
        namespaces = {ns.name for participant in spec.participants for ns in (participant.execution_namespace, participant.build_namespace)}
        for name in [management, "pool-foreign", *sorted(namespaces)]:
            await asyncio.to_thread(core.create_namespace, {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": name}})
        for resource in documents["configuration"]:
            method = core.create_namespaced_service_account if resource["kind"] == "ServiceAccount" else core.create_namespaced_config_map
            await asyncio.to_thread(method, management, resource)
        for resource in documents["authority"]:
            if resource["kind"] == "ClusterRole":
                await asyncio.to_thread(rbac.create_cluster_role, resource)
            elif resource["kind"] == "ClusterRoleBinding":
                await asyncio.to_thread(rbac.create_cluster_role_binding, resource)
            elif resource["kind"] == "Role":
                await asyncio.to_thread(rbac.create_namespaced_role, resource["metadata"]["namespace"], resource)
            else:
                await asyncio.to_thread(rbac.create_namespaced_role_binding, resource["metadata"]["namespace"], resource)
        deployment, = documents["workload"]
        created = await asyncio.to_thread(apps.create_namespaced_deployment, management, deployment)
        assert created.spec.replicas == 0
        assert not (await asyncio.to_thread(core.list_namespaced_pod, management)).items
        issued = await asyncio.to_thread(core.create_namespaced_service_account_token, "loom-pool-gateway", management,
            client.AuthenticationV1TokenRequest(spec=client.V1TokenRequestSpec(audiences=[])))
        configuration = core.api_client.configuration
        async with httpx.AsyncClient(base_url=configuration.host, verify=ssl.create_default_context(cafile=configuration.ssl_ca_cert),
                trust_env=False, headers={"Authorization": "Bearer " + issued.status.token}, timeout=20) as http:
            for namespace in sorted(namespaces):
                assert (await http.get("/api/v1/namespaces/" + namespace)).status_code == 200
                path = "/apis/batch/v1/namespaces/" + namespace + "/jobs"
                job = {"apiVersion": "batch/v1", "kind": "Job", "metadata": {"name": "pool-role-probe"},
                    "spec": {"suspend": True, "template": {"spec": {"restartPolicy": "Never",
                        "containers": [{"name": "probe", "image": "busybox:1.36"}]}}}}
                assert (await http.post(path, json=job)).status_code == 201
                assert (await http.patch(path + "/pool-role-probe", json={"spec": {"suspend": True}},
                    headers={"Content-Type": "application/merge-patch+json"})).status_code == 403
                assert (await http.delete(path + "/pool-role-probe")).status_code in {200, 202}
                assert (await http.get("/api/v1/namespaces/" + namespace + "/secrets")).status_code == 403
            assert (await http.get("/api/v1/namespaces/pool-foreign")).status_code == 403
            assert (await http.post("/apis/batch/v1/namespaces/pool-foreign/jobs", json=job)).status_code == 403
            assert (await http.get("/api/v1/namespaces")).status_code == 403
            assert (await http.get("/apis/rbac.authorization.k8s.io/v1/clusterroles")).status_code == 403
    finally:
        await asyncio.to_thread(container.stop)
