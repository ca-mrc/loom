"""Disposable Kubernetes validates stopped runtimes and retired writer roles."""
from __future__ import annotations

import asyncio
import copy
import os
import ssl

import httpx
import pytest

from tests.integration.test_execution_actuator_k3s import _load_client, _start_k3s
from tests.ops.test_nebius_pool_collector_runtime import collector_inputs as collector_inputs
from tests.ops.test_nebius_pool_runtime import desired_profile
from tests.ops.test_nebius_pool_runtime import runtime_inputs as runtime_inputs
from tests.unit.test_nebius_management_render import management_inputs as management_inputs
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs

pytestmark = pytest.mark.skipif(os.environ.get("LOOM_RUN_DISPOSABLE_K3S") != "1",
    reason="requires explicitly disposable Kubernetes")


@pytest.mark.timeout(180)
async def test_actual_disabled_runtimes_and_participant_roles_deny_all_job_writes(runtime_inputs, collector_inputs):
    from kubernetes import client
    from scripts.ops.nebius_pool_runtime import (
        participant_readonly_roles,
        wire_collector,
        wire_manager,
        wire_participant,
    )

    request, actuators, services, manager = runtime_inputs
    roles = participant_readonly_roles(request=request)
    container = await asyncio.to_thread(_start_k3s, ephemeral_storage_floor="1Gi")
    try:
        _, core, _ = await asyncio.to_thread(_load_client, container)
        rbac = client.RbacAuthorizationV1Api(core.api_client)
        apps = client.AppsV1Api(core.api_client)
        batch = client.BatchV1Api(core.api_client)
        namespaces = {request.registration.binding.namespace, "pool-foreign", *(row.namespace for row in request.guards),
            *(ns.name for row in request.registration.spec.participants for ns in (row.execution_namespace, row.build_namespace))}
        for namespace in sorted(namespaces):
            await asyncio.to_thread(core.create_namespace, {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": namespace}})
        for participant in request.registration.spec.participants:
            await asyncio.to_thread(core.create_namespaced_service_account, participant.execution_namespace.name,
                {"apiVersion": "v1", "kind": "ServiceAccount", "metadata": {"name": "loom-execution-actuator"}})
            # Install the old role names first. Rendering a differently named
            # reader role would leave these writes active and fail the probes.
            for namespace, name in ((participant.execution_namespace.name, "loom-execution-actuator"),
                    (participant.build_namespace.name, "loom-task-image-builder")):
                await asyncio.to_thread(rbac.create_namespaced_role, namespace, {
                    "apiVersion": "rbac.authorization.k8s.io/v1", "kind": "Role", "metadata": {"name": name},
                    "rules": [{"apiGroups": ["batch"], "resources": ["jobs"], "verbs": ["create", "get", "list", "watch", "delete"]}]})
                await asyncio.to_thread(rbac.create_namespaced_role_binding, namespace, {
                    "apiVersion": "rbac.authorization.k8s.io/v1", "kind": "RoleBinding", "metadata": {"name": name},
                    "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": name},
                    "subjects": [{"kind": "ServiceAccount", "name": "loom-execution-actuator", "namespace": participant.execution_namespace.name}]})
        for resource in roles:
            if resource["kind"] == "ClusterRole":
                await asyncio.to_thread(rbac.create_cluster_role, resource)
            elif resource["kind"] == "ClusterRoleBinding":
                await asyncio.to_thread(rbac.create_cluster_role_binding, resource)
            elif resource["kind"] == "Role":
                await asyncio.to_thread(rbac.replace_namespaced_role, resource["metadata"]["name"], resource["metadata"]["namespace"], resource)
            else:
                await asyncio.to_thread(rbac.replace_namespaced_role_binding, resource["metadata"]["name"], resource["metadata"]["namespace"], resource)
        targets = [wire_manager(request=request, original=manager)]
        for guard in request.guards:
            targets.extend(wire_participant(request=request, participant_id=guard.participant_id,
                management_origin="https://manage.example.com", actuator=actuators[guard.participant_id],
                service=services[guard.participant_id], runtime_profile=desired_profile(request, services[guard.participant_id])).values())
        for target in targets:
            target = copy.deepcopy(target)
            for key in ("uid", "resourceVersion"):
                target["metadata"].pop(key)
            installed = await asyncio.to_thread(apps.create_namespaced_deployment, target["metadata"]["namespace"], target)
            assert installed.spec.replicas == 0
        collector_request, old_collector, configmap = collector_inputs
        wired = wire_collector(request=collector_request, original=old_collector, config_map=configmap,
            management_origin="https://manage.example.com")
        config, = wired["configuration"]
        await asyncio.to_thread(core.create_namespaced_config_map, config["metadata"]["namespace"], config)
        collector, = wired["workload"]
        for key in ("uid", "resourceVersion"):
            collector["metadata"].pop(key)
        installed_collector = await asyncio.to_thread(batch.create_namespaced_cron_job, collector["metadata"]["namespace"], collector)
        assert installed_collector.spec.suspend is True
        for namespace in namespaces:
            assert not (await asyncio.to_thread(core.list_namespaced_pod, namespace)).items
        configuration = core.api_client.configuration
        for participant in request.registration.spec.participants:
            issued = await asyncio.to_thread(core.create_namespaced_service_account_token, "loom-execution-actuator",
                participant.execution_namespace.name, client.AuthenticationV1TokenRequest(spec=client.V1TokenRequestSpec(audiences=[])))
            async with httpx.AsyncClient(base_url=configuration.host, verify=ssl.create_default_context(cafile=configuration.ssl_ca_cert),
                    headers={"Authorization": "Bearer " + issued.status.token}, timeout=20, trust_env=False) as http:
                for namespace in (participant.execution_namespace.name, participant.build_namespace.name):
                    assert (await http.get("/api/v1/namespaces/" + namespace)).status_code == 200
                    assert (await http.get("/api/v1/namespaces/" + namespace + "/pods")).status_code == 200
                    assert (await http.get("/apis/batch/v1/namespaces/" + namespace + "/jobs/missing")).status_code == 404
                    assert (await http.get("/api/v1/namespaces/" + namespace + "/secrets")).status_code == 403
                    path = "/apis/batch/v1/namespaces/" + namespace + "/jobs"
                    job = {"apiVersion": "batch/v1", "kind": "Job", "metadata": {"name": "forbidden-job"},
                        "spec": {"suspend": True, "template": {"spec": {"restartPolicy": "Never",
                            "containers": [{"name": "probe", "image": "busybox:1.36"}]}}}}
                    assert (await http.post(path, json=job)).status_code == 403
                    assert (await http.delete(path + "/forbidden-job")).status_code == 403
                assert (await http.get("/api/v1/namespaces/pool-foreign")).status_code == 403
                assert (await http.get("/api/v1/namespaces/pool-foreign/pods")).status_code == 403
                assert (await http.get("/api/v1/namespaces")).status_code == 403
    finally:
        await asyncio.to_thread(container.stop)
