"""Real Kubernetes stop/drain qualification; initial SQL closure is a fixture."""
from __future__ import annotations

import asyncio
import copy
import os
import ssl
import time
from dataclasses import replace
from uuid import UUID

import pytest

from tests.integration.test_execution_actuator_k3s import _load_client, _start_k3s
from tests.ops.test_nebius_pool_retirement import initialize, retire
from tests.ops.test_nebius_pool_retirement import retirement_inputs as retirement_inputs
from tests.ops.test_nebius_pool_retirement_live import Guards
from tests.ops.test_nebius_pool_runtime import runtime_inputs as runtime_inputs
from tests.unit.test_nebius_management_render import management_inputs as management_inputs
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs

pytestmark = pytest.mark.skipif(os.environ.get("LOOM_RUN_DISPOSABLE_K3S") != "1",
    reason="requires explicitly disposable Kubernetes")


@pytest.mark.timeout(240)
async def test_actual_controller_retirement_preserves_templates_waits_for_pods_and_replays_readonly(retirement_inputs, tmp_path):
    from kubernetes import client
    from scripts.ops.nebius_pool_retirement import retirement_documents
    from scripts.ops.nebius_pool_retirement_live import HTTPSPoolRetirementAPI

    from loom_service.pool_management.installation import PoolInstallation

    request = retirement_inputs
    container = await asyncio.to_thread(_start_k3s, ephemeral_storage_floor="1Gi")
    try:
        _, core, batch = await asyncio.to_thread(_load_client, container)
        apps = client.AppsV1Api(core.api_client)
        binding = request.migration.registration.binding
        names = {binding.namespace, *(guard.namespace for guard in request.migration.guards),
            *(ns.name for row in request.migration.registration.spec.participants for ns in (row.execution_namespace, row.build_namespace))}
        namespaces = {}
        for name in sorted(names):
            namespace = await asyncio.to_thread(core.create_namespace, {
                "apiVersion": "v1", "kind": "Namespace", "metadata": {"name": name, "labels": {
                    "loom.nebius/management-installation": binding.installation_id,
                    "pod-security.kubernetes.io/enforce": "restricted"}}})
            namespaces[name] = namespace.metadata.uid
        kube_system = await asyncio.to_thread(core.read_namespace, "kube-system")
        binding = replace(binding, namespace_uid=namespaces[binding.namespace], kube_system_uid=kube_system.metadata.uid)
        spec = request.migration.registration.spec.model_dump(mode="json")
        for participant in spec["participants"]:
            for field in ("execution_namespace", "build_namespace"):
                participant[field]["uid"] = namespaces[participant[field]["name"]]

        installed = {}
        for key, document in retirement_documents(request).items():
            document = copy.deepcopy(document)
            for field in ("uid", "resourceVersion"):
                document["metadata"].pop(field)
            namespace = document["metadata"]["namespace"]
            if document["kind"] == "Deployment":
                pod_spec = document["spec"]["template"]["spec"]
                # Original Nebius node selectors keep fake candidate images
                # unschedulable; real ReplicaSets and pending Pods still exist.
                assert pod_spec["nodeSelector"]
                await asyncio.to_thread(core.create_namespaced_service_account, namespace, {
                    "apiVersion": "v1", "kind": "ServiceAccount", "metadata": {"name": pod_spec["serviceAccountName"]}})
                result = await asyncio.to_thread(apps.create_namespaced_deployment, namespace, document)
            else:
                document["spec"]["suspend"] = True
                result = await asyncio.to_thread(batch.create_namespaced_cron_job, namespace, document)
            installed[key] = core.api_client.sanitize_for_serialization(result)
        by_identity = {(row["metadata"]["namespace"], row["metadata"]["name"]): row for row in installed.values()}
        migration = replace(request.migration,
            registration=replace(request.migration.registration, binding=binding, spec=PoolInstallation.model_validate(spec)),
            guards=tuple(replace(guard, namespace_uid=UUID(namespaces[guard.namespace]),
                controller=by_identity[guard.namespace, "loom-control-plane"]) for guard in request.migration.guards))
        request = replace(request, migration=migration,
            actuators=tuple(by_identity[row["metadata"]["namespace"], "loom-execution-actuator"] for row in request.actuators),
            collectors=tuple(by_identity[row["metadata"]["namespace"], "loom-execution-capacity-collector"] for row in request.collectors))
        initialize(request, tmp_path)

        # Hold one real pending controller Pod in deletion so a scale-to-zero
        # response cannot be mistaken for observed process drain.
        held_namespace = request.actuators[0]["metadata"]["namespace"]
        deadline = time.monotonic() + 45
        while True:
            pods = await asyncio.to_thread(core.list_namespaced_pod, held_namespace,
                label_selector="app.kubernetes.io/name=loom-execution-actuator")
            if len(pods.items) == 1:
                held_pod = pods.items[0]
                break
            assert time.monotonic() < deadline, "retained controller did not produce a Pod"
            await asyncio.sleep(0.5)
        await asyncio.to_thread(core.patch_namespaced_pod, held_pod.metadata.name, held_namespace,
            {"metadata": {"finalizers": ["qualification.loom.dev/retain"]}})
        configuration = core.api_client.configuration
        tls = ssl.create_default_context(cafile=configuration.ssl_ca_cert)
        tls.load_cert_chain(configuration.cert_file, configuration.key_file)
        methods = []
        with HTTPSPoolRetirementAPI(request=request, guards=Guards(request), api_server=configuration.host, ssl_context=tls) as api:
            api.client.event_hooks["request"].append(lambda message: methods.append(message.method))
            result = await asyncio.to_thread(retire, request, api, tmp_path)
            assert result["status"] == "pending_drain" and result["writer_migration_complete"] is False
            held = await asyncio.to_thread(core.read_namespaced_pod, held_pod.metadata.name, held_namespace)
            assert held.metadata.deletion_timestamp is not None
            await asyncio.to_thread(core.patch_namespaced_pod, held_pod.metadata.name, held_namespace,
                {"metadata": {"finalizers": None}})
            deadline = time.monotonic() + 60
            while True:
                result = await asyncio.to_thread(retire, request, api, tmp_path)
                if result["status"] == "old_pool_workloads_retired":
                    break
                assert result["status"] == "pending_drain"
                if time.monotonic() >= deadline:
                    diagnostics = {}
                    for key in retirement_documents(request):
                        current = await asyncio.to_thread(api.read, key)
                        namespace = current["metadata"]["namespace"]
                        pods = await asyncio.to_thread(core.list_namespaced_pod, namespace)
                        diagnostics[key] = {"generation": current["metadata"].get("generation"),
                            "replicas": current["spec"].get("replicas"), "status": current.get("status"),
                            "pods": [{"name": pod.metadata.name, "phase": pod.status.phase,
                                "deleting": str(pod.metadata.deletion_timestamp), "finalizers": pod.metadata.finalizers}
                                for pod in pods.items]}
                    pytest.fail("controller drain stalled: " + repr(diagnostics))
                await asyncio.sleep(0.5)
            assert methods.count("PATCH") == 9
            methods.clear()
            assert await asyncio.to_thread(retire, request, api, tmp_path) == result
            assert set(methods) == {"GET"}
            for key, original in retirement_documents(request).items():
                current = await asyncio.to_thread(api.read, key)
                assert current["metadata"]["uid"] == original["metadata"]["uid"]
                field = "jobTemplate" if current["kind"] == "CronJob" else "template"
                assert current["spec"][field] == original["spec"][field]
        for name in names:
            assert not (await asyncio.to_thread(core.list_namespaced_pod, name)).items
    finally:
        await asyncio.to_thread(container.stop)
