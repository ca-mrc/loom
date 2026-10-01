"""Actual HTTPS cutover, resource creation and drain; SQL is an explicit double."""
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
from tests.ops.test_nebius_pool_cutover import collector_inputs as collector_inputs
from tests.ops.test_nebius_pool_cutover import cutover_inputs as cutover_inputs
from tests.ops.test_nebius_pool_cutover import fencing_inputs as fencing_inputs
from tests.ops.test_nebius_pool_cutover import management_inputs as management_inputs
from tests.ops.test_nebius_pool_cutover import platform_inputs as platform_inputs
from tests.ops.test_nebius_pool_cutover import retirement_inputs as retirement_inputs
from tests.ops.test_nebius_pool_cutover import runtime_inputs as runtime_inputs
from tests.ops.test_nebius_pool_migration import MigrationAPI

pytestmark = pytest.mark.skipif(os.environ.get("LOOM_RUN_DISPOSABLE_K3S") != "1",
    reason="requires explicitly disposable Kubernetes")


@pytest.mark.timeout(300)
async def test_real_connected_cutover_stages_closed_workloads_and_replays_without_writes(cutover_inputs, tmp_path):
    from kubernetes import client
    from scripts.ops.nebius_ingress_stage import _key
    from scripts.ops.nebius_pool_cutover import cutover_documents, stage_pool_cutover
    from scripts.ops.nebius_pool_cutover_live import HTTPSPoolCutoverAPI
    from scripts.ops.nebius_pool_retirement import retirement_documents

    from loom_service.pool_management.installation import PoolInstallation

    request, tokens = cutover_inputs
    container = await asyncio.to_thread(_start_k3s, ephemeral_storage_floor="1Gi")
    try:
        _, core, _ = await asyncio.to_thread(_load_client, container)
        apps, batch, rbac = client.AppsV1Api(core.api_client), client.BatchV1Api(core.api_client), client.RbacAuthorizationV1Api(core.api_client)
        migration = request.fencing.retirement.migration
        binding = migration.registration.binding
        namespaces = {}
        for name in sorted({binding.namespace, *(row.namespace for row in migration.guards),
                *(ns.name for row in migration.registration.spec.participants for ns in (row.execution_namespace, row.build_namespace))}):
            namespace = await asyncio.to_thread(core.create_namespace, {"apiVersion": "v1", "kind": "Namespace", "metadata": {
                "name": name, "labels": {"loom.nebius/management-installation": binding.installation_id,
                    "pod-security.kubernetes.io/enforce": "restricted"}}})
            namespaces[name] = namespace.metadata.uid
        kube_system = await asyncio.to_thread(core.read_namespace, "kube-system")
        binding = replace(binding, namespace_uid=namespaces[binding.namespace], kube_system_uid=kube_system.metadata.uid)
        originals = {**retirement_documents(request.fencing.retirement), **cutover_documents(request)["producers"]}
        accounts, installed = set(), {}
        for key, document in originals.items():
            value = copy.deepcopy(document)
            for field in ("uid", "resourceVersion"):
                value["metadata"].pop(field)
            namespace = value["metadata"]["namespace"]
            if value["kind"] == "Deployment":
                pod = value["spec"]["template"]["spec"]
                method = apps.create_namespaced_deployment
            else:
                value["spec"]["suspend"] = True
                pod = value["spec"]["jobTemplate"]["spec"]["template"]["spec"]
                method = batch.create_namespaced_cron_job
            identity = (namespace, pod["serviceAccountName"])
            if identity not in accounts:
                await asyncio.to_thread(core.create_namespaced_service_account, namespace,
                    {"apiVersion": "v1", "kind": "ServiceAccount", "metadata": {"name": identity[1]}})
                accounts.add(identity)
            assert pod["nodeSelector"]  # Original fake images cannot schedule.
            result = await asyncio.to_thread(method, namespace, value)
            installed[key] = core.api_client.sanitize_for_serialization(result)
        config = copy.deepcopy(request.collector_config)
        for field in ("uid", "resourceVersion"):
            config["metadata"].pop(field)
        actual_config = await asyncio.to_thread(core.create_namespaced_config_map, config["metadata"]["namespace"], config)
        spec = migration.registration.spec.model_dump(mode="json")
        for participant in spec["participants"]:
            for field in ("execution_namespace", "build_namespace"):
                participant[field]["uid"] = namespaces[participant[field]["name"]]
        migration = replace(migration, registration=replace(migration.registration, binding=binding, spec=PoolInstallation.model_validate(spec)),
            guards=tuple(replace(row, namespace_uid=UUID(namespaces[row.namespace]), controller=installed[_key(row.controller)]) for row in migration.guards))
        retirement = replace(request.fencing.retirement, migration=migration,
            actuators=tuple(installed[_key(row)] for row in request.fencing.retirement.actuators),
            collectors=tuple(installed[_key(row)] for row in request.fencing.retirement.collectors))
        roles = []
        for original in request.fencing.originals:
            value = copy.deepcopy(original)
            for field in ("uid", "resourceVersion"):
                value["metadata"].pop(field)
            namespace, name = value["metadata"]["namespace"], value["metadata"]["name"]
            result = await asyncio.to_thread(rbac.create_namespaced_role, namespace, value)
            roles.append(core.api_client.sanitize_for_serialization(result))
            participant, = (row for row in migration.registration.spec.participants
                if namespace in {row.execution_namespace.name, row.build_namespace.name})
            await asyncio.to_thread(rbac.create_namespaced_role_binding, namespace, {"apiVersion": "rbac.authorization.k8s.io/v1",
                "kind": "RoleBinding", "metadata": {"name": name},
                "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": name},
                "subjects": [{"kind": "ServiceAccount", "name": "loom-execution-actuator", "namespace": participant.execution_namespace.name}]})
        request = replace(request, fencing=replace(request.fencing, retirement=retirement, originals=tuple(roles)),
            manager=installed[_key(request.manager)], services=tuple(installed[_key(row)] for row in request.services),
            collector_config=core.api_client.sanitize_for_serialization(actual_config))

        class Guards(MigrationAPI):
            # Only the already separately tested database/registration boundary
            # is doubled; all cluster writes, defaults, roles and drain are real.
            def runtime_role(self, target, action):
                assert self.guard(target, "observe")["status"] == "held"
                return {"status": "staged" if action == "stage" else "qualified"}

            def cutover_readiness_page(self, target, *, after):
                assert target in self.request.guards and after is None
                return {"status": "observed", "schema_revision": "0172", "rows": []}

        class Checks:
            def preflight(self, actual):
                assert actual == request

            def qualify_quiescence(self):
                pass  # No business DB or personal access exists in this fixture.

            def qualify_pending_origins(self, target, origins):
                assert target in migration.guards and origins == ()

        guards = Guards(migration)
        configuration = core.api_client.configuration
        tls = ssl.create_default_context(cafile=configuration.ssl_ca_cert)
        tls.load_cert_chain(configuration.cert_file, configuration.key_file)
        methods = []
        with HTTPSPoolCutoverAPI(request=request, tokens=tokens, migration=guards, guards=guards, checks=Checks(), history=Checks(),
                api_server=configuration.host, ssl_context=tls) as api:
            for http in (api.client, api.retirement.client, api.fencing.client):
                http.event_hooks["request"].append(lambda message: methods.append((message.method, message.url.path, str(message.url.query))))
            deadline = time.monotonic() + 120
            while True:
                result = await asyncio.to_thread(stage_pool_cutover, request=request, tokens=tokens, api=api,
                    state_dir=tmp_path / "cutover", anchor_dir=tmp_path / "anchor")
                if result["status"] == "pool_runtime_staged_closed":
                    break
                assert result["status"] in {"pending_producer_update", "pending_producer_drain", "pending_drain",
                    "pending_runtime_update", "pending_runtime_drain"}
                assert time.monotonic() < deadline, "actual cutover did not converge"
                await asyncio.sleep(0.5)
            assert result["writer_migration_complete"] is False
            for key in api.originals:
                workload = await asyncio.to_thread(api.read_workload, key)
                assert workload["spec"].get("replicas", 0) == 0
                assert workload["kind"] != "CronJob" or workload["spec"]["suspend"] is True
            for name in namespaces:
                assert not (await asyncio.to_thread(core.list_namespaced_pod, name)).items
            gateway = await asyncio.to_thread(apps.read_namespaced_deployment, "loom-pool-gateway", binding.namespace)
            assert gateway.spec.replicas == 0
            methods.clear()
            assert await asyncio.to_thread(stage_pool_cutover, request=request, tokens=tokens, api=api,
                state_dir=tmp_path / "cutover", anchor_dir=tmp_path / "anchor") == result
            assert all(method == "GET" or (method == "POST" and path.endswith("/selfsubjectrulesreviews"))
                for method, path, _ in methods)
    finally:
        await asyncio.to_thread(container.stop)
