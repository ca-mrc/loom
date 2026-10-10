"""Actual HTTPS cutover, resource creation and drain; SQL is an explicit double."""
from __future__ import annotations

import asyncio
import copy
import json
import os
import ssl
import time
from dataclasses import replace
from uuid import UUID

import pytest

from tests.cluster.pool_cutover_diagnostics import observe_cutover_stage
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
    from scripts.ops.nebius_pool_platform_authority import PoolPlatformAuthority
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
        platform_resources = []
        bootstrap_deadline = time.monotonic() + 30
        for name in ("cluster-admin", "system:controller:cronjob-controller", "system:controller:job-controller",
                "system:controller:generic-garbage-collector", "system:controller:namespace-controller",
                "system:controller:ttl-after-finished-controller"):
            for method in (rbac.read_cluster_role, rbac.read_cluster_role_binding):
                # API discovery/namespace creation can precede bootstrap RBAC.
                # Wait for actual objects, never manufacture their permissions.
                while True:
                    try:
                        document = await asyncio.to_thread(method, name)
                        break
                    except client.ApiException as error:
                        if error.status != 404:
                            raise
                        assert time.monotonic() < bootstrap_deadline, "disposable bootstrap RBAC did not appear"
                        await asyncio.sleep(0.1)
                platform_resources.append(core.api_client.sanitize_for_serialization(document))
        # This pinned K3s image adds its own RBAC after the upstream bootstrap.
        # An initially complete inventory can therefore precede its late
        # bindings/roles. Require the actual late roles and resolve bindings at
        # the same revision; never omit bindings or manufacture permissions.
        late_bootstrap_roles = {"clustercidrs-node", "system:k3s-controller"}
        while True:
            bootstrap_roles = await asyncio.to_thread(rbac.list_cluster_role)
            bootstrap_bindings = await asyncio.to_thread(rbac.list_cluster_role_binding,
                resource_version=bootstrap_roles.metadata.resource_version, resource_version_match="Exact")
            available = {row.metadata.name for row in bootstrap_roles.items}
            missing = ({row.role_ref.name for row in bootstrap_bindings.items} | late_bootstrap_roles) - available
            if not missing:
                break
            assert time.monotonic() < bootstrap_deadline, f"disposable bootstrap role references unresolved: {sorted(missing)}"
            await asyncio.sleep(0.1)
        platform_authority = PoolPlatformAuthority(schema_version="loom.pool-platform-authority.v1",
            kube_system_uid=kube_system.metadata.uid, resources=tuple(platform_resources))
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
            platform_authority=platform_authority,
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
                return {"status": "observed", "schema_revision": "0175", "rows": []}

        class Checks:
            def qualify_binding(self, actual, manager):
                assert actual == migration and manager == request.manager

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
                api_server=configuration.host, ssl_context=tls,
                state_dir=tmp_path / "cutover", anchor_dir=tmp_path / "anchor") as api:
            for http in (api.client, api.retirement.client, api.fencing.client):
                http.event_hooks["request"].append(lambda message: methods.append((message.method, message.url.path, str(message.url.query))))
            original_role = request.fencing.originals[0]
            role_namespace = original_role["metadata"]["namespace"]
            await asyncio.to_thread(rbac.create_namespaced_role_binding, role_namespace, {
                "apiVersion": "rbac.authorization.k8s.io/v1", "kind": "RoleBinding",
                "metadata": {"name": "foreign-shares-retained-role"},
                "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": original_role["metadata"]["name"]},
                "subjects": [{"kind": "ServiceAccount", "name": "default", "namespace": "kube-system"}]})
            with pytest.raises(ValueError, match="preserve_evidence"):
                await asyncio.to_thread(stage_pool_cutover, request=request, tokens=tokens, api=api,
                    state_dir=tmp_path / "cutover", anchor_dir=tmp_path / "anchor")
            assert not (tmp_path / "cutover").exists()
            assert methods and all(method == "GET" for method, _path, _query in methods)
            await asyncio.to_thread(rbac.delete_namespaced_role_binding, "foreign-shares-retained-role", role_namespace)
            methods.clear()

            # Cluster-wide authority is in scope even when the subject is not
            # one of the retiring accounts. This is only a disposable cluster.
            await asyncio.to_thread(rbac.create_cluster_role_binding, {
                "apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRoleBinding",
                "metadata": {"name": "unregistered-cluster-writer"},
                "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": "cluster-admin"},
                "subjects": [{"kind": "User", "apiGroup": "rbac.authorization.k8s.io", "name": "unregistered-automation"}]})
            with pytest.raises(ValueError, match="pool_retained_writer_binding_inventory_unqualified"):
                await asyncio.to_thread(api.preflight, request)
            assert not (tmp_path / "cutover").exists()
            assert methods and all(method == "GET" for method, _path, _query in methods)
            await asyncio.to_thread(rbac.delete_cluster_role_binding, "unregistered-cluster-writer")
            methods.clear()

            # CronJob authority is an indirect Job writer even when this
            # foreign account has no Job permission of its own.
            await asyncio.to_thread(rbac.create_namespaced_role, role_namespace, {
                "apiVersion": "rbac.authorization.k8s.io/v1", "kind": "Role",
                "metadata": {"name": "unregistered-cron-writer"},
                "rules": [{"apiGroups": ["batch"], "resources": ["cronjobs"], "verbs": ["create", "patch"]}]})
            await asyncio.to_thread(rbac.create_namespaced_role_binding, role_namespace, {
                "apiVersion": "rbac.authorization.k8s.io/v1", "kind": "RoleBinding",
                "metadata": {"name": "unregistered-cron-writer"},
                "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": "unregistered-cron-writer"},
                "subjects": [{"kind": "ServiceAccount", "name": "unregistered-account", "namespace": role_namespace}]})
            with pytest.raises(ValueError, match="pool_retained_writer_binding_inventory_unqualified"):
                await asyncio.to_thread(api.preflight, request)
            assert methods and all(method == "GET" for method, _path, _query in methods)
            assert not (tmp_path / "cutover").exists()
            await asyncio.to_thread(rbac.delete_namespaced_role_binding, "unregistered-cron-writer", role_namespace)
            await asyncio.to_thread(rbac.delete_namespaced_role, "unregistered-cron-writer", role_namespace)
            methods.clear()

            # Revoking the creator's grant does not remove a retained schedule.
            # Keep it suspended to avoid creating unrelated fixture Jobs; this
            # still must be rejected, not adopted or silently deleted.
            schedule = copy.deepcopy(request.fencing.retirement.collectors[0])
            schedule_namespace = schedule["metadata"]["namespace"]
            schedule["metadata"] = {"name": "unregistered-cron-producer", "namespace": schedule_namespace}
            schedule["spec"]["suspend"] = True
            schedule["spec"]["jobTemplate"]["spec"]["template"]["spec"]["serviceAccountName"] = "unregistered-account"
            installed_schedule = await asyncio.to_thread(batch.create_namespaced_cron_job, schedule_namespace, schedule)
            with pytest.raises(ValueError, match="pool_retained_writer_workload_inventory_unqualified"):
                await asyncio.to_thread(api.preflight, request)
            assert methods and all(method == "GET" for method, _path, _query in methods)
            assert not (tmp_path / "cutover").exists()
            retained_schedule = await asyncio.to_thread(batch.read_namespaced_cron_job, "unregistered-cron-producer", schedule_namespace)
            assert retained_schedule.metadata.uid == installed_schedule.metadata.uid
            await asyncio.to_thread(batch.delete_namespaced_cron_job, "unregistered-cron-producer", schedule_namespace,
                body=client.V1DeleteOptions(preconditions=client.V1Preconditions(uid=installed_schedule.metadata.uid)))
            methods.clear()

            # A second consumer still has the retained identity even when its
            # desired replicas are zero. Detect it before stopping any producer.
            foreign = copy.deepcopy(request.fencing.retirement.actuators[0])
            foreign["metadata"] = {"name": "unregistered-writer-consumer",
                "namespace": foreign["metadata"]["namespace"]}
            foreign["spec"]["replicas"] = 0
            labels = {"app.kubernetes.io/name": "unregistered-writer-consumer"}
            foreign["spec"]["selector"] = {"matchLabels": labels}
            foreign["spec"]["template"]["metadata"]["labels"] = labels
            foreign_namespace = foreign["metadata"]["namespace"]
            await asyncio.to_thread(apps.create_namespaced_deployment, foreign_namespace, foreign)
            with pytest.raises(ValueError, match="pool_retained_writer_workload_inventory_unqualified"):
                await asyncio.to_thread(api.preflight, request)
            assert not (tmp_path / "cutover").exists()
            assert methods and all(method == "GET" for method, _path, _query in methods)
            await asyncio.to_thread(apps.delete_namespaced_deployment, "unregistered-writer-consumer", foreign_namespace,
                propagation_policy="Foreground")
            deadline = time.monotonic() + 30
            while True:
                try:
                    await asyncio.to_thread(apps.read_namespaced_deployment, "unregistered-writer-consumer", foreign_namespace)
                except client.ApiException as error:
                    assert error.status == 404
                    break
                assert time.monotonic() < deadline, "test-only foreign controller did not drain"
                await asyncio.sleep(0.1)
            methods.clear()

            # Reproduce the resource-version race seen with a real controller
            # status update between read and dry-run. No template changes.
            preview = api.preview_workload
            raced = []

            def preview_after_status_update(key, before, desired):
                if not raced:
                    updated = apps.patch_namespaced_deployment_status(before["metadata"]["name"],
                        before["metadata"]["namespace"], {"status": {"conditions": [{
                            "type": "DisposablePreviewRace", "status": "False"}]}})
                    assert updated.metadata.resource_version != before["metadata"]["resourceVersion"]
                    raced.append(key)
                return preview(key, before, desired)

            api.preview_workload = preview_after_status_update
            stage_invocation = 0

            def observed_stage():
                nonlocal stage_invocation
                stage_invocation += 1
                # The exception retains its failed inventory and journal frames.
                # Emit only fixture identities/phase enums, without another read.
                with observe_cutover_stage(stage_invocation):
                    return stage_pool_cutover(request=request, tokens=tokens, api=api,
                        state_dir=tmp_path / "cutover", anchor_dir=tmp_path / "anchor")

            deadline = time.monotonic() + 120
            first = True
            while True:
                result = await asyncio.to_thread(observed_stage)
                if first:
                    assert result["status"] == "pending_producer_update"
                    assert raced == [_key(request.manager)]
                    record = json.loads((tmp_path / "cutover/cutover.json").read_text())
                    assert record["producers"][raced[0]] == {"phase": "prepared", "expected": None}
                    first = False
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
