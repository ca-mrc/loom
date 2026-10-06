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
from tests.ops.test_nebius_pool_dormant import dormant_consumer
from tests.ops.test_nebius_pool_retirement import initialize, retire
from tests.ops.test_nebius_pool_retirement import retirement_inputs as retirement_inputs
from tests.ops.test_nebius_pool_retirement_live import Guards
from tests.ops.test_nebius_pool_role_fencing import fencing_inputs as fencing_inputs
from tests.ops.test_nebius_pool_runtime import guest_runtime_inputs as guest_runtime_inputs
from tests.ops.test_nebius_pool_runtime import runtime_inputs as runtime_inputs
from tests.unit.test_nebius_management_render import management_inputs as management_inputs
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs

pytestmark = pytest.mark.skipif(os.environ.get("LOOM_RUN_DISPOSABLE_K3S") != "1",
    reason="requires explicitly disposable Kubernetes")


@pytest.mark.timeout(300)
async def test_native_terminal_job_history_qualifies_and_spec_edits_cannot_restart_it(fencing_inputs):
    from kubernetes.client.exceptions import ApiException
    from scripts.ops.nebius_pool_retirement import retirement_documents
    from scripts.ops.nebius_pool_role_fencing import (
        POOL_WRITER_WORKLOAD_COLLECTIONS,
        qualify_retained_writer_workloads,
    )

    # Only historical Job/Pod snapshots come from Kubernetes here. The unchanged
    # retained roots are fixtures; complete installed-pool acceptance is separate.
    guard = fencing_inputs.retirement.migration.guards[0]
    namespace = guard.namespace
    account = guard.controller["spec"]["template"]["spec"]["serviceAccountName"]
    originals = retirement_documents(fencing_inputs.retirement)
    container = await asyncio.to_thread(_start_k3s, ephemeral_storage_floor="1Gi")
    try:
        _, core, batch = await asyncio.to_thread(_load_client, container)
        await asyncio.to_thread(core.create_namespace, {"metadata": {"name": namespace}})
        await asyncio.to_thread(core.create_namespaced_service_account, namespace, {"metadata": {"name": account}})

        async def create_job(name, command):
            await asyncio.to_thread(batch.create_namespaced_job, namespace, {
                "apiVersion": "batch/v1", "kind": "Job", "metadata": {"name": name},
                "spec": {"backoffLimit": 0, "template": {"spec": {
                    "restartPolicy": "Never", "serviceAccountName": account,
                    "automountServiceAccountToken": False,
                    "containers": [{"name": "one-shot", "image": "busybox:1.36", "command": command}]}}}})

        async def snapshots():
            jobs = await asyncio.to_thread(batch.list_namespaced_job, namespace)
            pods = await asyncio.to_thread(core.list_namespaced_pod, namespace)
            result = []
            for collection, api, kind in ((jobs, "batch/v1", "Job"), (pods, "v1", "Pod")):
                rows = core.api_client.sanitize_for_serialization(collection)["items"]
                # Kubernetes list items omit TypeMeta; the production inventory
                # reader qualifies it from each enclosing typed collection.
                for row in rows:
                    row.setdefault("apiVersion", api)
                    row.setdefault("kind", kind)
                result.append(rows)
            return tuple(result)

        async def wait_terminal(names):
            deadline = time.monotonic() + 120
            while True:
                jobs, pods = await snapshots()
                terminal = {row["metadata"]["name"] for row in jobs if any(
                    condition["type"] in {"Complete", "Failed"} and condition["status"] == "True"
                    for condition in row.get("status", {}).get("conditions", []))}
                if names <= terminal:
                    return jobs, pods
                assert time.monotonic() < deadline, repr((jobs, pods))
                await asyncio.sleep(0.5)

        def qualify(jobs, pods):
            inventory = {resource: [copy.deepcopy(row) for row in originals.values() if row["kind"] == kind]
                for _api, resource, kind in POOL_WRITER_WORKLOAD_COLLECTIONS}
            inventory["jobs"] += jobs
            inventory["pods"] += pods
            qualify_retained_writer_workloads(fencing_inputs, inventory, originals=originals, expected=originals)

        histories = {"succeeded-history", "failed-history"}
        await create_job("succeeded-history", ["/bin/sh", "-c", "exit 0"])
        await create_job("failed-history", ["/bin/sh", "-c", "exit 1"])
        jobs, pods = await wait_terminal(histories)
        assert len(pods) == 2 and {pod["status"]["phase"] for pod in pods} == {"Succeeded", "Failed"}
        qualify(jobs, pods)
        history_uids = {row["metadata"]["uid"] for row in jobs}
        for name in sorted(histories):
            for spec in ({"suspend": True}, {"suspend": False}, {"parallelism": 2}, {"backoffLimit": 3}):
                await asyncio.to_thread(batch.patch_namespaced_job, name, namespace, {"spec": spec})
            for spec in ({"completions": 2}, {"template": {"spec": {"containers": [
                    {"name": "one-shot", "image": "busybox:1.36", "command": ["/bin/sleep", "60"]}]}}}):
                with pytest.raises(ApiException) as error:
                    await asyncio.to_thread(batch.patch_namespaced_job, name, namespace, {"spec": spec})
                assert error.value.status == 422
            await asyncio.to_thread(batch.patch_namespaced_job, name, namespace, {"spec": {"parallelism": 1}})
        for pod in pods:
            await asyncio.to_thread(core.delete_namespaced_pod, pod["metadata"]["name"], namespace)

        # A new Job must complete while these finished parents stay inert. This
        # proves controller liveness, not just a sleep with no reconciliation.
        await create_job("liveness-control", ["/bin/sh", "-c", "sleep 3; exit 0"])
        jobs, pods = await wait_terminal(histories | {"liveness-control"})
        deadline = time.monotonic() + 5
        while True:
            jobs, pods = await snapshots()
            assert not [pod for pod in pods if any(owner["uid"] in history_uids
                for owner in pod["metadata"].get("ownerReferences", []))]
            qualify(jobs, pods)
            if time.monotonic() >= deadline:
                break
            await asyncio.sleep(0.5)
    finally:
        await asyncio.to_thread(container.stop)


@pytest.mark.timeout(240)
async def test_actual_controller_retirement_preserves_templates_waits_for_pods_and_replays_readonly(retirement_inputs, fencing_inputs, guest_runtime_inputs, tmp_path):
    from kubernetes import client
    from scripts.ops.nebius_pool_retirement import retirement_documents
    from scripts.ops.nebius_pool_retirement_live import HTTPSPoolRetirementAPI
    from scripts.ops.nebius_pool_role_fencing import fence_pool_roles
    from scripts.ops.nebius_pool_role_fencing_live import HTTPSPoolRoleFenceAPI

    from loom_service.pool_management.installation import PoolInstallation

    migration, actuators, _, _, guest = guest_runtime_inputs
    request = replace(retirement_inputs, migration=migration, actuators=(*actuators.values(), guest))
    dormant = dormant_consumer(request)
    request = replace(request, dormant_consumers=(dormant,))
    container = await asyncio.to_thread(_start_k3s, ephemeral_storage_floor="1Gi")
    try:
        _, core, batch = await asyncio.to_thread(_load_client, container)
        apps = client.AppsV1Api(core.api_client)
        rbac = client.RbacAuthorizationV1Api(core.api_client)
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
        service_accounts = set()
        for key, document in retirement_documents(request).items():
            document = copy.deepcopy(document)
            for field in ("uid", "resourceVersion"):
                document["metadata"].pop(field)
            namespace = document["metadata"]["namespace"]
            pod_spec = (document["spec"]["template"]["spec"] if document["kind"] == "Deployment"
                else document["spec"]["jobTemplate"]["spec"]["template"]["spec"])
            service_account = (namespace, pod_spec["serviceAccountName"])
            if service_account not in service_accounts:
                await asyncio.to_thread(core.create_namespaced_service_account, namespace, {
                    "apiVersion": "v1", "kind": "ServiceAccount", "metadata": {"name": service_account[1]}})
                service_accounts.add(service_account)
            if document["kind"] == "Deployment":
                # Original Nebius node selectors keep fake candidate images
                # unschedulable; real ReplicaSets and pending Pods still exist.
                assert pod_spec["nodeSelector"]
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
            actuators=tuple(by_identity[row["metadata"]["namespace"], row["metadata"]["name"]] for row in request.actuators),
            collectors=tuple(by_identity[row["metadata"]["namespace"], "loom-execution-capacity-collector"] for row in request.collectors),
            dormant_consumers=(replace(dormant,
                actuator=by_identity[dormant.actuator["metadata"]["namespace"], dormant.actuator["metadata"]["name"]],
                collector=by_identity[dormant.collector["metadata"]["namespace"], dormant.collector["metadata"]["name"]]),))
        roles = []
        for original in fencing_inputs.originals:
            document = copy.deepcopy(original)
            for field in ("uid", "resourceVersion"):
                document["metadata"].pop(field)
            namespace, name = document["metadata"]["namespace"], document["metadata"]["name"]
            installed_role = await asyncio.to_thread(rbac.create_namespaced_role, namespace, document)
            roles.append(core.api_client.sanitize_for_serialization(installed_role))
            participant, = [row for row in migration.registration.spec.participants
                if namespace in {row.execution_namespace.name, row.build_namespace.name}]
            await asyncio.to_thread(rbac.create_namespaced_role_binding, namespace, {
                "apiVersion": "rbac.authorization.k8s.io/v1", "kind": "RoleBinding", "metadata": {"name": name},
                "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": name},
                "subjects": [{"kind": "ServiceAccount", "name": "loom-execution-actuator", "namespace": participant.execution_namespace.name}]})
        fencing = replace(fencing_inputs, retirement=request, originals=tuple(roles))
        initialize(request, tmp_path)

        # Hold one real pending controller Pod in deletion so a scale-to-zero
        # response cannot be mistaken for observed process drain.
        held_namespace = guest["metadata"]["namespace"]
        deadline = time.monotonic() + 45
        while True:
            pods = await asyncio.to_thread(core.list_namespaced_pod, held_namespace,
                label_selector="app.kubernetes.io/name=nebius-guest-fixture-actuator")
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
            assert methods.count("PATCH") == 12
            methods.clear()
            assert await asyncio.to_thread(retire, request, api, tmp_path) == result
            assert set(methods) == {"GET"}
            for key, original in retirement_documents(request).items():
                current = await asyncio.to_thread(api.read, key)
                assert current["metadata"]["uid"] == original["metadata"]["uid"]
                field = "jobTemplate" if current["kind"] == "CronJob" else "template"
                assert current["spec"][field] == original["spec"][field]
            with HTTPSPoolRoleFenceAPI(request=fencing, retirement=api, api_server=configuration.host, ssl_context=tls) as roles_api:
                methods.clear()
                roles_api.client.event_hooks["request"].append(lambda message: methods.append(message.method))
                reviews = []

                def retain_review(response):
                    if response.request.url.path == "/apis/authorization.k8s.io/v1/selfsubjectrulesreviews":
                        response.read()
                        reviews.append({"code": response.status_code, "review": response.json()})

                roles_api.client.event_hooks["response"].append(retain_review)
                # An unexpected group binding grants a named write that unnamed
                # access-review probes would miss. Role replacement alone must
                # not declare this identity fenced.
                extra_namespace = migration.registration.spec.participants[1].build_namespace.name
                source_namespace = migration.registration.spec.participants[0].execution_namespace.name
                await asyncio.to_thread(rbac.create_namespaced_role, extra_namespace, {
                    "apiVersion": "rbac.authorization.k8s.io/v1", "kind": "Role", "metadata": {"name": "extra-writer"},
                    "rules": [{"apiGroups": ["batch"], "resources": ["jobs"], "verbs": ["patch"], "resourceNames": ["retained-job"]}]})
                extra_binding = {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "RoleBinding", "metadata": {"name": "extra-writer"},
                    "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": "extra-writer"},
                    "subjects": [{"apiGroup": "rbac.authorization.k8s.io", "kind": "Group", "name": "system:serviceaccounts:" + source_namespace}]}
                await asyncio.to_thread(rbac.create_namespaced_role_binding, extra_namespace, extra_binding)
                with pytest.raises(ValueError):
                    await asyncio.to_thread(fence_pool_roles, request=fencing, api=roles_api,
                        state_dir=tmp_path / "state", anchor_dir=tmp_path / "anchor")
                assert methods.count("PATCH") == 6
                await asyncio.to_thread(rbac.delete_namespaced_role_binding, "extra-writer", extra_namespace)
                methods.clear()
                reviews.clear()
                try:
                    result = await asyncio.to_thread(fence_pool_roles, request=fencing, api=roles_api,
                        state_dir=tmp_path / "state", anchor_dir=tmp_path / "anchor")
                except ValueError:
                    pytest.fail("effective permission qualification failed: " + repr(reviews[-1:]))
                assert result["status"] == "participant_roles_restricted" and result["writer_migration_complete"] is False
                assert set(methods) == {"GET", "POST"}
                methods.clear()
                assert await asyncio.to_thread(fence_pool_roles, request=fencing, api=roles_api,
                    state_dir=tmp_path / "state", anchor_dir=tmp_path / "anchor") == result
                assert set(methods) == {"GET", "POST"}  # Nonpersisted authorization reviews only.
                # Dormancy never excuses an extra grant on this exact identity.
                extra_binding["subjects"] = [{"kind": "ServiceAccount", "namespace": source_namespace,
                    "name": "nebius-retained-remote-actuator"}]
                await asyncio.to_thread(rbac.create_namespaced_role_binding, extra_namespace, extra_binding)
                with pytest.raises(ValueError):
                    await asyncio.to_thread(fence_pool_roles, request=fencing, api=roles_api,
                        state_dir=tmp_path / "state", anchor_dir=tmp_path / "anchor")
                assert "PATCH" not in methods
                await asyncio.to_thread(rbac.delete_namespaced_role_binding, "extra-writer", extra_namespace)
        # These use real issued runtime tokens, not operator impersonation or
        # inspection of rendered rules. No probe Job is persisted.
        import httpx

        token_subjects = [(participant, "loom-execution-actuator", 404)
            for participant in migration.registration.spec.participants]
        token_subjects.extend((migration.registration.spec.participants[0], name, 403)
            for name in ("nebius-retained-remote-actuator", "nebius-retained-remote-collector"))
        for participant, account, missing_status in token_subjects:
            issued = await asyncio.to_thread(core.create_namespaced_service_account_token, account,
                participant.execution_namespace.name, client.AuthenticationV1TokenRequest(spec=client.V1TokenRequestSpec(audiences=[])))
            async with httpx.AsyncClient(base_url=configuration.host,
                    verify=ssl.create_default_context(cafile=configuration.ssl_ca_cert),
                    headers={"Authorization": "Bearer " + issued.status.token}, timeout=20, trust_env=False) as http:
                for namespace in (participant.execution_namespace.name, participant.build_namespace.name):
                    assert (await http.get("/apis/batch/v1/namespaces/" + namespace + "/jobs/missing")).status_code == missing_status
                    for verb in ("create", "update", "patch", "delete", "deletecollection"):
                        response = await http.post("/apis/authorization.k8s.io/v1/selfsubjectaccessreviews", json={
                            "apiVersion": "authorization.k8s.io/v1", "kind": "SelfSubjectAccessReview", "spec": {
                                "resourceAttributes": {"namespace": namespace, "group": "batch", "resource": "jobs", "verb": verb}}})
                        assert response.status_code == 201 and response.json()["status"]["allowed"] is False
        for name in names:
            assert not (await asyncio.to_thread(core.list_namespaced_pod, name)).items
    finally:
        await asyncio.to_thread(container.stop)
