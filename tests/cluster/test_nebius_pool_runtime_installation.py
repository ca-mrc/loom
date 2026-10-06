"""Disposable Kubernetes validates stopped runtimes and retired writer roles."""
from __future__ import annotations

import asyncio
import copy
import json
import os
import ssl
import subprocess
import time
from dataclasses import replace
from pathlib import Path
from uuid import UUID, uuid4

import httpx
import pytest
import yaml

from tests.integration.test_execution_actuator_k3s import _load_client, _start_k3s
from tests.ops.test_nebius_pool_collector_runtime import collector_inputs as collector_inputs
from tests.ops.test_nebius_pool_runtime import desired_profile
from tests.ops.test_nebius_pool_runtime import runtime_inputs as runtime_inputs
from tests.unit.test_nebius_management_render import management_inputs as management_inputs
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs

pytestmark = pytest.mark.skipif(os.environ.get("LOOM_RUN_DISPOSABLE_K3S") != "1",
    reason="requires explicitly disposable Kubernetes")


@pytest.mark.timeout(600)
def test_manager_source_initializer_starts_on_actual_fsgroup_emptydir(tmp_path):
    from loom_service.application_management.build_deployment import (
        SOURCE_CREDENTIALS_PATH,
        SOURCE_SPOOL_PATH,
        mount_application_source,
    )
    from loom_service.application_management.installation import ApplicationSourceUploadSettings
    from tests.integration.test_execution_actuator_k3s import (
        _build_image,
        _docker_platform,
        _import_image,
    )

    tag = 'cr.eu-north1.nebius.cloud/test/service:source-spool-' + uuid4().hex
    cluster = None
    try:
        _build_image(tag=tag, dockerfile='deploy/Dockerfile.service', platform=_docker_platform())
        cluster = _start_k3s(ephemeral_storage_floor='1Gi')
        _, core, _ = _load_client(cluster)
        namespace = 'source-spool'
        core.create_namespace({'metadata': {'name': namespace,
            'labels': {'pod-security.kubernetes.io/enforce': 'restricted'}}})
        image = _import_image(cluster, tag=tag, root=tmp_path, ordinal=0)
        core.create_namespaced_secret(namespace, {'metadata': {'name': 'source-credentials'},
            'stringData': {'credentials.json': '{}'}})
        # Execute the actual emitted initializer in the actual service image;
        # the main process only proves the private spool is usable after init.
        pod = {'restartPolicy': 'Never', 'automountServiceAccountToken': False,
            'securityContext': {'seccompProfile': {'type': 'RuntimeDefault'}},
            'containers': [{'name': 'spool-user', 'image': image,
                'securityContext': {'allowPrivilegeEscalation': False, 'readOnlyRootFilesystem': True,
                    'capabilities': {'drop': ['ALL']}},
                'command': ['python', '-c', 'import os,stat; from pathlib import Path; '
                    'p=Path("/var/run/loom-application-source/spool"); '
                    'assert p.parent.stat().st_mode & stat.S_ISGID; '
                    'assert os.getuid()==p.stat().st_uid==1000; '
                    'assert stat.S_IMODE(p.stat().st_mode)==0o700; '
                    '(p/"upload").write_bytes(b"private source"); '
                    'assert (p/"upload").read_bytes()==b"private source"']} ]}
        mount_application_source(pod, settings=ApplicationSourceUploadSettings(
            credentials_file=Path(SOURCE_CREDENTIALS_PATH) / 'credentials.json',
            spool_directory=Path(SOURCE_SPOOL_PATH)), secret_name='source-credentials', service_image=image)
        core.create_namespaced_pod(namespace, {'apiVersion': 'v1', 'kind': 'Pod',
            'metadata': {'name': 'source-spool', 'namespace': namespace}, 'spec': pod})
        deadline = time.monotonic() + 90
        while True:
            observed = core.read_namespaced_pod('source-spool', namespace)
            if observed.status.phase in {'Succeeded', 'Failed'}:
                break
            assert time.monotonic() < deadline, observed.status.to_dict()
            time.sleep(1)
        if observed.status.phase != 'Succeeded':
            statuses = [*(observed.status.init_container_statuses or []), *(observed.status.container_statuses or [])]
            logs = {row.name: core.read_namespaced_pod_log('source-spool', namespace, container=row.name)
                for row in statuses if row.state.terminated is not None}
            raise AssertionError({'status': observed.status.to_dict(), 'logs': logs})
        initializer, = observed.status.init_container_statuses
        assert initializer.name == 'prepare-application-source'
        assert initializer.state.terminated.exit_code == 0
    finally:
        if cluster is not None:
            cluster.stop()
        subprocess.run(['docker', 'image', 'rm', tag], capture_output=True, check=False)


@pytest.mark.timeout(600)
def test_fixed_preflight_reads_kubelet_inside_production_actuator_image_without_operator_credentials(runtime_inputs, tmp_path, monkeypatch):
    from kubernetes import client
    from scripts.ops.nebius_pool_migration_guard import KubectlPoolGuardAPI

    from loom_service.pool_management.installation import PoolInstallation
    from tests.cluster.test_nebius_shared_ingress import _run
    from tests.integration.test_execution_actuator_k3s import (
        _build_image,
        _docker_platform,
        _import_image,
    )

    request, _, _, _ = runtime_inputs
    target = request.guards[0]
    participant = next(row for row in request.registration.spec.participants if row.participant_id == target.participant_id)
    namespace = participant.execution_namespace.name
    target_id = participant.targets[0].target_id
    tag = 'cr.eu-north1.nebius.cloud/test/actuator:pool-telemetry-' + uuid4().hex
    cluster = None
    try:
        _build_image(tag=tag, dockerfile='deploy/Dockerfile.execution-actuator', platform=_docker_platform())
        cluster = _start_k3s(node_name='telemetry-node', ephemeral_storage_floor='1Gi')
        _, core, _ = _load_client(cluster)
        apps, rbac = client.AppsV1Api(core.api_client), client.RbacAuthorizationV1Api(core.api_client)
        namespaces = {name: core.create_namespace({'metadata': {'name': name,
            'labels': {'pod-security.kubernetes.io/enforce': 'restricted'}}}).metadata.uid
            for name in (target.namespace, namespace)}
        image = _import_image(cluster, tag=tag, root=tmp_path, ordinal=0)
        core.create_namespaced_service_account(namespace, {'metadata': {'name': 'loom-execution-actuator'}})
        # No Job writer, Secret reader, token minting, proxy or exec privilege is
        # delivered to this Pod. First prove denial with only Node identity GET.
        role = {'apiVersion': 'rbac.authorization.k8s.io/v1', 'kind': 'ClusterRole',
            'metadata': {'name': 'pool-telemetry-reader'},
            'rules': [{'apiGroups': [''], 'resources': ['nodes'], 'verbs': ['get']}]}
        rbac.create_cluster_role(role)
        rbac.create_cluster_role_binding({'metadata': {'name': 'pool-telemetry-reader'},
            'roleRef': {'apiGroup': 'rbac.authorization.k8s.io', 'kind': 'ClusterRole', 'name': 'pool-telemetry-reader'},
            'subjects': [{'kind': 'ServiceAccount', 'name': 'loom-execution-actuator', 'namespace': namespace}]})
        original = {'apiVersion': 'apps/v1', 'kind': 'Deployment',
            'metadata': {'name': 'loom-execution-actuator', 'namespace': namespace},
            'spec': {'replicas': 1, 'selector': {'matchLabels': {'app.kubernetes.io/name': 'loom-execution-actuator'}},
                'template': {'metadata': {'labels': {'app.kubernetes.io/name': 'loom-execution-actuator'}},
                    'spec': {'serviceAccountName': 'loom-execution-actuator', 'automountServiceAccountToken': True,
                        'securityContext': {'runAsNonRoot': True, 'runAsUser': 65532, 'seccompProfile': {'type': 'RuntimeDefault'}},
                        'containers': [{'name': 'actuator', 'image': image, 'command': ['sleep', '600'],
                            'securityContext': {'allowPrivilegeEscalation': False, 'capabilities': {'drop': ['ALL']}},
                            'env': [{'name': 'LOOM_EXECUTION_ACTUATOR_' + key, 'value': value} for key, value in {
                                'DB_URL': 'unused-no-sql', 'CONTROLLER_ID': 'telemetry-check', 'NAMESPACE': namespace,
                                'TARGET_ID': target_id}.items()]}]}}}}
        apps.create_namespaced_deployment(namespace, original)
        _run(cluster, 'kubectl', 'rollout', 'status', 'deployment/loom-execution-actuator', '-n', namespace, '--timeout=60s')
        original = core.api_client.sanitize_for_serialization(apps.read_namespaced_deployment('loom-execution-actuator', namespace))
        spec = request.registration.spec.model_dump(mode='json')
        spec['participants'][0]['execution_namespace']['uid'] = namespaces[namespace]
        target = replace(target, namespace_uid=UUID(namespaces[target.namespace]))
        request = replace(request, guards=(target, *request.guards[1:]), registration=replace(request.registration,
            spec=PoolInstallation.model_validate(spec),
            binding=replace(request.registration.binding, kube_system_uid=core.read_namespace('kube-system').metadata.uid)))
        kubeconfig = tmp_path / 'telemetry-kubeconfig'
        kubeconfig.write_text('disposable-operator-transport-never-mounted')
        kubeconfig.chmod(0o600)
        api = KubectlPoolGuardAPI(request=request, kubeconfig=kubeconfig, executable=Path('/usr/bin/kubectl'))
        commands = []
        def transport(args):
            if args[0] == 'exec':
                commands.append(args)
            return json.loads(_run(cluster, 'kubectl', *args))
        monkeypatch.setattr(api, '_run', transport)
        api._runtime(target, original=original)  # Real Pod lineage and token defaults qualify.
        api.qualify_runtime_telemetry(target, original=original)
        assert api.telemetry_report() == {'status': 'unavailable', 'checks': 1, 'unavailable': 1,
            'reasons': ['kubelet_authorization']}
        assert len(commands) == 1
        role['rules'].append({'apiGroups': [''], 'resources': ['nodes/stats'], 'verbs': ['get']})
        rbac.patch_cluster_role('pool-telemetry-reader', role)
        # Kubelet caches webhook authorization denials. Repeated fixed reads,
        # not repeated mutations or weaker credentials, observe propagation.
        deadline = time.monotonic() + 60
        while True:
            api.qualify_runtime_telemetry(target, original=original)
            if api.telemetry_report() == {'status': 'available', 'checks': 1, 'unavailable': 0, 'reasons': []}:
                break
            assert api.telemetry_report() == {'status': 'unavailable', 'checks': 1, 'unavailable': 1,
                'reasons': ['kubelet_authorization']}
            assert time.monotonic() < deadline, 'in-Pod nodes/stats authorization did not become usable'
            time.sleep(1)
        assert all(row[12] == 'telemetry-node' and row[13] == core.read_node('telemetry-node').metadata.uid for row in commands)
        assert all('disposable-operator-transport' not in argument for row in commands for argument in row)
        # The production image/settings and projected authority ran; the
        # sleeping fixture is not acceptance of the actual controller loop.
    finally:
        if cluster is not None:
            cluster.stop()
        subprocess.run(['docker', 'image', 'rm', tag], capture_output=True, check=False)


@pytest.mark.timeout(180)
async def test_real_kubelet_stats_remain_available_without_proxy_or_execution_authority():
    from kubernetes import client

    from loom_execution_actuator.kubernetes_api import InClusterKubernetesJobApi

    container = await asyncio.to_thread(_start_k3s, node_name="stats-node", ephemeral_storage_floor="1Gi")
    scoped_client = None
    try:
        _, core, _ = await asyncio.to_thread(_load_client, container)
        rbac = client.RbacAuthorizationV1Api(core.api_client)
        namespace = "pool-usage"
        await asyncio.to_thread(core.create_namespace, {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": namespace}})
        await asyncio.to_thread(core.create_namespaced_service_account, namespace,
            {"apiVersion": "v1", "kind": "ServiceAccount", "metadata": {"name": "loom-execution-actuator"}})
        documents = list(yaml.safe_load_all((Path(__file__).resolve().parents[2]
            / "deploy/k8s/nebius-execution-actuator.yaml").read_text()))
        role, = (row for row in documents if row["kind"] == "ClusterRole")
        binding, = (row for row in documents if row["kind"] == "ClusterRoleBinding")
        binding["subjects"][0]["namespace"] = namespace
        await asyncio.to_thread(rbac.create_cluster_role, role)
        await asyncio.to_thread(rbac.create_cluster_role_binding, binding)
        issued = await asyncio.to_thread(core.create_namespaced_service_account_token, "loom-execution-actuator", namespace,
            client.AuthenticationV1TokenRequest(spec=client.V1TokenRequestSpec(audiences=[])))
        configuration = client.Configuration()
        configuration.host = core.api_client.configuration.host
        configuration.ssl_ca_cert = core.api_client.configuration.ssl_ca_cert
        configuration.api_key["authorization"] = issued.status.token
        configuration.api_key_prefix["authorization"] = "Bearer"
        scoped_client = client.ApiClient(configuration)  # No administrator certificate.
        actuator = InClusterKubernetesJobApi(client_module=client,
            core_api=client.CoreV1Api(scoped_client), batch_api=client.BatchV1Api(scoped_client))
        # Node bootstrap and the first cgroup sample follow API discovery.
        deadline = time.monotonic() + 30
        while True:
            nodes = await asyncio.to_thread(core.list_node)
            if nodes.items:
                break
            assert time.monotonic() < deadline, "disposable node did not register"
            await asyncio.sleep(1)
        summary = await actuator.resource_summary(node_name="stats-node")
        assert summary["node"]["nodeName"] == "stats-node"
        assert summary["node"]["cpu"]["usageCoreNanoSeconds"] >= 0
        assert summary["node"]["memory"]["workingSetBytes"] >= 0
        assert summary["node"]["fs"]["usedBytes"] >= 0
        async with httpx.AsyncClient(base_url=configuration.host,
                verify=ssl.create_default_context(cafile=configuration.ssl_ca_cert), trust_env=False,
                headers={"Authorization": "Bearer " + issued.status.token}, timeout=20) as http:
            assert (await http.get("/api/v1/nodes/stats-node/proxy/stats/summary")).status_code == 403
            assert (await http.get("/api/v1/namespaces/" + namespace + "/secrets")).status_code == 403
            assert (await http.post("/api/v1/namespaces/" + namespace + "/pods/missing/exec")).status_code == 403
        node = nodes.items[0]
        address, = (row.address for row in node.status.addresses if row.type == "InternalIP")
        async with httpx.AsyncClient(base_url="https://" + address + ":10250",
                verify=ssl.create_default_context(cafile=configuration.ssl_ca_cert), trust_env=False,
                headers={"Authorization": "Bearer " + issued.status.token}, timeout=20) as kubelet:
            assert (await kubelet.get("/pods")).status_code == 403
            assert (await kubelet.get("/exec/pool-usage/missing/execution")).status_code == 403
    finally:
        if scoped_client is not None:
            scoped_client.close()
        await asyncio.to_thread(container.stop)


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
                    # A missing Pod returns 404 only after the API authorizes
                    # scoped log reads. Reader roles must still deny exec.
                    assert (await http.get("/api/v1/namespaces/" + namespace + "/pods/missing/log")).status_code == 404
                    assert (await http.post("/api/v1/namespaces/" + namespace + "/pods/missing/exec")).status_code == 403
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
                assert (await http.get("/api/v1/namespaces/pool-foreign/pods/missing/log")).status_code == 403
                assert (await http.get("/api/v1/namespaces")).status_code == 403
    finally:
        await asyncio.to_thread(container.stop)
