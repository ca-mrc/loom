"""Real CNI proof of independent dev runtime ingress and task egress."""
from __future__ import annotations

import asyncio
import os
import subprocess
from uuid import uuid4

import pytest

from tests.cluster.test_nebius_shared_ingress import _add_failure_diagnostics
from tests.integration.test_execution_actuator_k3s import (
    _build_image,
    _docker_platform,
    _import_image,
    _load_client,
    _pod_probe,
    _start_k3s,
    _wait_for_allowed_peer,
    _wait_for_pod,
    _wait_for_policy_programming,
)
from tests.ops.test_nebius_development_pool_retained import (
    application_management_inputs as application_management_inputs,
)
from tests.ops.test_nebius_development_pool_retained import (
    application_material as application_material,
)
from tests.ops.test_nebius_development_pool_retained import build_inputs as build_inputs
from tests.ops.test_nebius_development_pool_retained import capacity_checks as capacity_checks
from tests.ops.test_nebius_development_pool_retained import cloud as cloud
from tests.ops.test_nebius_development_pool_retained import completed_pool as completed_pool
from tests.ops.test_nebius_development_pool_retained import connected as connected
from tests.ops.test_nebius_development_pool_retained import database_runtime, network_runtime
from tests.ops.test_nebius_development_pool_retained import development_inputs as development_inputs
from tests.ops.test_nebius_development_pool_retained import entry as entry
from tests.ops.test_nebius_development_pool_retained import handoff as handoff
from tests.ops.test_nebius_development_pool_retained import installation as installation
from tests.ops.test_nebius_development_pool_retained import inventory as inventory
from tests.ops.test_nebius_development_pool_retained import live as live
from tests.ops.test_nebius_development_pool_retained import management_inputs as management_inputs
from tests.ops.test_nebius_development_pool_retained import manager_entry as manager_entry
from tests.ops.test_nebius_development_pool_retained import material as material
from tests.ops.test_nebius_development_pool_retained import (
    original_development_inputs as original_development_inputs,
)
from tests.ops.test_nebius_development_pool_retained import (
    original_manager_entry as original_manager_entry,
)
from tests.ops.test_nebius_development_pool_retained import (
    original_platform_inputs as original_platform_inputs,
)
from tests.ops.test_nebius_development_pool_retained import (
    original_pool_inputs as original_pool_inputs,
)
from tests.ops.test_nebius_development_pool_retained import platform_inputs as platform_inputs
from tests.ops.test_nebius_development_pool_retained import pool_entry as pool_entry
from tests.ops.test_nebius_development_pool_retained import pool_inputs as pool_inputs
from tests.ops.test_nebius_development_pool_retained import preflight as preflight
from tests.ops.test_nebius_development_pool_retained import provider_checks as provider_checks
from tests.ops.test_nebius_development_pool_retained import publication as publication
from tests.ops.test_nebius_development_pool_retained import published_source as published_source
from tests.ops.test_nebius_development_pool_retained import retained as retained
from tests.ops.test_nebius_development_pool_retained import route as route
from tests.ops.test_nebius_development_pool_retained import source_checkout as source_checkout
from tests.ops.test_nebius_development_pool_retained import tls_material as tls_material

pytestmark = pytest.mark.skipif(os.environ.get('LOOM_RUN_DISPOSABLE_K3S') != '1',
    reason='requires explicitly disposable Kubernetes')


@pytest.mark.timeout(300)
@pytest.mark.parametrize('manager_entry', ['foundation-runtime'], indirect=True)
@pytest.mark.parametrize('retained', [False], indirect=True)
async def test_dev_runtime_network_allows_required_peers_and_denies_cross_scope(completed_pool, tmp_path):
    from kubernetes import client, utils

    request = database_runtime(completed_pool)
    policies = network_runtime(request)
    spec = request.manager.retained.request.registration.spec
    ex, build, shared = 'loom-nebius-dev-execution', 'loom-nebius-dev-execution-build', 'loom-dev'
    tag = 'docker.io/library/loom-dev-runtime-network:' + uuid4().hex
    cluster = None
    try:
        platform = await asyncio.to_thread(_docker_platform)
        await asyncio.to_thread(_build_image, tag=tag,
            dockerfile='tests/fixtures/execution_runtime_fixture/Dockerfile', platform=platform)
        cluster = await asyncio.to_thread(_start_k3s, ephemeral_storage_floor='1Gi')
        _, core, _ = await asyncio.to_thread(_load_client, cluster)
        api = client.ApiClient()
        image = await asyncio.to_thread(_import_image, cluster, tag=tag, root=tmp_path, ordinal=1)
        for namespace in (shared, ex, build, 'foreign-runtime'):
            # Even matching installation/pool labels cannot bypass the exact namespace name.
            await asyncio.to_thread(core.create_namespace, {'metadata': {'name': namespace, 'labels': {
                'loom.nebius/management-installation': str(spec.installation_id),
                'loom.nebius/pool': str(spec.pool_id), 'pod-security.kubernetes.io/enforce': 'restricted'}}})
            await asyncio.to_thread(core.create_namespaced_service_account, namespace,
                {'metadata': {'name': 'network-fixture'}, 'automountServiceAccountToken': False})
        # Include the actual retained foundation policies: additive runtime rules
        # must work without replacing its shared-internal/personal/public access.
        base = [row['observed'] for row in request.foundation.phases['config']['resources'].values()
            if row['observed']['kind'] == 'NetworkPolicy']
        for document in [*base, *policies]:
            await asyncio.to_thread(utils.create_from_dict, api, document)

        def pod(name, namespace, labels, command):
            return {'apiVersion': 'v1', 'kind': 'Pod', 'metadata': {
                'name': name, 'namespace': namespace, 'labels': labels}, 'spec': {
                'restartPolicy': 'Never', 'automountServiceAccountToken': False,
                'serviceAccountName': 'network-fixture',
                'securityContext': {'runAsNonRoot': True, 'runAsUser': 1000,
                    'seccompProfile': {'type': 'RuntimeDefault'}},
                'containers': [{'name': 'fixture', 'image': image, 'imagePullPolicy': 'IfNotPresent',
                    'command': ['/fixture', *command], 'securityContext': {'allowPrivilegeEscalation': False,
                        'capabilities': {'drop': ['ALL']}, 'readOnlyRootFilesystem': True}}]}}

        servers = {}
        expected = {}
        for name, app, port in (('postgres', 'loom-postgres', 5432), ('gateway', 'loom-llm-gateway', 9100),
                ('control', 'loom-control-plane', 8080), ('wrong-port', 'loom-llm-gateway', 8090)):
            await asyncio.to_thread(core.create_namespaced_pod, shared,
                pod(name, shared, {'app': app, 'fixture-server': name}, ['server', str(port)]))
            server = await asyncio.to_thread(_wait_for_pod, core, shared, name)
            await asyncio.to_thread(core.create_namespaced_service, shared, {'metadata': {'name': name}, 'spec': {
                'selector': {'fixture-server': name}, 'ports': [{'port': port, 'targetPort': port, 'protocol': 'TCP'}]}})
            servers[name] = (f'http://{server.status.pod_ip}:{port}', f'http://{name}.{shared}.svc.cluster.local:{port}')
            attached = ['default-deny-ingress', 'development-internal']
            if app in {'loom-postgres', 'loom-llm-gateway'}:
                attached.append('development-runtime-' + ('postgres' if app == 'loom-postgres' else 'gateway'))
            expected[name] = (server.status.pod_ip, tuple(attached))
            await _wait_for_allowed_peer(core, shared, name, f'http://127.0.0.1:{port}')

        actuator = {'app.kubernetes.io/name': 'loom-execution-actuator'}
        task = {'app.kubernetes.io/component': 'execution-unit'}
        clients = {}
        for name, namespace, labels in (('actuator', ex, actuator), ('task', ex, task), ('unlabeled', ex, {}),
                ('build', build, {**actuator, **task}), ('foreign', 'foreign-runtime', {**actuator, **task})):
            await asyncio.to_thread(core.create_namespaced_pod, namespace, pod(name, namespace, labels, ['idle']))
            observed = await asyncio.to_thread(_wait_for_pod, core, namespace, name)
            clients[name] = (namespace, name)
            if name == 'task':
                expected[name] = (observed.status.pod_ip,
                    ('loom-execution-attempt-default-deny', 'loom-execution-attempt-egress'))
        await _wait_for_policy_programming(cluster, expected)
        for name, urls in servers.items():
            # The foundation's existing shared-side access still reaches every listener.
            for url in urls:
                await _wait_for_allowed_peer(core, shared, 'postgres', url)
            for client_name, (namespace, pod_name) in clients.items():
                for url in urls:
                    if (client_name, name) in {('actuator', 'postgres'), ('task', 'gateway')}:
                        await _wait_for_allowed_peer(core, namespace, pod_name, url)
                    else:
                        denied = await asyncio.to_thread(_pod_probe, core, namespace, pod_name, url)
                        assert 'exit:1 reason:network ' in denied or 'exit:1 reason:timeout ' in denied, (
                            client_name, name, url, denied)

        # Real task egress cannot reach another execution Pod, even on gateway's port.
        await asyncio.to_thread(core.create_namespaced_pod, ex,
            pod('peer', ex, {}, ['server', '9100']))
        peer = await asyncio.to_thread(_wait_for_pod, core, ex, 'peer')
        peer_url = f'http://{peer.status.pod_ip}:9100'
        await _wait_for_allowed_peer(core, ex, 'actuator', peer_url)
        denied = await asyncio.to_thread(_pod_probe, core, ex, 'task', peer_url)
        assert 'exit:1 reason:network ' in denied or 'exit:1 reason:timeout ' in denied, denied
    except (Exception, pytest.fail.Exception) as exc:
        if cluster is not None:
            await asyncio.to_thread(_add_failure_diagnostics, cluster, shared, exc)
        raise
    finally:
        if cluster is not None:
            await asyncio.to_thread(cluster.stop)
        await asyncio.to_thread(subprocess.run, ['docker', 'image', 'rm', '--force', tag],
            capture_output=True, check=False)
