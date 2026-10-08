"""Real public-phase API create/replay and restricted shared-ingress CNI access."""
from __future__ import annotations

import asyncio
import json
import os
import ssl
import subprocess
import tempfile
from dataclasses import replace
from pathlib import Path
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
from tests.ops.test_nebius_development_public import (
    application_management_inputs as application_management_inputs,
)
from tests.ops.test_nebius_development_public import application_material as application_material
from tests.ops.test_nebius_development_public import installation as installation
from tests.ops.test_nebius_development_public import management_inputs as management_inputs
from tests.ops.test_nebius_development_public import material as material
from tests.ops.test_nebius_development_public import platform_inputs as platform_inputs
from tests.ops.test_nebius_development_public import public_request
from tests.ops.test_nebius_development_public import tls_material as tls_material

pytestmark = pytest.mark.skipif(os.environ.get('LOOM_RUN_DISPOSABLE_K3S') != '1',
    reason='requires explicitly disposable Kubernetes')


@pytest.mark.timeout(300)
async def test_public_phase_replays_real_resources_and_exposes_only_web_api_to_ingress(installation, tmp_path):
    from kubernetes import client, utils
    from scripts.ops.nebius_application_setup import (
        HTTPSApplicationSetupAPI,
        stage_application_setup,
    )
    from scripts.ops.nebius_development_management_install import _setup
    from scripts.ops.nebius_management_material import ManagementBinding

    from loom_service.environment_management.deployment import ManagementDeployment

    request = public_request(installation[0])
    tag = 'docker.io/library/loom-dev-public-test:' + uuid4().hex
    container = None
    try:
        platform = await asyncio.to_thread(_docker_platform)
        await asyncio.to_thread(_build_image, tag=tag,
            dockerfile='tests/fixtures/execution_runtime_fixture/Dockerfile', platform=platform)
        with tempfile.TemporaryDirectory(prefix='loom-dev-public-test-') as directory:
            container = await asyncio.to_thread(_start_k3s, ephemeral_storage_floor='1Gi')
            _, core, _ = await asyncio.to_thread(_load_client, container)
            kube = core.api_client.configuration
            trust = ssl.create_default_context(cafile=kube.ssl_ca_cert)
            trust.load_cert_chain(kube.cert_file, kube.key_file)
            endpoint = 'https://127.0.0.1:' + str(container.get_exposed_port(6443))
            image = await asyncio.to_thread(_import_image, container, tag=tag, root=Path(directory), ordinal=1)
            foundation = request.deployment.installation.foundation
            namespaces = {}
            for name in ('loom-dev', request.binding.namespace, foundation.ingress_namespace, 'foreign-ingress'):
                namespaces[name] = await asyncio.to_thread(core.create_namespace, {'metadata': {
                    'name': name, 'labels': {'loom.nebius/management-installation': request.binding.installation_id,
                        'pod-security.kubernetes.io/enforce': 'restricted'}}})
                await asyncio.to_thread(core.create_namespaced_service_account, name, {
                    'metadata': {'name': 'network-fixture'}, 'automountServiceAccountToken': False})
            binding = ManagementBinding(request.binding.installation_id, request.binding.namespace,
                namespaces[request.binding.namespace].metadata.uid, core.read_namespace('kube-system').metadata.uid)
            raw = request.deployment.model_dump(mode='json')
            raw['installation']['applications']['runtime']['kubernetes']['endpoint'] = endpoint
            config = foundation.platform_config
            config['kubernetes_api_server'] = endpoint
            raw['installation']['foundation']['platform_config_json'] = json.dumps(config)
            request = replace(request, deployment=ManagementDeployment.model_validate(raw),
                shared_namespace_uid=namespaces['loom-dev'].metadata.uid)
            setup = _setup(request, binding)
            with HTTPSApplicationSetupAPI(request=setup, phase='development-public',
                    api_server=endpoint, ssl_context=trust) as api:
                responses = []
                api.client.event_hooks['response'].append(lambda response: responses.append(
                    (response.request.method, str(response.request.url), response.status_code)))
                receipt = stage_application_setup(request=setup, phase='development-public', api=api, state_dir=tmp_path / 'public')
                assert len(receipt['resource_uids']) == 2
                writes = [row for row in responses if row[0] == 'POST' and 'dryRun' not in row[1]]
                assert len(writes) == 2 and all(row[2] == 201 for row in writes)
                responses.clear()
                assert stage_application_setup(request=setup, phase='development-public', api=api,
                    state_dir=tmp_path / 'public') == receipt
                assert all(row[0] == 'GET' for row in responses)

            with client.ApiClient() as kube_api:
                await asyncio.to_thread(utils.create_from_dict, kube_api, {
                    'apiVersion': 'networking.k8s.io/v1', 'kind': 'NetworkPolicy',
                    'metadata': {'name': 'shared-deny', 'namespace': 'loom-dev'},
                    'spec': {'podSelector': {}, 'policyTypes': ['Ingress'], 'ingress': []}})

            def pod(name, namespace, labels, command):
                return {'apiVersion': 'v1', 'kind': 'Pod', 'metadata': {
                    'name': name, 'namespace': namespace, 'labels': labels}, 'spec': {
                        'serviceAccountName': 'network-fixture', 'automountServiceAccountToken': False,
                        'restartPolicy': 'Never', 'securityContext': {'runAsNonRoot': True, 'runAsUser': 1000,
                            'seccompProfile': {'type': 'RuntimeDefault'}}, 'containers': [{
                                'name': 'fixture', 'image': image, 'imagePullPolicy': 'IfNotPresent',
                                'command': ['/fixture', *command], 'securityContext': {
                                    'allowPrivilegeEscalation': False, 'readOnlyRootFilesystem': True,
                                    'capabilities': {'drop': ['ALL']}}}]}}

            servers = {}
            targets = [('web', 'loom-web', 8080), ('api', 'loom-service', 8090),
                ('control', 'loom-control-plane', 8080), ('database', 'loom-postgres', 5432)]
            for name, app, port in targets:
                await asyncio.to_thread(core.create_namespaced_pod, 'loom-dev',
                    pod(name, 'loom-dev', {'app': app}, ['server', str(port)]))
                servers[name] = await asyncio.to_thread(_wait_for_pod, core, 'loom-dev', name)
            clients = [('ingress', foundation.ingress_namespace, foundation.ingress_controller_label),
                ('wrong-pod', foundation.ingress_namespace, 'not-ingress'),
                ('wrong-namespace', 'foreign-ingress', foundation.ingress_controller_label)]
            for name, namespace, label in clients:
                await asyncio.to_thread(core.create_namespaced_pod, namespace,
                    pod(name, namespace, {'app.kubernetes.io/name': label}, ['idle']))
                await asyncio.to_thread(_wait_for_pod, core, namespace, name)
            await _wait_for_policy_programming(container, {name: (servers[name].status.pod_ip,
                ('shared-deny', 'loom-development-public') if name in {'web', 'api'} else ('shared-deny',))
                for name, _, _ in targets})
            for name, _, port in targets:
                await _wait_for_allowed_peer(core, 'loom-dev', name, f'http://127.0.0.1:{port}')
                url = f'http://{servers[name].status.pod_ip}:{port}'
                for source, namespace, _ in clients:
                    if source == 'ingress' and name in {'web', 'api'}:
                        await _wait_for_allowed_peer(core, namespace, source, url)
                    else:
                        result = await asyncio.to_thread(_pod_probe, core, namespace, source, url)
                        assert 'exit:1 reason:network ' in result or 'exit:1 reason:timeout ' in result, result
    except (Exception, pytest.fail.Exception) as error:
        if container is not None:
            await asyncio.to_thread(_add_failure_diagnostics, container, 'loom-dev', error)
        raise
    finally:
        if container is not None:
            await asyncio.to_thread(container.stop)
        await asyncio.to_thread(subprocess.run, ['docker', 'image', 'rm', '--force', tag],
            capture_output=True, check=False)
