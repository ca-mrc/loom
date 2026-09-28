"""A running disposable old process retires before the fixed new template."""
from __future__ import annotations

import copy
import json
import os
import ssl
import time
from dataclasses import replace
from uuid import uuid4

import httpx
import pytest

from tests.integration.test_execution_actuator_k3s import (
    _build_image,
    _docker_platform,
    _import_image,
    _load_client,
    _start_k3s,
)
from tests.ops.test_nebius_application_setup import setup_request as setup_request
from tests.ops.test_nebius_management_switch import switch_inputs as switch_inputs
from tests.unit.test_nebius_management_render import (
    application_management_inputs as application_management_inputs,
)
from tests.unit.test_nebius_management_render import management_inputs as management_inputs
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs

pytestmark = pytest.mark.skipif(os.environ.get('LOOM_RUN_DISPOSABLE_K3S') != '1',
                                reason='requires explicitly disposable Kubernetes')


@pytest.mark.timeout(240)
def test_fixed_switch_retires_running_old_pods_and_preserves_deployment_uid(switch_inputs, tmp_path):
    from kubernetes import client
    from scripts.ops.nebius_application_setup import (
        HTTPSApplicationSetupAPI,
        application_setup_ready,
        stage_application_setup,
    )
    from scripts.ops.nebius_management_material import ManagementBinding
    from scripts.ops.nebius_management_switch import (
        HTTPSManagementSwitchAPI,
        ManagementSwitchRequest,
        activate_management,
        retire_management,
    )

    from loom_service.environment_management.deployment import ManagementDeployment

    old_request, _ = switch_inputs
    container = _start_k3s(ephemeral_storage_floor='1Gi')
    try:
        _, core, _ = _load_client(container)
        tag = 'docker.io/library/loom-management-switch:' + uuid4().hex
        _build_image(tag=tag, dockerfile='tests/fixtures/execution_runtime_fixture/Dockerfile', platform=_docker_platform())
        image = _import_image(container, tag=tag, root=tmp_path, ordinal=1)
        config = core.api_client.configuration
        trust = ssl.create_default_context(cafile=config.ssl_ca_cert)
        trust.load_cert_chain(config.cert_file, config.key_file)
        endpoint = 'https://127.0.0.1:' + str(container.get_exposed_port(6443))
        raw = old_request.setup.deployment.model_dump(mode='json')
        raw['installation']['applications']['runtime']['kubernetes']['endpoint'] = endpoint
        foundation = raw['installation']['foundation']
        platform = json.loads(foundation['platform_config_json'])
        platform['kubernetes_api_server'] = endpoint
        foundation['platform_config_json'] = json.dumps(platform)
        deployment = ManagementDeployment.model_validate(raw)
        management = core.create_namespace({'metadata': {'name': deployment.namespace, 'labels': {
            'loom.nebius/management-installation': str(deployment.installation_id),
            'pod-security.kubernetes.io/enforce': 'restricted'}}})
        shared = core.create_namespace({'metadata': {'name': deployment.installation.applications.shared.platform_namespace}})
        binding = ManagementBinding(str(deployment.installation_id), deployment.namespace,
            management.metadata.uid, core.read_namespace('kube-system').metadata.uid)
        setup = replace(old_request.setup, deployment=deployment, binding=binding, shared_namespace_uid=shared.metadata.uid)
        core.create_namespaced_service_account(binding.namespace, {'metadata': {'name': 'loom-management-provisioner'},
            'automountServiceAccountToken': False})
        original = copy.deepcopy(old_request.original)
        for key in ('uid', 'resourceVersion', 'generation'):
            original['metadata'].pop(key)
        original.pop('status')
        pod = original['spec']['template']['spec']
        for key in ('nodeSelector', 'volumes', 'initContainers'):
            pod.pop(key, None)
        pod['terminationGracePeriodSeconds'] = 1
        pod['containers'] = [{'name': 'service', 'image': image, 'command': ['/fixture', 'idle'],
            'securityContext': {'runAsNonRoot': True, 'allowPrivilegeEscalation': False,
                'capabilities': {'drop': ['ALL']}, 'seccompProfile': {'type': 'RuntimeDefault'}}}]
        apps = client.AppsV1Api(core.api_client)
        apps.create_namespaced_deployment(binding.namespace, original)
        deadline = time.monotonic() + 60
        while True:
            pods = core.list_namespaced_pod(binding.namespace, label_selector='app=loom-service').items
            if len(pods) == 1 and pods[0].status.phase == 'Running':
                old_pod_uid = pods[0].metadata.uid
                break
            assert time.monotonic() < deadline, 'disposable old management process did not run'
            time.sleep(0.2)
        path = '/apis/apps/v1/namespaces/' + binding.namespace + '/deployments/loom-service'
        with httpx.Client(base_url=endpoint, verify=trust, trust_env=False, timeout=20) as observer:
            original = observer.get(path).raise_for_status().json()
        # A CREATE fence also blocks delayed old-controller requests, without
        # killing the already-running Pod or denying new-manager/database Pods.
        with HTTPSApplicationSetupAPI(request=setup, phase='retirement', api_server=endpoint, ssl_context=trust) as api:
            fence_args = dict(request=setup, phase='retirement', api=api, state_dir=tmp_path / 'fence')
            stage_application_setup(**fence_args)
            deadline = time.monotonic() + 20
            while not application_setup_ready(**fence_args):
                assert time.monotonic() < deadline, 'management retirement policy did not type-check'
                time.sleep(0.1)
        core.create_namespaced_service_account(binding.namespace, {'metadata': {'name': 'loom-application-provisioner'},
            'automountServiceAccountToken': False})
        for account in ('loom-application-provisioner', 'default'):
            allowed = {'apiVersion': 'v1', 'kind': 'Pod', 'metadata': {'name': 'allowed-' + uuid4().hex},
                'spec': copy.deepcopy(pod)}
            allowed['spec']['serviceAccountName'] = account
            assert core.create_namespaced_pod(binding.namespace, allowed, dry_run='All').metadata.uid
        request = ManagementSwitchRequest(setup=setup, original=original)
        with HTTPSManagementSwitchAPI(request=request, api_server=endpoint, ssl_context=trust) as api:
            args = dict(request=request, api=api, state_dir=tmp_path / 'switch')
            deadline = time.monotonic() + 30
            while not retire_management(**args):
                assert time.monotonic() < deadline, 'old management process did not retire'
                time.sleep(0.2)
            assert core.list_namespaced_pod(binding.namespace, label_selector='app=loom-service').items == []
            assert api.read()['metadata']['uid'] == original['metadata']['uid']
            assert activate_management(**args) is True
            assert activate_management(**args) is True
            current = api.read()
        assert current['spec']['replicas'] == 1
        assert current['spec']['template']['spec']['serviceAccountName'] == 'loom-application-provisioner'
        assert current['spec']['template']['spec']['containers'][0]['image'] == setup.candidate['images']['service']['image_ref']
        assert all(pod.metadata.uid != old_pod_uid for pod in core.list_namespaced_pod(binding.namespace).items)
        # New runtime credentials/images are intentionally not installed here.
        # API cutover/retirement is proven, not management readiness or login.
    finally:
        container.stop()
