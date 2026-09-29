"""Real API defaulting and native controller drain for two manager refreshes.

No runtime image or database is installed in this fixture. These observations
qualify the Kubernetes mechanism, not installed application readiness.
"""
from __future__ import annotations

import base64
import copy
import json
import os
import ssl
import time
from dataclasses import replace
from uuid import uuid4

import pytest
import yaml

from tests.integration.test_execution_actuator_k3s import _load_client, _start_k3s
from tests.ops.test_nebius_management_refresh import refresh_request as refresh_request
from tests.unit.test_nebius_management_render import (
    application_management_inputs as application_management_inputs,
)
from tests.unit.test_nebius_management_render import management_inputs as management_inputs
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs

pytestmark = pytest.mark.skipif(os.environ.get('LOOM_RUN_DISPOSABLE_K3S') != '1',
    reason='requires explicitly disposable Kubernetes')


@pytest.mark.timeout(240)
def test_repeat_refresh_preserves_retained_identity_and_observes_native_drain(refresh_request, tmp_path):
    from kubernetes import client
    from scripts.ops.nebius_ingress_stage import _snapshot
    from scripts.ops.nebius_management_material import ManagementBinding
    from scripts.ops.nebius_management_refresh_live import HTTPSManagementRefreshSwitchAPI
    from scripts.ops.nebius_management_refresh_resources import (
        HTTPSManagementRefreshResourcesAPI,
        ManagementRefreshResourcesRequest,
        refresh_resources_ready,
        stage_refresh_resources,
    )
    from scripts.ops.nebius_management_refresh_switch import (
        ManagementRefreshSwitchRequest,
        switch_refresh,
    )

    from loom_service.environment_management.deployment import ManagementDeployment

    cluster = _start_k3s(ephemeral_storage_floor='2Gi')
    try:
        _, core, _ = _load_client(cluster)
        apps = client.AppsV1Api(core.api_client)
        endpoint = 'https://127.0.0.1:' + str(cluster.get_exposed_port(6443))
        kube = yaml.safe_load(cluster.exec(['cat', '/etc/rancher/k3s/k3s.yaml']).output)
        trust = ssl.create_default_context(cadata=base64.b64decode(
            kube['clusters'][0]['cluster']['certificate-authority-data']).decode())
        user = kube['users'][0]['user']
        certificate, key = tmp_path / 'client.crt', tmp_path / 'client.key'
        certificate.write_bytes(base64.b64decode(user['client-certificate-data']))
        key.write_bytes(base64.b64decode(user['client-key-data']))
        key.chmod(0o600)
        trust.load_cert_chain(certificate, key)

        def local(deployment):
            raw = deployment.model_dump(mode='json')
            raw['installation']['applications']['runtime']['kubernetes']['endpoint'] = endpoint
            foundation = raw['installation']['foundation']
            platform = json.loads(foundation['platform_config_json'])
            platform['kubernetes_api_server'] = endpoint
            foundation['platform_config_json'] = json.dumps(platform)
            return ManagementDeployment.model_validate(raw)

        request = replace(refresh_request, before=local(refresh_request.before), after=local(refresh_request.after))
        namespace = request.after.namespace
        ns = core.create_namespace({'metadata': {'name': namespace, 'labels': {
            'loom.nebius/management-installation': str(request.after.installation_id),
            'pod-security.kubernetes.io/enforce': 'restricted'}}})
        shared = core.create_namespace({'metadata': {'name': request.after.installation.applications.shared.platform_namespace}})
        binding = ManagementBinding(str(request.after.installation_id), namespace, ns.metadata.uid,
            core.read_namespace('kube-system').metadata.uid)
        for name in ('loom-platform', 'loom-application-provisioner'):
            core.create_namespaced_service_account(namespace, {'metadata': {'name': name}, 'automountServiceAccountToken': False})
        retained = core.create_namespaced_secret(namespace, {'metadata': {'name': 'retained-test-material'},
            'immutable': True, 'stringData': {'test': 'disposable-only'}})
        apps.create_namespaced_deployment(namespace, _snapshot(request.active))
        prior_states = {}
        for iteration in range(2):
            (tmp_path / str(iteration)).mkdir(mode=0o700)
            deadline = time.monotonic() + 30
            while not core.list_namespaced_pod(namespace, label_selector='app=loom-service').items:
                assert time.monotonic() < deadline, 'disposable Deployment did not create a Pod'
                time.sleep(0.2)
            active = core.api_client.sanitize_for_serialization(apps.read_namespaced_deployment('loom-service', namespace))
            if iteration:
                candidate, profile = copy.deepcopy(request.candidate), copy.deepcopy(request.profile)
                candidate['images']['service']['image_ref'] = candidate['images']['service']['image_ref'].split('@')[0] + '@sha256:' + '8' * 64
                profile['task_image_ref'] = candidate['images']['service']['image_ref']
                request = replace(request, before=request.after, candidate=candidate, profile=profile)
            request = replace(request, active=active)
            switch = ManagementRefreshSwitchRequest(request, uuid4())
            resources = ManagementRefreshResourcesRequest(switch, binding, shared.metadata.uid, '0168', '0168')
            for phase in ('config', 'manager-probe', 'shared-probe', 'backup', 'migration', 'post-migration-probe'):
                with HTTPSManagementRefreshResourcesAPI(request=resources, phase=phase, api_server=endpoint, ssl_context=trust) as api:
                    args = dict(request=resources, phase=phase, api=api, state_dir=tmp_path / str(iteration) / phase)
                    receipt = stage_refresh_resources(**args)
                    assert stage_refresh_resources(**args) == receipt
                    assert refresh_resources_ready(**args) is (phase == 'config')
            with HTTPSManagementRefreshSwitchAPI(request=switch, binding=binding, shared_namespace_uid=shared.metadata.uid,
                    api_server=endpoint, ssl_context=trust, activation_check=lambda _request: True) as api:
                args = dict(request=switch, api=api, state_dir=tmp_path / str(iteration) / 'switch')
                while not switch_refresh(**args, activate=False):
                    assert time.monotonic() < deadline, 'native Deployment/ReplicaSet drain did not converge'
                    time.sleep(0.2)
                assert not core.list_namespaced_pod(namespace, label_selector='app=loom-service').items
                assert all(row.spec.replicas == 0 and row.status.observed_generation >= row.metadata.generation
                    for row in apps.list_namespaced_replica_set(namespace, label_selector='app=loom-service').items)
                assert switch_refresh(**args, activate=True) is True
                assert switch_refresh(**args, activate=True) is True
                assert api.read()['metadata']['uid'] == active['metadata']['uid']
            assert core.read_namespaced_secret('retained-test-material', namespace).metadata.uid == retained.metadata.uid
            for path, content in prior_states.items():
                assert path.read_bytes() == content
            prior_states.update({path: path.read_bytes() for path in (tmp_path / str(iteration)).rglob('*.json')})
    finally:
        cluster.stop()
