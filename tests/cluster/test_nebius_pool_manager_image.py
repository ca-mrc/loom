"""Actual image CAS, token mounts and Pod drain; not installed Nebius acceptance.

Ops tests own authenticated history/SQL qualification. This test isolates the
same HTTPS image transport against real API defaulting and running fixture Pods.
"""
from __future__ import annotations

import copy
import os
import ssl
import subprocess
import time
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
from scripts.ops.nebius_ingress_stage import _key, _snapshot
from scripts.ops.nebius_pool_manager_image_live import HTTPSPoolManagerImageAPI

from tests.integration.test_execution_actuator_k3s import (
    _build_image,
    _docker_platform,
    _import_image,
    _load_client,
    _start_k3s,
)

pytestmark = pytest.mark.skipif(os.environ.get('LOOM_RUN_DISPOSABLE_K3S') != '1',
    reason='requires explicitly disposable Kubernetes')


@pytest.mark.timeout(300)
def test_native_image_switch_preserves_projected_identity_and_proves_pod_drain(tmp_path):
    from kubernetes import client

    tags = ['docker.io/library/loom-image-correction-' + uuid4().hex + ':fixture' for _ in range(2)]
    cluster = None
    try:
        _build_image(tag=tags[0], dockerfile='tests/fixtures/execution_runtime_fixture/Dockerfile', platform=_docker_platform())
        subprocess.run(['docker', 'tag', *tags], check=True, capture_output=True, timeout=30)
        cluster = _start_k3s(ephemeral_storage_floor='1Gi')
        _, core, _ = _load_client(cluster)
        images = [_import_image(cluster, tag=tag, root=tmp_path, ordinal=index) for index, tag in enumerate(tags)]
        assert images[0] != images[1]
        namespace = 'image-correction-' + uuid4().hex[:12]
        core.create_namespace({'metadata': {'name': namespace,
            'labels': {'pod-security.kubernetes.io/enforce': 'restricted'}}})
        core.create_namespaced_service_account(namespace, {'metadata': {'name': 'fixture'},
            'automountServiceAccountToken': False})
        security = {'runAsNonRoot': True, 'runAsUser': 1000, 'allowPrivilegeEscalation': False,
            'readOnlyRootFilesystem': True, 'capabilities': {'drop': ['ALL']},
            'seccompProfile': {'type': 'RuntimeDefault'}}
        pod = {'serviceAccountName': 'fixture', 'automountServiceAccountToken': False,
            'terminationGracePeriodSeconds': 1, 'volumes': [{'name': 'identity', 'projected': {
                'sources': [{'serviceAccountToken': {'path': 'token', 'audience': 'loom-fixture', 'expirationSeconds': 600}}]}}],
            'containers': [{'name': 'loom-service', 'image': images[0], 'imagePullPolicy': 'IfNotPresent',
                'command': ['/fixture', 'idle'], 'securityContext': security,
                'volumeMounts': [{'name': 'identity', 'mountPath': '/var/run/fixture', 'readOnly': True}]}],
            'initContainers': [{'name': 'prepare', 'image': images[0], 'imagePullPolicy': 'IfNotPresent',
                'command': ['/fixture', 'phase', 'prepare'], 'securityContext': security}]}
        apps = client.AppsV1Api(core.api_client)
        apps.create_namespaced_deployment(namespace, {'apiVersion': 'apps/v1', 'kind': 'Deployment',
            'metadata': {'name': 'loom-service', 'namespace': namespace}, 'spec': {'replicas': 1,
                'strategy': {'type': 'Recreate'}, 'selector': {'matchLabels': {'app': 'loom-service'}},
                'template': {'metadata': {'labels': {'app': 'loom-service'}}, 'spec': pod}}})

        def running():
            rows = core.list_namespaced_pod(namespace).items
            return rows[0] if len(rows) == 1 and rows[0].status.phase == 'Running' else None

        deadline = time.monotonic() + 60
        while (old_pod := running()) is None:
            assert time.monotonic() < deadline, 'old fixture Pod did not run'
            time.sleep(0.1)
        configuration = core.api_client.configuration
        tls = ssl.create_default_context(cafile=configuration.ssl_ca_cert)
        tls.load_cert_chain(configuration.cert_file, configuration.key_file)
        path = '/apis/apps/v1/namespaces/' + namespace + '/deployments/loom-service'
        with httpx.Client(base_url=configuration.host, verify=tls, trust_env=False, timeout=20) as http:
            original = http.get(path).raise_for_status().json()
            desired = _snapshot(original)
            for container in (*desired['spec']['template']['spec']['containers'], *desired['spec']['template']['spec']['initContainers']):
                container['image'] = images[1]
            stopped, replaced = _snapshot(original), copy.deepcopy(desired)
            stopped['spec']['replicas'] = replaced['spec']['replicas'] = 0
            documents = (_snapshot(original), stopped, replaced, desired)
            record = {'phases': {phase: {'phase': 'prepared', 'before_resource_version': None}
                for phase in ('stop', 'template', 'start')}}

            class NativeImage(HTTPSPoolManagerImageAPI):
                def _qualify_binding(self):
                    return record

                def _scope(self):
                    pass  # Authenticated history/SQL is covered by owning ops tests.

                def qualify_closed(self):
                    pass

            api = object.__new__(NativeImage)
            api.request = SimpleNamespace(manager=original)
            api.closed = {_key(original): original}
            api.documents = documents
            api.image_binding = SimpleNamespace(candidate={'images': {'service': {'image_ref': images[1]}}})
            api.parent = SimpleNamespace(client=http,
                _request=lambda method, resource: http.request(method, resource).raise_for_status().json())
            for index, phase in enumerate(('stop', 'template', 'start')):
                target = documents[index + 1]
                deadline = time.monotonic() + 60
                while True:
                    if phase != 'stop' and not api.manager_drained(_key(original), documents[index]):
                        assert time.monotonic() < deadline, 'manager Pods did not drain'
                        time.sleep(0.1)
                        continue
                    before = api.read_workload(_key(original))
                    if api.preview_repair(phase, before, target) is None:
                        assert time.monotonic() < deadline
                        continue
                    record['phases'][phase] = {'phase': 'intent', 'before_resource_version': before['metadata']['resourceVersion']}
                    if api.patch_repair(phase, before, target):
                        record['phases'][phase]['phase'] = 'applied'
                        break
                    # Only an explicit API-server CAS rejection permits retry.
                    record['phases'][phase] = {'phase': 'prepared', 'before_resource_version': None}
                    assert time.monotonic() < deadline
                if phase == 'template':
                    assert not core.list_namespaced_pod(namespace).items
            deadline = time.monotonic() + 60
            while (new_pod := running()) is None:
                assert time.monotonic() < deadline, 'new fixture Pod did not run'
                time.sleep(0.1)
            after = api.read_workload(_key(original))
            assert after['metadata']['uid'] == original['metadata']['uid']
            assert after['spec'] == desired['spec']
            assert new_pod.metadata.uid != old_pod.metadata.uid
            assert new_pod.spec.automount_service_account_token is False
            assert new_pod.spec.volumes[0].projected.sources[0].service_account_token.audience == 'loom-fixture'
    finally:
        if cluster is not None:
            cluster.stop()
        subprocess.run(['docker', 'image', 'rm', *tags], capture_output=True, timeout=30, check=False)
