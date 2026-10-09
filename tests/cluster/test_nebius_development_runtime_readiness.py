"""Exercise dev rollout inspection against real Pod/ReplicaSet API defaults."""
from __future__ import annotations

import os
import subprocess
import time
from uuid import uuid4

import pytest

from tests.integration.test_execution_actuator_k3s import (
    _build_image,
    _docker_platform,
    _import_image,
    _load_client,
    _start_k3s,
)

pytestmark = pytest.mark.skipif(os.environ.get('LOOM_RUN_DISPOSABLE_K3S') != '1',
    reason='requires explicitly disposable Kubernetes')


@pytest.mark.timeout(240)
def test_actual_runtime_deployment_start_and_drain_keep_native_defaults(tmp_path):
    from kubernetes import client
    from scripts.ops.nebius_development_runtime_readiness import qualify_started_deployment
    from scripts.ops.nebius_ingress_stage import _snapshot
    from scripts.ops.nebius_pool_retirement import qualify_closed_workload_drain

    namespace, name = 'loom-dev', 'loom-runtime-readiness'
    tag = 'docker.io/library/loom-runtime-readiness:' + uuid4().hex
    cluster = None
    try:
        _build_image(tag=tag, dockerfile='tests/fixtures/execution_runtime_fixture/Dockerfile', platform=_docker_platform())
        cluster = _start_k3s(ephemeral_storage_floor='1Gi')
        _, core, _ = _load_client(cluster)
        apps = client.AppsV1Api(core.api_client)
        image = _import_image(cluster, tag=tag, root=tmp_path, ordinal=1)
        core.create_namespace({'metadata': {'name': namespace, 'labels': {'pod-security.kubernetes.io/enforce': 'restricted'}}})
        core.create_namespaced_service_account(namespace, {'metadata': {'name': name}, 'automountServiceAccountToken': True})
        apps.create_namespaced_deployment(namespace, {'apiVersion': 'apps/v1', 'kind': 'Deployment',
            'metadata': {'name': name}, 'spec': {'replicas': 1, 'selector': {'matchLabels': {'app': name}},
                'template': {'metadata': {'labels': {'app': name}}, 'spec': {
                    'automountServiceAccountToken': True, 'serviceAccountName': name,
                    'securityContext': {'runAsNonRoot': True, 'runAsUser': 65532,
                        'seccompProfile': {'type': 'RuntimeDefault'}},
                    'containers': [{'name': 'fixture', 'image': image, 'imagePullPolicy': 'IfNotPresent',
                        'command': ['/fixture', 'server', '8080'], 'securityContext': {
                            'allowPrivilegeEscalation': False, 'readOnlyRootFilesystem': True,
                            'capabilities': {'drop': ['ALL']}}}]}}}})

        def observation():
            serialize = core.api_client.sanitize_for_serialization
            return (serialize(apps.read_namespaced_deployment(name, namespace)),
                serialize(apps.list_namespaced_replica_set(namespace, limit=1000)),
                serialize(core.list_namespaced_pod(namespace, limit=1000)))

        deadline = time.monotonic() + 90
        while True:
            controller, children, pods = observation()
            if qualify_started_deployment(current=controller, children=children, pods=pods, region='eu-north1'):
                break
            assert time.monotonic() < deadline, 'actual runtime Pod did not become ready'
            time.sleep(0.5)
        assert len(pods['items']) == 1
        pod = pods['items'][0]
        assert pod['spec']['nodeName'] and pod['spec']['volumes'][0]['projected']['sources']
        original = controller
        apps.patch_namespaced_deployment(name, namespace, {'spec': {'replicas': 0}})
        deadline = time.monotonic() + 60
        while True:
            controller, children, pods = observation()
            stopped = _snapshot(original)
            stopped['spec']['replicas'] = 0
            if qualify_closed_workload_drain(original=original, desired=stopped, current=controller, children=children, pods=pods):
                break
            assert time.monotonic() < deadline, 'actual runtime Pod did not drain'
            time.sleep(0.5)
        assert pods['items'] == []
    finally:
        if cluster is not None:
            cluster.stop()
        subprocess.run(['docker', 'image', 'rm', '-f', tag], capture_output=True, check=False)
