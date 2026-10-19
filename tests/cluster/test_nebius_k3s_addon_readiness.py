"""Native addon absence cannot satisfy an opt-in disposable K3s prerequisite."""
from __future__ import annotations

import os
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError

import pytest

from tests.integration.test_execution_actuator_k3s import _load_client, _start_k3s

pytestmark = pytest.mark.skipif(os.environ.get('LOOM_RUN_DISPOSABLE_K3S') != '1',
    reason='requires explicitly disposable Kubernetes')


def test_unknown_native_addon_is_rejected_before_connecting():
    with pytest.raises(ValueError, match='unknown disposable k3s native addon'):
        _load_client(object(), required_addons=('registry',))


@pytest.mark.timeout(300)
@pytest.mark.parametrize('addon, deployment_name', [('dns', 'coredns'), ('storage', 'local-path-provisioner')])
def test_required_native_addon_waits_for_restored_deployment(addon, deployment_name):
    from kubernetes import client

    cluster = _start_k3s(ephemeral_storage_floor='1Gi')
    try:
        _, core, _ = _load_client(cluster, required_addons=(addon,))
        apps = client.AppsV1Api(core.api_client)
        original = apps.read_namespaced_deployment(deployment_name, 'kube-system')
        count = original.spec.replicas
        selector = ','.join(f'{key}={value}' for key, value in original.spec.selector.match_labels.items())
        apps.patch_namespaced_deployment_scale(deployment_name, 'kube-system', {'spec': {'replicas': 0}})
        try:
            deadline = time.monotonic() + 60
            while core.list_namespaced_pod('kube-system', label_selector=selector, _request_timeout=5).items:
                assert time.monotonic() < deadline, 'controlled native addon did not stop'
                time.sleep(0.25)
            # API discovery remains usable even while this real native addon is absent.
            _load_client(cluster)
            with ThreadPoolExecutor(max_workers=1) as executor:
                pending = executor.submit(_load_client, cluster, required_addons=(addon,))
                try:
                    with pytest.raises(TimeoutError):
                        pending.result(timeout=1)
                finally:
                    # Restore only this disposable Deployment; no namespace/policy bypass.
                    apps.patch_namespaced_deployment_scale(deployment_name, 'kube-system', {'spec': {'replicas': count}})
                assert pending.result(timeout=95)[1] is not None
        finally:
            apps.patch_namespaced_deployment_scale(deployment_name, 'kube-system', {'spec': {'replicas': count}})
    finally:
        cluster.stop()
