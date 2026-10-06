"""Scheduled success through the protected readout, not installed acceptance."""
from __future__ import annotations

import json
import os
import time

import pytest
from scripts.ops.nebius_pool_startup_inspection import pool_startup_diagnostics

from tests.cluster.test_nebius_shared_ingress import PYTHON, _run
from tests.integration.test_execution_actuator_k3s import _load_client, _start_k3s
from tests.ops.test_nebius_management_gateway import operation

pytestmark = pytest.mark.skipif(os.environ.get('LOOM_RUN_DISPOSABLE_K3S') != '1',
    reason='requires explicitly disposable Kubernetes')


@pytest.mark.timeout(240)
def test_scheduled_collector_success_exposes_only_bound_identity_and_image_digest(tmp_path):
    cluster = _start_k3s(ephemeral_storage_floor='1Gi')
    try:
        _, core, batch = _load_client(cluster)
        metadata = operation(tmp_path)
        namespace = 'collector-observation'
        core.create_namespace({'metadata': {'name': namespace}})
        config_name = 'loom-pool-collector-' + 'b' * 32
        core.create_namespaced_config_map(namespace, {'metadata': {'name': config_name,
            'labels': {'loom.nebius/management-installation': metadata['installation_id']}}, 'immutable': True,
            'data': {'LOOM_EXECUTION_CAPACITY_COLLECTOR_COLLECTION_MODE': 'pool'}})
        core.create_namespaced_config_map(namespace, {'metadata': {'name': 'probe'},
            'data': {'loom_execution_capacity_collector.py': 'print("completed")\n'}})
        job_spec = {'backoffLimit': 0, 'activeDeadlineSeconds': 45, 'template': {'spec': {
            'automountServiceAccountToken': True, 'restartPolicy': 'Never',
            'containers': [{'name': 'collector', 'image': PYTHON,
                'command': ['python', '-m', 'loom_execution_capacity_collector'],
                'envFrom': [{'configMapRef': {'name': config_name}}],
                'env': [{'name': 'PYTHONPATH', 'value': '/probe'}],
                'volumeMounts': [{'name': 'probe', 'mountPath': '/probe', 'readOnly': True}]}],
            'initContainers': [{'name': 'prepare', 'image': PYTHON, 'command': ['python', '-c', 'pass']}],
            'volumes': [{'name': 'probe', 'configMap': {'name': 'probe'}}]}}}
        cron = batch.create_namespaced_cron_job(namespace, {'apiVersion': 'batch/v1', 'kind': 'CronJob',
            'metadata': {'name': 'loom-execution-capacity-collector'}, 'spec': {
                'suspend': False, 'schedule': '* * * * *', 'concurrencyPolicy': 'Forbid',
                'jobTemplate': {'spec': job_spec}}})

        class Kube:
            def get(self, kind, name, ns):
                return json.loads(_run(cluster, 'kubectl', 'get', kind, name, '-n', ns, '-o', 'json'))

            def run(self, *args, **kwargs):
                raise AssertionError('successful observation must not read logs')

        deadline = time.monotonic() + 120
        while True:
            pods = json.loads(_run(cluster, 'kubectl', 'get', 'pods', '-n', namespace, '-o', 'json'))['items']
            result = pool_startup_diagnostics(Kube(), pods, operation_json=json.dumps(metadata),
                execution_namespace=namespace)
            if result['collector_completion'] is not None:
                break
            assert time.monotonic() < deadline, 'scheduled collector completion not observed'
            time.sleep(0.5)
        observed = result['collector_completion']
        assert observed['controller_uid'] == cron.metadata.uid
        assert observed['image_sha256'] == PYTHON.split('@sha256:')[1]
        job = batch.read_namespaced_job(observed['job'], namespace)
        pod = core.read_namespaced_pod(observed['pod'], namespace)
        assert observed['job_uid'] == job.metadata.uid
        assert observed['pod_uid'] == pod.metadata.uid
        assert pod.status.phase == 'Succeeded' and job.status.succeeded == 1
        assert result['workloads'] == []
        assert 'docker.io' not in json.dumps(result)
    finally:
        cluster.stop()
