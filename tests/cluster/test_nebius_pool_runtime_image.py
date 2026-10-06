"""Actual CronJob image CAS and child-drain boundaries, not Nebius acceptance."""
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
from scripts.ops.nebius_pool_manager_image_history import IMAGE_MARKER, RuntimeImageRepairBinding
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
def test_native_collector_image_switch_detects_late_child_and_runs_corrected_scheduled_job(tmp_path):
    from kubernetes import client

    tags = ['docker.io/library/loom-collector-correction-' + uuid4().hex + ':fixture' for _ in range(2)]
    cluster = None
    try:
        _build_image(tag=tags[0], dockerfile='tests/fixtures/execution_runtime_fixture/Dockerfile', platform=_docker_platform())
        subprocess.run(['docker', 'tag', *tags], check=True, capture_output=True, timeout=30)
        cluster = _start_k3s(ephemeral_storage_floor='1Gi')
        _, core, _ = _load_client(cluster)
        deadline = time.monotonic() + 60
        while not core.list_node().items:
            assert time.monotonic() < deadline, 'disposable node did not register'
            time.sleep(0.1)
        images = [_import_image(cluster, tag=tag, root=tmp_path, ordinal=index) for index, tag in enumerate(tags)]
        namespace = 'collector-correction-' + uuid4().hex[:12]
        core.create_namespace({'metadata': {'name': namespace,
            'labels': {'pod-security.kubernetes.io/enforce': 'restricted'}}})
        security = {'runAsNonRoot': True, 'runAsUser': 1000, 'allowPrivilegeEscalation': False,
            'readOnlyRootFilesystem': True, 'capabilities': {'drop': ['ALL']},
            'seccompProfile': {'type': 'RuntimeDefault'}}
        pod = {'restartPolicy': 'Never', 'automountServiceAccountToken': False,
            'terminationGracePeriodSeconds': 1,
            'containers': [{'name': 'collector', 'image': images[0], 'imagePullPolicy': 'IfNotPresent',
                'command': ['/fixture', 'phase', 'collector'], 'securityContext': security}],
            'initContainers': [{'name': 'prepare', 'image': images[0], 'imagePullPolicy': 'IfNotPresent',
                'command': ['/fixture', 'phase', 'prepare'], 'securityContext': security}]}
        batch = client.BatchV1Api(core.api_client)
        batch.create_namespaced_cron_job(namespace, {'apiVersion': 'batch/v1', 'kind': 'CronJob',
            'metadata': {'name': 'collector', 'namespace': namespace}, 'spec': {
                'schedule': '* * * * *', 'concurrencyPolicy': 'Forbid', 'suspend': False,
                'jobTemplate': {'spec': {'backoffLimit': 0, 'activeDeadlineSeconds': 30,
                    'template': {'metadata': {'labels': {'app': 'collector'}}, 'spec': pod}}}}})
        configuration = core.api_client.configuration
        tls = ssl.create_default_context(cafile=configuration.ssl_ca_cert)
        tls.load_cert_chain(configuration.cert_file, configuration.key_file)
        path = '/apis/batch/v1/namespaces/' + namespace + '/cronjobs/collector'
        with httpx.Client(base_url=configuration.host, verify=tls, trust_env=False, timeout=20) as http:
            original = http.get(path).raise_for_status().json()
            desired = _snapshot(original)
            for row in (*desired['spec']['jobTemplate']['spec']['template']['spec']['containers'],
                    *desired['spec']['jobTemplate']['spec']['template']['spec']['initContainers']):
                row['image'] = images[1]
            repair_id = uuid4()
            isolated = _snapshot(original)
            isolated['metadata'].setdefault('annotations', {})[IMAGE_MARKER] = str(repair_id)
            stopped, replaced = copy.deepcopy(isolated), copy.deepcopy(desired)
            replaced['metadata'].setdefault('annotations', {})[IMAGE_MARKER] = str(repair_id)
            stopped['spec']['suspend'] = replaced['spec']['suspend'] = True
            documents = (_snapshot(original), isolated, stopped, replaced, desired)
            record = {'phases': {phase: {'phase': 'prepared', 'before_resource_version': None}
                for phase in ('isolate', 'stop', 'template', 'start')}}

            class NativeImage(HTTPSPoolManagerImageAPI):
                def _qualify_binding(self):
                    return record

                def _scope(self):
                    pass  # Owning ops tests exercise real history/SQL scope.

                def qualify_closed(self):
                    pass

            api = object.__new__(NativeImage)
            api.closed = {_key(original): original}
            api.documents = documents
            # Transport-only fixture; immutable binding validation has ops tests.
            api.image_binding = RuntimeImageRepairBinding.model_construct(target='collector',
                operation_id=repair_id, candidate={'images': {'execution_actuator': {'image_ref': images[1]}}})
            api.parent = SimpleNamespace(client=http,
                _request=lambda method, resource: http.request(method, resource).raise_for_status().json())

            def drained(document):
                deadline = time.monotonic() + 60
                while not api.manager_drained(_key(original), document):
                    assert time.monotonic() < deadline, 'collector children did not terminate'
                    time.sleep(0.1)

            for index, phase in enumerate(('isolate', 'stop', 'template', 'start')):
                if phase in {'template', 'start'}:
                    drained(documents[index])
                if phase == 'template':
                    # A CronJob can have no children at one read, yet an already
                    # dispatched old Job can arrive after suspension. Hold this
                    # injected child suspended so the race assertion is stable.
                    late_spec = copy.deepcopy(original['spec']['jobTemplate']['spec'])
                    late_spec['suspend'] = True
                    batch.create_namespaced_job(namespace, {'apiVersion': 'batch/v1', 'kind': 'Job',
                        'metadata': {'name': 'late-old-child', 'namespace': namespace, 'ownerReferences': [{
                            'apiVersion': 'batch/v1', 'kind': 'CronJob', 'name': 'collector',
                            'uid': original['metadata']['uid'], 'controller': True, 'blockOwnerDeletion': True}]},
                        'spec': late_spec})
                    assert api.manager_drained(_key(original), documents[index]) is False
                    before = api.read_workload(_key(original))
                    with pytest.raises(ValueError, match='pool_repair_update_unconfirmed'):
                        api.preview_repair(phase, before, documents[index + 1])
                    batch.patch_namespaced_job('late-old-child', namespace, {'spec': {'suspend': False}})
                    drained(documents[index])
                deadline = time.monotonic() + 60
                while True:
                    before = api.read_workload(_key(original))
                    assert api.preview_repair(phase, before, documents[index + 1]) is not None
                    if phase == 'isolate':
                        # Real controller status churn invalidates a pre-preview
                        # version even though the complete desired spec is stable.
                        batch.patch_namespaced_cron_job_status('collector', namespace,
                            {'status': {'lastSuccessfulTime': '2026-01-01T00:00:00Z'}})
                        record['phases'][phase] = {'phase': 'intent',
                            'before_resource_version': before['metadata']['resourceVersion']}
                        assert api.patch_repair(phase, before, documents[index + 1]) is False
                        record['phases'][phase] = {'phase': 'prepared', 'before_resource_version': None}
                        latest = api.read_workload(_key(original))
                        assert latest['metadata']['resourceVersion'] != before['metadata']['resourceVersion']
                        assert _snapshot(latest) == _snapshot(before)
                        before = latest
                        assert api.preview_repair(phase, before, documents[index + 1]) is not None
                    record['phases'][phase] = {'phase': 'intent', 'before_resource_version': before['metadata']['resourceVersion']}
                    if api.patch_repair(phase, before, documents[index + 1]):
                        record['phases'][phase]['phase'] = 'applied'
                        break
                    record['phases'][phase] = {'phase': 'prepared', 'before_resource_version': None}
                    assert time.monotonic() < deadline
            # This is an actual controller-created Job, not the injected child.
            deadline = time.monotonic() + 90
            while True:
                jobs = batch.list_namespaced_job(namespace).items
                corrected = [job for job in jobs if job.spec.template.spec.containers[0].image == images[1]
                    and any(c.type == 'Complete' and c.status == 'True' for c in job.status.conditions or [])]
                if corrected:
                    break
                assert time.monotonic() < deadline, 'corrected scheduled Job did not succeed'
                time.sleep(0.2)
            job = corrected[-1]
            assert job.metadata.owner_references[0].uid == original['metadata']['uid']
            completed_pods = [row for row in core.list_namespaced_pod(namespace).items
                if any(owner.uid == job.metadata.uid for owner in row.metadata.owner_references or [])]
            assert completed_pods and all(row.status.phase == 'Succeeded' for row in completed_pods)
            assert all(container.image == images[1] for row in completed_pods
                for container in [*row.spec.containers, *row.spec.init_containers])
            after = api.read_workload(_key(original))
            assert after['metadata']['uid'] == original['metadata']['uid']
            assert after['spec'] == desired['spec'] and IMAGE_MARKER not in after['metadata'].get('annotations', {})
    finally:
        if cluster is not None:
            cluster.stop()
        subprocess.run(['docker', 'image', 'rm', *tags], capture_output=True, timeout=30, check=False)
