"""Admission uses real shared-pool task/application Jobs, including deadlines."""
from __future__ import annotations

import copy
import importlib
import os
import time
from dataclasses import asdict

import pytest

from loom_service.pool_management.profiles import PoolProfileCatalog
from tests.integration.test_execution_actuator_k3s import _load_client, _start_k3s
from tests.unit.test_nebius_application_image_renderer import build_inputs as build_inputs
from tests.unit.test_nebius_pool_application_image_render import pool_inputs as application_inputs
from tests.unit.test_nebius_pool_application_image_render import render as render_application
from tests.unit.test_nebius_pool_task_image_render import build_inputs as task_inputs
from tests.unit.test_nebius_pool_task_image_render import render as render_task

pytestmark = pytest.mark.skipif(os.environ.get('LOOM_RUN_DISPOSABLE_K3S') != '1',
    reason='requires explicitly disposable Kubernetes')


def policy_inputs(build_inputs):
    task = task_inputs()
    application = application_inputs(build_inputs)
    task_profile, application_profile = task[2], application[2]
    catalog = PoolProfileCatalog.model_validate({'schema_version': 'loom.pool-profiles.v1',
        'image_admission_keyring': {}, 'execution': [],
        'task_images': [asdict(task_profile)], 'application_images': [asdict(application_profile)]})
    jobs = [render_task(*task).job, render_application(*application).job]
    pods = [{'apiVersion': 'v1', 'kind': 'Pod', 'metadata': {
        **job['spec']['template']['metadata'], 'name': 'build-probe', 'namespace': job['metadata']['namespace']},
        'spec': job['spec']['template']['spec']} for job in jobs]
    return task_profile.target.namespace, catalog, pods


def documents(namespace, catalog):
    module = 'scripts.ops.nebius_development_build_policy'
    if importlib.util.find_spec(module) is None:
        pytest.fail('closed-profile development build admission policy is missing')
    return importlib.import_module(module).build_policy_documents(namespace, catalog)


@pytest.mark.timeout(180)
def test_pooled_build_policy_compiles_and_constrains_both_native_workloads(build_inputs):
    from kubernetes import client, utils
    from kubernetes.client.exceptions import ApiException

    namespace, catalog, pods = policy_inputs(build_inputs)
    policies = documents(namespace, catalog)
    cluster = _start_k3s(ephemeral_storage_floor='1Gi')
    try:
        _, core, _ = _load_client(cluster)
        admission = client.AdmissionregistrationV1Api(core.api_client)
        client.SchedulingV1Api(core.api_client).create_priority_class({'apiVersion': 'scheduling.k8s.io/v1',
            'kind': 'PriorityClass', 'metadata': {'name': 'unapproved-build-priority'}, 'value': 1000000,
            'globalDefault': False, 'preemptionPolicy': 'PreemptLowerPriority'})
        for ns in (namespace, 'unrelated-builds'):
            core.create_namespace({'metadata': {'name': ns,
                'labels': {'pod-security.kubernetes.io/enforce': 'restricted'}}})
            core.create_namespaced_service_account(ns, {'metadata': {'name': 'build-sa'},
                'automountServiceAccountToken': False})
        # A rootless build is correctly forbidden before the dedicated policy.
        with pytest.raises(ApiException) as caught:
            core.create_namespaced_pod(namespace, pods[0], dry_run='All')
        assert caught.value.status == 403 and 'PodSecurity' in caught.value.body
        for policy in policies:
            utils.create_from_dict(core.api_client, policy)
        deadline = time.monotonic() + 25
        while True:
            policy = admission.read_validating_admission_policy(policies[0]['metadata']['name'])
            if policy.status and policy.status.type_checking:
                assert not policy.status.type_checking.expression_warnings
                break
            assert time.monotonic() < deadline, 'build policy compilation did not finish'
            time.sleep(0.1)
        core.patch_namespace(namespace, {'metadata': {'labels': {'pod-security.kubernetes.io/enforce': 'privileged'}}})
        invalid = copy.deepcopy(pods[0])
        invalid['spec']['initContainers'][1]['securityContext']['privileged'] = True
        while True:
            try:
                core.create_namespaced_pod(namespace, invalid, dry_run='All')
            except ApiException as exc:
                assert exc.status == 403 and policies[0]['metadata']['name'] in exc.body, exc.body
                break
            assert time.monotonic() < deadline, 'build policy was not enforced'
            time.sleep(0.1)
        for pod in pods:
            core.create_namespaced_pod(namespace, pod, dry_run='All')
            for damage in ('host-network', 'host-pid', 'process-sharing', 'token', 'host-volume', 'credential-mount',
                    'extra-container', 'extra-init', 'trusted-command', 'trusted-image', 'trusted-env',
                    'builder-root', 'builder-capability', 'trusted-escalation', 'env-from', 'hook',
                    'node-name', 'node-group', 'account', 'resources', 'projected-token', 'claim-secret',
                    'termination-secret', 'priority'):
                changed = copy.deepcopy(pod)
                spec = changed['spec']
                prepare, build = spec['initContainers']
                if damage in {'host-network', 'host-pid', 'process-sharing', 'token'}:
                    spec[{'host-network': 'hostNetwork', 'host-pid': 'hostPID',
                        'process-sharing': 'shareProcessNamespace', 'token': 'automountServiceAccountToken'}[damage]] = True
                elif damage == 'host-volume':
                    spec['volumes'][1] = {'name': 'build', 'hostPath': {'path': '/etc'}}
                elif damage == 'credential-mount':
                    build['volumeMounts'].append({'name': 'source', 'mountPath': '/credentials'})
                elif damage == 'extra-container':
                    spec['containers'].append({**copy.deepcopy(build), 'name': 'extra'})
                elif damage == 'extra-init':
                    spec['initContainers'].append({**copy.deepcopy(build), 'name': 'extra'})
                elif damage == 'trusted-command':
                    prepare['command'] = ['sh', '-c', 'echo unapproved']
                elif damage == 'trusted-image':
                    prepare['image'] = 'registry.example/unapproved@sha256:' + 'f' * 64
                elif damage == 'trusted-env':
                    prepare['env'].append({'name': 'PYTHONPATH', 'value': '/loom/build'})
                elif damage == 'builder-root':
                    build['securityContext'].update(runAsUser=0, runAsNonRoot=False)
                elif damage == 'builder-capability':
                    build['securityContext']['capabilities']['add'].append('SYS_ADMIN')
                elif damage == 'trusted-escalation':
                    prepare['securityContext']['allowPrivilegeEscalation'] = True
                elif damage == 'env-from':
                    build['envFrom'] = [{'secretRef': {'name': 'source-reader'}}]
                elif damage == 'hook':
                    spec['containers'][0]['lifecycle'] = {'postStart': {'exec': {'command': ['sh', '-c', 'echo unsafe']}}}
                elif damage == 'node-name':
                    spec['nodeName'] = 'bypass-scheduler'
                elif damage == 'node-group':
                    spec['nodeSelector']['nebius.com/node-group-id'] = 'foreign-group'
                elif damage == 'account':
                    spec['serviceAccountName'] = 'default'
                elif damage == 'resources':
                    build['resources']['limits']['cpu'] = '64'
                elif damage == 'projected-token':
                    spec['volumes'].append({'name': 'token', 'projected': {'sources': [
                        {'serviceAccountToken': {'path': 'token'}}]}})
                elif damage == 'termination-secret':
                    prepare['terminationMessagePath'] = '/var/run/loom-task-build/source/secret-key'
                elif damage == 'priority':
                    spec['priorityClassName'] = 'unapproved-build-priority'
                else:
                    next(v for v in spec['volumes'] if v['name'] == 'source')['secret']['secretName'] = 'foreign-secret'
                with pytest.raises(ApiException) as denied:
                    core.create_namespaced_pod(namespace, changed, dry_run='All')
                assert denied.value.status == 403 and policies[0]['metadata']['name'] in denied.value.body, (damage, denied.value.body)
        # This namespace-scoped policy must not govern a foreign/staging namespace.
        core.patch_namespace('unrelated-builds', {'metadata': {'labels': {'pod-security.kubernetes.io/enforce': 'privileged'}}})
        invalid['metadata']['namespace'] = 'unrelated-builds'
        core.create_namespaced_pod('unrelated-builds', invalid, dry_run='All')
        assert core.list_namespaced_pod(namespace).items == []
    finally:
        cluster.stop()
