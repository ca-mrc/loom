"""Admission uses real shared-pool task/application Jobs, including deadlines."""
from __future__ import annotations

import copy
import importlib
import json
import os
import time
from dataclasses import asdict
from types import SimpleNamespace
from unittest.mock import Mock, call

import pytest

from loom_service.pool_management.profiles import PoolProfileCatalog
from tests.integration.test_execution_actuator_k3s import _load_client, _start_k3s
from tests.unit.test_nebius_application_image_renderer import build_inputs as build_inputs
from tests.unit.test_nebius_pool_application_image_render import pool_inputs as application_inputs
from tests.unit.test_nebius_pool_application_image_render import render as render_application
from tests.unit.test_nebius_pool_task_image_render import build_inputs as task_inputs
from tests.unit.test_nebius_pool_task_image_render import render as render_task


def wait_for_namespace_admission(core, namespace, pod, *, deadline, policy_name=None):
    """Wait for admission's namespace informer after a restricted-to-privileged patch."""
    from kubernetes.client.exceptions import ApiException

    forbidden = f'pods "{pod["metadata"]["name"]}" is forbidden: '
    while True:
        try:
            core.create_namespaced_pod(namespace, pod, dry_run='All')
        except ApiException as exc:
            try:
                message = json.loads(exc.body)['message']
            except (KeyError, TypeError, ValueError):
                message = ''
            if not isinstance(message, str):
                message = ''
            # The API patch can precede PSA's informer update. This old denial
            # is pending, never evidence that our own policy is enforced.
            stale_psa = exc.status == 403 and message.startswith(
                forbidden + 'violates PodSecurity "restricted:latest":')
            if not stale_psa:
                if policy_name is None:
                    raise
                assert exc.status == 403 and message.startswith(
                    forbidden + f"ValidatingAdmissionPolicy '{policy_name}' "), exc.body
                return
        else:
            if policy_name is None:
                return
        assert time.monotonic() < deadline, 'namespace admission did not converge'
        time.sleep(0.1)


def admission_error(message, *, status=403):
    from kubernetes.client.exceptions import ApiException

    error = ApiException(status=status)
    error.body = json.dumps({'kind': 'Status', 'status': 'Failure', 'reason': 'Forbidden',
        'code': status, 'message': f'pods "build-probe" is forbidden: {message}'})
    return error


@pytest.fixture
def namespace_admission(monkeypatch):
    clock = SimpleNamespace(now=0.0)
    monkeypatch.setattr(time, 'monotonic', lambda: clock.now)
    monkeypatch.setattr(time, 'sleep', lambda seconds: setattr(clock, 'now', clock.now + seconds))
    return SimpleNamespace(core=Mock(), pod={'metadata': {'name': 'build-probe'}}, clock=clock,
        stale=lambda: admission_error('violates PodSecurity "restricted:latest": privileged'),
        policy_name='loom-build-dev-native-build-v1')


def test_namespace_admission_waits_for_named_policy_after_stale_psa(namespace_admission):
    case = namespace_admission
    case.core.create_namespaced_pod.side_effect = [case.stale(), case.stale(),
        admission_error(f"ValidatingAdmissionPolicy '{case.policy_name}' with binding 'build' denied request: privileged")]
    wait_for_namespace_admission(case.core, 'builds', case.pod, deadline=1, policy_name=case.policy_name)
    assert case.core.create_namespaced_pod.call_args_list == [call('builds', case.pod, dry_run='All')] * 3


def test_namespace_admission_waits_for_foreign_acceptance_after_stale_psa(namespace_admission):
    case = namespace_admission
    case.core.create_namespaced_pod.side_effect = [case.stale(), case.stale(), object()]
    wait_for_namespace_admission(case.core, 'unrelated-builds', case.pod, deadline=1)
    assert case.core.create_namespaced_pod.call_args_list == [call('unrelated-builds', case.pod, dry_run='All')] * 3


@pytest.mark.parametrize('policy_name', ['loom-build-dev-native-build-v1', None])
@pytest.mark.parametrize(('status', 'message'), [
    (403, "ValidatingAdmissionPolicy 'unrelated-policy' denied request: privileged"),
    (403, "ValidatingAdmissionPolicy 'loom-build-dev-native-build-v1-other' denied request: privileged"),
    (403, 'violates PodSecurity "baseline:latest": privileged'),
    (403, 'violates PodSecurity "restricted:v1.36": privileged'),
    (403, 'webhook denied request mentioning PodSecurity "restricted:latest"'),
    (401, 'violates PodSecurity "restricted:latest": privileged'),
    (500, 'violates PodSecurity "restricted:latest": privileged'),
])
def test_namespace_admission_rejects_unexpected_errors(namespace_admission, policy_name, status, message):
    from kubernetes.client.exceptions import ApiException

    case = namespace_admission
    error = admission_error(message, status=status)
    case.core.create_namespaced_pod.side_effect = error
    with pytest.raises((ApiException, AssertionError)):
        wait_for_namespace_admission(case.core, 'builds', case.pod, deadline=1, policy_name=policy_name)
    assert case.core.create_namespaced_pod.call_count == 1
    assert case.clock.now == 0


@pytest.mark.parametrize('policy_name', ['loom-build-dev-native-build-v1', None])
def test_namespace_admission_bounds_stale_psa_wait(namespace_admission, policy_name):
    case = namespace_admission
    case.core.create_namespaced_pod.side_effect = case.stale()
    with pytest.raises(AssertionError, match='namespace admission did not converge'):
        wait_for_namespace_admission(case.core, 'builds', case.pod, deadline=0.2, policy_name=policy_name)
    assert case.core.create_namespaced_pod.call_count == 3
    assert case.clock.now == pytest.approx(0.2)


def test_namespace_admission_requires_policy_rejection(namespace_admission):
    case = namespace_admission
    case.core.create_namespaced_pod.return_value = object()
    with pytest.raises(AssertionError, match='namespace admission did not converge'):
        wait_for_namespace_admission(case.core, 'builds', case.pod, deadline=0.2, policy_name=case.policy_name)
    assert case.core.create_namespaced_pod.call_count == 3


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
@pytest.mark.skipif(os.environ.get('LOOM_RUN_DISPOSABLE_K3S') != '1',
    reason='requires explicitly disposable Kubernetes')
@pytest.mark.parametrize('runtime_overhead', [None, 50, 500])
def test_pooled_build_policy_compiles_and_constrains_both_native_workloads(build_inputs, runtime_overhead):
    from kubernetes import client, utils
    from kubernetes.client.exceptions import ApiException
    from scripts.ops.nebius_shared_pool_build_activation import build_policy_rejection_probe

    namespace, catalog, pods = policy_inputs(build_inputs)
    if runtime_overhead is not None:
        raw = catalog.model_dump(mode='json')
        for kind in ('task_images', 'application_images'):
            for profile in raw[kind]:
                profile['target']['runtime_class_name'] = 'build-runtime-fixture'
                profile['runtime_class_overhead'] = {'cpu_millis': 50, 'memory_mib': 32, 'storage_mib': 8}
        catalog = PoolProfileCatalog.model_validate(raw)
        for pod in pods:
            pod['spec']['runtimeClassName'] = 'build-runtime-fixture'
    policies = documents(namespace, catalog)
    cluster = _start_k3s(ephemeral_storage_floor='1Gi')
    try:
        _, core, _ = _load_client(cluster)
        admission = client.AdmissionregistrationV1Api(core.api_client)
        if runtime_overhead is not None:
            client.NodeV1Api(core.api_client).create_runtime_class({'apiVersion': 'node.k8s.io/v1',
                'kind': 'RuntimeClass', 'metadata': {'name': 'build-runtime-fixture'}, 'handler': 'runc',
                'overhead': {'podFixed': {'cpu': f'{runtime_overhead}m', 'memory': '32Mi', 'ephemeral-storage': '8Mi'}}})
        client.SchedulingV1Api(core.api_client).create_priority_class({'apiVersion': 'scheduling.k8s.io/v1',
            'kind': 'PriorityClass', 'metadata': {'name': 'unapproved-build-priority'}, 'value': 1000000,
            'globalDefault': False, 'preemptionPolicy': 'PreemptLowerPriority'})
        for ns in (namespace, 'unrelated-builds'):
            core.create_namespace({'metadata': {'name': ns,
                'labels': {'pod-security.kubernetes.io/enforce': 'restricted'}}})
            core.create_namespaced_service_account(ns, {'metadata': {'name': 'build-sa'},
                'automountServiceAccountToken': False})
        # The handoff probe must pass PSA restricted in the absence of our
        # policy; an unrelated PSA denial must not qualify policy enforcement.
        probe = build_policy_rejection_probe(namespace=namespace, service_account='build-sa',
            config=catalog.task_images[0].settings.job_config())
        core.create_namespaced_pod(namespace, probe, dry_run='All')
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
        while True:
            try:
                core.create_namespaced_pod(namespace, probe, dry_run='All')
            except ApiException as exc:
                assert exc.status == 403 and policies[0]['metadata']['name'] in exc.body, exc.body
                assert 'Only sequential prepare, build and publish phases are allowed.' in exc.body
                break
            assert time.monotonic() < deadline, 'pre-activation probe was not denied by the fixed policy'
            time.sleep(0.1)
        core.patch_namespace(namespace, {'metadata': {'labels': {'pod-security.kubernetes.io/enforce': 'privileged'}}})
        invalid = copy.deepcopy(pods[0])
        invalid['spec']['initContainers'][1]['securityContext']['privileged'] = True
        wait_for_namespace_admission(core, namespace, invalid, deadline=deadline,
            policy_name=policies[0]['metadata']['name'])
        for pod in pods:
            if runtime_overhead == 500:
                with pytest.raises(ApiException) as denied:
                    core.create_namespaced_pod(namespace, pod, dry_run='All')
                assert denied.value.status == 403 and policies[0]['metadata']['name'] in denied.value.body
                continue
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
        if runtime_overhead == 500:
            return
        # This namespace-scoped policy must not govern a foreign/staging namespace.
        core.patch_namespace('unrelated-builds', {'metadata': {'labels': {'pod-security.kubernetes.io/enforce': 'privileged'}}})
        invalid['metadata']['namespace'] = 'unrelated-builds'
        wait_for_namespace_admission(core, 'unrelated-builds', invalid, deadline=time.monotonic() + 25)
        assert core.list_namespaced_pod(namespace).items == []
        # One deliberately unschedulable fixture Pod exercises update-only API
        # routes. No build runs: this disposable node has no matching group label.
        existing = core.create_namespaced_pod(namespace, pods[0])
        assert not existing.spec.node_name
        ephemeral = {'spec': {'ephemeralContainers': [{'name': 'debug', 'image': pods[0]['spec']['containers'][0]['image'],
            'command': ['sh'], 'securityContext': {'privileged': True}}]}}
        with pytest.raises(ApiException) as denied:
            core.patch_namespaced_pod_ephemeralcontainers('build-probe', namespace, ephemeral, dry_run='All')
        assert denied.value.status == 403 and policies[0]['metadata']['name'] in denied.value.body
        with pytest.raises(ApiException) as denied:
            core.patch_namespaced_pod('build-probe', namespace, {'spec': {'containers': [{
                'name': 'publish', 'image': 'registry.example/foreign@sha256:' + 'f' * 64}]}}, dry_run='All')
        assert denied.value.status == 403 and policies[0]['metadata']['name'] in denied.value.body
        with pytest.raises(ApiException) as denied:
            # The pinned client predates the resize convenience method; use the
            # same authenticated client and actual Kubernetes subresource route.
            core.api_client.call_api('/api/v1/namespaces/{namespace}/pods/{name}/resize', 'PATCH',
                path_params={'namespace': namespace, 'name': 'build-probe'}, query_params=[('dryRun', 'All')],
                header_params={'Content-Type': 'application/strategic-merge-patch+json'},
                body={'spec': {'containers': [{'name': 'publish', 'resources': {
                    'requests': {'cpu': '2'}, 'limits': {'cpu': '2'}}}]}},
                auth_settings=['BearerToken'], _return_http_data_only=True)
        assert denied.value.status == 403 and policies[0]['metadata']['name'] in denied.value.body
    finally:
        cluster.stop()
