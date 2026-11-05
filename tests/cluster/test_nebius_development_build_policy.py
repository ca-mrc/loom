"""Admission uses real shared-pool task/application Jobs, including deadlines."""
from __future__ import annotations

import copy
import importlib
import json
import os
import re
import subprocess
import time
from dataclasses import asdict
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import Mock, call

import pytest

from loom_service.pool_management.profiles import PoolProfileCatalog
from tests.integration.test_execution_actuator_k3s import (
    _build_timing,
    _k3s_system_image_archive,
    _load_client,
    _start_k3s,
)
from tests.unit.test_nebius_application_image_renderer import build_inputs as build_inputs
from tests.unit.test_nebius_pool_application_image_render import pool_inputs as application_inputs
from tests.unit.test_nebius_pool_application_image_render import render as render_application
from tests.unit.test_nebius_pool_task_image_render import build_inputs as task_inputs
from tests.unit.test_nebius_pool_task_image_render import render as render_task

# Startup milestones only: component starts, image import, node registration
# and controller leadership. Component flags are dropped and lines cut short.
_K3S_MILESTONE = re.compile(r'Starting k3s v|Imported images|Running kube-(?:apiserver|controller-manager|scheduler)'
    r'|k3s is up and running|[Rr]egistered node|leaderelection|validatingadmissionpolicy')


def _k3s_startup_milestones(output):
    lines = [line.split(' --', 1)[0][:160] for line in output.splitlines() if _K3S_MILESTONE.search(line)]
    return '\n'.join(lines if len(lines) <= 40 else [*lines[:20], '...', *lines[-20:]])


def _controller_manager_leadership(core, wall_start):
    """Seconds from test start until kube-controller-manager first led, if it has."""
    from kubernetes import client
    from kubernetes.client.exceptions import ApiException

    try:
        lease = client.CoordinationV1Api(core.api_client).read_namespaced_lease(
            'kube-controller-manager', 'kube-system', _request_timeout=(2, 3))
    except ApiException as error:
        return 'absent' if error.status == 404 else f'unavailable: ApiException {error.status}'
    except Exception as error:
        return f'unavailable: {type(error).__name__}'
    acquired = lease.spec.acquire_time if lease.spec else None
    return round(acquired.timestamp() - wall_start, 2) if acquired else 'not acquired'


def _add_build_policy_diagnostics(cluster, observed_policy, error, *, phases=None, leadership=None):
    """Retain bounded evidence from this test's disposable node before teardown."""
    if phases is not None:
        error.add_note('build policy phases (seconds from test start): ' + json.dumps(phases)
            + f'; controller-manager leader lease: {leadership}')
    try:
        metadata = getattr(observed_policy, 'metadata', None)
        status = getattr(observed_policy, 'status', None)
        snapshot = {'generation': getattr(metadata, 'generation', None),
            'status': status.to_dict() if status is not None else None}
        error.add_note('last observed build policy status: ' + json.dumps(snapshot, default=str)[-16384:])
    except Exception as diagnostic_error:
        error.add_note(f'build policy status unavailable: {type(diagnostic_error).__name__}')
    try:
        container_id = cluster.get_wrapped_container().id
    except Exception as diagnostic_error:
        error.add_note(f'disposable node unavailable: {type(diagnostic_error).__name__}')
        return
    commands = [
        ('node logs', ['docker', 'logs', '--tail=150', container_id], None, 16384),
        ('container state', ['docker', 'inspect', '--format={{json .State}}', container_id], None, 16384),
        ('node conditions', ['docker', 'exec', container_id, 'kubectl', '--request-timeout=2s',
            'get', 'nodes', '-o=jsonpath={range .items[*]}{.metadata.name}{": "}{.status.conditions}{"\\n"}{end}'],
            None, 16384),
        # Process states and kernel stacks show a blocked wait (D-state,
        # wchan) inside the node; the k3s log tail alone cannot.
        ('node processes', ['docker', 'exec', container_id, 'sh', '-c',
            'ps -ef; for p in /proc/[0-9]*; do echo "$p"; '
            'cat "$p/comm" "$p/wchan" "$p/stack" 2>/dev/null; done'], None, 32768),
        ('k3s startup milestones', ['docker', 'logs', container_id], _k3s_startup_milestones, 8192),
    ]
    for label, command, summarize, limit in commands:
        try:
            result = subprocess.run(command, text=True, capture_output=True, timeout=5)
            output = result.stdout + result.stderr
            error.add_note(f'{label} (exit {result.returncode}):\n'
                + (summarize(output) if summarize else output)[-limit:])
        except Exception as diagnostic_error:
            error.add_note(f'{label} unavailable: {type(diagnostic_error).__name__}')


def test_build_policy_diagnostics_retain_status_and_bound_node_reads(monkeypatch):
    cluster = SimpleNamespace(get_wrapped_container=lambda: SimpleNamespace(id='owned-node'))
    observed = SimpleNamespace(metadata=SimpleNamespace(generation=1),
        status=SimpleNamespace(to_dict=lambda: {'observed_generation': None, 'type_checking': None}),
        spec={'private': 'do-not-record-spec'})
    error = AssertionError('build policy compilation did not finish')
    commands = []

    def read(command, **kwargs):
        commands.append(command)
        assert kwargs == {'text': True, 'capture_output': True, 'timeout': 5}
        if command == ['docker', 'logs', 'owned-node']:
            return SimpleNamespace(returncode=0, stderr='', stdout=(
                'time="t0" level=info msg="Starting k3s v1.36"\n'
                'time="t1" level=info msg="Running kube-controller-manager --private-flag=do-not-record-flag"\n'
                + 'unrelated progress line\n' * 500
                + 'I1104 leaderelection.go:271] successfully acquired lease kube-system/kube-controller-manager\n'))
        return SimpleNamespace(returncode=0, stdout='node evidence ' + 'x' * 40000, stderr='')

    monkeypatch.setattr(subprocess, 'run', read)
    _add_build_policy_diagnostics(cluster, observed, error,
        phases={'api_ready': 31.5, 'policies_created': 32.0}, leadership='absent')
    notes = error.__notes__
    assert notes[0] == ('build policy phases (seconds from test start): '
        '{"api_ready": 31.5, "policies_created": 32.0}; controller-manager leader lease: absent')
    assert '"observed_generation": null' in notes[1]
    assert '"generation": 1' in notes[1]
    assert '"type_checking": null' in notes[1]
    assert [command[:3] for command in commands] == [['docker', 'logs', '--tail=150'],
        ['docker', 'inspect', '--format={{json .State}}'], ['docker', 'exec', 'owned-node'],
        ['docker', 'exec', 'owned-node'], ['docker', 'logs', 'owned-node']]
    assert '"$p/wchan" "$p/stack"' in commands[3][-1]
    assert len(notes[4]) < 33000 and all(len(note) < 17000 for note in notes[:4])
    milestones = notes[-1].splitlines()
    assert milestones[0] == 'k3s startup milestones (exit 0):'
    assert len(milestones) == 4 and 'successfully acquired lease' in milestones[-1]
    assert 'unrelated progress' not in notes[-1] and 'do-not-record-flag' not in notes[-1]
    assert 'do-not-record-spec' not in '\n'.join(notes)


def test_build_policy_diagnostics_failures_do_not_hide_original(monkeypatch):
    cluster = SimpleNamespace(get_wrapped_container=lambda: SimpleNamespace(id='owned-node'))
    error = AssertionError('original policy failure')
    commands = []

    def unavailable(command, **kwargs):
        commands.append(command)
        raise subprocess.TimeoutExpired(command, 5, output='private diagnostic data')

    monkeypatch.setattr(subprocess, 'run', unavailable)
    _add_build_policy_diagnostics(cluster, None, error)
    assert str(error) == 'original policy failure'
    assert len(commands) == 5
    assert sum('TimeoutExpired' in note for note in error.__notes__) == 5
    assert 'private diagnostic data' not in '\n'.join(error.__notes__)


def test_build_policy_diagnostics_missing_node_preserves_original():
    def unavailable():
        raise RuntimeError('private container error')

    error = AssertionError('original policy failure')
    _add_build_policy_diagnostics(SimpleNamespace(get_wrapped_container=unavailable), None, error)
    assert str(error) == 'original policy failure'
    assert error.__notes__[-1] == 'disposable node unavailable: RuntimeError'


@pytest.mark.parametrize(('lease', 'expected'), [
    (SimpleNamespace(spec=SimpleNamespace(acquire_time=datetime.fromtimestamp(130.5, tz=UTC))), 30.5),
    (SimpleNamespace(spec=SimpleNamespace(acquire_time=None)), 'not acquired'),
    (404, 'absent'),
    (500, 'unavailable: ApiException 500'),
    (TimeoutError(), 'unavailable: TimeoutError'),
])
def test_controller_manager_leadership_is_relative_and_never_raises(monkeypatch, lease, expected):
    from kubernetes import client
    from kubernetes.client.exceptions import ApiException

    def read(self, name, namespace, **kwargs):
        assert (name, namespace, kwargs) == ('kube-controller-manager', 'kube-system', {'_request_timeout': (2, 3)})
        if isinstance(lease, int):
            raise ApiException(status=lease)
        if isinstance(lease, Exception):
            raise lease
        return lease

    monkeypatch.setattr(client.CoordinationV1Api, 'read_namespaced_lease', read)
    core = SimpleNamespace(api_client=client.ApiClient(client.Configuration(host='http://127.0.0.1:1')))
    assert _controller_manager_leadership(core, 100.0) == expected


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


def test_owner_id_projection_is_exact_for_both_pooled_build_workloads(build_inputs):
    namespace, catalog, pods = policy_inputs(build_inputs)
    policy, _ = documents(namespace, catalog)
    expressions = '\n'.join(row['expression'] for row in policy['spec']['validations'])
    assert 'm.subPath == "subuid"' in expressions and 'm.subPath == "subgid"' in expressions
    assert '!has(m.subPath)' in expressions
    assert "!has(m.subPathExpr)" in expressions and "!has(m.mountPropagation)" in expressions
    assert "v.configMap.items.map(i, [i.key, i.path]) == [['subuid','subuid'],['subgid','subgid']]" in expressions
    assert "v.configMap.name == variables.p.volumes.filter(c, c.name == 'claim')[0].configMap.name" in expressions
    for pod in pods:
        spec = pod['spec']
        claim = next(v for v in spec['volumes'] if v['name'] == 'claim')['configMap']['name']
        subids = next(v for v in spec['volumes'] if v['name'] == 'builder-subids')['configMap']
        assert subids['name'] == claim
        assert subids['items'] == [{'key': key, 'path': key} for key in ('subuid', 'subgid')]
        mounts = spec['initContainers'][1]['volumeMounts']
        assert [m for m in mounts if m['name'] == 'builder-subids'] == [
            {'name': 'builder-subids', 'mountPath': '/etc/' + key, 'subPath': key, 'readOnly': True}
            for key in ('subuid', 'subgid')]


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
    # Failure-boundary timing for #2611: where a cold runner spends its time,
    # and whether the controller manager led before the policy budget began.
    started, wall_start, phases, leadership = time.monotonic(), time.time(), {}, None

    def reached(phase):
        phases[phase] = round(time.monotonic() - started, 2)

    _k3s_system_image_archive()
    reached('system_images')
    cluster = _start_k3s(ephemeral_storage_floor='1Gi')
    reached('node_started')
    observed_policy = core = None
    outcome = 'interrupted'
    try:
        _, core, _ = _load_client(cluster)
        reached('api_ready')
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
        reached('policies_created')
        deadline = time.monotonic() + 25
        while True:
            policy = admission.read_validating_admission_policy(policies[0]['metadata']['name'])
            observed_policy = policy
            if policy.status and policy.status.type_checking:
                assert not policy.status.type_checking.expression_warnings
                reached('type_checked')
                break
            assert time.monotonic() < deadline, 'build policy compilation did not finish'
            time.sleep(0.1)
        while True:
            try:
                core.create_namespaced_pod(namespace, probe, dry_run='All')
            except ApiException as exc:
                assert exc.status == 403 and policies[0]['metadata']['name'] in exc.body, exc.body
                assert 'Only sequential prepare, build and publish phases are allowed.' in exc.body
                reached('probe_denied')
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
                    'termination-secret', 'priority', 'subids-whole-configmap', 'subids-claim-key',
                    'subids-foreign-configmap', 'subids-writable', 'subids-wrong-subpath',
                    'subids-wrong-path', 'subids-expression', 'ordinary-subpath'):
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
                elif damage.startswith('subids-'):
                    volume = next(v for v in spec['volumes'] if v['name'] == 'builder-subids')['configMap']
                    mount = next(m for m in build['volumeMounts'] if m['mountPath'] == '/etc/subuid')
                    if damage == 'subids-whole-configmap':
                        volume.pop('items')
                    elif damage == 'subids-claim-key':
                        volume['items'][0]['key'] = 'claim.json'
                    elif damage == 'subids-foreign-configmap':
                        volume['name'] = 'loom-pool-' + 'f' * 32
                    elif damage == 'subids-writable':
                        mount['readOnly'] = False
                    elif damage == 'subids-wrong-subpath':
                        mount['subPath'] = 'claim.json'
                    elif damage == 'subids-wrong-path':
                        mount['mountPath'] = '/etc/passwd'
                    elif damage == 'subids-expression':
                        mount.pop('subPath')
                        mount['subPathExpr'] = '$(CLAIM)'
                elif damage == 'ordinary-subpath':
                    next(m for m in build['volumeMounts'] if m['name'] == 'build')['subPath'] = 'unapproved'
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
            outcome = 'passed'
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
        outcome = 'passed'
    except BaseException as error:
        outcome = type(error).__name__
        if core is not None and leadership is None:
            leadership = _controller_manager_leadership(core, wall_start)
        _add_build_policy_diagnostics(cluster, observed_policy, error, phases=phases, leadership=leadership)
        raise
    finally:
        # Read after every assertion so this lookup never spends the test's budgets.
        if core is not None and leadership is None:
            leadership = _controller_manager_leadership(core, wall_start)
        _build_timing({'role': 'build-policy', 'event': 'phases', 'case': str(runtime_overhead),
            'outcome': outcome, 'phases': phases, 'controller_manager_leader': leadership})
        cluster.stop()
