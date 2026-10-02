"""History qualification reads the management DB, not the participant's DB."""
from __future__ import annotations

import base64
import copy
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest
from tests.ops.test_nebius_pool_database_guard import database_guard as database_guard
from tests.ops.test_nebius_pool_runtime import runtime_inputs as runtime_inputs
from tests.unit.test_nebius_management_render import management_inputs as management_inputs
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs

from loom.nebius_pool_priority import PoolWorkOriginV1


@pytest.fixture
def management_history(database_guard, monkeypatch):
    from scripts.ops.nebius_pool_origin_history import (
        KubectlPoolHistoryAPI,
        PoolManagementHistoryTarget,
    )

    guards, previous = database_guard
    request = previous.request
    binding = request.registration.binding
    namespace = binding.namespace
    resources = copy.deepcopy((previous.database, previous.service, previous.pod, previous.secret))
    database, service, pod, secret = resources
    for row in resources:
        row['metadata']['namespace'] = namespace
    secret['data'] = {'service-url': base64.b64encode(
        f'postgresql+psycopg://loom_service:private-marker@loom-postgres.{namespace}.svc:5432/loom'.encode()).decode()}
    manager = copy.deepcopy(previous.target.controller)
    manager['metadata'].update(namespace=namespace, name='loom-service')
    manager['spec']['template']['spec']['containers'] = [{'name': 'loom-service', 'image': 'registry.example/service@sha256:' + 'a' * 64,
        'env': [{'name': 'LOOM_SVC_DB_URL', 'valueFrom': {'secretKeyRef': {'name': 'loom-platform-db', 'key': 'service-url'}}}]}]
    target = PoolManagementHistoryTarget(namespace=namespace, namespace_uid=UUID(binding.namespace_uid), controller=manager,
        database=replace(previous.target.database, statefulset=copy.deepcopy(database), service=copy.deepcopy(service)))
    origin = PoolWorkOriginV1(data_environment_id=request.registration.spec.participants[0].environment_id,
        submission_id=uuid4(), kind='environment', application=None)
    report = {'schema': 'loom.pool-management-history.v1', 'schema_revision': '0173', 'read_only': True,
        'rows': [{'ordinal': 1, 'origin': origin.model_dump(mode='json'), 'application': None, 'operation': None}]}
    state = SimpleNamespace(request=request, target=target, participant=previous.target, origin=origin, report=report,
        database=database, service=service, pod=pod, secret=secret, calls=[], executed=False, after_drift=False)
    endpoints = copy.deepcopy(previous.endpoints)
    for row in endpoints['items']:
        row['metadata']['namespace'] = namespace
        row['endpoints'][0]['targetRef']['namespace'] = namespace

    def run(args):
        state.calls.append(args)
        if args[0] == 'exec':
            assert args[:17] == ['exec', '-n', namespace, 'pod/loom-postgres-0', '-c', 'loom-postgres', '--',
                'psql', '-X', '-qAt', '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', 'loom', '-c']
            state.executed = True
            return state.report
        assert args[0] == 'get'
        kind, name = args[1:3]
        if kind == 'namespace':
            uid = binding.kube_system_uid if name == 'kube-system' else binding.namespace_uid
            return {'apiVersion': 'v1', 'kind': 'Namespace', 'metadata': {'name': name, 'uid': uid}}
        if kind == '--raw':
            if name == f'/apis/discovery.k8s.io/v1/namespaces/{namespace}/endpointslices?labelSelector=kubernetes.io%2Fservice-name%3Dloom-postgres&limit=100':
                return copy.deepcopy(endpoints)
            assert name == f'/api/v1/namespaces/{namespace}/pods?labelSelector=app%3Dloom-postgres&limit=100'
            result = copy.deepcopy(state.pod)
            if state.executed and state.after_drift:
                result['metadata']['uid'] = str(uuid4())
            return {'apiVersion': 'v1', 'kind': 'PodList', 'metadata': {'resourceVersion': '1'}, 'items': [result]}
        assert args[-2:] == ['-o', 'json'] and args[3:5] == ['-n', namespace]
        return {'secret': state.secret, 'service': state.service, 'statefulset': state.database}[kind]

    api = KubectlPoolHistoryAPI(request=request, target=target, kubeconfig=guards.kubeconfig, executable=Path('/usr/bin/kubectl'))
    monkeypatch.setattr(api, '_run', run)
    return api, state


def test_history_observer_binds_management_namespace_and_replays_only_reads(management_history):
    api, state = management_history
    for _ in range(2):
        assert api.qualify_pending_origins(state.participant, (state.origin,)) is None
    assert sum(row[0] == 'exec' for row in state.calls) == 2
    assert all(row[0] in {'get', 'exec'} for row in state.calls)
    assert all(state.request.guards[0].namespace not in row for row in state.calls)


@pytest.mark.parametrize('damage', [None, 'settings', 'pooled_settings', 'pod', 'secret', 'history', 'backend'])
def test_manager_runtime_uses_the_retained_management_database(management_history, monkeypatch, damage):
    """A template Secret alone cannot qualify the manager's effective settings."""
    import json
    import os
    import subprocess
    import sys

    from scripts.ops.nebius_pool_migration import PoolMigrationError

    api, state = management_history
    manager = copy.deepcopy(state.target.controller)
    manager['spec']['selector'] = {'matchLabels': {'app': 'loom-service'}}
    manager['spec']['template']['metadata']['labels'] = {'app': 'loom-service'}
    state.target = replace(state.target, controller=copy.deepcopy(manager))
    # Reconstruct the real reader so its immutable history hash binds this input.
    selected = type(api)(request=api.request, target=state.target, kubeconfig=api.kubeconfig, executable=Path('/usr/bin/kubectl'))
    manager['status'] = {'observedGeneration': 1, 'replicas': 1, 'updatedReplicas': 1, 'availableReplicas': 1, 'readyReplicas': 1}
    namespace = state.target.namespace
    replica = {'apiVersion': 'apps/v1', 'kind': 'ReplicaSet', 'metadata': {'name': 'loom-service-abc',
        'namespace': namespace, 'uid': str(uuid4()), 'ownerReferences': [{'apiVersion': 'apps/v1', 'kind': 'Deployment',
            'name': 'loom-service', 'uid': manager['metadata']['uid'], 'controller': True}]},
        'spec': {'template': copy.deepcopy(manager['spec']['template'])}}
    pod = {'apiVersion': 'v1', 'kind': 'Pod', 'metadata': {'name': 'loom-service-abc-def', 'namespace': namespace,
        'uid': str(uuid4()), 'labels': {'app': 'loom-service'}, 'ownerReferences': [{'apiVersion': 'apps/v1', 'kind': 'ReplicaSet',
            'name': replica['metadata']['name'], 'uid': replica['metadata']['uid'], 'controller': True}]},
        'spec': copy.deepcopy(manager['spec']['template']['spec']),
        'status': {'phase': 'Running', 'containerStatuses': [{'name': 'loom-service', 'ready': True}]}}
    environment = {key: value for key, value in os.environ.items() if not key.startswith(('LOOM_', 'DATABASE_'))}
    environment.update(LOOM_SVC_DB_URL=base64.b64decode(state.secret['data']['service-url']).decode(),
        LOOM_SVC_MINIO_ACCESS_KEY='fixture', LOOM_SVC_MINIO_SECRET_KEY='fixture')
    if damage in {'settings', 'pooled_settings'}:
        environment['LOOM_SVC_DB_URL_POOL' if damage == 'pooled_settings' else 'LOOM_SVC_DB_URL'] = (
            'postgresql+psycopg://foreign:private-marker@loom-postgres.foreign.svc:5432/loom')
    elif damage == 'history':
        selected.target.controller['metadata']['uid'] = str(uuid4())
    elif damage == 'backend':
        state.database['metadata']['uid'] = str(uuid4())
    processes = []

    def run(args):
        if args[0] == 'exec':
            assert args[:9] == ['exec', '-n', namespace, 'pod/loom-service-abc-def', '-c', 'loom-service', '--', 'python', '-c']
            result = subprocess.run([sys.executable, *args[8:]], capture_output=True, check=False, timeout=30,
                cwd=api.kubeconfig.parent, env=environment)
            processes.append(result)
            if result.returncode:
                raise ValueError('private-transport-marker')
            if damage == 'secret':
                state.secret['metadata']['resourceVersion'] = 'different'
            return json.loads(result.stdout)
        if args[:2] == ['get', 'deployment']:
            assert args[2:5] == ['loom-service', '-n', namespace]
            return copy.deepcopy(manager)
        if args[:2] == ['get', 'replicaset']:
            return copy.deepcopy(replica)
        if args[:2] == ['get', '--raw'] and args[2] == f'/api/v1/namespaces/{namespace}/pods?labelSelector=app%3Dloom-service&limit=100':
            observed = copy.deepcopy(pod)
            if damage == 'pod' and processes:
                observed['metadata']['uid'] = str(uuid4())
            return {'apiVersion': 'v1', 'kind': 'PodList', 'metadata': {'resourceVersion': '1'}, 'items': [observed]}
        return api._run(args)

    monkeypatch.setattr(selected, '_run', run)
    if damage:
        with pytest.raises(PoolMigrationError) as error:
            selected.qualify_manager_database()
        assert error.value.stage == 'management_runtime_database'
    else:
        selected.qualify_manager_database()
        assert len(processes) == 1 and json.loads(processes[0].stdout) == {'status': 'qualified'}
    assert all('private-marker' not in (row.stdout + row.stderr).decode() for row in processes)
    assert not state.executed  # No SQL query or write is part of this probe.


@pytest.mark.parametrize('override', [
    {'value': 'postgresql+psycopg://foreign:private-marker@foreign.svc/loom'},
    {'valueFrom': {'secretKeyRef': {'name': 'foreign-db', 'key': 'url'}}},
])
def test_management_database_binding_rejects_an_unqualified_effective_pool_url(management_history, override):
    api, state = management_history
    state.target.controller['spec']['template']['spec']['containers'][0]['env'].append(
        {'name': 'LOOM_SVC_DB_URL_POOL', **override})
    with pytest.raises(ValueError):
        api._database(state.target, url_variable='LOOM_SVC_DB_URL')
    assert not any(row[0] == 'exec' for row in state.calls)


def test_history_scope_rejects_another_manager_or_migration_before_any_command(management_history):
    from scripts.ops.nebius_pool_migration import PoolMigrationError

    api, state = management_history
    assert api.qualify_binding(state.request, state.target.controller) is None
    wrong = copy.deepcopy(state.target.controller)
    wrong['metadata']['uid'] = str(uuid4())
    with pytest.raises(PoolMigrationError) as error:
        api.qualify_binding(state.request, wrong)
    assert error.value.stage == 'management_history_binding'
    request = replace(state.request, guards=state.request.guards[:-1])
    with pytest.raises(PoolMigrationError) as error:
        api.qualify_binding(request, state.target.controller)
    assert error.value.stage == 'management_history_binding'
    assert state.calls == []


@pytest.mark.parametrize('damage', ['database', 'secret', 'after_drift', 'participant', 'config', 'environment', 'report', 'origin', 'missing'])
def test_management_history_rejects_identity_drift_or_unqualified_pages(management_history, damage):
    from scripts.ops.nebius_pool_migration import PoolMigrationError

    api, state = management_history
    participant, origin = state.participant, state.origin
    if damage == 'database':
        state.database['metadata']['uid'] = str(uuid4())
    elif damage == 'secret':
        state.secret['metadata']['resourceVersion'] = '8'
    elif damage == 'after_drift':
        state.after_drift = True
    elif damage == 'participant':
        participant = replace(participant, namespace='foreign')
    elif damage == 'config':
        api.kubeconfig.write_text('changed-private-config')
    elif damage == 'environment':
        origin = origin.model_copy(update={'data_environment_id': uuid4()})
    elif damage == 'report':
        state.report['private-marker'] = 'unqualified'
    elif damage == 'origin':
        state.report['rows'][0]['origin']['submission_id'] = str(uuid4())
    else:
        state.report['rows'].clear()
    with pytest.raises(PoolMigrationError) as error:
        api.qualify_pending_origins(participant, (origin,))
    assert 'private-marker' not in str(error.value)
    assert sum(row[0] == 'exec' for row in state.calls) == (1 if damage in {'after_drift', 'report', 'origin', 'missing'} else 0)
