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
    report = {'schema': 'loom.pool-management-history.v1', 'schema_revision': '0172', 'read_only': True,
        'rows': [{'ordinal': 1, 'origin': origin.model_dump(mode='json'), 'application': None, 'operation': None}]}
    state = SimpleNamespace(request=request, target=target, participant=previous.target, origin=origin, report=report,
        database=database, service=service, pod=pod, secret=secret, calls=[], executed=False, after_drift=False)

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
