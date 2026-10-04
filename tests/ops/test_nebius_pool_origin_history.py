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
from tests.ops.test_nebius_pool_gateway_probe import projected_gateway as projected_gateway
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
    report = {'schema': 'loom.pool-management-history.v1', 'schema_revision': '0174', 'read_only': True,
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


@pytest.mark.parametrize('damage', [None, 'report', 'backend', 'credential', 'authority', 'schema', 'truthy', 'extra'])
@pytest.mark.parametrize('active', [False, True])
def test_registration_observer_uses_exact_management_backend(management_history, damage, active):
    from scripts.ops.nebius_pool_migration import PoolMigrationError

    from loom_service.pool_management.capacity import digest

    api, state = management_history
    spec = state.request.registration.spec
    state.report = {'schema': 'loom.pool-active-authority.v1' if active else 'loom.pool-startup-closed.v1', 'operation_id': str(spec.operation_id),
        'installation_sha256': digest(spec.model_dump(mode='json')), 'read_only': True, 'qualified': True}
    if damage == 'report':
        state.report['qualified'] = False
    elif damage == 'schema':
        state.report['schema'] = 'loom.pool-startup-closed.v1' if active else 'loom.pool-active-authority.v1'
    elif damage == 'truthy':
        state.report['qualified'] = 1
    elif damage == 'extra':
        state.report['unexpected'] = True
    elif damage == 'backend':
        state.after_drift = True
    elif damage == 'credential':
        state.secret['metadata']['resourceVersion'] = 'changed'
    elif damage == 'authority':
        api.kubeconfig.write_bytes(b'changed')
    qualify = api.qualify_active_pool if active else api.qualify_closed_pool
    if damage:
        with pytest.raises(PoolMigrationError) as error:
            qualify()
        assert error.value.stage == ('active_pool_authority' if active else 'startup_closed_registration')
    else:
        assert qualify() is None
        assert qualify() is None
        commands = [row for row in state.calls if row[0] == 'exec']
        assert len(commands) == 2
        assert all(row[:7] == ['exec', '-n', state.target.namespace, 'pod/loom-postgres-0', '-c', 'loom-postgres', '--'] for row in commands)
        assert all(state.request.guards[0].namespace not in row for row in state.calls)


@pytest.mark.parametrize('damage', ['contract', 'target', 'authority', 'credential'])
def test_active_pool_rechecks_authority_after_the_database_read(management_history, monkeypatch, damage):
    from scripts.ops.nebius_pool_migration import PoolMigrationError

    from loom_service.pool_management.capacity import digest

    api, state = management_history
    spec = state.request.registration.spec
    state.report = {'schema': 'loom.pool-active-authority.v1', 'operation_id': str(spec.operation_id),
        'installation_sha256': digest(spec.model_dump(mode='json')), 'read_only': True, 'qualified': True}
    original = api._run

    def run(args):
        result = original(args)
        if args[0] == 'exec':
            if damage == 'contract':
                state.request.guards[0].controller['spec']['template']['metadata']['annotations'] = {'drift': 'true'}
            elif damage == 'target':
                api.target.controller['spec']['replicas'] = 0
            elif damage == 'authority':
                api.kubeconfig.write_bytes(b'changed-after-read')
            else:
                state.secret['metadata']['resourceVersion'] = 'changed-after-read'
        return result

    monkeypatch.setattr(api, '_run', run)
    with pytest.raises(PoolMigrationError) as error:
        api.qualify_active_pool()
    assert error.value.stage == 'active_pool_authority'
    assert sum(row[0] == 'exec' for row in state.calls) == 1


@pytest.mark.parametrize('damage', [None, 'settings', 'pooled_settings', 'pod', 'secret', 'history', 'backend'])
@pytest.mark.parametrize('successor', [False, True, 'legacy'])
def test_manager_runtime_uses_the_retained_management_database(management_history, monkeypatch, damage, successor):
    """A template Secret alone cannot qualify the manager's effective settings."""
    import json
    import os
    import subprocess
    import sys

    from scripts.ops.nebius_pool_migration import PoolMigrationError

    legacy, successor = successor == 'legacy', successor is True
    api, state = management_history
    manager = copy.deepcopy(state.target.controller)
    manager['spec']['selector'] = {'matchLabels': {'app': 'loom-service'}}
    manager['spec']['template']['metadata']['labels'] = {'app': 'loom-service'}
    if legacy:
        manager['spec']['template']['spec']['containers'][0]['env'].append({'name': 'LOOM_SVC_SERVICE_MODE', 'value': 'management'})
    state.target = replace(state.target, controller=copy.deepcopy(manager))
    # Reconstruct the real reader so its immutable history hash binds this input.
    selected = type(api)(request=api.request, target=state.target, kubeconfig=api.kubeconfig, executable=Path('/usr/bin/kubectl'))
    options = {}
    if successor:
        manager['metadata'].setdefault('annotations', {})['loom.nebius/pool-cutover'] = 'fixture'
        manager['spec']['template']['spec']['containers'][0]['image'] = 'registry.example.com/loom@sha256:' + 'b' * 64
        catalog = api.kubeconfig.parent / 'profiles.json'
        catalog.write_text(api.request.registration.spec.profiles.model_dump_json())
        manager['spec']['template']['spec']['containers'][0]['env'].append(
            {'name': 'LOOM_SVC_POOL_PROFILES_FILE', 'value': str(catalog)})
        manager['spec']['template']['spec']['containers'][0]['env'].append(
            {'name': 'LOOM_SVC_SERVICE_MODE', 'value': 'management'})
        options['expected'] = copy.deepcopy(manager)
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
    if successor:
        environment.update(LOOM_SVC_SERVICE_MODE='management', LOOM_SVC_POOL_PROFILES_FILE=str(catalog))
    elif legacy:
        environment['LOOM_SVC_SERVICE_MODE'] = 'management'
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
            selected.qualify_manager_database(**options)
        assert error.value.stage == 'management_runtime_database'
    else:
        selected.qualify_manager_database(**options)
        assert len(processes) == 1 and json.loads(processes[0].stdout) == {'status': 'qualified'}
        if successor:
            selected.qualify_manager_pool_settings(expected=options['expected'])
            assert len(processes) == 2 and json.loads(processes[-1].stdout) == {'status': 'qualified'}
        elif legacy:
            selected.qualify_manager_legacy_settings(expected=manager)
            assert len(processes) == 2 and json.loads(processes[-1].stdout) == {'status': 'qualified'}
            environment['LOOM_SVC_POOL_PROFILES_FILE'] = '/private-successor-catalog.json'
            with pytest.raises(PoolMigrationError) as error:
                selected.qualify_manager_legacy_settings(expected=manager)
            assert error.value.stage == 'management_legacy_settings'
    assert all('private-marker' not in (row.stdout + row.stderr).decode() for row in processes)
    assert not state.executed  # No SQL query or write is part of this probe.


@pytest.mark.parametrize('damage', [None, 'db_settings', 'machine_settings', 'epoch_settings', 'token',
    'original_running', 'unready', 'uid', 'pod', 'replica_owner', 'template', 'account', 'name',
    'registration', 'db_reference', 'secret', 'backend', 'late_secret', 'late_backend', 'history', 'authority',
    'kubernetes', 'kubernetes_uid', 'late_kubernetes_secret', 'late_kubernetes_backend', 'late_kubernetes_pod',
    'capacity', 'late_capacity_secret', 'late_capacity_backend', 'late_capacity_pod'])
def test_gateway_runtime_binds_closed_child_identity_settings_and_management_database(management_history, projected_gateway, monkeypatch, damage):
    _gateway_runtime_case(management_history, projected_gateway, monkeypatch, damage, action='probe')


@pytest.mark.parametrize('damage', [None, 'db_settings', 'token', 'original_running', 'unready', 'uid',
    'pod', 'template', 'registration', 'secret', 'backend', 'history', 'authority', 'kubernetes', 'capacity',
    'late_capacity_pod', 'open_report', 'open_unknown', 'open_secret', 'open_backend', 'open_pod', 'open_authority'])
def test_pool_opening_uses_qualified_gateway_once(management_history, projected_gateway, monkeypatch, damage):
    _gateway_runtime_case(management_history, projected_gateway, monkeypatch, damage, action='open')


def _gateway_runtime_case(management_history, projected_gateway, monkeypatch, damage, *, action):
    """The new gateway has no running predecessor; only its scalar start is valid."""
    import hashlib
    import json
    import os
    import subprocess
    import sys

    from scripts.ops.nebius_pool_migration import PoolMigrationError
    from scripts.ops.nebius_pool_startup_capacity import (
        BOUND_POOL_ACTIVATION_COMMAND,
        BOUND_POOL_CAPACITY_COMMAND,
    )

    from loom_service.pool_management.installation import PoolInstallation
    from loom_service.pool_management.installation_render import render_gateway

    api, state = management_history
    spec = api.request.registration.spec.model_dump(mode='json')
    machine, = (row for row in spec['machines'] if row['role'] == 'gateway')
    token = api.kubeconfig.parent / 'gateway-token'
    token.write_text('private-gateway-marker')
    token.chmod(0o600)
    machine['token_sha256'] = hashlib.sha256(token.read_bytes()).hexdigest()
    migration = replace(api.request, registration=replace(api.request.registration, spec=PoolInstallation.model_validate(spec)))
    selected = type(api)(request=migration, target=state.target, kubeconfig=api.kubeconfig, executable=Path('/usr/bin/kubectl'))
    namespace, name = state.target.namespace, 'loom-pool-gateway'
    original, = render_gateway(migration.registration.spec, namespace=namespace,
        service_image=migration.registration.candidate['images']['service']['image_ref'],
        kubernetes_endpoint='https://kubernetes.default.svc')['workload']
    original['metadata'].update(uid=str(uuid4()), resourceVersion='1')
    container, = original['spec']['template']['spec']['containers']
    rows = {row['name']: row for row in container['env']}
    rows['LOOM_POOL_GATEWAY_BEARER_TOKEN_FILE']['value'] = str(token)
    wire, projected, _, _, _ = projected_gateway
    wire['namespaces'].clear()
    wire['namespaces'].update({ns.name: str(ns.uid) for participant in migration.registration.spec.participants
        for ns in (participant.execution_namespace, participant.build_namespace)})
    rows['LOOM_POOL_GATEWAY_KUBERNETES']['value'] = json.dumps(projected['kubernetes'])
    expected = copy.deepcopy(original)
    expected['spec']['replicas'] = 1
    current = copy.deepcopy(expected)
    current['status'] = {'observedGeneration': 1, 'replicas': 1, 'updatedReplicas': 1, 'availableReplicas': 1, 'readyReplicas': 1}
    replica = {'apiVersion': 'apps/v1', 'kind': 'ReplicaSet', 'metadata': {'name': name + '-abc', 'namespace': namespace,
        'uid': str(uuid4()), 'ownerReferences': [{'apiVersion': 'apps/v1', 'kind': 'Deployment',
            'name': name, 'uid': original['metadata']['uid'], 'controller': True}]},
        'spec': {'template': copy.deepcopy(expected['spec']['template'])}}
    pod = {'apiVersion': 'v1', 'kind': 'Pod', 'metadata': {'name': name + '-abc-def', 'namespace': namespace,
        'uid': str(uuid4()), 'labels': {'app.kubernetes.io/name': name},
        'ownerReferences': [{'apiVersion': 'apps/v1', 'kind': 'ReplicaSet', 'name': replica['metadata']['name'],
            'uid': replica['metadata']['uid'], 'controller': True}]}, 'spec': copy.deepcopy(expected['spec']['template']['spec']),
        'status': {'phase': 'Running', 'containerStatuses': [{'name': 'gateway', 'ready': True}]}}
    environment = {key: value for key, value in os.environ.items() if not key.startswith(('LOOM_', 'DATABASE_'))}
    environment.update({row['name']: row['value'] for row in container['env'] if 'value' in row})
    environment['LOOM_POOL_GATEWAY_DB_URL'] = base64.b64decode(state.secret['data']['service-url']).decode()
    if damage == 'db_settings':
        environment['LOOM_POOL_GATEWAY_DB_URL'] = 'postgresql+psycopg://foreign:private-marker@foreign.svc/loom'
    elif damage == 'machine_settings':
        environment['LOOM_POOL_GATEWAY_MACHINE_ID'] = str(uuid4())
    elif damage == 'epoch_settings':
        environment['LOOM_POOL_GATEWAY_ADMISSION_EPOCH'] = str(spec['admission_epoch'] + 1)
    elif damage == 'token':
        token.write_text('private-other-marker')
    elif damage == 'original_running':
        original['spec']['replicas'] = 1
    elif damage == 'unready':
        current['status']['readyReplicas'] = 0
    elif damage == 'uid':
        current['metadata']['uid'] = str(uuid4())
    elif damage == 'replica_owner':
        replica['metadata']['ownerReferences'][0]['uid'] = str(uuid4())
    elif damage in {'template', 'account', 'name', 'registration', 'db_reference'}:
        for document in (expected, current):
            if damage == 'template':
                document['spec']['template']['spec']['containers'][0]['image'] = 'registry.example/foreign@sha256:' + 'f' * 64
            elif damage == 'account':
                document['spec']['template']['spec']['serviceAccountName'] = 'foreign'
            elif damage == 'name':
                document['metadata']['name'] = 'foreign'
            else:
                settings = {row['name']: row for row in document['spec']['template']['spec']['containers'][0]['env']}
                if damage == 'registration':
                    settings['LOOM_POOL_GATEWAY_MACHINE_ID']['value'] = str(uuid4())
                else:
                    settings['LOOM_POOL_GATEWAY_DB_URL']['valueFrom']['secretKeyRef']['key'] = 'controller-url'
    elif damage == 'backend':
        state.database['metadata']['uid'] = str(uuid4())
    elif damage == 'history':
        selected.target.controller['metadata']['uid'] = str(uuid4())
    elif damage == 'authority':
        api.kubeconfig.write_bytes(b'private-changed-authority')
    elif damage == 'kubernetes':
        wire['damage'] = 'unauthorized'
    elif damage == 'kubernetes_uid':
        wire['damage'] = 'uid'
    processes, calls = [], []

    def run(args):
        calls.append(args)
        if args[0] == 'exec':
            assert args[:9] == ['exec', '-n', namespace, 'pod/' + pod['metadata']['name'], '-c', 'gateway', '--', 'python', '-c']
            assert 'private-' not in repr(args)
            if args[9] in {BOUND_POOL_CAPACITY_COMMAND, BOUND_POOL_ACTIVATION_COMMAND}:
                # This fixture doubles the remote database only; owning
                # integration tests execute the exact command against PostgreSQL.
                assert len(args) == 12 and all(len(value) == 64 for value in args[10:])
                opening = args[9] == BOUND_POOL_ACTIVATION_COMMAND
                # Bind the exact registration and a fresh challenge; a report
                # from another installation cannot satisfy this exec contract.
                import hmac

                from scripts.ops.nebius_pool_startup_capacity import expected_startup_capacity
                response = hmac.new(bytes.fromhex(args[10]), json.dumps(expected_startup_capacity(migration.registration.spec),
                    sort_keys=True, separators=(',', ':')).encode(), 'sha256').hexdigest()
                assert args[11] == response
                if opening:
                    assert len(processes) == 4 and calls[-1][10] != next(row[10] for row in calls if row[0] == 'exec' and row[9] == BOUND_POOL_CAPACITY_COMMAND)
                result = SimpleNamespace(returncode=1 if damage == 'capacity' or (opening and damage == 'open_unknown') else 0,
                    stdout=b'{"status": "global"}\n' if opening and damage != 'open_report' else b'{"status": "qualified"}\n', stderr=b'')
            else:
                result = subprocess.run([sys.executable, *args[8:]], capture_output=True, check=False, timeout=30,
                    cwd=api.kubeconfig.parent, env=environment)
            processes.append(result)
            if result.returncode:
                raise ValueError('private-probe-error')
            if damage == 'secret':
                state.secret['metadata']['resourceVersion'] = 'changed'
            if len(processes) == 2 and damage == 'late_secret':
                state.secret['metadata']['resourceVersion'] = 'changed'
            elif len(processes) == 2 and damage == 'late_backend':
                state.database['metadata']['uid'] = str(uuid4())
            elif len(processes) == 3 and damage == 'late_kubernetes_secret':
                state.secret['metadata']['resourceVersion'] = 'changed'
            elif len(processes) == 3 and damage == 'late_kubernetes_backend':
                state.database['metadata']['uid'] = str(uuid4())
            elif len(processes) == 3 and damage == 'late_kubernetes_pod':
                pod['metadata']['uid'] = str(uuid4())
            elif len(processes) == 4 and damage == 'late_capacity_secret':
                state.secret['metadata']['resourceVersion'] = 'changed'
            elif len(processes) == 4 and damage == 'late_capacity_backend':
                state.database['metadata']['uid'] = str(uuid4())
            elif len(processes) == 4 and damage == 'late_capacity_pod':
                pod['metadata']['uid'] = str(uuid4())
            elif len(processes) == 5 and damage == 'open_secret':
                state.secret['metadata']['resourceVersion'] = 'changed'
            elif len(processes) == 5 and damage == 'open_backend':
                state.database['metadata']['uid'] = str(uuid4())
            elif len(processes) == 5 and damage == 'open_pod':
                pod['metadata']['uid'] = str(uuid4())
            elif len(processes) == 5 and damage == 'open_authority':
                selected.kubeconfig.write_bytes(b'private-changed-authority')
            return json.loads(result.stdout)
        if args[:2] == ['get', 'deployment']:
            assert args[2:5] == [name, '-n', namespace]
            return copy.deepcopy(current)
        if args[:2] == ['get', 'replicaset']:
            return copy.deepcopy(replica)
        if args[:2] == ['get', '--raw'] and args[2] == f'/api/v1/namespaces/{namespace}/pods?labelSelector=app.kubernetes.io%2Fname%3D{name}&limit=100':
            observed = copy.deepcopy(pod)
            if damage == 'pod' and processes:
                observed['metadata']['uid'] = str(uuid4())
            return {'apiVersion': 'v1', 'kind': 'PodList', 'metadata': {'resourceVersion': '1'}, 'items': [observed]}
        return api._run(args)

    monkeypatch.setattr(selected, '_run', run)
    invoke = selected.qualify_gateway_runtime if action == 'probe' else selected.open_pool
    if damage:
        with pytest.raises(PoolMigrationError) as error:
            invoke(original=original, expected=expected)
        assert error.value.stage == ('gateway_runtime' if action == 'probe' else 'activation_open') and 'private-' not in str(error.value)
    else:
        assert invoke(original=original, expected=expected) is None
        assert len(processes) == (4 if action == 'probe' else 5) and all(row.returncode == 0 for row in processes)
        assert {path for _, path, _ in wire['requests']} == {'/api/v1/namespaces/' + name for name in wire['namespaces']}
    assert sum(row[0] == 'exec' and row[9] == BOUND_POOL_ACTIVATION_COMMAND for row in calls) == (
        1 if action == 'open' and (damage is None or damage.startswith('open_')) else 0)
    assert all('private-' not in (row.stdout + row.stderr).decode() for row in processes)
    assert all(row[0] in {'get', 'exec'} for row in calls)
    assert not state.executed


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
