"""Guard observation survives CP retirement but cannot follow another database."""
from __future__ import annotations

import base64
import copy
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest
from tests.ops.test_nebius_pool_runtime import guest_runtime_inputs as guest_runtime_inputs
from tests.ops.test_nebius_pool_runtime import runtime_inputs as runtime_inputs
from tests.unit.test_nebius_application_image_renderer import build_inputs as build_inputs
from tests.unit.test_nebius_management_render import management_inputs as management_inputs
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


@pytest.fixture
def database_guard(runtime_inputs, platform_inputs, tmp_path, monkeypatch):
    from scripts.ops.nebius_pool_migration import PoolGuardDatabase
    from scripts.ops.nebius_pool_migration_guard import KubectlPoolGuardAPI

    from loom.nebius_platform_render import build_platform

    request, _, _, _ = runtime_inputs
    target = request.guards[0]
    config, candidate, profile = copy.deepcopy(platform_inputs)
    config['namespace'] = target.namespace
    documents = build_platform(config, candidate, profile, {}, repo_root=Path(__file__).resolve().parents[2])
    database, = [row for row in documents['20-database.yaml'] if row['kind'] == 'StatefulSet']
    service, = [row for row in documents['20-database.yaml'] if row['kind'] == 'Service']
    for row in (database, service):
        row['metadata'].update(uid=str(uuid4()), resourceVersion='1', generation=1)
    database['status'] = {'observedGeneration': 1, 'replicas': 1, 'readyReplicas': 1,
        'currentReplicas': 1, 'updatedReplicas': 1, 'currentRevision': 'db-rev', 'updateRevision': 'db-rev'}
    pod = {'apiVersion': 'v1', 'kind': 'Pod', 'metadata': {
        'name': 'loom-postgres-0', 'namespace': target.namespace, 'uid': str(uuid4()),
        'labels': {**database['spec']['template']['metadata']['labels'], 'controller-revision-hash': 'db-rev'},
        'ownerReferences': [{'apiVersion': 'apps/v1', 'kind': 'StatefulSet', 'name': 'loom-postgres',
            'uid': database['metadata']['uid'], 'controller': True}]},
        'spec': copy.deepcopy(database['spec']['template']['spec']),
        'status': {'phase': 'Running', 'podIP': '10.20.0.2', 'podIPs': [{'ip': '10.20.0.2'}],
            'containerStatuses': [{'name': 'loom-postgres', 'ready': True, 'restartCount': 0}]}}
    pod['spec']['volumes'].append({'name': 'data', 'persistentVolumeClaim': {'claimName': 'data-loom-postgres-0'}})
    secret = {'apiVersion': 'v1', 'kind': 'Secret', 'metadata': {'namespace': target.namespace,
        'name': 'loom-platform-db', 'uid': str(uuid4()), 'resourceVersion': '7'}, 'data': {
            'control-plane-url': base64.b64encode(
                f'postgresql+psycopg://loom_cp:private-marker@loom-postgres.{target.namespace}.svc:5432/loom'.encode()).decode()}}
    target = replace(target, database=PoolGuardDatabase(statefulset=copy.deepcopy(database), service=copy.deepcopy(service),
        credential_uid=UUID(secret['metadata']['uid']), credential_resource_version='7'))
    request = replace(request, guards=(target, *request.guards[1:]))
    kubeconfig = tmp_path / 'kubeconfig'
    kubeconfig.write_text('test-config')
    kubeconfig.chmod(0o600)
    api = KubectlPoolGuardAPI(request=request, kubeconfig=kubeconfig, executable=Path('/usr/bin/kubectl'))
    state = SimpleNamespace(request=request, target=target, database=database, service=service, pod=pod, secret=secret,
        calls=[], status='held', continuation=False, second_pod=False, executed=False, after_drift=False, exec_hook=None)
    state.endpoints = {'apiVersion': 'discovery.k8s.io/v1', 'kind': 'EndpointSliceList',
        'metadata': {'resourceVersion': '3'}, 'items': [{
            'apiVersion': 'discovery.k8s.io/v1', 'kind': 'EndpointSlice', 'metadata': {
                'name': 'loom-postgres-abcde', 'namespace': target.namespace, 'uid': str(uuid4()),
                'labels': {'kubernetes.io/service-name': 'loom-postgres'},
                'ownerReferences': [{'apiVersion': 'v1', 'kind': 'Service', 'name': 'loom-postgres',
                    'uid': service['metadata']['uid'], 'controller': True}]},
            'addressType': 'IPv4', 'ports': [{'name': service['spec']['ports'][0].get('name'), 'port': 5432, 'protocol': 'TCP'}],
            'endpoints': [{'addresses': ['10.20.0.2'], 'conditions': {'ready': True, 'serving': True, 'terminating': False},
                'targetRef': {'kind': 'Pod', 'namespace': target.namespace, 'name': 'loom-postgres-0',
                    'uid': pod['metadata']['uid']}}]}]}

    def run(args):
        state.calls.append(args)
        if args[0] == 'exec':
            assert args[:7] == ['exec', '-n', target.namespace, 'pod/loom-postgres-0', '-c', 'loom-postgres', '--']
            assert args[7:17] == ['psql', '-X', '-qAt', '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', 'loom', '-c']
            state.executed = True
            if state.exec_hook:
                return state.exec_hook(args[-1])
            return {'status': state.status}
        assert args[0] == 'get'
        kind, name = args[1:3]
        if kind == 'namespace':
            uid = request.registration.binding.kube_system_uid if name == 'kube-system' else str(target.namespace_uid)
            return {'apiVersion': 'v1', 'kind': 'Namespace', 'metadata': {'name': name, 'uid': uid}}
        if kind == '--raw':
            if args == ['get', '--raw', f'/apis/discovery.k8s.io/v1/namespaces/{target.namespace}/endpointslices?labelSelector=kubernetes.io%2Fservice-name%3Dloom-postgres&limit=100']:
                endpoints = copy.deepcopy(state.endpoints)
                if state.executed and getattr(state, 'endpoint_after_drift', False):
                    endpoints['items'][0]['endpoints'][0]['targetRef']['uid'] = str(uuid4())
                return endpoints
            assert args == ['get', '--raw', f'/api/v1/namespaces/{target.namespace}/pods?labelSelector=app%3Dloom-postgres&limit=100']
            rows = [state.pod] * (2 if state.second_pod else 1)
            if state.executed and state.after_drift:
                rows = copy.deepcopy(rows)
                rows[0]['metadata']['uid'] = str(uuid4())
            return {'apiVersion': 'v1', 'kind': 'PodList', 'metadata': {
                'resourceVersion': '1', 'continue': 'next' if state.continuation else ''}, 'items': rows}
        assert kind in {'statefulset', 'service', 'secret'}, 'guard must not depend on the retired CP'
        expected_name = 'loom-platform-db' if kind == 'secret' else 'loom-postgres'
        assert name == expected_name
        return copy.deepcopy({'statefulset': state.database, 'service': state.service, 'secret': state.secret}[kind])

    monkeypatch.setattr(api, '_run', run)
    return api, state


@pytest.mark.parametrize('damage', ['wrong_pod', 'wrong_address', 'wrong_service', 'wrong_port', 'not_ready',
    'terminating', 'extra_backend', 'missing_backend', 'pagination', 'after_drift'])
def test_database_read_requires_actual_service_to_postgres_backend_correspondence(database_guard, damage):
    from scripts.ops.nebius_pool_migration import PoolMigrationError

    api, state = database_guard
    row = state.endpoints['items'][0]
    endpoint = row['endpoints'][0]
    if damage == 'wrong_pod':
        endpoint['targetRef']['uid'] = str(uuid4())
    elif damage == 'wrong_address':
        endpoint['addresses'] = ['10.20.0.3']
    elif damage == 'wrong_service':
        row['metadata']['ownerReferences'][0]['uid'] = str(uuid4())
    elif damage == 'wrong_port':
        row['ports'][0]['port'] = 5433
    elif damage == 'not_ready':
        endpoint['conditions']['ready'] = False
    elif damage == 'terminating':
        endpoint['conditions']['terminating'] = True
    elif damage == 'extra_backend':
        extra = copy.deepcopy(endpoint)
        extra['targetRef']['uid'] = str(uuid4())
        row['endpoints'].append(extra)
    elif damage == 'missing_backend':
        state.endpoints['items'] = []
    elif damage == 'pagination':
        state.endpoints['metadata']['continue'] = 'next'
    else:
        state.endpoint_after_drift = True
    with pytest.raises(PoolMigrationError):
        api.guard(state.target, 'observe')
    assert sum(args[0] == 'exec' for args in state.calls) == (1 if damage == 'after_drift' else 0)


@pytest.mark.parametrize('families', [('IPv4',), ('IPv6',), ('IPv4', 'IPv6')])
def test_database_backend_qualifies_every_service_address_family(database_guard, monkeypatch, families):
    from scripts.ops.nebius_pool_migration_guard import KubectlPoolGuardAPI

    original, state = database_guard
    addresses = {'IPv4': '10.20.0.2', 'IPv6': 'fd00::2'}
    state.service['spec']['ipFamilies'] = list(families)
    state.pod['status']['podIP'] = addresses[families[0]]
    state.pod['status']['podIPs'] = [{'ip': addresses[family]} for family in families]
    row = state.endpoints['items'][0]
    state.endpoints['items'] = []
    for family in families:
        endpoint = copy.deepcopy(row)
        endpoint['metadata'].update(uid=str(uuid4()), name='loom-postgres-' + family.lower())
        endpoint['addressType'] = family
        endpoint['endpoints'][0]['addresses'] = [addresses[family]]
        state.endpoints['items'].append(endpoint)
    target = replace(state.target, database=replace(state.target.database, service=copy.deepcopy(state.service)))
    request = replace(state.request, guards=(target, *state.request.guards[1:]))
    api = KubectlPoolGuardAPI(request=request, kubeconfig=original.kubeconfig, executable=Path('/usr/bin/kubectl'))
    monkeypatch.setattr(api, '_run', original._run)
    assert api.guard(target, 'observe') == {'status': 'held'}


@pytest.mark.parametrize('status', ['open', 'held', 'skipped_locked'])
def test_stopped_control_plane_does_not_prevent_bound_readonly_guard_observation(database_guard, status):
    api, state = database_guard
    state.status = status
    assert api.guard(state.target, 'observe') == {'status': status}
    command, = [args for args in state.calls if args[0] == 'exec']
    assert command[-1].startswith("BEGIN READ ONLY; SET LOCAL statement_timeout='10s';")
    assert command[-1].endswith('ROLLBACK;')
    assert str(state.request.registration.spec.operation_id) in command[-1]
    assert state.request.registration.candidate['candidate_sha'] in command[-1]


@pytest.mark.parametrize('damage', ['database_uid', 'database_template', 'database_status', 'selector', 'secret_uid',
    'secret_version', 'url_host', 'url_database', 'url_port', 'url_scheme', 'url_query', 'env_override', 'pod_owner', 'pod_image',
    'pod_security', 'pod_init', 'pod_readiness', 'pod_storage', 'extra_pod', 'pagination', 'after_drift', 'release'])
def test_database_guard_rejects_ambiguous_or_changed_identity(database_guard, damage):
    from scripts.ops.nebius_pool_migration import PoolMigrationError

    api, state = database_guard
    action = 'observe'
    if damage == 'database_uid':
        state.database['metadata']['uid'] = str(uuid4())
    elif damage == 'database_template':
        state.database['spec']['template']['spec']['containers'][0]['image'] = 'foreign:latest'
    elif damage == 'database_status':
        state.database['status']['readyReplicas'] = 0
    elif damage == 'selector':
        state.service['spec']['selector'] = {'app': 'another-database'}
    elif damage.startswith('secret_'):
        state.secret['metadata']['uid' if damage == 'secret_uid' else 'resourceVersion'] = str(uuid4())
    elif damage.startswith('url_'):
        raw = base64.b64decode(state.secret['data']['control-plane-url']).decode()
        old, new = {'url_host': ('loom-postgres.', 'foreign.'), 'url_database': ('/loom', '/foreign'),
            'url_port': (':5432/', ':5433/'), 'url_scheme': ('postgresql+psycopg:', 'https:'),
            'url_query': ('/loom', '/loom?host=foreign')}[damage]
        state.secret['data']['control-plane-url'] = base64.b64encode(raw.replace(old, new).encode()).decode()
    elif damage == 'env_override':
        state.target.controller['spec']['template']['spec']['containers'][0]['envFrom'] = [{'secretRef': {'name': 'foreign'}}]
    elif damage == 'pod_owner':
        state.pod['metadata']['ownerReferences'][0]['uid'] = str(uuid4())
    elif damage == 'pod_image':
        state.pod['spec']['containers'][0]['image'] = 'foreign:latest'
    elif damage == 'pod_security':
        state.pod['spec']['hostPID'] = True
    elif damage == 'pod_init':
        state.pod['spec']['initContainers'] = [{'name': 'foreign', 'image': 'foreign:latest'}]
    elif damage == 'pod_readiness':
        state.pod['status']['containerStatuses'][0]['ready'] = False
    elif damage == 'pod_storage':
        state.pod['spec']['volumes'][-1]['persistentVolumeClaim']['claimName'] = 'another-database'
    elif damage == 'extra_pod':
        state.second_pod = True
    elif damage == 'pagination':
        state.continuation = True
    elif damage == 'after_drift':
        state.after_drift = True
    else:
        action = 'release'
    with pytest.raises(PoolMigrationError) as error:
        api.guard(state.target, action)
    assert 'private-marker' not in str(error.value)
    assert sum(args[0] == 'exec' for args in state.calls) == (1 if damage == 'after_drift' else 0)


def test_acquire_checks_database_identity_before_writing(database_guard):
    from scripts.ops.nebius_pool_migration import PoolMigrationError

    api, state = database_guard
    state.secret['metadata']['uid'] = str(uuid4())
    with pytest.raises(PoolMigrationError):
        api.guard(state.target, 'acquire')
    assert any(args[:2] == ['get', 'secret'] for args in state.calls)
    assert not any(args[0] == 'exec' for args in state.calls)


@pytest.mark.parametrize('action,status', [('stage', 'staged'), ('observe', 'qualified'), ('inspect', 'qualified')])
def test_runtime_role_stage_binds_original_database_without_the_retired_controller(database_guard, action, status):
    api, state = database_guard
    state.status = status
    assert api.runtime_role(state.target, action) == {'status': status}
    command, = [args for args in state.calls if args[0] == 'exec']
    assert command[:7] == ['exec', '-n', state.target.namespace, 'pod/loom-postgres-0', '-c', 'loom-postgres', '--']


@pytest.mark.parametrize('damage', ['database_uid', 'after_drift', 'wrong_report', 'release', 'extra_report', 'config'])
def test_runtime_role_stage_denies_drift_unknown_receipts_and_arbitrary_actions(database_guard, damage):
    from scripts.ops.nebius_pool_migration import PoolMigrationError

    api, state = database_guard
    state.status, action = 'staged', 'stage'
    if damage == 'database_uid':
        state.database['metadata']['uid'] = str(uuid4())
    elif damage == 'after_drift':
        state.after_drift = True
    elif damage == 'wrong_report':
        state.status = 'released'
    elif damage == 'release':
        action = 'release'
    elif damage == 'extra_report':
        state.calls.clear()
        state.exec_hook = lambda query: {'status': 'staged', 'unqualified': 'private-marker'}
    else:
        api.kubeconfig.write_text('changed-private-config')
    with pytest.raises(PoolMigrationError):
        api.runtime_role(state.target, action)
    assert sum(args[0] == 'exec' for args in state.calls) == (0 if damage in {'database_uid', 'release', 'config'} else 1)


@pytest.mark.parametrize('damage', ['database_uid', 'after_drift', 'extra_report', 'config', 'cursor', 'origin', 'environment', 'source'])
def test_cutover_database_pages_bind_identity_and_reject_unsafe_receipts(database_guard, damage):
    from scripts.ops.nebius_pool_migration import PoolMigrationError

    api, state = database_guard
    participant = next(row for row in state.request.registration.spec.participants
        if row.participant_id == state.target.participant_id)
    identity = str(uuid4())
    report = {'status': 'observed', 'schema_revision': '0174', 'rows': [{
        'key': 'batch:' + identity, 'source_matches': True, 'origin': {
            'schema_version': 'loom.pool-work-origin.v1', 'data_environment_id': str(participant.environment_id),
            'submission_id': identity, 'kind': 'environment', 'application': None}}]}
    after = None
    if damage == 'database_uid':
        state.database['metadata']['uid'] = str(uuid4())
    elif damage == 'after_drift':
        state.after_drift = True
    elif damage == 'extra_report':
        report['private-marker'] = 'unqualified'
    elif damage == 'config':
        api.kubeconfig.write_text('changed-private-config')
    elif damage == 'cursor':
        after = "batch:';DELETE FROM trials;--"
    elif damage == 'origin':
        report['rows'][0]['origin'] = None
    elif damage == 'environment':
        report['rows'][0]['origin']['data_environment_id'] = str(uuid4())
    else:
        report['rows'][0]['source_matches'] = False
    state.exec_hook = lambda query: report
    with pytest.raises(PoolMigrationError) as error:
        api.cutover_readiness_page(state.target, after=after)
    assert 'private-marker' not in str(error.value)
    assert sum(args[0] == 'exec' for args in state.calls) == (0 if damage in {'database_uid', 'config', 'cursor'} else 1)


def test_database_observer_never_exposes_unqualified_query_output(database_guard):
    from scripts.ops.nebius_pool_migration import PoolMigrationError

    api, state = database_guard
    state.exec_hook = lambda sql: {'status': 'held', 'private-marker': 'unexpected-data'}
    with pytest.raises(PoolMigrationError) as error:
        api.guard(state.target, 'observe')
    assert 'private-marker' not in str(error.value)


@pytest.mark.parametrize('source', [
    {'value': 'postgresql+psycopg://user:private-marker@other-database/loom'},
    {'valueFrom': {'secretKeyRef': {'name': 'another-database', 'key': 'pool-url'}}},
])
def test_direct_database_binding_cannot_ignore_pooled_engine_override(database_guard, monkeypatch, source):
    from scripts.ops.nebius_pool_migration import PoolMigrationError
    from scripts.ops.nebius_pool_migration_guard import KubectlPoolGuardAPI

    api, state = database_guard
    state.target.controller['spec']['template']['spec']['containers'][0]['env'].append(
        {'name': 'LOOM_CP_DB_URL_POOL', **source})
    # Construct a new binding to this original template, not a post-bind drift.
    replacement = KubectlPoolGuardAPI(request=state.request, kubeconfig=api.kubeconfig, executable=Path('/usr/bin/kubectl'))
    monkeypatch.setattr(replacement, '_run', api._run)
    with pytest.raises(PoolMigrationError):
        replacement.guard(state.target, 'observe')
    assert not any(args[0] == 'exec' for args in state.calls)


def test_bound_acquisition_uses_live_controller_then_observation_survives_retirement(database_guard, monkeypatch):
    api, state = database_guard
    controller = copy.deepcopy(state.target.controller)
    controller['status'] = {'observedGeneration': 1, 'replicas': 1, 'readyReplicas': 1,
        'updatedReplicas': 1, 'availableReplicas': 1}
    replica = {'apiVersion': 'apps/v1', 'kind': 'ReplicaSet', 'metadata': {
        'name': 'loom-control-plane-abc', 'namespace': state.target.namespace, 'uid': str(uuid4()),
        'ownerReferences': [{'apiVersion': 'apps/v1', 'kind': 'Deployment', 'name': 'loom-control-plane',
            'uid': controller['metadata']['uid'], 'controller': True}]}, 'spec': {
                'template': copy.deepcopy(controller['spec']['template'])}}
    pod = {'apiVersion': 'v1', 'kind': 'Pod', 'metadata': {
        'name': 'loom-control-plane-abc-def', 'namespace': state.target.namespace, 'uid': str(uuid4()),
        'labels': {'app': 'loom-control-plane'}, 'ownerReferences': [{'apiVersion': 'apps/v1', 'kind': 'ReplicaSet',
            'name': replica['metadata']['name'], 'uid': replica['metadata']['uid'], 'controller': True}]},
        'spec': copy.deepcopy(controller['spec']['template']['spec']), 'status': {
            'phase': 'Running', 'containerStatuses': [{'name': 'loom-control-plane', 'ready': True}]}}
    database_run = api._run
    writes = []
    operations = []

    def run(args):
        operations.append(args)
        if args[:2] == ['get', 'deployment']:
            return controller
        if args[:2] == ['get', 'replicaset']:
            return replica
        if args == ['get', '--raw', f'/api/v1/namespaces/{state.target.namespace}/pods?labelSelector=app%3Dloom-control-plane&limit=100']:
            return {'apiVersion': 'v1', 'kind': 'PodList', 'metadata': {'resourceVersion': '1'}, 'items': [pod]}
        if args[:1] == ['exec'] and args[5] == 'loom-control-plane':
            assert args[3] == 'pod/loom-control-plane-abc-def'
            assert args[7:9] == ['python', '-c']
            assert args[10:13] == ['acquire', str(state.request.registration.spec.operation_id),
                state.request.registration.candidate['candidate_sha']]
            assert len(args) == 15 and len(bytes.fromhex(args[13])) == 32
            assert len(bytes.fromhex(args[14])) == 32
            assert not any('private-marker' in argument for argument in args)
            writes.append(args)
            return {'status': 'acquired', 'active': {'trials': 0}}
        return database_run(args)

    monkeypatch.setattr(api, '_run', run)
    assert api.guard(state.target, 'acquire') == {'status': 'acquired'}
    assert len(writes) == 1
    credential_reads = [index for index, args in enumerate(operations) if args[:2] == ['get', 'secret']]
    write, = [index for index, args in enumerate(operations) if args[:1] == ['exec']]
    assert min(credential_reads) < write < max(credential_reads)
    monkeypatch.setattr(api, '_run', database_run)
    assert api.guard(state.target, 'observe') == {'status': 'held'}


@pytest.fixture(params=['controller', 'service', 'actuator', 'guest'])
def workload_database(request, database_guard, guest_runtime_inputs, monkeypatch):
    """Real rendered Pods/settings; only the remote Kubernetes transport is doubled."""
    import json
    import os
    import subprocess
    import sys

    from scripts.ops.nebius_pool_migration_guard import KubectlPoolGuardAPI

    previous, state = database_guard
    migration, actuators, services, _, guest = guest_runtime_inputs
    target = state.target
    migration = replace(migration, guards=(target, *migration.guards[1:]))
    original = {'controller': target.controller, 'service': services[target.participant_id],
        'actuator': actuators[target.participant_id], 'guest': guest}[request.param]
    controller = copy.deepcopy(original)
    controller['status'] = {'observedGeneration': 1, 'replicas': 1, 'readyReplicas': 1,
        'updatedReplicas': 1, 'availableReplicas': 1}
    namespace, name = original['metadata']['namespace'], original['metadata']['name']
    container, = original['spec']['template']['spec']['containers']
    variable = {'controller': 'LOOM_CP_DB_URL', 'service': 'LOOM_SVC_DB_URL',
        'actuator': 'LOOM_EXECUTION_ACTUATOR_DB_URL', 'guest': 'LOOM_EXECUTION_ACTUATOR_DB_URL'}[request.param]
    reference, = (row['valueFrom']['secretKeyRef'] for row in container['env'] if row['name'] == variable)
    url = f'postgresql+psycopg://fixture:private-runtime-marker@loom-postgres.{target.namespace}.svc:5432/loom'
    if namespace == target.namespace:
        secret = state.secret
    else:
        secret = {'apiVersion': 'v1', 'kind': 'Secret', 'metadata': {'name': reference['name'],
            'namespace': namespace, 'uid': str(uuid4()), 'resourceVersion': '11'}, 'data': {}}
    secret['data'][reference['key']] = base64.b64encode(url.encode()).decode()
    replica = {'apiVersion': 'apps/v1', 'kind': 'ReplicaSet', 'metadata': {
        'name': name + '-abc', 'namespace': namespace, 'uid': str(uuid4()),
        'ownerReferences': [{'apiVersion': 'apps/v1', 'kind': 'Deployment', 'name': name,
            'uid': controller['metadata']['uid'], 'controller': True}]},
        'spec': {'template': copy.deepcopy(controller['spec']['template'])}}
    pod = {'apiVersion': 'v1', 'kind': 'Pod', 'metadata': {
        'name': name + '-abc-def', 'namespace': namespace, 'uid': str(uuid4()),
        'labels': copy.deepcopy(controller['spec']['selector']['matchLabels']),
        'ownerReferences': [{'apiVersion': 'apps/v1', 'kind': 'ReplicaSet', 'name': replica['metadata']['name'],
            'uid': replica['metadata']['uid'], 'controller': True}]},
        'spec': copy.deepcopy(controller['spec']['template']['spec']),
        'status': {'phase': 'Running', 'containerStatuses': [{'name': container['name'], 'ready': True}]}}
    environment = {key: value for key, value in os.environ.items() if not key.startswith(('LOOM_', 'DATABASE_'))}
    environment.update({variable: url, 'LOOM_CP_MINIO_ACCESS_KEY': 'fixture', 'LOOM_CP_MINIO_SECRET_KEY': 'fixture',
        'LOOM_CP_STEP_JWT_SIGNING_KEY': 'runtime-database-test-key-00000000',
        'LOOM_SVC_MINIO_ACCESS_KEY': 'fixture', 'LOOM_SVC_MINIO_SECRET_KEY': 'fixture',
        'LOOM_EXECUTION_ACTUATOR_NAMESPACE': namespace, 'LOOM_EXECUTION_ACTUATOR_CONTROLLER_ID': pod['metadata']['name'],
        'LOOM_EXECUTION_ACTUATOR_TARGET_ID': 'fixture-target'})
    api = KubectlPoolGuardAPI(request=migration, kubeconfig=previous.kubeconfig, executable=Path('/usr/bin/kubectl'))
    state.original, state.controller, state.runtime_pod, state.replica = original, controller, pod, replica
    state.runtime_secret, state.runtime_environment, state.variable = secret, environment, variable
    state.credential = (UUID(secret['metadata']['uid']), secret['metadata']['resourceVersion'])
    state.commands, state.processes, state.runtime_after_drift = [], [], False
    database_run = previous._run

    def run(args):
        if args[:1] == ['exec']:
            assert args[:7] == ['exec', '-n', namespace, 'pod/' + pod['metadata']['name'], '-c', container['name'], '--']
            assert args[7:9] == ['python', '-c']
            state.commands.append(args)
            result = subprocess.run([sys.executable, *args[8:]], capture_output=True, check=False,
                timeout=30, cwd=previous.kubeconfig.parent, env=environment)
            state.processes.append(result)
            if result.returncode:
                raise ValueError('private-transport-marker')
            return json.loads(result.stdout)
        if args[:2] == ['get', 'namespace'] and args[2] == namespace and namespace != target.namespace:
            participant = next(row for row in migration.registration.spec.participants if row.participant_id == target.participant_id)
            return {'apiVersion': 'v1', 'kind': 'Namespace', 'metadata': {'name': namespace,
                'uid': str(participant.execution_namespace.uid)}}
        if args[:2] == ['get', 'secret'] and args[2] == reference['name'] and args[4] == namespace:
            return copy.deepcopy(secret)
        if args[:2] == ['get', 'deployment']:
            assert args[2:5] == [name, '-n', namespace]
            return copy.deepcopy(controller)
        if args[:2] == ['get', 'replicaset']:
            assert args[2:5] == [replica['metadata']['name'], '-n', namespace]
            return copy.deepcopy(replica)
        if args[:2] == ['get', '--raw'] and '/pods?' in args[2] and 'loom-postgres' not in args[2]:
            current = copy.deepcopy(pod)
            if state.commands and state.runtime_after_drift:
                current['metadata']['uid'] = str(uuid4())
            return {'apiVersion': 'v1', 'kind': 'PodList', 'metadata': {'resourceVersion': '1'}, 'items': [current]}
        return database_run(args)

    monkeypatch.setattr(api, '_run', run)
    return api, state


def qualify_workload(api, state):
    return api.qualify_runtime_database(state.target, original=state.original,
        credential_uid=state.credential[0], credential_resource_version=state.credential[1])


def running_successor(state):
    """Keep original authority immutable while installing a replacement fixture."""
    expected = copy.deepcopy(state.original)
    expected['metadata'].setdefault('annotations', {})['loom.nebius/pool-cutover'] = 'fixture'
    expected['spec']['template']['spec']['containers'][0]['image'] = 'registry.example.com/loom@sha256:' + 'b' * 64
    state.controller.update(copy.deepcopy(expected))
    state.replica['spec']['template'] = copy.deepcopy(expected['spec']['template'])
    state.runtime_pod['spec'] = copy.deepcopy(expected['spec']['template']['spec'])
    return expected


def test_successor_database_probe_uses_expected_template_but_retains_original_authority(workload_database):
    from scripts.ops.nebius_pool_migration import PoolMigrationError

    api, state = workload_database
    expected = running_successor(state)
    # Journal quantity normalization must not reject the same live resource.
    for spec in (state.controller['spec']['template']['spec'], state.replica['spec']['template']['spec'], state.runtime_pod['spec']):
        spec['containers'][0]['resources'] = {'requests': {'cpu': '500m', 'memory': '512Mi'}}
    expected['spec']['template']['spec']['containers'][0]['resources'] = {'requests': {'cpu': '0.5', 'memory': '536870912'}}
    with pytest.raises(PoolMigrationError):
        qualify_workload(api, state)  # Original-only preflight must not adopt it.
    assert not state.commands
    api.qualify_runtime_database(state.target, original=state.original, expected=expected,
        credential_uid=state.credential[0], credential_resource_version=state.credential[1])
    assert len(state.processes) == 1 and state.processes[0].stdout == b'{"status": "qualified"}\n'
    assert all('private-' not in arg for command in state.commands for arg in command)


@pytest.mark.parametrize('workload_database', ['service', 'actuator'], indirect=True)
@pytest.mark.parametrize('damage', ['uid', 'namespace', 'selector', 'container', 'service_account',
    'credential_key', 'pooled_url', 'env_from', 'not_ready', 'late_pod', 'effective_url'])
def test_successor_database_probe_cannot_widen_retained_identity_or_credentials(workload_database, damage):
    from scripts.ops.nebius_pool_migration import PoolMigrationError

    api, state = workload_database
    expected = running_successor(state)
    spec = expected['spec']['template']['spec']
    if damage == 'uid':
        expected['metadata']['uid'] = str(uuid4())
    elif damage == 'namespace':
        expected['metadata']['namespace'] = 'foreign'
    elif damage == 'selector':
        expected['spec']['selector'] = {'matchLabels': {'app': 'foreign'}}
    elif damage == 'container':
        spec['containers'][0]['name'] = 'foreign'
    elif damage == 'service_account':
        spec['serviceAccountName'] = 'foreign-admin'
    elif damage == 'credential_key':
        entry, = (row for row in spec['containers'][0]['env'] if row['name'] == state.variable)
        reference = entry['valueFrom']['secretKeyRef']
        state.runtime_secret['data']['foreign'] = state.runtime_secret['data'][reference['key']]
        reference['key'] = 'foreign'
    elif damage == 'pooled_url':
        spec['containers'][0]['env'].append({'name': state.variable + '_POOL', 'value': 'private-override'})
    elif damage == 'env_from':
        spec['containers'][0]['envFrom'] = [{'secretRef': {'name': 'foreign'}}]
    elif damage == 'not_ready':
        state.runtime_pod['status']['containerStatuses'][0]['ready'] = False
    elif damage == 'late_pod':
        state.runtime_after_drift = True
    elif damage == 'effective_url':
        state.runtime_environment[state.variable] += '?application_name=private-override'
    # Even an exactly matching live replacement cannot change original scope.
    state.controller.update(copy.deepcopy(expected))
    state.replica['spec']['template'] = copy.deepcopy(expected['spec']['template'])
    state.runtime_pod['spec'] = copy.deepcopy(spec)
    with pytest.raises(PoolMigrationError) as error:
        api.qualify_runtime_database(state.target, original=state.original, expected=expected,
            credential_uid=state.credential[0], credential_resource_version=state.credential[1])
    assert error.value.stage == 'runtime_database'
    if damage not in {'late_pod', 'effective_url'}:
        assert not state.commands
    assert all(b'private-' not in result.stdout + result.stderr for result in state.processes)


@pytest.mark.parametrize('damage', [None, 'settings', 'scope', 'late_pod', 'not_ready', 'private_inputs'])
@pytest.mark.parametrize('builder_position', [None, 'first', 'last'])
@pytest.mark.parametrize('runtime_inputs', [('development', 'production', 'staging')], indirect=True)
def test_successor_pool_settings_use_retained_pod_and_registered_material(
        workload_database, guest_runtime_inputs, build_inputs, tmp_path, monkeypatch, damage, builder_position):
    import hashlib
    import json

    from scripts.ops.nebius_pool_migration import PoolMigrationError
    from scripts.ops.nebius_pool_runtime import wire_participant
    from tests.integration.test_nebius_pool_installation import add_application_builder
    from tests.ops.test_nebius_pool_runtime import desired_profile, env

    from loom_service.pool_management.installation import PoolInstallation

    previous, state = workload_database
    _, actuators, services, _, guest = guest_runtime_inputs
    spec = previous.request.registration.spec.model_dump(mode='json')
    token = tmp_path / 'pool-token'
    token.write_text('private-runtime-machine-marker')
    token.chmod(0o600)
    for machine in spec['machines']:
        if machine['participant_id'] == str(state.target.participant_id):
            machine['token_sha256'] = hashlib.sha256(token.read_bytes()).hexdigest()
    if builder_position is not None:
        spec, builder_id, _ = add_application_builder(spec, build_inputs[0].recipe)
        builder, = (row for row in spec['machines'] if row['machine_id'] == str(builder_id))
        assert builder['participant_id'] == str(state.target.participant_id)
        assert builder['token_sha256'] != hashlib.sha256(token.read_bytes()).hexdigest()
        if builder_position == 'first':
            spec['machines'].remove(builder)
            spec['machines'].insert(0, builder)
    migration = replace(previous.request, registration=replace(previous.request.registration, spec=PoolInstallation.model_validate(spec)))
    api = type(previous)(request=migration, kubeconfig=previous.kubeconfig, executable=Path('/usr/bin/kubectl'))
    monkeypatch.setattr(api, '_run', previous._run)
    identity = state.target.participant_id
    targets = wire_participant(request=migration, participant_id=identity, management_origin='https://manage.example.com',
        actuator=actuators[identity], service=services[identity], guest_actuators=(guest,),
        runtime_profile=desired_profile(migration, services[identity]))
    expected, = (value for value in targets.values() if value['metadata']['name'] == state.original['metadata']['name'])
    expected['spec']['replicas'] = 1
    rows = env(expected)
    pool_variable = next((name for name in rows if name.endswith(('_GLOBAL_POOL', '_GLOBAL_POOL_JSON'))), None)
    if pool_variable:
        pool = json.loads(rows[pool_variable]['value'])
        pool['bearer_token_file'] = str(token)
        rows[pool_variable]['value'] = json.dumps(pool)
    state.controller.update(copy.deepcopy(expected))
    state.replica['spec']['template'] = copy.deepcopy(expected['spec']['template'])
    state.runtime_pod['spec'] = copy.deepcopy(expected['spec']['template']['spec'])
    state.runtime_environment.update({name: row['value'] for name, row in rows.items() if 'value' in row})
    if damage == 'settings':
        if pool_variable:
            token.write_text('private-foreign-machine-marker')
        else:
            state.runtime_environment.pop('LOOM_SVC_POOL_SUBMISSION_SOURCE_JSON')
    elif damage == 'scope':
        expected['metadata']['uid'] = str(uuid4())
    elif damage == 'late_pod':
        state.runtime_after_drift = True
    elif damage == 'not_ready':
        state.runtime_pod['status']['containerStatuses'][0]['ready'] = False
    elif damage == 'private_inputs':
        api.kubeconfig.write_text('private-changed-authority-marker')
    if damage:
        with pytest.raises(PoolMigrationError) as error:
            api.qualify_runtime_pool_settings(state.target, original=state.original, expected=expected)
        assert error.value.stage == 'runtime_pool_settings'
        if damage in {'scope', 'not_ready', 'private_inputs'}:
            assert not state.commands
    else:
        assert api.qualify_runtime_pool_settings(state.target, original=state.original, expected=expected) is None
        assert len(state.commands) == 1 and state.processes[0].stdout == b'{"status": "qualified"}\n'
    assert all('private-' not in arg for command in state.commands for arg in command)
    assert all(b'private-' not in process.stdout + process.stderr for process in state.processes)


@pytest.mark.parametrize('damage', [None, 'settings', 'global', 'scope', 'template', 'late_pod', 'not_ready', 'private_inputs'])
def test_legacy_settings_require_original_template_and_same_ready_pod(workload_database, monkeypatch, damage):
    from scripts.ops.nebius_pool_migration import PoolMigrationError

    previous, state = workload_database
    original = state.original
    container, = original['spec']['template']['spec']['containers']
    rows = {row['name']: row for row in container['env']}
    if 'LOOM_CP_EXECUTION_IMAGE_ADMISSION_PUBLIC_KEYS_JSON' in rows:
        rows['LOOM_CP_EXECUTION_IMAGE_ADMISSION_PUBLIC_KEYS_JSON']['value'] = '{"schema_version":1,"keys":[]}'
    api = type(previous)(request=previous.request, kubeconfig=previous.kubeconfig, executable=Path('/usr/bin/kubectl'))
    monkeypatch.setattr(api, '_run', previous._run)
    expected = copy.deepcopy(original)
    expected['metadata'].setdefault('annotations', {})['loom.nebius/pool-cutover'] = 'fixture'
    state.runtime_environment.update({name: row['value'] for name, row in rows.items() if 'value' in row})
    if damage == 'scope':
        expected['metadata']['uid'] = str(uuid4())
    elif damage == 'template':
        expected['spec']['template']['spec']['containers'][0]['image'] = 'registry.example.com/changed@sha256:' + 'e' * 64
    elif damage == 'late_pod':
        state.runtime_after_drift = True
    elif damage == 'not_ready':
        state.runtime_pod['status']['containerStatuses'][0]['ready'] = False
    elif damage == 'private_inputs':
        api.kubeconfig.write_text('private-changed-authority-marker')
    elif damage in {'settings', 'global'}:
        if container['name'] == 'loom-control-plane':
            name, value = ('LOOM_CP_SERVICE_EXECUTION_SCHEDULER_ENABLED', 'false') if damage == 'settings' else (
                'LOOM_CP_SERVICE_EXECUTION_GLOBAL_POOL_JSON', '{}')
        elif container['name'] == 'loom-service':
            name, value = ('LOOM_SVC_SERVICE_MODE', 'api_only') if damage == 'settings' else ('LOOM_SVC_POOL_PROFILES_FILE', '/private-global.json')
        else:
            name, value = ('LOOM_EXECUTION_ACTUATOR_TARGET_ID', 'foreign-target') if damage == 'settings' else ('LOOM_EXECUTION_ACTUATOR_GLOBAL_POOL', '{}')
        state.runtime_environment[name] = value
    state.controller.update(copy.deepcopy(expected))
    state.replica['spec']['template'] = copy.deepcopy(expected['spec']['template'])
    state.runtime_pod['spec'] = copy.deepcopy(expected['spec']['template']['spec'])
    if damage:
        with pytest.raises(PoolMigrationError) as error:
            api.qualify_runtime_legacy_settings(state.target, original=original, expected=expected)
        assert error.value.stage == 'runtime_legacy_settings'
        if damage in {'scope', 'template', 'not_ready', 'private_inputs'}:
            assert not state.commands
    else:
        assert api.qualify_runtime_legacy_settings(state.target, original=original, expected=expected) is None
        assert len(state.processes) == 1 and state.processes[0].stdout == b'{"status": "qualified"}\n'
    assert all('private-' not in arg for command in state.commands for arg in command)
    assert all(b'private-' not in process.stdout + process.stderr for process in state.processes)


def test_each_running_database_consumer_is_qualified_without_sql_or_credential_output(workload_database):
    api, state = workload_database
    assert qualify_workload(api, state) is None
    assert qualify_workload(api, state) is None
    assert len(state.commands) == 2 and state.commands[0][-2:] != state.commands[1][-2:]
    assert all('private-runtime-marker' not in arg for command in state.commands for arg in command)
    assert all(result.returncode == 0 and result.stdout == b'{"status": "qualified"}\n' and not result.stderr
        for result in state.processes)


@pytest.mark.parametrize('damage', [None, 'scale_zero', 'missing_host', 'partial', 'duplicate',
    'deleted', 'late_node', 'late_pod', 'denied', 'wrong_report', 'unknown_target', 'service_account',
    'probe_tls', 'probe_tls_detail', 'probe_tls_invalid', 'probe_extra', 'probe_unknown',
    'optional_late_node', 'optional_late_pod', 'probe_kubelet_network', 'probe_kubelet_http',
    'probe_kubelet_authorization', 'probe_counters', 'probe_api_tls'])
@pytest.mark.parametrize('successor', [False, True])
def test_runtime_telemetry_uses_retained_actuator_and_every_pool_node(workload_database, monkeypatch, damage, successor):
    from scripts.ops.nebius_pool_migration import PoolMigrationError

    api, state = workload_database
    selector = api.request.registration.spec.node_selector
    state.runtime_pod['spec']['nodeName'] = 'platform-node'
    def node(name, labels):
        return {'apiVersion': 'v1', 'kind': 'Node', 'metadata': {'name': name,
            'uid': str(uuid4()), 'resourceVersion': '1', 'labels': labels},
            'status': {'addresses': [{'type': 'InternalIP', 'address': '10.20.0.2'}]}}
    nodes = [node('platform-node', {}), node('pool-node', selector), node('foreign-node', {'foreign': 'true'})]
    if damage == 'scale_zero':
        nodes.pop(1)
    elif damage == 'missing_host':
        nodes.pop(0)
    elif damage == 'duplicate':
        nodes.append(copy.deepcopy(nodes[1]))
    elif damage == 'deleted':
        nodes[1]['metadata']['deletionTimestamp'] = '2026-10-01T00:00:00Z'
    elif damage in {'late_pod', 'optional_late_pod'}:
        state.runtime_after_drift = True
    elif damage == 'unknown_target':
        for row in state.original['spec']['template']['spec']['containers'][0].get('env', []):
            if row['name'] == 'LOOM_EXECUTION_ACTUATOR_TARGET_ID':
                row['value'] = 'foreign'
    elif damage == 'service_account':
        for document in (state.original, state.controller, state.replica):
            document['spec']['template']['spec']['serviceAccountName'] = 'foreign-admin'
        state.runtime_pod['spec']['serviceAccountName'] = 'foreign-admin'
    expected = running_successor(state) if successor else None
    state.runtime_pod['spec']['nodeName'] = 'platform-node'
    options = {'expected': expected} if successor else {}
    previous, commands = api._run, []

    def run(args):
        if args[:2] == ['get', '--raw'] and args[2].startswith('/api/v1/nodes?'):
            items = copy.deepcopy(nodes)
            if damage in {'late_node', 'optional_late_node'} and commands:
                items[1]['metadata']['uid'] = str(uuid4())
            return {'apiVersion': 'v1', 'kind': 'NodeList',
                'metadata': {'resourceVersion': '9', **({'continue': 'same'} if damage == 'partial' else {})}, 'items': items}
        if args[:1] == ['exec']:
            assert args[:7] == ['exec', '-n', state.original['metadata']['namespace'],
                'pod/' + state.runtime_pod['metadata']['name'], '-c', 'actuator', '--']
            assert args[7:9] == ['python', '-c']
            assert len(args) == 14
            assert args[10] == state.original['metadata']['namespace']
            assert args[11] == next(row['value'] for row in state.original['spec']['template']['spec']['containers'][0]['env']
                if row['name'] == 'LOOM_EXECUTION_ACTUATOR_TARGET_ID')
            commands.append(args)
            state.commands.append(args)
            if damage == 'denied':
                raise ValueError('private-telemetry-marker')
            if damage in {'probe_tls_detail', 'probe_tls_invalid', 'optional_late_node', 'optional_late_pod'}:
                return {'status': 'blocked', 'stage': 'tls_kubelet_verify_' + ('256' if damage == 'probe_tls_invalid' else '20')}
            if damage in {'probe_kubelet_network', 'probe_kubelet_http', 'probe_kubelet_authorization', 'probe_counters'}:
                return {'status': 'blocked', 'stage': damage.removeprefix('probe_')}
            if damage == 'probe_api_tls':
                return {'status': 'blocked', 'stage': 'tls_api_verify_19'}
            if damage in {'probe_tls', 'probe_extra', 'probe_unknown'}:
                return {'status': 'blocked', 'stage': 'private-telemetry-marker' if damage == 'probe_unknown' else 'tls',
                    **({'private-token': 'never expose'} if damage == 'probe_extra' else {})}
            return {'status': 'qualified', 'node_name': args[12],
                'node_uid': str(uuid4()) if damage == 'wrong_report' else args[13]}
        return previous(args)

    monkeypatch.setattr(api, '_run', run)
    actuator = state.original['spec']['template']['spec']['containers'][0]['name'] == 'actuator'
    optional = {'probe_tls_detail', 'probe_kubelet_network', 'probe_kubelet_http',
        'probe_kubelet_authorization', 'probe_counters'}
    if not actuator or damage not in {None, 'scale_zero', *optional}:
        with pytest.raises(PoolMigrationError) as error:
            api.qualify_runtime_telemetry(state.target, original=state.original, **options)
        phase = ('binding' if not actuator or damage in {'unknown_target', 'service_account'} else
            'nodes' if damage in {'missing_host', 'partial', 'duplicate', 'deleted'} else
            'recheck' if damage in {'late_node', 'late_pod', 'optional_late_node', 'optional_late_pod'} else
            'tls_api_verify_19' if damage == 'probe_api_tls' else
            'tls' if damage == 'probe_tls' else 'tls_kubelet_verify_20' if damage == 'probe_tls_detail' else 'probe')
        assert error.value.stage == 'runtime_telemetry_' + phase
        assert 'private-' not in str(error.value)
        assert api.telemetry_report() == {'status': 'not_observed', 'checks': 0, 'unavailable': 0, 'reasons': []}
    else:
        api.qualify_runtime_telemetry(state.target, original=state.original, **options)
        assert [command[12] for command in commands] == (['platform-node'] if damage == 'scale_zero' else ['platform-node', 'pool-node'])
        for command in commands:
            assert command[13] == next(row['metadata']['uid'] for row in nodes if row['metadata']['name'] == command[12])
        assert api.telemetry_report() == {'status': 'unavailable' if damage in optional else 'available',
            'checks': len(commands), 'unavailable': len(commands) if damage in optional else 0,
            'reasons': ['tls_kubelet_verify_20' if damage == 'probe_tls_detail' else damage.removeprefix('probe_')]
                if damage in optional else []}
        # A later observation replaces this actuator's previous samples; neither
        # a previous success nor a previous warning is permanently sticky.
        damage = None if damage in optional else 'probe_counters'
        commands.clear()
        api.qualify_runtime_telemetry(state.target, original=state.original, **options)
        assert api.telemetry_report() == {'status': 'available' if damage is None else 'unavailable',
            'checks': len(commands), 'unavailable': 0 if damage is None else len(commands),
            'reasons': [] if damage is None else ['counters']}
    if not actuator or damage in {'missing_host', 'partial', 'duplicate', 'deleted', 'unknown_target', 'service_account'}:
        assert not commands


@pytest.mark.parametrize(('damage', 'stage'), [
    (None, None), ('namespace', 'settings'), ('target', 'settings'), ('remote', 'settings'),
    ('node_uid', 'identity'), ('denied', 'kubelet_authorization'), ('node_api_denied', 'authorization'),
    ('missing_counter', 'counters'), ('boolean_counter', 'counters'), ('negative_counter', 'counters'),
    ('old_image', 'reader'), ('tls', 'tls_kubelet'), ('timeout', 'kubelet_network'), ('connect', 'kubelet_network'),
    ('http_failure', 'kubelet_http'), ('payload', 'payload'), ('close', 'close'),
    ('node_address', 'address'), ('bearer', 'authority'), ('summary_identity', 'payload'),
    ('client', 'client'), ('tls_close', 'close'),
    ('missing_ca', 'authority'), ('unreadable_ca', 'authority'),
    ('node_api_tls', 'tls_api'), ('node_api_tls_verify', 'tls_api_verify_20'),
    ('tls_unknown', 'tls_unknown_verify_10'), ('tls_trust', 'tls_kubelet_verify_20'),
    ('tls_name', 'tls_kubelet_verify_64'), ('tls_expired', 'tls_kubelet_verify_10'),
    ('tls_zero', 'tls_kubelet_verify_0'), ('tls_max', 'tls_kubelet_verify_255'),
    ('tls_large', 'tls_kubelet'), ('tls_negative', 'tls_kubelet'),
    ('tls_bool', 'tls_kubelet'), ('tls_string', 'tls_kubelet'),
    ('tls_cycle', 'reader'), ('tls_deep', 'reader'),
    ('mixed_transport', 'reader'),
    ('http_close', 'reader'), ('http_protocol', 'reader'),
    ('tls_unknown_plain', 'tls'),
])
def test_fixed_telemetry_probe_runs_real_settings_and_direct_reader_without_credentials_in_output(monkeypatch, capsys, tmp_path, damage, stage):
    import json
    import os
    import ssl
    import sys

    import httpx
    from kubernetes import client, config
    from scripts.ops.nebius_pool_migration_guard import _BOUND_TELEMETRY_COMMAND
    from urllib3.response import HTTPResponse

    from loom_execution_actuator.kubernetes_api import InClusterKubernetesJobApi

    uid = 'aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaaaa'
    settings = {'DB_URL': 'private-no-sql-marker', 'CONTROLLER_ID': 'observer-1',
        'NAMESPACE': 'telemetry', 'TARGET_ID': 'nebius-dev'}
    if damage in {'namespace', 'target'}:
        settings['NAMESPACE' if damage == 'namespace' else 'TARGET_ID'] = 'foreign'
    if damage == 'remote':
        settings.update(KUBERNETES_ENDPOINT='https://remote.example.com', KUBERNETES_CA_FILE='/remote/ca',
            KUBERNETES_NEBIUS_CREDENTIALS_FILE='/remote/key')
    for key in os.environ:
        if key.startswith('LOOM_EXECUTION_ACTUATOR_'):
            monkeypatch.delenv(key)
    for key, value in settings.items():
        monkeypatch.setenv('LOOM_EXECUTION_ACTUATOR_' + key, value)
    monkeypatch.setattr(sys, 'argv', ['-c', 'telemetry', 'nebius-dev', 'node-1', uid])
    configuration = client.Configuration()
    configuration.host = 'https://kubernetes.default.svc'
    configuration.ssl_ca_cert = '/mounted/ca.crt'
    configuration.api_key['authorization'] = 'bearer private-runtime-token'
    if damage == 'bearer':
        configuration.api_key.clear()
    monkeypatch.setattr(client.Configuration, '_default', None)
    def load_config():
        if damage == 'client':
            raise RuntimeError('private-runtime-token')
        client.Configuration.set_default(configuration)
    monkeypatch.setattr(config, 'load_incluster_config', load_config)
    requests, closed = [], []
    tls_codes = {'tls_trust': 20, 'tls_name': 64, 'tls_expired': 10,
        'tls_zero': 0, 'tls_max': 255, 'tls_large': 256, 'tls_negative': -1,
        'tls_bool': True, 'tls_string': '20'}
    def node_read(_self, method, url, *args, **kwargs):
        assert (method, url) == ('GET', 'https://kubernetes.default.svc/api/v1/nodes/node-1')
        if damage == 'node_api_denied':
            from kubernetes.client.exceptions import ApiException
            raise ApiException(status=403, reason='private-runtime-token')
        if damage in {'node_api_tls', 'node_api_tls_verify'}:
            from urllib3.exceptions import MaxRetryError, SSLError
            try:
                verification = ssl.SSLCertVerificationError('private-runtime-token')
                if damage == 'node_api_tls_verify':
                    verification.verify_code = 20
                raise verification
            except ssl.SSLError as error:
                try:
                    raise SSLError(error)
                except SSLError as wrapped:
                    raise MaxRetryError(None, 'private-node-api-url', wrapped) from wrapped
        return HTTPResponse(body=json.dumps({'apiVersion': 'v1', 'kind': 'Node',
            'metadata': {'name': 'node-1', 'uid': str(uuid4()) if damage == 'node_uid' else uid},
            'status': {'addresses': [{'type': 'InternalIP',
                'address': '8.8.8.8' if damage == 'node_address' else '10.20.0.2'}]}}).encode(), status=200)
    def summary(_self, url, **kwargs):
        assert url == 'https://10.20.0.2:10250/stats/summary'
        assert kwargs['headers'] == {'Authorization': 'bearer private-runtime-token'}
        requests.append(url)
        if damage == 'mixed_transport':
            from urllib3.exceptions import SSLError
            raise httpx.ConnectError('private-runtime-token') from SSLError('private-api-marker')
        if damage == 'http_protocol':
            raise httpx.RemoteProtocolError('private-runtime-token') from OSError('private-protocol-marker')
        if damage == 'http_close':
            raise httpx.ReadTimeout('private-runtime-token')
        if damage in {'tls_cycle', 'tls_deep'}:
            wrapped = ssl.SSLError('private-runtime-token')
            if damage == 'tls_cycle':
                wrapped.__cause__ = wrapped
            else:
                verification = ssl.SSLCertVerificationError('private-runtime-token')
                verification.verify_code = 20
                cause = verification
                for _ in range(8):
                    outer = RuntimeError('private-runtime-token')
                    outer.__cause__ = cause
                    cause = outer
                wrapped.__cause__ = cause
            raise httpx.ConnectError('private-runtime-token') from wrapped
        if damage in {'tls', 'tls_close'} or damage in tls_codes:
            import httpcore
            try:
                verification = ssl.SSLCertVerificationError('private-runtime-token')
                if damage in tls_codes:
                    verification.verify_code = tls_codes[damage]
                raise verification
            except ssl.SSLError as error:
                try:
                    raise httpcore.ConnectError('private-runtime-token') from error
                except httpcore.ConnectError as wrapped:
                    raise httpx.ConnectError('private-runtime-token') from wrapped
        if damage in {'timeout', 'connect'}:
            raise (httpx.ReadTimeout if damage == 'timeout' else httpx.ConnectError)('private-runtime-token')
        if damage == 'payload':
            return httpx.Response(200, content=b'private-not-json', request=httpx.Request('GET', url))
        node = {'nodeName': 'foreign' if damage == 'summary_identity' else 'node-1', 'cpu': {'usageCoreNanoSeconds': 1000},
            'memory': {'workingSetBytes': 2000}, 'fs': {'usedBytes': 3000}}
        if damage == 'missing_counter':
            del node['cpu']
        elif damage == 'boolean_counter':
            node['memory']['workingSetBytes'] = True
        elif damage == 'negative_counter':
            node['fs']['usedBytes'] = -1
        return httpx.Response(403 if damage == 'denied' else 503 if damage == 'http_failure' else 200,
            json={'node': node, 'pods': [{'private-foreign-workload-marker': 'never emitted'}]}, request=httpx.Request('GET', url))
    actual_close = InClusterKubernetesJobApi.close
    async def close(self):
        closed.append(True)
        await actual_close(self)
        if damage in {'close', 'tls_close'}:
            raise RuntimeError('private-runtime-token')
    async def legacy_summary(self, *, node_name):
        pytest.fail('legacy unpinned reader executed')
    actual_create_context = ssl.create_default_context
    def create_context(**kwargs):
        if damage == 'tls_unknown_plain':
            raise ssl.SSLError('private-runtime-token')
        if damage == 'tls_unknown':
            verification = ssl.SSLCertVerificationError('private-runtime-token')
            verification.verify_code = 10
            raise verification
        if damage == 'missing_ca':
            return actual_create_context(cafile=str(tmp_path / 'private-missing-ca.crt'))
        if damage == 'unreadable_ca':
            raise PermissionError('private-unreadable-ca.crt')
        return ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    monkeypatch.setattr(client.ApiClient, 'request', node_read)
    monkeypatch.setattr(httpx.Client, 'get', summary)
    if damage == 'http_close':
        actual_exit = httpx.Client.__exit__
        def failed_exit(self, *args):
            actual_exit(self, *args)
            raise RuntimeError('private-close-marker')
        monkeypatch.setattr(httpx.Client, '__exit__', failed_exit)
    monkeypatch.setattr(ssl, 'create_default_context', create_context)
    monkeypatch.setattr(InClusterKubernetesJobApi, 'close', close)
    if damage == 'old_image':
        monkeypatch.setattr(InClusterKubernetesJobApi, 'resource_summary', legacy_summary)
    exec(_BOUND_TELEMETRY_COMMAND, {})
    output = capsys.readouterr()
    assert 'private-' not in output.out + output.err
    if damage:
        assert json.loads(output.out) == {'status': 'blocked', 'stage': stage}
        assert not output.err
    else:
        assert json.loads(output.out) == {'status': 'qualified', 'node_name': 'node-1', 'node_uid': uid}
        assert not output.err
    if damage in {'namespace', 'target', 'remote', 'client'}:
        assert not requests and not closed
    else:
        assert closed == [True]
    if damage in {'missing_ca', 'unreadable_ca', 'node_api_tls', 'node_api_tls_verify', 'tls_unknown', 'tls_unknown_plain'}:
        assert not requests


@pytest.mark.parametrize('damage', ['loaded_url', 'secret_identity', 'secret_version', 'foreign_database',
    'pod_owner', 'pod_template', 'not_ready', 'after_drift'])
def test_runtime_database_qualification_rejects_drift_in_every_consumer(workload_database, damage):
    from scripts.ops.nebius_pool_migration import PoolMigrationError

    api, state = workload_database
    if damage == 'loaded_url':
        state.runtime_environment[state.variable] += '?application_name=private-override'
    elif damage == 'secret_identity':
        state.runtime_secret['metadata']['uid'] = str(uuid4())
    elif damage == 'secret_version':
        state.runtime_secret['metadata']['resourceVersion'] = '12'
    elif damage == 'foreign_database':
        for key in state.runtime_secret['data']:
            raw = base64.b64decode(state.runtime_secret['data'][key]).decode().replace('loom-postgres.', 'foreign.')
            state.runtime_secret['data'][key] = base64.b64encode(raw.encode()).decode()
        state.runtime_environment[state.variable] = state.runtime_environment[state.variable].replace('loom-postgres.', 'foreign.')
    elif damage == 'pod_owner':
        state.runtime_pod['metadata']['ownerReferences'][0]['uid'] = str(uuid4())
    elif damage == 'pod_template':
        state.runtime_pod['spec']['containers'][0]['image'] = 'foreign:latest'
    elif damage == 'not_ready':
        state.runtime_pod['status']['containerStatuses'][0]['ready'] = False
    else:
        state.runtime_after_drift = True
    with pytest.raises(PoolMigrationError) as error:
        qualify_workload(api, state)
    assert 'private-' not in str(error.value)
    assert len(state.commands) == (1 if damage in {'loaded_url', 'after_drift'} else 0)
    assert all(b'private-' not in result.stdout + result.stderr for result in state.processes)


@pytest.mark.parametrize('workload_database', ['service'], indirect=True)
@pytest.mark.parametrize('source', ['pooled', 'dotenv'])
def test_service_probe_checks_effective_settings_including_pooled_and_image_local_overrides(workload_database, source):
    from scripts.ops.nebius_pool_migration import PoolMigrationError

    api, state = workload_database
    alternate = state.runtime_environment[state.variable] + '?application_name=private-override'
    if source == 'pooled':
        state.runtime_environment['LOOM_SVC_DB_URL_POOL'] = alternate
    else:
        (api.kubeconfig.parent / '.env').write_text('LOOM_SVC_DB_URL_POOL=' + alternate + '\n')
    with pytest.raises(PoolMigrationError):
        qualify_workload(api, state)
    assert len(state.commands) == 1
    assert all(b'private-' not in result.stdout + result.stderr for result in state.processes)


@pytest.mark.parametrize('workload_database', ['actuator'], indirect=True)
@pytest.mark.parametrize('damage', [None, 'audience', 'ca', 'writable_mount', 'foreign_mount'])
def test_runtime_recognizes_only_standard_kubernetes_automounted_authority(workload_database, damage):
    from scripts.ops.nebius_pool_migration import PoolMigrationError

    api, state = workload_database
    assert state.original['spec']['template']['spec']['automountServiceAccountToken'] is True
    pod = state.runtime_pod['spec']
    token = {'name': 'kube-api-access-abcde', 'projected': {'defaultMode': 420, 'sources': [
        {'serviceAccountToken': {'expirationSeconds': 3607, 'path': 'token'}},
        {'configMap': {'name': 'kube-root-ca.crt', 'items': [{'key': 'ca.crt', 'path': 'ca.crt'}]}},
        {'downwardAPI': {'items': [{'path': 'namespace', 'fieldRef': {'apiVersion': 'v1', 'fieldPath': 'metadata.namespace'}}]}},
    ]}}
    mount = {'name': token['name'], 'readOnly': True, 'mountPath': '/var/run/secrets/kubernetes.io/serviceaccount'}
    pod.setdefault('volumes', []).append(token)
    pod['containers'][0].setdefault('volumeMounts', []).append(mount)
    if damage == 'audience':
        token['projected']['sources'][0]['serviceAccountToken']['audience'] = 'foreign'
    elif damage == 'ca':
        token['projected']['sources'][1]['configMap']['name'] = 'foreign'
    elif damage == 'writable_mount':
        mount['readOnly'] = False
    elif damage == 'foreign_mount':
        mount['mountPath'] = '/var/run/other'
    if damage:
        with pytest.raises(PoolMigrationError):
            qualify_workload(api, state)
        assert not state.commands
    else:
        assert qualify_workload(api, state) is None
        assert len(state.commands) == 1
