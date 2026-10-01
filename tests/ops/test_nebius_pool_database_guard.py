"""Guard observation survives CP retirement but cannot follow another database."""
from __future__ import annotations

import base64
import copy
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest
from tests.ops.test_nebius_pool_runtime import runtime_inputs as runtime_inputs
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


@pytest.mark.parametrize('action,status', [('stage', 'staged'), ('observe', 'qualified')])
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
    report = {'status': 'observed', 'schema_revision': '0172', 'rows': [{
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
