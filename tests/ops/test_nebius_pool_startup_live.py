"""Fixed HTTPS startup consumes real parent journals and current closure checks."""
from __future__ import annotations

import copy
import json
import ssl
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
from scripts.ops.nebius_ingress_stage import _key
from tests.ops import test_nebius_pool_startup as startup_fixtures
from tests.ops.test_nebius_pool_startup import closed_startup as closed_startup
from tests.ops.test_nebius_pool_startup import collector_inputs as collector_inputs
from tests.ops.test_nebius_pool_startup import (
    cutover_binding_inventory as cutover_binding_inventory,
)
from tests.ops.test_nebius_pool_startup import fencing_inputs as fencing_inputs
from tests.ops.test_nebius_pool_startup import management_inputs as management_inputs
from tests.ops.test_nebius_pool_startup import platform_inputs as platform_inputs
from tests.ops.test_nebius_pool_startup import retirement_inputs as retirement_inputs
from tests.ops.test_nebius_pool_startup import runtime_inputs as runtime_inputs

from loom.db.schema_startup import service_schema_head

unbound_cutover_inputs = startup_fixtures.cutover_inputs


@pytest.fixture
def cutover_inputs(unbound_cutover_inputs, platform_inputs):
    from scripts.ops.nebius_pool_migration import PoolGuardDatabase

    from loom.nebius_platform_render import build_platform

    request, tokens = unbound_cutover_inputs
    migration = request.fencing.retirement.migration
    guards = []
    for guard in migration.guards:
        config, candidate, profile = copy.deepcopy(platform_inputs)
        config['namespace'] = guard.namespace
        documents = build_platform(config, candidate, profile, {}, repo_root=Path(__file__).resolve().parents[2])
        database, = [row for row in documents['20-database.yaml'] if row['kind'] == 'StatefulSet']
        service, = [row for row in documents['20-database.yaml'] if row['kind'] == 'Service']
        for document in (database, service):
            document['metadata'].update(uid=str(uuid4()), resourceVersion='1')
        guards.append(replace(guard, database=PoolGuardDatabase(statefulset=database, service=service,
            credential_uid=uuid4(), credential_resource_version='7',
            actuator_credential_uid=uuid4(), actuator_credential_resource_version='11')))
    return replace(request, fencing=replace(request.fencing,
        retirement=replace(request.fencing.retirement, migration=replace(migration, guards=tuple(guards))))), tokens


@pytest.fixture
def startup_http(closed_startup, cutover_binding_inventory):
    from scripts.ops.nebius_pool_cutover import cutover_documents
    from scripts.ops.nebius_pool_cutover_live import HTTPSPoolCutoverAPI

    request, tokens, closed, external, _, root = closed_startup
    migration = request.fencing.retirement.migration
    binding = migration.registration.binding
    namespaces = {binding.namespace: binding.namespace_uid, 'kube-system': binding.kube_system_uid,
        **{row.namespace: str(row.namespace_uid) for row in migration.guards},
        **{ns.name: str(ns.uid) for row in migration.registration.spec.participants
            for ns in (row.execution_namespace, row.build_namespace)}}
    inventories = copy.deepcopy(cutover_binding_inventory)
    inventories['roles'] = list(closed.fencing.roles.values())
    collections = {'Role': 'roles', 'RoleBinding': 'rolebindings', 'ClusterRole': 'clusterroles', 'ClusterRoleBinding': 'clusterrolebindings'}
    for row in closed.resources.resources.values():
        if row['kind'] in collections:
            inventories[collections[row['kind']]].append(copy.deepcopy(row))
    objects = {**copy.deepcopy(closed.resources.resources), **external.documents,
        **{_key(guard.database.statefulset): copy.deepcopy(guard.database.statefulset)
            for guard in migration.guards if guard.database is not None}}
    paths = {}
    for key, row in objects.items():
        kind = row['kind']
        resource = {'Deployment': 'deployments', 'StatefulSet': 'statefulsets', 'CronJob': 'cronjobs', 'ConfigMap': 'configmaps',
            'Secret': 'secrets', 'ServiceAccount': 'serviceaccounts', **collections}[kind]
        prefix = '/api/v1' if row['apiVersion'] == 'v1' else '/apis/' + row['apiVersion']
        paths[prefix + ('/namespaces/' + row['metadata']['namespace'] if row['metadata'].get('namespace') else '')
            + '/' + resource + '/' + row['metadata']['name']] = key
    state = SimpleNamespace(objects=objects, writes=[], calls=[], failure=None, fail_key=_key(request.manager),
        closed_reads=0, fail_closed=False, fail_guard=False, previews=[], inventories=inventories,
        gateway_reviews=[], gateway_review_damage=None, gateway_extra_rules={})
    gateway_authority = cutover_documents(request)['authority']

    def closed_database():
        state.closed_reads += 1
        if state.fail_closed:
            raise ValueError('private-marker')

    history = SimpleNamespace(qualify_binding=closed.qualify_binding, qualify_closed_pool=closed_database,
        qualify_pending_origins=lambda target, origins: None)

    def guard(target, action):
        assert target in migration.guards and action == 'observe'
        return {'status': 'open' if state.fail_guard else 'held'}

    def runtime_role(target, action):
        assert target.participant_id in closed.acl_staged and action == 'observe'
        return {'status': 'qualified'}

    guards = SimpleNamespace(request=migration, guard=guard, runtime_role=runtime_role,
        cutover_readiness_page=lambda target, after: {'status': 'observed', 'schema_revision': service_schema_head(), 'rows': []})

    def respond(message):
        state.calls.append(message)
        path = message.url.path
        if path == '/apis/authorization.k8s.io/v1/selfsubjectrulesreviews':
            assert message.method == 'POST'
            assert message.headers['Impersonate-User'] == f'system:serviceaccount:{binding.namespace}:loom-pool-gateway'
            assert message.headers.get_list('Impersonate-Group') == [
                'system:serviceaccounts', 'system:serviceaccounts:' + binding.namespace, 'system:authenticated']
            namespace = json.loads(message.content)['spec']['namespace']
            state.gateway_reviews.append(namespace)
            rules = [copy.deepcopy(rule) for row in gateway_authority if row['kind'] == 'ClusterRole'
                or (row['kind'] == 'Role' and row['metadata']['namespace'] == namespace) for rule in row['rules']]
            rules.extend(state.gateway_extra_rules.get(namespace, []))
            if state.gateway_review_damage == 'late_guard':
                state.fail_guard = True
            return httpx.Response(201, json={'apiVersion': 'authorization.k8s.io/v1', 'kind': 'SelfSubjectRulesReview',
                'spec': {}, 'status': {'incomplete': state.gateway_review_damage == 'incomplete',
                    'resourceRules': rules, 'nonResourceRules': []}})
        if path in {'/api/v1/namespaces/' + name for name in namespaces}:
            assert message.method == 'GET'
            name = path.rsplit('/', 1)[1]
            return httpx.Response(200, json={'apiVersion': 'v1', 'kind': 'Namespace', 'metadata': {'name': name,
                'uid': namespaces[name], 'labels': {'loom.nebius/management-installation': binding.installation_id,
                    'pod-security.kubernetes.io/enforce': 'restricted'}}})
        if path in paths:
            key = paths[path]
            if message.method == 'GET':
                return httpx.Response(200, json=state.objects[key])
            assert message.method == 'PATCH'
            before = state.objects[key]
            field = 'suspend' if before['kind'] == 'CronJob' else 'replicas'
            patches = json.loads(message.content)
            if patches[-1]['path'] == '/metadata/annotations':
                assert patches == [
                    {'op': 'test', 'path': '/metadata/uid', 'value': before['metadata']['uid']},
                    {'op': 'test', 'path': '/metadata/resourceVersion', 'value': before['metadata']['resourceVersion']},
                    {'op': 'test', 'path': '/metadata', 'value': before['metadata']},
                    {'op': 'test', 'path': '/spec', 'value': before['spec']},
                    {'op': 'add', 'path': '/metadata/annotations', 'value': {
                        **before['metadata'].get('annotations', {}), 'loom.nebius/pool-startup-fence': str(migration.registration.spec.operation_id)}}]
                desired = copy.deepcopy(before)
                desired['metadata']['annotations'] = patches[-1]['value']
                if message.url.params:
                    assert dict(message.url.params) == {'dryRun': 'All'}
                    return httpx.Response(200, json=desired)
                assert json.loads((root / 'cutover/startup-fence.json').read_bytes())['workloads'][key] == {'phase': 'intent', 'expected': None}
                state.writes.append(key)
                if state.failure == 'before':
                    raise httpx.ReadTimeout('private-marker')
                desired['metadata']['resourceVersion'] = str(int(before['metadata']['resourceVersion']) + 1)
                state.objects[key] = desired
                if state.failure == 'after':
                    raise httpx.ReadTimeout('private-marker')
                return httpx.Response(200, json=desired)
            assert patches == [
                {'op': 'test', 'path': '/metadata/uid', 'value': before['metadata']['uid']},
                {'op': 'test', 'path': '/metadata/resourceVersion', 'value': before['metadata']['resourceVersion']},
                {'op': 'test', 'path': '/spec', 'value': before['spec']},
                {'op': 'replace', 'path': '/spec/' + field, 'value': False if field == 'suspend' else 1}]
            desired = copy.deepcopy(before)
            desired['spec'][field] = False if field == 'suspend' else 1
            if message.url.params:
                assert dict(message.url.params) == {'dryRun': 'All'}
                state.previews.append(key)
                return httpx.Response(200, json=desired)
            intent = json.loads((root / 'cutover/startup.json').read_bytes())['workloads'][key]
            assert intent == {'phase': 'intent', 'before_resource_version': before['metadata']['resourceVersion']}
            state.writes.append(key)
            if key == state.fail_key:
                if state.failure == 'before':
                    raise httpx.ReadTimeout('private-marker')
                if state.failure in {'conflict', 'unqualified_conflict'}:
                    return httpx.Response(409, json={'apiVersion': 'v1', 'kind': 'Status',
                        'status': 'Failure', 'reason': 'Conflict' if state.failure == 'conflict' else 'foreign', 'code': 409})
            desired['metadata']['resourceVersion'] = str(int(before['metadata']['resourceVersion']) + 1)
            state.objects[key] = desired
            if key == state.fail_key and state.failure == 'after':
                raise httpx.ReadTimeout('private-marker')
            return httpx.Response(200, json=desired)
        resource = path.rsplit('/', 1)[1]
        assert message.method == 'GET' and dict(message.url.params).get('limit') == '100'
        if resource in inventories:
            rows = inventories[resource]
        else:
            kinds = {'deployments': 'Deployment', 'cronjobs': 'CronJob', 'pods': 'Pod', 'jobs': 'Job', 'replicasets': 'ReplicaSet',
                'statefulsets': 'StatefulSet', 'daemonsets': 'DaemonSet', 'replicationcontrollers': 'ReplicationController'}
            rows = [row for row in state.objects.values() if row['kind'] == kinds[resource]]
        kind = {'roles': 'Role', 'rolebindings': 'RoleBinding', 'clusterroles': 'ClusterRole', 'clusterrolebindings': 'ClusterRoleBinding',
            'deployments': 'Deployment', 'cronjobs': 'CronJob', 'pods': 'Pod', 'jobs': 'Job', 'replicasets': 'ReplicaSet',
            'statefulsets': 'StatefulSet', 'daemonsets': 'DaemonSet', 'replicationcontrollers': 'ReplicationController'}[resource]
        version = ('rbac.authorization.k8s.io/v1' if resource in inventories else 'batch/v1' if resource in {'cronjobs', 'jobs'}
            else 'v1' if resource in {'pods', 'replicationcontrollers'} else 'apps/v1')
        return httpx.Response(200, json={'apiVersion': version, 'kind': kind + 'List', 'metadata': {'resourceVersion': '7'}, 'items': rows})

    @contextmanager
    def connect():
        from scripts.ops.nebius_pool_startup_live import HTTPSPoolStartupAPI

        with HTTPSPoolCutoverAPI(request=request, tokens=tokens, migration=closed.migration, guards=guards, checks=closed,
                history=history, api_server='https://cluster.example', ssl_context=ssl.create_default_context(),
                state_dir=root / 'cutover', anchor_dir=root / 'cutover-anchor') as parent:
            parent.client.close()
            parent.client = httpx.Client(base_url=parent.api_server, transport=httpx.MockTransport(respond))
            # Only remote effective-access/role observation is doubled here.
            parent.fencing.read_role = closed.fencing.read_role
            parent.fencing.verify_readonly = closed.fencing.verify_readonly
            yield HTTPSPoolStartupAPI(parent=parent), state
    return connect


@pytest.mark.parametrize('failure', [None, 'before', 'after', 'conflict', 'unqualified_conflict'])
def test_fixed_https_startup_uses_scalar_cas_and_never_retries_unknown_outcomes(startup_http, closed_startup, failure):
    from scripts.ops.nebius_pool_startup import stage_pool_startup

    request, _, _, _, dormant, root = closed_startup
    with startup_http() as (api, state):
        state.failure = failure
        before = copy.deepcopy(state.objects)
        def run():
            return stage_pool_startup(request=request, api=api, state_dir=root / 'cutover', anchor_dir=root / 'cutover-anchor')
        result = run()
        if failure in {'before', 'unqualified_conflict'}:
            assert result['status'] == 'pending_startup_outcome'
            state.failure = None
            assert run() == result and state.writes == [state.fail_key]
        elif failure == 'conflict':
            assert result['status'] == 'pending_startup_update'
            state.failure = None
            assert run()['status'] == 'pool_startup_staged_closed'
            assert len(state.writes) == 13
        else:
            assert result['status'] == 'pool_startup_staged_closed'
            assert len(state.writes) == 12 and state.closed_reads > 12
            state.calls.clear()
            assert run() == result and all(call.method == 'GET' for call in state.calls)
        assert all(state.objects[_key(row)] == before[_key(row)] for row in (dormant.actuator, dormant.collector))
        assert all(state.objects[_key(guard.database.statefulset)] == before[_key(guard.database.statefulset)]
            for guard in request.fencing.retirement.migration.guards if guard.database is not None)


def test_live_scope_checks_exact_inputs_and_fresh_identity_without_rerendering(startup_http, closed_startup, monkeypatch):
    from scripts.ops import nebius_pool_cutover_live as cutover

    request, _, _, _, _, _ = closed_startup
    with startup_http() as (api, state):
        render = cutover.cutover_documents
        calls = []

        def counted(value):
            calls.append(None)
            return render(value)

        monkeypatch.setattr(cutover, 'cutover_documents', counted)
        state.calls.clear()
        for _ in range(3):
            api.parent._scope()
        # Every scope check still reads the actual namespace identities.
        identity_reads = [call for call in state.calls if call.url.path == '/api/v1/namespaces/kube-system']
        assert len(identity_reads) == 3
        assert calls == []
        # Dataclass equality alone treats this invalid type as unchanged.
        request.manager['spec']['replicas'] = True
        with pytest.raises(ValueError, match='pool cutover inputs changed'):
            api.parent._scope()


def test_live_scope_rechecks_image_admission_after_clock_rollback(startup_http, closed_startup, monkeypatch):
    from datetime import datetime, timedelta

    from loom import execution_image_admission as admission

    request, _, _, _, _, _ = closed_startup
    with startup_http() as (api, state):
        api.parent._scope()
        before_issuance = min(row.statement.issued_at for profile in request.profiles.values()
            for row in profile.image_admission.admissions) - timedelta(hours=1)

        class EarlierClock(datetime):
            @classmethod
            def now(cls, tz=None):
                return before_issuance

        monkeypatch.setattr(admission, 'datetime', EarlierClock)
        with pytest.raises(ValueError):
            api.parent._scope()
        assert state.writes == []


@pytest.mark.parametrize('damage', ['closed', 'guard', 'material', 'role', 'unanchored_start'])
def test_fixed_startup_refuses_live_authority_drift_and_out_of_journal_patch(startup_http, closed_startup, damage):
    from scripts.ops.nebius_ingress_stage import _snapshot
    from scripts.ops.nebius_pool_startup import stage_pool_startup

    request, _, closed, _, _, root = closed_startup
    with startup_http() as (api, state):
        if damage == 'closed':
            state.fail_closed = True
        elif damage == 'guard':
            state.fail_guard = True
        elif damage == 'material':
            secret = next(row for row in state.objects.values() if row['kind'] == 'Secret')
            secret['metadata']['uid'] = 'aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaaaa'
        elif damage == 'role':
            next(iter(closed.fencing.roles.values()))['rules'].append({'apiGroups': ['batch'], 'resources': ['jobs'], 'verbs': ['create']})
        if damage == 'unanchored_start':
            before = state.objects[_key(request.manager)]
            desired = _snapshot(before)
            desired['spec']['replicas'] = 1
            with pytest.raises(ValueError):
                api.start_workload(_key(request.manager), before, desired)
        else:
            with pytest.raises(ValueError) as error:
                stage_pool_startup(request=request, api=api, state_dir=root / 'cutover', anchor_dir=root / 'cutover-anchor')
            assert 'private-marker' not in str(error.value)
        assert state.writes == []


@pytest.mark.parametrize('damage', [None, 'incomplete', 'unanchored', 'uid', 'manager', 'database', 'telemetry',
    'manager_settings', 'participant_settings', 'gateway_runtime', 'late_guard', 'late_runtime'])
def test_started_database_proof_derives_exact_successors_and_rechecks_closure(startup_http, closed_startup, damage):
    from scripts.ops.nebius_ingress_stage import _uid
    from scripts.ops.nebius_management_switch import _stable
    from scripts.ops.nebius_pool_migration import PoolMigrationError
    from scripts.ops.nebius_pool_startup import stage_pool_startup

    request, _, _, external, dormant, root = closed_startup
    if damage == 'incomplete':
        external.fail_key, external.failure = _key(request.manager), 'before'
    result = stage_pool_startup(request=request, api=external, state_dir=root / 'cutover', anchor_dir=root / 'cutover-anchor')
    assert result['status'] == ('pending_startup_outcome' if damage == 'incomplete' else 'pool_startup_staged_closed')
    with startup_http() as (api, state):
        state.objects.update(copy.deepcopy(external.documents))
        if damage == 'unanchored':
            (root / 'cutover-anchor' / (str(request.fencing.retirement.migration.registration.spec.operation_id) + '-startup.json')).unlink()
        elif damage == 'uid':
            state.objects[_key(request.manager)]['metadata']['uid'] = 'aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaaaa'
        probes = []

        def expected_runtime(original, expected):
            assert _stable(expected) == _stable(state.objects[_key(original)])
            assert _uid(expected) == _uid(original)
            assert expected['spec']['replicas'] == 1
            assert original not in (dormant.actuator, dormant.collector)

        def manager(*, expected):
            expected_runtime(request.manager, expected)
            probes.append(('manager', _key(request.manager)))
            if damage == 'manager':
                raise PoolMigrationError('management_runtime_database')

        def database(target, *, original, expected, credential_uid, credential_resource_version):
            expected_runtime(original, expected)
            binding = target.database
            actuator = original['metadata']['namespace'] != target.namespace
            assert (credential_uid, credential_resource_version) == (
                (binding.actuator_credential_uid, binding.actuator_credential_resource_version) if actuator
                else (binding.credential_uid, binding.credential_resource_version))
            probes.append(('database', _key(original)))
            if damage == 'database':
                raise PoolMigrationError('runtime_database')

        def telemetry(target, *, original, expected):
            expected_runtime(original, expected)
            assert original in request.fencing.retirement.actuators
            probes.append(('telemetry', _key(original)))
            if damage == 'telemetry':
                raise PoolMigrationError('runtime_telemetry')
            if damage == 'late_guard':
                state.fail_guard = True
            elif damage == 'late_runtime':
                state.objects[_key(request.manager)]['spec']['replicas'] = 0

        def manager_settings(*, expected):
            expected_runtime(request.manager, expected)
            probes.append(('manager_settings', _key(request.manager)))
            if damage == 'manager_settings':
                raise PoolMigrationError('management_pool_settings')

        def participant_settings(target, *, original, expected):
            expected_runtime(original, expected)
            probes.append(('participant_settings', _key(original)))
            if damage == 'participant_settings':
                raise PoolMigrationError('runtime_pool_settings')

        gateway_key = 'Deployment:' + request.fencing.retirement.migration.registration.binding.namespace + ':loom-pool-gateway'
        def gateway_runtime(*, original, expected):
            expected_runtime(original, expected)
            assert _key(original) == gateway_key and original == api.closed[gateway_key]
            assert original['spec']['replicas'] == 0
            probes.append(('gateway_runtime', gateway_key))
            if damage == 'gateway_runtime':
                raise PoolMigrationError('gateway_runtime')

        # Remote probe transport is doubled here; the owning probe tests run
        # real settings and reject unrelated lineage, credentials and backends.
        api.parent.history.qualify_manager_database = manager
        api.parent.history.qualify_manager_pool_settings = manager_settings
        api.parent.history.qualify_gateway_runtime = gateway_runtime
        api.parent.guards.qualify_runtime_database = database
        api.parent.guards.qualify_runtime_telemetry = telemetry
        api.parent.guards.qualify_runtime_pool_settings = participant_settings
        if damage:
            with pytest.raises(ValueError, match='pool_startup_database_runtimes_unqualified'):
                api.qualify_database_runtimes()
            if damage in {'incomplete', 'unanchored', 'uid'}:
                assert not probes
        else:
            assert api.qualify_database_runtimes() is None
            originals = [*(guard.controller for guard in request.fencing.retirement.migration.guards),
                *request.services, *request.fencing.retirement.actuators]
            assert set(probes) == {('manager', _key(request.manager)),
                ('manager_settings', _key(request.manager)),
                ('gateway_runtime', gateway_key),
                *(('database', _key(row)) for row in originals),
                *(('participant_settings', _key(row)) for row in originals),
                *(('telemetry', _key(row)) for row in request.fencing.retirement.actuators)}
            assert len(probes) == 3 + 2 * len(originals) + len(request.fencing.retirement.actuators)
        assert not state.writes and all(call.method == 'GET' for call in state.calls)


@pytest.mark.parametrize('damage', [None, 'unstarted', 'incomplete', 'extra_named', 'foreign_named', 'late_guard'])
def test_started_gateway_authority_is_effective_and_cannot_ignore_foreign_grants(startup_http, closed_startup, damage):
    from scripts.ops.nebius_pool_startup import stage_pool_startup

    request, _, _, external, _, root = closed_startup
    if damage != 'unstarted':
        assert stage_pool_startup(request=request, api=external, state_dir=root / 'cutover',
            anchor_dir=root / 'cutover-anchor')['status'] == 'pool_startup_staged_closed'
    with startup_http() as (api, state):
        state.objects.update(copy.deepcopy(external.documents))
        migration = request.fencing.retirement.migration
        state.gateway_review_damage = damage
        if damage in {'extra_named', 'foreign_named'}:
            namespace = 'foreign-team' if damage == 'foreign_named' else migration.registration.binding.namespace
            rule = {'apiGroups': ['batch'], 'resources': ['jobs'], 'verbs': ['patch'], 'resourceNames': ['hidden-job']}
            state.gateway_extra_rules[namespace] = [rule]
            if damage == 'foreign_named':
                state.inventories['roles'].append({'apiVersion': 'rbac.authorization.k8s.io/v1', 'kind': 'Role',
                    'metadata': {'namespace': namespace, 'name': 'foreign-role', 'uid': str(uuid4()), 'resourceVersion': '1'}, 'rules': [rule]})
                state.inventories['rolebindings'].append({'apiVersion': 'rbac.authorization.k8s.io/v1', 'kind': 'RoleBinding',
                    'metadata': {'namespace': namespace, 'name': 'foreign-binding', 'uid': str(uuid4()), 'resourceVersion': '1'},
                    'roleRef': {'apiGroup': 'rbac.authorization.k8s.io', 'kind': 'Role', 'name': 'foreign-role'},
                    'subjects': [{'kind': 'ServiceAccount', 'name': 'loom-pool-gateway', 'namespace': migration.registration.binding.namespace}]})
        if damage:
            with pytest.raises(ValueError, match='pool_startup_gateway_authority_unqualified'):
                api.qualify_gateway_authority()
            if damage == 'foreign_named':
                assert 'foreign-team' in state.gateway_reviews
        else:
            api.qualify_gateway_authority()
            expected = {migration.registration.binding.namespace, *(guard.namespace for guard in migration.guards),
                *(ns.name for participant in migration.registration.spec.participants for ns in (participant.execution_namespace, participant.build_namespace))}
            assert set(state.gateway_reviews) == expected
        assert not state.writes
        assert all(call.method == 'GET' or call.url.path == '/apis/authorization.k8s.io/v1/selfsubjectrulesreviews' for call in state.calls)
        assert all('Impersonate-User' not in call.headers for call in state.calls if call.method == 'GET')
