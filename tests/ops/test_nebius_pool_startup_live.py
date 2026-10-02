"""Fixed HTTPS startup consumes real parent journals and current closure checks."""
from __future__ import annotations

import copy
import json
import ssl
from contextlib import contextmanager
from types import SimpleNamespace

import httpx
import pytest
from scripts.ops.nebius_ingress_stage import _key
from tests.ops.test_nebius_pool_startup import closed_startup as closed_startup
from tests.ops.test_nebius_pool_startup import collector_inputs as collector_inputs
from tests.ops.test_nebius_pool_startup import (
    cutover_binding_inventory as cutover_binding_inventory,
)
from tests.ops.test_nebius_pool_startup import cutover_inputs as cutover_inputs
from tests.ops.test_nebius_pool_startup import fencing_inputs as fencing_inputs
from tests.ops.test_nebius_pool_startup import management_inputs as management_inputs
from tests.ops.test_nebius_pool_startup import platform_inputs as platform_inputs
from tests.ops.test_nebius_pool_startup import retirement_inputs as retirement_inputs
from tests.ops.test_nebius_pool_startup import runtime_inputs as runtime_inputs


@pytest.fixture
def startup_http(closed_startup, cutover_binding_inventory):
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
    objects = {**copy.deepcopy(closed.resources.resources), **external.documents}
    paths = {}
    for key, row in objects.items():
        kind = row['kind']
        resource = {'Deployment': 'deployments', 'CronJob': 'cronjobs', 'ConfigMap': 'configmaps',
            'Secret': 'secrets', 'ServiceAccount': 'serviceaccounts', **collections}[kind]
        prefix = '/api/v1' if row['apiVersion'] == 'v1' else '/apis/' + row['apiVersion']
        paths[prefix + ('/namespaces/' + row['metadata']['namespace'] if row['metadata'].get('namespace') else '')
            + '/' + resource + '/' + row['metadata']['name']] = key
    state = SimpleNamespace(objects=objects, writes=[], calls=[], failure=None, fail_key=_key(request.manager),
        closed_reads=0, fail_closed=False, fail_guard=False, previews=[])

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
        cutover_readiness_page=lambda target, after: {'status': 'observed', 'schema_revision': '0172', 'rows': []})

    def respond(message):
        state.calls.append(message)
        path = message.url.path
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
