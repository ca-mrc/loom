"""Connected shutdown sends fixed CAS and waits for real process inventory rules."""
from __future__ import annotations

import copy
import json
from uuid import uuid4

import httpx
import pytest
from scripts.ops.nebius_ingress_stage import _key
from tests.ops.test_nebius_pool_startup_fence_live import activation_http as activation_http
from tests.ops.test_nebius_pool_startup_fence_live import closed_startup as closed_startup
from tests.ops.test_nebius_pool_startup_fence_live import collector_inputs as collector_inputs
from tests.ops.test_nebius_pool_startup_fence_live import (
    cutover_binding_inventory as cutover_binding_inventory,
)
from tests.ops.test_nebius_pool_startup_fence_live import cutover_inputs as cutover_inputs
from tests.ops.test_nebius_pool_startup_fence_live import fencing_inputs as fencing_inputs
from tests.ops.test_nebius_pool_startup_fence_live import management_inputs as management_inputs
from tests.ops.test_nebius_pool_startup_fence_live import platform_inputs as platform_inputs
from tests.ops.test_nebius_pool_startup_fence_live import retirement_inputs as retirement_inputs
from tests.ops.test_nebius_pool_startup_fence_live import runtime_inputs as runtime_inputs
from tests.ops.test_nebius_pool_startup_fence_live import startup_http as startup_http
from tests.ops.test_nebius_pool_startup_fence_live import (
    unbound_cutover_inputs as unbound_cutover_inputs,
)


@pytest.mark.timeout(600)
def test_connected_shutdown_is_intent_bound_observes_unknown_and_waits_for_terminating_pods(activation_http, closed_startup, monkeypatch):
    from scripts.ops.nebius_pool_shutdown import stop_pool_successors
    from scripts.ops.nebius_pool_startup_fence import fence_pool_startup

    request, _, _, _, _, root = closed_startup
    with activation_http() as (api, state, advance):
        assert advance(cancel=True)['status'] == 'pool_activation_cancelled'
        assert fence_pool_startup(request=request, api=api, state_dir=root / 'cutover',
            anchor_dir=root / 'cutover-anchor')['status'] == 'startup_writes_fenced'
        api.parent.history.recovery_pool_drained = lambda: True
        api.parent.guards.recovery_participant_drained = lambda target: True
        manager_key = _key(request.manager)
        before = copy.deepcopy(state.objects[manager_key])
        desired = copy.deepcopy(before)
        desired['spec']['replicas'] = 0
        # A fixed payload still cannot bypass durable shutdown intent.
        with pytest.raises(ValueError):
            api.stop_workload(manager_key, before, desired)
        assert not state.writes
        original_transport = api.parent.client._transport
        mode = 'before'
        pending_pod = True
        patches = []
        full_checks, collection_reads = 0, 0
        first_intent_counts = []
        verify_retained = api.verify_retained

        def verify():
            nonlocal full_checks
            full_checks += 1
            return verify_retained()

        monkeypatch.setattr(api, 'verify_retained', verify)

        def respond(message):
            nonlocal collection_reads
            path = message.url.path
            if message.method == 'GET' and message.url.params.get('limit') == '100':
                collection_reads += 1
            if message.method == 'PATCH':
                kind = 'CronJob' if '/cronjobs/' in path else 'Deployment'
                namespace, name = path.split('/')[-3], path.split('/')[-1]
                key = kind + ':' + namespace + ':' + name
                current = state.objects[key]
                field, value = ('suspend', True) if kind == 'CronJob' else ('replicas', 0)
                body = json.loads(message.content)
                assert body == [
                    {'op': 'test', 'path': '/metadata/uid', 'value': current['metadata']['uid']},
                    {'op': 'test', 'path': '/metadata/resourceVersion', 'value': current['metadata']['resourceVersion']},
                    {'op': 'test', 'path': '/metadata', 'value': current['metadata']},
                    {'op': 'test', 'path': '/spec', 'value': current['spec']},
                    {'op': 'replace', 'path': '/spec/' + field, 'value': value}]
                proposed = copy.deepcopy(current)
                proposed['spec'][field] = value
                if message.url.params:
                    assert dict(message.url.params) == {'dryRun': 'All'}
                    return httpx.Response(200, json=proposed)
                record = json.loads((root / 'cutover/shutdown.json').read_bytes())['workloads'][key]
                assert record == {'phase': 'intent', 'before_resource_version': current['metadata']['resourceVersion']}
                if not patches:
                    first_intent_counts.append((full_checks, collection_reads))
                patches.append(key)
                if mode == 'before':
                    raise httpx.ReadTimeout('private-marker')
                proposed['metadata'].update(resourceVersion=str(int(current['metadata']['resourceVersion']) + 1), generation=2)
                proposed['status'] = {'observedGeneration': 2} if kind == 'Deployment' else {'active': []}
                state.objects[key] = proposed
                if mode == 'after':
                    raise httpx.ReadTimeout('private-marker')
                return httpx.Response(200, json=proposed)
            if dict(message.url.params) == {'limit': '1000'}:
                resource = path.rsplit('/', 1)[1]
                kind, version = {'pods': ('Pod', 'v1'), 'jobs': ('Job', 'batch/v1'), 'replicasets': ('ReplicaSet', 'apps/v1')}[resource]
                items = []
                manager = state.objects[manager_key]
                if resource == 'pods' and pending_pod and path.split('/')[-2] == manager['metadata']['namespace']:
                    items.append({'apiVersion': 'v1', 'kind': 'Pod', 'metadata': {'name': 'terminating-manager',
                        'namespace': manager['metadata']['namespace'], 'uid': str(uuid4()),
                        'labels': manager['spec']['selector']['matchLabels'], 'deletionTimestamp': '2026-10-02T00:00:00Z'}})
                return httpx.Response(200, json={'apiVersion': version, 'kind': kind + 'List',
                    'metadata': {'resourceVersion': '50'}, 'items': items})
            return original_transport.handle_request(message)
        with httpx.MockTransport(respond) as transport:
            monkeypatch.setattr(api.parent.client, '_transport', transport)
            def run():
                return stop_pool_successors(request=request, api=api, state_dir=root / 'cutover', anchor_dir=root / 'cutover-anchor')
            first = run()
            assert first['status'] == 'pending_shutdown_outcome'
            assert first_intent_counts == [(4, 96)]
            assert run() == first and len(patches) == 1
            # The same original stop arrives; no redispatch is authorized.
            current = state.objects[patches[0]]
            current['spec']['suspend' if current['kind'] == 'CronJob' else 'replicas'] = True if current['kind'] == 'CronJob' else 0
            current['metadata'].update(resourceVersion=str(int(current['metadata']['resourceVersion']) + 1), generation=2)
            current['status'] = {'observedGeneration': 2} if current['kind'] == 'Deployment' else {'active': []}
            mode = 'after'
            assert run()['status'] == 'pending_successor_drain'
            pending_pod = False
            assert run()['status'] == 'pool_successors_stopped'
            assert len(patches) == len(set(patches)) == len(api.targets)
            assert advance(cancel=True)['status'] == 'pool_activation_cancelled'


@pytest.mark.timeout(600)
@pytest.mark.parametrize('damage', [None, 'uid', 'spec', 'authorization', 'drain_authorization', 'fence', 'busy', 'ancestry'])
def test_shutdown_reads_cas_after_slow_drain_and_rejects_real_drift(
        activation_http, closed_startup, monkeypatch, damage):
    from scripts.ops.nebius_pool_shutdown import stop_pool_successors
    from scripts.ops.nebius_pool_startup_fence import fence_pool_startup

    request, _, _, _, _, root = closed_startup
    with activation_http() as (api, state, advance):
        assert advance(cancel=True)['status'] == 'pool_activation_cancelled'
        assert fence_pool_startup(request=request, api=api, state_dir=root / 'cutover',
            anchor_dir=root / 'cutover-anchor')['status'] == 'startup_writes_fenced'
        api.parent.history.recovery_pool_drained = lambda: True
        api.parent.guards.recovery_participant_drained = lambda target: True
        original_drain = api.recovery_drained
        drains = 0

        def drain():
            nonlocal drains
            result = original_drain()
            drains += 1
            # The CronJob controller updates status while the expensive recovery
            # qualification runs. UID and spec remain unchanged in the regression.
            for key in api.targets:
                current = state.objects[key]
                current['metadata']['resourceVersion'] = str(int(current['metadata']['resourceVersion']) + 1)
            if damage in {'uid', 'spec'} and drains == 2:
                current = state.objects[next(reversed(api.targets))]
                if damage == 'uid':
                    current['metadata']['uid'] = str(uuid4())
                else:
                    current['spec']['suspend' if current['kind'] == 'CronJob' else 'replicas'] = (
                        True if current['kind'] == 'CronJob' else 7)
            return result

        monkeypatch.setattr(api, 'recovery_drained', drain)
        stop = api.stop_workload

        def dispatch(*args, **kwargs):
            # Drift after the stage's dry-run and rechecks must be rejected by
            # the actual dispatch boundary before it records any write intent.
            if damage == 'authorization':
                state.role_damage = True
            elif damage == 'drain_authorization':
                def pool_drain():
                    state.role_damage = True
                    return True
                api.parent.history.recovery_pool_drained = pool_drain
            elif damage == 'fence':
                state.guards[next(iter(state.guards))] = 'open'
            elif damage == 'busy':
                api.parent.history.recovery_pool_drained = lambda: False
            elif damage == 'ancestry':
                path = root / 'cutover/cutover.json'
                path.write_bytes(path.read_bytes() + b' ')
            return stop(*args, **kwargs)

        monkeypatch.setattr(api, 'stop_workload', dispatch)
        original_transport = api.parent.client._transport
        writes = []

        def respond(message):
            if message.method == 'PATCH':
                kind = 'CronJob' if '/cronjobs/' in message.url.path else 'Deployment'
                key = kind + ':' + message.url.path.split('/')[-3] + ':' + message.url.path.split('/')[-1]
                current = state.objects[key]
                patch = json.loads(message.content)
                version = next(row['value'] for row in patch if row['path'] == '/metadata/resourceVersion')
                if version != current['metadata']['resourceVersion']:
                    return httpx.Response(422, json={'apiVersion': 'v1', 'kind': 'Status',
                        'status': 'Failure', 'code': 422, 'reason': 'Invalid'})
                assert patch[:4] == [
                    {'op': 'test', 'path': '/metadata/uid', 'value': current['metadata']['uid']},
                    {'op': 'test', 'path': '/metadata/resourceVersion', 'value': version},
                    {'op': 'test', 'path': '/metadata', 'value': current['metadata']},
                    {'op': 'test', 'path': '/spec', 'value': current['spec']}]
                proposed = copy.deepcopy(current)
                field = 'suspend' if kind == 'CronJob' else 'replicas'
                proposed['spec'][field] = True if kind == 'CronJob' else 0
                if not message.url.params:
                    row = json.loads((root / 'cutover/shutdown.json').read_bytes())['workloads'][key]
                    assert row == {'phase': 'intent', 'before_resource_version': version}
                    writes.append(key)
                    proposed['metadata'].update(resourceVersion=str(int(version) + 1),
                        generation=current['metadata'].get('generation', 1) + 1)
                    proposed['status'] = {'observedGeneration': proposed['metadata']['generation']}
                    state.objects[key] = proposed
                return httpx.Response(200, json=proposed)
            if dict(message.url.params) == {'limit': '1000'}:
                resource = message.url.path.rsplit('/', 1)[1]
                kind, version = {'pods': ('Pod', 'v1'), 'jobs': ('Job', 'batch/v1'),
                    'replicasets': ('ReplicaSet', 'apps/v1')}[resource]
                return httpx.Response(200, json={'apiVersion': version, 'kind': kind + 'List',
                    'metadata': {'resourceVersion': '50'}, 'items': []})
            return original_transport.handle_request(message)

        with httpx.MockTransport(respond) as transport:
            monkeypatch.setattr(api.parent.client, '_transport', transport)
            arguments = dict(request=request, api=api, state_dir=root / 'cutover', anchor_dir=root / 'cutover-anchor')
            if damage:
                with pytest.raises(ValueError):
                    stop_pool_successors(**arguments)
                assert not writes
                journal = json.loads((root / 'cutover/shutdown.json').read_bytes())
                assert all(row['phase'] == 'prepared' for row in journal['workloads'].values())
            else:
                assert stop_pool_successors(**arguments)['status'] == 'pool_successors_stopped'
                assert len(writes) == len(set(writes)) == len(api.targets)
