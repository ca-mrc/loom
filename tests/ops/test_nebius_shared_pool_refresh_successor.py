"""Complete prelaunch pool refresh qualification, retained for explicit manual runs."""
from __future__ import annotations

import copy
import json
import ssl
from dataclasses import replace
from pathlib import Path

import certifi
import pytest
from scripts.ops.nebius_ingress_stage import _key
from tests.ops.test_nebius_pool_predecessor import finish_cutover, pool_refresh_http
from tests.ops.test_nebius_pool_refresh_successor import (
    application_management_inputs as application_management_inputs,
)
from tests.ops.test_nebius_pool_refresh_successor import (
    application_material as application_material,
)
from tests.ops.test_nebius_pool_refresh_successor import (
    checks as checks,
)
from tests.ops.test_nebius_pool_refresh_successor import (
    cloud as cloud,
)
from tests.ops.test_nebius_pool_refresh_successor import (
    collector_inputs as collector_inputs,
)
from tests.ops.test_nebius_pool_refresh_successor import (
    completed_upgrade as completed_upgrade,
)
from tests.ops.test_nebius_pool_refresh_successor import (
    connected_refresh as connected_refresh,
)
from tests.ops.test_nebius_pool_refresh_successor import (
    cutover_inputs as cutover_inputs,
)
from tests.ops.test_nebius_pool_refresh_successor import (
    database_guard as database_guard,
)
from tests.ops.test_nebius_pool_refresh_successor import (
    entry_inputs as entry_inputs,
)
from tests.ops.test_nebius_pool_refresh_successor import (
    fencing_inputs as fencing_inputs,
)
from tests.ops.test_nebius_pool_refresh_successor import (
    guest_runtime_inputs as guest_runtime_inputs,
)
from tests.ops.test_nebius_pool_refresh_successor import (
    installation as installation,
)
from tests.ops.test_nebius_pool_refresh_successor import (
    management_inputs as management_inputs,
)
from tests.ops.test_nebius_pool_refresh_successor import (
    material as material,
)
from tests.ops.test_nebius_pool_refresh_successor import (
    ordinary_rollout as ordinary_rollout,
)
from tests.ops.test_nebius_pool_refresh_successor import (
    platform_inputs as platform_inputs,
)
from tests.ops.test_nebius_pool_refresh_successor import (
    private_cutover as private_cutover,
)
from tests.ops.test_nebius_pool_refresh_successor import (
    private_upgrade as private_upgrade,
)
from tests.ops.test_nebius_pool_refresh_successor import (
    refresh_database as refresh_database,
)
from tests.ops.test_nebius_pool_refresh_successor import (
    retirement_inputs as retirement_inputs,
)
from tests.ops.test_nebius_pool_refresh_successor import (
    runtime_inputs as runtime_inputs,
)


def test_refresh_database_markers_use_real_pod_backend_and_runtime_settings(refresh_database, monkeypatch):
    from scripts.ops.nebius_pool_migration_guard import KubectlPoolGuardAPI

    api, state, bound, pool, calls = refresh_database
    retained = copy.deepcopy(state.target.database)
    observed = copy.deepcopy((state.database, state.service, state.pod, state.endpoints))
    history = {path: path.read_bytes() for path in pool.history}
    assert api._database(state.target) == state.pod
    connected = KubectlPoolGuardAPI(request=api.request, kubeconfig=api.kubeconfig,
        executable=Path(api.prefix[0]), refresh=bound)
    monkeypatch.setattr(connected, '_run', api._run)
    connected.qualify_runtime_database(state.target, original=state.original, expected=copy.deepcopy(state.controller),
        credential_uid=state.credential[0], credential_resource_version=state.credential[1])
    assert len(state.processes) == 1 and state.processes[0].stdout == b'{"status": "qualified"}\n'
    assert state.target.database == retained
    assert (state.database, state.service, state.pod, state.endpoints) == observed
    assert {path: path.read_bytes() for path in history} == history
    assert not bound.state_dir.exists()
    assert all(args[0] == 'get' or (args[0] == 'exec' and args[7:9] == ['python', '-c']) for args in calls)


def test_refresh_database_marker_projection_keeps_strict_guard_boundaries(refresh_database, monkeypatch):
    from scripts.ops.nebius_pool_migration import PoolMigrationError

    api, state, bound, pool, calls = refresh_database
    observed = [state.database, state.service, state.pod, state.secret, state.endpoints, state.runtime_pod]
    snapshots = [copy.deepcopy(row) for row in observed]
    retained = copy.deepcopy(state.target.database)
    selected = copy.deepcopy(bound.successor.resources)
    history = {path: path.read_bytes() for path in pool.history}
    revision = 'loom.nebius/configuration-revision'
    transport = api._run
    damage = None
    def read(args):
        result = transport(args)
        if args[0] == 'exec':
            if damage == 'late_revision':
                state.database['spec']['template']['metadata']['annotations'][revision] += '-drift'
            elif damage == 'late_pod':
                state.runtime_pod['metadata']['uid'] = 'e8bd7eef-f5c5-4e91-bc51-cbb54982937d'
            elif damage == 'late_credential':
                state.secret['metadata']['resourceVersion'] = '999'
        return result
    monkeypatch.setattr(api, '_run', read)
    for damage in ('no_refresh', 'no_ordinary', 'database_uid', 'source_revision', 'unknown_annotation',
            'unknown_template_annotation', 'database_image', 'pvc', 'service_uid', 'service_selector',
            'service_port', 'service_allocation', 'source_database_image', 'source_service_port',
            'secret_uid', 'secret_version', 'backend_owner', 'backend_target',
            'backend_address', 'late_revision', 'late_pod', 'late_credential'):
        for row, saved in zip(observed, snapshots, strict=True):
            row.clear()
            row.update(copy.deepcopy(saved))
        state.commands.clear()
        state.processes.clear()
        api.refresh = bound
        if damage == 'no_refresh':
            api.refresh = None
        elif damage == 'no_ordinary':
            api.refresh = replace(bound, successor=None)
        elif damage == 'database_uid':
            state.database['metadata']['uid'] = '22d0e5b5-99a6-4e55-8127-5459e9bf7cd8'
        elif damage == 'source_revision':
            state.database['spec']['template']['metadata']['annotations'][revision] += '-drift'
        elif damage == 'unknown_annotation':
            state.database['metadata']['annotations']['unrelated'] = 'drift'
        elif damage == 'unknown_template_annotation':
            state.database['spec']['template']['metadata']['annotations']['unrelated'] = 'drift'
        elif damage == 'database_image':
            state.database['spec']['template']['spec']['containers'][0]['image'] += '-drift'
        elif damage == 'pvc':
            state.database['spec']['volumeClaimTemplates'][0]['spec']['storageClassName'] = 'foreign'
        elif damage == 'service_uid':
            state.service['metadata']['uid'] = '708cf812-b95b-47eb-a9c9-c3e0fb023ec8'
        elif damage == 'service_selector':
            state.service['spec']['selector'] = {'app': 'foreign'}
        elif damage == 'service_port':
            state.service['spec']['ports'][0]['targetPort'] = 5433
        elif damage == 'service_allocation':
            state.service['spec']['clusterIP'] = '10.96.0.99'
        elif damage in {'source_database_image', 'source_service_port'}:
            resources = copy.deepcopy(bound.successor.resources)
            namespace = state.target.namespace
            if damage == 'source_database_image':
                resources['StatefulSet:' + namespace + ':loom-postgres']['spec']['template']['spec']['containers'][0]['image'] += '-drift'
            else:
                resources['Service:' + namespace + ':loom-postgres']['spec']['ports'][0]['targetPort'] = 5433
            api.refresh = replace(bound, successor=replace(bound.successor, resources=resources))
        elif damage == 'secret_uid':
            state.secret['metadata']['uid'] = '31a8e991-7665-4102-8d97-ce290123552b'
        elif damage == 'secret_version':
            state.secret['metadata']['resourceVersion'] = '999'
        elif damage == 'backend_owner':
            state.endpoints['items'][0]['metadata']['ownerReferences'][0]['uid'] = '76947d04-e08f-4466-a215-279fd6988472'
        elif damage == 'backend_target':
            state.endpoints['items'][0]['endpoints'][0]['targetRef']['uid'] = 'a2bb56bb-58a1-4376-a15d-1e20be2d59f4'
        elif damage == 'backend_address':
            state.endpoints['items'][0]['endpoints'][0]['addresses'] = ['10.20.0.3']
        with pytest.raises(PoolMigrationError) as error:
            api.qualify_runtime_database(state.target, original=state.original, expected=copy.deepcopy(state.controller),
                credential_uid=state.credential[0], credential_resource_version=state.credential[1])
        assert error.value.stage == 'runtime_database', damage
        assert len(state.commands) == (1 if damage.startswith('late_') else 0), damage
        assert state.target.database == retained
    assert {path: path.read_bytes() for path in history} == history
    assert bound.successor.resources == selected
    assert not bound.state_dir.exists()
    assert all(args[0] == 'get' or (args[0] == 'exec' and args[7:9] == ['python', '-c']) for args in calls)


def test_refresh_database_projection_leaves_nonprimary_binding_exact(refresh_database):
    _, state, bound, _, _ = refresh_database
    original, current, service = [copy.deepcopy(row) for row in (
        state.target.database.statefulset, state.database, state.service)]
    for row in (original, current, service):
        row['metadata']['namespace'] = 'other-retained-participant'
    before = copy.deepcopy((original, current, service))
    observed, expected = bound.database_documents(original=original, current=current, service=service)
    assert observed == current and expected == original
    assert (original, current, service) == before


@pytest.mark.timeout(420)
@pytest.mark.parametrize(('ordinary_rollout', 'private_cutover', 'target_revision'), [(None, None, '0177'),
    ('NETWORK_SSD_NON_REPLICATED', None, '0177'),
    ({'task_image_builder': {'max_concurrent': None}}, None, '0177'),
    ({'execution_disk_type': 'NETWORK_SSD_NON_REPLICATED', 'task_image_builder': {'max_concurrent': None}}, None, '0177'),
    (None, {'kubectl.kubernetes.io/restartedAt': '2026-10-01T08:30:00Z'}, '0177'),
    (None, {'kubectl.kubernetes.io/restartedAt': '2026-10-01T08:30:00Z', 'test-historical-unrelated': 'retained'}, '0177'),
    (None, {'kubectl.kubernetes.io/restartedAt': '2026-10-01T08:30:00Z'}, '0178')],
    ids=['ssd', 'nrd', 'resource-only-builder', 'nrd-resource-only-builder', 'retained-service-restart',
        'reject-historical-unrelated', 'retained-service-restart-schema178'],
    indirect=['ordinary_rollout', 'private_cutover'])
def test_completed_ordinary_successor_preflight_is_read_only_and_rejects_drift(
        private_cutover, ordinary_rollout, connected_refresh, monkeypatch, target_revision):
    import asyncio
    from contextlib import contextmanager

    from scripts.ops import nebius_management_refresh_entry as refresh_entry
    from scripts.ops import nebius_pool_cutover_entry as cutover_entry
    from scripts.ops.nebius_pool_migration_guard import KubectlPoolGuardAPI
    from scripts.ops.nebius_pool_predecessor import PoolPredecessorV1, load_completed_pool
    from scripts.ops.nebius_pool_refresh import PoolManagerRefresh
    from tests.ops.test_nebius_management_refresh_entry import context, private_refresh
    from tests.ops.test_nebius_pool_cutover_entry import save_private

    operation, _, root = private_cutover
    _, result = finish_cutover(operation, legacy=True)
    pool = load_completed_pool(PoolPredecessorV1(operation=operation,
        completion_sha256=result['completion_sha256']), original=root)
    successor = asyncio.run(ordinary_rollout[0]())
    metadata, payload, _ = private_refresh(root, pool)
    payload.update(manager_revision='0174', target_manager_revision=target_revision)
    payload['platform_rollout'] = successor.reference.model_dump(mode='json')
    payload['deployment']['installation']['foundation']['platform_config_json'] = json.dumps(successor.config)
    save_private(metadata, payload)
    selected_context = context(metadata)
    request, state = selected_context.request, Path(metadata['state_dir'])
    if (successor.config.get('execution_disk_type') == 'NETWORK_SSD_NON_REPLICATED'
            or successor.config['task_image_builder']['max_concurrent'] is None):
        # Valid disk/builder references grant no refresh authority without the
        # existing completed ordinary-rollout binding, including builder-only changes.
        unbound = copy.deepcopy(payload)
        unbound.pop('platform_rollout')
        save_private(metadata, unbound)
        with pytest.raises(refresh_entry.EntryError):
            context(metadata)
        with pytest.raises(ValueError, match='pool_refresh_projection_unqualified'):
            PoolManagerRefresh(root, pool, replace(request, platform_rollout=None), state).qualify()
        foreign = copy.deepcopy(payload)
        foundation = foreign['deployment']['installation']['foundation']
        config = json.loads(foundation['platform_config_json'])
        config['execution_disk_type'] = 'NETWORK_HDD'
        foundation['platform_config_json'] = json.dumps(config)
        save_private(metadata, foreign)
        with pytest.raises(refresh_entry.EntryError):
            context(metadata)
        save_private(metadata, payload)
    bound = PoolManagerRefresh(root, pool, request, state, successor=successor)
    original_history = {path: path.read_bytes() for path in pool.history}
    selected_resources = copy.deepcopy(successor.resources)
    with pool_refresh_http(bound)() as (verifier, external):
        service_key = 'Deployment:' + successor.config['namespace'] + ':loom-service'
        original_service, = (row for row in pool.context.request.services if _key(row) == service_key)
        restart = 'kubectl.kubernetes.io/restartedAt'
        frozen = original_service['spec']['template']['metadata'].get('annotations', {}).get(restart)
        retained_service = copy.deepcopy(original_service)
        if frozen is not None:
            assert pool.completion.workloads[service_key]['spec']['template']['metadata']['annotations'][restart] == frozen
            assert restart not in successor.resources[service_key]['spec']['template']['metadata'].get('annotations', {})
            # Model the independent current GET, not a live object synthesized
            # from the implementation's projected workload options.
            external.objects[service_key]['spec']['template']['metadata'].setdefault('annotations', {})[restart] = frozen
        unrelated = 'test-historical-unrelated'
        historical_unknown = original_service['spec']['template']['metadata'].get('annotations', {}).get(unrelated)
        if historical_unknown is not None:
            assert unrelated not in successor.resources[service_key]['spec']['template']['metadata'].get('annotations', {})
            external.objects[service_key]['spec']['template']['metadata'].setdefault('annotations', {})[unrelated] = historical_unknown
        before = copy.deepcopy(external.objects)
        dormant = pool.context.request.fencing.retirement.dormant_consumers[0]
        for original in (dormant.actuator, dormant.collector):
            assert _key(original) not in successor.resources
            assert bound.successor_document(pool.completion.workloads[_key(original)]) == pool.completion.workloads[_key(original)]
            assert external.objects[_key(original)]['spec'].get('replicas', 0) == 0
            if original['kind'] == 'CronJob':
                assert external.objects[_key(original)]['spec']['suspend'] is True
        # Run the actual entry runtime qualifier with fixed reader credentials;
        # only remote Pod/SQL/telemetry I/O is doubled.
        guards = object.__new__(KubectlPoolGuardAPI)
        guards.request = pool.context.request.fencing.retirement.migration
        guards._get = lambda _kind, name, namespace: external.objects['Deployment:' + namespace + ':' + name]
        runtime_checks, telemetry_checks = [], []
        def database_probe(target, *, original, expected, credential_uid, credential_resource_version):
            assert expected == external.objects[_key(original)]
            binding = target.database
            actuator = original['metadata']['namespace'] != target.namespace
            assert (credential_uid, credential_resource_version) == ((binding.actuator_credential_uid,
                binding.actuator_credential_resource_version) if actuator else
                (binding.credential_uid, binding.credential_resource_version))
            runtime_checks.append(_key(original))
        def telemetry_probe(target, *, original, expected):
            assert expected == external.objects[_key(original)]
            telemetry_checks.append(_key(original))
        guards.qualify_runtime_database, guards.qualify_runtime_telemetry = database_probe, telemetry_probe
        if historical_unknown is not None:
            # The exception must retain only the known frozen restart stamp,
            # not copy every historical annotation into current source output.
            with pytest.raises(cutover_entry.EntryError, match='pool cutover runtime databases unqualified'):
                cutover_entry.qualify_pool_runtime_databases(pool.context, guards, refresh=bound)
            assert external.objects == before
            assert original_service == retained_service and successor.resources == selected_resources
            assert {path: path.read_bytes() for path in original_history} == original_history
            assert not state.exists() and not Path(metadata['anchor_dir']).exists()
            return
        cutover_entry.qualify_pool_runtime_databases(pool.context, guards, refresh=bound)
        assert len(runtime_checks) == len(guards.request.guards) * 3
        assert len(telemetry_checks) == len(guards.request.guards)
        actual_service = external.objects[service_key]
        damages = ('changed', 'removed', 'unknown') if frozen is not None else ('new', 'unknown')
        strict = replace(bound, successor=None)
        assert strict.matches_workload(original_service, original_service, original_service)
        for damage in damages:
            saved = copy.deepcopy(actual_service)
            strict_actual = copy.deepcopy(original_service)
            for document in (actual_service, strict_actual):
                annotations = document['spec']['template']['metadata'].setdefault('annotations', {})
                if damage == 'removed':
                    annotations.pop(restart)
                elif damage == 'unknown':
                    annotations[unrelated] = 'live-drift'
                else:
                    annotations[restart] = '2026-10-02T08:30:00Z'
            # These are current GET changes only; the original and completion
            # remain sealed. Both real entry and the no-successor matcher deny.
            assert not strict.matches_workload(strict_actual, original_service, original_service), damage
            with pytest.raises(cutover_entry.EntryError, match='pool cutover runtime databases unqualified'):
                cutover_entry.qualify_pool_runtime_databases(pool.context, guards, refresh=bound)
            actual_service.clear()
            actual_service.update(saved)
        cutover_entry.qualify_pool_runtime_databases(pool.context, guards, refresh=bound)
        assert external.objects == before
        assert original_service == retained_service and successor.resources == selected_resources
        assert {path: path.read_bytes() for path in original_history} == original_history
        assert not state.exists() and not Path(metadata['anchor_dir']).exists()
        verifier.qualify()
        external.failure = 'manager_schema'
        with pytest.raises(ValueError):
            verifier.qualify()
        external.failure = None
        old_api, live = connected_refresh
        live.request, live.directory, live.anchor, live.operation = request, state, Path(metadata['anchor_dir']), metadata
        live.manager.clear()
        live.manager.update(copy.deepcopy(pool.active))
        live.manager['metadata'].update(resourceVersion='30', generation=5)
        live.values.update(external.objects)
        live.values[_key(pool.active)] = live.manager
        external.objects = live.values
        connections = []
        @contextmanager
        def connect(inputs, ingress, *, foundation_candidate):
            assert inputs == root.original_inputs and ingress == root.ingress
            assert foundation_candidate == selected_context.inputs.foundation_candidate
            yield old_api.checks, old_api.ssl_context, old_api.token
        @contextmanager
        def reader(actual, *, refresh):
            assert actual == pool.context and refresh == bound
            connections.append(refresh)
            yield verifier.parent
        async def publication(actual, baseline):
            assert actual == selected_context and baseline == pool
            return successor
        trust = ssl.create_default_context(cafile=certifi.where())
        certificate = ssl.DER_cert_to_PEM_cert(trust.get_ca_certs(binary_form=True)[0]).encode()
        def ca(path, limit):
            # The old install fixture stores the literal 'fixture-ca'. Supply
            # valid read-only TLS trust at this external certificate boundary.
            assert path == root.original_inputs.operator_connection.ca_file
            return certificate
        monkeypatch.setattr(refresh_entry, '_private', ca)
        monkeypatch.setattr(refresh_entry, 'connected_checks', connect)
        monkeypatch.setattr(refresh_entry, 'ApplicationUpgradePrerequisites', lambda **_: old_api.checks)
        monkeypatch.setattr(refresh_entry, '_platform_successor', publication)
        monkeypatch.setattr(cutover_entry, 'connected_pool_api', reader)
        before = copy.deepcopy(external.objects)
        try:
            report = refresh_entry.execute_refresh(selected_context, metadata, 'preflight')
        except Exception as error:
            pytest.fail('owning preflight stage: ' + str(getattr(error, 'stage', type(error).__name__)))
        assert report['status'] == 'preflight_qualified'
        assert len(connections) == 1
        assert all(call.method == 'GET' for call in live.calls)
        assert external.objects == before
        assert original_service == retained_service and successor.resources == selected_resources
        assert {path: path.read_bytes() for path in original_history} == original_history
        assert not state.exists()
        assert all(call.method == 'GET' or call.url.path == '/apis/authorization.k8s.io/v1/selfsubjectrulesreviews'
            for call in external.calls)
        key = 'Deployment:' + successor.config['namespace'] + ':loom-service'
        actual = external.objects[key]
        for damage in ('image', 'env', 'uid', 'destination'):
            saved = copy.deepcopy(actual)
            if damage == 'image':
                actual['spec']['template']['spec']['containers'][0]['image'] += '-unrelated'
            elif damage == 'env':
                actual['spec']['template']['spec']['containers'][0]['env'].append({'name': 'UNRELATED', 'value': 'yes'})
            elif damage == 'uid':
                actual['metadata']['uid'] = 'foreign'
            else:
                actual['metadata']['namespace'] = 'foreign'
            with pytest.raises(ValueError):
                verifier.qualify()
            actual.clear()
            actual.update(saved)
        verifier.qualify()
        assert external.objects == before
