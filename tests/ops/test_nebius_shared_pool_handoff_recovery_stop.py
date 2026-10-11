"""Shared handoff recovery fences lifecycle regressions."""
from __future__ import annotations

from tests.ops.test_nebius_shared_pool_handoff import (
    Path as Path,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    SharedPhaseAPI as SharedPhaseAPI,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    _key as _key,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    _shared_recovery_database_io as _shared_recovery_database_io,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    application_management_inputs as application_management_inputs,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    application_material as application_material,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    build_inputs as build_inputs,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    capacity_checks as capacity_checks,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    cloud as cloud,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    collector_cloud as collector_cloud,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    completed_pool as completed_pool,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    connected as connected,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    copy as copy,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    dev_pool_inputs as dev_pool_inputs,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    development_inputs as development_inputs,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    entry as entry,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    handoff as handoff,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    installation as installation,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    inventory as inventory,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    live as live,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    management_inputs as management_inputs,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    manager_entry as manager_entry,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    material as material,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    original_cloud as original_cloud,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    original_development_inputs as original_development_inputs,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    original_manager_entry as original_manager_entry,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    original_platform_inputs as original_platform_inputs,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    original_pool_inputs as original_pool_inputs,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    platform_inputs as platform_inputs,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    pool_entry as pool_entry,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    pool_inputs as pool_inputs,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    preflight as preflight,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    provider_checks as provider_checks,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    publication as publication,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    published_source as published_source,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    publisher_cloud as publisher_cloud,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    pytest as pytest,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    pytestmark as pytestmark,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    retained as retained,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    route as route,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    shared_guard_transport as shared_guard_transport,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    shared_operation as shared_operation,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    shared_runtime as shared_runtime,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    source_checkout as source_checkout,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    staging_history as staging_history,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    staging_observation as staging_observation,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    tls_material as tls_material,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    uuid4 as uuid4,
)


@pytest.mark.timeout(900)
@pytest.mark.parametrize('forward_commit', [False, True])
def test_shared_recovery_stop_fences_delayed_forward_cas_without_replaying_unknown_stops(
        shared_operation, shared_guard_transport, shared_runtime, completed_pool, handoff, monkeypatch, forward_commit):
    """Actual forward/recovery children and transports; only external I/O doubled."""
    import importlib.util
    import json
    import ssl
    from contextlib import ExitStack

    import httpx
    from scripts.ops import nebius_certificates as private_state
    from scripts.ops import nebius_shared_pool_guards as guard_child
    from scripts.ops import nebius_shared_pool_operation as parent
    from scripts.ops import nebius_shared_pool_recovery_drain as drain
    from scripts.ops import nebius_shared_pool_recovery_fence as fence
    from scripts.ops import nebius_shared_pool_workloads as forward
    from scripts.ops.nebius_development_runtime_install import _read
    from scripts.ops.nebius_development_runtime_live import runtime_resource_path
    from scripts.ops.nebius_shared_pool_recovery_fence_live import SharedPoolRecoveryFenceTransport
    from tests.ops.test_nebius_development_pool_retained import populate_runtime_pods

    name = 'scripts.ops.nebius_shared_pool_recovery_workloads'
    assert importlib.util.find_spec(name) is not None, 'connected terminal shared workload stop is missing'
    stop = __import__(name, fromlist=['HTTPSSharedRecoveryWorkloads'])
    assert callable(getattr(stop, 'observe_shared_recovery_workload_drain', None)), 'dependent recovery needs current process drain'
    args, bindings = shared_operation
    runtime = args['runtime']
    completed = bindings.plan.development.runtime
    manager = runtime.database.manager.retained.request.retained
    state = Path(manager.operation['state_dir']).parent / 'shared-pool-handoff'
    child = state / 'recovery-stop'
    roster = tuple(str(target.participant_id) for target in bindings.migration.guards)
    roots = dict(bindings.plan.staging.originals)
    roots.update({key: completed.resources[key] for key in (*completed.plan.originals, *completed.plan.fixed['workloads'])})
    roots[_key(bindings.plan.development.gateway_original)] = bindings.plan.development.gateway_original
    rows, documents, writes = copy.deepcopy(roots), {}, []
    mode = {'forward': True, 'forward_delayed': None, 'forward_fault': True, 'busy_preview': True,
        'recovery_count': 0, 'deferred': None, 'lose_read': False, 'retain_pod': False}
    forward_phases = {'forward-stop', 'forward-wire', 'forward-start'}

    class Remote:
        mode = 'closed'

        def __init__(self):
            self.local = dict.fromkeys(roster, 'held')
            self.reads = []
            self.pool_counts = dict.fromkeys(('unstarted_requests', 'active_requests', 'unconfirmed_creates', 'unqualified_releases'), 0)
            self.local_counts = {key: dict.fromkeys(('trials', 'executions', 'builds', 'build_cleanup',
                'execution_outboxes', 'build_outboxes'), 0) for key in roster}

        def verify_retained(self):
            pass  # Full writer/provider composition is a separate required parent boundary.

        def pool_state(self):
            return self.mode

        def guard_state(self, participant):
            return self.local[participant]

        def fence_pool(self):
            self.mode = 'fenced'

        def fence_guard(self, participant):
            self.local[participant] = 'fenced'

    remote = Remote()
    guard_api, database_model = shared_guard_transport
    _shared_recovery_database_io(remote=remote, bindings=bindings, guard_api=guard_api, database_model=database_model,
        shared_runtime=shared_runtime, completed_pool=completed_pool, handoff=handoff, monkeypatch=monkeypatch)

    def refresh():
        documents.clear()
        for row in rows.values():
            meta = row['metadata']
            meta.setdefault('generation', 1)
            meta.setdefault('resourceVersion', '1')
            if row['kind'] == 'Deployment':
                row['status'] = {'observedGeneration': meta['generation'], **{field: row['spec']['replicas']
                    for field in ('replicas', 'readyReplicas', 'updatedReplicas', 'availableReplicas')}}
            documents[runtime_resource_path(row) + '/' + meta['name']] = copy.deepcopy(row)
            for version, resource, kind in (('apps/v1', 'replicasets', 'ReplicaSet'), ('v1', 'pods', 'Pod'), ('batch/v1', 'jobs', 'Job')):
                prefix = '/api/v1' if version == 'v1' else '/apis/' + version
                documents.setdefault(prefix + '/namespaces/' + meta['namespace'] + '/' + resource,
                    {'apiVersion': version, 'kind': kind + 'List', 'metadata': {'resourceVersion': '1'}, 'items': []})
        populate_runtime_pods(documents)

    def apply(path, patches, *, persist):
        current = copy.deepcopy(documents[path])
        key = _key(current)
        assert patches[0] == {'op': 'test', 'path': '/metadata/uid', 'value': roots[key]['metadata']['uid']}
        assert patches[1]['op'] == 'test' and patches[1]['path'] == '/metadata/resourceVersion'
        for change in patches:
            parts = change['path'].strip('/').split('/')
            target = current
            for field in parts[:-1]:
                target = target[field]
            field = parts[-1]
            if change['op'] == 'test':
                if target[field] != change['value']:
                    return httpx.Response(409, json={'apiVersion': 'v1', 'kind': 'Status', 'status': 'Failure',
                        'code': 409, 'reason': 'Conflict'})
            elif change['op'] == 'remove':
                del target[field]
            else:
                assert change['op'] in {'add', 'replace'}
                target[field] = change['value']
        if persist:
            current['metadata']['resourceVersion'] = str(int(current['metadata']['resourceVersion']) + 1)
            current['metadata']['generation'] += 1
            rows[key] = current
            writes.append(key)
            refresh()
        return httpx.Response(200, json=current)

    def handle(message):
        path = message.url.path
        assert path in documents, 'no database, backup or unrelated workload belongs to recovery stop'
        if message.method == 'GET':
            if mode['lose_read']:
                mode['lose_read'] = False
                raise httpx.ReadError('simulated unavailable stop readback')
            value = copy.deepcopy(documents[path])
            if mode['retain_pod'] and path == '/api/v1/namespaces/' + retained_pod['metadata']['namespace'] + '/pods':
                value['items'].append(copy.deepcopy(retained_pod))
            return httpx.Response(200, json=value)
        assert message.method == 'PATCH'
        patches = json.loads(message.content)
        if message.url.params.get('dryRun') == 'All':
            if not mode['forward'] and mode['busy_preview']:
                mode['busy_preview'] = False
                remote.pool_counts['active_requests'] = 1
            return apply(path, patches, persist=False)
        assert not message.url.query
        if mode['forward']:
            is_start = any(change.get('value', {}).get('replicas') == 1
                for change in patches if change['path'] == '/spec' and change['op'] == 'replace')
            if is_start and mode['forward_fault']:
                mode['forward_fault'] = False
                mode['forward_delayed'] = (path, patches)
                if forward_commit:
                    apply(path, patches, persist=True)
                    mode['lose_read'] = True
                raise httpx.ReadError('simulated uncertain forward start')
        else:
            assert remote.mode == 'fenced' and set(remote.local.values()) == {'fenced'}
            assert not any(remote.pool_counts.values())
            key = _key(documents[path])
            assert _read(child / 'stop.json')['resources'][key]['status'] == 'intent'
            mode['recovery_count'] += 1
            if mode['recovery_count'] == 2:
                mode['deferred'] = (path, patches)
                raise httpx.ReadError('simulated uncommitted stop reply loss')
            result = apply(path, patches, persist=True)
            if mode['recovery_count'] == 1:
                mode['lose_read'] = True
                raise httpx.ReadError('simulated committed stop reply loss')
            return result
        return apply(path, patches, persist=True)

    refresh()
    retained_pod = copy.deepcopy(next(pod for collection in documents.values() if collection['kind'] == 'PodList'
        for pod in collection['items']))
    with ExitStack() as stack:
        class API(SharedPhaseAPI):
            fence_api = None
            stop_api = None

            def prepare(self, phase, bound, directory):
                if phase == 'forward-guards':
                    guard_child.prepare_shared_pool_guards(bindings=bound, state_dir=directory)
                elif phase in forward_phases:
                    forward.prepare_shared_staging_workloads(bindings=bound, phase=phase, state_dir=directory)
                elif phase == 'recovery-fence':
                    fence.prepare_shared_recovery_fence(bindings=bound, state_dir=directory)
                elif phase == 'recovery-drain':
                    drain.prepare_shared_recovery_drain(bindings=bound, state_dir=directory)
                elif phase == 'recovery-stop':
                    stop.prepare_shared_recovery_workloads(bindings=bound, state_dir=directory)
                else:
                    super().prepare(phase, bound, directory)

            def validate_prepared_phase(self, phase, directory):
                if phase == 'recovery-stop':
                    stop.validate_shared_recovery_workloads(bindings=bindings, state_dir=directory, complete=False, prepared=True)
                elif phase in {'forward-guards', *forward_phases, 'recovery-fence', 'recovery-drain'}:
                    self.validate_phase(phase, directory, complete=False)
                else:
                    super().validate_prepared_phase(phase, directory)

            def validate_phase(self, phase, directory, *, complete):
                if phase == 'forward-guards':
                    guard_child.validate_shared_pool_guards(bindings=bindings, state_dir=directory, complete=complete)
                elif phase in forward_phases:
                    forward.validate_shared_staging_workloads(bindings=bindings, phase=phase, state_dir=directory, complete=complete)
                elif phase == 'recovery-fence':
                    fence.validate_shared_recovery_fence(bindings=bindings, state_dir=directory, complete=complete)
                elif phase == 'recovery-drain':
                    drain.validate_shared_recovery_drain(bindings=bindings, state_dir=directory, complete=complete)
                elif phase == 'recovery-stop':
                    stop.validate_shared_recovery_workloads(bindings=bindings, state_dir=directory, complete=complete)
                else:
                    super().validate_phase(phase, directory, complete=complete)

            def advance(self, phase, bound, directory):
                if phase == 'forward-guards':
                    path = directory / 'guards.json'
                    record = _read(path)
                    record['guards'].update(dict.fromkeys(roster, 'held'))
                    private_state._atomic_json(path, record)
                    return True
                if phase == 'recovery-fence':
                    if self.fence_api is None:
                        self.fence_api = SharedPoolRecoveryFenceTransport(bindings=bound, kubeconfig=guard_api.kubeconfig,
                            executable=Path(guard_api.prefix[0]), qualify=remote.verify_retained)
                    return fence.advance_shared_recovery_fence(bindings=bound, api=self.fence_api, state_dir=directory)
                if phase == 'recovery-drain':
                    return drain.advance_shared_recovery_drain(bindings=bound, guards=self.fence_api.guards,
                        history=self.fence_api.history, state_dir=directory, qualify=remote.verify_retained)
                if phase not in {*forward_phases, 'recovery-stop'}:
                    return super().advance(phase, bound, directory)
                def qualify(request, selected, writing):
                    assert request == runtime and selected == phase
                def drained():
                    return drain.observe_shared_recovery_drain(bindings=bound, guards=self.fence_api.guards,
                        history=self.fence_api.history, state_dir=state, qualify=remote.verify_retained)
                options = {'bindings': bound, 'api_server': runtime.database.foundation.inputs.config['kubernetes_api_server'],
                    'ssl_context': ssl.create_default_context(), 'qualify_runtime': qualify,
                    'observe_ready': lambda *_: True}
                connection = stack.enter_context(stop.HTTPSSharedRecoveryWorkloads(**options, recovery_drained=drained)
                    if phase == 'recovery-stop' else forward.HTTPSSharedStagingWorkloads(**options, phase=phase))
                connection_type = type(connection.client)
                connection.client.close()
                connection.client = connection_type(base_url=connection.api_server, transport=httpx.MockTransport(handle))
                if phase == 'recovery-stop':
                    self.stop_api = connection
                    return stop.advance_shared_recovery_workloads(bindings=bound, api=connection, state_dir=directory)
                return forward.advance_shared_staging_workloads(bindings=bound, phase=phase, api=connection, state_dir=directory)

        api = API(parent)
        api.block = 'recovery-revoke'
        kwargs = {**args, 'bindings': bindings, 'api': api, 'action': 'install'}
        with pytest.raises(ValueError):
            parent.run_shared_pool_operation(**kwargs)
        assert mode['forward_delayed'] is not None
        assert _read(state / 'operation.json')['forward']['forward-start']['status'] == 'started'
        frozen = {path: path.read_bytes() for path in state.glob('forward-*/*') if path.is_file()}
        mode['forward'] = False
        kwargs['action'] = 'recover'
        before = list(writes)
        assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'recovery-stop'
        assert writes == before
        assert all(row['status'] == 'prepared' for row in _read(child / 'stop.json')['resources'].values())
        remote.pool_counts['active_requests'] = 0
        # Missing/byte-altered frozen evidence must never be reconstructed by
        # recovery, even when its decoded JSON would still be equivalent.
        receipt = state / 'forward-start' / 'transition.json'
        retained = receipt.read_bytes()
        displaced = receipt.with_suffix('.held')
        receipt.rename(displaced)
        with pytest.raises(ValueError):
            parent.run_shared_pool_operation(**kwargs)
        assert writes == before
        displaced.rename(receipt)
        receipt.write_bytes(retained + b'\n')
        with pytest.raises(ValueError):
            parent.run_shared_pool_operation(**kwargs)
        assert writes == before
        receipt.write_bytes(retained)
        # A later root drifting must prevent any earlier root from being
        # stopped, not merely fail when iteration eventually reaches it.
        last = next(reversed(roots))
        original = copy.deepcopy(rows[last])
        for changed in ('uid', 'template'):
            if changed == 'uid':
                rows[last]['metadata']['uid'] = str(uuid4())
            else:
                rows[last]['spec']['template']['spec']['containers'][0]['image'] = 'unqualified.invalid/replacement:latest'
            refresh()
            with pytest.raises(ValueError):
                parent.run_shared_pool_operation(**kwargs)
            assert writes == before
            rows[last] = copy.deepcopy(original)
            refresh()
        with pytest.raises(ValueError):
            parent.run_shared_pool_operation(**kwargs)
        assert len(writes) == len(before) + 1
        assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'recovery-stop'
        assert mode['deferred'] is not None and mode['recovery_count'] == 2
        assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'recovery-stop'
        assert mode['recovery_count'] == 2  # Never replay the unresolved stop.
        assert apply(*mode['deferred'], persist=True).status_code == 200
        mode['retain_pod'] = True
        assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'recovery-stop'
        mode['retain_pod'] = False
        assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'recovery-revoke'
        assert set(writes[len(before):]) == set(roots)
        assert all(writes[len(before):].count(key) == 1 for key in roots)
        for row in rows.values():
            assert row['spec']['suspend' if row['kind'] == 'CronJob' else 'replicas'] == (True if row['kind'] == 'CronJob' else 0)
            assert row['metadata']['annotations'][stop.MARKER] == str(bindings.migration.registration.spec.operation_id)
        assert apply(*mode['forward_delayed'], persist=True).status_code == 409
        assert all(path.read_bytes() == raw for path, raw in frozen.items())
        stop.validate_shared_recovery_workloads(bindings=bindings, state_dir=child, complete=True)
        before = list(writes)
        assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'recovery-revoke'
        assert writes == before
        # A completed receipt proves past shutdown only. The following revoke
        # phase needs the same actual HTTP descendant checks and fresh SQL drain.
        def current_drain():
            return stop.observe_shared_recovery_workload_drain(bindings=bindings, api=api.stop_api, state_dir=child)
        assert current_drain() is True
        remote.pool_counts['active_requests'] = 1
        assert current_drain() is False
        remote.pool_counts['active_requests'] = 0
        mode['retain_pod'] = True
        assert current_drain() is False
        mode['retain_pod'] = False
        assert current_drain() is True
        with pytest.raises(ValueError):
            stop.advance_shared_recovery_workloads(bindings=bindings, api=api.stop_api, state_dir=child)
        assert writes == before
