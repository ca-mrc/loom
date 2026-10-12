"""Shared handoff recovery authority lifecycle regressions."""
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


@pytest.mark.timeout(600)
@pytest.mark.parametrize('committed', [False, True])
def test_shared_recovery_restores_current_templates_closed_without_replaying_unknown_writes(shared_operation, committed):
    """Real template HTTP/CAS/descendant reads; predecessor phase and SQL effects doubled."""
    import importlib.util
    import json
    import ssl
    from contextlib import ExitStack

    import httpx
    from scripts.ops import nebius_certificates as private_state
    from scripts.ops import nebius_shared_pool_gateway as forward_gateway
    from scripts.ops import nebius_shared_pool_guards as guards
    from scripts.ops import nebius_shared_pool_operation as parent
    from scripts.ops import nebius_shared_pool_recovery_drain as drain
    from scripts.ops import nebius_shared_pool_recovery_fence as fence
    from scripts.ops import nebius_shared_pool_recovery_gateway as gateway
    from scripts.ops import nebius_shared_pool_recovery_machines as machines
    from scripts.ops import nebius_shared_pool_recovery_workloads as stop
    from scripts.ops import nebius_shared_pool_workloads as staging
    from scripts.ops.nebius_development_runtime_install import _read
    from scripts.ops.nebius_development_runtime_live import runtime_resource_path
    from scripts.ops.nebius_ingress_stage import _snapshot, _uid
    from scripts.ops.nebius_management_stage import _comparison_snapshot
    from tests.ops.test_nebius_development_pool_retained import populate_runtime_pods

    name = 'scripts.ops.nebius_shared_pool_recovery_templates'
    assert importlib.util.find_spec(name) is not None, 'current-staging stopped template restoration is missing'
    templates = __import__(name, fromlist=['HTTPSSharedRecoveryTemplates'])
    args, bindings = shared_operation
    runtime = args['runtime']
    manager = runtime.database.manager.retained.request.retained
    state = Path(manager.operation['state_dir']).parent / 'shared-pool-handoff'
    child = state / 'recovery-restore-workloads'
    captured = copy.deepcopy(bindings.plan.staging.originals)
    rows, documents, effects = {}, {}, []
    mode = {'busy': False, 'busy_preview': True, 'retained_pod': False, 'pending': None, 'calls': 0}

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

    rows.update(copy.deepcopy(captured))
    refresh()
    retained_pod = copy.deepcopy(next(pod for collection in documents.values() if collection['kind'] == 'PodList'
        for pod in collection['items']))

    def apply(path, changes, *, persist):
        value = copy.deepcopy(documents[path])
        key = _key(value)
        assert key in captured, 'restoration cannot change new dev/global, database or backup roots'
        assert changes[0] == {'op': 'test', 'path': '/metadata/uid', 'value': _uid(captured[key])}
        assert changes[1]['op'] == 'test' and changes[1]['path'] == '/metadata/resourceVersion'
        for change in changes:
            fields = change['path'].strip('/').split('/')
            target = value
            for field in fields[:-1]:
                target = target[field]
            field = fields[-1]
            if change['op'] == 'test':
                if target[field] != change['value']:
                    return httpx.Response(409, json={'apiVersion': 'v1', 'kind': 'Status', 'status': 'Failure',
                        'code': 409, 'reason': 'Conflict'})
            elif change['op'] == 'remove':
                del target[field]
            else:
                assert change['op'] in {'add', 'replace'}
                target[field] = change['value']
        assert value['spec']['suspend'] is True if value['kind'] == 'CronJob' else value['spec']['replicas'] == 0
        if persist:
            value['metadata']['resourceVersion'] = str(int(value['metadata']['resourceVersion']) + 1)
            value['metadata']['generation'] += 1
            rows[key] = value
            effects.append(key)
            refresh()
        return httpx.Response(200, json=value)

    def handle(message):
        path = message.url.path
        assert path in documents
        if message.method == 'GET':
            value = copy.deepcopy(documents[path])
            if mode['retained_pod'] and path == '/api/v1/namespaces/' + retained_pod['metadata']['namespace'] + '/pods':
                value['items'].append(copy.deepcopy(retained_pod))
            return httpx.Response(200, json=value)
        assert message.method == 'PATCH'
        changes = json.loads(message.content)
        if message.url.params.get('dryRun') == 'All':
            if mode['busy_preview']:
                mode['busy_preview'], mode['busy'] = False, True
            return apply(path, changes, persist=False)
        assert not mode['busy'] and not mode['retained_pod']
        key = _key(documents[path])
        assert _read(child / 'templates.json')['resources'][key]['status'] == 'intent'
        mode['calls'] += 1
        if mode['calls'] == 1:
            mode['pending'] = path, changes
            if committed:
                apply(path, changes, persist=True)
            raise httpx.ReadError('simulated uncertain template restoration')
        return apply(path, changes, persist=True)

    children = {
        'recovery-fence': (fence.prepare_shared_recovery_fence, fence.validate_shared_recovery_fence, 'fence.json'),
        'recovery-drain': (drain.prepare_shared_recovery_drain, drain.validate_shared_recovery_drain, 'drain.json'),
        'recovery-stop': (stop.prepare_shared_recovery_workloads, stop.validate_shared_recovery_workloads, 'stop.json'),
        'recovery-restore-workloads': (templates.prepare_shared_recovery_templates, templates.validate_shared_recovery_templates, 'templates.json'),
    }
    workload_phases = {'forward-stop': 'stop', 'forward-wire': 'replace', 'forward-start': 'start'}
    with ExitStack() as stack:
        class API(SharedPhaseAPI):
            template_api = None

            def prepare(self, phase, bound, directory):
                if phase == 'forward-guards':
                    guards.prepare_shared_pool_guards(bindings=bound, state_dir=directory)
                elif phase in workload_phases:
                    staging.prepare_shared_staging_workloads(bindings=bound, phase=phase, state_dir=directory)
                elif phase == 'forward-gateway':
                    forward_gateway.prepare_shared_gateway(bindings=bound, state_dir=directory)
                elif phase in children:
                    children[phase][0](bindings=bound, state_dir=directory)
                elif phase == 'recovery-revoke':
                    machines.prepare_shared_recovery_machines(bindings=bound, state_dir=directory)
                    gateway.prepare_shared_recovery_gateway(bindings=bound, state_dir=directory)
                else:
                    super().prepare(phase, bound, directory)

            def validate_prepared_phase(self, phase, directory):
                if phase in children:
                    children[phase][1](bindings=bindings, state_dir=directory, complete=False, prepared=True)
                elif phase == 'recovery-revoke':
                    machines.validate_shared_recovery_machines(bindings=bindings, state_dir=directory, complete=False, prepared=True)
                    gateway.validate_shared_recovery_gateway(bindings=bindings, state_dir=directory, complete=False, prepared=True)
                elif phase in {'forward-guards', 'forward-gateway', *workload_phases}:
                    self.validate_phase(phase, directory, complete=False)
                else:
                    super().validate_prepared_phase(phase, directory)

            def validate_phase(self, phase, directory, *, complete):
                if phase == 'forward-guards':
                    guards.validate_shared_pool_guards(bindings=bindings, state_dir=directory, complete=complete)
                elif phase in workload_phases:
                    staging.validate_shared_staging_workloads(bindings=bindings, phase=phase, state_dir=directory, complete=complete)
                elif phase == 'forward-gateway':
                    forward_gateway.validate_shared_gateway(bindings=bindings, state_dir=directory, complete=complete)
                elif phase in children:
                    children[phase][1](bindings=bindings, state_dir=directory, complete=complete)
                elif phase == 'recovery-revoke':
                    machines.validate_shared_recovery_machines(bindings=bindings, state_dir=directory, complete=complete)
                    gateway.validate_shared_recovery_gateway(bindings=bindings, state_dir=directory, complete=complete)
                else:
                    super().validate_phase(phase, directory, complete=complete)

            def advance(self, phase, bound, directory):
                if phase == 'forward-gateway':
                    return False  # Recovery precedes any new gateway grant.
                if phase == 'forward-guards':
                    path = directory / 'guards.json'
                    record = _read(path)
                    record['guards'].update(dict.fromkeys(record['guards'], 'held'))
                    private_state._atomic_json(path, record)
                    return True
                if phase in workload_phases:
                    _, targets = staging._inputs(bound, state, phase, staging._frame(bound, state))
                    path = directory / 'transition.json'
                    record = _read(path)
                    for key, row in record['resources'].items():
                        row.update(status='applied', expected=_comparison_snapshot(targets[key]))
                    private_state._atomic_json(path, record)
                    return True
                if phase in children and phase != 'recovery-restore-workloads':
                    path = directory / children[phase][2]
                    record = _read(path)
                    if phase == 'recovery-fence':
                        record['pool'] = 'fenced'
                        record['guards'].update(dict.fromkeys(record['guards'], 'fenced'))
                    elif phase == 'recovery-drain':
                        record['status'] = 'observed'
                    else:
                        rows.clear()
                        for key, choices in stop._sources(bound, state).items():
                            before = _snapshot(choices[-1])
                            before['metadata']['uid'] = _uid(choices[-1])
                            expected = stop._stopped(bound, before)
                            record['resources'][key] = {'status': 'stopped', 'before': before,
                                'before_resource_version': '1', 'expected': expected}
                            rows[key] = {**copy.deepcopy(expected), 'metadata': {**expected['metadata'],
                                'uid': _uid(before), 'resourceVersion': '2'}}
                        refresh()
                    private_state._atomic_json(path, record)
                    return True
                if phase == 'recovery-revoke':
                    path = directory / 'machines.json'
                    private_state._atomic_json(path, {**_read(path), 'status': 'revoked'})
                    path = directory / 'gateway.json'
                    record = _read(path)
                    for row in record['roles'].values():
                        row['status'] = 'absent'
                    private_state._atomic_json(path, record)
                    return True
                if phase != 'recovery-restore-workloads':
                    return super().advance(phase, bound, directory)
                if self.template_api is None:
                    def qualify(request, selected, writing):
                        assert request == runtime and selected == phase
                    self.template_api = stack.enter_context(templates.HTTPSSharedRecoveryTemplates(bindings=bound,
                        api_server=runtime.database.foundation.inputs.config['kubernetes_api_server'],
                        ssl_context=ssl.create_default_context(), qualify_runtime=qualify,
                        recovery_drained=lambda: not mode['busy']))
                    connection_type = type(self.template_api.client)
                    self.template_api.client.close()
                    self.template_api.client = connection_type(base_url=self.template_api.api_server, transport=httpx.MockTransport(handle))
                return templates.advance_shared_recovery_templates(bindings=bound, api=self.template_api, state_dir=directory)

        api = API(parent)
        api.block = 'recovery-restore-roles'
        kwargs = {**args, 'bindings': bindings, 'api': api, 'action': 'install'}

        def run_operation():
            try:
                return parent.run_shared_pool_operation(**kwargs)
            except ValueError as exc:
                import traceback

                frames, cause = [], exc
                while cause is not None:
                    frames.extend(f'{Path(frame.filename).name}:{frame.lineno}:{frame.name}'
                        for frame in traceback.extract_tb(cause.__traceback__))
                    cause = cause.__context__
                raise ValueError('template failure at ' + ', '.join(frames)) from None

        assert run_operation()['phase'] == 'forward-gateway'
        frozen = {path: path.read_bytes() for path in state.glob('forward-*/**/*.json')}
        kwargs['action'] = 'recover'
        assert run_operation()['phase'] == 'recovery-restore-workloads'
        assert mode['calls'] == 0
        assert {row['status'] for row in _read(child / 'templates.json')['resources'].values()} == {'prepared'}
        untouched = {key: copy.deepcopy(row) for key, row in rows.items() if key not in captured}
        receipt = state / 'recovery-stop/stop.json'
        receipt_bytes = receipt.read_bytes()
        receipt.write_bytes(receipt_bytes + b'\n')
        try:
            with pytest.raises(ValueError):
                api.template_api.drained()
        finally:
            receipt.write_bytes(receipt_bytes)
        target_key = next(key for key in api.template_api.targets if captured[key]['kind'] == 'Deployment')
        target_before = copy.deepcopy(api.template_api.targets[target_key])
        api.template_api.targets[target_key]['spec']['replicas'] = 1
        try:
            with pytest.raises(ValueError):
                api.template_api.drained()
        finally:
            api.template_api.targets[target_key] = target_before
        assert mode['calls'] == 0
        mode['busy'], mode['retained_pod'] = False, True
        assert run_operation()['phase'] == 'recovery-restore-workloads'
        assert mode['calls'] == 0
        mode['retained_pod'] = False
        result = run_operation()
        assert mode['pending'] is not None
        if not committed:
            assert result['phase'] == 'recovery-restore-workloads'
            assert mode['calls'] == 1
            assert run_operation()['phase'] == 'recovery-restore-workloads' and mode['calls'] == 1
            assert apply(*mode['pending'], persist=True).status_code == 200
            result = run_operation()
        assert result['phase'] == 'recovery-restore-roles'
        assert effects and len(effects) == len(set(effects)) and set(effects) <= captured.keys()
        assert all(rows[key] == value for key, value in untouched.items())
        for key, before in captured.items():
            expected = copy.deepcopy(before['spec'])
            expected['suspend' if before['kind'] == 'CronJob' else 'replicas'] = True if before['kind'] == 'CronJob' else 0
            assert rows[key]['spec'] == expected
            assert _uid(rows[key]) == _uid(before)
        assert all(path.read_bytes() == raw for path, raw in frozen.items())
        calls = mode['calls']
        with pytest.raises(ValueError):
            templates.advance_shared_recovery_templates(bindings=bindings, api=api.template_api, state_dir=child)
        assert mode['calls'] == calls
