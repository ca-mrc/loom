"""Shared handoff recovery release lifecycle regressions."""
from __future__ import annotations

from tests.ops.test_nebius_shared_pool_handoff import (
    Path as Path,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    SharedPhaseAPI as SharedPhaseAPI,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    _exercise_completed_recovery_readers as _exercise_completed_recovery_readers,
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


@pytest.mark.timeout(600)
@pytest.mark.parametrize('committed', [False, True])
def test_shared_recovery_restarts_only_current_staging_with_real_pod_readiness(shared_operation, committed):
    """Real restart CAS/process reads; prior phase effects and SQL probes doubled."""
    import importlib.util
    import json
    import ssl
    from contextlib import ExitStack
    from types import SimpleNamespace

    import httpx
    from scripts.ops import nebius_certificates as private_state
    from scripts.ops import nebius_shared_pool_guards as guards
    from scripts.ops import nebius_shared_pool_operation as parent
    from scripts.ops import nebius_shared_pool_recovery_drain as drain
    from scripts.ops import nebius_shared_pool_recovery_fence as fence
    from scripts.ops import nebius_shared_pool_recovery_gateway as gateway
    from scripts.ops import nebius_shared_pool_recovery_machines as machines
    from scripts.ops import nebius_shared_pool_recovery_roles as roles
    from scripts.ops import nebius_shared_pool_recovery_templates as templates
    from scripts.ops import nebius_shared_pool_recovery_workloads as stop
    from scripts.ops import nebius_shared_pool_workloads as staging
    from scripts.ops.nebius_development_runtime_install import _read
    from scripts.ops.nebius_development_runtime_live import runtime_resource_path
    from scripts.ops.nebius_ingress_stage import _snapshot, _uid
    from scripts.ops.nebius_management_switch import _stable
    from tests.ops.test_nebius_development_pool_retained import populate_runtime_pods

    name = 'scripts.ops.nebius_shared_pool_recovery_restart'
    assert importlib.util.find_spec(name) is not None, 'current-staging recovery restart is missing'
    module = __import__(name, fromlist=['HTTPSSharedRecoveryRestart'])
    args, bindings = shared_operation
    runtime = args['runtime']
    manager = runtime.database.manager.retained.request.retained
    state = Path(manager.operation['state_dir']).parent / 'shared-pool-handoff'
    child = state / 'recovery-restart'
    captured = copy.deepcopy(bindings.plan.staging.originals)
    documents, rows = {}, {}
    mode = {'busy': False, 'busy_preview': True, 'late_busy': False, 'writing_calls': 0,
        'pending': None, 'unready': True, 'successor_pod': False, 'gateway_writer': False, 'roles_missing': False,
        'late_pod': None, 'bad_pod': None, 'challenge_reads': 0, 'unrelated_pod': False, 'late_unrelated': False}
    attempts, effects, challenges = [], [], []

    def refresh():
        documents.clear()
        for row in rows.values():
            meta = row['metadata']
            meta.setdefault('generation', 1)
            meta.setdefault('resourceVersion', '2')
            if row['kind'] == 'Deployment':
                row['status'] = {'observedGeneration': meta['generation'], **{field: row['spec']['replicas']
                    for field in ('replicas', 'readyReplicas', 'updatedReplicas', 'availableReplicas')}}
            documents[runtime_resource_path(row) + '/' + meta['name']] = copy.deepcopy(row)
            for version, resource, kind in (('apps/v1', 'replicasets', 'ReplicaSet'), ('v1', 'pods', 'Pod'), ('batch/v1', 'jobs', 'Job')):
                prefix = '/api/v1' if version == 'v1' else '/apis/' + version
                documents.setdefault(prefix + '/namespaces/' + meta['namespace'] + '/' + resource,
                    {'apiVersion': version, 'kind': kind + 'List', 'metadata': {'resourceVersion': '1'}, 'items': []})
        populate_runtime_pods(documents)

    def apply(key, changes, *, persist):
        value = copy.deepcopy(rows[key])
        assert key in captured, 'restart cannot alter new dev/global, database or backup roots'
        assert changes[0] == {'op': 'test', 'path': '/metadata/uid', 'value': _uid(captured[key])}
        assert changes[1]['op'] == 'test' and changes[1]['path'] == '/metadata/resourceVersion'
        assert any(row['op'] == 'test' and row['path'] == '/spec' for row in changes)
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
        assert value['spec'] == captured[key]['spec']
        assert value['metadata']['annotations'][stop.MARKER] == str(bindings.migration.registration.spec.operation_id)
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
            if value['kind'] == 'PodList':
                for pod in value['items']:
                    if mode['unready'] or (mode['bad_pod'] is not None and all(
                            pod['metadata'].get('labels', {}).get(label) == wanted
                            for label, wanted in rows[mode['bad_pod']]['spec']['selector']['matchLabels'].items())):
                        pod['status']['conditions'][0]['status'] = 'False'
                if mode['unrelated_pod'] and path == '/api/v1/namespaces/' + captured[target_key]['metadata']['namespace'] + '/pods':
                    value['items'].append({'apiVersion': 'v1', 'kind': 'Pod', 'metadata': {'name': 'unrelated',
                        'namespace': captured[target_key]['metadata']['namespace'], 'uid': '00000000-0000-4000-8000-000000000778',
                        'labels': {'app': 'unrelated'}}, 'spec': {}, 'status': {'phase': 'Running'}})
            if value['kind'] == 'PodList' and mode['successor_pod']:
                root = next(row for key, row in rows.items() if key not in captured and row['kind'] == 'Deployment')
                if path == '/api/v1/namespaces/' + root['metadata']['namespace'] + '/pods':
                    value['items'].append({'apiVersion': 'v1', 'kind': 'Pod',
                        'metadata': {'name': 'retained-successor', 'namespace': root['metadata']['namespace'],
                            'uid': '00000000-0000-4000-8000-000000000777', 'labels': copy.deepcopy(root['spec']['selector']['matchLabels'])},
                        'spec': copy.deepcopy(root['spec']['template']['spec']), 'status': {'phase': 'Running'}})
            return httpx.Response(200, json=value)
        assert message.method == 'PATCH'
        key, changes = _key(documents[path]), json.loads(message.content)
        if message.url.params.get('dryRun') == 'All':
            if mode['busy_preview']:
                mode['busy_preview'], mode['busy'] = False, True
            return apply(key, changes, persist=False)
        assert not mode['busy'] and not mode['successor_pod'] and not mode['gateway_writer'] and not mode['roles_missing']
        assert _read(child / 'restart.json')['resources'][key]['status'] == 'intent'
        attempts.append(key)
        if len(attempts) == 1:
            mode['pending'] = key, changes
            if committed:
                apply(key, changes, persist=True)
            raise httpx.ReadError('simulated uncertain current-staging restart')
        return apply(key, changes, persist=True)

    children = {
        'forward-guards': (guards.prepare_shared_pool_guards, guards.validate_shared_pool_guards, 'guards.json'),
        'recovery-fence': (fence.prepare_shared_recovery_fence, fence.validate_shared_recovery_fence, 'fence.json'),
        'recovery-drain': (drain.prepare_shared_recovery_drain, drain.validate_shared_recovery_drain, 'drain.json'),
        'recovery-stop': (stop.prepare_shared_recovery_workloads, stop.validate_shared_recovery_workloads, 'stop.json'),
        'recovery-restore-workloads': (templates.prepare_shared_recovery_templates, templates.validate_shared_recovery_templates, 'templates.json'),
        'recovery-restore-roles': (roles.prepare_shared_recovery_roles, roles.validate_shared_recovery_roles, 'roles.json'),
        'recovery-restart': (module.prepare_shared_recovery_restart, module.validate_shared_recovery_restart, 'restart.json'),
    }
    with ExitStack() as stack:
        class API(SharedPhaseAPI):
            restart_api = None

            def prepare(self, phase, bound, directory):
                if phase in children:
                    children[phase][0](bindings=bound, state_dir=directory)
                elif phase == 'forward-stop':
                    staging.prepare_shared_staging_workloads(bindings=bound, phase=phase, state_dir=directory)
                elif phase == 'recovery-revoke':
                    machines.prepare_shared_recovery_machines(bindings=bound, state_dir=directory)
                    gateway.prepare_shared_recovery_gateway(bindings=bound, state_dir=directory)
                else:
                    super().prepare(phase, bound, directory)

            def validate_prepared_phase(self, phase, directory):
                if phase in children and phase != 'forward-guards':
                    children[phase][1](bindings=bindings, state_dir=directory, complete=False, prepared=True)
                elif phase in {'forward-guards', 'forward-stop', 'recovery-revoke'}:
                    self.validate_phase(phase, directory, complete=False)
                else:
                    super().validate_prepared_phase(phase, directory)

            def validate_phase(self, phase, directory, *, complete):
                if phase in children:
                    children[phase][1](bindings=bindings, state_dir=directory, complete=complete)
                elif phase == 'forward-stop':
                    staging.validate_shared_staging_workloads(bindings=bindings, phase=phase, state_dir=directory, complete=complete)
                elif phase == 'recovery-revoke':
                    machines.validate_shared_recovery_machines(bindings=bindings, state_dir=directory, complete=complete)
                    gateway.validate_shared_recovery_gateway(bindings=bindings, state_dir=directory, complete=complete)
                else:
                    super().validate_phase(phase, directory, complete=complete)

            def advance(self, phase, bound, directory):
                if phase == 'forward-stop':
                    return False  # Test recovery before any forward workload dispatch.
                if phase == 'recovery-revoke':
                    path = directory / 'machines.json'
                    private_state._atomic_json(path, {**_read(path), 'status': 'revoked'})
                    path = directory / 'gateway.json'
                    record = _read(path)
                    for row in record['roles'].values():
                        row['status'] = 'absent'
                    private_state._atomic_json(path, record)
                    return True
                if phase in children and phase != 'recovery-restart':
                    path = directory / children[phase][2]
                    record = _read(path)
                    if phase == 'forward-guards':
                        record['guards'].update(dict.fromkeys(record['guards'], 'held'))
                    elif phase == 'recovery-fence':
                        record['pool'] = 'fenced'
                        record['guards'].update(dict.fromkeys(record['guards'], 'fenced'))
                    elif phase == 'recovery-drain':
                        record['status'] = 'observed'
                    elif phase == 'recovery-stop':
                        for key, choices in stop._sources(bound, state).items():
                            before = _snapshot(choices[-1])
                            before['metadata']['uid'] = _uid(choices[-1])
                            expected = stop._stopped(bound, before)
                            record['resources'][key] = {'status': 'stopped', 'before': before,
                                'before_resource_version': '1', 'expected': expected}
                            rows[key] = {**copy.deepcopy(expected), 'metadata': {**expected['metadata'],
                                'uid': _uid(before), 'resourceVersion': '2'}}
                    elif phase == 'recovery-restore-workloads':
                        before, targets = templates._inputs(bound, state)
                        for key, row in record['resources'].items():
                            row.update(status='restored', before_resource_version=
                                '2' if _stable(before[key]) != _stable(targets[key]) else None)
                            rows[key] = {**copy.deepcopy(targets[key]), 'metadata': {**targets[key]['metadata'],
                                'uid': _uid(before[key]), 'resourceVersion': '3'}}
                    elif phase == 'recovery-restore-roles':
                        for row in record['roles'].values():
                            row.update(status='restored', before_resource_version='201')
                    private_state._atomic_json(path, record)
                    refresh()
                    return True
                if phase != 'recovery-restart':
                    return super().advance(phase, bound, directory)
                if self.restart_api is None:
                    def qualify(request, selected, writing):
                        assert request == runtime and selected == phase
                        if writing and mode['late_busy']:
                            mode['writing_calls'] += 1
                            if mode['writing_calls'] == 4:
                                mode['busy'] = True

                    def permissions(fault):
                        if mode[fault]:
                            raise ValueError('simulated changed recovery authority')

                    def ready(request, selected, before, desired, actual):
                        assert request == runtime and selected == phase
                        assert _key(before) in captured and actual['spec'] == desired['spec']
                        challenges.append(_key(actual))
                        if _key(actual) == mode['late_pod']:
                            mode['challenge_reads'] += 1
                            if mode['challenge_reads'] == 2:
                                if mode['late_unrelated']:
                                    mode['unrelated_pod'] = True
                                else:
                                    mode['bad_pod'] = _key(actual)
                        return True

                    self.restart_api = stack.enter_context(module.HTTPSSharedRecoveryRestart(bindings=bound,
                        api_server=runtime.database.foundation.inputs.config['kubernetes_api_server'],
                        ssl_context=ssl.create_default_context(), qualify_runtime=qualify, observe_ready=ready,
                        recovery_drained=lambda: not mode['busy'],
                        roles=SimpleNamespace(bindings=bound, state=state / 'recovery-restore-roles',
                            verify_permissions=lambda: permissions('roles_missing')),
                        gateway=SimpleNamespace(bindings=bound, state=state / 'recovery-revoke',
                            verify_permissions=lambda: permissions('gateway_writer'))))
                    connection_type = type(self.restart_api.client)
                    self.restart_api.client.close()
                    self.restart_api.client = connection_type(base_url=self.restart_api.api_server, transport=httpx.MockTransport(handle))
                return module.advance_shared_recovery_restart(bindings=bound, api=self.restart_api, state_dir=directory)

        api = API(parent)
        api.block = 'recovery-reopen'
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
                raise ValueError('restart failure at ' + ', '.join(frames)) from None

        assert run_operation()['phase'] == 'forward-stop'
        frozen = {path: path.read_bytes() for path in state.glob('forward-*/**/*.json')}
        kwargs['action'] = 'recover'
        assert run_operation()['phase'] == 'recovery-restart'
        assert not attempts and mode['busy']
        untouched = {key: copy.deepcopy(row) for key, row in rows.items() if key not in captured}
        assert {row['status'] for row in _read(child / 'restart.json')['resources'].values()} <= {'prepared', 'started'}
        assert 'intent' not in {row['status'] for row in _read(child / 'restart.json')['resources'].values()}
        receipt = state / 'recovery-restore-roles/roles.json'
        saved = receipt.read_bytes()
        receipt.write_bytes(saved + b'\n')
        try:
            with pytest.raises(ValueError):
                api.restart_api.ready()
        finally:
            receipt.write_bytes(saved)
        target_key = next(key for key in captured if captured[key]['kind'] == 'Deployment')
        target = copy.deepcopy(api.restart_api.targets[target_key])
        api.restart_api.targets[target_key]['spec']['replicas'] = 2
        try:
            with pytest.raises(ValueError):
                api.restart_api.ready()
        finally:
            api.restart_api.targets[target_key] = target
        mode['busy'], mode['successor_pod'] = False, True
        assert run_operation()['phase'] == 'recovery-restart'
        assert not attempts
        mode['successor_pod'] = False
        for fault in ('gateway_writer', 'roles_missing'):
            mode[fault] = True
            with pytest.raises(ValueError):
                run_operation()
            assert not attempts
            mode[fault] = False
        mode['late_busy'] = True
        assert run_operation()['phase'] == 'recovery-restart'
        assert mode['busy'] and not attempts
        assert 'intent' not in {row['status'] for row in _read(child / 'restart.json')['resources'].values()}
        mode['busy'], mode['late_busy'] = False, False
        assert run_operation()['phase'] == 'recovery-restart'
        assert mode['pending'] is not None and len(attempts) == 1
        assert run_operation()['phase'] == 'recovery-restart' and len(attempts) == 1
        if not committed:
            assert apply(*mode['pending'], persist=True).status_code == 200
        # Actual unready Pods hold recovery before another root is started.
        assert run_operation()['phase'] == 'recovery-restart' and len(attempts) == 1
        mode['unready'] = False
        assert run_operation()['phase'] == 'recovery-reopen'
        changed = {key for key, value in captured.items()
            if value['spec']['suspend' if value['kind'] == 'CronJob' else 'replicas'] != (True if value['kind'] == 'CronJob' else 0)}
        assert set(attempts) == set(effects) == changed and len(attempts) == len(set(attempts))
        assert set(challenges) == captured.keys()
        for key, value in captured.items():
            assert rows[key]['spec'] == value['spec'] and _uid(rows[key]) == _uid(value)
            assert rows[key]['metadata']['annotations'] == {**value['metadata'].get('annotations', {}),
                stop.MARKER: str(bindings.migration.registration.spec.operation_id)}
        assert all(rows[key] == value for key, value in untouched.items())
        assert all(path.read_bytes() == raw for path, raw in frozen.items())
        assert api.restart_api.ready()
        mode['late_pod'] = next(key for key in reversed(api.restart_api.originals)
            if key in captured and captured[key]['kind'] == 'Deployment')
        assert api.restart_api.ready() is False
        assert mode['bad_pod'] is not None
        mode['bad_pod'], mode['challenge_reads'], mode['late_unrelated'] = None, 0, True
        assert api.restart_api.ready()  # Unrelated Pod activity is not a fixed-root readiness failure.
        assert mode['unrelated_pod']
        mode['late_pod'] = None
        mode['unready'] = True
        assert api.restart_api.ready() is False
        mode['unready'], mode['busy'] = False, True
        assert api.restart_api.ready()  # Later reopening observation is not an idle-work requirement.
        calls = len(attempts)
        with pytest.raises(ValueError):
            module.advance_shared_recovery_restart(bindings=bindings, api=api.restart_api, state_dir=child)
        assert len(attempts) == calls
