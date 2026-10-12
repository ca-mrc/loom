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
@pytest.mark.parametrize('winner', ['readonly', 'original', 'pending', 'binding'])
def test_shared_recovery_gateway_closes_partial_grants_without_replaying_unknown_writes(shared_operation, winner):
    """Real forward/recovery gateway HTTP; prior phase effects and machine readbacks doubled."""
    import importlib.util
    import json
    import ssl
    from contextlib import ExitStack

    import httpx
    from scripts.ops import nebius_certificates as private_state
    from scripts.ops import nebius_shared_pool_gateway as forward
    from scripts.ops import nebius_shared_pool_guards as guards
    from scripts.ops import nebius_shared_pool_operation as parent
    from scripts.ops import nebius_shared_pool_recovery_drain as drain
    from scripts.ops import nebius_shared_pool_recovery_fence as fence
    from scripts.ops import nebius_shared_pool_recovery_machines as machines
    from scripts.ops import nebius_shared_pool_recovery_workloads as stop
    from scripts.ops import nebius_shared_pool_workloads as staging
    from scripts.ops.nebius_development_runtime_install import _read
    from scripts.ops.nebius_development_runtime_live import runtime_resource_path
    from scripts.ops.nebius_ingress_stage import _snapshot, _uid

    name = 'scripts.ops.nebius_shared_pool_recovery_gateway'
    assert importlib.util.find_spec(name) is not None, 'partial shared gateway retirement is missing'
    gateway = __import__(name, fromlist=['HTTPSSharedRecoveryGateway'])
    args, bindings = shared_operation
    runtime = args['runtime']
    manager = runtime.database.manager.retained.request.retained
    state = Path(manager.operation['state_dir']).parent / 'shared-pool-handoff'
    child = state / 'recovery-revoke'
    catalog = bindings.plan.development.authority
    role_keys = [key for key, row in catalog.items() if row['kind'] == 'Role']
    fault_key = next(key for key, row in catalog.items() if row['kind'] == 'RoleBinding'
        and row['metadata']['namespace'] == catalog[role_keys[0]]['metadata']['namespace']) if winner == 'binding' else role_keys[1]
    namespaces = {ns.name: str(ns.uid) for row in bindings.migration.registration.spec.participants
        for ns in (row.execution_namespace, row.build_namespace)}
    namespaces[manager.binding.namespace] = manager.binding.namespace_uid
    paths = {runtime_resource_path(row) + '/' + row['metadata']['name']: key for key, row in catalog.items()}
    stored, effects, reviews = {}, [], []
    mode = {'forward': True, 'pending_original': None, 'busy_preview': True, 'busy': False,
        'pending_patch': None, 'patches': 0, 'barriers': 0, 'pending_barrier': None, 'foreign': True}
    foreign_role = {'apiVersion': 'rbac.authorization.k8s.io/v1', 'kind': 'Role',
        'metadata': {'name': 'foreign-writer', 'namespace': 'foreign-gateway-test', 'uid': str(uuid4()), 'resourceVersion': '1'},
        'rules': [{'apiGroups': ['batch'], 'resources': ['jobs'], 'verbs': ['create'], 'resourceNames': ['named-job']}]}
    foreign_binding = {'apiVersion': foreign_role['apiVersion'], 'kind': 'RoleBinding',
        'metadata': {**foreign_role['metadata'], 'uid': str(uuid4())},
        'roleRef': {'apiGroup': 'rbac.authorization.k8s.io', 'kind': 'Role', 'name': 'foreign-writer'},
        'subjects': [{'apiGroup': 'rbac.authorization.k8s.io', 'kind': 'Group', 'name': 'system:authenticated'}]}

    def inventory():
        return [*stored.values(), *([foreign_role, foreign_binding] if mode['foreign'] and not mode['forward'] else [])]

    def conflict(reason='AlreadyExists'):
        return httpx.Response(409, json={'apiVersion': 'v1', 'kind': 'Status', 'status': 'Failure', 'code': 409, 'reason': reason})

    def create(document, *, persist):
        key = _key(document)
        if key in stored:
            return conflict()
        result = copy.deepcopy(document)
        result['metadata'].update(uid=str(uuid4()), resourceVersion='1')
        if persist:
            stored[key] = result
            effects.append(('create', key))
        return httpx.Response(201, json=result)

    def patch(key, changes, *, persist):
        value = copy.deepcopy(stored[key])
        for change in changes:
            fields = change['path'].strip('/').split('/')
            target = value
            for field in fields[:-1]:
                target = target[field]
            field = fields[-1]
            if change['op'] == 'test':
                if target[field] != change['value']:
                    return conflict('Conflict')
            else:
                assert change['op'] in {'add', 'replace'}
                target[field] = change['value']
        if persist:
            value['metadata']['resourceVersion'] = str(int(value['metadata']['resourceVersion']) + 1)
            stored[key] = value
            effects.append(('patch', key))
        return httpx.Response(200, json=value)

    def handle(message):
        path = message.url.path
        if message.method == 'GET':
            if path.startswith('/api/v1/namespaces/'):
                namespace = path.rsplit('/', 1)[-1]
                return httpx.Response(200, json={'apiVersion': 'v1', 'kind': 'Namespace',
                    'metadata': {'name': namespace, 'uid': namespaces[namespace], 'resourceVersion': '1'}})
            if path in paths:
                value = stored.get(paths[path])
                return httpx.Response(404) if value is None else httpx.Response(200, json=value)
            kind = {'roles': 'Role', 'rolebindings': 'RoleBinding', 'clusterroles': 'ClusterRole',
                'clusterrolebindings': 'ClusterRoleBinding'}[path.rsplit('/', 1)[-1]]
            return httpx.Response(200, json={'apiVersion': 'rbac.authorization.k8s.io/v1', 'kind': kind + 'List',
                'metadata': {'resourceVersion': '1', 'continue': ''}, 'items': [row for row in inventory() if row['kind'] == kind]})
        body = json.loads(message.content)
        if message.method == 'POST' and path.endswith('/selfsubjectrulesreviews'):
            assert message.headers['Impersonate-User'] == 'system:serviceaccount:' + manager.binding.namespace + ':loom-pool-gateway'
            namespace = body['spec']['namespace']
            reviews.append(namespace)
            rows, rules = {_key(row): row for row in inventory()}, []
            for binding in rows.values():
                if binding['kind'] not in {'RoleBinding', 'ClusterRoleBinding'}:
                    continue
                if binding['kind'] == 'RoleBinding' and binding['metadata']['namespace'] != namespace:
                    continue
                reference = binding['roleRef']
                key = reference['kind'] + ':' + (binding['metadata']['namespace'] if reference['kind'] == 'Role' else '-') + ':' + reference['name']
                if key in rows:
                    rules.extend(copy.deepcopy(rows[key]['rules']))
            return httpx.Response(201, json={'apiVersion': 'authorization.k8s.io/v1', 'kind': 'SelfSubjectRulesReview',
                'spec': {}, 'status': {'incomplete': False, 'resourceRules': rules, 'nonResourceRules': []}})
        if message.method == 'POST':
            key = _key(body)
            assert key in catalog and path == runtime_resource_path(catalog[key])
            if message.url.params.get('dryRun') == 'All':
                return create(body, persist=False)
            if mode['forward']:
                if key == fault_key:
                    mode['pending_original'] = copy.deepcopy(body)
                    raise httpx.ReadError('simulated delayed forward grant creation')
                return create(body, persist=True)
            assert body['kind'] == 'Role' and key == fault_key and not mode['busy']
            assert _read(child / 'gateway.json')['roles'][key]['status'] == 'create_intent'
            mode['barriers'] += 1
            if winner == 'original':
                assert create(mode['pending_original'], persist=True).status_code == 201
                return create(body, persist=True)  # The original won: a known name conflict.
            if winner == 'pending':
                mode['pending_barrier'] = copy.deepcopy(body)
                raise httpx.ReadError('simulated uncommitted barrier reply loss')
            result = create(body, persist=True)
            assert result.status_code == 201
            raise httpx.ReadError('simulated committed barrier reply loss')
        assert message.method == 'PATCH' and paths[path] in role_keys and not mode['forward']
        key = paths[path]
        if message.url.params.get('dryRun') == 'All':
            if mode['busy_preview']:
                mode['busy_preview'], mode['busy'] = False, True
            return patch(key, body, persist=False)
        assert not mode['busy'] and _read(child / 'gateway.json')['roles'][key]['status'] == 'patch_intent'
        mode['patches'] += 1
        if mode['patches'] == 1:
            mode['pending_patch'] = (key, body)
            raise httpx.ReadError('simulated uncommitted Role reduction')
        result = patch(key, body, persist=True)
        assert result.status_code == 200
        raise httpx.ReadError('simulated committed Role reduction reply loss')

    class MachineReadback:
        def __init__(self):
            self.bindings, self.state = bindings, child

        def drained(self):
            return not mode['busy']

        def machine_authority(self):
            return 'revoked'

    children = {
        'recovery-fence': (fence.prepare_shared_recovery_fence, fence.validate_shared_recovery_fence, 'fence.json'),
        'recovery-drain': (drain.prepare_shared_recovery_drain, drain.validate_shared_recovery_drain, 'drain.json'),
        'recovery-stop': (stop.prepare_shared_recovery_workloads, stop.validate_shared_recovery_workloads, 'stop.json'),
    }
    workload_phases = {'forward-stop': 'stop', 'forward-wire': 'replace', 'forward-start': 'start'}
    with ExitStack() as stack:
        class API(SharedPhaseAPI):
            gateway_api = None

            def prepare(self, phase, bound, directory):
                if phase == 'forward-guards':
                    guards.prepare_shared_pool_guards(bindings=bound, state_dir=directory)
                elif phase in workload_phases:
                    staging.prepare_shared_staging_workloads(bindings=bound, phase=phase, state_dir=directory)
                elif phase == 'forward-gateway':
                    forward.prepare_shared_gateway(bindings=bound, state_dir=directory)
                elif phase in children:
                    children[phase][0](bindings=bound, state_dir=directory)
                elif phase == 'recovery-revoke':
                    machines.prepare_shared_recovery_machines(bindings=bound, state_dir=directory)
                    gateway.prepare_shared_recovery_gateway(bindings=bound, state_dir=directory)
                else:
                    super().prepare(phase, bound, directory)

            def validate_prepared_phase(self, phase, directory):
                if phase == 'recovery-revoke':
                    machines.validate_shared_recovery_machines(bindings=bindings, state_dir=directory, complete=False, prepared=True)
                    gateway.validate_shared_recovery_gateway(bindings=bindings, state_dir=directory, complete=False, prepared=True)
                elif phase in {'forward-guards', 'forward-gateway', *workload_phases, *children}:
                    self.validate_phase(phase, directory, complete=False)
                else:
                    super().validate_prepared_phase(phase, directory)

            def validate_phase(self, phase, directory, *, complete):
                if phase == 'forward-guards':
                    guards.validate_shared_pool_guards(bindings=bindings, state_dir=directory, complete=complete)
                elif phase in workload_phases:
                    staging.validate_shared_staging_workloads(bindings=bindings, phase=phase, state_dir=directory, complete=complete)
                elif phase == 'forward-gateway':
                    forward.validate_shared_gateway(bindings=bindings, state_dir=directory, complete=complete)
                elif phase in children:
                    children[phase][1](bindings=bindings, state_dir=directory, complete=complete)
                elif phase == 'recovery-revoke':
                    machines.validate_shared_recovery_machines(bindings=bindings, state_dir=directory, complete=complete)
                    gateway.validate_shared_recovery_gateway(bindings=bindings, state_dir=directory, complete=complete)
                else:
                    super().validate_phase(phase, directory, complete=complete)

            def advance(self, phase, bound, directory):
                if phase == 'forward-guards':
                    path = directory / 'guards.json'
                    record = _read(path)
                    record['guards'].update(dict.fromkeys(record['guards'], 'held'))
                    private_state._atomic_json(path, record)
                    return True
                if phase in workload_phases:
                    from scripts.ops.nebius_management_stage import _comparison_snapshot

                    _, targets = staging._inputs(bound, state, phase, staging._frame(bound, state))
                    path = directory / 'transition.json'
                    record = _read(path)
                    for key, row in record['resources'].items():
                        row.update(status='applied', expected=_comparison_snapshot(targets[key]))
                    private_state._atomic_json(path, record)
                    return True
                if phase in children:
                    path = directory / children[phase][2]
                    record = _read(path)
                    if phase == 'recovery-fence':
                        record['pool'] = 'fenced'
                        record['guards'].update(dict.fromkeys(record['guards'], 'fenced'))
                    elif phase == 'recovery-drain':
                        record['status'] = 'observed'
                    else:
                        for key, choices in stop._sources(bound, state).items():
                            before = _snapshot(choices[-1])
                            before['metadata']['uid'] = _uid(choices[-1])
                            record['resources'][key] = {'status': 'stopped', 'before': before,
                                'before_resource_version': '1', 'expected': stop._stopped(bound, before)}
                    private_state._atomic_json(path, record)
                    return True
                if phase not in {'forward-gateway', 'recovery-revoke'}:
                    return super().advance(phase, bound, directory)
                options = {'bindings': bound, 'api_server': runtime.database.foundation.inputs.config['kubernetes_api_server'],
                    'ssl_context': ssl.create_default_context()}
                if phase == 'forward-gateway':
                    connection = stack.enter_context(forward.HTTPSSharedGateway(**options,
                        qualify_runtime=lambda *_: None, observe_ready=lambda *_: False))
                elif self.gateway_api is None:
                    path = directory / 'machines.json'
                    record = _read(path)
                    record['status'] = 'revoked'  # This already-tested predecessor is an explicit double here.
                    private_state._atomic_json(path, record)
                    connection = self.gateway_api = stack.enter_context(gateway.HTTPSSharedRecoveryGateway(**options, machines=MachineReadback()))
                else:
                    connection = self.gateway_api
                if not hasattr(connection, 'test_transport_installed'):
                    connection_type = type(connection.client)
                    connection.client.close()
                    connection.client = connection_type(base_url=connection.api_server, transport=httpx.MockTransport(handle))
                    connection.test_transport_installed = True
                if phase == 'forward-gateway':
                    return forward.advance_shared_gateway(bindings=bound, api=connection, state_dir=directory)
                return gateway.advance_shared_recovery_gateway(bindings=bound, api=connection, state_dir=directory)

        api = API(parent)
        api.block = 'recovery-restore-workloads'
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
                raise ValueError('gateway failure at ' + ', '.join(frames)) from None

        with pytest.raises(ValueError) as failure:
            run_operation()
        assert mode['pending_original'] is not None and role_keys[0] in stored, str(failure.value)
        original_uid = _uid(stored[role_keys[0]])
        frozen = {path: path.read_bytes() for path in state.glob('forward-*/**/*.json')}
        mode['forward'] = False
        kwargs['action'] = 'recover'
        assert run_operation()['phase'] == 'recovery-revoke'
        assert mode['patches'] == 0 and mode['barriers'] == 0
        assert _read(child / 'gateway.json')['roles'][role_keys[0]]['status'] == 'original'
        mode['busy'] = False
        assert run_operation()['phase'] == 'recovery-revoke'
        assert mode['patches'] == 1 and mode['pending_patch'] is not None
        assert run_operation()['phase'] == 'recovery-revoke'
        assert mode['patches'] == 1
        assert patch(*mode['pending_patch'], persist=True).status_code == 200
        if winner == 'pending':
            assert run_operation()['phase'] == 'recovery-revoke'
            assert mode['barriers'] == 1 and mode['pending_barrier'] is not None
            assert run_operation()['phase'] == 'recovery-revoke'
            assert mode['barriers'] == 1
            assert create(mode['pending_barrier'], persist=True).status_code == 201
        with pytest.raises(ValueError) as failure:
            run_operation()  # Real effective review must reject the extra named writer.
        assert 'foreign-gateway-test' in reviews, str(failure.value)
        assert _uid(stored[role_keys[0]]) == original_uid
        for key in role_keys:
            if key in stored:
                assert [rule['verbs'] for rule in stored[key]['rules']] == [['get'], ['get'], ['get', 'list']]
        mode['foreign'] = False
        assert run_operation()['phase'] == 'recovery-restore-workloads'
        if winner == 'binding':
            assert create(mode['pending_original'], persist=True).status_code == 201
            api.gateway_api.verify_permissions()  # A late binding can now grant only the pinned read rights.
        else:
            assert create(mode['pending_original'], persist=True).status_code == 409
            assert mode['barriers'] == 1
        assert mode['patches'] == (2 if winner == 'original' else 1)
        assert all(path.read_bytes() == raw for path, raw in frozen.items())
        assert set(row['status'] for row in _read(child / 'gateway.json')['roles'].values()) <= {'restricted', 'absent'}
        before = list(effects)
        with pytest.raises(ValueError):
            gateway.advance_shared_recovery_gateway(bindings=bindings, api=api.gateway_api, state_dir=child)
        assert effects == before
