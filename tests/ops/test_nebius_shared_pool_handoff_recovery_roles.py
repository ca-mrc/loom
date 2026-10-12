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
def test_shared_recovery_restores_current_roles_with_delayed_forward_barrier(shared_operation, committed):
    """Real Role CAS/reviews; predecessor effects and SQL/process qualification doubled."""
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
    from scripts.ops import nebius_shared_pool_recovery_templates as templates
    from scripts.ops import nebius_shared_pool_recovery_workloads as stop
    from scripts.ops import nebius_shared_pool_roles as forward
    from scripts.ops import nebius_shared_pool_workloads as staging
    from scripts.ops.nebius_development_runtime_install import _read
    from scripts.ops.nebius_development_runtime_live import runtime_resource_path
    from scripts.ops.nebius_ingress_stage import _snapshot, _uid
    from scripts.ops.nebius_management_stage import _comparison_snapshot

    name = 'scripts.ops.nebius_shared_pool_recovery_roles'
    assert importlib.util.find_spec(name) is not None, 'current-staging Role restoration is missing'
    module = __import__(name, fromlist=['HTTPSSharedRecoveryRoles'])
    args, bindings = shared_operation
    runtime = args['runtime']
    manager = runtime.database.manager.retained.request.retained
    state = Path(manager.operation['state_dir']).parent / 'shared-pool-handoff'
    child = state / 'recovery-restore-roles'
    originals = copy.deepcopy(bindings.plan.staging_role_originals)
    roles = copy.deepcopy(originals)
    catalog = {_key(row): copy.deepcopy(row) for row in bindings.plan.staging.attachment.resources
        if row['kind'] in {'Role', 'ClusterRole', 'RoleBinding', 'ClusterRoleBinding'}}
    fixed_bindings = {key: row for key, row in catalog.items() if row['kind'].endswith('Binding')}
    foreign = {'apiVersion': 'rbac.authorization.k8s.io/v1', 'kind': 'RoleBinding',
        'metadata': {'name': 'foreign', 'namespace': 'outside-pool', 'uid': str(uuid4()), 'resourceVersion': '1'},
        'subjects': [{'kind': 'Group', 'apiGroup': 'rbac.authorization.k8s.io', 'name': 'system:authenticated'}],
        'roleRef': {'apiGroup': 'rbac.authorization.k8s.io', 'kind': 'Role', 'name': 'foreign'}}
    current_bindings = {**copy.deepcopy(fixed_bindings), _key(foreign): foreign}
    first = min(originals)
    inbound = copy.deepcopy(next(row for row in fixed_bindings.values()
        if row['roleRef']['kind'] == 'Role' and row['roleRef']['name'] == originals[first]['metadata']['name']))
    inbound['metadata'].update(name='foreign-inbound', uid=str(uuid4()))
    inbound['subjects'] = [{'kind': 'ServiceAccount', 'namespace': 'outside-pool', 'name': 'stranger'}]
    current_bindings[_key(inbound)] = inbound
    identity = bindings.migration.registration.binding
    namespaces = {'kube-system': identity.kube_system_uid, identity.namespace: identity.namespace_uid,
        **{guard.namespace: str(guard.namespace_uid) for guard in bindings.migration.guards},
        **{ns.name: str(ns.uid) for participant in bindings.migration.registration.spec.participants
            for ns in (participant.execution_namespace, participant.build_namespace)}}
    mode = {'recovering': False, 'busy': False, 'busy_preview': True, 'late_fault': None, 'writing_calls': 0, 'extra': False, 'missing': False,
        'incomplete': False, 'gateway_writer': False, 'pending_forward': None, 'pending_restore': None, 'calls': 0}
    attempts, effects, reviews, qualification = [], [], [], []

    def apply(key, changes, *, persist):
        value = copy.deepcopy(roles[key])
        assert changes[0] == {'op': 'test', 'path': '/metadata/uid', 'value': _uid(originals[key])}
        assert changes[1]['op'] == 'test' and changes[1]['path'] == '/metadata/resourceVersion'
        assert any(row['op'] == 'test' and row['path'] == '/rules' for row in changes)
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
        if persist:
            value['metadata']['resourceVersion'] = str(int(value['metadata']['resourceVersion']) + 1)
            roles[key] = value
            effects.append((mode['recovering'], key))
        return httpx.Response(200, json=value)

    def handle(message):
        path = message.url.path
        if path.startswith('/api/v1/namespaces/'):
            assert message.method == 'GET'
            namespace = path.rsplit('/', 1)[-1]
            return httpx.Response(200, json={'apiVersion': 'v1', 'kind': 'Namespace',
                'metadata': {'name': namespace, 'uid': namespaces[namespace], 'resourceVersion': '1'}})
        if path in ('/apis/rbac.authorization.k8s.io/v1/rolebindings',
                '/apis/rbac.authorization.k8s.io/v1/clusterrolebindings'):
            assert message.method == 'GET'
            kind = 'RoleBinding' if path.endswith('/rolebindings') else 'ClusterRoleBinding'
            return httpx.Response(200, json={'apiVersion': 'rbac.authorization.k8s.io/v1', 'kind': kind + 'List',
                'metadata': {'resourceVersion': '71'},
                'items': [copy.deepcopy(row) for row in current_bindings.values() if row['kind'] == kind]})
        if path == '/apis/authorization.k8s.io/v1/selfsubjectrulesreviews':
            assert message.method == 'POST'
            account = message.headers['Impersonate-User']
            _, _, account_namespace, account_name = account.split(':')
            groups = {'system:serviceaccounts', 'system:serviceaccounts:' + account_namespace, 'system:authenticated'}
            assert set(message.headers.get_list('Impersonate-Group', split_commas=True)) == groups
            namespace = json.loads(message.content)['spec']['namespace']
            reviews.append((account, namespace))
            rules = []
            for row in fixed_bindings.values():
                if row['kind'] == 'RoleBinding' and row['metadata']['namespace'] != namespace:
                    continue
                if not any(item['kind'] == 'ServiceAccount' and (item['namespace'], item['name']) == (account_namespace, account_name)
                        for item in row.get('subjects', [])):
                    continue
                reference = row['roleRef']
                key = reference['kind'] + ':' + (row['metadata']['namespace'] if reference['kind'] == 'Role' else '-') + ':' + reference['name']
                rules.extend(copy.deepcopy(roles.get(key, catalog.get(key))['rules']))
            if namespace == 'outside-pool' and mode['extra']:
                rules.append({'apiGroups': ['batch'], 'resources': ['jobs'], 'verbs': ['create'], 'resourceNames': ['one-job']})
            if mode['missing']:
                rules = []
            return httpx.Response(201, json={'apiVersion': 'authorization.k8s.io/v1', 'kind': 'SelfSubjectRulesReview',
                'spec': {}, 'status': {'incomplete': mode['incomplete'], 'resourceRules': rules, 'nonResourceRules': []}})
        assert 'Impersonate-User' not in message.headers
        for key, row in {**catalog, **bindings.plan.staging_reader_authority}.items():
            if path == runtime_resource_path(row) + '/' + row['metadata']['name']:
                if message.method == 'GET':
                    if not mode['recovering'] and committed and mode['pending_forward'] is not None:
                        raise httpx.ReadError('simulated lost committed forward readback')
                    value = roles.get(key, current_bindings.get(key, catalog.get(key)))
                    return httpx.Response(404) if value is None else httpx.Response(200, json=value)
                assert message.method == 'PATCH' and key in originals
                changes = json.loads(message.content)
                if message.url.params.get('dryRun') == 'All':
                    if mode['busy_preview']:
                        mode['busy_preview'], mode['busy'] = False, True
                    return apply(key, changes, persist=False)
                if not mode['recovering']:
                    assert _read(state / 'forward-roles/roles.json')['roles'][key] == 'intent'
                    assert mode['pending_forward'] is None
                    mode['pending_forward'] = key, changes
                    if committed:
                        apply(key, changes, persist=True)
                    raise httpx.ReadError('simulated uncertain forward Role reduction')
                assert not mode['busy'] and not mode['gateway_writer']
                assert _read(child / 'roles.json')['roles'][key]['status'] == 'intent'
                attempts.append(key)
                mode['calls'] += 1
                if mode['calls'] == 1:
                    mode['pending_restore'] = key, changes
                    if committed:
                        apply(key, changes, persist=True)
                    raise httpx.ReadError('simulated uncertain restoration')
                return apply(key, changes, persist=True)
        raise AssertionError('unexpected Role recovery request')

    children = {
        'forward-guards': (guards.prepare_shared_pool_guards, guards.validate_shared_pool_guards, 'guards.json'),
        'forward-roles': (forward.prepare_shared_staging_roles, forward.validate_shared_staging_roles, 'roles.json'),
        'recovery-fence': (fence.prepare_shared_recovery_fence, fence.validate_shared_recovery_fence, 'fence.json'),
        'recovery-drain': (drain.prepare_shared_recovery_drain, drain.validate_shared_recovery_drain, 'drain.json'),
        'recovery-stop': (stop.prepare_shared_recovery_workloads, stop.validate_shared_recovery_workloads, 'stop.json'),
        'recovery-restore-workloads': (templates.prepare_shared_recovery_templates, templates.validate_shared_recovery_templates, 'templates.json'),
        'recovery-restore-roles': (module.prepare_shared_recovery_roles, module.validate_shared_recovery_roles, 'roles.json'),
    }
    with ExitStack() as stack:
        def connect(transport):
            value = stack.enter_context(transport)
            connection_type = type(value.client)
            value.client.close()
            value.client = connection_type(base_url=value.api_server, transport=httpx.MockTransport(handle))
            return value

        class API(SharedPhaseAPI):
            role_api = None

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
                if phase == 'forward-guards':
                    children[phase][1](bindings=bindings, state_dir=directory, complete=False)
                elif phase in children:
                    children[phase][1](bindings=bindings, state_dir=directory, complete=False, prepared=True)
                elif phase in {'forward-stop', 'recovery-revoke'}:
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
                if phase == 'forward-roles':
                    connection = connect(forward.HTTPSSharedStagingRoles(bindings=bound,
                        api_server=runtime.database.foundation.inputs.config['kubernetes_api_server'],
                        ssl_context=ssl.create_default_context(), qualify_runtime=lambda current, writing: None))
                    return forward.advance_shared_staging_roles(bindings=bound, api=connection, state_dir=directory)
                if phase == 'recovery-revoke':
                    path = directory / 'machines.json'
                    private_state._atomic_json(path, {**_read(path), 'status': 'revoked'})
                    path = directory / 'gateway.json'
                    record = _read(path)
                    for row in record['roles'].values():
                        row['status'] = 'absent'
                    private_state._atomic_json(path, record)
                    return True
                if phase == 'forward-stop':
                    _, targets = staging._inputs(bound, state, phase, staging._frame(bound, state))
                    path = directory / 'transition.json'
                    record = _read(path)
                    for key, row in record['resources'].items():
                        row.update(status='applied', expected=_comparison_snapshot(targets[key]))
                    private_state._atomic_json(path, record)
                    return True
                if phase in children and phase != 'recovery-restore-roles':
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
                            record['resources'][key] = {'status': 'stopped', 'before': before,
                                'before_resource_version': '1', 'expected': stop._stopped(bound, before)}
                    elif phase == 'recovery-restore-workloads':
                        from scripts.ops.nebius_management_switch import _stable

                        before, targets = templates._inputs(bound, state)
                        for key, row in record['resources'].items():
                            row.update(status='restored', before_resource_version=
                                '2' if _stable(before[key]) != _stable(targets[key]) else None)
                    private_state._atomic_json(path, record)
                    return True
                if phase != 'recovery-restore-roles':
                    return super().advance(phase, bound, directory)
                if self.role_api is None:
                    def qualify(request, selected, writing):
                        assert request == runtime and selected == phase
                        qualification.append(writing)
                        if writing and mode['late_fault']:
                            mode['writing_calls'] += 1
                            if mode['writing_calls'] == 4:
                                mode[mode['late_fault']] = True

                    def gateway_denied():
                        if mode['gateway_writer']:
                            raise ValueError('simulated revived gateway write authority')

                    self.role_api = connect(module.HTTPSSharedRecoveryRoles(bindings=bound,
                        api_server=runtime.database.foundation.inputs.config['kubernetes_api_server'],
                        ssl_context=ssl.create_default_context(), qualify_runtime=qualify,
                        templates=SimpleNamespace(bindings=bound, state=state / 'recovery-restore-workloads', drained=lambda: not mode['busy']),
                        gateway=SimpleNamespace(bindings=bound, state=state / 'recovery-revoke', verify_permissions=gateway_denied)))
                return module.advance_shared_recovery_roles(bindings=bound, api=self.role_api, state_dir=directory)

        api = API(parent)
        api.block = 'recovery-restart'
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
                raise ValueError('Role recovery failure at ' + ', '.join(frames)) from None

        with pytest.raises(ValueError) as forward_failure:
            run_operation()
        assert mode['pending_forward'] is not None, str(forward_failure.value)
        frozen = {path: path.read_bytes() for path in state.glob('forward-*/**/*.json')}
        kwargs['action'], mode['recovering'] = 'recover', True
        with pytest.raises(ValueError) as inbound_failure:
            run_operation()  # Do not grant a restored fixed Role to a foreign subject.
        assert api.role_api is not None, str(inbound_failure.value)
        assert '_bindings' in str(inbound_failure.value)
        assert not attempts
        del current_bindings[_key(inbound)]
        mode['extra'] = True
        with pytest.raises(ValueError):
            run_operation()
        assert not attempts and any(namespace == 'outside-pool' for _, namespace in reviews)
        mode['extra'], mode['gateway_writer'] = False, True
        with pytest.raises(ValueError):
            run_operation()
        assert not attempts
        mode['gateway_writer'] = False
        assert run_operation()['phase'] == 'recovery-restore-roles'
        assert not attempts and mode['busy']
        assert {row['status'] for row in _read(child / 'roles.json')['roles'].values()} == {'prepared'}
        receipt = state / 'recovery-restore-workloads/templates.json'
        saved = receipt.read_bytes()
        receipt.write_bytes(saved + b'\n')
        try:
            with pytest.raises(ValueError):
                api.role_api.verify_permissions()
        finally:
            receipt.write_bytes(saved)
        target = copy.deepcopy(api.role_api.targets[first])
        api.role_api.targets[first]['rules'] = []
        try:
            with pytest.raises(ValueError):
                api.role_api.verify_permissions()
        finally:
            api.role_api.targets[first] = target
        mode['busy'], mode['late_fault'] = False, 'busy'
        assert run_operation()['phase'] == 'recovery-restore-roles'
        assert mode['busy'] and not attempts
        assert {row['status'] for row in _read(child / 'roles.json')['roles'].values()} == {'prepared'}
        mode['busy'], mode['late_fault'], mode['writing_calls'] = False, 'gateway_writer', 0
        with pytest.raises(ValueError):
            run_operation()
        assert mode['gateway_writer'] and not attempts
        assert {row['status'] for row in _read(child / 'roles.json')['roles'].values()} == {'prepared'}
        mode['late_fault'], mode['gateway_writer'] = None, False
        result = run_operation()
        assert mode['pending_restore'] is not None
        if not committed:
            assert result['phase'] == 'recovery-restore-roles' and len(attempts) == 1
            assert run_operation()['phase'] == 'recovery-restore-roles' and len(attempts) == 1
            assert apply(*mode['pending_restore'], persist=True).status_code == 200
            result = run_operation()
        assert result['phase'] == 'recovery-restart'
        assert attempts == sorted(originals)
        assert apply(*mode['pending_forward'], persist=True).status_code == 409
        for key, original in originals.items():
            assert roles[key]['rules'] == original['rules']
            assert roles[key]['metadata'].get('labels') == original['metadata'].get('labels')
            expected = {**original['metadata'].get('annotations', {}),
                stop.MARKER: str(bindings.migration.registration.spec.operation_id)}
            assert roles[key]['metadata']['annotations'] == expected
            assert _uid(roles[key]) == _uid(original)
        assert all(current_bindings[key] == row for key, row in fixed_bindings.items())
        assert all(path.read_bytes() == raw for path, raw in frozen.items())
        assert True in qualification
        for fault in ('missing', 'incomplete', 'extra'):
            mode[fault] = True
            with pytest.raises(ValueError):
                api.role_api.verify_permissions()
            mode[fault] = False
        api.role_api.verify_permissions()  # Completed recovery rights remain observable during restart.
        calls = len(attempts)
        with pytest.raises(ValueError):
            module.advance_shared_recovery_roles(bindings=bindings, api=api.role_api, state_dir=child)
        assert len(attempts) == calls
