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
def test_shared_recovery_machine_revocation_uses_fixed_sql_and_current_stop_drain(
        shared_operation, shared_guard_transport, shared_runtime, completed_pool, handoff, monkeypatch, committed):
    """Actual revoke/SQL/drain transports; prior stop effects and parent census doubled."""
    import importlib.util
    import ssl
    from contextlib import ExitStack

    import httpx
    from scripts.ops import nebius_certificates as private_state
    from scripts.ops import nebius_shared_pool_guards as guards
    from scripts.ops import nebius_shared_pool_operation as parent
    from scripts.ops import nebius_shared_pool_recovery_drain as drain
    from scripts.ops import nebius_shared_pool_recovery_fence as fence
    from scripts.ops import nebius_shared_pool_recovery_workloads as stop
    from scripts.ops.nebius_development_runtime_install import _read
    from scripts.ops.nebius_development_runtime_live import runtime_resource_path
    from scripts.ops.nebius_ingress_stage import _snapshot, _uid
    from scripts.ops.nebius_shared_pool_recovery_fence_live import SharedPoolRecoveryFenceTransport
    from tests.ops.test_nebius_development_pool_retained import populate_runtime_pods

    name = 'scripts.ops.nebius_shared_pool_recovery_machines'
    assert importlib.util.find_spec(name) is not None, 'shared recovery machine retirement is missing'
    machines = __import__(name, fromlist=['SharedRecoveryMachineTransport'])
    args, bindings = shared_operation
    runtime = args['runtime']
    manager = runtime.database.manager.retained.request.retained
    state = Path(manager.operation['state_dir']).parent / 'shared-pool-handoff'
    child = state / 'recovery-revoke'
    roster = tuple(str(target.participant_id) for target in bindings.migration.guards)
    documents, http_calls = {}, []
    mode = {'retain_pod': False}
    retained_pod = None

    class Remote:
        mode = 'closed'
        machines = 'active'
        dispatches = 0
        busy_after_machine_read = False
        change_receipt_after_machine_read = False
        open_during_qualification = False

        def __init__(self):
            self.local = dict.fromkeys(roster, 'open')
            self.reads = []
            self.pool_counts = dict.fromkeys(('unstarted_requests', 'active_requests', 'unconfirmed_creates', 'unqualified_releases'), 0)
            self.local_counts = {key: dict.fromkeys(('trials', 'executions', 'builds', 'build_cleanup',
                'execution_outboxes', 'build_outboxes'), 0) for key in roster}

        def verify_retained(self):
            # The complete writer/provider parent is a separate integration
            # boundary; inject a race in its final callback, not a fake verdict.
            if self.open_during_qualification:
                self.open_during_qualification = False
                self.local[roster[0]] = 'open'

        def after_machine_observation(self):
            if self.busy_after_machine_read:
                self.busy_after_machine_read = False
                self.pool_counts['active_requests'] = 1
            if self.change_receipt_after_machine_read:
                self.change_receipt_after_machine_read = False
                receipt = child / 'machines.json'
                receipt.write_bytes(receipt.read_bytes() + b'\n')

        def pool_state(self):
            return self.mode

        def guard_state(self, participant):
            return self.local[participant]

        def fence_pool(self):
            self.mode = 'fenced'

        def fence_guard(self, participant):
            self.local[participant] = 'fenced'

        def retire_machines(self):
            assert _read(child / 'machines.json')['status'] == 'intent'
            assert not mode['retain_pod'] and set(self.local.values()) == {'fenced'}
            self.dispatches += 1
            if committed:
                self.machines = 'revoked'
            raise TimeoutError('simulated machine retirement reply loss')

    remote = Remote()
    guard_api, database_model = shared_guard_transport
    _shared_recovery_database_io(remote=remote, bindings=bindings, guard_api=guard_api, database_model=database_model,
        shared_runtime=shared_runtime, completed_pool=completed_pool, handoff=handoff, monkeypatch=monkeypatch)

    def inventory(rows):
        documents.clear()
        for original in rows.values():
            row = copy.deepcopy(original)
            meta = row['metadata']
            meta.update(generation=2, resourceVersion='2')
            if row['kind'] == 'Deployment':
                row['status'] = {'observedGeneration': 2, **{field: row['spec']['replicas']
                    for field in ('replicas', 'readyReplicas', 'updatedReplicas', 'availableReplicas')}}
            documents[runtime_resource_path(row) + '/' + meta['name']] = row
            for version, resource, kind in (('apps/v1', 'replicasets', 'ReplicaSet'), ('v1', 'pods', 'Pod'), ('batch/v1', 'jobs', 'Job')):
                prefix = '/api/v1' if version == 'v1' else '/apis/' + version
                documents.setdefault(prefix + '/namespaces/' + meta['namespace'] + '/' + resource,
                    {'apiVersion': version, 'kind': kind + 'List', 'metadata': {'resourceVersion': '2'}, 'items': []})
        populate_runtime_pods(documents)

    def handle(message):
        assert message.method == 'GET', 'machine revocation must not replay Kubernetes stop writes'
        http_calls.append(message.url.path)
        value = copy.deepcopy(documents[message.url.path])
        if mode['retain_pod'] and message.url.path == '/api/v1/namespaces/' + retained_pod['metadata']['namespace'] + '/pods':
            value['items'].append(copy.deepcopy(retained_pod))
        return httpx.Response(200, json=value)

    children = {
        'recovery-fence': (fence.prepare_shared_recovery_fence, fence.validate_shared_recovery_fence),
        'recovery-drain': (drain.prepare_shared_recovery_drain, drain.validate_shared_recovery_drain),
        'recovery-stop': (stop.prepare_shared_recovery_workloads, stop.validate_shared_recovery_workloads),
        'recovery-revoke': (machines.prepare_shared_recovery_machines, machines.validate_shared_recovery_machines),
    }
    with ExitStack() as stack:
        class API(SharedPhaseAPI):
            fence_api = None
            machine_api = None
            machines_done = False

            def prepare(self, phase, bound, directory):
                if phase == 'forward-guards':
                    guards.prepare_shared_pool_guards(bindings=bound, state_dir=directory)
                else:
                    children[phase][0](bindings=bound, state_dir=directory)

            def validate_prepared_phase(self, phase, directory):
                if phase == 'forward-guards':
                    guards.validate_shared_pool_guards(bindings=bindings, state_dir=directory, complete=False)
                else:
                    children[phase][1](bindings=bindings, state_dir=directory, complete=False, prepared=True)

            def validate_phase(self, phase, directory, *, complete):
                if phase == 'forward-guards':
                    guards.validate_shared_pool_guards(bindings=bindings, state_dir=directory, complete=complete)
                else:
                    children[phase][1](bindings=bindings, state_dir=directory, complete=complete)

            def advance(self, phase, bound, directory):
                nonlocal retained_pod
                if phase == self.block:
                    return False
                if phase == 'recovery-fence':
                    self.fence_api = SharedPoolRecoveryFenceTransport(bindings=bound, kubeconfig=guard_api.kubeconfig,
                        executable=Path(guard_api.prefix[0]), qualify=remote.verify_retained)
                    return fence.advance_shared_recovery_fence(bindings=bound, api=self.fence_api, state_dir=directory)
                if phase == 'recovery-drain':
                    return drain.advance_shared_recovery_drain(bindings=bound, guards=self.fence_api.guards,
                        history=self.fence_api.history, state_dir=directory, qualify=remote.verify_retained)
                if phase == 'recovery-stop':
                    # Deliberately double the earlier stop effects, not the
                    # current HTTP process-drain or SQL transaction under test.
                    sources = stop._sources(bound, state)
                    inventory({key: choices[0] for key, choices in sources.items()})
                    retained_pod = copy.deepcopy(next(pod for row in documents.values() if row['kind'] == 'PodList' for pod in row['items']))
                    record = _read(directory / 'stop.json')
                    stopped = {}
                    for key, choices in sources.items():
                        before = _snapshot(choices[0])
                        before['metadata']['uid'] = _uid(choices[0])
                        expected = stop._stopped(bound, before)
                        record['resources'][key] = {'status': 'stopped', 'before': before,
                            'before_resource_version': '1', 'expected': expected}
                        stopped[key] = copy.deepcopy(expected)
                        stopped[key]['metadata']['uid'] = _uid(before)
                    private_state._atomic_json(directory / 'stop.json', record)
                    inventory(stopped)
                    return True
                assert phase == 'recovery-revoke'
                if self.machine_api is None:
                    def current_drain():
                        return drain.observe_shared_recovery_drain(bindings=bound, guards=self.fence_api.guards,
                            history=self.fence_api.history, state_dir=state, qualify=remote.verify_retained)
                    observer = stack.enter_context(stop.HTTPSSharedRecoveryWorkloads(bindings=bound,
                        api_server=runtime.database.foundation.inputs.config['kubernetes_api_server'],
                        ssl_context=ssl.create_default_context(), qualify_runtime=lambda *_: remote.verify_retained(),
                        recovery_drained=current_drain, observe_ready=lambda *_: True))
                    connection_type = type(observer.client)
                    observer.client.close()
                    observer.client = connection_type(base_url=observer.api_server, transport=httpx.MockTransport(handle))
                    self.machine_api = machines.SharedRecoveryMachineTransport(bindings=bound, workloads=observer,
                        guards=self.fence_api.guards, history=self.fence_api.history)
                self.machines_done = machines.advance_shared_recovery_machines(bindings=bound, api=self.machine_api, state_dir=directory)
                return False  # Gateway RBAC retirement is still required; never restore staging here.

        api = API(parent)
        api.block = 'forward-guards'
        kwargs = {**args, 'bindings': bindings, 'api': api, 'action': 'install'}
        assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-guards'
        api.block = 'recovery-revoke'
        kwargs['action'] = 'recover'
        assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'recovery-revoke'
        api.block = None
        remote.pool_counts['active_requests'] = 1
        assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'recovery-revoke'
        assert remote.dispatches == 0 and _read(child / 'machines.json')['status'] == 'prepared'
        remote.pool_counts['active_requests'] = 0
        mode['retain_pod'] = True
        assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'recovery-revoke'
        assert remote.dispatches == 0 and _read(child / 'machines.json')['status'] == 'prepared'
        mode['retain_pod'] = False
        # Exercise the sealed transport independently of the controller's
        # earlier drain: work arriving after its machine read must remain a
        # known pre-dispatch wait, without publishing intent.
        remote.busy_after_machine_read = True
        assert api.machine_api.retire_machines(record_intent=lambda: pytest.fail('busy transport must not publish intent')) is False
        assert remote.dispatches == 0 and _read(child / 'machines.json')['status'] == 'prepared'
        remote.pool_counts['active_requests'] = 0
        remote.open_during_qualification = True
        with pytest.raises(ValueError):
            api.machine_api.machine_authority()
        remote.local[roster[0]] = 'fenced'
        receipt = child / 'machines.json'
        raw = receipt.read_bytes()
        remote.change_receipt_after_machine_read = True
        with pytest.raises(ValueError):
            api.machine_api.machine_authority()
        receipt.write_bytes(raw)
        displaced = receipt.with_suffix('.held')
        receipt.rename(displaced)
        with pytest.raises(ValueError):
            parent.run_shared_pool_operation(**kwargs)
        assert remote.dispatches == 0 and not receipt.exists()
        displaced.rename(receipt)
        remote.machines = 'revoked'
        with pytest.raises(ValueError):
            parent.run_shared_pool_operation(**kwargs)  # Cannot adopt unexplained revocation without this child's intent.
        assert remote.dispatches == 0
        remote.machines = 'active'
        assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'recovery-revoke'
        assert remote.dispatches == 1 and api.machines_done is committed
        assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'recovery-revoke'
        assert remote.dispatches == 1  # Neither an unknown nor a completed transaction is reissued.
        remote.machines = 'revoked'  # The deferred transaction may now have committed.
        assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'recovery-revoke'
        assert api.machines_done is True and remote.dispatches == 1
        machines.validate_shared_recovery_machines(bindings=bindings, state_dir=child, complete=True)
        assert http_calls and _read(state / 'operation.json')['recovery']['recovery-restore-workloads']['status'] == 'prepared'


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
