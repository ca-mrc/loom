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
