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


@pytest.mark.timeout(240)
def test_shared_recovery_runtime_inventory_reaches_nested_probes_without_staging_scope(
        shared_operation, shared_runtime, completed_pool, handoff):
    """Real anchored stop receipts and inventory; only remote phase effects/HTTP are doubled."""
    import ssl

    import httpx
    from scripts.ops import nebius_certificates as private_state
    from scripts.ops import nebius_shared_pool_guards as guards
    from scripts.ops import nebius_shared_pool_operation as parent
    from scripts.ops import nebius_shared_pool_recovery_drain as drain
    from scripts.ops import nebius_shared_pool_recovery_fence as fence
    from scripts.ops import nebius_shared_pool_recovery_workloads as stop
    from scripts.ops import nebius_shared_pool_runtime_observation as runtime_observation
    from scripts.ops.nebius_development_runtime_install import _read
    from scripts.ops.nebius_development_runtime_live import runtime_resource_path
    from scripts.ops.nebius_development_runtime_observation import HTTPSDevelopmentRuntimeObserver
    from scripts.ops.nebius_ingress_stage import _snapshot, _uid
    from tests.ops.test_nebius_development_pool_retained import (
        populate_runtime_pods,
        runtime_http_inventory,
    )

    assert callable(getattr(runtime_observation, 'shared_recovery_runtime_view', None)), 'recovery runtime inventory is missing'
    args, bindings = shared_operation
    runtime, runtime_parent = shared_runtime
    completed = bindings.plan.development.runtime
    manager = runtime.database.manager.retained.request.retained
    state = Path(manager.operation['state_dir']).parent / 'shared-pool-handoff'
    children = {
        'forward-guards': (guards.prepare_shared_pool_guards, guards.validate_shared_pool_guards, 'guards.json'),
        'recovery-fence': (fence.prepare_shared_recovery_fence, fence.validate_shared_recovery_fence, 'fence.json'),
        'recovery-drain': (drain.prepare_shared_recovery_drain, drain.validate_shared_recovery_drain, 'drain.json'),
        'recovery-stop': (stop.prepare_shared_recovery_workloads, stop.validate_shared_recovery_workloads, 'stop.json'),
    }

    class API(SharedPhaseAPI):
        def prepare(self, phase, bound, directory):
            children[phase][0](bindings=bound, state_dir=directory)

        def validate_prepared_phase(self, phase, directory):
            if phase == 'forward-guards':
                record = children[phase][1](bindings=bindings, state_dir=directory, complete=False)
                assert set(record['guards'].values()) == {'prepared'}
            else:
                children[phase][1](bindings=bindings, state_dir=directory, complete=False, prepared=True)

        def validate_phase(self, phase, directory, *, complete):
            children[phase][1](bindings=bindings, state_dir=directory, complete=complete)

        def advance(self, phase, bound, directory):
            if phase in {'forward-guards', 'recovery-stop'}:
                return False
            path = directory / children[phase][2]
            record = _read(path)
            if phase == 'recovery-fence':
                record['pool'] = 'fenced'
                record['guards'].update(dict.fromkeys(record['guards'], 'fenced'))
            else:
                assert phase == 'recovery-drain'
                record['status'] = 'observed'
            private_state._atomic_json(path, record)
            return True

    api = API(parent)
    kwargs = {**args, 'bindings': bindings, 'api': api}
    assert parent.run_shared_pool_operation(**kwargs, action='install')['phase'] == 'forward-guards'
    assert parent.run_shared_pool_operation(**kwargs, action='recover')['phase'] == 'recovery-stop'
    sources = stop._sources(bindings, state)
    dev_keys = [key for key in sources if key in completed.resources
        and completed.resources[key]['kind'] == 'Deployment'
        and completed.resources[key]['metadata']['name'] != 'loom-pool-gateway']
    assert len(dev_keys) >= 2
    stopped_key, nested_key = dev_keys[0], dev_keys[-1]
    calls, documents, handler = runtime_http_inventory(runtime, runtime_parent, completed_pool[3].server, handoff[2])
    journal = state / 'recovery-stop/stop.json'
    record = _read(journal)
    for key, choices in sources.items():
        original = choices[0]
        before = _snapshot(original)
        before['metadata']['uid'] = _uid(original)
        expected = stop._stopped(bindings, before)
        record['resources'][key].update(status='intent' if key == stopped_key else 'stopped',
            before=before, before_resource_version='500', expected=expected)
        if key == stopped_key:
            break
    private_state._atomic_json(journal, record)
    target = copy.deepcopy(record['resources'][stopped_key]['expected'])
    target['metadata']['uid'] = _uid(completed.resources[stopped_key])
    target_path = runtime_resource_path(target) + '/' + target['metadata']['name']
    assert target_path in documents
    documents[target_path] = target

    def connect(observer, read=handler):
        for connection in (observer, observer.manager, observer.foundation):
            connection.client.close()
            connection.client = type(completed_pool[3].api.client)(base_url=connection.api_server,
                transport=httpx.MockTransport(read))

    with HTTPSDevelopmentRuntimeObserver(request=runtime,
            api_server=runtime.database.foundation.inputs.config['kubernetes_api_server'],
            ssl_context=ssl.create_default_context()) as observer:
        connect(observer)
        with pytest.raises(ValueError):
            observer.inspect(state_dir=completed.state_dir)
        observed = observer.inspect(state_dir=completed.state_dir, _shared_handoff=bindings)
        assert observed[stopped_key]['spec']['replicas'] == 0
        assert observed[nested_key]['spec']['replicas'] == 1
        raw = journal.read_bytes()
        # Removing intent must make this live recovery marker unauthorized.
        record['resources'][stopped_key] = stop._prepared()
        private_state._atomic_json(journal, record)
        with pytest.raises(ValueError):
            observer.inspect(state_dir=completed.state_dir, _shared_handoff=bindings)
        journal.write_bytes(raw)
        documents[target_path]['metadata']['uid'] = str(uuid4())
        with pytest.raises(ValueError):
            observer.inspect(state_dir=completed.state_dir, _shared_handoff=bindings)
        documents[target_path]['metadata']['uid'] = _uid(completed.resources[stopped_key])

    populate_runtime_pods(documents)
    with runtime_observation.HTTPSSharedPoolRuntimeProbes(bindings=bindings,
            api_server=runtime.database.foundation.inputs.config['kubernetes_api_server'], token='fixture-token',
            ssl_context=ssl.create_default_context(cadata=runtime.database.foundation.bootstrap['material']['loom-platform-db']['ca.crt'])) as probes:
        connect(probes)
        _, current, pod = probes._running(nested_key, completed.state_dir)
        assert _key(current) == nested_key and pod['status']['phase'] == 'Running'
        changed = []

        def changing_read(message):
            response = handler(message)
            if not changed:
                journal.write_bytes(raw + b'\n')
                changed.append(True)
            return response

        connect(probes, changing_read)
        with pytest.raises(ValueError):
            probes._running(nested_key, completed.state_dir)
        assert changed
        journal.write_bytes(raw)
    assert calls and all(message.method == 'GET' for message in calls)
    staging_namespaces = {row['metadata']['namespace'] for row in bindings.plan.staging.originals.values()}
    assert all('/namespaces/' + namespace + '/' not in message.url.path
        for message in calls for namespace in staging_namespaces)


@pytest.mark.timeout(240)
@pytest.mark.parametrize('connection', ['protocol', 'database'])
@pytest.mark.parametrize('forward_point', ['partial-acquire', 'partial-release'])
def test_shared_recovery_fence_preserves_forward_history_and_observes_unknown_writes(
        shared_operation, forward_point, connection, shared_guard_transport, shared_runtime, completed_pool, handoff, monkeypatch):
    """Own recovery authority; only remote transactions and other phases doubled."""
    import importlib.util

    from scripts.ops import nebius_certificates as private_state
    from scripts.ops import nebius_shared_pool_activation as activation
    from scripts.ops import nebius_shared_pool_guards as guards
    from scripts.ops import nebius_shared_pool_operation as parent
    from scripts.ops.nebius_development_runtime_install import _read

    name = 'scripts.ops.nebius_shared_pool_recovery_fence'
    assert importlib.util.find_spec(name) is not None, 'shared terminal recovery fence is missing'
    module = __import__(name, fromlist=['advance_shared_recovery_fence'])
    live = None
    if connection == 'database':
        name = 'scripts.ops.nebius_shared_pool_recovery_fence_live'
        assert importlib.util.find_spec(name) is not None, 'fixed shared recovery transactions are missing'
        live = __import__(name, fromlist=['SharedPoolRecoveryFenceTransport'])
    args, bindings = shared_operation
    manager = args['runtime'].database.manager.retained.request.retained
    state = Path(manager.operation['state_dir']).parent / 'shared-pool-handoff'
    child = state / 'recovery-fence'
    roster = tuple(str(target.participant_id) for target in bindings.migration.guards)
    effects = []

    class Remote:
        migration = bindings.migration
        mode = 'closed' if forward_point == 'partial-acquire' else 'global'
        local = dict.fromkeys(roster, 'open' if forward_point == 'partial-acquire' else 'held')
        local[roster[0]] = 'held' if forward_point == 'partial-acquire' else 'open'
        checks = 0
        damage = None

        def verify_retained(self):
            self.checks += 1

        def qualify_runtime(self):
            pytest.fail('terminal fencing must remain possible with an unhealthy gateway')

        def pool_state(self):
            if self.damage == 'receipt-bytes':
                path = child / 'fence.json'
                path.write_bytes(path.read_bytes() + b'\n')
            return self.mode

        def guard_state(self, participant):
            return self.local[participant]

        def fence_pool(self):
            assert _read(child / 'fence.json')['pool'] == 'intent'
            assert (Path(manager.operation['anchor_dir']) / 'shared-pool-recovery.json').exists()
            effects.append(('pool', None))
            raise TimeoutError('simulated unconfirmed pool fence')

        def fence_guard(self, participant):
            assert self.mode == 'fenced'
            assert _read(child / 'fence.json')['guards'][participant] == 'intent'
            effects.append(('guard', participant))
            if participant == roster[0]:
                self.local[participant] = 'fenced'
            raise TimeoutError('simulated committed or uncommitted guard fence reply loss')

    remote = Remote()
    guard_api, database_model = shared_guard_transport
    if connection == 'database':
        _shared_recovery_database_io(remote=remote, bindings=bindings, guard_api=guard_api, database_model=database_model,
            shared_runtime=shared_runtime, completed_pool=completed_pool, handoff=handoff, monkeypatch=monkeypatch)

    class API(SharedPhaseAPI):
        interrupted = False
        fence_api = remote if connection == 'protocol' else None

        def prepare(self, phase, bound, directory):
            if phase == 'forward-guards':
                guards.prepare_shared_pool_guards(bindings=bound, state_dir=directory)
            elif phase == 'forward-activate':
                activation.prepare_shared_pool_activation(bindings=bound, state_dir=directory)
            elif phase == 'recovery-fence':
                module.prepare_shared_recovery_fence(bindings=bound, state_dir=directory)
                if not self.interrupted:
                    self.interrupted = True
                    raise TimeoutError('simulated interrupted local preparation')
            else:
                super().prepare(phase, bound, directory)

        def validate_prepared_phase(self, phase, directory):
            if phase == 'forward-guards':
                assert set(guards.validate_shared_pool_guards(bindings=bindings, state_dir=directory, complete=False)['guards'].values()) == {'prepared'}
            elif phase == 'forward-activate':
                activation.validate_shared_pool_activation(bindings=bindings, state_dir=directory, complete=False, prepared=True)
            elif phase == 'recovery-fence':
                module.validate_shared_recovery_fence(bindings=bindings, state_dir=directory, complete=False, prepared=True)
            else:
                super().validate_prepared_phase(phase, directory)

        def validate_phase(self, phase, directory, *, complete):
            if phase == 'forward-guards':
                guards.validate_shared_pool_guards(bindings=bindings, state_dir=directory, complete=complete)
            elif phase == 'forward-activate':
                activation.validate_shared_pool_activation(bindings=bindings, state_dir=directory, complete=complete)
            elif phase == 'recovery-fence':
                module.validate_shared_recovery_fence(bindings=bindings, state_dir=directory, complete=complete)
            else:
                super().validate_phase(phase, directory, complete=complete)

        def advance(self, phase, bound, directory):
            if phase == 'forward-guards':
                path = directory / 'guards.json'
                record = _read(path)
                record['guards'].update(dict.fromkeys(roster, 'held') if forward_point == 'partial-release'
                    else {roster[0]: 'acquire_intent'})
                private_state._atomic_json(path, record)
                return forward_point == 'partial-release'
            if phase == 'forward-activate':
                path = directory / 'activation.json'
                record = _read(path)
                record['opening'] = 'opened'
                record['guards'][roster[0]]['release'] = 'intent'
                private_state._atomic_json(path, record)
                return False
            if phase == 'recovery-fence':
                if self.fence_api is None:
                    self.fence_api = live.SharedPoolRecoveryFenceTransport(bindings=bound,
                        kubeconfig=guard_api.kubeconfig, executable=Path(guard_api.prefix[0]), qualify=remote.verify_retained)
                return module.advance_shared_recovery_fence(bindings=bound, api=self.fence_api, state_dir=directory)
            return super().advance(phase, bound, directory)

    api = API(parent)
    kwargs = {**args, 'bindings': bindings, 'api': api, 'action': 'install'}
    expected = 'forward-guards' if forward_point == 'partial-acquire' else 'forward-activate'
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == expected
    frozen = {path: path.read_bytes() for path in state.glob('forward-*/*') if path.is_file()}
    with pytest.raises((ValueError, FileNotFoundError)):
        module.prepare_shared_recovery_fence(bindings=bindings, state_dir=child)
    assert not child.exists() and effects == []
    kwargs['action'] = 'recover'
    with pytest.raises(ValueError):
        parent.run_shared_pool_operation(**kwargs)
    assert child.exists() and effects == []  # Only local preparation was interrupted.
    original = remote.local[roster[0]]
    remote.local[roster[0]] = 'foreign'
    with pytest.raises(ValueError):
        parent.run_shared_pool_operation(**kwargs)
    assert effects == []
    remote.local[roster[0]] = original
    api.fence_api.migration = None
    checks = remote.checks
    with pytest.raises(ValueError):
        parent.run_shared_pool_operation(**kwargs)
    assert remote.checks == checks and effects == []
    api.fence_api.migration = bindings.migration
    previous = remote.mode
    remote.mode = 'global' if previous == 'closed' else 'closed'
    with pytest.raises(ValueError):
        parent.run_shared_pool_operation(**kwargs)
    assert effects == []
    remote.mode = previous
    try:
        result = parent.run_shared_pool_operation(**kwargs)
    except Exception as error:
        import traceback
        cause = error
        while cause is not None:
            print(type(cause).__name__, [(row.name, row.lineno) for row in traceback.extract_tb(cause.__traceback__)])
            cause = cause.__context__
        raise
    assert result['phase'] == 'recovery-fence'
    assert effects == [('pool', None)]
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'recovery-fence'
    assert effects == [('pool', None)]  # An unknown fence is never retried.
    receipt = child / 'fence.json'
    saved, checks = receipt.read_bytes(), remote.checks
    receipt.unlink()
    with pytest.raises(ValueError):
        parent.run_shared_pool_operation(**kwargs)
    assert remote.checks == checks and not receipt.exists()
    receipt.write_bytes(saved)
    receipt.chmod(0o600)
    remote.damage = 'receipt-bytes'
    with pytest.raises(ValueError):
        parent.run_shared_pool_operation(**kwargs)
    assert effects == [('pool', None)]
    receipt.write_bytes(saved)
    receipt.chmod(0o600)
    remote.damage = None
    remote.mode = 'fenced'  # A delayed pool-fence commit is now observable.
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'recovery-fence'
    expected_effects = [('pool', None), *(('guard', key) for key in roster)]
    assert effects == expected_effects and remote.local[roster[0]] == 'fenced'
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'recovery-fence'
    assert effects == expected_effects
    remote.local[roster[1]] = 'fenced'
    api.block = 'recovery-drain'
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'recovery-drain'
    assert effects == expected_effects
    module.validate_shared_recovery_fence(bindings=bindings, state_dir=child, complete=True)
    assert all(path.read_bytes() == raw for path, raw in frozen.items())
    with pytest.raises(ValueError):
        module.advance_shared_recovery_fence(bindings=bindings, api=api.fence_api, state_dir=child)
    assert effects == expected_effects


@pytest.mark.timeout(240)
def test_shared_recovery_drain_reads_all_databases_fresh_before_restoration(
        shared_operation, shared_guard_transport, shared_runtime, completed_pool, handoff, monkeypatch):
    """A prior zero cannot authorize stopping writers while cleanup is active."""
    import importlib.util

    from scripts.ops import nebius_shared_pool_guards as guards
    from scripts.ops import nebius_shared_pool_operation as parent
    from scripts.ops import nebius_shared_pool_recovery_fence as fence
    from scripts.ops.nebius_shared_pool_recovery_fence_live import SharedPoolRecoveryFenceTransport

    name = 'scripts.ops.nebius_shared_pool_recovery_drain'
    assert importlib.util.find_spec(name) is not None, 'connected shared recovery drain is missing'
    drain = __import__(name, fromlist=['advance_shared_recovery_drain'])
    args, bindings = shared_operation
    manager = args['runtime'].database.manager.retained.request.retained
    state = Path(manager.operation['state_dir']).parent / 'shared-pool-handoff'
    child = state / 'recovery-drain'
    roster = tuple(str(target.participant_id) for target in bindings.migration.guards)

    class Remote:
        mode = 'closed'
        damage = None

        def __init__(self):
            self.local = dict.fromkeys(roster, 'open')
            self.reads, self.effects = [], []
            self.pool_counts = {'unstarted_requests': 1, 'active_requests': 0, 'unconfirmed_creates': 0, 'unqualified_releases': 0}
            self.local_counts = {key: {'trials': 0, 'executions': 0, 'builds': 0, 'build_cleanup': 0,
                'execution_outboxes': 0, 'build_outboxes': 0} for key in roster}

        def verify_retained(self):
            if self.damage == 'authority':
                raise ValueError('simulated retained authority change')
            if self.damage == 'late-guard' and self.reads:
                self.local[roster[0]] = 'open'

        def pool_state(self):
            if self.damage == 'receipt' and self.reads:
                path = child / 'drain.json'
                path.write_bytes(path.read_bytes() + b'\n')
            return self.mode

        def guard_state(self, participant):
            return self.local[participant]

        def fence_pool(self):
            self.effects.append('pool')
            self.mode = 'fenced'

        def fence_guard(self, participant):
            self.effects.append(participant)
            self.local[participant] = 'fenced'

    remote = Remote()
    guard_api, database_model = shared_guard_transport
    _shared_recovery_database_io(remote=remote, bindings=bindings, guard_api=guard_api, database_model=database_model,
        shared_runtime=shared_runtime, completed_pool=completed_pool, handoff=handoff, monkeypatch=monkeypatch)

    class API(SharedPhaseAPI):
        fence_api = None

        def prepare(self, phase, bound, directory):
            if phase == 'forward-guards':
                guards.prepare_shared_pool_guards(bindings=bound, state_dir=directory)
            elif phase == 'recovery-fence':
                fence.prepare_shared_recovery_fence(bindings=bound, state_dir=directory)
            elif phase == 'recovery-drain':
                drain.prepare_shared_recovery_drain(bindings=bound, state_dir=directory)
            else:
                super().prepare(phase, bound, directory)

        def validate_prepared_phase(self, phase, directory):
            if phase in {'forward-guards', 'recovery-fence', 'recovery-drain'}:
                self.validate_phase(phase, directory, complete=False)
            else:
                super().validate_prepared_phase(phase, directory)

        def validate_phase(self, phase, directory, *, complete):
            if phase == 'forward-guards':
                guards.validate_shared_pool_guards(bindings=bindings, state_dir=directory, complete=complete)
            elif phase == 'recovery-fence':
                fence.validate_shared_recovery_fence(bindings=bindings, state_dir=directory, complete=complete)
            elif phase == 'recovery-drain':
                drain.validate_shared_recovery_drain(bindings=bindings, state_dir=directory, complete=complete)
            else:
                super().validate_phase(phase, directory, complete=complete)

        def advance(self, phase, bound, directory):
            if phase == 'forward-guards':
                return False
            if phase == 'recovery-fence':
                if self.fence_api is None:
                    self.fence_api = SharedPoolRecoveryFenceTransport(bindings=bound, kubeconfig=guard_api.kubeconfig,
                        executable=Path(guard_api.prefix[0]), qualify=remote.verify_retained)
                return fence.advance_shared_recovery_fence(bindings=bound, api=self.fence_api, state_dir=directory)
            if phase == 'recovery-drain':
                return drain.advance_shared_recovery_drain(bindings=bound, guards=self.fence_api.guards,
                    history=self.fence_api.history, state_dir=directory, qualify=remote.verify_retained)
            return super().advance(phase, bound, directory)

    api = API(parent)
    kwargs = {**args, 'bindings': bindings, 'api': api, 'action': 'install'}
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-guards'
    api.block = 'recovery-stop'
    kwargs['action'] = 'recover'
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'recovery-drain'
    assert remote.effects == ['pool', *roster] and remote.reads == ['pool', *roster]
    receipt = child / 'drain.json'
    saved = receipt.read_bytes()
    for target, key in ((remote.pool_counts, 'unconfirmed_creates'),
            (remote.local_counts[roster[0]], 'execution_outboxes'), (remote.local_counts[roster[1]], 'build_cleanup')):
        remote.pool_counts['unstarted_requests'] = 0
        target[key] = 1
        remote.reads.clear()
        assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'recovery-drain'
        assert remote.reads == ['pool', *roster] and receipt.read_bytes() == saved
        target[key] = 0
    for damage in ('authority', 'receipt', 'late-guard'):
        remote.reads.clear()
        remote.damage = damage
        with pytest.raises(ValueError):
            parent.run_shared_pool_operation(**kwargs)
        assert remote.effects == ['pool', *roster]
        if damage == 'authority':
            assert remote.reads == []
        receipt.write_bytes(saved)
        receipt.chmod(0o600)
        remote.local[roster[0]] = 'fenced'
    remote.damage = None
    remote.reads.clear()
    api.block = 'recovery-stop'
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'recovery-stop'
    drain.validate_shared_recovery_drain(bindings=bindings, state_dir=child, complete=True)
    remote.pool_counts['unconfirmed_creates'] = 1
    remote.reads.clear()
    assert drain.observe_shared_recovery_drain(bindings=bindings, guards=api.fence_api.guards,
        history=api.fence_api.history, state_dir=state, qualify=remote.verify_retained) is False
    assert remote.reads == ['pool', *roster] and remote.effects == ['pool', *roster]
    remote.pool_counts['unconfirmed_creates'] = 0
    api.block = None
    assert parent.run_shared_pool_operation(**kwargs)['status'] == 'staging_restored_dev_closed'
    drain.validate_shared_recovery_drain(bindings=bindings, state_dir=child, complete=True)
    with pytest.raises(ValueError):
        drain.advance_shared_recovery_drain(bindings=bindings, guards=api.fence_api.guards,
            history=api.fence_api.history, state_dir=child, qualify=remote.verify_retained)
    reads = list(remote.reads)
    with pytest.raises(ValueError):
        drain.observe_shared_recovery_drain(bindings=bindings, guards=api.fence_api.guards,
            history=api.fence_api.history, state_dir=state, qualify=remote.verify_retained)
    assert remote.reads == reads
