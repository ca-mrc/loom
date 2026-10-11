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
def test_shared_recovery_reopens_only_staging_once_and_requires_fresh_terminal_health(
        shared_operation, shared_guard_transport, shared_runtime, completed_pool, handoff, monkeypatch, committed):
    """Real reopening/DB adapters; prior effects and runtime/provider probes doubled."""
    import importlib.util
    from types import SimpleNamespace

    from scripts.ops import nebius_certificates as private_state
    from scripts.ops import nebius_shared_pool_guards as guards
    from scripts.ops import nebius_shared_pool_operation as parent
    from scripts.ops import nebius_shared_pool_recovery_drain as drain
    from scripts.ops import nebius_shared_pool_recovery_fence as fence
    from scripts.ops import nebius_shared_pool_recovery_gateway as gateway
    from scripts.ops import nebius_shared_pool_recovery_machines as machines
    from scripts.ops import nebius_shared_pool_recovery_restart as restart
    from scripts.ops import nebius_shared_pool_recovery_roles as roles
    from scripts.ops import nebius_shared_pool_recovery_templates as templates
    from scripts.ops import nebius_shared_pool_recovery_workloads as stop
    from scripts.ops.nebius_development_runtime_install import _read
    from scripts.ops.nebius_ingress_stage import _snapshot, _uid
    from scripts.ops.nebius_management_switch import _stable
    from scripts.ops.nebius_pool_origin_history import KubectlPoolHistoryAPI
    from scripts.ops.nebius_shared_pool_forward_processes import _gateway_history_target

    name = 'scripts.ops.nebius_shared_pool_recovery_reopening'
    assert importlib.util.find_spec(name) is not None, 'staging-only recovery reopening is missing'
    module = __import__(name, fromlist=['SharedRecoveryReopeningTransport'])
    args, bindings = shared_operation
    manager = args['runtime'].database.manager.retained.request.retained
    state = Path(manager.operation['state_dir']).parent / 'shared-pool-handoff'
    child = state / 'recovery-reopen'
    staging_id = str(bindings.plan.staging.attachment.participant.participant_id)
    roster = tuple(str(target.participant_id) for target in bindings.migration.guards)
    dev_id, = (key for key in roster if key != staging_id)

    class Remote:
        mode = 'fenced'
        machines = 'revoked'
        healthy = False
        late_busy = False
        late_guard = False
        change_receipt = False

        def __init__(self):
            self.local = dict.fromkeys(roster, 'fenced')
            self.reads, self.dispatches = [], []
            self.pool_counts = dict.fromkeys(('unstarted_requests', 'active_requests', 'unconfirmed_creates', 'unqualified_releases'), 0)
            self.local_counts = {key: dict.fromkeys(('trials', 'executions', 'builds', 'build_cleanup',
                'execution_outboxes', 'build_outboxes'), 0) for key in roster}

        def verify_retained(self):
            pass  # Full-parent writer/provider checks are a separate integration boundary.

        def ready(self):
            if self.late_busy:
                self.late_busy = False
                self.local_counts[staging_id]['build_outboxes'] = 1
            if self.late_guard:
                self.late_guard = False
                self.local[dev_id] = 'open'
            if self.change_receipt:
                self.change_receipt = False
                receipt = state / 'recovery-restart/restart.json'
                receipt.write_bytes(receipt.read_bytes() + b'\n')
            return self.healthy

        def after_machine_observation(self):
            pass

        def pool_state(self):
            return self.mode

        def guard_state(self, participant):
            return self.local[participant]

        def release_staging(self, participant):
            assert participant == staging_id, 'recovery must never reopen the cancelled dev participant'
            assert _read(child / 'reopen.json')['status'] == 'intent'
            assert self.mode == 'fenced' and self.machines == 'revoked' and self.healthy
            assert set(self.local.values()) == {'fenced'}
            assert not any(self.pool_counts.values()) and not any(self.local_counts[participant].values())
            self.dispatches.append(participant)
            if committed:
                self.local[participant] = 'open'
                self.local_counts[participant]['trials'] = 1  # Newly admitted staging work is allowed.
            raise TimeoutError('simulated lost recovery release reply')

    remote = Remote()
    guard_api, database_model = shared_guard_transport
    _shared_recovery_database_io(remote=remote, bindings=bindings, guard_api=guard_api, database_model=database_model,
        shared_runtime=shared_runtime, completed_pool=completed_pool, handoff=handoff, monkeypatch=monkeypatch)
    credential = guard_api._get('secret', 'loom-platform-db', bindings.migration.registration.binding.namespace)
    history = KubectlPoolHistoryAPI(request=bindings.migration, target=_gateway_history_target(bindings, credential),
        kubeconfig=guard_api.kubeconfig, executable=Path(guard_api.prefix[0]))
    children = {
        'forward-guards': (guards.prepare_shared_pool_guards, guards.validate_shared_pool_guards, 'guards.json'),
        'recovery-fence': (fence.prepare_shared_recovery_fence, fence.validate_shared_recovery_fence, 'fence.json'),
        'recovery-drain': (drain.prepare_shared_recovery_drain, drain.validate_shared_recovery_drain, 'drain.json'),
        'recovery-stop': (stop.prepare_shared_recovery_workloads, stop.validate_shared_recovery_workloads, 'stop.json'),
        'recovery-restore-workloads': (templates.prepare_shared_recovery_templates, templates.validate_shared_recovery_templates, 'templates.json'),
        'recovery-restore-roles': (roles.prepare_shared_recovery_roles, roles.validate_shared_recovery_roles, 'roles.json'),
        'recovery-restart': (restart.prepare_shared_recovery_restart, restart.validate_shared_recovery_restart, 'restart.json'),
        'recovery-reopen': (module.prepare_shared_recovery_reopening, module.validate_shared_recovery_reopening, 'reopen.json'),
        'recovery-verify': (module.prepare_shared_recovery_verification, module.validate_shared_recovery_verification, 'verify.json'),
    }

    class API(SharedPhaseAPI):
        reopen_api = None
        interrupt_verification = False
        completed_readers_checked = False

        def qualify(self, bound, record, *, recovering):
            super().qualify(bound, record, recovering=recovering)
            path = state / 'recovery-verify/verify.json'
            if (self.interrupt_verification and path.exists() and _read(path)['status'] == 'observed'
                    and record['recovery']['recovery-verify']['status'] == 'started'):
                self.interrupt_verification = False
                raise TimeoutError('simulated interruption after saving verification')

        def prepare(self, phase, bound, directory):
            if phase == 'recovery-revoke':
                machines.prepare_shared_recovery_machines(bindings=bound, state_dir=directory)
                gateway.prepare_shared_recovery_gateway(bindings=bound, state_dir=directory)
            else:
                children[phase][0](bindings=bound, state_dir=directory)

        def validate_prepared_phase(self, phase, directory):
            if phase in {'forward-guards', 'recovery-revoke'}:
                self.validate_phase(phase, directory, complete=False)
            else:
                children[phase][1](bindings=bindings, state_dir=directory, complete=False, prepared=True)

        def validate_phase(self, phase, directory, *, complete):
            if phase == 'recovery-revoke':
                machines.validate_shared_recovery_machines(bindings=bindings, state_dir=directory, complete=complete)
                gateway.validate_shared_recovery_gateway(bindings=bindings, state_dir=directory, complete=complete)
            else:
                children[phase][1](bindings=bindings, state_dir=directory, complete=complete)

        def advance(self, phase, bound, directory):
            if phase == self.block:
                return False
            if phase == 'recovery-revoke':
                path = directory / 'machines.json'
                private_state._atomic_json(path, {**_read(path), 'status': 'revoked'})
                path = directory / 'gateway.json'
                record = _read(path)
                for row in record['roles'].values():
                    row['status'] = 'absent'
                private_state._atomic_json(path, record)
                return True
            if phase in {'recovery-reopen', 'recovery-verify'}:
                if not self.completed_readers_checked:
                    _exercise_completed_recovery_readers(bound)
                    self.completed_readers_checked = True
                if self.reopen_api is None:
                    self.reopen_api = module.SharedRecoveryReopeningTransport(bindings=bound, guards=guard_api,
                        history=history, qualify=remote.verify_retained,
                        restart=SimpleNamespace(bindings=bound, state=state / 'recovery-restart', ready=remote.ready))
                function = module.advance_shared_recovery_reopening if phase == 'recovery-reopen' else module.advance_shared_recovery_verification
                return function(bindings=bound, api=self.reopen_api, state_dir=directory)
            # Earlier phase effects are doubled; their real validators still
            # qualify these journals. This is not a full-parent acceptance test.
            path = directory / children[phase][2]
            record = _read(path)
            if phase == 'recovery-fence':
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
            elif phase in {'recovery-restore-workloads', 'recovery-restart'}:
                component = templates if phase == 'recovery-restore-workloads' else restart
                before, targets = component._inputs(bound, state)
                for key, row in record['resources'].items():
                    row.update(status='restored' if component is templates else 'started',
                        before_resource_version='2' if _stable(before[key]) != _stable(targets[key]) else None)
            elif phase == 'recovery-restore-roles':
                for row in record['roles'].values():
                    row.update(status='restored', before_resource_version='201')
            else:
                pytest.fail('unexpected predecessor phase: ' + phase)
            private_state._atomic_json(path, record)
            return True

    api = API(parent)
    api.block = 'forward-guards'
    kwargs = {**args, 'bindings': bindings, 'api': api, 'action': 'install'}
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-guards'
    frozen = {path: path.read_bytes() for path in state.glob('forward-*/**/*.json')}
    api.block, kwargs['action'] = 'recovery-verify', 'recover'
    try:
        result = parent.run_shared_pool_operation(**kwargs)
    except ValueError as exc:
        import traceback

        frames, cause = [], exc
        while cause is not None:
            frames.extend(f'{type(cause).__name__}:{Path(frame.filename).name}:{frame.lineno}:{frame.name}'
                for frame in traceback.extract_tb(cause.__traceback__))
            cause = cause.__context__
        raise AssertionError('reopening failed at ' + ', '.join(frames)) from None
    assert result['phase'] == 'recovery-reopen'
    assert not remote.dispatches and _read(child / 'reopen.json')['status'] == 'prepared'
    remote.healthy, remote.late_busy = True, True
    assert api.reopen_api.release_staging(record_intent=lambda: pytest.fail('busy release must not consume intent')) is False
    assert not remote.dispatches and _read(child / 'reopen.json')['status'] == 'prepared'
    remote.local_counts[staging_id]['build_outboxes'] = 0
    remote.late_guard = True
    with pytest.raises(ValueError):
        api.reopen_api.observe()
    remote.local[dev_id] = 'fenced'
    receipt = state / 'recovery-restart/restart.json'
    raw = receipt.read_bytes()
    remote.change_receipt = True
    try:
        with pytest.raises(ValueError):
            api.reopen_api.observe()
    finally:
        receipt.write_bytes(raw)
    receipt = child / 'reopen.json'
    displaced = receipt.with_suffix('.held')
    receipt.rename(displaced)
    with pytest.raises(ValueError):
        parent.run_shared_pool_operation(**kwargs)
    assert not receipt.exists() and not remote.dispatches
    displaced.rename(receipt)
    remote.local[staging_id] = 'open'
    with pytest.raises(ValueError):
        parent.run_shared_pool_operation(**kwargs)  # An open guard cannot be adopted before intent.
    remote.local[staging_id] = 'fenced'
    for _ in range(2):
        result = parent.run_shared_pool_operation(**kwargs)
        assert result['phase'] == ('recovery-verify' if committed else 'recovery-reopen')
        assert remote.dispatches == [staging_id]
        api.reopen_api = None  # A reconstructed transport must not redispatch either outcome.
    remote.local[staging_id] = 'open'  # Resolve the deferred commit without replay.
    remote.local_counts[staging_id]['trials'] = 1
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'recovery-verify'
    api.block, remote.healthy = None, False
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'recovery-verify'
    assert _read(state / 'recovery-verify/verify.json')['status'] == 'prepared'
    remote.healthy = True
    remote.local_counts[dev_id]['build_cleanup'] = 1
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'recovery-verify'
    remote.local_counts[dev_id]['build_cleanup'] = 0
    remote.machines = 'active'
    with pytest.raises(ValueError):
        parent.run_shared_pool_operation(**kwargs)
    remote.machines = 'revoked'
    api.interrupt_verification = True
    with pytest.raises(ValueError):
        parent.run_shared_pool_operation(**kwargs)
    assert _read(state / 'recovery-verify/verify.json')['status'] == 'observed'
    remote.healthy = False
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'recovery-verify'
    remote.healthy = True
    remote.reads.clear()
    result = parent.run_shared_pool_operation(**kwargs)
    assert result['status'] == 'staging_restored_dev_closed' and result['admission_open'] is False
    assert remote.local == {staging_id: 'open', dev_id: 'fenced'}
    assert remote.mode == 'fenced' and remote.machines == 'revoked'
    assert remote.dispatches == [staging_id] and staging_id not in remote.reads
    assert 'pool' in remote.reads and dev_id in remote.reads
    assert all(path.read_bytes() == raw for path, raw in frozen.items())
    with pytest.raises(ValueError):
        module.advance_shared_recovery_reopening(bindings=bindings, api=api.reopen_api, state_dir=child)
    assert remote.dispatches == [staging_id]
