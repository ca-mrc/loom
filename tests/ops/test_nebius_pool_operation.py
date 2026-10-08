"""One operation resumes real migration journals, never an earlier writer phase."""
from __future__ import annotations

import copy
import json
from types import SimpleNamespace

import pytest
from scripts.ops.nebius_ingress_stage import _key
from tests.ops.test_nebius_pool_cutover import CutoverAPI
from tests.ops.test_nebius_pool_cutover import collector_inputs as collector_inputs
from tests.ops.test_nebius_pool_cutover import (
    cutover_binding_inventory as cutover_binding_inventory,
)
from tests.ops.test_nebius_pool_cutover import cutover_inputs as cutover_inputs
from tests.ops.test_nebius_pool_cutover import fencing_inputs as fencing_inputs
from tests.ops.test_nebius_pool_cutover import retirement_inputs as retirement_inputs
from tests.ops.test_nebius_pool_legacy_reopening import ReopeningAPI
from tests.ops.test_nebius_pool_runtime import runtime_inputs as runtime_inputs
from tests.ops.test_nebius_pool_startup import StartupAPI
from tests.unit.test_nebius_management_render import management_inputs as management_inputs
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


@pytest.fixture
def operation(cutover_inputs, tmp_path, monkeypatch):
    from scripts.ops import nebius_pool_operation as target
    from tests.ops.test_nebius_pool_gateway_retirement import GatewayAPI
    from tests.ops.test_nebius_pool_legacy_restart import RestartAPI
    from tests.ops.test_nebius_pool_machine_retirement import MachineAPI
    from tests.ops.test_nebius_pool_role_restoration import RoleAPI
    from tests.ops.test_nebius_pool_template_restoration import TemplateAPI

    request, tokens = cutover_inputs
    parent = CutoverAPI(request)
    parent.state_dir, parent.anchor_dir, parent.refresh = tmp_path / 'cutover', tmp_path / 'cutover-anchor', None
    state = SimpleNamespace(parent=parent, startup=None, runtime=None, startup_failure=None)

    def startup(*, parent):
        assert parent is state.parent
        if state.startup is None:
            state.startup = StartupAPI(request, parent, parent.state_dir)
            state.startup.fail_key = _key(request.manager)
            state.startup.failure = state.startup_failure
        return state.startup

    class RemoteAPI(ReopeningAPI):
        def successor_drained(self, key, desired):
            assert desired['spec']['suspend' if desired['kind'] == 'CronJob' else 'replicas'] == (
                True if desired['kind'] == 'CronJob' else 0)
            return self.processes_drained

    def runtime(*, parent):
        assert parent is state.parent
        if state.runtime is None:
            fixture = request, tokens, parent, startup(parent=parent), None, tmp_path
            machine = MachineAPI(fixture)
            gateway = GatewayAPI(fixture, machine)
            template = TemplateAPI(fixture, gateway)
            roles = RoleAPI(fixture, template)
            restart = RestartAPI(fixture, roles)
            state.runtime = RemoteAPI(fixture, restart)
            state.runtime.anchor = parent.anchor_dir
        return state.runtime

    monkeypatch.setattr(target, 'HTTPSPoolStartupAPI', startup)
    monkeypatch.setattr(target, 'HTTPSPoolActivationAPI', runtime)
    state.run = lambda action='install': target.run_pool_operation(parent=parent, tokens=tokens, action=action)
    state.connect_runtime = lambda: runtime(parent=parent)
    return state


@pytest.mark.timeout(420)
def test_fixed_operation_closes_starts_opens_completes_and_replays_without_mutation(operation):
    from scripts.ops.nebius_pool_operation import PoolOperationError

    state = operation
    assert state.run('preflight') == {'status': 'preflight_qualified',
        'operation_id': str(state.parent.request.fencing.retirement.migration.registration.spec.operation_id)}
    assert not state.parent.state_dir.exists() and not state.parent.anchor_dir.exists()
    result = state.run()
    assert result['status'] == 'pool_cutover_completed' and result['outcome'] == 'global'
    assert result['acceptance_verified'] is False
    assert state.runtime.mode == 'global' and set(state.runtime.guards.values()) == {'open'}
    assert state.startup.requests and state.runtime.runtime_checks == 1
    history = {path: path.read_bytes() for path in state.parent.state_dir.parent.rglob('*.json')}
    writes = copy.deepcopy((state.startup.requests, state.runtime.calls))
    state.runtime.ready = False  # Never reuse the closed-mode startup probe after opening.
    assert state.run() == result
    assert state.run('preflight')['status'] == 'preflight_qualified'
    with pytest.raises(PoolOperationError):
        state.run('rollback')  # Frozen global ancestry cannot be rewritten in place.
    assert (state.startup.requests, state.runtime.calls) == writes
    assert {path: path.read_bytes() for path in history} == history


@pytest.mark.timeout(420)
def test_fixed_operation_resumes_unknown_opening_without_restarting_or_repeating(operation):
    from scripts.ops.nebius_pool_operation import PoolOperationError

    state = operation
    state.startup_failure = 'conflict'
    assert state.run() == {'status': 'pending', 'phase': 'startup',
        'operation_id': str(state.parent.request.fencing.retirement.migration.registration.spec.operation_id)}
    state.startup.failure = None
    runtime = state.connect_runtime()
    runtime.failure = ('open', 'before')
    assert state.run()['phase'] == 'activation'
    writes = copy.deepcopy((state.startup.requests, runtime.calls))
    runtime.failure = None
    assert state.run()['phase'] == 'activation'
    assert (state.startup.requests, runtime.calls) == writes
    assert not (state.parent.state_dir / 'completion.json').exists()
    runtime.mode = 'global'  # The original transaction is now observed.
    assert state.run()['outcome'] == 'global'
    assert runtime.calls.count(('open', None)) == 1
    assert state.startup.requests == writes[0]
    runtime.retained = False
    with pytest.raises(PoolOperationError) as error:
        state.run()
    assert error.value.stage == 'pool_completion'


@pytest.mark.timeout(600)
def test_fixed_operation_rolls_back_partial_startup_and_resumes_pending_legacy_release(operation):
    from scripts.ops.nebius_pool_operation import PoolOperationError

    state = operation
    state.startup_failure = 'before'
    assert state.run()['phase'] == 'startup'
    assert len(state.startup.requests) == 1
    runtime = state.connect_runtime()
    first, second, *rest = runtime.guards
    runtime.release_failures = {second: 'before'}
    assert state.run('rollback')['phase'] == 'legacy-reopening'
    assert runtime.mode == 'fenced' and runtime.machine_phase == 'revoked'
    assert runtime.guards[first] == 'open' and runtime.guards[second] == 'fenced'
    writes = copy.deepcopy((runtime.calls, runtime.fence_calls, runtime.stop_calls,
        runtime.machine_calls, runtime.role_calls, runtime.template_calls, runtime.legacy_calls, runtime.restart_calls, runtime.releases))
    assert state.run('rollback')['phase'] == 'legacy-reopening'
    assert (runtime.calls, runtime.fence_calls, runtime.stop_calls, runtime.machine_calls, runtime.role_calls,
        runtime.template_calls, runtime.legacy_calls, runtime.restart_calls, runtime.releases) == writes
    with pytest.raises(PoolOperationError):
        state.run()  # An install replay must not reverse the recovery direction.
    runtime.guards[second] = 'open'
    runtime.busy.add(second)
    result = state.run('rollback')
    assert result['outcome'] == 'legacy' and result['acceptance_verified'] is False
    assert state.run('rollback') == result
    assert len(state.startup.requests) == 1 and runtime.releases == [first, second, *rest]
    assert not [call for call in runtime.calls if call[0] == 'open']


def _completed_template_resume(operation, monkeypatch):
    from scripts.ops import nebius_pool_operation as target

    state = operation
    state.startup_failure = 'before'
    assert state.run()['phase'] == 'startup'
    runtime = state.connect_runtime()
    operation_id = str(state.parent.request.fencing.retirement.migration.registration.spec.operation_id)
    # Stop after the real template stage completes, before any role journal.
    with monkeypatch.context() as setup:
        setup.setattr(target, 'restore_pool_roles', lambda **kwargs: {
            'status': 'pending_role_fixture', 'operation_id': operation_id})
        assert state.run('rollback')['phase'] == 'role-restoration'
    record = json.loads((state.parent.state_dir / 'template-restoration.json').read_bytes())
    assert all(item['phase'] == 'restored' for item in record['workloads'].values())
    assert not (state.parent.state_dir / 'role-restoration.json').exists()
    return state, runtime


@pytest.mark.timeout(300)
@pytest.mark.parametrize('failure', [None, 'retained', 'cleanup', 'processes'])
def test_completed_template_resume_enters_fresh_role_qualification(operation, monkeypatch, failure):
    from scripts.ops import nebius_pool_operation as target
    from scripts.ops import nebius_pool_role_restoration as roles

    state, runtime = _completed_template_resume(operation, monkeypatch)
    calls = []
    template, qualify = target.restore_pool_templates, roles.qualify_role_restoration

    def template_stage(**kwargs):
        calls.append('template')
        return template(**kwargs)

    def role_qualification(*args, **kwargs):
        calls.append('role-qualification')
        return qualify(*args, **kwargs)

    monkeypatch.setattr(target, 'restore_pool_templates', template_stage)
    monkeypatch.setattr(roles, 'qualify_role_restoration', role_qualification)
    monkeypatch.setattr(runtime, 'preview_legacy_role', lambda *args: None)
    if failure == 'retained':
        runtime.retained = False
    elif failure == 'cleanup':
        runtime.cleanup_drained = False
    elif failure == 'processes':
        runtime.processes_drained = False
    if failure == 'retained':
        with pytest.raises(target.PoolOperationError) as error:
            state.run('rollback')
        assert error.value.stage == 'pool_role_restoration'
        assert calls == []  # Role observe rejects before the drain qualifier.
    else:
        result = state.run('rollback')
        assert result['phase'] == 'role-restoration' and result['status'] == 'pending'
        assert calls == ['role-qualification']
    path = state.parent.state_dir / 'role-restoration.json'
    assert path.exists() is (failure is None)
    if path.exists():
        assert all(row['phase'] == 'prepared' for row in json.loads(path.read_bytes())['roles'].values())
    assert not runtime.legacy_calls


@pytest.mark.timeout(300)
@pytest.mark.parametrize('failure', ['before', 'conflict'])
def test_unfinished_template_resume_keeps_original_stage_and_unknown_intent(operation, monkeypatch, failure):
    from scripts.ops import nebius_pool_operation as target

    state = operation
    state.startup_failure = 'before'
    assert state.run()['phase'] == 'startup'
    runtime = state.connect_runtime()
    runtime.template_failure = failure
    assert state.run('rollback')['phase'] == 'template-restoration'
    path = state.parent.state_dir / 'template-restoration.json'
    original = path.read_bytes()
    saved_calls = list(runtime.template_calls)
    assert saved_calls
    item = json.loads(original)['workloads'][saved_calls[-1]]
    assert item['phase'] == ('intent' if failure == 'before' else 'prepared')
    runtime.template_failure = None
    monkeypatch.setattr(runtime, 'preview_legacy_template', lambda *args: None)
    calls = []
    template = target.restore_pool_templates

    def template_stage(**kwargs):
        calls.append('template')
        return template(**kwargs)

    monkeypatch.setattr(target, 'restore_pool_templates', template_stage)
    assert state.run('rollback')['phase'] == 'template-restoration'
    assert calls == ['template'] and runtime.template_calls == saved_calls
    assert path.read_bytes() == original
    assert not (state.parent.state_dir / 'role-restoration.json').exists()


@pytest.mark.timeout(300)
@pytest.mark.parametrize('damage', ['template-restoration.json', 'shutdown.json', 'template-anchor'])
def test_completed_template_resume_rejects_damaged_evidence(operation, monkeypatch, damage):
    from scripts.ops.nebius_pool_operation import PoolOperationError

    state, runtime = _completed_template_resume(operation, monkeypatch)
    if damage == 'template-anchor':
        operation_id = state.parent.request.fencing.retirement.migration.registration.spec.operation_id
        path = state.parent.anchor_dir / (str(operation_id) + '-template-restoration.json')
    else:
        path = state.parent.state_dir / damage
    document = json.loads(path.read_bytes())
    document['unexpected'] = True
    path.write_text(json.dumps(document))
    with pytest.raises(PoolOperationError) as error:
        state.run('rollback')
    assert error.value.stage == 'pool_template_restoration'
    assert not (state.parent.state_dir / 'role-restoration.json').exists()
    assert not runtime.legacy_calls


@pytest.mark.timeout(600)
def test_shutdown_pending_receipt_distinguishes_fixed_reasons_without_exporting_unknown_text(operation, monkeypatch):
    from scripts.ops import nebius_pool_operation as target

    state = operation
    state.startup_failure = 'before'
    assert state.run()['phase'] == 'startup'
    operation_id = str(state.parent.request.fencing.retirement.migration.registration.spec.operation_id)
    for reason in ('pending_pool_cleanup', 'pending_shutdown_update',
            'pending_shutdown_outcome', 'pending_successor_drain', 'pending_private-marker'):
        monkeypatch.setattr(target, 'stop_pool_successors',
            lambda status=reason, **kwargs: {'status': status, 'operation_id': operation_id})
        result = state.run('rollback')
        assert result['status'] == 'pending' and result['phase'] == 'shutdown'
        if reason == 'pending_private-marker':
            assert 'pending_reason' not in result and 'private-marker' not in str(result)
        else:
            assert result['pending_reason'] == reason
        assert not (state.parent.state_dir / 'completion.json').exists()


def test_fixed_operation_refuses_concurrent_dispatch_or_unknown_action(operation):
    from scripts.ops import nebius_certificates as private_state
    from scripts.ops.nebius_pool_operation import PoolOperationError

    state = operation
    with pytest.raises(PoolOperationError):
        state.run('open')
    assert not state.parent.state_dir.exists()
    private_state._private_directory(state.parent.anchor_dir)
    with private_state._locked_state(state.parent.anchor_dir / 'dispatch'):
        with pytest.raises(PoolOperationError):
            state.run()
    assert not state.parent.state_dir.exists() and state.startup is None


@pytest.mark.parametrize(('message', 'stage'), [
    ('pool_retained_writer_binding_inventory_unqualified', 'writer_bindings'),
    ('pool_retained_writer_workload_inventory_unqualified', 'writer_workloads'),
    ('pool cutover connected prerequisites unqualified', 'connected_prerequisites'),
    ('pool cutover initial capacity unqualified', 'capacity'),
    ('pool cutover inputs changed', 'scope'),
    ('pool cutover namespace differs', 'scope'),
    ('pool_cutover_database_report_unqualified', 'database_report'),
    ('pool_cutover_pending_source_unqualified', 'pending_source'),
    ('pool_cutover_pending_page_unqualified', 'pending_page'),
    ('pool_management_history_origin_unqualified', 'origin_history'),
    ('private credential: never-export', None),
    ('pool_retained_writer_binding_inventory_unqualified private-value', None),
])
def test_preflight_reports_only_exact_fixed_failure_codes_without_writes(operation, monkeypatch, message, stage):
    from scripts.ops.nebius_pool_operation import PoolOperationError

    def fail(request):
        raise ValueError(message)

    monkeypatch.setattr(operation.parent, 'preflight', fail)
    with pytest.raises(PoolOperationError) as error:
        operation.run('preflight')
    assert error.value.stage == 'pool_preflight' + ('_' + stage if stage else '')
    assert 'private-value' not in str(error.value) and 'never-export' not in str(error.value)
    assert not operation.parent.state_dir.exists() and not operation.parent.anchor_dir.exists()


@pytest.mark.parametrize(('detail', 'stage'), [
    ('cutover_readiness', 'database_readiness'),
    ('management_origin_history', 'origin_history'),
    ('private-value', None),
])
def test_preflight_reports_only_allowlisted_database_adapter_stages(operation, monkeypatch, detail, stage):
    from scripts.ops.nebius_pool_migration import PoolMigrationError
    from scripts.ops.nebius_pool_operation import PoolOperationError

    def fail(request):
        raise PoolMigrationError(detail)

    monkeypatch.setattr(operation.parent, 'preflight', fail)
    with pytest.raises(PoolOperationError) as error:
        operation.run('preflight')
    assert error.value.stage == 'pool_preflight' + ('_' + stage if stage else '')
    assert not operation.parent.state_dir.exists() and not operation.parent.anchor_dir.exists()


def test_install_keeps_its_mutating_phase_even_for_recognized_preflight_error(operation, monkeypatch):
    from scripts.ops.nebius_pool_operation import PoolOperationError

    def fail(request):
        raise ValueError('pool_retained_writer_binding_inventory_unqualified')

    monkeypatch.setattr(operation.parent, 'preflight', fail)
    with pytest.raises(PoolOperationError) as error:
        operation.run('install')
    assert error.value.stage == 'pool_cutover'


@pytest.mark.parametrize(('damage', 'stage'), [('binding', 'writer_bindings'), ('database', 'database_report')])
def test_real_readonly_preflight_failure_reaches_the_protected_gateway(
        operation, cutover_inputs, cutover_binding_inventory, tmp_path, monkeypatch, damage, stage):
    import json

    from scripts.ops.nebius_management_gateway import safe_report
    from scripts.ops.nebius_pool_operation import PoolOperationError
    from tests.ops.test_nebius_management_gateway import pool_operation
    from tests.ops.test_nebius_pool_cutover import binding_preflight

    request, tokens = cutover_inputs
    if damage == 'binding':
        cutover_binding_inventory['rolebindings'][0]['subjects'] = []

    def invalid_database(target, *, after):
        return {'status': 'observed', 'schema_revision': '0000', 'rows': []}

    def preflight(actual):
        assert actual == request
        binding_preflight(request, tokens, cutover_binding_inventory,
            database_read=invalid_database if damage == 'database' else None)

    monkeypatch.setattr(operation.parent, 'preflight', preflight)
    with pytest.raises(PoolOperationError) as error:
        operation.run('preflight')
    metadata = pool_operation(tmp_path)
    report = {key: metadata[key] for key in ('source_sha', 'candidate', 'installation_id', 'namespace', 'operation_id')}
    report.update(status='blocked', stage=error.value.stage)
    assert safe_report(json.dumps(safe_report(json.dumps(report).encode(), metadata)).encode(), metadata)['stage'] == (
        'pool_preflight_' + stage)
    assert not operation.parent.state_dir.exists() and not operation.parent.anchor_dir.exists()
