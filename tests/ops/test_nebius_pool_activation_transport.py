"""Activation recovery binds retained DBs without needing application processes."""
from __future__ import annotations

from uuid import uuid4

import pytest
from tests.ops.test_nebius_pool_database_guard import database_guard as database_guard
from tests.ops.test_nebius_pool_origin_history import management_history as management_history
from tests.ops.test_nebius_pool_runtime import runtime_inputs as runtime_inputs
from tests.unit.test_nebius_management_render import management_inputs as management_inputs
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


@pytest.mark.parametrize('action,status', [('observe', 'closed'), ('observe', 'global'), ('observe', 'fenced'), ('fence', 'fenced')])
def test_pool_activation_recovery_uses_retained_management_database(management_history, action, status):
    from loom_service.pool_management.capacity import digest

    api, state = management_history
    spec = api.request.registration.spec
    state.report = {'schema': 'loom.pool-activation-state.v1', 'operation_id': str(spec.operation_id),
        'installation_sha256': digest(spec.model_dump(mode='json')), 'state': status}
    assert api.activation_pool(action) == status
    command, = [row for row in state.calls if row[0] == 'exec']
    assert command[:7] == ['exec', '-n', state.target.namespace, 'pod/loom-postgres-0', '-c', 'loom-postgres', '--']
    assert all(row[0] in {'get', 'exec'} for row in state.calls)


@pytest.mark.parametrize('action,status', [('observe', 'held'), ('observe', 'open'), ('observe', 'fenced'),
    ('observe', 'foreign'), ('release', 'open'), ('fence', 'fenced')])
def test_local_activation_guard_uses_only_bound_database(database_guard, action, status):
    api, state = database_guard
    state.exec_hook = lambda query: {'schema': 'loom.pool-local-guard.v1',
        'operation_id': str(api.request.registration.spec.operation_id), 'participant_id': str(state.target.participant_id), 'status': status}
    assert api.activation_guard(state.target, action) == status
    assert sum(row[0] == 'exec' for row in state.calls) == 1


@pytest.mark.parametrize('scope', ['pool', 'guard'])
@pytest.mark.parametrize('damage', ['backend', 'credential', 'authority', 'late_authority', 'report', 'operation', 'extra', 'wrong_effect'])
def test_activation_transport_drift_is_unconfirmed_and_never_retried(management_history, database_guard, monkeypatch, scope, damage):
    from scripts.ops.nebius_pool_migration import PoolMigrationError

    from loom_service.pool_management.capacity import digest

    api, state = management_history if scope == 'pool' else database_guard
    spec = api.request.registration.spec
    report = ({'schema': 'loom.pool-activation-state.v1', 'operation_id': str(spec.operation_id),
        'installation_sha256': digest(spec.model_dump(mode='json')), 'state': 'fenced'} if scope == 'pool'
        else {'schema': 'loom.pool-local-guard.v1', 'operation_id': str(spec.operation_id),
            'participant_id': str(state.target.participant_id), 'status': 'fenced'})
    if damage == 'backend':
        state.after_drift = True
    elif damage == 'credential':
        state.secret['metadata']['resourceVersion'] = 'changed'
    elif damage == 'authority':
        api.kubeconfig.write_bytes(b'private-changed-authority')
    elif damage == 'report':
        report['schema'] = 'foreign'
    elif damage == 'operation':
        report['operation_id'] = str(uuid4())
    elif damage == 'extra':
        report['extra'] = 'private-marker'
    elif damage == 'wrong_effect':
        report['state' if scope == 'pool' else 'status'] = 'global' if scope == 'pool' else 'held'
    if scope == 'pool':
        state.report = report
    else:
        state.exec_hook = lambda query: report
    original = api._run
    def run(args):
        result = original(args)
        if damage == 'late_authority' and args[0] == 'exec':
            api.kubeconfig.write_bytes(b'private-changed-authority')
        return result
    monkeypatch.setattr(api, '_run', run)
    with pytest.raises(PoolMigrationError) as error:
        if scope == 'pool':
            api.activation_pool('fence')
        else:
            api.activation_guard(state.target, 'fence')
    assert 'private-' not in str(error.value)
    assert sum(row[0] == 'exec' for row in state.calls) == (0 if damage in {'credential', 'authority'} else 1)


def test_active_role_inspection_rechecks_operator_authority_after_sql(database_guard):
    from scripts.ops.nebius_pool_migration import PoolMigrationError

    api, state = database_guard
    def inspect(query):
        api.kubeconfig.write_bytes(b'private-changed-authority')
        return {'status': 'qualified'}
    state.exec_hook = inspect
    with pytest.raises(PoolMigrationError):
        api.runtime_role(state.target, 'inspect')
    assert sum(row[0] == 'exec' for row in state.calls) == 1


@pytest.mark.parametrize('action,status', [('observe', 'active'), ('observe', 'revoked'), ('revoke', 'revoked')])
@pytest.mark.parametrize('damage', [None, 'backend', 'authority', 'late_authority', 'report', 'lost_reply'])
def test_machine_retirement_transport_uses_retained_database_without_retry(management_history, monkeypatch, action, status, damage):
    from scripts.ops.nebius_pool_machine_database import pool_machine_retirement_sql
    from scripts.ops.nebius_pool_migration import PoolMigrationError

    from loom_service.pool_management.capacity import digest

    api, state = management_history
    spec = api.request.registration.spec
    state.report = {'schema': 'loom.pool-machine-retirement.v1', 'operation_id': str(spec.operation_id),
        'installation_sha256': digest(spec.model_dump(mode='json')), 'state': status}
    if damage == 'backend':
        state.after_drift = True
    elif damage == 'authority':
        api.kubeconfig.write_bytes(b'private-changed-authority')
    elif damage == 'report':
        state.report['state'] = 'foreign'
    original = api._run
    def run(args):
        value = original(args)
        if args[0] == 'exec':
            assert args[-1] == pool_machine_retirement_sql(spec, action=action)
            if damage == 'late_authority':
                api.kubeconfig.write_bytes(b'private-changed-authority')
            elif damage == 'lost_reply':
                raise OSError('private-marker')
        return value
    monkeypatch.setattr(api, '_run', run)
    if damage is None:
        assert api.machine_retirement(action) == status
    else:
        with pytest.raises(PoolMigrationError) as error:
            api.machine_retirement(action)
        assert 'private-' not in str(error.value)
    commands = [row for row in state.calls if row[0] == 'exec']
    assert len(commands) == (0 if damage == 'authority' else 1)
    if commands:
        assert commands[0][:7] == ['exec', '-n', state.target.namespace, 'pod/loom-postgres-0', '-c', 'loom-postgres', '--']


@pytest.mark.parametrize('scope', ['pool', 'guard'])
@pytest.mark.parametrize('damage', [None, 'pending', 'backend', 'authority', 'late_authority', 'report'])
def test_recovery_drain_reads_exact_retained_database_and_rejects_drift(management_history, database_guard, monkeypatch, scope, damage):
    from scripts.ops.nebius_pool_migration import PoolMigrationError

    from loom_service.pool_management.capacity import digest

    api, state = management_history if scope == 'pool' else database_guard
    spec = api.request.registration.spec
    report = {'schema': 'loom.pool-recovery-drain.v1' if scope == 'pool' else 'loom.pool-participant-drain.v1',
        'operation_id': str(spec.operation_id), 'read_only': True,
        'counts': ({'unstarted_requests': 0, 'active_requests': 0, 'unconfirmed_creates': 0, 'unqualified_releases': 0}
            if scope == 'pool' else {'trials': 0, 'executions': 0, 'builds': 0, 'build_cleanup': 0,
                'execution_outboxes': 0, 'build_outboxes': 0})}
    if scope == 'pool':
        report['installation_sha256'] = digest(spec.model_dump(mode='json'))
        state.report = report
    else:
        report.update(participant_id=str(state.target.participant_id), candidate_sha=api.request.registration.candidate['candidate_sha'])
        state.exec_hook = lambda query: report
    def run():
        return api.recovery_pool_drained() if scope == 'pool' else api.recovery_participant_drained(state.target)
    if damage == 'pending':
        report['counts'][next(iter(report['counts']))] = 1
    elif damage == 'backend':
        state.after_drift = True
    elif damage == 'authority':
        api.kubeconfig.write_bytes(b'private-changed-authority')
    elif damage == 'report':
        report['read_only'] = 1
    original = api._run
    def bound(args):
        result = original(args)
        if damage == 'late_authority' and args[0] == 'exec':
            api.kubeconfig.write_bytes(b'private-changed-authority')
        return result
    monkeypatch.setattr(api, '_run', bound)
    if damage in {None, 'pending'}:
        assert run() is (damage is None)
    else:
        with pytest.raises(PoolMigrationError) as error:
            run()
        assert 'private-' not in str(error.value)
    commands = [row for row in state.calls if row[0] == 'exec']
    assert len(commands) == (0 if damage == 'authority' else 1)
    if commands:
        assert commands[0][:7] == ['exec', '-n', state.target.namespace, 'pod/loom-postgres-0', '-c', 'loom-postgres', '--']
