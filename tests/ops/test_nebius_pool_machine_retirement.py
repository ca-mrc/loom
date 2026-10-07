"""Machine retirement follows fresh process/journal drain and never restores writers."""
from __future__ import annotations

import json

import pytest
from tests.ops.test_nebius_pool_shutdown import ShutdownAPI, cancelled, shutdown
from tests.ops.test_nebius_pool_shutdown import closed_startup as closed_startup
from tests.ops.test_nebius_pool_shutdown import collector_inputs as collector_inputs
from tests.ops.test_nebius_pool_shutdown import (
    cutover_binding_inventory as cutover_binding_inventory,
)
from tests.ops.test_nebius_pool_shutdown import cutover_inputs as cutover_inputs
from tests.ops.test_nebius_pool_shutdown import fencing_inputs as fencing_inputs
from tests.ops.test_nebius_pool_shutdown import management_inputs as management_inputs
from tests.ops.test_nebius_pool_shutdown import platform_inputs as platform_inputs
from tests.ops.test_nebius_pool_shutdown import retirement_inputs as retirement_inputs
from tests.ops.test_nebius_pool_shutdown import runtime_inputs as runtime_inputs


class MachineAPI(ShutdownAPI):
    def __init__(self, fixture):
        super().__init__(fixture)
        self.machine_phase = 'active'
        self.machine_calls = 0
        self.machine_failure = None

    def machine_authority(self):
        return self.machine_phase

    def retire_machines(self):
        assert json.loads((self.state / 'machine-retirement.json').read_bytes())['phase'] == 'intent'
        assert self.cleanup_drained and self.processes_drained and self.mode == 'fenced'
        self.machine_calls += 1
        if self.machine_failure == 'before':
            raise OSError('private-marker')
        self.machine_phase = 'revoked'
        if self.machine_failure == 'after':
            raise OSError('private-marker')


def stopped(fixture):
    prior = cancelled(fixture, started=False)
    assert shutdown(fixture, prior)['status'] == 'pool_successors_stopped'
    api = MachineAPI(fixture)
    api.mode, api.guards = prior.mode, prior.guards.copy()
    return api


def retire(fixture, api):
    from scripts.ops.nebius_pool_machine_retirement import retire_pool_machines

    request, _, _, _, _, root = fixture
    return retire_pool_machines(request=request, api=api, state_dir=root / 'cutover', anchor_dir=root / 'cutover-anchor')


def test_machine_boundary_rejects_authority_drift_during_final_drain(closed_startup, monkeypatch):
    from scripts.ops.nebius_pool_machine_retirement import qualify_machine_retirement_drain

    api = stopped(closed_startup)
    original = api.verify_retained
    drains = 0

    def drain():
        nonlocal drains
        drains += 1
        return True

    def verify():
        if drains == 2:
            raise ValueError('authority changed during final ledger read')
        return original()

    monkeypatch.setattr(api, 'recovery_drained', drain)
    monkeypatch.setattr(api, 'verify_retained', verify)
    with pytest.raises(ValueError):
        qualify_machine_retirement_drain(api.request, api, state=api.state, anchor=api.root / 'cutover-anchor')
    assert drains == 2 and not api.machine_calls


@pytest.mark.parametrize('failure', [None, 'before', 'after'])
def test_retirement_is_anchored_once_and_unknown_replies_only_observe(closed_startup, failure):
    api = stopped(closed_startup)
    api.machine_failure = failure
    result = retire(closed_startup, api)
    if failure == 'before':
        assert result['status'] == 'pending_machine_retirement'
        assert retire(closed_startup, api) == result and api.machine_calls == 1
        api.machine_phase = 'revoked'  # The original transaction becomes visible.
    assert retire(closed_startup, api) == {'status': 'pool_machines_retired',
        'operation_id': str(api.request.fencing.retirement.migration.registration.spec.operation_id),
        'legacy_restore_allowed': False}
    assert api.machine_calls == 1 and not api.stop_calls


@pytest.mark.parametrize('pending', ['cleanup', 'processes'])
def test_machine_retirement_requires_fresh_drain_not_previous_shutdown_result(closed_startup, pending):
    api = stopped(closed_startup)
    if pending == 'cleanup':
        api.cleanup_drained = False
    else:
        api.processes_drained = False
    assert retire(closed_startup, api)['status'] == ('pending_pool_cleanup' if pending == 'cleanup' else 'pending_successor_drain')
    assert api.machine_calls == 0 and api.machine_phase == 'active'


@pytest.mark.parametrize('damage', ['no_shutdown', 'unowned_revocation', 'pool', 'guard', 'anchor', 'shutdown'])
def test_machine_retirement_refuses_unqualified_recovery_without_mutation(closed_startup, damage):
    if damage == 'no_shutdown':
        api = MachineAPI(closed_startup)
    else:
        api = stopped(closed_startup)
    if damage == 'unowned_revocation':
        api.machine_phase = 'revoked'
    elif damage == 'pool':
        api.mode = 'global'
    elif damage == 'guard':
        api.guards[next(iter(api.guards))] = 'open'
    elif damage in {'anchor', 'shutdown'}:
        assert retire(closed_startup, api)['status'] == 'pool_machines_retired'
        api.machine_calls = 0
        if damage == 'anchor':
            next((api.root / 'cutover-anchor').glob('*-machine-retirement.json')).unlink()
        else:
            (api.state / 'shutdown.json').write_bytes(b'{}')
    with pytest.raises(ValueError) as error:
        retire(closed_startup, api)
    assert 'private-' not in str(error.value) and api.machine_calls == 0


@pytest.mark.parametrize('damage', ['state', 'operation', 'digest', 'extra'])
def test_machine_retirement_report_requires_exact_scope_and_known_state(damage):
    from scripts.ops.nebius_pool_machine_database import qualify_machine_retirement_report
    from tests.integration.test_nebius_pool_installation import installation

    from loom_service.pool_management.capacity import digest
    from loom_service.pool_management.installation import PoolInstallation

    spec = PoolInstallation.model_validate(installation()[0])
    report = {'schema': 'loom.pool-machine-retirement.v1', 'operation_id': str(spec.operation_id),
        'installation_sha256': digest(spec.model_dump(mode='json')), 'state': 'revoked'}
    assert qualify_machine_retirement_report(spec, report) == 'revoked'
    if damage == 'state':
        report['state'] = 'partial'
    elif damage == 'operation':
        report['operation_id'] = 'foreign'
    elif damage == 'digest':
        report['installation_sha256'] = '0' * 64
    else:
        report['private'] = 'unrequested'
    with pytest.raises(ValueError):
        qualify_machine_retirement_report(spec, report)
