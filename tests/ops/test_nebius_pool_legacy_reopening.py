"""Recovery reopening is journaled once and tolerates active opened owners."""
from __future__ import annotations

import copy
import json

import pytest
from scripts.ops.nebius_ingress_stage import _key
from tests.ops.test_nebius_pool_legacy_restart import RestartAPI, restart, roles_restored
from tests.ops.test_nebius_pool_legacy_restart import closed_startup as closed_startup
from tests.ops.test_nebius_pool_legacy_restart import collector_inputs as collector_inputs
from tests.ops.test_nebius_pool_legacy_restart import (
    cutover_binding_inventory as cutover_binding_inventory,
)
from tests.ops.test_nebius_pool_legacy_restart import cutover_inputs as cutover_inputs
from tests.ops.test_nebius_pool_legacy_restart import fencing_inputs as fencing_inputs
from tests.ops.test_nebius_pool_legacy_restart import management_inputs as management_inputs
from tests.ops.test_nebius_pool_legacy_restart import platform_inputs as platform_inputs
from tests.ops.test_nebius_pool_legacy_restart import retirement_inputs as retirement_inputs
from tests.ops.test_nebius_pool_legacy_restart import runtime_inputs as runtime_inputs


class ReopeningAPI(RestartAPI):
    def __init__(self, fixture, prior):
        super().__init__(fixture, prior)
        self.releases, self.local_drains, self.runtime_proofs = [], [], 0
        self.release_failures, self.busy = {}, set()
        self.runtime_ready, self.runtime_drift = True, None

    def pool_recovery_drained(self):
        return self.cleanup_drained

    def participant_recovery_drained(self, participant):
        assert self.guards[participant] == 'fenced', 'opened legacy work must not be subjected to the closed guard probe'
        self.local_drains.append(participant)
        return participant not in self.busy

    def qualify_legacy_runtimes(self):
        assert set(self.guards.values()) == {'fenced'}
        self.qualify_reopening_runtimes()

    def qualify_reopening_runtimes(self):
        self.runtime_proofs += 1
        if not self.runtime_ready:
            raise ValueError('private-runtime-marker')
        if self.runtime_drift == 'guard':
            self.guards[next(iter(self.guards))] = 'open'
        elif self.runtime_drift == 'workload':
            self.startup.documents[_key(self.request.manager)]['spec']['replicas'] = 0

    def release_recovery_guard(self, participant):
        record = json.loads((self.state / 'legacy-reopening.json').read_bytes())
        assert record['guards'][participant] == 'intent'
        assert self.guards[participant] == 'fenced' and self.mode == 'fenced' and self.machine_phase == 'revoked'
        self.releases.append(participant)
        failure = self.release_failures.get(participant)
        if failure == 'before':
            raise OSError('private-marker')
        self.guards[participant] = 'open'
        self.busy.add(participant)  # Existing queued work can immediately start.
        if failure == 'after':
            raise OSError('private-marker')


def restarted(fixture):
    api = roles_restored(fixture)
    assert restart(api)['status'] == 'pool_legacy_restart_staged_closed'
    return ReopeningAPI(fixture, api)


def reopen(api):
    from scripts.ops.nebius_pool_legacy_reopening import reopen_pool_legacy

    return reopen_pool_legacy(request=api.request, api=api, state_dir=api.state, anchor_dir=api.root / 'cutover-anchor')


@pytest.mark.timeout(420)
def test_reopening_settles_lost_replies_without_repeating_release_or_draining_open_owners(closed_startup):
    from scripts.ops.nebius_pool_legacy_reopening import reopen_pool_legacy

    assert callable(reopen_pool_legacy)
    api = restarted(closed_startup)
    participants = tuple(str(row.participant_id) for row in api.request.fencing.retirement.migration.guards)
    first, second, *rest = participants
    api.release_failures = {first: 'after', second: 'before'}
    original = {name: (api.state / name).read_bytes() for name in ('activation.json', 'legacy-restart.json')}
    result = reopen(api)
    assert result == {'status': 'pending_legacy_guard_release', 'operation_id': str(api.request.fencing.retirement.migration.registration.spec.operation_id),
        'legacy_admission_open': False, 'global_admission_open': False}
    assert api.releases == [first, second]
    assert api.guards[first] == 'open' and api.guards[second] == 'fenced'
    api.local_drains.clear()
    assert reopen(api) == result and api.releases == [first, second]
    assert first not in api.local_drains
    api.guards[second] = 'open'  # Only observation settles the original release.
    api.busy.add(second)
    api.local_drains.clear()
    result = reopen(api)
    assert result['status'] == 'pool_legacy_reopened' and result['legacy_admission_open'] is True
    assert result['global_admission_open'] is False and api.mode == 'fenced' and api.machine_phase == 'revoked'
    assert api.releases == [first, second, *rest]
    assert not {first, second}.intersection(api.local_drains)
    assert reopen(api) == result and api.releases == [first, second, *rest]
    assert set(api.guards.values()) == {'open'} and api.runtime_proofs >= len(participants) + 1
    assert {name: (api.state / name).read_bytes() for name in original} == original


@pytest.mark.timeout(420)
def test_reopening_refuses_unanchored_or_changed_authority_before_release(closed_startup):
    from scripts.ops.nebius_pool_legacy_reopening import reopen_pool_legacy

    assert callable(reopen_pool_legacy)
    api = restarted(closed_startup)
    api.runtime_ready = False
    with pytest.raises(ValueError, match='pool_legacy_reopening_unconfirmed'):
        reopen(api)
    assert not api.releases
    api.runtime_ready = True
    # Create prepared evidence without reaching a release by making the second
    # runtime proof fail (the first proves the initially fully closed runtime).
    qualify = api.qualify_reopening_runtimes
    def refuse_prepared():
        if (api.state / 'legacy-reopening.json').exists():
            raise ValueError('private-runtime-marker')
        qualify()
    api.qualify_reopening_runtimes = refuse_prepared
    with pytest.raises(ValueError, match='pool_legacy_reopening_unconfirmed'):
        reopen(api)
    api.qualify_reopening_runtimes = qualify
    journal = api.state / 'legacy-reopening.json'
    receipt, workloads = journal.read_bytes(), copy.deepcopy(api.startup.documents)
    marker = api.root / 'cutover-anchor' / (str(api.request.fencing.retirement.migration.registration.spec.operation_id) + '-legacy-reopening.json')
    restart_path = api.state / 'legacy-restart.json'
    saved_restart, saved_marker = restart_path.read_bytes(), marker.read_bytes()
    participant = next(iter(api.guards))
    for damage in ('restart', 'anchor', 'journal', 'pool', 'machine', 'gateway', 'legacy_rights', 'open', 'foreign', 'runtime', 'late_guard', 'late_workload'):
        if damage == 'restart':
            restart_path.write_text('{}')
        elif damage == 'anchor':
            marker.write_text('{}')
        elif damage == 'journal':
            journal.write_text('{}')
        elif damage == 'pool':
            api.mode = 'global'
        elif damage == 'machine':
            api.machine_phase = 'active'
        elif damage == 'gateway':
            api.effective_readonly = False
        elif damage == 'legacy_rights':
            api.effective_legacy = False
        elif damage in {'open', 'foreign'}:
            api.guards[participant] = damage
        elif damage == 'runtime':
            api.runtime_ready = False
        else:
            api.runtime_drift = 'guard' if damage == 'late_guard' else 'workload'
        with pytest.raises(ValueError, match='pool_legacy_reopening_unconfirmed'):
            reopen(api)
        assert not api.releases
        api.mode, api.machine_phase = 'fenced', 'revoked'
        api.guards[participant] = 'fenced'
        api.runtime_ready = api.effective_readonly = api.effective_legacy = True
        api.runtime_drift = None
        api.startup.documents = copy.deepcopy(workloads)
        restart_path.write_bytes(saved_restart)
        marker.write_bytes(saved_marker)
        journal.write_bytes(receipt)
    api.cleanup_drained = False
    assert reopen(api)['status'] == 'pending_pool_cleanup'
    api.cleanup_drained = True
    api.busy.add(participant)
    assert reopen(api)['status'] == 'pending_pool_cleanup'
    api.busy.clear()
    api.processes_drained = False
    assert reopen(api)['status'] == 'pending_successor_drain'
    assert not api.releases
