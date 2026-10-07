"""Exact predecessor restart remains behind recovery admission fences."""
from __future__ import annotations

import copy
import json
from types import SimpleNamespace

import pytest
from scripts.ops.nebius_ingress_stage import _key
from scripts.ops.nebius_management_switch import _stable
from tests.ops.test_nebius_pool_role_restoration import RoleAPI, restore_roles, templates_restored
from tests.ops.test_nebius_pool_role_restoration import closed_startup as closed_startup
from tests.ops.test_nebius_pool_role_restoration import collector_inputs as collector_inputs
from tests.ops.test_nebius_pool_role_restoration import (
    cutover_binding_inventory as cutover_binding_inventory,
)
from tests.ops.test_nebius_pool_role_restoration import cutover_inputs as cutover_inputs
from tests.ops.test_nebius_pool_role_restoration import fencing_inputs as fencing_inputs
from tests.ops.test_nebius_pool_role_restoration import management_inputs as management_inputs
from tests.ops.test_nebius_pool_role_restoration import platform_inputs as platform_inputs
from tests.ops.test_nebius_pool_role_restoration import retirement_inputs as retirement_inputs
from tests.ops.test_nebius_pool_role_restoration import runtime_inputs as runtime_inputs


class RestartAPI(RoleAPI):
    def __init__(self, fixture, prior):
        super().__init__(fixture, prior)
        self.legacy_roles = copy.deepcopy(prior.legacy_roles)
        self.restart_calls, self.restart_failure = [], None

    def qualify_gateway_readonly(self):
        if not self.effective_readonly:
            raise ValueError('private-marker')

    def preview_legacy_restart(self, key, before, desired):
        assert before == self.startup.documents[key]
        return copy.deepcopy(desired)

    def restart_legacy_workload(self, key, before, desired, *, record_intent):
        from scripts.ops.nebius_pool_legacy_restart import qualify_legacy_restart
        from scripts.ops.nebius_pool_template_restoration import RecoveryDrainPending

        pending = qualify_legacy_restart(self.request, self, state=self.state, anchor=self.root / 'cutover-anchor')
        if pending is not None:
            return RecoveryDrainPending(pending)
        before = self.read_workload(key)
        record_intent(before)
        assert json.loads((self.state / 'legacy-restart.json').read_bytes())['workloads'][key] == {
            'phase': 'intent', 'before_resource_version': before['metadata']['resourceVersion']}
        assert self.machine_phase == 'revoked' and self.mode == 'fenced' and set(self.guards.values()) == {'fenced'}
        self.restart_calls.append(key)
        if self.restart_failure == 'before':
            raise OSError('private-marker')
        if self.restart_failure == 'conflict':
            return False
        current = copy.deepcopy(before)
        field = 'suspend' if current['kind'] == 'CronJob' else 'replicas'
        current['spec'][field] = desired['spec'][field]
        current['metadata']['resourceVersion'] = str(int(current['metadata']['resourceVersion']) + 1)
        self.startup.documents[key] = current
        if self.restart_failure == 'after':
            raise OSError('private-marker')
        return True


def roles_restored(fixture):
    prior = templates_restored(fixture)
    assert restore_roles(prior)['status'] == 'pool_legacy_roles_restored_closed'
    return RestartAPI(fixture, prior)


def restart(api):
    from scripts.ops.nebius_pool_legacy_restart import restart_pool_legacy

    return restart_pool_legacy(request=api.request, api=api, state_dir=api.state, anchor_dir=api.root / 'cutover-anchor')


@pytest.mark.timeout(300)
def test_restart_boundary_rejects_authority_drift_during_final_drain(closed_startup, monkeypatch):
    from scripts.ops.nebius_pool_legacy_restart import qualify_legacy_restart

    api = roles_restored(closed_startup)
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
        qualify_legacy_restart(api.request, api, state=api.state, anchor=api.root / 'cutover-anchor')
    assert drains == 2 and not api.restart_calls


@pytest.mark.timeout(300)
def test_legacy_restart_restores_exact_original_scalars_and_keeps_gateway_dormant_and_guards_closed(closed_startup):
    from scripts.ops.nebius_pool_retirement import retirement_documents

    api = roles_restored(closed_startup)
    before, roles = copy.deepcopy(api.startup.documents), copy.deepcopy(api.legacy_roles)
    originals = {**retirement_documents(api.request.fencing.retirement),
        **{_key(row): row for row in (api.request.manager, *api.request.services)}}
    result = restart(api)
    assert result['status'] == 'pool_legacy_restart_staged_closed'
    assert result['legacy_restore_allowed'] is False and result['runtime_verified'] is False
    expected_calls = set()
    for key, row in api.startup.documents.items():
        expected = copy.deepcopy(before[key])
        if key in originals:
            field = 'suspend' if row['kind'] == 'CronJob' else 'replicas'
            expected['spec'][field] = originals[key]['spec'][field]
            # Template restoration already canonicalizes Kubernetes quantities;
            # restart must preserve that spec byte-for-byte except this scalar.
            assert _stable(row)['spec'] == _stable(originals[key])['spec']
        if _stable(expected) != _stable(before[key]):
            expected_calls.add(key)
            expected['metadata']['resourceVersion'] = str(int(before[key]['metadata']['resourceVersion']) + 1)
        assert row == expected
    assert set(api.restart_calls) == expected_calls and len(api.restart_calls) == len(expected_calls)
    assert api.legacy_roles == roles and set(api.guards.values()) == {'fenced'} and api.mode == 'fenced'
    assert restart(api) == result and len(api.restart_calls) == len(expected_calls)


@pytest.mark.parametrize('failure', ['before', 'after', 'conflict'])
@pytest.mark.timeout(300)
def test_legacy_restart_unknown_replies_observe_without_repeating_the_update(closed_startup, failure):
    from scripts.ops.nebius_pool_startup import startup_workload_options

    api = roles_restored(closed_startup)
    api.restart_failure = failure
    result = restart(api)
    api.restart_failure = None
    if failure == 'before':
        assert result['status'] == 'pending_legacy_restart_outcome'
        assert restart(api) == result and len(api.restart_calls) == 1
        key, = api.restart_calls
        choices = startup_workload_options(api.request, state_dir=api.state, anchor_dir=api.root / 'cutover-anchor')
        assert len(choices[key]) == 2
        actual = api.startup.documents[key]
        actual['spec'] = copy.deepcopy(choices[key][1]['spec'])
        with pytest.raises(ValueError):
            restart(api)  # No new version: not an observed restart CAS.
        actual['metadata']['resourceVersion'] = str(int(actual['metadata']['resourceVersion']) + 1)
    elif failure == 'conflict':
        assert result['status'] == 'pending_legacy_restart_update'
    assert restart(api)['status'] == 'pool_legacy_restart_staged_closed'
    assert len(api.restart_calls) == len(set(api.restart_calls)) + int(failure == 'conflict')


@pytest.mark.timeout(300)
def test_legacy_restart_rejects_changed_or_incomplete_authority_before_any_write(closed_startup):
    api = roles_restored(closed_startup)
    role_journal = api.state / 'role-restoration.json'
    saved = role_journal.read_bytes()
    role_journal.write_text('{}')
    with pytest.raises(ValueError):
        restart(api)
    role_journal.write_bytes(saved)
    for attribute, bad in [('machine_phase', 'active'), ('mode', 'global'), ('effective_readonly', False), ('effective_legacy', False)]:
        prior = getattr(api, attribute)
        setattr(api, attribute, bad)
        with pytest.raises(ValueError):
            restart(api)
        setattr(api, attribute, prior)
    participant = next(iter(api.guards))
    api.guards[participant] = 'open'
    with pytest.raises(ValueError):
        restart(api)
    api.guards[participant] = 'fenced'
    api.cleanup_drained = False
    assert restart(api)['status'] == 'pending_pool_cleanup'
    api.cleanup_drained = True
    api.processes_drained = False
    assert restart(api)['status'] == 'pending_successor_drain'
    assert not api.restart_calls


@pytest.mark.parametrize('artifact', ['journal', 'anchor'])
def test_orphan_legacy_restart_evidence_cannot_fall_back_to_original_startup(cutover_inputs, tmp_path, artifact):
    from scripts.ops.nebius_pool_startup import startup_workload_options

    request, _ = cutover_inputs
    state, anchor = tmp_path / 'restart-state', tmp_path / 'restart-anchor'
    state.mkdir()
    anchor.mkdir()
    assert startup_workload_options(request, state_dir=state, anchor_dir=anchor) is None
    operation = request.fencing.retirement.migration.registration.spec.operation_id
    path = state / 'legacy-restart.json' if artifact == 'journal' else anchor / (str(operation) + '-legacy-restart.json')
    path.write_text('{}')
    path.chmod(0o600)
    with pytest.raises((ValueError, OSError)):
        startup_workload_options(request, state_dir=state, anchor_dir=anchor)


def legacy_runtime_readiness_case(closed_startup):
    from scripts.ops.nebius_pool_activation_live import HTTPSPoolActivationAPI

    qualify = HTTPSPoolActivationAPI.qualify_legacy_runtimes
    api = roles_restored(closed_startup)
    api.anchor = api.root / 'cutover-anchor'
    assert restart(api)['status'] == 'pool_legacy_restart_staged_closed'
    request, migration = api.request, api.request.fencing.retirement.migration
    manager_key = _key(request.manager)
    probes, failure = [], None
    originals = [*(row.controller for row in migration.guards), *request.services, *request.fencing.retirement.actuators]
    journal = api.state / 'legacy-restart.json'
    receipt = journal.read_bytes()
    workloads = copy.deepcopy(api.startup.documents)
    calls = list(api.restart_calls)

    def probe(kind, original, expected):
        assert expected == api.startup.documents[_key(original)]
        assert expected['metadata']['uid'] == original['metadata']['uid']
        assert _stable(expected)['spec'] == _stable(original)['spec']
        assert expected['spec']['replicas'] == 1
        probes.append((kind, _key(original)))
        if failure == kind:
            raise ValueError('private-runtime-marker')
        if kind == 'telemetry':
            if failure == 'late_guard':
                api.guards[str(migration.guards[0].participant_id)] = 'open'
            elif failure == 'late_runtime':
                api.startup.documents[manager_key]['spec']['replicas'] = 0

    def database(target, *, original, expected, credential_uid, credential_resource_version):
        binding = target.database
        actuator = original['metadata']['namespace'] != target.namespace
        assert (credential_uid, credential_resource_version) == (
            (binding.actuator_credential_uid, binding.actuator_credential_resource_version) if actuator
            else (binding.credential_uid, binding.credential_resource_version))
        probe('database', original, expected)

    api.parent = SimpleNamespace(history=SimpleNamespace(
        qualify_binding=lambda actual, manager: (actual == migration and manager == request.manager) or pytest.fail('changed authority'),
        qualify_manager_database=lambda *, expected: probe('manager', request.manager, expected),
        qualify_manager_legacy_settings=lambda *, expected: probe('manager_settings', request.manager, expected)),
        guards=SimpleNamespace(qualify_runtime_database=database,
            qualify_runtime_legacy_settings=lambda target, *, original, expected: probe('settings', original, expected),
            qualify_runtime_telemetry=lambda target, *, original, expected: probe('telemetry', original, expected)))
    api._legacy_runtime_probes = lambda expected: HTTPSPoolActivationAPI._legacy_runtime_probes(api, expected)
    assert qualify(api) is None
    expected_probes = {('manager', manager_key), ('manager_settings', manager_key),
        *(('database', _key(row)) for row in originals), *(('settings', _key(row)) for row in originals),
        *(('telemetry', _key(row)) for row in request.fencing.retirement.actuators)}
    assert len(probes) == len(expected_probes) and set(probes) == expected_probes
    for failure in ('manager', 'manager_settings', 'database', 'settings', 'telemetry', 'late_guard', 'late_runtime', 'journal'):
        probes.clear()
        if failure == 'journal':
            broken = json.loads(receipt)
            broken['workloads'][manager_key]['phase'] = 'intent'
            journal.write_text(json.dumps(broken))
        with pytest.raises(ValueError, match='pool_legacy_runtimes_unqualified'):
            qualify(api)
        if failure == 'journal':
            assert not probes
        api.guards = {str(row.participant_id): 'fenced' for row in migration.guards}
        api.startup.documents = copy.deepcopy(workloads)
        journal.write_bytes(receipt)
    assert api.mode == 'fenced' and api.machine_phase == 'revoked'
    assert api.restart_calls == calls and journal.read_bytes() == receipt
