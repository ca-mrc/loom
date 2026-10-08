"""Fixed recovery dispatch connects anchored reopening to retained transports."""
from __future__ import annotations

import json
from types import SimpleNamespace

import pytest
from scripts.ops.nebius_ingress_stage import _key
from scripts.ops.nebius_management_switch import _stable
from tests.ops.test_nebius_pool_legacy_reopening import reopen, restarted
from tests.ops.test_nebius_pool_startup_live import closed_startup as closed_startup
from tests.ops.test_nebius_pool_startup_live import collector_inputs as collector_inputs
from tests.ops.test_nebius_pool_startup_live import cutover_inputs as cutover_inputs
from tests.ops.test_nebius_pool_startup_live import fencing_inputs as fencing_inputs
from tests.ops.test_nebius_pool_startup_live import management_inputs as management_inputs
from tests.ops.test_nebius_pool_startup_live import platform_inputs as platform_inputs
from tests.ops.test_nebius_pool_startup_live import retirement_inputs as retirement_inputs
from tests.ops.test_nebius_pool_startup_live import runtime_inputs as runtime_inputs
from tests.ops.test_nebius_pool_startup_live import (
    unbound_cutover_inputs as unbound_cutover_inputs,
)


class DispatchInterrupted(BaseException):
    """A process interruption that bypasses ordinary adapter error handling."""


def prepared_dispatch(fixture):
    from scripts.ops.nebius_pool_activation_live import HTTPSPoolActivationAPI

    api = restarted(fixture)
    qualify = api.qualify_reopening_runtimes
    def stop_before_release():
        if (api.state / 'legacy-reopening.json').exists():
            raise ValueError('runtime-not-ready')
        qualify()
    api.qualify_reopening_runtimes = stop_before_release
    with pytest.raises(ValueError, match='pool_legacy_reopening_unconfirmed'):
        reopen(api)
    api.qualify_reopening_runtimes = lambda: HTTPSPoolActivationAPI.qualify_reopening_runtimes(api)
    api._scope = lambda: None
    api._guard = lambda key: next(row for row in api.request.fencing.retirement.migration.guards
                                if str(row.participant_id) == key)
    api._legacy_runtime_probes = lambda expected: None
    writes = []
    def release(target):
        key = str(target.participant_id)
        assert json.loads((api.state / 'legacy-reopening.json').read_bytes())['guards'][key] == 'intent'
        writes.append(key)
        api.guards[key] = 'open'
        return 'open'
    api.parent = SimpleNamespace(guards=SimpleNamespace(release_recovery_guard=release))
    api.release_recovery_guard = lambda key, **kwargs: HTTPSPoolActivationAPI.release_recovery_guard(api, key, **kwargs)
    return api, writes


@pytest.mark.timeout(300)
@pytest.mark.parametrize('failure', [ValueError, TimeoutError, DispatchInterrupted])
def test_reopening_actual_runtime_failure_preserves_prepared_and_can_resume(closed_startup, failure):
    api, writes = prepared_dispatch(closed_startup)
    dispatch = api.release_recovery_guard
    active = False
    def actual(key, **kwargs):
        nonlocal active
        active = True
        try:
            return dispatch(key, **kwargs)
        finally:
            active = False
    def probes(expected):
        if active:
            raise failure('runtime-proof-interrupted')
    api.release_recovery_guard = actual
    api._legacy_runtime_probes = probes
    try:
        reopen(api)
    except (ValueError, DispatchInterrupted):
        pass
    assert not writes
    assert set(json.loads((api.state / 'legacy-reopening.json').read_bytes())['guards'].values()) == {'prepared'}
    api._legacy_runtime_probes = lambda expected: None
    assert reopen(api)['status'] == 'pool_legacy_reopened'
    assert writes == [str(row.participant_id) for row in api.request.fencing.retirement.migration.guards]


@pytest.mark.timeout(300)
@pytest.mark.parametrize('damage', ['missing', 'unchanged', 'released', 'runtime_journal'])
def test_reopening_callback_must_record_exact_intent_after_fresh_proof(closed_startup, damage):
    from scripts.ops.nebius_pool_activation_live import HTTPSPoolActivationAPI

    api, writes = prepared_dispatch(closed_startup)
    participant = str(api.request.fencing.retirement.migration.guards[0].participant_id)
    journal = api.state / 'legacy-reopening.json'
    callbacks = []
    def callback():
        callbacks.append(participant)
        if damage == 'unchanged':
            return
        record = json.loads(journal.read_bytes())
        record['guards'][participant] = 'released' if damage == 'released' else 'intent'
        journal.write_text(json.dumps(record))
    if damage == 'runtime_journal':
        def tamper(expected):
            record = json.loads(journal.read_bytes())
            record['guards'][participant] = 'intent'
            journal.write_text(json.dumps(record))
        api._legacy_runtime_probes = tamper
    with pytest.raises(ValueError, match='pool_legacy_guard_release_unconfirmed'):
        HTTPSPoolActivationAPI.release_recovery_guard(api, participant,
            **({} if damage == 'missing' else {'record_intent': callback}))
    assert not writes
    assert callbacks == ([] if damage in {'missing', 'runtime_journal'} else [participant])


@pytest.mark.timeout(600)
def test_fixed_reopening_dispatch_records_intent_and_uses_phase_aware_runtime_and_drain(closed_startup):
    from scripts.ops.nebius_pool_activation_live import HTTPSPoolActivationAPI

    dispatch = HTTPSPoolActivationAPI.release_recovery_guard
    api = restarted(closed_startup)
    api.anchor = api.root / 'cutover-anchor'
    migration = api.request.fencing.retirement.migration
    participants = [str(row.participant_id) for row in migration.guards]
    first, second, *rest = participants
    proofs, drains, writes = [], [], []

    def runtime(kind, original, expected):
        assert expected == api.startup.documents[_key(original)]
        assert expected['metadata']['uid'] == original['metadata']['uid']
        assert _stable(expected)['spec'] == _stable(original)['spec']
        proofs.append((kind, _key(original)))

    def database(target, *, original, expected, credential_uid, credential_resource_version):
        binding = target.database
        actuator = original['metadata']['namespace'] != target.namespace
        assert (credential_uid, credential_resource_version) == (
            (binding.actuator_credential_uid, binding.actuator_credential_resource_version) if actuator
            else (binding.credential_uid, binding.credential_resource_version))
        runtime('database', original, expected)

    def drain(target):
        assert target in migration.guards and api.guards[str(target.participant_id)] == 'fenced'
        drains.append(str(target.participant_id))
        return True

    def release(target):
        participant = str(target.participant_id)
        assert target in migration.guards and api.guards[participant] == 'fenced'
        assert json.loads((api.state / 'legacy-reopening.json').read_bytes())['guards'][participant] == 'intent'
        assert api.mode == 'fenced' and api.machine_phase == 'revoked'
        assert proofs  # The fixed dispatcher cannot bypass actual runtime checks.
        writes.append(participant)
        if participant == second:
            raise OSError('private-lost-before')
        api.guards[participant] = 'open'
        if participant == first:
            raise OSError('private-lost-after')
        return 'open'

    api.parent = SimpleNamespace(history=SimpleNamespace(
        qualify_binding=lambda request, manager: None,
        qualify_manager_database=lambda *, expected: runtime('manager_db', api.request.manager, expected),
        qualify_manager_legacy_settings=lambda *, expected: runtime('manager_settings', api.request.manager, expected),
        recovery_pool_drained=lambda: True),
        guards=SimpleNamespace(qualify_runtime_database=database,
            qualify_runtime_legacy_settings=lambda target, *, original, expected: runtime('settings', original, expected),
            qualify_runtime_telemetry=lambda target, *, original, expected: runtime('telemetry', original, expected),
            recovery_participant_drained=drain, release_recovery_guard=release))
    api._scope = api.verify_retained
    api._guard = lambda key: next(row for row in migration.guards if str(row.participant_id) == key)
    api._legacy_runtime_probes = lambda expected: HTTPSPoolActivationAPI._legacy_runtime_probes(api, expected)
    api.qualify_legacy_runtimes = lambda: HTTPSPoolActivationAPI.qualify_legacy_runtimes(api)
    api.qualify_reopening_runtimes = lambda: HTTPSPoolActivationAPI.qualify_reopening_runtimes(api)
    api.pool_recovery_drained = lambda: HTTPSPoolActivationAPI.pool_recovery_drained(api)
    api.participant_recovery_drained = lambda key: HTTPSPoolActivationAPI.participant_recovery_drained(api, key)
    api.release_recovery_guard = lambda key, **kwargs: dispatch(api, key, **kwargs)
    with pytest.raises(ValueError, match='pool_legacy_guard_release_unconfirmed'):
        dispatch(api, first)  # Completed restart alone is not release authority.
    assert not writes
    assert reopen(api)['status'] == 'pending_legacy_guard_release'
    assert writes == [first, second] and api.guards[first] == 'open'
    with pytest.raises(ValueError, match='pool_legacy_runtimes_unqualified'):
        api.qualify_legacy_runtimes()  # Original all-closed proof stays strict.
    drains.clear()
    assert reopen(api)['status'] == 'pending_legacy_guard_release'
    assert first not in drains and writes == [first, second]
    api.machine_phase = 'active'
    with pytest.raises(ValueError, match='pool_legacy_reopening_unconfirmed'):
        reopen(api)
    assert writes == [first, second]
    api.machine_phase = 'revoked'
    api.guards[second] = 'open'
    drains.clear()
    assert reopen(api)['status'] == 'pool_legacy_reopened'
    assert writes == [first, second, *rest] and not {first, second}.intersection(drains)
    with pytest.raises(ValueError, match='pool_legacy_guard_release_unconfirmed'):
        dispatch(api, first)  # Completed release is never fresh dispatch authority.
    assert writes == [first, second, *rest]
    expected = [*(row.controller for row in migration.guards), *api.request.services, *api.request.fencing.retirement.actuators]
    assert set(proofs) == {('manager_db', _key(api.request.manager)), ('manager_settings', _key(api.request.manager)),
        *(('database', _key(row)) for row in expected), *(('settings', _key(row)) for row in expected),
        *(('telemetry', _key(row)) for row in api.request.fencing.retirement.actuators)}
