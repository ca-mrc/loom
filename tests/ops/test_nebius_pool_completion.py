"""Terminal handoff cannot certify an incomplete or changed capacity migration."""
from __future__ import annotations

import copy
import hashlib
import json
from uuid import uuid4

import pytest
from scripts.ops.nebius_ingress_stage import _key
from scripts.ops.nebius_management_switch import _stable
from tests.ops.test_nebius_pool_activation_stage import ActivationAPI, advance, start
from tests.ops.test_nebius_pool_legacy_reopening import closed_startup as closed_startup
from tests.ops.test_nebius_pool_legacy_reopening import collector_inputs as collector_inputs
from tests.ops.test_nebius_pool_legacy_reopening import (
    cutover_binding_inventory as cutover_binding_inventory,
)
from tests.ops.test_nebius_pool_legacy_reopening import cutover_inputs as cutover_inputs
from tests.ops.test_nebius_pool_legacy_reopening import fencing_inputs as fencing_inputs
from tests.ops.test_nebius_pool_legacy_reopening import management_inputs as management_inputs
from tests.ops.test_nebius_pool_legacy_reopening import platform_inputs as platform_inputs
from tests.ops.test_nebius_pool_legacy_reopening import reopen, restarted
from tests.ops.test_nebius_pool_legacy_reopening import retirement_inputs as retirement_inputs
from tests.ops.test_nebius_pool_legacy_reopening import runtime_inputs as runtime_inputs


def complete(api):
    from scripts.ops.nebius_pool_completion import complete_pool_cutover

    return complete_pool_cutover(request=api.request, api=api, state_dir=api.state,
        anchor_dir=api.root / 'cutover-anchor')


def load(api, checksum):
    from scripts.ops.nebius_pool_completion import load_pool_completion

    return load_pool_completion(request=api.request, state_dir=api.state,
        anchor_dir=api.root / 'cutover-anchor', completion_sha256=checksum)


@pytest.mark.timeout(420)
def test_global_completion_binds_terminal_journals_and_uids_without_closed_mode_probe(closed_startup):
    from scripts.ops.nebius_pool_completion import complete_pool_cutover

    assert callable(complete_pool_cutover)
    api = ActivationAPI(closed_startup)
    with pytest.raises(ValueError, match='pool_completion_unqualified'):
        complete(api)
    start(closed_startup)
    api.failure = ('release', 'before')
    assert advance(closed_startup, api)['status'] == 'pending_guard_release'
    with pytest.raises(ValueError, match='pool_completion_unqualified'):
        complete(api)
    api.failure = None
    api.guards[next(iter(api.guards))] = 'open'
    assert advance(closed_startup, api)['status'] == 'pool_activation_complete'
    api.ready = False  # Completion records phase/identity, not runtime acceptance.
    calls, runtime_checks = list(api.calls), api.runtime_checks
    history = {path: path.read_bytes() for path in api.root.rglob('*.json')}
    result = complete(api)
    assert result['status'] == 'pool_cutover_completed' and result['outcome'] == 'global'
    assert result['acceptance_verified'] is False
    receipt = load(api, result['completion_sha256'])
    manager = receipt.workloads[_key(api.request.manager)]
    assert receipt.outcome == 'global'
    assert manager['metadata']['uid'] == api.request.manager['metadata']['uid']
    assert manager['spec']['replicas'] == 1
    assert manager['spec']['strategy'] == {'type': 'Recreate'}
    assert 'resourceVersion' not in manager['metadata']
    assert any(row['name'] == 'pool-profiles' for row in manager['spec']['template']['spec']['volumes'])
    assert all(path.read_bytes() == raw for path, raw in history.items())
    before = {path: path.read_bytes() for path in api.root.rglob('*.json')}
    assert complete(api) == result
    assert {path: path.read_bytes() for path in before} == before
    assert api.calls == calls and api.runtime_checks == runtime_checks
    assert all(hashlib.sha256(path.read_bytes()).hexdigest() == checksum for path, checksum in receipt.history.items())

    # Historical loading remains usable without live Pods; active use must still
    # refuse a replaced manager, changed admission or an unrelated open guard.
    manager_key = _key(api.request.manager)
    saved = copy.deepcopy(api.startup.documents[manager_key])
    first = next(iter(api.guards))
    for damage in ('uid', 'template', 'mode', 'guard', 'retained', 'orphan_recovery'):
        if damage == 'uid':
            api.startup.documents[manager_key]['metadata']['uid'] = str(uuid4())
        elif damage == 'template':
            api.startup.documents[manager_key]['spec']['replicas'] = 0
        elif damage == 'mode':
            api.mode = 'fenced'
        elif damage == 'guard':
            api.guards[first] = 'foreign'
        elif damage == 'retained':
            api.retained = False
        else:
            (api.state / 'legacy-reopening.json').write_text('{}')
        with pytest.raises(ValueError, match='pool_completion_unqualified') as error:
            complete(api)
        assert 'private-' not in str(error.value)
        api.startup.documents[manager_key] = copy.deepcopy(saved)
        api.mode, api.guards[first], api.retained = 'global', 'open', True
        (api.state / 'legacy-reopening.json').unlink(missing_ok=True)
    assert api.calls == calls and (api.state / 'completion.json').read_bytes() == before[api.state / 'completion.json']


@pytest.mark.timeout(420)
def test_completion_preserves_frozen_bytes_and_recovers_only_matching_local_intent(closed_startup, monkeypatch):
    from scripts.ops import nebius_pool_completion as completion

    start(closed_startup)
    api = ActivationAPI(closed_startup)
    advance(closed_startup, api)
    write = completion.private_state._atomic_json
    path = api.state / 'completion.json'
    def lost_write(target, value):
        if target == path:
            raise OSError('private-write-marker')
        write(target, value)
    monkeypatch.setattr(completion.private_state, '_atomic_json', lost_write)
    with pytest.raises(ValueError, match='pool_completion_unqualified'):
        complete(api)
    marker = api.root / 'cutover-anchor' / (str(api.request.fencing.retirement.migration.registration.spec.operation_id) + '-completion.json')
    anchored = marker.read_bytes()
    assert not path.exists()
    monkeypatch.setattr(completion.private_state, '_atomic_json', write)
    result = complete(api)
    assert marker.read_bytes() == anchored
    original = path.read_bytes()
    for damaged in (original + b'\n', b'{}'):
        path.write_bytes(damaged)
        with pytest.raises(ValueError, match='pool_completion_unqualified'):
            complete(api)
        with pytest.raises(ValueError, match='pool_completion_unqualified'):
            load(api, result['completion_sha256'])
        assert path.read_bytes() == damaged
    path.write_bytes(original)
    marker.unlink()
    with pytest.raises(ValueError, match='pool_completion_unqualified'):
        complete(api)
    marker.write_bytes(anchored)
    activation = api.state / 'activation.json'
    original_activation = activation.read_bytes()
    activation.write_bytes(original_activation + b'\n')
    with pytest.raises(ValueError, match='pool_completion_unqualified'):
        load(api, result['completion_sha256'])
    activation.write_bytes(original_activation)
    # A caller cannot choose legacy/global, an arbitrary UID or another roster
    # by rewriting the receipt, even when passing its new hash to the reader.
    altered = json.loads(original)
    altered['outcome'] = 'legacy'
    path.write_text(json.dumps(altered))
    with pytest.raises(ValueError, match='pool_completion_unqualified'):
        load(api, hashlib.sha256(path.read_bytes()).hexdigest())


@pytest.mark.timeout(600)
def test_legacy_completion_keeps_global_fenced_and_allows_reopened_owner_work(closed_startup):
    from scripts.ops.nebius_pool_completion import complete_pool_cutover

    assert callable(complete_pool_cutover)
    api = restarted(closed_startup)
    with pytest.raises(ValueError, match='pool_completion_unqualified'):
        complete(api)
    assert reopen(api)['status'] == 'pool_legacy_reopened'
    calls, releases = list(api.calls), list(api.releases)
    api.local_drains.clear()
    result = complete(api)
    assert result['outcome'] == 'legacy' and result['acceptance_verified'] is False
    receipt = load(api, result['completion_sha256'])
    manager = receipt.workloads[_key(api.request.manager)]
    assert manager['spec'] == _stable(api.request.manager)['spec']
    assert set(api.guards.values()) == {'open'} and api.busy == set(api.guards)
    assert not api.local_drains
    assert complete(api) == result
    assert api.calls == calls and api.releases == releases
    assert not api.local_drains
    for damage in ('machines', 'gateway', 'global_work'):
        if damage == 'machines':
            api.machine_phase = 'active'
        elif damage == 'gateway':
            api.effective_readonly = False
        else:
            api.cleanup_drained = False
        with pytest.raises(ValueError, match='pool_completion_unqualified'):
            complete(api)
        api.machine_phase, api.effective_readonly, api.cleanup_drained = 'revoked', True, True
