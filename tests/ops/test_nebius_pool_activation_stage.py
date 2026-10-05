"""Anchored pool opening and cancellation dispatch each uncertain write once."""
from __future__ import annotations

import json

import pytest
from scripts.ops.nebius_ingress_stage import _key
from tests.ops.test_nebius_pool_startup import closed_startup as closed_startup
from tests.ops.test_nebius_pool_startup import collector_inputs as collector_inputs
from tests.ops.test_nebius_pool_startup import (
    cutover_binding_inventory as cutover_binding_inventory,
)
from tests.ops.test_nebius_pool_startup import cutover_inputs as cutover_inputs
from tests.ops.test_nebius_pool_startup import fencing_inputs as fencing_inputs
from tests.ops.test_nebius_pool_startup import management_inputs as management_inputs
from tests.ops.test_nebius_pool_startup import platform_inputs as platform_inputs
from tests.ops.test_nebius_pool_startup import retirement_inputs as retirement_inputs
from tests.ops.test_nebius_pool_startup import runtime_inputs as runtime_inputs
from tests.ops.test_nebius_pool_startup import start


class ActivationAPI:
    """Remote transports only are doubled; journals and phase recovery are real."""

    def __init__(self, fixture):
        self.request, _, _, self.startup, _, self.root = fixture
        self.state = self.root / 'cutover'
        self.mode = 'closed'
        self.guards = {str(row.participant_id): 'held' for row in self.request.fencing.retirement.migration.guards}
        self.calls = []
        self.failure = None
        self.ready = True
        self.retained = True
        self.runtime_checks = 0

    def read_workload(self, key):
        return self.startup.read_workload(key)

    def verify_retained(self):
        if not self.retained:
            raise ValueError('private-retained-marker')

    def qualify_runtime(self):
        self.runtime_checks += 1
        assert self.mode == 'closed' and set(self.guards.values()) == {'held'}
        if not self.ready:
            raise ValueError('private-runtime-marker')

    def pool_state(self):
        return self.mode

    def guard_state(self, participant):
        return self.guards[participant]

    def _write(self, operation, participant=None):
        record = json.loads((self.state / 'activation.json').read_bytes())
        if operation in {'open', 'fence'}:
            assert record['opening' if operation == 'open' else 'cancellation'] == 'intent'
        else:
            assert record['guards'][participant]['release' if operation == 'release' else 'fence'] == 'intent'
        if operation == 'release':
            assert record['opening'] == 'opened' and self.mode == 'global'
        if operation == 'guard-fence':
            assert record['cancellation'] == 'fenced' and self.mode == 'fenced'
        self.calls.append((operation, participant))
        if self.failure == (operation, 'before'):
            raise OSError('private-write-marker')
        if operation == 'open':
            self.mode = 'global'
        elif operation == 'fence':
            self.mode = 'fenced'
        else:
            self.guards[participant] = 'open' if operation == 'release' else 'fenced'
        if self.failure == (operation, 'after'):
            raise OSError('private-write-marker')

    def open_pool(self):
        self._write('open')

    def fence_pool(self):
        self._write('fence')

    def release_guard(self, participant):
        self._write('release', participant)

    def fence_guard(self, participant):
        self._write('guard-fence', participant)


def advance(fixture, api, *, cancel=False):
    from scripts.ops.nebius_pool_activation_stage import advance_pool_activation

    request, _, _, _, _, root = fixture
    return advance_pool_activation(request=request, api=api, state_dir=root / 'cutover',
        anchor_dir=root / 'cutover-anchor', cancel=cancel)


def test_opening_is_anchored_before_dispatch_and_all_guards_follow_global_proof(closed_startup):
    start(closed_startup)
    api = ActivationAPI(closed_startup)
    result = advance(closed_startup, api)
    assert result['status'] == 'pool_activation_complete'
    assert result['admission_open'] is True and result['legacy_restore_allowed'] is False
    expected = [('open', None), *(('release', key) for key in api.guards)]
    assert api.calls == expected and api.runtime_checks == 1
    assert advance(closed_startup, api) == result and api.calls == expected
    assert api.runtime_checks == 1
    assert set(api.guards.values()) == {'open'}


@pytest.mark.parametrize('operation', ['open', 'release'])
@pytest.mark.parametrize('when', ['before', 'after'])
def test_uncertain_opening_and_guard_release_are_observed_without_repeating(closed_startup, operation, when):
    start(closed_startup)
    api = ActivationAPI(closed_startup)
    api.failure = (operation, when)
    result = advance(closed_startup, api)
    api.failure = None
    if when == 'before':
        assert result['status'] == ('pending_pool_opening' if operation == 'open' else 'pending_guard_release')
        writes = list(api.calls)
        assert advance(closed_startup, api) == result and api.calls == writes
        # A later read can settle the original single in-flight operation.
        if operation == 'open':
            api.mode = 'global'
        else:
            api.guards[next(iter(api.guards))] = 'open'
    assert advance(closed_startup, api)['status'] == 'pool_activation_complete'
    assert api.calls.count(('open', None)) == 1
    assert all(api.calls.count(('release', key)) == 1 for key in api.guards)


@pytest.mark.parametrize('damage', ['partial_startup', 'runtime', 'pool', 'guard', 'retained', 'anchor', 'startup_hash',
    'guard_roster', 'guard_phase', 'cancellation_phase'])
def test_unqualified_opening_never_dispatches_or_releases(closed_startup, damage):
    if damage != 'partial_startup':
        start(closed_startup)
    api = ActivationAPI(closed_startup)
    if damage == 'runtime':
        api.ready = False
    elif damage == 'pool':
        api.mode = 'global'
    elif damage == 'guard':
        api.guards[next(iter(api.guards))] = 'open'
    elif damage == 'retained':
        api.retained = False
    elif damage in {'anchor', 'startup_hash', 'guard_roster', 'guard_phase', 'cancellation_phase'}:
        api.failure = ('open', 'before')
        advance(closed_startup, api)
        api.calls.clear()
        if damage == 'anchor':
            next((api.root / 'cutover-anchor').glob('*-activation.json')).unlink()
        elif damage == 'startup_hash':
            path = api.state / 'startup.json'
            path.write_bytes(path.read_bytes() + b'\n')
        else:
            path = api.state / 'activation.json'
            record = json.loads(path.read_bytes())
            key = next(iter(record['guards']))
            if damage == 'guard_roster':
                del record['guards'][key]
            elif damage == 'guard_phase':
                record['guards'][key]['release'] = 'released'
            else:
                record['guards'][key]['fence'] = 'intent'
            path.write_text(json.dumps(record))
    with pytest.raises(ValueError, match='pool_activation_unconfirmed') as error:
        advance(closed_startup, api)
    assert 'private-' not in str(error.value) and not api.calls


@pytest.mark.parametrize('initial', ['no_startup', 'partial_startup', 'opening_pending', 'release_partial', 'opened'])
def test_cancellation_fences_global_then_local_authority_without_runtime_health(closed_startup, initial):
    if initial == 'partial_startup':
        startup = closed_startup[3]
        startup.failure = 'before'
        startup.fail_key = _key(closed_startup[0].manager)
        start(closed_startup)
    elif initial != 'no_startup':
        start(closed_startup)
    api = ActivationAPI(closed_startup)
    if initial in {'opening_pending', 'opened'}:
        api.failure = ('open', 'before') if initial == 'opening_pending' else None
        advance(closed_startup, api)
    elif initial == 'release_partial':
        api.failure = ('release', 'before')
        advance(closed_startup, api)
        api.guards[next(iter(api.guards))] = 'open'
        assert advance(closed_startup, api)['status'] == 'pending_guard_release'
    api.failure, api.ready = None, False
    before = len(api.calls)
    result = advance(closed_startup, api, cancel=True)
    assert result['status'] == 'pool_activation_cancelled'
    assert result['admission_open'] is False and result['legacy_restore_allowed'] is False
    assert api.calls[before:] == [('fence', None), *(('guard-fence', key) for key in api.guards)]
    calls = list(api.calls)
    assert advance(closed_startup, api, cancel=True) == result and api.calls == calls
    with pytest.raises(ValueError):
        advance(closed_startup, api)
    with pytest.raises(ValueError):
        start(closed_startup)
    assert api.calls == calls


@pytest.mark.parametrize('operation', ['fence', 'guard-fence'])
@pytest.mark.parametrize('when', ['before', 'after'])
def test_uncertain_cancellation_writes_never_repeat_or_enable_legacy_restore(closed_startup, operation, when):
    start(closed_startup)
    api = ActivationAPI(closed_startup)
    advance(closed_startup, api)
    api.failure = (operation, when)
    result = advance(closed_startup, api, cancel=True)
    api.failure = None
    if when == 'before':
        assert result['status'] == ('pending_pool_fence' if operation == 'fence' else 'pending_guard_fence')
        calls = list(api.calls)
        assert advance(closed_startup, api, cancel=True) == result and api.calls == calls
        if operation == 'fence':
            api.mode = 'fenced'
        else:
            api.guards[next(iter(api.guards))] = 'fenced'
    result = advance(closed_startup, api, cancel=True)
    assert result['status'] == 'pool_activation_cancelled' and result['legacy_restore_allowed'] is False
    assert api.calls.count(('fence', None)) == 1
    assert all(api.calls.count(('guard-fence', key)) == 1 for key in api.guards)
