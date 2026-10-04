"""Cancellation settles delayed startup CAS without assuming zero replicas is proof."""
from __future__ import annotations

import copy
import json

import pytest
from scripts.ops.nebius_ingress_stage import _key
from tests.ops.test_nebius_pool_activation_stage import ActivationAPI, advance
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


class FenceAPI(ActivationAPI):
    def __init__(self, fixture):
        super().__init__(fixture)
        self.fence_calls = []
        self.fence_failure = None

    def preview_startup_fence(self, key, before, desired):
        assert before == self.startup.documents[key]
        return copy.deepcopy(desired)

    def fence_startup(self, key, before, desired):
        row = json.loads((self.state / 'startup-fence.json').read_bytes())['workloads'][key]
        assert row == {'phase': 'intent', 'expected': None}
        assert self.mode == 'fenced' and set(self.guards.values()) == {'fenced'}
        self.fence_calls.append(key)
        if self.fence_failure == 'before':
            raise OSError('private-marker')
        if self.fence_failure == 'conflict':
            return False
        current = copy.deepcopy(desired)
        current['metadata'].update(uid=before['metadata']['uid'], resourceVersion=str(int(before['metadata']['resourceVersion']) + 1))
        self.startup.documents[key] = current
        if self.fence_failure == 'after':
            raise OSError('private-marker')
        return True


def pending_cancelled(fixture):
    request, _, _, startup, _, _ = fixture
    key = _key(request.manager)
    startup.fail_key, startup.failure = key, 'before'
    assert start(fixture)['status'] == 'pending_startup_outcome'
    api = FenceAPI(fixture)
    assert advance(fixture, api, cancel=True)['status'] == 'pool_activation_cancelled'
    return api, key


def fence(fixture, api):
    from scripts.ops.nebius_pool_startup_fence import fence_pool_startup

    request, _, _, _, _, root = fixture
    return fence_pool_startup(request=request, api=api, state_dir=root / 'cutover', anchor_dir=root / 'cutover-anchor')


def test_metadata_fence_invalidates_delayed_start_and_is_replayable(closed_startup):
    from scripts.ops.nebius_pool_cutover import retained_cutover_workloads
    from scripts.ops.nebius_pool_startup import startup_workload_options

    api, key = pending_cancelled(closed_startup)
    request, _, _, startup, dormant, root = closed_startup
    before = copy.deepcopy(startup.documents)
    result = fence(closed_startup, api)
    assert result['status'] == 'startup_writes_fenced' and result['legacy_restore_allowed'] is False
    assert api.fence_calls == [key]
    actual = startup.documents[key]
    assert actual['spec'] == before[key]['spec'] and actual['spec']['replicas'] == 0
    assert actual['metadata']['resourceVersion'] != before[key]['metadata']['resourceVersion']
    assert actual['metadata']['annotations']['loom.nebius/pool-startup-fence'] == str(request.fencing.retirement.migration.registration.spec.operation_id)
    assert all(startup.documents[name] == value for name, value in before.items() if name != key)
    assert startup.documents[_key(dormant.actuator)] == before[_key(dormant.actuator)]
    assert fence(closed_startup, api) == result and api.fence_calls == [key]
    assert advance(closed_startup, api, cancel=True)['status'] == 'pool_activation_cancelled'
    options = startup_workload_options(request, state_dir=root / 'cutover', anchor_dir=root / 'cutover-anchor')
    assert len(options[key]) == 1 and options[key][0]['metadata']['annotations'] == actual['metadata']['annotations']
    retained = retained_cutover_workloads(request, state_dir=root / 'cutover', anchor_dir=root / 'cutover-anchor', observed=startup.documents)
    assert retained[key]['metadata']['annotations'] == actual['metadata']['annotations']
    with pytest.raises(ValueError):
        start(closed_startup)


@pytest.mark.parametrize('failure', ['before', 'after', 'conflict'])
def test_unknown_fence_observes_without_repeating_and_definite_conflict_can_resume(closed_startup, failure):
    api, key = pending_cancelled(closed_startup)
    api.fence_failure = failure
    result = fence(closed_startup, api)
    api.fence_failure = None
    if failure == 'before':
        assert result['status'] == 'pending_startup_fence'
        assert fence(closed_startup, api) == result and api.fence_calls == [key]
        # The original startup wins after a lost fence response. Both pending
        # old-version requests are now invalid; no second fence is dispatched.
        current = api.startup.documents[key]
        current['spec']['replicas'] = 1
        current['metadata']['resourceVersion'] = str(int(current['metadata']['resourceVersion']) + 1)
    elif failure == 'conflict':
        assert result['status'] == 'pending_startup_fence_update'
    assert fence(closed_startup, api)['status'] == 'startup_writes_fenced'
    assert api.fence_calls == [key] * (2 if failure == 'conflict' else 1)


@pytest.mark.parametrize('started', [False, True])
def test_already_changed_version_needs_no_mutation(closed_startup, started):
    api, key = pending_cancelled(closed_startup)
    document = api.startup.documents[key]
    document['metadata']['resourceVersion'] = str(int(document['metadata']['resourceVersion']) + 1)
    if started:
        document['spec']['replicas'] = 1
    before = copy.deepcopy(api.startup.documents)
    assert fence(closed_startup, api)['status'] == 'startup_writes_fenced'
    assert not api.fence_calls and before == api.startup.documents


@pytest.mark.parametrize('start_first', [False, True])
def test_no_uncertain_startup_needs_no_mutation(closed_startup, start_first):
    if start_first:
        start(closed_startup)
    api = FenceAPI(closed_startup)
    advance(closed_startup, api, cancel=True)
    assert fence(closed_startup, api)['status'] == 'startup_writes_fenced'
    assert not api.fence_calls


@pytest.mark.parametrize('damage', ['uid', 'spec', 'foreign_marker', 'pool', 'guard', 'retained', 'activation', 'startup',
    'missing_anchor', 'stale_version', 'changed_settled'])
def test_fence_rejects_unqualified_scope_and_recovery_evidence(closed_startup, damage):
    api, key = pending_cancelled(closed_startup)
    if damage == 'uid':
        api.startup.documents[key]['metadata']['uid'] = 'aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaaaa'
    elif damage == 'spec':
        api.startup.documents[key]['spec']['template']['spec']['containers'][0]['image'] = 'foreign'
    elif damage == 'foreign_marker':
        api.startup.documents[key]['metadata'].setdefault('annotations', {})['loom.nebius/pool-startup-fence'] = 'foreign'
    elif damage == 'pool':
        api.mode = 'global'
    elif damage == 'guard':
        api.guards[next(iter(api.guards))] = 'held'
    elif damage == 'retained':
        api.retained = False
    elif damage in {'activation', 'startup'}:
        (api.state / (damage + '.json')).write_bytes(b'{}')
    else:
        original_version = api.startup.documents[key]['metadata']['resourceVersion']
        fence(closed_startup, api)
        _, _, _, _, _, root = closed_startup
        if damage == 'missing_anchor':
            next((root / 'cutover-anchor').glob('*-startup-fence.json')).unlink()
        elif damage == 'stale_version':
            api.startup.documents[key]['metadata']['resourceVersion'] = original_version
        else:
            api.startup.documents[key]['spec']['replicas'] = 1
        api.fence_calls.clear()
    with pytest.raises(ValueError) as error:
        fence(closed_startup, api)
    assert 'private-' not in str(error.value) and not api.fence_calls
