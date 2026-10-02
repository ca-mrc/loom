"""Successor shutdown retains cleanup until drain, and cannot restore writers."""
from __future__ import annotations

import copy
import json

import pytest
from scripts.ops.nebius_ingress_stage import _key
from tests.ops.test_nebius_pool_startup_fence import FenceAPI, advance, fence, start
from tests.ops.test_nebius_pool_startup_fence import closed_startup as closed_startup
from tests.ops.test_nebius_pool_startup_fence import collector_inputs as collector_inputs
from tests.ops.test_nebius_pool_startup_fence import (
    cutover_binding_inventory as cutover_binding_inventory,
)
from tests.ops.test_nebius_pool_startup_fence import cutover_inputs as cutover_inputs
from tests.ops.test_nebius_pool_startup_fence import fencing_inputs as fencing_inputs
from tests.ops.test_nebius_pool_startup_fence import management_inputs as management_inputs
from tests.ops.test_nebius_pool_startup_fence import platform_inputs as platform_inputs
from tests.ops.test_nebius_pool_startup_fence import retirement_inputs as retirement_inputs
from tests.ops.test_nebius_pool_startup_fence import runtime_inputs as runtime_inputs


class ShutdownAPI(FenceAPI):
    def __init__(self, fixture):
        super().__init__(fixture)
        self.cleanup_drained = True
        self.processes_drained = True
        self.stop_calls = []
        self.stop_failure = None
        self.drain_calls = []

    def recovery_drained(self):
        return self.cleanup_drained

    def preview_stop(self, key, before, desired):
        assert before == self.startup.documents[key]
        return copy.deepcopy(desired)

    def stop_workload(self, key, before, desired):
        row = json.loads((self.state / 'shutdown.json').read_bytes())['workloads'][key]
        assert row['phase'] == 'intent' and row['before_resource_version'] == before['metadata']['resourceVersion']
        assert self.cleanup_drained and self.mode == 'fenced' and set(self.guards.values()) == {'fenced'}
        self.stop_calls.append(key)
        if self.stop_failure == 'before':
            raise OSError('private-marker')
        if self.stop_failure == 'conflict':
            return False
        current = copy.deepcopy(desired)
        current['metadata'].update(uid=before['metadata']['uid'], resourceVersion=str(int(before['metadata']['resourceVersion']) + 1))
        self.startup.documents[key] = current
        if self.stop_failure == 'after':
            raise OSError('private-marker')
        return True

    def successor_drained(self, key, desired):
        from scripts.ops.nebius_management_switch import _stable

        # The retained projection canonicalizes Kubernetes resource quantities;
        # transport readback may still use equivalent Gi/milli spellings.
        assert _stable(self.startup.documents[key]) == _stable(desired)
        self.drain_calls.append(key)
        return self.processes_drained


def cancelled(fixture, *, started=True):
    if started:
        assert start(fixture)['status'] == 'pool_startup_staged_closed'
    api = ShutdownAPI(fixture)
    assert advance(fixture, api, cancel=True)['status'] == 'pool_activation_cancelled'
    assert fence(fixture, api)['status'] == 'startup_writes_fenced'
    return api


def shutdown(fixture, api):
    from scripts.ops.nebius_pool_shutdown import stop_pool_successors

    request, _, _, _, _, root = fixture
    return stop_pool_successors(request=request, api=api, state_dir=root / 'cutover', anchor_dir=root / 'cutover-anchor')


def test_shutdown_changes_only_successor_scale_after_cleanup_and_replays_without_writes(closed_startup):
    from scripts.ops.nebius_pool_startup import startup_workload_options

    request, _, _, startup, dormant, root = closed_startup
    api = cancelled(closed_startup)
    original = copy.deepcopy(startup.documents)
    api.cleanup_drained = False
    assert shutdown(closed_startup, api)['status'] == 'pending_pool_cleanup'
    assert not api.stop_calls and original == startup.documents
    api.cleanup_drained = True
    result = shutdown(closed_startup, api)
    assert result['status'] == 'pool_successors_stopped' and result['legacy_restore_allowed'] is False
    assert api.stop_calls == list(reversed(startup.requests))
    assert api.stop_calls[-1] == _key(request.manager)
    for key, before in original.items():
        current = startup.documents[key]
        expected = copy.deepcopy(before['spec'])
        if key in startup.requests:
            expected['suspend' if before['kind'] == 'CronJob' else 'replicas'] = True if before['kind'] == 'CronJob' else 0
        assert current['spec'] == expected
        assert current['metadata']['uid'] == before['metadata']['uid']
    assert startup.documents[_key(dormant.actuator)] == original[_key(dormant.actuator)]
    assert startup.documents[_key(dormant.collector)] == original[_key(dormant.collector)]
    assert shutdown(closed_startup, api) == result and len(api.stop_calls) == len(startup.requests)
    options = startup_workload_options(request, state_dir=root / 'cutover', anchor_dir=root / 'cutover-anchor')
    assert all(len(options[key]) == 1 and options[key][0]['spec'][
        'suspend' if startup.documents[key]['kind'] == 'CronJob' else 'replicas'] == (
            True if startup.documents[key]['kind'] == 'CronJob' else 0) for key in startup.requests)
    assert advance(closed_startup, api, cancel=True)['status'] == 'pool_activation_cancelled'


@pytest.mark.parametrize('failure', ['before', 'after', 'conflict'])
def test_unknown_stop_never_redispatches_and_definite_rejection_can_resume(closed_startup, failure):
    api = cancelled(closed_startup)
    api.stop_failure = failure
    result = shutdown(closed_startup, api)
    api.stop_failure = None
    if failure == 'before':
        assert result['status'] == 'pending_shutdown_outcome'
        assert shutdown(closed_startup, api) == result and len(api.stop_calls) == 1
        key = api.stop_calls[0]
        value = api.startup.documents[key]
        value['spec']['suspend' if value['kind'] == 'CronJob' else 'replicas'] = True if value['kind'] == 'CronJob' else 0
        value['metadata']['resourceVersion'] = str(int(value['metadata']['resourceVersion']) + 1)
    elif failure == 'conflict':
        assert result['status'] == 'pending_shutdown_update'
    assert shutdown(closed_startup, api)['status'] == 'pool_successors_stopped'
    assert len(api.stop_calls) == len(api.startup.requests) + int(failure == 'conflict')


@pytest.mark.parametrize('started', [False, True])
def test_process_drain_is_required_even_when_roots_are_already_stopped(closed_startup, started):
    api = cancelled(closed_startup, started=started)
    api.processes_drained = False
    assert shutdown(closed_startup, api)['status'] == 'pending_successor_drain'
    assert bool(api.stop_calls) is started
    assert api.drain_calls
    api.processes_drained = True
    assert shutdown(closed_startup, api)['status'] == 'pool_successors_stopped'


@pytest.mark.parametrize('late_start', [False, True])
def test_partial_startup_shutdown_preserves_its_settled_fence_and_never_starts_other_roots(closed_startup, late_start):
    request, _, _, startup, _, _ = closed_startup
    manager_key = _key(request.manager)
    gateway_key = 'Deployment:' + request.manager['metadata']['namespace'] + ':loom-pool-gateway'
    startup.fail_key, startup.failure = gateway_key, 'before'
    assert start(closed_startup)['status'] == 'pending_startup_outcome'
    assert startup.requests == [manager_key, gateway_key]
    if late_start:
        startup.documents[gateway_key]['spec']['replicas'] = 1
        startup.documents[gateway_key]['metadata']['resourceVersion'] = str(
            int(startup.documents[gateway_key]['metadata']['resourceVersion']) + 1)
    api = cancelled(closed_startup, started=False)
    annotations = copy.deepcopy(startup.documents[gateway_key]['metadata'].get('annotations', {}))
    if not late_start:
        assert annotations['loom.nebius/pool-startup-fence'] == str(request.fencing.retirement.migration.registration.spec.operation_id)
    assert shutdown(closed_startup, api)['status'] == 'pool_successors_stopped'
    assert api.stop_calls == ([gateway_key, manager_key] if late_start else [manager_key])
    assert startup.documents[gateway_key]['metadata'].get('annotations', {}) == annotations
    assert startup.requests == [manager_key, gateway_key]
    assert advance(closed_startup, api, cancel=True)['status'] == 'pool_activation_cancelled'


@pytest.mark.parametrize('damage', ['pool', 'guard', 'retained', 'startup_fence', 'uid', 'template', 'missing_anchor'])
def test_shutdown_refuses_drift_without_restoration_or_extra_writes(closed_startup, damage):
    api = cancelled(closed_startup)
    if damage == 'pool':
        api.mode = 'global'
    elif damage == 'guard':
        api.guards[next(iter(api.guards))] = 'open'
    elif damage == 'retained':
        api.retained = False
    elif damage == 'startup_fence':
        (api.state / 'startup-fence.json').write_bytes(b'{}')
    elif damage in {'uid', 'template'}:
        key = api.startup.requests[0]
        value = api.startup.documents[key]
        if damage == 'uid':
            value['metadata']['uid'] = 'aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaaaa'
        else:
            value['spec']['template']['spec']['containers'][0]['image'] = 'foreign'
    else:
        assert shutdown(closed_startup, api)['status'] == 'pool_successors_stopped'
        next((api.root / 'cutover-anchor').glob('*-shutdown.json')).unlink()
        api.stop_calls.clear()
    with pytest.raises(ValueError) as error:
        shutdown(closed_startup, api)
    assert 'private-' not in str(error.value) and not api.stop_calls
