"""Connected activation retains actual journals and HTTPS scope during recovery."""
from __future__ import annotations

import json
from contextlib import contextmanager

import pytest
from scripts.ops.nebius_ingress_stage import _key
from tests.ops.test_nebius_pool_startup_live import closed_startup as closed_startup
from tests.ops.test_nebius_pool_startup_live import collector_inputs as collector_inputs
from tests.ops.test_nebius_pool_startup_live import (
    cutover_binding_inventory as cutover_binding_inventory,
)
from tests.ops.test_nebius_pool_startup_live import cutover_inputs as cutover_inputs
from tests.ops.test_nebius_pool_startup_live import fencing_inputs as fencing_inputs
from tests.ops.test_nebius_pool_startup_live import management_inputs as management_inputs
from tests.ops.test_nebius_pool_startup_live import platform_inputs as platform_inputs
from tests.ops.test_nebius_pool_startup_live import retirement_inputs as retirement_inputs
from tests.ops.test_nebius_pool_startup_live import runtime_inputs as runtime_inputs
from tests.ops.test_nebius_pool_startup_live import startup_http as startup_http
from tests.ops.test_nebius_pool_startup_live import unbound_cutover_inputs as unbound_cutover_inputs


@pytest.fixture
def activation_http(startup_http, closed_startup):
    from scripts.ops.nebius_pool_activation_live import HTTPSPoolActivationAPI
    from scripts.ops.nebius_pool_activation_stage import advance_pool_activation
    from scripts.ops.nebius_pool_startup import stage_pool_startup

    request, _, _, _, _, root = closed_startup
    state_dir, anchor_dir = root / 'cutover', root / 'cutover-anchor'

    @contextmanager
    def connect(*, start=True):
        with startup_http() as (startup, state):
            if start:
                assert stage_pool_startup(request=request, api=startup, state_dir=state_dir,
                    anchor_dir=anchor_dir)['status'] == 'pool_startup_staged_closed'
            parent = startup.parent
            state.mode = 'closed'
            state.guards = {str(row.participant_id): 'held' for row in parent.guards.request.guards}
            state.activation_writes = []
            state.runtime_checks = []
            state.role_checks = []
            state.activation_failure = None
            state.role_damage = False
            state.writes.clear()
            state.calls.clear()

            def write(action, participant=None):
                record = json.loads((state_dir / 'activation.json').read_bytes())
                if action in {'open', 'fence'}:
                    assert record['opening' if action == 'open' else 'cancellation'] == 'intent'
                else:
                    assert record['guards'][participant]['release' if action == 'release' else 'fence'] == 'intent'
                    assert state.mode == ('global' if action == 'release' else 'fenced')
                state.activation_writes.append((action, participant))
                if state.activation_failure == (action, 'before'):
                    raise OSError('private-marker')
                if action in {'open', 'fence'}:
                    state.mode = 'global' if action == 'open' else 'fenced'
                else:
                    state.guards[participant] = 'open' if action == 'release' else 'fenced'
                if state.activation_failure == (action, 'after'):
                    raise OSError('private-marker')

            def pool(action):
                if action != 'observe':
                    assert action == 'fence'
                    write('fence')
                return state.mode

            def guard(target, action):
                assert target in parent.guards.request.guards
                key = str(target.participant_id)
                if action != 'observe':
                    assert action in {'release', 'fence'}
                    write('release' if action == 'release' else 'guard-fence', key)
                return state.guards[key]

            def role(target, action):
                assert target in parent.guards.request.guards and action in {'inspect', 'observe'}
                state.role_checks.append(action)
                if state.role_damage or (action == 'observe' and state.mode != 'closed'):
                    raise ValueError('private-marker')
                return {'status': 'qualified'}

            def runtime(component, **kwargs):
                assert state.mode == 'closed' and set(state.guards.values()) == {'held'}
                assert kwargs['expected'] == state.objects[_key(kwargs['expected'])]
                state.runtime_checks.append(component)

            def open_pool(*, original, expected):
                assert original == startup.closed[_key(original)] and original['spec']['replicas'] == 0
                assert expected == state.objects[_key(original)] and expected['spec']['replicas'] == 1
                write('open')

            parent.history.activation_pool = pool
            parent.history.open_pool = open_pool
            parent.history.qualify_manager_database = lambda **kwargs: runtime('manager-db', **kwargs)
            parent.history.qualify_manager_pool_settings = lambda **kwargs: runtime('manager-settings', **kwargs)
            parent.history.qualify_gateway_runtime = lambda **kwargs: runtime('gateway', **kwargs)
            parent.guards.activation_guard = guard
            parent.guards.runtime_role = role
            parent.guards.qualify_runtime_database = lambda target, **kwargs: runtime('participant-db', **kwargs)
            parent.guards.qualify_runtime_pool_settings = lambda target, **kwargs: runtime('participant-settings', **kwargs)
            parent.guards.qualify_runtime_telemetry = lambda target, **kwargs: runtime('telemetry', **kwargs)
            original_readiness = parent._qualify_database_readiness
            def readiness():
                assert state.mode == 'closed', 'initial idle checks cannot be used for recovery'
                original_readiness()
            parent._qualify_database_readiness = readiness
            api = HTTPSPoolActivationAPI(parent=parent)
            def advance(*, cancel=False):
                return advance_pool_activation(request=request, api=api, state_dir=state_dir,
                    anchor_dir=anchor_dir, cancel=cancel)
            yield api, state, advance
    return connect


# These cases traverse several complete phases through the real retained-scope
# and HTTP readers; keep their finite limit separate from one-step unit tests.
@pytest.mark.timeout(420)
def test_connected_completion_qualifies_open_pool_without_replaying_closed_runtime(activation_http):
    from scripts.ops.nebius_pool_completion import complete_pool_cutover, load_pool_completion

    with activation_http() as (api, state, advance):
        assert advance()['status'] == 'pool_activation_complete'
        before = list(state.activation_writes)
        state.runtime_checks.clear()
        state.writes.clear()
        result = complete_pool_cutover(request=api.request, api=api, state_dir=api.state, anchor_dir=api.anchor)
        assert result['outcome'] == 'global' and result['acceptance_verified'] is False
        completed = load_pool_completion(request=api.request, state_dir=api.state, anchor_dir=api.anchor,
            completion_sha256=result['completion_sha256'])
        assert completed.workloads[_key(api.request.manager)]['metadata']['uid'] == api.request.manager['metadata']['uid']
        assert not state.runtime_checks and not state.writes and state.activation_writes == before
        state.role_damage = True
        with pytest.raises(ValueError, match='pool_completion_unqualified'):
            complete_pool_cutover(request=api.request, api=api, state_dir=api.state, anchor_dir=api.anchor)
        assert not state.writes and state.activation_writes == before


@pytest.mark.timeout(300)
def test_connected_open_release_and_cancel_preserve_authority_and_intent(activation_http):
    with activation_http() as (_, state, advance):
        assert advance()['status'] == 'pool_activation_complete'
        assert state.activation_writes == [('open', None), *(('release', key) for key in state.guards)]
        assert {'manager-db', 'gateway', 'participant-db', 'telemetry'} <= set(state.runtime_checks)
        state.runtime_checks.clear()
        state.role_checks.clear()
        state.fail_closed = True  # Simulates active work/unavailable startup proof.
        assert advance()['status'] == 'pool_activation_complete'
        assert advance(cancel=True)['status'] == 'pool_activation_cancelled'
        assert state.activation_writes[-(1 + len(state.guards)):] == [('fence', None), *(('guard-fence', key) for key in state.guards)]
        assert state.mode == 'fenced' and set(state.guards.values()) == {'fenced'}
        assert not state.runtime_checks and set(state.role_checks) == {'inspect'}
        assert not state.writes  # Activation never mutates workload roots.
        assert advance(cancel=True)['legacy_restore_allowed'] is False


def test_connected_cancel_without_started_or_healthy_successors(activation_http):
    with activation_http(start=False) as (_, state, advance):
        state.fail_closed = True
        assert advance(cancel=True)['status'] == 'pool_activation_cancelled'
        assert not state.runtime_checks and not state.writes
        assert not any(action == 'open' for action, _ in state.activation_writes)


@pytest.mark.parametrize('when', ['before', 'after'])
@pytest.mark.timeout(180)
def test_connected_opening_unknown_outcome_is_observed_not_resent(activation_http, when):
    with activation_http() as (_, state, advance):
        state.activation_failure = ('open', when)
        result = advance()
        state.activation_failure = None
        if when == 'before':
            assert result['status'] == 'pending_pool_opening'
            assert advance() == result
            state.mode = 'global'  # The original delayed dispatch settles.
        assert advance()['status'] == 'pool_activation_complete'
        assert state.activation_writes.count(('open', None)) == 1


@pytest.mark.parametrize('damage', ['material', 'role', 'inputs', 'dormant', 'acl'])
def test_connected_recovery_rejects_drift_without_mutation(activation_http, closed_startup, damage):
    _, _, closed, _, dormant, _ = closed_startup
    with activation_http(start=False) as (_, state, advance):
        if damage == 'material':
            next(row for row in state.objects.values() if row['kind'] == 'Secret')['metadata']['uid'] = 'aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaaaa'
        elif damage == 'role':
            next(iter(closed.fencing.roles.values()))['rules'].append({'apiGroups': ['batch'], 'resources': ['jobs'], 'verbs': ['create']})
        elif damage == 'inputs':
            closed.unqualified_preflight = True
        elif damage == 'dormant':
            state.objects[_key(dormant.collector)]['spec']['suspend'] = False
        else:
            state.role_damage = True
        with pytest.raises(ValueError) as error:
            advance(cancel=True)
        assert 'private-' not in str(error.value)
        assert not state.activation_writes and not state.writes


def test_connected_writes_require_anchored_intent(activation_http):
    with activation_http(start=False) as (api, state, _):
        key = next(iter(state.guards))
        for call, args in ((api.open_pool, ()), (api.fence_pool, ()),
                (api.release_guard, (key,)), (api.fence_guard, (key,)), (api.fence_guard, ('foreign',))):
            with pytest.raises(ValueError):
                call(*args)
        assert not state.activation_writes
