"""Terminal startup CAS uses the activation parent's exact HTTPS authority."""
from __future__ import annotations

import copy

import pytest
from scripts.ops.nebius_ingress_stage import _key
from tests.ops.test_nebius_pool_activation_live import activation_http as activation_http
from tests.ops.test_nebius_pool_activation_live import closed_startup as closed_startup
from tests.ops.test_nebius_pool_activation_live import collector_inputs as collector_inputs
from tests.ops.test_nebius_pool_activation_live import (
    cutover_binding_inventory as cutover_binding_inventory,
)
from tests.ops.test_nebius_pool_activation_live import cutover_inputs as cutover_inputs
from tests.ops.test_nebius_pool_activation_live import fencing_inputs as fencing_inputs
from tests.ops.test_nebius_pool_activation_live import management_inputs as management_inputs
from tests.ops.test_nebius_pool_activation_live import platform_inputs as platform_inputs
from tests.ops.test_nebius_pool_activation_live import retirement_inputs as retirement_inputs
from tests.ops.test_nebius_pool_activation_live import runtime_inputs as runtime_inputs
from tests.ops.test_nebius_pool_activation_live import startup_http as startup_http
from tests.ops.test_nebius_pool_activation_live import (
    unbound_cutover_inputs as unbound_cutover_inputs,
)
from tests.ops.test_nebius_pool_startup import start


@pytest.mark.timeout(180)
@pytest.mark.parametrize('failure', [None, 'before', 'after'])
def test_connected_fence_uses_fixed_patch_and_readonly_unknown_recovery(activation_http, closed_startup, failure):
    from scripts.ops.nebius_pool_startup_fence import fence_pool_startup, marked_startup_document

    request, _, _, startup, _, root = closed_startup
    key = _key(request.manager)
    startup.fail_key, startup.failure = key, 'before'
    assert start(closed_startup)['status'] == 'pending_startup_outcome'
    with activation_http(start=False) as (api, state, advance):
        assert advance(cancel=True)['status'] == 'pool_activation_cancelled'
        before = copy.deepcopy(state.objects[key])
        desired = marked_startup_document(before, request.fencing.retirement.migration.registration.spec.operation_id)
        # Even the fixed transport cannot write before its own anchored intent.
        with pytest.raises(ValueError):
            api.fence_startup(key, before, desired)
        assert state.writes == []
        state.failure = failure
        def run():
            return fence_pool_startup(request=request, api=api, state_dir=root / 'cutover', anchor_dir=root / 'cutover-anchor')
        result = run()
        state.failure = None
        if failure == 'before':
            assert result['status'] == 'pending_startup_fence'
            assert run() == result
            # The same original fence becomes observable; replay cannot resend it.
            state.objects[key]['metadata']['annotations'] = desired['metadata']['annotations']
            state.objects[key]['metadata']['resourceVersion'] = str(int(before['metadata']['resourceVersion']) + 1)
        assert run()['status'] == 'startup_writes_fenced'
        assert state.writes == [key]
        assert state.objects[key]['spec'] == before['spec']
        assert advance(cancel=True)['status'] == 'pool_activation_cancelled'
