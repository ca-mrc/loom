"""Connected machine retirement uses parent authority only after process drain."""
from __future__ import annotations

import json

import httpx
import pytest
from tests.ops.test_nebius_pool_shutdown_live import activation_http as activation_http
from tests.ops.test_nebius_pool_shutdown_live import closed_startup as closed_startup
from tests.ops.test_nebius_pool_shutdown_live import collector_inputs as collector_inputs
from tests.ops.test_nebius_pool_shutdown_live import (
    cutover_binding_inventory as cutover_binding_inventory,
)
from tests.ops.test_nebius_pool_shutdown_live import cutover_inputs as cutover_inputs
from tests.ops.test_nebius_pool_shutdown_live import fencing_inputs as fencing_inputs
from tests.ops.test_nebius_pool_shutdown_live import management_inputs as management_inputs
from tests.ops.test_nebius_pool_shutdown_live import platform_inputs as platform_inputs
from tests.ops.test_nebius_pool_shutdown_live import retirement_inputs as retirement_inputs
from tests.ops.test_nebius_pool_shutdown_live import runtime_inputs as runtime_inputs
from tests.ops.test_nebius_pool_shutdown_live import startup_http as startup_http
from tests.ops.test_nebius_pool_shutdown_live import (
    unbound_cutover_inputs as unbound_cutover_inputs,
)


# Traverses cancellation, fencing, shutdown, retirement and full replay through
# retained-scope readers. The first 240s run reached the final replay barrier.
@pytest.mark.timeout(360)
def test_connected_retirement_binds_intent_and_survives_revocation_and_lost_reply(activation_http, closed_startup, monkeypatch):
    from scripts.ops.nebius_pool_machine_retirement import retire_pool_machines
    from scripts.ops.nebius_pool_shutdown import stop_pool_successors
    from scripts.ops.nebius_pool_startup_fence import fence_pool_startup

    request, _, _, _, _, root = closed_startup
    with activation_http(start=False) as (api, state, advance):
        with pytest.raises(ValueError):
            api.retire_machines()
        assert advance(cancel=True)['status'] == 'pool_activation_cancelled'
        assert fence_pool_startup(request=request, api=api, state_dir=root / 'cutover',
            anchor_dir=root / 'cutover-anchor')['status'] == 'startup_writes_fenced'
        api.parent.history.recovery_pool_drained = lambda: True
        api.parent.guards.recovery_participant_drained = lambda target: True
        for key in api.closed:
            current = state.objects[key]
            current['metadata']['generation'] = 1
            current['status'] = {'observedGeneration': 1} if current['kind'] == 'Deployment' else {'active': []}
        machine_state = {'phase': 'active', 'writes': 0}
        def machines(action):
            if action == 'revoke':
                assert json.loads((root / 'cutover/machine-retirement.json').read_bytes())['phase'] == 'intent'
                machine_state['writes'] += 1
                machine_state['phase'] = 'revoked'
                raise OSError('private-marker')
            assert action == 'observe'
            return machine_state['phase']
        api.parent.history.machine_retirement = machines
        original_transport = api.parent.client._transport
        def respond(message):
            if dict(message.url.params) == {'limit': '1000'}:
                assert message.method == 'GET'
                resource = message.url.path.rsplit('/', 1)[1]
                kind, version = {'pods': ('Pod', 'v1'), 'jobs': ('Job', 'batch/v1'), 'replicasets': ('ReplicaSet', 'apps/v1')}[resource]
                return httpx.Response(200, json={'apiVersion': version, 'kind': kind + 'List',
                    'metadata': {'resourceVersion': '50'}, 'items': []})
            return original_transport.handle_request(message)
        with httpx.MockTransport(respond) as transport:
            monkeypatch.setattr(api.parent.client, '_transport', transport)
            assert stop_pool_successors(request=request, api=api, state_dir=root / 'cutover',
                anchor_dir=root / 'cutover-anchor')['status'] == 'pool_successors_stopped'
            def run():
                return retire_pool_machines(request=request, api=api, state_dir=root / 'cutover', anchor_dir=root / 'cutover-anchor')
            assert run()['status'] == 'pool_machines_retired'
            assert run()['legacy_restore_allowed'] is False
            assert machine_state == {'phase': 'revoked', 'writes': 1}
            with pytest.raises(ValueError):
                api.retire_machines()
            assert not state.writes
