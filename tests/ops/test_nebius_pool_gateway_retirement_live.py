"""Exact gateway Role retirement through real journals and HTTP consumers."""
from __future__ import annotations

import copy
import json
from contextlib import contextmanager

import httpx
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
from tests.ops.test_nebius_pool_gateway_retirement import (
    READER_RULES,
    gateway_retire,
    machine_retired,
)


@pytest.fixture
def retirement_http(activation_http, closed_startup):
    machine = machine_retired(closed_startup)

    @contextmanager
    def connect():
        with activation_http(start=False) as (api, state, _):
            state.mode, state.guards = machine.mode, machine.guards.copy()
            state.gateway_writes, state.gateway_failure = [], None
            state.machine_phase = 'revoked'
            for row in state.objects.values():
                if row['kind'] == 'Deployment':
                    row['metadata']['generation'] = 1
                    row['status'] = {'observedGeneration': 1, 'replicas': 0}
                elif row['kind'] == 'CronJob':
                    row['status'] = {'active': []}

            def machine_state(action):
                assert action == 'observe', 'Role retirement must never dispatch database writes'
                return state.machine_phase

            api.parent.history.machine_retirement = machine_state
            api.parent.history.recovery_pool_drained = lambda: True
            api.parent.guards.recovery_participant_drained = lambda target: True
            original_transport = api.parent.client._transport

            def apply_role(key):
                desired = copy.deepcopy(state.objects[key])
                desired['rules'] = copy.deepcopy(READER_RULES)
                desired['metadata']['resourceVersion'] = str(int(desired['metadata']['resourceVersion']) + 1)
                state.objects[key] = desired
                state.inventories['roles'] = [desired if _key(row) == key else row for row in state.inventories['roles']]
                return desired

            def respond(message):
                path = message.url.path
                if path.endswith('/selfsubjectrulesreviews'):
                    response = original_transport.handle_request(message)
                    namespace = json.loads(message.content)['spec']['namespace']
                    review = response.json()
                    review['status']['resourceRules'] = [copy.deepcopy(rule)
                        for key, row in state.objects.items() if key in machine.authority
                        and (row['kind'] == 'ClusterRole' or (row['kind'] == 'Role'
                            and row['metadata']['namespace'] == namespace)) for rule in row['rules']]
                    review['status']['resourceRules'].extend(state.gateway_extra_rules.get(namespace, []))
                    return httpx.Response(201, json=review)
                if message.method == 'GET' and message.url.params.get('limit') == '1000':
                    kind, version = {'replicasets': ('ReplicaSet', 'apps/v1'), 'jobs': ('Job', 'batch/v1'),
                        'pods': ('Pod', 'v1')}[path.rsplit('/', 1)[1]]
                    return httpx.Response(200, json={'apiVersion': version, 'kind': kind + 'List',
                        'metadata': {'resourceVersion': '7'}, 'items': []})
                if message.method == 'PATCH' and '/roles/' in path:
                    key = 'Role:' + path.split('/namespaces/')[1].replace('/roles/', ':')
                    before = state.objects[key]
                    assert message.headers['Content-Type'] == 'application/json-patch+json'
                    assert json.loads(message.content) == [
                        {'op': 'test', 'path': '/metadata/uid', 'value': before['metadata']['uid']},
                        {'op': 'test', 'path': '/metadata/resourceVersion', 'value': before['metadata']['resourceVersion']},
                        {'op': 'test', 'path': '/metadata', 'value': before['metadata']},
                        {'op': 'test', 'path': '/rules', 'value': before['rules']},
                        {'op': 'replace', 'path': '/rules', 'value': READER_RULES}]
                    if message.url.params:
                        assert dict(message.url.params) == {'dryRun': 'All'}
                        return httpx.Response(200, json={**before, 'rules': READER_RULES})
                    assert json.loads((api.state / 'gateway-retirement.json').read_bytes())['roles'][key] == {
                        'phase': 'intent', 'before_resource_version': before['metadata']['resourceVersion']}
                    state.gateway_writes.append(key)
                    if state.gateway_failure == 'before':
                        raise httpx.ReadTimeout('private-marker')
                    desired = apply_role(key)
                    if state.gateway_failure == 'after':
                        raise httpx.ReadTimeout('private-marker')
                    return httpx.Response(200, json=desired)
                return original_transport.handle_request(message)

            api.parent.client._transport = httpx.MockTransport(respond)
            yield api, state, apply_role
    return connect


# Six namespace Roles plus repeated recovery traverse every retained-scope
# barrier. The complete run measured 899.94s; retain assertions with CI headroom.
@pytest.mark.timeout(1200)
def test_connected_role_retirement_preserves_cas_intent_and_effective_authority(retirement_http, closed_startup):
    with retirement_http() as (api, state, apply_role):
        key = next(key for key in state.objects if key.startswith('Role:'))
        before = copy.deepcopy(state.objects)
        desired = {**before[key], 'rules': READER_RULES}
        # A caller cannot use the connected transport without anchored intent.
        with pytest.raises(ValueError):
            api.restrict_gateway_role(key, before[key], desired)
        state.gateway_failure = 'before'
        result = gateway_retire(closed_startup, api)
        assert result['status'] == 'pending_gateway_role_outcome'
        assert len(state.gateway_writes) == 1
        state.gateway_failure = None
        assert gateway_retire(closed_startup, api) == result and len(state.gateway_writes) == 1
        apply_role(state.gateway_writes[0])  # Only the original delayed request settles.
        state.gateway_failure = 'after'
        result = gateway_retire(closed_startup, api)
        assert result['status'] == 'pool_gateway_roles_retired' and result['legacy_restore_allowed'] is False
        assert len(state.gateway_writes) == len(set(state.gateway_writes)) == 6
        assert not state.writes and not state.activation_writes
        for key, original in before.items():
            expected = copy.deepcopy(original)
            if key in state.gateway_writes:
                expected['rules'] = READER_RULES
                expected['metadata']['resourceVersion'] = str(int(original['metadata']['resourceVersion']) + 1)
            assert state.objects[key] == expected
        assert state.gateway_reviews
        # Both retained readers must consume the exact same restricted projection.
        api._qualify_retained_resources()
        api.parent.qualify_writer_bindings()
        namespace = api.request.fencing.retirement.migration.registration.binding.namespace
        state.gateway_extra_rules[namespace] = [{'apiGroups': ['batch'], 'resources': ['jobs'],
            'verbs': ['create'], 'resourceNames': ['foreign']}]
        with pytest.raises(ValueError):
            api.qualify_gateway_retired()
        state.gateway_extra_rules.clear()
        api.qualify_gateway_retired()
        # Completed retirement is read-only; direct dispatch cannot repeat it.
        key = state.gateway_writes[0]
        with pytest.raises(ValueError):
            api.restrict_gateway_role(key, before[key], {**before[key], 'rules': READER_RULES})
        assert len(state.gateway_writes) == 6
