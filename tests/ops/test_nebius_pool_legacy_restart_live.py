"""Fixed scalar legacy restart cannot revive the successor gateway or intake."""
from __future__ import annotations

import copy
import json
from types import SimpleNamespace

import httpx
import pytest
from scripts.ops.nebius_ingress_stage import _key
from scripts.ops.nebius_management_switch import _stable
from tests.ops.test_nebius_pool_gateway_retirement import GatewayAPI, gateway_retire
from tests.ops.test_nebius_pool_legacy_restart import RestartAPI, restart
from tests.ops.test_nebius_pool_role_restoration import RoleAPI, restore_roles
from tests.ops.test_nebius_pool_role_restoration_live import activation_http as activation_http
from tests.ops.test_nebius_pool_role_restoration_live import closed_startup as closed_startup
from tests.ops.test_nebius_pool_role_restoration_live import collector_inputs as collector_inputs
from tests.ops.test_nebius_pool_role_restoration_live import (
    cutover_binding_inventory as cutover_binding_inventory,
)
from tests.ops.test_nebius_pool_role_restoration_live import cutover_inputs as cutover_inputs
from tests.ops.test_nebius_pool_role_restoration_live import fencing_inputs as fencing_inputs
from tests.ops.test_nebius_pool_role_restoration_live import management_inputs as management_inputs
from tests.ops.test_nebius_pool_role_restoration_live import platform_inputs as platform_inputs
from tests.ops.test_nebius_pool_role_restoration_live import retirement_http as retirement_http
from tests.ops.test_nebius_pool_role_restoration_live import retirement_inputs as retirement_inputs
from tests.ops.test_nebius_pool_role_restoration_live import runtime_inputs as runtime_inputs
from tests.ops.test_nebius_pool_role_restoration_live import startup_http as startup_http
from tests.ops.test_nebius_pool_role_restoration_live import (
    unbound_cutover_inputs as unbound_cutover_inputs,
)
from tests.ops.test_nebius_pool_template_restoration import TemplateAPI, restore


@pytest.mark.timeout(600)
def test_legacy_restart_transport_keeps_intent_after_lost_reply_and_qualifies_running_old_template(retirement_http, closed_startup):
    with retirement_http() as (api, state, apply_gateway):
        machine = SimpleNamespace(mode=state.mode, guards=state.guards, machine_phase='revoked')
        gateway = GatewayAPI(closed_startup, machine)
        assert gateway_retire(closed_startup, gateway)['status'] == 'pool_gateway_roles_retired'
        for key in gateway.role_calls:
            apply_gateway(key)
        templates = TemplateAPI(closed_startup, gateway)
        assert restore(closed_startup, templates)['status'] == 'pool_legacy_templates_restored_closed'
        for key, row in templates.startup.documents.items():
            state.objects[key]['spec'] = copy.deepcopy(row['spec'])
            state.objects[key]['metadata']['resourceVersion'] = row['metadata']['resourceVersion']
        remote_roles = RoleAPI(closed_startup, templates)
        assert restore_roles(remote_roles)['status'] == 'pool_legacy_roles_restored_closed'
        roles = closed_startup[2].fencing.roles
        roles.update(copy.deepcopy(remote_roles.legacy_roles))
        state.inventories['roles'] = [roles.get(_key(row), row) for row in state.inventories['roles']]
        remote = RestartAPI(closed_startup, remote_roles)
        remote.restart_failure = 'before'
        assert restart(remote)['status'] == 'pending_legacy_restart_outcome'
        key, = remote.restart_calls
        assert key == _key(api.request.manager)
        before = copy.deepcopy(state.objects[key])
        desired = _stable(before)
        desired['spec']['replicas'] = 1
        path = api._path(key)
        transport = api.parent.client._transport
        writes, previews = [], []
        journal = api.state / 'legacy-restart.json'
        intent = journal.read_bytes()
        originals = copy.deepcopy(state.objects)
        participants = api.request.fencing.retirement.migration.registration.spec.participants

        def respond(message):
            subject = message.headers.get('Impersonate-User', '')
            if message.url.path.endswith('/selfsubjectrulesreviews') and not subject.endswith(':loom-pool-gateway'):
                namespace = json.loads(message.content)['spec']['namespace']
                rules = []
                for participant in participants:
                    if subject != f'system:serviceaccount:{participant.execution_namespace.name}:loom-execution-actuator':
                        continue
                    rules.append({'apiGroups': [''], 'resources': ['namespaces'], 'verbs': ['get'],
                        'resourceNames': [participant.execution_namespace.name, participant.build_namespace.name]})
                    if namespace in {participant.execution_namespace.name, participant.build_namespace.name}:
                        rules.extend(copy.deepcopy(next(row['rules'] for row in roles.values() if row['metadata']['namespace'] == namespace)))
                return httpx.Response(201, json={'apiVersion': 'authorization.k8s.io/v1', 'kind': 'SelfSubjectRulesReview',
                    'spec': {}, 'status': {'incomplete': False, 'resourceRules': rules, 'nonResourceRules': []}})
            if message.method != 'PATCH' or message.url.path != path:
                return transport.handle_request(message)
            assert json.loads(message.content) == [
                {'op': 'test', 'path': '/metadata/uid', 'value': before['metadata']['uid']},
                {'op': 'test', 'path': '/metadata/resourceVersion', 'value': before['metadata']['resourceVersion']},
                {'op': 'test', 'path': '/metadata', 'value': before['metadata']},
                {'op': 'test', 'path': '/spec', 'value': before['spec']},
                {'op': 'replace', 'path': '/spec/replicas', 'value': 1}]
            assert message.headers['Content-Type'] == 'application/json-patch+json'
            updated = copy.deepcopy(before)
            updated['spec']['replicas'] = 1
            if message.url.params:
                assert dict(message.url.params) == {'dryRun': 'All'}
                previews.append(key)
                return httpx.Response(200, json=updated)
            assert journal.read_bytes() == intent
            writes.append(key)
            updated['metadata']['resourceVersion'] = str(int(before['metadata']['resourceVersion']) + 1)
            state.objects[key] = updated
            raise httpx.ReadTimeout('private-marker')

        api.parent.client._transport = httpx.MockTransport(respond)
        prepared = json.loads(intent)
        prepared['workloads'][key] = {'phase': 'prepared', 'before_resource_version': None}
        journal.write_text(json.dumps(prepared))
        with pytest.raises(ValueError):
            api.restart_legacy_workload(key, before, desired)
        assert api.preview_legacy_restart(key, before, desired) == desired
        assert previews == [key] and not writes
        journal.write_bytes(intent)
        widened = copy.deepcopy(desired)
        widened['spec']['replicas'] = 2
        with pytest.raises(ValueError):
            api.restart_legacy_workload(key, before, widened)
        state.machine_phase = 'active'
        with pytest.raises(ValueError):
            api.restart_legacy_workload(key, before, desired)
        state.machine_phase = 'revoked'
        with pytest.raises(ValueError) as error:
            api.restart_legacy_workload(key, before, desired)
        assert 'private-' not in str(error.value) and writes == [key]
        api.verify_retained()
        assert _stable(api.read_workload(key)) == desired
        assert state.mode == 'fenced' and set(state.guards.values()) == {'fenced'}
        assert {name: row for name, row in state.objects.items() if name != key} == {
            name: row for name, row in originals.items() if name != key}
        assert not state.activation_writes and journal.read_bytes() == intent
        # A running old manager is valid recovery state, but it must not let a
        # new gateway process or effective write grant pass the restart barrier.
        from scripts.ops.nebius_pool_legacy_restart import qualify_legacy_restart

        assert qualify_legacy_restart(api.request, api, state=api.state, anchor=api.anchor) is None
        manager = api.request.fencing.retirement.migration.registration.binding.namespace
        state.gateway_extra_rules[manager] = [{'apiGroups': ['batch'], 'resources': ['jobs'],
            'verbs': ['create'], 'resourceNames': ['foreign-job']}]
        with pytest.raises(ValueError):
            qualify_legacy_restart(api.request, api, state=api.state, anchor=api.anchor)
        assert writes == [key]
