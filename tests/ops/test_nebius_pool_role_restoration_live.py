"""Partial retained Role restoration keeps exact scope and effective isolation."""
from __future__ import annotations

import copy
import json
from types import SimpleNamespace

import httpx
import pytest
from scripts.ops.nebius_ingress_stage import _key
from scripts.ops.nebius_management_switch import _stable
from tests.ops.test_nebius_pool_gateway_retirement import GatewayAPI, gateway_retire
from tests.ops.test_nebius_pool_role_restoration import RoleAPI, restore_roles
from tests.ops.test_nebius_pool_template_restoration import TemplateAPI, restore
from tests.ops.test_nebius_pool_template_restoration_live import activation_http as activation_http
from tests.ops.test_nebius_pool_template_restoration_live import closed_startup as closed_startup
from tests.ops.test_nebius_pool_template_restoration_live import (
    collector_inputs as collector_inputs,
)
from tests.ops.test_nebius_pool_template_restoration_live import (
    cutover_binding_inventory as cutover_binding_inventory,
)
from tests.ops.test_nebius_pool_template_restoration_live import cutover_inputs as cutover_inputs
from tests.ops.test_nebius_pool_template_restoration_live import fencing_inputs as fencing_inputs
from tests.ops.test_nebius_pool_template_restoration_live import (
    management_inputs as management_inputs,
)
from tests.ops.test_nebius_pool_template_restoration_live import platform_inputs as platform_inputs
from tests.ops.test_nebius_pool_template_restoration_live import retirement_http as retirement_http
from tests.ops.test_nebius_pool_template_restoration_live import (
    retirement_inputs as retirement_inputs,
)
from tests.ops.test_nebius_pool_template_restoration_live import runtime_inputs as runtime_inputs
from tests.ops.test_nebius_pool_template_restoration_live import startup_http as startup_http
from tests.ops.test_nebius_pool_template_restoration_live import (
    unbound_cutover_inputs as unbound_cutover_inputs,
)


@pytest.mark.timeout(600)
def test_fixed_role_restoration_cas_lost_reply_and_retained_subject_isolation(retirement_http, closed_startup, monkeypatch):
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
        remote = RoleAPI(closed_startup, templates)
        remote.legacy_failure = 'before'
        assert restore_roles(remote)['status'] == 'pending_role_restoration_outcome'
        key, = remote.legacy_calls
        before = copy.deepcopy(remote.legacy_roles[key])
        original = next(row for row in api.request.fencing.originals if _key(row) == key)
        desired = _stable(original)
        path = '/apis/rbac.authorization.k8s.io/v1/namespaces/' + original['metadata']['namespace'] + '/roles/' + original['metadata']['name']
        journal = api.state / 'role-restoration.json'
        intent = journal.read_bytes()
        transport = api.parent.client._transport
        writes, previews, reviews = [], [], []
        extra = []
        roles = closed_startup[2].fencing.roles
        participants = api.request.fencing.retirement.migration.registration.spec.participants
        owner = next(row for row in participants if row.execution_namespace.name == original['metadata']['namespace'])
        owner_subject = f'system:serviceaccount:{owner.execution_namespace.name}:loom-execution-actuator'
        untouched = copy.deepcopy(state.objects)

        def respond(message):
            subject = message.headers.get('Impersonate-User', '')
            if message.url.path.endswith('/selfsubjectrulesreviews') and not subject.endswith(':loom-pool-gateway'):
                namespace = json.loads(message.content)['spec']['namespace']
                reviews.append((subject, namespace))
                rules = []
                for participant in participants:
                    if subject != f'system:serviceaccount:{participant.execution_namespace.name}:loom-execution-actuator':
                        continue
                    rules.append({'apiGroups': [''], 'resources': ['namespaces'], 'verbs': ['get'],
                        'resourceNames': [participant.execution_namespace.name, participant.build_namespace.name]})
                    if namespace in {participant.execution_namespace.name, participant.build_namespace.name}:
                        rules.extend(copy.deepcopy(next(row['rules'] for row in roles.values() if row['metadata']['namespace'] == namespace)))
                if subject == owner_subject and namespace == owner.build_namespace.name:
                    rules.extend(extra)
                return httpx.Response(201, json={'apiVersion': 'authorization.k8s.io/v1', 'kind': 'SelfSubjectRulesReview',
                    'spec': {}, 'status': {'incomplete': False, 'resourceRules': rules, 'nonResourceRules': []}})
            if message.method != 'PATCH' or message.url.path != path:
                return transport.handle_request(message)
            assert json.loads(message.content) == [
                {'op': 'test', 'path': '/metadata/uid', 'value': before['metadata']['uid']},
                {'op': 'test', 'path': '/metadata/resourceVersion', 'value': before['metadata']['resourceVersion']},
                {'op': 'test', 'path': '/metadata', 'value': before['metadata']},
                {'op': 'test', 'path': '/rules', 'value': before['rules']},
                {'op': 'replace', 'path': '/rules', 'value': desired['rules']},
                {'op': 'remove', 'path': '/metadata/annotations'}]
            assert message.headers['Content-Type'] == 'application/json-patch+json'
            updated = copy.deepcopy(before)
            updated['rules'] = copy.deepcopy(desired['rules'])
            updated['metadata'].pop('annotations')
            if message.url.params:
                assert dict(message.url.params) == {'dryRun': 'All'}
                previews.append(key)
                return httpx.Response(200, json=updated)
            assert journal.read_bytes() == intent
            writes.append(key)
            updated['metadata']['resourceVersion'] = str(int(before['metadata']['resourceVersion']) + 1)
            roles[key] = updated
            state.inventories['roles'] = [updated if _key(row) == key else row for row in state.inventories['roles']]
            raise httpx.ReadTimeout('private-marker')

        api.parent.client._transport = httpx.MockTransport(respond)
        prepared = json.loads(intent)
        prepared['roles'][key] = {'phase': 'prepared', 'before_resource_version': None}
        journal.write_text(json.dumps(prepared))
        with pytest.raises(ValueError):
            api.restore_legacy_role(key, before, desired)
        assert api.preview_legacy_role(key, before, desired) == desired
        assert previews == [key] and not writes
        def record_intent(fresh):
            nonlocal intent
            current = json.loads(journal.read_bytes())
            assert current['roles'][key] == {'phase': 'prepared', 'before_resource_version': None}
            current['roles'][key] = {'phase': 'intent', 'before_resource_version': fresh['metadata']['resourceVersion']}
            journal.write_text(json.dumps(current))
            intent = journal.read_bytes()

        widened = copy.deepcopy(desired)
        widened['rules'][0]['verbs'].append('patch')
        with pytest.raises(ValueError):
            api.restore_legacy_role(key, before, widened, record_intent=record_intent)
        state.machine_phase = 'active'
        with pytest.raises(ValueError):
            api.restore_legacy_role(key, before, desired, record_intent=record_intent)
        state.machine_phase = 'revoked'
        assert json.loads(journal.read_bytes())['roles'][key]['phase'] == 'prepared'
        from scripts.ops import nebius_pool_activation_live as activation_live

        qualify = activation_live.qualify_role_restoration
        def advance_version_after_qualification(*args, **kwargs):
            pending = qualify(*args, **kwargs)
            assert pending is None
            roles[key]['metadata']['resourceVersion'] = str(int(before['metadata']['resourceVersion']) + 1)
            before['metadata']['resourceVersion'] = roles[key]['metadata']['resourceVersion']
            return pending
        stale = copy.deepcopy(before)
        monkeypatch.setattr(activation_live, 'qualify_role_restoration', advance_version_after_qualification)
        with pytest.raises(ValueError) as error:
            api.restore_legacy_role(key, stale, desired, record_intent=record_intent)
        assert 'private-' not in str(error.value) and writes == [key]
        # An uncertain durable intent cannot be dispatched again, even when a
        # caller supplies a new intent callback.
        with pytest.raises(ValueError):
            api.restore_legacy_role(key, before, desired, record_intent=record_intent)
        assert _stable(api.read_legacy_role(key)) == desired
        api.verify_retained()
        assert state.objects == untouched and not state.writes and not state.activation_writes
        assert reviews and journal.read_bytes() == intent
        # Restored execution writes do not permit even a named write in the
        # same owner's still-readonly build namespace.
        extra.append({'apiGroups': ['batch'], 'resources': ['jobs'], 'verbs': ['create'], 'resourceNames': ['foreign-job']})
        with pytest.raises(ValueError):
            api.verify_retained()
        extra.clear()
        api.qualify_legacy_roles()
        assert writes == [key] and journal.read_bytes() == intent
