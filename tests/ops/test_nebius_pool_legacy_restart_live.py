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
from tests.ops.test_nebius_pool_legacy_restart import RestartAPI, restart, roles_restored
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
from tests.ops.test_nebius_pool_role_restoration_live import startup_http as startup_http
from tests.ops.test_nebius_pool_role_restoration_live import (
    unbound_cutover_inputs as unbound_cutover_inputs,
)
from tests.ops.test_nebius_pool_template_restoration import TemplateAPI, gateway_retired, restore
from tests.support.pool_transport import runtime_inputs as runtime_inputs


@pytest.mark.timeout(600)
@pytest.mark.parametrize('phase', ['template', 'restart'])
@pytest.mark.parametrize('damage', [None, 'uid', 'metadata', 'spec'])
def test_recovery_cas_uses_final_controller_version_after_qualification(closed_startup, monkeypatch, phase, damage):
    from scripts.ops import nebius_pool_activation_live as live
    from scripts.ops.nebius_pool_startup import closed_startup_documents

    api = gateway_retired(closed_startup) if phase == 'template' else roles_restored(closed_startup)
    journal_name = 'template-restoration.json' if phase == 'template' else 'legacy-restart.json'
    qualify_name = 'qualify_template_restoration' if phase == 'template' else 'qualify_legacy_restart'
    patch_method = getattr(live.HTTPSPoolActivationAPI, '_legacy_' + phase + '_patch')
    writes = []

    def respond(message):
        kind = 'CronJob' if '/cronjobs/' in message.url.path else 'Deployment'
        key = kind + ':' + message.url.path.split('/')[-3] + ':' + message.url.path.split('/')[-1]
        current = api.startup.documents[key]
        patches = json.loads(message.content)
        version = next(row['value'] for row in patches if row['path'] == '/metadata/resourceVersion')
        if version != current['metadata']['resourceVersion']:
            return httpx.Response(422, json={'apiVersion': 'v1', 'kind': 'Status',
                'status': 'Failure', 'code': 422, 'reason': 'Invalid'})
        assert patches[:4] == [
            {'op': 'test', 'path': '/metadata/uid', 'value': current['metadata']['uid']},
            {'op': 'test', 'path': '/metadata/resourceVersion', 'value': version},
            {'op': 'test', 'path': '/metadata', 'value': current['metadata']},
            {'op': 'test', 'path': '/spec', 'value': current['spec']}]
        desired = copy.deepcopy(current)
        if phase == 'template':
            desired['spec'] = patches[-1]['value']
        else:
            desired['spec'][patches[-1]['path'].split('/')[-1]] = patches[-1]['value']
        if not message.url.params:
            journal = json.loads((api.state / journal_name).read_bytes())
            assert journal['workloads'][key] == {'phase': 'intent', 'before_resource_version': version}
            writes.append(key)
            desired['metadata']['resourceVersion'] = str(int(version) + 1)
            api.startup.documents[key] = desired
        return httpx.Response(200, json=desired)

    with httpx.Client(base_url='https://kubernetes.invalid', transport=httpx.MockTransport(respond)) as client:
        closed, _ = closed_startup_documents(api.request, state_dir=api.state, anchor_dir=api.root / 'cutover-anchor')
        adapter = SimpleNamespace(request=api.request, state=api.state, anchor=api.root / 'cutover-anchor',
            closed=closed,
            parent=SimpleNamespace(client=client), _scope=lambda: None,
            _path=lambda key: '/apis/' + ('batch/v1' if key.startswith('CronJob:') else 'apps/v1')
                + '/namespaces/' + key.split(':')[1] + '/' + ('cronjobs' if key.startswith('CronJob:') else 'deployments')
                + '/' + key.split(':')[2],
            **{name: getattr(api, name) for name in ('read_workload', 'verify_retained', 'recovery_drained',
                'pool_state', 'machine_authority', 'guard_state', 'qualify_legacy_roles',
                'qualify_gateway_readonly', 'qualify_gateway_retired', 'successor_drained') if hasattr(api, name)})
        qualify = getattr(live, qualify_name)
        active, qualifications, dispatches = [], [], []

        def slow_qualification(*args, **kwargs):
            result = qualify(*args, **kwargs)
            qualifications.append(active[-1])
            # Real controllers may update status during the ledger/inventory
            # checks without changing any UID, stable metadata or spec.
            for current in api.startup.documents.values():
                current['metadata']['resourceVersion'] = str(int(current['metadata']['resourceVersion']) + 1)
            if damage is not None and dispatches:
                current = api.startup.documents[active[-1]]
                if damage == 'uid':
                    current['metadata']['uid'] = 'foreign-controller'
                elif damage == 'metadata':
                    current['metadata'].setdefault('annotations', {})['foreign'] = 'changed'
                else:
                    current['spec']['foreign'] = 'changed'
            return result

        def preview(key, before, desired):
            active.append(key)
            return copy.deepcopy(desired) if patch_method(adapter, key, before, desired, preview=True) else None

        def dispatch(key, before, desired, **kwargs):
            dispatches.append(key)
            return patch_method(adapter, key, before, desired, preview=False, **kwargs)

        monkeypatch.setattr(live, qualify_name, slow_qualification)
        monkeypatch.setattr(api, 'preview_legacy_' + phase, preview)
        monkeypatch.setattr(api, 'restore_legacy_template' if phase == 'template' else 'restart_legacy_workload', dispatch)
        if damage is not None:
            with pytest.raises(ValueError, match='unconfirmed_preserve_evidence'):
                restore(closed_startup, api) if phase == 'template' else restart(api)
            assert qualifications and dispatches == [active[-1]] and not writes
            journal = json.loads((api.state / journal_name).read_bytes())
            assert journal['workloads'][active[-1]] == {'phase': 'prepared', 'before_resource_version': None}
            return
        result = restore(closed_startup, api) if phase == 'template' else restart(api)
        assert result['status'] == ('pool_legacy_templates_restored_closed' if phase == 'template'
            else 'pool_legacy_restart_staged_closed')
        assert writes and len(writes) == len(set(writes))


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
        def record_intent(fresh):
            assert fresh == before and json.loads(journal.read_bytes()) == prepared
            journal.write_bytes(intent)

        # An uncertain attempt may only be observed, never reissued or rebound.
        journal.write_bytes(intent)
        with pytest.raises(ValueError):
            api.restart_legacy_workload(key, before, desired, record_intent=record_intent)
        assert not writes and journal.read_bytes() == intent
        journal.write_text(json.dumps(prepared))
        widened = copy.deepcopy(desired)
        widened['spec']['replicas'] = 2
        with pytest.raises(ValueError):
            api.restart_legacy_workload(key, before, widened, record_intent=record_intent)
        state.machine_phase = 'active'
        with pytest.raises(ValueError):
            api.restart_legacy_workload(key, before, desired, record_intent=record_intent)
        state.machine_phase = 'revoked'
        with pytest.raises(ValueError) as error:
            api.restart_legacy_workload(key, before, desired, record_intent=record_intent)
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
