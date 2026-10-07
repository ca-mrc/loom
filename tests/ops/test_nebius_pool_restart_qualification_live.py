"""Fixed scalar legacy restart cannot revive the successor gateway or intake."""
from __future__ import annotations

import copy
import json
from collections import Counter
from types import SimpleNamespace

import httpx
import pytest
from scripts.ops.nebius_ingress_stage import _key
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


class FirstRestartMutation(BaseException):
    """Stop before HTTP mutation without entering unknown-write recovery."""


@pytest.mark.timeout(600)
@pytest.mark.parametrize('scenario', ['prepared', 'new', 'drain', 'authority', 'processes', 'unknown'])
def test_restart_prepared_dispatch_qualifies_once(retirement_http, closed_startup, monkeypatch, scenario):
    from scripts.ops.nebius_pool_legacy_restart import restart_pool_legacy

    with retirement_http() as (api, state, apply_gateway):
        gateway = GatewayAPI(closed_startup, SimpleNamespace(mode=state.mode, guards=state.guards, machine_phase='revoked'))
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
        if scenario != 'new':
            remote = RestartAPI(closed_startup, remote_roles)
            monkeypatch.setattr(remote, 'preview_legacy_restart', lambda *_: None)
            assert restart(remote)['status'] == 'pending_legacy_restart_update'
        journal = api.state / 'legacy-restart.json'
        original_transport = api.parent.client._transport
        participants = api.request.fencing.retirement.migration.registration.spec.participants
        counts = Counter()
        writes = []

        def respond(message):
            subject = message.headers.get('Impersonate-User', '')
            if message.method == 'GET' and message.url.params.get('limit') == '100':
                counts['pages'] += 1
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
            if message.method != 'PATCH':
                return original_transport.handle_request(message)
            key = 'Deployment:' + message.url.path.split('/')[-3] + ':' + message.url.path.split('/')[-1]
            assert key == _key(api.request.manager)
            current = state.objects[key]
            patches = json.loads(message.content)
            assert patches[:4] == [
                {'op': 'test', 'path': '/metadata/uid', 'value': current['metadata']['uid']},
                {'op': 'test', 'path': '/metadata/resourceVersion', 'value': current['metadata']['resourceVersion']},
                {'op': 'test', 'path': '/metadata', 'value': current['metadata']},
                {'op': 'test', 'path': '/spec', 'value': current['spec']}]
            desired = copy.deepcopy(current)
            assert patches[4]['path'] == '/spec/replicas'
            desired['spec']['replicas'] = patches[4]['value']
            if message.url.params:
                assert dict(message.url.params) == {'dryRun': 'All'}
                counts['preview'] += 1
                return httpx.Response(200, json=desired)
            assert json.loads(journal.read_bytes())['workloads'][key] == {
                'phase': 'intent', 'before_resource_version': current['metadata']['resourceVersion']}
            writes.append(key)
            if scenario == 'unknown':
                raise httpx.ReadTimeout('private-lost-reply')
            raise FirstRestartMutation

        monkeypatch.setattr(api.parent.client, '_transport', httpx.MockTransport(respond))
        verify = api.verify_retained
        def counted_verify():
            counts['verify'] += 1
            return verify()
        monkeypatch.setattr(api, 'verify_retained', counted_verify)
        dispatch = api.restart_legacy_workload
        def actual_dispatch(key, *args, **kwargs):
            counts['dispatch'] += 1
            if scenario == 'drain':
                api.parent.history.recovery_pool_drained = lambda: False
            elif scenario == 'authority':
                state.role_damage = True
            elif scenario == 'processes':
                gateway_key = 'Deployment:' + api.request.fencing.retirement.migration.registration.binding.namespace + ':loom-pool-gateway'
                state.objects[gateway_key]['status']['observedGeneration'] = 0
            return dispatch(key, *args, **kwargs)
        monkeypatch.setattr(api, 'restart_legacy_workload', actual_dispatch)
        def run():
            return restart_pool_legacy(request=api.request, api=api, state_dir=api.state, anchor_dir=api.anchor)

        if scenario in {'prepared', 'new'}:
            with pytest.raises(FirstRestartMutation):
                run()
            print('restart first CAS', scenario, dict(counts))
            assert counts['verify'] == (11 if scenario == 'new' else 4)
            assert counts['dispatch'] == counts['preview'] == len(writes) == 1
        elif scenario == 'authority':
            with pytest.raises(ValueError, match='unconfirmed_preserve_evidence'):
                run()
            assert counts['dispatch'] == counts['preview'] == 1 and not writes
        elif scenario == 'unknown':
            assert run()['status'] == 'pending_legacy_restart_outcome'
            saved = journal.read_bytes()
            assert run()['status'] == 'pending_legacy_restart_outcome'
            assert counts['dispatch'] == counts['preview'] == len(writes) == 1
            assert journal.read_bytes() == saved
        else:
            assert run()['status'] == ('pending_pool_cleanup' if scenario == 'drain' else 'pending_successor_drain')
            assert counts['dispatch'] == counts['preview'] == 1 and not writes
        if scenario in {'drain', 'authority', 'processes'}:
            assert all(row == {'phase': 'prepared', 'before_resource_version': None}
                for row in json.loads(journal.read_bytes())['workloads'].values())
