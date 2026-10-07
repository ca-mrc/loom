"""Bound connected recovery qualification work before the first real CAS."""
from __future__ import annotations

import copy
import json
from collections import Counter
from types import SimpleNamespace

import httpx
import pytest
from tests.ops.test_nebius_pool_gateway_retirement import GatewayAPI, gateway_retire
from tests.ops.test_nebius_pool_role_restoration import RoleAPI
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


class FirstMutationObserved(BaseException):
    """Stop measurement before transport dispatch, outside unknown-write handling."""


@pytest.mark.timeout(600)
@pytest.mark.parametrize('phase', ['gateway', 'template', 'role'])
@pytest.mark.parametrize(('prepared', 'damage'), [(False, None), (True, None),
    (True, 'authority'), (True, 'drain'), (True, 'processes')])
def test_connected_first_recovery_cas_bounds_full_qualification(retirement_http, closed_startup, monkeypatch, phase, prepared, damage):
    from scripts.ops.nebius_pool_gateway_retirement import retire_gateway_roles
    from scripts.ops.nebius_pool_role_restoration import restore_pool_roles
    from scripts.ops.nebius_pool_template_restoration import restore_pool_templates

    with retirement_http() as (api, state, apply_gateway):
        machine = SimpleNamespace(mode=state.mode, guards=state.guards, machine_phase='revoked')
        gateway = GatewayAPI(closed_startup, machine)
        preparation_api = gateway
        if phase != 'gateway':
            assert gateway_retire(closed_startup, gateway)['status'] == 'pool_gateway_roles_retired'
            for key in gateway.role_calls:
                apply_gateway(key)
            templates = TemplateAPI(closed_startup, gateway)
            preparation_api = templates
            if phase == 'role':
                assert restore(closed_startup, templates)['status'] == 'pool_legacy_templates_restored_closed'
                for key, row in templates.startup.documents.items():
                    state.objects[key]['spec'] = copy.deepcopy(row['spec'])
                    state.objects[key]['metadata']['resourceVersion'] = row['metadata']['resourceVersion']
                preparation_api = RoleAPI(closed_startup, templates)
        stage, preview_name, journal_name, items_name = {
            'gateway': (retire_gateway_roles, 'preview_gateway_role', 'gateway-retirement.json', 'roles'),
            'template': (restore_pool_templates, 'preview_legacy_template', 'template-restoration.json', 'workloads'),
            'role': (restore_pool_roles, 'preview_legacy_role', 'role-restoration.json', 'roles'),
        }[phase]
        def run():
            return stage(request=api.request, api=api, state_dir=api.state, anchor_dir=api.anchor)

        roles = closed_startup[2].fencing.roles
        participants = api.request.fencing.retirement.migration.registration.spec.participants
        counts = Counter()
        original_transport = api.parent.client._transport

        def respond(message):
            if message.method == 'GET' and message.url.params.get('limit') == '100':
                counts['inventory_pages'] += 1
            if (message.method == 'GET' and message.url.params.get('limit') == '1000'
                    and message.url.path.rsplit('/', 1)[-1] in {'replicasets', 'jobs', 'pods'}):
                counts['process_collections'] += 1
            subject = message.headers.get('Impersonate-User', '')
            if phase == 'role' and message.url.path.endswith('/selfsubjectrulesreviews') and not subject.endswith(':loom-pool-gateway'):
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
            resource = message.url.path.split('/')[-2]
            kind = {'roles': 'Role', 'cronjobs': 'CronJob', 'deployments': 'Deployment'}[resource]
            key = kind + ':' + message.url.path.split('/')[-3] + ':' + message.url.path.split('/')[-1]
            current = copy.deepcopy(roles[key] if phase == 'role' else state.objects[key])
            patches = json.loads(message.content)
            field = 'rules' if kind == 'Role' else 'spec'
            assert patches[:4] == [
                {'op': 'test', 'path': '/metadata/uid', 'value': current['metadata']['uid']},
                {'op': 'test', 'path': '/metadata/resourceVersion', 'value': current['metadata']['resourceVersion']},
                {'op': 'test', 'path': '/metadata', 'value': current['metadata']},
                {'op': 'test', 'path': '/' + field, 'value': current[field]}]
            if not message.url.params:
                journal = json.loads((api.state / journal_name).read_bytes())
                assert journal[items_name][key] == {'phase': 'intent', 'before_resource_version': current['metadata']['resourceVersion']}
                raise FirstMutationObserved
            assert dict(message.url.params) == {'dryRun': 'All'}
            counts['previews'] += 1
            current[field] = copy.deepcopy(patches[4]['value'])
            for patch in patches[5:]:
                assert patch['path'] == '/metadata/annotations'
                if patch['op'] == 'remove':
                    current['metadata'].pop('annotations')
                else:
                    assert patch['op'] == 'replace'
                    current['metadata']['annotations'] = copy.deepcopy(patch['value'])
            return httpx.Response(200, json=current)

        monkeypatch.setattr(api.parent.client, '_transport', httpx.MockTransport(respond))
        if prepared:
            # Build the prepared journal through the real stage, using the same
            # remote double as the preceding fixture stages. Measurement resumes
            # it through the connected adapter and real authority/drain readers.
            with monkeypatch.context() as preparation:
                preparation.setattr(preparation_api, preview_name, lambda *_: None)
                assert stage(request=api.request, api=preparation_api, state_dir=api.state,
                    anchor_dir=api.anchor)['status'].endswith('_update')
            phases = {row['phase'] for row in json.loads((api.state / journal_name).read_bytes())[items_name].values()}
            assert 'prepared' in phases and phases <= {'prepared', 'restored'}
        counts.clear()
        verify = api.verify_retained
        def counted_verify():
            counts['verify_retained'] += 1
            return verify()
        monkeypatch.setattr(api, 'verify_retained', counted_verify)
        pool_drained = api.parent.history.recovery_pool_drained
        def counted_pool_drained():
            counts['pool_drain_reads'] += 1
            return pool_drained()
        monkeypatch.setattr(api.parent.history, 'recovery_pool_drained', counted_pool_drained)
        if damage is not None:
            method = {'gateway': 'restrict_gateway_role', 'template': 'restore_legacy_template',
                'role': 'restore_legacy_role'}[phase]
            dispatch = getattr(api, method)
            dispatched = []
            def drift_before_dispatch(key, *args, **kwargs):
                # The stage and dry-run already succeeded. The actual adapter
                # must still reject newly missing rights or fresh drain failures.
                dispatched.append(key)
                if damage == 'authority':
                    state.role_damage = True
                elif damage == 'drain':
                    api.parent.history.recovery_pool_drained = lambda: False
                else:
                    controller = next(state.objects[name] for name in api.targets if name.startswith('Deployment:'))
                    controller['status']['observedGeneration'] = 0
                return dispatch(key, *args, **kwargs)
            monkeypatch.setattr(api, method, drift_before_dispatch)
            if phase in {'template', 'role'}:
                if damage == 'authority':
                    with pytest.raises(ValueError, match='unconfirmed_preserve_evidence'):
                        run()
                else:
                    assert run()['status'] == ('pending_pool_cleanup' if damage == 'drain' else 'pending_successor_drain')
            else:
                assert run()['status'] == ('pending_gateway_role_outcome' if phase == 'gateway' else 'pending_role_restoration_outcome')
            assert len(dispatched) == 1 and counts['previews'] == 1
            row = json.loads((api.state / journal_name).read_bytes())[items_name][dispatched[0]]
            assert row['phase'] == ('prepared' if phase in {'template', 'role'} else 'intent')
            assert not state.gateway_writes and not state.writes and not state.activation_writes
            return
        with pytest.raises(FirstMutationObserved):
            run()
        print('first recovery CAS', phase, 'prepared', prepared, dict(counts))
        assert counts['previews'] == 1
        assert counts['verify_retained'] == ({'gateway': 8, 'template': 6, 'role': 6} if prepared
            else {'gateway': 11, 'template': 27, 'role': 13})[phase]
        if phase == 'template':
            assert counts['inventory_pages'] == (146 if prepared else 656)
            assert counts['pool_drain_reads'] == (4 if prepared else 16)
            assert counts['process_collections'] == (4 if prepared else 16) * len(api.targets)
        assert not state.gateway_writes and not state.writes and not state.activation_writes
