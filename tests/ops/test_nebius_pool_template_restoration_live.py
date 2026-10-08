"""Connected template CAS and drain readers share anchored partial restoration."""
from __future__ import annotations

import copy
import json
from collections import Counter
from types import SimpleNamespace

import httpx
import pytest
from scripts.ops.nebius_management_switch import _stable
from tests.ops.test_nebius_pool_gateway_retirement import GatewayAPI, gateway_retire
from tests.ops.test_nebius_pool_gateway_retirement_live import activation_http as activation_http
from tests.ops.test_nebius_pool_gateway_retirement_live import closed_startup as closed_startup
from tests.ops.test_nebius_pool_gateway_retirement_live import collector_inputs as collector_inputs
from tests.ops.test_nebius_pool_gateway_retirement_live import (
    cutover_binding_inventory as cutover_binding_inventory,
)
from tests.ops.test_nebius_pool_gateway_retirement_live import cutover_inputs as cutover_inputs
from tests.ops.test_nebius_pool_gateway_retirement_live import fencing_inputs as fencing_inputs
from tests.ops.test_nebius_pool_gateway_retirement_live import (
    management_inputs as management_inputs,
)
from tests.ops.test_nebius_pool_gateway_retirement_live import platform_inputs as platform_inputs
from tests.ops.test_nebius_pool_gateway_retirement_live import retirement_http as retirement_http
from tests.ops.test_nebius_pool_gateway_retirement_live import (
    retirement_inputs as retirement_inputs,
)
from tests.ops.test_nebius_pool_gateway_retirement_live import runtime_inputs as runtime_inputs
from tests.ops.test_nebius_pool_gateway_retirement_live import startup_http as startup_http
from tests.ops.test_nebius_pool_gateway_retirement_live import (
    unbound_cutover_inputs as unbound_cutover_inputs,
)
from tests.ops.test_nebius_pool_template_restoration import TemplateAPI, restore


@pytest.mark.timeout(180)
@pytest.mark.parametrize('damage', [None, 'shutdown.json', 'cutover.json', 'template-restoration.json'])
def test_single_successor_drain_reads_real_recovery_projection(retirement_http, closed_startup, monkeypatch, damage):
    from scripts.ops import nebius_certificates as private_state
    from scripts.ops.nebius_pool_shutdown import _shutdown_record

    with retirement_http() as (api, state, apply_role):
        machine = SimpleNamespace(mode=state.mode, guards=state.guards, machine_phase='revoked')
        gateway = GatewayAPI(closed_startup, machine)
        assert gateway_retire(closed_startup, gateway)['status'] == 'pool_gateway_roles_retired'
        for key in gateway.role_calls:
            apply_role(key)
        remote = TemplateAPI(closed_startup, gateway)
        monkeypatch.setattr(remote, 'preview_legacy_template', lambda *_: None)
        assert restore(closed_startup, remote)['status'] == 'pending_template_restoration_update'
        targets = _shutdown_record(api.request, state=api.state, anchor=api.anchor)[2]
        key = next(iter(targets))
        operation = api.request.fencing.retirement.migration.registration.spec.operation_id
        marker = api.anchor / (str(operation) + '-cutover.json')
        private_read = private_state._private_read
        counts = Counter()
        transport = api.parent.client._transport

        def read(path, **kwargs):
            counts['private_reads'] += 1
            counts['closure_reads'] += path == marker
            return private_read(path, **kwargs)

        def respond(message):
            response = transport.handle_request(message)
            if message.method == 'GET' and message.url.params.get('limit') == '1000':
                counts['process_collections'] += 1
                if damage is not None and message.url.path.endswith('/pods'):
                    path = api.state / damage
                    document = json.loads(path.read_bytes())
                    document['unexpected'] = True
                    path.write_text(json.dumps(document))
                    counts['damage_between_reads'] += 1
            return response

        monkeypatch.setattr(private_state, '_private_read', read)
        monkeypatch.setattr(api.parent.client, '_transport', httpx.MockTransport(respond))
        if damage is not None:
            with pytest.raises(ValueError, match='pool_successor_drain_unconfirmed'):
                api.successor_drained(key, targets[key])
            assert counts['damage_between_reads'] == 1
            assert not state.writes and not state.gateway_writes
            return
        assert api.successor_drained(key, targets[key]) is True
        print('single successor drain', dict(counts))
        assert counts['process_collections'] == 2
        assert counts['closure_reads'] == 2


def test_original_shutdown_drain_stays_strict_without_restoration(retirement_http):
    from scripts.ops.nebius_pool_shutdown import _shutdown_record

    with retirement_http() as (api, state, _):
        targets = _shutdown_record(api.request, state=api.state, anchor=api.anchor)[2]
        key = next(key for key in targets if key.startswith('Deployment:'))
        assert api.successor_drained(key, targets[key]) is True
        state.objects[key]['status']['observedGeneration'] = 0
        assert api.successor_drained(key, targets[key]) is False
        state.objects[key]['status']['observedGeneration'] = 1
        # An orphan restoration journal cannot fall back to the original proof.
        (api.state / 'template-restoration.json').write_text('{}')
        with pytest.raises(ValueError):
            api.successor_drained(key, targets[key])


@pytest.mark.timeout(600)
def test_fixed_template_patch_uses_intent_and_keeps_unobserved_generation_undrained(retirement_http, closed_startup):
    from scripts.ops.nebius_pool_shutdown import _shutdown_record
    from scripts.ops.nebius_pool_template_restoration import _template_record

    with retirement_http() as (api, state, apply_role):
        # Prior stages have independent connected coverage. Build their exact
        # completed journals here through the production stage and remote double.
        machine = SimpleNamespace(mode=state.mode, guards=state.guards, machine_phase='revoked')
        gateway = GatewayAPI(closed_startup, machine)
        assert gateway_retire(closed_startup, gateway)['status'] == 'pool_gateway_roles_retired'
        for key in gateway.role_calls:
            apply_role(key)
        remote = TemplateAPI(closed_startup, gateway)
        _, originals, targets, _, _ = _template_record(api.request, state=api.state, anchor=api.anchor)
        key = next(key for key in targets if key.startswith('Deployment:') and originals[key] != targets[key])
        remote.template_fail_key = key
        remote.template_failure = 'before'
        assert restore(closed_startup, remote)['status'] == 'pending_template_restoration_outcome'
        assert remote.template_calls[-1] == key
        # Restore order includes CronJobs. Carry their already-settled remote
        # effects into HTTP readback before testing a Deployment generation.
        for previous in remote.template_calls[:-1]:
            state.objects[previous]['spec'] = copy.deepcopy(remote.startup.documents[previous]['spec'])
            state.objects[previous]['metadata']['resourceVersion'] = remote.startup.documents[previous]['metadata']['resourceVersion']
        before = copy.deepcopy(state.objects[key])
        desired = _template_record(api.request, state=api.state, anchor=api.anchor)[2][key]
        path = api._path(key)
        transport = api.parent.client._transport
        writes, previews = [], []

        def respond(message):
            if message.method != 'PATCH' or message.url.path != path:
                return transport.handle_request(message)
            assert json.loads(message.content) == [
                {'op': 'test', 'path': '/metadata/uid', 'value': before['metadata']['uid']},
                {'op': 'test', 'path': '/metadata/resourceVersion', 'value': before['metadata']['resourceVersion']},
                {'op': 'test', 'path': '/metadata', 'value': before['metadata']},
                {'op': 'test', 'path': '/spec', 'value': before['spec']},
                {'op': 'replace', 'path': '/spec', 'value': desired['spec']}]
            assert message.headers['Content-Type'] == 'application/json-patch+json'
            updated = copy.deepcopy(before)
            updated['spec'] = copy.deepcopy(desired['spec'])
            if message.url.params:
                assert dict(message.url.params) == {'dryRun': 'All'}
                previews.append(key)
                return httpx.Response(200, json=updated)
            assert json.loads((api.state / 'template-restoration.json').read_bytes())['workloads'][key] == {
                'phase': 'intent', 'before_resource_version': before['metadata']['resourceVersion']}
            writes.append(key)
            updated['metadata'].update(resourceVersion=str(int(before['metadata']['resourceVersion']) + 1), generation=2)
            # The old observedGeneration must not prove the new template drained.
            assert updated['status']['observedGeneration'] == 1
            state.objects[key] = updated
            raise httpx.ReadTimeout('private-marker')

        api.parent.client._transport = httpx.MockTransport(respond)
        journal = api.state / 'template-restoration.json'
        intent = journal.read_bytes()
        prepared = json.loads(intent)
        prepared['workloads'][key] = {'phase': 'prepared', 'before_resource_version': None}
        journal.write_text(json.dumps(prepared))
        with pytest.raises(ValueError):
            api.restore_legacy_template(key, before, desired)
        assert api.preview_legacy_template(key, before, desired) == desired
        assert previews == [key] and not writes
        def record_intent(fresh):
            assert fresh == before and json.loads(journal.read_bytes()) == prepared
            journal.write_bytes(intent)

        # An uncertain attempt may only be observed, never reissued or rebound.
        journal.write_bytes(intent)
        with pytest.raises(ValueError):
            api.restore_legacy_template(key, before, desired, record_intent=record_intent)
        assert not writes and journal.read_bytes() == intent
        journal.write_text(json.dumps(prepared))
        broadened = copy.deepcopy(desired)
        broadened['spec']['replicas'] = 1
        with pytest.raises(ValueError):
            api.restore_legacy_template(key, before, broadened, record_intent=record_intent)
        state.machine_phase = 'active'
        with pytest.raises(ValueError):
            api.restore_legacy_template(key, before, desired, record_intent=record_intent)
        state.machine_phase = 'revoked'
        with pytest.raises(ValueError) as error:
            api.restore_legacy_template(key, before, desired, record_intent=record_intent)
        assert 'private-' not in str(error.value) and writes == [key]
        actual = api.read_workload(key)
        assert _stable(actual) == desired and actual['spec']['replicas'] == 0
        assert actual['metadata']['annotations'] == before['metadata']['annotations']
        api.verify_retained()  # Includes shared writer/entry/workload projections.
        stopped = _shutdown_record(api.request, state=api.state, anchor=api.anchor)[2][key]
        assert api.successor_drained(key, stopped) is False
        state.objects[key]['status']['observedGeneration'] = 2
        assert api.successor_drained(key, stopped) is True
        assert writes == [key] and journal.read_bytes() == intent
