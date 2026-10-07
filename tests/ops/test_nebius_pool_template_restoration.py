"""Restore retained legacy specs while both writer sets remain disabled."""
from __future__ import annotations

import copy
import json

import pytest
from scripts.ops.nebius_ingress_stage import _key
from scripts.ops.nebius_management_switch import _stable
from tests.ops.test_nebius_pool_gateway_retirement import (
    GatewayAPI,
    gateway_retire,
    machine_retired,
)
from tests.ops.test_nebius_pool_gateway_retirement import closed_startup as closed_startup
from tests.ops.test_nebius_pool_gateway_retirement import collector_inputs as collector_inputs
from tests.ops.test_nebius_pool_gateway_retirement import (
    cutover_binding_inventory as cutover_binding_inventory,
)
from tests.ops.test_nebius_pool_gateway_retirement import cutover_inputs as cutover_inputs
from tests.ops.test_nebius_pool_gateway_retirement import fencing_inputs as fencing_inputs
from tests.ops.test_nebius_pool_gateway_retirement import management_inputs as management_inputs
from tests.ops.test_nebius_pool_gateway_retirement import platform_inputs as platform_inputs
from tests.ops.test_nebius_pool_gateway_retirement import retirement_inputs as retirement_inputs
from tests.ops.test_nebius_pool_gateway_retirement import runtime_inputs as runtime_inputs


class TemplateAPI(GatewayAPI):
    def __init__(self, fixture, prior):
        super().__init__(fixture, prior)
        self.authority = copy.deepcopy(prior.authority)
        self.template_calls, self.template_failure = [], None
        self.template_fail_key = None
        self.drain_targets = {key: _stable(row) for key, row in self.startup.documents.items()}

    def successor_drained(self, key, desired):
        # This double supplies only the remote process observation. The real
        # stage verifies projections; the connected test covers this reader.
        assert _stable(desired) == self.drain_targets[key]
        return self.processes_drained

    def preview_legacy_template(self, key, before, desired):
        assert before == self.startup.documents[key]
        return copy.deepcopy(desired)

    def restore_legacy_template(self, key, before, desired, *, record_intent):
        before = self.read_workload(key)
        record_intent(before)
        assert json.loads((self.state / 'template-restoration.json').read_bytes())['workloads'][key] == {
            'phase': 'intent', 'before_resource_version': before['metadata']['resourceVersion']}
        assert self.machine_phase == 'revoked' and self.mode == 'fenced' and set(self.guards.values()) == {'fenced'}
        self.template_calls.append(key)
        failure = self.template_failure if self.template_fail_key in (None, key) else None
        if failure == 'before':
            raise OSError('private-marker')
        if failure == 'conflict':
            return False
        current = copy.deepcopy(before)
        current['spec'] = copy.deepcopy(desired['spec'])
        current['metadata']['resourceVersion'] = str(int(current['metadata']['resourceVersion']) + 1)
        self.startup.documents[key] = current
        if failure == 'after':
            raise OSError('private-marker')
        return True


def gateway_retired(fixture):
    prior = machine_retired(fixture)
    assert gateway_retire(fixture, prior)['status'] == 'pool_gateway_roles_retired'
    return TemplateAPI(fixture, prior)


def restore(fixture, api):
    from scripts.ops.nebius_pool_template_restoration import restore_pool_templates

    return restore_pool_templates(request=api.request, api=api, state_dir=api.state, anchor_dir=api.root / 'cutover-anchor')


# These cases include the predecessor retirement plus full restoration/replay.
@pytest.mark.timeout(180)
def test_restoration_recovers_exact_old_specs_but_keeps_every_writer_stopped(closed_startup):
    from scripts.ops.nebius_pool_cutover import retained_cutover_workloads
    from scripts.ops.nebius_pool_retirement import retirement_documents

    api = gateway_retired(closed_startup)
    request, _, _, startup, dormant, root = closed_startup
    before, authority = copy.deepcopy(startup.documents), copy.deepcopy(api.authority)
    result = restore(closed_startup, api)
    assert result['status'] == 'pool_legacy_templates_restored_closed' and result['legacy_restore_allowed'] is False
    originals = {**retirement_documents(request.fencing.retirement),
        **{_key(row): row for row in (request.manager, *request.services)}}
    changed = set()
    for key, actual in startup.documents.items():
        expected = copy.deepcopy(before[key])
        if key in originals:
            expected['spec'] = copy.deepcopy(originals[key]['spec'])
            expected['spec']['suspend' if actual['kind'] == 'CronJob' else 'replicas'] = True if actual['kind'] == 'CronJob' else 0
        if _stable(expected) != _stable(before[key]):
            changed.add(key)
            expected['metadata']['resourceVersion'] = str(int(before[key]['metadata']['resourceVersion']) + 1)
        assert _stable(actual) == _stable(expected)
        assert actual['metadata'] == expected['metadata']
        assert actual['spec']['suspend' if actual['kind'] == 'CronJob' else 'replicas'] == (True if actual['kind'] == 'CronJob' else 0)
    assert changed == set(api.template_calls) and changed
    assert startup.documents[_key(dormant.actuator)] == before[_key(dormant.actuator)]
    assert startup.documents[_key(dormant.collector)] == before[_key(dormant.collector)]
    assert api.authority == authority and not api.role_calls
    assert restore(closed_startup, api) == result and len(api.template_calls) == len(changed)
    retained = retained_cutover_workloads(request, state_dir=root / 'cutover', anchor_dir=root / 'cutover-anchor', observed=startup.documents)
    assert all(_stable(row) == _stable(startup.documents[key]) for key, row in retained.items())


@pytest.mark.parametrize('failure', ['before', 'after', 'conflict'])
@pytest.mark.timeout(180)
def test_template_lost_replies_observe_and_only_definite_rejection_can_retry(closed_startup, failure):
    from scripts.ops.nebius_pool_startup import startup_workload_options

    api = gateway_retired(closed_startup)
    api.template_failure = failure
    result = restore(closed_startup, api)
    api.template_failure = None
    if failure == 'before':
        assert result['status'] == 'pending_template_restoration_outcome'
        assert restore(closed_startup, api) == result and len(api.template_calls) == 1
        key = api.template_calls[0]
        choices = startup_workload_options(api.request, state_dir=api.state, anchor_dir=api.root / 'cutover-anchor')
        assert len(choices[key]) == 2
        current = api.startup.documents[key]
        current['spec'] = copy.deepcopy(choices[key][1]['spec'])
        current['metadata']['resourceVersion'] = str(int(current['metadata']['resourceVersion']) + 1)
    elif failure == 'conflict':
        assert result['status'] == 'pending_template_restoration_update'
    assert restore(closed_startup, api)['status'] == 'pool_legacy_templates_restored_closed'
    assert len(api.template_calls) == len(set(api.template_calls)) + int(failure == 'conflict')


@pytest.mark.parametrize('pending', ['cleanup', 'processes'])
def test_template_restoration_requires_fresh_drain_before_any_change(closed_startup, pending):
    api = gateway_retired(closed_startup)
    setattr(api, 'cleanup_drained' if pending == 'cleanup' else 'processes_drained', False)
    assert restore(closed_startup, api)['status'] == ('pending_pool_cleanup' if pending == 'cleanup' else 'pending_successor_drain')
    assert not api.template_calls


@pytest.mark.parametrize('damage', ['gateway_journal', 'effective', 'machine', 'uid', 'spec', 'stale_version',
    pytest.param('anchor', marks=pytest.mark.timeout(180))])
def test_template_restoration_rejects_foreign_or_unretired_authority(closed_startup, damage):
    api = gateway_retired(closed_startup)
    key = _key(api.request.manager)
    if damage == 'gateway_journal':
        (api.state / 'gateway-retirement.json').write_bytes(b'{}')
    elif damage == 'effective':
        api.effective_readonly = False
    elif damage == 'machine':
        api.machine_phase = 'active'
    elif damage == 'uid':
        api.startup.documents[key]['metadata']['uid'] = 'aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaaaa'
    elif damage == 'spec':
        api.startup.documents[key]['spec']['replicas'] = 1
    elif damage == 'stale_version':
        from scripts.ops.nebius_pool_template_restoration import _template_record

        api.template_failure = 'before'
        assert restore(closed_startup, api)['status'] == 'pending_template_restoration_outcome'
        key, = api.template_calls
        targets = _template_record(api.request, state=api.state, anchor=api.root / 'cutover-anchor')[2]
        api.startup.documents[key]['spec'] = copy.deepcopy(targets[key]['spec'])
        api.template_calls.clear()  # No corresponding new resourceVersion: not a settled CAS.
    else:
        assert restore(closed_startup, api)['status'] == 'pool_legacy_templates_restored_closed'
        next((api.root / 'cutover-anchor').glob('*-template-restoration.json')).unlink()
        api.template_calls.clear()
    with pytest.raises(ValueError) as error:
        restore(closed_startup, api)
    assert 'private-' not in str(error.value) and not api.template_calls
