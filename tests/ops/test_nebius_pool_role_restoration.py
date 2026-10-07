"""Only completed closed recovery may restore exact retained participant Roles."""
from __future__ import annotations

import copy
import json

import pytest
from scripts.ops.nebius_ingress_stage import _key
from scripts.ops.nebius_management_switch import _stable
from tests.ops.test_nebius_pool_template_restoration import TemplateAPI, gateway_retired, restore
from tests.ops.test_nebius_pool_template_restoration import closed_startup as closed_startup
from tests.ops.test_nebius_pool_template_restoration import collector_inputs as collector_inputs
from tests.ops.test_nebius_pool_template_restoration import (
    cutover_binding_inventory as cutover_binding_inventory,
)
from tests.ops.test_nebius_pool_template_restoration import cutover_inputs as cutover_inputs
from tests.ops.test_nebius_pool_template_restoration import fencing_inputs as fencing_inputs
from tests.ops.test_nebius_pool_template_restoration import management_inputs as management_inputs
from tests.ops.test_nebius_pool_template_restoration import platform_inputs as platform_inputs
from tests.ops.test_nebius_pool_template_restoration import retirement_inputs as retirement_inputs
from tests.ops.test_nebius_pool_template_restoration import runtime_inputs as runtime_inputs


class RoleAPI(TemplateAPI):
    def __init__(self, fixture, prior):
        super().__init__(fixture, prior)
        self.drain_targets = copy.deepcopy(prior.drain_targets)
        self.legacy_roles = copy.deepcopy(fixture[2].fencing.roles)
        self.legacy_calls, self.legacy_failure = [], None
        self.effective_legacy = True

    def read_legacy_role(self, key):
        return copy.deepcopy(self.legacy_roles[key])

    def preview_legacy_role(self, key, before, desired):
        assert before == self.legacy_roles[key]
        return copy.deepcopy(desired)

    def restore_legacy_role(self, key, before, desired, *, record_intent):
        from scripts.ops.nebius_pool_role_restoration import qualify_role_restoration
        from scripts.ops.nebius_pool_template_restoration import RecoveryDrainPending

        pending = qualify_role_restoration(self.request, self, state=self.state, anchor=self.root / 'cutover-anchor')
        if pending is not None:
            return RecoveryDrainPending(pending)
        before = self.read_legacy_role(key)
        record_intent(before)
        assert json.loads((self.state / 'role-restoration.json').read_bytes())['roles'][key] == {
            'phase': 'intent', 'before_resource_version': before['metadata']['resourceVersion']}
        assert self.mode == 'fenced' and set(self.guards.values()) == {'fenced'} and self.machine_phase == 'revoked'
        self.legacy_calls.append(key)
        if self.legacy_failure == 'before':
            raise OSError('private-marker')
        if self.legacy_failure == 'conflict':
            return False
        current = copy.deepcopy(before)
        current['rules'] = copy.deepcopy(desired['rules'])
        if 'annotations' in desired['metadata']:
            current['metadata']['annotations'] = copy.deepcopy(desired['metadata']['annotations'])
        else:
            current['metadata'].pop('annotations', None)
        current['metadata']['resourceVersion'] = str(int(current['metadata']['resourceVersion']) + 1)
        self.legacy_roles[key] = current
        if self.legacy_failure == 'after':
            raise OSError('private-marker')
        return True

    def qualify_legacy_roles(self):
        if not self.effective_legacy:
            raise ValueError('private-effective-marker')


def templates_restored(fixture):
    prior = gateway_retired(fixture)
    assert restore(fixture, prior)['status'] == 'pool_legacy_templates_restored_closed'
    return RoleAPI(fixture, prior)


def restore_roles(api):
    from scripts.ops.nebius_pool_role_restoration import restore_pool_roles

    return restore_pool_roles(request=api.request, api=api, state_dir=api.state, anchor_dir=api.root / 'cutover-anchor')


@pytest.mark.timeout(240)
def test_roles_return_to_exact_original_rights_without_starting_any_workload(closed_startup):
    api = templates_restored(closed_startup)
    workloads, gateway = copy.deepcopy(api.startup.documents), copy.deepcopy(api.authority)
    before = copy.deepcopy(api.legacy_roles)
    result = restore_roles(api)
    assert result['status'] == 'pool_legacy_roles_restored_closed' and result['legacy_restore_allowed'] is False
    assert set(api.legacy_calls) == set(before) and len(api.legacy_calls) == 6
    for original in api.request.fencing.originals:
        key = _key(original)
        actual = api.legacy_roles[key]
        assert _stable(actual) == _stable(original)
        assert actual['metadata']['uid'] == original['metadata']['uid']
        assert actual['metadata']['resourceVersion'] != before[key]['metadata']['resourceVersion']
    assert restore_roles(api) == result and len(api.legacy_calls) == 6
    assert api.startup.documents == workloads and api.authority == gateway
    assert api.mode == 'fenced' and set(api.guards.values()) == {'fenced'}


@pytest.mark.parametrize('failure', ['before', 'after', 'conflict'])
@pytest.mark.timeout(240)
def test_lost_role_restoration_only_observes_and_definite_rejection_can_resume(closed_startup, failure):
    api = templates_restored(closed_startup)
    api.legacy_failure = failure
    result = restore_roles(api)
    api.legacy_failure = None
    if failure == 'before':
        assert result['status'] == 'pending_role_restoration_outcome'
        assert restore_roles(api) == result and len(api.legacy_calls) == 1
        key = api.legacy_calls[0]
        original = next(row for row in api.request.fencing.originals if _key(row) == key)
        actual = copy.deepcopy(original)
        actual['metadata']['resourceVersion'] = str(int(api.legacy_roles[key]['metadata']['resourceVersion']) + 1)
        api.legacy_roles[key] = actual
    elif failure == 'conflict':
        assert result['status'] == 'pending_role_restoration_update'
    assert restore_roles(api)['status'] == 'pool_legacy_roles_restored_closed'
    assert len(api.legacy_calls) == 6 + int(failure == 'conflict')


@pytest.mark.parametrize('damage', ['templates', 'machine', 'gateway', 'uid', 'rules', 'effective', 'cleanup', 'processes'])
@pytest.mark.timeout(180)
def test_restoring_roles_requires_exact_retired_authority_and_fresh_drain(closed_startup, damage):
    api = templates_restored(closed_startup)
    key = next(iter(api.legacy_roles))
    if damage == 'templates':
        (api.state / 'template-restoration.json').write_bytes(b'{}')
    elif damage == 'machine':
        api.machine_phase = 'active'
    elif damage == 'gateway':
        api.effective_readonly = False
    elif damage == 'uid':
        api.legacy_roles[key]['metadata']['uid'] = 'aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaaaa'
    elif damage == 'rules':
        api.legacy_roles[key]['rules'][0]['verbs'].append('patch')
    elif damage == 'effective':
        api.effective_legacy = False
    else:
        setattr(api, 'cleanup_drained' if damage == 'cleanup' else 'processes_drained', False)
    if damage in {'cleanup', 'processes'}:
        assert restore_roles(api)['status'] == ('pending_pool_cleanup' if damage == 'cleanup' else 'pending_successor_drain')
    else:
        with pytest.raises(ValueError) as error:
            restore_roles(api)
        assert 'private-' not in str(error.value)
    assert len(api.legacy_calls) == (6 if damage == 'effective' else 0)
