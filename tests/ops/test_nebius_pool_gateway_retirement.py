"""Retire only the anchored gateway Roles; no binding changes or legacy restore."""
from __future__ import annotations

import copy
import json

import pytest
from tests.ops.test_nebius_pool_machine_retirement import MachineAPI, retire, stopped
from tests.ops.test_nebius_pool_machine_retirement import closed_startup as closed_startup
from tests.ops.test_nebius_pool_machine_retirement import collector_inputs as collector_inputs
from tests.ops.test_nebius_pool_machine_retirement import (
    cutover_binding_inventory as cutover_binding_inventory,
)
from tests.ops.test_nebius_pool_machine_retirement import cutover_inputs as cutover_inputs
from tests.ops.test_nebius_pool_machine_retirement import fencing_inputs as fencing_inputs
from tests.ops.test_nebius_pool_machine_retirement import management_inputs as management_inputs
from tests.ops.test_nebius_pool_machine_retirement import platform_inputs as platform_inputs
from tests.ops.test_nebius_pool_machine_retirement import retirement_inputs as retirement_inputs
from tests.ops.test_nebius_pool_machine_retirement import runtime_inputs as runtime_inputs

READER_RULES = [
    {'apiGroups': ['batch'], 'resources': ['jobs'], 'verbs': ['get']},
    {'apiGroups': [''], 'resources': ['configmaps'], 'verbs': ['get']},
    {'apiGroups': [''], 'resources': ['pods'], 'verbs': ['get', 'list']},
]


class GatewayAPI(MachineAPI):
    def __init__(self, fixture, machine):
        super().__init__(fixture)
        self.mode, self.guards, self.machine_phase = machine.mode, machine.guards.copy(), machine.machine_phase
        child = json.loads((self.state / 'authority/stage.json').read_bytes())
        self.authority = {}
        for key, item in child['resources'].items():
            row = copy.deepcopy(item['observed'])
            row['metadata'].update(uid=item['uid'], resourceVersion='1')
            self.authority[key] = row
        self.role_calls = []
        self.role_failure = None
        self.effective_readonly = True
        self.permission_checks = 0

    def read_gateway_authority(self, key):
        return copy.deepcopy(self.authority[key])

    def preview_gateway_role(self, key, before, desired):
        assert before == self.authority[key]
        return copy.deepcopy(desired)

    def restrict_gateway_role(self, key, before, desired):
        item = json.loads((self.state / 'gateway-retirement.json').read_bytes())['roles'][key]
        assert item == {'phase': 'intent', 'before_resource_version': before['metadata']['resourceVersion']}
        assert self.machine_phase == 'revoked' and self.cleanup_drained and self.processes_drained
        self.role_calls.append(key)
        if self.role_failure == 'before':
            raise OSError('private-marker')
        if self.role_failure == 'conflict':
            return False
        current = copy.deepcopy(before)
        current['rules'] = copy.deepcopy(desired['rules'])
        current['metadata']['resourceVersion'] = str(int(current['metadata']['resourceVersion']) + 1)
        self.authority[key] = current
        if self.role_failure == 'after':
            raise OSError('private-marker')
        return True

    def qualify_gateway_retired(self):
        self.qualify_gateway_readonly()

    def qualify_gateway_readonly(self):
        self.permission_checks += 1
        if not self.effective_readonly:
            raise ValueError('private-effective-authority')
        assert all(row['rules'] == READER_RULES for row in self.authority.values() if row['kind'] == 'Role')


def machine_retired(fixture):
    machine = stopped(fixture)
    assert retire(fixture, machine)['status'] == 'pool_machines_retired'
    return GatewayAPI(fixture, machine)


def gateway_retire(fixture, api):
    from scripts.ops.nebius_pool_gateway_retirement import retire_gateway_roles

    request, _, _, _, _, root = fixture
    return retire_gateway_roles(request=request, api=api, state_dir=root / 'cutover', anchor_dir=root / 'cutover-anchor')


def test_gateway_retirement_changes_only_fixed_roles_and_preserves_bindings(closed_startup):
    from scripts.ops.nebius_pool_gateway_retirement import gateway_retirement_options

    api = machine_retired(closed_startup)
    original = copy.deepcopy(api.authority)
    result = gateway_retire(closed_startup, api)
    assert result['status'] == 'pool_gateway_roles_retired' and result['legacy_restore_allowed'] is False
    assert set(api.role_calls) == {key for key, row in original.items() if row['kind'] == 'Role'}
    for key, before in original.items():
        expected = copy.deepcopy(before)
        if before['kind'] == 'Role':
            expected['rules'] = READER_RULES
            expected['metadata']['resourceVersion'] = '2'
        assert api.authority[key] == expected
    assert gateway_retire(closed_startup, api) == result
    assert len(api.role_calls) == 6 and api.permission_checks == 2
    options = gateway_retirement_options(api.request, state=api.state, anchor=api.root / 'cutover-anchor')
    assert all(len(values) == 1 for values in options.values())
    assert all(values[0]['rules'] == READER_RULES for key, values in options.items() if key.startswith('Role:'))


@pytest.mark.parametrize('failure', ['before', 'after', 'conflict'])
def test_gateway_unknown_role_updates_only_observe_and_definite_rejection_can_resume(closed_startup, failure):
    api = machine_retired(closed_startup)
    api.role_failure = failure
    result = gateway_retire(closed_startup, api)
    api.role_failure = None
    if failure == 'before':
        assert result['status'] == 'pending_gateway_role_outcome'
        assert gateway_retire(closed_startup, api) == result and len(api.role_calls) == 1
        row = api.authority[api.role_calls[0]]
        row['rules'], row['metadata']['resourceVersion'] = copy.deepcopy(READER_RULES), '2'
    elif failure == 'conflict':
        assert result['status'] == 'pending_gateway_role_update'
    assert gateway_retire(closed_startup, api)['status'] == 'pool_gateway_roles_retired'
    assert len(api.role_calls) == 6 + int(failure == 'conflict')


@pytest.mark.parametrize('pending', ['cleanup', 'processes'])
def test_gateway_retirement_retains_fresh_drain_barriers(closed_startup, pending):
    api = machine_retired(closed_startup)
    if pending == 'cleanup':
        api.cleanup_drained = False
    else:
        api.processes_drained = False
    assert gateway_retire(closed_startup, api)['status'] == ('pending_pool_cleanup' if pending == 'cleanup' else 'pending_successor_drain')
    assert not api.role_calls


@pytest.mark.parametrize('damage', ['machine', 'machine_journal', 'uid', 'rules', 'binding', 'anchor', 'effective'])
def test_gateway_retirement_refuses_foreign_or_unqualified_authority(closed_startup, damage):
    api = machine_retired(closed_startup)
    key = next(key for key in api.authority if key.startswith('Role:'))
    if damage == 'machine':
        api.machine_phase = 'active'
    elif damage == 'machine_journal':
        (api.state / 'machine-retirement.json').write_bytes(b'{}')
    elif damage == 'uid':
        api.authority[key]['metadata']['uid'] = 'aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaaaa'
    elif damage == 'rules':
        api.authority[key]['rules'][0]['verbs'].append('patch')
    elif damage == 'binding':
        key = next(key for key in api.authority if key.startswith('RoleBinding:'))
        api.authority[key]['subjects'][0]['name'] = 'foreign'
    elif damage == 'effective':
        api.effective_readonly = False
    else:
        assert gateway_retire(closed_startup, api)['status'] == 'pool_gateway_roles_retired'
        next((api.root / 'cutover-anchor').glob('*-gateway-retirement.json')).unlink()
        api.role_calls.clear()
    with pytest.raises(ValueError) as error:
        gateway_retire(closed_startup, api)
    assert 'private-' not in str(error.value)
    assert len(api.role_calls) == (6 if damage == 'effective' else 0)
