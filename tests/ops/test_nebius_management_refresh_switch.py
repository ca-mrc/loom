"""Refresh cutover preserves uncertainty and never starts two managers."""
from __future__ import annotations

import copy
import json
from dataclasses import replace
from uuid import uuid4

import pytest
from tests.ops.test_nebius_management_refresh import refresh_request as refresh_request
from tests.unit.test_nebius_management_render import (
    application_management_inputs as application_management_inputs,
)
from tests.unit.test_nebius_management_render import (
    management_inputs as management_inputs,
)
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


class API:
    def __init__(self, request):
        self.document = copy.deepcopy(request.render.active)
        self.request = request
        self.calls = []
        self.drained = False
        self.qualified = False
        self.failure = None

    def read(self):
        return copy.deepcopy(self.document)

    def desired(self, action):
        from scripts.ops.nebius_management_refresh_switch import refresh_target

        result = refresh_target(self.request, action)
        result['metadata'].update(uid=self.document['metadata']['uid'],
            resourceVersion=str(int(self.document['metadata']['resourceVersion']) + 1),
            generation=self.document['metadata']['generation'] + 1)
        return result

    def preview(self, before, operation_id):
        assert before == self.document and operation_id == str(self.request.operation_id)
        return self.desired('activate')

    def patch(self, before, action, operation_id):
        assert before == self.document and operation_id == str(self.request.operation_id)
        self.calls.append(action)
        if self.failure == 'conflict':
            return False
        if self.failure == 'before':
            raise OSError('private uncertain transport failure')
        self.document = self.desired(action)
        if self.failure == 'after':
            raise OSError('private uncertain transport reply')
        return True

    def retired(self):
        return self.drained

    def activation_ready(self):
        return self.qualified


@pytest.fixture
def refresh(refresh_request):
    from scripts.ops.nebius_management_refresh_switch import ManagementRefreshSwitchRequest

    request = ManagementRefreshSwitchRequest(refresh_request, uuid4())
    return request, API(request)


def run(refresh, state, *, activate=False):
    from scripts.ops.nebius_management_refresh_switch import switch_refresh

    return switch_refresh(request=refresh[0], api=refresh[1], state_dir=state, activate=activate)


def test_cutover_waits_for_drain_and_qualified_migration(refresh, tmp_path):
    request, api = refresh
    before = copy.deepcopy(api.document)
    assert run(refresh, tmp_path) is False
    assert api.calls == ['retire']
    assert api.document['spec']['replicas'] == 0
    assert api.document['spec']['template'] == before['spec']['template']
    assert run(refresh, tmp_path, activate=True) is False
    api.drained = True
    assert run(refresh, tmp_path) is True
    assert run(refresh, tmp_path, activate=True) is False
    api.qualified = True
    assert run(refresh, tmp_path, activate=True) is True
    assert run(refresh, tmp_path, activate=True) is True
    assert api.calls == ['retire', 'activate']
    assert api.document['spec']['replicas'] == 1
    assert api.document['metadata']['uid'] == before['metadata']['uid']
    assert api.document['spec']['template']['spec']['containers'][0]['image'] == request.render.candidate['images']['service']['image_ref']
    assert api.document['metadata']['annotations']['loom.nebius/management-upgrade-id'] == before['metadata']['annotations']['loom.nebius/management-upgrade-id']
    assert (tmp_path / 'cutover.json').stat().st_mode & 0o777 == 0o600
    assert json.loads((tmp_path / 'cutover.json').read_text())['original'] == before


@pytest.mark.parametrize('action', ['retire', 'activate'])
@pytest.mark.parametrize('failure', ['before', 'after'])
def test_unknown_write_is_observed_but_never_resent(refresh, tmp_path, action, failure):
    _, api = refresh
    if action == 'activate':
        run(refresh, tmp_path)
        api.drained = api.qualified = True
    api.failure = failure
    for _ in range(2):
        if failure == 'before':
            with pytest.raises(ValueError, match='unresolved'):
                run(refresh, tmp_path, activate=action == 'activate')
        else:
            run(refresh, tmp_path, activate=action == 'activate')
    assert api.calls.count(action) == 1
    assert json.loads((tmp_path / 'cutover.json').read_text())['phase'] in {
        'retire_intent', 'stopped', 'activate_intent', 'active'}


@pytest.mark.parametrize('activate', [False, True])
def test_definite_conflict_can_retry_after_requalification(refresh, tmp_path, activate):
    _, api = refresh
    if activate:
        run(refresh, tmp_path)
        api.drained = api.qualified = True
    api.failure = 'conflict'
    assert run(refresh, tmp_path, activate=activate) is False
    api.failure = None
    run(refresh, tmp_path, activate=activate)
    assert api.calls.count('activate' if activate else 'retire') == 2


@pytest.mark.parametrize('damage', ['operation', 'uid', 'template', 'lost', 'phase', 'active'])
def test_stale_or_damaged_history_never_authorizes_a_write(refresh, tmp_path, damage):
    request, api = refresh
    run(refresh, tmp_path)
    calls = list(api.calls)
    if damage == 'operation':
        request = replace(request, operation_id=uuid4())
    elif damage == 'uid':
        api.document['metadata']['uid'] = str(uuid4())
    elif damage == 'template':
        api.document['spec']['template']['spec']['containers'][0]['image'] += '-drift'
    elif damage == 'lost':
        (tmp_path / 'cutover.json').unlink()
    else:
        path = tmp_path / 'cutover.json'
        record = json.loads(path.read_text())
        record[damage] = 'active' if damage == 'phase' else {}
        path.write_text(json.dumps(record))
    api.drained = api.qualified = True
    with pytest.raises(ValueError):
        run((request, api), tmp_path, activate=True)
    assert api.calls == calls


def test_activation_cannot_start_without_retirement_or_reverse_after_activation(refresh, tmp_path):
    with pytest.raises(ValueError):
        run(refresh, tmp_path, activate=True)
    run(refresh, tmp_path)
    refresh[1].drained = refresh[1].qualified = True
    run(refresh, tmp_path, activate=True)
    with pytest.raises(ValueError):
        run(refresh, tmp_path)
    assert refresh[1].calls == ['retire', 'activate']
