"""Fixed upgrade retires old processes and never repeats an uncertain update."""
from __future__ import annotations

import copy
import json
from dataclasses import replace
from uuid import uuid4

import pytest
from tests.ops.test_nebius_application_setup import setup_request as setup_request
from tests.unit.test_nebius_management_render import (
    application_management_inputs as application_management_inputs,
)
from tests.unit.test_nebius_management_render import management_inputs as management_inputs
from tests.unit.test_nebius_management_render import render
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


class SwitchAPI:
    def __init__(self, request, target):
        self.document = copy.deepcopy(request.original)
        self.target = target
        self.calls = []
        self.failure = None
        self.processes = True

    def read(self):
        return copy.deepcopy(self.document)

    def desired(self, before, action, operation_id):
        from scripts.ops.nebius_management_switch import MARKER

        result = copy.deepcopy(before)
        result['metadata'].setdefault('annotations', {})[MARKER] = operation_id
        result['spec']['replicas'] = 0 if action == 'retire' else 1
        if action == 'activate':
            result['spec']['template'] = copy.deepcopy(self.target['spec']['template'])
        result['metadata']['generation'] += 1
        result['metadata']['resourceVersion'] = str(int(before['metadata']['resourceVersion']) + 1)
        return result

    def preview(self, before, operation_id):
        return self.desired(before, 'activate', operation_id)

    def patch(self, before, action, operation_id):
        self.calls.append(action)
        assert before['metadata']['resourceVersion'] == self.document['metadata']['resourceVersion']
        if self.failure == 'conflict':
            return False
        if self.failure == 'before':
            raise OSError('uncertain fixture write')
        self.document = self.desired(before, action, operation_id)
        if self.failure == 'after':
            raise OSError('uncertain fixture reply')
        return True

    def retired(self):
        return not self.processes


@pytest.fixture
def switch_inputs(setup_request, management_inputs):
    from scripts.ops.nebius_management_switch import ManagementSwitchRequest

    from loom_service.environment_management.deployment import render_management

    setup, _ = setup_request
    original = copy.deepcopy(next(doc for doc in render(management_inputs).files['40-services.yaml']
        if doc['kind'] == 'Deployment'))
    original['metadata'].update(uid=str(uuid4()), resourceVersion='11', generation=1)
    original['status'] = {'observedGeneration': 1, 'replicas': 1, 'readyReplicas': 1}
    target = next(doc for doc in render_management(setup.deployment, candidate=setup.candidate,
        profile=setup.profile, repo_root=setup.repo_root).files['40-services.yaml'] if doc['kind'] == 'Deployment')
    request = ManagementSwitchRequest(setup=setup, original=original)
    return request, SwitchAPI(request, target)


def retire(inputs, state):
    from scripts.ops.nebius_management_switch import retire_management

    return retire_management(request=inputs[0], api=inputs[1], state_dir=state)


def activate(inputs, state):
    from scripts.ops.nebius_management_switch import activate_management

    return activate_management(request=inputs[0], api=inputs[1], state_dir=state)


def test_switch_preserves_original_and_waits_for_old_processes(switch_inputs, tmp_path):
    state = tmp_path / 'switch'
    request, api = switch_inputs
    original = copy.deepcopy(request.original)
    assert retire(switch_inputs, state) is False
    assert activate(switch_inputs, state) is False
    assert api.calls == ['retire']
    assert api.document['spec']['template'] == original['spec']['template']
    api.processes = False
    assert retire(switch_inputs, state) is True
    assert activate(switch_inputs, state) is True
    current = copy.deepcopy(api.document)
    assert activate(switch_inputs, state) is True
    assert api.document == current and api.calls == ['retire', 'activate']
    assert api.document['metadata']['uid'] == original['metadata']['uid']
    assert api.document['spec']['template']['spec']['serviceAccountName'] == 'loom-application-provisioner'
    assert api.document['spec']['selector'] == original['spec']['selector']
    assert json.loads((state / 'switch.json').read_text())['original'] == original
    assert (state / 'switch.json').stat().st_mode & 0o777 == 0o600


@pytest.mark.parametrize('action', ['retire', 'activate'])
@pytest.mark.parametrize('failure', ['before', 'after'])
def test_uncertain_patch_reconciles_without_resending(switch_inputs, tmp_path, action, failure):
    from scripts.ops.nebius_management_stage import ManagementStageError

    state = tmp_path / 'switch'
    _, api = switch_inputs
    if action == 'activate':
        retire(switch_inputs, state)
        api.processes = False
    api.failure = failure
    run = retire if action == 'retire' else activate
    if failure == 'before':
        for _ in range(2):
            with pytest.raises(ManagementStageError, match='unresolved'):
                run(switch_inputs, state)
    else:
        run(switch_inputs, state)
        run(switch_inputs, state)
    assert api.calls.count(action) == 1


@pytest.mark.parametrize('damage', ['uid', 'template', 'shared-uid'])
def test_drift_or_changed_input_cannot_be_adopted(switch_inputs, tmp_path, damage):
    from scripts.ops.nebius_management_stage import ManagementStageError

    request, api = switch_inputs
    state = tmp_path / 'switch'
    retire(switch_inputs, state)
    if damage == 'uid':
        api.document['metadata']['uid'] = str(uuid4())
    elif damage == 'template':
        api.document['spec']['template']['spec']['containers'][0]['image'] = 'foreign/image:latest'
    else:
        switch_inputs = replace(request, setup=replace(request.setup, shared_namespace_uid=str(uuid4()))), api
    with pytest.raises(ManagementStageError):
        retire(switch_inputs, state)
    assert api.calls == ['retire']


def test_definite_conflict_can_be_reobserved_but_not_retried_in_call(switch_inputs, tmp_path):
    state = tmp_path / 'switch'
    _, api = switch_inputs
    api.failure = 'conflict'
    assert retire(switch_inputs, state) is False
    assert api.calls == ['retire']
    api.failure = None
    assert retire(switch_inputs, state) is False
    assert api.calls == ['retire', 'retire']


def test_activation_needs_retained_retirement_and_cannot_hide_defaulted_privilege(switch_inputs, tmp_path):
    from scripts.ops.nebius_management_stage import ManagementStageError

    state = tmp_path / 'switch'
    _, api = switch_inputs
    with pytest.raises(ManagementStageError):
        activate(switch_inputs, state)
    assert not api.calls
    retire(switch_inputs, state)
    api.processes = False
    original = api.preview

    def unsafe(before, operation_id):
        result = original(before, operation_id)
        result['spec']['template']['spec']['hostNetwork'] = True
        return result

    api.preview = unsafe
    with pytest.raises(ManagementStageError, match='defaulting'):
        activate(switch_inputs, state)
    assert api.calls == ['retire']
