"""Runtime changes retain UID/CAS evidence and never replay ambiguous writes."""
from __future__ import annotations

import copy
import importlib
import json
from uuid import uuid4

import pytest


class RuntimeAPI:
    def __init__(self, originals):
        self.rows = copy.deepcopy(originals)
        self.patches = []
        self.ready = False
        self.failure = None
        self.preview_rejected = False
        self.default_change = None

    def qualify(self):
        pass

    def read_workload(self, key):
        return copy.deepcopy(self.rows[key])

    def preview_workload(self, key, before, desired):
        assert before == self.rows[key]
        if self.preview_rejected:
            return None
        value = copy.deepcopy(desired)
        value['metadata']['uid'] = before['metadata']['uid']
        if self.default_change:
            self.default_change(value)
        return value

    def patch_workload(self, key, before, desired):
        assert before == self.rows[key]
        self.patches.append(key)
        if self.failure == 'rejected':
            return False
        if self.failure == 'before':
            raise OSError('ambiguous transport')
        value = self.preview_workload(key, before, desired)
        value['metadata']['resourceVersion'] = str(int(before['metadata']['resourceVersion']) + 1)
        self.rows[key] = value
        if self.failure == 'after':
            raise OSError('lost reply')
        return True

    def workload_ready(self, key, expected):
        return self.ready


@pytest.fixture
def transition(tmp_path):
    originals, targets = {}, {}
    for name in ('loom-service', 'loom-control-plane'):
        key = 'Deployment:loom-dev:' + name
        row = {'apiVersion': 'apps/v1', 'kind': 'Deployment', 'metadata': {
            'name': name, 'namespace': 'loom-dev', 'uid': str(uuid4()), 'resourceVersion': '1'},
            'spec': {'replicas': 1, 'selector': {'matchLabels': {'app': name}},
                'template': {'metadata': {'labels': {'app': name}}, 'spec': {
                    'containers': [{'name': name, 'image': 'example/image@sha256:' + 'a' * 64,
                        'securityContext': {'allowPrivilegeEscalation': False}}]}}}}
        originals[key] = row
        target = copy.deepcopy(row)
        target['metadata'].pop('uid')
        target['metadata'].pop('resourceVersion')
        target['spec']['replicas'] = 0
        targets[key] = target
    return originals, targets, RuntimeAPI(originals), tmp_path / 'transition'


def run(transition):
    name = 'scripts.ops.nebius_development_runtime_transition'
    if importlib.util.find_spec(name) is None:
        pytest.fail('anchored development workload transition is missing')
    originals, targets, api, state = transition
    return importlib.import_module(name).advance_runtime_workloads(
        originals=originals, targets=targets, api=api, state_dir=state,
        input_digest='sha256:' + 'a' * 64, phase='stop')


def test_transition_waits_for_drain_and_resumes_without_repatch(transition):
    originals, _, api, state = transition
    assert run(transition) is False
    assert api.patches == ['Deployment:loom-dev:loom-service']
    api.ready = True
    assert run(transition) is True
    assert run(transition) is True
    assert api.patches == list(originals)
    saved = json.loads((state / 'transition.json').read_bytes())
    assert all(row['status'] == 'applied' for row in saved['resources'].values())
    assert {key: row['metadata']['uid'] for key, row in api.rows.items()} == {
        key: row['metadata']['uid'] for key, row in originals.items()}


def test_transition_persists_known_preview_wait_before_returning_pending(transition):
    _, _, api, state = transition
    api.preview_rejected = True
    assert run(transition) is False
    assert (state / 'transition.json').is_file()
    assert not api.patches
    api.preview_rejected, api.ready = False, True
    assert run(transition) is True
    assert len(api.patches) == 2


@pytest.mark.parametrize('failure', ['before', 'after', 'rejected'])
def test_transition_distinguishes_rejected_from_ambiguous_writes(transition, failure):
    _, _, api, _ = transition
    api.failure, api.ready = failure, True
    if failure == 'before':
        for _ in range(2):
            with pytest.raises(ValueError, match='development runtime transition unqualified'):
                run(transition)
        assert len(api.patches) == 1
    elif failure == 'after':
        assert run(transition) is True
        assert run(transition) is True
        assert len(api.patches) == 2
    else:
        assert run(transition) is False
        api.failure = None
        assert run(transition) is True
        assert len(api.patches) == 3


@pytest.mark.parametrize('damage', ['uid', 'template', 'defaults'])
def test_transition_qualifies_late_entry_before_any_patch(transition, damage):
    _, _, api, _ = transition
    last = api.rows['Deployment:loom-dev:loom-control-plane']
    if damage == 'uid':
        last['metadata']['uid'] = str(uuid4())
    elif damage == 'template':
        last['spec']['template']['spec']['containers'][0]['image'] = 'foreign/image'
    else:
        def mutate(row):
            if row['metadata']['name'] == 'loom-control-plane':
                row['spec']['template']['spec']['containers'][0]['securityContext']['privileged'] = True
        api.default_change = mutate
    with pytest.raises(ValueError, match='development runtime transition unqualified'):
        run(transition)
    assert not api.patches


@pytest.mark.parametrize('damage', ['expected', 'identity', 'target'])
def test_transition_refuses_changed_recovery_evidence(transition, damage):
    _, targets, api, state = transition
    assert run(transition) is False
    before = list(api.patches)
    path = state / 'transition.json'
    saved = json.loads(path.read_bytes())
    if damage == 'target':
        targets['Deployment:loom-dev:loom-service']['spec']['replicas'] = 1
    elif damage == 'identity':
        saved['input_digest'] = 'sha256:' + 'b' * 64
    else:
        saved['resources']['Deployment:loom-dev:loom-control-plane']['expected']['spec']['replicas'] = 5
    path.write_text(json.dumps(saved))
    with pytest.raises(ValueError, match='development runtime transition unqualified'):
        run(transition)
    assert api.patches == before
