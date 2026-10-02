"""Closed successors never replay an uncertain start or restart dormant writers."""
from __future__ import annotations

import copy
import hashlib
import json
from dataclasses import replace
from uuid import uuid4

import pytest
from scripts.ops.nebius_ingress_stage import _key
from tests.ops.test_nebius_pool_cutover import CutoverAPI, run
from tests.ops.test_nebius_pool_cutover import collector_inputs as collector_inputs
from tests.ops.test_nebius_pool_cutover import (
    cutover_binding_inventory as cutover_binding_inventory,
)
from tests.ops.test_nebius_pool_cutover import cutover_inputs as cutover_inputs
from tests.ops.test_nebius_pool_cutover import fencing_inputs as fencing_inputs
from tests.ops.test_nebius_pool_cutover import retirement_inputs as retirement_inputs
from tests.ops.test_nebius_pool_dormant import dormant_consumer
from tests.ops.test_nebius_pool_runtime import runtime_inputs as runtime_inputs
from tests.unit.test_nebius_management_render import management_inputs as management_inputs
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


class StartupAPI:
    """Only Kubernetes and live closure qualification are doubled."""

    def __init__(self, request, closed, state):
        self.request, self.state = request, state
        self.documents = {**copy.deepcopy(closed.documents),
            **{key: copy.deepcopy(row) for key, row in closed.resources.resources.items() if row['kind'] == 'Deployment'}}
        self.mode = 'closed'
        self.epoch = request.fencing.retirement.migration.registration.spec.admission_epoch
        self.guards_held = True
        self.requests = []
        self.failure = None
        self.fail_key = None
        self.closed_checks = 0

    def qualify_closed(self):
        self.closed_checks += 1
        if (self.mode != 'closed' or not self.guards_held
                or self.epoch != self.request.fencing.retirement.migration.registration.spec.admission_epoch):
            raise ValueError('private-marker')

    def read_workload(self, key):
        return copy.deepcopy(self.documents[key])

    def preview_workload(self, key, before, desired):
        assert before == self.documents[key]
        if self.failure == 'preview' and key == self.fail_key:
            return None
        return copy.deepcopy(desired)

    def start_workload(self, key, before, desired):
        assert before == self.documents[key]
        intent = json.loads((self.state / 'startup.json').read_bytes())['workloads'][key]
        assert intent['phase'] == 'intent' and intent['before_resource_version'] == before['metadata']['resourceVersion']
        self.requests.append(key)
        if self.failure == 'conflict' and key == self.fail_key:
            return False
        if self.failure == 'before' and key == self.fail_key:
            raise OSError('private-marker')
        current = copy.deepcopy(desired)
        current['metadata'].update(uid=before['metadata']['uid'], resourceVersion=str(int(before['metadata']['resourceVersion']) + 1))
        self.documents[key] = current
        if self.failure == 'after' and key == self.fail_key:
            raise OSError('private-marker')
        return True


@pytest.fixture
def closed_startup(cutover_inputs, tmp_path):
    request, tokens = cutover_inputs
    dormant = dormant_consumer(request.fencing.retirement)
    request = replace(request, fencing=replace(request.fencing,
        retirement=replace(request.fencing.retirement, dormant_consumers=(dormant,))))
    closed = CutoverAPI(request)
    assert run(request, tokens, closed, tmp_path)['status'] == 'pool_runtime_staged_closed'
    api = StartupAPI(request, closed, tmp_path / 'cutover')
    return request, tokens, closed, api, dormant, tmp_path


def start(fixture):
    from scripts.ops.nebius_pool_startup import stage_pool_startup

    request, _, _, api, _, root = fixture
    return stage_pool_startup(request=request, api=api, state_dir=root / 'cutover', anchor_dir=root / 'cutover-anchor')


def test_startup_changes_only_fixed_replica_fields_and_preserves_closed_roots(closed_startup):
    request, _, _, api, dormant, root = closed_startup
    original = copy.deepcopy(api.documents)
    result = start(closed_startup)
    assert result == {'status': 'pool_startup_staged_closed',
        'operation_id': str(request.fencing.retirement.migration.registration.spec.operation_id),
        'admission_open': False, 'runtime_verified': False}
    assert len(api.requests) == 12 and len(set(api.requests)) == 12
    assert api.closed_checks >= len(api.requests) + 1
    assert _key(dormant.actuator) not in api.requests and _key(dormant.collector) not in api.requests
    assert api.documents[_key(dormant.actuator)] == original[_key(dormant.actuator)]
    assert api.documents[_key(dormant.collector)] == original[_key(dormant.collector)]
    for key in api.requests:
        before, current = original[key], api.documents[key]
        field = 'suspend' if before['kind'] == 'CronJob' else 'replicas'
        expected = copy.deepcopy(before['spec'])
        expected[field] = False if field == 'suspend' else 1
        assert current['spec'] == expected and current['metadata']['uid'] == before['metadata']['uid']
    assert sum(row['kind'] == 'CronJob' for key, row in api.documents.items() if key in api.requests) == 1
    assert start(closed_startup) == result and len(api.requests) == 12
    journal = json.loads((root / 'cutover/startup.json').read_bytes())
    assert all(item['phase'] == 'started' for item in journal['workloads'].values())


@pytest.mark.parametrize('damage', ['mode', 'epoch', 'guards', 'uid', 'template', 'dormant', 'partial', 'unanchored'])
def test_startup_refuses_incomplete_closure_or_live_drift_before_any_write(closed_startup, damage):
    _, _, _, api, dormant, root = closed_startup
    first = next(iter(api.documents))
    if damage == 'mode':
        api.mode = 'global'
    elif damage == 'epoch':
        api.epoch += 1
    elif damage == 'guards':
        api.guards_held = False
    elif damage == 'uid':
        api.documents[first]['metadata']['uid'] = str(uuid4())
    elif damage == 'template':
        api.documents[first]['spec']['jobTemplate']['spec']['template']['spec']['containers'][0]['image'] = 'foreign'
    elif damage == 'dormant':
        api.documents[_key(dormant.collector)]['spec']['suspend'] = False
    elif damage == 'partial':
        path = root / 'cutover/cutover.json'
        record = json.loads(path.read_bytes())
        record['runtime'][next(iter(record['runtime']))] = {'phase': 'prepared', 'expected': None}
        path.write_text(json.dumps(record))
    else:
        next((root / 'cutover-anchor').glob('*-cutover.json')).unlink()
    with pytest.raises(ValueError) as error:
        start(closed_startup)
    assert 'private-marker' not in str(error.value) and not api.requests


@pytest.mark.parametrize('failure', ['before', 'after', 'conflict', 'preview'])
def test_startup_reconciles_lost_replies_without_repeating_uncertain_writes(closed_startup, failure):
    request, _, _, api, _, root = closed_startup
    api.fail_key = _key(request.manager)
    api.failure = failure
    first = start(closed_startup)
    if failure == 'before':
        assert first['status'] == 'pending_startup_outcome'
        api.failure = None
        assert start(closed_startup) == first
        assert api.requests == [api.fail_key]
        journal = json.loads((root / 'cutover/startup.json').read_bytes())
        assert journal['workloads'][api.fail_key]['phase'] == 'intent'
    elif failure == 'after':
        assert first['status'] == 'pool_startup_staged_closed'
        assert start(closed_startup) == first and len(api.requests) == 12
    else:
        assert first['status'] == 'pending_startup_update'
        assert len(api.requests) == (1 if failure == 'conflict' else 0)
        api.failure = None
        assert start(closed_startup)['status'] == 'pool_startup_staged_closed'
        assert len(api.requests) == (13 if failure == 'conflict' else 12)


def test_startup_cannot_resume_after_closed_parent_evidence_changes(closed_startup):
    _, _, _, api, _, root = closed_startup
    assert start(closed_startup)['status'] == 'pool_startup_staged_closed'
    path = root / 'cutover/cutover.json'
    record = json.loads(path.read_bytes())
    path.write_text(json.dumps(record, indent=4))
    with pytest.raises(ValueError):
        start(closed_startup)
    assert len(api.requests) == 12


@pytest.mark.parametrize('outcome', ['before', 'after'])
def test_recovery_selects_only_journaled_before_or_after_an_uncertain_start(closed_startup, outcome):
    from scripts.ops.nebius_pool_cutover import retained_cutover_workloads

    request, _, _, api, _, root = closed_startup
    api.fail_key, api.failure = _key(request.manager), 'before'
    assert start(closed_startup)['status'] == 'pending_startup_outcome'
    # A delayed request may commit after the first read observed the old state.
    if outcome == 'after':
        api.documents[api.fail_key]['spec']['replicas'] = 1
        api.documents[api.fail_key]['metadata']['resourceVersion'] = '100'
    expected = retained_cutover_workloads(request, state_dir=root / 'cutover',
        anchor_dir=root / 'cutover-anchor', observed=api.documents)
    assert expected[api.fail_key]['spec']['replicas'] == (1 if outcome == 'after' else 0)
    assert len(api.requests) == 1
    api.documents[api.fail_key]['spec']['replicas'] = 2
    with pytest.raises(ValueError):
        retained_cutover_workloads(request, state_dir=root / 'cutover',
            anchor_dir=root / 'cutover-anchor', observed=api.documents)
    assert len(api.requests) == 1


def test_closed_stage_cannot_replay_after_startup_intent_even_if_nothing_started(closed_startup):
    request, tokens, closed, api, _, root = closed_startup
    api.fail_key, api.failure = _key(request.manager), 'before'
    assert start(closed_startup)['status'] == 'pending_startup_outcome'
    prior_events = copy.deepcopy(closed.events)
    evidence = (root / 'cutover/cutover.json').read_bytes()
    with pytest.raises(ValueError):
        run(request, tokens, closed, root)
    assert closed.events == prior_events
    assert (root / 'cutover/cutover.json').read_bytes() == evidence


@pytest.mark.parametrize('outcome', ['before', 'after'])
def test_https_inventory_qualifies_both_sides_of_pending_start_without_writes(cutover_inputs, cutover_binding_inventory, tmp_path, outcome):
    from scripts.ops.nebius_pool_cutover import cutover_documents
    from scripts.ops.nebius_pool_startup import stage_pool_startup
    from tests.ops.test_nebius_pool_cutover import binding_preflight, writer_workload_inventory

    request, tokens = cutover_inputs
    closed = CutoverAPI(request)
    assert run(request, tokens, closed, tmp_path)['status'] == 'pool_runtime_staged_closed'
    api = StartupAPI(request, closed, tmp_path / 'cutover')
    api.fail_key, api.failure = _key(request.manager), 'before'
    assert stage_pool_startup(request=request, api=api, state_dir=tmp_path / 'cutover',
        anchor_dir=tmp_path / 'cutover-anchor')['status'] == 'pending_startup_outcome'
    if outcome == 'after':
        api.documents[api.fail_key]['spec']['replicas'] = 1
        api.documents[api.fail_key]['metadata']['resourceVersion'] = '100'
    rows = cutover_binding_inventory
    rows['roles'] = list(copy.deepcopy(closed.fencing.roles).values())
    resources = {'Role': 'roles', 'RoleBinding': 'rolebindings', 'ClusterRole': 'clusterroles', 'ClusterRoleBinding': 'clusterrolebindings'}
    for document in cutover_documents(request)['authority']:
        rows[resources[document['kind']]].append(copy.deepcopy(closed.resources.resources[_key(document)]))
    calls = binding_preflight(request, tokens, rows, journal=(tmp_path / 'cutover', tmp_path / 'cutover-anchor'),
        workloads=writer_workload_inventory(request, originals=api.documents.values()))
    assert calls and all(call.method == 'GET' for call in calls)
    assert api.requests == [api.fail_key]


def test_startup_rejects_gateway_receipt_that_does_not_match_its_fixed_target(closed_startup):
    _, _, _, api, _, root = closed_startup
    child_path = root / 'cutover/workload/stage.json'
    child = json.loads(child_path.read_bytes())
    key, item = next(iter(child['resources'].items()))
    item['observed']['spec']['template']['spec']['containers'][0]['image'] = 'foreign'
    api.documents[key]['spec']['template']['spec']['containers'][0]['image'] = 'foreign'
    child_path.write_text(json.dumps(child))
    # A parent checksum alone is not proof the child observed its fixed target.
    path = root / 'cutover/cutover.json'
    parent = json.loads(path.read_bytes())
    parent['phases']['workload'] = hashlib.sha256(child_path.read_bytes()).hexdigest()
    path.write_text(json.dumps(parent))
    with pytest.raises(ValueError):
        start(closed_startup)
    assert not api.requests
