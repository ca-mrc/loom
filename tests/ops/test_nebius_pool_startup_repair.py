"""Fixed source startup recovery preserves the original closed installation."""
from __future__ import annotations

import copy
import json
from dataclasses import replace
from pathlib import Path
from uuid import uuid4

import pytest
from scripts.ops.nebius_ingress_stage import _key
from scripts.ops.nebius_pool_cutover import cutover_documents
from scripts.ops.nebius_pool_cutover_entry import load_pool_cutover_inputs
from tests.ops.test_nebius_pool_application_cutover import (
    application_management_inputs as application_management_inputs,
)
from tests.ops.test_nebius_pool_application_cutover import (
    application_material as application_material,
)
from tests.ops.test_nebius_pool_application_cutover import (
    build_inputs as build_inputs,
)
from tests.ops.test_nebius_pool_application_cutover import (
    builder_cutover_inputs as builder_cutover_inputs,
)
from tests.ops.test_nebius_pool_application_cutover import (
    checks as checks,
)
from tests.ops.test_nebius_pool_application_cutover import (
    cloud as cloud,
)
from tests.ops.test_nebius_pool_application_cutover import (
    collector_inputs as collector_inputs,
)
from tests.ops.test_nebius_pool_application_cutover import (
    completed_upgrade as completed_upgrade,
)
from tests.ops.test_nebius_pool_application_cutover import (
    cutover_inputs as cutover_inputs,
)
from tests.ops.test_nebius_pool_application_cutover import (
    database_guard as database_guard,
)
from tests.ops.test_nebius_pool_application_cutover import (
    entry_inputs as entry_inputs,
)
from tests.ops.test_nebius_pool_application_cutover import (
    fencing_inputs as fencing_inputs,
)
from tests.ops.test_nebius_pool_application_cutover import (
    installation as installation,
)
from tests.ops.test_nebius_pool_application_cutover import (
    management_inputs as management_inputs,
)
from tests.ops.test_nebius_pool_application_cutover import (
    material as material,
)
from tests.ops.test_nebius_pool_application_cutover import (
    platform_inputs as platform_inputs,
)
from tests.ops.test_nebius_pool_application_cutover import (
    private_cutover as private_cutover,
)
from tests.ops.test_nebius_pool_application_cutover import (
    private_upgrade as private_upgrade,
)
from tests.ops.test_nebius_pool_application_cutover import (
    retirement_inputs as retirement_inputs,
)
from tests.ops.test_nebius_pool_application_cutover import (
    runtime_inputs as runtime_inputs,
)
from tests.ops.test_nebius_pool_application_cutover import (
    save_private,
)


@pytest.fixture
def historical_cutover(builder_cutover_inputs):
    operation, payload, _, credentials = builder_cutover_inputs
    payload.pop('source_delivery_version')
    save_private(operation, payload)
    context = load_pool_cutover_inputs(operation)
    original = copy.deepcopy(cutover_documents(context.request)['runtime'][_key(context.request.manager)])
    original['metadata'].update(uid=context.request.manager['metadata']['uid'], resourceVersion='31')
    original['spec']['replicas'] = 1
    return context, original, credentials


def test_repair_changes_only_source_path_initializer_and_immutable_config(historical_cutover):
    from scripts.ops.nebius_pool_startup_repair import source_repair_documents

    context, original, _ = historical_cutover
    saved = copy.deepcopy(original)
    repaired, config = source_repair_documents(context.request, original)
    expected = copy.deepcopy(original)
    expected['metadata'].pop('uid')
    expected['metadata'].pop('resourceVersion')
    pod = expected['spec']['template']['spec']
    new_pod = repaired['spec']['template']['spec']
    for volume in pod['volumes']:
        if volume['name'] == 'management-config':
            volume['configMap']['name'] = config['metadata']['name']
    for container in (*pod['containers'], *pod['initContainers']):
        for mount in container.get('volumeMounts', []):
            if mount['name'] == 'application-source':
                mount['mountPath'] = '/run/loom-application-source'
        if container['name'] == 'prepare-application-source':
            new_init, = (row for row in new_pod['initContainers'] if row['name'] == container['name'])
            assert new_init['command'][-1] == '/run/loom-application-source/spool'
            assert new_init['command'] != container['command']
            container['command'] = new_init['command']
    revision = repaired['spec']['template']['metadata']['annotations']['loom.nebius/configuration-revision']
    expected['spec']['template']['metadata']['annotations']['loom.nebius/configuration-revision'] = revision
    assert config['metadata']['name'] == 'loom-management-applications-' + revision[7:19]
    assert config['kind'] == 'ConfigMap' and config['immutable'] is True
    old_config, = (row for row in cutover_documents(context.request)['configuration'] if row['kind'] == 'ConfigMap'
        and row['metadata']['name'].startswith('loom-management-applications-'))
    old_settings = json.loads(old_config['data']['installation.json'])
    old_settings['applications']['runtime']['source_upload']['spool_directory'] = '/run/loom-application-source/spool'
    assert json.loads(config['data']['installation.json']) == old_settings
    assert repaired == expected
    assert original == saved


@pytest.mark.parametrize('damage', ['current_version', 'uid', 'image', 'env', 'mount', 'replicas'])
def test_repair_refuses_foreign_or_nonhistorical_manager(historical_cutover, damage):
    from scripts.ops.nebius_pool_startup_repair import source_repair_documents

    context, original, _ = historical_cutover
    request = context.request
    if damage == 'current_version':
        request = replace(request, application_delivery=replace(request.application_delivery, source_delivery_version='v2'))
    elif damage == 'uid':
        original['metadata']['uid'] = str(uuid4())
    elif damage == 'replicas':
        original['spec']['replicas'] = 0
    else:
        pod = original['spec']['template']['spec']
        if damage == 'image':
            pod['containers'][0]['image'] = 'foreign'
        elif damage == 'env':
            pod['containers'][0]['env'].append({'name': 'FOREIGN', 'value': '1'})
        else:
            mount, = (row for row in pod['containers'][0]['volumeMounts'] if row['name'] == 'application-source')
            mount['mountPath'] = '/foreign'
    with pytest.raises(ValueError):
        source_repair_documents(request, original)


@pytest.fixture
def prepared_repair(historical_cutover):
    from scripts.ops.nebius_pool_activation_stage import advance_pool_activation
    from scripts.ops.nebius_pool_cutover import stage_pool_cutover
    from scripts.ops.nebius_pool_migration import _hash
    from scripts.ops.nebius_pool_startup import stage_pool_startup
    from scripts.ops.nebius_pool_startup_repair import PoolStartupRepairBinding
    from tests.ops.test_nebius_pool_activation_stage import ActivationAPI
    from tests.ops.test_nebius_pool_cutover import CutoverAPI
    from tests.ops.test_nebius_pool_startup import StartupAPI

    context, _, credentials = historical_cutover
    request = context.request
    state, anchor = Path(context.operation['state_dir']), Path(context.operation['anchor_dir'])
    closed = CutoverAPI(request)
    for document in closed.documents.values():
        document['metadata'].setdefault('resourceVersion', '1')
    assert stage_pool_cutover(request=request, tokens=context.tokens, api=closed,
        source_credentials=credentials, state_dir=state, anchor_dir=anchor)['status'] == 'pool_runtime_staged_closed'
    startup = StartupAPI(request, closed, state)
    assert stage_pool_startup(request=request, api=startup,
        state_dir=state, anchor_dir=anchor)['status'] == 'pool_startup_staged_closed'
    activation = ActivationAPI((request, None, None, startup, None, state.parent))
    activation.state, activation.ready = state, False
    with pytest.raises(ValueError):
        advance_pool_activation(request=request, api=activation, state_dir=state, anchor_dir=anchor)
    binding = PoolStartupRepairBinding(operation_id=uuid4(), source_sha='9' * 40,
        original_operation_sha256='8' * 64, inputs_sha256=context.operation['inputs_sha256'],
        closure_sha256=_hash(state / 'cutover.json'), startup_sha256=_hash(state / 'startup.json'),
        activation_sha256=_hash(state / 'activation.json'))
    api = RepairAPI(startup, closed.resources, activation)
    return context, binding, api, state, anchor


class RepairAPI:
    """Remote read/CAS/create boundaries only; real closure and phase journals."""

    def __init__(self, startup, resources, activation):
        self.startup, self.resources, self.activation = startup, resources, activation
        self.calls = []
        self.failure = None
        self.pending = None
        self.drained = True

    def qualify_closed(self):
        self.startup.qualify_closed()

    def read_workload(self, key):
        return self.startup.read_workload(key)

    def manager_drained(self, key, desired):
        assert self.startup.documents[key]['spec']['replicas'] == desired['spec']['replicas'] == 0
        return self.drained

    def preview_repair(self, phase, before, desired):
        assert before == self.startup.documents[_key(before)]
        return copy.deepcopy(desired)

    def patch_repair(self, phase, before, desired):
        assert before == self.startup.documents[_key(before)]
        record = json.loads((self.startup.state / 'startup-repair.json').read_bytes())
        assert record['phases'][phase] == {'phase': 'intent', 'before_resource_version': before['metadata']['resourceVersion']}
        self.calls.append(phase)
        if self.failure == (phase, 'before'):
            self.pending = (copy.deepcopy(before), copy.deepcopy(desired))
            raise OSError('private-repair-failure')
        actual = copy.deepcopy(desired)
        actual['metadata'].update(uid=before['metadata']['uid'], resourceVersion=str(int(before['metadata']['resourceVersion']) + 1))
        self.startup.documents[_key(before)] = actual
        if self.failure == (phase, 'after'):
            raise OSError('private-repair-failure')
        return True


def repair(fixture):
    from scripts.ops.nebius_pool_startup_repair import repair_pool_startup

    context, binding, api, state, anchor = fixture
    return repair_pool_startup(request=context.request, binding=binding, api=api, state_dir=state, anchor_dir=anchor)


def test_preopening_repair_preserves_original_journals_and_replays_readonly(prepared_repair):
    from scripts.ops.nebius_pool_startup import startup_workload_options

    context, _, api, state, anchor = prepared_repair
    originals = {name: (state / name).read_bytes() for name in ('cutover.json', 'startup.json', 'activation.json')}
    result = repair(prepared_repair)
    assert result['status'] == 'pool_startup_repaired_closed' and result['admission_open'] is False
    assert api.calls == ['stop', 'template', 'start']
    options = startup_workload_options(context.request, state_dir=state, anchor_dir=anchor)
    manager, = options[_key(context.request.manager)]
    assert manager['spec']['replicas'] == 1
    initializer, = (row for row in manager['spec']['template']['spec']['initContainers']
        if row['name'] == 'prepare-application-source')
    assert initializer['command'][-1] == '/run/loom-application-source/spool'
    assert repair(prepared_repair) == result and api.calls == ['stop', 'template', 'start']
    assert originals == {name: (state / name).read_bytes() for name in originals}
    assert api.activation.calls == [] and api.activation.mode == 'closed'


@pytest.mark.parametrize('damage', ['opening', 'cancellation', 'guard', 'startup', 'closure',
    'binding', 'mode', 'guards', 'uid', 'recovery', 'completion'])
def test_preopening_repair_refuses_ineligible_entry_without_writes(prepared_repair, damage):
    context, binding, api, state, anchor = prepared_repair
    if damage in {'opening', 'cancellation', 'guard'}:
        path = state / 'activation.json'
        record = json.loads(path.read_bytes())
        if damage == 'guard':
            record['guards'][next(iter(record['guards']))]['release'] = 'intent'
        else:
            record[damage] = 'intent'
        path.write_text(json.dumps(record))
    elif damage in {'closure', 'startup'}:
        path = state / ('cutover.json' if damage == 'closure' else 'startup.json')
        path.write_bytes(path.read_bytes() + b'\n')
    elif damage == 'binding':
        binding = binding.model_copy(update={'startup_sha256': 'f' * 64})
    elif damage == 'mode':
        api.startup.mode = 'global'
    elif damage == 'guards':
        api.startup.guards_held = False
    elif damage == 'uid':
        api.startup.documents[_key(context.request.manager)]['metadata']['uid'] = str(uuid4())
    else:
        (state / ('shutdown.json' if damage == 'recovery' else 'completion.json')).write_text('{}')
    before = copy.deepcopy(api.resources.resources)
    with pytest.raises(ValueError, match='repair'):
        repair((context, binding, api, state, anchor))
    assert api.calls == [] and api.resources.resources == before
    assert not (state / 'startup-repair.json').exists()


@pytest.mark.parametrize('phase', ['stop', 'template', 'start'])
@pytest.mark.parametrize('when', ['before', 'after'])
def test_repair_never_repeats_uncertain_manager_writes(prepared_repair, phase, when):
    from scripts.ops.nebius_pool_startup import startup_workload_options

    context, _, api, state, anchor = prepared_repair
    api.failure = (phase, when)
    result = repair(prepared_repair)
    api.failure = None
    calls = list(api.calls)
    if when == 'before':
        assert result['status'] == 'pending_source_repair_outcome'
        assert repair(prepared_repair) == result and api.calls == calls
        options = startup_workload_options(context.request, state_dir=state, anchor_dir=anchor)
        assert len(options[_key(context.request.manager)]) == 2
        # The initial request can commit later. Its exact submitted object,
        # rather than a new write, must be accepted on the next observation.
        before, desired = api.pending
        desired['metadata'].update(uid=before['metadata']['uid'],
            resourceVersion=str(int(before['metadata']['resourceVersion']) + 1))
        api.startup.documents[_key(before)] = desired
        assert repair(prepared_repair)['status'] == 'pool_startup_repaired_closed'
        assert api.calls.count(phase) == 1
    else:
        assert result['status'] == 'pool_startup_repaired_closed'
        assert repair(prepared_repair) == result and api.calls == calls
    assert calls.count(phase) == 1


def test_repair_waits_for_actual_manager_drain_before_changing_template(prepared_repair):
    _, _, api, _, _ = prepared_repair
    api.drained = False
    assert repair(prepared_repair)['status'] == 'pending_source_repair_drain'
    assert api.calls == ['stop']
    api.drained = True
    assert repair(prepared_repair)['status'] == 'pool_startup_repaired_closed'
    assert api.calls == ['stop', 'template', 'start']


def test_partial_repair_cannot_open_admission_even_if_runtime_callback_reports_ready(prepared_repair):
    from scripts.ops.nebius_pool_activation_stage import advance_pool_activation

    context, _, api, state, anchor = prepared_repair
    api.failure = ('template', 'before')
    assert repair(prepared_repair)['status'] == 'pending_source_repair_outcome'
    api.activation.ready = True
    with pytest.raises(ValueError):
        advance_pool_activation(request=context.request, api=api.activation, state_dir=state, anchor_dir=anchor)
    assert api.activation.calls == [] and api.activation.mode == 'closed'


def test_runtime_reader_qualifies_repaired_started_template(prepared_repair):
    from types import SimpleNamespace

    from scripts.ops.nebius_pool_startup import closed_startup_documents
    from scripts.ops.nebius_pool_startup_live import HTTPSPoolStartupAPI

    context, _, api, state, anchor = prepared_repair
    assert repair(prepared_repair)['status'] == 'pool_startup_repaired_closed'
    closed, targets = closed_startup_documents(context.request, state_dir=state, anchor_dir=anchor)
    adapter = SimpleNamespace(request=context.request, state=state, anchor=anchor, closed=closed,
        targets=targets, _scope=lambda: None, read_workload=api.read_workload)
    observed = HTTPSPoolStartupAPI._started_workloads(adapter)
    assert observed[_key(context.request.manager)] == api.read_workload(_key(context.request.manager))


def test_completed_repair_binds_ancestry_and_remains_a_valid_refresh_predecessor(prepared_repair):
    from scripts.ops.nebius_pool_activation_stage import advance_pool_activation
    from scripts.ops.nebius_pool_completion import complete_pool_cutover, load_pool_completion
    from scripts.ops.nebius_pool_predecessor import PoolPredecessorV1, load_completed_pool

    context, _, api, state, anchor = prepared_repair
    assert repair(prepared_repair)['status'] == 'pool_startup_repaired_closed'
    api.activation.ready = True
    assert advance_pool_activation(request=context.request, api=api.activation,
        state_dir=state, anchor_dir=anchor)['status'] == 'pool_activation_complete'
    result = complete_pool_cutover(request=context.request, api=api.activation, state_dir=state, anchor_dir=anchor)
    completed = load_pool_completion(request=context.request, state_dir=state, anchor_dir=anchor,
        completion_sha256=result['completion_sha256'])
    child = state / 'source-repair-configuration/stage.json'
    assert {state / 'startup-repair.json', child} <= set(completed.history)
    predecessor = PoolPredecessorV1(operation=context.operation, completion_sha256=result['completion_sha256'])
    loaded = load_completed_pool(predecessor, original=context.original)
    assert loaded.deployment.installation.applications.runtime.source_upload.spool_directory == Path('/run/loom-application-source/spool')
    assert loaded.active['metadata']['uid'] == context.request.manager['metadata']['uid']
    child.write_bytes(child.read_bytes() + b'\n')
    with pytest.raises(ValueError):
        load_completed_pool(predecessor, original=context.original)


@pytest.mark.parametrize('phase', ['stop', 'template', 'start', 'complete'])
@pytest.mark.parametrize('late_commit', [False, True])
def test_rollback_settles_every_repair_projection_before_successor_shutdown(prepared_repair, phase, late_commit):
    from scripts.ops.nebius_pool_activation_stage import advance_pool_activation
    from scripts.ops.nebius_pool_shutdown import stop_pool_successors
    from scripts.ops.nebius_pool_startup_fence import fence_pool_startup
    from tests.ops.test_nebius_pool_shutdown import ShutdownAPI

    context, _, repair_api, state, anchor = prepared_repair
    if phase != 'complete':
        repair_api.failure = (phase, 'before')
    repair(prepared_repair)
    pending = repair_api.pending
    if late_commit and pending is not None:
        before, desired = pending
        desired = copy.deepcopy(desired)
        desired['metadata'].update(uid=before['metadata']['uid'],
            resourceVersion=str(int(before['metadata']['resourceVersion']) + 1))
        repair_api.startup.documents[_key(before)] = desired
    api = ShutdownAPI((context.request, None, None, repair_api.startup, None, state.parent))
    api.state = state
    assert advance_pool_activation(request=context.request, api=api, state_dir=state,
        anchor_dir=anchor, cancel=True)['status'] == 'pool_activation_cancelled'
    assert fence_pool_startup(request=context.request, api=api, state_dir=state,
        anchor_dir=anchor)['status'] == 'startup_writes_fenced'
    if pending is not None:
        assert api.read_workload(_key(context.request.manager))['metadata']['resourceVersion'] != pending[0]['metadata']['resourceVersion']
    assert api.fence_calls == ([_key(context.request.manager)] if pending is not None and not late_commit else [])
    assert stop_pool_successors(request=context.request, api=api, state_dir=state,
        anchor_dir=anchor)['status'] == 'pool_successors_stopped'
    assert api.read_workload(_key(context.request.manager))['spec']['replicas'] == 0
    with pytest.raises(ValueError):
        repair(prepared_repair)


@pytest.mark.parametrize('failure', ['before', 'after', 'conflict'])
def test_uncertain_repair_fence_does_not_enable_shutdown_or_duplicate_writes(prepared_repair, failure):
    from scripts.ops.nebius_pool_activation_stage import advance_pool_activation
    from scripts.ops.nebius_pool_shutdown import stop_pool_successors
    from scripts.ops.nebius_pool_startup_fence import fence_pool_startup
    from tests.ops.test_nebius_pool_shutdown import ShutdownAPI

    context, _, repair_api, state, anchor = prepared_repair
    repair_api.failure = ('start', 'before')
    repair(prepared_repair)
    api = ShutdownAPI((context.request, None, None, repair_api.startup, None, state.parent))
    api.state, api.fence_failure = state, failure
    advance_pool_activation(request=context.request, api=api, state_dir=state, anchor_dir=anchor, cancel=True)
    arguments = dict(request=context.request, api=api, state_dir=state, anchor_dir=anchor)
    first = fence_pool_startup(**arguments)
    api.fence_failure = None
    if failure == 'before':
        assert first['status'] == 'pending_startup_fence'
        assert fence_pool_startup(**arguments) == first and len(api.fence_calls) == 1
        with pytest.raises(ValueError):
            stop_pool_successors(**arguments)
        # Either original request can win. A later resourceVersion invalidates
        # both old-version CAS requests without permitting a second fence write.
        before, desired = repair_api.pending
        desired['metadata'].update(uid=before['metadata']['uid'],
            resourceVersion=str(int(before['metadata']['resourceVersion']) + 1))
        repair_api.startup.documents[_key(before)] = desired
    assert fence_pool_startup(**arguments)['status'] == 'startup_writes_fenced'
    assert len(api.fence_calls) == (2 if failure == 'conflict' else 1)
    assert stop_pool_successors(**arguments)['status'] == 'pool_successors_stopped'


@pytest.mark.parametrize('phase', ['stop', 'start'])
def test_repair_fence_https_tests_exact_pending_uid_version_metadata_and_spec(prepared_repair, phase):
    from types import SimpleNamespace

    import httpx
    from scripts.ops.nebius_pool_activation_live import HTTPSPoolActivationAPI
    from scripts.ops.nebius_pool_activation_stage import advance_pool_activation
    from scripts.ops.nebius_pool_startup_fence import fence_pool_startup

    context, _, repair_api, state, anchor = prepared_repair
    repair_api.failure = (phase, 'before')
    repair(prepared_repair)
    remote = repair_api.activation
    advance_pool_activation(request=context.request, api=remote, state_dir=state, anchor_dir=anchor, cancel=True)
    key = _key(context.request.manager)
    writes = []

    def respond(message):
        assert message.method == 'PATCH'
        assert message.url.path == '/apis/apps/v1/namespaces/' + context.request.manager['metadata']['namespace'] + '/deployments/loom-service'
        before = remote.read_workload(key)
        patches = json.loads(message.content)
        assert patches[:4] == [
            {'op': 'test', 'path': '/metadata/uid', 'value': before['metadata']['uid']},
            {'op': 'test', 'path': '/metadata/resourceVersion', 'value': before['metadata']['resourceVersion']},
            {'op': 'test', 'path': '/metadata', 'value': before['metadata']},
            {'op': 'test', 'path': '/spec', 'value': before['spec']}]
        assert len(patches) == 5 and patches[-1]['op'] == 'add' and patches[-1]['path'] == '/metadata/annotations'
        desired = copy.deepcopy(before)
        desired['metadata']['annotations'] = patches[-1]['value']
        if message.url.params:
            assert dict(message.url.params) == {'dryRun': 'All'}
        else:
            assert json.loads((state / 'startup-fence.json').read_bytes())['workloads'][key]['phase'] == 'intent'
            desired['metadata']['resourceVersion'] = str(int(before['metadata']['resourceVersion']) + 1)
            remote.startup.documents[key] = desired
            writes.append(key)
        return httpx.Response(200, json=desired)

    with httpx.Client(base_url='https://kubernetes.invalid', transport=httpx.MockTransport(respond)) as client:
        parent = SimpleNamespace(request=context.request, state_dir=state, anchor_dir=anchor, client=client, _scope=lambda: None)
        api = HTTPSPoolActivationAPI(parent=parent)
        api.verify_retained, api.read_workload = remote.verify_retained, remote.read_workload
        api.pool_state, api.guard_state = remote.pool_state, remote.guard_state
        assert fence_pool_startup(request=context.request, api=api, state_dir=state, anchor_dir=anchor)['status'] == 'startup_writes_fenced'
    assert writes == [key]
