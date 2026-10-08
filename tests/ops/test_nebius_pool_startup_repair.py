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


def test_repair_projection_bounds_duplicate_derivation_without_aliasing_or_hiding_drift(historical_cutover, monkeypatch):
    from scripts.ops import nebius_pool_cutover as cutover
    from scripts.ops.nebius_pool_startup_repair import source_repair_documents

    context, original, _ = historical_cutover
    derive = cutover._cutover_documents
    calls = []

    def counted(request):
        calls.append(request.application_delivery.source_delivery_version)
        return derive(request)

    monkeypatch.setattr(cutover, '_cutover_documents', counted)
    first, config = source_repair_documents(context.request, original)
    pristine = copy.deepcopy(first), copy.deepcopy(config)
    first['spec']['replicas'] = 99
    config['data']['installation.json'] = '{}'
    assert source_repair_documents(context.request, original) == pristine
    assert source_repair_documents(context.request, copy.deepcopy(original)) == pristine
    assert calls == ['v1', 'v2']
    original['metadata']['uid'] = str(uuid4())
    with pytest.raises(ValueError):
        source_repair_documents(context.request, original)


def test_reused_repair_projection_still_checks_current_image_admission(historical_cutover, monkeypatch):
    from scripts.ops import nebius_pool_cutover as cutover
    from scripts.ops.nebius_pool_startup_repair import source_repair_documents

    context, original, _ = historical_cutover
    source_repair_documents(context.request, original)

    def expired(request):
        raise ValueError('expired admission')

    monkeypatch.setattr(cutover, 'qualify_cutover_image_admission', expired)
    with pytest.raises(ValueError, match='pool_source_repair_projection_unqualified'):
        source_repair_documents(context.request, original)


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
    api.closed = closed
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


@pytest.mark.parametrize('damage', [None, 'foreign_uid', 'altered_config', 'lost_record'])
def test_repaired_config_live_readback_is_bound_to_journaled_create(prepared_repair, damage):
    from scripts.ops.nebius_pool_startup_repair import qualify_repair_configuration

    context, _, api, state, anchor = prepared_repair
    repair(prepared_repair)
    path = state / 'source-repair-configuration/stage.json'
    child = json.loads(path.read_bytes())
    key, = child['resources']
    actual = api.resources.resources[key]
    if damage == 'foreign_uid':
        actual['metadata']['uid'] = str(uuid4())
    elif damage == 'altered_config':
        actual['data']['installation.json'] = '{}'
    elif damage == 'lost_record':
        path.unlink()
    if damage is None:
        qualify_repair_configuration(context.request, state=state, anchor=anchor, read=api.resources.get_resource)
    else:
        with pytest.raises(ValueError):
            qualify_repair_configuration(context.request, state=state, anchor=anchor, read=api.resources.get_resource)


@pytest.mark.parametrize('failure', [None, 'config-before', 'template-before', 'start-after'])
def test_connected_repair_creates_only_fixed_config_and_uses_exact_manager_cas(prepared_repair, failure):
    from types import SimpleNamespace

    import httpx
    from scripts.ops.nebius_management_transport import ManagementKubernetesTransport
    from scripts.ops.nebius_pool_startup_repair import repair_pool_startup
    from scripts.ops.nebius_pool_startup_repair_live import HTTPSPoolStartupRepairAPI

    context, binding, remote, state, anchor = prepared_repair
    key = _key(context.request.manager)
    namespace = context.request.manager['metadata']['namespace']
    prefix = '/api/v1/namespaces/' + namespace
    workload = '/apis/apps/v1/namespaces/' + namespace + '/deployments/loom-service'
    objects, writes = {}, []

    def respond(message):
        path, method = message.url.path, message.method
        if method == 'GET':
            if path == workload:
                actual = remote.startup.documents[key]
                actual['metadata']['generation'] = 1
                actual['status'] = {'observedGeneration': 1, 'replicas': actual['spec']['replicas']}
                return httpx.Response(200, json=actual)
            if path.endswith('/replicasets') or path.endswith('/pods'):
                return httpx.Response(200, json={'apiVersion': 'apps/v1' if path.endswith('/replicasets') else 'v1',
                    'kind': 'ReplicaSetList' if path.endswith('/replicasets') else 'PodList',
                    'metadata': {'resourceVersion': '100'}, 'items': []})
            assert path.startswith(prefix + '/configmaps/loom-management-applications-')
            return httpx.Response(200, json=objects[path]) if path in objects else httpx.Response(404)
        if method == 'POST':
            assert path == prefix + '/configmaps'
            desired = json.loads(message.content)
            assert desired['kind'] == 'ConfigMap' and desired['immutable'] is True
            assert desired['metadata']['name'].startswith('loom-management-applications-')
            desired['metadata'].update(uid=str(uuid4()), resourceVersion='1')
            if message.url.params:
                assert dict(message.url.params) == {'dryRun': 'All'}
            else:
                child = json.loads((state / 'source-repair-configuration/stage.json').read_bytes())
                assert child['resources'][_key(desired)]['status'] == 'create_intent'
                writes.append('config')
                if failure == 'config-before':
                    raise httpx.ReadTimeout('private-unknown-create')
                objects[path + '/' + desired['metadata']['name']] = desired
            return httpx.Response(201, json=desired)
        assert method == 'PATCH' and path == workload
        actual = remote.read_workload(key)
        patches = json.loads(message.content)
        assert patches[:4] == [
            {'op': 'test', 'path': '/metadata/uid', 'value': actual['metadata']['uid']},
            {'op': 'test', 'path': '/metadata/resourceVersion', 'value': actual['metadata']['resourceVersion']},
            {'op': 'test', 'path': '/metadata', 'value': actual['metadata']},
            {'op': 'test', 'path': '/spec', 'value': actual['spec']}]
        assert len(patches) == 5 and patches[-1]['op'] == 'replace'
        change = patches[-1]
        desired = copy.deepcopy(actual)
        if change['path'] == '/spec/template':
            phase = 'template'
            assert actual['spec']['replicas'] == 0
            desired['spec']['template'] = change['value']
        else:
            assert change['path'] == '/spec/replicas' and type(change['value']) is int
            phase = 'stop' if change['value'] == 0 else 'start'
            desired['spec']['replicas'] = change['value']
        if message.url.params:
            assert dict(message.url.params) == {'dryRun': 'All'}
        else:
            journal = json.loads((state / 'startup-repair.json').read_bytes())
            assert journal['phases'][phase]['phase'] == 'intent'
            writes.append(phase)
            if failure == phase + '-before':
                raise httpx.ReadTimeout('private-unknown-patch')
            desired['metadata']['resourceVersion'] = str(int(actual['metadata']['resourceVersion']) + 1)
            remote.startup.documents[key] = desired
            if failure == phase + '-after':
                raise httpx.ReadTimeout('private-lost-reply')
        return httpx.Response(200, json=desired)

    class TransportOnlyRepair(HTTPSPoolStartupRepairAPI):
        def qualify_closed(self):
            # The owning connected-parent tests cover SQL/provider/credential
            # qualification. Keep this test on actual HTTP/CAS/drain behavior.
            self._qualify_binding()
            remote.qualify_closed()

    with httpx.Client(base_url='https://kubernetes.invalid', transport=httpx.MockTransport(respond)) as client:
        parent = SimpleNamespace(request=context.request, state_dir=state, anchor_dir=anchor,
            binding=context.request.fencing.retirement.migration.registration.binding,
            client=client, _scope=lambda: None, error_type=ValueError)
        parent._request = lambda method, path, **kwargs: ManagementKubernetesTransport._request(parent, method, path, **kwargs)
        api = TransportOnlyRepair(parent=parent, binding=binding)
        arguments = dict(request=context.request, binding=binding, api=api, state_dir=state, anchor_dir=anchor)
        if failure == 'config-before':
            for _ in range(2):
                with pytest.raises(ValueError):
                    repair_pool_startup(**arguments)
            assert writes == ['config']
        else:
            result = repair_pool_startup(**arguments)
            assert result['status'] == ('pending_source_repair_outcome' if failure == 'template-before'
                else 'pool_startup_repaired_closed')
            before = list(writes)
            assert repair_pool_startup(**arguments) == result and writes == before
            assert writes == (['config', 'stop', 'template'] if failure == 'template-before'
                else ['config', 'stop', 'template', 'start'])


def test_bound_operation_repairs_then_qualifies_and_completes_original_pool(prepared_repair, monkeypatch):
    from types import SimpleNamespace

    from scripts.ops import nebius_pool_operation as target

    context, binding, api, state, anchor = prepared_repair
    parent = SimpleNamespace(request=context.request, state_dir=state, anchor_dir=anchor, refresh=None)
    monkeypatch.setattr(target, 'HTTPSPoolStartupRepairAPI', lambda **kwargs: api, raising=False)
    monkeypatch.setattr(target, 'HTTPSPoolActivationAPI', lambda **kwargs: api.activation)
    api.activation.ready = True
    from scripts.ops import nebius_certificates as private_state

    with private_state._locked_state(anchor / 'dispatch'):
        with pytest.raises(target.PoolOperationError):
            target.run_pool_operation(parent=parent, tokens=context.tokens, action='install', repair_binding=binding)
    assert api.calls == []
    result = target.run_pool_operation(parent=parent, tokens=context.tokens, action='install', repair_binding=binding)
    assert result['status'] == 'pool_cutover_completed' and result['outcome'] == 'global'
    assert result['operation_id'] == context.operation['operation_id']
    assert api.calls == ['stop', 'template', 'start'] and api.activation.runtime_checks == 2
    assert target.run_pool_operation(parent=parent, tokens=context.tokens, action='install', repair_binding=binding) == result
    assert api.calls == ['stop', 'template', 'start']


def test_unbound_dispatch_cannot_advance_an_anchored_repair(prepared_repair, monkeypatch):
    from types import SimpleNamespace

    from scripts.ops import nebius_pool_operation as target

    context, _, api, state, anchor = prepared_repair
    repair(prepared_repair)
    api.activation.ready = True
    parent = SimpleNamespace(request=context.request, state_dir=state, anchor_dir=anchor, refresh=None)
    monkeypatch.setattr(target, 'HTTPSPoolActivationAPI', lambda **kwargs: api.activation)
    with pytest.raises(target.PoolOperationError):
        target.run_pool_operation(parent=parent, tokens=context.tokens, action='install')
    assert api.activation.calls == []


@pytest.fixture
def private_repair(prepared_repair):
    import hashlib

    from scripts.ops import nebius_certificates as private_state
    from tests.ops.test_nebius_pool_repair_authority import repair_operation

    context, binding, _, state, _ = prepared_repair
    root = state.parent.parent.parent
    operation = repair_operation(root.parent)
    operation.update(operation_id=str(binding.operation_id), source_sha=binding.source_sha, candidate=binding.source_sha,
        original_operation_id=context.operation['operation_id'], namespace=context.operation['namespace'],
        installation_id=context.operation['installation_id'])
    directory = root / 'pool-repair' / str(binding.operation_id)
    operation.update(state_dir=str(directory / 'state'), anchor_dir=str(directory / 'anchor'), inputs_path=str(directory / 'inputs.json'))
    private_state._private_directory(directory.parent)
    private_state._private_directory(directory)
    binding = binding.model_copy(update={'original_operation_sha256': hashlib.sha256(
        json.dumps(context.operation, sort_keys=True, separators=(',', ':')).encode()).hexdigest()})
    payload = {'schema_version': 'loom.nebius-pool-startup-repair-private-inputs.v1',
        'original_operation': context.operation, 'binding': binding.model_dump(mode='json')}
    save_private(operation, payload)
    return operation, payload, context


def test_repair_entry_loads_original_without_rewriting_private_contract(private_repair):
    from scripts.ops.nebius_pool_repair_entry import load_pool_repair_inputs

    operation, _, original = private_repair
    before = Path(original.operation['inputs_path']).read_bytes()
    context = load_pool_repair_inputs(operation)
    assert context.original == original and context.operation == operation
    assert Path(original.operation['inputs_path']).read_bytes() == before
    assert context.original.inputs.source_delivery_version == 'v1'


@pytest.mark.parametrize('damage', ['source', 'original_hash', 'inputs_hash', 'closure', 'startup', 'activation', 'operation_id'])
def test_repair_entry_rejects_changed_original_or_entry_binding(private_repair, damage):
    from scripts.ops.nebius_management_entry import EntryError
    from scripts.ops.nebius_pool_repair_entry import load_pool_repair_inputs

    operation, payload, _ = private_repair
    field = {'source': 'source_sha', 'original_hash': 'original_operation_sha256', 'inputs_hash': 'inputs_sha256',
        'closure': 'closure_sha256', 'startup': 'startup_sha256', 'activation': 'activation_sha256', 'operation_id': 'operation_id'}[damage]
    payload['binding'][field] = str(uuid4()) if damage == 'operation_id' else ('a' * 40 if damage == 'source' else 'a' * 64)
    save_private(operation, payload)
    with pytest.raises(EntryError):
        load_pool_repair_inputs(operation)


@pytest.mark.parametrize('action', ['install', 'blocked'])
def test_management_entry_runs_repair_and_reports_both_identities(private_repair, prepared_repair, monkeypatch, capsys, action):
    from contextlib import contextmanager
    from types import SimpleNamespace

    from scripts.ops import nebius_management_entry as entry
    from scripts.ops import nebius_pool_operation as operation_runner
    from scripts.ops import nebius_pool_repair_entry as repair_entry

    operation, _, original = private_repair
    _, _, api, state, anchor = prepared_repair
    path = Path(operation['inputs_path']).with_name('operation.json')
    path.write_text(json.dumps(operation))
    path.chmod(0o600)
    connection_events = []

    @contextmanager
    def connected(selected):
        assert selected == original
        connection_events.append('open')
        try:
            if action == 'blocked':
                raise operation_runner.PoolOperationError('startup_repair') from RuntimeError('private-payload')
            yield SimpleNamespace(request=original.request, state_dir=state, anchor_dir=anchor, refresh=None,
                checks=SimpleNamespace(), guards=SimpleNamespace(telemetry_report=lambda: {
                    'status': 'available', 'checks': 1, 'unavailable': 0, 'reasons': []}))
        finally:
            connection_events.append('close')

    monkeypatch.setattr(repair_entry, 'connected_pool_api', connected)
    monkeypatch.setattr(operation_runner, 'HTTPSPoolStartupRepairAPI', lambda **kwargs: api)
    monkeypatch.setattr(operation_runner, 'HTTPSPoolActivationAPI', lambda **kwargs: api.activation)
    api.activation.ready = True
    assert entry.main(str(path), 'install') == 0
    report = json.loads(capsys.readouterr().out)
    assert report['operation_id'] == operation['operation_id']
    assert report['original_operation_id'] == original.operation['operation_id']
    assert report['status'] == ('blocked' if action == 'blocked' else 'pool_cutover_completed'), report
    if action == 'blocked':
        assert report['stage'] == 'pool_startup_repair'
        assert api.calls == []
    else:
        assert report['outcome'] == 'global' and report['acceptance_verified'] is False
        assert api.calls == ['stop', 'template', 'start']
    assert connection_events == ['open', 'close']
    assert 'private-payload' not in json.dumps(report)


@pytest.mark.parametrize('change', ['status', 'busy', 'spec', 'uid'])
def test_manager_drain_distinguishes_controller_status_updates_from_workload_drift(prepared_repair, change):
    from types import SimpleNamespace

    from scripts.ops.nebius_pool_startup_repair_live import HTTPSPoolStartupRepairAPI

    context, binding, _, state, anchor = prepared_repair
    reads = []

    class DrainReader(HTTPSPoolStartupRepairAPI):
        def qualify_closed(self):
            self._qualify_binding()

        def read_workload(self, key):
            current = copy.deepcopy(self.documents[1])
            current['metadata'].update(uid=context.request.manager['metadata']['uid'], resourceVersion=str(len(reads) + 2), generation=2)
            current['status'] = {'observedGeneration': 2, 'replicas': 0}
            if reads:
                if change == 'spec':
                    current['spec']['replicas'] = 1
                elif change == 'uid':
                    current['metadata']['uid'] = str(uuid4())
                elif change == 'busy':
                    current['status']['replicas'] = 1
                else:
                    current['status']['conditions'] = []
            reads.append(current)
            return current

    def request(method, path):
        assert method == 'GET' and path.endswith(('/pods?limit=1000', '/replicasets?limit=1000'))
        replicas = '/replicasets' in path
        return {'apiVersion': 'apps/v1' if replicas else 'v1', 'kind': 'ReplicaSetList' if replicas else 'PodList',
            'metadata': {'resourceVersion': '100'}, 'items': []}

    parent = SimpleNamespace(request=context.request, state_dir=state, anchor_dir=anchor, _scope=lambda: None,
        binding=context.request.fencing.retirement.migration.registration.binding, _request=request)
    reader = DrainReader(parent=parent, binding=binding)
    if change in {'status', 'busy'}:
        assert reader.manager_drained(_key(context.request.manager), reader.documents[1]) is (change == 'status')
    else:
        with pytest.raises(ValueError):
            reader.manager_drained(_key(context.request.manager), reader.documents[1])


def test_repair_preflight_qualifies_completed_history_without_requiring_closed_admission(prepared_repair, monkeypatch):
    from types import SimpleNamespace

    from scripts.ops import nebius_pool_operation as target
    from scripts.ops.nebius_pool_startup_repair_live import HTTPSPoolStartupRepairAPI

    context, binding, api, state, anchor = prepared_repair
    parent = SimpleNamespace(request=context.request, state_dir=state, anchor_dir=anchor, refresh=None, _scope=lambda: None)
    monkeypatch.setattr(target, 'HTTPSPoolStartupRepairAPI', lambda **kwargs: api)
    monkeypatch.setattr(target, 'HTTPSPoolActivationAPI', lambda **kwargs: api.activation)
    api.activation.ready = True
    result = target.run_pool_operation(parent=parent, tokens=context.tokens, action='install', repair_binding=binding)
    assert result['outcome'] == 'global'

    class EntryReader(HTTPSPoolStartupRepairAPI):
        def qualify_closed(self):
            self._qualify_binding()

    monkeypatch.setattr(target, 'HTTPSPoolStartupRepairAPI', EntryReader)
    assert target.run_pool_operation(parent=parent, tokens=context.tokens, action='preflight', repair_binding=binding) == {
        'status': 'preflight_qualified', 'operation_id': context.operation['operation_id']}
    assert api.calls == ['stop', 'template', 'start']


@pytest.mark.parametrize('when', ['before', 'during'])
def test_repair_private_drift_at_retained_barrier_rejects_without_later_effects(private_repair, when):
    from types import SimpleNamespace

    from scripts.ops.nebius_management_entry import EntryError
    from scripts.ops.nebius_pool_repair_entry import _RepairChecks, load_pool_repair_inputs

    operation, _, _ = private_repair
    context = load_pool_repair_inputs(operation)
    path = Path(operation['inputs_path'])
    calls = []

    def check(request):
        assert request == context.original.request
        calls.append('read-only-prerequisites')
        if when == 'during':
            path.write_bytes(path.read_bytes() + b'\n')

    checks = _RepairChecks(SimpleNamespace(preflight=check), context)
    if when == 'before':
        path.write_bytes(path.read_bytes() + b'\n')
    with pytest.raises(EntryError):
        checks.preflight(context.original.request)
    assert calls == ([] if when == 'before' else ['read-only-prerequisites'])


def _original_recovery(name, monkeypatch):
    """Execute reviewed, hash-pinned readers/writers from original 80c12 tooling."""
    import hashlib
    import sys
    from types import ModuleType

    checksums = {'startup_fence': '9f6983ad070aa9ee4fc2e8690dbc07daf31a0b9ca0785aff71af96deb6ef0563',
        'startup': '7b49fea9462d076a52cfac27ef4ce7692c18dbfd32d9645546bf741c94620ef3',
        'completion': '36f9598683208571692be89f75899b4e153c98299ce97c1bfbe79cb6ae908b27'}
    path = Path(__file__).parents[1] / 'fixtures/nebius' / ('original-pool-' + name + '.py.txt')
    raw = path.read_bytes()
    assert hashlib.sha256(raw).hexdigest() == checksums[name]
    module = ModuleType('_loom_original_80c12_' + name)
    monkeypatch.setitem(sys.modules, module.__name__, module)
    exec(compile(raw, str(path), 'exec'), module.__dict__)
    return module


@pytest.mark.parametrize('entry', ['prepared', 'intent', 'late_stop', 'late_shutdown'])
@pytest.mark.timeout(420)
def test_original_tooling_fence_can_be_resumed_without_rewriting_its_bytes(prepared_repair, monkeypatch, entry):
    from scripts.ops.nebius_pool_activation_stage import advance_pool_activation
    from scripts.ops.nebius_pool_shutdown import stop_pool_successors
    from scripts.ops.nebius_pool_startup_fence import fence_pool_startup
    from tests.ops.test_nebius_pool_shutdown import ShutdownAPI

    context, _, remote, state, anchor = prepared_repair
    if entry == 'prepared':
        monkeypatch.setattr(remote, 'preview_repair', lambda *args: None)
    else:
        remote.failure = ('stop', 'before')
    repair(prepared_repair)
    original = _original_recovery('startup_fence', monkeypatch)
    api = ShutdownAPI((context.request, None, None, remote.startup, None, state.parent))
    arguments = dict(request=context.request, api=api, state_dir=state, anchor_dir=anchor)
    assert advance_pool_activation(**arguments, cancel=True)['status'] == 'pool_activation_cancelled'
    # The historical fence dynamically imports its original startup reader.
    # Restore both module bindings immediately after the old writer returns.
    import sys

    with monkeypatch.context() as legacy:
        legacy.setitem(sys.modules, 'scripts.ops.nebius_pool_startup', _original_recovery('startup', legacy))
        legacy.setitem(sys.modules, 'scripts.ops.nebius_pool_startup_fence', original)
        assert original.fence_pool_startup(**arguments)['status'] == 'startup_writes_fenced'
    before = {path: path.read_bytes() for path in (state / 'startup-fence.json',
        anchor / (context.operation['operation_id'] + '-startup-fence.json'))}
    if entry == 'late_shutdown':
        stop = api.stop_workload

        def uncertain_manager_stop(key, observed, desired, **kwargs):
            api.stop_failure = 'before' if key == _key(context.request.manager) else None
            return stop(key, observed, desired, **kwargs)

        monkeypatch.setattr(api, 'stop_workload', uncertain_manager_stop)
        assert stop_pool_successors(**arguments)['status'] == 'pending_shutdown_outcome'
    if entry in {'late_stop', 'late_shutdown'}:
        pending, desired = remote.pending
        desired = copy.deepcopy(desired)
        desired['metadata'].update(uid=pending['metadata']['uid'],
            resourceVersion=str(int(pending['metadata']['resourceVersion']) + 1))
        remote.startup.documents[_key(context.request.manager)] = desired
    assert fence_pool_startup(**arguments)['status'] == 'startup_writes_fenced'
    assert stop_pool_successors(**arguments)['status'] == 'pool_successors_stopped'
    actual = api.read_workload(_key(context.request.manager))
    assert actual['spec']['replicas'] == 0
    if remote.pending is not None:
        assert actual['metadata']['resourceVersion'] != remote.pending[0]['metadata']['resourceVersion']
    if entry == 'late_stop':
        assert _key(context.request.manager) not in api.stop_calls
    elif entry == 'late_shutdown':
        assert api.stop_calls.count(_key(context.request.manager)) == 1
    assert all(path.read_bytes() == raw for path, raw in before.items())
    if remote.pending is not None:
        # Once shutdown is settled, no restoration consumer may accept the
        # unresolved repair's original object version as invalidation proof.
        from scripts.ops.nebius_pool_startup_fence import observe_recovery_workloads

        remote.startup.documents[_key(context.request.manager)]['metadata']['resourceVersion'] = (
            remote.pending[0]['metadata']['resourceVersion'])
        with pytest.raises(ValueError):
            observe_recovery_workloads(context.request, api, state=state, anchor=anchor)


@pytest.fixture
def original_fenced_repair(prepared_repair, monkeypatch):
    import sys

    from scripts.ops.nebius_pool_activation_stage import advance_pool_activation
    from tests.ops.test_nebius_pool_shutdown import ShutdownAPI

    context, _, remote, state, anchor = prepared_repair
    remote.failure = ('stop', 'before')
    repair(prepared_repair)
    api = ShutdownAPI((context.request, None, None, remote.startup, None, state.parent))
    arguments = dict(request=context.request, api=api, state_dir=state, anchor_dir=anchor)
    assert advance_pool_activation(**arguments, cancel=True)['status'] == 'pool_activation_cancelled'
    with monkeypatch.context() as legacy:
        original = _original_recovery('startup_fence', legacy)
        legacy.setitem(sys.modules, 'scripts.ops.nebius_pool_startup', _original_recovery('startup', legacy))
        legacy.setitem(sys.modules, 'scripts.ops.nebius_pool_startup_fence', original)
        assert original.fence_pool_startup(**arguments)['status'] == 'startup_writes_fenced'
    return prepared_repair, api, arguments


@pytest.mark.parametrize('damage', ['stop_applied', 'template_intent', 'start_intent', 'uid', 'spec', 'version', 'marker', 'journal'])
def test_original_recovery_refuses_later_repair_or_unqualified_stop(original_fenced_repair, damage):
    from scripts.ops.nebius_pool_shutdown import stop_pool_successors
    from scripts.ops.nebius_pool_startup_fence import fence_pool_startup
    from scripts.ops.nebius_pool_startup_repair import original_recovery_repair

    fixture, api, arguments = original_fenced_repair
    context, _, remote, state, anchor = fixture
    if damage in {'stop_applied', 'template_intent', 'start_intent'}:
        path = state / 'startup-repair.json'
        record = json.loads(path.read_bytes())
        record['phases']['stop']['phase'] = 'applied'
        if damage != 'stop_applied':
            record['phases']['template'] = {'phase': 'intent', 'before_resource_version': '33'}
        if damage == 'start_intent':
            record['phases']['template']['phase'] = 'applied'
            record['phases']['start'] = {'phase': 'intent', 'before_resource_version': '34'}
        path.write_text(json.dumps(record, sort_keys=True))
        with pytest.raises(ValueError):
            original_recovery_repair(context.request, state=state, anchor=anchor)
    elif damage in {'marker', 'journal'}:
        path = (anchor / (context.operation['operation_id'] + '-startup-fence.json')
            if damage == 'marker' else state / 'startup-fence.json')
        path.write_bytes(path.read_bytes() + b'\n')
        with pytest.raises(ValueError):
            fence_pool_startup(**arguments)
    else:
        manager = remote.startup.documents[_key(context.request.manager)]
        manager['spec']['replicas'] = 0
        if damage == 'uid':
            manager['metadata']['uid'] = str(uuid4())
        elif damage == 'spec':
            manager['spec']['template']['spec']['containers'][0]['image'] = 'foreign'
        # 'version' keeps the original CAS resourceVersion despite scale zero.
        with pytest.raises(ValueError):
            stop_pool_successors(**arguments)
    assert _key(context.request.manager) not in api.stop_calls
    assert not (state / 'template-restoration.json').exists()


@pytest.mark.timeout(600)
def test_original_legacy_completion_is_preserved_with_repair_ancestry(original_fenced_repair, monkeypatch):
    from types import SimpleNamespace

    from scripts.ops import nebius_pool_operation as target
    from scripts.ops.nebius_pool_completion import complete_pool_cutover, load_pool_completion
    from scripts.ops.nebius_pool_migration import _hash
    from tests.ops.test_nebius_pool_gateway_retirement import GatewayAPI
    from tests.ops.test_nebius_pool_legacy_reopening import ReopeningAPI
    from tests.ops.test_nebius_pool_legacy_restart import RestartAPI
    from tests.ops.test_nebius_pool_machine_retirement import MachineAPI
    from tests.ops.test_nebius_pool_role_restoration import RoleAPI
    from tests.ops.test_nebius_pool_template_restoration import TemplateAPI

    fixture, cancelled_api, _ = original_fenced_repair
    context, binding, remote, state, anchor = fixture
    chain = context.request, context.tokens, remote.closed, remote.startup, None, state.parent
    machine = MachineAPI(chain)
    gateway = GatewayAPI(chain, machine)
    template = TemplateAPI(chain, gateway)
    roles = RoleAPI(chain, template)
    restart = RestartAPI(chain, roles)

    class RecoveryAPI(ReopeningAPI):
        def successor_drained(self, key, desired):
            assert (desired['spec']['suspend'] is True if desired['kind'] == 'CronJob'
                else desired['spec']['replicas'] == 0)
            return self.processes_drained

    runtime = RecoveryAPI(chain, restart)
    runtime.anchor = anchor
    runtime.mode, runtime.guards = cancelled_api.mode, cancelled_api.guards
    parent = SimpleNamespace(request=context.request, state_dir=state, anchor_dir=anchor, refresh=None)
    monkeypatch.setattr(target, 'HTTPSPoolActivationAPI', lambda **kwargs: runtime)
    original = _original_recovery('completion', monkeypatch)
    with monkeypatch.context() as legacy:
        legacy.setattr(target, 'complete_pool_cutover', original.complete_pool_cutover)
        result = target.run_pool_operation(parent=parent, tokens=context.tokens, action='rollback', repair_binding=binding)
    assert result['outcome'] == 'legacy'
    before = {path: path.read_bytes() for directory in (state, anchor) for path in directory.rglob('*.json')}
    receipt = json.loads((state / 'completion.json').read_bytes())
    assert str(state / 'startup-repair.json') not in receipt['phase_sha256']
    loaded = load_pool_completion(request=context.request, state_dir=state, anchor_dir=anchor,
        completion_sha256=result['completion_sha256'])
    assert loaded.outcome == 'legacy'
    for path in (state / 'startup-repair.json', anchor / (context.operation['operation_id'] + '-startup-repair.json'),
            state / 'source-repair-configuration/stage.json'):
        assert loaded.history[path] == _hash(path)
    assert complete_pool_cutover(request=context.request, api=runtime, state_dir=state, anchor_dir=anchor) == result
    assert all(path.read_bytes() == raw for path, raw in before.items())
    # Historical loading must not silently drop altered supplemental ancestry.
    path = state / 'startup-repair.json'
    record = json.loads(path.read_bytes())
    record['phases']['stop']['phase'] = 'applied'
    path.write_text(json.dumps(record, sort_keys=True))
    with pytest.raises(ValueError):
        load_pool_completion(request=context.request, state_dir=state, anchor_dir=anchor,
            completion_sha256=result['completion_sha256'])
