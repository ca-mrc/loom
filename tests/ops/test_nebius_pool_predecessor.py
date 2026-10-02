"""The refresh baseline must come from a completed, private-input-bound cutover."""
from __future__ import annotations

import copy
import json
from pathlib import Path
from uuid import uuid4

import pytest
from scripts.ops.nebius_ingress_stage import _key
from tests.ops.test_nebius_pool_cutover_entry import (
    application_management_inputs as application_management_inputs,
)
from tests.ops.test_nebius_pool_cutover_entry import application_material as application_material
from tests.ops.test_nebius_pool_cutover_entry import checks as checks
from tests.ops.test_nebius_pool_cutover_entry import cloud as cloud
from tests.ops.test_nebius_pool_cutover_entry import collector_inputs as collector_inputs
from tests.ops.test_nebius_pool_cutover_entry import completed_upgrade as completed_upgrade
from tests.ops.test_nebius_pool_cutover_entry import cutover_inputs as cutover_inputs
from tests.ops.test_nebius_pool_cutover_entry import database_guard as database_guard
from tests.ops.test_nebius_pool_cutover_entry import entry_inputs as entry_inputs
from tests.ops.test_nebius_pool_cutover_entry import fencing_inputs as fencing_inputs
from tests.ops.test_nebius_pool_cutover_entry import installation as installation
from tests.ops.test_nebius_pool_cutover_entry import management_inputs as management_inputs
from tests.ops.test_nebius_pool_cutover_entry import material as material
from tests.ops.test_nebius_pool_cutover_entry import platform_inputs as platform_inputs
from tests.ops.test_nebius_pool_cutover_entry import private_cutover as private_cutover
from tests.ops.test_nebius_pool_cutover_entry import private_upgrade as private_upgrade
from tests.ops.test_nebius_pool_cutover_entry import retirement_inputs as retirement_inputs
from tests.ops.test_nebius_pool_cutover_entry import runtime_inputs as runtime_inputs


def finish_cutover(operation):
    from scripts.ops.nebius_pool_activation_stage import advance_pool_activation
    from scripts.ops.nebius_pool_completion import complete_pool_cutover
    from scripts.ops.nebius_pool_cutover import stage_pool_cutover
    from scripts.ops.nebius_pool_cutover_entry import load_pool_cutover_inputs
    from scripts.ops.nebius_pool_startup import stage_pool_startup
    from tests.ops.test_nebius_pool_activation_stage import ActivationAPI
    from tests.ops.test_nebius_pool_cutover import CutoverAPI
    from tests.ops.test_nebius_pool_startup import StartupAPI

    context = load_pool_cutover_inputs(operation)
    state, anchor = Path(operation['state_dir']), Path(operation['anchor_dir'])
    closed = CutoverAPI(context.request)
    # Completed history omits volatile metadata. A real Kubernetes GET supplies
    # a current version; the external-I/O double must do the same for CAS.
    for document in closed.documents.values():
        document['metadata'].setdefault('resourceVersion', '1')
    assert stage_pool_cutover(request=context.request, tokens=context.tokens, api=closed,
        state_dir=state, anchor_dir=anchor)['status'] == 'pool_runtime_staged_closed'
    startup = StartupAPI(context.request, closed, state)
    assert stage_pool_startup(request=context.request, api=startup, state_dir=state,
        anchor_dir=anchor)['status'] == 'pool_startup_staged_closed'
    api = ActivationAPI((context.request, context.tokens, closed, startup, None, state.parent))
    api.state = state
    assert advance_pool_activation(request=context.request, api=api, state_dir=state,
        anchor_dir=anchor)['status'] == 'pool_activation_complete'
    result = complete_pool_cutover(request=context.request, api=api, state_dir=state, anchor_dir=anchor)
    return context, result


@pytest.mark.timeout(420)
def test_completed_pool_derives_catalog_baseline_and_preserves_original_authority(private_cutover):
    from scripts.ops.nebius_management_refresh import ManagementRefreshRenderRequest, render_refresh
    from scripts.ops.nebius_pool_predecessor import PoolPredecessorV1, load_completed_pool

    operation, _, root = private_cutover
    context, result = finish_cutover(operation)
    selector = PoolPredecessorV1(operation=operation, completion_sha256=result['completion_sha256'])
    before = {path: path.read_bytes() for path in Path(operation['inputs_path']).parent.parent.parent.rglob('*.json')}
    pool = load_completed_pool(selector, original=root)
    assert pool.selector == selector and pool.completion.outcome == 'global'
    assert pool.deployment.pool_catalog_operation_id == context.inputs.installation.operation_id
    original = root.deployment.model_dump(mode='json')
    expected = copy.deepcopy(original)
    expected['pool_catalog_operation_id'] = operation['operation_id']
    assert pool.deployment.model_dump(mode='json') == expected
    assert root.deployment.model_dump(mode='json') == original
    assert pool.active == pool.completion.workloads[_key(context.request.manager)]
    assert pool.active['metadata']['uid'] == root.active['metadata']['uid']
    assert pool.history[Path(operation['inputs_path'])] == operation['inputs_sha256']
    assert set(root.history) <= set(pool.history) and set(pool.completion.history) <= set(pool.history)
    assert len(pool.history) <= 128
    # Exercise the existing next-upgrade renderer: no duplicate pool renderer or
    # removal of its strict catalog-preservation contract is needed.
    render = ManagementRefreshRenderRequest(pool.deployment, pool.deployment, pool.active,
        context.inputs.candidate, context.inputs.profile, root.upgrade.setup.repo_root)
    target = render_refresh(render).deployment
    profiles, = (row for row in target['spec']['template']['spec']['volumes'] if row['name'] == 'pool-profiles')
    assert profiles['configMap']['name'] == 'loom-pool-profiles-' + context.inputs.installation.operation_id.hex
    assert {path: path.read_bytes() for path in before} == before

    # The receipt and private input hashes select one operation, not any other
    # completed upgrade or an arbitrary caller-supplied post-cutover manager.
    for damage in ('input_hash', 'completion_hash', 'operation', 'original'):
        altered = selector.model_dump()
        supplied = root
        if damage == 'input_hash':
            altered['operation']['inputs_sha256'] = '0' * 64
        elif damage == 'completion_hash':
            altered['completion_sha256'] = '0' * 64
        elif damage == 'operation':
            altered['operation']['operation_id'] = str(uuid4())
        else:
            from dataclasses import replace

            active = copy.deepcopy(root.active)
            active['metadata']['uid'] = str(uuid4())
            supplied = replace(root, active=active)
        with pytest.raises(ValueError, match='pool_predecessor_unqualified'):
            load_completed_pool(PoolPredecessorV1.model_validate(altered), original=supplied)
    # Altering still-hashed phase bytes cannot be hidden by an unchanged receipt.
    activation = Path(operation['state_dir']) / 'activation.json'
    record = json.loads(activation.read_bytes())
    record['cancellation'] = 'intent'
    activation.write_text(json.dumps(record))
    with pytest.raises(ValueError, match='pool_predecessor_unqualified'):
        load_completed_pool(selector, original=root)


@pytest.mark.timeout(420)
def test_successive_refresh_completions_keep_the_qualified_pool_baseline(private_cutover):
    import ssl
    from types import SimpleNamespace

    from scripts.ops.nebius_management_refresh_connected import HTTPSManagementRefreshInstaller
    from scripts.ops.nebius_management_stage import ManagementStageError
    from scripts.ops.nebius_pool_predecessor import PoolPredecessorV1, load_completed_pool
    from scripts.ops.nebius_pool_refresh import PoolManagerRefresh
    from tests.ops.test_nebius_management_refresh_entry import context, private_refresh
    from tests.ops.test_nebius_management_refresh_predecessor import (
        complete_refresh,
        load_refresh,
        refresh_case,
    )

    operation, _, root = private_cutover
    _, result = finish_cutover(operation)
    selector = PoolPredecessorV1(operation=operation, completion_sha256=result['completion_sha256'])
    pool = load_completed_pool(selector, original=root)
    metadata, _, _ = private_refresh(root, pool)
    bound = context(metadata)
    assert bound.predecessor == pool
    assert bound.request.pool_baseline == selector.model_dump(mode='json')
    prior = pool
    histories = []
    for _ in range(2):
        refreshed, case = complete_refresh(root, prior, pool_baseline=selector.model_dump(mode='json'))
        completed = load_refresh(refreshed, root)
        assert completed.pool_baseline == pool
        metadata, _, _ = private_refresh(root, completed)
        inherited = context(metadata)
        assert inherited.predecessor == completed
        assert inherited.request.pool_baseline == selector.model_dump(mode='json')
        projected = PoolManagerRefresh(root, completed, inherited.request, Path(metadata['state_dir'])).workload_options()
        assert projected[_key(pool.context.request.manager)] == (completed.active,)
        assert completed.deployment.pool_catalog_operation_id == pool.deployment.pool_catalog_operation_id
        assert completed.active['metadata']['uid'] == pool.active['metadata']['uid']
        assert all(completed.history.get(path) == checksum for path, checksum in pool.history.items())
        assert len(completed.history) <= 128
        histories.append(len(completed.history))
        receipt = json.loads((case[2] / 'completion.json').read_bytes())
        assert receipt['contract']['pool_baseline'] == selector.model_dump(mode='json')
        prior = completed
    assert histories[0] == histories[1], 'ordinary refresh ancestry must remain bounded'
    # Without its pool baseline, a self-consistent pool-wired refresh still fails
    # the rooted check; do not accept its caller-supplied before snapshot alone.
    unbound, _ = complete_refresh(root, prior)
    with pytest.raises(ValueError, match='refresh_predecessor_unqualified'):
        load_refresh(unbound, root)
    completion = Path(operation['state_dir']) / 'completion.json'
    saved = completion.read_bytes()
    completion.write_bytes(saved + b'\n')
    with pytest.raises(ValueError, match='refresh_predecessor_unqualified'):
        load_refresh(refreshed, root)
    completion.write_bytes(saved)
    assert load_refresh(refreshed, root) == prior
    # Receipt support must not accidentally admit a live refresh through the
    # legacy manager-only verifier, including when its request drops the field.
    for baseline in (selector.model_dump(mode='json'), None):
        request, _, state, _ = refresh_case(root, prior, pool_baseline=baseline)
        with pytest.raises(ManagementStageError, match='pool-aware'):
            with HTTPSManagementRefreshInstaller(request=request, original=root, predecessor=prior,
                state_dir=state, api_server=root.original_inputs.operator_connection.endpoint, ssl_context=ssl.create_default_context(),
                runtime_ca_pem=None, checks=SimpleNamespace()):
                pass


def test_predecessor_recursion_is_bounded_and_failure_does_not_poison_next_load():
    from contextlib import ExitStack

    from scripts.ops.nebius_management_refresh_predecessor import _predecessor_scope

    operation = str(uuid4())
    with _predecessor_scope('refresh', operation):
        with pytest.raises(ValueError):
            with _predecessor_scope('refresh', operation):
                pytest.fail('cyclic ancestry was admitted')
    with pytest.raises(ValueError), ExitStack() as stack:
        for _ in range(9):
            stack.enter_context(_predecessor_scope('pool-cutover', str(uuid4())))
    with _predecessor_scope('refresh', operation):
        pass  # Both duplicate and depth-limit failures release their scope.


@pytest.mark.timeout(420)
def test_refresh_projects_only_manager_from_anchored_steps_and_uncertain_writes(private_cutover):
    from scripts.ops.nebius_ingress_stage import _snapshot, _uid
    from scripts.ops.nebius_management_refresh_install import ManagementRefreshInstallError
    from scripts.ops.nebius_management_refresh_switch import refresh_target
    from scripts.ops.nebius_pool_predecessor import PoolPredecessorV1, load_completed_pool
    from scripts.ops.nebius_pool_refresh import PoolManagerRefresh
    from tests.ops.test_nebius_management_refresh_install import run
    from tests.ops.test_nebius_management_refresh_predecessor import refresh_case

    operation, _, root = private_cutover
    _, result = finish_cutover(operation)
    selector = PoolPredecessorV1(operation=operation, completion_sha256=result['completion_sha256'])
    pool = load_completed_pool(selector, original=root)
    case = refresh_case(root, pool, pool_baseline=selector.model_dump(mode='json'))
    request, api, state, _ = case
    bound = PoolManagerRefresh(root, pool, request, state)
    manager = _key(pool.context.request.manager)

    def check(*allowed):
        before = {path: path.read_bytes() for path in (*pool.history, *state.rglob('*.json'))}
        options = bound.workload_options()
        assert set(options) == set(pool.completion.workloads)
        assert tuple(_snapshot(row) for row in options[manager]) == tuple(_snapshot(row) for row in allowed)
        assert all(_uid(row) == _uid(pool.active) for row in options[manager])
        assert {key: value for key, value in options.items() if key != manager} == {
            key: (value,) for key, value in pool.completion.workloads.items() if key != manager}
        assert {path: path.read_bytes() for path in before} == before

    check(pool.active)
    api.switch.failure = 'before'
    with pytest.raises(ManagementRefreshInstallError):
        run(case)
    stopped = refresh_target(request.resources.switch, 'retire')
    check(pool.active, stopped)
    # Simulate readback of the previously uncertain CAS, not a new write.
    api.switch.document = api.switch.desired('retire')
    api.switch.failure, api.pending = None, 'manager-probe'
    assert run(case)['phase'] == 'manager-probe'
    check(stopped)
    assert api.switch.calls == ['retire']
    api.pending, api.switch.failure = None, 'before'
    with pytest.raises(ManagementRefreshInstallError):
        run(case)
    active = json.loads((state / 'switch/cutover.json').read_bytes())['active']
    check(stopped, active)
    api.switch.document = api.switch.desired('activate')
    api.switch.failure = None
    assert run(case)['status'] == 'management_refreshed'
    check(active)
    assert api.switch.calls == ['retire', 'activate']


@pytest.mark.timeout(420)
def test_refresh_projection_rejects_unbound_history_and_tampered_journals(private_cutover):
    from dataclasses import replace

    from scripts.ops.nebius_pool_predecessor import PoolPredecessorV1, load_completed_pool
    from scripts.ops.nebius_pool_refresh import PoolManagerRefresh
    from tests.ops.test_nebius_management_refresh_install import run
    from tests.ops.test_nebius_management_refresh_predecessor import refresh_case

    operation, _, root = private_cutover
    _, result = finish_cutover(operation)
    selector = PoolPredecessorV1(operation=operation, completion_sha256=result['completion_sha256'])
    pool = load_completed_pool(selector, original=root)
    case = refresh_case(root, pool, pool_baseline=selector.model_dump(mode='json'))
    request, _, state, anchor = case
    bound = PoolManagerRefresh(root, pool, request, state)
    assert run(case)['status'] == 'management_refreshed'
    expected = bound.workload_options()
    for altered in (replace(bound, state_dir=state.parent / 'foreign'),
            replace(bound, request=replace(request, pool_baseline=None)),
            replace(bound, request=replace(request, history={}))):
        with pytest.raises(ValueError, match='pool_refresh_projection_unqualified'):
            altered.workload_options()
    stopped = copy.deepcopy(pool.active)
    stopped['spec']['replicas'] = 0
    stopped['metadata'].setdefault('annotations', {})['loom.nebius/management-refresh-id'] = str(uuid4())
    unqualified = replace(request, resources=replace(request.resources,
        switch=replace(request.resources.switch, initial_stopped=stopped)))
    with pytest.raises(ValueError, match='pool_refresh_projection_unqualified'):
        replace(bound, request=unqualified).workload_options()
    parent_path, switch_path = state / 'refresh.json', state / 'switch/cutover.json'
    paths = (parent_path, switch_path, anchor / (str(request.resources.switch.operation_id) + '.json'),
        state / 'post-migration-probe/stage.json', Path(operation['state_dir']) / 'completion.json')
    saved = {path: path.read_bytes() for path in paths}
    for path in paths:
        path.write_bytes(b'{}')
        with pytest.raises(ValueError, match='pool_refresh_projection_unqualified'):
            bound.workload_options()
        path.write_bytes(saved[path])
    # A valid-looking target is not permission to activate before parent proofs.
    parent = json.loads(saved[parent_path])
    parent['activation_started'] = False
    parent_path.write_text(json.dumps(parent))
    with pytest.raises(ValueError, match='pool_refresh_projection_unqualified'):
        bound.workload_options()
    parent_path.write_bytes(saved[parent_path])
    assert bound.workload_options() == expected
