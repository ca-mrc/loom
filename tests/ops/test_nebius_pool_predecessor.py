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
