"""Live pool refresh readback preserves the original completed authority."""
from __future__ import annotations

import copy
from uuid import uuid4

import pytest
from scripts.ops.nebius_ingress_stage import _key
from tests.ops.test_nebius_management_refresh_connected import (
    connected_refresh as connected_refresh,
)
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
from tests.ops.test_nebius_pool_predecessor import finish_cutover, pool_refresh_http


@pytest.mark.timeout(900)
@pytest.mark.parametrize('legacy', [False, True], ids=['global', 'legacy'])
def test_pool_refresh_live_preserves_open_work_and_rejects_authority_drift(private_cutover, legacy):
    from scripts.ops.nebius_pool_predecessor import PoolPredecessorV1, load_completed_pool
    from scripts.ops.nebius_pool_refresh import PoolManagerRefresh
    from tests.ops.test_nebius_management_refresh_install import run
    from tests.ops.test_nebius_management_refresh_predecessor import refresh_case

    operation, _, root = private_cutover
    context, result = finish_cutover(operation, legacy=legacy)
    selector = PoolPredecessorV1(operation=operation, completion_sha256=result['completion_sha256'])
    pool = load_completed_pool(selector, original=root)
    assert pool.completion.outcome == ('legacy' if legacy else 'global')
    case = refresh_case(root, pool, pool_baseline=selector.model_dump(mode='json'))
    assert run(case)['status'] == 'management_refreshed'
    bound = PoolManagerRefresh(root, pool, case[0], case[2])
    before = {path: path.read_bytes() for path in (*pool.history, *case[2].rglob('*.json'))}
    with pool_refresh_http(bound)() as (api, external):
        api.qualify()
        objects = copy.deepcopy(external.objects)
        assert external.reviews
        # Live DB and provider failures must propagate without leaking their
        # payload. An open global pool is deliberately not required to be idle.
        failures = ['pool', 'credentials', 'guard', 'late_guard', 'database_role',
            'provider', 'gateway_rights', 'legacy_rights'] + (['global_busy'] if legacy else [])
        for failure in failures:
            external.failure = failure
            with pytest.raises(ValueError, match=r'^pool_refresh_live_unqualified$'):
                api.qualify()
            external.failure = None
        gateway = 'Deployment:' + context.request.fencing.retirement.migration.registration.binding.namespace + ':loom-pool-gateway'
        manager, participant = _key(context.request.manager), _key(context.request.services[0])
        material = next(key for key, row in objects.items() if row['kind'] == 'Secret')
        for key in (manager, participant, gateway, material):
            external.objects[key]['metadata']['uid'] = str(uuid4())
            with pytest.raises(ValueError, match=r'^pool_refresh_live_unqualified$'):
                api.qualify()
            external.objects = copy.deepcopy(objects)
        if legacy:
            external.objects[gateway]['status']['replicas'] = 1
            with pytest.raises(ValueError, match=r'^pool_refresh_live_unqualified$'):
                api.qualify()
            external.objects = copy.deepcopy(objects)
        else:
            external.failure = 'global_busy'
            api.qualify()  # The global ledger need not drain for an upgrade.
            external.failure = None
        api.qualify()
        assert external.objects == objects
        assert all(call.method == 'GET' or call.url.path == '/apis/authorization.k8s.io/v1/selfsubjectrulesreviews'
            for call in external.calls)
    assert {path: path.read_bytes() for path in before} == before
