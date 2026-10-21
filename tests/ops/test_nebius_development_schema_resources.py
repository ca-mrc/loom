"""Schema upgrade delivers all consumers without changing retained credentials."""
from __future__ import annotations

import copy
import importlib
import json

import pytest
from tests.ops.test_nebius_development_schema_projection import (
    application_management_inputs as application_management_inputs,
)
from tests.ops.test_nebius_development_schema_projection import (
    application_material as application_material,
)
from tests.ops.test_nebius_development_schema_projection import (
    build_inputs as build_inputs,
)
from tests.ops.test_nebius_development_schema_projection import (
    capacity_checks as capacity_checks,
)
from tests.ops.test_nebius_development_schema_projection import (
    cloud as cloud,
)
from tests.ops.test_nebius_development_schema_projection import (
    collector_cloud as collector_cloud,
)
from tests.ops.test_nebius_development_schema_projection import (
    completed_pool as completed_pool,
)
from tests.ops.test_nebius_development_schema_projection import (
    connected as connected,
)
from tests.ops.test_nebius_development_schema_projection import (
    development_inputs as development_inputs,
)
from tests.ops.test_nebius_development_schema_projection import (
    entry as entry,
)
from tests.ops.test_nebius_development_schema_projection import (
    handoff as handoff,
)
from tests.ops.test_nebius_development_schema_projection import (
    installation as installation,
)
from tests.ops.test_nebius_development_schema_projection import (
    inventory as inventory,
)
from tests.ops.test_nebius_development_schema_projection import (
    live as live,
)
from tests.ops.test_nebius_development_schema_projection import (
    management_inputs as management_inputs,
)
from tests.ops.test_nebius_development_schema_projection import (
    manager_entry as manager_entry,
)
from tests.ops.test_nebius_development_schema_projection import (
    material as material,
)
from tests.ops.test_nebius_development_schema_projection import (
    original_cloud as original_cloud,
)
from tests.ops.test_nebius_development_schema_projection import (
    original_development_inputs as original_development_inputs,
)
from tests.ops.test_nebius_development_schema_projection import (
    original_manager_entry as original_manager_entry,
)
from tests.ops.test_nebius_development_schema_projection import (
    original_platform_inputs as original_platform_inputs,
)
from tests.ops.test_nebius_development_schema_projection import (
    original_pool_inputs as original_pool_inputs,
)
from tests.ops.test_nebius_development_schema_projection import (
    platform_inputs as platform_inputs,
)
from tests.ops.test_nebius_development_schema_projection import (
    pool_entry as pool_entry,
)
from tests.ops.test_nebius_development_schema_projection import (
    pool_inputs as pool_inputs,
)
from tests.ops.test_nebius_development_schema_projection import (
    preflight as preflight,
)
from tests.ops.test_nebius_development_schema_projection import (
    provider_checks as provider_checks,
)
from tests.ops.test_nebius_development_schema_projection import (
    publication as publication,
)
from tests.ops.test_nebius_development_schema_projection import (
    published_source as published_source,
)
from tests.ops.test_nebius_development_schema_projection import (
    publisher_cloud as publisher_cloud,
)
from tests.ops.test_nebius_development_schema_projection import (
    retained as retained,
)
from tests.ops.test_nebius_development_schema_projection import (
    route as route,
)
from tests.ops.test_nebius_development_schema_projection import (
    source_checkout as source_checkout,
)
from tests.ops.test_nebius_development_schema_projection import (
    successor_inputs as successor_inputs,
)
from tests.ops.test_nebius_development_schema_projection import (
    tls_material as tls_material,
)

pytestmark = [
    pytest.mark.parametrize('manager_entry', ['foundation-runtime-build-material-split-schema'], indirect=True),
    pytest.mark.parametrize('published_source', ['0175'], indirect=True),
    pytest.mark.parametrize('retained', [False], indirect=True),
]


def test_schema_resources_stop_before_replacing_every_bound_consumer(successor_inputs):
    from scripts.ops.nebius_development_schema_projection import (
        project_development_schema_successor,
    )
    name = 'scripts.ops.nebius_development_schema_resources'
    assert importlib.util.find_spec(name), 'connected schema runtime resources are missing'
    original = copy.deepcopy(successor_inputs)
    projection = project_development_schema_successor(**successor_inputs)
    plan = importlib.import_module(name).schema_runtime_resources(runtime=successor_inputs['runtime'], projection=projection)
    assert successor_inputs == original
    assert set(plan.originals) == set(plan.stopped) == set(plan.targets)
    assert all(row['spec'].get('replicas', 0) == 0 and row['spec'].get('suspend', True)
        for row in plan.stopped.values())
    assert all(row['metadata'].get('namespace') != 'loom-staging' for row in [*plan.targets.values(), *plan.fixed.values()])
    assert not any(row['kind'] == 'Secret' for row in plan.fixed.values())
    old_resources = plan.source
    for key, target in plan.targets.items():
        assert target['metadata']['uid'] == old_resources[key]['metadata']['uid']
        if target['kind'] == 'Deployment':
            old_pod = old_resources[key]['spec']['template']['spec']
            new_pod = target['spec']['template']['spec']
            assert [v for v in new_pod.get('volumes', []) if 'secret' in v] == [v for v in old_pod.get('volumes', []) if 'secret' in v]
            for container in new_pod['containers']:
                for setting in container.get('env', []):
                    if setting['name'] in {'LOOM_CP_SERVICE_EXECUTION_GLOBAL_POOL_JSON', 'LOOM_EXECUTION_ACTUATOR_GLOBAL_POOL'}:
                        assert json.loads(setting['value'])['participant'] == projection.participant.model_dump(mode='json')
    manager = plan.targets['Deployment:loom-nebius-management-dev:loom-service']
    volumes = {v['name']: v for v in manager['spec']['template']['spec']['volumes']}
    catalog = plan.fixed['ConfigMap:loom-nebius-management-dev:' + volumes['pool-profiles']['configMap']['name']]
    assert json.loads(catalog['data']['profiles.json']) == projection.after_pool.profiles.model_dump(mode='json')
    assert plan.targets['Deployment:loom-nebius-management-dev:loom-pool-gateway']['spec']['replicas'] == 0
