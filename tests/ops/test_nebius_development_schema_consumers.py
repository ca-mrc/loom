"""Upgraded development ancestry selects exact downstream SQL and resources."""
from __future__ import annotations

import copy
from dataclasses import replace
from uuid import uuid4

import pytest
from scripts.ops.nebius_pool_activation_database import fence_pool_activation_sql
from scripts.ops.nebius_pool_machine_database import (
    pool_machine_retirement_sql,
    pool_recovery_machine_state_sql,
)
from scripts.ops.nebius_pool_startup_database import pool_startup_closed_sql
from tests.integration.test_nebius_pool_installation import installation as installation_document
from tests.ops.test_nebius_development_schema_projection import (
    application_management_inputs as application_management_inputs,
)
from tests.ops.test_nebius_development_schema_projection import (
    application_material as application_material,
)
from tests.ops.test_nebius_development_schema_projection import build_inputs as build_inputs
from tests.ops.test_nebius_development_schema_projection import capacity_checks as capacity_checks
from tests.ops.test_nebius_development_schema_projection import cloud as cloud
from tests.ops.test_nebius_development_schema_projection import collector_cloud as collector_cloud
from tests.ops.test_nebius_development_schema_projection import completed_pool as completed_pool
from tests.ops.test_nebius_development_schema_projection import connected as connected
from tests.ops.test_nebius_development_schema_projection import (
    development_inputs as development_inputs,
)
from tests.ops.test_nebius_development_schema_projection import entry as entry
from tests.ops.test_nebius_development_schema_projection import handoff as handoff
from tests.ops.test_nebius_development_schema_projection import installation as installation
from tests.ops.test_nebius_development_schema_projection import inventory as inventory
from tests.ops.test_nebius_development_schema_projection import live as live
from tests.ops.test_nebius_development_schema_projection import (
    management_inputs as management_inputs,
)
from tests.ops.test_nebius_development_schema_projection import manager_entry as manager_entry
from tests.ops.test_nebius_development_schema_projection import material as material
from tests.ops.test_nebius_development_schema_projection import original_cloud as original_cloud
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
from tests.ops.test_nebius_development_schema_projection import platform_inputs as platform_inputs
from tests.ops.test_nebius_development_schema_projection import pool_entry as pool_entry
from tests.ops.test_nebius_development_schema_projection import pool_inputs as pool_inputs
from tests.ops.test_nebius_development_schema_projection import preflight as preflight
from tests.ops.test_nebius_development_schema_projection import provider_checks as provider_checks
from tests.ops.test_nebius_development_schema_projection import publication as publication
from tests.ops.test_nebius_development_schema_projection import published_source as published_source
from tests.ops.test_nebius_development_schema_projection import publisher_cloud as publisher_cloud
from tests.ops.test_nebius_development_schema_projection import retained as retained
from tests.ops.test_nebius_development_schema_projection import route as route
from tests.ops.test_nebius_development_schema_projection import source_checkout as source_checkout
from tests.ops.test_nebius_development_schema_projection import successor_inputs as successor_inputs
from tests.ops.test_nebius_development_schema_projection import tls_material as tls_material

from loom_service.pool_management.installation import PoolInstallation


@pytest.mark.parametrize('query', [pool_startup_closed_sql, fence_pool_activation_sql,
    pool_recovery_machine_state_sql, lambda spec, **kwargs: pool_machine_retirement_sql(spec, action='revoke', **kwargs)])
@pytest.mark.parametrize('revision', ['0178', '0179', '0180'])
def test_selected_manager_schema_is_exact_without_changing_legacy_default(query, revision):
    spec = PoolInstallation.model_validate(installation_document(('staging', 'development'))[0])
    original = query(spec)
    assert "IS DISTINCT FROM '0176'" in original
    selected = query(spec, manager_revision=revision)
    assert f"IS DISTINCT FROM '{revision}'" in selected
    assert "IS DISTINCT FROM '0176'" not in selected
    assert query(spec) == original
    with pytest.raises(ValueError):
        query(spec, manager_revision='0199')


def completed_schema_successor(inputs, connection, tmp_path, *, runtime_reference=None):
    """Real parent receipts and completion loader; only external effects are doubled."""
    from scripts.ops.nebius_development_schema_upgrade import (
        load_completed_development_schema_successor,
        run_development_schema_upgrade,
    )
    from tests.ops.test_nebius_development_schema_upgrade import (
        SchemaParentAPI,
        retain_schema_upgrade,
    )

    retained = retain_schema_upgrade(inputs, connection, tmp_path, runtime_reference=runtime_reference)
    api = SchemaParentAPI(retained)
    assert run_development_schema_upgrade(retained=retained, api=api, action='apply')['status'] == 'complete'
    return load_completed_development_schema_successor(runtime=retained.runtime.request, reference=retained.reference)


@pytest.mark.parametrize('manager_entry', ['foundation-runtime-build-material-split-schema'], indirect=True)
@pytest.mark.parametrize('published_source', ['0175'], indirect=True)
@pytest.mark.parametrize('retained', [False], indirect=True)
def test_activation_consumes_schema_successor_without_rewriting_installation(successor_inputs, completed_pool, tmp_path):
    from scripts.ops.nebius_development_activation import prepare_development_activation
    from scripts.ops.nebius_development_runtime_retained import load_completed_runtime

    original = copy.deepcopy(successor_inputs['runtime'])
    successor = completed_schema_successor(successor_inputs, completed_pool[1]['operator_connection'], tmp_path)
    plan = prepare_development_activation(original.request, schema_successor=successor)
    selected = plan.selection
    assert selected.manager_revision == '0178'
    assert selected.registration.spec == successor.projection.after_pool
    assert selected.deployment == successor.projection.after_manager
    assert selected.profiles.application_images[0].recipe.schema_revision == '0178'
    assert plan.gateway_target['spec']['template']['metadata']['annotations']['loom.nebius/pool-operation'] == str(successor.projection.after_pool.operation_id)
    assert plan.gateway_original['spec']['replicas'] == 0
    assert plan.gateway_target['spec']['replicas'] == 1
    assert plan.runtime == original == load_completed_runtime(original.request)
    assert all(path.read_bytes() == raw for path, raw in original.files.items())
    assert prepare_development_activation(original.request).selection.manager_revision == '0176'
    changed = copy.deepcopy(successor.resources)
    changed['Deployment:loom-nebius-management-dev:loom-pool-gateway']['spec']['replicas'] = 1
    with pytest.raises(ValueError, match='unqualified'):
        prepare_development_activation(original.request, schema_successor=replace(successor, resources=changed))
    with pytest.raises(ValueError, match='unqualified'):
        prepare_development_activation(original.request, schema_successor=replace(successor,
            reference=successor.reference.model_copy(update={'operation_id': uuid4()})))
