"""A repair authority cannot impersonate or replace the original pool operation."""
from __future__ import annotations

import json
from uuid import uuid4

import pytest
from tests.ops.test_nebius_management_gateway import pool_operation


def repair_operation(tmp_path, version='v1'):
    original = pool_operation(tmp_path)
    operation_id = str(uuid4())
    root = tmp_path / 'nebius-management/pool-repair' / operation_id
    return {**original, 'schema': 'loom.nebius-pool-startup-repair-operation.' + version,
        'operation_id': operation_id, 'original_operation_id': original['operation_id'],
        'state_dir': str(root / 'state'), 'anchor_dir': str(root / 'anchor'), 'inputs_path': str(root / 'inputs.json')}


@pytest.fixture(params=['v1', 'v2', 'v3'])
def repair_version(request):
    return request.param


@pytest.mark.parametrize('action', ['qualify', 'preflight', 'install', 'rollback'])
def test_repair_authority_has_its_own_namespace_and_fixed_complete_directions(tmp_path, action, repair_version):
    from scripts.ops.nebius_management_gateway import validate_action

    validate_action(action, repair_operation(tmp_path, repair_version))


@pytest.mark.parametrize('damage', ['original_path', 'original_id', 'nil_original', 'missing_original', 'source', 'stage'])
def test_repair_authority_rejects_borrowed_identity_or_arbitrary_phase(tmp_path, damage, repair_version):
    from scripts.ops.nebius_management_gateway import GatewayError, validate_action

    operation = repair_operation(tmp_path, repair_version)
    if damage == 'original_path':
        operation['state_dir'] = operation['state_dir'].replace('/pool-repair/', '/pool-cutover/')
    elif damage == 'original_id':
        operation['original_operation_id'] = operation['operation_id']
    elif damage == 'nil_original':
        operation['original_operation_id'] = '00000000-0000-0000-0000-000000000000'
    elif damage == 'missing_original':
        del operation['original_operation_id']
    elif damage == 'source':
        operation['source_sha'] = 'f' * 40
    with pytest.raises(GatewayError):
        validate_action('template' if damage == 'stage' else 'install', operation)


@pytest.mark.parametrize('result', [
    {'status': 'preflight_qualified'},
    {'status': 'pending', 'phase': 'startup-repair'},
    {'status': 'pending', 'phase': 'manager-image'},
    {'status': 'blocked', 'stage': 'pool_startup_repair'},
    {'status': 'pool_cutover_completed', 'outcome': 'global', 'completion_sha256': 'b' * 64, 'acceptance_verified': False},
])
def test_repair_report_binds_both_operations_without_claiming_acceptance(tmp_path, result, repair_version):
    from scripts.ops.nebius_management_gateway import GatewayError, safe_report

    operation = repair_operation(tmp_path, repair_version)
    metadata = {key: operation[key] for key in ('source_sha', 'candidate', 'installation_id', 'namespace',
        'operation_id', 'original_operation_id')}
    report = {**metadata, **result}
    assert safe_report(json.dumps(report).encode(), operation) == report
    report['original_operation_id'] = str(uuid4())
    with pytest.raises(GatewayError):
        safe_report(json.dumps(report).encode(), operation)
