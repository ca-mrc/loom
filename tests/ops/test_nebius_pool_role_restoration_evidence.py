"""Recovery anchors and version evidence cannot be substituted during replay."""
from __future__ import annotations

import copy
import json

import pytest
from scripts.ops.nebius_management_switch import _stable
from tests.ops import test_nebius_pool_role_restoration as restoration_fixtures
from tests.ops.test_nebius_pool_role_restoration import closed_startup as closed_startup
from tests.ops.test_nebius_pool_role_restoration import collector_inputs as collector_inputs
from tests.ops.test_nebius_pool_role_restoration import (
    cutover_binding_inventory as cutover_binding_inventory,
)
from tests.ops.test_nebius_pool_role_restoration import cutover_inputs as cutover_inputs
from tests.ops.test_nebius_pool_role_restoration import management_inputs as management_inputs
from tests.ops.test_nebius_pool_role_restoration import platform_inputs as platform_inputs
from tests.ops.test_nebius_pool_role_restoration import restore_roles, templates_restored
from tests.ops.test_nebius_pool_role_restoration import retirement_inputs as retirement_inputs
from tests.ops.test_nebius_pool_role_restoration import runtime_inputs as runtime_inputs

unannotated_fencing_inputs = restoration_fixtures.fencing_inputs


@pytest.fixture
def fencing_inputs(unannotated_fencing_inputs):
    # Freeze actual predecessor annotations before any migration journal exists.
    unannotated_fencing_inputs.originals[0]['metadata']['annotations'] = {'owner.example/retained': 'keep'}
    return unannotated_fencing_inputs


@pytest.mark.timeout(240)
def test_partial_restoration_requires_parent_anchor_version_and_preserves_original_annotations(closed_startup):
    from scripts.ops.nebius_pool_role_restoration import restored_role_options

    api = templates_restored(closed_startup)
    api.legacy_failure = 'before'
    assert restore_roles(api)['status'] == 'pending_role_restoration_outcome'
    key, = api.legacy_calls
    anchor = api.root / 'cutover-anchor'
    operation = api.request.fencing.retirement.migration.registration.spec.operation_id
    path = api.state / 'role-restoration.json'
    marker = anchor / (str(operation) + '-role-restoration.json')
    original_journal, original_marker = path.read_bytes(), marker.read_bytes()

    def options():
        return restored_role_options(api.request, state=api.state, anchor=anchor)

    baseline = options()
    assert len(baseline[key]) == 2
    # Each damaged receipt must fail, not fall back to the restricted catalog.
    for damaged in (path, marker, api.state / 'template-restoration.json'):
        saved = damaged.read_bytes()
        damaged.write_text('{}')
        with pytest.raises(ValueError):
            options()
        damaged.write_bytes(saved)
    marker.unlink()
    with pytest.raises(ValueError):
        options()
    marker.write_bytes(original_marker)
    marker.chmod(0o600)
    for damage in ('foreign_operation', 'prepared_version', 'restored_no_version', 'unknown_role'):
        record = json.loads(original_journal)
        if damage == 'foreign_operation':
            record['operation_id'] = 'aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaaaa'
        elif damage == 'prepared_version':
            record['roles'][key]['phase'] = 'prepared'
        elif damage == 'restored_no_version':
            record['roles'][key] = {'phase': 'restored', 'before_resource_version': None}
        else:
            record['roles']['Role:foreign:writer'] = record['roles'].pop(key)
        path.write_text(json.dumps(record))
        with pytest.raises(ValueError):
            options()
    path.write_bytes(original_journal)
    assert options() == baseline and api.legacy_calls == [key]

    # Desired content with the unchanged version cannot prove the intended CAS.
    retained = copy.deepcopy(api.request.fencing.originals[0])
    version = api.legacy_roles[key]['metadata']['resourceVersion']
    retained['metadata']['resourceVersion'] = version
    api.legacy_roles[key] = retained
    with pytest.raises(ValueError):
        restore_roles(api)
    assert path.read_bytes() == original_journal and api.legacy_calls == [key]
    retained['metadata']['resourceVersion'] = str(int(version) + 1)
    api.legacy_failure = None
    assert restore_roles(api)['status'] == 'pool_legacy_roles_restored_closed'
    assert _stable(api.legacy_roles[key]) == _stable(api.request.fencing.originals[0])
    assert api.legacy_roles[key]['metadata']['annotations'] == {'owner.example/retained': 'keep'}
    assert len(api.legacy_calls) == 6 and set(api.guards.values()) == {'fenced'}
