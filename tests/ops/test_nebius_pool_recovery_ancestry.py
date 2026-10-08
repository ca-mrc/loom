"""Read shared recovery ancestors once, without retaining evidence between calls."""
from __future__ import annotations

import json
from collections import Counter

import pytest
from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_pool_shutdown import _shutdown_record
from scripts.ops.nebius_pool_template_restoration import _template_record
from tests.ops.test_nebius_pool_template_restoration import closed_startup as closed_startup
from tests.ops.test_nebius_pool_template_restoration import collector_inputs as collector_inputs
from tests.ops.test_nebius_pool_template_restoration import (
    cutover_binding_inventory as cutover_binding_inventory,
)
from tests.ops.test_nebius_pool_template_restoration import cutover_inputs as cutover_inputs
from tests.ops.test_nebius_pool_template_restoration import fencing_inputs as fencing_inputs
from tests.ops.test_nebius_pool_template_restoration import gateway_retired, restore
from tests.ops.test_nebius_pool_template_restoration import management_inputs as management_inputs
from tests.ops.test_nebius_pool_template_restoration import platform_inputs as platform_inputs
from tests.ops.test_nebius_pool_template_restoration import retirement_inputs as retirement_inputs
from tests.ops.test_nebius_pool_template_restoration import runtime_inputs as runtime_inputs


@pytest.mark.timeout(180)
def test_recovery_record_reads_each_shared_closure_once(closed_startup, monkeypatch):
    api = gateway_retired(closed_startup)
    anchor = api.root / 'cutover-anchor'
    operation = api.request.fencing.retirement.migration.registration.spec.operation_id
    closure_marker = anchor / (str(operation) + '-cutover.json')
    original_read = private_state._private_read
    reads = Counter()

    def read(path, **kwargs):
        reads[path] += 1
        return original_read(path, **kwargs)

    monkeypatch.setattr(private_state, '_private_read', read)
    counts = {}
    for reader in (_shutdown_record, _template_record):
        reads.clear()
        first = reader(api.request, state=api.state, anchor=anchor)
        counts[reader.__name__] = reads[closure_marker]
        reads.clear()
        assert reader(api.request, state=api.state, anchor=anchor) == first
        assert reads[closure_marker] == counts[reader.__name__]
    assert counts == {'_shutdown_record': 1, '_template_record': 1}


@pytest.mark.timeout(180)
def test_recovery_record_rechecks_ancestor_and_current_journals_between_calls(closed_startup, monkeypatch):
    api = gateway_retired(closed_startup)
    monkeypatch.setattr(api, 'preview_legacy_template', lambda *args: None)
    assert restore(closed_startup, api)['status'] == 'pending_template_restoration_update'
    anchor = api.root / 'cutover-anchor'
    operation = api.request.fencing.retirement.migration.registration.spec.operation_id

    def read():
        return _template_record(api.request, state=api.state, anchor=anchor)

    expected = read()
    assert expected[-1] is not None
    paths = [api.state / name for name in (
        'cutover.json', 'writers/registration/stage.json', 'activation.json',
        'startup-fence.json', 'shutdown.json', 'machine-retirement.json',
        'gateway-retirement.json', 'template-restoration.json')]
    paths.append(anchor / (str(operation) + '-machine-retirement.json'))
    for path in paths:
        original = path.read_bytes()
        document = json.loads(original)
        document['unexpected'] = True
        try:
            path.write_text(json.dumps(document))
            with pytest.raises(ValueError):
                read()
        finally:
            path.write_bytes(original)
        assert read() == expected
    # This fixture cancelled before startup. A newly appearing unanchored
    # journal must also be observed, rather than reuse the earlier absence.
    startup = api.state / 'startup.json'
    assert not startup.exists()
    try:
        private_state._atomic_json(startup, {})
        with pytest.raises(ValueError):
            read()
    finally:
        startup.unlink()
    assert read() == expected
