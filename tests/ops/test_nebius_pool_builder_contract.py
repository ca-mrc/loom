"""Historical builder defaults cannot silently change an anchored cutover contract."""
from __future__ import annotations

import copy
import json
from dataclasses import replace

import pytest
from scripts.ops import nebius_certificates as private_state
from scripts.ops import nebius_pool_cutover as cutover
from tests.ops.test_nebius_pool_cutover import collector_inputs as collector_inputs
from tests.ops.test_nebius_pool_cutover import cutover_inputs as cutover_inputs
from tests.ops.test_nebius_pool_cutover import fencing_inputs as fencing_inputs
from tests.ops.test_nebius_pool_cutover import management_inputs as management_inputs
from tests.ops.test_nebius_pool_cutover import platform_inputs as platform_inputs
from tests.ops.test_nebius_pool_cutover import retirement_inputs as retirement_inputs
from tests.ops.test_nebius_pool_cutover import runtime_inputs as runtime_inputs

from loom.nebius_platform_render import digest
from loom_service.pool_management.installation import PoolInstallation


def _historical_payload(value):
    """The pre-I/O-policy schema, including its JSON-valued configuration data.

    This constructs old input/evidence before sealing it. It never changes the
    native reader, chooses its expected digest, or repairs already sealed bytes.
    """
    if isinstance(value, dict):
        return {key: _historical_payload(item) for key, item in value.items()
            if key != "io_full_avg10_max"}
    if isinstance(value, list):
        return [_historical_payload(item) for item in value]
    if isinstance(value, str) and value.startswith(("{", "[")):
        try:
            parsed = json.loads(value)
        except ValueError:
            return value
        old = _historical_payload(parsed)
        if old == parsed:
            return value
        # Preserve the renderer's original ordering and JSON whitespace. Only
        # the historical field omission may differ in this fixture.
        for separators in ((",", ":"), (", ", ": ")):
            if json.dumps(parsed, separators=separators) == value:
                return json.dumps(old, separators=separators)
        raise AssertionError("unexpected historical configuration JSON representation")
    return value


def _request_with_builder_pressure(request, value=None):
    request = copy.deepcopy(request)
    migration = request.fencing.retirement.migration
    payload = _historical_payload(migration.registration.spec.model_dump(mode="json"))
    if value is not None:
        for profile in payload["profiles"]["task_images"]:
            profile["settings"]["io_full_avg10_max"] = value
    spec = PoolInstallation.model_validate(payload)
    registration = replace(migration.registration, spec=spec)
    migration = replace(migration, registration=registration)
    actuators = copy.deepcopy(request.fencing.retirement.actuators)
    for actuator in actuators:
        for container in actuator["spec"]["template"]["spec"]["containers"]:
            for variable in container.get("env", []):
                if variable["name"] == "LOOM_EXECUTION_ACTUATOR_TASK_IMAGE_BUILDER":
                    settings = _historical_payload(json.loads(variable["value"]))
                    if value is not None:
                        settings["io_full_avg10_max"] = value
                    variable["value"] = json.dumps(settings, separators=(",", ":"))
    retirement = replace(request.fencing.retirement, migration=migration, actuators=actuators)
    return replace(request, fencing=replace(request.fencing, retirement=retirement))


def _seal_prepared(request, documents, contract, tmp_path):
    state, anchor = tmp_path / "cutover", tmp_path / "anchor"
    private_state._private_directory(state)
    private_state._private_directory(anchor)
    operation = str(request.fencing.retirement.migration.registration.spec.operation_id)
    identity = {"schema": "loom.nebius-pool-cutover.v1", "operation_id": operation,
        "state_dir": str(state), "contract_sha256": digest(contract)}
    record = {**identity,
        "producers": {key: {"phase": "prepared", "expected": None}
            for key in documents["producers"]},
        "fenced": None,
        "runtime_access": dict.fromkeys((str(row.participant_id)
            for row in request.fencing.retirement.migration.guards), "prepared"),
        "phases": {},
        "runtime": {key: {"phase": "prepared", "expected": None}
            for key in documents["runtime"]}}
    private_state._atomic_json(anchor / (operation + "-cutover.json"), identity)
    private_state._atomic_json(state / "cutover.json", record)
    return state, anchor, record


def test_legacy_builder_input_reopens_its_original_anchored_cutover(cutover_inputs, tmp_path):
    original, _ = cutover_inputs
    request = _request_with_builder_pressure(original)
    settings = request.fencing.retirement.migration.registration.spec.profiles.task_images[0].settings
    assert settings.io_full_avg10_max == 50.0
    assert "io_full_avg10_max" not in settings.model_fields_set
    documents = cutover.cutover_documents(request)
    historical_contract = _historical_payload(cutover._contract(request, documents))
    state, anchor, record = _seal_prepared(request, documents, historical_contract, tmp_path)
    before = {path: path.read_bytes() for directory in (state, anchor)
        for path in directory.iterdir()}
    before_request = copy.deepcopy(request)

    assert cutover._read_cutover_record(request, documents, state, anchor) == record

    assert all(path.read_bytes() == raw for path, raw in before.items())
    assert request == before_request


@pytest.mark.parametrize("pressure", [50.0, 17.5])
def test_explicit_builder_pressure_is_bound_and_cannot_replace_history(
        cutover_inputs, tmp_path, pressure):
    original, _ = cutover_inputs
    request = _request_with_builder_pressure(original, pressure)
    documents = cutover.cutover_documents(request)
    contract = cutover._contract(request, documents)
    state, anchor, record = _seal_prepared(request, documents, contract, tmp_path)
    before = {path: path.read_bytes() for directory in (state, anchor)
        for path in directory.iterdir()}
    before_request = copy.deepcopy(request)
    assert cutover._read_cutover_record(request, documents, state, anchor) == record

    for replacement in (None, 50.0 if pressure != 50.0 else 17.5):
        changed = _request_with_builder_pressure(request, replacement)
        changed_documents = cutover.cutover_documents(changed)
        with pytest.raises(ValueError):
            cutover._read_cutover_record(changed, changed_documents, state, anchor)

    assert all(path.read_bytes() == raw for path, raw in before.items())
    assert request == before_request
