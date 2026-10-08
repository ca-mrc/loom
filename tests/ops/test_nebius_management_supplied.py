"""Supplied runtime authorities are immutable, private and isolated from bootstrap."""
from __future__ import annotations

import base64
import copy
import json

import pytest
from tests.ops.test_nebius_management_authority_stage import inputs as inputs


@pytest.fixture
def material():
    return {
        "loom-management-cloud": {"credentials.json": '{"subject-credentials":{"type":"JWT"}}'},
        "loom-management-publications": {"token": "scoped-publication-test-token"},
        "loom-platform-storage": {"backup-access-key": "backup-test-access", "backup-secret-key": "backup-test-secret"},
    }


def run(inputs, material, state):
    from scripts.ops.nebius_management_supplied import deliver_supplied_material

    return deliver_supplied_material(binding=inputs[1], material=material, api=inputs[2], state_dir=state)


def test_supplied_authorities_are_delivered_once_without_exposing_values(inputs, material, tmp_path):
    first = run(inputs, material, tmp_path / "state")
    original = copy.deepcopy(inputs[2].resources)
    assert run(inputs, material, tmp_path / "state") == first
    assert len(inputs[2].creates) == 3
    assert inputs[2].resources == original
    assert set(first) == {"status", "installation_id", "namespace", "namespace_uid", "secret_uids"}
    for secret in original.values():
        assert secret["immutable"] is True
        for name, data in secret["data"].items():
            assert base64.b64decode(data).decode() == material[secret["metadata"]["name"]][name]
            assert data not in json.dumps(first)


@pytest.mark.parametrize("failure", ["before", "after"])
def test_unknown_supplied_write_is_only_reconciled_by_readback(inputs, material, tmp_path, failure):
    from scripts.ops.nebius_management_stage import ManagementStageError

    inputs[2].failure = failure
    for _ in range(2):
        if failure == "before":
            with pytest.raises(ManagementStageError, match="unresolved"):
                run(inputs, material, tmp_path / "state")
        else:
            run(inputs, material, tmp_path / "state")
    assert len(inputs[2].creates) == (1 if failure == "before" else 3)


def test_changed_supplied_input_cannot_rotate_retained_credentials(inputs, material, tmp_path):
    from scripts.ops.nebius_management_stage import ManagementStageError

    run(inputs, material, tmp_path / "state")
    material["loom-management-publications"]["token"] = "changed-token"
    with pytest.raises(ManagementStageError, match="journal"):
        run(inputs, material, tmp_path / "state")
    assert len(inputs[2].creates) == 3


def test_supplied_defaulting_cannot_add_unqualified_credentials(inputs, material, tmp_path):
    from scripts.ops.nebius_management_stage import ManagementStageError

    inputs[2].default_change = lambda doc: doc["data"].update({"unqualified": "dW5xdWFsaWZpZWQ="})
    with pytest.raises(ManagementStageError, match="defaulting"):
        run(inputs, material, tmp_path / "state")
    assert not inputs[2].creates


@pytest.mark.parametrize("change", ["bootstrap", "missing", "extra", "empty", "cloud_json"])
def test_supplied_material_rejects_non_fixed_or_incomplete_secrets(inputs, material, tmp_path, change):
    from scripts.ops.nebius_management_stage import ManagementStageError

    if change == "bootstrap":
        material["loom-platform-db"] = {"service-url": "private-unqualified-input"}
    elif change == "missing":
        del material["loom-platform-storage"]["backup-access-key"]
    elif change == "extra":
        material["loom-platform-storage"]["foreign-bucket-key"] = "private-unqualified-input"
    elif change == "empty":
        material["loom-management-publications"]["token"] = ""
    else:
        material["loom-management-cloud"]["credentials.json"] = "private-unqualified-input"
    with pytest.raises(ManagementStageError, match="supplied") as error:
        run(inputs, material, tmp_path / "state")
    assert "private-unqualified-input" not in str(error.value)
    assert not inputs[2].creates


def test_application_only_material_needs_no_legacy_cloud_key(inputs, material, tmp_path):
    from scripts.ops.nebius_management_supplied import deliver_supplied_material

    del material['loom-management-cloud']
    args = dict(binding=inputs[1], material=material, api=inputs[2],
                state_dir=tmp_path / 'state', application_only=True)
    receipt = deliver_supplied_material(**args)
    assert set(receipt['secret_uids']) == {'loom-management-publications', 'loom-platform-storage'}
    assert deliver_supplied_material(**args) == receipt
    assert len(inputs[2].creates) == 2


@pytest.mark.parametrize('application_only', [False, True])
def test_supplied_material_cannot_cross_manager_modes(inputs, material, tmp_path, application_only):
    from scripts.ops.nebius_management_stage import ManagementStageError
    from scripts.ops.nebius_management_supplied import deliver_supplied_material

    if not application_only:
        del material['loom-management-cloud']
    with pytest.raises(ManagementStageError, match='supplied'):
        deliver_supplied_material(binding=inputs[1], material=material, api=inputs[2],
                                  state_dir=tmp_path / 'state', application_only=application_only)
    assert not inputs[2].creates
