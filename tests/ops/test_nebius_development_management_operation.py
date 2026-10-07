"""Dev-manager metadata cannot select the retained manager or foundation state."""
from __future__ import annotations

import copy
import importlib
from pathlib import Path

import pytest


def module():
    return importlib.import_module("scripts.ops.nebius_development_management_operation")


def operation(tmp_path):
    identity = "a1111111-2222-4333-8444-555555555555"
    root = tmp_path / "operator/.loom/nebius-development-management" / identity
    return {
        "schema": "loom.nebius-development-management-operation.v1",
        "source_sha": "a" * 40, "candidate": "a" * 40,
        "installation_id": identity, "namespace": "loom-nebius-management-dev",
        "inputs_path": str(root / "inputs.json"), "state_dir": str(root / "state"),
        "anchor_dir": str(root.parent.parent / "nebius-development-management-anchors" / identity),
        "inputs_sha256": "b" * 64,
    }


def test_canonical_identity_resolves_only_its_own_root_and_operator_home(tmp_path):
    value = operation(tmp_path)
    before = copy.deepcopy(value)
    assert module().validate_operation(value) is None
    assert module().operation_root(value) == Path(value["inputs_path"]).parent
    assert module().operator_home(value) == tmp_path / "operator"
    assert value == before
    assert not (tmp_path / "operator").exists(), "validation must not create installation state"


@pytest.mark.parametrize("namespace", [
    "loom-nebius-platform", "loom-dev", "loom-nebius-management",
    "loom-nebius-management-dev-other", "loom-dev-alice",
])
@pytest.mark.parametrize("consumer", ["validate_operation", "operation_root", "operator_home"])
def test_every_consumer_refuses_another_namespace(tmp_path, namespace, consumer):
    with pytest.raises(ValueError, match="^development management operation unqualified$"):
        getattr(module(), consumer)(operation(tmp_path) | {"namespace": namespace})


@pytest.mark.parametrize("field,replacement", [
    ("schema", "loom.nebius-management-operation.v1"),
    ("schema", "loom.nebius-development-operation.v1"),
    ("schema", "loom.nebius-development-management-operation.v2"),
    ("installation_id", "00000000-0000-0000-0000-000000000000"),
    ("installation_id", "A1111111-2222-4333-8444-555555555555"),
    ("installation_id", "not-a-uuid"),
    ("installation_id", "a1111111-2222-4333-8444-555555555556"),
    ("source_sha", "c" * 40),
    ("source_sha", "a" * 39),
    ("candidate", "c" * 40),
    ("inputs_sha256", "b" * 63),
    ("inputs_sha256", "B" * 64),
    ("inputs_sha256", "z" * 64),
    ("inputs_sha256", True),
    ("source_sha", "a" * 1025),
])
def test_changed_or_unqualified_identity_is_rejected(tmp_path, field, replacement):
    with pytest.raises(ValueError, match="^development management operation unqualified$"):
        module().validate_operation(operation(tmp_path) | {field: replacement})


@pytest.mark.parametrize("field", [
    "schema", "source_sha", "candidate", "installation_id", "namespace",
    "inputs_path", "state_dir", "anchor_dir", "inputs_sha256",
])
def test_missing_identity_field_is_rejected(tmp_path, field):
    value = operation(tmp_path)
    del value[field]
    with pytest.raises(ValueError):
        module().validate_operation(value)


def test_extra_fields_cannot_request_other_actions_or_caller_readiness(tmp_path):
    for extra in ({"action": "rollback"}, {"ready": "true"}, {"foundation_namespace": "loom-nebius-platform"}):
        with pytest.raises(ValueError):
            module().validate_operation(operation(tmp_path) | extra)


@pytest.mark.parametrize("damage", [
    "legacy-root", "foundation-root", "wrong-owner-directory", "wrong-input-name",
    "wrong-state-name", "nested-anchor", "other-anchor-id", "relative-input",
    "dot-input", "trailing-separator", "traversal", "space", "root-home",
])
def test_layout_cannot_retarget_or_alias_other_installation_state(tmp_path, damage):
    value = operation(tmp_path)
    root = Path(value["inputs_path"]).parent
    if damage in {"legacy-root", "foundation-root", "wrong-owner-directory", "root-home"}:
        replacement = {
            "legacy-root": root.parent.parent / "nebius-management",
            "foundation-root": root.parent.parent / "nebius-development" / root.name,
            "wrong-owner-directory": tmp_path / "operator/not-loom/nebius-development-management" / root.name,
            "root-home": Path("/.loom/nebius-development-management") / root.name,
        }[damage]
        value.update(inputs_path=str(replacement / "inputs.json"), state_dir=str(replacement / "state"),
            anchor_dir=str(replacement.parent.parent / "nebius-development-management-anchors" / root.name))
    elif damage == "wrong-input-name":
        value["inputs_path"] = str(root / "staging-inputs.json")
    elif damage == "wrong-state-name":
        value["state_dir"] = str(root / "retirement")
    elif damage == "nested-anchor":
        value["anchor_dir"] = str(root / "state/anchor")
    elif damage == "other-anchor-id":
        value["anchor_dir"] += "-other"
    elif damage == "relative-input":
        value["inputs_path"] = "inputs.json"
    elif damage == "dot-input":
        value["inputs_path"] = str(root) + "/./inputs.json"
    elif damage == "trailing-separator":
        value["state_dir"] += "/"
    elif damage == "traversal":
        value["inputs_path"] = str(root) + "/../inputs.json"
    elif damage == "space":
        value["inputs_path"] = str(root / "private input.json")
    with pytest.raises(ValueError, match="^development management operation unqualified$"):
        module().validate_operation(value)


@pytest.mark.parametrize("target", ["operator", "inputs_path", "state_dir", "anchor_dir"])
def test_symlinked_owner_or_input_paths_fail_without_followup_writes(tmp_path, target):
    value = operation(tmp_path)
    selected = tmp_path / "operator" if target == "operator" else Path(value[target])
    selected.parent.mkdir(parents=True, exist_ok=True)
    real = tmp_path / "retained-private-material"
    real.mkdir()
    selected.symlink_to(real, target_is_directory=True)
    with pytest.raises(ValueError, match="^development management operation unqualified$"):
        module().validate_operation(value)
    assert list(real.iterdir()) == []


@pytest.mark.parametrize("value", [None, [], {"private-input": "secret-material-marker"}])
def test_diagnostic_never_contains_private_input(value):
    with pytest.raises(ValueError, match="^development management operation unqualified$"):
        module().validate_operation(value)
