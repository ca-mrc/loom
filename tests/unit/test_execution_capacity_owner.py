"""Capacity sharing is an explicit immutable catalog binding, never inferred."""

import hashlib
import json
from pathlib import Path

import pytest

from loom.execution_contract import ExecutionTargetV1, nebius_guest_execution_class


def _ordinary():
    raw = json.loads((Path(__file__).resolve().parents[2] / "config/service-execution-topology.json").read_text())
    return ExecutionTargetV1.model_validate(raw["targets"][0])


def test_ordinary_target_bytes_stay_unchanged():
    target = _ordinary()
    assert hashlib.sha256(target.model_dump_json().encode()).hexdigest() == (
        "edb0b528182984f569e1a8abe09b5a8fbc57970fc9691c83a9387cd8cd051216")
    assert ExecutionTargetV1.model_validate({
        **target.model_dump(mode="json"), "capacity_owner_target_id": None,
    }).model_dump_json() == target.model_dump_json()


def test_guest_capacity_owner_is_explicit_and_immutable():
    owner = _ordinary()
    alias = ExecutionTargetV1.model_validate({
        **owner.model_dump(mode="json"), "target_id": owner.target_id + "-guest",
        "health_check_id": owner.health_check_id + "-guest",
        "execution_class_id": nebius_guest_execution_class().class_id,
        "capacity_owner_target_id": owner.target_id,
    })
    assert alias.capacity_owner_target_id == owner.target_id
    assert alias.namespace_name == owner.namespace_name
    assert ExecutionTargetV1.model_validate_json(alias.model_dump_json()) == alias
    with pytest.raises(ValueError):
        alias.capacity_owner_target_id = "another-target"


@pytest.mark.parametrize("damage", ["self", "nonguest"])
def test_capacity_alias_cannot_be_self_owned_or_an_ordinary_target(damage):
    owner = _ordinary()
    raw = {**owner.model_dump(mode="json"), "capacity_owner_target_id": owner.target_id}
    if damage == "self":
        raw["execution_class_id"] = nebius_guest_execution_class().class_id
    else:
        raw["target_id"] += "-alias"
    with pytest.raises(ValueError):
        ExecutionTargetV1.model_validate(raw)
