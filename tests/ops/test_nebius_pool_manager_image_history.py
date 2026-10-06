"""Bounded image correction ancestry never rewrites the original pool."""
from __future__ import annotations

import copy
import hashlib
import json
from uuid import uuid4

import pytest
from tests.ops.test_nebius_pool_startup_repair import (
    application_management_inputs as application_management_inputs,
)
from tests.ops.test_nebius_pool_startup_repair import application_material as application_material
from tests.ops.test_nebius_pool_startup_repair import build_inputs as build_inputs
from tests.ops.test_nebius_pool_startup_repair import (
    builder_cutover_inputs as builder_cutover_inputs,
)
from tests.ops.test_nebius_pool_startup_repair import checks as checks
from tests.ops.test_nebius_pool_startup_repair import cloud as cloud
from tests.ops.test_nebius_pool_startup_repair import collector_inputs as collector_inputs
from tests.ops.test_nebius_pool_startup_repair import completed_upgrade as completed_upgrade
from tests.ops.test_nebius_pool_startup_repair import cutover_inputs as cutover_inputs
from tests.ops.test_nebius_pool_startup_repair import database_guard as database_guard
from tests.ops.test_nebius_pool_startup_repair import entry_inputs as entry_inputs
from tests.ops.test_nebius_pool_startup_repair import fencing_inputs as fencing_inputs
from tests.ops.test_nebius_pool_startup_repair import historical_cutover as historical_cutover
from tests.ops.test_nebius_pool_startup_repair import installation as installation
from tests.ops.test_nebius_pool_startup_repair import management_inputs as management_inputs
from tests.ops.test_nebius_pool_startup_repair import material as material
from tests.ops.test_nebius_pool_startup_repair import platform_inputs as platform_inputs
from tests.ops.test_nebius_pool_startup_repair import prepared_repair as prepared_repair
from tests.ops.test_nebius_pool_startup_repair import private_cutover as private_cutover
from tests.ops.test_nebius_pool_startup_repair import private_upgrade as private_upgrade
from tests.ops.test_nebius_pool_startup_repair import repair
from tests.ops.test_nebius_pool_startup_repair import retirement_inputs as retirement_inputs
from tests.ops.test_nebius_pool_startup_repair import runtime_inputs as runtime_inputs
from tests.support.execution_image_admission import signed_image_admission_bundle


@pytest.fixture
def image_repair_case(prepared_repair):
    from scripts.ops.nebius_pool_manager_image_history import ManagerImageRepairBinding

    context, _, api, state, anchor = prepared_repair
    assert repair(prepared_repair)["status"] == "pool_startup_repaired_closed"
    candidate = copy.deepcopy(context.inputs.candidate)
    profile = copy.deepcopy(context.inputs.profile)
    candidate["candidate_sha"] = profile["candidate_sha"] = "d" * 40
    candidate["images"]["service"]["image_ref"] = candidate["images"]["service"]["image_ref"].split("@")[0] + "@sha256:" + "e" * 64
    profile["task_image_ref"] = candidate["images"]["service"]["image_ref"]
    profile["image_admission"] = signed_image_admission_bundle(tuple(
        profile[key] for key in ("task_image_ref", "runtime_image_ref", "agent_image_ref") if profile.get(key)
    )).model_dump(mode="json")
    publication = context.inputs.publication.model_copy(update={"candidate_id": uuid4(), "source_sha": "d" * 40})
    binding = ManagerImageRepairBinding(
        operation_id=uuid4(), source_sha="d" * 40, ordinal=1,
        original_operation_sha256=hashlib.sha256(json.dumps(context.operation, sort_keys=True, separators=(",", ":")).encode()).hexdigest(),
        inputs_sha256=context.operation["inputs_sha256"],
        closure_sha256=hashlib.sha256((state / "cutover.json").read_bytes()).hexdigest(),
        startup_sha256=hashlib.sha256((state / "startup.json").read_bytes()).hexdigest(),
        activation_sha256=hashlib.sha256((state / "activation.json").read_bytes()).hexdigest(),
        source_repair_sha256=hashlib.sha256((state / "startup-repair.json").read_bytes()).hexdigest(),
        predecessor_sha256=None, publication=publication, candidate=candidate, profile=profile,
    )
    return context, api, state, anchor, binding


def entry(fixture):
    from scripts.ops.nebius_pool_manager_image_history import manager_image_entry

    context, _, state, anchor, binding = fixture
    return manager_image_entry(context.request, binding, state=state, anchor=anchor)


def persist(value, *, complete=False, anchor_only=False):
    from scripts.ops.nebius_certificates import _atomic_json

    _atomic_json(value.marker, value.identity)
    if not anchor_only:
        _atomic_json(value.path, {**value.identity, "phases": {
            name: {"phase": "applied" if complete else "prepared",
                   "before_resource_version": str(index + 100) if complete else None}
            for index, name in enumerate(("stop", "template", "start"))
        }})


def test_image_entry_preserves_all_original_history_and_returns_fixed_stopped_targets(image_repair_case):
    context, _, state, anchor, _ = image_repair_case
    old = {path: path.read_bytes() for root in (state, anchor) for path in root.iterdir() if path.is_file()}
    value = entry(image_repair_case)
    assert value.record is None and not value.anchored
    assert [row["spec"]["replicas"] for row in value.documents] == [1, 0, 0, 1]
    images = [row["spec"]["template"]["spec"]["containers"][0]["image"] for row in value.documents]
    assert images[0] == images[1] != images[2] == images[3]
    assert value.identity["operation_id"] == str(context.inputs.installation.operation_id)
    assert old == {path: path.read_bytes() for path in old}


def test_anchored_enrollment_prefix_can_be_loaded_without_inventing_write_intent(image_repair_case):
    from scripts.ops.nebius_pool_manager_image_history import load_manager_image_chain

    value = entry(image_repair_case)
    persist(value, anchor_only=True)
    loaded = entry(image_repair_case)
    assert loaded.anchored and loaded.record is None
    context, _, state, anchor, _ = image_repair_case
    assert load_manager_image_chain(context.request, state=state, anchor=anchor) == (loaded,)
    persist(loaded)
    assert all(row["phase"] == "prepared" for row in entry(image_repair_case).record["phases"].values())


@pytest.mark.parametrize("damage", ["activation", "source_repair", "ordinal", "prior", "foreign_image", "signature"])
def test_image_entry_rejects_changed_scope_or_unqualified_history(image_repair_case, damage):
    context, api, state, anchor, binding = image_repair_case
    if damage in {"activation", "source_repair"}:
        path = state / ("activation.json" if damage == "activation" else "startup-repair.json")
        path.write_bytes(path.read_bytes() + b"\n")
    elif damage == "ordinal":
        binding = binding.model_copy(update={"ordinal": 2})
    elif damage == "prior":
        binding = binding.model_copy(update={"predecessor_sha256": "a" * 64})
    elif damage == "foreign_image":
        value = copy.deepcopy(binding.candidate)
        value["images"]["service"]["image_ref"] = "foreign/service@sha256:" + "e" * 64
        binding = binding.model_copy(update={"candidate": value})
    else:
        value = copy.deepcopy(binding.profile)
        value["image_admission"]["admissions"][0]["signature_base64"] = "AAAA"
        binding = binding.model_copy(update={"profile": value})
    with pytest.raises(ValueError, match="manager_image"):
        entry((context, api, state, anchor, binding))


def test_new_image_entry_requires_completed_tail_and_exact_predecessor_hash(image_repair_case):
    from scripts.ops.nebius_pool_manager_image_history import load_manager_image_chain

    context, api, state, anchor, binding = image_repair_case
    first = entry(image_repair_case)
    persist(first)
    candidate, profile = copy.deepcopy(binding.candidate), copy.deepcopy(binding.profile)
    candidate["candidate_sha"] = profile["candidate_sha"] = "f" * 40
    candidate["images"]["service"]["image_ref"] = candidate["images"]["service"]["image_ref"].replace("e" * 64, "f" * 64)
    profile["task_image_ref"] = candidate["images"]["service"]["image_ref"]
    profile["image_admission"] = signed_image_admission_bundle(tuple(
        profile[key] for key in ("task_image_ref", "runtime_image_ref", "agent_image_ref") if profile.get(key)
    )).model_dump(mode="json")
    next_binding = binding.model_copy(update={"operation_id": uuid4(), "ordinal": 2,
        "source_sha": "f" * 40,
        "publication": binding.publication.model_copy(update={"candidate_id": uuid4(), "source_sha": "f" * 40,
            "run_id": binding.publication.run_id + 1, "artifact_id": binding.publication.artifact_id + 1}),
        "predecessor_sha256": hashlib.sha256(first.path.read_bytes()).hexdigest(), "candidate": candidate, "profile": profile})
    with pytest.raises(ValueError, match="manager_image"):
        entry((context, api, state, anchor, next_binding))
    persist(first, complete=True)
    saved = first.path.read_bytes()
    next_binding = next_binding.model_copy(update={"predecessor_sha256": hashlib.sha256(saved).hexdigest()})
    second = entry((context, api, state, anchor, next_binding))
    assert second.documents[0] == first.documents[-1]
    persist(second)
    assert len(load_manager_image_chain(context.request, state=state, anchor=anchor)) == 2
    assert first.path.read_bytes() == saved
    first.path.write_bytes(saved + b"\n")
    with pytest.raises(ValueError, match="manager_image"):
        load_manager_image_chain(context.request, state=state, anchor=anchor)
