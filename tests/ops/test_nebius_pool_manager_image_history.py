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
    candidate["images"]["service"]["image_ref"] = candidate["images"]["service"]["image_ref"].split("@")[0] + "@sha256:" + "8" * 64
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
    candidate["images"]["service"]["image_ref"] = candidate["images"]["service"]["image_ref"].replace("8" * 64, "f" * 64)
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


@pytest.mark.parametrize("damage", ["orphan_record", "gap", "extra", "anchor_bytes", "phase_order"])
def test_image_chain_rejects_orphan_gapped_or_changed_evidence(image_repair_case, damage):
    from scripts.ops.nebius_certificates import _atomic_json
    from scripts.ops.nebius_pool_manager_image_history import load_manager_image_chain

    context, _, state, anchor, _ = image_repair_case
    value = entry(image_repair_case)
    persist(value)
    if damage == "orphan_record":
        value.marker.unlink()
    elif damage == "gap":
        value.marker.rename(anchor / value.marker.name.replace("-01.json", "-02.json"))
    elif damage == "extra":
        _atomic_json(state / "manager-image-99.json", {})
    elif damage == "anchor_bytes":
        value.marker.write_bytes(value.marker.read_bytes() + b"\n")
    else:
        record = json.loads(value.path.read_bytes())
        record["phases"]["template"] = {"phase": "intent", "before_resource_version": "100"}
        _atomic_json(value.path, record)
    with pytest.raises(ValueError, match="manager_image"):
        load_manager_image_chain(context.request, state=state, anchor=anchor)


def test_historical_image_chain_survives_activation_but_new_entry_does_not(image_repair_case):
    from scripts.ops.nebius_certificates import _atomic_json
    from scripts.ops.nebius_pool_manager_image_history import load_manager_image_chain

    context, _, state, anchor, _ = image_repair_case
    value = entry(image_repair_case)
    persist(value, complete=True)
    activation = json.loads((state / "activation.json").read_bytes())
    activation["opening"] = "intent"
    _atomic_json(state / "activation.json", activation)
    assert len(load_manager_image_chain(context.request, state=state, anchor=anchor)) == 1
    value.marker.unlink()
    value.path.unlink()
    with pytest.raises(ValueError, match="manager_image"):
        entry(image_repair_case)


class ImageAPI:
    """Only the remote CAS boundary is doubled; evidence and projections are real."""

    def __init__(self, fixture):
        self.fixture = fixture
        self.startup = fixture[1].startup
        self.calls = []
        self.failure = None
        self.pending = None
        self.drained = True

    def qualify_closed(self):
        self.startup.qualify_closed()

    def read_workload(self, key):
        return self.startup.read_workload(key)

    def manager_drained(self, key, desired):
        assert self.startup.documents[key]["spec"]["replicas"] == desired["spec"]["replicas"] == 0
        return self.drained

    def preview_repair(self, phase, before, desired):
        return copy.deepcopy(desired)

    def patch_repair(self, phase, before, desired):
        from scripts.ops.nebius_ingress_stage import _key

        current = entry(self.fixture)
        assert current.record["phases"][phase] == {
            "phase": "intent", "before_resource_version": before["metadata"]["resourceVersion"]}
        assert before == self.startup.documents[_key(before)]
        self.calls.append(phase)
        self.pending = (copy.deepcopy(before), copy.deepcopy(desired))
        if self.failure == (phase, "before"):
            raise OSError("synthetic lost response")
        self.deliver()
        if self.failure == (phase, "after"):
            raise OSError("synthetic lost response")
        return True

    def deliver(self):
        from scripts.ops.nebius_ingress_stage import _key

        before, desired = self.pending
        assert self.startup.documents[_key(before)] == before
        desired["metadata"].update(uid=before["metadata"]["uid"],
            resourceVersion=str(int(before["metadata"]["resourceVersion"]) + 1))
        self.startup.documents[_key(before)] = desired
        self.pending = None


def switch(fixture, api):
    from scripts.ops.nebius_pool_manager_image_stage import repair_manager_image

    context, _, state, anchor, binding = fixture
    return repair_manager_image(request=context.request, binding=binding, api=api, state_dir=state, anchor_dir=anchor)


@pytest.mark.parametrize("phase", ["stop", "template", "start"])
@pytest.mark.parametrize("loss", ["before", "after"])
def test_manager_image_switch_reconciles_lost_cas_without_duplicate_write(image_repair_case, phase, loss):
    api = ImageAPI(image_repair_case)
    api.failure = (phase, loss)
    result = switch(image_repair_case, api)
    if loss == "before":
        assert result["status"] == "pending_manager_image_outcome"
        calls = list(api.calls)
        assert switch(image_repair_case, api)["status"] == "pending_manager_image_outcome"
        assert api.calls == calls
        api.deliver()
    assert switch(image_repair_case, api)["status"] == "pool_manager_image_repaired_closed"
    assert api.calls == ["stop", "template", "start"]
    assert all(row["phase"] == "applied" for row in entry(image_repair_case).record["phases"].values())


def test_manager_image_switch_waits_for_actual_drain(image_repair_case):
    api = ImageAPI(image_repair_case)
    api.drained = False
    assert switch(image_repair_case, api)["status"] == "pending_manager_image_drain"
    assert api.calls == ["stop"]
    assert switch(image_repair_case, api)["status"] == "pending_manager_image_drain"
    api.drained = True
    assert switch(image_repair_case, api)["status"] == "pool_manager_image_repaired_closed"
    assert api.calls == ["stop", "template", "start"]


@pytest.mark.parametrize("interrupt_at", [1, 2, 3, 4, 5, 6, 7, 8])
def test_manager_image_switch_recovers_each_local_persistence_boundary(image_repair_case, monkeypatch, interrupt_at):
    from scripts.ops import nebius_certificates as private_state

    api = ImageAPI(image_repair_case)
    save = private_state._atomic_json
    writes = 0

    def interrupted(path, value):
        nonlocal writes
        save(path, value)
        writes += 1
        if writes == interrupt_at:
            raise OSError("synthetic process interruption after persistence")

    with monkeypatch.context() as scoped:
        scoped.setattr(private_state, "_atomic_json", interrupted)
        with pytest.raises(ValueError, match="manager_image"):
            switch(image_repair_case, api)
    # A durable intent with no sent write cannot be disproved by same-version
    # readback. It remains pending for cancellation/fencing, never resent.
    result = switch(image_repair_case, api)
    if interrupt_at in {3, 5, 7}:
        assert result["status"] == "pending_manager_image_outcome"
        assert len(api.calls) == (interrupt_at - 3) // 2
    else:
        assert result["status"] == "pool_manager_image_repaired_closed"
        assert api.calls == ["stop", "template", "start"]


def test_prepared_image_correction_blocks_activation_even_with_ready_old_runtime(image_repair_case):
    from scripts.ops.nebius_pool_activation_stage import advance_pool_activation

    context, remote, state, anchor, _ = image_repair_case
    persist(entry(image_repair_case))
    remote.activation.ready = True
    with pytest.raises(ValueError):
        advance_pool_activation(request=context.request, api=remote.activation, state_dir=state, anchor_dir=anchor)
    assert remote.activation.calls == [] and remote.activation.mode == "closed"


def test_completed_image_correction_supports_runtime_activation_completion_and_refresh(image_repair_case):
    from types import SimpleNamespace

    from scripts.ops.nebius_ingress_stage import _key
    from scripts.ops.nebius_pool_activation_stage import advance_pool_activation
    from scripts.ops.nebius_pool_completion import complete_pool_cutover, load_pool_completion
    from scripts.ops.nebius_pool_predecessor import PoolPredecessorV1, load_completed_pool
    from scripts.ops.nebius_pool_startup import closed_startup_documents
    from scripts.ops.nebius_pool_startup_live import HTTPSPoolStartupAPI

    context, remote, state, anchor, binding = image_repair_case
    assert switch(image_repair_case, ImageAPI(image_repair_case))["status"] == "pool_manager_image_repaired_closed"
    closed, targets = closed_startup_documents(context.request, state_dir=state, anchor_dir=anchor)
    adapter = SimpleNamespace(request=context.request, state=state, anchor=anchor, closed=closed,
        targets=targets, _scope=lambda: None, read_workload=remote.read_workload)
    actual = HTTPSPoolStartupAPI._started_workloads(adapter)
    assert actual[_key(context.request.manager)]["spec"]["template"]["spec"]["containers"][0]["image"] == binding.candidate["images"]["service"]["image_ref"]
    remote.activation.ready = True
    assert advance_pool_activation(request=context.request, api=remote.activation,
        state_dir=state, anchor_dir=anchor)["status"] == "pool_activation_complete"
    result = complete_pool_cutover(request=context.request, api=remote.activation, state_dir=state, anchor_dir=anchor)
    completed = load_pool_completion(request=context.request, state_dir=state, anchor_dir=anchor,
        completion_sha256=result["completion_sha256"])
    value = entry(image_repair_case)
    assert {value.path, value.marker, state / "startup-repair.json"} <= set(completed.history)
    predecessor = PoolPredecessorV1(operation=context.operation, completion_sha256=result["completion_sha256"])
    loaded = load_completed_pool(predecessor, original=context.original)
    assert loaded.active["spec"]["template"]["spec"]["containers"][0]["image"] == binding.candidate["images"]["service"]["image_ref"]
    assert loaded.deployment.installation.applications.shared.runtime_profile_json == context.predecessor.deployment.installation.applications.shared.runtime_profile_json


@pytest.mark.parametrize("phase", ["stop", "template", "start"])
@pytest.mark.parametrize("late_commit", [False, True])
def test_cancellation_fences_image_cas_before_successor_shutdown(image_repair_case, phase, late_commit):
    from scripts.ops.nebius_ingress_stage import _key
    from scripts.ops.nebius_pool_activation_stage import advance_pool_activation
    from scripts.ops.nebius_pool_shutdown import stop_pool_successors
    from scripts.ops.nebius_pool_startup_fence import fence_pool_startup
    from tests.ops.test_nebius_pool_shutdown import ShutdownAPI

    context, remote, state, anchor, _ = image_repair_case
    image_api = ImageAPI(image_repair_case)
    image_api.failure = (phase, "before")
    assert switch(image_repair_case, image_api)["status"] == "pending_manager_image_outcome"
    old_version = image_api.pending[0]["metadata"]["resourceVersion"]
    if late_commit:
        image_api.deliver()
    api = ShutdownAPI((context.request, None, None, remote.startup, None, state.parent))
    api.state = state
    assert advance_pool_activation(request=context.request, api=api, state_dir=state,
        anchor_dir=anchor, cancel=True)["status"] == "pool_activation_cancelled"
    assert fence_pool_startup(request=context.request, api=api, state_dir=state,
        anchor_dir=anchor)["status"] == "startup_writes_fenced"
    assert api.read_workload(_key(context.request.manager))["metadata"]["resourceVersion"] != old_version
    assert api.fence_calls == ([] if late_commit else [_key(context.request.manager)])
    assert stop_pool_successors(request=context.request, api=api, state_dir=state,
        anchor_dir=anchor)["status"] == "pool_successors_stopped"
    with pytest.raises(ValueError):
        switch(image_repair_case, image_api)
