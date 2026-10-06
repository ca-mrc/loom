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


def runtime_binding(fixture, target, predecessor=None, *, image_digit="9"):
    from scripts.ops import nebius_pool_manager_image_history as history

    # A missing versioned binding is an explicit unsupported behavior, not a
    # fixture/import error; the retained legacy model must stay strict.
    assert hasattr(history, "RuntimeImageRepairBinding"), "targeted image corrections are unsupported"
    binding = fixture[-1]
    data = binding.model_dump(mode="json")
    data.update(schema_version="loom.nebius-pool-runtime-image-binding.v2", target=target,
        operation_id=str(uuid4()), ordinal=1 if predecessor is None else predecessor.binding.ordinal + 1,
        predecessor_sha256=None if predecessor is None else hashlib.sha256(predecessor.path.read_bytes()).hexdigest())
    component = "execution_actuator" if target == "collector" else "service"
    image = data["candidate"]["images"][component]["image_ref"]
    data["candidate"]["images"][component]["image_ref"] = image.split("@")[0] + "@sha256:" + image_digit * 64
    if component == "service":
        data["profile"]["task_image_ref"] = data["candidate"]["images"][component]["image_ref"]
        data["profile"]["image_admission"] = signed_image_admission_bundle(tuple(
            data["profile"][key] for key in ("task_image_ref", "runtime_image_ref", "agent_image_ref")
            if data["profile"].get(key))).model_dump(mode="json")
    return history.RuntimeImageRepairBinding.model_validate(data)


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
            for index, name in enumerate(("isolate", "stop", "template", "start"))
        }})


def test_image_entry_preserves_all_original_history_and_returns_fixed_stopped_targets(image_repair_case):
    context, _, state, anchor, _ = image_repair_case
    old = {path: path.read_bytes() for root in (state, anchor) for path in root.iterdir() if path.is_file()}
    value = entry(image_repair_case)
    assert value.record is None and not value.anchored
    assert [row["spec"]["replicas"] for row in value.documents] == [1, 1, 0, 0, 1]
    images = [row["spec"]["template"]["spec"]["containers"][0]["image"] for row in value.documents]
    assert images[0] == images[1] == images[2] != images[3] == images[4]
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


@pytest.mark.parametrize("target,kind,component", [
    ("collector", "CronJob", "execution_actuator"), ("gateway", "Deployment", "service"),
])
def test_targeted_image_entry_uses_retained_workload_and_exact_component(image_repair_case, target, kind, component):
    from scripts.ops.nebius_ingress_stage import _key
    from scripts.ops.nebius_pool_manager_image_history import ManagerImageRepairBinding
    from scripts.ops.nebius_pool_startup import closed_startup_documents, startup_workload_options

    context, api, state, anchor, _ = image_repair_case
    first = entry(image_repair_case)
    persist(first, complete=True)
    retained = first.path.read_bytes(), first.marker.read_bytes()
    binding = runtime_binding(image_repair_case, target, first)
    with pytest.raises(ValueError):
        ManagerImageRepairBinding.model_validate(binding.model_dump())
    value = entry((context, api, state, anchor, binding))
    assert value.documents[0]["kind"] == kind
    _, targets = closed_startup_documents(context.request, state_dir=state, anchor_dir=anchor)
    key = _key(value.documents[0])
    assert key != _key(context.request.manager)
    assert value.documents[0] == targets[key]
    assert value.documents[0]["metadata"]["name"] == (
        "loom-execution-capacity-collector" if target == "collector" else "loom-pool-gateway")
    desired = copy.deepcopy(value.documents[0])
    pod = (desired["spec"]["template"]["spec"] if kind == "Deployment"
        else desired["spec"]["jobTemplate"]["spec"]["template"]["spec"])
    for container in (*pod["containers"], *pod.get("initContainers", [])):
        container["image"] = binding.candidate["images"][component]["image_ref"]
    assert value.documents[-1] == desired
    field = "suspend" if kind == "CronJob" else "replicas"
    assert [row["spec"][field] for row in value.documents] == (
        [False, False, True, True, False] if kind == "CronJob" else [1, 1, 0, 0, 1])
    persist(value, complete=True)
    options = startup_workload_options(context.request, state_dir=state, anchor_dir=anchor)
    assert options[key] == (desired,)
    assert options[_key(context.request.manager)] == (first.documents[-1],)
    assert retained == (first.path.read_bytes(), first.marker.read_bytes())


def test_mixed_image_history_folds_each_key_and_fences_only_pending_tail(image_repair_case):
    from scripts.ops.nebius_ingress_stage import _key
    from scripts.ops.nebius_pool_manager_image_history import load_manager_image_chain
    from scripts.ops.nebius_pool_startup import (
        _startup_record,
        closed_startup_documents,
        startup_workload_options,
    )
    from scripts.ops.nebius_pool_startup_fence import _fence_sources

    context, api, state, anchor, _ = image_repair_case
    manager = entry(image_repair_case)
    persist(manager, complete=True)
    collector = entry((context, api, state, anchor, runtime_binding(image_repair_case, "collector", manager)))
    persist(collector, complete=True)
    gateway = entry((context, api, state, anchor, runtime_binding(image_repair_case, "gateway", collector)))
    persist(gateway, complete=True)
    last = entry((context, api, state, anchor, runtime_binding(image_repair_case, "collector", gateway, image_digit="a")))
    assert last.documents[0] == collector.documents[-1]
    persist(last)
    options = startup_workload_options(context.request, state_dir=state, anchor_dir=anchor)
    assert options[_key(manager.documents[0])] == (manager.documents[-1],)
    assert options[_key(gateway.documents[0])] == (gateway.documents[-1],)
    assert options[_key(collector.documents[0])] == (collector.documents[-1],)
    closed, targets = closed_startup_documents(context.request, state_dir=state, anchor_dir=anchor)
    _, startup = _startup_record(context.request, state=state, anchor=anchor, closed=closed, targets=targets)
    sources, history = _fence_sources(context.request, state=state, anchor=anchor,
        closed=closed, targets=targets, startup=startup)
    for completed in (manager, collector, gateway):
        assert sources[_key(completed.documents[0])] == (None, (completed.documents[-1],))
    assert history["manager_image_sha256"] is not None
    assert len(load_manager_image_chain(context.request, state=state, anchor=anchor)) == 4


def test_targeted_image_correction_rejects_first_enrollment(image_repair_case):
    binding = runtime_binding(image_repair_case, "collector")
    with pytest.raises(ValueError, match="manager_image"):
        entry((*image_repair_case[:-1], binding))


@pytest.mark.parametrize("damage", ["unknown_target", "unknown_version", "foreign_registry", "same_image"])
def test_targeted_image_correction_rejects_unqualified_target(image_repair_case, damage):
    from scripts.ops.nebius_pool_startup import closed_startup_documents

    context, api, state, anchor, _ = image_repair_case
    first = entry(image_repair_case)
    persist(first, complete=True)
    binding = runtime_binding(image_repair_case, "collector", first)
    if damage in {"unknown_target", "unknown_version"}:
        binding = binding.model_copy(update={"target" if damage == "unknown_target" else "schema_version": "unknown"})
    else:
        candidate = copy.deepcopy(binding.candidate)
        if damage == "foreign_registry":
            candidate["images"]["execution_actuator"]["image_ref"] = "foreign/collector@sha256:" + "a" * 64
        else:
            _, targets = closed_startup_documents(context.request, state_dir=state, anchor_dir=anchor)
            collector, = (row for row in targets.values() if row["kind"] == "CronJob")
            candidate["images"]["execution_actuator"]["image_ref"] = collector["spec"]["jobTemplate"]["spec"]["template"]["spec"]["containers"][0]["image"]
        binding = binding.model_copy(update={"candidate": candidate})
    with pytest.raises(ValueError, match="manager_image"):
        entry((context, api, state, anchor, binding))


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
        field = "suspend" if desired["kind"] == "CronJob" else "replicas"
        assert self.startup.documents[key]["spec"][field] == desired["spec"][field]
        assert desired["spec"][field] == (True if field == "suspend" else 0)
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


def selected_image_case(fixture, target):
    if target == "manager":
        return fixture
    assert switch(fixture, ImageAPI(fixture))["status"] == "pool_manager_image_repaired_closed"
    return (*fixture[:-1], runtime_binding(fixture, target, entry(fixture)))


@pytest.mark.parametrize("target", ["manager", "collector", "gateway"])
@pytest.mark.parametrize("phase", ["isolate", "stop", "template", "start"])
@pytest.mark.parametrize("loss", ["before", "after"])
def test_manager_image_switch_reconciles_lost_cas_without_duplicate_write(image_repair_case, phase, loss, target):
    image_repair_case = selected_image_case(image_repair_case, target)
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
    assert api.calls == ["isolate", "stop", "template", "start"]
    assert all(row["phase"] == "applied" for row in entry(image_repair_case).record["phases"].values())


def test_manager_image_switch_waits_for_actual_drain(image_repair_case):
    api = ImageAPI(image_repair_case)
    api.drained = False
    assert switch(image_repair_case, api)["status"] == "pending_manager_image_drain"
    assert api.calls == ["isolate", "stop"]
    assert switch(image_repair_case, api)["status"] == "pending_manager_image_drain"
    api.drained = True
    assert switch(image_repair_case, api)["status"] == "pool_manager_image_repaired_closed"
    assert api.calls == ["isolate", "stop", "template", "start"]


@pytest.mark.parametrize("interrupt_at", [1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
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
    if interrupt_at in {3, 5, 7, 9}:
        assert result["status"] == "pending_manager_image_outcome"
        assert len(api.calls) == (interrupt_at - 3) // 2
    else:
        assert result["status"] == "pool_manager_image_repaired_closed"
        assert api.calls == ["isolate", "stop", "template", "start"]


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


@pytest.mark.parametrize("target", ["manager", "collector", "gateway"])
@pytest.mark.parametrize("phase", ["isolate", "stop", "template", "start"])
@pytest.mark.parametrize("late_commit", [False, True])
def test_cancellation_fences_image_cas_before_successor_shutdown(image_repair_case, phase, late_commit, target):
    from scripts.ops.nebius_ingress_stage import _key
    from scripts.ops.nebius_pool_activation_stage import advance_pool_activation
    from scripts.ops.nebius_pool_shutdown import stop_pool_successors
    from scripts.ops.nebius_pool_startup_fence import fence_pool_startup
    from tests.ops.test_nebius_pool_shutdown import ShutdownAPI

    image_repair_case = selected_image_case(image_repair_case, target)
    context, remote, state, anchor, _ = image_repair_case
    key = _key(entry(image_repair_case).documents[0])
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
    assert api.read_workload(key)["metadata"]["resourceVersion"] != old_version
    assert api.fence_calls == ([] if late_commit else [key])
    assert stop_pool_successors(request=context.request, api=api, state_dir=state,
        anchor_dir=anchor)["status"] == "pool_successors_stopped"
    with pytest.raises(ValueError):
        switch(image_repair_case, image_api)


@pytest.mark.parametrize("target", ["manager", "collector", "gateway"])
@pytest.mark.parametrize("loss", [None, "before", "after"])
def test_https_image_switch_sends_only_exact_uid_version_metadata_spec_cas(image_repair_case, loss, target):
    from types import SimpleNamespace

    import httpx
    from scripts.ops.nebius_ingress_stage import _key
    from scripts.ops.nebius_management_transport import ManagementKubernetesTransport
    from scripts.ops.nebius_pool_manager_image_live import HTTPSPoolManagerImageAPI
    from scripts.ops.nebius_pool_runtime_image import runtime_image_template

    image_repair_case = selected_image_case(image_repair_case, target)
    context, remote, state, anchor, binding = image_repair_case
    original = entry(image_repair_case).documents[0]
    key = _key(original)
    cron = original["kind"] == "CronJob"
    prefix = "/apis/batch/v1/namespaces/" if cron else "/apis/apps/v1/namespaces/"
    workload = prefix + original["metadata"]["namespace"] + ("/cronjobs/" if cron else "/deployments/") + original["metadata"]["name"]
    template_path = "/spec/jobTemplate/spec/template" if cron else "/spec/template"
    field = "suspend" if cron else "replicas"
    component = "execution_actuator" if cron else "service"
    writes = []

    def respond(message):
        path = message.url.path
        if message.method == "GET":
            if path == workload:
                actual = remote.startup.documents[key]
                actual["metadata"]["generation"] = 1
                actual["status"] = {} if cron else {"observedGeneration": 1, "replicas": actual["spec"]["replicas"]}
                return httpx.Response(200, json=actual)
            assert path.endswith("/jobs" if cron else "/replicasets") or path.endswith("/pods")
            return httpx.Response(200, json={"apiVersion": "v1" if path.endswith("/pods") else "batch/v1" if cron else "apps/v1",
                "kind": "PodList" if path.endswith("/pods") else "JobList" if cron else "ReplicaSetList",
                "metadata": {"resourceVersion": "100"}, "items": []})
        assert message.method == "PATCH" and path == workload
        before = remote.read_workload(key)
        patches = json.loads(message.content)
        assert patches[:4] == [
            {"op": "test", "path": "/metadata/uid", "value": before["metadata"]["uid"]},
            {"op": "test", "path": "/metadata/resourceVersion", "value": before["metadata"]["resourceVersion"]},
            {"op": "test", "path": "/metadata", "value": before["metadata"]},
            {"op": "test", "path": "/spec", "value": before["spec"]}]
        change, desired = patches[4], copy.deepcopy(before)
        if change["path"] == template_path:
            phase = "template"
            assert before["spec"][field] == (True if cron else 0)
            expected = copy.deepcopy(runtime_image_template(before))
            for container in (*expected["spec"]["containers"], *expected["spec"]["initContainers"]):
                container["image"] = binding.candidate["images"][component]["image_ref"]
            assert change["value"] == expected
            if cron:
                desired["spec"]["jobTemplate"]["spec"]["template"] = expected
            else:
                desired["spec"]["template"] = expected
        elif change["path"] == "/metadata/annotations":
            phase = "isolate"
            assert change["op"] == "add" and len(patches) == 5
            desired["metadata"]["annotations"] = {**before["metadata"].get("annotations", {}),
                "loom.nebius/manager-image-repair": str(binding.operation_id)}
            assert change["value"] == desired["metadata"]["annotations"]
        else:
            assert change["path"] == "/spec/" + field
            phase = "stop" if change["value"] == (True if cron else 0) else "start"
            desired["spec"][field] = change["value"]
        if phase == "start":
            assert patches[5:] == [{"op": "remove", "path": "/metadata/annotations/loom.nebius~1manager-image-repair"}]
            del desired["metadata"]["annotations"]["loom.nebius/manager-image-repair"]
        elif phase != "isolate":
            assert len(patches) == 5 and change["op"] == "replace"
        if message.url.params:
            assert dict(message.url.params) == {"dryRun": "All"}
        else:
            assert entry(image_repair_case).record["phases"][phase]["phase"] == "intent"
            writes.append(phase)
            if phase == "template" and loss == "before":
                raise httpx.ReadTimeout("synthetic lost request")
            desired["metadata"]["resourceVersion"] = str(int(before["metadata"]["resourceVersion"]) + 1)
            remote.startup.documents[key] = desired
            if phase == "template" and loss == "after":
                raise httpx.ReadTimeout("synthetic lost response")
        return httpx.Response(200, json=desired)

    class TransportOnlyImage(HTTPSPoolManagerImageAPI):
        def qualify_closed(self):
            self._qualify_binding()
            remote.qualify_closed()

    with httpx.Client(base_url="https://kubernetes.invalid", transport=httpx.MockTransport(respond)) as client:
        parent = SimpleNamespace(request=context.request, state_dir=state, anchor_dir=anchor,
            client=client, _scope=lambda: None, error_type=ValueError)
        parent._request = lambda method, path, **kwargs: ManagementKubernetesTransport._request(parent, method, path, **kwargs)
        api = TransportOnlyImage(parent=parent, binding=binding)
        result = switch(image_repair_case, api)
        assert result["status"] == ("pending_manager_image_outcome" if loss == "before" else "pool_manager_image_repaired_closed")
        assert switch(image_repair_case, api) == result
        assert writes == (["isolate", "stop", "template"] if loss == "before" else ["isolate", "stop", "template", "start"])


def test_https_cancellation_fences_image_intent_not_completed_source_repair(image_repair_case):
    from types import SimpleNamespace

    import httpx
    from scripts.ops.nebius_ingress_stage import _key
    from scripts.ops.nebius_pool_activation_live import HTTPSPoolActivationAPI
    from scripts.ops.nebius_pool_activation_stage import advance_pool_activation
    from scripts.ops.nebius_pool_startup import closed_startup_documents
    from scripts.ops.nebius_pool_startup_fence import fence_pool_startup
    from tests.ops.test_nebius_pool_shutdown import ShutdownAPI

    context, remote, state, anchor, _ = image_repair_case
    image_api = ImageAPI(image_repair_case)
    image_api.failure = ("template", "before")
    assert switch(image_repair_case, image_api)["status"] == "pending_manager_image_outcome"
    api = ShutdownAPI((context.request, None, None, remote.startup, None, state.parent))
    api.state = state
    assert advance_pool_activation(request=context.request, api=api, state_dir=state,
        anchor_dir=anchor, cancel=True)["status"] == "pool_activation_cancelled"
    key = _key(context.request.manager)
    writes = []

    def respond(message):
        before = remote.read_workload(key)
        patches = json.loads(message.content)
        assert message.method == "PATCH" and message.url.path.endswith("/deployments/loom-service")
        assert patches[:4] == [
            {"op": "test", "path": "/metadata/uid", "value": before["metadata"]["uid"]},
            {"op": "test", "path": "/metadata/resourceVersion", "value": before["metadata"]["resourceVersion"]},
            {"op": "test", "path": "/metadata", "value": before["metadata"]},
            {"op": "test", "path": "/spec", "value": before["spec"]}]
        assert len(patches) == 5 and patches[-1]["path"] == "/metadata/annotations"
        desired = copy.deepcopy(before)
        desired["metadata"]["annotations"] = patches[-1]["value"]
        if not message.url.params:
            writes.append(patches)
            desired["metadata"]["resourceVersion"] = str(int(before["metadata"]["resourceVersion"]) + 1)
            remote.startup.documents[key] = desired
        return httpx.Response(200, json=desired)

    with httpx.Client(base_url="https://kubernetes.invalid", transport=httpx.MockTransport(respond)) as client:
        closed, targets = closed_startup_documents(context.request, state_dir=state, anchor_dir=anchor)
        adapter = SimpleNamespace(request=context.request, state=state, anchor=anchor, closed=closed, targets=targets,
            parent=SimpleNamespace(client=client), _scope=lambda: None, _path=lambda key: "/deployments/loom-service",
            verify_retained=api.verify_retained, pool_state=api.pool_state, guard_state=api.guard_state)
        api.preview_startup_fence = lambda key, before, desired: (copy.deepcopy(desired)
            if HTTPSPoolActivationAPI._startup_fence_patch(adapter, key, before, desired, preview=True) else None)
        api.fence_startup = lambda key, before, desired: HTTPSPoolActivationAPI._startup_fence_patch(
            adapter, key, before, desired, preview=False)
        assert fence_pool_startup(request=context.request, api=api, state_dir=state,
            anchor_dir=anchor)["status"] == "startup_writes_fenced"
        assert len(writes) == 1


def test_bound_image_operation_completes_original_pool_and_unbound_install_refuses(image_repair_case, monkeypatch):
    from types import SimpleNamespace

    from scripts.ops import nebius_pool_operation as target

    context, remote, state, anchor, binding = image_repair_case
    api = ImageAPI(image_repair_case)
    parent = SimpleNamespace(request=context.request, state_dir=state, anchor_dir=anchor, refresh=None)
    monkeypatch.setattr(target, "HTTPSPoolManagerImageAPI", lambda **kwargs: api, raising=False)
    monkeypatch.setattr(target, "HTTPSPoolActivationAPI", lambda **kwargs: remote.activation)
    remote.activation.ready = True
    persist(entry(image_repair_case))
    with pytest.raises(target.PoolOperationError):
        target.run_pool_operation(parent=parent, tokens=context.tokens, action="install")
    assert remote.activation.calls == []
    result = target.run_pool_operation(parent=parent, tokens=context.tokens, action="install", image_binding=binding)
    assert result["status"] == "pool_cutover_completed" and result["outcome"] == "global"
    assert result["operation_id"] == context.operation["operation_id"]
    assert api.calls == ["isolate", "stop", "template", "start"]


@pytest.fixture
def private_image_repair(image_repair_case, monkeypatch):
    from pathlib import Path

    from scripts.ops import nebius_certificates as private_state
    from scripts.ops import nebius_pool_image_entry as target
    from tests.ops.test_nebius_pool_repair_authority import repair_operation
    from tests.ops.test_nebius_pool_startup_repair import save_private

    context, _, state, _, binding = image_repair_case
    root = state.parent.parent.parent
    operation = repair_operation(root.parent, "v2")
    directory = root / "pool-repair" / str(binding.operation_id)
    operation.update(operation_id=str(binding.operation_id), source_sha=binding.source_sha, candidate=binding.source_sha,
        original_operation_id=context.operation["operation_id"], namespace=context.operation["namespace"],
        installation_id=context.operation["installation_id"], state_dir=str(directory / "state"),
        anchor_dir=str(directory / "anchor"), inputs_path=str(directory / "inputs.json"))
    private_state._private_directory(directory.parent)
    private_state._private_directory(directory)
    payload = {"schema_version": "loom.nebius-pool-manager-image-private-inputs.v1",
        "original_operation": context.operation, "binding": binding.model_dump(mode="json")}
    save_private(operation, payload)
    proof = Path(operation["inputs_path"]).with_name("manager-schema.json")
    private_state._atomic_json(proof, {"schema": "loom.nebius-manager-schema.v1", "source_sha": binding.source_sha,
        "revision": "0174"})
    monkeypatch.setattr(target, "SCHEMA_PROOF_PATH", proof)
    return operation, payload, proof, context


def test_image_entry_binds_original_history_and_packaged_schema_before_connection(private_image_repair):
    from pathlib import Path

    from scripts.ops.nebius_pool_image_entry import load_image_repair_inputs

    operation, _, _, original = private_image_repair
    before = Path(original.operation["inputs_path"]).read_bytes()
    context = load_image_repair_inputs(operation)
    assert context.original == original
    assert context.operation == operation
    assert Path(original.operation["inputs_path"]).read_bytes() == before


@pytest.mark.parametrize("damage", [None, "old_operation", "old_private", "legacy_binding"])
def test_targeted_image_private_entry_requires_matching_operation_and_binding_versions(
        private_image_repair, image_repair_case, damage):
    from scripts.ops.nebius_management_entry import EntryError
    from scripts.ops.nebius_pool_image_entry import load_image_repair_inputs
    from tests.ops.test_nebius_pool_startup_repair import save_private

    operation, payload, _, original = private_image_repair
    first = entry(image_repair_case)
    persist(first, complete=True)
    binding = runtime_binding(image_repair_case, "collector", first)
    payload.update(schema_version="loom.nebius-pool-runtime-image-private-inputs.v2",
        binding=binding.model_dump(mode="json"))
    operation.update(schema="loom.nebius-pool-startup-repair-operation.v3", operation_id=str(binding.operation_id))
    # Fresh authority has its own directory; never overwrite the original image
    # operation. This fixture has not enrolled its private operation externally.
    previous_id = str(image_repair_case[-1].operation_id)
    for field in ("inputs_path", "state_dir", "anchor_dir"):
        operation[field] = operation[field].replace(previous_id, str(binding.operation_id))
    from pathlib import Path

    from scripts.ops import nebius_certificates as private_state

    private_state._private_directory(Path(operation["inputs_path"]).parent)
    if damage == "old_operation":
        operation["schema"] = "loom.nebius-pool-startup-repair-operation.v2"
    elif damage == "old_private":
        payload["schema_version"] = "loom.nebius-pool-manager-image-private-inputs.v1"
    elif damage == "legacy_binding":
        del payload["binding"]["schema_version"], payload["binding"]["target"]
    save_private(operation, payload)
    if damage is not None:
        with pytest.raises(EntryError):
            load_image_repair_inputs(operation)
    else:
        context = load_image_repair_inputs(operation)
        assert context.original == original and context.inputs.binding.target == "collector"


@pytest.mark.parametrize("damage", ["schema_source", "schema_revision", "original_hash", "inputs_hash", "candidate", "closure"])
def test_image_entry_rejects_unqualified_source_schema_or_parent(private_image_repair, damage):
    from scripts.ops import nebius_certificates as private_state
    from scripts.ops.nebius_management_entry import EntryError
    from scripts.ops.nebius_pool_image_entry import load_image_repair_inputs
    from tests.ops.test_nebius_pool_startup_repair import save_private

    operation, payload, proof, _ = private_image_repair
    if damage.startswith("schema_"):
        content = json.loads(proof.read_bytes())
        content["source_sha" if damage == "schema_source" else "revision"] = "a" * 40 if damage == "schema_source" else "0175"
        private_state._atomic_json(proof, content)
    elif damage == "candidate":
        payload["binding"]["candidate"]["candidate_sha"] = "f" * 40
    else:
        field = {"original_hash": "original_operation_sha256", "inputs_hash": "inputs_sha256", "closure": "closure_sha256"}[damage]
        payload["binding"][field] = "a" * 64
    save_private(operation, payload)
    with pytest.raises(EntryError):
        load_image_repair_inputs(operation)


@pytest.mark.parametrize("damage", [None, "failed_run", "missing_gate", "tampered", "candidate_bytes", "private_drift"])
def test_image_repair_resolves_real_protected_catalog_before_operator_connection(private_image_repair, monkeypatch, damage):
    from contextlib import contextmanager
    from pathlib import Path
    from types import SimpleNamespace

    from scripts.ops import nebius_pool_image_entry as target
    from scripts.ops.nebius_pool_operation import PoolOperationError
    from tests.ops.test_nebius_pool_cutover_entry import publication_http, save_private

    operation, payload, _, original = private_image_repair
    selected = {key: payload["binding"][key] for key in ("publication", "candidate", "profile")}
    stub = {"inputs_path": str(Path(operation["inputs_path"]).with_name("publication-fixture.json"))}
    responses, wire = publication_http.__wrapped__((stub, selected, original.original), monkeypatch)
    save_private(operation, payload)
    if damage == "failed_run":
        responses["actions/runs/100/attempts/1"]["conclusion"] = "failure"
    elif damage == "missing_gate":
        responses["commits/" + "b" * 40 + "/check-runs"]["check_runs"].pop()
    elif damage == "tampered":
        wire["payload"] += b"tampered"
    elif damage == "candidate_bytes":
        payload["binding"]["candidate"]["source_archive_sha256"] = "sha256:" + "a" * 64
        save_private(operation, payload)
    elif damage == "private_drift":
        wire["during_read"] = lambda: Path(operation["inputs_path"]).write_bytes(b"{}")
    context = target.load_image_repair_inputs(operation)
    connections = []

    @contextmanager
    def connected(selected):
        assert selected == original
        connections.append("opened")
        yield SimpleNamespace(checks=object(), guards=SimpleNamespace(telemetry_report=lambda: {}))

    monkeypatch.setattr(target, "connected_pool_api", connected)
    monkeypatch.setattr(target, "run_pool_operation", lambda **kwargs: {
        "status": "preflight_qualified", "operation_id": original.operation["operation_id"]})
    if damage is None:
        result = target.execute_image_repair(context, "preflight")
        assert result["operation_id"] == operation["operation_id"]
        assert result["original_operation_id"] == original.operation["operation_id"]
        assert connections == ["opened"]
    else:
        with pytest.raises((PoolOperationError, target.EntryError)):
            target.execute_image_repair(context, "preflight")
        assert connections == []


def test_legacy_activation_cannot_open_while_an_image_stop_may_arrive_late(image_repair_case, monkeypatch):
    from scripts.ops import nebius_pool_activation_stage as activation
    from scripts.ops import nebius_pool_manager_image_history as images

    context, remote, state, anchor, _ = image_repair_case
    api = ImageAPI(image_repair_case)
    api.failure = ("stop", "before")
    assert switch(image_repair_case, api)["status"] == "pending_manager_image_outcome"
    remote.activation.ready = True
    # Old v1 tooling has no image-history hooks, but does perform exact retained
    # Deployment checks and serialize dispatch. Keep those checks real here.
    with monkeypatch.context() as legacy:
        legacy.setattr(images, "load_manager_image_chain", lambda *args, **kwargs: ())
        with pytest.raises(ValueError):
            activation.advance_pool_activation(request=context.request, api=remote.activation,
                state_dir=state, anchor_dir=anchor)
    assert remote.activation.calls == [] and remote.activation.mode == "closed"


def test_late_isolation_after_legacy_opening_cannot_lead_to_a_manager_stop(image_repair_case, monkeypatch):
    from scripts.ops import nebius_pool_manager_image_history as images
    from scripts.ops.nebius_ingress_stage import _key
    from scripts.ops.nebius_pool_activation_stage import advance_pool_activation

    context, remote, state, anchor, _ = image_repair_case
    api = ImageAPI(image_repair_case)
    api.failure = ("isolate", "before")
    assert switch(image_repair_case, api)["status"] == "pending_manager_image_outcome"
    with monkeypatch.context() as legacy:
        legacy.setattr(images, "load_manager_image_chain", lambda *args, **kwargs: ())
        remote.activation.ready = True
        assert advance_pool_activation(request=context.request, api=remote.activation,
            state_dir=state, anchor_dir=anchor)["status"] == "pool_activation_complete"
    api.deliver()
    with pytest.raises(ValueError):
        switch(image_repair_case, api)
    assert api.calls == ["isolate"]
    assert remote.read_workload(_key(context.request.manager))["spec"]["replicas"] == 1


@pytest.mark.parametrize(("moment", "loss"), [("prepared", None), ("intent", None), ("prepared_fence", None), ("late_isolate", None),
    ("late_shutdown", None), ("late_shutdown", "before"), ("late_shutdown", "after"),
    ("late_shutdown", "conflict"), ("stopped", None)])
@pytest.mark.timeout(420)
def test_legacy_fence_resumes_image_enrollment_without_rewriting_frozen_evidence(image_repair_case, monkeypatch, moment, loss, request):
    from types import SimpleNamespace

    import httpx
    from scripts.ops import nebius_certificates as private_state
    from scripts.ops import nebius_pool_manager_image_history as images
    from scripts.ops.nebius_ingress_stage import _key
    from scripts.ops.nebius_pool_activation_live import HTTPSPoolActivationAPI
    from scripts.ops.nebius_pool_activation_stage import advance_pool_activation
    from scripts.ops.nebius_pool_shutdown import stop_pool_successors
    from scripts.ops.nebius_pool_startup import closed_startup_documents
    from scripts.ops.nebius_pool_startup_fence import fence_pool_startup, observe_recovery_workloads
    from tests.ops.test_nebius_pool_shutdown import ShutdownAPI

    context, remote, state, anchor, _ = image_repair_case
    image_api = ImageAPI(image_repair_case)
    if moment == "prepared":
        persist(entry(image_repair_case))
    else:
        image_api.failure = ("isolate", "before")
        assert switch(image_repair_case, image_api)["status"] == "pending_manager_image_outcome"
    api = ShutdownAPI((context.request, None, None, remote.startup, None, state.parent))
    api.state = state
    args = dict(request=context.request, api=api, state_dir=state, anchor_dir=anchor)
    key = _key(context.request.manager)
    with monkeypatch.context() as legacy:
        legacy.setattr(images, "load_manager_image_chain", lambda *args, **kwargs: ())
        assert advance_pool_activation(**args, cancel=True)["status"] == "pool_activation_cancelled"
        if moment == "prepared_fence":
            save = private_state._atomic_json

            def interrupt(path, value):
                save(path, value)
                if path == state / "startup-fence.json":
                    raise OSError("synthetic interruption after old fence enrollment")

            legacy.setattr(private_state, "_atomic_json", interrupt)
            with pytest.raises(ValueError):
                fence_pool_startup(**args)
        else:
            assert fence_pool_startup(**args)["status"] == "startup_writes_fenced"
        if moment in {"late_shutdown", "stopped"}:
            stop = api.stop_workload

            def uncertain(key_, before, desired):
                api.stop_failure = "before" if moment == "late_shutdown" and key_ == key else None
                return stop(key_, before, desired)

            legacy.setattr(api, "stop_workload", uncertain)
            assert stop_pool_successors(**args)["status"] == (
                "pending_shutdown_outcome" if moment == "late_shutdown" else "pool_successors_stopped")
    frozen = {path: path.read_bytes() for path in (state / "startup-fence.json",
        anchor / (context.operation["operation_id"] + "-startup-fence.json"))
        if moment != "prepared_fence" or path.parent == anchor}
    if moment in {"prepared_fence", "late_isolate", "late_shutdown"}:
        image_api.deliver()
    writes, pending = [], []
    if moment == "late_shutdown":
        def respond(message):
            before = remote.read_workload(key)
            patches = json.loads(message.content)
            assert message.method == "PATCH"
            assert patches == [
                {"op": "test", "path": "/metadata/uid", "value": before["metadata"]["uid"]},
                {"op": "test", "path": "/metadata/resourceVersion", "value": before["metadata"]["resourceVersion"]},
                {"op": "test", "path": "/metadata", "value": before["metadata"]},
                {"op": "test", "path": "/spec", "value": before["spec"]},
                {"op": "replace", "path": "/spec/replicas", "value": 0},
                {"op": "remove", "path": "/metadata/annotations/loom.nebius~1manager-image-repair"}]
            desired = copy.deepcopy(before)
            desired["spec"]["replicas"] = 0
            del desired["metadata"]["annotations"]["loom.nebius/manager-image-repair"]
            if message.url.params:
                return httpx.Response(200, json=desired)
            row = json.loads((state / "shutdown.json").read_bytes())["workloads"][key]
            assert row["phase"] == "intent"
            assert row["isolation_stop"] == {"phase": "intent", "before_resource_version": before["metadata"]["resourceVersion"]}
            assert row["before_resource_version"] != before["metadata"]["resourceVersion"]
            writes.append(patches)
            desired["metadata"]["resourceVersion"] = str(int(before["metadata"]["resourceVersion"]) + 1)
            if len(writes) == 1 and loss == "conflict":
                return httpx.Response(409, json={"apiVersion": "v1", "kind": "Status", "status": "Failure",
                    "code": 409, "reason": "Conflict"})
            if loss == "before":
                pending.append(desired)
                raise httpx.ReadTimeout("synthetic lost request")
            remote.startup.documents[key] = desired
            if loss == "after":
                raise httpx.ReadTimeout("synthetic lost response")
            return httpx.Response(200, json=desired)

        client = httpx.Client(base_url="https://kubernetes.invalid", transport=httpx.MockTransport(respond))
        request.addfinalizer(client.close)
        closed, _ = closed_startup_documents(context.request, state_dir=state, anchor_dir=anchor)
        adapter = SimpleNamespace(request=context.request, state=state, anchor=anchor, closed=closed,
            parent=SimpleNamespace(client=client), _scope=lambda: None,
            _path=lambda key: "/deployments/loom-service", recovery_drained=api.recovery_drained)
        monkeypatch.setattr(api, "stop_workload", lambda key, before, desired:
            HTTPSPoolActivationAPI._stop_patch(adapter, key, before, desired, preview=False))
        monkeypatch.setattr(api, "preview_stop", lambda key, before, desired: copy.deepcopy(desired)
            if HTTPSPoolActivationAPI._stop_patch(adapter, key, before, desired, preview=True) else None)
    api.stop_failure = None
    assert fence_pool_startup(**args)["status"] == "startup_writes_fenced"
    if loss == "before":
        assert stop_pool_successors(**args)["status"] == "pending_shutdown_outcome"
        assert stop_pool_successors(**args)["status"] == "pending_shutdown_outcome"
        assert len(writes) == 1 and len(pending) == 1
        remote.startup.documents[key] = pending[0]
    elif loss == "conflict":
        assert stop_pool_successors(**args)["status"] == "pending_shutdown_update"
    assert stop_pool_successors(**args)["status"] == "pool_successors_stopped"
    if moment == "late_shutdown":
        assert len(writes) == (2 if loss == "conflict" else 1)
        client.close()
    actual = api.read_workload(key)
    assert actual["spec"]["replicas"] == 0
    assert "loom.nebius/manager-image-repair" not in actual["metadata"].get("annotations", {})
    assert all(path.read_bytes() == raw for path, raw in frozen.items())
    if moment != "prepared":
        version = entry(image_repair_case).record["phases"]["isolate"]["before_resource_version"]
        assert actual["metadata"]["resourceVersion"] != version
        remote.startup.documents[key]["metadata"]["resourceVersion"] = version
        with pytest.raises(ValueError):
            observe_recovery_workloads(context.request, api, state=state, anchor=anchor)


@pytest.mark.timeout(600)
def test_legacy_completion_preserves_receipt_and_carries_unstarted_image_ancestry(image_repair_case, monkeypatch):
    from types import SimpleNamespace

    from scripts.ops import nebius_pool_manager_image_history as images
    from scripts.ops import nebius_pool_operation as operation
    from scripts.ops.nebius_pool_completion import complete_pool_cutover, load_pool_completion
    from tests.ops.test_nebius_pool_gateway_retirement import GatewayAPI
    from tests.ops.test_nebius_pool_legacy_reopening import ReopeningAPI
    from tests.ops.test_nebius_pool_legacy_restart import RestartAPI
    from tests.ops.test_nebius_pool_machine_retirement import MachineAPI
    from tests.ops.test_nebius_pool_role_restoration import RoleAPI
    from tests.ops.test_nebius_pool_template_restoration import TemplateAPI

    context, remote, state, anchor, _ = image_repair_case
    image_api = ImageAPI(image_repair_case)
    image_api.failure = ("isolate", "before")
    assert switch(image_repair_case, image_api)["status"] == "pending_manager_image_outcome"
    chain = context.request, context.tokens, remote.closed, remote.startup, None, state.parent
    machine = MachineAPI(chain)
    gateway = GatewayAPI(chain, machine)
    template = TemplateAPI(chain, gateway)
    roles = RoleAPI(chain, template)
    restart = RestartAPI(chain, roles)

    class RecoveryAPI(ReopeningAPI):
        def successor_drained(self, key, desired):
            assert desired["spec"].get("suspend", desired["spec"].get("replicas")) in (True, 0)
            return self.processes_drained

    runtime = RecoveryAPI(chain, restart)
    parent = SimpleNamespace(request=context.request, state_dir=state, anchor_dir=anchor, refresh=None)
    monkeypatch.setattr(operation, "HTTPSPoolActivationAPI", lambda **kwargs: runtime)
    with monkeypatch.context() as legacy:
        legacy.setattr(images, "load_manager_image_chain", lambda *args, **kwargs: ())
        legacy.setattr(images, "manager_image_paths", lambda *args, **kwargs: ())
        result = operation.run_pool_operation(parent=parent, tokens=context.tokens, action="rollback")
    assert result["outcome"] == "legacy"
    frozen = {path: path.read_bytes() for root in (state, anchor) for path in root.rglob("*.json")}
    loaded = load_pool_completion(request=context.request, state_dir=state, anchor_dir=anchor,
        completion_sha256=result["completion_sha256"])
    assert loaded.outcome == "legacy"
    retained = entry(image_repair_case)
    assert {retained.path, retained.marker} <= set(loaded.history)
    assert complete_pool_cutover(request=context.request, api=runtime, state_dir=state, anchor_dir=anchor) == result
    assert all(path.read_bytes() == raw for path, raw in frozen.items())
    from scripts.ops.nebius_certificates import _atomic_json

    changed = json.loads(retained.path.read_bytes())
    changed["phases"]["isolate"]["phase"] = "applied"
    changed["phases"]["stop"] = {"phase": "intent", "before_resource_version": "999"}
    _atomic_json(retained.path, changed)
    with pytest.raises(ValueError):
        load_pool_completion(request=context.request, state_dir=state, anchor_dir=anchor,
            completion_sha256=result["completion_sha256"])
