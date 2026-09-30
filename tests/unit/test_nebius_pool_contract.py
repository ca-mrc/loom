"""Pool identity cannot alias independent databases or grant Kubernetes authority."""
from __future__ import annotations

import copy
import json
from uuid import UUID

import pytest
from pydantic import ValidationError


def participant(**changes):
    value = {
        "participant_id": "10000000-0000-4000-8000-000000000001",
        "installation_id": "20000000-0000-4000-8000-000000000001",
        "environment_id": "30000000-0000-4000-8000-000000000001",
        "incarnation": "40000000-0000-4000-8000-000000000001",
        "pool_id": "50000000-0000-4000-8000-000000000001",
        "binding_revision": 1,
        "admission_epoch": 1,
        "execution_namespace": {
            "name": "loom-dev-execution", "uid": "60000000-0000-4000-8000-000000000001",
        },
        "build_namespace": {
            "name": "loom-dev-build", "uid": "60000000-0000-4000-8000-000000000002",
        },
        "targets": [{
            "target_id": "nebius-default",
            "profile_id": "70000000-0000-4000-8000-000000000001",
            "workload_kinds": ["trial", "verifier", "task_image_build", "application_image_build"],
        }],
    }
    return value | changes


def key(**changes):
    return {
        "participant_id": "10000000-0000-4000-8000-000000000001",
        "workload_kind": "trial",
        "local_work_id": "80000000-0000-4000-8000-000000000001",
        "generation": 1,
    } | changes


def test_same_local_work_id_in_two_participants_has_distinct_durable_key():
    from loom.nebius_pool_contract import PoolRequestKeyV1

    alice = PoolRequestKeyV1.model_validate(key())
    bob = PoolRequestKeyV1.model_validate(key(participant_id="10000000-0000-4000-8000-000000000002"))
    assert alice.local_work_id == bob.local_work_id
    assert alice.storage_key() == (
        UUID("10000000-0000-4000-8000-000000000001"), "trial",
        UUID("80000000-0000-4000-8000-000000000001"), 1,
    )
    assert bob.storage_key() != alice.storage_key()
    assert PoolRequestKeyV1.model_validate(key(generation=2)).storage_key() != alice.storage_key()
    assert PoolRequestKeyV1.model_validate(key(workload_kind="verifier")).storage_key() != alice.storage_key()


@pytest.mark.parametrize("field", ["participant_id", "local_work_id"])
def test_nil_request_identity_is_not_a_valid_global_key(field):
    from loom.nebius_pool_contract import PoolRequestKeyV1

    with pytest.raises(ValidationError):
        PoolRequestKeyV1.model_validate(key(**{field: str(UUID(int=0))}))


@pytest.mark.parametrize("generation", [0, -1, True, "1", 1.5, 2**63])
def test_request_generation_must_be_a_positive_database_integer(generation):
    from loom.nebius_pool_contract import PoolRequestKeyV1

    with pytest.raises(ValidationError):
        PoolRequestKeyV1.model_validate(key(generation=generation))


def test_aliases_share_namespace_but_local_target_names_are_not_globally_unique():
    from loom.nebius_pool_contract import PoolParticipantV1

    payload = participant()
    payload["targets"].append({
        "target_id": "nebius-guest", "profile_id": "70000000-0000-4000-8000-000000000002",
        "workload_kinds": ["trial", "verifier"],
    })
    first = PoolParticipantV1.model_validate(payload)
    second = PoolParticipantV1.model_validate(participant(
        participant_id="10000000-0000-4000-8000-000000000002",
        environment_id="30000000-0000-4000-8000-000000000002",
        incarnation="40000000-0000-4000-8000-000000000002",
        execution_namespace={"name": "loom-staging-execution", "uid": "60000000-0000-4000-8000-000000000003"},
        build_namespace={"name": "loom-staging-build", "uid": "60000000-0000-4000-8000-000000000004"},
        targets=[{"target_id": "nebius-default", "profile_id": "70000000-0000-4000-8000-000000000003",
                  "workload_kinds": ["trial"]}],
    ))
    assert first.target("nebius-guest", "trial").profile_id == UUID("70000000-0000-4000-8000-000000000002")
    assert second.target("nebius-default", "trial").target_id == first.target("nebius-default", "trial").target_id
    assert second.target("nebius-default", "trial").profile_id != first.target("nebius-default", "trial").profile_id
    with pytest.raises(ValueError, match="target_workload_unavailable"):
        first.target("nebius-guest", "task_image_build")
    with pytest.raises(ValueError, match="target_workload_unavailable"):
        first.target("missing", "trial")


@pytest.mark.parametrize("damage", ["name", "uid", "target", "kind", "empty_targets", "empty_kinds"])
def test_ambiguous_or_empty_participant_binding_is_rejected(damage):
    from loom.nebius_pool_contract import PoolParticipantV1

    payload = participant()
    if damage in {"name", "uid"}:
        payload["build_namespace"][damage] = payload["execution_namespace"][damage]
    elif damage == "target":
        payload["targets"].append(copy.deepcopy(payload["targets"][0]))
    elif damage == "kind":
        payload["targets"][0]["workload_kinds"].append("trial")
    elif damage == "empty_targets":
        payload["targets"] = []
    else:
        payload["targets"][0]["workload_kinds"] = []
    with pytest.raises(ValidationError):
        PoolParticipantV1.model_validate(payload)


@pytest.mark.parametrize("field", ["participant_id", "installation_id", "environment_id", "incarnation", "pool_id"])
def test_nil_participant_identity_is_rejected(field):
    from loom.nebius_pool_contract import PoolParticipantV1

    with pytest.raises(ValidationError):
        PoolParticipantV1.model_validate(participant(**{field: str(UUID(int=0))}))


@pytest.mark.parametrize("field", ["binding_revision", "admission_epoch"])
@pytest.mark.parametrize("value", [0, -1, True, "1", 2**63])
def test_binding_epochs_cannot_coerce_invalid_database_values(field, value):
    from loom.nebius_pool_contract import PoolParticipantV1

    with pytest.raises(ValidationError):
        PoolParticipantV1.model_validate(participant(**{field: value}))


@pytest.mark.parametrize("path,value", [
    (("execution_namespace", "name"), "other/namespace"),
    (("build_namespace", "name"), "UPPERCASE"),
    (("execution_namespace", "uid"), str(UUID(int=0))),
    (("targets", 0, "target_id"), "bad target"),
    (("targets", 0, "profile_id"), str(UUID(int=0))),
    (("targets", 0, "workload_kinds"), ["arbitrary_pod"]),
])
def test_nested_authority_identifiers_and_workload_kinds_are_bounded(path, value):
    from loom.nebius_pool_contract import PoolParticipantV1

    payload = participant()
    cursor = payload
    for part in path[:-1]:
        cursor = cursor[part]
    cursor[path[-1]] = value
    with pytest.raises(ValidationError):
        PoolParticipantV1.model_validate(payload)


@pytest.mark.parametrize("extra", ["namespace", "job", "secret_name", "service_account", "node_selector"])
def test_request_identity_does_not_admit_caller_kubernetes_authority(extra):
    from loom.nebius_pool_contract import PoolRequestKeyV1

    with pytest.raises(ValidationError):
        PoolRequestKeyV1.model_validate(key(**{extra: "caller-selected"}))


def test_binding_roundtrip_is_detached_from_mutable_input_and_frozen():
    from loom.nebius_pool_contract import PoolParticipantV1

    payload = participant()
    binding = PoolParticipantV1.model_validate_json(json.dumps(payload))
    payload["targets"][0]["workload_kinds"].clear()
    payload["execution_namespace"]["name"] = "foreign"
    assert binding.execution_namespace.name == "loom-dev-execution"
    assert binding.target("nebius-default", "trial").workload_kinds == (
        "trial", "verifier", "task_image_build", "application_image_build",
    )
    with pytest.raises(ValidationError):
        binding.execution_namespace.name = "foreign"
    assert PoolParticipantV1.model_validate_json(binding.model_dump_json()) == binding


def receipt(state="reserved", **changes):
    started = state not in {"reserved", "cancelled_unstarted"}
    return {
        "reservation_id": "90000000-0000-4000-8000-000000000001",
        "pool_id": "50000000-0000-4000-8000-000000000001",
        "request_key": key(), "admission_epoch": 1, "request_sha256": "a" * 64,
        "phase": state, "plan_sha256": "b" * 64 if started else None,
        "job_uid": "a0000000-0000-4000-8000-000000000001" if state == "observed" else None,
        "cleanup_observation_id": "b0000000-0000-4000-8000-000000000001" if state == "released" else None,
    } | changes


@pytest.mark.parametrize("phase,charged", [
    ("reserved", True), ("create_intent", True), ("observed", True),
    ("cleanup_intent", True), ("released", False), ("cancelled_unstarted", False),
])
def test_uncertain_and_cleanup_states_retain_capacity(phase, charged):
    from loom.nebius_pool_contract import PoolReceiptV1

    assert PoolReceiptV1.model_validate(receipt(phase)).capacity_charged is charged


@pytest.mark.parametrize("phase,changes", [
    ("reserved", {"plan_sha256": "b" * 64}),
    ("cancelled_unstarted", {"job_uid": "a0000000-0000-4000-8000-000000000001"}),
    ("create_intent", {"plan_sha256": None}),
    ("create_intent", {"job_uid": "a0000000-0000-4000-8000-000000000001"}),
    ("observed", {"job_uid": None}),
    ("observed", {"plan_sha256": None}),
    ("cleanup_intent", {"plan_sha256": None}),
    ("cleanup_intent", {"cleanup_observation_id": "b0000000-0000-4000-8000-000000000001"}),
    ("released", {"cleanup_observation_id": None}),
    ("released", {"plan_sha256": None}),
])
def test_receipt_cannot_drop_intent_or_invent_release_without_evidence_reference(phase, changes):
    from loom.nebius_pool_contract import PoolReceiptV1

    with pytest.raises(ValidationError):
        PoolReceiptV1.model_validate(receipt(phase, **changes))


@pytest.mark.parametrize("field,value", [
    ("reservation_id", str(UUID(int=0))), ("pool_id", str(UUID(int=0))),
    ("admission_epoch", True), ("admission_epoch", 0),
    ("request_sha256", "A" * 64), ("request_sha256", "a" * 63),
    ("plan_sha256", "private/raw/payload"),
    ("job_uid", str(UUID(int=0))), ("cleanup_observation_id", str(UUID(int=0))),
    ("phase", "failed"), ("phase", "expired"), ("phase", "cancelled"),
])
def test_receipt_identity_and_phase_cannot_bypass_resource_accounting(field, value):
    from loom.nebius_pool_contract import PoolReceiptV1

    with pytest.raises(ValidationError):
        PoolReceiptV1.model_validate(receipt("released", **{field: value}))


_PHASES = ("reserved", "create_intent", "observed", "cleanup_intent", "released", "cancelled_unstarted")
_EDGES = {
    ("reserved", "create_intent"), ("reserved", "cancelled_unstarted"),
    ("create_intent", "observed"), ("create_intent", "cleanup_intent"),
    ("observed", "cleanup_intent"), ("cleanup_intent", "released"),
}


@pytest.mark.parametrize("previous", _PHASES)
@pytest.mark.parametrize("following", _PHASES)
def test_receipt_transition_cannot_skip_cleanup_or_reactivate_terminal_request(previous, following):
    from loom.nebius_pool_contract import PoolReceiptV1, validate_pool_receipt_transition

    first = PoolReceiptV1.model_validate(receipt(previous))
    payload = receipt(following)
    if previous == "observed" and following in {"cleanup_intent", "released"}:
        payload["job_uid"] = str(first.job_uid)
    second = PoolReceiptV1.model_validate(payload)
    if previous == following or (previous, following) in _EDGES:
        validate_pool_receipt_transition(first, second)
    else:
        with pytest.raises(ValueError):
            validate_pool_receipt_transition(first, second)


@pytest.mark.parametrize("field,value", [
    ("reservation_id", "90000000-0000-4000-8000-000000000002"),
    ("pool_id", "50000000-0000-4000-8000-000000000002"),
    ("request_key", key(generation=2)), ("admission_epoch", 2),
    ("request_sha256", "c" * 64), ("plan_sha256", "c" * 64),
    ("job_uid", None), ("job_uid", "a0000000-0000-4000-8000-000000000002"),
])
def test_cleanup_transition_cannot_replace_frozen_request_plan_or_observed_job(field, value):
    from loom.nebius_pool_contract import PoolReceiptV1, validate_pool_receipt_transition

    first = PoolReceiptV1.model_validate(receipt("observed"))
    payload = receipt("cleanup_intent", job_uid=str(first.job_uid)) | {field: value}
    with pytest.raises(ValueError):
        validate_pool_receipt_transition(first, PoolReceiptV1.model_validate(payload))


def test_replaying_released_receipt_cannot_change_cleanup_evidence():
    from loom.nebius_pool_contract import PoolReceiptV1, validate_pool_receipt_transition

    first = PoolReceiptV1.model_validate(receipt("released"))
    second = PoolReceiptV1.model_validate(receipt(
        "released", cleanup_observation_id="b0000000-0000-4000-8000-000000000002"))
    with pytest.raises(ValueError):
        validate_pool_receipt_transition(first, second)
