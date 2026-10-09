"""The installed large-object mode is distinct from ordinary HTTP recovery."""
from __future__ import annotations

import copy
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from loom_control_plane import object_version_recovery as target


def ordinary_payload():
    return {
        "operation_id": "11111111-1111-4111-8111-111111111111",
        "trial_id": "22222222-2222-4222-8222-222222222222",
        "artifact_id": "33333333-3333-4333-8333-333333333333",
        "expected_storage_sha256": "sha256:" + "a" * 64,
        "expected_index_sha256": "sha256:" + "b" * 64,
        "objects": [{"registry_id": "44444444-4444-4444-8444-444444444444", "version_id": "v1"}],
    }


def operator_payload():
    return {
        **ordinary_payload(),
        "mode": "single_large_object_v1",
        "team_id": "55555555-5555-4555-8555-555555555555",
        "candidate_sha": "c" * 40,
        "schema_head": "0174",
    }


def test_ordinary_request_identity_remains_compatible():
    request = target.RecoveryRequest.model_validate(ordinary_payload())
    assert target.metadata_digest(request.identity()) == (
        "sha256:14901eedd109740596beb0d742d1cde5be0e6cb868b3da3680e47ccd0ff261bb"
    )


def test_http_model_rejects_operator_fields():
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        target.RecoveryRequest.model_validate(operator_payload())


@pytest.mark.parametrize("versions", [None, ["v2", "v1"]])
def test_operator_accepts_one_complete_single_or_two_version_object(versions):
    payload = operator_payload()
    if versions is not None:
        payload["objects"][0]["equivalent_version_ids"] = versions
    request = target.SingleObjectRecoveryRequest.model_validate(payload)
    assert request.apply is False and request.plan_sha256 is None
    identity = copy.deepcopy(payload)
    if versions is not None:
        identity["objects"][0]["equivalent_version_ids"] = sorted(versions)
    assert request.identity() == identity
    assert target.verification_byte_limit(request) == 4 * 1024**3


@pytest.mark.parametrize("missing", ["mode", "team_id", "candidate_sha", "schema_head"])
def test_operator_requires_explicit_scope_binding(missing):
    payload = operator_payload()
    payload.pop(missing)
    with pytest.raises(ValidationError):
        target.SingleObjectRecoveryRequest.model_validate(payload)


@pytest.mark.parametrize("field,value", [
    ("mode", "unbounded"),
    ("team_id", "00000000-0000-0000-0000-000000000000"),
    ("team_id", True),
    ("candidate_sha", "C" * 40),
    ("candidate_sha", "c" * 39),
    ("schema_head", 174),
    ("schema_head", "174"),
    ("byte_limit", 8 * 1024**3),
    ("apply", True),
])
def test_operator_rejects_malformed_or_implicit_authority(field, value):
    payload = {**operator_payload(), field: value}
    with pytest.raises(ValidationError):
        target.SingleObjectRecoveryRequest.model_validate(payload)


@pytest.mark.parametrize("damage", ["second_object", "third_copy"])
def test_operator_cannot_expand_object_or_copy_count(damage):
    payload = operator_payload()
    if damage == "second_object":
        payload["objects"].append({
            "registry_id": "66666666-6666-4666-8666-666666666666", "version_id": "v1",
        })
    else:
        payload["objects"][0]["equivalent_version_ids"] = ["v1", "v2", "v3"]
    with pytest.raises(ValidationError):
        target.SingleObjectRecoveryRequest.model_validate(payload)


@pytest.mark.parametrize("field,value", [
    ("team_id", "77777777-7777-4777-8777-777777777777"),
    ("candidate_sha", "d" * 40),
    ("schema_head", "0175"),
])
def test_operator_scope_is_part_of_the_audited_identity(field, value):
    request = target.SingleObjectRecoveryRequest.model_validate(operator_payload())
    changed = target.SingleObjectRecoveryRequest.model_validate({**operator_payload(), field: value})
    assert target.metadata_digest(request.identity()) != target.metadata_digest(changed.identity())
    assert request.identity()["mode"] == "single_large_object_v1"


def test_ordinary_or_untyped_request_cannot_select_large_budget():
    ordinary = target.RecoveryRequest.model_validate(ordinary_payload())
    untyped = SimpleNamespace(mode="single_large_object_v1", byte_limit=4 * 1024**3)
    assert target.verification_byte_limit(ordinary) == 256 * 1024**2
    assert target.verification_byte_limit(untyped) == 256 * 1024**2
