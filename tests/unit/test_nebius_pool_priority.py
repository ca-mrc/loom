"""Priority comes from protected registration and explicit recorded origin."""
from __future__ import annotations

from uuid import UUID

import pytest
from pydantic import ValidationError

from tests.unit.test_nebius_pool_contract import participant


def origin(kind="environment", **changes):
    return {
        "data_environment_id": "30000000-0000-4000-8000-000000000001",
        "submission_id": "90000000-0000-4000-8000-000000000001",
        "kind": kind,
        "application": ({
            "application_id": "10000000-0000-4000-8000-000000000002",
            "incarnation": "40000000-0000-4000-8000-000000000002",
            "deployment_generation": 2,
            "release_id": "70000000-0000-4000-8000-000000000002",
            "source_digest": "sha256:" + "a" * 64,
        } if kind == "application" else None),
    } | changes


@pytest.mark.parametrize("workload_kind", ["trial", "verifier", "task_image_build"])
def test_environment_and_personal_order_is_the_same_for_execution_and_builds(workload_kind):
    from loom.nebius_pool_contract import PoolParticipantV1
    from loom.nebius_pool_priority import PoolWorkOriginV1, pool_request_priority

    cases = [
        ("production", "environment", 0), ("staging", "environment", 1),
        ("development", "environment", 2), ("development", "application", 3),
    ]
    for environment_class, kind, expected in cases:
        binding = PoolParticipantV1.model_validate(participant(environment_class=environment_class))
        recorded = PoolWorkOriginV1.model_validate(origin(kind))
        assert pool_request_priority(binding, recorded, workload_kind=workload_kind) == expected


def test_personal_source_build_is_personal_before_an_application_exists():
    from loom.nebius_pool_contract import PoolParticipantV1
    from loom.nebius_pool_priority import PoolWorkOriginV1, pool_request_priority

    binding = PoolParticipantV1.model_validate(participant(environment_class="development"))
    recorded = PoolWorkOriginV1.model_validate(origin("personal_build"))
    assert recorded.application is None
    assert pool_request_priority(binding, recorded, workload_kind="application_image_build") == 3


@pytest.mark.parametrize("environment_class,kind,workload_kind", [
    ("production", "application", "trial"), ("staging", "application", "trial"),
    ("production", "personal_build", "application_image_build"),
    ("development", "personal_build", "trial"),
    ("development", "environment", "application_image_build"),
    ("development", "application", "application_image_build"),
])
def test_origin_cannot_widen_environment_or_workload_scope(environment_class, kind, workload_kind):
    from loom.nebius_pool_contract import PoolParticipantV1
    from loom.nebius_pool_priority import PoolWorkOriginV1, pool_request_priority

    binding = PoolParticipantV1.model_validate(participant(environment_class=environment_class))
    with pytest.raises(ValueError, match="pool_origin_scope"):
        pool_request_priority(binding, PoolWorkOriginV1.model_validate(origin(kind)), workload_kind=workload_kind)


@pytest.mark.parametrize("damage", ["other_data", "nil_data", "nil_submission", "missing_kind", "missing_application", "extra_priority"])
def test_unknown_or_caller_promoted_origin_cannot_become_shared_work(damage):
    from loom.nebius_pool_contract import PoolParticipantV1
    from loom.nebius_pool_priority import PoolWorkOriginV1, pool_request_priority

    value = origin("application")
    if damage == "other_data":
        value["data_environment_id"] = "30000000-0000-4000-8000-000000000002"
    elif damage == "nil_data":
        value["data_environment_id"] = str(UUID(int=0))
    elif damage == "nil_submission":
        value["submission_id"] = str(UUID(int=0))
    elif damage == "missing_kind":
        del value["kind"]
    elif damage == "missing_application":
        value["application"] = None
    else:
        value["priority"] = 0
    with pytest.raises(ValueError):
        pool_request_priority(PoolParticipantV1.model_validate(participant(environment_class="development")),
            PoolWorkOriginV1.model_validate(value), workload_kind="trial")


@pytest.mark.parametrize("field,value", [
    ("application_id", str(UUID(int=0))), ("incarnation", str(UUID(int=0))),
    ("release_id", str(UUID(int=0))), ("deployment_generation", 0),
    ("deployment_generation", True), ("source_digest", "a" * 40),
])
def test_personal_origin_keeps_an_exact_frozen_deployment_source(field, value):
    from loom.nebius_pool_priority import PoolWorkOriginV1

    payload = origin("application")
    payload["application"][field] = value
    with pytest.raises(ValidationError):
        PoolWorkOriginV1.model_validate(payload)
