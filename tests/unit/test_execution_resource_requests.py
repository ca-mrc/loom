from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import pytest

from loom.db.schema import ExecutionPriceSnapshot
from loom.execution_contract import workload_requirements_from_task
from loom.execution_runtime_contract import (
    ContainerResourcesV1,
    ExecutionResourceRequestsV1,
    ExecutionRuntimePlanV1,
    runtime_pod_resources,
)
from loom.pipeline.keys import canonical_digest
from loom.service_execution_materialization import (
    ServiceExecutionRuntimeProfileV1,
    compile_service_execution_plan,
)
from loom.task_image_materialization import resolve_prepared_task
from loom_control_plane.execution_finance import estimate_execution_cost
from loom_execution_actuator.renderer import ExecutionTargetRuntime, render_execution_job
from tests.unit.test_execution_actuator import _lease
from tests.unit.test_service_execution_materialization import _REVISION, _provenance
from tests.unit.test_service_execution_prepared_image import _prepared
from tests.unit.test_service_execution_terminus_plan import _inputs


def _requests():
    return {
        "controller": {"cpu_millis": 100, "memory_mib": 256, "ephemeral_storage_mib": 256},
        "task_sandbox": {"cpu_millis": 250, "memory_mib": 512, "ephemeral_storage_mib": 1024},
        "verifier_sandbox": {"cpu_millis": 50, "memory_mib": 128, "ephemeral_storage_mib": 512},
    }


def _profile(profile, requests=None, revision=_REVISION):
    return ServiceExecutionRuntimeProfileV1.model_validate({
        **profile.model_dump(mode="json"),
        "task_resource_requests": {
            "selected-task": {"task_revision_sha256": revision, "requests": requests or _requests()},
        },
    })


def _compile(task, trial, profile, **kwargs):
    return compile_service_execution_plan(
        task=task, trial=trial, profile=profile, source_provenance=_provenance(),
        task_revision_sha256=_REVISION, **kwargs,
    )


def _render(plan, task, trial=None):
    lease = _lease()
    lease.runtime_contract_json = plan.canonical_payload()
    lease.runtime_contract_sha256 = canonical_digest(lease.runtime_contract_json)
    lease.workload_requirements_json = workload_requirements_from_task(task, trial).model_dump(mode="json")
    lease.workload_requirements_sha256 = canonical_digest(lease.workload_requirements_json)
    return render_execution_job(lease, target=ExecutionTargetRuntime(
        target_id=lease.target_id, namespace=lease.namespace_name,
    ))["spec"]["template"]["spec"]


@pytest.mark.parametrize("acquired_at, expected_daily_costs, expected_total", [
    (datetime(2026, 9, 16, 12, tzinfo=UTC), ((date(2026, 9, 16), 2398),), 2398),
    (datetime(2026, 9, 16, 23, 35, tzinfo=UTC),
     ((date(2026, 9, 16), 1000), (date(2026, 9, 17), 1399)), 2399),
], ids=["same-day", "cross-midnight"])
def test_scoped_requests_render_and_price_same_pod_without_changing_task_limits(
    acquired_at, expected_daily_costs, expected_total,
):
    task, trial, profile = _inputs()
    before = task.model_dump(mode="json")
    baseline = _compile(task, trial, profile)
    configured = _profile(profile)
    plan = _compile(task, trial, configured, task_id="selected-task")
    assert task.model_dump(mode="json") == before
    assert plan.task_revision_sha256 == baseline.task_revision_sha256
    assert plan.command_identity_sha256 == baseline.command_identity_sha256
    assert plan.task_resources == baseline.task_resources
    assert plan.sidecars == baseline.sidecars
    assert plan.execution_resources == baseline.execution_resources
    assert "task_resource_requests" not in profile.model_dump(mode="json")
    assert "resource_requests" not in baseline.canonical_payload()
    assert _compile(task, trial, configured, task_id="other-task") == baseline
    pod, baseline_pod = _render(plan, task, trial), _render(baseline, task, trial)
    assert pod["volumes"] == baseline_pod["volumes"]
    assert pod["initContainers"][0]["resources"] == baseline_pod["initContainers"][0]["resources"]
    actual = {row["name"]: row for row in [*pod["containers"], *pod["initContainers"][1:]]}
    original = {row["name"]: row for row in [*baseline_pod["containers"], *baseline_pod["initContainers"][1:]]}
    for name, key in (("execution", "controller"), ("task-sandbox", "task_sandbox")):
        assert actual[name]["resources"]["limits"] == original[name]["resources"]["limits"]
        expected = _requests()[key]
        assert actual[name]["resources"]["requests"] == {
            "cpu": f"{expected['cpu_millis']}m", "memory": f"{expected['memory_mib']}Mi",
            "ephemeral-storage": f"{expected['ephemeral_storage_mib']}Mi",
        }
    totals = runtime_pod_resources(plan)
    assert totals.model_dump() == {
        "cpu_millis": 350, "memory_mib": 768, "ephemeral_storage_mib": 1280,
    }
    # Each UTC day's reservation rounds up independently. Keep the resource
    # pricing assertion deterministic and exercise both sides of midnight.
    cost = estimate_execution_cost(plan, ExecutionPriceSnapshot(
        base_microusd_per_hour=0, vcpu_microusd_per_hour=1000,
        memory_gib_microusd_per_hour=1024, ephemeral_storage_gib_microusd_per_hour=1024,
    ), acquired_at=acquired_at, deadline_at=acquired_at + timedelta(hours=1))
    assert (cost.requested_cpu_millis, cost.requested_memory_mib,
            cost.requested_ephemeral_storage_mib) == (350, 768, 1280)
    assert cost.duration_seconds == 3600
    assert cost.daily_costs == expected_daily_costs
    assert cost.estimated_cost_microusd == expected_total


def test_one_role_override_defaults_other_roles_to_limits():
    task, trial, profile = _inputs()
    plan = _compile(task, trial, _profile(profile, {"controller": _requests()["controller"]}),
                    task_id="selected-task")
    assert plan.container_request("task-sandbox") == plan.task_resources
    assert runtime_pod_resources(plan).cpu_millis == 1100
    assert ExecutionRuntimePlanV1.model_validate(plan.canonical_payload()) == plan
    expected = {"controller": _requests()["controller"]}
    assert plan.canonical_payload()["resource_requests"] == expected
    frozen = _profile(profile, expected).model_dump(mode="json")
    assert frozen["task_resource_requests"]["selected-task"]["requests"] == expected
    assert ServiceExecutionRuntimeProfileV1.model_validate(frozen).model_dump(mode="json") == frozen


@pytest.mark.parametrize("value", [True, False, 1.5, 1.0, "1"])
@pytest.mark.parametrize("dimension", ["cpu_millis", "memory_mib", "ephemeral_storage_mib"])
def test_new_request_vectors_require_integers_without_changing_legacy_limits(value, dimension):
    vector = {**_requests()["controller"], dimension: value}
    with pytest.raises(ValueError, match="valid integer"):
        ExecutionResourceRequestsV1.model_validate({"controller": vector})
    # Historical limit parsing remains compatible; only new scheduling input is strict.
    legacy = ContainerResourcesV1.model_validate({**_requests()["controller"], dimension: "1"})
    assert getattr(legacy, dimension) == 1


def test_explicit_null_roles_are_omitted_and_empty_requests_still_rejected():
    requests = ExecutionResourceRequestsV1.model_validate({
        "controller": _requests()["controller"], "task_sandbox": None,
    })
    assert requests.model_dump(mode="json") == {"controller": _requests()["controller"]}
    with pytest.raises(ValueError, match="at least one"):
        ExecutionResourceRequestsV1.model_validate({"controller": None})


@pytest.mark.parametrize("role", ["controller", "task_sandbox", "verifier_sandbox"])
@pytest.mark.parametrize("dimension", ["cpu_millis", "memory_mib", "ephemeral_storage_mib"])
def test_oversized_requests_are_rejected_by_compiler_and_frozen_plan(role, dimension):
    task, trial, profile = _inputs()
    baseline = _compile(task, trial, profile)
    requests = _requests()
    requests[role][dimension] = getattr(baseline.task_resources, dimension) + 1
    with pytest.raises(ValueError, match="exceed hard limits"):
        _compile(task, trial, _profile(profile, requests), task_id="selected-task")
    with pytest.raises(ValueError, match="exceed hard limits"):
        ExecutionRuntimePlanV1.model_validate({**baseline.canonical_payload(),
                                             "resource_requests": requests})


def test_source_revision_and_unsupported_execution_cannot_silently_ignore_requests():
    task, trial, profile = _inputs()
    with pytest.raises(ValueError, match="selected task identity"):
        _compile(task, trial, _profile(profile))
    with pytest.raises(ValueError, match="source revision"):
        _compile(task, trial, _profile(profile, revision="sha256:" + "9" * 64),
                 task_id="selected-task")
    with pytest.raises(ValueError, match="automatic native terminus-2"):
        _compile(task, trial.model_copy(update={"agent_name": "direct-completion"}),
                 _profile(profile), task_id="selected-task")
    payload = _compile(task, trial, profile).canonical_payload()
    payload.update(resource_requests=_requests(), sidecars=[])
    with pytest.raises(ValueError, match="isolated attempt controller"):
        ExecutionRuntimePlanV1.model_validate(payload)


def test_prepared_task_preserves_source_revision_and_requests():
    task, trial, profile, grant = _prepared()
    plan = _compile(task, trial, _profile(profile), task_id="selected-task", task_image_grant=grant)
    assert plan.task_revision_sha256 == "sha256:" + grant.task_checksum
    assert plan.resource_requests is not None
    pod = _render(plan, resolve_prepared_task(task, grant), trial)
    assert pod["initContainers"][1]["resources"]["requests"]["cpu"] == "250m"
    with pytest.raises(ValueError, match="frozen task"):
        _compile(task, trial, _profile(profile), task_id="selected-task",
                 task_image_grant=grant.model_copy(update={"task_checksum": "9" * 64}))


@pytest.mark.parametrize("requests", [{}, {"unknown": _requests()["controller"]},
                                     {"controller": {**_requests()["controller"], "cpu_millis": 0}}])
def test_invalid_request_vectors_are_rejected(requests):
    with pytest.raises(ValueError):
        ExecutionResourceRequestsV1.model_validate(requests)
