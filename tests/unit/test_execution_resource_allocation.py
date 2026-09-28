from copy import deepcopy

import pytest

from loom.execution_contract import workload_requirements_from_task
from loom.execution_resource_allocation import allocate_node_resources, resource_allocation_summary
from loom.execution_runtime_contract import (
    ContainerResourcesV1,
    ExecutionRuntimePlanV1,
    runtime_pod_resources,
    validate_runtime_plan_requirements,
)
from loom.service_execution_materialization import (
    ControllerComputeResourcesV1,
    compile_service_execution_plan,
)
from tests.unit.test_service_execution_materialization import _REVISION, _provenance
from tests.unit.test_service_execution_terminus_plan import _inputs


def _plan(memory=4096):
    task, trial, profile = _inputs()
    task = task.model_copy(update={"environment": task.environment.model_copy(update={"memory_mb": memory})})
    profile = profile.model_copy(update={
        "controller_resources": ControllerComputeResourcesV1(cpu_millis=1000, memory_mib=1024),
    })
    return task, compile_service_execution_plan(
        task=task, trial=trial, profile=profile, task_revision_sha256=_REVISION,
        source_provenance=_provenance(),
    )


@pytest.mark.parametrize("node_cpu,node_memory,declared,expected", [
    (16_000, 240 * 1024, 4096, 7168),
    (32_000, 480 * 1024, 4096, 7168),
    (16_000, 112 * 1024, 4096, 4096),
    (16_000, 240 * 1024, 12 * 1024, 12 * 1024),
])
def test_node_share_preserves_minima_and_reserves_every_resident_role(node_cpu, node_memory, declared, expected):
    task, plan = _plan(declared)
    frozen = deepcopy(plan.canonical_payload())
    resolved = allocate_node_resources(plan, target_id="pool-a", usable_node=ContainerResourcesV1(
        cpu_millis=node_cpu, memory_mib=node_memory, ephemeral_storage_mib=512 * 1024,
    ))
    assert resolved.task_resources.memory_mib == expected
    assert resolved.container_request("task-sandbox").memory_mib == expected
    assert "verifier-sandbox" not in {item.role_name for item in resolved.sidecars}
    assert resolved.resource_requests.verifier_sandbox is None
    assert runtime_pod_resources(resolved).memory_mib == 1024 + expected
    validate_runtime_plan_requirements(resolved, workload_requirements_from_task(task))
    assert plan.canonical_payload() == frozen
    assert ExecutionRuntimePlanV1.model_validate(resolved.canonical_payload()) == resolved
    assert resource_allocation_summary(resolved)["pod_requests"]["memory_mib"] == 1024 + expected


def test_large_task_cannot_be_shrunk_to_fit_and_memory_cannot_be_underreserved():
    _, plan = _plan(12 * 1024)
    with pytest.raises(ValueError, match="exceeds_node_allocatable"):
        allocate_node_resources(plan, target_id="small", usable_node=ContainerResourcesV1(
            cpu_millis=16_000, memory_mib=12 * 1024, ephemeral_storage_mib=512 * 1024,
        ))
    resolved = allocate_node_resources(plan, target_id="large", usable_node=ContainerResourcesV1(
        cpu_millis=16_000, memory_mib=240 * 1024, ephemeral_storage_mib=512 * 1024,
    ))
    payload = resolved.canonical_payload()
    payload["resource_requests"]["task_sandbox"]["memory_mib"] = 1024
    with pytest.raises(ValueError, match="memory requests must equal"):
        ExecutionRuntimePlanV1.model_validate(payload)


def test_mixed_workloads_consume_actual_allocations_in_existing_placement():
    from loom_control_plane.execution_placement import cold_sample, plan_placement
    from loom_execution_capacity_collector.contracts import CapacityPlacement, ResourceTotals
    from tests.execution_placement_fixtures import placement_fixture

    placement = CapacityPlacement.model_validate(placement_fixture(target_id="pool-a"))
    sample = cold_sample(placement)
    assert sample is not None
    usable = ContainerResourcesV1(
        cpu_millis=sample.allocatable.cpu_millis - sample.daemonset_requests.cpu_millis,
        memory_mib=sample.allocatable.memory_mib - sample.daemonset_requests.memory_mib,
        ephemeral_storage_mib=sample.allocatable.storage_mib - sample.daemonset_requests.storage_mib,
    )
    demands = []
    for i in range(30):
        _, plan = _plan((4 if i % 2 else 12) * 1024)
        allocated = allocate_node_resources(plan, target_id="pool-a", usable_node=usable)
        total = runtime_pod_resources(allocated)
        demands.append((str(i), ResourceTotals(cpu_millis=total.cpu_millis,
                         memory_mib=total.memory_mib, storage_mib=total.ephemeral_storage_mib)))
    placed = plan_placement(placement, demands, sample=sample)
    assert placed.additional_nodes > 0
    total_memory = sum(r.memory_mib for _, r in demands)
    assert total_memory <= (len(placement.nodes) + placed.additional_nodes) * usable.memory_mib


def test_renderer_uses_the_frozen_allocation_for_every_container():
    from loom.pipeline.keys import canonical_digest
    from loom_execution_actuator.renderer import ExecutionTargetRuntime, render_execution_job
    from tests.unit.test_execution_actuator import _lease

    task, plan = _plan()
    lease = _lease()
    resolved = allocate_node_resources(plan, target_id=lease.target_id, usable_node=ContainerResourcesV1(
        cpu_millis=16_000, memory_mib=240 * 1024, ephemeral_storage_mib=512 * 1024,
    ))
    lease.runtime_contract_json = resolved.canonical_payload()
    lease.runtime_contract_sha256 = canonical_digest(lease.runtime_contract_json)
    lease.workload_requirements_json = workload_requirements_from_task(task).model_dump(mode="json")
    manifest = render_execution_job(lease, target=ExecutionTargetRuntime(
        target_id=lease.target_id, namespace=lease.namespace_name,
    ))
    pod = manifest["spec"]["template"]["spec"]
    containers = {c["name"]: c for c in pod["containers"] + pod["initContainers"]}
    for role in ("task-sandbox",):
        assert containers[role]["resources"]["requests"]["memory"] == "7168Mi"
        assert containers[role]["resources"]["limits"]["memory"] == "7168Mi"
    assert containers["execution"]["resources"]["requests"]["memory"] == "1024Mi"
