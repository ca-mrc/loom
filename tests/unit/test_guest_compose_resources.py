"""Compose sidecars must fit alongside the unchanged task declaration."""
import pytest

from loom.execution_contract import workload_requirements_from_task
from loom.execution_resource_allocation import allocate_node_resources
from loom.execution_runtime_contract import ContainerResourcesV1, validate_runtime_plan_requirements
from loom.guest_compose import compose_resource_envelope
from tests.unit.test_compose_image_inputs import prepared_plan


def test_compose_admission_and_placement_include_every_service_and_guest_daemon():
    task, trial, plan = prepared_plan()
    env = task.environment
    envelope = compose_resource_envelope(env.native_compose, main_cpus=env.cpus,
                                        main_memory_mb=env.memory_mb, main_storage_mb=env.storage_mb)
    assert plan.task_resources == ContainerResourcesV1(cpu_millis=round(envelope.cpus * 1000),
        memory_mib=envelope.memory_mb, ephemeral_storage_mib=envelope.storage_mb)
    assert plan.workspace_mib == envelope.storage_mb
    assert plan.controller_resources.ephemeral_storage_mib == (
        envelope.storage_mb + plan.runtime_volume_mib + env.storage_mb + 3072)
    requirements = workload_requirements_from_task(task, trial)
    validate_runtime_plan_requirements(plan, requirements)
    with pytest.raises(ValueError, match='exceeds_node_allocatable'):
        allocate_node_resources(plan, target_id='too-small', usable_node=ContainerResourcesV1(
            cpu_millis=64000, memory_mib=env.memory_mb, ephemeral_storage_mib=512 * 1024))
    allocated = allocate_node_resources(plan, target_id='adequate', usable_node=ContainerResourcesV1(
        cpu_millis=64000, memory_mib=240 * 1024, ephemeral_storage_mib=512 * 1024))
    validate_runtime_plan_requirements(allocated, requirements)
    assert allocated.task_resources.memory_mib >= envelope.memory_mb
