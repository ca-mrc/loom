from __future__ import annotations

from uuid import UUID, uuid4

import pytest

from loom.execution_contract import workload_requirements_from_task
from loom.execution_image_admission import (
    ImageAdmissionError,
    validate_execution_image_admission_bundle,
)
from loom.execution_runtime_contract import ExecutionRuntimePlanV1
from loom.models.task import TaskConfig
from loom.pipeline.keys import canonical_digest
from loom.service_execution_materialization import (
    compile_service_execution_plan,
)
from loom.task_image_materialization import TaskImageExecutionGrantV1, resolve_prepared_task
from loom_execution_actuator.renderer import ExecutionTargetRuntime, render_execution_job
from tests.unit.test_execution_actuator import _lease
from tests.unit.test_service_execution_materialization import _REVISION, _provenance
from tests.unit.test_service_execution_terminus_plan import _CONTROLLER, _inputs

_PREPARED_IMAGE = "registry.example/user-task@sha256:" + "f" * 64


def _prepared():
    task, trial, profile = _inputs()
    raw = task.model_dump(mode="json")
    raw["environment"].update(docker_image=None, dockerfile="environment/Dockerfile", cpu_arch="any")
    task = TaskConfig.model_validate(raw)
    grant = TaskImageExecutionGrantV1(
        schema_version="loom.task-image-execution-grant.v1",
        materialization_id=uuid4(), materialization_key="1" * 64,
        cpu_arch="x86_64", task_checksum=_REVISION.removeprefix("sha256:"),
        task_config=task.model_dump(mode="json"), task_source="s3://tasks/task.tar.gz",
        task_source_provenance=_provenance(), registry_images={"task": _PREPARED_IMAGE},
    )
    return task, trial, profile, grant


def _compile(task, trial, profile, grant):
    return compile_service_execution_plan(
        task=task, trial=trial, profile=profile, task_image_grant=grant,
        source_provenance=_provenance(), task_revision_sha256=_REVISION,
    )


def test_ready_task_compiles_and_renders_without_platform_image_publication():
    task, trial, profile, grant = _prepared()
    plan = _compile(task, trial, profile, grant)
    assert plan.task_image_materialization_id == grant.materialization_id
    assert plan.task_image_ref == _PREPARED_IMAGE
    assert plan.agent_image_ref == _CONTROLLER
    assert set(plan.published_image_refs()) == {_CONTROLLER, profile.runtime_image_ref}
    assert task.environment.dockerfile is not None
    assert task.environment.docker_image is None
    resolved = resolve_prepared_task(task, grant)
    assert resolved.environment.cpu_arch == "x86_64"
    assert resolved.environment.dockerfile is None
    lease = _lease()
    lease.runtime_contract_json = plan.canonical_payload()
    lease.runtime_contract_sha256 = canonical_digest(lease.runtime_contract_json)
    lease.workload_requirements_json = workload_requirements_from_task(resolved, trial).model_dump(mode="json")
    lease.workload_requirements_sha256 = canonical_digest(lease.workload_requirements_json)
    rendered = render_execution_job(lease, target=ExecutionTargetRuntime(
        target_id=lease.target_id, namespace=lease.namespace_name,
    ))
    pod = rendered["spec"]["template"]["spec"]
    containers = pod["containers"]
    assert next(item for item in containers if item["name"] == "execution")["image"] == _CONTROLLER
    sandboxes = [item for item in pod["initContainers"] if "sandbox" in item["name"]]
    assert len(sandboxes) == 1
    assert all(item["image"] == _PREPARED_IMAGE for item in sandboxes)


@pytest.mark.parametrize("damage", ["revision", "config", "provenance", "direct"])
def test_prepared_compiler_rejects_an_unrelated_snapshot_or_trusted_task_process(damage):
    task, trial, profile, grant = _prepared()
    if damage == "revision":
        grant = grant.model_copy(update={"task_checksum": "9" * 64})
    elif damage == "config":
        task = task.model_copy(update={"environment": task.environment.model_copy(update={"cpus": 2})})
    elif damage == "provenance":
        grant = grant.model_copy(update={"task_source_provenance": {}})
    else:
        trial = trial.model_copy(update={"agent_name": "direct-completion"})
    with pytest.raises(ValueError, match="frozen task"):
        _compile(task, trial, profile, grant)


def test_old_plan_omits_optional_materialization_identity():
    task, trial, profile = _inputs()
    plan = compile_service_execution_plan(task=task, trial=trial, profile=profile,
        source_provenance=_provenance(), task_revision_sha256=_REVISION)
    assert "task_image_materialization_id" not in plan.canonical_payload()
    assert ExecutionRuntimePlanV1.model_validate(plan.canonical_payload()) == plan


@pytest.mark.parametrize("identity,controller", [(UUID(int=0), _CONTROLLER), (uuid4(), None)])
def test_prepared_plan_requires_nonzero_identity_and_trusted_controller(identity, controller):
    task, trial, profile, grant = _prepared()
    payload = _compile(task, trial, profile, grant).canonical_payload()
    payload.update(task_image_materialization_id=str(identity), agent_image_ref=controller)
    with pytest.raises(ValueError):
        ExecutionRuntimePlanV1.model_validate(payload)


@pytest.mark.parametrize("missing", ["controller", "runtime"])
def test_prepared_image_never_exempts_trusted_platform_images(missing):
    task, trial, profile, grant = _prepared()
    plan = _compile(task, trial, profile, grant)
    ref = _CONTROLLER if missing == "controller" else profile.runtime_image_ref
    bundle = plan.image_admission.model_copy(update={"admissions": tuple(
        item for item in plan.image_admission.admissions if item.statement.image_ref != ref
    )})
    with pytest.raises(ImageAdmissionError):
        validate_execution_image_admission_bundle(bundle, required_image_refs=plan.published_image_refs())
