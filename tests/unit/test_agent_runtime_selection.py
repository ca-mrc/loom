from copy import deepcopy
from uuid import uuid4

import pytest
from pydantic import ValidationError

from loom.agent_runtime import AgentRuntimeReleaseV1
from loom.models.batch import Combination
from loom.models.task import TaskConfig
from loom.models.trial import TrialConfig
from loom.service_execution_materialization import (
    ControllerComputeResourcesV1,
    compile_service_execution_plan,
    freeze_agent_runtime_releases,
    runtime_profile_rejections,
)
from loom.task_image_materialization import TaskImageExecutionGrantV1
from loom_service.batch_runner import _materialize_trial_config
from tests.support.agent_runtime import release
from tests.support.execution_image_admission import signed_image_admission_bundle
from tests.unit.test_service_execution_materialization import _profile, _provenance, _task, _trial


@pytest.mark.parametrize("same_controller", [False, True])
def test_large_artifact_transport_requires_the_qualified_controller(same_controller: bool) -> None:
    runtime = release()
    profile = _profile().model_copy(update={
        "supports_task_artifact_inputs": True,
        "agent_image_ref": runtime.agent_image_ref if same_controller else None,
    })
    profile = freeze_agent_runtime_releases(profile, (runtime,))
    task = _task()
    task = task.model_copy(update={"steps": [task.steps[0].model_copy(update={
        "artifacts": [f"out/part-{index:04}.json" for index in range(515)],
    })]})
    trial = _trial().model_copy(update={"agent_name": "terminus-2", "agent_version": runtime.agent_version})
    arguments = dict(task=task, trial=trial, task_revision_sha256="sha256:" + "c" * 64,
                     source_provenance=_provenance(), profile=profile)
    if not same_controller:
        with pytest.raises(ValidationError, match="process environment value is invalid"):
            compile_service_execution_plan(**arguments)
    else:
        plan = compile_service_execution_plan(**arguments)
        assert plan.main.environment["LOOM_TASK_ARTIFACTS_FROM_INPUT"] == "1"


@pytest.mark.parametrize("independent_controller", [False, True])
def test_versions_select_exact_controller_and_frozen_trial_json(
    independent_controller: bool,
) -> None:
    a, b = release(), release("harbor-b", "9")
    profile = freeze_agent_runtime_releases(_profile(), (a, b))
    legacy_profile = profile
    if independent_controller:
        profile = profile.model_copy(update={
            "controller_resources": ControllerComputeResourcesV1(cpu_millis=1000, memory_mib=2048),
        })
    plans = []
    for runtime in (a, b):
        combo = Combination(
            agent_name="terminus-2",
            agent_version=runtime.agent_version,
            agent_model=_trial().agent_model,
        )
        trial = TrialConfig.model_validate(_materialize_trial_config({}, combo.model_dump()))
        plan = compile_service_execution_plan(
            task=_task(),
            trial=trial,
            task_revision_sha256="sha256:" + "c" * 64,
            source_provenance=_provenance(),
            profile=profile,
        )
        assert plan.agent_image_ref == runtime.agent_image_ref
        assert plan.runtime_image_ref == profile.runtime_image_ref
        assert (plan.controller_resources is not None) == independent_controller
        old_plan = compile_service_execution_plan(
            task=_task(), trial=trial, task_revision_sha256="sha256:" + "c" * 64,
            source_provenance=_provenance(), profile=legacy_profile,
        )
        # The runtime image supplies the Go plan reader. An older, explicitly
        # selected Harbor image receives the same Python argv/task/trial inputs;
        # controller_resources is never injected into those versioned inputs.
        assert plan.main == old_plan.main
        assert plan.verifier == old_plan.verifier
        assert plan.task_input == old_plan.task_input
        assert plan.task_image_ref == _task().environment.docker_image
        assert all(sidecar.image_ref == plan.task_image_ref for sidecar in plan.sidecars)
        assert trial.agent_version in plan.main.environment["LOOM_TASK_TRIAL_JSON"]
        assert {a.statement.image_ref for a in plan.image_admission.admissions} == set(
            plan.published_image_refs()
        )
        plans.append(plan)
    assert plans[0].command_identity_sha256 != plans[1].command_identity_sha256
    assert profile.agent_image_ref is None  # Explicit binding does not require changing default.


def test_unknown_version_never_falls_back_and_legacy_remains_default() -> None:
    a = release()
    profile = _profile().model_copy(
        update={
            "agent_image_ref": a.agent_image_ref,
            "image_admission": signed_image_admission_bundle(
                (_profile().task_image_ref, _profile().runtime_image_ref, a.agent_image_ref)
            ),
        }
    )
    legacy = _trial().model_copy(update={"agent_name": "terminus-2"})
    assert runtime_profile_rejections(_task(), legacy, profile) == ()
    wrong = legacy.model_copy(update={"agent_version": "missing"})
    assert runtime_profile_rejections(_task(), wrong, profile) == (
        "agent_version_not_in_runtime_profile",
    )
    with pytest.raises(ValueError):
        compile_service_execution_plan(
            task=_task(),
            trial=wrong,
            task_revision_sha256="sha256:" + "c" * 64,
            source_provenance=_provenance(),
            profile=profile,
        )


def test_prepared_image_keeps_selected_controller_admission() -> None:
    a = release()
    profile = freeze_agent_runtime_releases(_profile(), (a,))
    raw = _task().model_dump(mode="json")
    raw["environment"].update(docker_image=None, dockerfile="Dockerfile")
    task = TaskConfig.model_validate(raw)
    trial = _trial().model_copy(
        update={"agent_name": "terminus-2", "agent_version": a.agent_version}
    )
    grant = TaskImageExecutionGrantV1(
        schema_version="loom.task-image-execution-grant.v1",
        materialization_id=uuid4(),
        materialization_key="f" * 64,
        cpu_arch="x86_64",
        task_checksum="c" * 64,
        task_config=task.model_dump(mode="json"),
        task_source=None,
        task_source_provenance=_provenance(),
        registry_images={"task": "registry.example/task@sha256:" + "d" * 64},
    )
    plan = compile_service_execution_plan(
        task=task,
        trial=trial,
        task_revision_sha256="sha256:" + "c" * 64,
        source_provenance=_provenance(),
        profile=profile,
        task_image_grant=grant,
    )
    assert plan.agent_image_ref == a.agent_image_ref
    assert grant.registry_images["task"] not in plan.published_image_refs()
    assert a.agent_image_ref in plan.published_image_refs()


def test_release_rejects_user_image_subject_and_bad_versions() -> None:
    raw = release().model_dump(mode="json")
    raw["agent_image_ref"] = "registry.example/other@sha256:" + "4" * 64
    with pytest.raises(ValidationError, match="subject"):
        AgentRuntimeReleaseV1.model_validate(raw)
    for value in ("https://user/image", "", "a" * 129):
        bad = deepcopy(release().model_dump(mode="json"))
        bad["agent_version"] = value
        with pytest.raises(ValidationError):
            AgentRuntimeReleaseV1.model_validate(bad)


def test_admissions_deduplicate_for_two_labels_of_same_image() -> None:
    profile = freeze_agent_runtime_releases(_profile(), (release(), release("alias")))
    refs = [a.statement.image_ref for a in profile.image_admission.admissions]
    assert len(refs) == len(set(refs))
