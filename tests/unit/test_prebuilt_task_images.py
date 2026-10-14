from __future__ import annotations

import pytest

from loom.service_execution_materialization import (
    ServiceExecutionRuntimeProfileV1,
    compile_service_execution_plan,
    execution_selection_rejections,
)
from tests.support.execution_image_admission import signed_image_admission_bundle
from tests.unit.test_service_execution_materialization import (
    _REVISION,
    _RUNTIME_IMAGE,
    _TASK_IMAGE,
    _profile,
    _provenance,
    _task,
    _trial,
)

TAG = "ghcr.io/terminal-bench/task:rev6"
IMAGE = "ghcr.io/terminal-bench/task@sha256:" + "7" * 64
CONTROLLER = "registry.example/controller@sha256:" + "9" * 64


def profile(pins=None):
    payload = _profile().model_dump(mode="json")
    payload.update(
        agent_image_ref=CONTROLLER,
        prebuilt_image_pins=pins or {TAG: IMAGE},
        image_admission=signed_image_admission_bundle(
            (_TASK_IMAGE, _RUNTIME_IMAGE, CONTROLLER, IMAGE)
        ).model_dump(mode="json"),
    )
    return ServiceExecutionRuntimeProfileV1.model_validate(payload)


def test_submission_and_compiler_freeze_admitted_pin_without_changing_source():
    task = _task()
    task = task.model_copy(
        update={"environment": task.environment.model_copy(update={"docker_image": TAG})}
    )
    original = task.model_dump(mode="json")
    trial = _trial().model_copy(update={"agent_name": "terminus-2"})
    assert (
        execution_selection_rejections(task, trial, profile(), source_provenance=_provenance())
        == ()
    )
    plan = compile_service_execution_plan(
        task=task,
        trial=trial,
        profile=profile(),
        source_provenance=_provenance(),
        task_revision_sha256=_REVISION,
    )
    assert plan.task_image_ref == IMAGE
    assert IMAGE in {row.statement.image_ref for row in plan.image_admission.admissions}
    assert task.model_dump(mode="json") == original
    # Equivalent raw-digest config is distinguishable from a resolved source-tag execution.
    digest_task = task.model_copy(
        update={"environment": task.environment.model_copy(update={"docker_image": IMAGE})}
    )
    direct = compile_service_execution_plan(
        task=digest_task,
        trial=trial,
        profile=profile(),
        source_provenance=_provenance(),
        task_revision_sha256=_REVISION,
    )
    assert plan.command_identity_sha256 != direct.command_identity_sha256


def test_unmapped_tag_keeps_existing_admission_failure():
    task = _task()
    task = task.model_copy(
        update={
            "environment": task.environment.model_copy(update={"docker_image": TAG + "-changed"})
        }
    )
    trial = _trial().model_copy(update={"agent_name": "terminus-2"})
    reasons = execution_selection_rejections(
        task, trial, profile(), source_provenance=_provenance()
    )
    assert "immutable_task_image_required" in reasons
    assert "task_image_not_in_runtime_profile" in reasons


def test_unsigned_pin_and_implicit_latest_are_rejected():
    with pytest.raises(ValueError, match="admission coverage"):
        profile({TAG: IMAGE.replace("7" * 64, "8" * 64)})
    with pytest.raises(ValueError, match="explicit tags"):
        profile({"ghcr.io/terminal-bench/task": IMAGE})


def test_empty_pin_map_preserves_historical_profile_serialization():
    payload = _profile().model_dump(mode="json")
    assert "prebuilt_image_pins" not in payload
    assert (
        ServiceExecutionRuntimeProfileV1.model_validate(payload).model_dump(mode="json") == payload
    )
