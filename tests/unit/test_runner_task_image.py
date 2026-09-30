"""#2054: direct-completion tasks may leave `docker_image` unset and run in the
platform runner image frozen into each execution plan, so ordinary service
upgrades stop invalidating compatible response-only tasks."""

from __future__ import annotations

import io
import tarfile
import tomllib
from pathlib import Path, PurePosixPath

import pytest

from loom.agent_model_acceptance_taskset import (
    TASK_ID,
    AgentModelAcceptanceTaskSetError,
    build_response_only_taskset,
)
from loom.execution_contract import workload_requirements_from_task
from loom.execution_runtime_contract import validate_runtime_plan_requirements
from loom.models.task import TaskConfig
from loom.service_execution_materialization import (
    automatic_service_execution_rejections,
    compile_service_execution_plan,
    resolve_runner_task_image,
    runtime_profile_rejections,
    uses_runner_task_image,
)
from tests.unit.test_service_execution_materialization import (
    _profile,
    _provenance,
    _task,
    _trial,
)

_OTHER_IMAGE = "registry.example/loom-service@sha256:" + "7" * 64


def _with_env(task: TaskConfig, **env: object) -> TaskConfig:
    return task.model_copy(
        update={"environment": task.environment.model_copy(update=env)},
    )


def _runner_task() -> TaskConfig:
    return _with_env(_task(), docker_image=None)


@pytest.mark.parametrize("agent_name", ["direct-completion", "litellm"])
def test_runner_image_task_is_admitted(agent_name: str) -> None:
    trial = _trial().model_copy(update={"agent_name": agent_name})
    task = _runner_task()

    assert uses_runner_task_image(task, trial)
    reasons = automatic_service_execution_rejections(task, trial, source_provenance=_provenance())
    assert "immutable_task_image_required" not in reasons
    assert runtime_profile_rejections(task, trial, _profile()) == ()


def test_plan_freezes_runner_image_and_matches_resolved_requirements() -> None:
    task, trial, profile = _runner_task(), _trial(), _profile()

    plan = compile_service_execution_plan(
        task=task,
        trial=trial,
        task_revision_sha256="sha256:" + "c" * 64,
        source_provenance=_provenance(),
        profile=profile,
    )

    assert plan.task_image_ref == profile.task_image_ref
    # What the Control Plane checks at reservation time.
    resolved = resolve_runner_task_image(task, plan.task_image_ref)
    assert resolved.environment.docker_image == plan.task_image_ref
    validate_runtime_plan_requirements(plan, workload_requirements_from_task(resolved, trial))
    # The stored task itself is never rewritten.
    assert task.environment.docker_image is None


def test_after_an_upgrade_the_same_task_still_runs() -> None:
    upgraded = _profile().model_copy(update={"task_image_ref": _OTHER_IMAGE})

    assert runtime_profile_rejections(_runner_task(), _trial(), upgraded) == ()


def test_pinned_image_keeps_exact_semantics() -> None:
    pinned = _with_env(_task(), docker_image=_OTHER_IMAGE)

    assert not uses_runner_task_image(pinned, _trial())
    assert runtime_profile_rejections(pinned, _trial(), _profile()) == (
        "task_image_not_in_runtime_profile",
    )
    assert resolve_runner_task_image(pinned, _profile().task_image_ref) is pinned


def test_dockerfile_task_is_not_silently_run_in_the_runner_image() -> None:
    built = _with_env(_task(), docker_image=None, dockerfile=PurePosixPath("Dockerfile"))

    assert not uses_runner_task_image(built, _trial())
    reasons = automatic_service_execution_rejections(built, _trial(), source_provenance=_provenance())
    assert "immutable_task_image_required" in reasons


def test_terminus_still_requires_a_task_image() -> None:
    trial = _trial().model_copy(update={"agent_name": "terminus-2"})

    assert not uses_runner_task_image(_runner_task(), trial)
    reasons = automatic_service_execution_rejections(
        _runner_task(), trial, source_provenance=_provenance(),
    )
    assert "immutable_task_image_required" in reasons


# --- the response-only acceptance fixture ---------------------------------


def _bundle_task(output: Path) -> TaskConfig:
    with tarfile.open(fileobj=io.BytesIO((output / "bundle.tar.gz").read_bytes())) as archive:
        member = archive.extractfile("tasks/response-only-arithmetic/task.toml")
        assert member is not None
        return TaskConfig.model_validate(tomllib.loads(member.read().decode()))


def test_fixture_defaults_to_runner_image_and_is_admitted(tmp_path: Path) -> None:
    evidence = build_response_only_taskset(output_dir=tmp_path / "ts")

    task = _bundle_task(tmp_path / "ts")
    assert task.task.id == TASK_ID
    assert task.environment.docker_image is None
    assert task.steps[0].required_artifacts == ["answer.txt"]
    assert evidence["task_image_ref"] is None
    reasons = automatic_service_execution_rejections(task, _trial(), source_provenance=_provenance())
    assert "immutable_task_image_required" not in reasons
    assert runtime_profile_rejections(task, _trial(), _profile()) == ()


def test_fixture_can_pin_an_exact_image(tmp_path: Path) -> None:
    build_response_only_taskset(output_dir=tmp_path / "ts", task_image_ref=_OTHER_IMAGE)

    assert _bundle_task(tmp_path / "ts").environment.docker_image == _OTHER_IMAGE


def test_fixture_is_deterministic(tmp_path: Path) -> None:
    first = build_response_only_taskset(output_dir=tmp_path / "a")
    second = build_response_only_taskset(output_dir=tmp_path / "b")

    assert first["bundle_sha256"] == second["bundle_sha256"]


def test_fixture_refuses_a_non_empty_output(tmp_path: Path) -> None:
    (tmp_path / "ts").mkdir()
    (tmp_path / "ts" / "leftover").write_text("x")

    with pytest.raises(AgentModelAcceptanceTaskSetError):
        build_response_only_taskset(output_dir=tmp_path / "ts")
