"""Unit tests for TaskSet intent normalization (#242 sub-plan 2)."""

from __future__ import annotations

import pytest

from loom.models.taskset import TaskSetLimits, UserTaskSetManifest
from loom.taskset.intents import normalize_intents

_BASE = {
    "apiVersion": "loom.taskset/v1",
    "kind": "UserTaskSet",
    "metadata": {"name": "my-tasks", "display_name": "My Tasks"},
    "source": {"type": "https", "locator": "https://example/data.jsonl"},
    "instance_mapping": {"prompt": "row.q"},
    "task_template": {
        "task": {"id": "1", "name": "t"},
        "environment": {"os": "linux"},
        "agent": {"name": "default"},
        "steps": [{}],
    },
}


def test_defaults_to_trajectory_generation() -> None:
    manifest = UserTaskSetManifest.model_validate(_BASE)
    normalized = normalize_intents(manifest, verifier_file_present=False)
    assert normalized.manifest_intents == ["trajectory_generation"]
    assert normalized.effective_intents == ["trajectory_generation"]
    assert normalized.capabilities == ["trajectory-only"]


def test_verifier_infers_evaluation_when_file_present() -> None:
    manifest = UserTaskSetManifest.model_validate({
        **_BASE,
        "verifier": {"type": "pytest", "file": "verifier/test.py"},
    })
    normalized = normalize_intents(manifest, verifier_file_present=True)
    assert normalized.manifest_intents == ["trajectory_generation"]
    assert normalized.effective_intents == ["evaluation", "trajectory_generation"]
    assert normalized.inferred_intents == ["evaluation"]
    assert normalized.capabilities == ["both"]
    assert normalized.warnings[0].code == "evaluation_inferred_from_verifier"


def test_explicit_evaluation_not_inferred() -> None:
    manifest = UserTaskSetManifest.model_validate({
        **_BASE,
        "intents": ["trajectory_generation", "evaluation"],
        "verifier": {"type": "script", "file": "v.sh"},
    })
    normalized = normalize_intents(manifest, verifier_file_present=True)
    assert normalized.inferred_intents == []
    assert [warning.code for warning in normalized.warnings] == ["default_max_instances_applied"]
    assert normalized.capabilities == ["both"]


@pytest.mark.parametrize("overrides", [
    {}, {"limits": None}, {"limits": {}}, {"limits": {"timeout_per_task_s": 10}},
])
def test_omitted_max_instances_warns_with_effective_selection_limit(overrides) -> None:
    manifest = UserTaskSetManifest.model_validate({**_BASE, **overrides})
    normalized = normalize_intents(manifest, verifier_file_present=False)
    assert [warning.code for warning in normalized.warnings] == ["default_max_instances_applied"]
    message = normalized.warnings[0].message
    assert f"at most {TaskSetLimits().max_instances} tasks" in message
    assert "limits.max_instances" in message
    assert "explicitly" in message


@pytest.mark.parametrize("max_instances", [TaskSetLimits().max_instances, 1000])
def test_explicit_max_instances_does_not_warn(max_instances: int) -> None:
    manifest = UserTaskSetManifest.model_validate({**_BASE, "limits": {"max_instances": max_instances}})
    assert normalize_intents(manifest, verifier_file_present=False).warnings == ()
