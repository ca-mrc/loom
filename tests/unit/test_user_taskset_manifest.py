"""Unit tests for ``UserTaskSetManifest`` (#242 sub-plan 2)."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from loom.models.taskset import UserTaskSetManifest

_MINIMAL = {
    "apiVersion": "loom.taskset/v1",
    "kind": "UserTaskSet",
    "metadata": {"name": "my-tasks", "display_name": "My Tasks"},
    "source": {"type": "hf", "locator": "org/dataset"},
    "instance_mapping": {"prompt": "row.question"},
    "task_template": {
        "task": {"id": "{{ instance.task_id }}", "name": "t"},
        "environment": {"os": "linux"},
        "agent": {"name": "default"},
        "steps": [{"artifacts": ["out.txt"]}],
    },
}


@pytest.mark.parametrize("source_type", ["hf", "git", "https", "jsonl-inline"])
def test_accepts_each_source_type(source_type: str) -> None:
    data = {
        **_MINIMAL,
        "source": {"type": source_type, "locator": "loc"},
    }
    manifest = UserTaskSetManifest.model_validate(data)
    assert manifest.source.type == source_type


@pytest.mark.parametrize("source_type", ["hf", "git", "https", "jsonl-inline"])
@pytest.mark.parametrize("mode", ["python", "json"])
def test_non_bundle_manifest_round_trip_omits_bundle_dialect(source_type: str, mode: str) -> None:
    manifest = UserTaskSetManifest.model_validate({
        **_MINIMAL,
        "source": {"type": source_type, "locator": "loc"},
    })
    dumped = manifest.model_dump(mode=mode, by_alias=True, exclude_none=True)
    assert "task_toml_format" not in dumped["source"]
    assert UserTaskSetManifest.model_validate(dumped) == manifest
    assert UserTaskSetManifest.model_validate_json(manifest.model_dump_json(by_alias=True)) == manifest


@pytest.mark.parametrize("task_toml_format", ["terminal-bench", "loom", "auto"])
def test_bundle_manifest_round_trip_preserves_effective_dialect(task_toml_format: str) -> None:
    manifest = UserTaskSetManifest.model_validate({
        "apiVersion": "loom.taskset/v1",
        "kind": "UserTaskSet",
        "metadata": {"name": "bundle-tasks", "display_name": "Bundle Tasks"},
        "source": {
            "type": "bundle-upload", "locator": "bundle.tar.gz",
            "task_toml_format": task_toml_format,
        },
    })
    restored = UserTaskSetManifest.model_validate_json(manifest.model_dump_json(by_alias=True))
    assert restored.source.task_toml_format == task_toml_format


def test_trajectory_only_without_verifier() -> None:
    manifest = UserTaskSetManifest.model_validate(_MINIMAL)
    assert manifest.verifier is None
    assert manifest.intents is None


def test_both_intents_with_verifier() -> None:
    manifest = UserTaskSetManifest.model_validate({
        **_MINIMAL,
        "intents": ["trajectory_generation", "evaluation"],
        "verifier": {"type": "pytest", "file": "verifier/test.py"},
    })
    assert manifest.intents == ["trajectory_generation", "evaluation"]


def test_rejects_unregistered_manifest_verifier_type() -> None:
    with pytest.raises(ValidationError):
        UserTaskSetManifest.model_validate({
            **_MINIMAL,
            "verifier": {"type": "regex", "file": "verifier/pattern.txt"},
        })


def test_rejects_extra_top_level_field() -> None:
    with pytest.raises(ValidationError):
        UserTaskSetManifest.model_validate({**_MINIMAL, "extra": True})


def test_rejects_missing_required_fields() -> None:
    with pytest.raises(ValidationError):
        UserTaskSetManifest.model_validate({"apiVersion": "loom.taskset/v1"})


def test_evaluation_without_verifier_rejected() -> None:
    with pytest.raises(ValidationError, match="verifier_required_for_evaluation"):
        UserTaskSetManifest.model_validate({
            **_MINIMAL,
            "intents": ["evaluation"],
        })


def test_bundle_upload_omitted_task_toml_format_stays_auto() -> None:
    manifest = UserTaskSetManifest.model_validate({
        "apiVersion": "loom.taskset/v1",
        "kind": "UserTaskSet",
        "metadata": {"name": "bundle-tasks", "display_name": "Bundle Tasks"},
        "source": {"type": "bundle-upload", "locator": "bundle.tar.gz"},
    })
    assert manifest.source.task_toml_format == "auto"
    assert "task_toml_format" not in manifest.source.model_fields_set


@pytest.mark.parametrize("task_toml_format", ["terminal-bench", "loom", "auto"])
def test_bundle_upload_accepts_task_toml_format(task_toml_format: str) -> None:
    manifest = UserTaskSetManifest.model_validate({
        "apiVersion": "loom.taskset/v1",
        "kind": "UserTaskSet",
        "metadata": {"name": "bundle-tasks", "display_name": "Bundle Tasks"},
        "source": {
            "type": "bundle-upload",
            "locator": "bundle.tar.gz",
            "task_toml_format": task_toml_format,
        },
    })
    assert manifest.source.task_toml_format == task_toml_format


def test_bundle_upload_rejects_unknown_task_toml_format() -> None:
    with pytest.raises(ValidationError):
        UserTaskSetManifest.model_validate({
            "apiVersion": "loom.taskset/v1",
            "kind": "UserTaskSet",
            "metadata": {"name": "bundle-tasks", "display_name": "Bundle Tasks"},
            "source": {
                "type": "bundle-upload",
                "locator": "bundle.tar.gz",
                "task_toml_format": "harbor",
            },
        })


@pytest.mark.parametrize("task_toml_format", ["terminal-bench", "loom", "auto"])
def test_non_bundle_source_rejects_task_toml_format(task_toml_format: str) -> None:
    with pytest.raises(ValidationError, match="task_toml_format_only_for_bundle_upload"):
        UserTaskSetManifest.model_validate({
            **_MINIMAL,
            "source": {
                "type": "hf",
                "locator": "org/dataset",
                "task_toml_format": task_toml_format,
            },
        })


def test_bundle_upload_evaluation_uses_per_task_verifiers() -> None:
    manifest = UserTaskSetManifest.model_validate({
        "apiVersion": "loom.taskset/v1",
        "kind": "UserTaskSet",
        "metadata": {"name": "bundle-tasks", "display_name": "Bundle Tasks"},
        "intents": ["evaluation"],
        "source": {
            "type": "bundle-upload",
            "locator": "bundle.tar.gz",
            "subset": "tasks",
        },
    })
    assert manifest.source.type == "bundle-upload"
    assert manifest.instance_mapping == {}
    assert manifest.task_template == {}
    assert manifest.verifier is None


@pytest.mark.parametrize("bad_locator", [
    "../bundle.tar.gz",
    "/tmp/bundle.tar.gz",
    "bundle.zip",
])
def test_bundle_upload_rejects_unsafe_or_unsupported_locator(bad_locator: str) -> None:
    with pytest.raises(ValidationError):
        UserTaskSetManifest.model_validate({
            "apiVersion": "loom.taskset/v1",
            "kind": "UserTaskSet",
            "metadata": {"name": "bundle-tasks", "display_name": "Bundle Tasks"},
            "source": {"type": "bundle-upload", "locator": bad_locator},
        })


def test_non_bundle_sources_require_row_mapping_and_template() -> None:
    with pytest.raises(ValidationError, match="instance_mapping_required"):
        UserTaskSetManifest.model_validate({
            "apiVersion": "loom.taskset/v1",
            "kind": "UserTaskSet",
            "metadata": {"name": "row-tasks", "display_name": "Row Tasks"},
            "source": {"type": "jsonl-inline", "locator": "{\"id\":\"1\"}"},
        })


@pytest.mark.parametrize("bad_path", [
    "../escape.py",
    "/abs/verifier.py",
    "verifier/../../etc/passwd",
])
def test_rejects_unsafe_verifier_file_path(bad_path: str) -> None:
    with pytest.raises(ValidationError):
        UserTaskSetManifest.model_validate({
            **_MINIMAL,
            "verifier": {"type": "pytest", "file": bad_path},
        })


@pytest.mark.parametrize("bad_path", [
    "../transform.py",
    "/transform.py",
])
def test_rejects_unsafe_transform_file_path(bad_path: str) -> None:
    with pytest.raises(ValidationError):
        UserTaskSetManifest.model_validate({
            **_MINIMAL,
            "transform": {"file": bad_path},
        })


@pytest.mark.parametrize("bad_slug", ["../escape", "has/slash", "has.dot", ""])
def test_rejects_path_traversal_slug(bad_slug: str) -> None:
    with pytest.raises(ValidationError):
        UserTaskSetManifest.model_validate({
            **_MINIMAL,
            "metadata": {"name": bad_slug, "display_name": "X"},
        })
