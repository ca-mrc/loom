"""Resolver for shared vs separate grading."""

import pytest

from loom.models.task import TaskConfig
from loom.models.trial import TrialConfig
from loom.models.types import ModelSpec
from loom.verifier_runtime import apply_legacy_verifier_default, resolve_verifier_env_mode


def _task(env_mode: str = "separate") -> TaskConfig:
    return TaskConfig.model_validate({
        "task": {"id": "task-1", "name": "mode"},
        "environment": {"os": "linux", "cpu_arch": "x86_64", "gpu_vendor": "none"},
        "agent": {"name": "oracle"},
        "verifier": {"name": "script", "env_mode": env_mode},
    })


def _trial(**updates: object) -> TrialConfig:
    return TrialConfig(
        agent_name="terminus-2",
        agent_model=ModelSpec(provider="openai", name="glm-5.2"),
        **updates,
    )


def test_omitted_trial_override_uses_task_mode() -> None:
    assert resolve_verifier_env_mode(_task("separate"), _trial()) == "separate"
    assert resolve_verifier_env_mode(_task("shared"), _trial()) == "shared"


def test_batch_override_wins_over_task() -> None:
    assert resolve_verifier_env_mode(
        _task("separate"), _trial(verifier_env_mode="shared"),
    ) == "shared"
    assert resolve_verifier_env_mode(
        _task("shared"), _trial(verifier_env_mode="separate"),
    ) == "separate"


@pytest.mark.parametrize("prefix", ["", "sha256:"])
def test_repaired_legacy_revision_freezes_separate_in_trial(prefix: str) -> None:
    task, trial = _task("shared"), _trial()
    original = task.model_dump(mode="json")
    effective = apply_legacy_verifier_default(
        task, trial, task_checksum=prefix + "a" * 64,
        legacy_separate_verifier_checksum="a" * 64, source_provenance={},
    )
    assert effective.verifier_env_mode == "separate"
    assert resolve_verifier_env_mode(task, effective) == "separate"
    assert task.model_dump(mode="json") == original
    assert trial.verifier_env_mode is None
    assert TrialConfig.model_validate(effective.model_dump(mode="json")) == effective


@pytest.mark.parametrize("override", ["shared", "separate"])
def test_explicit_mode_wins_over_legacy_compatibility(override: str) -> None:
    trial = _trial(verifier_env_mode=override)
    assert apply_legacy_verifier_default(
        _task("shared"), trial, task_checksum="a" * 64,
        legacy_separate_verifier_checksum="a" * 64, source_provenance={},
    ) is trial


@pytest.mark.parametrize("marker,checksum,provenance", [
    (None, "a" * 64, {}),
    ("a" * 64, "b" * 64, {}),
    ("a" * 64, "a" * 64, {"bundle_content_manifest_sha256": "c" * 64}),
    ("a" * 64, "a" * 64, {"bundle_content_manifest_sha256": ""}),
])
def test_legacy_default_never_leaks_to_new_or_manifest_revision(marker, checksum, provenance) -> None:
    task, trial = _task("shared"), _trial()
    assert apply_legacy_verifier_default(
        task, trial, task_checksum=checksum,
        legacy_separate_verifier_checksum=marker, source_provenance=provenance,
    ) is trial
    assert resolve_verifier_env_mode(task, trial) == "shared"
