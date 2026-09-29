"""Resolver for shared vs separate grading."""

from loom.models.task import TaskConfig
from loom.models.trial import TrialConfig
from loom.models.types import ModelSpec
from loom.verifier_runtime import resolve_verifier_env_mode


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
