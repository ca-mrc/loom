"""Submit-time admission covers the independent grading environment too."""
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from fastapi import HTTPException

from loom.models.networking import GatewayOnly, NoNetwork, Public, PublicWeb
from loom.models.task import EnvironmentConfig
from loom.service_execution_materialization import (
    ServiceExecutionRuntimeProfileV1,
    execution_selection_rejections,
)
from loom_service.execution_admission import admit_execution_backend
from tests.unit.test_service_execution_materialization import _provenance
from tests.unit.test_service_execution_terminus_plan import _inputs


def _inputs_with_verifier(policy=None):
    task, trial, profile = _inputs()
    env = EnvironmentConfig(
        os="linux", docker_image=task.environment.docker_image,
        cpus=3, memory_mb=6144, storage_mb=12288,
        baseline_network_policy=policy or NoNetwork(),
    )
    task = task.model_copy(update={"verifier": task.verifier.model_copy(update={
        "env_mode": "separate", "environment": env,
    })})
    return task, trial, profile


async def test_submission_rejects_verifier_public_before_creating_execution(monkeypatch):
    task, trial, profile = _inputs_with_verifier(Public())
    rows = Mock()
    rows.all.return_value = [("task", task.model_dump(mode="json"), _provenance(), "a" * 64, None,
                             "s3://artifacts/tasks/task/")]
    session = SimpleNamespace(execute=AsyncMock(return_value=rows))
    monkeypatch.setattr("loom_service.execution_admission.get_service_execution_backend_pools",
                        AsyncMock(return_value=[SimpleNamespace(pool_name="nebius-cpu")]))
    with pytest.raises(HTTPException) as error:
        await admit_execution_backend(
            session, backend="nebius", task_ids=["task"],
            trial_config=trial.model_dump(mode="json"), combinations=[],
            runtime_profile_json=profile.model_dump_json(), resolve_versions=False,
        )
    assert error.value.status_code == 400
    assert error.value.detail["rejection_reasons"]["task"] == [
        "verifier.environment: network_access_unsupported",
    ]


@pytest.mark.parametrize("policy", [NoNetwork(), GatewayOnly(), PublicWeb()])
def test_supported_verifier_network_is_admitted_without_rewriting_task(policy):
    task, trial, profile = _inputs_with_verifier(policy)
    if isinstance(policy, PublicWeb):
        profile = ServiceExecutionRuntimeProfileV1.model_validate({
            **profile.model_dump(mode="json"), "supports_task_web_egress": True,
            "execution_class_id": "linux-amd64-cpu-web-pod-v1",
        })
    before = task.model_dump_json()
    assert not execution_selection_rejections(task, trial, profile, source_provenance=_provenance())
    assert task.model_dump_json() == before


def test_verifier_web_requires_deployment_egress_support():
    task, trial, profile = _inputs_with_verifier(PublicWeb())
    assert execution_selection_rejections(task, trial, profile, source_provenance=_provenance()) == (
        "verifier.environment: task_web_egress_unsupported",
    )


def test_verifier_gpu_requirement_is_not_hidden_by_cpu_agent():
    task, trial, profile = _inputs_with_verifier()
    env = task.verifier.environment.model_copy(update={"gpu_vendor": "nvidia", "gpus": 1})
    task = task.model_copy(update={"verifier": task.verifier.model_copy(update={"environment": env})})
    assert "verifier.environment: gpu_unsupported" in execution_selection_rejections(
        task, trial, profile, source_provenance=_provenance(),
    )


def test_verifier_build_is_admitted_only_for_preparation():
    task, trial, profile = _inputs_with_verifier()
    env = task.verifier.environment.model_copy(update={"docker_image": None,
                                                       "dockerfile": "tests/Dockerfile"})
    task = task.model_copy(update={"verifier": task.verifier.model_copy(update={"environment": env})})
    assert not execution_selection_rejections(
        task, trial, profile, source_provenance=_provenance(), allow_task_image_preparation=True,
    )
    reasons = execution_selection_rejections(task, trial, profile, source_provenance=_provenance())
    assert "verifier.environment.dockerfile: prepared_image_required" in reasons
    assert "verifier.environment: immutable_image_required" in reasons


def test_mutable_verifier_image_is_not_admitted_as_prepared_build():
    task, trial, profile = _inputs_with_verifier()
    env = task.verifier.environment.model_copy(update={"docker_image": "registry.example/verifier:latest"})
    task = task.model_copy(update={"verifier": task.verifier.model_copy(update={"environment": env})})
    assert "verifier.environment: immutable_image_required" in execution_selection_rejections(
        task, trial, profile, source_provenance=_provenance(), allow_task_image_preparation=True,
    )


def test_skipped_verifier_does_not_reject_unused_network_or_gpu():
    task, trial, profile = _inputs_with_verifier(Public())
    env = task.verifier.environment.model_copy(update={"gpu_vendor": "nvidia", "gpus": 1})
    task = task.model_copy(update={"verifier": task.verifier.model_copy(update={"environment": env})})
    trial = trial.model_copy(update={"skip_verifier": True})
    assert not execution_selection_rejections(task, trial, profile, source_provenance=_provenance())


@pytest.mark.parametrize("user", ["root", "image"])
def test_verifier_identity_requires_ready_runtime_even_for_standard_agent(user):
    task, trial, profile = _inputs_with_verifier()
    env = task.verifier.environment.model_copy(update={
        "user": user, "docker_image": None, "dockerfile": "tests/Dockerfile",
    })
    task = task.model_copy(update={"verifier": task.verifier.model_copy(update={"environment": env})})
    profile = profile.model_copy(update={"supports_task_identity": False})
    assert "task_identity_runtime_unavailable" in execution_selection_rejections(
        task, trial, profile, source_provenance=_provenance(), allow_task_image_preparation=True,
    )


@pytest.mark.parametrize("updates", [{"cpus": 129}, {"memory_mb": 1_048_577}, {"storage_mb": 1_048_577}])
def test_verifier_resource_limits_are_rejected_before_plan_compilation(updates):
    task, trial, profile = _inputs_with_verifier()
    env = task.verifier.environment.model_copy(update=updates)
    task = task.model_copy(update={"verifier": task.verifier.model_copy(update={"environment": env})})
    assert "verifier.environment: resource_limits_out_of_range" in execution_selection_rejections(
        task, trial, profile, source_provenance=_provenance(),
    )


def test_compose_guest_agent_keeps_independent_verifier_in_ordinary_class():
    from loom.models.compose import ComposeServiceSpec, NativeComposeSpec
    from tests.unit.test_guest_execution_materialization import _guest_inputs

    task, trial, _ = _inputs_with_verifier()
    _, _, profile = _guest_inputs()
    profile = profile.model_copy(update={"supports_native_compose": True})
    environment = task.environment.model_copy(update={
        "native_compose": NativeComposeSpec(
            source="environment/compose.yaml", services=(
                ComposeServiceSpec(name="main", image=task.environment.docker_image),
                ComposeServiceSpec(name="database", image=task.environment.docker_image),
            ),
        ),
    })
    task = task.model_copy(update={"environment": environment})
    assert not execution_selection_rejections(task, trial, profile, source_provenance=_provenance())


def test_hosted_network_override_applies_to_verifier_class_validation():
    task, trial, profile = _inputs_with_verifier(Public())
    trial = trial.model_copy(update={"baseline_network_policy_override": GatewayOnly()})
    assert not execution_selection_rejections(task, trial, profile, source_provenance=_provenance())
