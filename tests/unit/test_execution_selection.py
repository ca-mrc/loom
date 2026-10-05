"""#2314: one validated selection of harness, network, verification and isolation."""

import pytest

from loom.execution_contract import (
    NEBIUS_CPU_EXECUTION_CLASS_V1,
    NEBIUS_CPU_GUEST_EXECUTION_CLASS_V1,
    IsolationLevel,
    effective_guest_capabilities,
    evaluate_execution_admission,
    workload_requirements_from_task,
)
from loom.execution_runtime_contract import (
    ExecutionRuntimePlanV1,
    validate_runtime_plan_requirements,
)
from loom.models.networking import PublicWeb
from loom.models.task import TaskConfig
from loom.models.trial import TrialConfig
from loom.service_execution_materialization import (
    ServiceExecutionRuntimeProfileV1,
    automatic_service_execution_rejections,
    compile_service_execution_plan,
    execution_selection_rejections,
)
from tests.unit.test_service_execution_materialization import _REVISION, _provenance
from tests.unit.test_service_execution_terminus_plan import _inputs


def _root(task: TaskConfig, **environment) -> TaskConfig:
    raw = task.model_dump(mode="json")
    raw["environment"].update(user="root", **environment)
    raw["verifier"]["user"] = "root"
    return TaskConfig.model_validate(raw)


def _guest_ready(profile: ServiceExecutionRuntimeProfileV1) -> ServiceExecutionRuntimeProfileV1:
    return ServiceExecutionRuntimeProfileV1.model_validate({
        **profile.model_dump(mode="json"), "guest_runtime": "qemu-tcg-v1",
        "supports_task_identity": True, "runtime_volume_mib": 1024,
    })


def _compile(task, trial, profile):
    return compile_service_execution_plan(task=task, trial=trial, profile=profile,
        source_provenance=_provenance(), task_revision_sha256=_REVISION)


def _reasons(task, trial, profile):
    return execution_selection_rejections(task, trial, profile, source_provenance=_provenance())


def test_auto_is_the_default_and_keeps_plans_identical():
    task, trial, profile = _inputs()
    auto = TrialConfig.model_validate({**trial.model_dump(mode="json"), "isolation": "auto"})
    assert auto.isolation is None and auto == trial
    assert _compile(task, auto, profile).canonical_payload() == _compile(task, trial, profile).canonical_payload()


def test_resolver_follows_the_selection_but_never_drops_declared_capabilities():
    task, trial, _ = _inputs()
    guest_task = _root(task, execution_requirements={"capabilities": ["nested_docker"]})
    assert effective_guest_capabilities(task, trial) is None
    assert effective_guest_capabilities(task, trial.model_copy(update={"isolation": "guest"})) == frozenset()
    assert effective_guest_capabilities(guest_task, trial) == {"nested_docker"}
    assert effective_guest_capabilities(guest_task, trial.model_copy(update={"isolation": "guest"})) == {
        "nested_docker"}
    assert effective_guest_capabilities(guest_task, trial.model_copy(update={"isolation": "container"})) is None


def test_forced_guest_on_an_ordinary_task_compiles_a_plain_guest_plan():
    task, trial, profile = _inputs()
    task, profile = _root(task), _guest_ready(profile)
    trial = trial.model_copy(update={"isolation": "guest"})
    assert _reasons(task, trial, profile) == ()
    plan = _compile(task, trial, profile)
    assert plan.execution_class_id == "linux-amd64-cpu-guest-v1"
    assert plan.verifier_execution == "in_attempt"
    for sidecar in plan.canonical_payload()["sidecars"]:
        assert sidecar["guest_execution"]["capabilities"] == []
    requirements = workload_requirements_from_task(task, trial)
    assert requirements.isolation_level == IsolationLevel.DEDICATED_GUEST_KERNEL
    validate_runtime_plan_requirements(plan, requirements)
    with pytest.raises(ValueError, match="guest plan does not match"):
        validate_runtime_plan_requirements(plan, workload_requirements_from_task(task))
    assert ExecutionRuntimePlanV1.model_validate(plan.canonical_payload()) == plan


def test_forced_guest_is_admitted_and_accounted_only_on_the_guest_class():
    task, trial, _ = _inputs()
    task = _root(task)
    forced = workload_requirements_from_task(task, trial.model_copy(update={"isolation": "guest"}))
    guest = evaluate_execution_admission(forced, NEBIUS_CPU_GUEST_EXECUTION_CLASS_V1)
    pod = evaluate_execution_admission(forced, NEBIUS_CPU_EXECUTION_CLASS_V1)
    assert guest.compatible and guest.execution_class_id == "linux-amd64-cpu-guest-v1"
    assert not pod.compatible
    assert "isolation_level_unsupported" in {reason.code for reason in pod.reasons}
    ordinary = workload_requirements_from_task(task, trial)
    assert evaluate_execution_admission(ordinary, NEBIUS_CPU_EXECUTION_CLASS_V1).compatible
    assert not evaluate_execution_admission(ordinary, NEBIUS_CPU_GUEST_EXECUTION_CLASS_V1).compatible


def test_container_cannot_satisfy_declared_guest_capabilities():
    task, trial, profile = _inputs()
    task, profile = _root(task, execution_requirements={"capabilities": ["nested_docker"]}), _guest_ready(profile)
    trial = trial.model_copy(update={"isolation": "container"})
    assert "isolation_container_unsatisfiable" in _reasons(task, trial, profile)
    with pytest.raises(ValueError, match="isolation_container_unsatisfiable"):
        _compile(task, trial, profile)


def test_guest_rejects_response_only_harnesses():
    task, trial, _ = _inputs()
    trial = trial.model_copy(update={"agent_name": "direct-completion", "isolation": "guest"})
    reasons = automatic_service_execution_rejections(task, trial, source_provenance=_provenance())
    assert "isolation_guest_response_only" in reasons
    assert "guest_private_sandboxes_required" not in reasons


def test_guest_rejects_an_explicit_shared_verifier_but_keeps_task_authored_shared_guests():
    task, trial, profile = _inputs()
    task, profile = _root(task), _guest_ready(profile)
    for selected in (
        trial.model_copy(update={"isolation": "guest", "verifier_env_mode": "shared"}),
        trial.model_copy(update={"isolation": "guest"}),
    ):
        shared_task = task.model_copy(update={"verifier": task.verifier.model_copy(update={"env_mode": "shared"})})
        assert "isolation_guest_shared_unsupported" in _reasons(shared_task, selected, profile)
    authored = _root(task, execution_requirements={"capabilities": ["nested_docker"]})
    authored = authored.model_copy(update={"verifier": authored.verifier.model_copy(update={"env_mode": "shared"})})
    reasons = automatic_service_execution_rejections(authored, trial, source_provenance=_provenance(),
                                                     supported_capabilities=frozenset({"nested_docker"}))
    assert "isolation_guest_shared_unsupported" not in reasons
    overridden = trial.model_copy(update={"verifier_env_mode": "shared"})
    assert "isolation_guest_shared_unsupported" in automatic_service_execution_rejections(
        authored, overridden, source_provenance=_provenance(), supported_capabilities=frozenset({"nested_docker"}))


def test_combined_validator_returns_every_reason_across_axes():
    task, trial, profile = _inputs()
    trial = trial.model_copy(update={
        "isolation": "guest", "verifier_env_mode": "shared",
        "baseline_network_policy_override": PublicWeb(), "agent_version": "0.0.0-unpinned",
    })
    reasons = _reasons(task, trial, profile)
    for expected in (
        "isolation_guest_shared_unsupported", "guest_root_identity_required",
        "network_policy_override_not_supported", "guest_runtime_unavailable",
        "agent_version_not_in_runtime_profile",
    ):
        assert expected in reasons
    assert len(reasons) == len(set(reasons))
    assert "runtime_profile_unavailable" in _reasons(task, trial, None)
