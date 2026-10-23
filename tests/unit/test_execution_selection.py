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
from loom.execution_selection import (
    ExecutionSelectionV1,
    apply_verification_selection,
    execution_selection_readback,
    requested_verification,
    resolved_execution_selection,
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
from loom.terminal_result_semantics import explicit_skip_verifier
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
    raw = _root(task).model_dump(mode="json")
    raw["verifier"]["user"] = None
    task, profile = TaskConfig.model_validate(raw), _guest_ready(profile)
    trial = trial.model_copy(update={"isolation": "guest"})
    assert task.environment.user == "root" and task.verifier.user is None
    assert _reasons(task, trial, profile) == ()
    plan = _compile(task, trial, profile)
    assert plan.execution_class_id == "linux-amd64-cpu-guest-v1"
    assert plan.verifier_execution == "in_attempt"
    for sidecar in plan.canonical_payload()["sidecars"]:
        assert sidecar["guest_execution"]["capabilities"] == []
        assert sidecar["identity"]["run_as_user"] == sidecar["identity"]["run_as_group"] == 0
    requirements = workload_requirements_from_task(task, trial)
    assert requirements.isolation_level == IsolationLevel.DEDICATED_GUEST_KERNEL
    validate_runtime_plan_requirements(plan, requirements)
    with pytest.raises(ValueError, match="guest plan does not match"):
        validate_runtime_plan_requirements(plan, workload_requirements_from_task(task))
    assert ExecutionRuntimePlanV1.model_validate(plan.canonical_payload()) == plan


def test_forced_guest_runs_an_ordinary_agent_inside_a_root_launcher():
    task, trial, profile = _inputs()
    profile = _guest_ready(profile)
    trial = trial.model_copy(update={"isolation": "guest"})
    assert task.environment.user == "agent" and task.verifier.user is None
    assert "guest_root_identity_required" not in _reasons(task, trial, profile)
    plan = _compile(task, trial, profile)
    for sidecar in plan.canonical_payload()["sidecars"]:
        assert sidecar["identity"]["run_as_user"] == 0
        assert sidecar["guest_task_identity"]["run_as_user"] == 65532
        assert sidecar["guest_task_identity"]["home"] == "/home/agent"


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
        "isolation_guest_shared_unsupported",
        "guest_runtime_unavailable",
        "agent_version_not_in_runtime_profile",
    ):
        assert expected in reasons
    assert len(reasons) == len(set(reasons))
    assert "runtime_profile_unavailable" in _reasons(task, trial, None)


def test_readback_without_a_stored_agent_requests_no_harness():
    requested = execution_selection_readback({"isolation": "container"}, None)["requested"]
    assert requested["harness"] is None and requested["isolation"] == "container"


def test_readback_reports_requested_axes_beside_the_frozen_plan():
    task, trial, profile = _inputs()
    task, profile = _root(task), _guest_ready(profile)
    forced = trial.model_copy(update={"isolation": "guest"})
    config = forced.model_dump(mode="json", exclude_defaults=True)
    assert execution_selection_readback(config, None) == {
        "requested": {
            "harness": {"name": forced.agent_name, "version": forced.agent_version},
            "network_policy": None, "verification": None, "isolation": "guest",
        },
        "effective": None,
    }
    effective = execution_selection_readback(config, _compile(task, forced, profile))["effective"]
    assert effective["isolation"] == "guest"
    assert effective["execution_class_id"] == "linux-amd64-cpu-guest-v1"
    assert effective["verification"] == "shared" and effective["fresh_sandbox_grading"] is False
    ordinary = execution_selection_readback(
        trial.model_dump(mode="json", exclude_defaults=True), _compile(*_inputs()))
    assert ordinary["requested"]["isolation"] == "auto"
    assert ordinary["effective"]["isolation"] == "container"


def test_skipped_is_a_verification_axis_and_maps_to_skip_verifier():
    selected = ExecutionSelectionV1.model_validate({
        "schema_version": "loom.execution-selection.v1",
        "verification": "skipped",
        "network_policy": {"mode": "gateway-only"},
    })
    trial_config: dict[str, object] = {}
    assert apply_verification_selection(
        trial_config, selected.verification, purpose="trajectory_generation",
    ) is None
    assert trial_config == {"skip_verifier": True}
    assert requested_verification(trial_config) == "skipped"
    assert explicit_skip_verifier(trial_config) is True
    assert apply_verification_selection(
        {}, "skipped", purpose="evaluation",
    ) == "verification skipped is only allowed with --purpose trajectory_generation."


def test_resolved_selection_reports_skipped_without_resolving_shared_or_separate():
    task, trial, _ = _inputs()
    skipped = trial.model_copy(update={"skip_verifier": True, "verifier_env_mode": None})
    assert resolved_execution_selection(task, skipped)["verification"] == "skipped"
    assert execution_selection_readback(
        skipped.model_dump(mode="json", exclude_defaults=True), None,
    )["requested"]["verification"] == "skipped"


@pytest.mark.parametrize(
    ("agent", "mode", "skip", "declared", "rejected"),
    [
        ("terminus-2", "separate", False, False, True),
        ("terminus-2", "separate", False, True, False),
        ("terminus-2", "shared", False, False, False),
        ("terminus-2", "separate", True, False, False),
        ("direct-completion", "separate", False, False, False),
    ],
)
def test_separate_verification_requires_declared_artifacts(agent, mode, skip, declared, rejected):
    # #2589: a fresh verifier receives only declared artifacts. A workspace
    # task that declares none, such as every TB2 task, is graded in place.
    task, trial, profile = _inputs()
    if not declared:
        task = task.model_copy(update={"steps": [
            step.model_copy(update={"artifacts": [], "required_artifacts": []}) for step in task.steps
        ]})
    trial = trial.model_copy(update={"agent_name": agent, "verifier_env_mode": mode, "skip_verifier": skip})

    reasons = _reasons(task, trial, profile)

    assert ("separate_verifier_requires_declared_artifacts" in reasons) is rejected
