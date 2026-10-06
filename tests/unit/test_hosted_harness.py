"""#2288: hosted harnesses are typed specs; the common planner asks the spec,
never the agent name."""

from __future__ import annotations

import pytest

import loom.hosted_harness as hosted
from loom.execution_contract import VerifierTopology, workload_requirements_from_task
from loom.hosted_harness import (
    CODEX,
    DIRECT_COMPLETION,
    HOSTED_HARNESSES,
    NATIVE_EXECUTION_AGENT_NAMES,
    ORACLE,
    RESPONSE_RUNNER_MODULE,
    SANDBOX_CONTROLLER_MODULE,
    TERMINUS_2,
    HostedHarnessSpec,
    NativeOutput,
    harnesses_supporting,
    hosted_harness,
    is_workspace_harness,
    workspace_controller_phases,
)
from loom.models.trial import TrialConfig
from loom.models.types import ModelSpec
from loom.service_execution_materialization import (
    automatic_service_execution_rejections,
    compile_service_execution_plan,
    controller_image_for_trial,
    runtime_profile_rejections,
)
from tests.unit.test_service_execution_materialization import _REVISION, _provenance
from tests.unit.test_service_execution_terminus_plan import _inputs

_MODEL = ModelSpec(provider="openai", name="gpt-5")


def _workspace(name: str, **kwargs: object) -> HostedHarnessSpec:
    base: dict[str, object] = {
        "name": name, "execution_kind": "workspace", "controller_module": SANDBOX_CONTROLLER_MODULE,
        "controller_phase": name, "controller_image": "harness-controller", "model": "required",
        "gateway_protocol": "openai-chat-completions", "trace_format": "terminus",
        "required_driver_capabilities": frozenset({"exec", "upload", "download"}),
    }
    return HostedHarnessSpec(**{**base, **kwargs})  # type: ignore[arg-type]


def test_registry_resolves_names_and_aliases() -> None:
    assert hosted_harness("litellm") is hosted_harness("direct-completion")
    assert hosted_harness("terminus-2") is TERMINUS_2
    assert hosted_harness("oracle") is ORACLE
    assert hosted_harness("no-such-agent") is None
    assert hosted_harness("codex") is CODEX
    assert hosted_harness(None) is None
    assert NATIVE_EXECUTION_AGENT_NAMES == {"direct-completion", "litellm", "terminus-2", "oracle", "codex"}
    assert is_workspace_harness("oracle") and not is_workspace_harness("litellm")
    assert not is_workspace_harness("no-such-agent")


def test_harness_only_features_are_declared_not_inferred() -> None:
    assert harnesses_supporting("agent_continuation") == ("terminus-2",)
    assert harnesses_supporting("pinned_versions") == ("terminus-2",)
    assert harnesses_supporting("task_resource_requests") == ("terminus-2",)
    assert ORACLE.stages_solution and ORACLE.model == "forbidden" and not ORACLE.native_outputs
    assert [item.relative_path for item in TERMINUS_2.native_outputs] == [
        "artifacts/harbor/trajectory.json", "artifacts/harbor/recording.cast",
    ]


def test_controller_implements_every_workspace_phase() -> None:
    from loom.service_execution_sandbox_task import CONTROLLER_PHASES

    assert set(workspace_controller_phases()) == CONTROLLER_PHASES


def test_controller_binding_and_gateway_protocol_are_declared() -> None:
    assert (DIRECT_COMPLETION.controller_module, DIRECT_COMPLETION.controller_image) == (
        RESPONSE_RUNNER_MODULE, "service-runner",
    )
    for spec in (TERMINUS_2, ORACLE):
        assert (spec.controller_module, spec.controller_image) == (SANDBOX_CONTROLLER_MODULE, "harness-controller")
    assert DIRECT_COMPLETION.gateway_protocol == TERMINUS_2.gateway_protocol == "openai-chat-completions"
    assert ORACLE.gateway_protocol is None


@pytest.mark.parametrize(("kwargs", "message"), [
    ({"execution_kind": "response-only"}, "bind"),
    ({"controller_module": RESPONSE_RUNNER_MODULE}, "bind"),
    ({"controller_image": "service-runner"}, "bind"),
    ({"gateway_protocol": None}, "Gateway protocol"),
    ({"model": "forbidden"}, "Gateway protocol"),
    ({"required_driver_capabilities": frozenset()}, "driver operations"),
    ({"stages_solution": True}, "model-free"),
])
def test_invalid_specs_are_refused(kwargs: dict, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        _workspace("x", **kwargs)


def test_response_only_spec_cannot_acquire_sandbox_capabilities() -> None:
    with pytest.raises(ValueError, match="no task sandbox"):
        HostedHarnessSpec(
            name="x", execution_kind="response-only", controller_module=RESPONSE_RUNNER_MODULE,
            controller_phase="x", controller_image="service-runner", model="forbidden",
            gateway_protocol=None, trace_format="completion-calls", stages_solution=True,
        )


def test_names_and_phases_must_be_unique() -> None:
    with pytest.raises(ValueError, match="declared twice"):
        hosted._index((ORACLE, _workspace("other", aliases=("oracle",))))
    with pytest.raises(ValueError, match="not unique"):
        hosted._index((ORACLE, _workspace("other", controller_phase="oracle")))


# --- a new workspace harness needs only a spec, not new planner branches ----


_FUTURE = _workspace(
    "future-agent",
    native_outputs=(NativeOutput("agent/native.jsonl", "artifacts/native.jsonl", "agent_native", True),),
)
_STREAMING = _workspace(
    "streaming-agent",
    required_driver_capabilities=frozenset({"exec", "exec_streaming", "upload", "download"}),
)
_UNAVAILABLE = _workspace("unavailable-agent", readiness="unavailable")


@pytest.fixture
def _registered(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(hosted, "HOSTED_HARNESSES", hosted._index(
        (*{spec.name: spec for spec in HOSTED_HARNESSES.values()}.values(), _FUTURE, _STREAMING, _UNAVAILABLE),
    ))


def _trial(name: str) -> TrialConfig:
    return TrialConfig(agent_name=name, agent_model=_MODEL)


@pytest.mark.usefixtures("_registered")
def test_new_workspace_harness_reuses_the_common_planner() -> None:
    task, _, profile = _inputs()
    trial = _trial("future-agent")

    assert automatic_service_execution_rejections(task, trial, source_provenance=_provenance()) == ()
    requirements = workload_requirements_from_task(task, trial)
    assert requirements.verifier_topology == VerifierTopology.SEPARATE_EXECUTION
    plan = compile_service_execution_plan(
        task=task, trial=trial, profile=profile, source_provenance=_provenance(),
        task_revision_sha256=_REVISION,
    )

    assert plan.main.argv[3:5] == (SANDBOX_CONTROLLER_MODULE, "future-agent")
    assert plan.agent_image_ref == profile.agent_image_ref
    assert [s.role_name for s in plan.sidecars] == ["task-sandbox"]
    paths = {item.relative_path for item in plan.output_declarations}
    assert {"artifacts/native.jsonl", "trajectory/events.jsonl", "artifacts/workspace.tar"} <= paths
    assert not any(path.startswith("artifacts/harbor/") for path in paths)


@pytest.mark.usefixtures("_registered")
def test_harness_features_stay_with_the_harness() -> None:
    task, _, _ = _inputs()
    continuing = task.model_copy(update={"agent": task.agent.model_copy(update={"continue_until_timeout": True})})

    reasons = automatic_service_execution_rejections(continuing, _trial("future-agent"), source_provenance=_provenance())
    assert "agent_continuation_unsupported" in reasons
    assert "agent_continuation_unsupported" not in automatic_service_execution_rejections(
        continuing, _trial("terminus-2"), source_provenance=_provenance(),
    )


@pytest.mark.usefixtures("_registered")
def test_unavailable_harness_fails_closed() -> None:
    task, _, _ = _inputs()

    assert not hosted_harness("unavailable-agent").natively_runnable  # type: ignore[union-attr]
    reasons = automatic_service_execution_rejections(
        task, _trial("unavailable-agent"), source_provenance=_provenance(),
    )
    assert "direct_completion_required" in reasons


@pytest.mark.usefixtures("_registered")
def test_streaming_harness_runs_natively_and_on_guests() -> None:
    from tests.unit.test_guest_execution_materialization import _guest_inputs

    task, _, _ = _inputs()
    assert hosted_harness("streaming-agent").natively_runnable  # type: ignore[union-attr]
    assert automatic_service_execution_rejections(
        task, _trial("streaming-agent"), source_provenance=_provenance(),
    ) == ()

    guest_task, _, guest_profile = _guest_inputs("nested_docker")
    reasons = automatic_service_execution_rejections(
        guest_task, _trial("streaming-agent"), source_provenance=_provenance(),
        supported_capabilities=guest_profile.supported_guest_capabilities,
    )
    # Supervised processes are qualified through the guest channel (#2362).
    assert "guest_driver_capabilities_unsupported" not in reasons
    assert "guest_driver_capabilities_unsupported" not in automatic_service_execution_rejections(
        guest_task, _trial("future-agent"), source_provenance=_provenance(),
        supported_capabilities=guest_profile.supported_guest_capabilities,
    )


def test_controller_image_comes_from_the_spec_binding() -> None:
    _, trial, profile = _inputs()

    assert controller_image_for_trial(profile, trial) == profile.agent_image_ref
    assert controller_image_for_trial(profile, _trial("litellm")) is None
    assert controller_image_for_trial(profile, _trial("no-such-agent")) is None


def test_missing_controller_binding_fails_closed_before_compilation() -> None:
    task, trial, profile = _inputs()
    unbound = profile.model_copy(update={"agent_image_ref": None})

    assert runtime_profile_rejections(task, trial, unbound) == ("terminus_controller_unavailable",)
    with pytest.raises(ValueError, match="no Terminus controller"):
        compile_service_execution_plan(
            task=task, trial=trial, profile=unbound, source_provenance=_provenance(),
            task_revision_sha256=_REVISION,
        )


def test_unknown_harness_never_falls_through_to_direct_completion() -> None:
    task, _, profile = _inputs()

    reasons = automatic_service_execution_rejections(task, _trial("no-such-agent"), source_provenance=_provenance())
    assert "direct_completion_required" in reasons
    with pytest.raises(ValueError, match="incompatible"):
        compile_service_execution_plan(
            task=task, trial=_trial("no-such-agent"), profile=profile, source_provenance=_provenance(),
            task_revision_sha256=_REVISION,
        )


def test_materializer_trace_format_comes_from_the_spec() -> None:
    from loom_control_plane.service_execution_materializer import _trace_format

    assert _trace_format("terminus-2") == "terminus"
    assert _trace_format("oracle") == "oracle"
    assert _trace_format("litellm") == "completion-calls"
    assert _trace_format("unknown") == "completion-calls"
