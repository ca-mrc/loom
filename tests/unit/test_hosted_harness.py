"""#2288: hosted harnesses are typed specs; the common planner asks the spec,
never the agent name."""

from __future__ import annotations

import pytest

import loom.hosted_harness as hosted
from loom.execution_contract import VerifierTopology, workload_requirements_from_task
from loom.hosted_harness import (
    HOSTED_HARNESSES,
    NATIVE_EXECUTION_AGENT_NAMES,
    ORACLE,
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
)
from tests.unit.test_service_execution_materialization import _REVISION, _provenance
from tests.unit.test_service_execution_terminus_plan import _inputs

_MODEL = ModelSpec(provider="openai", name="gpt-5")


def test_registry_resolves_names_and_aliases() -> None:
    assert hosted_harness("litellm") is hosted_harness("direct-completion")
    assert hosted_harness("terminus-2") is TERMINUS_2
    assert hosted_harness("oracle") is ORACLE
    assert hosted_harness("codex") is None
    assert hosted_harness(None) is None
    assert NATIVE_EXECUTION_AGENT_NAMES == {"direct-completion", "litellm", "terminus-2", "oracle"}
    assert is_workspace_harness("oracle") and not is_workspace_harness("litellm")
    assert not is_workspace_harness("codex")


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


@pytest.mark.parametrize(("kwargs", "message"), [
    ({"execution_kind": "response-only", "stages_solution": True, "model": "forbidden"}, "no task sandbox"),
    ({"execution_kind": "workspace"}, "driver operations"),
    ({"execution_kind": "workspace", "required_driver_capabilities": frozenset({"exec"}),
      "stages_solution": True}, "model-free"),
])
def test_invalid_specs_are_refused(kwargs: dict, message: str) -> None:
    base = {"name": "x", "controller_phase": "x", "model": "required", "trace_format": "terminus"}
    with pytest.raises(ValueError, match=message):
        HostedHarnessSpec(**{**base, **kwargs})


def test_names_and_phases_must_be_unique() -> None:
    clash = HostedHarnessSpec(
        name="other", aliases=("oracle",), execution_kind="workspace", controller_phase="other",
        model="required", trace_format="terminus", required_driver_capabilities=frozenset({"exec"}),
    )
    with pytest.raises(ValueError, match="declared twice"):
        hosted._index((ORACLE, clash))
    same_phase = HostedHarnessSpec(
        name="other", execution_kind="workspace", controller_phase="oracle",
        model="required", trace_format="terminus", required_driver_capabilities=frozenset({"exec"}),
    )
    with pytest.raises(ValueError, match="not unique"):
        hosted._index((ORACLE, same_phase))


# --- a new workspace harness needs only a spec, not new planner branches ----


_FUTURE = HostedHarnessSpec(
    name="future-agent", execution_kind="workspace", controller_phase="future-agent",
    model="required", trace_format="terminus",
    required_driver_capabilities=frozenset({"exec", "upload", "download"}),
    native_outputs=(NativeOutput("agent/native.jsonl", "artifacts/native.jsonl", "agent_native", True),),
)
_STREAMING = HostedHarnessSpec(
    name="streaming-agent", execution_kind="workspace", controller_phase="streaming-agent",
    model="required", trace_format="terminus",
    required_driver_capabilities=frozenset({"exec", "exec_streaming", "upload", "download"}),
)


@pytest.fixture
def _registered(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(hosted, "HOSTED_HARNESSES", hosted._index(
        (*{spec.name: spec for spec in HOSTED_HARNESSES.values()}.values(), _FUTURE, _STREAMING),
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

    assert plan.main.argv[4] == "future-agent"
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
def test_harness_needing_an_unsupported_driver_operation_fails_closed() -> None:
    task, _, _ = _inputs()

    assert not _STREAMING.natively_runnable
    reasons = automatic_service_execution_rejections(task, _trial("streaming-agent"), source_provenance=_provenance())
    assert "direct_completion_required" in reasons


def test_unknown_harness_never_falls_through_to_direct_completion() -> None:
    task, _, profile = _inputs()

    reasons = automatic_service_execution_rejections(task, _trial("codex"), source_provenance=_provenance())
    assert "direct_completion_required" in reasons
    with pytest.raises(ValueError, match="incompatible"):
        compile_service_execution_plan(
            task=task, trial=_trial("codex"), profile=profile, source_provenance=_provenance(),
            task_revision_sha256=_REVISION,
        )


def test_materializer_trace_format_comes_from_the_spec() -> None:
    from loom_control_plane.service_execution_materializer import _trace_format

    assert _trace_format("terminus-2") == "terminus"
    assert _trace_format("oracle") == "oracle"
    assert _trace_format("litellm") == "completion-calls"
    assert _trace_format("unknown") == "completion-calls"
