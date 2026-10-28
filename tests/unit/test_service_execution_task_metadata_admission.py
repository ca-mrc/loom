"""Task metadata admission follows the selected immutable adapter capability."""

from pathlib import PurePosixPath

import pytest

from loom.models.mcp import MCPConnection
from loom.models.networking import Public, PublicWeb
from loom.models.trial import TrialConfig
from loom.service_execution_materialization import (
    automatic_service_execution_rejections,
    compile_service_execution_plan,
    execution_selection_rejections,
    freeze_agent_runtime_releases,
)
from tests.support.agent_runtime import release
from tests.unit.test_independent_verifier_submission_admission import _inputs_with_verifier
from tests.unit.test_service_execution_materialization import _REVISION, _provenance
from tests.unit.test_service_execution_terminus_plan import _inputs


def _with_metadata(task, kind):
    values = ({"skills_dir": PurePosixPath("/app/.agents/skills")} if kind == "skills" else {
        "mcp_servers": [MCPConnection(name="playwright", transport="sse", url="http://playwright-mcp:3080/sse")],
    })
    return task.model_copy(update={"environment": task.environment.model_copy(update=values)})


@pytest.mark.parametrize("kind", ["skills", "mcp"])
@pytest.mark.parametrize("qualified", [False, True])
def test_default_controller_requires_published_metadata_capability(kind, qualified):
    task, trial, profile = _inputs()
    task = _with_metadata(task, kind)
    profile = profile.model_copy(update={"supports_terminus_environment_metadata": qualified})
    reasons = execution_selection_rejections(task, trial, profile, source_provenance=_provenance())
    if not qualified:
        assert reasons == ("terminus_environment_metadata_runtime_unavailable",)
        with pytest.raises(ValueError, match="terminus_environment_metadata_runtime_unavailable"):
            compile_service_execution_plan(task=task, trial=trial, profile=profile,
                source_provenance=_provenance(), task_revision_sha256=_REVISION)
    else:
        assert not reasons
        assert compile_service_execution_plan(task=task, trial=trial, profile=profile,
            source_provenance=_provenance(), task_revision_sha256=_REVISION).agent_image_ref == profile.agent_image_ref


@pytest.mark.parametrize("kind", ["skills", "mcp"])
@pytest.mark.parametrize("bridge,harbor,qualified", [
    ("1.0", "0.24.0", False), ("1.1", "0.18.0", False), ("1.1", "0.24.0", True),
])
def test_explicit_controller_cannot_inherit_new_default_capability(kind, bridge, harbor, qualified):
    task, trial, profile = _inputs()
    task = _with_metadata(task, kind)
    runtime = release().model_copy(update={"loom_bridge_revision": bridge, "harbor_version": harbor})
    profile = freeze_agent_runtime_releases(profile.model_copy(update={
        "supports_terminus_environment_metadata": True,
    }), (runtime,))
    trial = trial.model_copy(update={"agent_version": runtime.agent_version})
    reasons = execution_selection_rejections(task, trial, profile, source_provenance=_provenance())
    assert ("terminus_environment_metadata_runtime_unavailable" not in reasons) is qualified


@pytest.mark.parametrize("kind", ["skills", "mcp"])
def test_oracle_utility_keeps_native_ignore_semantics(kind):
    task, _, profile = _inputs()
    task = _with_metadata(task, kind)
    trial = TrialConfig(agent_name="oracle", agent_model=None)
    assert not execution_selection_rejections(task, trial, profile, source_provenance=_provenance())


@pytest.mark.parametrize("kind", ["skills", "mcp"])
def test_other_workspace_harness_does_not_claim_terminus_metadata_support(kind):
    task, trial, _ = _inputs()
    task = _with_metadata(task, kind)
    trial = trial.model_copy(update={"agent_name": "codex"})
    assert "extended_environment_unsupported" in automatic_service_execution_rejections(
        task, trial, source_provenance=_provenance())


def test_old_release_remains_usable_for_tasks_without_metadata():
    task, trial, profile = _inputs()
    runtime = release()
    profile = freeze_agent_runtime_releases(profile, (runtime,))
    trial = trial.model_copy(update={"agent_version": runtime.agent_version})
    assert not execution_selection_rejections(task, trial, profile, source_provenance=_provenance())


def test_old_frozen_profile_does_not_acquire_new_serialized_capability():
    _, _, profile = _inputs()
    assert "supports_terminus_environment_metadata" not in profile.model_dump(mode="json")


@pytest.mark.parametrize("case", ["old", "skip", "oracle", "new_no_verifier_egress"])
def test_metadata_and_independent_grader_admission_do_not_bypass_each_other(case):
    task, trial, profile = _inputs_with_verifier(PublicWeb() if case == "new_no_verifier_egress" else Public())
    task = _with_metadata(task, "mcp")
    profile = profile.model_copy(update={"supports_terminus_environment_metadata": case != "new_no_verifier_egress"})
    if case == "oracle":
        trial = TrialConfig(agent_name="oracle", agent_model=None)
    else:
        runtime = release().model_copy(update={
            "loom_bridge_revision": "1.1" if case == "new_no_verifier_egress" else "1.0",
            "harbor_version": "0.24.0",
        })
        profile = freeze_agent_runtime_releases(profile, (runtime,))
        trial = trial.model_copy(update={"agent_version": runtime.agent_version, "skip_verifier": case == "skip"})
    reasons = execution_selection_rejections(task, trial, profile, source_provenance=_provenance())
    assert ("terminus_environment_metadata_runtime_unavailable" in reasons) is (case in {"old", "skip"})
    verifier_reasons = [r for r in reasons if r.startswith("verifier.environment:")]
    assert verifier_reasons == ([] if case == "skip" else [
        "verifier.environment: task_web_egress_unsupported" if case == "new_no_verifier_egress"
        else "verifier.environment: network_access_unsupported",
    ])
