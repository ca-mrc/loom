"""A failed exact-scope feedback run must prevent heavy runner fan-out."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
import yaml
from scripts.ci_fast_feedback import FEEDBACK_JOBS, enforce
from scripts.plan_ci_validations import plan_validations
from tests.ops.test_ci_images_parallel_builds import _condition

ROOT = Path(__file__).resolve().parents[2]


def needs() -> dict:
    return {
        "workflow-plan": {"result": "success", "outputs": {
            "gate_mode": "full", **dict.fromkeys(FEEDBACK_JOBS.values(), "true"),
        }},
        **{job: {"result": "success"} for job in FEEDBACK_JOBS},
    }


@pytest.mark.parametrize("job", FEEDBACK_JOBS)
@pytest.mark.parametrize("result", ["failure", "cancelled", "skipped"])
def test_selected_feedback_failure_stops_all_heavy_work(job, result):
    value = needs()
    value[job]["result"] = result
    with pytest.raises(ValueError, match="feedback rejected"):
        enforce(value)
    value["workflow-plan"]["outputs"][FEEDBACK_JOBS[job]] = "false"
    if result == "skipped":
        enforce(value)
    else:
        with pytest.raises(ValueError):
            enforce(value)


def test_feedback_cli_executes_actual_aggregation_step():
    workflow = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())
    job = workflow["jobs"]["fast-feedback"]
    assert set(job["needs"]) == {"workflow-plan", *FEEDBACK_JOBS}
    step = next(step for step in job["steps"] if step.get("run") == "python3 scripts/ci_fast_feedback.py enforce")
    assert step["env"] == {"FEEDBACK_NEEDS": "${{ toJSON(needs) }}"}
    value = needs()
    for expected in (0, 1):
        if expected:
            value["integration"]["result"] = "failure"
        result = subprocess.run(
            ["bash", "-c", step["run"]], cwd=ROOT, capture_output=True, text=True,
            env={**os.environ, "PATH": str(Path(sys.executable).parent) + os.pathsep + os.environ["PATH"],
                 "FEEDBACK_NEEDS": json.dumps(value)},
        )
        assert result.returncode == expected, result.stderr


def test_feedback_owns_mandatory_cli_probe_in_candidate_planners_hosted_environment():
    workflows = ROOT / ".github/workflows"
    job = yaml.safe_load((workflows / "ci.yml").read_text())["jobs"]["fast-feedback"]
    candidate = yaml.safe_load((workflows / "nebius-candidate.yml").read_text())["jobs"]["pool-ops-plan"]
    assert job["runs-on"] == candidate["runs-on"] == "ubuntu-24.04"
    commands = [step.get("run") for step in job["steps"]]
    probe = next(step for step in job["steps"] if step.get("run") == "python3 scripts/check_github_cli_logs.py")
    assert commands.index("python3 scripts/ci_fast_feedback.py enforce") < job["steps"].index(probe)
    assert job["steps"].index(probe) < commands.index("python3 scripts/ci_fast_feedback.py check-base")
    assert not probe.get("if") and not probe.get("continue-on-error")
    assert "env" not in probe  # The probe needs no real GitHub token.


@pytest.mark.parametrize(("job_name", "selector"), [
    ("nebius-pool", "nebius_pool"), ("runtime-payload", "runtime_payload"),
    ("integration-docker", "integration_docker"), ("web-runtime", "web_runtime"),
])
def test_successful_feedback_releases_selected_heavy_work_after_unselected_ancestor_skip(job_name, selector):
    # Native run 39979536847 had successful feedback but selected heavy work
    # skipped because GitHub's implicit success() includes skipped ancestors.
    job = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())["jobs"][job_name]
    values = {
        "needs.workflow-plan.result": "success", "needs.fast-feedback.result": "success",
        "needs.workflow-plan.outputs.gate_mode": "full",
        f"needs.workflow-plan.outputs.{selector}": "true",
        "needs.tests-packages.result": "skipped",
    }
    assert _condition(job["if"], values)
    assert not _condition(job["if"], values, cancelled=True)
    for result in ("failure", "cancelled", "skipped"):
        assert not _condition(job["if"], {**values, "needs.fast-feedback.result": result})
        assert not _condition(job["if"], {**values, "needs.workflow-plan.result": result})
    assert not _condition(job["if"], {**values, f"needs.workflow-plan.outputs.{selector}": "false"})


@pytest.mark.parametrize(("workflow", "job_name"), [
    ("images", "plan"), ("images", "trivy-binary"), ("images", "build"),
    ("images", "nebius-harness-build"), ("cluster-smoke", "plan"),
    ("cluster-smoke", "cluster-contract"), ("staging-smoke", "plan"),
    ("staging-smoke", "system-smoke"),
])
def test_reusable_nodes_allow_skipped_outer_ancestors_but_require_their_own_dependencies(workflow, job_name):
    job = yaml.safe_load((ROOT / f".github/workflows/{workflow}.yml").read_text())["jobs"][job_name]
    values = {
        "needs.outer-unselected.result": "skipped", "needs.plan.result": "success",
        "needs.trivy-binary.result": "success", "needs.plan.outputs.gate_mode": "full",
        "needs.plan.outputs.required": "true", "needs.plan.outputs.images": '[{"image":"service"}]',
        "needs.plan.outputs.ordinary_builds": '[{"image":"service"}]',
        "needs.plan.outputs.harbor_required": "true",
    }
    condition = job.get("if", "1 == 1")
    assert _condition(condition, values)
    assert not _condition(condition, values, cancelled=True)
    for dependency in job.get("needs", []):
        for result in ("failure", "cancelled", "skipped"):
            assert not _condition(condition, {**values, f"needs.{dependency}.result": result})


@pytest.mark.parametrize("event,selector", [
    ("pull_request", None), ("pull_request", "nebius_pool"),
    ("pull_request", "integration_docker"), ("pull_request", "runtime_payload"),
    ("pull_request", "web_runtime"), ("pull_request", "images"),
    ("pull_request", "cluster_smoke"), ("pull_request", "staging_smoke"),
    ("merge_group", "images"), ("workflow_dispatch", "images"),
    ("schedule", "images"), ("push", "images"),
])
def test_base_preflight_runs_only_after_quick_success_for_pr_heavy_work(event, selector):
    job = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())["jobs"]["fast-feedback"]
    step = next(step for step in job["steps"] if step.get("run") == "python3 scripts/ci_fast_feedback.py check-base")
    assert job["steps"].index(step) > next(
        index for index, item in enumerate(job["steps"])
        if item.get("run") == "python3 scripts/ci_fast_feedback.py enforce"
    )
    assert not step.get("continue-on-error")
    values = {f"needs.workflow-plan.outputs.{name}": "false" for name in (
        "nebius_pool", "integration_docker", "runtime_payload", "web_runtime", "images", "cluster_smoke", "staging_smoke",
    )}
    values["github.event_name"] = event
    if selector:
        values[f"needs.workflow-plan.outputs.{selector}"] = "true"
    assert _condition(step["if"], values) is (event == "pull_request" and selector is not None)
    assert not _condition(step["if"], {**values, "needs.quick.result": "failure"})
    assert step["env"] == {
        "GH_TOKEN": "${{ github.token }}",
        "PR_BASE_REF": "${{ github.event.pull_request.base.ref }}",
        "PR_BASE_SHA": "${{ github.event.pull_request.base.sha }}",
    }


@pytest.mark.parametrize(("event", "label", "dependent"), [
    ("pull_request", None, None),
    ("merge_group", None, None),
    ("pull_request", "ci:images", "images"),
    ("pull_request", "cluster-smoke", "cluster_smoke"),
    ("pull_request", "staging-smoke", "staging_smoke"),
    ("pull_request", "ci:integration", "integration"),
    ("pull_request", "ci:integration-docker", "integration_docker"),
    ("pull_request", "ci:coverage-summary", "coverage_summary"),
    ("workflow_dispatch", None, "nebius_pool"),
    ("schedule", None, "nebius_pool"),
])
def test_docs_feedback_follows_actual_selected_work(event, label, dependent):
    workflow = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())
    plan = plan_validations(changed_paths=["docs/user-guide.md"],
                            labels={label} if label else set(), event_name=event)
    outputs = plan.github_outputs()
    if dependent:
        assert outputs[dependent] == "true"
    else:
        assert plan.docs_only
        assert not plan.selected_heavy_checks()
    values = {f"needs.workflow-plan.outputs.{key}": value for key, value in outputs.items()}
    value = {"workflow-plan": {"result": "success", "outputs": outputs},
             **{job: {"result": "success" if outputs[selector] == "true" else "skipped"}
                for job, selector in FEEDBACK_JOBS.items()}}
    values.update({f"needs.{job}.result": result["result"] for job, result in value.items()})
    assert _condition(workflow["jobs"]["fast-feedback"]["if"], values) is bool(dependent)
    if dependent:
        enforce(value)
        # A selected manual baseline must also reach its actual heavy jobs.
        if event in {"workflow_dispatch", "schedule"}:
            values = {f"needs.workflow-plan.outputs.{key}": value for key, value in outputs.items()}
            values.update({"needs.workflow-plan.result": "success", "needs.fast-feedback.result": "success"})
            for job in ("nebius-pool", "runtime-payload", "web-runtime"):
                assert _condition(workflow["jobs"][job]["if"], values)
