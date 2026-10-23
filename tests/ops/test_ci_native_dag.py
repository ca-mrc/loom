"""Selected heavy work uses native dependencies without occupying waiting VMs."""

import json
import os
import re
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
SPECIALISTS = {
    "images": ("images", "images-gate"),
    "cluster": ("cluster-smoke", "cluster-smoke-gate"),
    "staging": ("staging-smoke", "staging-smoke-gate"),
}


@pytest.mark.parametrize("gate", ["repository-checks", "images-gate", "cluster-smoke-gate", "staging-smoke-gate"])
@pytest.mark.parametrize("event", ["pull_request", "merge_group", "workflow_dispatch", "schedule", "push"])
@pytest.mark.parametrize("mode", ["full", "filtered", ""])
def test_skipped_manual_or_filtered_jobs_cannot_emit_required_names(gate, event, mode):
    jobs = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())["jobs"]
    name = jobs[gate]["name"]
    if name.startswith("${{"):
        values = {"github.event_name": event, "needs.workflow-plan.outputs.gate_mode": mode}
        expression = re.sub(r"(?:github|needs)\.[A-Za-z0-9_.-]+", lambda match: repr(values[match[0]]), name[3:-2])
        expression = expression.replace("&&", " and ").replace("||", " or ")
        name = eval(f"({expression})", {"__builtins__": {}})
    assert (name == gate) is (event in {"pull_request", "merge_group"} and mode == "full")


def test_specialists_wait_in_the_native_dag_without_allocating_runners():
    jobs = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())["jobs"]
    for kind, (workflow, gate) in SPECIALISTS.items():
        call = jobs[f"{kind}-validation"]
        assert call["uses"] == f"./.github/workflows/{workflow}.yml"
        assert set(call["needs"]) == {"workflow-plan", "fast-feedback"}
        assert "runs-on" not in call and "steps" not in call
        assert f"{kind}-validation" in jobs[gate]["needs"]
        assert "vars.LOOM_CI_REUSABLE_SUPPLY_READY == 'true'" in jobs[gate]["runs-on"]


@pytest.mark.parametrize("kind", SPECIALISTS)
@pytest.mark.parametrize("event", ["pull_request", "merge_group", "workflow_dispatch", "push"])
@pytest.mark.parametrize("feedback", ["success", "failure", "cancelled", "skipped"])
def test_actual_call_guard_starts_only_selected_work_after_feedback(kind, event, feedback):
    from tests.ops.test_ci_images_parallel_builds import _condition

    jobs = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())["jobs"]
    selector = {"images": "images", "cluster": "cluster_smoke", "staging": "staging_smoke"}[kind]
    for selected in ("true", "false"):
        values = {
            "github.event_name": event,
            "needs.workflow-plan.result": "success",
            "needs.workflow-plan.outputs.gate_mode": "full",
            f"needs.workflow-plan.outputs.{selector}": selected,
            "needs.fast-feedback.result": feedback,
        }
        expression = jobs[f"{kind}-validation"]["if"]
        expected = event in {"pull_request", "merge_group"} and feedback == "success" and selected == "true"
        assert _condition(expression, values) is expected
        assert not _condition(expression, values, cancelled=True)


def test_candidate_supplementary_matrix_stops_siblings_after_failure():
    jobs = yaml.safe_load((ROOT / ".github/workflows/nebius-candidate.yml").read_text())["jobs"]
    assert jobs["pool-ops-tests"]["strategy"]["fail-fast"] is True
    assert "pool-ops-tests" in jobs["publish"]["needs"]


@pytest.mark.parametrize("kind", SPECIALISTS)
@pytest.mark.parametrize(("selected", "feedback", "result", "accepted"), [
    ("true", "success", "success", True),
    ("true", "failure", "success", False),
    ("true", "cancelled", "success", False),
    ("true", "skipped", "success", False),
    ("true", "success", "failure", False),
    ("true", "success", "cancelled", False),
    ("true", "success", "skipped", False),
    ("false", "skipped", "skipped", True),
    ("false", "success", "failure", False),
    ("false", "success", "success", False),
    ("", "success", "success", False),
])
def test_required_gate_executes_actual_selected_result_check(kind, selected, feedback, result, accepted):
    jobs = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())["jobs"]
    _, gate = SPECIALISTS[kind]
    step = jobs[gate]["steps"][-1]
    selector = {"images": "images", "cluster": "cluster_smoke", "staging": "staging_smoke"}[kind]
    value = {
        "workflow-plan": {"result": "success", "outputs": {"gate_mode": "full", selector: selected}},
        "fast-feedback": {"result": feedback},
        f"{kind}-validation": {"result": result},
    }
    run = subprocess.run(["bash", "-c", step["run"]], cwd=ROOT,
                         env={**os.environ, "FEEDBACK_NEEDS": json.dumps(value),
                              "PATH": str(Path(sys.executable).parent) + os.pathsep + os.environ["PATH"]},
                         capture_output=True, text=True)
    assert (run.returncode == 0) is accepted, run.stderr


@pytest.mark.parametrize("kind", SPECIALISTS)
@pytest.mark.parametrize(("plan_result", "gate_mode"), [
    ("failure", "full"), ("cancelled", "full"), ("success", "filtered"), ("success", ""),
])
def test_required_gate_rejects_failed_or_unqualified_planning(kind, plan_result, gate_mode):
    from scripts.ci_fast_feedback import enforce_specialist

    selector = {"images": "images", "cluster": "cluster_smoke", "staging": "staging_smoke"}[kind]
    value = {
        "workflow-plan": {"result": plan_result, "outputs": {"gate_mode": gate_mode, selector: "true"}},
        "fast-feedback": {"result": "success"},
        f"{kind}-validation": {"result": "success"},
    }
    with pytest.raises(ValueError):
        enforce_specialist(value, selector)
