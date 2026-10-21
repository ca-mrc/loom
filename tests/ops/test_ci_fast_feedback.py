"""A failed exact-scope feedback run must prevent heavy runner fan-out."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from copy import deepcopy
from pathlib import Path

import pytest
import yaml
from scripts.ci_fast_feedback import FEEDBACK_JOBS, enforce, pages, wait_for_feedback

ROOT = Path(__file__).resolve().parents[2]
HEAD = "a" * 40
TITLE = f"gate=full / head={HEAD} / base={'b' * 40} / updated=now / action=synchronize / label=none / labels=000000 / pull=42"


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
    step = job["steps"][-1]
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


class API:
    def __init__(self):
        self.source = {"event": "pull_request", "head_sha": HEAD, "display_title": TITLE,
                       "path": ".github/workflows/images.yml"}
        self.run = {**self.source, "path": ".github/workflows/ci.yml", "id": 7, "run_attempt": 1, "status": "in_progress"}
        self.jobs = [{"name": "fast-feedback", "run_attempt": 1, "status": "completed", "conclusion": "success"}]
        self.now = 0.0
        self.polls = 0

    def fetch(self, path):
        if path.endswith("/runs/9"):
            return self.source
        if "/workflows/ci.yml/runs?" in path:
            assert f"head_sha={HEAD}" in path
            assert "event=" + self.source["event"] in path
            self.polls += 1
            return {"workflow_runs": [self.run]}
        assert "/runs/7/jobs?filter=latest&per_page=100&page=" in path
        return {"jobs": self.jobs}

    def wait(self, seconds=60):
        return wait_for_feedback("ca-mrc/loom", 9, timeout_seconds=seconds, fetch=self.fetch,
                                 clock=lambda: self.now, sleep=self.advance)

    def advance(self, seconds):
        self.now += seconds


@pytest.mark.parametrize("source", ["images", "cluster-smoke", "staging-smoke"])
@pytest.mark.parametrize("event", ["pull_request", "merge_group"])
def test_all_heavy_sources_accept_only_successful_native_feedback(source, event):
    api = API()
    api.source.update(event=event, path=f".github/workflows/{source}.yml")
    api.run["event"] = event
    assert api.wait() == 7


@pytest.mark.parametrize("result", ["failure", "cancelled", "skipped", "neutral", "timed_out"])
def test_unsuccessful_native_stage_fails_immediately(result):
    api = API()
    api.jobs[0]["conclusion"] = result
    with pytest.raises(ValueError, match="did not succeed"):
        api.wait()
    assert api.now == 0


@pytest.mark.parametrize("changed", ["head", "base", "labels", "update", "event", "path"])
def test_old_head_base_or_scope_success_cannot_release_work(changed):
    api = API()
    if changed == "head":
        api.run["head_sha"] = "c" * 40
    elif changed == "base":
        api.run["display_title"] = TITLE.replace("b" * 40, "c" * 40)
    elif changed == "labels":
        api.run["display_title"] = TITLE.replace("000000", "001000")
    elif changed == "update":
        api.run["display_title"] = TITLE.replace("updated=now", "updated=before")
    elif changed == "event":
        api.run["event"] = "push"
    else:
        api.run["path"] = ".github/workflows/untrusted.yml"
    with pytest.raises(TimeoutError):
        api.wait()
    assert api.now == 60


def test_waits_for_queued_stage_and_does_not_wait_for_heavy_jobs():
    api = API()
    api.jobs[0].update(status="queued", conclusion=None)
    advance = api.advance

    def complete(seconds):
        advance(seconds)
        api.jobs[0].update(status="completed", conclusion="success")

    api.advance = complete
    assert api.wait() == 7
    assert api.now == 15 and api.run["status"] == "in_progress"


def test_cancelled_workflow_without_stage_and_duplicate_stage_fail_closed():
    api = API()
    api.run["status"] = "completed"
    api.jobs = []
    with pytest.raises(ValueError, match="ended without"):
        api.wait()
    api.jobs = [deepcopy(API().jobs[0])] * 2
    with pytest.raises(ValueError, match="ambiguous"):
        api.wait()


def test_reads_all_job_pages_before_deciding_stage_is_missing():
    paths = []

    def fetch(path):
        paths.append(path)
        return {"jobs": [{"name": "other"}] * 100 if path.endswith("&page=1") else API().jobs}

    jobs = pages("/jobs?filter=latest", "jobs", fetch)
    assert len(paths) == 2 and jobs[-1]["name"] == "fast-feedback"


@pytest.mark.parametrize("source", ["images", "cluster-smoke", "staging-smoke"])
def test_source_plan_waits_before_exposing_success_to_heavy_jobs(source):
    workflow = yaml.safe_load((ROOT / f".github/workflows/{source}.yml").read_text())
    plan = workflow["jobs"]["plan"]
    wait = plan["steps"][-1]
    assert wait["run"] == "python3 scripts/ci_fast_feedback.py wait --timeout-seconds 5400"
    assert "pull_request" in wait["if"] and "merge_group" in wait["if"]
    assert "== 'true'" in wait["if"]
    assert wait["env"] == {"GH_TOKEN": "${{ github.token }}"}
    assert plan["permissions"] == {"contents": "read", "actions": "read"}
    assert "continue-on-error" not in wait and "continue-on-error" not in plan
    for name, job in workflow["jobs"].items():
        if name != "plan":
            assert "plan" in job["needs"]


def test_success_from_an_earlier_attempt_cannot_release_a_rerun():
    api = API()
    api.run["run_attempt"] = 2
    with pytest.raises(TimeoutError):
        api.wait()
    api.run["status"] = "completed"
    with pytest.raises(ValueError, match="ended without"):
        api.wait()


def test_docs_only_does_not_allocate_a_feedback_runner():
    workflow = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())
    assert "docs_only != 'true'" in workflow["jobs"]["fast-feedback"]["if"]


def test_wait_backs_off_without_extending_its_deadline():
    api = API()
    api.jobs[0].update(status="queued", conclusion=None)
    intervals = []
    advance = api.advance

    def record(seconds):
        intervals.append(seconds)
        advance(seconds)

    api.advance = record
    with pytest.raises(TimeoutError):
        api.wait(seconds=300)
    assert intervals == [15, 30, 60, 120, 75]
    assert api.now == 300
