"""Skipped publications get an explanation without opening the deployment path."""
from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def workflow():
    return yaml.load(
        (ROOT / ".github/workflows/nebius-rollout.yml").read_text(),
        Loader=yaml.BaseLoader,
    )


def selected_jobs(workflow, *, conclusion="success", enabled="true",
                  event="workflow_run", operation="", repository="qianyi-sun/loom",
                  head_repository="qianyi-sun/loom", ref="refs/heads/dev",
                  check_status="ready", check_result="success", repository_id=None,
                  head_repository_id=None):
    trusted_names = {"qianyi-sun/loom", "ca-mrc/loom"}
    repository_id = repository_id or ("1281629473" if repository in trusted_names else "999")
    head_repository_id = head_repository_id or (1281629473 if head_repository in trusted_names else 999)
    context = {
        "github": SimpleNamespace(
            repository=repository, repository_id=repository_id, event_name=event, ref=ref,
            event=SimpleNamespace(workflow_run=SimpleNamespace(
                conclusion=conclusion,
                head_repository=SimpleNamespace(full_name=head_repository, id=head_repository_id),
            )),
        ),
        "vars": SimpleNamespace(NEBIUS_AUTO_ROLLOUT_ENABLED=enabled),
        "inputs": SimpleNamespace(operation=operation),
        "needs": SimpleNamespace(check=SimpleNamespace(
            outputs=SimpleNamespace(status=check_status),
        )),
    }
    # Evaluate this workflow's boolean/attribute-only job conditions with event
    # fixtures, so a new explanation cannot accidentally open the rollout path.
    eligible = {
        name for name, job in workflow["jobs"].items()
        if eval(job["if"].replace("&&", "and").replace("||", "or"),
                {"__builtins__": {}}, context)
    }
    if "check" not in eligible or check_result != "success":
        eligible.discard("rollout")
    return eligible


@pytest.mark.parametrize(("conclusion", "enabled", "expected"), [
    ("success", "true", {"check", "rollout"}),
    ("success", "false", {"explain-skip"}),
    ("success", "", {"explain-skip"}),
    ("failure", "true", {"explain-skip"}),
    ("failure", "false", {"explain-skip"}),
    ("cancelled", "true", {"explain-skip"}),
    ("timed_out", "true", {"explain-skip"}),
    ("skipped", "true", {"explain-skip"}),
])
def test_completed_publication_routing(workflow, conclusion, enabled, expected):
    assert selected_jobs(workflow, conclusion=conclusion, enabled=enabled) == expected


@pytest.mark.parametrize("operation", ["inspect", "certificate"])
@pytest.mark.parametrize("enabled", ["true", "false"])
def test_manual_readback_and_certificate_remain_isolated(workflow, operation, enabled):
    assert selected_jobs(workflow, event="workflow_dispatch", operation=operation,
                         enabled=enabled) == {operation}


@pytest.mark.parametrize(("enabled", "expected"), [("true", {"check", "rollout"}), ("false", set())])
def test_manual_rollout_keeps_existing_enablement_requirement(workflow, enabled, expected):
    assert selected_jobs(workflow, event="workflow_dispatch", operation="rollout",
                         enabled=enabled) == expected


@pytest.mark.parametrize("context", [
    {"head_repository": "untrusted/fork"},
    {"repository": "untrusted/fork", "head_repository": "untrusted/fork"},
    {"event": "workflow_dispatch", "operation": "rollout", "ref": "refs/heads/feature"},
])
def test_ineligible_sources_do_not_start_any_job(workflow, context):
    assert selected_jobs(workflow, conclusion="failure", **context) == set()


@pytest.mark.parametrize("repository", ["qianyi-sun/loom", "ca-mrc/loom"])
def test_repository_transfer_preserves_automatic_rollout(workflow, repository):
    assert selected_jobs(workflow, repository=repository, head_repository=repository) == {
        "check", "rollout",
    }


@pytest.mark.parametrize("identity", [
    {"repository_id": "999"},
    {"head_repository_id": 999},
])
def test_matching_repository_name_does_not_authorize_another_repository(workflow, identity):
    assert selected_jobs(workflow, repository="ca-mrc/loom", head_repository="ca-mrc/loom",
                         **identity) == set()


def test_explanation_has_no_protected_environment_or_deployment_credentials(workflow):
    job = workflow["jobs"]["explain-skip"]
    assert job["permissions"] == {"contents": "read"}
    assert "environment" not in job
    assert "env" not in job
    assert "secrets." not in json.dumps(job)
    checkout, report = job["steps"]
    assert checkout["uses"].startswith("actions/checkout@")
    assert checkout["with"] == {"ref": "dev", "persist-credentials": "false"}
    assert "uses" not in report
    assert report["env"] == {
        "AUTO_ROLLOUT_ENABLED": "${{ vars.NEBIUS_AUTO_ROLLOUT_ENABLED }}",
    }


@pytest.mark.parametrize(("conclusion", "enabled", "reason"), [
    ("failure", "true", "Upstream candidate publication failed"),
    ("cancelled", "true", "was cancelled"),
    ("success", "false", "NEBIUS_AUTO_ROLLOUT_ENABLED is not true"),
])
def test_selected_explanation_command_writes_actions_summary(
    workflow, tmp_path, conclusion, enabled, reason,
):
    assert selected_jobs(workflow, conclusion=conclusion, enabled=enabled) == {"explain-skip"}
    event_path = tmp_path / "event.json"
    event_path.write_text(json.dumps({
        "action": "completed",
        "workflow_run": {
            "id": 567, "conclusion": conclusion, "head_sha": "a" * 40,
            "head_repository": {"full_name": "qianyi-sun/loom"},
        },
    }))
    summary_path = tmp_path / "summary.md"
    report = workflow["jobs"]["explain-skip"]["steps"][1]
    result = subprocess.run(
        ["bash", "-e", "-c", report["run"]], cwd=ROOT,
        env={
            "PATH": os.environ["PATH"],
            "GITHUB_EVENT_PATH": str(event_path),
            "GITHUB_STEP_SUMMARY": str(summary_path),
            "GITHUB_REPOSITORY": "qianyi-sun/loom",
            "AUTO_ROLLOUT_ENABLED": enabled,
        },
        capture_output=True, text=True, check=True,
    )
    assert reason in summary_path.read_text()
    assert "https://github.com/qianyi-sun/loom/actions/runs/567" in summary_path.read_text()
    assert reason in result.stdout


@pytest.mark.parametrize(("enabled", "status", "expected"), [
    ("false", "ready", {"explain-skip"}),
    ("", "ready", {"explain-skip"}),
    ("true", "ready", {"check", "rollout"}),
    ("true", "skipped_busy", {"check"}),
    ("true", "skipped_already_deployed", {"check"}),
    ("true", "blocked_recovery", {"check"}),
    ("true", "skipped_superseded", {"check"}),
    ("true", "skipped_no_successful_publication", {"check"}),
])
def test_schedule_checks_pending_candidate_and_only_deploys_when_ready(
    workflow, enabled, status, expected,
):
    assert selected_jobs(workflow, event="schedule", enabled=enabled,
                         check_status=status) == expected


@pytest.mark.parametrize("result", ["failure", "cancelled", "skipped"])
def test_failed_check_never_deploys_even_if_it_emitted_ready(workflow, result):
    assert selected_jobs(workflow, check_status="ready", check_result=result) == {"check"}


@pytest.mark.parametrize("operation", ["rollout", "recover"])
def test_manual_rollout_and_recovery_use_check_dependency(workflow, operation):
    assert selected_jobs(workflow, event="workflow_dispatch", operation=operation) == {
        "check", "rollout",
    }
    assert selected_jobs(workflow, event="workflow_dispatch", operation=operation,
                         enabled="false") == set()


def test_retry_schedule_preserves_serialization_and_current_operator_tooling(workflow):
    assert workflow["on"]["schedule"] == [{"cron": "7-57/10 * * * *"}]
    assert workflow["concurrency"] == {
        "group": "nebius-integration-rollout", "queue": "max", "cancel-in-progress": "false",
    }
    for name in ("check", "rollout"):
        job = workflow["jobs"][name]
        checkouts = [step for step in job["steps"]
                     if step.get("uses", "").startswith("actions/checkout@")]
        assert len(checkouts) == 1
        assert checkouts[0]["with"]["ref"] == "dev"
        assert checkouts[0]["with"]["fetch-depth"] == "0"
    assert workflow["jobs"]["rollout"]["needs"] == "check"
    assert workflow["jobs"]["check"]["outputs"]["status"] == (
        "${{ steps.readiness.outputs.status || steps.publication.outputs.status }}"
    )


def run_step_with_recorded_uv(tmp_path, command, **environment):
    binary = tmp_path / "uv"
    binary.write_text("#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$ARGV_FILE\"\n")
    binary.chmod(0o755)
    arguments = tmp_path / "argv"
    subprocess.run(["bash", "-e", "-c", command], check=True, cwd=ROOT, env={
        "PATH": f"{tmp_path}:{os.environ['PATH']}", "ARGV_FILE": str(arguments),
        "RUNNER_TEMP": str(tmp_path), **environment,
    })
    return arguments.read_text().splitlines()


@pytest.mark.parametrize(("operation", "publication_run_id"), [
    ("", "1234"), ("", ""), ("rollout", ""), ("recover", ""),
])
def test_selection_coalesces_publications_but_keeps_trigger_and_recovery_binding(
    workflow, tmp_path, operation, publication_run_id,
):
    select = next(step for step in workflow["jobs"]["check"]["steps"]
                  if step.get("id") == "publication")
    args = run_step_with_recorded_uv(tmp_path, select["run"], OPERATION=operation,
                                    RECOVERY_RUN_ID="123", PUBLICATION_RUN_ID=publication_run_id)
    assert "--run-id" not in args
    if operation == "recover":
        assert args[-4:] == ["--recovery-run-id", "123", "--recovery-dir", str(tmp_path / "recovery")]
    elif publication_run_id:
        assert args[-2:] == ["--trigger-run-id", publication_run_id]
    else:
        assert args[-1] == "select"


@pytest.mark.parametrize("automatic", ["true", "false"])
@pytest.mark.parametrize(("job_name", "command_name"), [("check", "check"), ("rollout", "run")])
def test_remote_commands_keep_automatic_failure_policy_and_cleanup_credentials(
    workflow, tmp_path, automatic, job_name, command_name,
):
    step = next(step for step in workflow["jobs"][job_name]["steps"]
                if f"nebius_idle_rollout.py {command_name} " in step.get("run", ""))
    args = run_step_with_recorded_uv(
        tmp_path, step["run"], AUTOMATIC=automatic, RECOVERY_EVIDENCE="",
        DEPLOY_SSH_KEY="test-only-key", DEPLOY_KNOWN_HOSTS="test-only-host-key",
        LOOM_DEPLOY_SSH_TARGET="test-gateway", REMOTE_KUBECONFIG="/test/kubeconfig",
        CLUSTER_ID="test-cluster", CANDIDATE_SHA="a" * 40,
    )
    assert ("--automatic" in args) == (automatic == "true")
    assert "--github" in args
    assert args[args.index("--candidate") + 1] == "a" * 40
    assert not list(tmp_path.glob("nebius-*-key"))
    assert not list(tmp_path.glob("nebius-*-known-hosts"))
    if job_name == "check":
        assert "--publication-dir" not in args
        assert "inputs.operation != 'recover'" in step["if"]


def test_recovery_loads_original_evidence_in_deployment_job(workflow, tmp_path):
    steps = workflow["jobs"]["rollout"]["steps"]
    recovery = next(step for step in steps if step.get("id") == "recovery")
    assert recovery["if"] == "inputs.operation == 'recover'"
    args = run_step_with_recorded_uv(tmp_path, recovery["run"], OPERATION="recover",
                                    RECOVERY_RUN_ID="123")
    assert args[-4:] == ["--recovery-run-id", "123", "--recovery-dir", str(tmp_path / "recovery")]
    deploy = next(step for step in steps if step.get("name") == "Deploy once if idle")
    assert deploy["env"]["RECOVERY_EVIDENCE"] == "${{ steps.recovery.outputs.recovery_evidence }}"
    assert deploy["env"]["CANDIDATE_SHA"] == "${{ needs.check.outputs.candidate_sha }}"
