import json

import pytest
from scripts.ops.nebius_rollout_reporting import emit_result, main


@pytest.mark.parametrize("guard,expected", [
    ({"active": {"trials": 0, "executions": 0, "builds": 2, "build_cleanup": 0}},
     "2 claimed/running image build(s)"),
    ({"active": {"trials": 0, "executions": 0, "builds": 0, "build_cleanup": 1}},
     "1 image build(s) awaiting cleanup"),
    ({"active": {"trials": 0, "executions": 1, "builds": 0, "build_cleanup": 0}},
     "1 active/finalizing execution lease(s)"),
    ({"reason": "admission_in_progress"}, "Task admission is in progress"),
    ({}, "detailed counts are unavailable"),
])
def test_busy_reasons_distinguish_execution_builds_cleanup_and_admission(
    monkeypatch, tmp_path, capsys, guard, expected,
):
    summary = tmp_path / "summary.md"
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(summary))
    emit_result({"status": "skipped_busy", "guard": guard})
    body = summary.read_text()
    stdout = capsys.readouterr().out
    assert expected in body and expected in stdout
    assert "next scheduled check" in body
    assert "approximately every 10 minutes" in body
    assert "No deployment was applied" in body
    if "active" in guard:
        assert "Historical failed image builds alone do not block rollout" in body
    else:
        assert "| Blocking activity |" not in body


@pytest.mark.parametrize("status,expected", [
    ("skipped_locked", "Another deployment or recovery owns the rollout guard"),
    ("skipped_superseded", "supersedes this candidate"),
    ("skipped_no_platform_candidate", "no available platform candidate artifact"),
    ("skipped_before_idle_rollout_support", "predates automatic idle rollout support"),
    ("skipped_no_candidate", "No successful dev candidate publication"),
    ("ready", "deployment has not run yet"),
    ("complete", "Candidate deployed"),
])
def test_other_outcomes_are_not_reported_as_busy(monkeypatch, tmp_path, capsys, status, expected):
    summary = tmp_path / "summary.md"
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(summary))
    emit_result({"status": status})
    assert expected in summary.read_text()
    log = capsys.readouterr().out
    assert expected in log
    assert ("::notice" in log) == status.startswith("skipped_")
    if status == "skipped_locked":
        assert "do not clear its guard" in summary.read_text()


@pytest.mark.parametrize("status,outcome,retry", [
    ("complete", "deployed", False),
    ("skipped_busy", "waiting_idle", True),
    ("skipped_locked", "blocked_recovery", False),
    ("blocked_recovery", "blocked_recovery", False),
    ("skipped_already_deployed", "skipped", False),
    ("skipped_publication_cancelled", "skipped", False),
    ("failed", "failed", False),
])
def test_outcomes_distinguish_deployment_from_skips_and_retry_only_busy(
    monkeypatch, tmp_path, capsys, status, outcome, retry,
):
    summary = tmp_path / "summary.md"
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(summary))
    emit_result({"status": status})
    body = summary.read_text()
    result = json.loads(capsys.readouterr().out.splitlines()[-1])
    assert result["status"] == status
    assert result["outcome"] == outcome
    assert result["automatic_retry"] is retry
    assert f"## Nebius rollout: {outcome}" in body
    assert f"Decision: `{status}`" in body
    assert ("Automatic retry: **yes**" if retry else "Automatic retry: **no**") in body
    if status == "skipped_already_deployed":
        assert "successful deployment record and live version" in body
    if outcome == "blocked_recovery":
        assert "do not clear its guard" in body
        assert "No deployment was applied" in body
    if outcome == "failed":
        assert "No deployment was applied" not in body
        assert "Inspect the failed phase" in body


def test_report_does_not_copy_untrusted_evidence_into_annotations_or_summary(monkeypatch, tmp_path, capsys):
    summary = tmp_path / "summary.md"
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(summary))
    emit_result({
        "status": "skipped_busy", "candidate_sha": "invalid-private-sha", "run_id": "invalid-private-id",
        "guard": {"reason": "private-reason\n::error::injected", "active": {
            "trials": "private-count", "executions": -1, "private-metric": 2,
        }},
        "private_diagnostic": "private-provider-error",
    })
    output = capsys.readouterr().out
    assert "private-" not in summary.read_text() + output
    assert "::error::injected" not in output
    assert json.loads(output.splitlines()[-1])["status"] == "skipped_busy"


def test_report_links_follow_current_repository_name(monkeypatch, tmp_path):
    summary = tmp_path / "summary.md"
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(summary))
    monkeypatch.setenv("GITHUB_REPOSITORY", "ca-mrc/loom")
    emit_result({"status": "skipped_busy", "candidate_sha": "a" * 40, "run_id": "123"})
    body = summary.read_text()
    assert "https://github.com/ca-mrc/loom/commit/" in body
    assert "https://github.com/ca-mrc/loom/actions/runs/123" in body


@pytest.mark.parametrize("conclusion", ["timed_out", "action_required", "skipped", "neutral"])
def test_other_publication_outcomes_keep_the_exact_conclusion(capsys, conclusion):
    emit_result({"status": "skipped_publication_unsuccessful", "conclusion": conclusion})
    assert f"did not succeed ({conclusion})" in capsys.readouterr().out


def test_disabled_schedule_reports_skip_without_publication_link(monkeypatch, tmp_path, capsys):
    event = tmp_path / "event.json"
    event.write_text(json.dumps({"schedule": "*/10 * * * *"}))
    summary = tmp_path / "summary.md"
    monkeypatch.setenv("GITHUB_EVENT_PATH", str(event))
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(summary))
    monkeypatch.setenv("AUTO_ROLLOUT_ENABLED", "false")
    monkeypatch.setattr("sys.argv", ["reporting", "publication"])
    assert main() == 0
    result = json.loads(capsys.readouterr().out.splitlines()[-1])
    assert result["status"] == "skipped_disabled"
    assert result["automatic_retry"] is False
    assert "Source candidate publication" not in summary.read_text()
