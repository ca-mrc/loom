#!/usr/bin/env python3
"""Explain rollout decisions in Actions without importing deployment tooling."""
from __future__ import annotations

import argparse
import json
import os
import re
from pathlib import Path

REPOSITORY = "qianyi-sun/loom"
ACTIVITY = {
    "trials": ("Claimed/running trials", "claimed/running trial(s)"),
    "executions": ("Execution leases still active or finalizing", "active/finalizing execution lease(s)"),
    "builds": ("Claimed/running image builds", "claimed/running image build(s)"),
    "build_cleanup": ("Image builds awaiting cleanup", "image build(s) awaiting cleanup"),
}
PUBLICATION_CONCLUSIONS = {
    "failure", "cancelled", "timed_out", "action_required", "stale", "skipped", "neutral", "startup_failure",
}


def activity_counts(result: dict) -> dict[str, int]:
    active = result.get("guard", {}).get("active", {})
    return {key: active[key] for key in ACTIVITY
            if type(active.get(key)) is int and active[key] >= 0}


def repository_name() -> str:
    # This reporter stays independent of deployment tooling and credentials.
    value = os.environ.get("GITHUB_REPOSITORY", REPOSITORY)
    return value if value in {REPOSITORY, "ca-mrc/loom"} else REPOSITORY


def explanation(result: dict) -> str:
    status = result["status"]
    if status == "skipped_publication_unsuccessful" and result.get("conclusion") in PUBLICATION_CONCLUSIONS:
        return f"Upstream candidate publication did not succeed ({result['conclusion']}); no deployment was attempted."
    if status == "skipped_busy":
        if result.get("guard", {}).get("reason") == "admission_in_progress":
            return "Task admission is in progress; the idle check could not reserve the environment."
        counts = activity_counts(result)
        reasons = [f"{count} {ACTIVITY[key][1]}" for key, count in counts.items() if count]
        return ("Work is still active: " + "; ".join(reasons) + "." if reasons
                else "The environment reported active work; detailed counts are unavailable.")
    return {
        "ready": "Candidate is eligible for deployment; deployment has not run yet and must acquire the idle guard.",
        "complete": "Candidate deployed; HTTPS and workload versions verified; dispatch resumed.",
        "skipped_locked": "Another deployment or recovery owns the rollout guard; dispatch is paused.",
        "blocked_recovery": "A previous deployment needs investigation or recovery; automatic deployment is blocked.",
        "failed": "Deployment failed; the environment may be partially updated or paused.",
        "skipped_already_deployed": "The successful deployment record and live version already match this candidate.",
        "skipped_superseded": "The deployed version supersedes this candidate, or changed during the idle check.",
        "skipped_no_candidate": "No successful dev candidate publication is available.",
        "skipped_no_platform_candidate": "Publication produced no available platform candidate artifact (for example, a harness-only build).",
        "skipped_before_idle_rollout_support": "This candidate predates automatic idle rollout support.",
        "skipped_publication_failed": "Upstream candidate publication failed; no deployment was attempted.",
        "skipped_publication_cancelled": "Upstream candidate publication was cancelled; no deployment was attempted.",
        "skipped_publication_unsuccessful": "Upstream candidate publication did not succeed; no deployment was attempted.",
        "skipped_disabled": "Automatic rollout is disabled: NEBIUS_AUTO_ROLLOUT_ENABLED is not true.",
    }.get(status, "See the sanitized deployment evidence for this result.")


def rollout_outcome(status: str) -> str:
    """Keep deployment decision codes stable while making their result explicit."""
    return {
        "complete": "deployed",
        "skipped_busy": "waiting_idle",
        "skipped_locked": "blocked_recovery",
        "blocked_recovery": "blocked_recovery",
        "failed": "failed",
        "ready": "ready",
    }.get(status, "skipped" if status.startswith("skipped_") else status)


def emit_result(result: dict) -> None:
    """Publish only decision fields, never raw deployment or provider evidence."""
    description = explanation(result)
    status = result["status"]
    outcome = rollout_outcome(status)
    automatic_retry = status == "skipped_busy"
    lines = [f"## Nebius rollout: {outcome}", "", f"Decision: `{status}`", "", description, "",
             f"Automatic retry: **{'yes' if automatic_retry else 'no'}**.", ""]
    sha = result.get("candidate_sha", result.get("sha", ""))
    if re.fullmatch(r"[0-9a-f]{40}", sha):
        lines += [f"Candidate: [`{sha[:12]}`](https://github.com/{repository_name()}/commit/{sha})", ""]
    run_id = str(result.get("run_id", ""))
    if run_id.isdigit():
        lines += [f"[Source candidate publication #{run_id}](https://github.com/{repository_name()}/actions/runs/{run_id})", ""]
    if counts := activity_counts(result):
        lines += ["| Blocking activity | Count |", "| --- | ---: |"]
        lines += [f"| {ACTIVITY[key][0]} | {count} |" for key, count in counts.items()]
        lines += ["", "Trial and execution counts can describe the same task; do not add them together.",
                  "Historical failed image builds alone do not block rollout; unfinished cleanup can.", ""]
    if status.startswith("skipped_") or status == "blocked_recovery":
        lines += ["**No deployment was applied by this run.**", ""]
        if outcome == "blocked_recovery":
            lines += ["Inspect the owning deployment/recovery before retrying; do not clear its guard.", ""]
        elif status == "skipped_busy":
            lines += ["The next scheduled check selects the latest eligible full platform publication, "
                      "approximately every 10 minutes while automatic rollout is enabled. "
                      "This run exits without reserving a deployment window or creating a backup; "
                      "GitHub may delay scheduled checks.", ""]
        elif status.startswith("skipped_publication_"):
            lines += ["Open the source publication above, resolve its failed/cancelled work, and publish successfully before rollout.", ""]
        # Fixed vocabulary and integer counts only: no raw external error strings.
        print(f"::notice title=Nebius rollout {outcome}::{description}")
    if outcome == "failed":
        lines += ["Inspect the failed phase and persisted dispatch guard before explicit retry or recovery; "
                  "failed deployments are not retried automatically.", ""]
    if summary := os.environ.get("GITHUB_STEP_SUMMARY"):
        with Path(summary).open("a") as stream:
            stream.write("\n".join(lines) + "\n")
    output = {"status": status, "outcome": outcome, "automatic_retry": automatic_retry,
              "description": description}
    if re.fullmatch(r"[0-9a-f]{40}", sha):
        output["candidate_sha"] = sha
    print(json.dumps(output))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("publication",))
    parser.parse_args()
    run = json.loads(Path(os.environ["GITHUB_EVENT_PATH"]).read_text()).get("workflow_run")
    if run is None:
        if os.environ.get("AUTO_ROLLOUT_ENABLED") != "true":
            emit_result({"status": "skipped_disabled"})
        return 0
    conclusion = run.get("conclusion")
    if conclusion != "success":
        status = {"failure": "skipped_publication_failed", "cancelled": "skipped_publication_cancelled"}.get(
            conclusion, "skipped_publication_unsuccessful")
    elif os.environ.get("AUTO_ROLLOUT_ENABLED") != "true":
        status = "skipped_disabled"
    else:
        return 0
    emit_result({"status": status, "run_id": run["id"], "sha": run["head_sha"], "conclusion": conclusion})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
