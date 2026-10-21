"""Share native CI feedback before the four source workflows start heavy jobs."""

from __future__ import annotations

import argparse
import json
import os
import re
import time
from collections.abc import Callable
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

FEEDBACK_JOBS = {
    "lint-and-static": "lint_and_static",
    "locked-environments": "locked_environments",
    "tests-packages": "tests_packages",
    "go-checks": "go_checks",
    "nebius-iac": "nebius_iac",
    "tests-root": "tests_root",
    "integration": "integration",
    "web-checks": "web_checks",
}


def enforce(needs: dict[str, Any]) -> None:
    plan = needs["workflow-plan"]
    if plan["result"] != "success" or plan["outputs"]["gate_mode"] != "full":
        raise ValueError("feedback requires a successful full planner")
    for job, selector in FEEDBACK_JOBS.items():
        selected = plan["outputs"].get(selector)
        result = needs[job]["result"]
        if (selected, result) not in {("true", "success"), ("false", "skipped")}:
            raise ValueError(f"feedback rejected {job}: selected={selected!r}, result={result!r}")


def get_json(path: str) -> dict[str, Any]:
    api = os.environ.get("GITHUB_API_URL", "https://api.github.com").rstrip("/")
    request = Request(api + path, headers={
        "Authorization": "Bearer " + os.environ["GH_TOKEN"],
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    })
    with urlopen(request, timeout=30) as response:
        value = json.load(response)
    if not isinstance(value, dict):
        raise ValueError("unexpected Actions API response")
    return value


def pages(path: str, key: str, fetch: Callable[[str], dict[str, Any]]) -> list[dict[str, Any]]:
    result = []
    separator = "&" if "?" in path else "?"
    for page in range(1, 11):
        value = fetch(f"{path}{separator}per_page=100&page={page}")
        rows = value[key]
        if not isinstance(rows, list):
            raise ValueError("invalid Actions API page")
        result.extend(rows)
        if len(rows) < 100:
            return result
    raise ValueError("Actions API pagination exceeded its bounded search")


def wait_for_feedback(
    repository: str,
    source_run_id: int,
    *,
    timeout_seconds: float,
    fetch: Callable[[str], dict[str, Any]] = get_json,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> int:
    """Match the shared native event identity, never just a green head check."""
    if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository):
        raise ValueError("invalid repository")
    if source_run_id <= 0 or not 0 < timeout_seconds <= 5400:
        raise ValueError("invalid feedback wait budget or source run")
    root = f"/repos/{repository}/actions"
    source = fetch(f"{root}/runs/{source_run_id}")
    event, head, title = source["event"], source["head_sha"], source["display_title"]
    if event not in {"pull_request", "merge_group"} or not re.fullmatch(r"[0-9a-f]{40}", head):
        raise ValueError("feedback waits require a PR or merge-group source SHA")
    if source["path"] not in {
        ".github/workflows/images.yml", ".github/workflows/cluster-smoke.yml",
        ".github/workflows/staging-smoke.yml",
    } or not re.match(rf"gate=full / head={head} / base=[0-9a-f]{{40}} / ", title):
        raise ValueError("source lacks the native full-validation identity")
    # The shared title binds base, PR number, event/update and all scope labels.
    # Both metadata events and regenerated merge groups therefore need their own
    # matching source run. No custom status/check publisher is involved.
    query = urlencode({"event": event, "head_sha": head})
    deadline = clock() + timeout_seconds
    poll_interval = 15.0
    while clock() < deadline:
        candidates = pages(f"{root}/workflows/ci.yml/runs?{query}", "workflow_runs", fetch)
        matching = [run for run in candidates if (
            run["event"] == event and run["head_sha"] == head
            and run["display_title"] == title and run["path"] == ".github/workflows/ci.yml"
        )]
        if matching:
            run = max(matching, key=lambda row: row["id"])
            jobs = pages(f"{root}/runs/{run['id']}/jobs?filter=latest", "jobs", fetch)
            feedback = [job for job in jobs if job["name"] == "fast-feedback"
                        and job["run_attempt"] == run["run_attempt"]]
            if len(feedback) > 1:
                raise ValueError("ambiguous native feedback job")
            if feedback and feedback[0]["status"] == "completed":
                if feedback[0]["conclusion"] != "success":
                    raise ValueError("native CI fast feedback did not succeed")
                return int(run["id"])
            if run["status"] == "completed":
                raise ValueError("native CI ended without successful fast feedback")
        sleep(min(poll_interval, max(0, deadline - clock())))
        poll_interval = min(120, poll_interval * 2)
    raise TimeoutError("matching native CI fast feedback did not finish within the wait budget")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("enforce")
    wait = commands.add_parser("wait")
    wait.add_argument("--timeout-seconds", type=int, default=5400)
    args = parser.parse_args()
    try:
        if args.command == "enforce":
            enforce(json.loads(os.environ["FEEDBACK_NEEDS"]))
        else:
            run_id = wait_for_feedback(
                os.environ["GITHUB_REPOSITORY"], int(os.environ["GITHUB_RUN_ID"]),
                timeout_seconds=args.timeout_seconds,
            )
            print(f"Native CI run {run_id} passed fast feedback; selected heavy work may start.")
    except (ValueError, KeyError, TimeoutError) as error:
        parser.exit(1, f"FAIL: {error}\n")


if __name__ == "__main__":
    main()
