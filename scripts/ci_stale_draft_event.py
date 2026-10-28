"""Decide whether a draft-payload PR event is stale against the live pull request.

GitHub evaluates required checks from the newest suite of a workflow on a head.
A Draft `synchronize` delivered in the same second as `ready_for_review` can get
a newer suite than the Ready run. Filtered, it would hide that run's required
contexts and block auto-merge on a green head. That stale event runs the full
gate instead, but only when the live PR is Ready on the same head and its
newest full suite is older than this run. GitHub can also drop the Ready run
entirely (#2677); when no full suite appears on a Ready head, this run is the
only one left to publish the required contexts and runs the full gate too.
API failures keep the event filtered, which cannot authorize a merge.
"""

from __future__ import annotations

import json
import os
import re
import time
import urllib.request
from collections.abc import Callable
from typing import Any

SHA = re.compile(r"[0-9a-f]{40}")
REPOSITORY = re.compile(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+")
# Both events arrive within seconds; allow the Ready run to become listable.
FULL_SUITE_ATTEMPTS = 4
FULL_SUITE_INTERVAL_SECONDS = 10.0


def read_json(api_url: str, token: str) -> Callable[[str], Any]:
    def read(endpoint: str) -> Any:
        request = urllib.request.Request(
            f"{api_url}/{endpoint}",
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {token}",
                "X-GitHub-Api-Version": "2022-11-28",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=20) as response:
                return json.loads(response.read())
        except (OSError, ValueError):
            raise ValueError(f"GitHub API read failed: {endpoint.split('?')[0]}") from None

    return read


def _suite_id(run: object) -> int:
    value = run.get("check_suite_id") if isinstance(run, dict) else None
    if type(value) is not int:
        raise ValueError("workflow run has no check suite")
    return value


def classify(
    *,
    repository: str,
    pull_number: str,
    head_sha: str,
    run_id: str,
    read: Callable[[str], Any],
    sleep: Callable[[float], None] = time.sleep,
) -> tuple[str, str]:
    """Return (`full` | `filtered`, reason) for a draft-payload event."""

    if not REPOSITORY.fullmatch(repository) or any(part in {".", ".."} for part in repository.split("/")):
        raise ValueError("invalid repository")
    if not pull_number.isdigit() or not run_id.isdigit() or not SHA.fullmatch(head_sha):
        raise ValueError("invalid pull request, run or head identity")

    pull = read(f"repos/{repository}/pulls/{pull_number}")
    if not isinstance(pull, dict) or not isinstance(pull.get("head"), dict):
        raise ValueError("invalid pull request response")
    draft, state, live_head = pull.get("draft"), pull.get("state"), pull["head"].get("sha")
    if type(draft) is not bool or not isinstance(state, str) or not isinstance(live_head, str):
        raise ValueError("invalid pull request response")
    if state != "open" or draft:
        return "filtered", f"live pull request is {'draft' if draft else state}"
    if live_head != head_sha:
        return "filtered", "live pull request has a newer head"

    own = read(f"repos/{repository}/actions/runs/{run_id}")
    own_suite = _suite_id(own)
    workflow_id = own.get("workflow_id")
    if type(workflow_id) is not int:
        raise ValueError("workflow run has no workflow")
    title = f"gate=full / head={head_sha} / "
    for attempt in range(FULL_SUITE_ATTEMPTS):
        if attempt:
            sleep(FULL_SUITE_INTERVAL_SECONDS)
        listing = read(
            f"repos/{repository}/actions/workflows/{workflow_id}/runs"
            f"?event=pull_request&head_sha={head_sha}&per_page=100"
        )
        runs = listing.get("workflow_runs") if isinstance(listing, dict) else None
        if not isinstance(runs, list) or listing.get("total_count") != len(runs):
            raise ValueError("incomplete workflow run listing")
        full = [
            _suite_id(run) for run in runs
            if isinstance(run, dict) and str(run.get("display_title", "")).startswith(title)
        ]
        if full:
            if max(full) > own_suite:
                return "filtered", "a newer full suite exists on this head"
            return "full", "stale draft event is newer than the Ready full suite on this head"
    # Ready on this head with no Ready run: GitHub dropped it. If it arrives
    # later its newer suite still wins; the cost is one extra full run.
    return "full", "live pull request is Ready on this head and has no full suite"


def main() -> int:
    try:
        mode, reason = classify(
            repository=os.environ["GITHUB_REPOSITORY"],
            pull_number=os.environ["PR_NUMBER"],
            head_sha=os.environ["HEAD_SHA"],
            run_id=os.environ["GITHUB_RUN_ID"],
            read=read_json(os.environ.get("GITHUB_API_URL", "https://api.github.com"), os.environ["GH_TOKEN"]),
        )
    except (KeyError, ValueError) as error:
        mode, reason = "filtered", f"live state unavailable ({error}); kept filtered"
        print(f"::warning::{reason}")
    print(f"draft event: {mode}: {reason}")
    with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as output:
        output.write(f"stale_draft={'true' if mode == 'full' else 'false'}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
