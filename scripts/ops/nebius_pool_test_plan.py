"""Reuse native CI's exact-commit pool coverage; execute only missing modules.

This plans tests, not branch admission. GitHub's four protected gates and the
candidate's image build/scan/publication remain unchanged. Unknown evidence is
missing coverage, so unavailable Actions metadata falls back to running tests.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from scripts.component_ownership import (
    _tracked_paths,
    load_manifest,
    selected_test_paths,
)

REPOSITORY = "ca-mrc/loom"
REPOSITORY_ID = 1281629473
Read = Callable[[str], list[dict[str, Any]]]


def github_pages(path: str) -> list[dict[str, Any]]:
    result = subprocess.run(
        ["gh", "api", "--paginate", "--slurp", f"repos/{REPOSITORY}/{path}"],
        check=True,
        capture_output=True,
        text=True,
        timeout=60,
    )
    pages = json.loads(result.stdout)
    if not isinstance(pages, list) or not all(isinstance(page, dict) for page in pages):
        raise ValueError("actions_read_incomplete")
    return pages


def rows(pages: list[dict[str, Any]], key: str) -> list[dict[str, Any]]:
    if not pages or not all(isinstance(page.get(key), list) for page in pages):
        raise ValueError("actions_read_incomplete")
    # Missing pages/jobs cannot prove coverage, but do not invalidate successful
    # owning jobs already returned for this exact run. Missing names naturally
    # leave their modules in the execution plan below.
    return [row for page in pages for row in page[key]]


def source_changes(root: Path, base: str, candidate: str) -> tuple[str, ...]:
    result = subprocess.run(
        ["git", "diff", "--name-only", "-z", base, candidate],
        cwd=root,
        check=True,
        capture_output=True,
        timeout=30,
    )
    return tuple(path.decode("utf-8") for path in result.stdout.split(b"\0") if path)


def candidate_changes(root: Path, candidate: str) -> tuple[str, ...]:
    # Protected dev uses one-PR squash groups. Checkout fetch-depth=2 supplies
    # this immutable candidate's actual first parent, not today's moving dev.
    return source_changes(root, candidate + "^", candidate)


def plan(root: Path, candidate: str, *, read: Read = github_pages) -> dict[str, Any]:
    if not re.fullmatch(r"[0-9a-f]{40}", candidate):
        raise ValueError("candidate_sha_invalid")
    manifest = load_manifest(root / "config/component-ownership.toml")
    tracked = _tracked_paths(root)
    policy = manifest.test_shard_policy("nebius-pool")
    if policy is None:
        raise ValueError("pool_shard_policy_missing")

    def test_paths(
        *, changed_paths: tuple[str, ...] = (), shard_index: int = 0, shard_count: int = 1
    ) -> tuple[str, ...]:
        return selected_test_paths(
            manifest,
            tracked_paths=tracked,
            lane="nebius-pool",
            repo_root=root,
            test_scope="nebius",
            suite="nebius-pool-ops",
            changed_paths=changed_paths,
            shard_index=shard_index,
            shard_count=shard_count,
        )

    try:
        changes = candidate_changes(root, candidate)
    except (OSError, ValueError, subprocess.SubprocessError):
        changes = ()  # Unknown boundary retains the full regression suite.
    required = set(test_paths(changed_paths=changes))
    covered: set[str] = set()
    reused_runs: list[int] = []
    unavailable = False
    try:
        runs = rows(
            read(
                "actions/workflows/ci.yml/runs?event=merge_group&status=success"
                f"&head_sha={candidate}&per_page=100"
            ),
            "workflow_runs",
        )
    except (OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError):
        runs = []
        unavailable = True
    for run in runs:
        try:
            # Only the actual native dev merge group's CI can be reused. PR
            # heads, a different checkout, filtered runs and renamed statuses
            # cannot stand in for this fixed candidate's successful jobs.
            if (
                run["head_sha"] != candidate
                or run["event"] != "merge_group"
                or run["status"] != "completed"
                or run["conclusion"] != "success"
                or run["path"] != ".github/workflows/ci.yml"
                or run["repository"]["id"] != REPOSITORY_ID
                or run["head_repository"]["id"] != REPOSITORY_ID
                or not run["head_branch"].startswith("gh-readonly-queue/dev/")
            ):
                continue
            title = re.match(
                r"^gate=full / head=([0-9a-f]{40}) / base=([0-9a-f]{40}) / ", run["display_title"]
            )
            if title is None or title[1] != candidate:
                continue
            [suite] = read(f"check-suites/{run['check_suite_id']}")
            if suite["app"]["id"] != 15368 or suite["head_sha"] != candidate:
                continue
            changes = source_changes(root, title[2], candidate)
            selected = set(test_paths(changed_paths=changes))
            # A job-only retry retains other successful jobs on this commit.
            # Use each job's latest result, not only jobs rerun in that attempt.
            jobs = rows(read(f"actions/runs/{run['id']}/jobs?filter=latest&per_page=100"), "jobs")
            successful = {
                job["name"]
                for job in jobs
                if job["status"] == "completed" and job["conclusion"] == "success"
            }
            verified: set[str] = set()
            for index in range(policy.shard_count):
                if f"nebius-pool ({index + 1}-of-{policy.shard_count})" in successful:
                    # Reconstruct the very same manifest selection used by CI.
                    # A passing narrowed job covers only its selected files,
                    # never all other files assigned to that shard.
                    paths = test_paths(shard_index=index, shard_count=policy.shard_count)
                    verified.update(set(paths) & selected)
            if verified:
                covered.update(verified)
                reused_runs.append(run["id"])
        except (OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError):
            # Retain already qualified coverage from other successful runs.
            unavailable = True
    covered &= required
    missing = required - covered
    matrix = []
    for index in range(policy.shard_count):
        paths = test_paths(shard_index=index, shard_count=policy.shard_count)
        pending = sorted(set(paths) & missing)
        if pending:
            matrix.append({"shard_index": index, "paths": pending})
    return dict(
        candidate_sha=candidate,
        required=bool(missing),
        required_files=len(required),
        reused_files=len(covered),
        missing_files=len(missing),
        reused_runs=reused_runs,
        evidence_unavailable=unavailable,
        matrix={"include": matrix},
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate", default=os.environ.get("GITHUB_SHA"), required=False)
    parser.add_argument("--github-output", type=Path)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=root, check=True, capture_output=True, text=True
    ).stdout.strip()
    if args.candidate != head:
        raise ValueError("candidate_checkout_mismatch")
    value = plan(root, head)
    if args.github_output:
        with args.github_output.open("a", encoding="utf-8") as stream:
            stream.write(f"required={str(value['required']).lower()}\n")
            stream.write("matrix=" + json.dumps(value["matrix"], separators=(",", ":")) + "\n")
    print(json.dumps(value, sort_keys=True))


if __name__ == "__main__":
    main()
