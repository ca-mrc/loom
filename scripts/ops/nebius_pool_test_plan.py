"""Reuse native CI's verified candidate-tree coverage; run missing modules.

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
import time
from collections.abc import Callable
from datetime import datetime, timedelta
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
CHECKOUT_ACTION = "actions/checkout@93cb6efe18208431cddfb8368fd83d5badbf9bfd"
EVIDENCE_REASONS = {
    "actions_read_incomplete", "checkout_log_too_large", "pool_checkout_unqualified",
    "checkout_time_unqualified", "checkout_log_incomplete", "pool_scope_unqualified",
    "pool_selection_unqualified", "checkout_sha_unqualified", "checkout_ref_unqualified",
    "candidate_reuse_budget_exhausted", "pool_job_unqualified", "candidate_commit_unqualified",
    "candidate_pr_unqualified",
}


def github_pages(path: str) -> list[dict[str, Any]]:
    result = subprocess.run(
        ["gh", "api", "--paginate", "--slurp", f"repos/{REPOSITORY}/{path}"],
        check=True,
        capture_output=True,
        text=True,
        timeout=60,
    )
    pages = json.loads(result.stdout)
    if not isinstance(pages, list):
        raise ValueError("actions_read_incomplete")
    # Object endpoints produce one object per page; association endpoints
    # (commit -> PRs) produce arrays. Keep the same read-only API boundary.
    values = [value for page in pages for value in (page if isinstance(page, list) else [page])]
    if not all(isinstance(value, dict) for value in values):
        raise ValueError("actions_read_incomplete")
    return values


def github_job_log(job_id: int) -> str:
    result = subprocess.run(
        ["gh", "api", f"repos/{REPOSITORY}/actions/jobs/{job_id}/logs"],
        check=True,
        capture_output=True,
        text=True,
        timeout=20,
    )
    if len(result.stdout) > 4 * 1024 * 1024:
        raise ValueError("checkout_log_too_large")
    return result.stdout


def checkout_inputs(job: dict[str, Any], log: str, number: int) -> tuple[str, tuple[str, ...]]:
    """Read the pinned checkout action, before any repository command runs.

    The run/check-suite SHA is the PR head, not the tested merge commit. Do
    not use today's mutable refs/pull merge ref or a later command's output.
    Missing/changed action output simply leaves this shard's tests pending.
    """
    [step] = [s for s in job["steps"] if s["name"] == f"Run {CHECKOUT_ACTION}"]
    if (
        step["number"] != 2
        or step["conclusion"] != "success"
        or not any(
            s["name"] == "Pytest — manifest-owned pool shard" and s["conclusion"] == "success"
            for s in job["steps"]
        )
    ):
        raise ValueError("pool_checkout_unqualified")
    start = datetime.fromisoformat(step["started_at"].replace("Z", "+00:00"))
    # Actions' step API timestamps have second precision; log timestamps do not.
    end = datetime.fromisoformat(step["completed_at"].replace("Z", "+00:00")) + timedelta(seconds=1)
    if start.tzinfo is None or end.tzinfo is None:
        raise ValueError("checkout_time_unqualified")
    lines: list[str] = []
    found = finished = False
    for line in log.lstrip("\ufeff").splitlines():
        stamp, _, body = line.partition(" ")
        if body == f"##[group]Run {CHECKOUT_ACTION}" and not found:
            found = True
        elif found and body.startswith("##[group]Run "):
            finished = True
            break
        if found:
            when = datetime.fromisoformat(stamp.replace("Z", "+00:00"))
            if not start <= when < end:
                raise ValueError("checkout_time_unqualified")
            lines.append(body)
    if not found or not finished:
        raise ValueError("checkout_log_incomplete")
    if (
        "  CI_TEST_SCOPE: nebius" not in lines
        or "  CI_PYTEST_MARKERS: not legacy_pool" not in lines
    ):
        raise ValueError("pool_scope_unqualified")
    [hint] = [
        line.removeprefix("  TEST_CHANGED_PATHS: ")
        for line in lines
        if line.startswith("  TEST_CHANGED_PATHS: ")
    ]
    paths = json.loads(hint)
    if not isinstance(paths, list) or not all(isinstance(path, str) for path in paths):
        raise ValueError("pool_selection_unqualified")
    [index] = [
        i
        for i, line in enumerate(lines)
        if re.fullmatch(r"\[command\]/\S*/git log -1 --format=%H", line)
    ]
    sha = lines[index + 1]
    if not re.fullmatch(r"[0-9a-f]{40}", sha):
        raise ValueError("checkout_sha_unqualified")
    ref = f"refs/remotes/pull/{number}/merge"
    if not any(line.endswith(f" +{sha}:{ref}") and " fetch " in line for line in lines) or not any(
        re.fullmatch(r"\[command\]/\S*/git checkout --progress --force " + re.escape(ref), line)
        for line in lines
    ):
        raise ValueError("checkout_ref_unqualified")
    return sha, tuple(paths)


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


def plan(
    root: Path,
    candidate: str,
    *,
    read: Read = github_pages,
    read_log: Callable[[int], str] | None = None,
) -> dict[str, Any]:
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
    errors: list[dict[str, Any]] = []
    association_counts: dict[str, int] = {}

    def unavailable_evidence(
        stage: str, error: Exception, *, run_id: Any = None, job_id: Any = None,
    ) -> None:
        nonlocal unavailable
        unavailable = True
        if len(errors) >= 25:
            return
        # Never serialize stderr, exception text, commands or response bodies:
        # even a JSON/OS/CLI failure may contain inherited credentials.
        kind = "validation"
        if isinstance(error, subprocess.TimeoutExpired):
            kind = "subprocess_timeout"
        elif isinstance(error, subprocess.CalledProcessError):
            kind = "github_cli"
        elif isinstance(error, json.JSONDecodeError):
            kind = "invalid_json"
        elif isinstance(error, OSError):
            kind = "os_error"
        elif isinstance(error, LookupError):
            kind = "missing_field"
        row: dict[str, Any] = {"stage": stage, "error": kind}
        if isinstance(error, subprocess.CalledProcessError) and isinstance(error.stderr, str):
            status = re.search(r"\(HTTP ([1-5][0-9]{2})\)", error.stderr)
            if status:
                row["http_status"] = int(status[1])
            for phrase, reason in (
                ("secondary rate limit", "secondary_rate_limit"),
                ("api rate limit exceeded", "rate_limit"),
                ("resource not accessible by integration", "permission_denied"),
                ("bad credentials", "bad_credentials"),
            ):
                if phrase in error.stderr.lower():
                    row["api_reason"] = reason
                    break
        if stage == "squash_pr_association":
            row.update(association_counts)
        if error.args and isinstance(error.args[0], str) and error.args[0] in EVIDENCE_REASONS:
            row["reason"] = error.args[0]
        for key, value in (("run_id", run_id), ("job_id", job_id)):
            if type(value) is int and value > 0:
                row[key] = value
        errors.append(row)

    stage = "merge_group_runs"
    try:
        runs = rows(
            read(
                "actions/workflows/ci.yml/runs?event=merge_group&status=success"
                f"&head_sha={candidate}&per_page=100"
            ),
            "workflow_runs",
        )
    except (OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError) as error:
        runs = []
        unavailable_evidence(stage, error)
    for run in runs:
        stage = "merge_group_run"
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
            stage = "merge_group_check_suite"
            [suite] = read(f"check-suites/{run['check_suite_id']}")
            if suite["app"]["id"] != 15368 or suite["head_sha"] != candidate:
                continue
            stage = "merge_group_selection"
            changes = source_changes(root, title[2], candidate)
            selected = set(test_paths(changed_paths=changes))
            # A job-only retry retains other successful jobs on this commit.
            # Use each job's latest result, not only jobs rerun in that attempt.
            stage = "merge_group_jobs"
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
        except (OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError) as error:
            # Retain already qualified coverage from other successful runs.
            unavailable_evidence(stage, error, run_id=run.get("id"))
    if required - covered:
        # Queue-off native squash creates a new commit, but its tested PR merge
        # can have exactly the same tree. Reuse only that merged PR's own jobs,
        # proving each actual checkout; never trust a green PR head by itself.
        deadline = time.monotonic() + 120
        api_cache: dict[str, list[dict[str, Any]]] = {}
        log_cache: dict[int, str] = {}

        def evidence(endpoint: str) -> list[dict[str, Any]]:
            if endpoint not in api_cache:
                if time.monotonic() >= deadline:
                    raise TimeoutError("candidate_reuse_budget_exhausted")
                api_cache[endpoint] = read(endpoint)
            return api_cache[endpoint]

        def job_log(job_id: int) -> str:
            if type(job_id) is not int or job_id <= 0:
                raise ValueError("pool_job_unqualified")
            if job_id not in log_cache:
                if time.monotonic() >= deadline:
                    raise TimeoutError("candidate_reuse_budget_exhausted")
                log_cache[job_id] = (read_log or github_job_log)(job_id)
            return log_cache[job_id]

        try:
            stage = "candidate_commit"
            [commit] = evidence(f"git/commits/{candidate}")
            [parent] = commit["parents"]
            base, tree = parent["sha"], commit["tree"]["sha"]
            if commit["sha"] != candidate or not all(
                re.fullmatch(r"[0-9a-f]{40}", value) for value in (base, tree)
            ):
                raise ValueError("candidate_commit_unqualified")
            stage = "squash_pr_association"
            associated = evidence(f"commits/{candidate}/pulls?per_page=100")
            association_counts["associated_prs"] = len(associated)
            candidates = [
                pr
                for pr in associated
                if (
                    pr["state"] == "closed"
                    and pr["merged_at"] is not None
                    and pr["merge_commit_sha"] == candidate
                    and pr["base"]["ref"] == "dev"
                    and pr["base"]["sha"] == base
                    and pr["base"]["repo"]["id"] == REPOSITORY_ID
                    and pr["head"]["repo"]["id"] == REPOSITORY_ID
                )
            ]
            association_counts["matching_prs"] = len(candidates)
            if len(candidates) != 1:
                raise ValueError("candidate_pr_unqualified")
            [pr] = candidates
            head, number = pr["head"]["sha"], pr["number"]
            if type(number) is not int or number <= 0 or not re.fullmatch(r"[0-9a-f]{40}", head):
                raise ValueError("candidate_pr_unqualified")
            stage = "squash_pr_runs"
            pr_runs = rows(
                evidence(
                    "actions/workflows/ci.yml/runs?event=pull_request&status=success"
                    f"&head_sha={head}&per_page=100"
                ),
                "workflow_runs",
            )
            for run in pr_runs:
                stage = "squash_run"
                try:
                    if (
                        run["event"] != "pull_request"
                        or run["head_sha"] != head
                        or run["status"] != "completed"
                        or run["conclusion"] != "success"
                        or run["path"] != ".github/workflows/ci.yml"
                        or run["repository"]["id"] != REPOSITORY_ID
                        or run["head_repository"]["id"] != REPOSITORY_ID
                        or run["head_branch"] != pr["head"]["ref"]
                        or not run["display_title"].startswith(
                            f"gate=full / head={head} / base={base} / "
                        )
                        or not run["display_title"].endswith(f" / pull={number}")
                    ):
                        continue
                    stage = "squash_check_suite"
                    [suite] = evidence(f"check-suites/{run['check_suite_id']}")
                    if suite["app"]["id"] != 15368 or suite["head_sha"] != head:
                        continue
                    stage = "squash_jobs"
                    jobs = rows(
                        evidence(f"actions/runs/{run['id']}/jobs?filter=latest&per_page=100"),
                        "jobs",
                    )
                    verified = set()
                    for index in range(policy.shard_count):
                        owning = [
                            job
                            for job in jobs
                            if job["name"] == f"nebius-pool ({index + 1}-of-{policy.shard_count})"
                        ]
                        if len(owning) != 1:
                            continue
                        job = owning[0]
                        if job["status"] != "completed" or job["conclusion"] != "success":
                            continue
                        try:
                            stage = "job_log"
                            log = job_log(job["id"])
                            stage = "checkout_binding"
                            checkout, changed = checkout_inputs(job, log, number)
                            stage = "tested_commit"
                            [tested] = evidence(f"git/commits/{checkout}")
                            if (
                                tested["sha"] != checkout
                                or tested["tree"]["sha"] != tree
                                or [p["sha"] for p in tested["parents"]] != [base, head]
                            ):
                                continue
                            stage = "test_selection"
                            verified.update(
                                test_paths(
                                    changed_paths=changed,
                                    shard_index=index,
                                    shard_count=policy.shard_count,
                                )
                            )
                        except (
                            OSError,
                            ValueError,
                            LookupError,
                            TypeError,
                            subprocess.SubprocessError,
                        ) as error:
                            unavailable_evidence(stage, error, run_id=run.get("id"), job_id=job.get("id"))
                    if verified:
                        covered.update(verified)
                        reused_runs.append(run["id"])
                except (OSError, ValueError, LookupError, TypeError, subprocess.SubprocessError) as error:
                    unavailable_evidence(stage, error, run_id=run.get("id"))
        except (OSError, ValueError, LookupError, TypeError, subprocess.SubprocessError) as error:
            unavailable_evidence(stage, error)
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
        evidence_errors=errors,
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
