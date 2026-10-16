"""Find optional exact-candidate OCI artifacts from the native dev image workflow.

This is a reuse hint, not admission or image validation. Native Actions performs
the download; the candidate still inspects every OCI image and runs all scans.
Unknown, expired or missing evidence means rebuild at the same candidate SHA.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import urllib.request
from collections.abc import Callable
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from scripts.component_ownership import NEBIUS_PLATFORM_IMAGES as COMPONENTS

from loom.repository_identity import REPOSITORY_ID

REPOSITORY = "ca-mrc/loom"
WORKFLOW = ".github/workflows/images.yml"
MAX_RUNS = 5
LOOKUP_SECONDS = 20
Read = Callable[[str], dict[str, Any]]
METADATA_ERRORS = (OSError, ValueError, KeyError, TypeError)


def github_read(path: str, *, timeout: float = 10) -> dict[str, Any]:
    """Read one bounded metadata page, without downloading artifact payloads."""
    token = os.environ.get("GH_TOKEN")
    if not token:
        raise ValueError("actions_metadata_token_unavailable")
    request = urllib.request.Request(
        f"https://api.github.com/repos/{REPOSITORY}/{path}",
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "loom-candidate-image-artifacts",
        },
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        value = json.load(response)
    if not isinstance(value, dict):
        raise ValueError("actions_metadata_invalid")
    return value


def plan(candidate: str, *, read: Read | None = None) -> dict[str, Any]:
    if not isinstance(candidate, str) or not re.fullmatch(r"[0-9a-f]{40}", candidate):
        raise ValueError("candidate_sha_invalid")
    value: dict[str, Any] = dict(
        candidate_sha=candidate,
        run_id="",
        has_artifacts=False,
        components=[],
        artifact_names=[],
        reason="no_qualified_producer",
        evidence_unavailable=False,
    )
    deadline = time.monotonic() + LOOKUP_SECONDS

    def read_metadata(path: str) -> dict[str, Any]:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("actions_metadata_lookup_deadline")
        # No retries or pagination loops. Metadata trouble must not consume the
        # build-time saving; the existing exact-SHA rebuild remains available.
        if read is None:
            return github_read(path, timeout=min(10, remaining))
        return read(path)

    try:
        runs = read_metadata(
            "actions/workflows/images.yml/runs?event=merge_group&status=success"
            f"&head_sha={candidate}&per_page={MAX_RUNS}"
        )["workflow_runs"]
        if not isinstance(runs, list):
            raise ValueError("actions_metadata_invalid")
    except METADATA_ERRORS:
        return value | dict(reason="actions_metadata_unavailable", evidence_unavailable=True)

    for run in runs[:MAX_RUNS]:
        try:
            if (
                run["head_sha"] != candidate
                or run["event"] != "merge_group"
                or run["status"] != "completed"
                or run["conclusion"] != "success"
                or run["path"] != WORKFLOW
                or run["repository"]["id"] != REPOSITORY_ID
                or run["head_repository"]["id"] != REPOSITORY_ID
                or not isinstance(run["head_branch"], str)
                or not run["head_branch"].startswith("gh-readonly-queue/dev/")
                or type(run["id"]) is not int
                or run["id"] <= 0
                or type(run["check_suite_id"]) is not int
            ):
                continue
            suite = read_metadata(f"check-suites/{run['check_suite_id']}")
            if (
                suite["app"]["id"] != 15368
                or suite["head_sha"] != candidate
                or suite["status"] != "completed"
                or suite["conclusion"] != "success"
                or suite["repository"]["id"] != REPOSITORY_ID
            ):
                continue
            artifacts = read_metadata(f"actions/runs/{run['id']}/artifacts?per_page=100")[
                "artifacts"
            ]
            if not isinstance(artifacts, list):
                raise ValueError("actions_metadata_invalid")
            # One run only. A missing component is rebuilt rather than stitched
            # to an older run, a PR head, or a tree-equivalent different SHA.
            available = {
                artifact["name"]
                for artifact in artifacts
                if artifact["expired"] is False and artifact["size_in_bytes"] > 0
            }
            components = [name for name in COMPONENTS if name + ".tar" in available]
            if components:
                return value | dict(
                    run_id=str(run["id"]),
                    has_artifacts=True,
                    components=components,
                    artifact_names=[name + ".tar" for name in components],
                    reason="qualified_producer",
                )
            value["reason"] = "no_matching_artifacts"
        except METADATA_ERRORS:
            value["evidence_unavailable"] = True
    if value["evidence_unavailable"]:
        value["reason"] = "actions_metadata_unavailable"
    return value


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate", default=os.environ.get("GITHUB_SHA"))
    parser.add_argument("--github-output", type=Path)
    args = parser.parse_args()
    value = plan(args.candidate)
    if args.github_output:
        with args.github_output.open("a", encoding="utf-8") as stream:
            for key in ("run_id", "has_artifacts", "components", "artifact_names", "reason"):
                output = value[key]
                if not isinstance(output, str):
                    output = json.dumps(output, separators=(",", ":"))
                stream.write(f"{key}={output}\n")
    print(json.dumps(value, sort_keys=True))


if __name__ == "__main__":
    main()
