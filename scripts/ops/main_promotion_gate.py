#!/usr/bin/env python3
"""Verify that a main promotion is the current release-gated dev candidate."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Protocol

SHA_RE = re.compile(r"[0-9a-f]{40}")
REPOSITORY_RE = re.compile(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+")
RELEASE_WORKFLOW_PATH = ".github/workflows/release-promotion-gate.yml"
RELEASE_WORKFLOW_ID = "release-promotion-gate.yml"
RELEASE_ARTIFACT_NAME = "release-gate-evidence"


class PromotionGateError(RuntimeError):
    """Raised when promotion evidence is absent, stale, or mismatched."""


class GitHubAPI(Protocol):
    def get(self, path: str) -> Mapping[str, Any]: ...


class GhCLI:
    def get(self, path: str) -> Mapping[str, Any]:
        result = subprocess.run(
            ["gh", "api", path],
            text=True,
            capture_output=True,
            check=False,
        )
        if result.returncode != 0:
            detail = result.stderr.strip() or "unknown gh api failure"
            raise PromotionGateError(f"GitHub API request failed for {path}: {detail}")
        try:
            payload = json.loads(result.stdout)
        except json.JSONDecodeError as exc:
            raise PromotionGateError(f"GitHub API returned invalid JSON for {path}") from exc
        if not isinstance(payload, Mapping):
            raise PromotionGateError(f"GitHub API returned a non-object for {path}")
        return payload


def _nested(payload: Mapping[str, Any], *keys: str) -> Any:
    current: Any = payload
    for key in keys:
        if not isinstance(current, Mapping) or key not in current:
            return None
        current = current[key]
    return current


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise PromotionGateError(message)


def verify_main_promotion(
    *,
    api: GitHubAPI,
    repository: str,
    candidate_sha: str,
    dispatch_sha: str,
    pr_number: int,
    release_gate_run_id: int,
) -> dict[str, Any]:
    _require(REPOSITORY_RE.fullmatch(repository) is not None, "repository is invalid")
    _require(SHA_RE.fullmatch(candidate_sha) is not None, "candidate_sha is invalid")
    _require(dispatch_sha == candidate_sha, "workflow dispatch SHA must match candidate_sha")
    _require(pr_number > 0, "pr_number must be positive")
    _require(release_gate_run_id > 0, "release_gate_run_id must be positive")

    prefix = f"repos/{repository}"
    pull = api.get(f"{prefix}/pulls/{pr_number}")
    _require(pull.get("state") == "open", "promotion pull request must be open")
    _require(pull.get("draft") is False, "promotion pull request must be ready")
    _require(_nested(pull, "base", "repo", "full_name") == repository, "PR base repository mismatch")
    _require(_nested(pull, "base", "ref") == "main", "PR base branch must be main")
    _require(_nested(pull, "head", "repo", "full_name") == repository, "PR must be same-repository")
    _require(_nested(pull, "head", "ref") == "dev", "PR source branch must be dev")
    _require(_nested(pull, "head", "sha") == candidate_sha, "PR head must match candidate_sha")

    dev = api.get(f"{prefix}/branches/dev")
    _require(_nested(dev, "commit", "sha") == candidate_sha, "candidate_sha must be current dev head")

    workflow = api.get(f"{prefix}/actions/workflows/{RELEASE_WORKFLOW_ID}")
    _require(workflow.get("path") == RELEASE_WORKFLOW_PATH, "release workflow path mismatch")
    workflow_id = workflow.get("id")
    _require(type(workflow_id) is int and workflow_id > 0, "release workflow id is invalid")

    run = api.get(f"{prefix}/actions/runs/{release_gate_run_id}")
    _require(run.get("id") == release_gate_run_id, "release gate run id mismatch")
    _require(run.get("workflow_id") == workflow_id, "run is not release-promotion-gate")
    _require(run.get("event") == "workflow_dispatch", "release gate must be workflow_dispatch")
    _require(run.get("status") == "completed", "release gate run is not complete")
    _require(run.get("conclusion") == "success", "release gate run did not succeed")
    _require(run.get("head_branch") == "dev", "release gate must be dispatched from dev")
    _require(run.get("head_sha") == candidate_sha, "release gate SHA must match candidate_sha")
    _require(_nested(run, "repository", "full_name") == repository, "release gate repository mismatch")

    artifacts = api.get(f"{prefix}/actions/runs/{release_gate_run_id}/artifacts?per_page=100")
    artifact_items = artifacts.get("artifacts")
    _require(isinstance(artifact_items, list), "release gate artifacts response is invalid")
    matching_artifacts = [
        artifact
        for artifact in artifact_items
        if isinstance(artifact, Mapping)
        and artifact.get("name") == RELEASE_ARTIFACT_NAME
        and artifact.get("expired") is False
    ]
    _require(len(matching_artifacts) == 1, "one unexpired release-gate-evidence artifact is required")

    return {
        "candidate_sha": candidate_sha,
        "dev_head": candidate_sha,
        "pr_number": pr_number,
        "release_gate_run_id": release_gate_run_id,
        "release_gate_artifact_id": matching_artifacts[0].get("id"),
        "repository": repository,
        "source": "dev",
        "target": "main",
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", required=True)
    parser.add_argument("--candidate-sha", required=True)
    parser.add_argument("--dispatch-sha", required=True)
    parser.add_argument("--pr-number", required=True, type=int)
    parser.add_argument("--release-gate-run-id", required=True, type=int)
    parser.add_argument('--output-json', type=Path, help='Retain exact successful gate selection for production qualification')
    return parser


def main() -> int:
    args = _parser().parse_args()
    try:
        result = verify_main_promotion(
            api=GhCLI(),
            repository=args.repository,
            candidate_sha=args.candidate_sha,
            dispatch_sha=args.dispatch_sha,
            pr_number=args.pr_number,
            release_gate_run_id=args.release_gate_run_id,
        )
        if args.output_json is not None:
            try:
                identity = {name: int(os.environ[key]) for name, key in
                    (('run_id', 'GITHUB_RUN_ID'), ('run_attempt', 'GITHUB_RUN_ATTEMPT'))}
            except (KeyError, ValueError):
                raise PromotionGateError('main gate run identity unavailable') from None
            if any(value <= 0 for value in identity.values()):
                raise PromotionGateError('main gate run identity unavailable')
            args.output_json.write_text(json.dumps(dict(result, schema_version='loom.main-promotion-evidence.v2',
                gate_run=identity), sort_keys=True) + '\n', encoding='utf-8')
    except (PromotionGateError, OSError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
