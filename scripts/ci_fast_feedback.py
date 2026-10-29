"""Enforce selected feedback and specialist results in the native CI dependency graph."""

from __future__ import annotations

import argparse
import json
import os
import re
import urllib.request
from collections.abc import Callable
from typing import Any
from urllib.parse import quote

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


SPECIALISTS = {
    "images": "images-validation",
    "cluster_smoke": "cluster-validation",
    "staging_smoke": "staging-validation",
}


def read_json(endpoint: str) -> Any:
    """Read native GitHub metadata without exposing CLI output on failures."""
    request = urllib.request.Request(
        f"{os.environ.get('GITHUB_API_URL', 'https://api.github.com').rstrip('/')}/{endpoint}",
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {os.environ.get('GH_TOKEN', '')}",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            return json.loads(response.read())
    except (OSError, ValueError):
        raise ValueError("base freshness API request failed") from None


def check_current_base(
    repository: str, base_ref: str, expected_sha: str, read: Callable[[str], Any] = read_json,
) -> None:
    """Avoid PR fan-out already obsolete under strict, non-queue admission."""
    if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository) or any(
        part in {".", ".."} for part in repository.split("/")
    ):
        raise ValueError("invalid repository for base freshness check")
    if not base_ref or any(ord(character) < 32 for character in base_ref):
        raise ValueError("invalid base ref for freshness check")
    if not re.fullmatch(r"[0-9a-f]{40}", expected_sha):
        raise ValueError("invalid expected base SHA")
    branch = quote(base_ref, safe="")
    rules = read(f"repos/{repository}/rules/branches/{branch}")
    if not isinstance(rules, list) or any(
        not isinstance(rule, dict) or not isinstance(rule.get("type"), str) or not rule["type"]
        for rule in rules
    ):
        raise ValueError("invalid branch rules for base freshness check")
    if any(rule["type"] == "merge_queue" for rule in rules):
        return  # GitHub regenerates queued merge groups when their base advances.
    strict = False
    for rule in rules:
        if rule["type"] == "required_status_checks":
            parameters = rule.get("parameters")
            value = parameters.get("strict_required_status_checks_policy") if isinstance(parameters, dict) else None
            if type(value) is not bool:
                raise ValueError("invalid required status check rules")
            strict |= value
    if not strict:
        return
    current = read(f"repos/{repository}/git/ref/heads/{branch}")
    current_sha = current.get("object", {}).get("sha") if isinstance(current, dict) and isinstance(current.get("object"), dict) else None
    if not isinstance(current_sha, str) or not re.fullmatch(r"[0-9a-f]{40}", current_sha):
        raise ValueError("invalid current base SHA")
    if current_sha != expected_sha:
        raise ValueError("strict base has advanced; update this PR before starting validation")


def enforce_specialist(needs: dict[str, Any], selector: str) -> None:
    plan = needs["workflow-plan"]
    if plan["result"] != "success" or plan["outputs"]["gate_mode"] != "full":
        raise ValueError("specialist requires a successful full planner")
    selected = plan["outputs"].get(selector)
    result = needs[SPECIALISTS[selector]]["result"]
    if (selected, result) not in {("true", "success"), ("false", "skipped")}:
        raise ValueError(f"specialist rejected {selector}: selected={selected!r}, result={result!r}")
    if selected == "true" and needs["fast-feedback"]["result"] != "success":
        raise ValueError("selected specialist requires successful fast feedback")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("enforce")
    commands.add_parser("check-base")
    specialist = commands.add_parser("enforce-specialist")
    specialist.add_argument("--selector", choices=SPECIALISTS, required=True)
    args = parser.parse_args()
    try:
        if args.command == "check-base":
            check_current_base(os.environ["GITHUB_REPOSITORY"], os.environ["PR_BASE_REF"], os.environ["PR_BASE_SHA"])
        else:
            value = json.loads(os.environ["FEEDBACK_NEEDS"])
            if args.command == "enforce":
                enforce(value)
            else:
                enforce_specialist(value, args.selector)
    except (ValueError, KeyError, TypeError) as error:
        parser.exit(1, f"FAIL: {error}\n")


if __name__ == "__main__":
    main()
