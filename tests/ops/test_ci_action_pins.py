from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
import yaml
from scripts.check_ci_action_pins import check_action_pins
from scripts.install_trivy import TRIVY_ARCHIVE_SHA256, TRIVY_RELEASE_URL

REPO_ROOT = Path(__file__).resolve().parents[2]

SHA_A = "a" * 40
SHA_B = "b" * 40
DOCKER_DIGEST = "c" * 64


def _write_lock(
    tmp_path: Path,
    actions: dict[str, dict[str, str]],
    *,
    schema_version: Any = 1,
) -> Path:
    lock_file = tmp_path / "actions-lock.json"
    lock_file.write_text(
        json.dumps(
            {
                "schema_version": schema_version,
                "actions": actions,
            },
        ),
        encoding="utf-8",
    )
    return lock_file


def _locked_action(sha: str = SHA_A) -> dict[str, str]:
    return {"sha": sha, "version": "v1"}


def _write_workflow(tmp_path: Path, uses: list[Any]) -> Path:
    workflows_dir = tmp_path / "workflows"
    workflows_dir.mkdir(exist_ok=True)
    steps = [{"name": f"action-{index}", "uses": value} for index, value in enumerate(uses)]
    workflow = {
        "name": "test",
        "on": "workflow_dispatch",
        "jobs": {
            "check": {
                "runs-on": "ubuntu-latest",
                "steps": steps,
            },
        },
    }
    (workflows_dir / "test.yml").write_text(
        yaml.safe_dump(workflow, sort_keys=False),
        encoding="utf-8",
    )
    return workflows_dir


def _errors(
    tmp_path: Path,
    *,
    uses: list[Any],
    actions: dict[str, dict[str, str]],
    schema_version: Any = 1,
) -> tuple[str, ...]:
    return check_action_pins(
        workflows_dir=_write_workflow(tmp_path, uses),
        lock_file=_write_lock(tmp_path, actions, schema_version=schema_version),
    ).errors


def test_repository_workflows_match_the_verified_action_lock() -> None:
    result = check_action_pins(
        workflows_dir=REPO_ROOT / ".github" / "workflows",
        lock_file=REPO_ROOT / "config" / "ci-actions-lock.json",
    )

    assert result.errors == ()
    assert result.workflow_count > 0
    assert result.reference_count > 0


def test_repository_workflows_use_only_actions_allowed_by_github_policy() -> None:
    result = check_action_pins(
        workflows_dir=REPO_ROOT / ".github" / "workflows",
        lock_file=REPO_ROOT / "config" / "ci-actions-lock.json",
    )

    forbidden = {
        action
        for action in result.remote_actions
        if not action.startswith(("actions/", "qianyi-sun/")) and action != "astral-sh/setup-uv"
    }

    assert forbidden == set()


def test_image_scans_use_repository_owned_trivy_installer() -> None:
    lock = json.loads(
        (REPO_ROOT / "config/ci-actions-lock.json").read_text(encoding="utf-8"),
    )
    workflow = yaml.safe_load(
        (REPO_ROOT / ".github/workflows/images.yml").read_text(encoding="utf-8"),
    )
    remote_trivy_uses = [
        step["uses"]
        for job in workflow["jobs"].values()
        for step in job.get("steps", [])
        if str(step.get("uses", "")).startswith("aquasecurity/trivy-action@")
    ]
    scan_steps = [
        (job_name, step["run"])
        for job_name, job in workflow["jobs"].items()
        for step in job.get("steps", [])
        if step.get("name") in {"Scan native image archive", "Scan trusted image archive"}
    ]
    installer_scripts = [
        step["run"]
        for job in workflow["jobs"].values()
        for step in job.get("steps", [])
        if step.get("name") == "Install and record pinned Trivy binary"
    ]

    assert "aquasecurity/trivy-action" not in lock["actions"]
    assert remote_trivy_uses == []
    assert len(scan_steps) == 2
    assert {job_name for job_name, _ in scan_steps} == {"build", "nebius-harness-build"}
    scan_scripts = [script for _, script in scan_steps]
    assert len(installer_scripts) == 1
    assert "python3 scripts/install_trivy.py" in installer_scripts[0]
    assert all("python3 scripts/install_trivy.py" not in script for script in scan_scripts)
    assert all(
        "/tmp/loom-trivy-binaries/${ARCHITECTURE}/trivy" in script for script in scan_scripts
    )
    assert TRIVY_RELEASE_URL.endswith("/v0.74.0")
    assert TRIVY_ARCHIVE_SHA256 == {
        "amd64": "2ae6fe3ee734b7fdf11335663e18c75ea12dccc76062f09f164a3b0f8be4371a",
        "arm64": "b94ce1976bbf3c15b514b605ee88be7c6d94a29be2302847ff01cb794d47aad5",
    }


def test_local_action_and_digest_pinned_docker_action_are_allowed(tmp_path: Path) -> None:
    assert (
        _errors(
            tmp_path,
            uses=[
                "./.github/actions/local",
                f"docker://ghcr.io/example/action@sha256:{DOCKER_DIGEST}",
            ],
            actions={},
        )
        == ()
    )


def test_remote_action_full_sha_matching_lock_is_allowed(tmp_path: Path) -> None:
    assert (
        _errors(
            tmp_path,
            uses=[f"owner/action@{SHA_A}"],
            actions={"owner/action": _locked_action()},
        )
        == ()
    )


def test_reusable_workflow_job_level_uses_is_discovered(tmp_path: Path) -> None:
    workflows_dir = tmp_path / "workflows"
    workflows_dir.mkdir()
    (workflows_dir / "reusable.yml").write_text(
        """name: reusable
on: workflow_dispatch
jobs:
  call:
    uses: owner/reusable/.github/workflows/check.yml@aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
""",
        encoding="utf-8",
    )
    result = check_action_pins(
        workflows_dir=workflows_dir,
        lock_file=_write_lock(tmp_path, {"owner/reusable": _locked_action()}),
    )

    assert result.errors == ()
    assert result.reference_count == 1
    assert result.remote_actions == ("owner/reusable",)


@pytest.mark.parametrize("schema_version", [True, 1.0, "1", None])
def test_schema_version_must_be_integer_one(
    tmp_path: Path,
    schema_version: Any,
) -> None:
    errors = _errors(
        tmp_path,
        uses=[f"owner/action@{SHA_A}"],
        actions={"owner/action": _locked_action()},
        schema_version=schema_version,
    )

    assert any("schema_version must be integer 1" in error for error in errors)


@pytest.mark.parametrize(
    "uses",
    [
        "owner/action@v1",
        "owner/action@main",
        "owner/action@${{ github.sha }}",
        f"owner/action@{'A' * 40}",
    ],
)
def test_tag_expression_and_uppercase_sha_references_fail_closed(
    tmp_path: Path,
    uses: str,
) -> None:
    errors = _errors(
        tmp_path,
        uses=[uses],
        actions={"owner/action": _locked_action()},
    )

    assert errors
    assert any(
        "full commit SHA" in error or "expressions are forbidden" in error for error in errors
    )


def test_remote_action_sha_must_exactly_match_lock(tmp_path: Path) -> None:
    errors = _errors(
        tmp_path,
        uses=[f"owner/action@{SHA_B}"],
        actions={"owner/action": _locked_action(SHA_A)},
    )

    assert any("does not match locked SHA" in error for error in errors)


def test_new_action_without_lock_entry_fails(tmp_path: Path) -> None:
    errors = _errors(
        tmp_path,
        uses=[f"owner/new-action@{SHA_A}"],
        actions={},
    )

    assert any("is not declared" in error for error in errors)


def test_stale_lock_entry_fails(tmp_path: Path) -> None:
    errors = _errors(
        tmp_path,
        uses=[f"owner/action@{SHA_A}"],
        actions={
            "owner/action": _locked_action(),
            "owner/stale": _locked_action(),
        },
    )

    assert any("stale action lock entry" in error and "owner/stale" in error for error in errors)


def test_same_action_cannot_use_multiple_shas(tmp_path: Path) -> None:
    errors = _errors(
        tmp_path,
        uses=[f"owner/action@{SHA_A}", f"owner/action/subpath@{SHA_B}"],
        actions={"owner/action": _locked_action(SHA_A)},
    )

    assert any("multiple commit SHAs" in error for error in errors)


def test_docker_action_without_sha256_digest_fails(tmp_path: Path) -> None:
    errors = _errors(
        tmp_path,
        uses=["docker://alpine:3.22"],
        actions={},
    )

    assert any("docker action must use" in error for error in errors)


def test_non_string_uses_fails(tmp_path: Path) -> None:
    errors = _errors(tmp_path, uses=[123], actions={})

    assert any("uses must be a string" in error for error in errors)


def test_duplicate_yaml_keys_fail_closed(tmp_path: Path) -> None:
    workflows_dir = tmp_path / "workflows"
    workflows_dir.mkdir()
    (workflows_dir / "duplicate.yml").write_text(
        """name: duplicate
on: workflow_dispatch
jobs:
  check:
    runs-on: ubuntu-latest
    runs-on: macos-latest
    steps: []
""",
        encoding="utf-8",
    )
    result = check_action_pins(
        workflows_dir=workflows_dir,
        lock_file=_write_lock(tmp_path, {}),
    )

    assert any("duplicate key 'runs-on'" in error for error in result.errors)


def test_duplicate_lock_keys_fail_closed(tmp_path: Path) -> None:
    workflows_dir = _write_workflow(tmp_path, [f"owner/action@{SHA_A}"])
    lock_file = tmp_path / "duplicate-lock.json"
    lock_file.write_text(
        '{"schema_version":1,"actions":{"owner/action":'
        f'{{"sha":"{SHA_A}","version":"v1"}},"owner/action":'
        f'{{"sha":"{SHA_A}","version":"v1"}}}}}}',
        encoding="utf-8",
    )

    result = check_action_pins(workflows_dir=workflows_dir, lock_file=lock_file)

    assert any("duplicate JSON key 'owner/action'" in error for error in result.errors)
