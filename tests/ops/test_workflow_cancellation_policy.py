from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]

EXPLICIT_NON_CANCELLABLE_WORKFLOWS = {
    ".github/workflows/ci-retry.yml": "classified-ci-retry-${{ inputs.source_run_id }}",
    ".github/workflows/main-promotion-gate.yml": (
        "main-promotion-gate-${{ inputs.candidate_sha }}"
    ),
    ".github/workflows/release-promotion-gate.yml": (
        "release-promotion-gate-${{ inputs.candidate_sha }}"
    ),
}


def _workflow(path: str) -> dict[str, Any]:
    return yaml.safe_load((REPO_ROOT / path).read_text(encoding="utf-8"))


@pytest.mark.parametrize(
    ("workflow_path", "expected_group"),
    EXPLICIT_NON_CANCELLABLE_WORKFLOWS.items(),
)
def test_mutating_workflows_are_serialized_without_cancellation(
    workflow_path: str,
    expected_group: str,
) -> None:
    concurrency = _workflow(workflow_path)["concurrency"]

    assert concurrency == {
        "group": expected_group,
        "cancel-in-progress": False,
    }


def test_manual_image_validation_is_outside_pr_cancellation_scope() -> None:
    workflow = _workflow(".github/workflows/images.yml")
    cancellation = " ".join(
        str(workflow["concurrency"]["cancel-in-progress"]).split()
    )

    assert "github.event_name == 'pull_request'" in cancellation
    assert "workflow_dispatch" not in cancellation
    assert "push" not in cancellation


def test_cancellable_macos_workflow_has_no_write_authority() -> None:
    workflow = _workflow(".github/workflows/macos-locked-environment.yml")

    assert workflow["concurrency"]["cancel-in-progress"] is True
    assert workflow["permissions"] == {"contents": "read"}
