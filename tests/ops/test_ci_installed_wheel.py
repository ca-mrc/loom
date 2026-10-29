"""Packaging drift must fail early with the affected resource identified."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from scripts.plan_ci_validations import plan_validations
from scripts.verify_installed_wheel import verify_asset

ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize("installed", [b"schema v1", b"schema v2", None])
def test_installed_asset_requires_current_source_and_identifies_drift(tmp_path, installed):
    source = tmp_path / "config/loom-schema.toml"
    source.parent.mkdir()
    source.write_bytes(b"schema v1")
    packaged = tmp_path / "installed/loom-schema.toml"
    packaged.parent.mkdir()
    if installed is not None:
        packaged.write_bytes(installed)
    if installed == source.read_bytes():
        verify_asset(source, packaged)
    else:
        with pytest.raises(ValueError, match="wheel asset") as error:
            verify_asset(source, packaged)
        assert str(source) in str(error.value) and str(packaged) in str(error.value)


def test_runtime_wheel_is_verified_before_test_fanout_in_existing_lint_environment():
    jobs = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())["jobs"]
    lint = jobs["lint-and-static"]
    step = next(step for step in lint["steps"] if step.get("name") == "Build and exercise non-editable runtime wheel")
    assert "--test-scope" in step["run"] and "verify_installed_wheel.py" in step["run"]
    assert not step.get("continue-on-error")
    assert "lint-and-static" in jobs["tests-root"]["needs"]
    assert "lint-and-static" in jobs["integration"]["needs"]
    # A selected wheel smoke must never live in a job the planner skips.
    for paths in (("config/loom-schema.toml",), ("src/loom_cli/data/loom-schema.toml",),
                  ("src/loom/task_sandbox_planner.py",), (".github/workflows/ci.yml",)):
        plan = plan_validations(changed_paths=paths, labels=set(), event_name="pull_request")
        assert not plan.runtime_payload or plan.lint_and_static
