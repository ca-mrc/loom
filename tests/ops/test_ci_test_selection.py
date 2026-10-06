"""Only independent test edits may narrow an otherwise complete test lane."""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
import yaml
from scripts.component_ownership import narrow_test_only_changes
from scripts.plan_ci_validations import plan_validations


def _repository(tmp_path: Path) -> tuple[str, ...]:
    files = {
        "tests/unit/test_alpha.py": "def test_alpha(): assert True\n",
        "tests/unit/test_beta.py": "def test_beta(): assert True\n",
        "src/loom/service.py": "VALUE = 1\n",
    }
    for name, text in files.items():
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
    return tuple(files)


def test_unreferenced_test_edit_selects_only_that_test(tmp_path: Path) -> None:
    tracked = _repository(tmp_path)
    selected = narrow_test_only_changes(
        (tracked[0], tracked[1]), changed_paths=(tracked[0],),
        tracked_paths=tracked, repo_root=tmp_path,
    )
    assert selected == (tracked[0],)


@pytest.mark.parametrize("change", ["src/loom/service.py", "tests/conftest.py", "tests/support/helper.py",
                                   "database/migrations/new.py", "unknown/file.bin", "tests/unit/deleted.py", "docs/usage.md"])
def test_shared_runtime_unknown_or_deleted_change_keeps_full_lane(tmp_path: Path, change: str) -> None:
    tracked = _repository(tmp_path)
    paths = (tracked[0], tracked[1])
    assert narrow_test_only_changes(paths, changed_paths=(tracked[0], change),
                                    tracked_paths=tracked, repo_root=tmp_path) == paths


def test_imported_test_fixture_keeps_full_lane(tmp_path: Path) -> None:
    tracked = _repository(tmp_path)
    (tmp_path / tracked[1]).write_text("from tests.unit.test_alpha import fixture\n")
    paths = (tracked[0], tracked[1])
    assert narrow_test_only_changes(paths, changed_paths=(tracked[0],),
                                    tracked_paths=tracked, repo_root=tmp_path) == paths


def test_no_diff_is_explicit_full_regression(tmp_path: Path) -> None:
    tracked = _repository(tmp_path)
    paths = (tracked[0], tracked[1])
    assert narrow_test_only_changes(paths, changed_paths=(),
                                    tracked_paths=tracked, repo_root=tmp_path) == paths


@pytest.mark.parametrize("event,labels", [("workflow_dispatch", set()),
                                         ("pull_request", {"ci:integration"}),
                                         ("pull_request", {"ci:coverage-summary"})])
def test_explicit_full_requests_do_not_export_a_reduced_test_diff(event: str, labels: set[str]) -> None:
    plan = plan_validations(changed_paths=["tests/unit/test_alpha.py"],
                            labels=labels, event_name=event)
    assert json.loads(plan.github_outputs()["test_changes"]) == []


@pytest.mark.parametrize("lane", ["tests-root", "tests-packages", "integration", "integration-docker"])
@pytest.mark.parametrize("selection_exit", [0, 17])
def test_empty_selection_is_safe_and_selector_failure_still_fails_the_job(
    tmp_path: Path, lane: str, selection_exit: int,
) -> None:
    root = Path(__file__).resolve().parents[2]
    workflow = yaml.safe_load((root / ".github/workflows/ci.yml").read_text())
    step = next(step for step in workflow["jobs"][lane]["steps"]
                if step.get("name", "").startswith("Pytest"))
    uv = tmp_path / "uv"
    uv.write_text('#!/usr/bin/env python3\nimport os,sys\n'
                  'assert "test-paths" in sys.argv, "empty selection reached pytest"\n'
                  'sys.exit(int(os.environ["SELECT_EXIT"]))\n')
    uv.chmod(0o755)
    result = subprocess.run(
        ["bash", "-c", step["run"]], text=True, capture_output=True,
        env={**os.environ, "PATH": f"{tmp_path}:{os.environ['PATH']}",
             "RUNNER_TEMP": str(tmp_path), "SHARD_INDEX": "0", "SHARD_COUNT": "2",
             "COVERAGE_ENABLED": "false", "TEST_CHANGED_PATHS": "[]",
             "SELECT_EXIT": str(selection_exit)},
    )
    assert result.returncode == selection_exit, result.stderr


@pytest.mark.parametrize("scope", ["all", "nebius"])
def test_root_workflow_runs_eight_complete_disjoint_fail_fast_shards(tmp_path: Path, scope: str) -> None:
    """Exercise the workflow/selector boundary, not a second shard algorithm."""
    root = Path(__file__).resolve().parents[2]
    workflow = yaml.safe_load((root / ".github/workflows/ci.yml").read_text())
    job = workflow["jobs"]["tests-root"]
    matrix = job["strategy"]["matrix"]["include"]
    step = next(row for row in job["steps"] if row.get("name", "").startswith("Pytest"))
    count = int(step["env"]["SHARD_COUNT"])
    assert count == 8
    assert sorted(row["shard_index"] for row in matrix) == list(range(count))
    complete = subprocess.run(
        [sys.executable, "scripts/component_ownership.py", "test-paths", "--lane", "tests-root",
         "--test-scope", scope], cwd=root, text=True, capture_output=True, check=True,
    ).stdout.splitlines()
    uv = tmp_path / "uv"
    uv.write_text(
        f"#!{sys.executable}\n"
        "import json, os, subprocess, sys\nfrom pathlib import Path\n"
        "if 'test-paths' in sys.argv:\n"
        "    sys.exit(subprocess.run([sys.executable, *sys.argv[sys.argv.index('python') + 1:]]).returncode)\n"
        "assert 'pytest' in sys.argv\n"
        "Path(os.environ['SELECTED_TEST_ARGS']).write_text(json.dumps(sys.argv))\n"
        "sys.exit(int(os.environ['PYTEST_EXIT']))\n"
    )
    uv.chmod(0o755)
    shards = []
    for row in matrix:
        recorded = tmp_path / f"args-{row['shard_index']}.json"
        # A failed test remains a failed shard, including the first shard.
        pytest_exit = 17 if row["shard_index"] == 0 else 0
        result = subprocess.run(
            ["bash", "-c", step["run"]], cwd=root, text=True, capture_output=True,
            env={**os.environ, "PATH": f"{tmp_path}:{os.environ['PATH']}",
                 "RUNNER_TEMP": str(tmp_path), "SHARD_INDEX": str(row["shard_index"]),
                 "SHARD_COUNT": str(count), "COVERAGE_ENABLED": "false", "TEST_CHANGED_PATHS": "[]",
                 "CI_TEST_SCOPE": scope, "SELECTED_TEST_ARGS": str(recorded), "PYTEST_EXIT": str(pytest_exit)},
        )
        assert result.returncode == pytest_exit, result.stderr
        arguments = json.loads(recorded.read_text())
        assert "-x" in arguments or "--maxfail=1" in arguments
        paths = {arg for arg in arguments if arg.startswith("tests/")}
        assert paths
        assert all(paths.isdisjoint(previous) for previous in shards)
        shards.append(paths)
    assert set().union(*shards) == set(complete)
    gateway = next(shard for shard in shards if "tests/ops/test_nebius_pool_gateway_retirement_live.py" in shard)
    assert gateway.isdisjoint({"tests/ops/test_nebius_pool_role_restoration_live.py",
                               "tests/ops/test_nebius_pool_template_restoration_live.py"})
    predecessor_groups = [next(index for index, shard in enumerate(shards) if path in shard) for path in (
        "tests/ops/test_nebius_pool_predecessor.py", "tests/ops/test_nebius_pool_predecessor_live.py",
        "tests/ops/test_nebius_pool_refresh_connected.py")]
    assert len(set(predecessor_groups)) == 3
    cancellation_groups = [next(index for index, shard in enumerate(shards) if path in shard) for path in (
        "tests/ops/test_nebius_pool_image_cancellation.py",
        "tests/ops/test_nebius_pool_image_cancellation_collector.py",
        "tests/ops/test_nebius_pool_image_cancellation_gateway.py")]
    assert len(set(cancellation_groups)) == 3
