"""Exercise the planner and shell boundaries without starting disposable k3s."""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
import yaml
from scripts import component_ownership as ownership

ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / ".github/workflows/cluster-smoke.yml"


@pytest.fixture(scope="module")
def cluster_inputs():
    manifest = ownership.load_manifest(ROOT / "config/component-ownership.toml")
    tracked = ownership._tracked_paths(ROOT)
    paths = ownership.test_paths_for_lane(manifest, tracked_paths=tracked, lane="cluster-smoke")
    independent = next(path for path in paths if ownership.narrow_test_only_changes(
        paths, changed_paths=(path,), tracked_paths=tracked, repo_root=ROOT,
    ) == (path,))
    return manifest, tracked, paths, independent


def _jobs():
    return yaml.safe_load(WORKFLOW.read_text())["jobs"]


def _plan(tmp_path: Path, *, changes: tuple[str, ...], scope="nebius", event="pull_request", labels=()):
    changed = tmp_path / "changed.txt"
    changed.write_text("\n".join(changes))
    output = tmp_path / "plan-output.txt"
    subprocess.run([
        sys.executable, "scripts/plan_ci_validations.py", "--changed-files", str(changed),
        "--labels-json", json.dumps(labels), "--event-name", event,
        "--test-scope", scope, "--github-output", str(output),
    ], cwd=ROOT, check=True, capture_output=True, text=True)
    return dict(line.split("=", 1) for line in output.read_text().splitlines())


def _manifest_step(tmp_path: Path, row, *, changes=(), scope="nebius", extra_env=None):
    step = next(step for step in _jobs()["cluster-contract"]["steps"] if step.get("id") == "manifest")
    output = tmp_path / "manifest-output.txt"
    output.touch()
    result = subprocess.run(["bash"], input=step["run"], cwd=ROOT, text=True, capture_output=True,
        env={**os.environ, "RUNNER_TEMP": str(tmp_path), "GITHUB_OUTPUT": str(output),
             "CI_TEST_SCOPE": scope, "TEST_CHANGED_PATHS": json.dumps(changes),
             "SHARD_INDEX": str(row["shard_index"]), "SHARD_COUNT": str(row["shard_count"]),
             **(extra_env or {})}, check=False)
    outputs = dict(line.split("=", 1) for line in output.read_text().splitlines())
    return result, outputs


def _pytest_step(tmp_path: Path, manifest_file: str, *, pytest_exit=0):
    step = next(step for step in _jobs()["cluster-contract"]["steps"]
                if "manifest-owned k3s" in step.get("name", ""))
    recorded = tmp_path / "pytest-arguments.json"
    uv = tmp_path / "uv"
    uv.write_text(f"#!{Path(sys.executable).resolve()}\n"
                  "import json, os, sys\nfrom pathlib import Path\n"
                  "assert sys.argv[1:4] == ['run', '--no-sync', 'pytest']\n"
                  "Path(os.environ['RECORDED_ARGS']).write_text(json.dumps(sys.argv[4:]))\n"
                  "sys.exit(int(os.environ['PYTEST_EXIT']))\n")
    uv.chmod(0o700)
    script = step["run"].replace("${{ steps.manifest.outputs.paths_file }}", manifest_file)
    result = subprocess.run(["bash"], input=script, cwd=ROOT, text=True, capture_output=True,
        env={**os.environ, "PATH": str(tmp_path) + os.pathsep + os.environ["PATH"],
             "CI_PYTEST_MARKERS": "not legacy_pool", "RECORDED_ARGS": str(recorded),
             "PYTEST_EXIT": str(pytest_exit)}, check=False)
    return result, json.loads(recorded.read_text()) if recorded.exists() else []


@pytest.mark.parametrize("scope", ["all", "nebius"])
def test_full_cluster_matrix_executes_complete_disjoint_manifests(tmp_path, cluster_inputs, scope):
    manifest, tracked, paths, _ = cluster_inputs
    plan = _plan(tmp_path, changes=(), scope=scope, event="workflow_dispatch", labels=("cluster-smoke",))
    matrix = json.loads(plan["cluster_smoke_matrix"])
    policy = manifest.test_shard_policy("cluster-smoke")
    assert plan["cluster_smoke"] == "true"
    assert len(matrix) == policy.shard_count
    seen = set()
    for row in matrix:
        directory = tmp_path / row["shard"]
        directory.mkdir()
        result, outputs = _manifest_step(directory, row, scope=scope)
        assert result.returncode == 0, result.stderr
        test_result, arguments = _pytest_step(directory, outputs["paths_file"])
        assert test_result.returncode == 0, test_result.stderr
        selected = [argument for argument in arguments if argument.startswith("tests/")]
        assert selected and seen.isdisjoint(selected)
        seen.update(selected)
        assert arguments[arguments.index("-m") + 1] == "not legacy_pool"
    assert seen == set(ownership.select_test_scope(manifest, paths, scope=scope))
    assert seen == set(ownership.selected_test_paths(
        manifest, tracked_paths=tracked, lane="cluster-smoke", repo_root=ROOT, test_scope=scope,
    ))


def test_independent_cluster_test_edit_starts_only_its_owner_shard(tmp_path, cluster_inputs):
    _, _, _, independent = cluster_inputs
    plan = _plan(tmp_path, changes=(independent,))
    matrix = json.loads(plan["cluster_smoke_matrix"])
    assert plan["cluster_smoke"] == "true" and len(matrix) == 1
    result, outputs = _manifest_step(tmp_path, matrix[0], changes=(independent,))
    assert result.returncode == 0, result.stderr
    test_result, arguments = _pytest_step(tmp_path, outputs["paths_file"])
    assert test_result.returncode == 0, test_result.stderr
    assert [argument for argument in arguments if argument.startswith("tests/")] == [independent]


@pytest.mark.parametrize("event,labels,shared", [
    ("pull_request", ("cluster-smoke",), False),
    ("workflow_dispatch", ("cluster-smoke",), False),
    ("pull_request", (), True),
])
def test_explicit_or_shared_cluster_inputs_keep_full_matrix(tmp_path, cluster_inputs, event, labels, shared):
    manifest, _, _, independent = cluster_inputs
    changes = ("tests/conftest.py",) if shared else (independent,)
    plan = _plan(tmp_path, changes=changes, event=event, labels=labels)
    assert len(json.loads(plan["cluster_smoke_matrix"])) == manifest.test_shard_policy("cluster-smoke").shard_count
    if labels or event == "workflow_dispatch":
        assert json.loads(plan["test_changes"]) == []


def test_manifest_is_selected_before_dependency_or_registry_installation():
    jobs = _jobs()
    contract = jobs["cluster-contract"]
    steps = contract["steps"]
    selector = next(step for step in steps if step.get("id") == "manifest")
    selection_index = steps.index(selector)
    for index, step in enumerate(steps):
        if "setup-uv@" in step.get("uses", "") or "uv sync" in step.get("run", "") or "apt-get" in step.get("run", ""):
            assert selection_index < index
    assert "python3 scripts/component_ownership.py" in selector["run"]
    assert jobs["plan"]["outputs"]["test_changes"] == "${{ steps.plan.outputs.test_changes }}"
    assert jobs["plan"]["outputs"]["cluster_smoke_matrix"] == "${{ steps.plan.outputs.cluster_smoke_matrix }}"
    assert contract["strategy"]["matrix"]["include"] == "${{ fromJSON(needs.plan.outputs.cluster_smoke_matrix) }}"
    assert contract["env"]["TEST_CHANGED_PATHS"] == "${{ needs.plan.outputs.test_changes }}"
    assert selector["env"]["SHARD_COUNT"] == "${{ matrix.shard_count }}"
    assert contract["strategy"]["fail-fast"] is False
    assert not contract.get("continue-on-error", False)
    assert all("validate_environment_isolation.py" not in step.get("run", "")
               and "loom cluster render" not in step.get("run", "") for step in steps)


@pytest.mark.parametrize("selector_exit,partial", [(19, True), (0, False)])
def test_failed_or_empty_selector_cannot_authorize_test_execution(tmp_path, selector_exit, partial):
    python = tmp_path / "python3"
    python.write_text(f"#!{Path(sys.executable).resolve()}\nimport sys\n"
                      + ("print('partial-selection.py')\n" if partial else "")
                      + f"sys.exit({selector_exit})\n")
    python.chmod(0o700)
    result, outputs = _manifest_step(tmp_path, {"shard_index": 0, "shard_count": 4},
        extra_env={"PATH": str(tmp_path) + os.pathsep + os.environ["PATH"]})
    assert result.returncode == (selector_exit or 1), result.stderr
    assert "paths_file" not in outputs


def test_pytest_failure_propagates(tmp_path, cluster_inputs):
    _, _, _, independent = cluster_inputs
    plan = _plan(tmp_path, changes=(independent,))
    result, outputs = _manifest_step(tmp_path, json.loads(plan["cluster_smoke_matrix"])[0], changes=(independent,))
    assert result.returncode == 0, result.stderr
    result, _ = _pytest_step(tmp_path, outputs["paths_file"], pytest_exit=9)
    assert result.returncode == 9


@pytest.mark.parametrize("status", ["success", "failure", "cancelled", "skipped"])
def test_selected_cluster_gate_requires_success(status):
    step = _jobs()["cluster-smoke-gate"]["steps"][0]
    result = subprocess.run(["bash"], input=step["run"], text=True, capture_output=True,
        env={**os.environ, "PLAN_RESULT": "success", "GATE_MODE": "full", "REQUIRED": "true",
             "CONTRACT_RESULT": status}, check=False)
    assert (result.returncode == 0) == (status == "success")
