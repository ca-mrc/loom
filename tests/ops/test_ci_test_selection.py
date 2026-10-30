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


@pytest.mark.parametrize("source", [
    'PIN = "tests/unit/test_alpha.py"\nassert PIN.endswith(".py")\n',
    'PATHS = {"tests/unit/test_alpha.py"}\ndef selected(path): chosen = path in PATHS\n',
    '@pytest.mark.parametrize("path", ["tests/unit/test_alpha.py"])\ndef test_selection(path): pass\n',
    'paths = ("tests/unit/test_alpha.py",)\nselect_test_scope(manifest, paths, scope="nebius")\n',
])
def test_ci_metadata_path_literals_do_not_make_a_test_a_fixture(tmp_path: Path, source: str) -> None:
    tracked = _repository(tmp_path)
    metadata = "tests/ops/test_component_ownership_manifest.py"
    target = tmp_path / metadata
    target.parent.mkdir(parents=True)
    target.write_text(source)
    assert narrow_test_only_changes(
        tracked[:2], changed_paths=(tracked[0],),
        tracked_paths=(*tracked, metadata), repo_root=tmp_path,
    ) == (tracked[0],)


@pytest.mark.parametrize("source", [
    'from tests.unit.test_alpha import fixture\n',
    'import importlib\nfixture = importlib.import_module("tests.unit.test_alpha")\n',
    'SOURCE = "from tests.unit.test_alpha import fixture"\n',
    'import runpy\nrunpy.run_path("tests/unit/test_alpha.py")\n',
    'import runpy\nPATH = "tests/unit/test_alpha.py"\nrunpy.run_path(PATH)\n',
    '@pytest.mark.parametrize("case", [runpy.run_path("tests/unit/test_alpha.py")])\ndef test_case(case): pass\n',
    'def fixture_path(): return "tests/unit/test_alpha.py"\nrunpy.run_path(fixture_path())\n',
    'def load_fixture(path="tests/unit/test_alpha.py"): return runpy.run_path(path)\nload_fixture()\n',
    'for path in ("tests/unit/test_alpha.py",):\n    runpy.run_path(path)\n',
    '@pytest.mark.parametrize("path", ["tests/unit/test_alpha.py"])\ndef test_loader(path): runpy.run_path(path)\n',
    'fixture.path = "tests/unit/test_alpha.py"\nrunpy.run_path(fixture.path)\n',
    'fixture["path"] = "tests/unit/test_alpha.py"\nrunpy.run_path(fixture["path"])\n',
    'assert all(runpy.run_path(path) for path in ["tests/unit/test_alpha.py"])\n',
    'paths = {"tests/unit/test_alpha.py"}\nassert all(runpy.run_path(path) for path in paths)\n',
])
def test_ci_metadata_still_preserves_real_or_dynamic_fixture_dependencies(
    tmp_path: Path, source: str,
) -> None:
    tracked = _repository(tmp_path)
    metadata = "tests/ops/test_component_ownership_manifest.py"
    target = tmp_path / metadata
    target.parent.mkdir(parents=True)
    target.write_text(source)
    assert narrow_test_only_changes(
        tracked[:2], changed_paths=(tracked[0],),
        tracked_paths=(*tracked, metadata), repo_root=tmp_path,
    ) == tracked[:2]


def test_non_metadata_test_path_string_keeps_the_unknown_consumer(tmp_path: Path) -> None:
    tracked = _repository(tmp_path)
    (tmp_path / tracked[1]).write_text(f'SOURCE_FILE = "{tracked[0]}"\n')
    assert narrow_test_only_changes(
        tracked[:2], changed_paths=(tracked[0],), tracked_paths=tracked, repo_root=tmp_path,
    ) == tracked[:2]


def test_independent_edit_does_not_start_empty_shards(tmp_path: Path) -> None:
    from dataclasses import replace

    from scripts import component_ownership as ownership

    tracked = _repository(tmp_path)
    manifest = ownership.load_manifest(Path(__file__).resolve().parents[2] / "config/component-ownership.toml")
    manifest = replace(manifest, test_sharding=tuple(replace(policy, pins=())
                       for policy in manifest.test_sharding))
    matrix = ownership.test_shard_matrix(
        manifest, tracked_paths=tracked, lane="tests-root", repo_root=tmp_path,
        changed_paths=(tracked[0],), test_scope="nebius",
    )
    assert len(matrix) == 1
    assert matrix[0]["shard_count"] == manifest.test_shard_policy("tests-root").shard_count
    selected = ownership.selected_test_paths(
        manifest, tracked_paths=tracked, lane="tests-root", repo_root=tmp_path,
        changed_paths=(tracked[0],), test_scope="nebius",
        shard_index=matrix[0]["shard_index"], shard_count=matrix[0]["shard_count"],
    )
    assert selected == (tracked[0],)


@pytest.mark.parametrize("include_shared_pool", [False, True])
def test_default_pool_balance_preserves_manual_layout(include_shared_pool: bool) -> None:
    from scripts import component_ownership as ownership

    root = Path(__file__).resolve().parents[2]
    manifest = ownership.load_manifest(root / "config/component-ownership.toml")
    tracked = ownership._tracked_paths(root)
    policy = manifest.test_shard_policy("nebius-pool")
    assert policy is not None
    complete = ownership.test_paths_for_lane(manifest, tracked_paths=tracked, lane="nebius-pool")
    original = [ownership.shard_paths(complete, shard_index=index, shard_count=policy.shard_count,
                                     strategy=policy.strategy, salt=policy.salt, pins=policy.pins)
                for index in range(policy.shard_count)]
    expected = [tuple(path for path in shard if include_shared_pool
                      or not manifest.test_owner_for_path(path).manual_only) for shard in original]
    retained = "tests/ops/test_nebius_development_pool_retained.py"
    assert retained in original[2]
    actual = [ownership.selected_test_paths(
        manifest, tracked_paths=tracked, lane="nebius-pool", repo_root=root,
        include_shared_pool=include_shared_pool, shard_index=index, shard_count=policy.shard_count,
    ) for index in range(policy.shard_count)]
    assert actual == expected
    flattened = [path for shard in actual for path in shard]
    assert len(flattened) == len(set(flattened))
    assert set(flattened) == {path for path in complete if include_shared_pool
                             or not manifest.test_owner_for_path(path).manual_only}
    matrix = ownership.test_shard_matrix(manifest, tracked_paths=tracked, lane="nebius-pool",
                                        repo_root=root, include_shared_pool=include_shared_pool)
    assert [row["shard_index"] for row in matrix] == [index for index, shard in enumerate(actual) if shard]
    if include_shared_pool:
        gateway = set(next(shard for shard in actual if "tests/ops/test_nebius_pool_gateway_retirement_live.py" in shard))
        assert gateway.isdisjoint({"tests/ops/test_nebius_pool_role_restoration_live.py",
                                   "tests/ops/test_nebius_pool_template_restoration_live.py"})
        predecessor_groups = [next(index for index, shard in enumerate(actual) if path in shard) for path in (
            "tests/ops/test_nebius_pool_predecessor.py", "tests/ops/test_nebius_pool_predecessor_live.py",
            "tests/ops/test_nebius_pool_refresh_connected.py")]
        assert len(set(predecessor_groups)) == 3


@pytest.mark.parametrize("include_shared_pool", [False, True])
def test_default_pool_pin_plans_the_same_independent_test_it_executes(
    tmp_path: Path, include_shared_pool: bool,
) -> None:
    from dataclasses import replace

    from scripts import component_ownership as ownership

    root = Path(__file__).resolve().parents[2]
    manifest = ownership.load_manifest(root / "config/component-ownership.toml")
    retained = "tests/ops/test_nebius_development_pool_retained.py"
    tracked = (retained, "tests/ops/test_nebius_legacy_pool_completion.py")
    for name in tracked:
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("def test_independent(): assert True\n")
    manifest = replace(manifest, test_sharding=tuple(
        replace(policy, pins=(ownership.TestShardPin(retained, 2),),
                default_pins=(ownership.TestShardPin(retained, 7),))
        if policy.lane == "nebius-pool" else policy for policy in manifest.test_sharding
    ))
    arguments = dict(manifest=manifest, tracked_paths=tracked, lane="nebius-pool", repo_root=tmp_path,
                     changed_paths=(retained,), include_shared_pool=include_shared_pool)
    matrix = ownership.test_shard_matrix(**arguments)
    if not include_shared_pool:
        assert matrix == ()
        assert ownership.selected_test_paths(**arguments) == ()
        return
    assert len(matrix) == 1
    assert matrix[0]["shard_index"] == 2
    selected = ownership.selected_test_paths(**arguments, shard_index=matrix[0]["shard_index"],
                                             shard_count=matrix[0]["shard_count"])
    assert selected == (retained,)


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
    job = workflow["jobs"][lane]
    dynamic = lane != "tests-packages"
    step = next(step for step in job["steps"]
                if step.get("id") == "manifest" if dynamic) if dynamic else next(
                    step for step in job["steps"] if step.get("name", "").startswith("Pytest"))
    stub = tmp_path / ("python3" if dynamic else "uv")
    stub.write_text(f"#!{Path(sys.executable).resolve()}\nimport os,sys\n"
                    'assert "test-paths" in sys.argv, "empty selection reached pytest"\n'
                    'sys.exit(int(os.environ["SELECT_EXIT"]))\n')
    stub.chmod(0o755)
    result = subprocess.run(
        ["bash", "-c", step["run"]], text=True, capture_output=True,
        env={**os.environ, "PATH": f"{tmp_path}:{os.environ['PATH']}",
             "RUNNER_TEMP": str(tmp_path), "SHARD_INDEX": "0", "SHARD_COUNT": "2",
             "COVERAGE_ENABLED": "false", "TEST_CHANGED_PATHS": "[]",
               "SHARED_POOL_SIMULATIONS": "false",
             "GITHUB_OUTPUT": str(tmp_path / "output"), "SELECT_EXIT": str(selection_exit)},
    )
    # A dynamic matrix promises a nonempty shard; a stale/empty selector must
    # fail before setup rather than claim that its selected checks succeeded.
    expected = selection_exit or (1 if dynamic else 0)
    assert result.returncode == expected, result.stderr


@pytest.mark.timeout(120)
@pytest.mark.parametrize(("scope", "lane", "include_shared_pool"), [
    ("all", "tests-root", False), ("nebius", "tests-root", False),
    ("nebius", "nebius-pool", False), ("nebius", "nebius-pool", True),
])
def test_shard_workflows_run_complete_disjoint_fail_fast_shards(
        tmp_path: Path, scope: str, lane: str, include_shared_pool: bool) -> None:
    """Exercise the workflow/selector boundary, not a second shard algorithm."""
    root = Path(__file__).resolve().parents[2]
    workflow = yaml.safe_load((root / ".github/workflows/ci.yml").read_text())
    job = workflow["jobs"][lane]
    from scripts import component_ownership as ownership
    manifest = ownership.load_manifest(root / "config/component-ownership.toml")
    tracked = ownership._tracked_paths(root)
    selection = dict(manifest=manifest, tracked_paths=tracked, lane=lane, repo_root=root,
                     test_scope=scope, include_shared_pool=include_shared_pool)
    matrix = ownership.test_shard_matrix(**selection)
    step = next(row for row in job["steps"] if row.get("name", "").startswith("Pytest"))
    selector_step = next(row for row in job["steps"] if row.get("id") == "manifest")
    if lane == "nebius-pool":
        # This lane contains only Nebius modules. Prove scope equivalence once
        # instead of spawning the same 20 selector/workflow processes twice.
        # A future scope difference must restore distinct execution coverage.
        all_paths = ownership.test_paths_for_lane(
            manifest, tracked_paths=tracked, lane=lane,
        )
        assert ownership.select_test_scope(manifest, all_paths, scope="all") == (
            ownership.select_test_scope(manifest, all_paths, scope="nebius")
        )
    count = manifest.test_shard_policy(lane).shard_count
    complete = ownership.selected_test_paths(**selection)
    shards = [ownership.selected_test_paths(**selection, shard_index=index, shard_count=count)
              for index in range(count)]
    assert sorted(row["shard_index"] for row in matrix) == [index for index, shard in enumerate(shards) if shard]
    flattened = [path for shard in shards for path in shard]
    assert len(flattened) == len(set(flattened))
    assert set(flattened) == set(complete)
    if not matrix:
        assert lane == "nebius-pool" and not include_shared_pool
        return
    uv = tmp_path / "uv"
    uv.write_text(
        f"#!{Path(sys.executable).resolve()}\n"
        "import json, os, sys\nfrom pathlib import Path\n"
        "assert 'pytest' in sys.argv\n"
        "Path(os.environ['SELECTED_TEST_ARGS']).write_text(json.dumps(sys.argv))\n"
        "sys.exit(int(os.environ['PYTEST_EXIT']))\n"
    )
    uv.chmod(0o755)
    # The full partition stays above; exercise the same shell with first/last
    # indices, failure/success and both scope/manual-flag modes.
    for row in (matrix[0], matrix[-1]):
        recorded = tmp_path / f"args-{row['shard_index']}.json"
        # A failed test remains a failed shard, including the first shard.
        pytest_exit = 17 if row["shard_index"] == 0 else 0
        env = {**os.environ, "PATH": f"{tmp_path}:{os.environ['PATH']}",
               "RUNNER_TEMP": str(tmp_path), "SHARD_INDEX": str(row["shard_index"]),
               "SHARD_COUNT": str(count), "COVERAGE_ENABLED": "false", "TEST_CHANGED_PATHS": "[]",
               "SHARED_POOL_SIMULATIONS": str(include_shared_pool).lower(),
               "CI_TEST_SCOPE": scope, "SELECTED_TEST_ARGS": str(recorded), "PYTEST_EXIT": str(pytest_exit),
               "GITHUB_OUTPUT": str(tmp_path / "output")}
        selected = subprocess.run(["bash", "-c", selector_step["run"]], cwd=root,
                                  env=env, text=True, capture_output=True)
        assert selected.returncode == 0, selected.stderr
        script = step["run"].replace("${{ steps.manifest.outputs.paths_file }}", str(tmp_path / f"{lane}-manifest.txt"))
        result = subprocess.run(["bash", "-c", script], cwd=root, env=env, text=True, capture_output=True)
        assert result.returncode == pytest_exit, result.stderr
        arguments = json.loads(recorded.read_text())
        assert "-x" in arguments or "--maxfail=1" in arguments
        paths = tuple(arg for arg in arguments if arg.startswith("tests/"))
        assert paths == shards[row["shard_index"]]
