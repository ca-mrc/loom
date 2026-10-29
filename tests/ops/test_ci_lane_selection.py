"""Small PRs avoid unrelated runners; shared inputs retain full validation."""
import json

import pytest
from scripts.plan_ci_validations import plan_validations


def plan(path, labels=()):
    return plan_validations(changed_paths=[path], labels=set(labels), event_name="pull_request")


def test_frontend_change_does_not_start_backend_baseline():
    outputs = plan("web/src/App.tsx").github_outputs()
    for lane in ("lint_and_static", "tests_root", "tests_packages", "go_checks", "runtime_payload", "nebius_iac", "locked_environments"):
        assert outputs[lane] == "false"
    assert outputs["web_checks"] == "true"


def test_unrelated_label_preserves_test_selection():
    p = "tests/ops/test_ci_hosted_execution.py"
    assert json.loads(plan(p, ["bug"]).github_outputs()["test_changes"]) == [p]


def test_go_only_change_does_not_start_python_lint():
    assert plan("internal/guestchannel/channel.go").github_outputs()["lint_and_static"] == "false"


def test_ignored_retired_source_still_gets_static_validation():
    assert plan("src/loom/integrations/behavior/retired.py").github_outputs()["lint_and_static"] == "true"


@pytest.mark.parametrize("path", [
    "uv.lock", "pyproject.toml", "packages/loom-launcher/pyproject.toml",
    "config/component-ownership.toml", "unknown/runtime.bin",
])
def test_shared_or_unknown_change_retains_baseline(path):
    outputs = plan(path).github_outputs()
    for lane in ("tests_root", "tests_packages", "go_checks", "runtime_payload", "nebius_iac", "locked_environments"):
        assert outputs[lane] == "true"


def test_explicit_coverage_restores_python_lanes_for_frontend_change():
    outputs = plan("web/src/App.tsx", ["ci:coverage-summary"]).github_outputs()
    assert outputs["tests_root"] == outputs["tests_packages"] == "true"


def test_scheduled_regression_requests_every_ci_test_lane_and_coverage():
    p = plan_validations(changed_paths=["docs/example.md"], labels=set(), event_name="schedule")
    outputs = p.github_outputs()
    for lane in ("tests_root", "tests_packages", "go_checks", "integration", "integration_docker", "coverage_summary", "web_checks"):
        assert outputs[lane] == "true"
    assert outputs["docs_only"] == "false"
    assert outputs["test_changes"] == "[]"


@pytest.mark.parametrize("changes,retained", [
    (("src/loom_service/api/routes/batches.py",), False),
    (("src/loom_llm_gateway/config.py",), False),
    (("database/migrations/versions/new.py",), True),
    (("src/loom/db/schema.py",), True),
    (("src/loom_cli/rollout/readonly_database_bootstrap.py",), True),
    (("unknown/input.bin",), True),
    (("src/loom_service/app.py", "uv.lock"), True),
    (("tests/integration/test_application_schema_reference.py",), True),
    ((), True),
])
def test_schema_component_scope_retains_database_and_unknown_inputs(changes, retained):
    from pathlib import Path

    from scripts.component_ownership import load_manifest, select_affected_test_suites

    root = Path(__file__).resolve().parents[2]
    manifest = load_manifest(root / "config/component-ownership.toml")
    heavy = "tests/integration/test_application_schema_reference.py"
    regular = "tests/integration/test_application_runtime_login.py"
    selected = select_affected_test_suites(manifest, (heavy, regular), changed_paths=changes)
    assert (heavy in selected) is retained
    assert regular in selected


@pytest.mark.parametrize(("changes", "retained"), [
    (("src/loom/config.py",), False),
    (("src/loom_service/app.py", "web/src/main.tsx"), False),
    (("docs/contributing/ci.md",), False),
    (("scripts/ops/inventory_legacy_structures.py",), False),
    (("tests/support/nebius_platform.py",), True),
    (("tests/support/nebius_phase_api.py",), True),
    (("scripts/ops/nebius_shared_pool_api.py",), True),
    (("scripts/ops/deploy_nebius_platform.py",), True),
    (("deploy/k8s/control-plane.yaml",), True),
    (("tests/ops/test_nebius_pool_runtime.py",), True),
    (("tests/ops/nebius_participant_external_fixture.py",), True),
    (("src/loom/config.py", "uv.lock"), True),
    (("config/component-ownership.toml",), True),
    ((), True),
])
def test_pool_operator_suite_runs_on_operator_inputs_only(changes, retained):
    # Operator inputs and imported fixtures retain their pool consumers. The
    # candidate reuses exact-commit native coverage instead of repeating it.
    from pathlib import Path

    from scripts.component_ownership import load_manifest, select_affected_test_suites

    root = Path(__file__).resolve().parents[2]
    manifest = load_manifest(root / "config/component-ownership.toml")
    pool = "tests/ops/test_nebius_shared_pool_handoff_census.py"
    regular = "tests/ops/test_plan_ci_validations.py"
    selected = select_affected_test_suites(manifest, (pool, regular), changed_paths=changes, repo_root=root)
    assert (pool in selected) is retained
    assert regular in selected


@pytest.mark.parametrize(("changes", "retained"), [
    (("scripts/ops/ci_reliability_metrics.py",), False),
    (("scripts/ops/ci_hosted_worker.py", "src/loom/config.py"), False),
    (("scripts/ops/nebius_shared_pool_api.py",), True),
    (("scripts/ops/nebius_management_transport.py",), True),
    (("scripts/ops/nebius_closed_registry.py",), True),
    (("tests/ops/nebius_participant_external_fixture.py",), True),
    (("tests/ops/conftest.py",), True),
    (("uv.lock",), True),
    ((), True),
])
def test_shared_pool_scope_tracks_consumers_not_the_entire_operator_lane(changes, retained):
    from pathlib import Path

    from scripts.component_ownership import _tracked_paths, load_manifest, selected_test_paths

    root = Path(__file__).resolve().parents[2]
    manifest = load_manifest(root / "config/component-ownership.toml")
    selected = selected_test_paths(manifest, tracked_paths=_tracked_paths(root),
        lane="nebius-pool", repo_root=root, changed_paths=changes, test_scope="nebius",
        include_shared_pool=True)
    assert any(Path(path).name.startswith("test_nebius_shared_pool_") for path in selected) is retained
    if not retained:
        assert selected == ()


@pytest.mark.parametrize(("diff", "retained"), [
    (("deploy/guest-runtime/sources.lock.json",), False),
    (("deploy/guest-runtime/sources.lock.json", "src/loom/execution_selection.py"), False),
    (("scripts/ops/nebius_shared_pool_api.py", "deploy/guest-runtime/README.md"), True),
    ((".github/workflows/ci.yml",), True),
    (("uv.lock",), True),
    ((), True),
])
def test_forced_full_regression_runs_shared_pool_only_for_an_affecting_diff(diff, retained):
    # An unowned path forces full regression (no narrowing hint), but the
    # affected_only shared-pool suite still follows the PR's actual diff.
    from pathlib import Path

    from scripts.component_ownership import _tracked_paths, load_manifest, selected_test_paths

    root = Path(__file__).resolve().parents[2]
    manifest = load_manifest(root / "config/component-ownership.toml")
    common = dict(tracked_paths=_tracked_paths(root), lane="nebius-pool", repo_root=root,
                  test_scope="nebius", include_shared_pool=True)
    full = selected_test_paths(manifest, **common)
    selected = selected_test_paths(manifest, changed_paths=(), diff_paths=diff, **common)
    shared = {path for path in full if Path(path).name.startswith("test_nebius_shared_pool_")}
    assert shared
    assert set(selected) == (set(full) if retained else set(full) - shared)


@pytest.mark.parametrize(("event", "labels", "kept"), [
    ("pull_request", (), True),
    ("merge_group", (), True),
    ("pull_request", ("bug",), True),
    ("pull_request", ("ci:integration",), True),
    ("push", (), False),
    ("schedule", (), False),
    ("workflow_dispatch", (), False),
])
def test_planner_keeps_the_pr_diff_when_it_forces_full_regression(event, labels, kept):
    from scripts.plan_ci_validations import LABEL_TO_CHECK

    assert set(labels) - {"bug"} <= LABEL_TO_CHECK.keys()
    paths = ["deploy/guest-runtime/sources.lock.json"]
    outputs = plan_validations(changed_paths=paths, labels=set(labels), event_name=event).github_outputs()
    assert outputs["test_changes"] == "[]"
    assert json.loads(outputs["test_diff"]) == (paths if kept else [])


def test_oversized_diff_falls_back_to_the_complete_suite():
    from dataclasses import replace

    baseline = plan("deploy/guest-runtime/README.md")
    paths = tuple(f"deploy/guest-runtime/{index:05}-{'x' * 80}.md" for index in range(800))
    assert replace(baseline, test_diff=paths).github_outputs()["test_diff"] == "[]"


@pytest.mark.parametrize(("change", "error"), [
    ({"affected_only": "yes"}, "affected_only must be a boolean"),
    ({"affected_only": True, "affected_paths": []}, "affected_only requires affected_paths"),
    ({"affected_only": True, "lane": "integration"}, "affected_only is supported only in lanes: nebius-pool"),
])
def test_affected_only_is_validated(change, error):
    from scripts.component_ownership import ManifestError, _test_suite

    raw = {"id": "heavy", "language": "python", "lane": "nebius-pool",
           "include_paths": ["tests/ops/test_heavy_*.py"], "affected_paths": ["scripts/ops/heavy.py"], **change}
    with pytest.raises(ManifestError, match=error):
        _test_suite(raw)


@pytest.mark.parametrize("changes", [
    ("tests/ops/nebius_participant_external_fixture.py",),
    ("scripts/ops/nebius_shared_pool_api.py",),
    ("scripts/ops/ci_reliability_metrics.py",),
    ("uv.lock",),
])
@pytest.mark.parametrize("lane", ["tests-root", "nebius-pool"])
def test_shard_manifests_execute_exactly_the_planned_selection(changes, lane, tmp_path):
    # Select from the whole lane, then shard: each job must run its part of the
    # planned set, never a set decided by which files share its shard.
    from dataclasses import replace
    from pathlib import Path

    from scripts.component_ownership import (
        TestShardPolicy,
        load_manifest,
        selected_test_paths,
        test_shard_matrix,
    )

    root = Path(__file__).resolve().parents[2]
    manifest = load_manifest(root / "config/component-ownership.toml")
    prefix = "test_nebius_shared_pool" if lane == "nebius-pool" else "test_ci_selection_fixture"
    tests = tuple(f"tests/ops/{prefix}_{suffix}.py" for suffix in ("alpha", "beta", "gamma", "delta"))
    files = {
        # A string-loaded test helper lives in another shard. Selecting within
        # each shard loses this edge and conservatively runs unrelated siblings.
        tests[0]: f'from importlib import import_module\nVALUE = import_module("tests.ops.{prefix}_beta").VALUE\n',
        tests[1]: "from tests.ops.nebius_participant_external_fixture import VALUE\n",
        tests[2]: "def test_unrelated(): pass\n",
        tests[3]: "def test_other(): pass\n",
        "tests/ops/nebius_participant_external_fixture.py": "from scripts.ops.nebius_shared_pool_api import VALUE\n",
        "scripts/ops/nebius_shared_pool_api.py": "VALUE = 1\n",
        "scripts/ops/ci_reliability_metrics.py": "VALUE = 2\n",
        "uv.lock": "",
    }
    for path, source in files.items():
        target = tmp_path / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(source)
    # Real ownership policy and real selection, but only the dependency graph
    # needed to test this contract. Whole-repository shard ownership is audited
    # separately; repeating its import graph once per real shard adds no case.
    manifest = replace(manifest, test_sharding=(TestShardPolicy(lane, 3, "round-robin", None, ()),))
    common = dict(tracked_paths=tuple(files), lane=lane, repo_root=tmp_path, changed_paths=changes,
                  include_shared_pool=True)
    planned = selected_test_paths(manifest, **common)
    fixture_edit = changes == ("tests/ops/nebius_participant_external_fixture.py",)
    full_lane = (lane == "tests-root" and not fixture_edit) or changes == ("uv.lock",)
    expected = tests if full_lane else (
        () if changes == ("scripts/ops/ci_reliability_metrics.py",) else tests[:2])
    assert set(planned) == set(expected)
    policy = manifest.test_shard_policy(lane)
    assert policy is not None
    shards = [selected_test_paths(manifest, **common, shard_index=index, shard_count=policy.shard_count)
              for index in range(policy.shard_count)]
    executed = [path for shard in shards for path in shard]
    assert sorted(executed) == sorted(planned) and len(executed) == len(set(executed))
    matrix = {row["shard_index"] for row in test_shard_matrix(manifest, **common)}
    assert matrix == {index for index, shard in enumerate(shards) if shard}


# Built from parts: narrowing keeps the full lane if any .py file names the module.
UNREFERENCED_POOL_TEST = "tests/ops/" + "_".join(("test_nebius_pool", "activation_transport")) + ".py"


@pytest.mark.parametrize(("path", "pool", "tests_root"), [
    ("src/loom/config.py", False, True),
    ("scripts/ops/deploy_nebius_platform.py", True, True),
    ("tests/ops/test_nebius_pool_runtime.py", True, True),
    # A pool-only edit narrows tests-root to nothing; that must not drop the pool lane.
    (UNREFERENCED_POOL_TEST, True, False),
])
def test_pool_suite_has_its_own_shards_so_tests_root_stays_small(tmp_path, path, pool, tests_root):
    import subprocess
    import sys
    from pathlib import Path

    root = Path(__file__).resolve().parents[2]
    changes, output = tmp_path / "changes", tmp_path / "output"
    changes.write_text(f"{path}\n")
    run = subprocess.run([sys.executable, "scripts/plan_ci_validations.py", "--changed-files", str(changes),
                          "--event-name", "pull_request", "--pull-request-action", "synchronize",
                          "--github-output", str(output), "--test-scope", "nebius"],
                         cwd=root, check=True, capture_output=True, text=True)
    values = dict(line.split("=", 1) for line in output.read_text().splitlines())
    assert values["nebius_pool"] == str(pool).lower()
    assert values["tests_root"] == str(tests_root).lower()
    # The printed diagnostic plan and the job outputs agree on every lane.
    printed = json.loads(run.stdout.splitlines()[-1])
    for name in ("tests_root", "nebius_pool", "integration", "integration_docker", "cluster_smoke"):
        assert values[name] == str(printed[name]).lower(), name
    pool_rows = json.loads(values["nebius_pool_matrix"])
    assert bool(pool_rows) is pool
    assert all(row["shard_count"] == 20 for row in pool_rows)
    assert all(row["shard_count"] == 4 for row in json.loads(values["tests_root_matrix"]))


def test_pool_operator_suite_gates_every_platform_candidate():
    from pathlib import Path

    import yaml
    from scripts.component_ownership import load_manifest

    root = Path(__file__).resolve().parents[2]
    manifest = load_manifest(root / "config/component-ownership.toml")
    [suite] = [item for item in manifest.test_suites if item.id == "nebius-pool-ops"]
    policy = manifest.test_shard_policy(suite.lane)
    assert policy is not None
    jobs = yaml.safe_load((root / ".github/workflows/nebius-candidate.yml").read_text())["jobs"]
    assert "pool-ops-plan" in jobs, "publication must reuse complete exact-commit coverage before scheduling tests"
    tests = jobs["pool-ops-tests"]
    assert "environment" not in tests and "secrets." not in json.dumps(tests)
    assert tests["needs"] == ["pool-ops-plan"]
    assert tests["strategy"]["matrix"] == "${{ fromJSON(needs.pool-ops-plan.outputs.matrix) }}"
    assert tests["if"] == "needs.pool-ops-plan.outputs.required == 'true'"
    run = "\n".join(step.get("run", "") for step in tests["steps"])
    assert 'os.environ["TEST_PATHS_JSON"]' in run
    assert "mapfile -t test_paths" in run and '"${test_paths[@]}"' in run
    assert jobs["pool-ops-plan"]["permissions"] == {
        "contents": "read", "actions": "read", "checks": "read", "pull-requests": "read"}
    assert "environment" not in jobs["pool-ops-plan"]
    assert jobs["publish"]["needs"] == ["pool-ops-plan", "pool-ops-tests"]
    assert "needs.pool-ops-plan.result == 'success'" in jobs["publish"]["if"]
    assert "needs.pool-ops-plan.outputs.required == 'false'" in jobs["publish"]["if"]
    assert "needs.pool-ops-tests.result == 'success'" in jobs["publish"]["if"]


def test_scheduled_workflow_shell_emits_full_plan(tmp_path):
    import os
    import subprocess
    from pathlib import Path

    import yaml

    root = Path(__file__).resolve().parents[2]
    workflow = yaml.safe_load((root / ".github/workflows/ci.yml").read_text())
    step = next(s for s in workflow["jobs"]["workflow-plan"]["steps"] if s.get("id") == "plan")
    output = tmp_path / "output"
    env = {**os.environ, **dict.fromkeys(step["env"], ""),
           "EVENT_NAME": "schedule", "HEAD_SHA": "HEAD", "PR_DRAFT": "false",
           "PR_BASE_CHANGED": "false", "GITHUB_OUTPUT": str(output)}
    run = subprocess.run(["bash", "-c", step["run"].replace("/tmp/loom-changed-files.txt", str(tmp_path / "changes"))],
                         cwd=root, env=env, capture_output=True, text=True)
    assert run.returncode == 0, run.stderr
    values = dict(line.split("=", 1) for line in output.read_text().splitlines())
    assert values["coverage_summary"] == values["integration_docker"] == values["web_checks"] == "true"
    assert values["test_changes"] == "[]"
    assert values["nebius_pool"] == values["tests_root"] == "true"


def test_cli_component_selection_preserves_full_and_affected_modes():
    import subprocess
    import sys
    from pathlib import Path

    root = Path(__file__).resolve().parents[2]
    heavy = "tests/integration/test_application_schema_reference.py"
    for changes, expected in [([], True), (["database/migrations/versions/new.py"], True),
                              (["src/loom_service/api/routes/batches.py"], False)]:
        run = subprocess.run([sys.executable, "scripts/component_ownership.py", "test-paths", "--lane", "integration",
                              "--changed-paths-json", json.dumps(changes)], cwd=root, capture_output=True, text=True)
        assert run.returncode == 0, run.stderr
        assert (heavy in run.stdout.splitlines()) is expected
        assert "tests/integration/test_application_runtime_login.py" in run.stdout.splitlines()


@pytest.mark.parametrize("path", [
    "tests/conftest.py", "tests/integration/conftest.py",
    "tests/integration/taskset_fixtures.py", "packages/loom-launcher/tests/conftest.py",
])
def test_shared_test_inputs_select_consumer_lanes(path):
    p = plan(path)
    assert p.tests_root and p.tests_packages and p.runtime_payload
    assert p.integration and p.integration_docker and p.cluster_smoke and p.staging_smoke


@pytest.mark.parametrize("extra", [(), ("web/src/App.tsx",), ("src/loom_service/app.py",)])
@pytest.mark.parametrize("deleted", [False, True])
def test_shared_or_deleted_test_module_restores_consumer_jobs(tmp_path, monkeypatch, extra, deleted):
    import scripts.plan_ci_validations as planner

    changed = "tests/unit/test_shared_fixture.py"
    consumer = "tests/integration/test_consumer.py"
    for name, source in [(changed, "def fixture(): pass\n"),
                         (consumer, "from tests.unit.test_shared_fixture import fixture\n")]:
        target = tmp_path / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(source)
    if deleted:
        (tmp_path / changed).unlink()
    monkeypatch.setattr(planner, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(planner, "_tracked_paths", lambda _: (changed, consumer))
    p = planner.plan_validations(changed_paths=(changed, *extra), labels=(), event_name="pull_request")
    assert p.integration and p.integration_docker and p.cluster_smoke and p.staging_smoke
    assert p.tests_root and p.tests_packages and p.runtime_payload


@pytest.mark.parametrize("workflow_name", ["ci", "images"])
def test_missing_plan_output_is_not_silently_treated_as_unselected(workflow_name):
    from pathlib import Path

    import yaml
    from scripts.plan_ci_validations import BASELINE_CHECKS

    root = Path(__file__).resolve().parents[2]
    workflow = yaml.safe_load((root / f".github/workflows/{workflow_name}.yml").read_text())
    job = "workflow-plan" if workflow_name == "ci" else "plan"
    for lane in BASELINE_CHECKS if workflow_name == "ci" else ("harbor_required",):
        expression = workflow["jobs"][job]["outputs"][lane]
        # With both step outputs absent, GitHub resolves references to empty
        # strings. A literal false fallback would hide that missing decision.
        operands = expression.removeprefix("${{").removesuffix("}}").strip().split("||")
        missing = next((part.strip().strip("'") for part in operands
                        if part.strip().startswith("'")), "")
        assert missing == "", f"{lane} masks an absent planner decision: {expression}"


@pytest.mark.parametrize("extra", [(), ("web/src/App.tsx",)])
def test_retired_ignored_inputs_do_not_restart_backend_jobs(extra):
    from scripts.plan_ci_validations import BASELINE_CHECKS

    p = plan_validations(changed_paths=("src/loom/integrations/behavior/stages/rollout.py", *extra),
                         labels=(), event_name="pull_request")
    assert p.lint_and_static
    assert not any(getattr(p, lane) for lane in BASELINE_CHECKS if lane != "lint_and_static")
    assert not p.integration and not p.integration_docker
    assert p.web_checks == bool(extra)


@pytest.mark.parametrize("path", ["src/loom_service/app.py", "src/loom_control_plane/service_execution.py",
                                  "src/loom/nebius_platform_render.py", "cmd/loom-execution-runtime/main.go"])
def test_ordinary_source_reuses_locked_install_in_owning_jobs(path):
    p = plan(path)
    assert p.tests_root or p.go_checks
    assert not p.locked_environments


@pytest.mark.parametrize("event", ["workflow_dispatch", "schedule"])
def test_full_regression_keeps_independent_locked_install(event):
    p = plan_validations(changed_paths=["src/loom_service/app.py"], labels=set(), event_name=event)
    assert p.locked_environments


@pytest.mark.parametrize("path", ["web/src/App.tsx", "web/src/auth/AuthContext.tsx",
                                  "web/package-lock.json", "deploy/Dockerfile.web",
                                  "deploy/nginx-spa.conf", "deploy/nginx-spa-security-headers.conf",
                                  "deploy/web-runtime-config.sh"])
def test_frontend_runtime_has_browser_and_image_checks_without_unrelated_compose(path):
    p = plan(path)
    assert p.web_checks and p.images
    assert not any((p.integration, p.integration_docker, p.cluster_smoke, p.staging_smoke,
                    p.tests_root, p.tests_packages, p.go_checks, p.runtime_payload,
                    p.locked_environments, p.nebius_iac))


def test_mixed_frontend_backend_and_explicit_labels_keep_system_smoke():
    p = plan_validations(changed_paths=["web/src/App.tsx", "src/loom_service/app.py"],
                         labels=set(), event_name="pull_request")
    assert p.staging_smoke and p.integration and p.tests_root and p.web_checks
    assert plan("web/src/App.tsx", ["staging-smoke"]).staging_smoke
    assert plan("web/src/App.tsx", ["cluster-smoke"]).cluster_smoke


def test_large_change_list_keeps_full_lanes_without_oversized_job_environment():
    from dataclasses import replace
    from pathlib import Path

    from scripts.component_ownership import load_manifest, select_affected_test_suites

    baseline = plan("unknown/runtime.bin")
    changes = tuple(f"tests/unit/test_retired_{index}_{'x' * 96}.py" for index in range(2000))
    assert len(json.dumps(changes).encode()) > 131072
    outputs = replace(baseline, test_changes=changes).github_outputs()
    assert outputs["test_changes"] == "[]"
    for lane in ("tests_root", "tests_packages", "integration", "integration_docker"):
        assert outputs[lane] == baseline.github_outputs()[lane] == "true"
    root = Path(__file__).resolve().parents[2]
    manifest = load_manifest(root / "config/component-ownership.toml")
    paths = ("tests/integration/test_application_schema_reference.py",
             "tests/integration/test_application_runtime_login.py")
    assert select_affected_test_suites(
        manifest, paths, changed_paths=tuple(json.loads(outputs["test_changes"]))
    ) == paths
