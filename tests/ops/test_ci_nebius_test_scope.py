"""Nebius admission retains common contracts; legacy validation stays callable."""
import os
import subprocess
import sys
from pathlib import Path

import pytest
import yaml
from scripts.component_ownership import load_manifest

ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize("lane,legacy,common", [
    ("tests-root", (




    ), ("tests/unit/test_nebius_platform_render.py",
        "tests/unit/test_native_build_source_isolation.py",
        "tests/unit/test_task_image_build_plan.py",)),
    ("integration", (
        "tests/integration/test_application_schema_reference.py",
        "tests/integration/test_application_runtime_login.py",




    ), (
        "tests/integration/test_nebius_platform_bootstrap.py",
        "tests/integration/test_application_migration_authority.py",
        "tests/integration/test_application_runtime_grants.py",
        "tests/integration/test_alembic_migrations.py",
        "tests/integration/test_nebius_task_image_controller.py",
        "tests/integration/test_task_image_materialization_store.py",
        "tests/integration/test_task_image_manifest_identity_store.py",
    )),
    ("integration-docker", (
                            ),
     ("tests/integration/test_nebius_restore.py",)),
    ("cluster-smoke", (),
     ("tests/integration/test_nebius_platform_k3s.py",)),
    ("go-checks", (), ()),
])
def test_real_cli_preserves_common_tests_and_manual_compatibility(lane, legacy, common):
    for scope in ("nebius", "all"):
        run = subprocess.run([sys.executable, "scripts/component_ownership.py", "test-paths",
                              "--lane", lane, "--test-scope", scope], cwd=ROOT, capture_output=True, text=True)
        assert run.returncode == 0, run.stderr
        paths = run.stdout.splitlines()
        assert set(common) <= set(paths)
        for path in legacy:
            assert (path in paths) == (scope == "all"), path


def test_compatibility_scope_does_not_disable_test_ownership():
    manifest = load_manifest(ROOT / "config/component-ownership.toml")
    assert manifest.compatibility_test_paths
    for path in ("tests/integration/test_application_workload_recovery.py",
                 "tests/integration/test_application_capacity_bootstrap_runtime.py"):
        assert len(manifest.test_owners_for_path(path)) == 1
        assert manifest.test_owners_for_path(path)[0].ci_enabled
        assert not manifest.ci_ignores_path(path)


def test_existing_manual_ci_runs_both_legacy_integration_tiers():
    workflow = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())
    assert "legacy_compatibility" in workflow[True]["workflow_dispatch"]["inputs"]
    step = next(s for s in workflow["jobs"]["workflow-plan"]["steps"] if s.get("id") == "plan")
    for key in ("DISPATCH_INTEGRATION", "DISPATCH_INTEGRATION_DOCKER"):
        assert "inputs.legacy_compatibility" in step["env"][key]
    assert "legacy_compatibility" in workflow["env"]["CI_TEST_SCOPE"]
    assert "legacy_compatibility" in workflow["env"]["CI_PYTEST_MARKERS"]


@pytest.mark.parametrize("workflow_name", ["ci", "cluster-smoke", "staging-smoke"])
def test_all_python_entrypoints_use_explicit_scope_and_marker(workflow_name):
    workflow = yaml.safe_load((ROOT / f".github/workflows/{workflow_name}.yml").read_text())
    assert "nebius" in workflow["env"]["CI_TEST_SCOPE"]
    assert "not legacy_pool" in workflow["env"]["CI_PYTEST_MARKERS"]
    for job in workflow["jobs"].values():
        for step in job.get("steps", []):
            run = step.get("run", "")
            if "uv run --no-sync pytest" in run:
                assert "CI_PYTEST_MARKERS" in run
            if "component_ownership.py test-paths --lane" in run:
                assert "--test-scope" in run


@pytest.mark.parametrize("scope,report_error,accepted", [
    ("nebius", False, True), ("all", False, False),
    ("nebius", True, False), ("all", True, False),
])
def test_changed_coverage_population_reports_without_reusing_old_floor(tmp_path, scope, report_error, accepted):
    workflow = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())
    step = next(s for s in workflow["jobs"]["fast-coverage"]["steps"]
                if s.get("name") == "Coverage gate + summary (fast tier)")
    uv = tmp_path / "uv"
    uv.write_text('#!/bin/sh\n'
                  'if [ "$REPORT_ERROR" = true ]; then exit 2; fi\n'
                  'echo 50\n'
                  'case "$*" in *--fail-under=70*) exit 1;; esac\n')
    uv.chmod(0o755)
    run = subprocess.run(["bash", "-c", step["run"]], cwd=tmp_path, capture_output=True, text=True,
                         env={**os.environ, "PATH": f"{tmp_path}:{os.environ['PATH']}",
                              "CI_TEST_SCOPE": scope, "REPORT_ERROR": str(report_error).lower(),
                              "GITHUB_STEP_SUMMARY": str(tmp_path / "summary")})
    assert (run.returncode == 0) is accepted, run.stderr



def test_scope_is_conservative_for_unknown_common_inputs_and_rejects_invalid_mode():
    from scripts.component_ownership import ManifestError, select_test_scope

    manifest = load_manifest(ROOT / "config/component-ownership.toml")
    paths = ("tests/integration/test_future_shared_contract.py", "tests/integration/test_alembic_migrations.py",
             "tests/integration/test_nebius_platform_k3s.py", "tests/unit/test_execution_capacity_controller.py")
    assert select_test_scope(manifest, paths, scope="nebius") == paths
    with pytest.raises(ManifestError, match="unknown test scope"):
        select_test_scope(manifest, paths, scope="typo")


def test_go_checks_cover_current_runtime_packages():
    jobs = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())["jobs"]
    commands = [step.get("run", "") for step in jobs["go-checks"]["steps"]]
    assert "go vet ./cmd/... ./internal/..." in commands
    assert "go test -race ./cmd/... ./internal/..." in commands


def test_retired_capacity_hooks_are_removed():
    workflow = (ROOT / ".github/workflows/ci.yml").read_text()
    assert "global_fleet_capacity_shadow_once.py" not in workflow
    assert "Global capacity Package 3 no-scheduler-mutation audit" not in workflow
