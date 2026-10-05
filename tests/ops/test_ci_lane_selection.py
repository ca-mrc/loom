"""Small PRs avoid unrelated runners; shared inputs retain full validation."""
import json

import pytest
from scripts.plan_ci_validations import plan_validations


def plan(path, labels=()):
    return plan_validations(changed_paths=[path], labels=set(labels), event_name="pull_request")


def test_frontend_change_does_not_start_backend_baseline():
    outputs = plan("web/src/App.tsx").github_outputs()
    for lane in ("tests_root", "tests_packages", "go_checks", "runtime_payload", "nebius_iac", "locked_environments"):
        assert outputs[lane] == "false"
    assert outputs["web_checks"] == "true"


def test_unrelated_label_preserves_test_selection():
    p = "tests/ops/test_ci_hosted_execution.py"
    assert json.loads(plan(p, ["bug"]).github_outputs()["test_changes"]) == [p]


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


@pytest.mark.parametrize("selected,result,accepted", [
    ("false", "skipped", True), ("true", "success", True),
    ("true", "skipped", False), ("true", "failure", False),
    ("false", "cancelled", False), ("invalid", "success", False), ("", "skipped", False),
])
def test_baseline_aggregator_checks_selected_results(selected, result, accepted):
    import subprocess
    from pathlib import Path

    import yaml

    root = Path(__file__).resolve().parents[2]
    workflow = yaml.safe_load((root / ".github/workflows/ci.yml").read_text())
    step = next(s for s in workflow["jobs"]["repository-checks"]["steps"]
                if s.get("name") == "Enforce selected validation results")
    env = {key: "success" if key.endswith("RESULT") else "false" for key in step["env"]}
    env.update(GATE_MODE="full", DOCS_ONLY="false", ROOT_SELECTED=selected, ROOT_RESULT=result)
    run = subprocess.run(["bash", "-c", step["run"]], env=env, capture_output=True, text=True)
    assert (run.returncode == 0) is accepted, run.stderr


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
    assert not any(getattr(p, lane) for lane in BASELINE_CHECKS)
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


def test_installed_wheel_smoke_imports_only_retained_modules():
    import ast
    from importlib.util import find_spec
    from pathlib import Path

    import yaml

    root = Path(__file__).resolve().parents[2]
    workflow = yaml.safe_load((root / ".github/workflows/ci.yml").read_text())
    step = next(item for item in workflow["jobs"]["tests-root"]["steps"]
                if item.get("name") == "Build and exercise non-editable runtime wheel")
    source = step["run"].split("<<'PY'\n", 1)[1].rsplit("\nPY", 1)[0]
    imports = []
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            imports.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imports.append(node.module)
    for module in imports:
        if module.startswith(("loom.", "loom_")):
            assert find_spec(module) is not None, module
