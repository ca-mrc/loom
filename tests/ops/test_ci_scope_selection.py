"""CI impact boundaries: unrelated work skips; common/unknown inputs stay full."""

import json
import os
import subprocess
from pathlib import Path

import pytest
import yaml
from scripts import component_ownership as ownership
from scripts.plan_ci_validations import HEAVY_CHECKS, plan_validations

ROOT = Path(__file__).resolve().parents[2]


def plan(*changes, event="pull_request", labels=()):
    return plan_validations(changed_paths=changes, labels=set(labels), event_name=event)


@pytest.mark.parametrize(
    "path",
    [
        "scripts/ops/nebius_archive_recovery.py",
        "scripts/ops/nebius_terminal_archive_recovery.py",
        "scripts/ops/nebius_terminal_cleanup_recovery.py",
    ],
)
def test_archive_recovery_keeps_lifecycle_regressions_without_unrelated_runners(path):
    value = plan(path)
    assert value.tests_root and value.integration and value.lint_and_static
    assert not value.unowned_runtime
    assert not any(
        (
            value.tests_packages,
            value.runtime_payload,
            value.go_checks,
            value.nebius_iac,
            value.images,
            value.cluster_smoke,
            value.staging_smoke,
        )
    )
    manifest = ownership.load_manifest(ROOT / "config/component-ownership.toml")
    assert not ownership.selected_test_paths(
        manifest,
        tracked_paths=ownership._tracked_paths(ROOT),
        lane="nebius-pool",
        repo_root=ROOT,
        changed_paths=(path,),
        test_scope="nebius",
    )


@pytest.mark.parametrize(
    "path",
    [
        "web/src/new-feature.test.tsx",
        "packages/loom-benchmarks/tests/" + "_".join(("test", "base")) + ".py",
        "cmd/loom-build-deadline/main_test.go",
    ],
)
def test_executable_test_edits_do_not_rebuild_product_images(path):
    assert not plan(path).images
    manifest = ownership.load_manifest(ROOT / "config/component-ownership.toml")
    assert not ownership.select_release_image_matrix(
        manifest,
        changed_paths=(path,),
        force_all=False,
        image_set="nebius",
    )
    assert ownership.select_release_image_matrix(
        manifest,
        changed_paths=(path,),
        force_all=True,
        image_set="nebius",
    )


def test_server_edit_does_not_repeat_isolated_fixture_payloads():
    value = plan("src/loom_service/routes/trials.py")
    assert value.tests_root and value.integration and value.staging_smoke and value.images
    assert not value.runtime_payload
    assert plan("scripts/runtime_payload_dispatch.py").runtime_payload
    assert plan("uv.lock").runtime_payload


def test_docker_test_edit_keeps_its_owner_without_fast_integration(tmp_path, monkeypatch):
    import scripts.plan_ci_validations as planner

    path = "tests/integration/" + "_".join(("test", "guest", "sandbox", "runtime")) + ".py"
    target = tmp_path / path
    target.parent.mkdir(parents=True)
    target.write_text("def test_independent(): pass\n")
    monkeypatch.setattr(planner, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(planner, "_tracked_paths", lambda _: (path, "README.md"))
    value = plan(path)
    assert value.integration_docker and not value.integration


def test_unknown_and_selection_authority_keep_all_lanes_and_full_manifests():
    for path in (
        "unknown/runtime.bin",
        "scripts/plan_ci_validations.py",
        "config/component-ownership.toml",
        "uv.lock",
    ):
        value = plan(path)
        assert value.selected_heavy_checks() == set(HEAVY_CHECKS)
        assert value.runtime_payload and value.go_checks and value.tests_packages
        assert value.test_changes == ()


def test_component_scope_follows_indirect_and_string_consumers(tmp_path):
    files = {
        "scripts/ops/component.py": "VALUE=1\n",
        "scripts/ops/adapter.py": "from scripts.ops.component import VALUE\n",
        "tests/ops/test_consumer.py": "SCRIPT='scripts/ops/adapter.py'\n",
        "tests/ops/test_unrelated.py": "def test_other(): pass\n",
    }
    for name, source in files.items():
        target = tmp_path / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(source)
    tests = tuple(name for name in files if name.startswith("tests/"))

    def select(changes):
        return ownership._component_test_consumers(
            tests,
            changes=changes,
            sources=("scripts/ops/component.py",),
            repo_root=tmp_path,
        )

    assert select(("scripts/ops/component.py",)) == frozenset(tests[:1])
    assert select(("scripts/ops/missing.py",)) == frozenset(tests)
    assert select(("scripts/ops/component.py", "uv.lock")) == frozenset(tests)


def test_shared_pool_scope_follows_operator_dependencies_and_preserves_unknown_entrypoints(tmp_path):
    files = {
        "scripts/ops/component.py": "VALUE=1\n",
        "scripts/ops/adapter.py": "from scripts.ops.component import VALUE\n",
        "scripts/ops/unrelated.py": "VALUE=2\n",
        "scripts/ops/nebius_shared_pool_new.py": "VALUE=3\n",
        "tests/support/fixture.py": "SCRIPT='scripts/ops/adapter.py'\n",
        "tests/ops/test_nebius_shared_pool_alpha.py": "from tests.support.fixture import SCRIPT\n",
        "tests/ops/test_nebius_shared_pool_beta.py": "def test_other(): pass\n",
    }
    for name, source in files.items():
        target = tmp_path / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(source)
    tests = tuple(name for name in files if Path(name).name.startswith("test_"))
    manifest = ownership.load_manifest(ROOT / "config/component-ownership.toml")

    def select(*changes):
        return ownership.select_affected_test_suites(manifest, tests,
            changed_paths=changes, repo_root=tmp_path)

    assert select("scripts/ops/component.py") == tests[:1]
    assert select("scripts/ops/unrelated.py") == ()
    assert select("scripts/ops/nebius_shared_pool_new.py") == tests
    assert select("scripts/ops/component.py", "uv.lock") == tests
    # Deleting a referenced operator is not evidence of no dependency.
    (tmp_path / "scripts/ops/component.py").unlink()
    assert select("scripts/ops/component.py") == tests
    (tmp_path / "tests/support/fixture.py").unlink()
    assert select("tests/support/fixture.py") == tests
    # Unavailable dependency evidence must not classify an edit as unrelated.
    (tmp_path / "tests/support/fixture.py").write_text("not python !")
    assert select("scripts/ops/unrelated.py") == tests


def test_helper_import_discovery_is_statement_bounded_and_fresh(tmp_path, monkeypatch):
    # Large literal fixtures are not imports. Traversal work must depend on
    # statements, while retaining imports inside every nested statement container.
    source = """PAYLOAD = PAYLOAD_VALUE
if True:
    import tests.support.alpha as alpha
class Nested:
    async def load(self):
        try:
            from tests.support import beta
        except Exception:
            import tests.support.gamma
        finally:
            match value:
                case _:
                    from tests.support.delta import VALUE
"""
    target = tmp_path / "tests/sample.py"
    target.parent.mkdir()
    (tmp_path / "tests/support").mkdir()
    for name in ("alpha", "beta", "gamma", "delta", "later"):
        (tmp_path / f"tests/support/{name}.py").write_text("import tests.sample\n")
    target.write_text(source.replace("PAYLOAD_VALUE", "[0]"))
    visited = 0
    children = ownership.ast.iter_child_nodes

    def count(node):
        nonlocal visited
        visited += 1
        return children(node)

    monkeypatch.setattr(ownership.ast, "iter_child_nodes", count)
    expected = frozenset({"tests/sample.py", "tests/support/alpha.py", "tests/support/beta.py",
                          "tests/support/gamma.py", "tests/support/delta.py"})
    assert ownership._imported_test_modules(("tests/sample.py",), repo_root=tmp_path) == expected
    small = visited
    visited = 0
    target.write_text(source.replace("PAYLOAD_VALUE", "[" + "0," * 5000 + "]")
                      + "from tests.support import later\n")
    assert ownership._imported_test_modules(("tests/sample.py",), repo_root=tmp_path) == (
        expected | {"tests/support/later.py"})
    assert visited <= 2 * small + 40, "fixture expressions multiplied import-discovery traversal work"


@pytest.mark.parametrize(
    "event,labels",
    [("schedule", ()), ("workflow_dispatch", ()), ("pull_request", ("ci:coverage-summary",))],
)
def test_explicit_regression_never_reduces_test_hint(event, labels):
    assert plan("docs/guide.md", event=event, labels=labels).test_changes == ()


@pytest.mark.parametrize("changed,runtime", [
    ("src/loom_service/routes/trials.py", False),
    ("web/src/App.tsx", True),
    ("uv.lock", True),
])
def test_frontend_contract_keeps_codegen_for_backend_api_changes(tmp_path, changed, runtime):
    from tests.ops.test_ci_images_parallel_builds import _condition

    selected = plan(changed)
    assert selected.web_checks
    assert selected.web_runtime is runtime
    workflow = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())
    job = workflow["jobs"]["web-checks"]
    context = {"needs.workflow-plan.result": "success"}
    context.update({f"needs.workflow-plan.outputs.{key}": value
                    for key, value in selected.github_outputs().items()})
    assert _condition(job["if"], context)
    step = next(step for step in job["steps"] if step.get("name") == "Verify generated API types")
    assert _condition(step.get("if", "True"), context)
    bindings = {f"${{{{ {key} }}}}": value for key, value in context.items()}
    env = {key: bindings.get(value, value) for key, value in step.get("env", {}).items()}
    npm = tmp_path / "npm"
    npm.write_text('#!/bin/sh\nprintf "%s\\n" "$*" > "$CODEGEN_CALL"\n')
    npm.chmod(0o755)
    recorded = tmp_path / "codegen-call"
    run = subprocess.run(["bash", "-e", "-o", "pipefail", "-c", step["run"]], cwd=tmp_path,
                         capture_output=True, text=True,
                         env={**os.environ, **env, "PATH": f"{tmp_path}:{os.environ['PATH']}",
                              "CODEGEN_CALL": str(recorded)})
    assert run.returncode == 0, run.stderr
    assert recorded.read_text().splitlines() == ["run gen-api -- --check"]


@pytest.mark.parametrize(
    "path,expected",
    [
        ("cmd/loom-build-deadline/main_linux.go", ("./cmd/loom-build-deadline/...",)),
        ("cmd/loom-llm-gateway-sandbox/auth_test.go", ("./cmd/loom-llm-gateway-sandbox/...",)),
        (
            "internal/guestchannel/channel.go",
            (
                "./cmd/loom-guest-runtime/...",
                "./cmd/loom-sandbox-runtime/...",
                "./internal/guestchannel/...",
            ),
        ),
        ("src/loom/auth.py", ("./cmd/...", "./internal/...")),
    ],
)
def test_go_component_and_crosslanguage_consumers(path, expected):
    assert plan(path).go_packages == expected


@pytest.mark.parametrize(
    "path,expected",
    [
        ("modules/execution-target/main.tf", ("modules/execution-target", "stack")),
        ("modules/platform/main.tf", ("modules/platform", "platform")),
        ("modules/regional-execution/main.tf", ("modules/regional-execution", "platform")),
    ],
)
def test_terraform_module_keeps_its_root_consumer(path, expected):
    assert plan("deploy/terraform/nebius/" + path).iac_targets == expected


def test_real_go_workflow_executes_selected_packages_and_preserves_race(tmp_path):
    workflow = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())
    steps = workflow["jobs"]["go-checks"]["steps"]
    log = tmp_path / "commands.jsonl"
    for name in ("go", "gofmt"):
        command = tmp_path / name
        command.write_text(
            "#!/usr/bin/env python3\nimport json,os,sys\n"
            "with open(os.environ['COMMAND_LOG'],'a') as out: out.write(json.dumps([os.path.basename(sys.argv[0]),*sys.argv[1:]])+'\\n')\n"
        )
        command.chmod(0o700)
    packages = plan("cmd/loom-build-deadline/main_linux.go").go_packages
    for name in ("gofmt", "go vet", "go test"):
        step = next(row for row in steps if row.get("name") == name)
        subprocess.run(
            ["bash", "-c", step["run"]],
            cwd=ROOT,
            check=True,
            env={
                **os.environ,
                "PATH": str(tmp_path) + os.pathsep + os.environ["PATH"],
                "RUNNER_TEMP": str(tmp_path),
                "COMMAND_LOG": str(log),
                "GO_PACKAGES_JSON": json.dumps(packages),
            },
            capture_output=True,
        )
    commands = [json.loads(row) for row in log.read_text().splitlines()]
    assert commands == [
        ["gofmt", "-l", "./cmd/loom-build-deadline"],
        ["go", "vet", *packages],
        ["go", "test", "-race", *packages],
    ]


def test_real_terraform_workflow_keeps_alias_mock_test_and_stack_validation(tmp_path):
    workflow = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())
    step = next(
        row
        for row in workflow["jobs"]["nebius-iac"]["steps"]
        if row.get("name") == "Validate pinned Nebius module and stack"
    )
    log = tmp_path / "terraform.jsonl"
    command = tmp_path / "terraform"
    command.write_text(
        "#!/usr/bin/env python3\nimport json,os,sys\n"
        "with open(os.environ['COMMAND_LOG'],'a') as out: out.write(json.dumps(sys.argv[1:])+'\\n')\n"
    )
    command.chmod(0o700)
    targets = plan("deploy/terraform/nebius/modules/execution-target/main.tf").iac_targets
    subprocess.run(
        ["bash", "-c", step["run"]],
        cwd=ROOT,
        check=True,
        env={
            **os.environ,
            "PATH": str(tmp_path) + os.pathsep + os.environ["PATH"],
            "RUNNER_TEMP": str(tmp_path),
            "COMMAND_LOG": str(log),
            "IAC_TARGETS_JSON": json.dumps(targets),
        },
        capture_output=True,
    )
    commands = [json.loads(row) for row in log.read_text().splitlines()]
    assert ["-chdir=deploy/terraform/nebius/modules/execution-target", "test"] in commands
    assert ["-chdir=deploy/terraform/nebius/stack", "validate"] in commands
    assert not any(
        "platform" in arg or "regional-execution" in arg for args in commands for arg in args
    )
    assert ["-chdir=deploy/terraform/nebius/modules/execution-target", "validate"] not in commands


def test_transitive_test_helpers_select_consumers_without_unrelated_modules(tmp_path):
    files = {
        "tests/unit/test_source.py": "def fixture(): pass\n",
        "tests/unit/test_middle.py": "from tests.unit.test_source import fixture\n",
        "tests/unit/test_consumer.py": "MODULE='tests.unit.test_middle'\n",
        "tests/unit/test_unrelated.py": "def test_unrelated(): pass\n",
    }
    for path, source in files.items():
        target = tmp_path / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(source)
    selected = ownership.narrow_test_only_changes(
        tuple(files),
        changed_paths=(next(iter(files)),),
        tracked_paths=tuple(files),
        repo_root=tmp_path,
    )
    assert selected == tuple(files)[:3]


def test_staging_selector_failure_cannot_be_lost_after_partial_output(tmp_path):
    workflow = yaml.safe_load((ROOT / ".github/workflows/staging-smoke.yml").read_text())
    step = next(
        row
        for row in workflow["jobs"]["system-smoke"]["steps"]
        if row.get("name", "").startswith("Pytest")
    )
    command = tmp_path / "uv"
    command.write_text('#!/bin/sh\nprintf "tests/system/partial.py\\n"\nexit 17\n')
    command.chmod(0o700)
    result = subprocess.run(
        ["bash", "-c", step["run"]],
        cwd=ROOT,
        env={
            **os.environ,
            "PATH": str(tmp_path) + os.pathsep + os.environ["PATH"],
            "RUNNER_TEMP": str(tmp_path),
            "TEST_CHANGED_PATHS": "[]",
        },
        capture_output=True,
    )
    assert result.returncode == 17
