from pathlib import Path

import pytest
from scripts.plan_ci_validations import GO_CROSSLANGUAGE_CONTRACTS, HEAVY_CHECKS, plan_validations

REPO_ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize("path", [
    "scripts/check_ci_action_pins.py", "scripts/check_ci_upgrade_policy.py",
    "scripts/check_repository_paths.py",
])
def test_owned_repository_policy_tools_do_not_start_runtime_regressions(path: str) -> None:
    plan = plan_validations(changed_paths=[path], labels=set(), event_name="pull_request")
    assert plan.tests_root
    assert not plan.unowned_runtime
    assert not plan.selected_heavy_checks()
    assert not any((plan.tests_packages, plan.go_checks, plan.runtime_payload,
                    plan.nebius_iac, plan.locked_environments))


def test_go_runtime_has_its_own_toolchain_and_qualification_route() -> None:
    plan = plan_validations(changed_paths=["internal/guestchannel/channel.go"],
                            labels=set(), event_name="pull_request")
    assert plan.go_checks and plan.integration_docker and plan.images
    assert not plan.unowned_runtime
    assert not any((plan.tests_root, plan.tests_packages, plan.runtime_payload,
                    plan.integration, plan.cluster_smoke, plan.staging_smoke, plan.nebius_iac))


def test_go_test_edit_does_not_rebuild_runtime_images() -> None:
    plan = plan_validations(changed_paths=["internal/guestchannel/channel_test.go"],
                            labels=set(), event_name="pull_request")
    assert plan.go_checks
    assert not plan.selected_heavy_checks()


def test_python_runtime_does_not_start_the_unrelated_go_toolchain() -> None:
    plan = plan_validations(changed_paths=["src/loom_service/routes/benchmarks.py"],
                            labels=set(), event_name="pull_request")
    assert plan.tests_root and plan.integration and plan.images and plan.staging_smoke
    assert not plan.go_checks


@pytest.mark.parametrize("path", sorted(GO_CROSSLANGUAGE_CONTRACTS))
def test_python_gateway_contracts_exercise_the_go_verifier(path, independent_test_repo) -> None:
    # Independent JWT test edits must retain Go even after test-owner narrowing.
    independent_test_repo(path)
    plan = plan_validations(changed_paths=[path], labels=set(), event_name="pull_request")
    assert plan.go_checks


def test_unknown_tool_still_keeps_all_validation_routes() -> None:
    plan = plan_validations(changed_paths=["scripts/new_unreviewed_runtime_tool.py"],
                            labels=set(), event_name="pull_request")
    assert plan.unowned_runtime
    assert plan.selected_heavy_checks() == set(HEAVY_CHECKS)
    assert all((plan.tests_root, plan.tests_packages, plan.runtime_payload,
                plan.go_checks, plan.nebius_iac, plan.locked_environments))


@pytest.fixture
def independent_test_repo(tmp_path, monkeypatch):
    """Exercise owner routing separately from shared-fixture expansion."""
    import scripts.plan_ci_validations as planner

    def prepare(path):
        if not path.startswith("tests/"):
            return
        target = tmp_path / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("def test_independent(): pass\n")
        monkeypatch.setattr(planner, "REPO_ROOT", tmp_path)
        monkeypatch.setattr(planner, "_tracked_paths", lambda _: (path, "README.md"))

    return prepare


@pytest.mark.parametrize("path", ["web/src/App.tsx", "web/src/auth/AuthContext.tsx", "deploy/Dockerfile.web"])
def test_frontend_changes_do_not_select_unrelated_python_integration(path: str) -> None:
    plan = plan_validations(changed_paths=[path], labels=set(), event_name="pull_request")
    assert plan.web_checks and plan.images
    assert not plan.staging_smoke
    assert not plan.integration
    assert not plan.integration_docker


@pytest.mark.parametrize("path", [
    "src/loom_task_image_authority/execution_store.py",
    "src/loom/task_image_materialization.py",
    "src/loom/db/schema.py",
    "src/loom/security/secret_store.py",
    "database/migrations/versions/new_task_image_schema.py",
    "tests/support/minio_images.py",
])
def test_image_materialization_and_fixture_dependencies_keep_heavy_coverage(path):
    plan = plan_validations(changed_paths=[path], labels=set(), event_name="pull_request")
    assert plan.integration_docker is True


@pytest.mark.parametrize(
    ("action", "action_label", "base_changed"),
    [
        ("opened", "", False),
        ("ready_for_review", "", False),
        ("reopened", "", False),
        ("synchronize", "", False),
        ("edited", "", True),
        ("labeled", "ci:integration", False),
        ("unlabeled", "ci:integration", False),
        ("unexpected-action", "", False),
    ],
)
def test_authoritative_non_draft_pr_event_runs_full_protected_gate(
    action: str,
    action_label: str,
    base_changed: bool,
) -> None:
    plan = plan_validations(
        changed_paths=["src/loom/config.py"],
        labels=set(),
        event_name="pull_request",
        pull_request_action=action,
        pull_request_action_label=action_label,
        pull_request_base_changed=base_changed,
    )

    assert plan.event_relevant is True
    assert plan.full_gate is True
    assert plan.gate_mode == "full"


@pytest.mark.parametrize(
    ("action", "action_label"),
    [
        ("edited", ""),
        ("labeled", "triage"),
        ("unlabeled", "priority:P1"),
        ("labeled", ""),
    ],
)
@pytest.mark.parametrize(
    "changed_paths",
    [
        ["docs/user-guide.md"],
        ["tests/ops/test_plan_ci_validations.py"],
        ["src/loom/config.py"],
        ["unknown-runtime/new-component.bin"],
    ],
)
def test_metadata_event_preserves_current_head_validation(
    action: str,
    action_label: str,
    changed_paths: list[str],
) -> None:
    plan = plan_validations(
        changed_paths=changed_paths,
        labels={"ci:images"},
        event_name="pull_request",
        pull_request_action=action,
        pull_request_action_label=action_label,
        pull_request_base_changed=False,
    )

    current_head_plan = plan_validations(
        changed_paths=changed_paths,
        labels={"ci:images"},
        event_name="pull_request",
        pull_request_action="synchronize",
    )

    # A newer metadata suite must report the same protected gates and selected
    # work as the successful head suite it supersedes, including unknown paths.
    assert plan == current_head_plan
    assert plan.event_relevant is True
    assert plan.full_gate is True
    assert plan.gate_mode == "full"


@pytest.mark.parametrize(
    "label",
    [
        "ci:integration",
        "ci:integration-docker",
        "ci:images",
        "cluster-smoke",
        "staging-smoke",
        "ci:coverage-summary",
    ],
)
@pytest.mark.parametrize("action", ["labeled", "unlabeled"])
def test_supported_selector_metadata_event_runs_full_gate(action: str, label: str) -> None:
    plan = plan_validations(
        changed_paths=["docs/user-guide.md"],
        labels=set(),
        event_name="pull_request",
        pull_request_action=action,
        pull_request_action_label=label,
    )

    assert plan.event_relevant is True
    assert plan.full_gate is True
    assert plan.gate_mode == "full"


def test_draft_pr_is_filtered() -> None:
    plan = plan_validations(
        changed_paths=["src/loom/config.py"],
        labels=set(),
        event_name="pull_request",
        pull_request_action="synchronize",
        pull_request_draft=True,
    )

    assert plan.event_relevant is False
    assert plan.full_gate is False
    assert plan.gate_mode == "filtered"


def test_converting_to_draft_filters_gate_until_ready_again() -> None:
    plan = plan_validations(
        changed_paths=["src/loom/config.py"],
        labels=set(),
        event_name="pull_request",
        pull_request_action="converted_to_draft",
        # The action itself must remain filtered even if a synthetic or
        # replayed payload has not yet reflected the new draft state.
        pull_request_draft=False,
    )

    assert plan.event_relevant is False
    assert plan.full_gate is False
    assert plan.gate_mode == "filtered"


def test_docs_only_selects_no_heavy_validation() -> None:
    plan = plan_validations(
        changed_paths=["docs/user-guide.md", "CONTRIBUTING.md"],
        labels=set(),
        event_name="pull_request",
    )
    assert plan.docs_only is True
    assert plan.selected_heavy_checks() == set()


@pytest.mark.parametrize(
    "path",
    [
        "deploy/Dockerfile.behavior-stage1-sim",
        "src/loom/integrations/behavior/contracts.py",
        "src/loom_service/behavior_pipeline_adapter.py",
        "tests/integrations/behavior/test_contracts.py",
        "third_party/behavior-stage1/omnigibson/omnigibson/__init__.py",
        "web/src/components/artifacts/BehaviorRolloutViewer.tsx",
        "web/src/components/artifacts/useBoundedJson.ts",
    ],
)
def test_disabled_behavior_paths_select_no_heavy_validation(path: str) -> None:
    plan = plan_validations(
        changed_paths=[path],
        labels=set(),
        event_name="pull_request",
    )

    assert plan.unowned_runtime is False
    assert plan.selected_heavy_checks() == set()


def test_labels_add_corresponding_checks() -> None:
    plan = plan_validations(
        changed_paths=["docs/user-guide.md"],
        labels={
            "ci:integration",
            "ci:integration-docker",
            "ci:images",
            "cluster-smoke",
            "staging-smoke",
            "ci:coverage-summary",
        },
        event_name="pull_request",
    )
    assert plan.integration is True
    assert plan.integration_docker is True
    assert plan.images is True
    assert plan.cluster_smoke is True
    assert plan.staging_smoke is True
    assert plan.coverage_summary is True


def test_coverage_summary_implies_integration() -> None:
    plan = plan_validations(
        changed_paths=["docs/user-guide.md"],
        labels={"ci:coverage-summary"},
        event_name="pull_request",
    )
    assert plan.coverage_summary is True
    assert plan.integration is True


def test_ci_internal_pr_labels_disable_docs_only_fast_path() -> None:
    for label in ("ci:integration", "ci:coverage-summary"):
        plan = plan_validations(
            changed_paths=["docs/user-guide.md"],
            labels={label},
            event_name="pull_request",
        )

        assert plan.docs_only is False
        assert plan.integration is True
        assert plan.coverage_summary is (label == "ci:coverage-summary")


def test_integration_selection_does_not_enable_optional_coverage() -> None:
    labeled_plan = plan_validations(
        changed_paths=["docs/user-guide.md"],
        labels={"ci:integration"},
        event_name="pull_request",
    )
    inferred_plan = plan_validations(
        changed_paths=["src/loom/config.py"],
        labels=set(),
        event_name="pull_request",
    )

    assert labeled_plan.integration is True
    assert labeled_plan.coverage_summary is False
    assert inferred_plan.integration is True
    assert inferred_plan.coverage_summary is False


def test_workflow_dispatch_integration_does_not_select_pr_coverage_summary() -> None:
    plan = plan_validations(
        changed_paths=["docs/user-guide.md"],
        labels={"ci:integration"},
        event_name="workflow_dispatch",
    )

    assert plan.integration is True
    assert plan.coverage_summary is False


def test_worker_driver_change_selects_all_runtime_gates() -> None:
    plan = plan_validations(
        changed_paths=["src/loom/driver/docker.py"],
        labels=set(),
        event_name="pull_request",
    )
    assert plan.integration is True
    assert plan.integration_docker is True
    assert plan.images is True
    assert plan.staging_smoke is True


def test_cluster_template_change_selects_cluster_and_staging() -> None:
    plan = plan_validations(
        changed_paths=["src/loom_cli/templates/k8s/worker.yaml.j2"],
        labels=set(),
        event_name="pull_request",
    )
    assert plan.cluster_smoke is True
    assert plan.staging_smoke is True
    assert plan.images is False


@pytest.mark.parametrize(
    "path",
    [
        "deploy/terraform/nebius/stack/main.tf",
        "scripts/check_nebius_iac.py",
        "tests/ops/test_nebius_iac.py",
    ],
)
def test_nebius_iac_change_uses_owned_validation_route(path: str, independent_test_repo) -> None:
    independent_test_repo(path)
    plan = plan_validations(
        changed_paths=[path],
        labels=set(),
        event_name="pull_request",
    )

    assert plan.integration is True
    assert plan.unowned_runtime is False
    assert plan.selected_heavy_checks() == {"integration"}


@pytest.mark.parametrize(
    "path",
    [
        ".github/workflows/release-promotion-gate.yml",
    ],
)
def test_release_contract_selects_cluster_and_staging(path: str) -> None:
    plan = plan_validations(
        changed_paths=[path],
        labels=set(),
        event_name="pull_request",
    )

    assert plan.cluster_smoke is True
    assert plan.staging_smoke is True


@pytest.mark.parametrize(
    "path",
    [
        ".github/workflows/staging-smoke.yml",
        "deploy/Dockerfile.web",
        "deploy/nginx-spa.conf",
        "deploy/nginx-spa-security-headers.conf",
        "deploy/web-runtime-config.sh",
        "src/loom_cli/templates/k8s/ingress.yaml.j2",
        "scripts/ops/frontend_security_headers.py",
        "scripts/ops/frontend_route_smoke.py",
        "tests/ops/test_frontend_security_headers.py",
        "web/package-lock.json",
        "web/package.json",
        "web/scripts/frontend-route-browser-smoke.mjs",
        "web/scripts/frontend-route-browser-smoke.test.mjs",
        "web/scripts/admin-browser-smoke.mjs",
        "web/scripts/admin-browser-smoke.test.mjs",
        "web/src/main.tsx",
    ],
)
def test_backend_smoke_routes_do_not_confuse_browser_inputs_with_compose(path: str) -> None:
    plan = plan_validations(
        changed_paths=[path],
        labels=set(),
        event_name="pull_request",
    )

    frontend = path.startswith("web/") or path in {
        "deploy/Dockerfile.web", "deploy/nginx-spa.conf",
        "deploy/nginx-spa-security-headers.conf", "deploy/web-runtime-config.sh",
    }
    assert plan.staging_smoke is not frontend
    if frontend:
        assert plan.web_checks


@pytest.mark.parametrize(
    "path",
    [
        "web/src/App.tsx",
        "web/src/api/schema.d.ts",
        "web/e2e/routes.spec.ts",
        "web/playwright.config.ts",
        "deploy/Dockerfile.web",
        "deploy/nginx-spa.conf",
        "deploy/nginx-spa-security-headers.conf",
        "deploy/web-runtime-config.sh",
        ".github/workflows/ci.yml",
        "config/component-ownership.toml",
        "scripts/component_ownership.py",
    ],
)
def test_frontend_quality_contract_selects_web_checks(path: str) -> None:
    plan = plan_validations(changed_paths=[path], labels=set(), event_name="pull_request")

    assert plan.web_checks is True
    assert f"path:{path}" in plan.reasons["web_checks"]


@pytest.mark.parametrize(
    "path",
    [
        "web/src/__tests__/AuthContext.test.tsx",
        "web/src/auth/AuthContext.tsx",
    ],
)
def test_browser_auth_contract_is_covered_by_browser_job(path: str) -> None:
    plan = plan_validations(
        changed_paths=[path],
        labels=set(),
        event_name="pull_request",
    )

    assert plan.web_checks
    assert not plan.cluster_smoke
    assert not plan.staging_smoke


def test_planner_change_selects_every_heavy_gate() -> None:
    plan = plan_validations(
        changed_paths=["scripts/plan_ci_validations.py"],
        labels=set(),
        event_name="pull_request",
    )
    assert plan.selected_heavy_checks() == {
        "integration",
        "integration_docker",
        "images",
        "cluster_smoke",
        "staging_smoke",
    }


@pytest.mark.parametrize(
    "path",
    [
        "tests/unit/test_metrics_enumeration.py",
        "tests/loom_cli/test_config.py",
        "tests/ops/test_release_runbook.py",
    ],
)
def test_manifest_owned_root_tests_do_not_select_unrelated_heavy_lanes(path: str, independent_test_repo) -> None:
    independent_test_repo(path)
    plan = plan_validations(changed_paths=[path], labels=set(), event_name="pull_request")

    assert plan.docs_only is False
    assert plan.unowned_runtime is False
    assert plan.selected_heavy_checks() == set()


def test_docs_plus_manifest_owned_root_test_keeps_only_the_root_test_lane(independent_test_repo) -> None:
    independent_test_repo("tests/unit/test_metrics_enumeration.py")
    plan = plan_validations(
        changed_paths=["docs/user-guide.md", "tests/unit/test_metrics_enumeration.py"],
        labels=set(),
        event_name="pull_request",
    )

    assert plan.docs_only is False
    assert plan.unowned_runtime is False
    assert plan.selected_heavy_checks() == set()


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        ("tests/contract/test_driver_contract.py", {"integration"}),
        (
            "tests/integration/test_docker_driver_exec.py",
            {"integration", "integration_docker"},
        ),
        ("tests/cluster/test_staging_k3s_render_contract.py", {"cluster_smoke"}),
        ("tests/system/test_full_stack_hello.py", {"staging_smoke"}),
    ],
)
def test_manifest_owned_heavy_tests_select_their_required_lanes(
    path: str,
    expected: set[str],
    independent_test_repo,
) -> None:
    independent_test_repo(path)
    plan = plan_validations(changed_paths=[path], labels=set(), event_name="pull_request")

    assert plan.unowned_runtime is False
    assert plan.selected_heavy_checks() == expected


@pytest.mark.parametrize(
    "path",
    [
        "config/component-ownership.toml",
        "scripts/component_ownership.py",
        "tests/ops/test_component_ownership_manifest.py",
    ],
)
def test_ownership_authority_change_selects_every_heavy_gate(path: str) -> None:
    plan = plan_validations(
        changed_paths=[path],
        labels=set(),
        event_name="pull_request",
    )

    assert plan.unowned_runtime is False
    assert plan.selected_heavy_checks() == set(HEAVY_CHECKS)
    assert all("ownership-authority-change" in plan.reasons[name] for name in HEAVY_CHECKS)


@pytest.mark.parametrize(
    "path",
    [
        "config/uv-toolchain.toml",
        "pyproject.toml",
        "uv.lock",
        "packages/loom-launcher/pyproject.toml",
        "packages/loom-benchmarks/pyproject.toml",
        "packages/loom-benchmark-terminal-bench-2/pyproject.toml",
    ],
)
def test_dependency_authority_changes_select_every_heavy_gate(path: str) -> None:
    plan = plan_validations(changed_paths=[path], labels=set(), event_name="pull_request")

    assert plan.unowned_runtime is False
    assert plan.selected_heavy_checks() == set(HEAVY_CHECKS)
    expected_reason = f"dependency-authority:{path}"
    assert all(expected_reason in plan.reasons[check] for check in HEAVY_CHECKS)


@pytest.mark.parametrize(
    "path",
    [
        "scripts/ops/release_gate.py",
        "scripts/ops/release_identity.py",
        "scripts/ops/verify_production_release_gate.sh",
        ".github/workflows/release-promotion-gate.yml",
        "tests/loom_cli/test_cluster_target_boundary.py",
        "tests/ops/test_release_identity.py",
        "tests/ops/test_release_promotion_gate.py",
        "tests/loom_cli/test_cluster_render.py",
    ],
)
def test_protected_deployment_paths_select_every_heavy_gate(path: str) -> None:
    plan = plan_validations(changed_paths=[path], labels=set(), event_name="pull_request")

    assert plan.unowned_runtime is False
    assert plan.selected_heavy_checks() == set(HEAVY_CHECKS)
    assert all("protected-deployment" in plan.reasons[check] for check in HEAVY_CHECKS)


@pytest.mark.parametrize(
    "path",
    [
        "deploy/catalog/unowned-fixture/tasks/hello-world/instruction.md",
        "deploy/catalog/unowned-fixture/tasks/oracle-hello-world/instruction.md",
        "unowned-runtime/new-input-two.bin",
        "unowned-runtime/new-input.bin",
    ],
)
def test_runtime_inputs_fail_safe_to_every_heavy_check(path: str) -> None:
    plan = plan_validations(changed_paths=[path], labels=set(), event_name="pull_request")

    assert plan.docs_only is False
    assert plan.unowned_runtime is True
    assert plan.selected_heavy_checks() == set(HEAVY_CHECKS)
    expected_reason = f"unowned-runtime-path:{path}"
    assert all(expected_reason in plan.reasons[check] for check in HEAVY_CHECKS)


def test_mixed_known_and_unowned_paths_preserve_unowned_runtime_signal() -> None:
    plan = plan_validations(
        changed_paths=["web/src/App.tsx", "unowned-runtime/new-input.bin"],
        labels=set(),
        event_name="pull_request",
    )

    assert plan.unowned_runtime is True
    assert plan.selected_heavy_checks() == set(HEAVY_CHECKS)


def test_codeowners_is_not_static_documentation() -> None:
    plan = plan_validations(
        changed_paths=[".github/CODEOWNERS"],
        labels=set(),
        event_name="pull_request",
    )

    assert plan.docs_only is False
    assert plan.selected_heavy_checks() == set(HEAVY_CHECKS)


def test_migration_change_selects_both_integration_tiers_images_and_staging() -> None:
    plan = plan_validations(
        changed_paths=["database/migrations/versions/1234_add_runtime_state.py"],
        labels=set(),
        event_name="pull_request",
    )

    assert plan.selected_heavy_checks() == {
        "integration",
        "integration_docker",
        "images",
        "staging_smoke",
    }


def test_capacity_guard_migration_change_selects_owned_integration_lane() -> None:
    plan = plan_validations(
        changed_paths=[
            "database/capacity_guard_migrations/versions/guard_0002_example.py",
        ],
        labels=set(),
        event_name="pull_request",
    )

    assert plan.integration is True
    assert plan.unowned_runtime is False


def test_unknown_retired_source_path_selects_integration_lane() -> None:
    plan = plan_validations(
        changed_paths=["src/unowned_retired_backend/store.py"],
        labels=set(),
        event_name="pull_request",
    )

    assert plan.integration is True


def test_every_docker_marked_integration_module_selects_docker() -> None:
    integration_dir = REPO_ROOT / "tests" / "integration"
    docker_marked_modules = [
        path
        for path in sorted(integration_dir.rglob("*.py"))
        if "pytest.mark.docker" in path.read_text(encoding="utf-8")
    ]
    assert docker_marked_modules

    for module in docker_marked_modules:
        relative_path = module.relative_to(REPO_ROOT).as_posix()
        plan = plan_validations(
            changed_paths=[relative_path],
            labels=set(),
            event_name="pull_request",
        )
        assert plan.integration_docker is True, relative_path


def test_docs_only_merge_group_skips_heavy_lanes() -> None:
    plan = plan_validations(
        changed_paths=["docs/merge-queue.md"],
        labels=set(),
        event_name="merge_group",
    )

    assert plan.event_relevant is True
    assert plan.full_gate is True
    assert plan.gate_mode == "full"
    assert plan.docs_only is True
    assert plan.selected_heavy_checks() == set()
    assert plan.web_checks is False


def test_merge_group_uses_combined_diff_ownership() -> None:
    plan = plan_validations(
        changed_paths=["src/loom/config.py"],
        labels=set(),
        event_name="merge_group",
    )

    assert plan.selected_heavy_checks() == {"integration", "images"}
    assert plan.web_checks is False


def test_unowned_merge_group_runtime_path_remains_fail_closed() -> None:
    path = "unowned-runtime/merge-queue-input.bin"
    plan = plan_validations(
        changed_paths=[path],
        labels=set(),
        event_name="merge_group",
    )

    assert plan.unowned_runtime is True
    assert plan.selected_heavy_checks() == set(HEAVY_CHECKS)
    expected_reason = f"unowned-runtime-path:{path}"
    assert all(expected_reason in plan.reasons[check] for check in HEAVY_CHECKS)


@pytest.mark.parametrize(("path", "checks"), [
    ("scripts/ops/deploy_nebius_platform.py", {"integration", "cluster_smoke"}),
    ("scripts/ops/render_nebius_platform.py", {"integration", "cluster_smoke"}),
    ("scripts/ops/verify_nebius_restore.py", {"integration"}),
    ("deploy/nebius/integration.platform.json.example", {"integration", "cluster_smoke"}),
    ("src/loom/nebius_platform_render.py", {"integration", "images", "cluster_smoke"}),
    ("tests/unit/test_nebius_platform_render.py", {"cluster_smoke"}),
])
def test_nebius_operator_paths_select_their_consumers(path, checks, independent_test_repo):
    independent_test_repo(path)
    plan = plan_validations(changed_paths=[path], labels=set(), event_name="pull_request")
    assert not plan.docs_only
    assert not plan.unowned_runtime
    assert plan.selected_heavy_checks() == checks


@pytest.mark.parametrize("path", [
    ".github/workflows/nebius-candidate.yml",
    "scripts/ops/nebius_candidate.py",
    "scripts/ops/nebius_registry_auth.py",
    "scripts/validate_trivy_release_report.py",
    "scripts/ops/new_nebius_operator.py",
])
def test_nebius_publication_authority_and_unknown_paths_keep_full_validation(path):
    plan = plan_validations(changed_paths=[path], labels=set(), event_name="pull_request")
    assert plan.selected_heavy_checks() == set(HEAVY_CHECKS)


def test_nebius_scoped_routing_cannot_remove_mixed_path_or_label_requirements():
    plan = plan_validations(
        changed_paths=["scripts/ops/verify_nebius_restore.py", "database/migrations/new_revision.py"],
        labels={"cluster-smoke"}, event_name="pull_request",
    )
    assert plan.selected_heavy_checks() == set(HEAVY_CHECKS)


@pytest.mark.parametrize("path", ["src/loom_service/routes/benchmarks.py", "src/loom_service/wire_responses.py", "scripts/export_openapi.py"])
def test_api_contract_changes_select_frontend_codegen_check(path):
    plan = plan_validations(changed_paths=[path], labels=set(), event_name="pull_request")
    assert plan.web_checks
