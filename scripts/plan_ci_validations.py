from __future__ import annotations

import argparse
import json
import os
from collections.abc import Collection, Sequence
from dataclasses import asdict, dataclass, replace
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING or __package__:
    from scripts.component_ownership import (
        Manifest,
        _tracked_paths,
        load_manifest,
        narrow_test_only_changes,
        select_release_image_matrix,
        test_shard_matrix,
    )
else:
    from component_ownership import (
        Manifest,
        _tracked_paths,
        load_manifest,
        narrow_test_only_changes,
        select_release_image_matrix,
        test_shard_matrix,
    )

HEAVY_CHECKS = (
    "integration",
    "integration_docker",
    "images",
    "cluster_smoke",
    "staging_smoke",
)

BASELINE_CHECKS = ("tests_root", "tests_packages", "go_checks", "runtime_payload", "nebius_iac", "locked_environments")

SUPPORTED_EVENTS = {"merge_group", "pull_request", "push", "workflow_dispatch", "schedule"}

LABEL_TO_CHECK = {
    "ci:integration": "integration",
    "ci:integration-docker": "integration_docker",
    "ci:images": "images",
    "cluster-smoke": "cluster_smoke",
    "staging-smoke": "staging_smoke",
    "ci:coverage-summary": "coverage_summary",
}

DOC_METADATA_PATHS = {
    ".github/PULL_REQUEST_TEMPLATE.md",
    ".editorconfig",
    ".gitignore",
    "CONTRIBUTING.md",
    "LICENSE",
    "README.md",
    "SECURITY.md",
}

DOCS_STATIC_SUFFIXES = {
    ".csv",
    ".gif",
    ".jpeg",
    ".jpg",
    ".json",
    ".jsonl",
    ".md",
    ".mdx",
    ".png",
    ".rst",
    ".svg",
    ".txt",
}

PLANNER_PATHS = {
    "scripts/plan_ci_validations.py",
    "tests/ops/test_plan_ci_validations.py",
}

OWNERSHIP_AUTHORITY_PATHS = {
    "config/component-ownership.toml",
    "scripts/component_ownership.py",
    "tests/ops/test_component_ownership_manifest.py",
}

# These tools validate repository policy, not an application runtime. Their
# focused root tests and lint execute the changed tool; unknown tools still
# retain the stronger fallback below.
REPOSITORY_POLICY_TOOLS = {
    "scripts/check_ci_action_pins.py",
    "scripts/check_ci_upgrade_policy.py",
    "scripts/check_repository_paths.py",
}
GO_INPUT_PREFIXES = (
    "cmd/loom-build-deadline/", "cmd/loom-llm-gateway-sandbox/",
    "cmd/loom-sandbox-runtime/", "cmd/loom-guest-runtime/",
    "cmd/loom-execution-runtime/", "internal/guestchannel/",
)
# The Go gateway verifies the same JWT and materialization contracts as these
# Python producers. Exercise its independent implementation when they change.
GO_CROSSLANGUAGE_CONTRACTS = {
    "src/loom/auth.py",
    "tests/unit/test_auth.py",
    "tests/unit/test_step_jwt.py",
    "src/loom/execution_runtime_contract.py",
    "src/loom/service_execution_materialization.py",
    "src/loom_llm_gateway/routes/service_execution.py",
}


def _is_go_input(path: str) -> bool:
    return path in {"go.mod", "go.sum"} or (path.startswith(GO_INPUT_PREFIXES) and path.endswith(".go"))

NEBIUS_IAC_EXACT = {
    "scripts/check_nebius_iac.py",
    "tests/ops/test_nebius_iac.py",
}

NEBIUS_IAC_PREFIXES = ("deploy/terraform/nebius/",)

# Operator tools have root and database tests; rendered manifests also need the
# disposable Kubernetes contract. Publication authority remains on full CI.
NEBIUS_PLATFORM_EXACT = {
    "scripts/ops/deploy_nebius_platform.py",
    "scripts/ops/render_nebius_platform.py",
    "scripts/ops/verify_nebius_restore.py",
}
NEBIUS_PLATFORM_PREFIXES = ("deploy/nebius/",)

PROTECTED_DEPLOYMENT_EXACT = {
    ".github/workflows/release-promotion-gate.yml",
    "scripts/ops/release_gate.py",
    "scripts/ops/release_identity.py",
    "scripts/ops/verify_production_release_gate.sh",
    "tests/loom_cli/test_cluster_target_boundary.py",
    "tests/loom_cli/test_cluster_hosted_retirement.py",
    "src/loom_cli/application_migration_contract.py",
    "src/loom_cli/migration_readiness.py",
    "tests/loom_cli/test_migration_readiness.py",
    "tests/loom_cli/test_cluster_render.py",
    "tests/ops/test_release_identity.py",
    "tests/ops/test_release_promotion_gate.py",
}

REPO_ROOT = Path(__file__).resolve().parents[1]
COMPONENT_OWNERSHIP_MANIFEST = REPO_ROOT / "config/component-ownership.toml"


@dataclass(frozen=True)
class ValidationPlan:
    event_relevant: bool
    full_gate: bool
    gate_mode: str
    docs_only: bool
    unowned_runtime: bool
    integration: bool
    integration_docker: bool
    images: bool
    cluster_smoke: bool
    staging_smoke: bool
    coverage_summary: bool
    web_checks: bool
    reasons: dict[str, tuple[str, ...]]
    test_changes: tuple[str, ...] = ()
    tests_root: bool = True
    tests_packages: bool = True
    go_checks: bool = True
    runtime_payload: bool = True
    nebius_iac: bool = True
    locked_environments: bool = True


    def selected_heavy_checks(self) -> set[str]:
        return {name for name in HEAVY_CHECKS if getattr(self, name)}

    def github_outputs(self) -> dict[str, str]:
        outputs = {
            name: str(bool(getattr(self, name))).lower()
            for name in (
                "event_relevant",
                "full_gate",
                "docs_only",
                "unowned_runtime",
                *HEAVY_CHECKS,
                *BASELINE_CHECKS,
                "coverage_summary",
                "web_checks",
            )
        }
        outputs["gate_mode"] = self.gate_mode
        test_changes = json.dumps(self.test_changes, separators=(",", ":"))
        # This optional narrowing hint becomes one job environment variable.
        # Stay below Linux's per-string exec limit; an empty hint retains the
        # complete selected lanes, including historical schema reconstruction.
        outputs["test_changes"] = test_changes if len(test_changes.encode()) <= 65536 else "[]"
        outputs["reasons_json"] = json.dumps(self.reasons, sort_keys=True, separators=(",", ":"))
        return outputs


def _pull_request_gate_mode(
    *,
    action: str,
    action_label: str,
    draft: bool,
    base_changed: bool,
) -> tuple[bool, bool, str]:
    """Return event relevance, full-gate eligibility, and the gate context mode.

    Every non-draft PR event emits the four protected contexts. A newer filtered
    metadata suite can leave native auto-merge blocked despite older successful
    checks on the same head. Only drafts are filtered before checkout; changed
    paths and the current labels still select the actual validation work.
    """

    if draft or action == "converted_to_draft":
        return False, False, "filtered"
    return True, True, "full"


def _is_documentation_path(path: str) -> bool:
    return (
        path in DOC_METADATA_PATHS
        or path.startswith(".github/ISSUE_TEMPLATE/")
        or (path.startswith("docs/") and Path(path).suffix in DOCS_STATIC_SUFFIXES)
    )


def _matches(path: str, *, exact: set[str], prefixes: tuple[str, ...]) -> bool:
    return path in exact or path.startswith(prefixes)


def _is_frontend_input(path: str) -> bool:
    return path.startswith("web/") or path in {
        "deploy/Dockerfile.web", "deploy/nginx-spa.conf",
        "deploy/nginx-spa-security-headers.conf", "deploy/web-runtime-config.sh",
    }


def _is_dependency_authority_path(path: str) -> bool:
    return path in {"config/uv-toolchain.toml", "pyproject.toml", "uv.lock"} or (
        path.startswith("packages/") and path.endswith("/pyproject.toml")
    )


def _is_protected_deployment_path(path: str) -> bool:
    return path in PROTECTED_DEPLOYMENT_EXACT


@lru_cache(maxsize=512)
def _test_owner_lanes(path: str) -> tuple[str, ...]:
    return tuple(owner.lane for owner in _component_ownership_manifest().test_owners_for_path(path))


@lru_cache(maxsize=1)
def _component_ownership_manifest() -> Manifest:
    return load_manifest(COMPONENT_OWNERSHIP_MANIFEST)


def plan_validations(
    *,
    changed_paths: Sequence[str],
    labels: Collection[str],
    event_name: str,
    pull_request_action: str = "opened",
    pull_request_action_label: str = "",
    pull_request_draft: bool = False,
    pull_request_base_changed: bool = False,
) -> ValidationPlan:
    if event_name not in SUPPORTED_EVENTS:
        raise ValueError(f"unsupported event_name: {event_name}")

    event_relevant = True
    full_gate = True
    gate_mode = "full"
    if event_name == "pull_request":
        event_relevant, full_gate, gate_mode = _pull_request_gate_mode(
            action=pull_request_action,
            action_label=pull_request_action_label,
            draft=pull_request_draft,
            base_changed=pull_request_base_changed,
        )

    paths = tuple(dict.fromkeys(path.strip() for path in changed_paths if path.strip()))
    docs_only = bool(paths) and all(_is_documentation_path(path) for path in paths)
    unowned_runtime = False
    selected = {name: False for name in (*HEAVY_CHECKS, "coverage_summary", "web_checks")}
    reasons: dict[str, list[str]] = {name: [] for name in selected}

    def select(name: str, reason: str) -> None:
        selected[name] = True
        reasons[name].append(reason)

    if event_name == "schedule":
        for name in selected:
            select(name, "scheduled-full-regression")

    for label in sorted(labels):
        if check := LABEL_TO_CHECK.get(label):
            select(check, f"label:{label}")

    if any(path in PLANNER_PATHS for path in paths):
        for name in HEAVY_CHECKS:
            select(name, "planner-change")

    if any(path in OWNERSHIP_AUTHORITY_PATHS for path in paths):
        for name in HEAVY_CHECKS:
            select(name, "ownership-authority-change")

    if any(_is_protected_deployment_path(path) for path in paths):
        for name in HEAVY_CHECKS:
            select(name, "protected-deployment")

    integration_exact = {
        ".github/workflows/ci.yml",
        "config/loom-schema.toml",
    }
    integration_prefixes = (
        "database/capacity_guard_migrations/",
        "src/",
        "packages/",
        "database/migrations/",
        "config/",
        "tests/integration/",
        "tests/contract/",
    )
    docker_exact = {
        "src/loom/driver/docker.py",
        ".github/workflows/ci.yml",
        "tests/support/minio_images.py",
    }
    docker_prefixes = (
        "src/loom_worker/",
        "src/loom/sandbox",
        # Image materialization and schema changes require Docker integration.
        "src/loom_task_image_authority/",
        "src/loom_control_plane/task_image_",
        "src/loom/task_image_",
        "src/loom/db/",
        "src/loom/security/",
        "database/migrations/",
        "packages/loom-launcher/",
        "tests/integration/",
    )
    image_exact = {
        ".dockerignore",
        ".github/workflows/images.yml",
        "pyproject.toml",
        "go.mod",
        "go.sum",
        "web/index.html",
        "web/package.json",
        "web/package-lock.json",
        "web/tailwind.config.js",
        "web/vite.config.ts",
        "deploy/nginx-spa.conf",
        "deploy/nginx-spa-security-headers.conf",
        "deploy/web-runtime-config.sh",
    }
    image_prefixes = (
        "deploy/Dockerfile.",
        "src/",
        "packages/",
        "web/src/",
        "cmd/",
        "database/migrations/",
    )
    cluster_exact = {
        ".github/workflows/cluster-smoke.yml",
        "src/loom/nebius_platform_render.py",
        "scripts/ops/render_nebius_platform.py",
        "scripts/ops/deploy_nebius_platform.py",
        "tests/unit/test_nebius_platform_render.py",
        ".github/workflows/release-promotion-gate.yml",
        "src/loom_cli/cluster_cmd.py",
        "src/loom_cli/cluster_config.py",
        "web/src/__tests__/AuthContext.test.tsx",
        "web/src/auth/AuthContext.tsx",
        "config/loom-schema.toml",
    }
    cluster_prefixes = (
        "deploy/nebius/",
        "src/loom_cli/templates/k8s/",
        "deploy/k8s/",
    )
    staging_exact = {
        ".github/workflows/staging-smoke.yml",
        ".github/workflows/release-promotion-gate.yml",
        "pyproject.toml",
        "config/loom-schema.toml",
        "src/loom/driver/docker.py",
        "deploy/Dockerfile.web",
        "deploy/nginx-spa.conf",
        "deploy/nginx-spa-security-headers.conf",
        "deploy/web-runtime-config.sh",
        "scripts/ops/frontend_security_headers.py",
        "scripts/ops/frontend_route_smoke.py",
        "src/loom_cli/templates/k8s/ingress.yaml.j2",
        "tests/ops/test_frontend_security_headers.py",
        "web/package-lock.json",
        "web/package.json",
        "web/scripts/frontend-route-browser-smoke.mjs",
        "web/scripts/frontend-route-browser-smoke.test.mjs",
        "web/src/__tests__/AuthContext.test.tsx",
        "web/src/auth/AuthContext.tsx",
        "web/src/main.tsx",
    }
    staging_prefixes = (
        "deploy/Dockerfile.",
        "src/loom_service/",
        "src/loom_control_plane/",
        "src/loom_llm_gateway/",
        "src/loom_worker/",
        "src/loom_family_orchestrator/",
        "src/loom_cli/templates/k8s/",
        "database/migrations/",
        "packages/",
        "web/",
    )
    web_quality_exact = {
        ".github/workflows/ci.yml",
        "config/component-ownership.toml",
        "deploy/Dockerfile.web",
        "deploy/nginx-spa.conf",
        "deploy/nginx-spa-security-headers.conf",
        "deploy/web-runtime-config.sh",
        "scripts/component_ownership.py",
        "scripts/export_openapi.py",
    }

    for path in paths:
        if _component_ownership_manifest().ci_ignores_path(path):
            continue
        if _is_documentation_path(path):
            continue
        test_owner_lanes = _test_owner_lanes(path)
        policy_tool = path in REPOSITORY_POLICY_TOOLS
        go_input = _is_go_input(path)
        matched_owner = (
            path in PLANNER_PATHS
            or path in OWNERSHIP_AUTHORITY_PATHS
            or _matches(path, exact=NEBIUS_IAC_EXACT, prefixes=NEBIUS_IAC_PREFIXES)
            or _matches(path, exact=NEBIUS_PLATFORM_EXACT, prefixes=NEBIUS_PLATFORM_PREFIXES)
            or _is_protected_deployment_path(path)
            or bool(test_owner_lanes)
            or policy_tool or go_input
        )
        for lane in test_owner_lanes:
            reason = f"test-owner:{lane}:{path}"
            if lane == "integration":
                select("integration", reason)
            elif lane == "integration-docker":
                # Retain the existing tests/integration contract: changes to a
                # Docker-owned module exercise both integration tiers.
                select("integration", reason)
                select("integration_docker", reason)
            elif lane == "cluster-smoke":
                select("cluster_smoke", reason)
            elif lane == "system-smoke":
                select("staging_smoke", reason)
        if _matches(path, exact=NEBIUS_IAC_EXACT, prefixes=NEBIUS_IAC_PREFIXES):
            select("integration", f"nebius-iac:{path}")
            matched_owner = True
        if _is_dependency_authority_path(path):
            for name in HEAVY_CHECKS:
                select(name, f"dependency-authority:{path}")
            matched_owner = True
        if go_input and not path.endswith("_test.go"):
            select("integration_docker", f"go-runtime:{path}")
            if select_release_image_matrix(_component_ownership_manifest(), changed_paths=(path,),
                                           force_all=False, image_set="nebius"):
                select("images", f"go-runtime:{path}")
        if not (policy_tool or go_input) and (path.startswith("tests/contract/") or (not test_owner_lanes and _matches(path, exact=integration_exact, prefixes=integration_prefixes))):
            select("integration", f"path:{path}")
            matched_owner = True
        elif not test_owner_lanes and not _is_frontend_input(path) and not (policy_tool or go_input):
            # Frontend inputs already select browser and image contracts below.
            # They do not change the Python runtime exercised by this lane.
            select("integration", f"non-doc-path:{path}")
        if path in docker_exact or (not test_owner_lanes and _matches(path, exact=set(), prefixes=docker_prefixes)):
            select("integration_docker", f"path:{path}")
            matched_owner = True
        image_match = _matches(
            path,
            exact=image_exact,
            prefixes=image_prefixes,
        ) and not path.startswith("src/loom_cli/templates/k8s/")
        if image_match and not (go_input and path.endswith("_test.go")):
            select("images", f"path:{path}")
            matched_owner = True
        if not _is_frontend_input(path) and _matches(path, exact=cluster_exact, prefixes=cluster_prefixes):
            select("cluster_smoke", f"path:{path}")
            matched_owner = True
        if not _is_frontend_input(path) and _matches(path, exact=staging_exact, prefixes=staging_prefixes):
            select("staging_smoke", f"path:{path}")
            matched_owner = True
        if path.startswith(("web/", "src/loom_service/")) or path in web_quality_exact:
            select("web_checks", f"path:{path}")
            matched_owner = True
        if not matched_owner:
            unowned_runtime = True
            reason = f"unowned-runtime-path:{path}"
            for name in HEAVY_CHECKS:
                select(name, reason)

    # Ownership globs include fixture libraries, not just executable tests.
    # Reuse the same independence check as test-paths before omitting consumer
    # jobs; retaining every file inside a skipped job would not provide coverage.
    # Inspect the test subset even in a mixed runtime/test diff.
    test_inputs = tuple(path for path in paths
                        if (path.startswith("tests/") or (path.startswith("packages/") and "/tests/" in path))
                        and not _is_documentation_path(path)
                        and not _component_ownership_manifest().ci_ignores_path(path))
    independent_tests: tuple[str, ...] | None = None
    if test_inputs:
        tracked = _tracked_paths(REPO_ROOT)
        independent = narrow_test_only_changes(
            tracked, changed_paths=test_inputs, tracked_paths=tracked, repo_root=REPO_ROOT,
        )
        if independent != tracked:
            independent_tests = independent
        else:
            for name in ("integration", "integration_docker", "cluster_smoke", "staging_smoke"):
                select(name, "shared-or-unknown-test-input")

    if selected["coverage_summary"]:
        select("integration", "coverage-summary-requires-integration")

    if any(selected[name] for name in ("integration", "integration_docker", "coverage_summary")):
        docs_only = False

    runtime_paths = tuple(path for path in paths if not _is_documentation_path(path)
                          and not _component_ownership_manifest().ci_ignores_path(path))
    force_baseline = (
        not paths or event_name in {"workflow_dispatch", "schedule"} or unowned_runtime
        or selected["coverage_summary"]
        or any(path in PLANNER_PATHS | OWNERSHIP_AUTHORITY_PATHS
               or path == ".github/workflows/ci.yml"
               or _is_dependency_authority_path(path) for path in paths)
    )
    backend_paths = tuple(path for path in runtime_paths
                          if not _is_frontend_input(path))
    baseline = {name: bool(backend_paths) for name in BASELINE_CHECKS}
    python_paths = tuple(path for path in backend_paths
                         if not _is_go_input(path) and path not in REPOSITORY_POLICY_TOOLS)
    baseline["tests_root"] = bool(python_paths) or any(path in REPOSITORY_POLICY_TOOLS for path in backend_paths)
    baseline["tests_packages"] = bool(python_paths)
    baseline["runtime_payload"] = bool(python_paths)
    baseline["go_checks"] = any(_is_go_input(path) or path in GO_CROSSLANGUAGE_CONTRACTS
                                for path in backend_paths)
    # Every Python validation job already syncs/checks the locked workspace.
    # The standalone install lane is for dependency/CI authority changes and
    # full regression, handled by force_baseline below.
    baseline["locked_environments"] = False
    baseline["nebius_iac"] = any(
        _matches(path, exact=NEBIUS_IAC_EXACT, prefixes=NEBIUS_IAC_PREFIXES)
        for path in runtime_paths
    )
    # Only independently editable test modules may suppress other owning jobs.
    if (runtime_paths == test_inputs and independent_tests is not None
            and not force_baseline and not set(labels) & LABEL_TO_CHECK.keys()):
        owners = {lane for path in independent_tests for lane in _test_owner_lanes(path)}
        crosslanguage_contract = any(path in GO_CROSSLANGUAGE_CONTRACTS for path in independent_tests)
        baseline.update(tests_root="tests-root" in owners,
                        tests_packages="tests-packages" in owners,
                        go_checks="go-checks" in owners or crosslanguage_contract,
                        runtime_payload="runtime-payload" in owners,
                        locked_environments=False)
    if force_baseline:
        baseline = dict.fromkeys(BASELINE_CHECKS, True)

    return ValidationPlan(
        event_relevant=event_relevant,
        full_gate=full_gate,
        gate_mode=gate_mode,
        docs_only=docs_only,
        unowned_runtime=unowned_runtime,
        integration=selected["integration"],
        integration_docker=selected["integration_docker"],
        images=selected["images"],
        cluster_smoke=selected["cluster_smoke"],
        staging_smoke=selected["staging_smoke"],
        coverage_summary=selected["coverage_summary"],
        web_checks=selected["web_checks"],
        reasons={name: tuple(values) for name, values in reasons.items()},
        # Explicit selector labels and manual runs retain full regression.
        test_changes=paths if event_name in {"pull_request", "merge_group"} and not set(labels) & LABEL_TO_CHECK.keys() else (),
        **baseline,
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--changed-files", type=Path, required=True)
    parser.add_argument("--labels-json", default="[]")
    parser.add_argument("--event-name", required=True)
    parser.add_argument("--pull-request-action", default="")
    parser.add_argument("--pull-request-action-label", default="")
    parser.add_argument(
        "--pull-request-draft",
        choices=("true", "false"),
        default="false",
    )
    parser.add_argument(
        "--pull-request-base-changed",
        choices=("true", "false"),
        default="false",
    )
    parser.add_argument("--github-output", type=Path, required=True)
    parser.add_argument("--test-scope", choices=("all", "nebius"), default=os.environ.get("CI_TEST_SCOPE", "all"))
    args = parser.parse_args()
    labels = json.loads(args.labels_json or "[]")
    if not isinstance(labels, list) or not all(isinstance(label, str) for label in labels):
        raise SystemExit("--labels-json must be a JSON array of strings")
    plan = plan_validations(
        changed_paths=args.changed_files.read_text(encoding="utf-8").splitlines(),
        labels=set(labels),
        event_name=args.event_name,
        pull_request_action=args.pull_request_action,
        pull_request_action_label=args.pull_request_action_label,
        pull_request_draft=args.pull_request_draft == "true",
        pull_request_base_changed=args.pull_request_base_changed == "true",
    )
    outputs = plan.github_outputs()
    effective_changes = tuple(json.loads(outputs["test_changes"]))
    tracked_paths = _tracked_paths(REPO_ROOT)
    for lane, selected_name, matrix_name in (
        ("tests-root", "tests_root", "tests_root_matrix"),
        ("integration", "integration", "integration_matrix"),
        ("integration-docker", "integration_docker", "integration_docker_matrix"),
        ("cluster-smoke", "cluster_smoke", "cluster_smoke_matrix"),
    ):
        matrix = test_shard_matrix(
            _component_ownership_manifest(), tracked_paths=tracked_paths, lane=lane,
            repo_root=REPO_ROOT, changed_paths=effective_changes, test_scope=args.test_scope,
        ) if getattr(plan, selected_name) and plan.gate_mode == "full" else ()
        outputs[matrix_name] = json.dumps(matrix, separators=(",", ":"))
        # A compatibility-only edit has no tests in the daily Nebius scope.
        # Jobs and the final gate must agree on that deliberate empty selection.
        outputs[selected_name] = str(bool(matrix)).lower()
        plan = replace(plan, **{selected_name: bool(matrix)})
    with args.github_output.open("a", encoding="utf-8") as handle:
        for name, value in outputs.items():
            handle.write(f"{name}={value}\n")
    print(json.dumps(asdict(plan), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
