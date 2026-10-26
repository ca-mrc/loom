from __future__ import annotations

import argparse
import json
import os
import subprocess
import tomllib
from collections.abc import Collection, Sequence
from dataclasses import asdict, dataclass, replace
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING or __package__:
    from scripts.component_ownership import (
        AFFECTED_ONLY_LANES,
        Manifest,
        _tracked_paths,
        load_manifest,
        narrow_test_only_changes,
        select_release_image_matrix,
        test_shard_matrix,
    )
else:
    from component_ownership import (
        AFFECTED_ONLY_LANES,
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

BASELINE_CHECKS = ("lint_and_static", "tests_root", "tests_packages", "go_checks", "runtime_payload", "nebius_iac", "locked_environments")

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

COMPONENT_OWNERSHIP_PATH = "config/component-ownership.toml"
MANIFEST_VALIDATION_TEST = "tests/ops/test_component_ownership_manifest.py"
# Path lists that may grow by registering files the same change adds, without
# counting as an ownership-authority change. Ignore lists, affected/unaffected
# paths and every other field keep the full regression.
REGISTRATION_FIELDS = frozenset({"include_paths", "exclude_paths", "pins"})
_GLOB_CHARACTERS = frozenset("*?[]{}")

OWNERSHIP_AUTHORITY_PATHS = {
    "config/component-ownership.toml",
    "scripts/component_ownership.py",
    "tests/ops/test_component_ownership_manifest.py",
}

# These tools validate repository policy or read CI evidence. Their
# focused root tests and lint execute the changed tool; unknown tools still
# retain the stronger fallback below.
REPOSITORY_POLICY_TOOLS = {
    "scripts/check_ci_action_pins.py",
    "scripts/check_ci_upgrade_policy.py",
    "scripts/check_repository_paths.py",
    "scripts/check_github_cli_logs.py",
    # Read-only Actions evidence/reporting tools have dedicated root
    # regressions. They do not change the pool lifecycle exercised by the
    # expensive operator lane; unknown CI tools still retain full coverage.
    "scripts/ops/nebius_pool_test_plan.py",
    "scripts/ops/ci_reliability_metrics.py",
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

# These tools are invoked by operators, not imported by application services,
# the virtual runtime payloads or package unit tests. Root regressions own their
# lifecycle/idle/backup contracts; the pool lane independently selects consumers.
# New/unclassified tools still take the unknown-runtime fallback.
OPERATOR_COMPONENT_PREFIXES = (
    "scripts/ops/nebius_pool_", "scripts/ops/nebius_shared_pool_",
    "scripts/ops/nebius_development_", "scripts/ops/nebius_management_",
    "scripts/ops/install_nebius",
)
# This live controller provisions CI workers. Its root contracts and native
# specialist pickups own its behavior; it is not a read-only metadata tool or
# an input to the platform's historical pool transition state machine.
CI_RUNNER_COMPONENTS = {"scripts/ops/ci_runner_supply.py"}
OPERATOR_COMPONENT_EXACT = CI_RUNNER_COMPONENTS | {
    "scripts/ops/nebius_archive_recovery.py",
    "scripts/ops/nebius_terminal_archive_recovery.py",
    "scripts/ops/nebius_terminal_cleanup_recovery.py",
    "scripts/ops/nebius_idle_rollout.py",
}
RUNTIME_PAYLOAD_TOOLS = {
    "scripts/runtime_payload_conformance.py", "scripts/runtime_payload_dispatch.py",
}


def _is_operator_component(path: str) -> bool:
    return path in OPERATOR_COMPONENT_EXACT or path.startswith(OPERATOR_COMPONENT_PREFIXES)


GO_ALL_PACKAGES = ("./cmd/...", "./internal/...")
IAC_ALL_TARGETS = ("modules/execution-target", "stack", "modules/platform",
                   "modules/regional-execution", "platform")


def go_packages_for_changes(paths: tuple[str, ...], *, full: bool) -> tuple[str, ...]:
    if full or any(path in {"go.mod", "go.sum"} | GO_CROSSLANGUAGE_CONTRACTS for path in paths):
        return GO_ALL_PACKAGES
    selected: set[str] = set()
    for path in paths:
        if path.startswith("internal/guestchannel/"):
            selected.update(("./internal/guestchannel/...", "./cmd/loom-guest-runtime/...",
                             "./cmd/loom-sandbox-runtime/..."))
        elif _is_go_input(path):
            selected.add("./" + "/".join(path.split("/")[:2]) + "/...")
    return tuple(sorted(selected)) or GO_ALL_PACKAGES


def iac_targets_for_changes(paths: tuple[str, ...], *, full: bool) -> tuple[str, ...]:
    if full:
        return IAC_ALL_TARGETS
    consumers = {
        "modules/execution-target/": ("modules/execution-target", "stack"),
        "stack/": ("modules/execution-target", "stack"),
        "modules/platform/": ("modules/platform", "platform"),
        "modules/regional-execution/": ("modules/regional-execution", "platform"),
        "platform/": ("modules/platform", "modules/regional-execution", "platform"),
    }
    selected: set[str] = set()
    for path in paths:
        if not path.startswith(NEBIUS_IAC_PREFIXES):
            continue
        relative = path.removeprefix(NEBIUS_IAC_PREFIXES[0])
        matches = [targets for prefix, targets in consumers.items() if relative.startswith(prefix)]
        if not matches:
            return IAC_ALL_TARGETS
        selected.update(matches[0])
    return tuple(target for target in IAC_ALL_TARGETS if target in selected) or IAC_ALL_TARGETS

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


def manifest_change_is_registration(base: object, head: object, added: frozenset[str],
                                    field: str = "") -> bool:
    """Return whether ``head`` only registers files that this change adds.

    New literal paths may join include/exclude lists, and new pins may place
    them. Any removal, reordering, changed value, new key, table or rule, glob
    or pre-existing file is an ownership-authority change.
    """
    def new_file(value: object) -> bool:
        return isinstance(value, str) and not _GLOB_CHARACTERS & set(value) and value in added

    if type(base) is not type(head):
        return False
    if isinstance(base, dict):
        assert isinstance(head, dict)
        return base.keys() == head.keys() and all(
            manifest_change_is_registration(base[key], head[key], added, key) for key in base)
    if not isinstance(base, list):
        return base == head
    assert isinstance(head, list)
    if len(base) == len(head) and all(isinstance(item, dict) for item in (*base, *head)):
        # Tables keep their positions; compare each one field by field.
        return all(manifest_change_is_registration(old, new, added, field)
                   for old, new in zip(base, head, strict=True))
    remaining, extra = iter(head), []
    for item in base:
        for candidate in remaining:
            if candidate == item:
                break
            extra.append(candidate)
        else:
            return False
    extra.extend(remaining)
    return not extra or (field in REGISTRATION_FIELDS and all(
        new_file(item) or (isinstance(item, dict) and set(item) == {"path", "shard_index"}
                           and new_file(item["path"]) and isinstance(item["shard_index"], int))
        for item in extra))


def manifest_registration_only(repo_root: Path, base_sha: str, changed_paths: Collection[str]) -> bool:
    """Compare the manifest with its merge base; unknown evidence keeps full regression."""
    if COMPONENT_OWNERSHIP_PATH not in changed_paths or not base_sha.strip("0"):
        return False

    def git(*args: str) -> str:
        return subprocess.run(["git", *args], cwd=repo_root, check=True, capture_output=True,
                              text=True, timeout=30).stdout

    try:
        base = git("merge-base", base_sha, "HEAD").strip()
        added = frozenset(git("diff", "--name-only", "--no-renames", "--diff-filter=A", base, "HEAD").splitlines())
        return manifest_change_is_registration(
            tomllib.loads(git("show", f"{base}:{COMPONENT_OWNERSHIP_PATH}")),
            tomllib.loads((repo_root / COMPONENT_OWNERSHIP_PATH).read_text(encoding="utf-8")),
            added,
        )
    except (OSError, ValueError, subprocess.SubprocessError):
        return False


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
    # The PR's actual diff, kept even when test_changes forces full regression,
    # so affected_only suites still run only when that diff affects them.
    test_diff: tuple[str, ...] = ()
    shared_pool_simulations: bool = False
    lint_and_static: bool = True
    tests_root: bool = True
    # Pool operator lane: eligible with tests-root; its own matrix decides the final selection.
    nebius_pool: bool = True
    tests_packages: bool = True
    go_checks: bool = True
    runtime_payload: bool = True
    nebius_iac: bool = True
    locked_environments: bool = True
    web_runtime: bool = True
    go_packages: tuple[str, ...] = GO_ALL_PACKAGES
    iac_targets: tuple[str, ...] = IAC_ALL_TARGETS


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
                "shared_pool_simulations",
                *HEAVY_CHECKS,
                *BASELINE_CHECKS,
                "nebius_pool",
                "coverage_summary",
                "web_checks",
                "web_runtime",
            )
        }
        outputs["gate_mode"] = self.gate_mode
        outputs["go_packages"] = json.dumps(self.go_packages, separators=(",", ":"))
        outputs["iac_targets"] = json.dumps(self.iac_targets, separators=(",", ":"))
        test_changes = json.dumps(self.test_changes, separators=(",", ":"))
        # This optional narrowing hint becomes one job environment variable.
        # Stay below Linux's per-string exec limit; an empty hint retains the
        # complete selected lanes, including historical schema reconstruction.
        outputs["test_changes"] = test_changes if len(test_changes.encode()) <= 65536 else "[]"
        # Same limit; an empty diff runs affected_only suites in full.
        test_diff = json.dumps(self.test_diff, separators=(",", ":"))
        outputs["test_diff"] = test_diff if len(test_diff.encode()) <= 65536 else "[]"
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
    manifest_registration_only: bool = False,
    shared_pool_simulations: bool = False,
) -> ValidationPlan:
    if event_name not in SUPPORTED_EVENTS:
        raise ValueError(f"unsupported event_name: {event_name}")

    if shared_pool_simulations and event_name != "workflow_dispatch":
        raise ValueError("shared-pool simulations require an explicit manual workflow_dispatch")

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
    authority_paths = OWNERSHIP_AUTHORITY_PATHS
    if manifest_registration_only and COMPONENT_OWNERSHIP_PATH in paths:
        # Only registers files this change adds: the manifest tests validate the
        # registration and the new files select their own owners.
        if MANIFEST_VALIDATION_TEST not in paths:
            authority_paths = OWNERSHIP_AUTHORITY_PATHS - {MANIFEST_VALIDATION_TEST}
        paths = tuple(dict.fromkeys(
            MANIFEST_VALIDATION_TEST if path == COMPONENT_OWNERSHIP_PATH else path for path in paths))
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

    if any(path in authority_paths for path in paths):
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
        "config/ci-buildkit.toml",
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
        operator_tool = _is_operator_component(path)
        go_input = _is_go_input(path)
        matched_owner = (
            path in PLANNER_PATHS
            or path in OWNERSHIP_AUTHORITY_PATHS
            or _matches(path, exact=NEBIUS_IAC_EXACT, prefixes=NEBIUS_IAC_PREFIXES)
            or _matches(path, exact=NEBIUS_PLATFORM_EXACT, prefixes=NEBIUS_PLATFORM_PREFIXES)
            or _is_protected_deployment_path(path)
            or bool(test_owner_lanes)
            or policy_tool or go_input or operator_tool
        )
        if path in CI_RUNNER_COMPONENTS:
            for name in ("images", "cluster_smoke", "staging_smoke"):
                select(name, "ci-runner-controller")
        for lane in test_owner_lanes:
            reason = f"test-owner:{lane}:{path}"
            if lane == "integration":
                select("integration", reason)
            elif lane == "integration-docker":
                select("integration_docker", reason)
            elif lane == "cluster-smoke":
                select("cluster_smoke", reason)
            elif lane == "system-smoke":
                select("staging_smoke", reason)
        if _matches(path, exact=NEBIUS_IAC_EXACT, prefixes=NEBIUS_IAC_PREFIXES):
            # Terraform has its own fmt, validate and module-test lane. The
            # Python integration suite does not apply Terraform plans.
            if path in NEBIUS_IAC_EXACT:
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
        if not (policy_tool or go_input or path in CI_RUNNER_COMPONENTS) and (path.startswith("tests/contract/") or (not test_owner_lanes and _matches(path, exact=integration_exact, prefixes=integration_prefixes))):
            select("integration", f"path:{path}")
            matched_owner = True
        elif (not test_owner_lanes and not _is_frontend_input(path)
              and not (policy_tool or go_input or path in CI_RUNNER_COMPONENTS)
              and not path.startswith(NEBIUS_IAC_PREFIXES)):
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
        image_payload = any(owner.execution_policy is not None
                            for owner in _component_ownership_manifest().test_owners_for_path(path))
        if image_match and (not test_owner_lanes or image_payload) and not (go_input and path.endswith("_test.go")):
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
        or any(path in PLANNER_PATHS | authority_paths
               or path == ".github/workflows/ci.yml"
               or _is_dependency_authority_path(path) for path in paths)
    )
    backend_paths = tuple(path for path in runtime_paths
                          if not _is_frontend_input(path)
                          and not path.startswith(NEBIUS_IAC_PREFIXES))
    baseline = {name: bool(backend_paths) for name in BASELINE_CHECKS}
    python_paths = tuple(path for path in backend_paths
                         if not _is_go_input(path) and path not in REPOSITORY_POLICY_TOOLS)
    # Static Python checks still inspect retired inputs, while pure frontend,
    # Go and Terraform edits do not need a Python toolchain runner.
    baseline["lint_and_static"] = (
        bool(python_paths)
        or any(path in REPOSITORY_POLICY_TOOLS for path in backend_paths)
        or any(_component_ownership_manifest().ci_ignores_path(path)
               and not _is_documentation_path(path) for path in paths)
    )
    baseline["tests_root"] = bool(python_paths) or any(path in REPOSITORY_POLICY_TOOLS for path in backend_paths)
    baseline["tests_packages"] = bool(python_paths)
    # Virtual payloads mount only the declared case, dispatcher and fixtures;
    # they do not import the server/operator checkout. Dependency and manifest
    # changes still force the complete baseline below.
    baseline["runtime_payload"] = any(
        path in RUNTIME_PAYLOAD_TOOLS or "runtime-payload" in _test_owner_lanes(path)
        for path in backend_paths
    ) or bool(test_inputs and independent_tests is None)
    if python_paths and all(_is_operator_component(path) for path in python_paths):
        baseline["tests_packages"] = False
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
        baseline.update(tests_root=bool(owners & {"tests-root", "nebius-pool"}),
                        tests_packages="tests-packages" in owners,
                        go_checks="go-checks" in owners or crosslanguage_contract,
                        runtime_payload="runtime-payload" in owners,
                        locked_environments=False)
    if force_baseline:
        baseline = dict.fromkeys(BASELINE_CHECKS, True)
    web_runtime = force_baseline or any(_is_frontend_input(path) or path in web_quality_exact for path in paths)
    if force_baseline:
        select("web_checks", "full-regression-or-shared-authority")

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
        test_changes=paths if event_name in {"pull_request", "merge_group"}
        and not force_baseline and not set(labels) & LABEL_TO_CHECK.keys() else (),
        test_diff=paths if event_name in {"pull_request", "merge_group"}
        and not set(labels) & LABEL_TO_CHECK.keys() else (),
        shared_pool_simulations=shared_pool_simulations,
        nebius_pool=baseline["tests_root"],
        web_runtime=web_runtime,
        go_packages=go_packages_for_changes(paths, full=force_baseline),
        iac_targets=iac_targets_for_changes(paths, full=force_baseline),
        **baseline,
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--changed-files", type=Path, required=True)
    parser.add_argument("--labels-json", default="[]")
    parser.add_argument("--shared-pool-simulations", choices=("true", "false"), default="false")
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
    parser.add_argument("--base-sha", default="")
    parser.add_argument("--github-output", type=Path, required=True)
    parser.add_argument("--test-scope", choices=("all", "nebius"), default=os.environ.get("CI_TEST_SCOPE", "all"))
    args = parser.parse_args()
    labels = json.loads(args.labels_json or "[]")
    if not isinstance(labels, list) or not all(isinstance(label, str) for label in labels):
        raise SystemExit("--labels-json must be a JSON array of strings")
    changed_paths = args.changed_files.read_text(encoding="utf-8").splitlines()
    plan = plan_validations(
        changed_paths=changed_paths,
        labels=set(labels),
        event_name=args.event_name,
        pull_request_action=args.pull_request_action,
        pull_request_action_label=args.pull_request_action_label,
        pull_request_draft=args.pull_request_draft == "true",
        pull_request_base_changed=args.pull_request_base_changed == "true",
        manifest_registration_only=manifest_registration_only(REPO_ROOT, args.base_sha, changed_paths),
        shared_pool_simulations=args.shared_pool_simulations == "true",
    )
    outputs = plan.github_outputs()
    effective_changes = tuple(json.loads(outputs["test_changes"]))
    effective_diff = tuple(json.loads(outputs["test_diff"]))
    tracked_paths = _tracked_paths(REPO_ROOT)
    for lane, selected_name, matrix_name in (
        ("tests-root", "tests_root", "tests_root_matrix"),
        # Its eligibility is fixed before tests-root narrows, so an empty
        # tests-root matrix never suppresses a selected pool matrix.
        ("nebius-pool", "nebius_pool", "nebius_pool_matrix"),
        ("integration", "integration", "integration_matrix"),
        ("integration-docker", "integration_docker", "integration_docker_matrix"),
        ("cluster-smoke", "cluster_smoke", "cluster_smoke_matrix"),
    ):
        matrix = test_shard_matrix(
            _component_ownership_manifest(), tracked_paths=tracked_paths, lane=lane,
            repo_root=REPO_ROOT, changed_paths=effective_changes, test_scope=args.test_scope,
            # Only lanes whose jobs receive TEST_DIFF_PATHS may plan with it.
            diff_paths=effective_diff if lane in AFFECTED_ONLY_LANES else (),
            include_shared_pool=plan.shared_pool_simulations,
        ) if getattr(plan, selected_name) and plan.gate_mode == "full" else ()
        outputs[matrix_name] = json.dumps(matrix, separators=(",", ":"))
        # A compatibility-only edit has no tests in the daily Nebius scope.
        # Jobs and the final gate must agree on that deliberate empty selection.
        outputs[selected_name] = str(bool(matrix)).lower()
        plan = replace(
            plan,
            tests_root=bool(matrix) if selected_name == "tests_root" else plan.tests_root,
            nebius_pool=bool(matrix) if selected_name == "nebius_pool" else plan.nebius_pool,
            integration=bool(matrix) if selected_name == "integration" else plan.integration,
            integration_docker=bool(matrix) if selected_name == "integration_docker" else plan.integration_docker,
            cluster_smoke=bool(matrix) if selected_name == "cluster_smoke" else plan.cluster_smoke,
        )
    with args.github_output.open("a", encoding="utf-8") as handle:
        for name, value in outputs.items():
            handle.write(f"{name}={value}\n")
    print(json.dumps(asdict(plan), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
