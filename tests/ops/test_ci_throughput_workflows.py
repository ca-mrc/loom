from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
import scripts.component_ownership as component_ownership
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]


def _workflow(path: str) -> dict[str, Any]:
    return yaml.safe_load((REPO_ROOT / path).read_text(encoding="utf-8"))


def _locked_action_sha(action: str) -> str:
    lock = json.loads(
        (REPO_ROOT / "config/ci-actions-lock.json").read_text(encoding="utf-8"),
    )
    return str(lock["actions"][action]["sha"])


def _yaml_documents(path: str) -> list[dict[str, Any]]:
    return [
        document
        for document in yaml.safe_load_all(
            (REPO_ROOT / path).read_text(encoding="utf-8"),
        )
        if document is not None
    ]


def _workflow_on(workflow: dict[str, Any]) -> dict[str, Any]:
    # PyYAML treats unquoted GitHub Actions key `on` as YAML 1.1 bool.
    return workflow.get("on", workflow.get(True))


def _image_matrix_step() -> dict[str, Any]:
    workflow = _workflow(".github/workflows/images.yml")
    return next(
        step
        for step in workflow["jobs"]["plan"]["steps"]
        if step.get("name") == "Select affected images"
    )


def _run_image_matrix_plan(
    tmp_path: Path,
    *,
    required: str,
    unowned_runtime: str,
    changed_paths: tuple[str, ...] = ("unowned-runtime/new-input.bin",),
) -> tuple[subprocess.CompletedProcess[str], str]:
    changed_files = tmp_path / "changed-files.txt"
    changed_files.write_text("\n".join(changed_paths) + "\n", encoding="utf-8")
    github_output = tmp_path / "github-output.txt"
    result = subprocess.run(
        ["bash"],
        input=_image_matrix_step()["run"],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        env={
            **os.environ,
            "EVENT_NAME": "pull_request",
            "BASE_BRANCH": "dev",
            "REQUIRED": required,
            "UNOWNED_RUNTIME": unowned_runtime,
            "CHANGED_FILES": str(changed_files),
            "GITHUB_OUTPUT": str(github_output),
        },
        check=False,
    )
    output = github_output.read_text(encoding="utf-8") if github_output.exists() else ""
    return result, output


def _github_output_value(output: str, key: str) -> str:
    values = dict(line.split("=", maxsplit=1) for line in output.splitlines())
    return values[key]


GATE_CONTRACTS = {
    ".github/workflows/ci.yml": ("repository-checks", "repository-checks"),
    ".github/workflows/images.yml": ("images-gate", "images-gate"),
    ".github/workflows/cluster-smoke.yml": (
        "cluster-smoke-gate",
        "cluster-smoke-gate",
    ),
    ".github/workflows/staging-smoke.yml": (
        "staging-smoke-gate",
        "staging-smoke-gate",
    ),
}

CI_SELECTOR_LABELS = (
    "ci:integration",
    "ci:integration-docker",
    "ci:images",
    "cluster-smoke",
    "staging-smoke",
    "ci:coverage-summary",
)

SOURCE_PLAN_CONTRACTS = {
    ".github/workflows/ci.yml": ("workflow-plan", "plan"),
    ".github/workflows/images.yml": ("plan", "required"),
    ".github/workflows/cluster-smoke.yml": ("plan", "plan"),
    ".github/workflows/staging-smoke.yml": ("plan", "plan"),
}


def _normalized_expression(value: str) -> str:
    return " ".join(value.split())


GITHUB_HOSTED_CONTROL_JOBS = {
    ".github/workflows/ci.yml": {
        "workflow-plan",
        "fast-coverage",
        "coverage-summary",
        "repository-checks",
        "images-gate",
        "cluster-smoke-gate",
        "staging-smoke-gate",
    },
    ".github/workflows/images.yml": {"plan", "images-gate"},
    ".github/workflows/cluster-smoke.yml": {"plan", "cluster-smoke-gate"},
    ".github/workflows/staging-smoke.yml": {"plan", "staging-smoke-gate"},
}


def test_hosted_workflows_do_not_depend_on_a_private_uv_mirror() -> None:
    workflow_paths = (
        ".github/workflows/ci.yml",
        ".github/workflows/cluster-smoke.yml",
        ".github/workflows/staging-smoke.yml",
    )

    for workflow_path in workflow_paths:
        setup_steps = [
            step
            for job in _workflow(workflow_path)["jobs"].values()
            for step in job.get("steps", [])
            if str(step.get("uses", "")).startswith("astral-sh/setup-uv@")
        ]
        assert setup_steps
        for step in setup_steps:
            assert "manifest-file" not in step["with"]


def test_control_jobs_keep_hosted_fallback_and_use_the_same_optional_queue() -> None:
    for workflow_path, job_ids in GITHUB_HOSTED_CONTROL_JOBS.items():
        jobs = _workflow(workflow_path)["jobs"]
        for job_id in job_ids:
            placement = jobs[job_id]["runs-on"]
            assert placement.endswith("|| 'ubuntu-latest' }}")
            assert "LOOM_CI_UNIFIED_RUNNERS" in placement
            assert "loom-ci-exec" in placement


def test_source_workflows_share_native_run_identity() -> None:
    workflows = {path: _workflow(path) for path in GATE_CONTRACTS}
    common_run_names = {workflow["run-name"] for workflow in workflows.values()}

    assert len(common_run_names) == 1
    for run_name in common_run_names:
        assert "' ' ||" not in run_name
        for mode in ("manual", "filtered", "full"):
            assert f"gate={mode} / head={{0}} / base={{1}}" in run_name
        for field in ("updated={2}", "action={3}", "label={4}", "labels={5}", "pull={6}"):
            assert field in run_name

        assert "github.event.pull_request.draft" in run_name
        assert "github.event.action == 'converted_to_draft'" in run_name
        assert "github.event.action == 'edited'" not in run_name
        assert "github.event.changes.base == null" not in run_name
        assert "fromJSON(" not in run_name
        for label in CI_SELECTOR_LABELS:
            assert label in run_name

    expected_pr_types = {
        "opened",
        "synchronize",
        "reopened",
        "ready_for_review",
        "converted_to_draft",
        "labeled",
        "unlabeled",
    }
    assert set(_workflow_on(workflows[".github/workflows/ci.yml"])["pull_request"]["types"]) == expected_pr_types
    for path, workflow in workflows.items():
        if path != ".github/workflows/ci.yml":
            assert set(_workflow_on(workflow)) == {"workflow_call", "workflow_dispatch"}


def test_source_workflows_have_no_publisher_generation_contract() -> None:
    for workflow_path, (plan_job_id, plan_step_id) in SOURCE_PLAN_CONTRACTS.items():
        workflow = _workflow(workflow_path)
        plan_job = workflow["jobs"][plan_job_id]
        plan_step = next(step for step in plan_job["steps"] if step.get("id") == plan_step_id)

        assert "publisher_active" not in plan_job["outputs"]
        assert plan_step["env"]["BASE_SHA"].startswith("${{ ")
        assert "TRUSTED_PROMOTION" not in plan_step["env"]
        assert "publisher_active" not in plan_step["run"]
        assert "authoritative-gates.yml" not in plan_step["run"]


CALLED_PLAN_CONTRACTS = {
    path: contract
    for path, contract in SOURCE_PLAN_CONTRACTS.items()
    if path != ".github/workflows/ci.yml"
}


def test_called_workflows_filter_only_draft_payloads_ci_did_not_upgrade() -> None:
    for workflow_path, (plan_job_id, plan_step_id) in CALLED_PLAN_CONTRACTS.items():
        workflow = _workflow(workflow_path)
        jobs = workflow["jobs"]
        plan_job = jobs[plan_job_id]
        event_step = plan_job["steps"][0]
        checkout_step = next(
            step
            for step in plan_job["steps"]
            if str(step.get("uses", "")).startswith("actions/checkout@")
        )
        planner_step = next(step for step in plan_job["steps"] if step.get("id") == plan_step_id)
        gate_id = GATE_CONTRACTS[workflow_path][0]

        assert event_step["id"] == "event"
        filtered_event = _normalized_expression(event_step["env"]["FILTERED_EVENT"])
        assert filtered_event == (
            "${{ github.event_name == 'pull_request' && !inputs.stale_draft && "
            "(github.event.pull_request.draft || "
            "github.event.action == 'converted_to_draft') }}"
        )
        assert "checkout_required=false" in event_step["run"]
        assert "gate_mode=filtered" in event_step["run"]
        assert checkout_step["if"] == "steps.event.outputs.checkout_required == 'true'"
        assert planner_step["if"] == "steps.event.outputs.checkout_required == 'true'"
        assert "needs." in jobs[gate_id]["if"]
        assert "outputs.gate_mode != 'filtered'" in jobs[gate_id]["if"]

        stale_input = _workflow_on(workflow)["workflow_call"]["inputs"]["stale_draft"]
        assert stale_input["type"] == "boolean"
        assert stale_input["default"] is False
        planner = next(step for step in plan_job["steps"] if "plan_ci_validations.py" in step.get("run", ""))
        assert planner["env"]["PR_STALE_DRAFT"] == "${{ inputs.stale_draft == true }}"
        assert '--pull-request-stale-draft "${PR_STALE_DRAFT:-false}"' in planner["run"]


def test_ci_rechecks_draft_payloads_against_the_live_pull_request() -> None:
    workflow = _workflow(".github/workflows/ci.yml")
    jobs = workflow["jobs"]
    plan_job = jobs["workflow-plan"]
    steps = plan_job["steps"]
    ids = [step.get("id") or step.get("uses", "").split("@")[0] for step in steps]
    assert ids[:4] == ["event", "actions/checkout", "stale", "filtered"]

    event, _checkout, stale, filtered = steps[:4]
    assert _normalized_expression(event["env"]["FILTERED_EVENT"]) == (
        "${{ github.event_name == 'pull_request' && "
        "(github.event.pull_request.draft || "
        "github.event.action == 'converted_to_draft') }}"
    )
    assert "gate_mode" not in event["run"]
    assert stale["if"] == "steps.event.outputs.draft_payload == 'true'"
    assert stale["run"] == "python3 scripts/ci_stale_draft_event.py"
    assert stale["env"]["GH_TOKEN"] == "${{ github.token }}"
    assert stale["env"]["HEAD_SHA"] == "${{ github.event.pull_request.head.sha }}"
    assert _normalized_expression(filtered["if"]) == (
        "steps.event.outputs.draft_payload == 'true' && "
        "steps.stale.outputs.stale_draft != 'true'"
    )
    assert "gate_mode=filtered" in filtered["run"]
    planner = next(step for step in steps if step.get("id") == "plan")
    assert planner["if"] == "steps.filtered.outcome == 'skipped'"
    assert planner["env"]["PR_STALE_DRAFT"] == "${{ steps.stale.outputs.stale_draft == 'true' }}"
    assert '--pull-request-stale-draft "${PR_STALE_DRAFT:-false}"' in planner["run"]
    assert plan_job["outputs"]["stale_draft"] == "${{ steps.stale.outputs.stale_draft || 'false' }}"
    assert plan_job["permissions"] == {"contents": "read", "pull-requests": "read", "actions": "read"}

    for caller in ("images-validation", "cluster-validation", "staging-validation"):
        assert jobs[caller]["with"] == {
            "stale_draft": "${{ needs.workflow-plan.outputs.stale_draft == 'true' }}",
        }
    for gate_id in ("repository-checks", "images-gate", "cluster-smoke-gate", "staging-smoke-gate"):
        assert "outputs.gate_mode != 'filtered'" in jobs[gate_id]["if"]


def test_draft_classifier_emits_no_checkout_contract(tmp_path: Path) -> None:
    for workflow_path, (plan_job_id, _plan_step_id) in CALLED_PLAN_CONTRACTS.items():
        event_step = _workflow(workflow_path)["jobs"][plan_job_id]["steps"][0]
        github_output = tmp_path / (Path(workflow_path).stem + "-output.txt")
        result = subprocess.run(
            ["bash"],
            input=event_step["run"],
            text=True,
            capture_output=True,
            env={
                **os.environ,
                "FILTERED_EVENT": "true",
                "GITHUB_OUTPUT": str(github_output),
            },
            check=False,
        )

        assert result.returncode == 0, (workflow_path, result.stderr)
        outputs = github_output.read_text(encoding="utf-8")
        assert "checkout_required=false" in outputs
        assert "gate_mode=filtered" in outputs
        assert "publisher_active" not in outputs


def test_ci_filter_step_emits_the_filtered_contract(tmp_path: Path) -> None:
    filtered = next(
        step for step in _workflow(".github/workflows/ci.yml")["jobs"]["workflow-plan"]["steps"]
        if step.get("id") == "filtered"
    )
    github_output = tmp_path / "output.txt"
    result = subprocess.run(
        ["bash"], input=filtered["run"], text=True, capture_output=True,
        env={**os.environ, "GITHUB_OUTPUT": str(github_output)}, check=False,
    )

    assert result.returncode == 0, result.stderr
    outputs = dict(line.split("=", 1) for line in github_output.read_text(encoding="utf-8").splitlines())
    assert outputs["gate_mode"] == "filtered"
    assert outputs["full_gate"] == "false"
    assert {key for key, value in outputs.items() if value == "true"} == {"docs_only"}


def test_source_gate_names_are_native_and_stable() -> None:
    jobs = _workflow(".github/workflows/ci.yml")["jobs"]
    expression = _normalized_expression(jobs["repository-checks"]["name"])
    assert expression == _normalized_expression(
        "${{ github.event_name == 'schedule' && 'repository-checks-scheduled' || "
        "github.event_name == 'workflow_dispatch' && 'repository-checks-manual' || "
        "github.event_name == 'push' && 'repository-checks-push' || "
        "needs.workflow-plan.outputs.gate_mode == 'full' && 'repository-checks' || "
        "'repository-checks-filtered' }}"
    )
    for path, (gate_id, protected_name) in GATE_CONTRACTS.items():
        assert gate_id in jobs
        if gate_id != "repository-checks":
            assert _normalized_expression(jobs[gate_id]["name"]) == _normalized_expression(
                "${{ "
                f"github.event_name == 'schedule' && '{protected_name}-scheduled' || "
                f"github.event_name == 'workflow_dispatch' && '{protected_name}-manual' || "
                f"github.event_name == 'push' && '{protected_name}-push' || "
                f"needs.workflow-plan.outputs.gate_mode == 'full' && '{protected_name}' || "
                f"'{protected_name}-filtered' "
                "}}"
            )
            assert _workflow(path)["jobs"][gate_id]["name"] == (
                protected_name.removesuffix("-gate") + " result"
            )
        assert "publisher_active" not in str(jobs[gate_id])
        assert f"{protected_name}-attempt" not in str(jobs[gate_id])


def test_push_aggregates_cannot_duplicate_protected_names_on_promotion_heads() -> None:
    jobs = _workflow(".github/workflows/ci.yml")["jobs"]
    assert "github.event_name == 'push' && 'repository-checks-push'" in (
        _normalized_expression(jobs["repository-checks"]["name"])
    )
    for gate_id in ("images-gate", "cluster-smoke-gate", "staging-smoke-gate"):
        condition = jobs[gate_id]["if"]
        assert "github.event_name == 'pull_request'" in condition
        assert "github.event_name == 'merge_group'" in condition
        assert "github.event_name == 'push'" not in condition


def test_no_workflow_can_write_custom_authoritative_states() -> None:
    workflow_paths = sorted((REPO_ROOT / ".github/workflows").glob("*.yml"))
    for workflow_file in workflow_paths:
        workflow_path = workflow_file.relative_to(REPO_ROOT).as_posix()
        workflow = _workflow(workflow_path)
        workflow_source = workflow_file.read_text(encoding="utf-8")

        assert "scripts/ops/authoritative_gate.py" not in workflow_source
        assert "AUTHORITATIVE_CONTEXT" not in workflow_source
        workflow_permissions = workflow.get("permissions", {})
        assert isinstance(workflow_permissions, dict)
        assert workflow_permissions.get("checks") != "write"
        assert workflow_permissions.get("statuses") != "write"

        for job_name, job in workflow["jobs"].items():
            effective_permissions = job.get("permissions", workflow_permissions)
            assert isinstance(effective_permissions, dict), job_name
            assert effective_permissions.get("checks") != "write", job_name
            assert effective_permissions.get("statuses") != "write", job_name


def _gate_script(workflow_path: str, gate_id: str) -> str:
    workflow = _workflow(workflow_path)
    gate_step = next(
        step
        for step in workflow["jobs"][gate_id]["steps"]
        if step.get("name", "").startswith("Enforce selected")
    )
    return gate_step["run"]


def test_images_builds_use_planner_selection() -> None:
    workflow = _workflow(".github/workflows/images.yml")
    jobs = workflow["jobs"]

    required_output = jobs["plan"]["outputs"]["required"]
    assert "steps.plan.outputs.required" in required_output
    assert "steps.event.outputs.required" in required_output
    assert set(jobs["build"]["needs"]) == {
        "plan",
        "trivy-binary",
    }
    assert "needs.plan.outputs.required == 'true'" in jobs["build"]["if"]


def test_images_workflow_uses_path_aware_matrix_plan() -> None:
    workflow = _workflow(".github/workflows/images.yml")
    jobs = workflow["jobs"]
    assert "push" not in _workflow_on(workflow)

    assert "plan" in jobs
    assert "images" in jobs["plan"]["outputs"]
    assert "ordinary_builds" in jobs["plan"]["outputs"]
    assert "stage1_images" not in jobs["plan"]["outputs"]
    build = jobs["build"]
    assert set(build["needs"]) == {
        "plan",
        "trivy-binary",
    }
    assert build["strategy"]["matrix"]["include"] == (
        "${{ fromJSON(needs.plan.outputs.ordinary_builds) }}"
    )
    plan_script = "\n".join(step.get("run", "") for step in jobs["plan"]["steps"] if "run" in step)
    assert "scripts/component_ownership.py" in plan_script
    assert "plan-images" in plan_script


def test_ci_push_safety_net_excludes_already_admitted_dev_merges() -> None:
    workflow = _workflow(".github/workflows/ci.yml")
    jobs = workflow["jobs"]

    assert _workflow_on(workflow)["push"] == {"branches": ["main"]}
    assert "refs/heads/dev" not in jobs["workflow-plan"].get("if", "")
    assert "refs/heads/dev" not in jobs["fast-coverage"]["if"]
    assert "refs/heads/dev" not in jobs["repository-checks"]["if"]


def test_behavior_stage1_image_jobs_are_absent() -> None:
    workflow = _workflow(".github/workflows/images.yml")
    jobs = workflow["jobs"]

    assert {"stage1-build", "stage1-publish", "stage1-publish-index"}.isdisjoint(jobs)
    assert "behavior-stage1-sim" not in str(workflow)


def test_images_matrix_plan_receives_shared_required_decision() -> None:
    env = _image_matrix_step()["env"]
    assert env["REQUIRED"] == ("${{ steps.required.outputs.images }}")
    assert env["UNOWNED_RUNTIME"] == ("${{ steps.required.outputs.unowned_runtime }}")


def test_images_required_unowned_runtime_path_selects_all_images(tmp_path: Path) -> None:
    result, output = _run_image_matrix_plan(
        tmp_path,
        required="true",
        unowned_runtime="true",
    )

    assert result.returncode == 0, result.stderr
    matrix = json.loads(_github_output_value(output, "images"))
    native_matrix = json.loads(_github_output_value(output, "native_builds"))
    assert _github_output_value(output, "required") == "true"
    assert len(matrix) == 7
    assert {entry["image"] for entry in matrix} == {'web', 'service', 'llm-gateway', 'execution-runtime', 'control-plane', 'execution-actuator', 'harbor-runtime'}
    assert all(set(entry) == {"image", "image_name", "dockerfile", "context"} for entry in matrix)
    assert len(native_matrix) == 14
    assert {(entry["architecture"], entry["platform"]) for entry in native_matrix} == {
        ("amd64", "linux/amd64"),
        ("arm64", "linux/arm64"),
    }


def test_images_mixed_known_and_unowned_paths_select_all_images(tmp_path: Path) -> None:
    result, output = _run_image_matrix_plan(
        tmp_path,
        required="true",
        unowned_runtime="true",
        changed_paths=("web/src/App.tsx", "unowned-runtime/new-input.bin"),
    )

    assert result.returncode == 0, result.stderr
    matrix = json.loads(_github_output_value(output, "images"))
    assert {entry["image"] for entry in matrix} == {'web', 'service', 'llm-gateway', 'execution-runtime', 'control-plane', 'execution-actuator', 'harbor-runtime'}


def test_frontend_security_policy_change_selects_only_web_image(tmp_path: Path) -> None:
    result, output = _run_image_matrix_plan(
        tmp_path,
        required="true",
        unowned_runtime="false",
        changed_paths=("deploy/nginx-spa-security-headers.conf",),
    )

    assert result.returncode == 0, result.stderr
    assert json.loads(_github_output_value(output, "images")) == [
        {
            "image": "web",
            "image_name": "loom-web",
            "dockerfile": "deploy/Dockerfile.web",
            "context": ".",
        },
    ]


def test_unowned_static_documentation_does_not_require_images(tmp_path: Path) -> None:
    result, output = _run_image_matrix_plan(
        tmp_path,
        required="false",
        unowned_runtime="false",
        changed_paths=("docs/user-guide.md",),
    )

    assert result.returncode == 0, result.stderr
    assert _github_output_value(output, "required") == "false"
    assert json.loads(_github_output_value(output, "images")) == []


@pytest.mark.parametrize("required", ["", "invalid"])
def test_images_matrix_plan_rejects_malformed_required(
    tmp_path: Path,
    required: str,
) -> None:
    result, output = _run_image_matrix_plan(
        tmp_path,
        required=required,
        unowned_runtime="false",
    )

    assert result.returncode != 0
    assert "FAIL: invalid planner boolean:" in result.stderr
    assert output == ""


@pytest.mark.parametrize("unowned_runtime", ["", "invalid"])
def test_images_matrix_plan_rejects_malformed_unowned_runtime(
    tmp_path: Path,
    unowned_runtime: str,
) -> None:
    result, output = _run_image_matrix_plan(
        tmp_path,
        required="true",
        unowned_runtime=unowned_runtime,
    )

    assert result.returncode != 0
    assert "FAIL: invalid planner boolean:" in result.stderr
    assert output == ""


def test_images_merge_groups_use_combined_diff_matrix_selection() -> None:
    workflow = _workflow(".github/workflows/images.yml")
    plan_script = "\n".join(
        step.get("run", "") for step in workflow["jobs"]["plan"]["steps"] if "run" in step
    )

    assert '"$EVENT_NAME" == "workflow_dispatch"' in plan_script
    assert '"$EVENT_NAME" == "merge_group"' not in plan_script
    assert '"$UNOWNED_RUNTIME" == "true"' in plan_script
    assert '"$REQUIRED" == "true"' in plan_script
    assert "--force-all" in plan_script
    assert "--fallback-all" in plan_script


def test_images_merge_groups_do_not_publish_or_write_cache() -> None:
    workflow = _workflow(".github/workflows/images.yml")
    build_steps = workflow["jobs"]["build"]["steps"]
    build_step = next(
        step
        for step in build_steps
        if step.get("name") == "Build without registry or cache write authority"
    )
    build_script = build_step["run"]

    assert "docker login" not in build_script
    assert "--push" not in build_script
    assert "--cache-to" not in build_script
    assert 'merge_group) image_tag="merge-group-${sha_short}"' in build_script
    assert "type=docker" in build_script
    assert ".docker.tar" in build_script
    assert "type=oci" not in build_script
    assert "type=registry" not in build_script


def test_untrusted_image_archives_are_scanned_job_local_and_never_uploaded() -> None:
    workflow = _workflow(".github/workflows/images.yml")
    build = workflow["jobs"]["build"]
    steps = build["steps"]
    build_step = next(
        step
        for step in steps
        if step.get("name") == "Build without registry or cache write authority"
    )
    scan_step = next(step for step in steps if step.get("name") == "Scan native image archive")

    assert build["permissions"] == {"contents": "read"}
    assert "GITHUB_TOKEN" not in json.dumps(build_step)
    assert "secrets." not in json.dumps(build_step)
    assert "docker login" not in json.dumps(build)
    assert "ghcr.io" not in json.dumps(build)
    assert '"$EVENT_NAME" == "pull_request"' in build_step["run"]
    assert "candidate-${HEAD_SHA}-${ARCHITECTURE}" in build_step["run"]
    assert scan_step["env"]["ARCHIVE"].endswith(".docker.tar")
    assert '--input "$ARCHIVE"' in scan_step["run"]
    assert all("upload-artifact" not in step.get("uses", "") for step in steps)


def test_candidate_images_use_verified_trivy_binaries_and_complete_scans() -> None:
    workflow = _workflow(".github/workflows/images.yml")
    trivy_binary = workflow["jobs"]["trivy-binary"]
    build = workflow["jobs"]["build"]

    assert trivy_binary["needs"] == ["plan"]
    assert trivy_binary["permissions"] == {"contents": "read"}
    assert trivy_binary["runs-on"].endswith("|| 'ubuntu-24.04' }}")
    assert "strategy" not in trivy_binary
    install = next(
        step
        for step in trivy_binary["steps"]
        if step.get("name") == "Install and record pinned Trivy binary"
    )
    upload = next(
        step
        for step in trivy_binary["steps"]
        if step.get("name") == "Upload exact verified Trivy binary"
    )
    assert "--architecture amd64" in install["run"]
    assert "arm64" not in install["run"]
    assert "python3 scripts/install_trivy.py" in install["run"]
    assert "sha256sum trivy > trivy.sha256" in install["run"]
    assert upload["with"]["name"] == "trivy-binaries-run-${{ github.run_id }}"
    assert upload["with"]["overwrite"] is True
    assert build["needs"] == [
        "plan",
        "trivy-binary",
    ]

    build_step_names = [step.get("name") for step in build["steps"]]
    build_script = next(
        step["run"]
        for step in build["steps"]
        if step.get("name") == "Build without registry or cache write authority"
    )
    build_scan = next(
        step for step in build["steps"] if step.get("name") == "Scan native image archive"
    )
    assert "type=docker,dest=${archive}" in build_script
    assert "uses" not in build_scan
    assert build_scan["shell"] == "bash"
    assert build_scan["env"] == {
        "ARCHIVE": "/tmp/${{ matrix.image }}-${{ matrix.architecture }}.docker.tar",
        "REPORT": "/tmp/${{ matrix.image }}-${{ matrix.architecture }}.trivy.json",
        "IMAGE_NAME": "${{ matrix.image }}",
        "ARCHITECTURE": "${{ matrix.architecture }}",
    }
    assert build_scan["run"].strip() == (
        "set -euo pipefail\n"
        'trivy_bin="/tmp/loom-trivy-binaries/${ARCHITECTURE}/trivy"\n'
        "set +e\n"
        '"$trivy_bin" --config /tmp/loom-trivy-release.yaml image \\\n'
        '  --input "$ARCHIVE" \\\n'
        "  --format json \\\n"
        '  --output "$REPORT" \\\n'
        "  --ignorefile /tmp/loom-trivy-release.ignore.yaml \\\n"
        "  --show-suppressed \\\n"
        '  --cache-dir "$RUNNER_TEMP/loom-trivy-cache"\n'
        "scan_status=$?\n"
        "set -e\n"
        "if (( scan_status != 0 )); then\n"
        '  python3 scripts/summarize_trivy_report.py "$REPORT" || true\n'
        '  exit "$scan_status"\n'
        "fi\n"
        "python3 scripts/validate_trivy_release_report.py \\\n"
        '  --component "$IMAGE_NAME" \\\n'
        '  --architecture "$ARCHITECTURE" \\\n'
        '  --report "$REPORT" \\\n'
        "  --ignore-file /tmp/loom-trivy-release.ignore.yaml \\\n"
        "  --no-exceptions"
    )
    assert build_step_names.index("Build without registry or cache write authority") < (
        build_step_names.index("Scan native image archive")
    )
    assert build_step_names.index("Download exact verified Trivy binary") < (
        build_step_names.index("Verify distributed Trivy binary")
    )
    assert build_step_names.index("Verify distributed Trivy binary") < (
        build_step_names.index("Scan native image archive")
    )


def test_image_builds_pass_exact_full_head_sha() -> None:
    workflow = _workflow(".github/workflows/images.yml")
    expected_steps = {
        "build": "Build without registry or cache write authority",
        "nebius-harness-build": "Build without registry or cache write authority",
    }

    for job_name, step_name in expected_steps.items():
        step = next(
            step for step in workflow["jobs"][job_name]["steps"] if step.get("name") == step_name
        )
        script = step["run"]
        assert "github.event.pull_request.head.sha" in step["env"]["HEAD_SHA"]
        assert step["env"]["BUILD_CONTEXT"] == "${{ matrix.context }}"
        assert '--build-arg "LOOM_BUILD_SHA=${HEAD_SHA}"' in script
        context_marker = 'build_args+=("$BUILD_CONTEXT")'
        assert context_marker in script
        assert script.index("LOOM_BUILD_SHA=${HEAD_SHA}") < script.rindex(context_marker)
        assert 'if [[ "$IMAGE_NAME" == "service" ]]' not in script
        assert "build_args+=(.)" not in script


def test_stable_gate_scripts_have_valid_bash_syntax() -> None:
    invalid_gates: dict[str, str] = {}
    for workflow_path, (gate_id, _) in GATE_CONTRACTS.items():
        result = subprocess.run(
            ["bash", "-n"],
            input=_gate_script(workflow_path, gate_id),
            text=True,
            capture_output=True,
            check=False,
        )
        if result.returncode:
            invalid_gates[gate_id] = result.stderr

    assert not invalid_gates, invalid_gates


def test_protected_workflows_cover_gate_authority_pr_transitions() -> None:
    workflow = _workflow(".github/workflows/ci.yml")
    pull_request = _workflow_on(workflow)["pull_request"]
    assert {
        "opened", "synchronize", "reopened", "ready_for_review",
        "converted_to_draft", "labeled", "unlabeled",
    } <= set(pull_request["types"])
    # Metadata edits cannot restart or cancel admission on an unchanged head.
    # The merge-group event owns validation against its actual combined base.
    assert "edited" not in pull_request["types"]
    assert "paths" not in pull_request
    assert "checks_requested" in _workflow_on(workflow)["merge_group"]["types"]


def test_manual_and_filtered_contexts_cannot_duplicate_protected_names() -> None:
    jobs = _workflow(".github/workflows/ci.yml")["jobs"]
    repository_name = jobs["repository-checks"]["name"]
    assert "'repository-checks-manual'" in repository_name
    assert "'repository-checks-filtered'" in repository_name
    for path, (gate_id, protected_name) in GATE_CONTRACTS.items():
        if gate_id == "repository-checks":
            continue
        gate = jobs[gate_id]
        assert "outputs.gate_mode != 'filtered'" in gate["if"]
        assert "github.event_name == 'pull_request'" in gate["if"]
        assert "github.event_name == 'merge_group'" in gate["if"]
        assert "workflow_dispatch" not in gate["if"]
        internal_name = _workflow(path)["jobs"][gate_id]["name"]
        assert protected_name not in internal_name
        assert internal_name.endswith(" result")


@pytest.mark.parametrize("required", ["", "invalid"])
@pytest.mark.parametrize(
    ("workflow_path", "gate_id", "result_env"),
    [
        (
            ".github/workflows/images.yml",
            "images-gate",
            {
                "EVENT_NAME": "pull_request",
                "BUILD_RESULT": "skipped",
                "HARBOR_REQUIRED": "true",
            "HARNESS_BUILD_RESULT": "skipped",
                "STANDARD_IMAGES": '[{"image":"service"}]',
            },
        ),
        (
            ".github/workflows/cluster-smoke.yml",
            "cluster-smoke-gate",
            {"CONTRACT_RESULT": "skipped"},
        ),
        (
            ".github/workflows/staging-smoke.yml",
            "staging-smoke-gate",
            {"SYSTEM_SMOKE_RESULT": "skipped"},
        ),
    ],
)
def test_optional_gate_scripts_fail_closed_for_invalid_required(
    workflow_path: str,
    gate_id: str,
    result_env: dict[str, str],
    required: str,
) -> None:
    result = subprocess.run(
        ["bash"],
        input=_gate_script(workflow_path, gate_id),
        text=True,
        capture_output=True,
        env={
            "PLAN_RESULT": "success",
            "GATE_MODE": "full",
            "REQUIRED": required,
            **result_env,
        },
        check=False,
    )

    assert result.returncode != 0, (workflow_path, required, result.stdout)
    assert "FAIL: invalid planner boolean required=" in result.stderr


@pytest.mark.parametrize(
    ("required", "heavy_result", "accepted"),
    [("true", "success", True), ("false", "skipped", True), ("false", "success", True),
     ("true", "failure", False), ("true", "cancelled", False), ("true", "skipped", False),
     ("false", "failure", False)],
)
@pytest.mark.parametrize(
    ("workflow_path", "gate_id", "result_names"),
    [
        (
            ".github/workflows/cluster-smoke.yml",
            "cluster-smoke-gate",
            ["CONTRACT_RESULT"],
        ),
        (
            ".github/workflows/staging-smoke.yml",
            "staging-smoke-gate",
            ["SYSTEM_SMOKE_RESULT"],
        ),
    ],
)
def test_optional_gate_scripts_preserve_result_semantics(
    workflow_path: str,
    gate_id: str,
    result_names: list[str],
    required: str,
    heavy_result: str,
    accepted: bool,
) -> None:
    result = subprocess.run(
        ["bash"],
        input=_gate_script(workflow_path, gate_id),
        text=True,
        capture_output=True,
        env={
            "PLAN_RESULT": "success",
            "GATE_MODE": "full",
            "REQUIRED": required,
            **dict.fromkeys(result_names, heavy_result),
        },
        check=False,
    )

    assert (result.returncode == 0) is accepted, (workflow_path, required, heavy_result, result.stderr)


@pytest.mark.parametrize(
    ("event_name", "required", "build_result"),
    [
        ("workflow_dispatch", "true", "skipped"),
        ("push", "true", "success"),
        ("pull_request", "false", "success"),
        ("invalid", "true", "success"),
    ],
)
def test_images_gate_rejects_missing_builds_or_invalid_events(
    event_name: str,
    required: str,
    build_result: str,
) -> None:
    result = subprocess.run(
        ["bash"],
        input=_gate_script(".github/workflows/images.yml", "images-gate"),
        text=True,
        capture_output=True,
        env={
            "EVENT_NAME": event_name,
            "PLAN_RESULT": "success",
            "GATE_MODE": "full",
            "REQUIRED": required,
            "BUILD_RESULT": build_result,
            "HARBOR_REQUIRED": "true",
            "HARNESS_BUILD_RESULT": build_result,
            "STANDARD_IMAGES": '[{"image":"service"}]',
        },
        check=False,
    )

    assert result.returncode != 0


REPOSITORY_SELECTED_RESULTS = {
    "LINT_SELECTED": "LINT_RESULT",
    "LOCKED_SELECTED": "LOCKED_RESULT",
    "ROOT_SELECTED": "ROOT_RESULT",
    "POOL_SELECTED": "POOL_RESULT",
    "PACKAGES_SELECTED": "PACKAGES_RESULT",
    "PAYLOAD_SELECTED": "PAYLOAD_RESULT",
    "IAC_SELECTED": "IAC_RESULT",
    "GO_SELECTED": "GO_RESULT",
    "INTEGRATION_SELECTED": "INTEGRATION_RESULT",
    "DOCKER_SELECTED": "DOCKER_RESULT",
    "COVERAGE_SELECTED": "COVERAGE_RESULT",
    "WEB_SELECTED": "WEB_RESULT",
    "WEB_RUNTIME_SELECTED": "WEB_RUNTIME_RESULT",
}


def _repository_result_environment(**overrides: str) -> dict[str, str]:
    env = {"PLAN_RESULT": "success", "GATE_MODE": "full", "DOCS_ONLY": "false",
           "FAST_COVERAGE_RESULT": "skipped", "FEEDBACK_RESULT": "success"}
    for selected, result in REPOSITORY_SELECTED_RESULTS.items():
        env[selected], env[result] = "false", "skipped"
    return {**env, **overrides}


def _run_repository_gate(**overrides: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["bash"], input=_gate_script(".github/workflows/ci.yml", "repository-checks"),
        text=True, capture_output=True, env=_repository_result_environment(**overrides), check=False,
    )


@pytest.mark.parametrize("planner_value", ["", "invalid"])
@pytest.mark.parametrize("planner_name", ["DOCS_ONLY", *REPOSITORY_SELECTED_RESULTS])
def test_repository_checks_fails_closed_for_invalid_planner_booleans(
    planner_name: str, planner_value: str,
) -> None:
    result = _run_repository_gate(**{planner_name: planner_value})
    assert result.returncode != 0
    assert "FAIL: invalid planner boolean" in result.stderr


@pytest.mark.parametrize("selected,validation_result", [
    ("true", "success"), ("false", "skipped"), ("false", "success"),
])
def test_repository_checks_preserves_result_semantics(selected: str, validation_result: str) -> None:
    env = {"FAST_COVERAGE_RESULT": validation_result}
    for selected_name, result_name in REPOSITORY_SELECTED_RESULTS.items():
        env[selected_name], env[result_name] = selected, validation_result
    result = _run_repository_gate(**env)
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("source", [*REPOSITORY_SELECTED_RESULTS, "FAST_COVERAGE_RESULT", "PLAN_RESULT", "FEEDBACK_RESULT"])
@pytest.mark.parametrize("failed_result", ["failure", "cancelled", "skipped"])
def test_repository_checks_rejects_each_missing_or_failed_selected_source(source: str, failed_result: str) -> None:
    if source in REPOSITORY_SELECTED_RESULTS:
        env = {source: "true", REPOSITORY_SELECTED_RESULTS[source]: failed_result}
    elif source == "FAST_COVERAGE_RESULT":
        env = {"COVERAGE_SELECTED": "true", "COVERAGE_RESULT": "success", source: failed_result}
    else:
        env = {source: failed_result}
    result = _run_repository_gate(**env)
    assert result.returncode != 0, (source, failed_result, result.stdout)


def test_reusable_validation_workflows_keep_internal_result_enforcement() -> None:
    contracts = {
        ".github/workflows/images.yml": (
            "images-gate",
            "images result",
            {
                "build": "BUILD_RESULT",
                "nebius-harness-build": "HARNESS_BUILD_RESULT",
            },
        ),
        ".github/workflows/cluster-smoke.yml": (
            "cluster-smoke-gate",
            "cluster-smoke result",
            {"cluster-contract": "CONTRACT_RESULT"},
        ),
        ".github/workflows/staging-smoke.yml": (
            "staging-smoke-gate",
            "staging-smoke result",
            {"system-smoke": "SYSTEM_SMOKE_RESULT"},
        ),
    }

    for workflow_path, (gate_id, gate_name, heavy_jobs) in contracts.items():
        workflow = _workflow(workflow_path)
        on_config = _workflow_on(workflow)
        jobs = workflow["jobs"]

        assert set(on_config) == {"workflow_call", "workflow_dispatch"}
        assert "concurrency" not in workflow
        assert "plan" in jobs
        assert "always()" in jobs[gate_id]["if"]
        assert gate_name in jobs[gate_id]["name"]
        assert set(jobs[gate_id]["needs"]) == {"plan", *heavy_jobs}

        plan_script = "\n".join(
            step.get("run", "") for step in jobs["plan"]["steps"] if "run" in step
        )
        assert "scripts/plan_ci_validations.py" in plan_script

        gate_step = next(
            step
            for step in jobs[gate_id]["steps"]
            if step.get("name", "").startswith("Enforce selected")
        )
        gate_env = gate_step["env"]
        gate_script = gate_step["run"]
        assert gate_env["PLAN_RESULT"] == "${{ needs.plan.result }}"
        assert gate_env["REQUIRED"] == "${{ needs.plan.outputs.required }}"
        assert '"$PLAN_RESULT"' in gate_script
        assert '"$REQUIRED"' in gate_script

        for job_id, result_env in heavy_jobs.items():
            assert "plan" in jobs[job_id]["needs"]
            assert "needs.plan.outputs.gate_mode == 'full'" in jobs[job_id]["if"]
            assert "needs.plan.outputs.required == 'true'" in jobs[job_id]["if"]
            assert gate_env[result_env] == f"${{{{ needs.{job_id}.result }}}}"
            assert f'"${result_env}"' in gate_script


def test_repository_gate_enforces_native_result_bindings() -> None:
    # Resolve the real workflow env before running its gate. Direct env injection
    # alone cannot catch a result wired to another successful dependency.
    gate = _workflow(".github/workflows/ci.yml")["jobs"]["repository-checks"]
    step = next(step for step in gate["steps"] if step.get("name") == "Enforce selected validation results")
    sources = {
        "workflow-plan", "fast-feedback", "lint-and-static", "locked-environments",
        "tests-root", "nebius-pool", "tests-packages", "runtime-payload", "nebius-iac",
        "go-checks", "integration", "integration-docker", "fast-coverage",
        "coverage-summary", "web-checks", "web-runtime",
    }
    outputs = dict.fromkeys((
        "lint_and_static", "locked_environments", "tests_root", "nebius_pool",
        "tests_packages", "runtime_payload", "nebius_iac", "go_checks",
        "integration", "integration_docker", "coverage_summary", "web_checks", "web_runtime",
    ), "true")
    outputs.update(gate_mode="full", docs_only="false")
    assert sources <= set(gate["needs"])
    assert "always()" in gate["if"]
    for failed in (None, *sorted(sources)):
        native = {
            f"${{{{ needs.{job}.result }}}}": "failure" if job == failed else "success"
            for job in gate["needs"]
        }
        native.update({f"${{{{ needs.workflow-plan.outputs.{key} }}}}": value
                       for key, value in outputs.items()})
        env = {key: native[value] for key, value in step["env"].items()}
        result = subprocess.run(["bash"], input=step["run"], text=True, capture_output=True,
                                env=env, check=False)
        assert (result.returncode == 0) is (failed is None), (failed, result.stdout, result.stderr)


@pytest.mark.parametrize("lane,output", [
    ("tests-root", "tests_root_matrix"), ("integration", "integration_matrix"),
    ("integration-docker", "integration_docker_matrix"),
])
@pytest.mark.parametrize("scope", ["all", "nebius"])
def test_python_test_shards_are_complete_and_non_overlapping(lane: str, output: str, scope: str) -> None:
    jobs = _workflow(".github/workflows/ci.yml")["jobs"]
    assert jobs[lane]["strategy"]["matrix"]["include"] == f"${{{{ fromJSON(needs.workflow-plan.outputs.{output}) }}}}"
    manifest = component_ownership.load_manifest(REPO_ROOT / "config/component-ownership.toml")
    tracked = component_ownership._tracked_paths(REPO_ROOT)
    matrix = component_ownership.test_shard_matrix(
        manifest, tracked_paths=tracked, lane=lane, repo_root=REPO_ROOT, test_scope=scope,
    )
    complete = component_ownership.selected_test_paths(
        manifest, tracked_paths=tracked, lane=lane, repo_root=REPO_ROOT, test_scope=scope,
    )
    shards = [set(component_ownership.selected_test_paths(
        manifest, tracked_paths=tracked, lane=lane, repo_root=REPO_ROOT, test_scope=scope,
        shard_index=row["shard_index"], shard_count=row["shard_count"],
    )) for row in matrix]
    assert shards and all(shards)
    assert all(left.isdisjoint(right) for i, left in enumerate(shards) for right in shards[i + 1:])
    assert set().union(*shards) == set(complete)
    if lane == "integration":
        auth, schema = "tests/integration/test_username_password_auth.py", "tests/integration/test_username_password_schema.py"
        assert any(auth in shard and schema in shard for shard in shards)
        assert complete.index(auth) < complete.index(schema)
    if lane == "integration-docker":
        consumers = component_ownership.guest_payload_consumers(complete, repo_root=REPO_ROOT)
        assert "tests/integration/test_codex_native_docker.py" in consumers
        assert [row["guest_payload"] for row in matrix] == [bool(shard & consumers) for shard in shards]
        assert "matrix.guest_payload" in next(step["if"] for step in jobs[lane]["steps"]
                  if step.get("name") == "Build and extract the admitted software-guest payload")


def test_ci_supports_merge_queue_merge_group_event() -> None:
    workflow = _workflow(".github/workflows/ci.yml")
    on_config = _workflow_on(workflow)

    assert "merge_group" in on_config
    assert "checks_requested" in on_config["merge_group"]["types"]


def test_ci_planner_uses_merge_base_for_pr_changed_paths_only() -> None:
    workflow = _workflow(".github/workflows/ci.yml")
    plan_script = next(
        step["run"]
        for step in workflow["jobs"]["workflow-plan"]["steps"]
        if step.get("id") == "plan"
    )

    assert 'pull_request)\n    git diff --name-only "$BASE_SHA...$HEAD_SHA"' in plan_script
    assert 'merge_group)\n    git diff --name-only "$BASE_SHA" "$HEAD_SHA"' in plan_script
    assert "pull_request|merge_group)" not in plan_script


def test_protected_workflows_cancel_only_authoritative_pr_runs() -> None:
    workflow = _workflow(".github/workflows/ci.yml")
    cancel = _normalized_expression(workflow["concurrency"]["cancel-in-progress"])
    assert cancel == (
        "${{ github.event_name == 'pull_request' && "
        "!(github.event.pull_request.draft || "
        "github.event.action == 'converted_to_draft') }}"
    )
    for path in GATE_CONTRACTS:
        if path != ".github/workflows/ci.yml":
            assert "concurrency" not in _workflow(path)


def test_protected_workflows_share_one_per_pr_admission_slot() -> None:
    workflow = _workflow(".github/workflows/ci.yml")
    group = _normalized_expression(workflow["concurrency"]["group"])

    assert "github.event.pull_request.number || github.ref" in group
    assert "github.run_id" in group
    assert "filtered-{0}" in group
    assert "authoritative" in group
    assert "github.event.pull_request.draft" in group
    assert "github.event.action == 'converted_to_draft'" in group
    assert "github.event.action == 'edited'" not in group
    assert "github.event.changes.base == null" not in group
    assert "fromJSON(" not in group
    assert "admission" not in group
    assert "background" not in group


def test_generic_rendered_images_are_covered_by_manifest_matrix() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "loom_cli",
            "cluster",
            "render",
            "--config",
            "tests/fixtures/cluster-render/staging.multinode.cluster.toml",
        ],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    active_images: set[str] = set()
    for document in yaml.safe_load_all(result.stdout):
        if not isinstance(document, dict) or document.get("kind") not in {
            "DaemonSet",
            "Deployment",
            "StatefulSet",
        }:
            continue
        spec = document.get("spec")
        if not isinstance(spec, dict) or spec.get("replicas", 1) == 0:
            continue
        template = spec.get("template")
        pod_spec = template.get("spec") if isinstance(template, dict) else None
        containers = pod_spec.get("containers") if isinstance(pod_spec, dict) else None
        if not isinstance(containers, list):
            continue
        for container in containers:
            image = container.get("image") if isinstance(container, dict) else None
            if not isinstance(image, str):
                continue
            image_name = image.rsplit("/", 1)[-1].split(":", 1)[0]
            if image_name.startswith("loom-"):
                active_images.add(image_name)

    manifest = component_ownership.load_manifest(REPO_ROOT / "config/component-ownership.toml")
    matrix_images = {
        row["image_name"]
        for row in component_ownership.release_images_for_runtime_policy(
            manifest,
            runtime_policy="start",
        )
    }
    assert active_images <= matrix_images
    assert "loom-family-orchestrator" in active_images
    assert "loom-worker" not in active_images


def test_web_nginx_has_same_raw_path_and_case_guard_as_controller() -> None:
    config = (REPO_ROOT / "deploy/nginx-spa.conf").read_text(encoding="utf-8")
    expected_map = (
        "map $request_uri $loom_ambiguous_path {\n"
        "    default 0;\n"
        "    ~*^[^?]*(?:%2f|%5c|\\x5c|//) 1;\n"
        '    "~*^/[^/?]*%[0-9a-f]{2}[^/?]*(?:/|[?]|$)" 1;\n'
        "    ~^/(?:dev|prod|staging)(?:/|\\?|$) 0;\n"
        "    ~*^/(?:dev|prod|staging)(?:/|\\?|$) 1;\n"
        "}\n"
    )

    assert expected_map in config
    assert "merge_slashes off;" in config
    assert "if ($loom_ambiguous_path) {\n        return 404;\n    }" in config
    assert "location = /staging {\n        return 308 /staging/$is_args$args;\n    }" in config
    assert "location ~ ^/(?:prod|dev|staging)/assets/(.+)$" in config
    assert ("location ~* ^/(?:.+/)+assets(?:/|$) {\n        return 404;\n    }") in config
    assert ("location ~* ^/(?:prod|dev|staging)(?:/|$) {\n        return 404;\n    }") in config


def test_staging_browser_route_smoke_waits_for_explicit_settled_state() -> None:
    main = (REPO_ROOT / "web/src/main.tsx").read_text(encoding="utf-8")
    smoke = (REPO_ROOT / "web/scripts/frontend-route-browser-smoke.mjs").read_text(encoding="utf-8")

    assert 'data-loom-mounted", "true"' in main
    assert 'data-loom-auth-settled", "true"' in main
    assert 'sessionStatus === "authenticated"' in main
    assert 'sessionStatus === "unavailable"' in main
    assert '? "error"' in main
    assert ': "anonymous"' in main
    assert 'waitUntil: "domcontentloaded"' in smoke
    assert "BLOCKING_ACTIVITY_QUIET_WINDOW_MS" in smoke
    assert "requestBlocksQuiescence" in smoke
    assert 'resourceType === "script"' in smoke
    assert "activeRequests.size === 0" in smoke
    assert "blocking browser activity did not become quiet" in smoke
    assert smoke.index("await page.close();") < smoke.index("const initialAnonymousAuthValid")
    assert smoke.index("await page.close();") < smoke.index("const observation = {")
    assert 'window.history.replaceState(null, "", directUrl)' in smoke
    assert "waitForTimeout(250)" not in smoke


def test_real_aws_s3_storage_smoke_is_not_a_pull_request_gate() -> None:
    workflow = _workflow(".github/workflows/staging-smoke.yml")
    jobs = workflow["jobs"]
    gate = jobs["staging-smoke-gate"]

    assert "smoke-storage-aws-s3" not in jobs
    assert set(gate["needs"]) == {"plan", "system-smoke"}
    assert "ci-aws" not in str(workflow)


def test_staging_gate_consumes_manifest_owned_system_smoke_lane() -> None:
    workflow = _workflow(".github/workflows/staging-smoke.yml")
    jobs = workflow["jobs"]
    system_smoke = jobs["system-smoke"]
    scripts = "\n".join(step.get("run", "") for step in system_smoke["steps"])
    pytest_step = next(
        step
        for step in system_smoke["steps"]
        if step.get("name") == "Pytest — manifest-owned system smoke lane"
    )
    diagnostics_step = next(
        step
        for step in system_smoke["steps"]
        if step.get("name") == "Show system-smoke compose diagnostics"
    )
    cleanup_step = next(
        step
        for step in system_smoke["steps"]
        if step.get("name") == "Cleanup system-smoke compose stack"
    )
    cleanup_script = " ".join(cleanup_step["run"].replace("\\\n", " ").split())

    assert set(system_smoke["needs"]) == {"plan"}
    assert "needs.plan.outputs.required == 'true'" in system_smoke["if"]
    assert "uv sync --locked --all-packages --extra dev --extra cluster --extra rollout" in scripts
    assert "uv pip check --python .venv/bin/python" in scripts
    assert (
        "uv run --no-sync python scripts/component_ownership.py test-paths --lane system-smoke"
        in scripts
    )
    assert 'pytest -m "${CI_PYTEST_MARKERS:-not legacy_pool}" --timeout=1200 --maxfail=1 "${test_paths[@]}"' in scripts
    assert (
        "--profile worker --profile task-image-builder down -v --remove-orphans --rmi local"
        in cleanup_script
    )
    assert pytest_step["env"]["LOOM_SYSTEM_SMOKE_DIAGNOSTICS"] == (
        "${{ runner.temp }}/system-smoke-compose.log"
    )
    assert diagnostics_step["if"] == "failure()"
    assert diagnostics_step["env"]["LOOM_SYSTEM_SMOKE_DIAGNOSTICS"] == (
        "${{ runner.temp }}/system-smoke-compose.log"
    )
    assert 'cat "${LOOM_SYSTEM_SMOKE_DIAGNOSTICS}"' in diagnostics_step["run"]
    assert cleanup_step["if"] == "always()"
    assert cleanup_step["env"]["LOOM_WORKER_TOKEN"] == "unused-cleanup-token"
    assert cleanup_step["env"]["LOOM_TASK_IMAGE_BUILDER_TOKEN"] == ("unused-cleanup-builder-token")


def test_repository_checks_writes_default_fast_coverage_summary() -> None:
    workflow = _workflow(".github/workflows/ci.yml")
    jobs = workflow["jobs"]
    coverage_step = next(
        step
        for step in jobs["fast-coverage"]["steps"]
        if step.get("name") == "Coverage gate + summary (fast tier)"
    )

    assert "GITHUB_STEP_SUMMARY" in coverage_step["run"]
    assert "coverage_floor=(--fail-under=70)" in coverage_step["run"]
    assert '"${CI_TEST_SCOPE:-all}" == "all"' in coverage_step["run"]


def test_combined_coverage_summary_is_opt_in() -> None:
    workflow = _workflow(".github/workflows/ci.yml")
    coverage_summary_if = workflow["jobs"]["coverage-summary"]["if"]

    assert "needs.workflow-plan.outputs.coverage_summary == 'true'" in coverage_summary_if


def test_repository_checks_uses_lightweight_coverage_tooling() -> None:
    workflow = _workflow(".github/workflows/ci.yml")
    fast_steps = workflow["jobs"]["fast-coverage"]["steps"]
    step_names = {step.get("name") for step in fast_steps}
    run_blocks = "\n".join(step.get("run", "") for step in fast_steps)

    assert "Install exact uv" in step_names
    assert "Set up Python 3.11" not in step_names
    assert "uv sync --locked --only-group ci-coverage --python 3.11" in run_blocks
    assert "uv run --no-sync coverage combine" in run_blocks
    assert "uv run --no-sync coverage xml" in run_blocks


def test_lint_and_static_does_not_restore_opaque_analysis_state() -> None:
    workflow = _workflow(".github/workflows/ci.yml")
    steps = workflow["jobs"]["lint-and-static"]["steps"]
    assert all(not str(step.get("uses", "")).startswith("actions/cache@") for step in steps)
    assert "Cache mypy" not in {step.get("name") for step in steps}


@pytest.mark.parametrize("lane", ["tests-root", "tests-packages", "integration"])
@pytest.mark.parametrize("coverage_enabled", ["false", "true"])
@pytest.mark.parametrize("test_exit", [0, 7])
def test_pytest_workflow_preserves_tests_and_failures_with_optional_integration_coverage(
    tmp_path: Path, lane: str, coverage_enabled: str, test_exit: int,
) -> None:
    job = _workflow(".github/workflows/ci.yml")["jobs"][lane]
    step = next(step for step in job["steps"] if step.get("name", "").startswith("Pytest"))
    uv = tmp_path / "uv"
    uv.write_text(
        f"#!{Path(sys.executable).resolve()}\n"
        "import json, os, sys\n"
        "from pathlib import Path\n"
        "if 'test-paths' in sys.argv:\n"
        "    print('tests/selected_first.py\\ntests/selected_second.py')\n"
        "elif 'pytest' in sys.argv:\n"
        "    Path(os.environ['ARGV_FILE']).write_text(json.dumps(sys.argv[1:]))\n"
        "    sys.exit(int(os.environ['TEST_EXIT']))\n"
        "else:\n"
        "    sys.exit(99)\n",
    )
    uv.chmod(0o755)
    argv_file = tmp_path / "argv.json"
    manifest_file = tmp_path / "selected-manifest.txt"
    manifest_file.write_text("tests/selected_first.py\ntests/selected_second.py\n")
    script = step["run"].replace("${{ steps.manifest.outputs.paths_file }}", str(manifest_file))
    result = subprocess.run(
        ["bash", "-c", script], cwd=REPO_ROOT, capture_output=True, text=True,
        env={**os.environ, "PATH": f"{tmp_path}:{os.environ['PATH']}",
             "RUNNER_TEMP": str(tmp_path), "SHARD_INDEX": "0", "SHARD_COUNT": "2",
             "COVERAGE_ENABLED": coverage_enabled, "ARGV_FILE": str(argv_file),
             "TEST_CHANGED_PATHS": "[]",
             "TEST_EXIT": str(test_exit)},
    )
    assert result.returncode == test_exit, result.stderr
    args = json.loads(argv_file.read_text())
    assert [arg for arg in args if arg.startswith("tests/")] == [
        "tests/selected_first.py", "tests/selected_second.py",
    ]
    instrumented = coverage_enabled == "true"
    assert ("--cov=src" in args) is instrumented
    assert ("--cov=packages" in args) is instrumented
    assert ("--cov-report=" in args) is instrumented
    assert "--cov-report=term" not in args and "--cov-report=xml" not in args
    if not instrumented:
        assert args[args.index("-p") + 1] == "no:cov"
    if lane == "integration":
        assert args[args.index("-m") + 1] == "not docker and (not legacy_pool)"
        assert job["env"]["COVERAGE_ENABLED"] == "${{ needs.workflow-plan.outputs.coverage_summary }}"
        upload = next(s for s in job["steps"] if s.get("name") == "Upload integration coverage data")
        assert "env.COVERAGE_ENABLED == 'true'" in upload["if"]


@pytest.mark.parametrize("manual_request", [None, "coverage_summary", "legacy_compatibility"])
def test_manual_requests_select_their_integration_tiers_through_workflow_planner(
    tmp_path: Path, manual_request: str | None,
) -> None:
    workflow = _workflow(".github/workflows/ci.yml")
    options = _workflow_on(workflow)["workflow_dispatch"]["inputs"]
    native = {f"inputs.{key}": option["default"] for key, option in options.items()}
    if manual_request is not None:
        native[f"inputs.{manual_request}"] = True
    native.update({"github.event_name": "workflow_dispatch", "github.ref": "refs/heads/dev",
                   "github.base_ref": ""})

    def resolve(expression):
        source = re.sub(r"(?:inputs|github)\.[A-Za-z0-9_.]+",
                        lambda match: repr(native[match[0]]), expression.strip()[3:-2])
        return eval(source.replace("&&", " and ").replace("||", " or "), {"__builtins__": {}})

    step = next(s for s in workflow["jobs"]["workflow-plan"]["steps"] if s.get("id") == "plan")
    dispatch_env = {key: str(resolve(value)).lower() for key, value in step["env"].items()
                    if key in {"DISPATCH_INTEGRATION", "DISPATCH_INTEGRATION_DOCKER",
                               "DISPATCH_COVERAGE_SUMMARY"}}
    test_scope = resolve(workflow["env"]["CI_TEST_SCOPE"])
    assert test_scope == ("all" if manual_request == "legacy_compatibility" else "nebius")
    git = tmp_path / "git"
    real_git = shutil.which("git")
    assert real_git is not None
    git.write_text("#!/bin/sh\n"
                   f'if [ "$1" = ls-files ]; then exec "{real_git}" "$@"; fi\n'
                   "printf '%s\\n' docs/user-guide.md\n")
    git.chmod(0o755)
    output = tmp_path / "output"
    script = step["run"].replace("/tmp/loom-changed-files.txt", str(tmp_path / "changes"))
    result = subprocess.run(
        ["bash", "-c", script], cwd=REPO_ROOT, capture_output=True, text=True,
        env={**os.environ, "PATH": f"{tmp_path}:{os.environ['PATH']}",
             "EVENT_NAME": "workflow_dispatch", "HEAD_SHA": "a" * 40,
             "PR_LABELS_JSON": "[]", "PR_ACTION": "", "PR_ACTION_LABEL": "",
             "PR_DRAFT": "false", "PR_BASE_CHANGED": "false",
             **dispatch_env, "CI_TEST_SCOPE": test_scope, "GITHUB_OUTPUT": str(output)},
    )
    assert result.returncode == 0, result.stderr
    values = dict(line.split("=", 1) for line in output.read_text().splitlines())
    assert values["coverage_summary"] == str(manual_request == "coverage_summary").lower()
    assert values["integration"] == str(manual_request is not None).lower()
    assert values["integration_docker"] == str(manual_request == "legacy_compatibility").lower()
    assert values["docs_only"] == str(manual_request is None).lower()


@pytest.mark.parametrize("job_name", ["tests-root", "integration"])
def test_affected_tests_wait_for_static_failures_and_allow_unselected_lanes(job_name: str) -> None:
    from tests.ops.test_ci_images_parallel_builds import _condition

    job = _workflow(".github/workflows/ci.yml")["jobs"][job_name]
    values = {f"needs.{name}.result": "skipped" for name in job["needs"]}
    values["needs.workflow-plan.result"] = "success"
    values.update({f"needs.workflow-plan.outputs.{name}": "false" for name in (
        "lint_and_static", "locked_environments", "tests_packages", "go_checks", "nebius_iac",
    )})
    values["needs.workflow-plan.outputs.tests_root"] = "true"
    values["needs.workflow-plan.outputs.integration"] = "true"
    values["needs.workflow-plan.outputs.gate_mode"] = "full"
    assert _condition(job["if"], values)
    values["needs.workflow-plan.outputs.lint_and_static"] = "true"
    for result in ("failure", "skipped", "cancelled"):
        values["needs.lint-and-static.result"] = result
        assert not _condition(job["if"], values)
    values["needs.lint-and-static.result"] = "success"
    assert _condition(job["if"], values)
    values["needs.workflow-plan.outputs.gate_mode"] = "filtered"
    assert not _condition(job["if"], values)


def test_admission_matrices_stop_siblings_but_manual_diagnostics_continue() -> None:
    from tests.ops.test_ci_images_parallel_builds import _condition

    jobs = _workflow(".github/workflows/ci.yml")["jobs"]
    for job_name in ("tests-root", "integration", "nebius-pool", "integration-docker"):
        policy = jobs[job_name]["strategy"]["fail-fast"]
        for event in ("pull_request", "merge_group", "workflow_dispatch", "schedule", "push"):
            assert _condition(policy.strip("${} "), {"github.event_name": event}) == (
                event in {"pull_request", "merge_group"}
            )
