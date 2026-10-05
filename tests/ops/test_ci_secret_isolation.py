from __future__ import annotations

import os
import platform
import re
import subprocess
from pathlib import Path
from typing import Any

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]


def _workflow(path: str) -> dict[str, Any]:
    return yaml.safe_load((REPO_ROOT / path).read_text(encoding="utf-8"))


def _workflow_on(workflow: dict[str, Any]) -> dict[str, Any]:
    # PyYAML treats the unquoted GitHub Actions key `on` as YAML 1.1 bool.
    return workflow.get("on", workflow.get(True))


def _checkout_steps(job: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        step
        for step in job.get("steps", [])
        if str(step.get("uses", "")).startswith("actions/checkout@")
    ]


def _run_blocks(job: dict[str, Any]) -> list[str]:
    return [str(step["run"]) for step in job.get("steps", []) if "run" in step]


def _named_step(job: dict[str, Any], name: str) -> dict[str, Any]:
    return next(step for step in job["steps"] if step.get("name") == name)


def _normalized_expression(value: str) -> str:
    return " ".join(value.split())


def _native_arch_env() -> dict[str, str]:
    machine = platform.machine().lower()
    if machine in {"arm64", "aarch64"}:
        return {"ARCHITECTURE": "arm64", "PLATFORM": "linux/arm64"}
    return {"ARCHITECTURE": "amd64", "PLATFORM": "linux/amd64"}


def _run_validation_step(
    step: dict[str, Any],
    *,
    env: dict[str, str],
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["bash"],
        input=step["run"],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        env={**os.environ, **env},
        check=False,
    )


@pytest.mark.parametrize(
    ("job_name", "matrix_output"),
    [("build", "ordinary_builds")],
)
def test_images_untrusted_build_is_read_only_and_cannot_publish_or_write_cache(
    job_name: str, matrix_output: str
) -> None:
    workflow = _workflow(".github/workflows/images.yml")
    build = workflow["jobs"][job_name]

    assert workflow["permissions"] == {"contents": "read"}
    assert build["permissions"] == {"contents": "read"}
    assert _normalized_expression(build["if"]) == (
        "needs.plan.outputs.gate_mode == 'full' && "
        "needs.plan.outputs.required == 'true' && "
        f"needs.plan.outputs.{matrix_output} != '[]'"
    )
    assert _checkout_steps(build)
    assert all(
        step.get("with", {}).get("persist-credentials") is False for step in _checkout_steps(build)
    )

    script = "\n".join(_run_blocks(build))
    assert "docker login" not in script
    assert "--push" not in script
    assert "type=docker,dest=${archive}" in script
    assert ".docker.tar" in script
    assert "type=oci" not in script
    assert "type=registry" not in script
    assert "ghcr.io" not in script
    assert "--cache-from" not in script
    assert "--cache-to" not in script
    assert "secrets." not in str(build)
    assert "${{" not in script




def test_images_manual_dispatch_is_build_only() -> None:
    from tests.ops.test_ci_images_parallel_builds import _condition

    workflow = _workflow(".github/workflows/images.yml")
    on_config = _workflow_on(workflow)
    assert "workflow_dispatch" in on_config
    assert not (on_config["workflow_dispatch"] or {}).get("inputs")
    for ref in ("refs/heads/main", "refs/heads/dev", "refs/heads/feature"):
        values = {
            "github.event_name": "workflow_dispatch",
            "github.ref": ref,
            "needs.plan.result": "success",
            "needs.trivy-binary.result": "success",
            "needs.plan.outputs.gate_mode": "full",
            "needs.plan.outputs.required": "true",
            "needs.plan.outputs.ordinary_builds": '[{"image":"service"}]',
        }
        assert _condition(workflow["jobs"]["build"]["if"], values)


def test_images_permissions_are_an_exact_job_allowlist() -> None:
    workflow = _workflow(".github/workflows/images.yml")
    jobs = workflow["jobs"]

    assert set(jobs) == {
        "plan",
        "trivy-binary",
        "build",
        "nebius-harness-build",
        "images-gate",
    }
    for job_name in (
        "plan",
        "trivy-binary",
        "build",
        "nebius-harness-build",
        "images-gate",
    ):
        effective = jobs[job_name].get("permissions", workflow["permissions"])
        assert effective == {"contents": "read"}
        assert "environment" not in jobs[job_name]
        assert "id-token" not in effective
        assert all(value != "write" for value in effective.values())


def test_images_secret_and_cache_authority_is_exact() -> None:
    workflow = _workflow(".github/workflows/images.yml")
    secret_references = [
        value
        for job in workflow["jobs"].values()
        for step in job.get("steps", [])
        for value in step.get("env", {}).values()
        if isinstance(value, str) and "secrets." in value
    ]

    assert secret_references == []
    for job in workflow["jobs"].values():
        assert job.get("continue-on-error") is not True
        for step in job.get("steps", []):
            assert not str(step.get("uses", "")).startswith("actions/cache@")
            assert step.get("continue-on-error") is not True


@pytest.mark.parametrize(
    "workflow_path",
    [".github/workflows/images.yml", ".github/workflows/staging-smoke.yml"],
)
def test_untrusted_workflows_disable_setup_uv_cache_writes(workflow_path: str) -> None:
    workflow = _workflow(workflow_path)
    setup_steps = [
        step
        for job in workflow["jobs"].values()
        for step in job.get("steps", [])
        if str(step.get("uses", "")).startswith("astral-sh/setup-uv@")
    ]

    for step in setup_steps:
        inputs = step.get("with", {})
        assert inputs.get("enable-cache") is True
        assert inputs.get("save-cache") == (
            "${{ github.event_name != 'pull_request' && github.event_name != 'merge_group' }}"
        )
    for job in workflow["jobs"].values():
        assert job.get("continue-on-error") is not True
        for step in job.get("steps", []):
            assert not str(step.get("uses", "")).startswith("actions/cache@")
            assert step.get("continue-on-error") is not True


def test_minio_fixture_cache_is_read_only_on_untrusted_events() -> None:
    from tests.ops.test_ci_images_parallel_builds import _condition

    def enabled(step: dict[str, Any], key: str, event: str) -> bool:
        expression = step["if"].replace("steps.manifest.outputs.minio_cache_key", repr(key))
        return _condition(expression, {"github.event_name": event})

    jobs = _workflow(".github/workflows/ci.yml")["jobs"]
    for name in ("integration", "integration-docker"):
        steps = jobs[name]["steps"]
        restore = next(step for step in steps if str(step.get("uses", "")).startswith("actions/cache/restore@"))
        save = next(step for step in steps if str(step.get("uses", "")).startswith("actions/cache/save@"))
        preparation = next(step["run"] for step in steps if "--cache-dir" in step.get("run", ""))
        export_guard = re.search(r'--save-cache "\$\{\{(.+?)\}\}"', preparation)
        assert export_guard is not None
        assert not any(str(step.get("uses", "")).startswith("actions/cache@") for step in steps)
        for event in ("pull_request", "merge_group", "push", "schedule", "workflow_dispatch"):
            assert _condition(export_guard[1], {"github.event_name": event}) is (
                event not in ("pull_request", "merge_group")
            )
            for key in ("none", "minio-source-recipe"):
                assert enabled(restore, key, event) is (key != "none")
                assert enabled(save, key, event) is (key != "none" and event not in ("pull_request", "merge_group"))


def test_staging_pr_gate_is_credential_free_and_does_not_depend_on_real_aws() -> None:
    workflow = _workflow(".github/workflows/staging-smoke.yml")
    jobs = workflow["jobs"]
    gate = jobs["staging-smoke-gate"]

    assert workflow["permissions"] == {"contents": "read"}
    assert "smoke-storage-aws-s3" not in jobs
    assert set(gate["needs"]) == {"plan", "system-smoke"}

    gate_step = _named_step(gate, "Enforce selected staging smoke results")
    assert "AWS_S3_RESULT" not in gate_step["env"]
    assert "AWS_S3_RESULT" not in gate_step["run"]
    assert "${{ secrets." not in str(workflow)
    assert "ci-aws" not in str(workflow)

    for job in jobs.values():
        effective = job.get("permissions", workflow["permissions"])
        assert effective == {"contents": "read"}
        assert "environment" not in job
        assert "id-token" not in effective
        assert all(value != "write" for value in effective.values())
        assert all(
            step.get("with", {}).get("persist-credentials") is False
            for step in _checkout_steps(job)
        )


@pytest.mark.parametrize(
    "workflow_path",
    [
        ".github/workflows/images.yml",
        ".github/workflows/staging-smoke.yml",
    ],
)
def test_untrusted_workflows_never_use_pull_request_target(workflow_path: str) -> None:
    workflow = _workflow(workflow_path)
    assert "pull_request_target" not in _workflow_on(workflow)


@pytest.mark.parametrize(
    "workflow_path",
    [
        ".github/workflows/images.yml",
        ".github/workflows/staging-smoke.yml",
    ],
)
def test_untrusted_workflow_shell_receives_context_only_through_env(
    workflow_path: str,
) -> None:
    workflow = _workflow(workflow_path)
    for job_name, job in workflow["jobs"].items():
        for step in job.get("steps", []):
            if "run" in step:
                assert "${{" not in step["run"], (workflow_path, job_name, step)


@pytest.mark.parametrize(
    ("job_name", "field", "payload", "error_marker"),
    [
        ("build", "IMAGE_NAME", "worker$(id)", "component ownership validation failed:"),
        (
            "build",
            "IMAGE_DIGEST_NAME",
            "loom-worker; id",
            "component ownership validation failed:",
        ),
        (
            "build",
            "DOCKERFILE",
            "../deploy/Dockerfile.worker",
            "component ownership validation failed:",
        ),
        ("build", "BUILD_CONTEXT", "..", "component ownership validation failed:"),
        ("build", "EVENT_NAME", "pull_request\npush", "FAIL:"),
        ("build", "REF_NAME", "dev; id", "FAIL:"),
        ("build", "PR_NUMBER", "--help", "FAIL:"),
        ("build", "HEAD_SHA", "abc`id`", "FAIL:"),
    ],
)
def test_image_input_validation_rejects_shell_metacharacters_and_ambiguous_values(
    job_name: str,
    field: str,
    payload: str,
    error_marker: str,
) -> None:
    workflow = _workflow(".github/workflows/images.yml")
    step = _named_step(workflow["jobs"][job_name], "Validate image build inputs")
    env = {
        "IMAGE_SET": "nebius",
        "IMAGE_NAME": "service",
        "IMAGE_DIGEST_NAME": "loom-service",
        "DOCKERFILE": "deploy/Dockerfile.service",
        "BUILD_CONTEXT": ".",
        "EVENT_NAME": "pull_request",
        "REF_NAME": "feature-safe",
        "PR_NUMBER": "42",
        "HEAD_SHA": "a" * 40,
        "BASE_SHA": "b" * 40,
        "REPOSITORY_OWNER": "qianyi-sun",
        "GHCR_ACTOR": "qianyi-sun",
        **_native_arch_env(),
    }
    env[field] = payload

    result = _run_validation_step(step, env=env)

    assert result.returncode != 0, (job_name, field, payload, result.stdout)
    assert error_marker in result.stderr


@pytest.mark.parametrize("job_name", ["build"])
def test_image_input_validation_never_evaluates_command_substitution(
    tmp_path: Path, job_name: str
) -> None:
    workflow = _workflow(".github/workflows/images.yml")
    step = _named_step(workflow["jobs"][job_name], "Validate image build inputs")
    sentinel = tmp_path / "shell-injection-ran"
    result = _run_validation_step(
        step,
        env={
            "IMAGE_SET": "nebius",
            "IMAGE_NAME": f"worker$(touch {sentinel})",
            "IMAGE_DIGEST_NAME": "loom-service",
            "DOCKERFILE": "deploy/Dockerfile.service",
            "BUILD_CONTEXT": ".",
            "EVENT_NAME": "pull_request",
            "REF_NAME": "42/merge",
            "PR_NUMBER": "42",
            "HEAD_SHA": "a" * 40,
            **_native_arch_env(),
        },
    )

    assert result.returncode != 0
    assert not sentinel.exists()


@pytest.mark.parametrize(
    ("job_name", "event_name", "ref_name", "pr_number"),
    [
        ("build", "pull_request", "42/merge", "42"),
        (
            "build",
            "merge_group",
            "gh-readonly-queue/dev/pr-42-deadbeef",
            "",
        ),
        ("build", "workflow_dispatch", "codex/ci-secret-isolation", ""),
    ],
)
def test_image_input_validation_accepts_actual_github_context_shapes(
    job_name: str,
    event_name: str,
    ref_name: str,
    pr_number: str,
) -> None:
    workflow = _workflow(".github/workflows/images.yml")
    step = _named_step(workflow["jobs"][job_name], "Validate image build inputs")
    result = _run_validation_step(
        step,
        env={
            "IMAGE_SET": "nebius",
        "IMAGE_NAME": "service",
            "IMAGE_DIGEST_NAME": "loom-service",
            "DOCKERFILE": "deploy/Dockerfile.service",
            "BUILD_CONTEXT": ".",
            "EVENT_NAME": event_name,
            "REF_NAME": ref_name,
            "PR_NUMBER": pr_number,
            "HEAD_SHA": "a" * 40,
            "BASE_SHA": "b" * 40,
            "REPOSITORY_OWNER": "qianyi-sun",
            "GHCR_ACTOR": "github-actions[bot]",
            **_native_arch_env(),
        },
    )

    assert result.returncode == 0, result.stderr


def test_secret_isolation_contract_does_not_depend_on_one_reviewer() -> None:
    assert not (REPO_ROOT / ".github/CODEOWNERS").exists()
