"""Task bundle compatibility preflight diagnostics (#387/#379/#369)."""

from __future__ import annotations

from pathlib import Path

import pytest

from loom.task_bundle_compat import (
    CompatibilitySeverity,
    TaskBundleCompatibilityIssue,
    collect_task_dir_compatibility_issues,
    validate_task_dir_compatibility,
)


def _write_task_dir(tmp_path: Path, dockerfile_text: str) -> Path:
    task_dir = tmp_path / "task"
    environment = task_dir / "environment"
    environment.mkdir(parents=True)
    (task_dir / "task.toml").write_text("schema_version = '1'\n")
    (environment / "Dockerfile").write_text(dockerfile_text, encoding="utf-8")
    return task_dir


def _write_arm64_task_dir(tmp_path: Path, dockerfile_text: str) -> Path:
    task_dir = _write_task_dir(tmp_path, dockerfile_text)
    (task_dir / "task.toml").write_text(
        "schema_version = '1'\n"
        "[task]\n"
        "id = 'arm-task'\n"
        "name = 'arm task'\n"
        "[environment]\n"
        "os = 'linux'\n"
        "cpu_arch = 'arm64'\n",
    )
    return task_dir


def test_issue_schema_is_user_readable_and_structured() -> None:
    issue = TaskBundleCompatibilityIssue(
        code="TASK_COMPAT_DNS_MUTATION",
        severity=CompatibilitySeverity.ERROR,
        path="environment/Dockerfile",
        line=3,
        phase="agent_layer_build",
        message="Dockerfile mutates DNS configuration before agent setup",
        hint="Move DNS breakage into the task runtime after agent installation.",
    )

    assert issue.model_dump(mode="json") == {
        "code": "TASK_COMPAT_DNS_MUTATION",
        "severity": "error",
        "path": "environment/Dockerfile",
        "line": 3,
        "phase": "agent_layer_build",
        "message": "Dockerfile mutates DNS configuration before agent setup",
        "hint": "Move DNS breakage into the task runtime after agent installation.",
        "evidence": {},
    }


def test_preflight_reports_dns_runtime_mutation(tmp_path: Path) -> None:
    task_dir = _write_task_dir(
        tmp_path,
        "FROM debian:bookworm\n"
        "COPY broken_resolv.conf /app/broken_resolv.conf\n"
        "RUN cp /app/broken_resolv.conf /etc/resolv.conf\n"
        "RUN sed -i 's/^hosts:.*/hosts: files/' /etc/nsswitch.conf\n",
    )

    issues = collect_task_dir_compatibility_issues(task_dir)

    assert [issue.code for issue in issues] == [
        "TASK_COMPAT_DNS_MUTATION",
        "TASK_COMPAT_DNS_MUTATION",
    ]
    assert issues[0].severity == CompatibilitySeverity.ERROR
    assert issues[0].path == "environment/Dockerfile"
    assert issues[0].line == 3
    assert issues[0].phase == "agent_layer_build"
    assert "DNS" in issues[0].message
    assert "/etc/resolv.conf" in issues[0].hint
    assert issues[0].evidence["target"] == "/etc/resolv.conf"


def test_preflight_reports_environment_file_expected_at_app_root(tmp_path: Path) -> None:
    task_dir = _write_task_dir(
        tmp_path,
        "FROM debian:bookworm\n"
        "COPY . /app/\n"
        "RUN chmod +x /app/setup_repo.sh && /app/setup_repo.sh\n",
    )
    (task_dir / "environment" / "setup_repo.sh").write_text("#!/bin/sh\n")

    issues = collect_task_dir_compatibility_issues(task_dir)

    assert len(issues) == 1
    issue = issues[0]
    assert issue.code == "TASK_COMPAT_APP_PATH_MISSING"
    assert issue.severity == CompatibilitySeverity.ERROR
    assert issue.path == "environment/Dockerfile"
    assert issue.line == 3
    assert issue.phase == "task_image_build"
    assert "environment/setup_repo.sh" in issue.message
    assert "/app/setup_repo.sh" in issue.message
    assert issue.evidence == {
        "referenced_path": "/app/setup_repo.sh",
        "existing_path": "environment/setup_repo.sh",
    }


def test_preflight_allows_app_root_reference_when_file_exists_at_context_root(
    tmp_path: Path,
) -> None:
    task_dir = _write_task_dir(
        tmp_path,
        "FROM debian:bookworm\n"
        "COPY . /app/\n"
        "RUN chmod +x /app/setup_repo.sh && /app/setup_repo.sh\n",
    )
    (task_dir / "setup_repo.sh").write_text("#!/bin/sh\n")

    assert collect_task_dir_compatibility_issues(task_dir) == []


def test_validate_task_dir_compatibility_raises_readable_summary(tmp_path: Path) -> None:
    task_dir = _write_task_dir(
        tmp_path,
        "FROM debian:bookworm\n"
        "COPY . /app/\n"
        "RUN cp /app/broken_resolv.conf /etc/resolv.conf\n",
    )

    with pytest.raises(ValueError) as excinfo:
        validate_task_dir_compatibility(task_dir)

    message = str(excinfo.value)
    assert "TASK_COMPAT_DNS_MUTATION" in message
    assert "environment/Dockerfile:3" in message
    assert "Move DNS breakage" in message


def test_preflight_reports_amd64_platform_for_arm64_task(tmp_path: Path) -> None:
    task_dir = _write_arm64_task_dir(
        tmp_path,
        "FROM --platform=linux/amd64 ubuntu:24.04\n"
        "RUN echo ok\n",
    )

    issues = collect_task_dir_compatibility_issues(task_dir)

    assert len(issues) == 1
    assert issues[0].code == "TASK_COMPAT_AMD64_PLATFORM"
    assert issues[0].phase == "task_image_build"
    assert issues[0].evidence == {"target_arch": "arm64", "platform": "linux/amd64"}


def test_preflight_reports_amd64_binary_url_for_arm64_task(tmp_path: Path) -> None:
    task_dir = _write_arm64_task_dir(
        tmp_path,
        "FROM ubuntu:24.04\n"
        "RUN curl -LO https://releases.hashicorp.com/terraform/1.7.0/"
        "terraform_1.7.0_linux_amd64.zip\n",
    )

    issues = collect_task_dir_compatibility_issues(task_dir)

    assert len(issues) == 1
    assert issues[0].code == "TASK_COMPAT_AMD64_ONLY_ASSET"
    assert issues[0].phase == "task_image_build"
    assert "linux_amd64" in issues[0].message
    assert issues[0].evidence["target_arch"] == "arm64"


@pytest.mark.parametrize(
    "heredoc",
    [
        "RUN python3 <<'PY'\nfrom datetime import datetime\nPY\n",
        'RUN python3 <<"PY"\nfrom datetime import datetime\nPY\n',
        "RUN <<-'SCRIPT'\n\tfrom datetime import datetime\n\tSCRIPT\n",
        "COPY <<FIRST <<'SECOND' /tmp/\nFROM alpine:3.20\nFIRST\nSHELL []\nSECOND\n",
        "RUN python3 \\\n  <<'PY'\nfrom datetime import datetime\nPY\n",
    ],
)
def test_heredoc_content_cannot_reset_workdir(tmp_path: Path, heredoc: str) -> None:
    task_dir = _write_task_dir(
        tmp_path,
        "FROM debian:bookworm\nWORKDIR /app\n" + heredoc + "RUN echo ok > /app/result\n",
    )
    assert collect_task_dir_compatibility_issues(task_dir) == []


def test_real_stage_after_heredoc_resets_workdir(tmp_path: Path) -> None:
    task_dir = _write_task_dir(
        tmp_path,
        "FROM debian:bookworm\nWORKDIR /app\n"
        "RUN cat <<EOF\nFROM fake\nEOF\n"
        "FROM debian:bookworm\nRUN echo ok > /app/result\n",
    )
    issues = collect_task_dir_compatibility_issues(task_dir)
    assert [(issue.code, issue.line) for issue in issues] == [
        ("TASK_COMPAT_APP_PARENT_MISSING", 7),
    ]


def test_unterminated_heredoc_reports_parse_boundary(tmp_path: Path) -> None:
    task_dir = _write_task_dir(
        tmp_path,
        "FROM debian:bookworm\nRUN cat <<EOF\nFROM fake\n",
    )
    issues = collect_task_dir_compatibility_issues(task_dir)
    assert [(issue.code, issue.line) for issue in issues] == [("TASK_COMPAT_DOCKERFILE_PARSE", 2)]
    assert "unterminated heredoc" in issues[0].message


@pytest.mark.parametrize("build_declaration", ["", "dockerfile = 'environment/Dockerfile'\n"])
def test_prebuilt_recipe_diagnostic_depends_on_actual_build(
    tmp_path: Path, build_declaration: str,
) -> None:
    task_dir = _write_task_dir(
        tmp_path, "FROM ubuntu:24.04\nRUN apt-get update && apt-get purge -y povray* || true\n",
    )
    (task_dir / "task.toml").write_text(
        "[environment]\ndocker_image = 'example/task:release'\n" + build_declaration,
    )
    issues = collect_task_dir_compatibility_issues(task_dir)
    assert len(issues) == 1
    assert issues[0].code == "TASK_COMPAT_BROAD_TRAILING_TRUE"
    if build_declaration:
        assert issues[0].severity == CompatibilitySeverity.ERROR
        with pytest.raises(ValueError, match="TASK_COMPAT_BROAD_TRAILING_TRUE"):
            validate_task_dir_compatibility(task_dir)
    else:
        assert issues[0].severity == CompatibilitySeverity.WARNING
        assert issues[0].evidence["build_usage"] == "unused_prebuilt_recipe"
        validate_task_dir_compatibility(task_dir)


def test_prebuilt_task_still_checks_verifier_dockerfile(tmp_path: Path) -> None:
    task_dir = _write_task_dir(tmp_path, "FROM ubuntu:24.04\n")
    (task_dir / "task.toml").write_text("[environment]\ndocker_image = 'example/task:release'\n")
    tests = task_dir / "tests"
    tests.mkdir()
    (tests / "Dockerfile").write_text(
        "FROM ubuntu:24.04\nRUN apt-get update && apt-get purge -y povray* || true\n",
    )
    with pytest.raises(ValueError, match="TASK_COMPAT_BROAD_TRAILING_TRUE"):
        validate_task_dir_compatibility(task_dir)
