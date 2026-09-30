"""Whole-input diagnostics must not confuse profile admission with equivalence."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
import tomli_w

from loom_cli import datasets_cmd


def _write_bundle(root: Path, name: str, **environment: Any) -> Path:
    bundle = root / "tasks" / name
    (bundle / "environment").mkdir(parents=True)
    (bundle / "tests").mkdir()
    config = {
        "task": {"id": name, "name": name},
        "environment": {
            "os": "linux",
            "dockerfile": "environment/Dockerfile",
            "docker_build_context": "environment",
            "workdir": "/app",
            "user": "agent",
            "network_policies_supported": ["gateway-only"],
            "baseline_network_policy": {"kind": "gateway-only"},
            **environment,
        },
        "agent": {"name": "oracle"},
        "verifier": {"name": "script", "args": {"script_path": "verifier/run.sh"}},
        "steps": [{"name": "main"}],
    }
    (bundle / "task.toml").write_text(tomli_w.dumps(config))
    (bundle / "instruction.md").write_text("Solve the task.\n")
    (bundle / "environment/Dockerfile").write_text("FROM ubuntu:24.04\nWORKDIR /app\n")
    (bundle / "tests/test.sh").write_text(
        "#!/bin/bash\n"
        "curl -LsSf https://astral.sh/uv/0.9.5/install.sh | sh\n"
        "source $HOME/.local/bin/env\n"
        "uvx -p 3.13 -w pytest==8.4.1 pytest /tests/test_outputs.py -rA\n"
        "if [ $? -eq 0 ]; then echo 1 > /logs/verifier/reward.txt; "
        "else echo 0 > /logs/verifier/reward.txt; fi\n"
    )
    (bundle / "tests/test_outputs.py").write_text("def test_result():\n    assert True\n")
    return bundle


def _report(root: Path, capsys: pytest.CaptureFixture[str], *, profile: bool = True) -> tuple[int, Any]:
    args = [
        "validate-local", str(root), "--id", "slice", "--display-name", "Slice",
        "--series", "custom", "--license-spdx", "MIT", "--source-subdir", "tasks",
        "--compatibility-report", "--json",
    ]
    if profile:
        args.extend(["--execution-profile", "nebius-terminus"])
    rc = datasets_cmd.dispatch(args)
    captured = capsys.readouterr()
    assert captured.err == ""
    return rc, json.loads(captured.out)


@pytest.mark.parametrize("writer", [
    "echo 'if uvx -p 3.13 -w pytest==8.4.1 pytest /tests/test_outputs.py -rA; then' >> /app/check.sh",
    "printf '%s\\n' 'python3 -m pytest /tests/test_outputs.py' > /app/check.sh",
    "printf '%s\\n' 'set -e' 'pytest /tests/test_outputs.py' > /app/check.sh",
    "printf '%s' 'py' 'test /tests/test_outputs.py' > /app/check.sh",
    "echo -n 'pytest /tests/test_outputs.py' > /app/check.sh",
    "printf '%s\\n' 'set -e # fail fast' 'pytest /tests/test_outputs.py' > /app/check.sh",
])
def test_report_blocks_image_authored_public_script_using_private_pytest(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], writer: str,
) -> None:
    bundle = _write_bundle(tmp_path, "public-checker")
    dockerfile = bundle / "environment/Dockerfile"
    dockerfile.write_text("FROM ubuntu:24.04\nWORKDIR /app\nRUN " + writer + "\n")
    before = {str(p): p.read_bytes() for p in bundle.rglob("*") if p.is_file()}
    rc, payload = _report(tmp_path, capsys)
    report, = payload["compatibility_report"]["tasks"]
    assert rc == 1 and report["status"] == "blocked"
    diagnostic, = [d for d in report["diagnostics"] if d["code"] == "agent_private_verifier_dependency"]
    assert diagnostic["category"] == "package_defect"
    assert diagnostic["source_location"] == f"{dockerfile}:3"
    assert "/app/check.sh" in diagnostic["reason"]
    assert "/tests/test_outputs.py" in diagnostic["reason"]
    assert "private" in diagnostic["suggested_action"]
    assert {str(p): p.read_bytes() for p in bundle.rglob("*") if p.is_file()} == before


@pytest.mark.parametrize("writer", [
    "echo '# pytest /tests/test_outputs.py' > /app/check.sh",
    "echo 'echo pytest /tests/test_outputs.py' > /app/check.sh",
    "echo 'pytest /tests/not-in-package.py' > /app/check.sh",
    "echo 'pytest /app/public_tests.py' > /app/check.sh",
    "echo 'pytest /tests/test_outputs.py' > /opt/verifier/check.sh",
    "pytest /tests/test_outputs.py",
    "printf '%s\\n' 'pytest /app/test_public.py' 'echo /tests/test_outputs.py' > /app/check.sh",
    "echo 'pytest --ignore /tests/test_outputs.py /app/test_public.py' > /app/check.sh",
    "echo 'pytest -k /tests/test_outputs.py /app/test_public.py' > /app/check.sh",
    "echo 'uvx cowsay pytest /tests/test_outputs.py' > /app/check.sh",
    "echo 'uvx --from pytest cowsay /tests/test_outputs.py' > /app/check.sh",
    "echo 'pytest /app/test_public.py > /tests/test_outputs.py' > /app/check.sh",
    "printf '%s\\n' 'pytest /app/test_public.py # public checks' 'echo /tests/test_outputs.py' > /app/check.sh",
    "echo 'echo \";\" pytest /tests/test_outputs.py' > /app/check.sh",
])
def test_report_does_not_infer_private_runtime_dependency_from_unrelated_text(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], writer: str,
) -> None:
    bundle = _write_bundle(tmp_path, "unrelated-checker")
    (bundle / "environment/Dockerfile").write_text(
        "FROM ubuntu:24.04\nWORKDIR /app\nRUN " + writer + "\n")
    _, payload = _report(tmp_path, capsys)
    report, = payload["compatibility_report"]["tasks"]
    assert not any(d["code"] == "agent_private_verifier_dependency" for d in report["diagnostics"])


@pytest.mark.parametrize(("final_base", "blocked"), [("builder", True), ("ubuntu:24.04", False)])
def test_private_script_diagnostic_follows_local_stage_inheritance(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], final_base: str, blocked: bool,
) -> None:
    bundle = _write_bundle(tmp_path, "staged-checker")
    (bundle / "environment/Dockerfile").write_text(
        "FROM ubuntu:24.04 AS builder\nWORKDIR /app\n"
        "RUN echo 'pytest /tests/test_outputs.py' > /app/check.sh\n"
        f"FROM {final_base}\nWORKDIR /app\n")
    rc, payload = _report(tmp_path, capsys)
    report, = payload["compatibility_report"]["tasks"]
    assert rc == int(blocked)
    assert any(d["code"] == "agent_private_verifier_dependency" for d in report["diagnostics"]) is blocked


@pytest.mark.parametrize(("source", "declared", "blocked", "line"), [
    ("FROM ubuntu:24.04\nWORKDIR /media/project\n", "/app", True, 2),
    ("FROM ubuntu:24.04\nWORKDIR /media/project\n", "/media/project", False, 2),
    ("FROM ubuntu:24.04 AS base\nWORKDIR /media/project\nFROM base\n", "/app", True, 2),
    ("FROM ubuntu:24.04 AS base\nWORKDIR /media\nFROM base\nWORKDIR project\n",
     "/media/project", False, 4),
    ("FROM ubuntu:24.04 AS base\nWORKDIR /media\nFROM base\nWORKDIR project\n",
     "/app", True, 4),
    ("FROM ubuntu:24.04\nWORKDIR project\n", "/app", True, 2),
    ("FROM ubuntu:24.04\nWORKDIR ${TASK_ROOT}\n", "/app", True, 2),
    ("FROM ubuntu:24.04 AS unused\nWORKDIR /media\nFROM ubuntu:24.04\nWORKDIR /app\n",
     "/app", False, 4),
    ("FROM ubuntu:24.04\nWORKDIR /app\nRUN <<EOF\nWORKDIR /tests\nEOF\n",
     "/app", False, 2),
])
def test_report_preserves_authored_workdir_or_explains_mismatch(
    tmp_path: Path, capsys: pytest.CaptureFixture[str],
    source: str, declared: str, blocked: bool, line: int,
) -> None:
    bundle = _write_bundle(tmp_path, "working-directory", workdir=declared)
    dockerfile = bundle / "environment/Dockerfile"
    dockerfile.write_text(source)
    before = {str(p): p.read_bytes() for p in bundle.rglob("*") if p.is_file()}
    rc, payload = _report(tmp_path, capsys)
    report, = payload["compatibility_report"]["tasks"]
    assert rc == int(blocked)
    assert report["status"] == ("blocked" if blocked else "converted")
    diagnostics = [d for d in report["diagnostics"] if d["code"] == "dockerfile_workdir_overridden"]
    assert bool(diagnostics) is blocked
    if blocked:
        assert diagnostics[0]["source_location"] == f"{dockerfile}:{line}"
    assert {str(p): p.read_bytes() for p in bundle.rglob("*") if p.is_file()} == before


@pytest.mark.parametrize(("source_workdir", "blocked"), [("/app", False), ("/workspace", True)])
def test_report_compares_docker_workdir_with_the_actual_profile_default(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], source_workdir: str, blocked: bool,
) -> None:
    bundle = _write_bundle(tmp_path, "default-working-directory")
    config = bundle / "task.toml"
    config.write_text(config.read_text().replace('workdir = "/app"\n', ""))
    (bundle / "environment/Dockerfile").write_text(f"FROM ubuntu:24.04\nWORKDIR {source_workdir}\n")
    rc, payload = _report(tmp_path, capsys)
    report, = payload["compatibility_report"]["tasks"]
    assert rc == int(blocked)
    diagnostics = [d for d in report["diagnostics"] if d["code"] == "dockerfile_workdir_overridden"]
    assert bool(diagnostics) is blocked
    if blocked:
        assert "preparation selects '/app'" in diagnostics[0]["reason"]


def test_report_covers_every_task_after_parse_and_adaptation_failures(
    tmp_path: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    broken = _write_bundle(tmp_path, "a-malformed")
    (broken / "task.toml").write_text("not = valid = TOML\n")
    unsupported = _write_bundle(tmp_path, "b-bootstrap")
    (unsupported / "tests/test.sh").write_text("#!/bin/bash\nunknown-installer pytest\n")
    _write_bundle(tmp_path, "c-supported")
    original = {str(p): p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()}

    rc, payload = _report(tmp_path, capsys)

    assert rc == 1
    assert payload["task_count"] == 3
    reports = payload["compatibility_report"]["tasks"]
    assert [row["task_id"] for row in reports] == [
        "slice/a-malformed", "slice/b-bootstrap", "slice/c-supported",
    ]
    assert [row["status"] for row in reports] == ["blocked", "blocked", "converted"]
    assert reports[0]["diagnostics"][0]["category"] == "package_defect"
    assert reports[1]["diagnostics"][0]["category"] == "unsupported_conversion"
    assert reports[2]["admission_passed"] is True
    assert reports[2]["changes"]
    assert all(
        diagnostic["source_location"] and diagnostic["reason"] and diagnostic["suggested_action"]
        for report in reports for diagnostic in report["diagnostics"]
    )
    assert {str(p): p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()} == original


@pytest.mark.parametrize("enabled", [False, True])
def test_nebius_report_retains_supported_completion_policy(tmp_path, capsys, enabled):
    import tomllib

    bundle = _write_bundle(tmp_path, "continuation")
    config = tomllib.loads((bundle / "task.toml").read_text())
    config["agent"]["continue_until_timeout"] = enabled
    (bundle / "task.toml").write_text(tomli_w.dumps(config))
    original = (bundle / "task.toml").read_bytes()
    rc, payload = _report(tmp_path, capsys)
    assert rc == 0
    report, = payload["compatibility_report"]["tasks"]
    assert report["admission_passed"] is True
    assert not any(row["code"] == "agent_completion_policy" for row in report["diagnostics"])
    assert (bundle / "task.toml").read_bytes() == original


def test_admission_pass_does_not_hide_changed_declared_requirements(
    tmp_path: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    bundle = _write_bundle(
        tmp_path, "requirements", user="root", workdir="/data",
        network_policies_supported=["public"], baseline_network_policy={"kind": "public"},
    )
    config = {
        "task": {"name": "upstream-task"},
        "environment": {
            "user": "root", "workdir": "/data", "allow_internet": True,
            "mutable_paths": ["/data", "/home/root"],
            "services": [{"name": "database"}],
        },
        "verifier": {"user": "root"},
    }
    (bundle / "task.toml").write_text(tomli_w.dumps(config))

    rc, payload = _report(tmp_path, capsys)

    assert rc == 1
    report = payload["compatibility_report"]["tasks"][0]
    assert report["status"] == "blocked"
    assert report["original_requirements"]["environment"]["allow_internet"] is True
    assert report["original_requirements"]["environment"]["mutable_paths"] == ["/data", "/home/root"]
    codes = {diagnostic["code"] for diagnostic in report["diagnostics"]}
    required_gaps = {"task_identity", "runtime_egress", "services", "verifier_identity"}
    assert required_gaps <= codes
    assert all(
        diagnostic["category"] == "runtime_capability"
        for diagnostic in report["diagnostics"] if diagnostic["code"] in required_gaps
    )


def test_report_without_profile_labels_schema_check_not_runtime_qualification(
    tmp_path: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    _write_bundle(tmp_path, "valid")

    rc, payload = _report(tmp_path, capsys, profile=False)

    assert rc == 0
    report = payload["compatibility_report"]["tasks"][0]
    assert report["status"] == "schema_valid"
    assert report["admission_passed"] is None
    assert payload["compatibility_report"]["runtime_verified"] is False


def test_report_supplies_raw_harbor_identity_from_source_context(
    tmp_path: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    bundle = _write_bundle(tmp_path, "anonymous")
    (bundle / "task.toml").write_text('[metadata]\nauthor_name = "A"\n[environment]\nos = "linux"\n')
    _write_bundle(tmp_path, "valid")

    rc, payload = _report(tmp_path, capsys, profile=False)

    assert rc == 0
    reports = payload["compatibility_report"]["tasks"]
    assert len(reports) == 2
    assert reports[0]["status"] == "schema_valid"
    assert reports[0]["task_id"] == "slice/anonymous"
    assert reports[1]["status"] == "schema_valid"


def test_missing_copy_sources_are_package_defects_without_creating_directories(
    tmp_path: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    bundle = _write_bundle(tmp_path, "copy-input")
    (bundle / "environment/Dockerfile").write_text(
        'FROM ubuntu:24.04\nCOPY ./task_file /app/task_file\n'
        'COPY --chown=65532:65532 ["missing.json", "/app/data.json"]\n'
    )

    rc, payload = _report(tmp_path, capsys, profile=False)

    assert rc == 1
    report = payload["compatibility_report"]["tasks"][0]
    assert report["status"] == "blocked"
    diagnostics = report["diagnostics"]
    assert [row["code"] for row in diagnostics] == ["missing_copy_source", "missing_copy_source"]
    assert diagnostics[0]["source_location"].endswith("environment/Dockerfile:2")
    assert "task_file" in diagnostics[0]["reason"]
    assert diagnostics[1]["source_location"].endswith("environment/Dockerfile:3")
    assert not (bundle / "environment/task_file").exists()


def test_copy_report_uses_build_context_and_ignores_heredoc_and_stage_sources(
    tmp_path: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    bundle = _write_bundle(tmp_path, "copy-input")
    (bundle / "environment/actual.txt").write_text("data\n")
    (bundle / "environment/Dockerfile").write_text(
        "FROM ubuntu:24.04 AS builder\n"
        "RUN <<'SCRIPT'\nCOPY fictional /heredoc/content\nSCRIPT\n"
        "FROM ubuntu:24.04\n"
        "COPY . /app/\n"
        "COPY --from=builder /generated /app/generated\n"
        "COPY *.txt /app/\n"
        "COPY <<'CONTENT' /app/file\ninline contents\nCONTENT\n"
    )

    rc, payload = _report(tmp_path, capsys, profile=False)

    assert rc == 0
    assert payload["compatibility_report"]["tasks"][0]["diagnostics"] == []


def test_unknown_config_type_still_emits_a_complete_json_report(
    tmp_path: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    bundle = _write_bundle(tmp_path, "wrong-type")
    (bundle / "task.toml").write_text(
        '[task]\nid = "date"\nname = "date"\n'
        '[environment]\nos = 2026-09-22\n'
        '[agent]\nname = "oracle"\n[verifier]\nname = "script"\n'
    )
    _write_bundle(tmp_path, "valid")

    rc, payload = _report(tmp_path, capsys, profile=False)

    assert rc == 1
    assert len(payload["compatibility_report"]["tasks"]) == 2
    assert payload["compatibility_report"]["tasks"][1]["status"] == "blocked"


def test_custom_verifier_entrypoint_is_not_reported_as_equivalent_conversion(
    tmp_path: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    bundle = _write_bundle(tmp_path, "custom-verifier")
    config_path = bundle / "task.toml"
    config_path.write_text(config_path.read_text().replace('script_path = "verifier/run.sh"', 'script_path = "private/check.sh"'))

    rc, payload = _report(tmp_path, capsys)

    assert rc == 1
    report = payload["compatibility_report"]["tasks"][0]
    assert report["admission_passed"] is True
    assert any(diagnostic["code"] == "verifier_entrypoint_changed" for diagnostic in report["diagnostics"])


def test_harbor_projection_cannot_hide_timeouts_capabilities_or_artifact_requirements(
    tmp_path: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    bundle = _write_bundle(tmp_path, "lossy-projection")
    (bundle / "task.toml").write_text(tomli_w.dumps({
        "task": {"name": "native"},
        "required_agent_capabilities": ["nested-containers"],
        "artifacts": ["/var/log/task.log"],
        "agent": {"max_timeout_sec": 17},
    }))

    rc, payload = _report(tmp_path, capsys)

    assert rc == 1
    report = payload["compatibility_report"]["tasks"][0]
    assert report["original_requirements"]["artifacts"] == ["/var/log/task.log"]
    codes = {diagnostic["code"] for diagnostic in report["diagnostics"]}
    assert "invalid_task_config" in codes
    assert any("agent.max_timeout_sec" in item["reason"] for item in report["diagnostics"])

    (bundle / "task.toml").write_text(tomli_w.dumps({
        "task": {"name": "native"},
        "required_agent_capabilities": ["nested-containers"],
        "artifacts": ["/var/log/task.log"],
        "agent": {"timeout_sec": 17},
    }))
    rc, payload = _report(tmp_path, capsys)
    assert rc == 1
    report = payload["compatibility_report"]["tasks"][0]
    codes = {diagnostic["code"] for diagnostic in report["diagnostics"]}
    assert {"structured_artifact_collection_runtime_unavailable", "agent_capabilities_unsupported"} <= codes
    assert "unmapped_artifact_requirement" not in codes
