from __future__ import annotations

import json
import shlex
import subprocess
import sys
from datetime import UTC, date, datetime
from pathlib import Path

import pytest
import yaml
from scripts import validate_trivy_release_report as validator
from scripts import write_trivy_release_policy as policy

ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize("finding", [None, "critical", "suppressed"])
@pytest.mark.parametrize("job", ["build", "nebius-harness-build"])
def test_actual_workflow_policy_commands_reject_findings(
    tmp_path: Path, finding: str | None, job: str,
) -> None:
    config, ignore, report = (tmp_path / name for name in ("trivy.yaml", "ignore.yaml", "report.json"))
    workflow = yaml.safe_load((ROOT / ".github/workflows/images.yml").read_text())
    steps = {step.get("name"): step for step in workflow["jobs"][job]["steps"]}
    component = "harbor-runtime" if job == "nebius-harness-build" else "execution-runtime"

    def command(script: str, source: str) -> list[str]:
        # Run the real workflow's writer and final validator, substituting only
        # runner paths and matrix variables. The real clock is unchanged.
        arguments = shlex.split(source[source.index(f"python3 scripts/{script}"):].replace("\\\n", ""))
        replacements = {
            "/tmp/loom-trivy-release.yaml": str(config),
            "/tmp/loom-trivy-release.ignore.yaml": str(ignore),
            "$IMAGE_NAME": component, "$ARCHITECTURE": "amd64", "$REPORT": str(report),
        }
        return [sys.executable, str(ROOT / arguments[1]),
                *(replacements.get(arg, arg) for arg in arguments[2:])]

    generate = subprocess.run(
        command("write_trivy_release_policy.py", steps["Generate controlled Trivy policy"]["run"]),
        cwd=tmp_path, capture_output=True, text=True,
    )
    assert generate.returncode == 0, generate.stderr
    assert config.read_bytes() == policy.TRIVY_CONFIG_BYTES
    assert ignore.read_bytes() == policy.TRIVY_NO_EXCEPTIONS_BYTES
    payload = _report(component)
    if finding == "critical":
        payload["Results"][0]["Vulnerabilities"] = [
            {"VulnerabilityID": "CVE-2026-13221", "Severity": "CRITICAL"},
        ]
    elif finding == "suppressed":
        payload["Results"][0]["ExperimentalModifiedFindings"] = [
            {"Type": "vulnerability", "Status": "ignored", "Source": str(ignore),
             "Statement": "previous exception",
             "Finding": {"VulnerabilityID": "CVE-2026-13221", "Severity": "CRITICAL"}},
        ]
    report.write_text(json.dumps(payload))
    checked = subprocess.run(
        command("validate_trivy_release_report.py", steps["Scan native image archive"]["run"]),
        cwd=tmp_path, capture_output=True, text=True,
    )
    assert checked.returncode == (0 if finding is None else 1), checked.stderr


def _report(component: str = "control-plane") -> dict:
    return {
        "SchemaVersion": 2, "ArtifactType": "container_image",
        "ArtifactName": f"/tmp/{component}-amd64.release.oci",
        "Trivy": {"Version": "0.74.0"},
        "Metadata": {"ImageConfig": {"architecture": "amd64", "os": "linux"}},
        "Results": [{"Target": "upgraded runtime", "Class": "os-pkgs", "Type": "debian"}],
    }


@pytest.mark.parametrize(
    "component", ["control-plane", "execution-actuator", "llm-gateway", "harbor-runtime", "service", "web", "execution-runtime"],
)
def test_nebius_without_exceptions_accepts_clean_images_after_legacy_expiry(
    tmp_path: Path, component: str,
) -> None:
    config, ignore = tmp_path / "trivy.yaml", tmp_path / "ignore.yaml"
    policy.write_release_policy(config, ignore, today=date(2099, 1, 1), use_exceptions=False)
    assert config.read_bytes() == policy.TRIVY_CONFIG_BYTES
    assert ignore.read_bytes() == b"vulnerabilities: []\n"
    report = tmp_path / "report.json"
    report.write_text(json.dumps(_report(component)))
    validator.validate_trivy_release_report(component, "amd64", report, ignore, use_exceptions=False)


@pytest.mark.parametrize("fault", ["critical", "suppressed", "old-ignore", "unknown-component"])
def test_nebius_cannot_reuse_exceptions_or_publish_a_critical_finding(
    tmp_path: Path, fault: str,
) -> None:
    config, ignore = tmp_path / "trivy.yaml", tmp_path / "ignore.yaml"
    policy.write_release_policy(config, ignore, use_exceptions=False)
    payload = _report()
    if fault == "critical":
        payload["Results"][0]["Vulnerabilities"] = [
            {"VulnerabilityID": "CVE-2026-13221", "Severity": "CRITICAL"},
        ]
    elif fault == "suppressed":
        payload["Results"][0]["ExperimentalModifiedFindings"] = [
            {"Type": "vulnerability", "Status": "ignored", "Source": str(ignore),
             "Statement": "a previously reviewed exception",
             "Finding": {"VulnerabilityID": "CVE-2026-13221", "Severity": "CRITICAL"}},
        ]
    elif fault == "old-ignore":
        ignore.write_bytes(policy.TRIVY_IGNORE_BYTES)
    report = tmp_path / "report.json"
    report.write_text(json.dumps(payload))
    with pytest.raises(validator.TrivyReportError):
        validator.validate_trivy_release_report(
            "unknown" if fault == "unknown-component" else "control-plane",
            "amd64", report, ignore, use_exceptions=False,
        )


def test_legacy_exception_writer_still_enforces_its_expiration(tmp_path: Path) -> None:
    with pytest.raises(policy.TrivyPolicyError, match="expired"):
        policy.write_release_policy(tmp_path / "trivy.yaml", tmp_path / "ignore.yaml", today=date(2099, 1, 1))


def test_legacy_report_preserves_dev_exception_expiration(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Later(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2099, 1, 1, tzinfo=UTC)

    monkeypatch.setattr(validator, "datetime", Later)
    ignore, report = tmp_path / "ignore.yaml", tmp_path / "report.json"
    ignore.write_bytes(policy.TRIVY_IGNORE_BYTES)
    report.write_text(json.dumps(_report()))
    with pytest.raises(validator.TrivyReportError, match="expired"):
        validator.validate_trivy_release_report("control-plane", "amd64", report, ignore)
