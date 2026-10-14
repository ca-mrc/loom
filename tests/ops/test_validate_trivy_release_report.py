from __future__ import annotations

import copy
import json
import subprocess
import sys
from pathlib import Path
from urllib.parse import quote

import pytest
from scripts import write_trivy_release_policy as policy

REPO_ROOT = Path(__file__).resolve().parents[2]
_PERL_CVES = (
    "CVE-2026-13221",
    "CVE-2026-42496",
    "CVE-2026-8376",
)
_POSTGRES_PERL_PACKAGES = (
    "libperl5.36",
    "perl",
    "perl-base",
    "perl-modules-5.36",
)
_REMEDIATED_COMPONENTS = (
    "control-plane",
    "egress-xds",
    "execution-actuator",
    "family-orchestrator",
    "llm-gateway",
    "pipeline-orchestrator",
    "worker",
)
_EMPTY_COMPONENTS = (
    "execution-runtime",
    "llm-gateway-sandbox",
    "service",
    "web",
)
_PERL_BASE_FINDINGS = frozenset(
    (vulnerability_id, "pkg:deb/debian/perl-base") for vulnerability_id in _PERL_CVES
)
_EXPECTED_FINDINGS = {
    "agent-sandbox": frozenset({("CVE-2026-43185", "pkg:deb/debian/linux-libc-dev")}),
    "rehearsal-postgres": frozenset(
        (vulnerability_id, f"pkg:deb/debian/{package}")
        for vulnerability_id in _PERL_CVES
        for package in _POSTGRES_PERL_PACKAGES
    )
    | {
        ("CVE-2023-45853", "pkg:deb/debian/zlib1g"),
        ("CVE-2025-7458", "pkg:deb/debian/libsqlite3-0"),
        ("CVE-2026-6653", "pkg:deb/debian/libxml2"),
    },
    "pipeline-core-fixture": _PERL_BASE_FINDINGS
    | {
        ("CVE-2023-45853", "pkg:deb/debian/zlib1g"),
        ("CVE-2025-7458", "pkg:deb/debian/libsqlite3-0"),
    },
    **{component: frozenset() for component in _REMEDIATED_COMPONENTS},
    **{component: frozenset() for component in _EMPTY_COMPONENTS},
}


def _version(package: str) -> tuple[str, str, str | None]:
    if package == "zlib1g":
        return "1:1.2.13.dfsg-1", "1.2.13.dfsg-1", "1"
    if package in _POSTGRES_PERL_PACKAGES:
        return "5.36.0-7+deb12u3", "5.36.0-7+deb12u3", None
    if package == "linux-libc-dev":
        return "6.12.101-1", "6.12.101-1", None
    if package == "libsqlite3-0":
        return "3.40.1-2+deb12u2", "3.40.1-2+deb12u2", None
    if package == "libxml2":
        return "2.9.14+dfsg-1.3~deb12u6", "2.9.14+dfsg-1.3~deb12u6", None
    raise AssertionError(f"test fixture has no version for {package}")


def _wrapper(
    vulnerability_id: str,
    base_purl: str,
    *,
    architecture: str,
    ignore_file: Path,
) -> dict[str, object]:
    package = base_purl.rsplit("/", maxsplit=1)[1]
    installed_version, purl_version, epoch = _version(package)
    purl_architecture = (
        "all"
        if package.startswith("perl-modules-") or package == "linux-libc-dev"
        else architecture
    )
    qualifiers = f"arch={purl_architecture}&distro=debian-13.6"
    if epoch is not None:
        qualifiers += f"&epoch={epoch}"
    statements = {
        exception.vulnerability_id: exception.statement for exception in policy.TRIVY_EXCEPTIONS
    }
    return {
        "Type": "vulnerability",
        "Status": "ignored",
        "Statement": statements[vulnerability_id],
        "Source": str(ignore_file),
        "Finding": {
            "VulnerabilityID": vulnerability_id,
            "PkgID": f"{package}@{installed_version}",
            "PkgName": package,
            "PkgIdentifier": {
                "PURL": f"{base_purl}@{quote(purl_version, safe='.-_~')}?{qualifiers}",
                "UID": "0123456789abcdef",
            },
            "InstalledVersion": installed_version,
            "Status": "affected",
            "Severity": "CRITICAL",
        },
    }


def _report(
    component: str,
    ignore_file: Path,
    *,
    architecture: str = "amd64",
) -> dict[str, object]:
    wrappers = [
        _wrapper(
            vulnerability_id,
            base_purl,
            architecture=architecture,
            ignore_file=ignore_file,
        )
        for vulnerability_id, base_purl in sorted(_EXPECTED_FINDINGS[component])
    ]
    artifact_name = f"/tmp/{component}-{architecture}.docker.tar"
    os_family = "debian"
    os_name = "13.6"
    os_type = "debian"
    return {
        "SchemaVersion": 2,
        "ArtifactName": artifact_name,
        "ArtifactType": "container_image",
        "Trivy": {"Version": "0.74.0"},
        "Metadata": {
            "OS": {"Family": os_family, "Name": os_name},
            "ImageConfig": {"architecture": architecture, "os": "linux"},
        },
        "Results": [
            {
                "Target": f"test ({os_family} {os_name})",
                "Class": "os-pkgs",
                "Type": os_type,
                "Vulnerabilities": [],
                "ExperimentalModifiedFindings": wrappers,
            }
        ],
    }


def _write_ignore_file(path: Path) -> None:
    path.write_bytes(policy.TRIVY_IGNORE_BYTES)


def _run_validator(
    tmp_path: Path,
    payload: dict[str, object] | bytes,
    *,
    component: str = "agent-sandbox",
    architecture: str = "amd64",
    ignore_file: Path | None = None,
) -> subprocess.CompletedProcess[str]:
    controlled_ignore = ignore_file or tmp_path / "loom-trivy-release.ignore.yaml"
    if ignore_file is None:
        _write_ignore_file(controlled_ignore)
    report = tmp_path / "trivy.json"
    report.write_bytes(
        payload if isinstance(payload, bytes) else json.dumps(payload).encode("utf-8")
    )
    return subprocess.run(
        [
            sys.executable,
            "-c",
            # Exercise historical inventories before their review expires;
            # the separate expiry tests still exercise the rejection boundary.
            "from datetime import UTC, datetime\n"
            "from unittest.mock import patch\n"
            "from scripts import validate_trivy_release_report as validator\n"
            "with patch.object(validator, 'datetime', wraps=datetime) as clock:\n"
            "    clock.now.return_value = datetime(2026, 9, 14, tzinfo=UTC)\n"
            "    validator.main()\n",
            "--component",
            component,
            "--architecture",
            architecture,
            "--report",
            str(report),
            "--ignore-file",
            str(controlled_ignore),
        ],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )


def _assert_rejected(result: subprocess.CompletedProcess[str]) -> None:
    assert result.returncode == 1
    assert result.stdout == ""
    assert result.stderr == "error: Trivy release report validation failed\n"


_SUPPORTED_COMPONENT_ARCHITECTURES = tuple(
    (component, architecture)
    for component in sorted(_EXPECTED_FINDINGS)
    for architecture in ("amd64", "arm64")
)


@pytest.mark.parametrize(("component", "architecture"), _SUPPORTED_COMPONENT_ARCHITECTURES)
def test_validator_accepts_each_exact_component_inventory(
    tmp_path: Path,
    component: str,
    architecture: str,
) -> None:
    ignore_file = tmp_path / "loom-trivy-release.ignore.yaml"
    _write_ignore_file(ignore_file)

    result = _run_validator(
        tmp_path,
        _report(component, ignore_file, architecture=architecture),
        component=component,
        architecture=architecture,
        ignore_file=ignore_file,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout == ""
    assert result.stderr == ""


@pytest.mark.parametrize(
    "component",
    (
        "control-plane",
        "egress-xds",
        "family-orchestrator",
        "llm-gateway",
        "pipeline-orchestrator",
    ),
)
def test_validator_accepts_repaired_python_slim_without_exceptions(
    tmp_path: Path,
    component: str,
) -> None:
    ignore_file = tmp_path / "loom-trivy-release.ignore.yaml"
    payload = _report(component, ignore_file)
    payload["ArtifactName"] = f"/tmp/{component}-amd64.docker.tar"

    result = _run_validator(tmp_path, payload, component=component)

    assert result.returncode == 0, result.stderr


def test_validator_accepts_service_alpine_inventory_without_debian_exceptions(
    tmp_path: Path,
) -> None:
    ignore_file = tmp_path / "loom-trivy-release.ignore.yaml"
    payload = _report("service", ignore_file)
    payload["Metadata"]["OS"] = {"Family": "alpine", "Name": "3.23.5"}  # type: ignore[index]
    payload["Results"][0]["Type"] = "alpine"  # type: ignore[index]

    result = _run_validator(tmp_path, payload, component="service")

    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("component", (*_REMEDIATED_COMPONENTS, "agent-sandbox"))
def test_repaired_components_reject_any_perl_suppression(tmp_path: Path, component: str) -> None:
    ignore_file = tmp_path / "loom-trivy-release.ignore.yaml"
    payload = _report(component, ignore_file)
    payload["Results"][0]["ExperimentalModifiedFindings"].append(  # type: ignore[index,union-attr]
        _wrapper("CVE-2026-13221", "pkg:deb/debian/perl-base",
            architecture="amd64", ignore_file=ignore_file)
    )
    _assert_rejected(_run_validator(tmp_path, payload, component=component))


def test_report_validator_independently_rejects_expired_review(monkeypatch) -> None:
    from datetime import UTC, datetime

    from scripts import validate_trivy_release_report as validator

    class ExpiredClock:
        @staticmethod
        def now(zone):
            assert zone is UTC
            return datetime(2026, 10, 14, tzinfo=UTC)

    monkeypatch.setattr(validator, "datetime", ExpiredClock)
    with pytest.raises(validator.TrivyReportError, match="expired"):
        validator._policy_statements()


@pytest.mark.parametrize(
    ("reviewed_at", "accepted"),
    (("2026-10-13T23:59:59+00:00", True),
     ("2026-10-14T00:00:00+00:00", False),
     ("2099-01-01T00:00:00+00:00", False)),
)
def test_legacy_cli_enforces_review_expiry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
    reviewed_at: str, accepted: bool,
) -> None:
    from datetime import UTC, datetime

    from scripts import validate_trivy_release_report as validator

    class ReviewClock:
        @staticmethod
        def now(zone):
            assert zone is UTC
            return datetime.fromisoformat(reviewed_at)

    ignore_file, report = tmp_path / "ignore.yaml", tmp_path / "report.json"
    _write_ignore_file(ignore_file)
    report.write_text(json.dumps(_report("agent-sandbox", ignore_file)))
    monkeypatch.setattr(validator, "datetime", ReviewClock)
    monkeypatch.setattr(sys, "argv", [
        "validate_trivy_release_report.py", "--component", "agent-sandbox",
        "--architecture", "amd64", "--report", str(report), "--ignore-file", str(ignore_file),
    ])
    if accepted:
        validator.main()
    else:
        with pytest.raises(SystemExit) as error:
            validator.main()
        assert error.value.code == 1
    output = capsys.readouterr()
    assert output.out == ""
    assert output.err == ("" if accepted else "error: Trivy release report validation failed\n")


def test_validator_rejects_a_missing_suppressed_finding(tmp_path: Path) -> None:
    ignore_file = tmp_path / "loom-trivy-release.ignore.yaml"
    payload = _report("agent-sandbox", ignore_file)
    wrappers = payload["Results"][0]["ExperimentalModifiedFindings"]  # type: ignore[index]
    wrappers.pop()  # type: ignore[union-attr]
    _assert_rejected(_run_validator(tmp_path, payload))


def test_validator_rejects_an_extra_component_finding(tmp_path: Path) -> None:
    ignore_file = tmp_path / "loom-trivy-release.ignore.yaml"
    payload = _report("service", ignore_file)
    agent_payload = _report("agent-sandbox", ignore_file)
    wrappers = payload["Results"][0]["ExperimentalModifiedFindings"]  # type: ignore[index]
    agent_wrappers = agent_payload["Results"][0]["ExperimentalModifiedFindings"]  # type: ignore[index]
    wrappers.append(copy.deepcopy(agent_wrappers[0]))  # type: ignore[union-attr,index]
    _assert_rejected(_run_validator(tmp_path, payload, component="service"))


def test_validator_rejects_a_duplicate_suppressed_finding(tmp_path: Path) -> None:
    ignore_file = tmp_path / "loom-trivy-release.ignore.yaml"
    payload = _report("agent-sandbox", ignore_file)
    wrappers = payload["Results"][0]["ExperimentalModifiedFindings"]  # type: ignore[index]
    wrappers.append(copy.deepcopy(wrappers[0]))  # type: ignore[union-attr,index]
    _assert_rejected(_run_validator(tmp_path, payload))


def test_validator_rejects_a_known_but_wrong_component(tmp_path: Path) -> None:
    ignore_file = tmp_path / "loom-trivy-release.ignore.yaml"
    payload = _report("agent-sandbox", ignore_file)
    _assert_rejected(_run_validator(tmp_path, payload, component="service"))


def test_validator_rejects_service_artifact_as_worker(tmp_path: Path) -> None:
    ignore_file = tmp_path / "loom-trivy-release.ignore.yaml"
    payload = _report("service", ignore_file)
    _assert_rejected(_run_validator(tmp_path, payload, component="worker"))


def test_validator_accepts_candidate_archive_identity(tmp_path: Path) -> None:
    ignore_file = tmp_path / "loom-trivy-release.ignore.yaml"
    payload = _report("service", ignore_file)
    payload["ArtifactName"] = "/tmp/service-amd64.docker.tar"
    result = _run_validator(tmp_path, payload, component="service")
    assert result.returncode == 0, result.stderr


def test_validator_accepts_trusted_release_archive_identity(tmp_path: Path) -> None:
    ignore_file = tmp_path / "loom-trivy-release.ignore.yaml"
    payload = _report("agent-sandbox", ignore_file, architecture="arm64")
    payload["ArtifactName"] = "/tmp/agent-sandbox-arm64.release.docker.tar"
    result = _run_validator(
        tmp_path,
        payload,
        component="agent-sandbox",
        architecture="arm64",
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize(
    "artifact_name",
    (
        "/var/tmp/service-amd64.docker.tar",
        "/tmp/worker-amd64.docker.tar",
        "/tmp/service-arm64.docker.tar",
        "/tmp/service-amd64.tar",
        "/tmp/service-amd64.docker.tar/extra",
    ),
)
def test_validator_rejects_an_invalid_archive_identity(
    tmp_path: Path,
    artifact_name: str,
) -> None:
    ignore_file = tmp_path / "loom-trivy-release.ignore.yaml"
    payload = _report("service", ignore_file)
    payload["ArtifactName"] = artifact_name
    _assert_rejected(_run_validator(tmp_path, payload, component="service"))


def test_validator_rejects_an_unknown_component(tmp_path: Path) -> None:
    ignore_file = tmp_path / "loom-trivy-release.ignore.yaml"
    payload = _report("agent-sandbox", ignore_file)
    _assert_rejected(_run_validator(tmp_path, payload, component="unknown-image"))


@pytest.mark.parametrize(
    ("field", "value"),
    (("Type", "secret"), ("Status", "suppressed")),
)
def test_validator_rejects_the_wrong_wrapper_contract(
    tmp_path: Path,
    field: str,
    value: str,
) -> None:
    ignore_file = tmp_path / "loom-trivy-release.ignore.yaml"
    payload = _report("agent-sandbox", ignore_file)
    wrapper = payload["Results"][0]["ExperimentalModifiedFindings"][0]  # type: ignore[index]
    wrapper[field] = value  # type: ignore[index]
    _assert_rejected(_run_validator(tmp_path, payload))


def test_validator_rejects_the_wrong_ignore_source(tmp_path: Path) -> None:
    ignore_file = tmp_path / "loom-trivy-release.ignore.yaml"
    payload = _report("agent-sandbox", ignore_file)
    wrapper = payload["Results"][0]["ExperimentalModifiedFindings"][0]  # type: ignore[index]
    wrapper["Source"] = "/tmp/uncontrolled.ignore.yaml"  # type: ignore[index]
    _assert_rejected(_run_validator(tmp_path, payload))


def test_validator_rejects_the_wrong_policy_statement(tmp_path: Path) -> None:
    ignore_file = tmp_path / "loom-trivy-release.ignore.yaml"
    payload = _report("agent-sandbox", ignore_file)
    wrapper = payload["Results"][0]["ExperimentalModifiedFindings"][0]  # type: ignore[index]
    wrapper["Statement"] = f"{wrapper['Statement']} Extra justification."  # type: ignore[index]
    _assert_rejected(_run_validator(tmp_path, payload))


def test_validator_rejects_noncritical_suppressed_evidence(tmp_path: Path) -> None:
    ignore_file = tmp_path / "loom-trivy-release.ignore.yaml"
    payload = _report("agent-sandbox", ignore_file)
    finding = payload["Results"][0]["ExperimentalModifiedFindings"][0]["Finding"]  # type: ignore[index]
    finding["Severity"] = "HIGH"  # type: ignore[index]
    _assert_rejected(_run_validator(tmp_path, payload))


def test_validator_rejects_a_nonempty_fixed_version(tmp_path: Path) -> None:
    ignore_file = tmp_path / "loom-trivy-release.ignore.yaml"
    payload = _report("agent-sandbox", ignore_file)
    finding = payload["Results"][0]["ExperimentalModifiedFindings"][0]["Finding"]  # type: ignore[index]
    finding["FixedVersion"] = "99.0-1"  # type: ignore[index]
    _assert_rejected(_run_validator(tmp_path, payload))


@pytest.mark.parametrize("status", ("affected", "fix_deferred", "will_not_fix"))
def test_validator_accepts_each_supported_nested_status(
    tmp_path: Path,
    status: str,
) -> None:
    ignore_file = tmp_path / "loom-trivy-release.ignore.yaml"
    payload = _report("agent-sandbox", ignore_file)
    finding = payload["Results"][0]["ExperimentalModifiedFindings"][0]["Finding"]  # type: ignore[index]
    finding["Status"] = status  # type: ignore[index]
    finding["FixedVersion"] = ""  # type: ignore[index]
    result = _run_validator(tmp_path, payload)
    assert result.returncode == 0, result.stderr


def test_validator_rejects_an_unsupported_nested_status(tmp_path: Path) -> None:
    ignore_file = tmp_path / "loom-trivy-release.ignore.yaml"
    payload = _report("agent-sandbox", ignore_file)
    finding = payload["Results"][0]["ExperimentalModifiedFindings"][0]["Finding"]  # type: ignore[index]
    finding["Status"] = "fixed"  # type: ignore[index]
    _assert_rejected(_run_validator(tmp_path, payload))


@pytest.mark.parametrize(
    "purl",
    (
        "pkg:deb/debian/linux-libc-dev?arch=all&distro=debian-13.6",
        "pkg:deb/debian/not-libperl@6.12.101-1?arch=all&distro=debian-13.6",
        "pkg:deb/debian/linux-libc-dev@6.12.101-2?arch=all&distro=debian-13.6",
        "pkg:deb/debian/linux-libc-dev@6.12.101-1?arch=all&distro=debian-13.6&epoch=1",
        "pkg:deb/debian/linux-libc-dev@6.12.101-1?arch=s390x&distro=debian-13.6",
        "pkg:deb/debian/linux-libc-dev@6.12.101-1?arch=all&distro=debian-12.0",
        "pkg:deb/debian/linux-libc-dev@6.12.101-1?arch=all&distro=debian-13.6&other=x",
    ),
)
def test_validator_rejects_a_malformed_or_inconsistent_versioned_purl(
    tmp_path: Path,
    purl: str,
) -> None:
    ignore_file = tmp_path / "loom-trivy-release.ignore.yaml"
    payload = _report("agent-sandbox", ignore_file)
    finding = payload["Results"][0]["ExperimentalModifiedFindings"][0]["Finding"]  # type: ignore[index]
    finding["PkgIdentifier"]["PURL"] = purl  # type: ignore[index]
    _assert_rejected(_run_validator(tmp_path, payload))


def test_validator_rejects_unignored_vulnerabilities(tmp_path: Path) -> None:
    ignore_file = tmp_path / "loom-trivy-release.ignore.yaml"
    payload = _report("agent-sandbox", ignore_file)
    payload["Results"][0]["Vulnerabilities"] = [  # type: ignore[index]
        {"VulnerabilityID": "CVE-2099-9999"}
    ]
    _assert_rejected(_run_validator(tmp_path, payload))


def test_validator_accepts_real_schema_omitted_empty_finding_groups(tmp_path: Path) -> None:
    ignore_file = tmp_path / "loom-trivy-release.ignore.yaml"
    payload = _report("service", ignore_file)
    payload["Results"][0].pop("Vulnerabilities")  # type: ignore[index]
    payload["Results"].append(  # type: ignore[union-attr]
        {
            "Target": "Python",
            "Class": "lang-pkgs",
            "Type": "python-pkg",
        }
    )
    result = _run_validator(tmp_path, payload, component="service")
    assert result.returncode == 0, result.stderr


def test_validator_rejects_empty_result_object(tmp_path: Path) -> None:
    ignore_file = tmp_path / "loom-trivy-release.ignore.yaml"
    payload = _report("web", ignore_file)
    payload["Results"] = [{}]
    _assert_rejected(_run_validator(tmp_path, payload, component="web"))


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("Target", ""),
        ("Class", ""),
        ("Class", "config"),
        ("Type", ""),
    ),
)
def test_validator_rejects_invalid_result_metadata(
    tmp_path: Path,
    field: str,
    value: str,
) -> None:
    ignore_file = tmp_path / "loom-trivy-release.ignore.yaml"
    payload = _report("web", ignore_file)
    payload["Results"][0][field] = value  # type: ignore[index]
    _assert_rejected(_run_validator(tmp_path, payload, component="web"))


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("Vulnerabilities", None),
        ("Vulnerabilities", {}),
        ("ExperimentalModifiedFindings", None),
        ("ExperimentalModifiedFindings", "none"),
    ),
)
def test_validator_rejects_present_nonarray_finding_groups(
    tmp_path: Path,
    field: str,
    value: object,
) -> None:
    ignore_file = tmp_path / "loom-trivy-release.ignore.yaml"
    payload = _report("agent-sandbox", ignore_file)
    payload["Results"][0][field] = value  # type: ignore[index]
    _assert_rejected(_run_validator(tmp_path, payload))


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("SchemaVersion", 1),
        ("ArtifactType", "filesystem"),
        ("Trivy", {"Version": "0.69.0"}),
        ("Results", []),
        ("Results", "not-a-list"),
    ),
)
def test_validator_rejects_a_malformed_report_shape(
    tmp_path: Path,
    field: str,
    value: object,
) -> None:
    ignore_file = tmp_path / "loom-trivy-release.ignore.yaml"
    payload = _report("agent-sandbox", ignore_file)
    payload[field] = value
    _assert_rejected(_run_validator(tmp_path, payload))


def test_validator_rejects_a_malformed_nested_finding_shape(tmp_path: Path) -> None:
    ignore_file = tmp_path / "loom-trivy-release.ignore.yaml"
    payload = _report("agent-sandbox", ignore_file)
    finding = payload["Results"][0]["ExperimentalModifiedFindings"][0]["Finding"]  # type: ignore[index]
    finding.pop("PkgIdentifier")  # type: ignore[union-attr]
    _assert_rejected(_run_validator(tmp_path, payload))


def test_validator_rejects_arch_all_for_an_architecture_specific_package(
    tmp_path: Path,
) -> None:
    ignore_file = tmp_path / "loom-trivy-release.ignore.yaml"
    payload = _report("rehearsal-postgres", ignore_file)
    finding = payload["Results"][0]["ExperimentalModifiedFindings"][0]["Finding"]  # type: ignore[index]
    identifier = finding["PkgIdentifier"]  # type: ignore[index]
    identifier["PURL"] = identifier["PURL"].replace("arch=amd64", "arch=all")  # type: ignore[index,union-attr]
    _assert_rejected(_run_validator(tmp_path, payload, component="rehearsal-postgres"))


def test_validator_rejects_malformed_json(tmp_path: Path) -> None:
    _assert_rejected(_run_validator(tmp_path, b'{"Results": [}'))


@pytest.mark.parametrize("nonfinite", (b"NaN", b"Infinity", b"1e999"))
def test_validator_rejects_nonfinite_json_numbers(
    tmp_path: Path,
    nonfinite: bytes,
) -> None:
    ignore_file = tmp_path / "loom-trivy-release.ignore.yaml"
    valid_json = json.dumps(_report("agent-sandbox", ignore_file)).encode("utf-8")
    payload = valid_json.replace(b"{", b'{"Unexpected": ' + nonfinite + b",", 1)
    _assert_rejected(_run_validator(tmp_path, payload))


def test_validator_rejects_duplicate_json_object_keys(tmp_path: Path) -> None:
    ignore_file = tmp_path / "loom-trivy-release.ignore.yaml"
    valid_json = json.dumps(_report("agent-sandbox", ignore_file)).encode("utf-8")
    payload = valid_json.replace(
        b'{"SchemaVersion": 2,',
        b'{"SchemaVersion": 2, "SchemaVersion": 2,',
        1,
    )
    _assert_rejected(_run_validator(tmp_path, payload))


def test_validator_rejects_deeply_nested_json_with_a_sanitized_error(
    tmp_path: Path,
) -> None:
    payload = b"[" * 2_000 + b"0" + b"]" * 2_000
    _assert_rejected(_run_validator(tmp_path, payload))


def test_validator_rejects_oversized_json_before_parsing(tmp_path: Path) -> None:
    _assert_rejected(_run_validator(tmp_path, b" " * (16 * 1024 * 1024 + 1)))


def test_validator_rejects_an_uncontrolled_ignore_file(tmp_path: Path) -> None:
    ignore_file = tmp_path / "loom-trivy-release.ignore.yaml"
    ignore_file.write_text("vulnerabilities: []\n", encoding="utf-8")
    payload = _report("agent-sandbox", ignore_file)
    _assert_rejected(
        _run_validator(tmp_path, payload, ignore_file=ignore_file),
    )


def test_legacy_validator_cli_rejects_expired_review(tmp_path: Path) -> None:
    ignore_file, report = tmp_path / "ignore.yaml", tmp_path / "report.json"
    _write_ignore_file(ignore_file)
    report.write_text(json.dumps(_report("agent-sandbox", ignore_file)))
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "from datetime import UTC, datetime\n"
            "from unittest.mock import patch\n"
            "from scripts import validate_trivy_release_report as validator\n"
            "with patch.object(validator, 'datetime', wraps=datetime) as clock:\n"
            "    clock.now.return_value = datetime(2026, 10, 14, tzinfo=UTC)\n"
            "    validator.main()\n",
            "--component", "agent-sandbox", "--architecture", "amd64",
            "--report", str(report), "--ignore-file", str(ignore_file),
        ],
        cwd=REPO_ROOT, text=True, capture_output=True, check=False,
    )
    _assert_rejected(result)
