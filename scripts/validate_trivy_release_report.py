#!/usr/bin/env python3
"""Validate suppressed-vulnerability scope and active findings from Trivy v0.74.0."""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import stat
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import cast
from urllib.parse import quote, unquote

if __package__ in {None, ""}:  # pragma: no cover - direct script execution
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.component_ownership import load_manifest
from scripts.write_trivy_release_policy import (
    TRIVY_EXCEPTIONS,
    TRIVY_IGNORE_BYTES,
    TRIVY_NO_EXCEPTIONS_BYTES,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
MAX_REPORT_BYTES = 16 * 1024 * 1024
_MAX_RESULTS = 256
_MAX_MODIFIED_FINDINGS = 64
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
    "harbor-runtime",
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
    "nebius-terminal-bench",
    "llm-gateway-sandbox",
    "service",
    "web",
)
_PERL_BASE_FINDINGS = frozenset(
    (vulnerability_id, "pkg:deb/debian/perl-base") for vulnerability_id in _PERL_CVES
)
_EXPECTED_FINDINGS: dict[str, frozenset[tuple[str, str]]] = {
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
_PURL = re.compile(
    r"^(?P<base>pkg:deb/debian/(?P<package>[a-z0-9][a-z0-9.+-]*))"
    r"@(?P<version>(?:[A-Za-z0-9._~-]|%[0-9A-F]{2})+)"
    r"\?arch=(?P<architecture>amd64|arm64|all)"
    r"&distro=(?P<distro>debian-[A-Za-z0-9.+~-]+)"
    r"(?:&epoch=(?P<epoch>[0-9]+))?$"
)


class TrivyReportError(RuntimeError):
    """The report does not satisfy the controlled vulnerability policy."""


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise TrivyReportError("duplicate JSON object key")
        result[key] = value
    return result


def _reject_nonfinite_json(value: str) -> object:
    raise TrivyReportError(f"non-finite JSON value: {value}")


def _parse_finite_float(value: str) -> float:
    parsed = float(value)
    if not math.isfinite(parsed):
        raise TrivyReportError("non-finite JSON number")
    return parsed


def _object(value: object) -> dict[str, object]:
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        raise TrivyReportError("expected a JSON object")
    return cast(dict[str, object], value)


def _array(value: object) -> list[object]:
    if not isinstance(value, list):
        raise TrivyReportError("expected a JSON array")
    return cast(list[object], value)


def _required_string(raw: dict[str, object], key: str) -> str:
    value = raw.get(key)
    if not isinstance(value, str) or not value:
        raise TrivyReportError(f"{key} must be a non-empty string")
    return value


def _read_regular_file(path: Path, limit: int) -> bytes:
    if not path.is_absolute():
        raise TrivyReportError("evidence paths must be absolute")
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags)
    with os.fdopen(descriptor, "rb") as handle:
        if not stat.S_ISREG(os.fstat(handle.fileno()).st_mode):
            raise TrivyReportError("evidence path must be a regular file")
        payload = handle.read(limit + 1)
    if len(payload) > limit:
        raise TrivyReportError("evidence file exceeds its size limit")
    return payload


def _policy_statements() -> dict[str, str]:
    statements = {exception.vulnerability_id: exception.statement for exception in TRIVY_EXCEPTIONS}
    if len(statements) != len(TRIVY_EXCEPTIONS):
        raise TrivyReportError("controlled exception identifiers are duplicated")
    if any(datetime.now(UTC).date() >= item.expires_at for item in TRIVY_EXCEPTIONS):
        raise TrivyReportError("controlled exception policy is expired")
    authorized = {
        (exception.vulnerability_id, purl)
        for exception in TRIVY_EXCEPTIONS
        for purl in exception.purls
    }
    expected = set().union(*_EXPECTED_FINDINGS.values())
    if authorized != expected:
        raise TrivyReportError("component inventory and exception policy have drifted")
    return statements


def _validate_release_component(component: str) -> None:
    manifest = load_manifest(REPO_ROOT / "config/component-ownership.toml")
    owners = manifest.release_components()
    if component == "harbor-runtime":
        # The independent Nebius publisher owns this disabled legacy-CI image.
        owners = tuple(item for item in manifest.components if item.kind == "release-image")
    elif component == "nebius-terminal-bench":
        # Fixed workload published only by Nebius, with a runtime-payload owner.
        owners = tuple(item for item in manifest.components if item.kind == "runtime-payload-image")
    matches = tuple(item for item in owners if item.id == component)
    if len(matches) != 1:
        raise TrivyReportError("release component authority is not unique")


def _validate_artifact_identity(
    component: str,
    architecture: str,
    artifact_name: str,
    expected_artifact: Path | None = None,
) -> None:
    if expected_artifact is not None:
        if not expected_artifact.is_absolute() or artifact_name != str(expected_artifact):
            raise TrivyReportError("report artifact is inconsistent")
        return
    expected_artifacts = {
        f"/tmp/{component}-{architecture}.docker.tar",
        f"/tmp/{component}-{architecture}.release.docker.tar",
        f"/tmp/{component}-{architecture}.release.oci",
    }
    if artifact_name not in expected_artifacts:
        raise TrivyReportError("report artifact is inconsistent")


def _validate_purl(
    finding: dict[str, object],
    *,
    architecture: str,
    distro: str,
) -> str:
    package = _required_string(finding, "PkgName")
    installed_version = _required_string(finding, "InstalledVersion")
    if _required_string(finding, "PkgID") != f"{package}@{installed_version}":
        raise TrivyReportError("package identifier is inconsistent")
    identifier = _object(finding.get("PkgIdentifier"))
    purl = _required_string(identifier, "PURL")
    match = _PURL.fullmatch(purl)
    if match is None or match["package"] != package:
        raise TrivyReportError("Debian PURL is malformed")

    raw_version = match["version"]
    decoded_version = unquote(raw_version)
    if quote(decoded_version, safe=".-_~") != raw_version:
        raise TrivyReportError("Debian PURL version is not canonical")

    epoch: str | None = None
    package_version = installed_version
    if ":" in installed_version:
        epoch, package_version = installed_version.split(":", maxsplit=1)
        if not epoch.isdigit() or ":" in package_version:
            raise TrivyReportError("installed Debian epoch is malformed")
    if decoded_version != package_version or match["epoch"] != epoch:
        raise TrivyReportError("Debian PURL version or epoch is inconsistent")
    expected_architecture = (
        "all"
        if package.startswith("perl-modules-") or package == "linux-libc-dev"
        else architecture
    )
    if match["architecture"] != expected_architecture:
        raise TrivyReportError("Debian PURL architecture is inconsistent")
    if match["distro"] != distro:
        raise TrivyReportError("Debian PURL distro is inconsistent")
    return match["base"]


def _validate_wrapper(
    value: object,
    *,
    architecture: str,
    distro: str,
    ignore_source: str,
    statements: dict[str, str],
) -> tuple[str, str]:
    wrapper = _object(value)
    if set(wrapper) != {"Type", "Status", "Statement", "Source", "Finding"}:
        raise TrivyReportError("modified-finding wrapper shape is invalid")
    if wrapper["Type"] != "vulnerability" or wrapper["Status"] != "ignored":
        raise TrivyReportError("modified-finding wrapper is not an ignored vulnerability")
    if wrapper["Source"] != ignore_source:
        raise TrivyReportError("modified-finding source is uncontrolled")

    finding = _object(wrapper.get("Finding"))
    vulnerability_id = _required_string(finding, "VulnerabilityID")
    if wrapper["Statement"] != statements.get(vulnerability_id):
        raise TrivyReportError("modified-finding statement is uncontrolled")
    exception = next(
        (item for item in TRIVY_EXCEPTIONS if item.vulnerability_id == vulnerability_id), None,
    )
    if exception is None or datetime.now(UTC).date() >= exception.expires_at:
        raise TrivyReportError("suppressed finding has no unexpired controlled exception")
    if finding.get("Severity") != "CRITICAL":
        raise TrivyReportError("suppressed vulnerability is not critical")
    if "FixedVersion" in finding and finding["FixedVersion"] != "":
        raise TrivyReportError("suppressed vulnerability has a fixed version")
    if finding.get("Status") not in {"affected", "fix_deferred", "will_not_fix"}:
        raise TrivyReportError("suppressed vulnerability status is unsupported")
    base_purl = _validate_purl(
        finding,
        architecture=architecture,
        distro=distro,
    )
    return vulnerability_id, base_purl


def validate_trivy_release_report(
    component: str,
    architecture: str,
    report: Path,
    ignore_file: Path,
    *,
    use_exceptions: bool = True,
    expected_artifact: Path | None = None,
) -> None:
    """Reject active vulnerabilities and any suppression outside its reviewed scope."""

    expected = _EXPECTED_FINDINGS.get(component)
    if expected is None or architecture not in {"amd64", "arm64"}:
        raise TrivyReportError("unknown component or architecture")
    _validate_release_component(component)
    ignore_bytes = TRIVY_IGNORE_BYTES if use_exceptions else TRIVY_NO_EXCEPTIONS_BYTES
    if _read_regular_file(ignore_file, len(ignore_bytes)) != ignore_bytes:
        raise TrivyReportError("controlled ignore file is invalid")
    statements = _policy_statements() if use_exceptions else {}
    if not use_exceptions:
        expected = frozenset()

    payload = _object(
        json.loads(
            _read_regular_file(report, MAX_REPORT_BYTES),
            object_pairs_hook=_reject_duplicate_keys,
            parse_constant=_reject_nonfinite_json,
            parse_float=_parse_finite_float,
        )
    )
    if payload.get("SchemaVersion") != 2 or payload.get("ArtifactType") != "container_image":
        raise TrivyReportError("unsupported Trivy report schema")
    if _object(payload.get("Trivy")).get("Version") != "0.74.0":
        raise TrivyReportError("unsupported Trivy version")
    artifact_name = _required_string(payload, "ArtifactName")
    _validate_artifact_identity(component, architecture, artifact_name, expected_artifact)

    metadata = _object(payload.get("Metadata"))
    image_config = _object(metadata.get("ImageConfig"))
    if image_config.get("architecture") != architecture or image_config.get("os") != "linux":
        raise TrivyReportError("report architecture is inconsistent")
    results = _array(payload.get("Results"))
    if not results or len(results) > _MAX_RESULTS:
        raise TrivyReportError("report result count is invalid")
    observed: list[tuple[str, str]] = []
    ignore_source = str(ignore_file)
    for result_value in results:
        result = _object(result_value)
        _required_string(result, "Target")
        result_class = _required_string(result, "Class")
        if result_class not in {"os-pkgs", "lang-pkgs"}:
            raise TrivyReportError("result class is unsupported")
        _required_string(result, "Type")
        if "Vulnerabilities" in result and _array(result["Vulnerabilities"]):
            raise TrivyReportError("report contains unignored vulnerabilities")
        modified = (
            _array(result["ExperimentalModifiedFindings"])
            if "ExperimentalModifiedFindings" in result
            else []
        )
        if len(observed) + len(modified) > _MAX_MODIFIED_FINDINGS:
            raise TrivyReportError("report contains too many modified findings")
        if modified:
            os_metadata = _object(metadata.get("OS"))
            if os_metadata.get("Family") != "debian":
                raise TrivyReportError("suppressed findings are not Debian findings")
            distro_name = _required_string(os_metadata, "Name")
            distro = f"debian-{distro_name}"
            observed.extend(
                _validate_wrapper(
                    wrapper,
                    architecture=architecture,
                    distro=distro,
                    ignore_source=ignore_source,
                    statements=statements,
                )
                for wrapper in modified
            )

    if len(observed) != len(set(observed)):
        raise TrivyReportError("report contains duplicate suppressed findings")
    if frozenset(observed) != expected:
        raise TrivyReportError("report suppressed inventory is not exact")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--component", required=True)
    parser.add_argument("--architecture", required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--ignore-file", type=Path, required=True)
    parser.add_argument(
        "--no-exceptions", action="store_true",
        help="Require an empty ignore file and reject every suppressed finding.",
    )
    arguments = parser.parse_args()
    try:
        validate_trivy_release_report(
            arguments.component,
            arguments.architecture,
            arguments.report,
            arguments.ignore_file,
            use_exceptions=not arguments.no_exceptions,
        )
    except (OSError, RecursionError, UnicodeError, ValueError, TrivyReportError):
        sys.stderr.write("error: Trivy release report validation failed\n")
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()


__all__ = ["MAX_REPORT_BYTES", "TrivyReportError", "validate_trivy_release_report"]
