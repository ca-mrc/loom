#!/usr/bin/env python3
"""Prepare exact upstream images for the existing signed Nebius admission path.

Does not build images, modify task packages, or apply a runtime profile. The
prepared profile enters the ordinary publication/rollout source of truth.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from scripts.install_trivy import install_trivy
from scripts.ops.collect_nebius_runtime_evidence_via_gateway import _severity, _validate_sbom
from scripts.ops.nebius_candidate import (
    _sign_admission,
    _trusted_signer,
    encoded,
    read_json,
    sha256,
)
from scripts.write_trivy_release_policy import TRIVY_CONFIG_BYTES

from loom.execution_image_admission import (
    ExecutionImageAdmissionBundleV1,
    ImageAdmissionKeyring,
    verify_execution_image_admission,
)
from loom.prebuilt_task_images import validate_prebuilt_image_pins
from loom.service_execution_materialization import ServiceExecutionRuntimeProfileV1

MAX_IMAGE_REPORT_BYTES = 64 * 1024**2


def _read_image_report(path: Path, *, label: str) -> bytes:
    """Bound each scanner report before parsing or retaining it in memory."""
    with path.open("rb") as handle:
        payload = handle.read(MAX_IMAGE_REPORT_BYTES + 1)
    if len(payload) > MAX_IMAGE_REPORT_BYTES:
        raise ValueError(f"{label} evidence exceeds the 64 MiB report budget")
    return payload


def _run(argv: list[str], *, timeout: int = 120) -> str:
    result = subprocess.run(argv, capture_output=True, text=True, timeout=timeout, check=False)
    if result.returncode:
        # Registry helpers may emit credentials/private network information.
        raise ValueError(f"image preparation command failed: {Path(argv[0]).name}")
    return result.stdout.strip()


def _write(path: Path, data: bytes) -> None:
    with path.open("xb") as handle:
        path.chmod(0o600)
        handle.write(data)


def resolve_image(source: str, crane: str) -> str:
    # Reuse the closed source tag grammar before passing an external reference.
    validate_prebuilt_image_pins({source: "registry.invalid/image@sha256:" + "a" * 64})
    digest = _run([crane, "digest", "--platform", "linux/amd64", source])
    if re.fullmatch(r"sha256:[0-9a-f]{64}", digest) is None:
        raise ValueError("upstream image resolution did not return an immutable digest")
    resolved = source.rsplit(":", 1)[0] + "@" + digest
    config = json.loads(_run([crane, "config", resolved]))
    if config.get("os") != "linux" or config.get("architecture") != "amd64":
        raise ValueError("upstream prebuilt image is not linux/amd64")
    return resolved


def scan_image(image: str, directory: Path, trivy: str, cache: Path) -> dict[str, str]:
    report = directory / "vulnerability.json"
    sbom = directory / "sbom.cdx.json"
    policy = directory / "trivy.yaml"
    ignore = directory / "trivyignore.yaml"
    _write(policy, TRIVY_CONFIG_BYTES)
    _write(ignore, b"vulnerabilities: []\n")
    common = [
        trivy,
        "--config",
        str(policy),
        "image",
        "--image-src",
        "remote",
        "--scanners",
        "vuln",
        "--timeout",
        "20m",
        "--ignorefile",
        str(ignore),
        "--ignore-unfixed=false",
        "--severity",
        "UNKNOWN,LOW,MEDIUM,HIGH,CRITICAL",
        "--exit-code",
        "0",
        "--cache-dir",
        str(cache),
        "--cache-backend",
        "memory",
    ]
    _run([*common, "--format", "json", "--output", str(report), image], timeout=1260)
    payload = _read_image_report(report, label="vulnerability")
    if json.loads(payload).get("ArtifactName") != image:
        raise ValueError("vulnerability evidence differs from the resolved image")
    severity = _severity(payload)  # Existing policy rejects CRITICAL, including unfixed findings.
    _run(
        [*common, "--skip-db-update", "--format", "cyclonedx", "--output", str(sbom), image],
        timeout=1260,
    )
    sbom_bytes = _read_image_report(sbom, label="SBOM")
    _validate_sbom(sbom_bytes)
    report.chmod(0o600)
    sbom.chmod(0o600)
    return {
        "image_ref": image,
        "sbom_sha256": sha256(sbom_bytes),
        "vulnerability_report_sha256": sha256(payload),
        "highest_vulnerability_severity": severity,
    }


def prepare(args: argparse.Namespace) -> dict[str, Any]:
    profile = ServiceExecutionRuntimeProfileV1.model_validate(read_json(args.profile))
    keyring_json = args.trusted_keyring.read_text()
    keyring = ImageAdmissionKeyring.from_json(keyring_json)
    key = _trusted_signer(args.signing_key, args.signing_key_id, keyring_json)
    verify_execution_image_admission(
        profile.image_admission, required_image_refs=profile.published_image_refs(), keyring=keyring
    )
    sources = read_json(args.images_json).get("images")
    if (
        not isinstance(sources, list)
        or not sources
        or any(type(source) is not str for source in sources)
    ):
        raise ValueError(
            "images JSON must contain a nonempty images list of explicit upstream tags"
        )
    if len(sources) != len(set(sources)):
        raise ValueError("images list contains duplicate references")
    validate_prebuilt_image_pins(
        {source: "registry.invalid/image@sha256:" + "a" * 64 for source in sources}
    )
    if args.output.exists():
        raise ValueError("output directory must not exist")
    args.output.mkdir(parents=True, mode=0o700)
    pins = {source: resolve_image(source, str(args.crane)) for source in sorted(sources)}
    # Existing pins cannot silently move in a publication assembled from a prior profile.
    if any(
        source in profile.prebuilt_image_pins and profile.prebuilt_image_pins[source] != resolved
        for source, resolved in pins.items()
    ):
        raise ValueError(
            "upstream tag changed from the prepared profile; create a new publication explicitly"
        )
    pins = {**profile.prebuilt_image_pins, **pins}
    validate_prebuilt_image_pins(pins)
    if len(set(profile.published_image_refs()) | set(pins.values())) > 128:
        raise ValueError("prepared platform and task images exceed the admission bundle bound")
    pin_document = {
        "schema_version": "loom.prebuilt-task-image-pins.v1",
        "platform": "linux/amd64",
        "images": pins,
    }
    _write(args.output / "image-pins.json", encoded(pin_document))
    admissions = {row.statement.image_ref: row for row in profile.image_admission.admissions}
    overlays = {}
    qualification = []
    with tempfile.TemporaryDirectory(prefix="loom-prebuilt-image-tools-") as temporary:
        tools = Path(temporary)
        trivy = str(install_trivy(tools))
        for index, (source, resolved) in enumerate(sorted(pins.items())):
            directory = args.output / f"image-{index:03d}"
            directory.mkdir(mode=0o700)
            # Harbor's standard extra_docker_compose overlay leaves canonical packages intact.
            overlay = directory / "harbor-image.yaml"
            _write(
                overlay,
                (
                    "services:\n  main:\n    image: " + resolved + "\n    platform: linux/amd64\n"
                ).encode(),
            )
            overlays[source] = str(overlay.relative_to(args.output))
            status = {"source": source, "resolved": resolved, "status": "reused_admission"}
            if resolved not in admissions:
                try:
                    row = scan_image(resolved, directory, trivy, tools / "cache")
                    admissions[resolved] = _sign_admission(
                        row,
                        sha256(TRIVY_CONFIG_BYTES),
                        sha256(encoded(pin_document)),
                        key=key,
                        signing_key_id=args.signing_key_id,
                    )
                    status["status"] = "qualified"
                except (ValueError, OSError, subprocess.SubprocessError) as exc:
                    status.update(
                        status="rejected",
                        reason="critical_vulnerability"
                        if "critical" in str(exc)
                        else "image_preparation_failed",
                        error_type=type(exc).__name__,
                    )
            qualification.append(status)
    _write(args.output / "image-qualification.json", encoded(qualification))
    _write(args.output / "harbor-overlays.json", encoded(overlays))
    if any(row["status"] == "rejected" for row in qualification):
        raise ValueError("prebuilt image qualification failed; no runtime profile was published")
    bundle = ExecutionImageAdmissionBundleV1(
        schema_version="loom.execution-image-admission.v1", admissions=tuple(admissions.values())
    )
    verify_execution_image_admission(bundle, required_image_refs=list(admissions), keyring=keyring)
    payload = profile.model_dump(mode="json")
    payload.update(image_admission=bundle.model_dump(mode="json"), prebuilt_image_pins=pins)
    prepared = ServiceExecutionRuntimeProfileV1.model_validate(payload)
    _write(args.output / "runtime-profile.json", encoded(prepared.model_dump(mode="json")))
    return {"prepared_images": len(pins), "profile": str(args.output / "runtime-profile.json")}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", type=Path, required=True)
    parser.add_argument("--images-json", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--crane", type=Path, default=Path("crane"))
    parser.add_argument("--signing-key", type=Path, required=True)
    parser.add_argument("--signing-key-id", required=True)
    parser.add_argument("--trusted-keyring", type=Path, required=True)
    try:
        result = prepare(parser.parse_args())
    except Exception as exc:
        print(f"Prebuilt task-image preparation failed ({type(exc).__name__})", file=sys.stderr)
        return 1
    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
