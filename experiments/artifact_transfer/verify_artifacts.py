"""Local experiment adapter; reuses the publisher's OCI and scan validators.

This module neither creates an admitted candidate nor invokes its protected build.
"""
from __future__ import annotations

import importlib
import json
import subprocess
import sys
import time
from pathlib import Path


def _publisher(source_root: Path):
    root = Path(source_root).resolve()
    sys.dont_write_bytecode = True
    for entry in (root, root / "src"):
        if str(entry) not in sys.path:
            sys.path.insert(0, str(entry))
    module = importlib.import_module("scripts.ops.nebius_candidate")
    if Path(module.__file__).resolve() != root / "scripts/ops/nebius_candidate.py":
        raise ValueError("publisher imported from another source tree; use a fresh process")
    return module


def inspect_all(source_root: Path, items: dict, assembly_sha: str, mode: str = "exact") -> dict:
    publisher = _publisher(source_root)
    if mode not in {"exact", "components"} or publisher.SHA.fullmatch(assembly_sha) is None:
        raise ValueError("invalid assembly SHA or inspection mode")
    if set(items) != set(publisher.COMPONENTS) or len(items) != 7:
        raise ValueError("assembly requires exactly all seven platform components")
    # Validate identities before spending time inspecting any archive.
    for component, item in items.items():
        sha = item.get("source_sha", "")
        if not isinstance(sha, str) or publisher.SHA.fullmatch(sha) is None:
            raise ValueError(f"{component}: invalid component source SHA")
        if mode == "exact" and sha != assembly_sha:
            raise ValueError(f"{component}: source SHA differs from exact assembly")
    results = {}
    for component in publisher.COMPONENTS:
        item = items[component]
        started = time.monotonic()
        archive = Path(item["archive"]).resolve()
        metadata = {}
        digest, binary = publisher.inspect_oci_archive(
            archive, candidate=item["source_sha"],
            runtime=component == "execution_runtime",
            runtime_metadata=metadata if component == "harbor_runtime" else None,
        )
        if item.get("digest") is not None and item["digest"] != digest:
            raise ValueError(f"{component}: manifest differs from expected digest")
        row = {"archive": str(archive), "source_sha": item["source_sha"], "digest": digest}
        if component == "execution_runtime":
            row["runtime_binary_sha256"] = binary
        if component == "harbor_runtime":
            publisher.validate_runtime_metadata(metadata)
            expected_version = item.get("agent_version", "nebius-" + item["source_sha"])
            if metadata["agent_version"] != expected_version:
                raise ValueError("built runtime agent version does not match publication")
            row["runtime_metadata"] = metadata
        row["inspect_seconds"] = time.monotonic() - started
        results[component] = row
    return {"mode": mode, "assembly_sha": assembly_sha, "images": results}


def scan_one(scanner: Path, archive: Path, component: str, owner_image: str,
             output_dir: Path, cache_dir: Path, *, source_root: Path) -> dict:
    publisher = _publisher(source_root)
    ownership = {
        row["image_name"]: row for row in publisher.release_image_matrix(
            publisher.load_manifest(Path(source_root) / "config/component-ownership.toml"),
            image_set="nebius",
        )
    }
    if component not in publisher.COMPONENTS:
        raise ValueError("unknown platform component")
    expected_owner = ownership[publisher.COMPONENTS[component]]["image"]
    if owner_image != expected_owner:
        raise ValueError("scan owner does not match existing component ownership")
    output = Path(output_dir).resolve() / component
    output.mkdir(parents=True, exist_ok=False)
    cache = Path(cache_dir).resolve()
    cache.mkdir(parents=True, exist_ok=True)
    policy, exceptions = output / "trivy.yaml", output / "ignore.yaml"
    report = output / f"{component}.vulnerability.json"
    policy_report = output / f"{component}.policy.json"
    sbom = output / f"{component}.sbom.cdx.json"
    timings, logs = {}, {}
    publisher.write_release_policy(policy, exceptions, use_exceptions=False)

    def run(stage: str, arguments: list[str]) -> None:
        started = time.monotonic()
        log = output / f"{stage}.log"
        logs[stage] = str(log)
        try:
            with log.open("wb") as stream:
                subprocess.run([str(scanner), "--cache-dir", str(cache), *arguments],
                               stdout=stream, stderr=subprocess.STDOUT, check=True)
        finally:
            timings[stage] = time.monotonic() - started
            (output / "scan-timings.json").write_text(json.dumps(timings, indent=2) + "\n")

    # Existing validator intentionally binds ArtifactName to this exact path.
    layout_path = Path(f"/tmp/{owner_image}-amd64.release.oci")
    with publisher.oci_scan_layout(Path(archive).resolve(), layout_path) as layout:
        run("policy_scan", ["--config", str(policy), "image", "--input", str(layout),
                            "--format", "json", "--output", str(policy_report),
                            "--ignorefile", str(exceptions), "--show-suppressed"])
        started = time.monotonic()
        validation_log = output / "policy_validation.log"
        logs["policy_validation"] = str(validation_log)
        try:
            publisher.validate_trivy_release_report(
                owner_image, "amd64", policy_report, exceptions, use_exceptions=False,
            )
        except Exception as error:
            validation_log.write_text(f"{type(error).__name__}: {error}\n")
            raise
        else:
            validation_log.write_text("existing no-exceptions release policy passed\n")
        finally:
            timings["policy_validation"] = time.monotonic() - started
            (output / "scan-timings.json").write_text(json.dumps(timings, indent=2) + "\n")
        run("vulnerability_report", ["image", "--input", str(layout), "--scanners", "vuln",
                                      "--format", "json", "--output", str(report),
                                      "--ignorefile", str(exceptions)])
        run("sbom", ["image", "--input", str(layout), "--format", "cyclonedx",
                     "--output", str(sbom)])
    return {"component": component, "policy_report": str(policy_report),
            "vulnerability_report": str(report), "sbom": str(sbom),
            "policy_sha256": publisher.sha256(policy.read_bytes() + exceptions.read_bytes()),
            "timings": timings, "logs": logs}
