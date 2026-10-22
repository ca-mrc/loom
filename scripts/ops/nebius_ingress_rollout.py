"""Protected ingress tooling publication and restricted gateway transport."""
from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import re
import shutil
import subprocess
import tarfile
import tempfile
import tomllib
import zipfile
from pathlib import Path
from typing import Any

from scripts.install_trivy import install_trivy
from scripts.ops.nebius_certificate_gateway import _write
from scripts.ops.nebius_ingress_bootstrap import (
    COMMANDS,
    LIMITS,
    MAX_BUNDLE,
    MAX_WHEEL,
    SCRIPTS,
    safe_report,
    unpack_bundle,
    validate_config,
)
from scripts.ops.nebius_ingress_image import DIGEST, mirror_ingress_image
from scripts.ops.nebius_registry_auth import refresh_registry_auth
from scripts.write_trivy_release_policy import TRIVY_VERSION, write_release_policy

ROOT = Path(__file__).resolve().parents[2]


class RolloutError(RuntimeError):
    """Fixed failure; remote payloads are never public diagnostics."""


def verify_source(config: dict[str, Any]) -> None:
    """Only the exact clean, already-integrated source may construct authority."""
    try:
        def git(*args: str) -> bytes:
            result = subprocess.run(["git", *args], cwd=ROOT, capture_output=True, timeout=30, check=False)
            if result.returncode:
                raise ValueError()
            return result.stdout.strip()

        validate_config(config)
        if (git("rev-parse", "HEAD").decode() != config["source_sha"]
                or git("status", "--porcelain", "--untracked-files=normal")):
            raise ValueError()
        git("merge-base", "--is-ancestor", config["source_sha"], "refs/remotes/origin/dev")
    except Exception:
        raise RolloutError("ingress tooling requires its exact clean integrated source") from None


def build_wheels(directory: Path, *, uv: Path) -> Path:
    """Build only committed source; canonical wheel bytes ignore host permissions."""
    evidence: dict[str, str | int] = {"phase": "prepare", "category": "unclassified"}
    try:
        lock = tomllib.loads((ROOT / "uv.lock").read_text())
        rows = [row for row in lock["package"] if row["name"] == "setuptools"]
        if len(rows) != 1:
            raise ValueError()
        backend = rows[0]
        constraints = directory / "build-constraints.txt"
        hashes = [wheel["hash"] for wheel in backend["wheels"]]
        if not hashes or any(not re.fullmatch(r"sha256:[0-9a-f]{64}", value) for value in hashes):
            raise ValueError()
        constraints.write_text("setuptools==" + backend["version"] + " " + " ".join("--hash=" + value for value in hashes) + "\n")
        wheels = directory / "wheels"
        wheels.mkdir(mode=0o700)
        source = directory / "source"
        source.mkdir(mode=0o700)
        evidence["phase"] = "archive"
        archived = subprocess.run(["git", "archive", "--format=tar", "HEAD"], cwd=ROOT,
                                  capture_output=True, timeout=60, check=False)
        if archived.returncode:
            evidence.update(category="process_exit", exit=archived.returncode)
        if archived.returncode or len(archived.stdout) > 128 * 1024 * 1024:
            raise ValueError()
        evidence["phase"] = "extract"
        with tarfile.open(fileobj=io.BytesIO(archived.stdout)) as archive:
            archive.extractall(source, filter="data")
        for package in ("loom", "loom-bundle-checksum"):
            evidence = {"phase": "build", "package": package, "category": "unclassified"}
            result = subprocess.run(
                [str(uv), "build", "--wheel", "--package", package, "--build-constraints", str(constraints),
                 "--require-hashes", "--no-create-gitignore", "--out-dir", str(wheels)],
                cwd=source, env={**os.environ, "SOURCE_DATE_EPOCH": "315532800"}, capture_output=True, timeout=180, check=False,
            )
            if result.returncode:
                # Only fixed categories leave this boundary, never captured output.
                # These are observed UV markers, not claims about the root cause.
                stderr = result.stderr[:65_536].lower()
                category = next((value for marker, value in (
                    (b"hash mismatch", "hash_mismatch"),
                    (b"failed to download", "artifact_download"),
                    (b"failed to fetch", "index_fetch"),
                    (b"no solution found when resolving", "dependency_resolution"),
                    (b"build backend returned an error", "build_backend"),
                ) if marker in stderr), "process_exit")
                evidence.update(category=category, exit=result.returncode)
                raise ValueError()
        evidence = {"phase": "canonicalize", "category": "unclassified"}
        for wheel in wheels.iterdir():
            canonical = io.BytesIO()
            with zipfile.ZipFile(wheel) as original, zipfile.ZipFile(canonical, "w") as target:
                for member in sorted(original.infolist(), key=lambda entry: entry.filename):
                    entry = zipfile.ZipInfo(member.filename, date_time=(1980, 1, 1, 0, 0, 0))
                    entry.create_system = 3
                    entry.compress_type = zipfile.ZIP_DEFLATED
                    # Commit mode retains executable assets, never caller umask.
                    entry.external_attr = (0o100755 if (member.external_attr >> 16) & 0o111 else 0o100644) << 16
                    target.writestr(entry, original.read(member))
            wheel.write_bytes(canonical.getvalue())
        return wheels
    except Exception as cause:
        if isinstance(cause, subprocess.TimeoutExpired):
            evidence["category"] = "timeout"
        elif isinstance(cause, OSError):
            evidence["category"] = "os_error"
            if type(cause.errno) is int:
                evidence["errno"] = cause.errno
        error = RolloutError("exact-source ingress wheels could not be built")
        error.add_note("wheel-build: " + json.dumps(evidence, sort_keys=True))
        raise error from None


def scan_ingress_image(directory: Path) -> dict[str, str]:
    """Scan the fixed upstream digest under the existing no-exceptions policy."""
    try:
        scanner = install_trivy(directory, architecture="amd64")
        policy, exceptions = directory / "trivy.yaml", directory / "ignore.yaml"
        write_release_policy(policy, exceptions, use_exceptions=False)
        report_path = directory / "ingress-policy.json"
        image = "docker.io/library/traefik@" + DIGEST
        result = subprocess.run(
            [str(scanner), "--cache-dir", str(directory / "trivy-cache"), "--config", str(policy),
             "image", "--image-src", "remote", "--platform", "linux/amd64", "--format", "json",
             "--output", str(report_path), "--ignorefile", str(exceptions), "--show-suppressed", image],
            capture_output=True, timeout=1300, check=False, env={"PATH": os.defpath, "LANG": "C.UTF-8"},
        )
        if result.returncode:
            raise RolloutError("ingress vulnerability qualification failed; inspect bounded scan report")
        with report_path.open("rb") as stream:
            raw = stream.read(16 * 1024 * 1024 + 1)
        if len(raw) > 16 * 1024 * 1024:
            raise ValueError()
        report = json.loads(raw)
        if (report["SchemaVersion"] != 2 or report["ArtifactName"] != image
                or report["ArtifactType"] != "container_image" or not isinstance(report["Results"], list)
                or not 1 <= len(report["Results"]) <= 256):
            raise ValueError()
        for row in report["Results"]:
            if row.get("ExperimentalModifiedFindings") or row.get("ModifiedFindings"):
                raise ValueError()
            findings = row.get("Vulnerabilities", [])
            if not isinstance(findings, list):
                raise ValueError()
            if any(finding["Severity"] not in {"UNKNOWN", "LOW", "MEDIUM", "HIGH"} for finding in findings):
                raise ValueError()
        return {"status": "scan_qualified", "image": image, "scanner_version": TRIVY_VERSION,
                "report_sha256": hashlib.sha256(raw).hexdigest(),
                "policy_sha256": hashlib.sha256(policy.read_bytes() + exceptions.read_bytes()).hexdigest()}
    except RolloutError:
        raise
    except Exception:
        raise RolloutError("ingress vulnerability evidence is unavailable or unqualified") from None


def build_bundle(config: dict[str, Any], *, uv: Path, requirements: Path, wheels: Path) -> bytes:
    try:
        validate_config(config)
        paths = {"uv": uv, "requirements.txt": requirements,
                 **{name: ROOT / name for name in SCRIPTS},
                 **{"wheels/" + path.name: path for path in wheels.iterdir()}}
        files = {}
        for name, path in paths.items():
            limit = LIMITS.get(name, MAX_WHEEL)
            with path.open("rb") as stream:
                files[name] = stream.read(limit + 1)
            if len(files[name]) > limit:
                raise RolloutError("ingress bundle member exceeds bound")
        files["installation.json"] = json.dumps(config, sort_keys=True).encode()
        files["manifest.json"] = json.dumps({name: hashlib.sha256(value).hexdigest() for name, value in files.items()},
                                           sort_keys=True).encode()
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            for name, value in sorted(files.items()):
                entry = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
                entry.compress_type = zipfile.ZIP_DEFLATED
                entry.external_attr = 0o100600 << 16
                archive.writestr(entry, value)
        result = buffer.getvalue()
        unpack_bundle(result)
        return result
    except Exception:
        raise RolloutError("ingress bundle is unqualified") from None


def transfer(content: bytes, *, action: str, target: str, key: Path, known_hosts: Path) -> dict[str, Any]:
    commands = {value: key for key, value in COMMANDS.items()}
    if (action not in commands or not re.fullmatch(r"[a-zA-Z0-9_.-]+@[a-zA-Z0-9_.-]+", target)
            or not key.is_absolute() or not known_hosts.is_absolute() or not 0 < len(content) <= MAX_BUNDLE):
        raise RolloutError("invalid protected ingress transport")
    args = ["ssh", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes", "-o", "IdentitiesOnly=yes",
            "-o", "ConnectTimeout=15", "-o", "ServerAliveInterval=15", "-o", "ServerAliveCountMax=3",
            "-o", "UserKnownHostsFile=" + str(known_hosts), "-i", str(key), target, commands[action]]
    try:
        result = subprocess.run(args, input=content, capture_output=True, timeout=2400, check=False)
        if result.returncode == 126:
            raise RolloutError("ingress transport authority rejected")
        if result.returncode:
            raise RolloutError("ingress operation incomplete; reconcile gateway state before retry")
        report = safe_report(result.stdout)
        expected = {"install": {"complete"}, "rollback": {"rolled_back"},
                    "dns": {"dns_published"},
                    "image-intent": {"image_copy_once", "image_readback_only"}}
        if report["status"] not in expected[action]:
            raise RolloutError("ingress report differs from requested operation")
        return report
    except (OSError, subprocess.TimeoutExpired):
        raise RolloutError("ingress outcome unknown; reconcile before retry") from None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--operation", choices=("install", "rollback", "dns"), required=True)
    parser.add_argument("--requirements", type=Path, required=True)
    parser.add_argument("--evidence-dir", type=Path, required=True)
    parser.add_argument("--prepare-bundle", type=Path, help="Operator preparation only; no registry or remote operations")
    args = parser.parse_args()
    result: dict[str, Any] = {"status": "blocked", "phase": "input"}
    phase = "input"
    try:
        raw = os.environ.get("NEBIUS_INGRESS_INSTALLATION_JSON", "")
        if not 0 < len(raw.encode()) <= 16_384:
            raise ValueError()
        config = json.loads(raw)
        verify_source(config)
        args.evidence_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
        executable = shutil.which("uv")
        if executable is None:
            raise ValueError()
        version = subprocess.run([executable, "--version"], capture_output=True, timeout=15, check=False)
        if version.returncode or version.stdout.strip() != b"uv 0.11.26 (x86_64-unknown-linux-gnu)":
            raise ValueError()
        phase = "tooling_preparation"
        with tempfile.TemporaryDirectory(prefix="loom-ingress-rollout-") as temporary:
            work = Path(temporary)
            wheels = build_wheels(work, uv=Path(executable))
            content = build_bundle(config, uv=Path(executable), requirements=args.requirements, wheels=wheels)
            if args.prepare_bundle is not None:
                _write(args.prepare_bundle, content)
                result = {"status": "prepared", "bundle_sha256": hashlib.sha256(content).hexdigest()}
            else:
                if args.operation == "install":
                    phase = "image_scan"
                    scan_dir = args.evidence_dir / "scan"
                    scan_dir.mkdir(mode=0o700)
                    scan = scan_ingress_image(scan_dir)
                    (args.evidence_dir / "image-scan.json").write_text(json.dumps(scan, sort_keys=True) + "\n")
                    phase = "image_publication"
                    prefix = config["image"].split("/loom-shared-ingress@", 1)[0]
                    region = prefix.split(".")[1]
                    credentials, auth = work / "registry-identity.json", work / "registry-auth.json"
                    _write(credentials, os.environ.pop("NEBIUS_REGISTRY_SERVICE_ACCOUNT_JSON").encode())
                    refresh_registry_auth(credentials, prefix, auth)
                    phase = "image_intent"
                    grant = transfer(content, action="image-intent", target=os.environ["LOOM_DEPLOY_SSH_TARGET"],
                                     key=Path(os.environ["LOOM_DEPLOY_SSH_KEY_FILE"]),
                                     known_hosts=Path(os.environ["LOOM_DEPLOY_SSH_KNOWN_HOSTS_FILE"]))
                    if (grant["image"] != config["image"] or grant["candidate"] != config["candidate"]
                            or grant["installation_id"] != config["binding"]["installation_id"]
                            or grant["namespace"] != config["binding"]["namespace"]
                            or grant["status"] not in {"image_copy_once", "image_readback_only"}):
                        raise ValueError()
                    (args.evidence_dir / "image-intent.json").write_text(json.dumps(grant, sort_keys=True) + "\n")
                    phase = "image_publication"
                    publication_state = work / "image-state"
                    try:
                        image = mirror_ingress_image(registry_prefix=prefix, region=region, auth_file=auth,
                                                     state_dir=publication_state, allow_copy=grant["status"] == "image_copy_once")
                        (args.evidence_dir / "image-publication.json").write_text(json.dumps(image, sort_keys=True) + "\n")
                    finally:
                        journal = publication_state / "image-mirror.json"
                        if journal.exists():
                            record = json.loads(journal.read_bytes())
                            # This journal contains only fixed non-secret image
                            # identity and phase; never publish registry auth.
                            (args.evidence_dir / "image-journal.json").write_text(json.dumps(
                                {key: record[key] for key in ("schema", "source", "destination", "region", "status")}, sort_keys=True,
                            ) + "\n")
                phase = "gateway_operation"
                result = transfer(content, action=args.operation, target=os.environ["LOOM_DEPLOY_SSH_TARGET"],
                                  key=Path(os.environ["LOOM_DEPLOY_SSH_KEY_FILE"]),
                                  known_hosts=Path(os.environ["LOOM_DEPLOY_SSH_KNOWN_HOSTS_FILE"]))
    except Exception:
        result = {"status": "blocked", "phase": phase}
    args.evidence_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    (args.evidence_dir / "ingress-result.json").write_text(json.dumps(result, sort_keys=True) + "\n")
    print(json.dumps(result, sort_keys=True))
    return 0 if result["status"] in {"prepared", "complete", "rolled_back", "dns_published"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
