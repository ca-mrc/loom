#!/usr/bin/env python3
"""Send pinned certificate tooling, but no Secrets, to the protected gateway."""
from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
if __package__ in {None, ""}:
    sys.path.insert(0, str(ROOT))

from scripts.ops.nebius_certificate_gateway import (  # noqa: E402
    LIMITS,
    MAX_BUNDLE,
    safe_report,
)
from scripts.ops.nebius_certificates import load_installation  # noqa: E402
from scripts.ops.nebius_development_management_certificate_gateway import (  # noqa: E402
    LIMITS as MANAGEMENT_LIMITS,
)
from scripts.ops.nebius_development_management_certificate_gateway import (  # noqa: E402
    management_report,
)


class RolloutError(RuntimeError):
    """Fixed protected-operation failure; no remote diagnostics are forwarded."""


class CertificateAuthorityDeniedError(RolloutError):
    """The forced command rejected its command or installed bundle authority."""


def build_bundle(config: dict[str, Any], *, uv: Path, requirements: Path,
                 management_only: bool = False) -> bytes:
    files = {"uv": uv.read_bytes(), "requirements.txt": requirements.read_bytes(),
             "installation.json": json.dumps(config, sort_keys=True).encode()}
    gateway = ("scripts/ops/nebius_development_management_certificate_gateway.py" if management_only
               else "scripts/ops/nebius_certificate_gateway.py")
    limits = MANAGEMENT_LIMITS if management_only else LIMITS
    for name in ("scripts/ops/nebius_certificates.py", "scripts/ops/nebius_dns_challenge.py", gateway):
        files[name] = (ROOT / name).read_bytes()
    files["manifest.json"] = json.dumps({name: hashlib.sha256(value).hexdigest() for name, value in files.items()},
                                       sort_keys=True).encode()
    if any(len(value) > limits[name] for name, value in files.items()):
        raise RolloutError("certificate tooling member exceeds bound")
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, value in sorted(files.items()):
            # Stable member metadata makes daily reuse content-addressable.
            entry = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
            entry.compress_type = zipfile.ZIP_DEFLATED
            entry.external_attr = 0o100600 << 16
            archive.writestr(entry, value)
    result = buffer.getvalue()
    if len(result) > MAX_BUNDLE:
        raise RolloutError("certificate tooling bundle exceeds bound")
    return result


def transfer(content: bytes, *, target: str, key: Path, known_hosts: Path,
             management_only: bool = False) -> dict[str, Any]:
    if (not re.fullmatch(r"[a-zA-Z0-9_.-]+@[a-zA-Z0-9_.-]+", target)
            or not key.is_absolute() or not known_hosts.is_absolute() or len(content) > MAX_BUNDLE):
        raise RolloutError("invalid protected certificate transport configuration")
    config: dict[str, Any] = {}
    if management_only:
        try:
            with zipfile.ZipFile(io.BytesIO(content)) as archive:
                if archive.getinfo("installation.json").file_size > LIMITS["installation.json"]:
                    raise ValueError()
                config = json.loads(archive.read("installation.json"))
            if not isinstance(config, dict) or config.get("schema") != "loom.nebius-management-certificate-installation.v1":
                raise ValueError()
        except (ValueError, KeyError, TypeError, zipfile.BadZipFile, RecursionError):
            raise RolloutError("management certificate bundle configuration invalid") from None
    operation = "loom-nebius-development-management-certificate-v1" if management_only else "loom-nebius-certificate-v1"
    command = ["ssh", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes", "-o", "IdentitiesOnly=yes",
               "-o", "ConnectTimeout=15", "-o", "ServerAliveInterval=15", "-o", "ServerAliveCountMax=3",
               "-o", "UserKnownHostsFile=" + str(known_hosts), "-i", str(key), target,
               operation]
    try:
        result = subprocess.run(command, input=content, capture_output=True, timeout=2400, check=False)
        if result.returncode == 126:
            raise CertificateAuthorityDeniedError("certificate transport authority rejected")
        if result.returncode:
            raise RolloutError("certificate gateway operation failed; preserve private state before retry")
        return management_report(result.stdout, config) if management_only else safe_report(result.stdout)
    except (OSError, subprocess.TimeoutExpired):
        raise RolloutError("certificate gateway outcome unknown; reconcile before retry") from None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--requirements", type=Path, required=True)
    parser.add_argument("--evidence-dir", type=Path, required=True)
    parser.add_argument("--prepare-bundle", type=Path,
                        help="Operator-only: write an exact bundle for forced-command installation; no SSH")
    parser.add_argument("--management-only", action="store_true",
                        help="Use only the dedicated exact-host dev-manager issuer configuration")
    args = parser.parse_args()
    result: dict[str, Any] = {"status": "blocked"}
    try:
        variable = ("NEBIUS_DEVELOPMENT_MANAGEMENT_CERTIFICATE_INSTALLATION_JSON" if args.management_only
                    else "NEBIUS_CERTIFICATE_INSTALLATION_JSON")
        raw = os.environ.get(variable, "")
        if not 0 < len(raw.encode()) <= 16_384:
            raise RolloutError("protected certificate installation input missing or oversized")
        with tempfile.TemporaryDirectory(prefix="loom-certificate-config-") as directory:
            path = Path(directory) / "installation.json"
            path.touch(mode=0o600)
            path.write_text(raw)
            config = load_installation(path, management_only=True) if args.management_only else load_installation(path)
        executable = shutil.which("uv")
        if executable is None:
            raise RolloutError("qualified tooling installer unavailable")
        version = subprocess.run([executable, "--version"], capture_output=True, timeout=15, check=False)
        if version.returncode or version.stdout.strip() != b"uv 0.11.26 (x86_64-unknown-linux-gnu)":
            raise RolloutError("tooling installer differs from qualified pin or architecture")
        content = build_bundle(config, uv=Path(executable), requirements=args.requirements,
                               management_only=args.management_only)
        if args.prepare_bundle is not None:
            descriptor = os.open(args.prepare_bundle, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(content)
                stream.flush()
                os.fsync(stream.fileno())
            result = {"status": "prepared", "bundle_sha256": hashlib.sha256(content).hexdigest()}
        else:
            result = transfer(content, target=os.environ["LOOM_DEPLOY_SSH_TARGET"],
                              key=Path(os.environ["LOOM_DEPLOY_SSH_KEY_FILE"]),
                              known_hosts=Path(os.environ["LOOM_DEPLOY_SSH_KNOWN_HOSTS_FILE"]),
                              management_only=args.management_only)
    except CertificateAuthorityDeniedError:
        result = {"status": "blocked", "reason": "certificate_transport_authority_rejected"}
    except Exception:
        # Neither exception messages nor SSH/client output are public evidence.
        result = {"status": "blocked", "reason": "certificate operation failed; reconcile private gateway state"}
    args.evidence_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    (args.evidence_dir / "certificate-result.json").write_text(json.dumps(result, sort_keys=True) + "\n")
    print(json.dumps(result, sort_keys=True))
    return 0 if result["status"] == "qualified" or (args.prepare_bundle is not None and result["status"] == "prepared") else 1


if __name__ == "__main__":
    raise SystemExit(main())
