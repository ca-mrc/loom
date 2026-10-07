"""Protected dev tooling publication; private installation inputs stay remote."""
from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import re
import shutil
import subprocess
import tempfile
import zipfile
from pathlib import Path
from typing import Any

from scripts.ops.nebius_certificate_gateway import _write
from scripts.ops.nebius_development_management_gateway import (
    COMMANDS,
    LIMITS,
    MAX_WHEEL,
    SOURCES,
    safe_report,
    unpack_bundle,
)
from scripts.ops.nebius_development_management_operation import validate_operation
from scripts.ops.nebius_development_preflight import prepare_development_source
from scripts.ops.nebius_ingress_rollout import build_wheels

ROOT = Path(__file__).resolve().parents[2]


class RolloutError(RuntimeError):
    """Only bounded operation evidence leaves the protected gateway."""


def verify_source(operation: dict[str, Any]) -> None:
    try:
        def git(*args: str) -> bytes:
            result = subprocess.run(["git", *args], cwd=ROOT, capture_output=True, timeout=30, check=False)
            if result.returncode:
                raise ValueError()
            return result.stdout.strip()

        validate_operation(operation)
        if (git("rev-parse", "HEAD").decode() != operation["source_sha"]
                or git("status", "--porcelain", "--untracked-files=normal")):
            raise ValueError()
        git("merge-base", "--is-ancestor", operation["source_sha"], "refs/remotes/origin/dev")
    except Exception:
        raise RolloutError("development tooling requires its exact clean integrated source") from None


def build_bundle(operation: dict[str, Any], *, source: dict[str, Any], uv: Path, requirements: Path, wheels: Path) -> bytes:
    try:
        validate_operation(operation)
        paths = {"uv": uv, "requirements.txt": requirements, **{name: ROOT / name for name in SOURCES},
            **{"wheels/" + path.name: path for path in wheels.iterdir()}}
        files = {}
        for name, path in paths.items():
            limit = LIMITS.get(name, MAX_WHEEL)
            with path.open("rb") as stream:
                files[name] = stream.read(limit + 1)
            if len(files[name]) > limit:
                raise ValueError()
        files["operation.json"] = json.dumps(operation, sort_keys=True).encode()
        files["development-management-source.json"] = json.dumps(source, sort_keys=True).encode()
        files["manifest.json"] = json.dumps({name: hashlib.sha256(value).hexdigest()
            for name, value in files.items()}, sort_keys=True).encode()
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            for name, value in sorted(files.items()):
                entry = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
                entry.create_system = 3
                entry.compress_type = zipfile.ZIP_DEFLATED
                entry.external_attr = 0o100600 << 16
                archive.writestr(entry, value)
        content = buffer.getvalue()
        unpack_bundle(content)
        return content
    except Exception:
        raise RolloutError("development bundle is unqualified") from None


def transfer(content: bytes, *, action: str, target: str, key: Path, known_hosts: Path) -> dict[str, Any]:
    commands = {value: key for key, value in COMMANDS.items()}
    if (action not in commands or not re.fullmatch(r"[a-zA-Z0-9_.-]+@[a-zA-Z0-9_.-]+", target)
            or not key.is_absolute() or not known_hosts.is_absolute()):
        raise RolloutError("invalid protected development transport")
    try:
        _, operation = unpack_bundle(content)
        args = ["ssh", "-F", "/dev/null", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes",
            "-o", "IdentitiesOnly=yes", "-o", "ConnectTimeout=15", "-o", "ServerAliveInterval=15",
            "-o", "ServerAliveCountMax=3", "-o", "UserKnownHostsFile=" + str(known_hosts),
            "-i", str(key), target, commands[action]]
        result = subprocess.run(args, input=content, capture_output=True, timeout=2400, check=False)
        if result.returncode:
            raise ValueError()
        return safe_report(result.stdout, operation, action)
    except Exception:
        raise RolloutError("development outcome unavailable; reconcile before retry") from None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--operation", choices=("preflight", "install"), required=True)
    parser.add_argument("--requirements", type=Path, required=True)
    parser.add_argument("--evidence-dir", type=Path, required=True)
    parser.add_argument("--prepare-bundle", type=Path, help="Operator preparation; no remote operation")
    args = parser.parse_args()
    phase = "input"
    result: dict[str, Any]
    try:
        raw = os.environ.get("NEBIUS_DEVELOPMENT_MANAGEMENT_OPERATION_JSON", "")
        if not 0 < len(raw.encode()) <= 16384:
            raise ValueError()
        operation = json.loads(raw)
        verify_source(operation)
        source = prepare_development_source(operation["source_sha"]).model_dump(mode="json")
        executable = shutil.which("uv")
        if executable is None:
            raise ValueError()
        version = subprocess.run([executable, "--version"], capture_output=True, timeout=15, check=False)
        if version.returncode or version.stdout.strip() != b"uv 0.11.26 (x86_64-unknown-linux-gnu)":
            raise ValueError()
        phase = "tooling_preparation"
        with tempfile.TemporaryDirectory(prefix="loom-development-management-rollout-") as temporary:
            wheels = build_wheels(Path(temporary), uv=Path(executable))
            content = build_bundle(operation, source=source, uv=Path(executable), requirements=args.requirements, wheels=wheels)
            verify_source(operation)
            if args.prepare_bundle:
                _write(args.prepare_bundle, content)
                result = {"status": "prepared", "bundle_sha256": hashlib.sha256(content).hexdigest(),
                    "source_sha": operation["source_sha"]}
            else:
                phase = "gateway_operation"
                result = transfer(content, action=args.operation, target=os.environ["LOOM_DEPLOY_SSH_TARGET"],
                    key=Path(os.environ["LOOM_DEPLOY_SSH_KEY_FILE"]),
                    known_hosts=Path(os.environ["LOOM_DEPLOY_SSH_KNOWN_HOSTS_FILE"]))
    except Exception:
        result = {"status": "blocked", "phase": phase}
    args.evidence_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    (args.evidence_dir / "development-management-result.json").write_text(json.dumps(result, sort_keys=True) + "\n")
    print(json.dumps(result, sort_keys=True))
    return 0 if result["status"] in {"prepared", "development_management_preflight_qualified", "pending", "development_management_installed"} else 1


if __name__ == "__main__":
    raise SystemExit(main())

