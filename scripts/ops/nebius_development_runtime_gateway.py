"""Stdlib-only, source-bound closed dev runtime installation; no initial-install/staging commands."""
from __future__ import annotations

import fcntl
import hashlib
import io
import json
import os
import re
import stat
import sys
import zipfile
from pathlib import Path
from typing import Any

from scripts.ops.nebius_certificate_gateway import _directory, _read, _write, run_private
from scripts.ops.nebius_development_pool_gateway import SOURCES as POOL_SOURCES
from scripts.ops.nebius_development_runtime_operation import (
    DIAGNOSTIC_STAGES,
    validate_operation,
)

SOURCES = POOL_SOURCES + tuple("scripts/ops/" + name + ".py" for name in (
    "nebius_development_runtime_entry", "nebius_development_runtime_operation",
    "nebius_development_runtime_gateway", "nebius_development_runtime_api",
    "nebius_database_readiness", "nebius_development_actuator_runtime",
    "nebius_development_build_cloud", "nebius_development_build_policy", "nebius_development_build_runtime",
    "nebius_development_catalog_runtime", "nebius_development_collector_cloud", "nebius_development_collector_runtime",
    "nebius_development_network_runtime", "nebius_development_pool_retained",
    "nebius_development_runtime_external", "nebius_development_runtime_install", "nebius_development_runtime_job",
    "nebius_development_runtime_live", "nebius_development_runtime_observation", "nebius_development_runtime_probes",
    "nebius_development_runtime_readiness", "nebius_development_runtime_render", "nebius_development_runtime_setup",
    "nebius_development_runtime_transition", "nebius_development_shared_runtime",
    "deploy_nebius_platform", "nebius_legacy_pool_completion", "nebius_management_gateway", "nebius_management_switch",
    "nebius_pool_activation_database", "nebius_pool_dormant", "nebius_pool_guard_activation", "nebius_pool_legacy_settings",
    "nebius_pool_migration", "nebius_pool_migration_guard", "nebius_pool_projection", "nebius_pool_recovery_database",
    "nebius_pool_recovery_release", "nebius_pool_retirement", "nebius_pool_runtime", "nebius_pool_runtime_settings",
    "nebius_pool_startup_database",
))
LIMITS = {**dict.fromkeys(SOURCES, 262144), "uv": 80 * 1024**2,
    "requirements.txt": 262144, "operation.json": 16384, "development-runtime-source.json": 4096, "manifest.json": 16384}
MAX_BUNDLE, MAX_WHEEL = 100 * 1024**2, 16 * 1024**2
COMMANDS = {"loom-nebius-development-runtime-preflight-v1": "preflight", "loom-nebius-development-runtime-install-v1": "install"}
# main emits a closed blocked report on an ordinary failure. Preserve that report
# for validation, without relaxing the watchdog's handling of crash/timeout/output.
_ENTRY = ("import sys; sys.path.insert(0, sys.argv[1]); "
    "from scripts.ops.nebius_development_runtime_entry import main; "
    "code=main(sys.argv[2], sys.argv[3]); raise SystemExit(0 if code in (0, 1) else 1)")


class GatewayError(RuntimeError):
    """Closed failure; preserve uncertain private installation state."""


def unpack_bundle(content: bytes) -> tuple[dict[str, bytes], dict[str, Any]]:
    try:
        if not 0 < len(content) <= MAX_BUNDLE:
            raise ValueError()
        with zipfile.ZipFile(io.BytesIO(content)) as archive:
            entries = archive.infolist()
            names = [entry.filename for entry in entries]
            wheels = {name for name in names if name.startswith("wheels/")}
            if (len(names) != len(set(names)) or set(names) != set(LIMITS) | wheels or len(wheels) != 2
                    or not all(sum(bool(re.fullmatch(r"wheels/" + package + r"-[0-9][0-9.]*-py3-none-any\.whl", name))
                        for name in wheels) == 1 for package in ("loom", "loom_bundle_checksum"))):
                raise ValueError()
            if any(entry.file_size > LIMITS.get(entry.filename, MAX_WHEEL) or entry.is_dir()
                    or stat.S_IFMT(entry.external_attr >> 16) not in {0, stat.S_IFREG}
                    or entry.flag_bits & 1 for entry in entries):
                raise ValueError()
            files = {entry.filename: archive.read(entry) for entry in entries}
        if json.loads(files["manifest.json"]) != {name: hashlib.sha256(value).hexdigest()
                for name, value in files.items() if name != "manifest.json"}:
            raise ValueError()
        operation = json.loads(files["operation.json"])
        validate_operation(operation)
        source = json.loads(files["development-runtime-source.json"])
        if (set(source) != {"source_sha", "source_archive_sha256"} or source["source_sha"] != operation["source_sha"]
                or not re.fullmatch(r"sha256:[0-9a-f]{64}", source["source_archive_sha256"])):
            raise ValueError()
        return files, operation
    except Exception:
        raise GatewayError("invalid development tooling bundle") from None


def command(release: Path, action: str) -> list[str]:
    if action not in {"qualify", "preflight", "install"}:
        raise GatewayError("development action outside fixed authority")
    return [str(release / "venv/bin/python"), "-I", "-c", _ENTRY,
        str(release), str(release / "operation.json"), action]


def prepare_release(content: bytes) -> Path:
    """Called only after whole-bundle authentication; never retry a partial setup."""
    files, operation = unpack_bundle(content)
    root = Path(operation["inputs_path"]).parent
    try:
        _directory(root)
        lock_path = root / "tooling.lock"
        descriptor = os.open(lock_path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
        with os.fdopen(descriptor, "rb+") as lock:
            _read(lock_path, 1)
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            releases = root / "releases"
            _directory(releases)
            release = releases / hashlib.sha256(content).hexdigest()
            if release.exists() or release.is_symlink():
                _directory(release)
                if _read(release / "complete", 64) != b"complete":
                    raise ValueError()
                for name, expected in files.items():
                    if _read(release / name, LIMITS.get(name, MAX_WHEEL)) != expected:
                        raise ValueError()
                return release
            if sum(1 for _ in releases.iterdir()) >= 8:
                raise ValueError()
            for path in (release, release / "scripts", release / "scripts/ops", release / "wheels",
                    release / "deploy", release / "deploy/k8s"):
                _directory(path)
            for name, value in files.items():
                _write(release / name, value, executable=name == "uv")
            uv, python = str(release / "uv"), str(release / "venv/bin/python")
            run_private([uv, "venv", "--no-config", "--no-cache", "--no-python-downloads",
                "--python", "/usr/bin/python3", str(release / "venv")], timeout=90)
            run_private([uv, "pip", "sync", "--no-config", "--no-cache", "--python", python,
                "--require-hashes", "--only-binary", ":all:", "--index-url", "https://pypi.org/simple",
                str(release / "requirements.txt")], timeout=600)
            run_private([uv, "pip", "install", "--no-config", "--no-cache", "--python", python,
                "--offline", "--no-deps", *sorted(str(release / name) for name in files if name.startswith("wheels/"))], timeout=90)
            if json.loads(run_private(command(release, "qualify"), timeout=60)) != {"status": "tooling_qualified"}:
                raise ValueError()
            _write(release / "complete", b"complete")
            return release
    except Exception:
        raise GatewayError("development tooling incomplete; retain private state") from None


def safe_report(raw: bytes, operation: dict[str, Any], action: str) -> dict[str, Any]:
    """Export only bounded closed-installation evidence, never credentials."""
    from uuid import UUID

    try:
        validate_operation(operation)
        if len(raw) > 16384 or action not in COMMANDS.values():
            raise ValueError()
        value = json.loads(raw)
        status = value["status"]
        allowed = ({"development_runtime_preflight_qualified"} if action == "preflight"
            else {"pending_database", "pending_catalog", "pending_stop", "pending_replace",
                "pending_control", "pending_start", "development_runtime_installed_closed"})
        if status not in allowed | {"blocked"}:
            raise ValueError()
        result = {"status": status}
        for key in ("source_sha", "installation_id", "operation_id", "namespace"):
            if (key in value and value[key] != operation[key]) or (status != "blocked" and key not in value):
                raise ValueError()
            result[key] = operation[key]
        if status == "blocked":
            if value["stage"] not in DIAGNOSTIC_STAGES:
                raise ValueError()
            result["stage"] = value["stage"]
        else:
            pool = value["pool_id"]
            if not isinstance(pool, str) or str(UUID(pool)) != pool or not UUID(pool).int:
                raise ValueError()
            result["pool_id"] = pool
            for key in ("admission_open", "writer_migration_complete"):
                if value[key] is not False:
                    raise ValueError()
                result[key] = False
        return result
    except Exception:
        raise GatewayError("invalid development runtime report") from None


def authorized_main(expected_sha256: str) -> int:
    action = COMMANDS.get(os.environ.get("SSH_ORIGINAL_COMMAND", ""))
    if action is None or not re.fullmatch(r"[0-9a-f]{64}", expected_sha256):
        return 126
    content = sys.stdin.buffer.read(MAX_BUNDLE + 1)
    if not 0 < len(content) <= MAX_BUNDLE or hashlib.sha256(content).hexdigest() != expected_sha256:
        return 126
    try:
        _, operation = unpack_bundle(content)
        release = prepare_release(content)
        report = safe_report(run_private(command(release, action), timeout=1800), operation, action)
        print(json.dumps(report, sort_keys=True))
        return 0  # delivery acknowledgement, not installed acceptance
    except Exception:
        print("protected development operation incomplete; preserve private recovery state", file=sys.stderr)
        return 1
