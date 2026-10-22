"""Stdlib-only fixed shared handoff delivery; separate from all CLOSED-runtime grants."""
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
from scripts.ops.nebius_development_runtime_gateway import SOURCES as RUNTIME_SOURCES
from scripts.ops.nebius_shared_pool_command import (
    DIAGNOSTIC_STAGES,
    FORWARD_PHASES,
    RECOVERY_PHASES,
    validate_operation,
)

SOURCES = RUNTIME_SOURCES + tuple("scripts/ops/" + name + ".py" for name in (
    "nebius_development_runtime_selection",
    "nebius_development_schema_catalog",
    "nebius_development_schema_jobs",
    "nebius_development_schema_projection",
    "nebius_development_schema_resources",
    "nebius_development_schema_upgrade",
    "nebius_environment_foundation",
    "nebius_environment_release_contracts",
    "nebius_environment_runtime",
    "nebius_environment_runtime_install",
    "nebius_foundation_readiness",
    "nebius_platform_namespace",
    "nebius_pool_administration_dispatch",
    "nebius_pool_administration_job",
    "nebius_pool_enrollment_issuer",
    "nebius_closed_registry",
    "nebius_pool_participant_catalog",
    "nebius_pool_participant_release",
    "nebius_pool_preactivation_database",
    "nebius_pool_preactivation_live",
    "nebius_pool_preactivation_operation",
    "nebius_pool_preactivation_registry",
    "nebius_pool_storage_binding",
    "nebius_pool_storage_capacity",
    "nebius_pool_storage_live",
    "nebius_pool_storage_operation",
    "nebius_pool_storage_registry",
    "nebius_pool_storage_transport",
    "nebius_shared_pool_activation_live",
    "nebius_shared_pool_api",
    "nebius_shared_pool_delivery_gateway",
    "nebius_shared_pool_drain",
    "nebius_shared_pool_entry",
    "nebius_shared_pool_recovery_collector",
    "nebius_shared_pool_recovery_databases",
    "nebius_shared_pool_recovery_fence_live",
    "nebius_shared_pool_recovery_processes",
))
LIMITS = {**dict.fromkeys(SOURCES, 262144), "uv": 80 * 1024**2,
    "requirements.txt": 262144, "operation.json": 16384, "shared-pool-source.json": 4096, "manifest.json": 65536}
MAX_BUNDLE, MAX_WHEEL = 100 * 1024**2, 16 * 1024**2
COMMANDS = {"loom-nebius-shared-pool-preflight-v1": "preflight", "loom-nebius-shared-pool-install-v1": "install",
    "loom-nebius-shared-pool-recover-v1": "recover", "loom-nebius-shared-pool-refresh-catalog-v1": "refresh-catalog",
    "loom-nebius-shared-pool-refresh-storage-v1": "refresh-storage"}
# main emits a closed blocked report on an ordinary failure. Preserve that report
# for validation, without relaxing the watchdog's handling of crash/timeout/output.
_ENTRY = ("import sys; sys.path.insert(0, sys.argv[1]); "
    "from scripts.ops.nebius_shared_pool_entry import main; "
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
        source = json.loads(files["shared-pool-source.json"])
        if (set(source) != {"source_sha", "source_archive_sha256"} or source["source_sha"] != operation["source_sha"]
                or not re.fullmatch(r"sha256:[0-9a-f]{64}", source["source_archive_sha256"])):
            raise ValueError()
        return files, operation
    except Exception:
        raise GatewayError("invalid shared pool tooling bundle") from None


def command(release: Path, action: str) -> list[str]:
    if action not in {"qualify", *COMMANDS.values()}:
        raise GatewayError("shared pool action outside fixed authority")
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
        raise GatewayError("shared pool tooling incomplete; retain private state") from None


def safe_report(raw: bytes, operation: dict[str, Any], action: str) -> dict[str, Any]:
    """Export exact direction-bound progress, never private inputs or false readiness."""
    from uuid import UUID

    try:
        validate_operation(operation)
        if len(raw) > 16384 or action not in COMMANDS.values():
            raise ValueError()
        value = json.loads(raw)
        status = value["status"]
        allowed = {"preflight": {"shared_pool_preflight_qualified"},
            "install": {"pending", "shared_pool_handoff_complete"},
            "recover": {"pending", "staging_restored_dev_closed"},
            "refresh-catalog": {"catalog_pending", "shared_pool_catalog_refreshed"},
            "refresh-storage": {"storage_pending", "shared_pool_storage_refreshed"}}[action]
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
            return result
        pool = value["pool_id"]
        if not isinstance(pool, str) or str(UUID(pool)) != pool or not UUID(pool).int:
            raise ValueError()
        result["pool_id"] = pool
        opened, complete = value["admission_open"], value["writer_migration_complete"]
        if type(opened) is not bool or type(complete) is not bool or opened != complete:
            raise ValueError()
        if status != "shared_pool_preflight_qualified" and opened != (status == "shared_pool_handoff_complete"):
            raise ValueError()
        result.update(admission_open=opened, writer_migration_complete=complete)
        if action in {"refresh-catalog", "refresh-storage"}:
            field = "preactivation_id" if action == "refresh-catalog" else "storage_id"
            selected = value[field]
            if not isinstance(selected, str) or str(UUID(selected)) != selected or not UUID(selected).int:
                raise ValueError()
            result[field] = selected
        if status == "pending":
            phases = FORWARD_PHASES if action == "install" else RECOVERY_PHASES
            if value["phase"] not in phases:
                raise ValueError()
            result["phase"] = value["phase"]
        elif "phase" in value:
            raise ValueError()
        return result
    except Exception:
        raise GatewayError("invalid shared pool report") from None


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
        print("protected shared pool operation incomplete; preserve private recovery state", file=sys.stderr)
        return 1
