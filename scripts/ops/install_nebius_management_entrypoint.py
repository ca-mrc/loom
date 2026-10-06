#!/usr/bin/env python3
"""Operator-only, stdlib installer for one exact management tooling authority.

This is not a remotely callable installer. Run the reviewed version through the
gateway's existing operator route; subsequent installation uses protected Actions.
"""
from __future__ import annotations

import argparse
import base64
import fcntl
import hashlib
import io
import json
import os
import re
import stat
import struct
import tempfile
import zipfile
from pathlib import Path
from typing import Any
from uuid import UUID

MAX_BUNDLE = 100 * 1024 * 1024
COMMANDS = ("loom-nebius-management-preflight-v1", "loom-nebius-management-install-v1")
SOURCES = ("scripts/ops/nebius_management_gateway.py", "scripts/ops/nebius_certificate_gateway.py")


class InstallError(RuntimeError):
    """Fixed installer failure; existing authority is preserved."""


def _read(path: Path, limit: int) -> bytes:
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except OSError:
        raise InstallError("private input unavailable") from None
    with os.fdopen(fd, "rb") as stream:
        info = os.fstat(stream.fileno())
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                or info.st_mode & 0o077 or info.st_nlink != 1):
            raise InstallError("private owned regular file required")
        value = stream.read(limit + 1)
    if len(value) > limit:
        raise InstallError("input exceeds bound")
    return value


def _directory(path: Path, *, create: bool = False) -> None:
    if not path.is_absolute() or path != path.resolve():
        raise InstallError("authority paths must not traverse symlinks")
    if create:
        path.mkdir(mode=0o700, exist_ok=True)
    info = path.stat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o022:
        raise InstallError("trusted owned parent directory required")
    if create and info.st_mode & 0o077:
        raise InstallError("authority directory must be private")


def _sync(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _key(value: str) -> str:
    lines = value.strip().splitlines()
    if len(lines) != 1:
        raise InstallError("one plain Ed25519 public key required")
    fields = lines[0].split()
    if len(fields) < 2 or fields[0] != "ssh-ed25519":
        raise InstallError("SSH options cannot be supplied by the key")
    try:
        raw = base64.b64decode(fields[1], validate=True)
    except ValueError:
        raise InstallError("invalid public key") from None
    if (len(raw) != 51 or raw[:19] != struct.pack(">I", 11) + b"ssh-ed25519" + struct.pack(">I", 32)):
        raise InstallError("invalid Ed25519 public key")
    return "ssh-ed25519 " + base64.b64encode(raw).decode()


def _entrypoint(source_hashes: dict[str, str], bundle_digest: str, *, pool: bool = False) -> bytes:
    # Both the management bootstrap and unchanged certificate supervisor are
    # reviewed local source. Never resolve either from caller-controlled imports.
    commands = (*COMMANDS, 'loom-nebius-pool-rollback-v1') if pool else COMMANDS
    return f'''import hashlib, os, stat, sys
from pathlib import Path
try:
    if os.environ.get("SSH_ORIGINAL_COMMAND") not in {commands!r}:
        raise ValueError()
    root = Path(__file__).resolve().parent
    for name, expected in {source_hashes!r}.items():
        path = root / name
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(fd, "rb") as stream:
            info = os.fstat(stream.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077 or info.st_nlink != 1:
                raise ValueError()
            source = stream.read(262145)
        if hashlib.sha256(source).hexdigest() != expected:
            raise ValueError()
    sys.path.insert(0, str(root))
    from scripts.ops.nebius_management_gateway import authorized_main
    code = authorized_main({bundle_digest!r})
except Exception:
    code = 126
raise SystemExit(code)
'''.encode()


def install(content: bytes, *, expected_sha256: str, public_key: str, apply: bool = False) -> dict[str, Any]:
    if (not re.fullmatch(r"[0-9a-f]{64}", expected_sha256) or not 0 < len(content) <= MAX_BUNDLE
            or hashlib.sha256(content).hexdigest() != expected_sha256):
        raise InstallError("bundle differs from approved digest")
    key = _key(public_key)
    try:
        with zipfile.ZipFile(io.BytesIO(content)) as archive:
            names = archive.namelist()
            if len(names) > 128 or len(set(names)) != len(names):
                raise InstallError("invalid tooling archive")
            for name, maximum in (("operation.json", 16384), *((name, 262144) for name in SOURCES)):
                if archive.getinfo(name).file_size > maximum:
                    raise InstallError("tooling member exceeds bound")
            config = json.loads(archive.read("operation.json"))
            source_files = {name: archive.read(name) for name in SOURCES}
            source_files.update({"scripts/__init__.py": b"", "scripts/ops/__init__.py": b""})
        identity = str(UUID(config["installation_id"]))
        if not re.fullmatch(r"[0-9a-f]{40}", config["source_sha"]):
            raise ValueError()
        if UUID(identity).int == 0:
            raise ValueError()
        state = Path(config["state_dir"])
        root = state.parent
        separated = {"loom.nebius-management-upgrade-operation.v1": "upgrade",
                     "loom.nebius-management-retirement-operation.v1": "retirement",
                     "loom.nebius-management-retirement-diagnostic-operation.v1": "retirement-diagnostic",
                     "loom.nebius-management-retirement-recovery-operation.v1": "retirement-recovery"}
        repair = config['schema'] == 'loom.nebius-pool-startup-repair-operation.v1'
        pool = config['schema'] == 'loom.nebius-pool-cutover-operation.v1' or repair
        if config["schema"] == "loom.nebius-management-refresh-operation.v1" or pool:
            operation_id = UUID(config["operation_id"])
            if (not operation_id.int or str(operation_id) != config["operation_id"]
                    or root.name != str(operation_id) or root.parent.name != ('pool-repair' if repair else 'pool-cutover' if pool else 'refresh')
                    or config["source_sha"] != config["candidate"]):
                raise ValueError()
            if repair and (str(UUID(config['original_operation_id'])) != config['original_operation_id']
                    or not UUID(config['original_operation_id']).int or config['original_operation_id'] == config['operation_id']):
                raise ValueError()
            root = root.parent.parent
        elif config["schema"] in separated:
            if root.name != separated[config["schema"]]:
                raise ValueError()
            root = root.parent
        elif config["schema"] != "loom.nebius-management-operation.v1":
            raise ValueError()
        if (state.name != "state" or root.name != "nebius-management" or root.parent.name != ".loom"
                or Path(config["anchor_dir"]) != state.parent / "anchor"
                or Path(config["inputs_path"]) != state.parent / "inputs.json"
                or not re.fullmatch(r"/[A-Za-z0-9_./-]+", str(state)) or state != state.resolve()):
            raise ValueError()
    except (ValueError, KeyError, TypeError, zipfile.BadZipFile):
        raise InstallError("invalid management installation bundle") from None
    home = root.parent.parent
    ssh = home / ".ssh"
    keys = ssh / "authorized_keys"
    for parent in (home, root.parent, ssh):
        _directory(parent)
    destination = root / "authority" / expected_sha256
    line = (f'restrict,command="/usr/bin/python3 -I {destination}/entrypoint.py" '
            f'{key} loom-nebius-management-{identity}\n').encode()

    def check_keys() -> bytes:
        previous = _read(keys, 1024 * 1024)
        same_key = re.compile(rb"(?:^|[ \t])ssh-ed25519[ \t]+" + re.escape(key.split()[1].encode()) + rb"(?:[ \t]|$)")
        for existing in previous.splitlines():
            if same_key.search(existing) and existing != line.rstrip(b"\n"):
                raise InstallError("public key already has different authority")
        return previous

    check_keys()
    source_hashes = {name: hashlib.sha256(value).hexdigest() for name, value in source_files.items()}
    report = {"schema": "loom.nebius-management-authority.v1", "status": "installed" if apply else "prepared",
              "installation_id": identity, "bundle_sha256": expected_sha256,
              "source_sha": config["source_sha"], "source_hashes": source_hashes,
              "public_key_sha256": hashlib.sha256(key.encode()).hexdigest()}
    if not apply:
        return report
    # Share the existing certificate installer lock: both preserve authorized_keys.
    lock_path = ssh / "loom-certificate-authority.lock"
    fd = os.open(lock_path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
    with os.fdopen(fd, "rb+") as lock:
        _read(lock_path, 1)
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        previous = check_keys()
        for directory in (root, root / "authority", destination, destination / "scripts", destination / "scripts/ops"):
            _directory(directory, create=True)
            _sync(directory.parent)
        files = {**source_files, "entrypoint.py": _entrypoint(source_hashes, expected_sha256, pool=pool),
                 "receipt.json": json.dumps(report, sort_keys=True).encode()}
        for name, value in files.items():
            path = destination / name
            if path.exists() or path.is_symlink():
                if _read(path, 262144) != value:
                    raise InstallError("installed authority differs; preserve for reconciliation")
            else:
                descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
                with os.fdopen(descriptor, "wb") as stream:
                    stream.write(value)
                    stream.flush()
                    os.fsync(stream.fileno())
        _sync(destination)
        if line.rstrip(b"\n") not in previous.splitlines():
            updated = previous + (b"\n" if previous and not previous.endswith(b"\n") else b"") + line
            with tempfile.NamedTemporaryFile(dir=ssh, prefix=".management-authority-", delete=False) as staging:
                temporary = Path(staging.name)
                staging.write(updated)
                staging.flush()
                os.fsync(staging.fileno())
            try:
                # Cooperative operator lock plus immediate readback preserves
                # detected concurrent edits; same-UID hostile writers are not
                # a supported trust boundary.
                if check_keys() != previous:
                    raise InstallError("authorized keys changed during installation")
                os.replace(temporary, keys)
                _sync(ssh)
            finally:
                temporary.unlink(missing_ok=True)
            if _read(keys, 1024 * 1024) != updated:
                raise InstallError("authorized key readback differs; reconcile before retry")
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--bundle-sha256", required=True)
    parser.add_argument("--public-key", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    try:
        result = install(_read(args.bundle, MAX_BUNDLE), expected_sha256=args.bundle_sha256,
                         public_key=_read(args.public_key, 16384).decode(), apply=args.apply)
    except Exception:
        print(json.dumps({"status": "blocked", "reason": "management authority installation requires reconciliation"}))
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
