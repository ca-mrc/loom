"""Operator-only dev grant. Run from the exact reviewed checkout, never over SSH.

Reuses stdlib file/key primitives, not management operation or grant authority.
No Kubernetes requests, private-input creation, or staging key replacement.
"""
from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import re
import tempfile
from pathlib import Path
from typing import Any

from scripts.ops.install_nebius_management_entrypoint import (
    InstallError,
    _directory,
    _key,
    _read,
    _sync,
)
from scripts.ops.nebius_development_management_gateway import COMMANDS, MAX_BUNDLE, unpack_bundle

BOOTSTRAP_SOURCES = ("scripts/ops/nebius_development_management_gateway.py", "scripts/ops/nebius_development_management_operation.py",
    "scripts/ops/nebius_certificate_gateway.py")


def _entrypoint(hashes: dict[str, str], digest: str) -> bytes:
    return f'''import hashlib, os, stat, sys
from pathlib import Path
try:
    if os.environ.get("SSH_ORIGINAL_COMMAND") not in {tuple(COMMANDS)!r}:
        raise ValueError()
    root = Path(__file__).resolve().parent
    for name, expected in {hashes!r}.items():
        fd = os.open(root / name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(fd, "rb") as stream:
            info = os.fstat(stream.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077 or info.st_nlink != 1:
                raise ValueError()
            raw = stream.read(262145)
        if hashlib.sha256(raw).hexdigest() != expected:
            raise ValueError()
    sys.path.insert(0, str(root))
    from scripts.ops.nebius_development_management_gateway import authorized_main
    code = authorized_main({digest!r})
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
        files, operation = unpack_bundle(content)
        root = Path(operation["state_dir"]).parent
        owner = root.parent.parent
        if owner.name != ".loom":
            raise ValueError()
    except Exception:
        raise InstallError("invalid development installation bundle") from None
    home, ssh = owner.parent, owner.parent / ".ssh"
    keys = ssh / "authorized_keys"
    for parent in (home, owner, ssh):
        _directory(parent)
    destination = root / "authority" / expected_sha256
    line = (f'restrict,command="/usr/bin/python3 -I {destination}/entrypoint.py" '
        f'{key} loom-nebius-development-management-{operation["installation_id"]}\n').encode()

    def check_keys() -> bytes:
        previous = _read(keys, 1024 * 1024)
        match = re.compile(rb"(?:^|[ \t])ssh-ed25519[ \t]+" + re.escape(key.split()[1].encode()) + rb"(?:[ \t]|$)")
        if any(match.search(existing) and existing != line.rstrip(b"\n") for existing in previous.splitlines()):
            raise InstallError("public key already has different authority")
        return previous

    check_keys()
    sources = {name: files[name] for name in BOOTSTRAP_SOURCES} | {"scripts/__init__.py": b"", "scripts/ops/__init__.py": b""}
    hashes = {name: hashlib.sha256(value).hexdigest() for name, value in sources.items()}
    report = {"schema": "loom.nebius-development-management-authority.v1", "status": "installed" if apply else "prepared",
        "installation_id": operation["installation_id"], "bundle_sha256": expected_sha256,
        "source_sha": operation["source_sha"], "source_hashes": hashes, "public_key_sha256": hashlib.sha256(key.encode()).hexdigest()}
    if not apply:
        return report
    # Cooperate with the existing operator installers without changing their keys.
    lock_path = ssh / "loom-certificate-authority.lock"
    fd = os.open(lock_path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
    with os.fdopen(fd, "rb+") as lock:
        _read(lock_path, 1)
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        previous = check_keys()
        for directory in (root.parent, root, Path(operation["anchor_dir"]).parent,
                root / "authority", destination, destination / "scripts", destination / "scripts/ops"):
            _directory(directory, create=True)
            _sync(directory.parent)
        sources.update({"entrypoint.py": _entrypoint(hashes, expected_sha256), "receipt.json": json.dumps(report, sort_keys=True).encode()})
        for name, value in sources.items():
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
            with tempfile.NamedTemporaryFile(dir=ssh, prefix=".development-authority-", delete=False) as staging:
                temporary = Path(staging.name)
                staging.write(updated)
                staging.flush()
                os.fsync(staging.fileno())
            try:
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
        print(json.dumps({"status": "blocked", "reason": "development authority installation requires reconciliation"}))
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

