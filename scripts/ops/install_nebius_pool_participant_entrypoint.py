"""Operator-only installer for separately pinned participant-release authority.

Run the reviewed installer on the operator host, not through a rollout key.
It qualifies private inputs and tooling before adding a distinct restricted key;
it performs no Kubernetes, guard, provider or live database operation.
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

from scripts.ops import nebius_pool_participant_gateway as gateway
from scripts.ops.install_nebius_management_entrypoint import (
    InstallError,
    _directory,
    _key,
    _read,
    _sync,
)
from scripts.ops.install_nebius_shared_pool_entrypoint import (
    BOOTSTRAP_SOURCES as SHARED_BOOTSTRAP,
)
from scripts.ops.nebius_pool_participant_dispatch import (
    ParticipantReleaseHost,
    load_participant_release_host,
)
from scripts.ops.nebius_shared_pool_command import operation_root, operator_home, validate_operation

ROOT = Path(__file__).resolve().parents[2]
BOOTSTRAP_SOURCES = (*SHARED_BOOTSTRAP, 'scripts/ops/nebius_pool_participant_gateway.py')


def _selection(configuration: Path, checksum: str) -> tuple[ParticipantReleaseHost, dict[str, Any], Path, dict[Path, bytes]]:
    try:
        host = load_participant_release_host(configuration, checksum, repo_root=ROOT)
        files = {configuration: _read(configuration, gateway.MAX_COMMAND),
            host.shared_operation: _read(host.shared_operation, gateway.MAX_COMMAND)}
        if len(files) != 2 or hashlib.sha256(files[host.shared_operation]).hexdigest() != host.shared_operation_sha256:
            raise ValueError
        operation = gateway._json(files[host.shared_operation])
        validate_operation(operation)
        original_root = operation_root(operation)
        if (host.shared_operation != original_root / 'operation.json'
                and (host.shared_operation.name != 'operation.json'
                    or host.shared_operation.parent.parent != original_root / 'releases'
                    or re.fullmatch(r'[0-9a-f]{64}', host.shared_operation.parent.name) is None)):
            raise ValueError
        home = operator_home(operation)
        owner = home / '.loom'
        identity = operation['installation_id']
        if (not configuration.is_relative_to(owner)
                or host.state_root != owner / 'nebius-participant-releases' / identity
                or host.anchor_dir != owner / 'nebius-participant-anchors' / identity):
            raise ValueError
        material = (host.operator_connection.ca_file, host.operator_connection.credentials_file)
        if len(set(material)) != 2 or set(material) & {*files, Path(operation['inputs_path'])}:
            raise ValueError
        for path in material:
            if not path.is_absolute() or path != path.resolve():
                raise ValueError
            files[path] = _read(path, 16 * 1024**2)
        return host, operation, home, files
    except Exception:
        raise InstallError('participant installation inputs unqualified') from None


def _entrypoint(hashes: dict[str, str], *, release: Path, configuration: Path,
        configuration_sha256: str, manifest_sha256: str) -> bytes:
    return f'''import hashlib, os, stat, sys
from pathlib import Path
try:
    if os.environ.get("SSH_ORIGINAL_COMMAND") not in {tuple(gateway.COMMANDS)!r}:
        raise ValueError()
    root = Path(__file__).resolve().parent
    for name, expected in {hashes!r}.items():
        path = root / name
        if path != path.resolve():
            raise ValueError()
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(fd, "rb") as stream:
            info = os.fstat(stream.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077 or info.st_nlink != 1:
                raise ValueError()
            raw = stream.read(262145)
        if hashlib.sha256(raw).hexdigest() != expected:
            raise ValueError()
    sys.path.insert(0, str(root))
    from scripts.ops.nebius_pool_participant_gateway import authorized_main
    code = authorized_main(Path({str(release)!r}), Path({str(configuration)!r}), {configuration_sha256!r}, {manifest_sha256!r})
except Exception:
    code = 126
raise SystemExit(code)
'''.encode()


def install(content: bytes, *, expected_sha256: str, public_key: str,
        configuration: Path, configuration_sha256: str, apply: bool = False) -> dict[str, Any]:
    if (type(apply) is not bool or re.fullmatch(r'[0-9a-f]{64}', expected_sha256) is None
            or not 0 < len(content) <= gateway.MAX_BUNDLE or hashlib.sha256(content).hexdigest() != expected_sha256):
        raise InstallError('bundle differs from approved digest')
    key = _key(public_key)
    try:
        files, source = gateway.unpack_bundle(content)
        selected = _selection(configuration, configuration_sha256)
        host, operation, home, original = selected
        root = home / '.loom/nebius-participant-delivery' / operation['installation_id']
        # This path enters an OpenSSH forced-command string, not a shell-quoted
        # general path. Reject metacharacters before preparing any installation.
        if re.fullmatch(r'/[A-Za-z0-9_./-]+', str(root)) is None:
            raise ValueError
        ssh, keys = home / '.ssh', home / '.ssh/authorized_keys'
        for parent in (home, home / '.loom', ssh):
            _directory(parent)
        authority = hashlib.sha256((expected_sha256 + ':' + configuration_sha256).encode()).hexdigest()
        destination = root / 'authority' / authority
        release = root / 'tooling/releases' / expected_sha256
        line = (f'restrict,command="/usr/bin/python3 -I {destination}/entrypoint.py" '
            f'{key} loom-nebius-participant-{operation["installation_id"]}\n').encode()
        bootstrap = {name: files[name] for name in BOOTSTRAP_SOURCES}
        bootstrap.update({'scripts/__init__.py': b'', 'scripts/ops/__init__.py': b''})
        hashes = {name: hashlib.sha256(raw).hexdigest() for name, raw in bootstrap.items()}
        manifest_sha256 = hashlib.sha256(files['manifest.json']).hexdigest()
    except Exception:
        raise InstallError('invalid participant authority installation') from None

    def check_keys() -> bytes:
        previous = _read(keys, 1024 * 1024)
        same = re.compile(rb'(?:^|[ \t])ssh-ed25519[ \t]+' + re.escape(key.split()[1].encode()) + rb'(?:[ \t]|$)')
        if any(same.search(row) and row != line.rstrip(b'\n') for row in previous.splitlines()):
            raise InstallError('public key already has different authority')
        return previous

    def unchanged() -> None:
        if _selection(configuration, configuration_sha256) != selected or any(
                _read(path, 16 * 1024**2) != before for path, before in original.items()):
            raise InstallError('participant installation inputs changed')

    check_keys()
    report = {'schema': 'loom.nebius-participant-authority.v1', 'status': 'installed' if apply else 'prepared',
        'installation_id': operation['installation_id'], 'bundle_sha256': expected_sha256,
        'source_sha': source['source_sha'], 'configuration_sha256': configuration_sha256,
        'source_hashes': hashes, 'public_key_sha256': hashlib.sha256(key.encode()).hexdigest()}
    if not apply:
        return report
    for directory in (root.parent, root, root / 'tooling', host.state_root.parent, host.state_root,
            host.anchor_dir.parent, host.anchor_dir):
        _directory(directory, create=True)
        _sync(directory.parent)
    if gateway.prepare_release(content, root=root / 'tooling') != release:
        raise InstallError('participant tooling selection differs')
    gateway.qualify_host(release, configuration, configuration_sha256, manifest_sha256)
    unchanged()
    gateway.verify_release(release, manifest_sha256)
    # Dependencies are prepared before taking the shared key-file lock. Other
    # installers need not wait for pip, and their newly added keys are preserved.
    lock_path = ssh / 'loom-certificate-authority.lock'
    fd = os.open(lock_path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
    with os.fdopen(fd, 'rb+') as lock:
        _read(lock_path, 1)
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        previous = check_keys()
        unchanged()
        for directory in (root / 'authority', destination, destination / 'scripts', destination / 'scripts/ops'):
            _directory(directory, create=True)
            _sync(directory.parent)
        bootstrap.update({'entrypoint.py': _entrypoint(hashes, release=release, configuration=configuration,
            configuration_sha256=configuration_sha256, manifest_sha256=manifest_sha256),
            'receipt.json': json.dumps(report, sort_keys=True).encode()})
        for name, raw in bootstrap.items():
            path = destination / name
            if path.exists() or path.is_symlink():
                if _read(path, 262144) != raw:
                    raise InstallError('installed participant authority differs; preserve for reconciliation')
            else:
                fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
                with os.fdopen(fd, 'wb') as stream:
                    stream.write(raw)
                    stream.flush()
                    os.fsync(stream.fileno())
        _sync(destination)
        if line.rstrip(b'\n') not in previous.splitlines():
            updated = previous + (b'\n' if previous and not previous.endswith(b'\n') else b'') + line
            with tempfile.NamedTemporaryFile(dir=ssh, prefix='.participant-authority-', delete=False) as staging:
                temporary = Path(staging.name)
                staging.write(updated)
                staging.flush()
                os.fsync(staging.fileno())
            try:
                unchanged()
                gateway.verify_release(release, manifest_sha256)
                if check_keys() != previous:
                    raise InstallError('authorized keys changed during installation')
                os.replace(temporary, keys)
                _sync(ssh)
            finally:
                temporary.unlink(missing_ok=True)
            if _read(keys, 1024 * 1024) != updated:
                raise InstallError('authorized key readback differs; reconcile before retry')
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--bundle', type=Path, required=True)
    parser.add_argument('--bundle-sha256', required=True)
    parser.add_argument('--public-key', type=Path, required=True)
    parser.add_argument('--configuration', type=Path, required=True)
    parser.add_argument('--configuration-sha256', required=True)
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    try:
        result = install(_read(args.bundle, gateway.MAX_BUNDLE), expected_sha256=args.bundle_sha256,
            public_key=_read(args.public_key, 16384).decode(), configuration=args.configuration,
            configuration_sha256=args.configuration_sha256, apply=args.apply)
    except Exception:
        print(json.dumps({'status': 'blocked', 'reason': 'participant authority installation requires reconciliation'}))
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
