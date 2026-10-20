"""Operator-only installation of an exact v2 lifecycle tooling/scope pair.

No Kubernetes, cloud or database operation occurs here. The public key gains only
four fixed data commands; operation plans remain private and separately scoped.
Existing staging, handoff and other environment keys are preserved byte-for-byte.
"""
from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import pwd
import re
import tempfile
from pathlib import Path
from typing import Any

from scripts.ops import nebius_environment_lifecycle_gateway as gateway
from scripts.ops.install_nebius_management_entrypoint import _directory, _key, _read, _sync
from scripts.ops.nebius_environment_lifecycle_entry import load_lifecycle_configuration

_BOOTSTRAP = ('scripts/ops/nebius_environment_lifecycle_gateway.py', 'scripts/ops/nebius_certificate_gateway.py')


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
    from scripts.ops.nebius_environment_lifecycle_gateway import authorized_main
    code = authorized_main(Path({str(release)!r}), Path({str(configuration)!r}), {configuration_sha256!r}, {manifest_sha256!r})
except Exception:
    code = 126
raise SystemExit(code)
'''.encode()


def install(content: bytes, *, expected_sha256: str, public_key: str, configuration: Path,
        configuration_sha256: str, home: Path, apply: bool = False) -> dict[str, Any]:
    try:
        if (type(apply) is not bool or type(content) is not bytes or not 0 < len(content) <= gateway.MAX_BUNDLE
                or re.fullmatch(r'[0-9a-f]{64}', expected_sha256) is None or hashlib.sha256(content).hexdigest() != expected_sha256):
            raise ValueError
        key = _key(public_key)
        files, source = gateway.unpack_bundle(content)
        host = load_lifecycle_configuration(configuration, configuration_sha256)
        owner, ssh = home / '.loom', home / '.ssh'
        for directory in (home, owner, ssh):
            _directory(directory)
        paths = (configuration,)
        if not configuration.is_relative_to(owner) or configuration == owner:
            raise ValueError
        originals = {path: _read(path, 1024**2) for path in paths}
        scope_sha256 = hashlib.sha256(host.scope.model_dump_json().encode()).hexdigest()
        root = owner / 'environment-lifecycle-delivery' / scope_sha256
        if re.fullmatch(r'/[A-Za-z0-9_./-]+', str(root)) is None:
            raise ValueError
        identity = hashlib.sha256((expected_sha256 + ':' + configuration_sha256).encode()).hexdigest()
        destination, release = root / 'authority' / identity, root / 'tooling/releases' / expected_sha256
        line = (f'restrict,command="/usr/bin/python3 -I {destination}/entrypoint.py" '
            f'{key} loom-environment-lifecycle-{scope_sha256}\n').encode()
        keys = ssh / 'authorized_keys'
        bootstrap = {name: files[name] for name in _BOOTSTRAP}
        bootstrap.update({'scripts/__init__.py': b'', 'scripts/ops/__init__.py': b''})
        hashes = {name: hashlib.sha256(raw).hexdigest() for name, raw in bootstrap.items()}
        manifest_sha256 = hashlib.sha256(files['manifest.json']).hexdigest()
    except Exception:
        raise ValueError('environment_lifecycle_installation_unqualified') from None

    def unchanged() -> None:
        if load_lifecycle_configuration(configuration, configuration_sha256) != host or any(
                _read(path, 1024**2) != raw for path, raw in originals.items()):
            raise ValueError('environment_lifecycle_installation_changed')

    def read_keys() -> bytes:
        previous = _read(keys, 1024**2)
        same = re.compile(rb'(?:^|[ \t])ssh-ed25519[ \t]+' + re.escape(key.split()[1].encode()) + rb'(?:[ \t]|$)')
        if any(same.search(row) and row != line.rstrip(b'\n') for row in previous.splitlines()):
            raise ValueError('environment_lifecycle_key_already_scoped')
        return previous

    read_keys()
    report = dict(schema_version='loom.environment-lifecycle-authority.v2', status='installed' if apply else 'prepared',
        scope=host.scope.model_dump(mode='json'), bundle_sha256=expected_sha256, source_sha=source['source_sha'],
        configuration_sha256=configuration_sha256, public_key_sha256=hashlib.sha256(key.encode()).hexdigest())
    if not apply:
        return report

    def mkdir(path: Path) -> None:
        selected = owner
        for part in path.relative_to(owner).parts:
            selected = selected / part
            _directory(selected, create=True)
            _sync(selected.parent)

    for path in (root / 'tooling',):
        mkdir(path)
    if gateway.prepare_release(content, root=root / 'tooling') != release:
        raise ValueError('environment_lifecycle_tooling_selection_changed')
    unchanged()
    gateway.verify_release(release, manifest_sha256)
    # Shared lock keeps distinct authorized-key installers from losing additions.
    lock_path = ssh / 'loom-certificate-authority.lock'
    fd = os.open(lock_path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
    with os.fdopen(fd, 'rb+') as lock:
        _read(lock_path, 1)
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        previous = read_keys()
        unchanged()
        mkdir(destination / 'scripts/ops')
        bootstrap.update({'entrypoint.py': _entrypoint(hashes, release=release, configuration=configuration,
            configuration_sha256=configuration_sha256, manifest_sha256=manifest_sha256),
            'receipt.json': json.dumps(report, sort_keys=True).encode()})
        for name, raw in bootstrap.items():
            path = destination / name
            if path.exists() or path.is_symlink():
                if _read(path, 262144) != raw:
                    raise ValueError('environment_lifecycle_bootstrap_changed_preserve_evidence')
            else:
                fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
                with os.fdopen(fd, 'wb') as stream:
                    stream.write(raw)
                    stream.flush()
                    os.fsync(stream.fileno())
        _sync(destination)
        if line.rstrip(b'\n') not in previous.splitlines():
            updated = previous + (b'\n' if previous and not previous.endswith(b'\n') else b'') + line
            with tempfile.NamedTemporaryFile(dir=ssh, prefix='.environment-lifecycle-', delete=False) as staging:
                temporary = Path(staging.name)
                staging.write(updated)
                staging.flush()
                os.fsync(staging.fileno())
            try:
                unchanged()
                gateway.verify_release(release, manifest_sha256)
                if read_keys() != previous:
                    raise ValueError('environment_lifecycle_authorized_keys_changed')
                os.replace(temporary, keys)
                _sync(ssh)
            finally:
                temporary.unlink(missing_ok=True)
            if _read(keys, 1024**2) != updated:
                raise ValueError('environment_lifecycle_key_readback_failed')
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
            configuration_sha256=args.configuration_sha256, home=Path(pwd.getpwuid(os.getuid()).pw_dir), apply=args.apply)
    except Exception:
        print(json.dumps({'status': 'blocked', 'reason': 'environment lifecycle authority requires reconciliation'}))
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
