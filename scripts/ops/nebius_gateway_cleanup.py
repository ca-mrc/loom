#!/usr/bin/env python3
"""Operator-only housekeeping of regenerable management-gateway bytecode.

No imports from retained tooling and no provider/database access. Reports never
include paths, process arguments, operation inputs or file contents. Linux only:
the process guard also covers older gateways between their existing lock stages.
"""
from __future__ import annotations

import argparse
import fcntl
import json
import os
import re
import stat
import time
from collections.abc import Iterator
from contextlib import ExitStack
from pathlib import Path
from typing import Any
from uuid import UUID

SINGLE_SCOPES = ('upgrade', 'retirement', 'retirement-diagnostic', 'retirement-recovery')
MULTI_SCOPES = ('refresh', 'pool-cutover', 'pool-repair')
MAX_ENTRIES = 250_000
MAX_SCOPES = 256
MAX_SECONDS = 240
CACHE_NAME = re.compile(r'(.+)\.cpython-[0-9]+[a-z]*(?:\.opt-[0-9]+)?\.pyc\Z')


class UnsafeStateError(Exception):
    """A fixed, payload-free refusal to perform maintenance."""


class BusyError(Exception):
    """Existing work has priority over scheduled housekeeping."""


def _trusted(info: os.stat_result, *, directory: bool = False) -> bool:
    kind = stat.S_ISDIR if directory else stat.S_ISREG
    return (kind(info.st_mode) and info.st_uid == os.getuid() and not info.st_mode & 0o022
            and (directory or info.st_nlink == 1))


class Scan:
    def __init__(self) -> None:
        self.entries = 0
        self.deadline = time.monotonic() + MAX_SECONDS

    def tick(self, count: int = 1) -> None:
        self.entries += count
        if self.entries > MAX_ENTRIES or time.monotonic() > self.deadline:
            raise UnsafeStateError


def _directory(path: Path) -> None:
    if path != path.resolve() or not _trusted(path.lstat(), directory=True):
        raise UnsafeStateError


def _scopes(root: Path, scan: Scan) -> list[Path]:
    result = [root]
    for name in SINGLE_SCOPES:
        path = root / name
        if os.path.lexists(path):
            _directory(path)
            result.append(path)
    for name in MULTI_SCOPES:
        parent = root / name
        if not os.path.lexists(parent):
            continue
        _directory(parent)
        for path in sorted(parent.iterdir()):
            scan.tick()
            try:
                identity = UUID(path.name)
            except ValueError:
                raise UnsafeStateError from None
            if not identity.int or str(identity) != path.name:
                raise UnsafeStateError
            _directory(path)
            result.append(path)
    if len(result) > MAX_SCOPES:
        raise UnsafeStateError
    return result


def _lock(path: Path, stack: ExitStack) -> None:
    # Never replace or truncate a lock: flock must refer to the same inode used
    # by the gateway. Creating an absent empty coordination file is harmless.
    fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
    stack.callback(os.close, fd)
    info = os.fstat(fd)
    if not _trusted(info) or info.st_mode & 0o077 or info.st_size:
        raise UnsafeStateError
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        raise BusyError from None


def _walk(path: Path, scan: Scan, *, skip_unsafe: bool = False) -> Iterator[tuple[Path, list[str], int]]:
    """Descriptor-relative walk, never following links or crossing devices."""
    _directory(path)
    device = path.stat().st_dev

    def failed(_: OSError) -> None:
        raise UnsafeStateError

    for parent, directories, files, fd in os.fwalk(path, follow_symlinks=False, onerror=failed):
        scan.tick(len(directories) + len(files) + 1)
        info = os.fstat(fd)
        if info.st_dev != device or not _trusted(info, directory=True):
            raise UnsafeStateError
        # fwalk keeps descriptors anchored to the opened parent. Do not descend
        # into writable, foreign, symlinked or mounted directories.
        for name in list(directories):
            child = os.stat(name, dir_fd=fd, follow_symlinks=False)
            if child.st_dev != device or not _trusted(child, directory=True):
                if not skip_unsafe:
                    raise UnsafeStateError
                directories.remove(name)
        yield Path(parent), files, fd


def _operation_locks(scope: Path, stack: ExitStack, scan: Scan) -> None:
    for name in ('anchor', 'state'):
        path = scope / name
        if os.path.lexists(path):
            for parent, files, _ in _walk(path, scan):
                if 'operation.lock' in files:
                    _lock(parent / 'operation.lock', stack)


def _legacy_processes(root: Path, scan: Scan) -> None:
    # A gateway executing preflight or moving between preparation and dispatch
    # may hold no operation lock. Its pinned entrypoint/release path is in argv.
    # Arguments are examined in memory only and never emitted as diagnostics.
    proc = Path('/proc')
    if not (proc / 'self/cmdline').is_file():
        raise UnsafeStateError
    prefix = os.fsencode(root) + b'/'
    for process in proc.iterdir():
        if not process.name.isdecimal() or int(process.name) == os.getpid():
            continue
        scan.tick()
        try:
            if process.stat().st_uid != os.getuid():
                continue
            with (process / 'cmdline').open('rb') as stream:
                command = stream.read(1_048_577)
            if len(command) > 1_048_576:
                raise UnsafeStateError
            if any(prefix in arg or arg == os.fsencode(root) for arg in command.split(b'\0')):
                raise BusyError
        except (FileNotFoundError, ProcessLookupError):
            continue  # A process which has exited cannot be using the tooling.


def _disk(root: Path) -> dict[str, Any]:
    info = os.statvfs(root)
    used = info.f_blocks - info.f_bfree
    available = info.f_bavail
    percent = 100 * used / max(1, used + available)
    return {'total_bytes': info.f_blocks * info.f_frsize,
            'available_bytes': available * info.f_frsize,
            'used_percent': round(percent, 2),
            'pressure': 'critical' if percent >= 90 else 'warning' if percent >= 80 else 'normal'}


def _complete(release: Path) -> bool:
    try:
        fd = os.open(release / 'complete', os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except FileNotFoundError:
        return False
    try:
        if not _trusted(os.fstat(fd)):
            raise UnsafeStateError
        return os.read(fd, 9) == b'complete'
    finally:
        os.close(fd)


def _candidates(release: Path, cutoff: float, scan: Scan) -> list[tuple[Path, os.stat_result]]:
    result = []
    # Only these two source-backed trees can contain disposable Python caches.
    for name in ('venv', 'scripts'):
        tree = release / name
        if not os.path.lexists(tree):
            continue
        for parent, files, fd in _walk(tree, scan, skip_unsafe=True):
            if parent.name != '__pycache__':
                continue
            for name in files:
                match = CACHE_NAME.fullmatch(name)
                info = os.stat(name, dir_fd=fd, follow_symlinks=False)
                if match is None or not _trusted(info) or info.st_mtime > cutoff:
                    continue
                source = parent.parent / (match[1] + '.py')
                try:
                    source_info = source.lstat()
                except FileNotFoundError:
                    continue
                if _trusted(source_info) and source_info.st_mode & stat.S_IRUSR:
                    result.append((parent / name, info))
    return result


def _unlink(root: Path, path: Path, expected: os.stat_result) -> None:
    # Reopen every component relative to verified descriptors. A changed parent
    # or candidate between inventory and deletion fails closed, never follows a
    # replacement symlink, and never recursively removes a directory.
    with ExitStack() as opened:
        fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        opened.callback(os.close, fd)
        device = os.fstat(fd).st_dev
        parts = path.relative_to(root).parts
        for name in parts[:-1]:
            fd = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
            opened.callback(os.close, fd)
            info = os.fstat(fd)
            if info.st_dev != device or not _trusted(info, directory=True):
                raise UnsafeStateError
        current = os.stat(parts[-1], dir_fd=fd, follow_symlinks=False)
        if current != expected or not _trusted(current):
            raise UnsafeStateError
        # Requalify the source immediately before unlinking the cache.
        match = CACHE_NAME.fullmatch(path.name)
        if match is None:
            raise UnsafeStateError
        source = path.parent.parent / (match[1] + '.py')
        source_info = source.lstat()
        if (source != source.resolve() or not _trusted(source_info)
                or not source_info.st_mode & stat.S_IRUSR):
            raise UnsafeStateError
        os.unlink(parts[-1], dir_fd=fd)


def clean(root: Path, *, apply: bool = False, min_age_days: int = 7) -> dict[str, Any]:
    """Report or remove old source-backed caches; all failures retain evidence."""
    report: dict[str, Any] = {
        'schema': 'loom.nebius-gateway-cleanup.v1', 'status': 'blocked',
        'stage': 'root',
        'apply': apply, 'scopes': 0, 'complete_releases': 0, 'incomplete_releases': 0,
        'candidate_files': 0, 'candidate_bytes': 0, 'deleted_files': 0, 'deleted_bytes': 0,
    }
    try:
        if (type(min_age_days) is not int or not 1 <= min_age_days <= 3650
                or not root.is_absolute() or root.name != 'nebius-management'):
            raise UnsafeStateError
        _directory(root)
        report['disk_before'] = _disk(root)
        scan = Scan()
        scopes = _scopes(root, scan)
        report['scopes'] = len(scopes)
        with ExitStack() as locks:
            report['stage'] = 'locks'
            # Take every preparation lock before inspecting any operation state.
            # Cross-scope recovery may execute a repair release under old locks.
            for scope in scopes:
                _lock(scope / 'tooling.lock', locks)
            for scope in scopes:
                _operation_locks(scope, locks, scan)
            report['stage'] = 'process_guard'
            _legacy_processes(root, scan)
            report['stage'] = 'inventory'
            candidates = []
            for scope in scopes:
                releases = scope / 'releases'
                if not os.path.lexists(releases):
                    continue
                _directory(releases)
                for release in sorted(releases.iterdir()):
                    scan.tick()
                    if not re.fullmatch('[0-9a-f]{64}', release.name):
                        raise UnsafeStateError
                    _directory(release)
                    if not _complete(release):
                        report['incomplete_releases'] += 1
                        continue
                    report['complete_releases'] += 1
                    candidates.extend(_candidates(release, time.time() - min_age_days * 86400, scan))
            report['candidate_files'] = len(candidates)
            report['candidate_bytes'] = sum(info.st_size for _, info in candidates)
            if apply:
                report['stage'] = 'delete'
                for path, info in candidates:
                    scan.tick()
                    _unlink(root, path, info)
                    report['deleted_files'] += 1
                    report['deleted_bytes'] += info.st_size
            report['disk_after'] = _disk(root)
            report['status'] = 'cleaned' if apply else 'reported'
            report['stage'] = 'complete'
    except BusyError:
        report['status'] = 'skipped_busy'
    except (OSError, UnsafeStateError, ValueError, RuntimeError):
        report['status'] = 'blocked'
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path.home() / '.loom/nebius-management')
    parser.add_argument('--apply', action='store_true', help='delete only qualified old bytecode caches')
    parser.add_argument('--min-age-days', type=int, default=7)
    args = parser.parse_args()
    report = clean(args.root, apply=args.apply, min_age_days=args.min_age_days)
    print(json.dumps(report, sort_keys=True))
    return 1 if report['status'] == 'blocked' else 0


if __name__ == '__main__':
    raise SystemExit(main())
