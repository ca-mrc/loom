#!/usr/bin/env python3
"""Add the fixed, read-only completion command to the existing rollout gateway.

Run from the reviewed merged source on the operator host. Existing kubectl and
inspection grants remain byte-for-byte intact; no key or pool state is changed.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import stat
import sys
import tempfile
from pathlib import Path

BEGIN = '# BEGIN loom legacy pool completion reader\n'
END = '# END loom legacy pool completion reader\n'


def private_path(path: Path, *, directory: bool = False) -> None:
    info = path.lstat()
    if (path.resolve() != path or info.st_uid != os.geteuid()
            or info.st_mode & 0o077
            or not (stat.S_ISDIR(info.st_mode) if directory else stat.S_ISREG(info.st_mode))):
        raise ValueError('gateway path must be private and owned')


def install(root: Path, source: bytes, revision: str) -> dict[str, str]:
    if not re.fullmatch(r'[0-9a-f]{40}', revision) or not source or len(source) > 65536:
        raise ValueError('reviewed source revision required')
    private_path(root, directory=True)
    wrapper = root / 'github-kubectl.py'
    private_path(wrapper)
    before = wrapper.read_bytes()
    if len(before) > 65536:
        raise ValueError('gateway wrapper too large')
    previous = before
    if before.startswith(BEGIN.encode()):
        if before.count(END.encode()) != 1:
            raise ValueError('completion reader prefix malformed')
        previous = before.split(END.encode(), 1)[1]
    elif BEGIN.encode() in before or END.encode() in before:
        raise ValueError('completion reader prefix misplaced')
    for directory in (root / 'legacy-pool-completion', root / 'legacy-pool-completion' / revision):
        directory.mkdir(mode=0o700, exist_ok=True)
        private_path(directory, directory=True)
    probe = root / 'legacy-pool-completion' / revision / 'probe.py'
    if probe.exists() or probe.is_symlink():
        private_path(probe)
        if probe.read_bytes() != source:
            raise ValueError('installed reader source differs')
    else:
        with probe.open('xb') as stream:
            os.fchmod(stream.fileno(), 0o600)
            stream.write(source)
    prefix = f'''{BEGIN}import os, shlex, sys
try:
    _completion_args = shlex.split(os.environ.get('SSH_ORIGINAL_COMMAND', ''))
except ValueError:
    sys.exit(126)
if _completion_args and _completion_args[0] == 'loom-nebius-legacy-pool-completion-v1':
    if len(_completion_args) != 2:
        sys.exit(126)
    os.execv({sys.executable!r}, ['python3', '-I', {str(probe)!r}, _completion_args[1]])
{END}'''.encode()
    desired = prefix + previous
    compile(desired, str(wrapper), 'exec')
    if before == desired:
        return {'status': 'unchanged', 'revision': revision}
    backup = probe.parent / 'previous-github-kubectl.py'
    if backup.exists() or backup.is_symlink():
        private_path(backup)
        if backup.read_bytes() != before:
            raise ValueError('retained gateway backup differs')
    else:
        with backup.open('xb') as stream:
            os.fchmod(stream.fileno(), 0o600)
            stream.write(before)
    fd, name = tempfile.mkstemp(prefix='.completion-reader-', dir=root)
    temporary = Path(name)
    try:
        with os.fdopen(fd, 'wb') as stream:
            stream.write(desired)
            stream.flush()
            os.fsync(stream.fileno())
        if wrapper.read_bytes() != before:
            raise ValueError('gateway changed during installation')
        os.replace(temporary, wrapper)
    finally:
        temporary.unlink(missing_ok=True)
    return {'status': 'installed', 'revision': revision}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-sha', required=True)
    args = parser.parse_args()
    try:
        report = install(Path.home() / '.loom/nebius-auto-rollout',
                         Path(__file__).with_name('nebius_legacy_pool_completion.py').read_bytes(), args.source_sha)
        print(json.dumps(report))
        return 0
    except Exception:
        print(json.dumps({'status': 'blocked'}))
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
