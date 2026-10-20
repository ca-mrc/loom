"""Installed fixed commands for independent environment lifecycle operations.

Operator-approved archives install immutable tooling separately. Public stdin
selects only an exact scope and previously allowlisted operation, never code.
"""
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
from uuid import UUID

from scripts.ops.nebius_certificate_gateway import _directory, _read, _write, run_private

CORE_SOURCES = frozenset('scripts/ops/' + name + '.py' for name in (
    'nebius_certificate_gateway', 'nebius_environment_lifecycle_gateway',
    'nebius_environment_lifecycle_entry', 'nebius_environment_lifecycle_inputs',
))
RENDER_SOURCES = ('deploy/k8s/nebius-execution-actuator.yaml', 'deploy/k8s/nebius-capacity-collector.yaml')
LIMITS = {**dict.fromkeys(RENDER_SOURCES, 262144), 'uv': 80 * 1024**2, 'requirements.txt': 262144,
    'lifecycle-tooling.json': 4096, 'manifest.json': 65536}
MAX_BUNDLE, MAX_WHEEL, MAX_COMMAND, MAX_SOURCE = 100 * 1024**2, 16 * 1024**2, 16384, 262144
COMMANDS = {'loom-environment-lifecycle-' + action + '-v2': action for action in ('prepare', 'apply', 'recover', 'status')}
PHASES = frozenset({'request', 'selection', 'installation', 'execution', 'provision', 'enroll', 'start',
    'activate', 'suspend', 'resume', 'retire', 'release', 'rollback', 'import', 'status', 'recovery', 'complete'})


def member_limit(name: str) -> int:
    return LIMITS.get(name, MAX_WHEEL if name.startswith('wheels/') else MAX_SOURCE)


_ENTRY = ('import sys;sys.path.insert(0,sys.argv[1]);'
    'from scripts.ops.nebius_environment_lifecycle_entry import main;'
    'code=main(*sys.argv[2:]);raise SystemExit(0 if code in (0,1) else 1)')


def _object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError
        result[key] = value
    return result


def _json(raw: bytes) -> dict[str, Any]:
    value = json.loads(raw, object_pairs_hook=_object)
    if not isinstance(value, dict):
        raise ValueError
    return value


def _members(names: list[str]) -> None:
    wheels = {name for name in names if name.startswith('wheels/')}
    scripts = {name for name in names if re.fullmatch(r'scripts/ops/[a-z][a-z0-9_]+\.py', name)}
    if (len(names) != len(set(names)) or len(names) > 256 or not CORE_SOURCES <= scripts
            or set(names) != set(LIMITS) | scripts | wheels or len(wheels) != 2
            or any(sum(bool(re.fullmatch(r'wheels/' + package + r'-[0-9][0-9.]*-py3-none-any\.whl', name))
                for name in wheels) != 1 for package in ('loom', 'loom_bundle_checksum'))):
        raise ValueError


def unpack_bundle(content: bytes) -> tuple[dict[str, bytes], dict[str, Any]]:
    try:
        if type(content) is not bytes or not 0 < len(content) <= MAX_BUNDLE:
            raise ValueError
        with zipfile.ZipFile(io.BytesIO(content)) as archive:
            entries = archive.infolist()
            _members([entry.filename for entry in entries])
            if any(entry.file_size > member_limit(entry.filename) or entry.is_dir()
                    or stat.S_IFMT(entry.external_attr >> 16) not in {0, stat.S_IFREG} or entry.flag_bits & 1 for entry in entries):
                raise ValueError
            files = {entry.filename: archive.read(entry) for entry in entries}
        if _json(files['manifest.json']) != {name: hashlib.sha256(raw).hexdigest() for name, raw in files.items() if name != 'manifest.json'}:
            raise ValueError
        source = _json(files['lifecycle-tooling.json'])
        if (set(source) != {'schema_version', 'source_sha', 'source_archive_sha256'}
                or source['schema_version'] != 'loom.environment-lifecycle-tooling.v2'
                or re.fullmatch(r'[0-9a-f]{40}', source['source_sha']) is None
                or re.fullmatch(r'sha256:[0-9a-f]{64}', source['source_archive_sha256']) is None):
            raise ValueError
        return files, source
    except Exception:
        raise ValueError('environment_lifecycle_tooling_unqualified') from None


def verify_release(release: Path, manifest_sha256: str) -> None:
    try:
        if (not release.is_absolute() or release != release.resolve() or release == Path('/')
                or re.fullmatch(r'[0-9a-f]{64}', manifest_sha256) is None):
            raise ValueError
        info = release.lstat()
        if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
            raise ValueError
        raw = _read(release / 'manifest.json', LIMITS['manifest.json'])
        if hashlib.sha256(raw).hexdigest() != manifest_sha256 or _read(release / 'complete', 64) != b'complete':
            raise ValueError
        manifest = _json(raw)
        _members([*manifest, 'manifest.json'])
        for name, checksum in manifest.items():
            path = release / name
            if path != path.resolve() or hashlib.sha256(_read(path, member_limit(name))).hexdigest() != checksum:
                raise ValueError
    except Exception:
        raise ValueError('environment_lifecycle_tooling_changed_preserve_evidence') from None


def prepare_release(content: bytes, *, root: Path) -> Path:
    files, _ = unpack_bundle(content)
    try:
        _directory(root)
        fd = os.open(root / 'tooling.lock', os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
        with os.fdopen(fd, 'rb+') as lock:
            _read(root / 'tooling.lock', 1)
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            releases = root / 'releases'
            _directory(releases)
            release = releases / hashlib.sha256(content).hexdigest()
            manifest_sha256 = hashlib.sha256(files['manifest.json']).hexdigest()
            if release.exists() or release.is_symlink():
                verify_release(release, manifest_sha256)
                return release
            if sum(1 for _ in releases.iterdir()) >= 8:
                raise ValueError
            for directory in (release, release / 'scripts', release / 'scripts/ops', release / 'wheels',
                    release / 'deploy', release / 'deploy/k8s'):
                _directory(directory)
            for name, raw in files.items():
                _write(release / name, raw, executable=name == 'uv')
            uv, python = str(release / 'uv'), str(release / 'venv/bin/python')
            run_private([uv, 'venv', '--no-config', '--no-cache', '--no-python-downloads',
                '--python', '/usr/bin/python3', str(release / 'venv')], timeout=90)
            run_private([uv, 'pip', 'sync', '--no-config', '--no-cache', '--python', python,
                '--require-hashes', '--only-binary', ':all:', '--index-url', 'https://pypi.org/simple', str(release / 'requirements.txt')], timeout=600)
            run_private([uv, 'pip', 'install', '--no-config', '--no-cache', '--python', python, '--offline', '--no-deps',
                *sorted(str(release / name) for name in files if name.startswith('wheels/'))], timeout=90)
            qualifier = ('import sys,json;sys.path.insert(0,sys.argv[1]);'
                'from scripts.ops.nebius_environment_lifecycle_entry import main;'
                'from scripts.ops.nebius_environment_lifecycle import execute_lifecycle;'
                'from scripts.ops import nebius_environment_lifecycle_inputs;'
                'assert callable(main) and callable(execute_lifecycle);'
                'print(json.dumps({"status":"tooling_qualified"}))')
            if _json(run_private([python, '-I', '-c', qualifier, str(release)], timeout=60)) != {'status': 'tooling_qualified'}:
                raise ValueError
            _write(release / 'complete', b'complete')
            verify_release(release, manifest_sha256)
            return release
    except Exception:
        raise ValueError('environment_lifecycle_tooling_incomplete_preserve_evidence') from None


def _uuid(value: Any) -> None:
    if not isinstance(value, str) or str(UUID(value)) != value or not UUID(value).int:
        raise ValueError


def validate_scope(value: Any) -> None:
    if (not isinstance(value, dict) or set(value) != {'installation_id', 'pool_id', 'participant_id',
            'environment_id', 'incarnation', 'authority'} or value['authority'] not in {'development', 'staging', 'production'}):
        raise ValueError('environment_lifecycle_scope_unqualified')
    for name in set(value) - {'authority'}:
        _uuid(value[name])


def parse_command(content: bytes, action: str) -> dict[str, Any]:
    if type(content) is not bytes or not 0 < len(content) <= MAX_COMMAND or action not in COMMANDS.values():
        raise ValueError
    value = _json(content)
    if (set(value) != {'schema_version', 'action', 'scope', 'operation_id'}
            or value['schema_version'] != 'loom.environment-lifecycle-command.v2' or value['action'] != action):
        raise ValueError
    validate_scope(value['scope'])
    _uuid(value['operation_id'])
    return value


def command(release: Path, configuration: Path, checksum: str, action: str, content: bytes) -> list[str]:
    parse_command(content, action)
    if (re.fullmatch(r'[0-9a-f]{64}', checksum) is None
            or any(not path.is_absolute() or path != path.resolve() or path == Path('/') for path in (release, configuration))):
        raise ValueError('environment_lifecycle_command_unqualified')
    return [str(release / 'venv/bin/python'), '-I', '-c', _ENTRY, str(release), str(configuration), checksum, action, content.decode()]


def safe_report(raw: bytes, content: bytes, action: str) -> dict[str, Any]:
    incoming = parse_command(content, action)
    if type(raw) is not bytes or not 0 < len(raw) <= 16384:
        raise ValueError
    report = _json(raw)
    required = {'schema_version', 'action', 'scope', 'operation_id', 'status', 'phase'}
    if (not required <= set(report) or set(report) - required - {'admission_open', 'execution_verified'}
            or report['schema_version'] != 'loom.environment-lifecycle-result.v2'
            or any(report[key] != incoming[key] for key in ('action', 'scope', 'operation_id'))
            or report['phase'] not in PHASES
            or report['status'] not in ({'prepared', 'blocked'} if action == 'prepare' else
                {'pending', 'observed', 'blocked'} if action == 'status' else {'pending', 'complete', 'blocked'})
            or ('admission_open' in report and type(report['admission_open']) is not bool)
            or ('execution_verified' in report and report['execution_verified'] is not False)):
        raise ValueError('environment_lifecycle_report_unqualified')
    return report


def authorized_main(release: Path, configuration: Path, checksum: str, manifest_sha256: str) -> int:
    action = COMMANDS.get(os.environ.get('SSH_ORIGINAL_COMMAND', ''))
    if action is None:
        return 126
    try:
        content = sys.stdin.buffer.read(MAX_COMMAND + 1)
        parse_command(content, action)
        verify_release(release, manifest_sha256)
        result = safe_report(run_private(command(release, configuration, checksum, action, content), timeout=2400), content, action)
        verify_release(release, manifest_sha256)
        print(json.dumps(result, sort_keys=True, allow_nan=False))
        return 0
    except Exception:
        print('protected environment lifecycle unavailable; preserve original reference', file=sys.stderr)
        return 1
