"""Stdlib forced-command boundary for independent v2 pool administration.

Bundles are operator-only installation inputs. Public commands select previously
authorized references; they cannot upload code or install/replace tooling. Legacy
participant and shared-handoff source lists and command grants remain unchanged.
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

SOURCES = tuple('scripts/ops/' + name + '.py' for name in (
    'nebius_certificate_gateway', 'nebius_certificates', 'nebius_ingress_gateway', 'nebius_ingress_stage',
    'nebius_management_authority_stage', 'nebius_management_bootstrap', 'nebius_management_evidence',
    'nebius_management_install', 'nebius_management_material', 'nebius_management_stage', 'nebius_management_storage',
    'nebius_management_supplied', 'nebius_management_transport', 'nebius_platform_namespace',
    'nebius_pool_administration_dispatch', 'nebius_pool_administration_gateway', 'nebius_pool_administration_job',
    'nebius_pool_registration',
))
LIMITS = {**dict.fromkeys(SOURCES, 262144), 'uv': 80 * 1024**2,
    'requirements.txt': 262144, 'administration-tooling.json': 4096, 'manifest.json': 65536}
MAX_BUNDLE, MAX_WHEEL, MAX_COMMAND = 100 * 1024**2, 16 * 1024**2, 16384
COMMANDS = {'loom-pool-administration-' + action + '-v2': action for action in ('prepare', 'apply', 'recover', 'status')}
_ENTRY = ('import sys;sys.path.insert(0,sys.argv[1]);'
    'from scripts.ops.nebius_pool_administration_dispatch import main;'
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
    if (len(names) != len(set(names)) or set(names) != set(LIMITS) | wheels or len(wheels) != 2
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
            if any(entry.file_size > LIMITS.get(entry.filename, MAX_WHEEL) or entry.is_dir()
                    or stat.S_IFMT(entry.external_attr >> 16) not in {0, stat.S_IFREG} or entry.flag_bits & 1 for entry in entries):
                raise ValueError
            files = {entry.filename: archive.read(entry) for entry in entries}
        if _json(files['manifest.json']) != {name: hashlib.sha256(raw).hexdigest() for name, raw in files.items() if name != 'manifest.json'}:
            raise ValueError
        source = _json(files['administration-tooling.json'])
        if (set(source) != {'schema_version', 'source_sha', 'source_archive_sha256'}
                or source['schema_version'] != 'loom.pool-administration-tooling.v2'
                or re.fullmatch(r'[0-9a-f]{40}', source['source_sha']) is None
                or re.fullmatch(r'sha256:[0-9a-f]{64}', source['source_archive_sha256']) is None):
            raise ValueError
        return files, source
    except Exception:
        raise ValueError('pool_administration_tooling_unqualified') from None


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
            if path != path.resolve() or hashlib.sha256(_read(path, LIMITS.get(name, MAX_WHEEL))).hexdigest() != checksum:
                raise ValueError
    except Exception:
        raise ValueError('pool_administration_tooling_changed_preserve_evidence') from None


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
            for directory in (release, release / 'scripts', release / 'scripts/ops', release / 'wheels'):
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
                'from scripts.ops.nebius_pool_administration_dispatch import main;assert callable(main);'
                'print(json.dumps({"status":"tooling_qualified"}))')
            if _json(run_private([python, '-I', '-c', qualifier, str(release)], timeout=60)) != {'status': 'tooling_qualified'}:
                raise ValueError
            _write(release / 'complete', b'complete')
            verify_release(release, manifest_sha256)
            return release
    except Exception:
        raise ValueError('pool_administration_tooling_incomplete_preserve_evidence') from None


def _uuid(value: Any) -> None:
    if not isinstance(value, str) or str(UUID(value)) != value or not UUID(value).int:
        raise ValueError


def _command(content: bytes, action: str) -> dict[str, Any]:
    if type(content) is not bytes or not 0 < len(content) <= MAX_COMMAND or action not in COMMANDS.values():
        raise ValueError
    value = _json(content)
    if (set(value) != {'schema_version', 'action', 'reference'} | ({'observation_id'} if action == 'status' else set())
            or value['schema_version'] != 'loom.pool-administration-command.v2' or value['action'] != action):
        raise ValueError
    reference = value['reference']
    identities = {'installation_id', 'pool_id', 'operation_id', 'authorization_id'}
    scoped = {'participant_id', 'environment_id', 'incarnation'}
    hashes = {'request_sha256', 'document_sha256'}
    if (not isinstance(reference, dict) or set(reference) != identities | scoped | hashes | {'authority', 'schema_version'}
            or reference['schema_version'] != 'loom.pool-administration-reference.v2'
            or reference['authority'] not in {'platform', 'development', 'staging', 'production'}):
        raise ValueError
    for key in identities | (scoped if reference['authority'] != 'platform' else set()):
        _uuid(reference[key])
    if (reference['authority'] == 'platform' and any(reference[key] is not None for key in scoped)) or any(
            not isinstance(reference[key], str) or re.fullmatch(r'[0-9a-f]{64}', reference[key]) is None for key in hashes):
        raise ValueError
    if action == 'status':
        _uuid(value['observation_id'])
    return value


def command(release: Path, configuration: Path, checksum: str, action: str, content: bytes) -> list[str]:
    _command(content, action)
    if (re.fullmatch(r'[0-9a-f]{64}', checksum) is None
            or any(not path.is_absolute() or path != path.resolve() or path == Path('/') for path in (release, configuration))):
        raise ValueError('pool_administration_command_unqualified')
    return [str(release / 'venv/bin/python'), '-I', '-c', _ENTRY, str(release), str(configuration), checksum, action, content.decode()]


def safe_report(raw: bytes, content: bytes, action: str) -> dict[str, Any]:
    incoming = _command(content, action)
    if type(raw) is not bytes or not 0 < len(raw) <= 65536:
        raise ValueError
    report = _json(raw)
    if report.get('schema_version') != 'loom.pool-administration-dispatch.v2':
        raise ValueError
    if report.get('status') == 'blocked':
        if set(report) != {'schema_version', 'status', 'stage'} or report['stage'] not in {'request', 'selection', 'execution', 'installation'}:
            raise ValueError
        return report
    status = report.get('status')
    allowed = {'prepare': {'prepared'}, 'apply': {'pending', 'complete'}, 'recover': {'pending', 'complete'}, 'status': {'pending', 'observed'}}
    proof = status in {'complete', 'observed'}
    if (status not in allowed[action] or set(report) != {'schema_version', 'action', 'reference', 'observation_id', 'status'} | ({'proof'} if proof else set())
            or report['action'] != action or report['reference'] != incoming['reference']
            or report['observation_id'] != incoming.get('observation_id')):
        raise ValueError
    result = {key: value for key, value in report.items() if key != 'proof'}
    if proof:
        item = report['proof']
        if not isinstance(item, dict) or set(item) != {'job_uid', 'pod_uid', 'receipt'}:
            raise ValueError
        for key in ('job_uid', 'pod_uid'):
            _uuid(item[key])
            result[key] = item[key]
        receipt = item['receipt']
        if (not isinstance(receipt, dict) or set(receipt) != {'schema_version', 'authorization_id', 'request_sha256', 'receipt', 'current'}
                or receipt['schema_version'] != 'loom.pool-administration-result.v2'
                or receipt['authorization_id'] != incoming['reference']['authorization_id']
                or receipt['request_sha256'] != incoming['reference']['request_sha256']):
            raise ValueError
        result['receipt_sha256'] = hashlib.sha256(json.dumps(receipt, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()).hexdigest()
    return result


def authorized_main(release: Path, configuration: Path, checksum: str, manifest_sha256: str) -> int:
    action = COMMANDS.get(os.environ.get('SSH_ORIGINAL_COMMAND', ''))
    if action is None:
        return 126
    try:
        content = sys.stdin.buffer.read(MAX_COMMAND + 1)
        _command(content, action)
        verify_release(release, manifest_sha256)
        result = safe_report(run_private(command(release, configuration, checksum, action, content), timeout=180), content, action)
        verify_release(release, manifest_sha256)
        print(json.dumps(result, sort_keys=True, allow_nan=False))
        return 0
    except Exception:
        print('protected pool administration unavailable; preserve original reference', file=sys.stderr)
        return 1
