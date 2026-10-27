"""Stdlib boundary for separately installed, fixed participant-release tooling.

The tooling archive is installer input, never a public release command. Existing
shared-handoff bundles and grants retain their original source/command scope.
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
from scripts.ops.nebius_shared_pool_delivery_gateway import SOURCES as SHARED_SOURCES

SOURCES = SHARED_SOURCES + tuple('scripts/ops/' + name + '.py' for name in (
    'nebius_idle_rollout', 'nebius_rollout_reporting',
    'nebius_pool_participant_adapter',
    'nebius_pool_participant_completion', 'nebius_pool_participant_creation',
    'nebius_pool_participant_dispatch', 'nebius_pool_participant_entry',
    'nebius_pool_participant_gateway', 'nebius_pool_participant_job',
    'nebius_pool_participant_observation', 'nebius_pool_participant_operation',
    'nebius_pool_participant_prepare', 'nebius_pool_participant_resource',
))
LIMITS = {**dict.fromkeys(SOURCES, 262144), 'uv': 80 * 1024**2,
    'requirements.txt': 262144, 'participant-tooling.json': 4096, 'manifest.json': 65536}
MAX_BUNDLE, MAX_WHEEL, MAX_COMMAND = 100 * 1024**2, 16 * 1024**2, 16384
COMMANDS = {'loom-nebius-participant-prepare-v1': 'prepare', 'loom-nebius-participant-execute-v1': 'execute',
    'loom-nebius-participant-recover-v1': 'recover'}
_ENTRY = ('import sys; sys.path.insert(0,sys.argv[1]); '
    'from scripts.ops.nebius_pool_participant_dispatch import main; '
    'code=main(*sys.argv[2:]); raise SystemExit(0 if code in (0,1) else 1)')
_SKIPS = frozenset({'skipped_busy', 'skipped_locked', 'skipped_superseded', 'blocked_recovery', 'skipped_already_deployed'})
_STAGES = frozenset({'request', 'installation', 'preparation', 'selection', 'execution'})


class GatewayError(RuntimeError):
    """Closed failure: no private paths, command output or request echo."""


def _object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError
        value[key] = item
    return value


def _json(raw: bytes) -> dict[str, Any]:
    value = json.loads(raw, object_pairs_hook=_object)
    if not isinstance(value, dict):
        raise ValueError
    return dict(value)


def _member_names(names: list[str]) -> set[str]:
    wheels = {name for name in names if name.startswith('wheels/')}
    if (len(names) != len(set(names)) or set(names) != set(LIMITS) | wheels or len(wheels) != 2
            or not all(sum(bool(re.fullmatch(r'wheels/' + package + r'-[0-9][0-9.]*-py3-none-any\.whl', name))
                for name in wheels) == 1 for package in ('loom', 'loom_bundle_checksum'))):
        raise ValueError
    return wheels


def unpack_bundle(content: bytes) -> tuple[dict[str, bytes], dict[str, Any]]:
    try:
        if not 0 < len(content) <= MAX_BUNDLE:
            raise ValueError
        with zipfile.ZipFile(io.BytesIO(content)) as archive:
            entries = archive.infolist()
            names = [entry.filename for entry in entries]
            _member_names(names)
            if any(entry.file_size > LIMITS.get(entry.filename, MAX_WHEEL) or entry.is_dir()
                    or stat.S_IFMT(entry.external_attr >> 16) not in {0, stat.S_IFREG} or entry.flag_bits & 1
                    for entry in entries):
                raise ValueError
            files = {entry.filename: archive.read(entry) for entry in entries}
        if _json(files['manifest.json']) != {name: hashlib.sha256(raw).hexdigest()
                for name, raw in files.items() if name != 'manifest.json'}:
            raise ValueError
        source = _json(files['participant-tooling.json'])
        if (set(source) != {'schema_version', 'source_sha', 'source_archive_sha256'}
                or source['schema_version'] != 'loom.nebius-participant-tooling.v1'
                or re.fullmatch(r'[0-9a-f]{40}', source['source_sha']) is None
                or re.fullmatch(r'sha256:[0-9a-f]{64}', source['source_archive_sha256']) is None):
            raise ValueError
        return files, source
    except Exception:
        raise GatewayError('invalid participant tooling bundle') from None


def verify_release(release: Path, manifest_sha256: str) -> None:
    """Read only: missing or changed installed files never trigger rebuilding."""
    try:
        if (not release.is_absolute() or release != release.resolve()
                or re.fullmatch(r'[0-9a-f]{64}', manifest_sha256) is None):
            raise ValueError
        info = release.lstat()
        if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
            raise ValueError
        if _read(release / 'complete', 64) != b'complete':
            raise ValueError
        raw = _read(release / 'manifest.json', LIMITS['manifest.json'])
        if hashlib.sha256(raw).hexdigest() != manifest_sha256:
            raise ValueError
        manifest = _json(raw)
        _member_names([*manifest, 'manifest.json'])
        for name, checksum in manifest.items():
            path = release / name
            if path != path.resolve() or hashlib.sha256(_read(path, LIMITS.get(name, MAX_WHEEL))).hexdigest() != checksum:
                raise ValueError
    except Exception:
        raise GatewayError('participant tooling changed; preserve installed state') from None


def prepare_release(content: bytes, *, root: Path) -> Path:
    """Installer-only setup of one exact reviewed bundle, never public dispatch."""
    files, _ = unpack_bundle(content)
    try:
        _directory(root)
        lock_path = root / 'tooling.lock'
        fd = os.open(lock_path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
        with os.fdopen(fd, 'rb+') as lock:
            _read(lock_path, 1)
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
            for directory in (release, release / 'scripts', release / 'scripts/ops',
                    release / 'wheels', release / 'deploy', release / 'deploy/k8s'):
                _directory(directory)
            for name, raw in files.items():
                _write(release / name, raw, executable=name == 'uv')
            uv, python = str(release / 'uv'), str(release / 'venv/bin/python')
            run_private([uv, 'venv', '--no-config', '--no-cache', '--no-python-downloads',
                '--python', '/usr/bin/python3', str(release / 'venv')], timeout=90)
            run_private([uv, 'pip', 'sync', '--no-config', '--no-cache', '--python', python,
                '--require-hashes', '--only-binary', ':all:', '--index-url', 'https://pypi.org/simple',
                str(release / 'requirements.txt')], timeout=600)
            run_private([uv, 'pip', 'install', '--no-config', '--no-cache', '--python', python,
                '--offline', '--no-deps', *sorted(str(release / name) for name in files if name.startswith('wheels/'))], timeout=90)
            qualifier = ('import sys,json;sys.path.insert(0,sys.argv[1]); '
                'from scripts.ops.nebius_pool_participant_dispatch import main; '
                'assert callable(main);print(json.dumps({"status":"tooling_qualified"}))')
            if _json(run_private([python, '-I', '-c', qualifier, str(release)], timeout=60)) != {'status': 'tooling_qualified'}:
                raise ValueError
            _write(release / 'complete', b'complete')
            verify_release(release, manifest_sha256)
            return release
    except Exception:
        raise GatewayError('participant tooling incomplete; preserve private state') from None


def command(release: Path, configuration: Path, checksum: str, action: str, content: bytes) -> list[str]:
    try:
        if (action not in COMMANDS.values() or not 0 < len(content) <= MAX_COMMAND
                or re.fullmatch(r'[0-9a-f]{64}', checksum) is None
                or any(not path.is_absolute() or path != path.resolve() or path == Path('/')
                    for path in (release, configuration))):
            raise ValueError
        return [str(release / 'venv/bin/python'), '-I', '-c', _ENTRY, str(release), str(configuration),
            checksum, action, content.decode()]
    except Exception:
        raise GatewayError('participant command outside fixed authority') from None


def qualify_host(release: Path, configuration: Path, checksum: str, manifest_sha256: str) -> None:
    """Installer-only import/configuration check, with no Kubernetes operations."""
    try:
        fixed = command(release, configuration, checksum, 'prepare', b'{}')
        qualifier = ('import sys,json;from pathlib import Path;sys.path.insert(0,sys.argv[1]); '
            'from scripts.ops.nebius_pool_participant_dispatch import load_participant_release_host; '
            'load_participant_release_host(Path(sys.argv[2]),sys.argv[3],repo_root=Path(sys.argv[1])); '
            'print(json.dumps({"status":"host_qualified"}))')
        verify_release(release, manifest_sha256)
        if _json(run_private([fixed[0], '-I', '-c', qualifier, str(release), str(configuration), checksum],
                timeout=60)) != {'status': 'host_qualified'}:
            raise ValueError
        verify_release(release, manifest_sha256)
    except Exception:
        raise GatewayError('participant host qualification unavailable') from None


def _uuid(value: Any) -> str:
    identity = UUID(value)
    if not identity.int or str(identity) != value:
        raise ValueError
    return str(identity)


def _reference(value: dict[str, Any]) -> None:
    if (set(value) != {'schema_version', 'operation_id', 'inputs_sha256', 'publication'}
            or value['schema_version'] != 'loom.nebius-participant-rollout-reference.v1'
            or re.fullmatch(r'[0-9a-f]{64}', value['inputs_sha256']) is None):
        raise ValueError
    _uuid(value['operation_id'])
    publication = value['publication']
    if (set(publication) != {'candidate_id', 'source_sha', 'run_id', 'run_attempt', 'artifact_id', 'artifact_sha256', 'pull_request'}
            or re.fullmatch(r'[0-9a-f]{40}', publication['source_sha']) is None
            or re.fullmatch(r'sha256:[0-9a-f]{64}', publication['artifact_sha256']) is None
            or any(type(publication[key]) is not int or publication[key] <= 0
                for key in ('run_id', 'run_attempt', 'artifact_id', 'pull_request'))):
        raise ValueError
    _uuid(publication['candidate_id'])


def safe_report(raw: bytes, content: bytes, action: str) -> dict[str, Any]:
    """Export only a closed, request-bound report from the pinned private entry."""
    try:
        if not 0 < len(raw) <= MAX_COMMAND or not 0 < len(content) <= MAX_COMMAND or action not in COMMANDS.values():
            raise ValueError
        request, value = _json(content), _json(raw)
        if request['schema_version'] != 'loom.nebius-participant-command.v1' or request['action'] != action:
            raise ValueError
        status = value['status']
        fields = {'schema_version', 'status'} | ({'stage'} if status == 'blocked' else set())
        if status != 'blocked' or 'release' in value:
            fields.add('release')
        allowed = {'prepare': {'prepared'}, 'execute': {'complete'} | _SKIPS, 'recover': {'complete'}}[action]
        if (set(value) != fields or value['schema_version'] != 'loom.nebius-participant-report.v1'
                or status not in allowed | {'blocked'} or (status == 'blocked' and value['stage'] not in _STAGES)):
            raise ValueError
        if 'release' not in value:
            return value
        binding = value['release']
        if set(binding) != {'reference', 'candidate_sha', 'previous_candidate_sha', 'guard_owner',
                'cluster_id', 'namespace', 'execution_namespace', 'environment_url'}:
            raise ValueError
        reference = binding['reference']
        _reference(reference)
        if action == 'prepare':
            if reference['operation_id'] != request['operation_id'] or reference['publication'] != request['publication']:
                raise ValueError
        elif reference != request['reference']:
            raise ValueError
        if (binding['candidate_sha'] != reference['publication']['source_sha']
                or re.fullmatch(r'[0-9a-f]{40}', binding['previous_candidate_sha']) is None
                or binding['guard_owner'] != 'rollout-' + UUID(reference['operation_id']).hex
                or any(re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,127}', binding[key]) is None
                    for key in ('cluster_id', 'namespace', 'execution_namespace'))
                or re.fullmatch(r'https://[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?', binding['environment_url']) is None):
            raise ValueError
        return value
    except Exception:
        raise GatewayError('invalid participant release report') from None


def authorized_main(release: Path, configuration: Path, checksum: str, manifest_sha256: str) -> int:
    """Fixed installed selections come from the grant; stdin is data, never code."""
    action = COMMANDS.get(os.environ.get('SSH_ORIGINAL_COMMAND', ''))
    if action is None:
        return 126
    content = sys.stdin.buffer.read(MAX_COMMAND + 1)
    if not 0 < len(content) <= MAX_COMMAND:
        return 126
    try:
        verify_release(release, manifest_sha256)
        result = safe_report(run_private(command(release, configuration, checksum, action, content), timeout=1800), content, action)
        verify_release(release, manifest_sha256)
        print(json.dumps(result, sort_keys=True))
        return 0  # Acknowledgement only; the bounded status decides readiness.
    except Exception:
        print('protected participant release unavailable; preserve original recovery reference', file=sys.stderr)
        return 1
