"""Stdlib boundary for separately installed, fixed participant-release tooling.

The tooling archive is installer input, never a public release command. Existing
shared-handoff bundles and grants retain their original source/command scope.
"""
from __future__ import annotations

import hashlib
import io
import json
import re
import stat
import zipfile
from pathlib import Path
from typing import Any
from uuid import UUID

from scripts.ops.nebius_shared_pool_delivery_gateway import SOURCES as SHARED_SOURCES

SOURCES = SHARED_SOURCES + tuple('scripts/ops/' + name + '.py' for name in (
    'nebius_idle_rollout', 'nebius_rollout_reporting',
    'nebius_pool_participant_adapter', 'nebius_pool_participant_catalog',
    'nebius_pool_participant_completion', 'nebius_pool_participant_creation',
    'nebius_pool_participant_dispatch', 'nebius_pool_participant_entry',
    'nebius_pool_participant_gateway', 'nebius_pool_participant_job',
    'nebius_pool_participant_observation', 'nebius_pool_participant_operation',
    'nebius_pool_participant_prepare', 'nebius_pool_participant_release', 'nebius_pool_participant_resource',
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


def unpack_bundle(content: bytes) -> tuple[dict[str, bytes], dict[str, Any]]:
    try:
        if not 0 < len(content) <= MAX_BUNDLE:
            raise ValueError
        with zipfile.ZipFile(io.BytesIO(content)) as archive:
            entries = archive.infolist()
            names = [entry.filename for entry in entries]
            wheels = {name for name in names if name.startswith('wheels/')}
            if (len(names) != len(set(names)) or set(names) != set(LIMITS) | wheels or len(wheels) != 2
                    or not all(sum(bool(re.fullmatch(r'wheels/' + package + r'-[0-9][0-9.]*-py3-none-any\.whl', name))
                        for name in wheels) == 1 for package in ('loom', 'loom_bundle_checksum'))):
                raise ValueError
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
