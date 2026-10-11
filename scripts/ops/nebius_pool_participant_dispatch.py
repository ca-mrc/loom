"""Fixed data-only release dispatch for separately installed host tooling.

This is not an SSH grant. The installed caller supplies the host configuration
and fixed action. Runner input cannot select files, credentials or executable
source. Only bounded public selectors and reports cross that boundary.
"""
from __future__ import annotations

import copy
import hashlib
import json
import os
import re
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import UUID

from scripts.ops import nebius_certificates as private_state
from scripts.ops import nebius_pool_participant_entry as entry
from scripts.ops.nebius_idle_rollout import recovery_record
from scripts.ops.nebius_pool_participant_operation import (
    ParticipantRolloutReference,
    RetainedParticipantRollout,
    load_participant_rollout,
)

from loom.nebius_kubernetes import NebiusKubernetesConnection
from loom_service.environment_management.candidates import ProtectedPublication, _json

COMMAND_SCHEMA = 'loom.nebius-participant-command.v1'
REPORT_SCHEMA = 'loom.nebius-participant-report.v1'
MAX_COMMAND = 16384
_SKIPS = frozenset({'skipped_busy', 'skipped_locked', 'skipped_superseded', 'blocked_recovery',
    'skipped_already_deployed'})


@dataclass(frozen=True, repr=False)
class ParticipantReleaseHost:
    """Trusted installed selection; never deserialized from a public command."""

    shared_operation: Path
    shared_operation_sha256: str
    state_root: Path
    anchor_dir: Path
    operator_connection: NebiusKubernetesConnection
    executable: Path
    repo_root: Path

    def connection(self) -> dict[str, Any]:
        return {name: getattr(self, name) for name in (
            'shared_operation', 'shared_operation_sha256', 'state_root', 'anchor_dir', 'operator_connection', 'executable')}


def load_participant_release_host(path: Path, checksum: str, *, repo_root: Path) -> ParticipantReleaseHost:
    """Load the installer's pinned private config, without accessing the cluster."""
    try:
        if (not path.is_absolute() or path != path.resolve() or re.fullmatch(r'[0-9a-f]{64}', checksum) is None
                or not repo_root.is_absolute() or repo_root != repo_root.resolve() or repo_root == Path('/')):
            raise ValueError
        raw = private_state._private_read(path, limit=MAX_COMMAND)
        if hashlib.sha256(raw).hexdigest() != checksum:
            raise ValueError
        value = _json(raw)
        fields = {'shared_operation', 'shared_operation_sha256', 'state_root', 'anchor_dir', 'operator_connection', 'executable'}
        if set(value) != fields | {'schema_version'} or value['schema_version'] != 'loom.nebius-participant-host.v1':
            raise ValueError
        if re.fullmatch(r'[0-9a-f]{64}', value['shared_operation_sha256']) is None:
            raise ValueError
        for name in ('shared_operation', 'state_root', 'anchor_dir', 'executable'):
            selected = Path(value[name])
            if (not selected.is_absolute() or selected == Path('/') or selected != selected.resolve()
                    or str(selected) != value[name]):
                raise ValueError
            value[name] = selected
        state, anchor = value['state_root'], value['anchor_dir']
        if state == anchor or state in anchor.parents or anchor in state.parents:
            raise ValueError
        connection = NebiusKubernetesConnection.model_validate(value['operator_connection'])
        value['operator_connection'] = connection
        return ParticipantReleaseHost(**{key: value[key] for key in fields}, repo_root=repo_root)
    except Exception:
        raise ValueError('participant host configuration unqualified; preserve evidence') from None


def _request(content: bytes, action: str) -> dict[str, Any]:
    if type(content) is not bytes or not 0 < len(content) <= MAX_COMMAND or action not in {'prepare', 'execute', 'recover'}:
        raise ValueError
    value = _json(content)
    fields = {'schema_version', 'action'} | ({'operation_id', 'publication'} if action == 'prepare'
        else {'reference', 'automatic' if action == 'execute' else 'recovery'})
    if set(value) != fields or value['schema_version'] != COMMAND_SCHEMA or value['action'] != action:
        raise ValueError
    if action == 'prepare':
        identity = UUID(value['operation_id'])
        publication = ProtectedPublication.model_validate(value['publication'])
        if (not identity.int or str(identity) != value['operation_id'] or not publication.candidate_id.int
                or publication.model_dump(mode='json') != value['publication']):
            raise ValueError
        return {**value, 'operation_id': identity, 'publication': publication}
    reference = ParticipantRolloutReference.model_validate(value['reference'])
    if reference.model_dump(mode='json') != value['reference']:
        raise ValueError
    if action == 'execute':
        if type(value['automatic']) is not bool:
            raise ValueError
    else:
        record = recovery_record(value['recovery'])
        if record != value['recovery'] or record.get('shared_pool_release') != value['reference']:
            raise ValueError
    return {**value, 'reference': reference}


def _binding(retained: RetainedParticipantRollout) -> dict[str, Any]:
    config = retained.inputs.config
    previous, = (row for row in retained.inputs.current if row['kind'] == 'ConfigMap'
        and row['metadata'].get('namespace') == config['namespace'] and row['metadata']['name'] == 'loom-platform-config')
    return {'reference': retained.reference.model_dump(mode='json'), 'guard_owner': retained.guard_owner,
        'candidate_sha': retained.inputs.candidate['candidate_sha'],
        'previous_candidate_sha': _json(previous['data']['profile.json'].encode())['candidate_sha'],
        **{key: config[key] for key in ('cluster_id', 'namespace', 'execution_namespace')},
        'environment_url': 'https://' + config['public_host']}


def _outcome(result: dict[str, Any], binding: dict[str, Any]) -> str:
    """A known status is not enough: completion must identify the actual release."""
    status = result['status']
    if status == 'complete':
        if (result.get('schema_version') != 'loom.nebius-deployment.v1' or result.get('mode') != 'apply'
                or result.get('shared_pool_release') != binding['reference']
                or any(result.get(key) != binding[key] for key in (
                    'candidate_sha', 'guard_owner', 'cluster_id', 'namespace', 'execution_namespace'))):
            raise ValueError
    elif status in _SKIPS:
        # Automatic checks return only their decision and candidate. Their
        # private activity/guard diagnostics are never part of the public report.
        if result.get('candidate_sha') != binding['candidate_sha']:
            raise ValueError
    else:
        raise ValueError
    return str(status)


def dispatch_participant_release(content: bytes, *, action: str, host: ParticipantReleaseHost) -> dict[str, Any]:
    """One qualified host call; failure never retries effects or exports errors.

    The runner must persist deployment intent before execute/recover. Transport
    loss leaves that reference for explicit recovery, not a fresh preparation.
    The existing private parent remains responsible for phase/guard authority.
    """
    stage = 'request'
    binding = None
    try:
        request = _request(content, action)
        if action == 'prepare':
            stage = 'preparation'
            retained = entry.prepare_retained_participant_release(publication=request['publication'],
                operation_id=request['operation_id'], repo_root=host.repo_root, **host.connection())
            if (retained.reference.operation_id != request['operation_id']
                    or retained.reference.publication != request['publication']):
                raise ValueError
        else:
            stage = 'selection'
            retained = load_participant_rollout(reference=request['reference'],
                state_root=host.state_root, anchor_dir=host.anchor_dir)
        if (retained.state_dir.parent != host.state_root or retained.anchor_dir != host.anchor_dir
                or load_participant_rollout(reference=retained.reference, state_root=host.state_root,
                    anchor_dir=host.anchor_dir) != retained):
            raise ValueError
        binding = _binding(retained)
        if action == 'prepare':
            return {'schema_version': REPORT_SCHEMA, 'status': 'prepared', 'release': binding}
        record = request['recovery'] if action == 'recover' else None
        if record is not None and any(record.get(key) != binding[key] for key in (
                'candidate_sha', 'previous_candidate_sha', 'guard_owner', 'cluster_id', 'namespace', 'execution_namespace')):
            raise ValueError
        stage = 'execution'
        result = entry.execute_retained_participant_release(reference=retained.reference,
            automatic=request.get('automatic', False), recovery=copy.deepcopy(record), **host.connection())
        return {'schema_version': REPORT_SCHEMA, 'status': _outcome(result, binding), 'release': binding}
    except Exception:
        return {'schema_version': REPORT_SCHEMA, 'status': 'blocked', 'stage': stage,
            **({'release': binding} if binding is not None else {})}


def main(configuration: str, checksum: str, action: str, command: str) -> int:
    """Installed process entry; only ``command`` is public, bounded data.

    The future restricted launcher must bind configuration, checksum, executable
    and tooling itself. This function does not install or expand any SSH grant.
    Deployment diagnostics remain in the parent's private evidence files.
    """
    stage = 'request'
    try:
        content = command.encode()
        _request(content, action)
        stage = 'installation'
        path = Path(configuration)
        host = load_participant_release_host(path, checksum, repo_root=Path(__file__).resolve().parents[2])
        with open(os.devnull, 'w') as private_output, redirect_stdout(private_output), redirect_stderr(private_output):
            result = dispatch_participant_release(content, action=action, host=host)
        # Replacing the protected selection during an operation cannot yield a
        # successful public acknowledgement, even if its private effect completed.
        if load_participant_release_host(path, checksum, repo_root=host.repo_root) != host:
            raise ValueError
    except Exception:
        result = {'schema_version': REPORT_SCHEMA, 'status': 'blocked', 'stage': stage}
    print(json.dumps(result, sort_keys=True, allow_nan=False))
    return 1 if result['status'] == 'blocked' else 0
