"""Anchored sequencing for the shared-pool handoff and terminal recovery.

This internal parent is not a deployment entry point. Its concrete adapter must
implement *both* directions and qualify current authority at each boundary. A
child owns intent-before-dispatch and uncertain-result readback; a parent phase
marker alone never makes a remote write safe to retry.
"""
from __future__ import annotations

import copy
import hashlib
import os
import stat
from pathlib import Path
from typing import Any, Literal, Protocol

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_development_runtime_install import DevelopmentRuntimeInstallRequest, _read
from scripts.ops.nebius_ingress_stage import _key, _uid
from scripts.ops.nebius_management_switch import _matches
from scripts.ops.nebius_pool_migration import migration_contract
from scripts.ops.nebius_pool_predecessor import CompletedPoolCutover
from scripts.ops.nebius_shared_pool_handoff import prepare_shared_pool_handoff
from scripts.ops.nebius_shared_pool_handoff_live import SharedPoolGuardBindings

from loom.nebius_platform_render import digest

FORWARD_PHASES = (
    'forward-guards', 'forward-drain', 'forward-stop', 'forward-roles',
    'forward-database', 'forward-material', 'forward-wire', 'forward-start',
    'forward-build-policy', 'forward-gateway', 'forward-activate', 'forward-verify',
)
RECOVERY_PHASES = (
    'recovery-fence', 'recovery-drain', 'recovery-stop', 'recovery-revoke',
    'recovery-restore-workloads', 'recovery-restore-roles', 'recovery-restart',
    'recovery-reopen', 'recovery-verify',
)
_COMPLETE = 'shared_pool_handoff_complete'
_RECOVERED = 'staging_restored_dev_closed'


class SharedPoolOperationAPI(Protocol):
    """Protected, phase-aware observations and durable child operations.

    ``preflight`` combines live writer, provider and SQL/process observations,
    qualifies current publication and requires absence of all new fixed targets.
    ``qualify`` must instead accept only the anchored phase's exact old/new
    choices after a transition; initial open-guard checks cannot be replayed.
    It rechecks current drain and authority before dependent mutations, never
    treating an earlier zero backlog as durable permission to write.

    ``validate_phase`` verifies the child's semantic identity, not just its
    bytes. ``advance`` persists each mutation's intent before dispatch and reads
    uncertain results on resumption. True means the whole child is complete;
    False means pending, never permission to skip to the following phase.
    """

    supported_phases: frozenset[str]

    def preflight(self, bindings: SharedPoolGuardBindings) -> None: ...
    def qualify(self, bindings: SharedPoolGuardBindings, record: dict[str, Any], *, recovering: bool) -> None: ...
    def validate_phase(self, phase: str, state: Path, *, complete: bool) -> None: ...
    def advance(self, phase: str, bindings: SharedPoolGuardBindings, state: Path) -> bool: ...


def _directory(path: Path) -> None:
    info = path.lstat()
    if (path != path.resolve() or not stat.S_ISDIR(info.st_mode)
            or info.st_uid != os.getuid() or info.st_mode & 0o077):
        raise ValueError


def _files(state: Path) -> dict[str, str]:
    """Pin every child receipt, including nested intents, not only its summary."""
    _directory(state)
    result = {}
    for index, path in enumerate(sorted(state.rglob('*'))):
        if index >= 2048 or path != path.resolve() or path.is_symlink():
            raise ValueError
        if path.is_dir():
            _directory(path)
            continue
        raw = private_state._private_read(path, limit=4 * 1024**2)
        # Child engines retain their empty flock files, which are not receipts.
        if path.name == 'operation.lock':
            if raw:
                raise ValueError
            continue
        result[str(path.relative_to(state))] = hashlib.sha256(raw).hexdigest()
    if not result:
        raise ValueError
    return result


def _qualify_bindings(*, runtime: DevelopmentRuntimeInstallRequest, observation: dict[str, Any],
        legacy: CompletedPoolCutover, repo_root: Path, bindings: SharedPoolGuardBindings) -> None:
    plan = prepare_shared_pool_handoff(runtime=runtime, observation=observation, legacy=legacy, repo_root=repo_root)
    migration = bindings.migration
    if (bindings.plan != plan or migration.registration != plan.migration.registration
            or bindings.input_digest != digest({'handoff': plan.input_digest, 'migration': migration_contract(migration)})):
        raise ValueError
    expected = {row.participant_id: row for row in plan.migration.guards}
    for guard in migration.guards:
        original = expected[guard.participant_id]
        if (guard.database is None or (guard.namespace, guard.namespace_uid) != (original.namespace, original.namespace_uid)
                or not _matches(guard.controller, original.controller, _uid(original.controller))):
            raise ValueError
        if original.database is not None:
            if guard.database != original.database:
                raise ValueError
            continue
        # Only the dev guard lacked a database in the pure handoff projection.
        # Its live binding must still refer to the actually installed database,
        # service and credentials; a self-consistent caller digest is not enough.
        foundation = runtime.database.foundation
        if guard.namespace != foundation.binding.bootstrap.namespace:
            raise ValueError
        for row in (guard.database.statefulset, guard.database.service):
            item, = (entry for entry in foundation.phases['database']['resources'].values()
                if _key(entry['observed']) == _key(row))
            if not _matches(row, item['observed'], item['uid']):
                raise ValueError
        if (str(guard.database.credential_uid) != foundation.bootstrap['secrets']['loom-platform-db']['uid']
                or str(guard.database.actuator_credential_uid) != _uid(plan.development.runtime.resources[_key(runtime.database.material[1])])):
            raise ValueError


def _history(bindings: SharedPoolGuardBindings) -> None:
    for path, checksum in bindings.plan.history.items():
        if (path != path.resolve()
                or hashlib.sha256(private_state._private_read(path, limit=4 * 1024**2)).hexdigest() != checksum):
            raise ValueError


def _frozen_forward(state: Path, record: dict[str, Any]) -> dict[str, Any]:
    return {'forward_sha256': digest(record['forward']), 'forward_files': {
        phase: _files(state / phase) for phase, item in record['forward'].items()
        if item['status'] != 'prepared'}}


def _record(*, state: Path, marker: Path, recovery_marker: Path, identity: dict[str, Any],
        api: SharedPoolOperationAPI) -> tuple[dict[str, Any], bool]:
    if not marker.exists() and not marker.is_symlink():
        if state.exists() or state.is_symlink() or recovery_marker.exists() or recovery_marker.is_symlink():
            raise ValueError
        return {**identity, 'outcome': 'pending', 'forward': {phase: {'status': 'prepared', 'files': None}
            for phase in FORWARD_PHASES}, 'recovery': {phase: {'status': 'prepared', 'files': None}
            for phase in RECOVERY_PHASES}}, False
    if _read(marker) != identity:
        raise ValueError
    _directory(state)
    record = _read(state / 'operation.json')
    if (set(record) != {*identity, 'outcome', 'forward', 'recovery'}
            or any(record[key] != value for key, value in identity.items())
            or record['outcome'] not in {'pending', _COMPLETE, _RECOVERED}):
        raise ValueError
    recovering = recovery_marker.exists() or recovery_marker.is_symlink()
    if recovering:
        if _read(recovery_marker) != {**identity, **_frozen_forward(state, record)} or record['outcome'] == _COMPLETE:
            raise ValueError
    elif record['outcome'] == _RECOVERED:
        raise ValueError
    permitted = {'operation.json'}
    for direction, phases in (('forward', FORWARD_PHASES), ('recovery', RECOVERY_PHASES)):
        sequence = record[direction]
        if not isinstance(sequence, dict) or set(sequence) != set(phases):
            raise ValueError
        unfinished = False
        for phase in phases:
            item = sequence[phase]
            if (not isinstance(item, dict) or set(item) != {'status', 'files'}
                    or item['status'] not in {'prepared', 'started', 'complete'}
                    or (unfinished and item['status'] != 'prepared')
                    or (direction == 'recovery' and not recovering and item['status'] != 'prepared')):
                raise ValueError
            child = state / phase
            if item['status'] == 'prepared':
                if item['files'] is not None or child.exists() or child.is_symlink():
                    raise ValueError
            else:
                permitted.add(phase)
                complete = item['status'] == 'complete'
                files = _files(child)
                if (complete and files != item['files']) or (not complete and item['files'] is not None):
                    raise ValueError
                api.validate_phase(phase, child, complete=complete)
                if _files(child) != files:
                    raise ValueError
            unfinished |= item['status'] != 'complete'
        if record['outcome'] == (_COMPLETE if direction == 'forward' else _RECOVERED) and unfinished:
            raise ValueError
    if {path.name for path in state.iterdir()} != permitted:
        raise ValueError
    return record, recovering


def run_shared_pool_operation(*, runtime: DevelopmentRuntimeInstallRequest, observation: dict[str, Any],
        legacy: CompletedPoolCutover, repo_root: Path, bindings: SharedPoolGuardBindings,
        api: SharedPoolOperationAPI, action: Literal['preflight', 'install', 'recover']) -> dict[str, Any]:
    """Advance only an exact anchored operation; never replay completed children.

    Recovery freezes the entire forward record in an independent marker before
    any recovery effect. Its presence selects that direction even if the process
    dies before the next journal update. Successful operations require a separate
    protected successor for later rollback; cancellation cannot rewrite success.
    """
    try:
        if action not in {'preflight', 'install', 'recover'} or api.supported_phases != frozenset((*FORWARD_PHASES, *RECOVERY_PHASES)):
            raise ValueError
        bindings = copy.deepcopy(bindings)
        _qualify_bindings(runtime=runtime, observation=observation, legacy=legacy, repo_root=repo_root, bindings=bindings)
        manager = runtime.database.manager.retained.request.retained
        state = Path(manager.operation['state_dir']).parent / 'shared-pool-handoff'
        anchor = Path(manager.operation['anchor_dir'])
        if not state.is_absolute() or state != state.resolve() or not anchor.is_absolute():
            raise ValueError
        _directory(anchor)
        marker, recovery_marker = anchor / 'shared-pool-handoff.json', anchor / 'shared-pool-recovery.json'
        identity = {'schema': 'loom.nebius-shared-pool-handoff.v1',
            'operation_id': str(bindings.migration.registration.spec.operation_id),
            'state_dir': str(state), 'input_digest': bindings.input_digest}
        base = {'operation_id': identity['operation_id'], 'pool_id': str(bindings.migration.registration.spec.pool_id),
            'admission_open': False, 'writer_migration_complete': False}

        def read() -> tuple[dict[str, Any], bool]:
            _history(bindings)
            result = _record(state=state, marker=marker, recovery_marker=recovery_marker, identity=identity, api=api)
            _history(bindings)
            return result

        def qualify(record: dict[str, Any], recovering: bool) -> None:
            _history(bindings)
            api.qualify(copy.deepcopy(bindings), copy.deepcopy(record), recovering=recovering)
            _history(bindings)

        record, recovering = read()
        if action == 'preflight':
            if marker.exists():
                qualify(record, recovering)
            else:
                api.preflight(copy.deepcopy(bindings))
            if read() != (record, recovering):
                raise ValueError
            return {**base, 'status': 'shared_pool_preflight_qualified',
                'admission_open': record['outcome'] == _COMPLETE, 'writer_migration_complete': record['outcome'] == _COMPLETE}
        # The lock is outside every completed installation history and prevents
        # install/recovery from competing across local protected invocations.
        with private_state._locked_state(anchor / 'shared-pool-handoff-lock'):
            record, recovering = read()
            if (action == 'install' and recovering) or (action == 'recover' and (not marker.exists() or record['outcome'] == _COMPLETE)):
                raise ValueError
            if not marker.exists():
                api.preflight(copy.deepcopy(bindings))
                if read() != (record, recovering):
                    raise ValueError
                private_state._atomic_json(marker, identity)
                private_state._private_directory(state)
                private_state._atomic_json(state / 'operation.json', record)
            qualify(record, recovering)
            if read() != (record, recovering):
                raise ValueError
            if action == 'recover' and not recovering:
                private_state._atomic_json(recovery_marker, {**identity, **_frozen_forward(state, record)})
                recovering = True
            phases = RECOVERY_PHASES if recovering else FORWARD_PHASES
            direction = 'recovery' if recovering else 'forward'
            for phase in phases:
                if record[direction][phase]['status'] == 'complete':
                    continue
                qualify(record, recovering)
                if read() != (record, recovering):
                    raise ValueError
                item = record[direction][phase]
                if item['status'] == 'prepared':
                    item['status'] = 'started'
                    private_state._atomic_json(state / 'operation.json', record)
                complete = api.advance(phase, copy.deepcopy(bindings), state / phase)
                if type(complete) is not bool or read() != (record, recovering):
                    raise ValueError
                qualify(record, recovering)
                if read() != (record, recovering):
                    raise ValueError
                if not complete:
                    return {**base, 'status': 'pending', 'phase': phase}
                api.validate_phase(phase, state / phase, complete=True)
                item.update(status='complete', files=_files(state / phase))
                private_state._atomic_json(state / 'operation.json', record)
                read()
            qualify(record, recovering)
            if read() != (record, recovering):
                raise ValueError
            record['outcome'] = _RECOVERED if recovering else _COMPLETE
            private_state._atomic_json(state / 'operation.json', record)
            read()
            return {**base, 'status': record['outcome'], 'admission_open': not recovering,
                'writer_migration_complete': not recovering}
    except Exception:
        raise ValueError('shared pool operation unconfirmed; preserve retained evidence') from None
