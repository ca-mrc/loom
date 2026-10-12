"""Fresh, read-only shared recovery drain before retiring successor writers.

Keep cleanup running while either side of any handoff is active. The local
receipt records an observation, never permission to reuse a historical zero.
Later mutation boundaries must call the observer again with their own current
retained writer/provider qualification.
"""
from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_development_runtime_install import _read
from scripts.ops.nebius_pool_migration_guard import KubectlPoolGuardAPI
from scripts.ops.nebius_pool_origin_history import KubectlPoolHistoryAPI
from scripts.ops.nebius_shared_pool_forward_processes import _gateway_history_target
from scripts.ops.nebius_shared_pool_handoff_live import SharedPoolGuardBindings
from scripts.ops.nebius_shared_pool_operation import RECOVERY_PHASES, _files
from scripts.ops.nebius_shared_pool_recovery_fence import (
    _recovery_frame,
    validate_shared_recovery_fence,
)

_PHASE = 'recovery-drain'


def _fenced_frame(bindings: SharedPoolGuardBindings, state: Path) -> dict[str, Any]:
    frame = _recovery_frame(bindings, state)
    if frame['recovery']['recovery-fence']['status'] != 'complete':
        raise ValueError
    validate_shared_recovery_fence(bindings=bindings, state_dir=state / 'recovery-fence', complete=True)
    return frame


def _active(bindings: SharedPoolGuardBindings, state: Path, status: str) -> dict[str, Any]:
    frame = _fenced_frame(bindings, state.parent)
    if (frame['outcome'] != 'pending' or state.name != _PHASE or status not in {'prepared', 'started'}
            or frame['recovery'][_PHASE]['status'] != status
            or any(frame['recovery'][later]['status'] != 'prepared' for later in RECOVERY_PHASES[2:])):
        raise ValueError
    return frame


def _identity(bindings: SharedPoolGuardBindings, state: Path) -> dict[str, str]:
    if state.name != _PHASE:
        raise ValueError
    return {'schema': 'loom.nebius-shared-recovery-drain.v1', 'input_digest': bindings.input_digest,
        'operation_id': str(bindings.migration.registration.spec.operation_id), 'state_dir': str(state)}


def validate_shared_recovery_drain(*, bindings: SharedPoolGuardBindings, state_dir: Path,
        complete: bool, prepared: bool = False) -> dict[str, Any]:
    frame = _fenced_frame(bindings, state_dir.parent)
    identity = _identity(bindings, state_dir)
    record = _read(state_dir / 'drain.json')
    if (type(complete) is not bool or type(prepared) is not bool or (complete and prepared)
            or set(record) != {*identity, 'status'} or any(record[key] != value for key, value in identity.items())
            or record['status'] not in {'prepared', 'observed'} or (complete and record['status'] != 'observed')
            or (prepared and (record['status'] != 'prepared' or frame['recovery'][_PHASE]['status'] != 'prepared'))):
        raise ValueError
    return record


def prepare_shared_recovery_drain(*, bindings: SharedPoolGuardBindings, state_dir: Path) -> None:
    frame = _active(bindings, state_dir, 'prepared')
    with private_state._locked_state(state_dir):
        path = state_dir / 'drain.json'
        if not path.exists() and not path.is_symlink():
            private_state._atomic_json(path, {**_identity(bindings, state_dir), 'status': 'prepared'})
        validate_shared_recovery_drain(bindings=bindings, state_dir=state_dir, complete=False, prepared=True)
        if _active(bindings, state_dir, 'prepared') != frame:
            raise ValueError


def observe_shared_recovery_drain(*, bindings: SharedPoolGuardBindings, guards: KubectlPoolGuardAPI,
        history: KubectlPoolHistoryAPI, state_dir: Path, qualify: Callable[[], None]) -> bool:
    """Read every ledger even when one is busy; recheck all terminal fences."""
    try:
        frame = _fenced_frame(bindings, state_dir)
        if frame['outcome'] != 'pending':
            raise ValueError
        files = _files(state_dir / _PHASE)

        def scope() -> None:
            if (guards.request != bindings.migration or history.request != bindings.migration
                    or guards.prefix != history.prefix or guards.kubeconfig != history.kubeconfig
                    or guards.kubeconfig_sha256 != history.kubeconfig_sha256 or not callable(qualify)):
                raise ValueError
            credential = guards._get('secret', 'loom-platform-db', bindings.migration.registration.binding.namespace)
            if _gateway_history_target(bindings, credential) != history.target:
                raise ValueError
            if _fenced_frame(bindings, state_dir) != frame or _files(state_dir / _PHASE) != files:
                raise ValueError

        def fenced() -> None:
            if history.activation_pool('observe') != 'fenced':
                raise ValueError
            for target in bindings.migration.guards:
                if guards.activation_guard(target, 'observe') != 'fenced':
                    raise ValueError

        scope()
        qualify()
        scope()
        fenced()
        values = [history.recovery_pool_drained()]
        values.extend(guards.recovery_participant_drained(target) for target in bindings.migration.guards)
        qualify()
        scope()
        fenced()
        scope()
        if any(type(value) is not bool for value in values):
            raise ValueError
        return all(values)
    except Exception:
        raise ValueError('shared recovery current drain unqualified') from None


def advance_shared_recovery_drain(*, bindings: SharedPoolGuardBindings, guards: KubectlPoolGuardAPI,
        history: KubectlPoolHistoryAPI, state_dir: Path, qualify: Callable[[], None]) -> bool:
    try:
        frame = _active(bindings, state_dir, 'started')
        with private_state._locked_state(state_dir):
            record = validate_shared_recovery_drain(bindings=bindings, state_dir=state_dir, complete=False)
            files = _files(state_dir)
            drained = observe_shared_recovery_drain(bindings=bindings, guards=guards, history=history,
                state_dir=state_dir.parent, qualify=qualify)
            if (_active(bindings, state_dir, 'started') != frame or _files(state_dir) != files
                    or validate_shared_recovery_drain(bindings=bindings, state_dir=state_dir, complete=False) != record):
                raise ValueError
            if not drained:
                return False
            private_state._atomic_json(state_dir / 'drain.json', {**_identity(bindings, state_dir), 'status': 'observed'})
            validate_shared_recovery_drain(bindings=bindings, state_dir=state_dir, complete=True)
            return True
    except Exception:
        raise ValueError('shared recovery drain unconfirmed; preserve retained evidence') from None
