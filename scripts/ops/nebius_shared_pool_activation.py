"""Shared-parent opening and ordered intake release, with no legacy cutover.

The concrete parent must provide qualified fixed runtime/database transports.
This internal child reuses the existing one-dispatch engine, never accepts a
caller manifest, and has no standalone command or recovery dispatch authority.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_development_runtime_install import _read
from scripts.ops.nebius_pool_activation_stage import (
    PoolActivationAPI,
    _guard_choices,
    _pool_choices,
    advance_activation_record,
)
from scripts.ops.nebius_pool_migration import PoolMigrationRequest
from scripts.ops.nebius_shared_pool_handoff_live import SharedPoolGuardBindings
from scripts.ops.nebius_shared_pool_operation import FORWARD_PHASES, _files
from scripts.ops.nebius_shared_pool_workloads import _frame

_PHASE = 'forward-activate'


class SharedPoolActivationAPI(PoolActivationAPI, Protocol):
    migration: PoolMigrationRequest

    def qualify_shared_state(self) -> None:
        """Fresh receipt-bound writers, processes, provider and pool authority.

        Use CLOSED checks before opening and active checks after it, including
        uncertain opening/release alternatives. Active work must not be mistaken
        for failed initial drain. Retained history alone cannot release intake.
        """
        ...


def _identity(bindings: SharedPoolGuardBindings, state: Path) -> dict[str, str]:
    if state.name != _PHASE:
        raise ValueError
    return {'schema': 'loom.nebius-shared-pool-activation.v1', 'input_digest': bindings.input_digest,
        'operation_id': str(bindings.migration.registration.spec.operation_id), 'state_dir': str(state)}


def _active(bindings: SharedPoolGuardBindings, state: Path, status: str) -> dict[str, Any]:
    frame = _frame(bindings, state.parent)
    manager = bindings.plan.development.runtime.request.database.manager.retained.request.retained
    recovery = Path(manager.operation['anchor_dir']) / 'shared-pool-recovery.json'
    index = FORWARD_PHASES.index(_PHASE)
    if (state.name != _PHASE or status not in {'prepared', 'started'} or frame['outcome'] != 'pending'
            or recovery.exists() or recovery.is_symlink() or frame['forward'][_PHASE]['status'] != status
            or any(frame['forward'][prior]['status'] != 'complete' for prior in FORWARD_PHASES[:index])
            or any(frame['forward'][later]['status'] != 'prepared' for later in FORWARD_PHASES[index + 1:])):
        raise ValueError
    return frame


def validate_shared_pool_activation(*, bindings: SharedPoolGuardBindings, state_dir: Path,
        complete: bool, prepared: bool = False) -> dict[str, Any]:
    """Validate only this child; the parent validates every predecessor's semantics."""
    frame = _frame(bindings, state_dir.parent)
    identity = _identity(bindings, state_dir)
    record = _read(state_dir / 'activation.json')
    participants = tuple(str(target.participant_id) for target in bindings.migration.guards)
    if (type(complete) is not bool or type(prepared) is not bool or (complete and prepared)
            or (prepared and frame['forward'][_PHASE]['status'] != 'prepared')
            or set(record) != {*identity, 'opening', 'cancellation', 'guards'}
            or any(record[key] != value for key, value in identity.items())
            or record['opening'] not in {'prepared', 'intent', 'opened'}
            or record['cancellation'] != 'prepared' or not isinstance(record['guards'], dict)
            or set(record['guards']) != set(participants)
            or (prepared and record['opening'] != 'prepared') or (complete and record['opening'] != 'opened')):
        raise ValueError
    unfinished = False
    for key in participants:
        row = record['guards'][key]
        if (not isinstance(row, dict) or set(row) != {'release', 'fence'} or row['fence'] != 'prepared'
                or row['release'] not in {'prepared', 'intent', 'released'}
                or (row['release'] != 'prepared' and record['opening'] != 'opened')
                or (unfinished and row['release'] != 'prepared')
                or (prepared and row['release'] != 'prepared') or (complete and row['release'] != 'released')):
            raise ValueError
        unfinished |= row['release'] != 'released'
    return record


def prepare_shared_pool_activation(*, bindings: SharedPoolGuardBindings, state_dir: Path) -> None:
    """Create local evidence only while the parent has no dispatch authority."""
    identity = _identity(bindings, state_dir)
    frame = _active(bindings, state_dir, 'prepared')
    with private_state._locked_state(state_dir):
        path = state_dir / 'activation.json'
        if not path.exists() and not path.is_symlink():
            private_state._atomic_json(path, {**identity, 'opening': 'prepared', 'cancellation': 'prepared',
                'guards': {str(target.participant_id): {'release': 'prepared', 'fence': 'prepared'}
                    for target in bindings.migration.guards}})
        validate_shared_pool_activation(bindings=bindings, state_dir=state_dir, complete=False, prepared=True)
        if _active(bindings, state_dir, 'prepared') != frame:
            raise ValueError


def advance_shared_pool_activation(*, bindings: SharedPoolGuardBindings, api: SharedPoolActivationAPI,
        state_dir: Path) -> bool:
    """Open once and release in retained roster order; uncertainty never retries."""
    try:
        if api.migration != bindings.migration:
            raise ValueError
        _active(bindings, state_dir, 'started')
        with private_state._locked_state(state_dir):
            record = validate_shared_pool_activation(bindings=bindings, state_dir=state_dir, complete=False)
            participants = tuple(str(target.participant_id) for target in bindings.migration.guards)

            def observe() -> tuple[str, dict[str, str]]:
                frame = _active(bindings, state_dir, 'started')
                if (api.migration != bindings.migration
                        or validate_shared_pool_activation(bindings=bindings, state_dir=state_dir, complete=False) != record):
                    raise ValueError
                files = _files(state_dir)
                api.verify_retained()
                api.qualify_shared_state()
                mode = api.pool_state()
                states = {key: api.guard_state(key) for key in participants}
                if mode not in _pool_choices(record) or any(value not in _guard_choices(record['guards'][key])
                        for key, value in states.items()):
                    raise ValueError
                if (api.migration != bindings.migration or _active(bindings, state_dir, 'started') != frame or _files(state_dir) != files
                        or validate_shared_pool_activation(bindings=bindings, state_dir=state_dir, complete=False) != record):
                    raise ValueError
                return mode, states

            def save() -> None:
                _active(bindings, state_dir, 'started')
                private_state._atomic_json(state_dir / 'activation.json', record)
                validate_shared_pool_activation(bindings=bindings, state_dir=state_dir, complete=False)

            observe()
            result = advance_activation_record(api=api, record=record, participants=participants,
                observe=observe, save=save)
            status = result['status']
            if not isinstance(status, str):
                raise ValueError
            complete = status == 'pool_activation_complete'
            validate_shared_pool_activation(bindings=bindings, state_dir=state_dir, complete=complete)
            return complete
    except Exception:
        raise ValueError('shared pool activation unconfirmed; preserve retained evidence') from None
