"""Staging-only runtime ACL setup through the existing database-bound adapter.

The dev database is already configured. No registration replay, direct connection,
schema migration, credential renewal or intake release belongs to this child.
The parent requalifies current drain and writer fencing before each dispatch.
"""
from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_development_runtime_install import _read
from scripts.ops.nebius_pool_migration import PoolGuardTarget
from scripts.ops.nebius_pool_migration_guard import KubectlPoolGuardAPI
from scripts.ops.nebius_shared_pool_handoff_live import SharedPoolGuardBindings
from scripts.ops.nebius_shared_pool_operation import FORWARD_PHASES
from scripts.ops.nebius_shared_pool_workloads import _frame

_PHASE = 'forward-database'


def _target(bindings: SharedPoolGuardBindings) -> PoolGuardTarget:
    participant = bindings.plan.staging.attachment.participant
    target, = (row for row in bindings.migration.guards if row.participant_id == participant.participant_id)
    if participant.environment_class != 'staging' or target.database is None:
        raise ValueError
    return target


def _identity(bindings: SharedPoolGuardBindings) -> dict[str, str]:
    return {'schema': 'loom.nebius-shared-pool-database.v1', 'input_digest': bindings.input_digest,
        'participant_id': str(_target(bindings).participant_id)}


def _active(bindings: SharedPoolGuardBindings, state: Path, status: str) -> dict[str, Any]:
    frame = _frame(bindings, state.parent)
    manager = bindings.plan.development.runtime.request.database.manager.retained.request.retained
    recovery = Path(manager.operation['anchor_dir']) / 'shared-pool-recovery.json'
    index = FORWARD_PHASES.index(_PHASE)
    if (state.name != _PHASE or status not in {'prepared', 'started'} or frame['outcome'] != 'pending'
            or recovery.exists() or recovery.is_symlink()
            or frame['forward'][_PHASE]['status'] != status
            or any(frame['forward'][prior]['status'] != 'complete' for prior in FORWARD_PHASES[:index])
            or any(frame['forward'][later]['status'] != 'prepared' for later in FORWARD_PHASES[index + 1:])):
        raise ValueError
    return frame


def validate_shared_staging_database(*, bindings: SharedPoolGuardBindings, state_dir: Path,
        complete: bool, prepared: bool = False) -> dict[str, Any]:
    frame = _frame(bindings, state_dir.parent)
    identity = _identity(bindings)
    record = _read(state_dir / 'database.json')
    if (state_dir.name != _PHASE or type(complete) is not bool or type(prepared) is not bool
            or (complete and prepared) or set(record) != {*identity, 'status'}
            or any(record[key] != value for key, value in identity.items())
            or record['status'] not in {'prepared', 'intent', 'staged'}
            or (complete and record['status'] != 'staged')
            or (prepared and (record['status'] != 'prepared' or frame['forward'][_PHASE]['status'] != 'prepared'))):
        raise ValueError
    return record


def prepare_shared_staging_database(*, bindings: SharedPoolGuardBindings, state_dir: Path) -> None:
    frame = _active(bindings, state_dir, 'prepared')
    with private_state._locked_state(state_dir):
        path = state_dir / 'database.json'
        if not path.exists() and not path.is_symlink():
            private_state._atomic_json(path, {**_identity(bindings), 'status': 'prepared'})
        validate_shared_staging_database(bindings=bindings, state_dir=state_dir, complete=False, prepared=True)
        if _active(bindings, state_dir, 'prepared') != frame:
            raise ValueError


def advance_shared_staging_database(*, bindings: SharedPoolGuardBindings, api: KubectlPoolGuardAPI,
        state_dir: Path, qualify: Callable[[], None]) -> bool:
    """One stage transaction; an uncertain result permits qualified readback only."""
    try:
        identity, target = _identity(bindings), _target(bindings)

        def scope() -> None:
            if api.request != bindings.migration or _identity(bindings) != identity:
                raise ValueError
            frame = _active(bindings, state_dir, 'started')
            child = validate_shared_staging_database(bindings=bindings, state_dir=state_dir, complete=False)
            qualify()
            if (_active(bindings, state_dir, 'started') != frame
                    or validate_shared_staging_database(bindings=bindings, state_dir=state_dir, complete=False) != child
                    or api.request != bindings.migration or _identity(bindings) != identity):
                raise ValueError

        scope()
        with private_state._locked_state(state_dir):
            scope()
            record = validate_shared_staging_database(bindings=bindings, state_dir=state_dir, complete=False)
            path = state_dir / 'database.json'
            if record['status'] == 'prepared':
                record['status'] = 'intent'
                private_state._atomic_json(path, record)
                scope()
                try:
                    api.runtime_role(target, 'stage')
                except Exception:
                    pass  # The fixed DB adapter may have committed before its response was lost.
            scope()
            if api.runtime_role(target, 'observe') != {'status': 'qualified'}:
                raise ValueError
            scope()
            if _read(path) != record:
                raise ValueError
            if record['status'] != 'staged':
                record['status'] = 'staged'
                private_state._atomic_json(path, record)
            validate_shared_staging_database(bindings=bindings, state_dir=state_dir, complete=True)
            return True
    except Exception:
        raise ValueError('shared staging database setup unconfirmed; preserve retained evidence') from None
