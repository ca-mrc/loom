"""Terminal shared-pool fencing under separately anchored recovery authority.

The frozen forward children supply the initial observation alternatives. Only
this recovery child's own intents permit a fenced successor. No forward receipt
is rewritten, no unknown write is repeated, and gateway health is not required.
Fencing alone does not permit restoration: current drain and writer retirement
remain separate, required recovery phases.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_development_runtime_install import _read
from scripts.ops.nebius_pool_activation_stage import _guard_choices, _pool_choices
from scripts.ops.nebius_pool_migration import PoolMigrationRequest
from scripts.ops.nebius_shared_pool_activation import validate_shared_pool_activation
from scripts.ops.nebius_shared_pool_guards import validate_shared_pool_guards
from scripts.ops.nebius_shared_pool_handoff_live import SharedPoolGuardBindings
from scripts.ops.nebius_shared_pool_operation import FORWARD_PHASES, RECOVERY_PHASES, _files
from scripts.ops.nebius_shared_pool_workloads import _frame

_PHASE = 'recovery-fence'


class SharedRecoveryFenceAPI(Protocol):
    migration: PoolMigrationRequest

    def verify_retained(self) -> None:
        """Current retained identities/backends and authority, not gateway health."""
        ...
    def pool_state(self) -> str: ...
    def guard_state(self, participant: str) -> str: ...
    def fence_pool(self) -> None: ...
    def fence_guard(self, participant: str) -> None: ...


def _recovery_frame(bindings: SharedPoolGuardBindings, state: Path) -> dict[str, Any]:
    frame = _frame(bindings, state)
    manager = bindings.plan.development.runtime.request.database.manager.retained.request.retained
    marker = Path(manager.operation['anchor_dir']) / 'shared-pool-recovery.json'
    if not marker.exists() or frame['outcome'] not in {'pending', 'staging_restored_dev_closed'}:
        raise ValueError
    return frame


def _active(bindings: SharedPoolGuardBindings, state: Path, status: str) -> dict[str, Any]:
    frame = _recovery_frame(bindings, state.parent)
    if (state.name != _PHASE or status not in {'prepared', 'started'} or frame['outcome'] != 'pending'
            or frame['recovery'][_PHASE]['status'] != status
            or any(frame['recovery'][later]['status'] != 'prepared' for later in RECOVERY_PHASES[1:])):
        raise ValueError
    return frame


def _entry_choices(bindings: SharedPoolGuardBindings, state: Path) -> tuple[tuple[str, ...], dict[str, tuple[str, ...]]]:
    """The actual frozen acquisition/opening receipts, never a legacy cutover."""
    frame = _recovery_frame(bindings, state)
    options: dict[str, tuple[str, ...]] = {str(row.participant_id): ('open',) for row in bindings.migration.guards}
    pool: tuple[str, ...] = ('closed',)
    child = state / 'forward-guards'
    phase = frame['forward']['forward-guards']['status']
    if phase != 'prepared' or child.exists() or child.is_symlink():
        files = _files(child, allow_empty=phase == 'prepared')
        if files or phase != 'prepared':
            record = validate_shared_pool_guards(bindings=bindings, state_dir=child, complete=phase == 'complete')
            if phase == 'prepared' and set(record['guards'].values()) != {'prepared'}:
                raise ValueError
            options = {key: {'prepared': ('open',), 'acquire_intent': ('open', 'held'), 'held': ('held',)}[value]
                for key, value in record['guards'].items()}
    child = state / 'forward-activate'
    phase = frame['forward']['forward-activate']['status']
    if phase != 'prepared' or child.exists() or child.is_symlink():
        files = _files(child, allow_empty=phase == 'prepared')
        if files or phase != 'prepared':
            if any(frame['forward'][prior]['status'] != 'complete' for prior in FORWARD_PHASES[:FORWARD_PHASES.index('forward-activate')]):
                raise ValueError
            record = validate_shared_pool_activation(bindings=bindings, state_dir=child,
                complete=phase == 'complete', prepared=phase == 'prepared')
            pool = _pool_choices(record)
            options = {key: _guard_choices(row) for key, row in record['guards'].items()}
    if _recovery_frame(bindings, state) != frame:
        raise ValueError
    return pool, options


def _identity(bindings: SharedPoolGuardBindings, state: Path) -> dict[str, str]:
    if state.name != _PHASE:
        raise ValueError
    return {'schema': 'loom.nebius-shared-recovery-fence.v1', 'input_digest': bindings.input_digest,
        'operation_id': str(bindings.migration.registration.spec.operation_id), 'state_dir': str(state)}


def validate_shared_recovery_fence(*, bindings: SharedPoolGuardBindings, state_dir: Path,
        complete: bool, prepared: bool = False) -> dict[str, Any]:
    frame = _recovery_frame(bindings, state_dir.parent)
    identity = _identity(bindings, state_dir)
    record = _read(state_dir / 'fence.json')
    participants = tuple(str(row.participant_id) for row in bindings.migration.guards)
    if (type(complete) is not bool or type(prepared) is not bool or (complete and prepared)
            or set(record) != {*identity, 'pool', 'guards'} or any(record[key] != value for key, value in identity.items())
            or record['pool'] not in {'prepared', 'intent', 'fenced'}
            or not isinstance(record['guards'], dict) or set(record['guards']) != set(participants)
            or (prepared and (record['pool'] != 'prepared' or frame['recovery'][_PHASE]['status'] != 'prepared'))
            or (complete and record['pool'] != 'fenced')):
        raise ValueError
    unfinished = False
    for key in participants:
        status = record['guards'][key]
        if (status not in {'prepared', 'intent', 'fenced'} or (status != 'prepared' and record['pool'] != 'fenced')
                or (unfinished and status != 'prepared') or (complete and status != 'fenced')
                or (prepared and status != 'prepared')):
            raise ValueError
        unfinished |= status != 'fenced'
    return record


def prepare_shared_recovery_fence(*, bindings: SharedPoolGuardBindings, state_dir: Path) -> None:
    frame = _active(bindings, state_dir, 'prepared')
    _entry_choices(bindings, state_dir.parent)
    with private_state._locked_state(state_dir):
        path = state_dir / 'fence.json'
        if not path.exists() and not path.is_symlink():
            private_state._atomic_json(path, {**_identity(bindings, state_dir), 'pool': 'prepared',
                'guards': {str(row.participant_id): 'prepared' for row in bindings.migration.guards}})
        validate_shared_recovery_fence(bindings=bindings, state_dir=state_dir, complete=False, prepared=True)
        if _active(bindings, state_dir, 'prepared') != frame:
            raise ValueError


def advance_shared_recovery_fence(*, bindings: SharedPoolGuardBindings, api: SharedRecoveryFenceAPI, state_dir: Path) -> bool:
    """Fence capacity first, then each local guard; unknown results are read-only."""
    try:
        if api.migration != bindings.migration:
            raise ValueError
        _active(bindings, state_dir, 'started')
        with private_state._locked_state(state_dir):
            record = validate_shared_recovery_fence(bindings=bindings, state_dir=state_dir, complete=False)
            participants = tuple(str(row.participant_id) for row in bindings.migration.guards)
            entry_pool, entry_guards = _entry_choices(bindings, state_dir.parent)

            def choices(status: str, original: tuple[str, ...]) -> tuple[str, ...]:
                return ('fenced',) if status == 'fenced' else (*original, 'fenced') if status == 'intent' else original

            def observe() -> tuple[str, dict[str, str]]:
                frame = _active(bindings, state_dir, 'started')
                if (api.migration != bindings.migration
                        or validate_shared_recovery_fence(bindings=bindings, state_dir=state_dir, complete=False) != record):
                    raise ValueError
                files = _files(state_dir)
                api.verify_retained()
                mode = api.pool_state()
                states = {key: api.guard_state(key) for key in participants}
                if (mode not in choices(record['pool'], entry_pool)
                        or any(value not in choices(record['guards'][key], entry_guards[key]) for key, value in states.items())
                        or api.migration != bindings.migration or _active(bindings, state_dir, 'started') != frame
                        or _files(state_dir) != files
                        or validate_shared_recovery_fence(bindings=bindings, state_dir=state_dir, complete=False) != record):
                    raise ValueError
                return mode, states

            def save() -> None:
                _active(bindings, state_dir, 'started')
                private_state._atomic_json(state_dir / 'fence.json', record)
                validate_shared_recovery_fence(bindings=bindings, state_dir=state_dir, complete=False)

            observe()
            if record['pool'] == 'prepared':
                record['pool'] = 'intent'
                save()
                mode, _ = observe()
                if mode != 'fenced':
                    try:
                        api.fence_pool()
                    except Exception:
                        pass  # Only readback may settle this intent.
            mode, _ = observe()
            if mode != 'fenced':
                return False
            if record['pool'] != 'fenced':
                record['pool'] = 'fenced'
                save()
            for key in participants:
                if record['guards'][key] == 'fenced':
                    continue
                observe()
                if record['guards'][key] == 'prepared':
                    record['guards'][key] = 'intent'
                    save()
                    _, states = observe()
                    if states[key] != 'fenced':
                        try:
                            api.fence_guard(key)
                        except Exception:
                            pass
                _, states = observe()
                if states[key] != 'fenced':
                    return False
                record['guards'][key] = 'fenced'
                save()
            observe()
            validate_shared_recovery_fence(bindings=bindings, state_dir=state_dir, complete=True)
            return True
    except Exception:
        raise ValueError('shared recovery fence unconfirmed; preserve retained evidence') from None
