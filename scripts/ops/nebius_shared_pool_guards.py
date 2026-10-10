"""Concrete, database-bound guard child for the anchored shared handoff.

Registration is already complete. This child only acquires intake guards, never
registers again, releases a guard or changes writer authority. Only the protected
shared parent may call it after complete forward/recovery preflight.
"""
from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_development_runtime_install import _read
from scripts.ops.nebius_pool_migration import PoolGuardTarget, migration_contract
from scripts.ops.nebius_pool_migration_guard import KubectlPoolGuardAPI
from scripts.ops.nebius_shared_pool_handoff_live import SharedPoolGuardBindings
from scripts.ops.nebius_shared_pool_operation import FORWARD_PHASES, RECOVERY_PHASES

from loom.nebius_platform_render import digest


def _identity(bindings: SharedPoolGuardBindings, state: Path) -> dict[str, Any]:
    manager = bindings.plan.development.runtime.request.database.manager.retained.request.retained
    if (state != Path(manager.operation['state_dir']).parent / 'shared-pool-handoff/forward-guards'
            or not state.is_absolute() or state != state.resolve()
            or any(target.database is None for target in bindings.migration.guards)
            or bindings.input_digest != digest({'handoff': bindings.plan.input_digest,
                'migration': migration_contract(bindings.migration)})):
        raise ValueError
    return {'schema': 'loom.nebius-shared-pool-guards.v1', 'input_digest': bindings.input_digest,
        'operation_id': str(bindings.migration.registration.spec.operation_id), 'state_dir': str(state)}


def validate_shared_pool_guards(*, bindings: SharedPoolGuardBindings, state_dir: Path,
        complete: bool) -> dict[str, Any]:
    """Validate historical child semantics without demanding guards stay held forever."""
    identity = _identity(bindings, state_dir)
    record = _read(state_dir / 'guards.json')
    keys = tuple(str(target.participant_id) for target in bindings.migration.guards)
    if (type(complete) is not bool or set(record) != {*identity, 'guards'}
            or any(record[key] != value for key, value in identity.items())
            or not isinstance(record['guards'], dict) or set(record['guards']) != set(keys)):
        raise ValueError
    unfinished = False
    for key in keys:
        status = record['guards'][key]
        if (status not in {'prepared', 'acquire_intent', 'held'} or (unfinished and status != 'prepared')
                or (complete and status != 'held')):
            raise ValueError
        unfinished |= status != 'held'
    return record


def _parent(bindings: SharedPoolGuardBindings, state: Path, status: str) -> dict[str, Any]:
    identity = _identity(bindings, state)
    manager = bindings.plan.development.runtime.request.database.manager.retained.request.retained
    anchor = Path(manager.operation['anchor_dir'])
    parent_identity = {'schema': 'loom.nebius-shared-pool-handoff.v1', 'operation_id': identity['operation_id'],
        'input_digest': bindings.input_digest, 'state_dir': str(state.parent)}
    parent = {**parent_identity, 'outcome': 'pending',
        'forward': {phase: {'status': status if phase == 'forward-guards' else 'prepared', 'files': None}
            for phase in FORWARD_PHASES},
        'recovery': {phase: {'status': 'prepared', 'files': None} for phase in RECOVERY_PHASES}}
    if (status not in {'prepared', 'started'} or _read(anchor / 'shared-pool-handoff.json') != parent_identity
            or _read(state.parent / 'operation.json') != parent
            or (anchor / 'shared-pool-recovery.json').exists() or (anchor / 'shared-pool-recovery.json').is_symlink()):
        raise ValueError
    return parent


def prepare_shared_pool_guards(*, bindings: SharedPoolGuardBindings, state_dir: Path) -> None:
    """Prepare only local evidence while parent dispatch authority is absent."""
    identity = _identity(bindings, state_dir)
    _parent(bindings, state_dir, 'prepared')
    with private_state._locked_state(state_dir):
        path = state_dir / 'guards.json'
        if not path.exists() and not path.is_symlink():
            private_state._atomic_json(path, {**identity,
                'guards': {str(target.participant_id): 'prepared' for target in bindings.migration.guards}})
        record = validate_shared_pool_guards(bindings=bindings, state_dir=state_dir, complete=False)
        if set(record['guards'].values()) != {'prepared'}:
            raise ValueError
        _parent(bindings, state_dir, 'prepared')


def advance_shared_pool_guards(*, bindings: SharedPoolGuardBindings, api: KubectlPoolGuardAPI,
        state_dir: Path, qualify: Callable[[], None]) -> bool:
    """Acquire once per exact intent; unknown outcomes allow observation only."""
    try:
        identity = _identity(bindings, state_dir)

        def scope() -> None:
            if api.request != bindings.migration or _identity(bindings, state_dir) != identity:
                raise ValueError
            _parent(bindings, state_dir, 'started')
            child = validate_shared_pool_guards(bindings=bindings, state_dir=state_dir, complete=False)
            qualify()
            _parent(bindings, state_dir, 'started')
            if validate_shared_pool_guards(bindings=bindings, state_dir=state_dir, complete=False) != child:
                raise ValueError

        scope()
        with private_state._locked_state(state_dir):
            path = state_dir / 'guards.json'
            scope()
            record = validate_shared_pool_guards(bindings=bindings, state_dir=state_dir, complete=False)

            def observed(target: PoolGuardTarget) -> str:
                scope()
                report = api.guard(target, 'observe')
                if report not in ({'status': 'open'}, {'status': 'held'}):
                    raise ValueError
                return str(report['status'])

            for target in bindings.migration.guards:
                key = str(target.participant_id)
                status = record['guards'][key]
                current = observed(target)
                if status == 'prepared':
                    if current != 'open':
                        raise ValueError
                    record['guards'][key] = 'acquire_intent'
                    private_state._atomic_json(path, record)
                    scope()
                    try:
                        result = api.guard(target, 'acquire')
                    except Exception:
                        result = None
                    if result == {'status': 'skipped_busy'}:
                        if observed(target) != 'open':
                            raise ValueError
                        record['guards'][key] = 'prepared'
                        private_state._atomic_json(path, record)
                        return False
                    if result not in (None, {'status': 'acquired'}):
                        raise ValueError
                    current = observed(target)
                if current != 'held':
                    raise ValueError
                if status != 'held':
                    record['guards'][key] = 'held'
                    private_state._atomic_json(path, record)
                # Closing a later participant must not conceal an earlier guard
                # changing owner or reopening while this operation proceeds.
                for prior in bindings.migration.guards:
                    if record['guards'][str(prior.participant_id)] == 'held' and observed(prior) != 'held':
                        raise ValueError
            scope()
            validate_shared_pool_guards(bindings=bindings, state_dir=state_dir, complete=True)
            return True
    except Exception:
        raise ValueError('shared pool guards unconfirmed; preserve retained evidence') from None
