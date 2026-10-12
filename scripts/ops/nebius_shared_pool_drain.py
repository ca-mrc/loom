"""Fresh closed-pool drain and retained backlog qualification for shared handoff.

These are real read-only database observations, never queue cancellation or
permission to reuse a historical zero. The parent also owns current writer and
process qualification; it must call the observation again before dependent writes.
"""
from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_development_runtime_install import _read
from scripts.ops.nebius_development_runtime_probes import HTTPSDevelopmentRuntimeProbes
from scripts.ops.nebius_pool_migration_guard import (
    KubectlPoolGuardAPI,
    qualify_cutover_readiness_page,
)
from scripts.ops.nebius_pool_origin_history import (
    pool_management_history_sql,
    qualify_management_history_page,
)
from scripts.ops.nebius_shared_pool_handoff_live import SharedPoolGuardBindings
from scripts.ops.nebius_shared_pool_operation import FORWARD_PHASES
from scripts.ops.nebius_shared_pool_workloads import _frame

_PHASE = 'forward-drain'


def _closed_frame(bindings: SharedPoolGuardBindings, state: Path) -> dict[str, Any]:
    frame = _frame(bindings, state)
    manager = bindings.plan.development.runtime.request.database.manager.retained.request.retained
    recovery = Path(manager.operation['anchor_dir']) / 'shared-pool-recovery.json'
    if (frame['outcome'] != 'pending' or frame['forward']['forward-guards']['status'] != 'complete'
            or recovery.exists() or recovery.is_symlink()):
        raise ValueError
    return frame


def _active(bindings: SharedPoolGuardBindings, state: Path, status: str) -> dict[str, Any]:
    frame = _closed_frame(bindings, state.parent)
    index = FORWARD_PHASES.index(_PHASE)
    if (state.name != _PHASE or status not in {'prepared', 'started'} or frame['forward'][_PHASE]['status'] != status
            or any(frame['forward'][later]['status'] != 'prepared' for later in FORWARD_PHASES[index + 1:])):
        raise ValueError
    return frame


def _identity(bindings: SharedPoolGuardBindings) -> dict[str, str]:
    return {'schema': 'loom.nebius-shared-pool-drain.v1', 'input_digest': bindings.input_digest}


def validate_shared_pool_drain(*, bindings: SharedPoolGuardBindings, state_dir: Path,
        complete: bool, prepared: bool = False) -> dict[str, Any]:
    frame = _frame(bindings, state_dir.parent)
    identity = _identity(bindings)
    record = _read(state_dir / 'drain.json')
    if (state_dir.name != _PHASE or type(complete) is not bool or type(prepared) is not bool
            or (complete and prepared) or set(record) != {*identity, 'status', 'pending_origins'}
            or any(record[key] != value for key, value in identity.items())
            or record['status'] not in {'prepared', 'observed'}
            or (complete and record['status'] != 'observed')
            or (prepared and (record['status'] != 'prepared' or frame['forward'][_PHASE]['status'] != 'prepared'))):
        raise ValueError
    if record['status'] == 'prepared':
        if record['pending_origins'] is not None:
            raise ValueError
    elif (not isinstance(record['pending_origins'], dict)
            or set(record['pending_origins']) != {str(row.participant_id) for row in bindings.migration.guards}
            or any(type(count) is not int or count < 0 for count in record['pending_origins'].values())):
        raise ValueError
    return record


def prepare_shared_pool_drain(*, bindings: SharedPoolGuardBindings, state_dir: Path) -> None:
    frame = _active(bindings, state_dir, 'prepared')
    with private_state._locked_state(state_dir):
        path = state_dir / 'drain.json'
        if not path.exists() and not path.is_symlink():
            private_state._atomic_json(path, {**_identity(bindings), 'status': 'prepared', 'pending_origins': None})
        validate_shared_pool_drain(bindings=bindings, state_dir=state_dir, complete=False, prepared=True)
        if _active(bindings, state_dir, 'prepared') != frame:
            raise ValueError


def observe_closed_shared_pool_drain(*, bindings: SharedPoolGuardBindings, guards: KubectlPoolGuardAPI,
        development: HTTPSDevelopmentRuntimeProbes, state_dir: Path, qualify: Callable[[], None]) -> dict[str, int]:
    """Re-read all participant pages and the distinct management source history.

    Held intake and exact CLOSED authority are required on both sides. Pending
    qualified origins are retained, not required to be empty. Successful old
    receipts are not consulted as evidence of present database drain.
    """
    try:
        frame = _closed_frame(bindings, state_dir)
        runtime = bindings.plan.development.runtime.request
        spec = bindings.migration.registration.spec
        manager_namespace = bindings.migration.registration.binding.namespace

        def scope() -> None:
            if guards.request != bindings.migration or development.request != runtime:
                raise ValueError
            qualify()
            if (guards.request != bindings.migration or development.request != runtime
                    or _closed_frame(bindings, state_dir) != frame):
                raise ValueError

        def held() -> None:
            for target in bindings.migration.guards:
                if guards.guard(target, 'observe') != {'status': 'held'}:
                    raise ValueError

        scope()
        held()
        development.qualify_closed_pool()
        result = {}
        for target in bindings.migration.guards:
            participant, = (row for row in spec.participants if row.participant_id == target.participant_id)
            cursor, count = None, 0
            while True:
                scope()
                page = guards.cutover_readiness_page(target, after=cursor)
                origins = qualify_cutover_readiness_page(page, participant=participant, after=cursor)
                before = development._database(manager_namespace)
                report = development._exec(before, 'loom-postgres',
                    ['psql', '-X', '-qAt', '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', 'loom', '-c',
                        pool_management_history_sql(origins)], max_output_bytes=4 * 1024**2)
                qualify_management_history_page(report, origins=origins, participant=participant, cluster_id=spec.cluster_id)
                if development._process_identity(development._database(manager_namespace)) != development._process_identity(before):
                    raise ValueError
                count += len(origins)
                if len(page['rows']) < 128:
                    break
                cursor = page['rows'][-1]['key']
            result[str(participant.participant_id)] = count
        development.qualify_closed_pool()
        held()
        scope()
        return result
    except Exception:
        raise ValueError('shared pool current drain unqualified') from None


def advance_shared_pool_drain(*, bindings: SharedPoolGuardBindings, guards: KubectlPoolGuardAPI,
        development: HTTPSDevelopmentRuntimeProbes, state_dir: Path, qualify: Callable[[], None]) -> bool:
    try:
        frame = _active(bindings, state_dir, 'started')
        with private_state._locked_state(state_dir):
            record = validate_shared_pool_drain(bindings=bindings, state_dir=state_dir, complete=False)

            def scope() -> None:
                if validate_shared_pool_drain(bindings=bindings, state_dir=state_dir, complete=False) != record:
                    raise ValueError
                qualify()
                if validate_shared_pool_drain(bindings=bindings, state_dir=state_dir, complete=False) != record:
                    raise ValueError

            pending = observe_closed_shared_pool_drain(bindings=bindings, guards=guards, development=development,
                state_dir=state_dir.parent, qualify=scope)
            if _active(bindings, state_dir, 'started') != frame:
                raise ValueError
            private_state._atomic_json(state_dir / 'drain.json', {**_identity(bindings),
                'status': 'observed', 'pending_origins': pending})
            validate_shared_pool_drain(bindings=bindings, state_dir=state_dir, complete=True)
            return True
    except Exception:
        raise ValueError('shared pool drain unconfirmed; preserve retained evidence') from None
